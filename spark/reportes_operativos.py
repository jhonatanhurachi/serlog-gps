"""
Serlog GPS - Pipeline de reportes operativos con Apache Spark (PySpark).

Lee el histórico completo de gps_data desde PostgreSQL (solo lectura, vía JDBC
particionado por timestamp) y genera, por vehículo:

  1. reporte_diario           km, velocidades, tiempo en movimiento/detenido,
                              eventos de geocerca y métricas de calidad por día.
  2. eventos_geocerca_diario  entradas/salidas por vehículo, día y geocerca.
  3. reporte_semanal          tendencias semanales (semana ISO, lunes).
  4. reporte_mensual          tendencias mensuales.
  5. calidad_datos_diaria     puntos, null island y puntos sin asignación por
                              dispositivo y día.
  6. control_cruzado          km de Spark vs. la misma lógica ejecutada en SQL.

Todos se exportan como Parquet y CSV en --salida. NUNCA escribe en PostgreSQL.

Reglas de calidad de datos (las mismas de
sql/functions/001_fn_distancia_recorrida.sql):
  * gps_data."timestamp" está en UTC; los días se calculan en America/Lima.
  * El vehículo se resuelve SOLO vía device_assignments, con rango semiabierto
    [assigned_from, assigned_to). gps_data.vehicle_id no se usa.
  * Los puntos POINT(0 0) (sin señal satelital) se excluyen de todo cálculo.
  * El 31/12/2025 (UTC) no tiene datos por un hueco en las particiones de gps_data.

Conexión (en este orden de prioridad; las credenciales nunca se imprimen):
  1. DATABASE_URL_READONLY = postgresql://usuario:clave@host:puerto/base?sslmode=require
  2. Variables DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE, DB_SSLMODE
  3. Archivo .env con esas mismas claves (ruta en ENV_FILE; por defecto ./.env)

Uso:
  python spark/reportes_operativos.py --salida spark/output
  python spark/reportes_operativos.py --desde 2026-09-01 --hasta 2026-09-27   # rango acotado

--desde/--hasta son fechas LOCALES (America/Lima); --hasta es exclusivo.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.parse import parse_qs, unquote, urlparse

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# --------------------------------------------------------------------------- #
# Parámetros de negocio (documentados en el README, sección Spark)
# --------------------------------------------------------------------------- #
ZONA_HORARIA = "America/Lima"

# Umbral de movimiento. El equipo GPS ya reporta 0 km/h al estar detenido (36 %
# de los puntos) y casi no hay lecturas entre 0 y 3 km/h; 5 km/h absorbe el
# jitter residual sin descartar maniobras reales a baja velocidad.
UMBRAL_MOVIMIENTO_KMH = 5.0

# Intervalo máximo entre dos puntos para contarlo como tiempo operativo. El
# muestreo normal es de 5 s (p99.9 = 85 s); un hueco mayor a 5 min significa
# equipo apagado o sin cobertura y se contabiliza como "sin datos".
MAX_INTERVALO_S = 300

# Velocidad implícita (distancia/tiempo entre puntos consecutivos) por encima de
# la cual un salto se considera físicamente imposible. Solo se reporta como
# métrica de calidad; no se excluye, para mantener la misma lógica que la
# función SQL fn_distancia_recorrida.
VEL_IMPLICITA_IMPOSIBLE_KMH = 150.0

# Un día cuenta como "activo" si recorrió al menos esta distancia EN MOVIMIENTO
# (km_en_movimiento), para que el jitter de un vehículo estacionado no lo active.
KM_MINIMO_DIA_ACTIVO = 1.0

JDBC_FETCHSIZE = 10_000
JDBC_DRIVER = "org.postgresql.Driver"

CONTROL_CRUZADO_DEFECTO = "1:2025-08-12,10:2025-08-13,8:2026-09-20"


# --------------------------------------------------------------------------- #
# Configuración de conexión
# --------------------------------------------------------------------------- #
def _leer_env_file(ruta: Path) -> dict[str, str]:
    """Parser tolerante de .env: acepta 'CLAVE = valor', comillas y CRLF."""
    valores: dict[str, str] = {}
    if not ruta.is_file():
        return valores
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, valor = linea.split("=", 1)
        valores[clave.strip()] = valor.strip().strip('"').strip("'")
    return valores


def configuracion_jdbc() -> tuple[str, dict[str, str]]:
    """Devuelve (jdbc_url, propiedades). La clave va en propiedades, no en la URL."""
    url = os.environ.get("DATABASE_URL_READONLY", "").strip()
    if url:
        u = urlparse(url)
        sslmode = parse_qs(u.query).get("sslmode", ["require"])[0]
        host, port = u.hostname, u.port or 5432
        base = u.path.lstrip("/")
        usuario, clave = unquote(u.username or ""), unquote(u.password or "")
    else:
        env = _leer_env_file(Path(os.environ.get("ENV_FILE", ".env")))
        get = lambda k, d="": os.environ.get(k) or env.get(k) or d  # noqa: E731
        host, port, base = get("DB_HOST"), get("DB_PORT", "5432"), get("DB_DATABASE")
        usuario, clave, sslmode = get("DB_USERNAME"), get("DB_PASSWORD"), get("DB_SSLMODE", "require")

    faltantes = [n for n, v in {"host": host, "base": base, "usuario": usuario, "clave": clave}.items() if not v]
    if faltantes:
        sys.exit(f"Faltan datos de conexión: {', '.join(faltantes)}. "
                 "Definir DATABASE_URL_READONLY o las variables DB_* (ver README).")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{base}?sslmode={sslmode}"
    props = {
        "user": usuario,
        "password": clave,
        "driver": JDBC_DRIVER,
        "fetchsize": str(JDBC_FETCHSIZE),
        # Segunda barrera además de los permisos del usuario: sesión de solo lectura.
        "sessionInitStatement": "SET default_transaction_read_only = on",
    }
    return jdbc_url, props


# --------------------------------------------------------------------------- #
# Spark
# --------------------------------------------------------------------------- #
def crear_spark(memoria_driver: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName("serlog-reportes-operativos")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", memoria_driver)
        # gps_data."timestamp" es timestamp SIN zona en UTC: la JVM y la sesión
        # deben estar en UTC para que JDBC no desplace las horas.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
    )
    jar = os.environ.get("JDBC_JAR")
    if jar:
        builder = builder.config("spark.jars", jar)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def leer_tabla(spark: SparkSession, url: str, props: dict, consulta: str) -> DataFrame:
    return spark.read.jdbc(url, f"({consulta}) AS q", properties=props)


def inicio_dia_utc(fecha_local: str) -> str:
    """Medianoche local de fecha_local expresada en UTC ('AAAA-MM-DD HH:MM:SS')."""
    local = datetime.fromisoformat(fecha_local).replace(tzinfo=ZoneInfo(ZONA_HORARIA))
    return local.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def leer_gps(spark: SparkSession, url: str, props: dict, desde_utc: str, hasta_utc: str,
             particiones: int) -> DataFrame:
    """Lee gps_data en paralelo, particionado por timestamp.

    partitionColumn + fetchsize: cada partición de Spark es una consulta con su
    propio rango de tiempo, y el driver JDBC trae las filas en lotes de
    JDBC_FETCHSIZE en lugar de cargar el resultado completo en memoria.
    numPartitions también limita las conexiones simultáneas contra la BD.
    La geografía se convierte a lon/lat en PostgreSQL para no transferir WKB.
    """
    consulta = f"""
        SELECT device_id,
               "timestamp"                  AS ts,
               speed,
               ST_X(location::geometry)     AS lon,
               ST_Y(location::geometry)     AS lat
          FROM gps_data
         WHERE "timestamp" >= TIMESTAMP '{desde_utc}'
           AND "timestamp" <  TIMESTAMP '{hasta_utc}'
    """
    # Se usa .format("jdbc") y no spark.read.jdbc(): este último convierte
    # lowerBound/upperBound a int y no admite una columna timestamp.
    lector = spark.read.format("jdbc").option("url", url)
    for clave, valor in props.items():
        lector = lector.option(clave, valor)
    return (
        lector.option("dbtable", f"({consulta}) AS g")
        .option("partitionColumn", "ts")
        .option("lowerBound", desde_utc)
        .option("upperBound", hasta_utc)
        .option("numPartitions", particiones)
        .load()
    )


# --------------------------------------------------------------------------- #
# Distancia geodésica (WGS84), equivalente a ST_Length/ST_Distance en geography
# --------------------------------------------------------------------------- #
_GEOD = None


@F.pandas_udf(DoubleType())  # tipo explícito: un string DDL exigiría una sesión activa al importar
def distancia_geodesica_m(lon1: pd.Series, lat1: pd.Series, lon2: pd.Series, lat2: pd.Series) -> pd.Series:
    """Distancia sobre el elipsoide WGS84 con GeographicLib (pyproj), el mismo
    algoritmo que usa PostGIS para geography. NULL si falta el punto anterior."""
    global _GEOD
    if _GEOD is None:
        from pyproj import Geod
        _GEOD = Geod(ellps="WGS84")
    salida = np.full(len(lon1), np.nan)
    ok = (lon1.notna() & lat1.notna() & lon2.notna() & lat2.notna()).to_numpy()
    if ok.any():
        _, _, d = _GEOD.inv(lon1.to_numpy()[ok], lat1.to_numpy()[ok],
                            lon2.to_numpy()[ok], lat2.to_numpy()[ok])
        salida[ok] = d
    return pd.Series(salida)


# --------------------------------------------------------------------------- #
# Transformaciones
# --------------------------------------------------------------------------- #
def resolver_vehiculo(gps: DataFrame, asignaciones: DataFrame) -> DataFrame:
    """Agrega fecha local y asigna cada punto a su asignación vigente.

    Rango semiabierto [assigned_from, assigned_to) sobre la fecha LOCAL: el día
    de un traspaso cuenta para el vehículo nuevo (igual que la función SQL).
    Los puntos sin asignación quedan con assignment_id NULL (métrica de calidad).
    """
    puntos = (
        gps.withColumn("ts_local", F.from_utc_timestamp("ts", ZONA_HORARIA))
        .withColumn("fecha", F.to_date("ts_local"))
        .withColumn("null_island", (F.col("lon") == 0) & (F.col("lat") == 0))
    )
    a = asignaciones.alias("a")
    cond = (
        (F.col("p.device_id") == F.col("a.device_id"))
        & (F.col("p.fecha") >= F.col("a.assigned_from"))
        & (F.col("a.assigned_to").isNull() | (F.col("p.fecha") < F.col("a.assigned_to")))
    )
    return (
        puntos.alias("p")
        .join(F.broadcast(a), cond, "left")
        .select("p.*", F.col("a.id").alias("assignment_id"), F.col("a.vehicle_id"))
    )


def calcular_segmentos(puntos_validos: DataFrame) -> DataFrame:
    """Une cada punto con el anterior de la misma asignación y el mismo día local.

    Es la misma línea que arma ST_MakeLine(... ORDER BY timestamp) GROUP BY da.id
    en la función SQL: los segmentos no cruzan asignaciones ni días.
    """
    w = Window.partitionBy("assignment_id", "fecha").orderBy("ts")
    seg = (
        puntos_validos
        .withColumn("prev_ts", F.lag("ts").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_speed", F.lag("speed").over(w))
        .withColumn("dist_m", distancia_geodesica_m("prev_lon", "prev_lat", "lon", "lat"))
        .withColumn("dt_s", F.col("ts").cast("double") - F.col("prev_ts").cast("double"))
    )
    intervalo_valido = F.col("dt_s").isNotNull() & (F.col("dt_s") <= MAX_INTERVALO_S)
    en_movimiento = F.coalesce(F.col("prev_speed"), F.lit(0.0)) >= UMBRAL_MOVIMIENTO_KMH
    return (
        seg
        .withColumn("s_movimiento", F.when(intervalo_valido & en_movimiento, F.col("dt_s")).otherwise(0.0))
        .withColumn("s_detenido", F.when(intervalo_valido & ~en_movimiento, F.col("dt_s")).otherwise(0.0))
        # Distancia sin jitter: solo segmentos donde alguno de los dos extremos supera el umbral.
        .withColumn("dist_movimiento_m",
                    F.when(en_movimiento | (F.col("speed") >= UMBRAL_MOVIMIENTO_KMH), F.col("dist_m")))
        .withColumn("salto_imposible",
                    (F.col("dt_s") > 0)
                    & (F.col("dist_m") / F.col("dt_s") * 3.6 > VEL_IMPLICITA_IMPOSIBLE_KMH))
    )


def reporte_diario(segmentos: DataFrame, null_island_por_dia: DataFrame,
                   eventos_dia: DataFrame, vehiculos: DataFrame) -> DataFrame:
    base = (
        segmentos.groupBy("vehicle_id", "fecha")
        .agg(
            F.round(F.sum("dist_m") / 1000.0, 2).alias("km_recorridos"),
            F.round(F.sum("dist_movimiento_m") / 1000.0, 2).alias("km_en_movimiento"),
            F.count("*").alias("puntos_validos"),
            F.round(F.avg("speed"), 2).alias("vel_promedio_kmh"),
            F.round(F.avg(F.when(F.col("speed") >= UMBRAL_MOVIMIENTO_KMH, F.col("speed"))), 2)
             .alias("vel_promedio_en_movimiento_kmh"),
            F.max("speed").alias("vel_maxima_kmh"),
            F.round(F.sum("s_movimiento") / 3600.0, 2).alias("horas_movimiento"),
            F.round(F.sum("s_detenido") / 3600.0, 2).alias("horas_detenido"),
            F.sum(F.col("salto_imposible").cast("int")).alias("saltos_imposibles"),
            F.min("ts_local").alias("primer_punto_local"),
            F.max("ts_local").alias("ultimo_punto_local"),
        )
        .fillna(0.0, subset=["km_recorridos", "km_en_movimiento"])
        .withColumn("horas_con_senal", F.round(F.col("horas_movimiento") + F.col("horas_detenido"), 2))
        .withColumn("pct_uso_dia", F.round(F.col("horas_movimiento") / 24.0 * 100, 1))
        .withColumn("pct_movimiento_sobre_senal",
                    F.round(F.col("horas_movimiento") / F.col("horas_con_senal") * 100, 1))
    )
    return (
        base.join(null_island_por_dia, ["vehicle_id", "fecha"], "left")
        .join(eventos_dia, ["vehicle_id", "fecha"], "left")
        .join(vehiculos, "vehicle_id", "left")
        .fillna(0, subset=["puntos_null_island", "entradas_geocerca", "salidas_geocerca"])
        .select(
            "vehicle_id", "placa", "fecha", "km_recorridos", "km_en_movimiento",
            "vel_promedio_kmh", "vel_promedio_en_movimiento_kmh", "vel_maxima_kmh",
            "horas_movimiento", "horas_detenido", "horas_con_senal",
            "pct_uso_dia", "pct_movimiento_sobre_senal",
            "entradas_geocerca", "salidas_geocerca",
            "puntos_validos", "puntos_null_island", "saltos_imposibles",
            "primer_punto_local", "ultimo_punto_local",
        )
    )


def agregar_periodo(diario: DataFrame, periodo: str) -> DataFrame:
    """Resumen semanal ('week', semana ISO desde el lunes) o mensual ('month')."""
    col_periodo = "semana_inicio" if periodo == "week" else "mes"
    activo = F.col("km_en_movimiento") >= KM_MINIMO_DIA_ACTIVO
    return (
        diario.withColumn(col_periodo, F.to_date(F.date_trunc(periodo, "fecha")))
        .groupBy("vehicle_id", "placa", col_periodo)
        .agg(
            F.round(F.sum("km_recorridos"), 2).alias("km_recorridos"),
            F.round(F.sum("km_en_movimiento"), 2).alias("km_en_movimiento"),
            F.sum(activo.cast("int")).alias("dias_activos"),
            F.round(F.sum("horas_movimiento"), 2).alias("horas_movimiento"),
            F.round(F.sum("horas_detenido"), 2).alias("horas_detenido"),
            F.max("vel_maxima_kmh").alias("vel_maxima_kmh"),
            # Promedio ponderado por puntos: equivale a promediar todos los puntos del período.
            F.round(F.sum(F.col("vel_promedio_kmh") * F.col("puntos_validos"))
                    / F.sum("puntos_validos"), 2).alias("vel_promedio_kmh"),
            F.sum("entradas_geocerca").alias("entradas_geocerca"),
            F.sum("salidas_geocerca").alias("salidas_geocerca"),
            F.sum("puntos_null_island").alias("puntos_null_island"),
            F.sum("saltos_imposibles").alias("saltos_imposibles"),
        )
        .withColumn("km_por_dia_activo",
                    F.round(F.col("km_en_movimiento") / F.when(F.col("dias_activos") > 0, F.col("dias_activos")), 2))
        .orderBy("vehicle_id", col_periodo)
    )


def control_cruzado(spark: SparkSession, url: str, props: dict, diario: DataFrame,
                    casos: list[tuple[int, str]]) -> DataFrame:
    """Compara km de Spark contra PostgreSQL para algunos (vehículo, día).

    Si fn_distancia_recorrida ya está instalada, la usa; si no, ejecuta su misma
    lógica como SELECT (solo lectura).
    """
    existe = leer_tabla(spark, url, props,
                        "SELECT to_regprocedure('public.fn_distancia_recorrida(integer,date,text)') IS NOT NULL AS e"
                        ).first()["e"]
    filas = []
    for veh, dia in casos:
        if existe:
            sql = f"SELECT fn_distancia_recorrida({veh}, DATE '{dia}') AS km"
        else:
            sql = f"""
                SELECT ROUND((COALESCE(SUM(ST_Length(t.tr::geography)), 0) / 1000.0)::numeric, 2) AS km
                  FROM (SELECT ST_MakeLine(g.location::geometry ORDER BY g."timestamp") AS tr
                          FROM device_assignments da
                          JOIN gps_data g ON g.device_id = da.device_id
                         WHERE da.vehicle_id = {veh}
                           AND da.assigned_from <= DATE '{dia}'
                           AND (da.assigned_to IS NULL OR da.assigned_to > DATE '{dia}')
                           AND g."timestamp" >= (DATE '{dia}'::timestamp AT TIME ZONE '{ZONA_HORARIA}') AT TIME ZONE 'UTC'
                           AND g."timestamp" <  ((DATE '{dia}' + 1)::timestamp AT TIME ZONE '{ZONA_HORARIA}') AT TIME ZONE 'UTC'
                           AND NOT (ST_X(g.location::geometry) = 0 AND ST_Y(g.location::geometry) = 0)
                         GROUP BY da.id) t
            """
        km_sql = leer_tabla(spark, url, props, sql).first()["km"]
        filas.append((veh, date.fromisoformat(dia), float(km_sql) if km_sql is not None else None,
                      "fn_distancia_recorrida" if existe else "SELECT equivalente"))
    sql_df = spark.createDataFrame(filas, "vehicle_id int, fecha date, km_sql double, fuente_sql string")
    return (
        sql_df.join(diario.select("vehicle_id", "fecha", F.col("km_recorridos").alias("km_spark")),
                    ["vehicle_id", "fecha"], "left")
        .withColumn("diferencia_km", F.round(F.col("km_spark") - F.col("km_sql"), 2))
        .withColumn("coincide", F.abs(F.col("diferencia_km")) <= 0.01)
        .select("vehicle_id", "fecha", "km_spark", "km_sql", "diferencia_km", "coincide", "fuente_sql")
    )


# --------------------------------------------------------------------------- #
# Salida
# --------------------------------------------------------------------------- #
def exportar(df: DataFrame, salida: Path, nombre: str, muestra: int = 15) -> int:
    """Escribe Parquet y CSV (un archivo cada uno) y muestra una muestra en consola."""
    df = df.cache()
    filas = df.count()
    df.coalesce(1).write.mode("overwrite").parquet(str(salida / "parquet" / nombre))
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(salida / "csv" / nombre))
    print(f"\n=== {nombre}: {filas:,} filas -> {salida}/{{parquet,csv}}/{nombre}")
    df.show(muestra, truncate=False)
    return filas


class Cronometro:
    def __init__(self) -> None:
        self.inicio = self.ultimo = time.perf_counter()
        self.etapas: list[tuple[str, float]] = []

    def marca(self, etapa: str) -> None:
        ahora = time.perf_counter()
        self.etapas.append((etapa, ahora - self.ultimo))
        print(f"[{ahora - self.inicio:7.1f}s] {etapa} ({ahora - self.ultimo:.1f}s)", flush=True)
        self.ultimo = ahora

    def total(self) -> float:
        return time.perf_counter() - self.inicio


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def parsear_args(argv: list[str] | None) -> argparse.Namespace:
    manana_local = (datetime.now(ZoneInfo(ZONA_HORARIA)) + timedelta(days=1)).strftime("%Y-%m-%d")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--salida", default="spark/output", help="carpeta de salida (Parquet y CSV)")
    p.add_argument("--desde", default="2024-01-01",
                   help="fecha local Lima inclusive, AAAA-MM-DD (defecto: todo el histórico)")
    p.add_argument("--hasta", default=manana_local,
                   help="fecha local Lima exclusiva, AAAA-MM-DD (defecto: mañana)")
    p.add_argument("--particiones", type=int, default=8,
                   help="particiones JDBC = conexiones simultáneas máximas contra la BD")
    p.add_argument("--memoria-driver", default=os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
    p.add_argument("--control-cruzado", default=CONTROL_CRUZADO_DEFECTO,
                   help="casos vehiculo:AAAA-MM-DD separados por coma; vacío para omitir")
    # parse_known_args: en notebooks (Jupyter) el kernel agrega argumentos propios (-f ...).
    args, _ = p.parse_known_args(argv)
    return args


def main(argv: list[str] | None = None) -> None:
    args = parsear_args(argv)
    salida = Path(args.salida)
    crono = Cronometro()

    url, props = configuracion_jdbc()
    desde_utc, hasta_utc = inicio_dia_utc(args.desde), inicio_dia_utc(args.hasta)
    print(f"Rango local [{args.desde}, {args.hasta}) = UTC [{desde_utc}, {hasta_utc})")
    spark = crear_spark(args.memoria_driver)
    crono.marca("Spark iniciado")

    # --- Dimensiones pequeñas -------------------------------------------------
    asignaciones = leer_tabla(spark, url, props,
                              "SELECT id, device_id, vehicle_id, assigned_from, assigned_to "
                              "FROM device_assignments WHERE device_id IS NOT NULL").cache()
    vehiculos = leer_tabla(spark, url, props,
                           "SELECT id AS vehicle_id, trim(plate) AS placa FROM vehicles").cache()
    eventos = (
        leer_tabla(spark, url, props,
                   "SELECT e.vehicle_id, e.geofence_id, f.name AS geocerca, e.event_type, "
                   "e.gps_timestamp AS ts FROM geofence_events e JOIN geofences f ON f.id = e.geofence_id "
                   f"WHERE e.gps_timestamp >= TIMESTAMP '{desde_utc}' AND e.gps_timestamp < TIMESTAMP '{hasta_utc}'")
        .withColumn("fecha", F.to_date(F.from_utc_timestamp("ts", ZONA_HORARIA)))
        .cache()
    )
    print(f"asignaciones={asignaciones.count()} vehiculos={vehiculos.count()} eventos_geocerca={eventos.count():,}")
    crono.marca("Dimensiones leídas")

    # --- Hechos: gps_data completo -------------------------------------------
    gps = leer_gps(spark, url, props, desde_utc, hasta_utc, args.particiones)
    puntos = resolver_vehiculo(gps, asignaciones).persist()
    total_puntos = puntos.count()
    print(f"puntos GPS leídos: {total_puntos:,} en {gps.rdd.getNumPartitions()} particiones JDBC")
    crono.marca("gps_data leído y vehículo resuelto")

    # --- Calidad de datos -----------------------------------------------------
    calidad = (
        puntos.groupBy("device_id", "fecha")
        .agg(F.count("*").alias("puntos"),
             F.sum(F.col("null_island").cast("int")).alias("puntos_null_island"),
             F.sum(F.col("assignment_id").isNull().cast("int")).alias("puntos_sin_asignacion"),
             F.min("ts").alias("primer_punto_utc"), F.max("ts").alias("ultimo_punto_utc"))
        .orderBy("device_id", "fecha")
    )
    null_island_dia = (
        puntos.where(F.col("null_island") & F.col("vehicle_id").isNotNull())
        .groupBy("vehicle_id", "fecha").agg(F.count("*").alias("puntos_null_island"))
    )

    # --- Segmentos, distancia y tiempos --------------------------------------
    validos = puntos.where(F.col("vehicle_id").isNotNull() & ~F.col("null_island"))
    segmentos = calcular_segmentos(validos)

    eventos_detalle = (
        eventos.groupBy("vehicle_id", "fecha", "geofence_id", "geocerca")
        .agg(F.sum((F.col("event_type") == "in").cast("int")).alias("entradas"),
             F.sum((F.col("event_type") == "out").cast("int")).alias("salidas"))
        .join(vehiculos, "vehicle_id", "left")
        .select("vehicle_id", "placa", "fecha", "geofence_id", "geocerca", "entradas", "salidas")
        .orderBy("vehicle_id", "fecha", "geofence_id")
    )
    eventos_dia = (
        eventos.groupBy("vehicle_id", "fecha")
        .agg(F.sum((F.col("event_type") == "in").cast("int")).alias("entradas_geocerca"),
             F.sum((F.col("event_type") == "out").cast("int")).alias("salidas_geocerca"))
    )

    diario = reporte_diario(segmentos, null_island_dia, eventos_dia, vehiculos).orderBy("vehicle_id", "fecha")

    # --- Exportación ----------------------------------------------------------
    resumen: dict[str, int] = {}
    resumen["reporte_diario"] = exportar(diario, salida, "reporte_diario")
    crono.marca("Reporte diario (distancia, velocidad, tiempos)")
    resumen["eventos_geocerca_diario"] = exportar(eventos_detalle, salida, "eventos_geocerca_diario")
    resumen["reporte_semanal"] = exportar(agregar_periodo(diario, "week"), salida, "reporte_semanal")
    resumen["reporte_mensual"] = exportar(agregar_periodo(diario, "month"), salida, "reporte_mensual", muestra=40)
    resumen["calidad_datos_diaria"] = exportar(calidad, salida, "calidad_datos_diaria", muestra=5)
    crono.marca("Geocercas, semanal, mensual y calidad")

    # --- Resumen de calidad en consola ---------------------------------------
    print("\n=== Calidad de datos (histórico procesado)")
    puntos.agg(
        F.count("*").alias("puntos_totales"),
        F.sum(F.col("null_island").cast("int")).alias("null_island_excluidos"),
        F.sum(F.col("assignment_id").isNull().cast("int")).alias("sin_asignacion_excluidos"),
    ).show(truncate=False)
    diario.agg(F.sum("saltos_imposibles").alias(f"saltos_>{VEL_IMPLICITA_IMPOSIBLE_KMH:.0f}kmh"),
               F.max("vel_maxima_kmh").alias("vel_maxima_reportada")).show(truncate=False)
    dias = calidad.groupBy("fecha").agg(F.sum("puntos").alias("p")).select("fecha")
    rango = dias.agg(F.min("fecha").alias("ini"), F.max("fecha").alias("fin")).first()
    if rango["ini"] is not None:
        calendario = spark.sql(f"SELECT explode(sequence(DATE '{rango['ini']}', DATE '{rango['fin']}')) AS fecha")
        sin_datos = calendario.join(dias, "fecha", "left_anti").orderBy("fecha")
        print(f"Días sin ningún punto GPS en toda la flota entre {rango['ini']} y {rango['fin']} (hora Lima):")
        sin_datos.show(60, truncate=False)

    # --- Control cruzado Spark vs SQL ----------------------------------------
    casos = [(int(v), d) for v, d in (c.split(":") for c in args.control_cruzado.split(",") if c.strip())]
    if casos:
        resumen["control_cruzado"] = exportar(control_cruzado(spark, url, props, diario, casos),
                                              salida, "control_cruzado")
        crono.marca("Control cruzado contra PostgreSQL")

    print("\n=== Tiempos por etapa")
    for etapa, seg in crono.etapas:
        print(f"  {etapa:<50} {seg:8.1f}s")
    print(f"  {'TOTAL':<50} {crono.total():8.1f}s   ({total_puntos:,} puntos GPS)")
    print("Filas exportadas:", resumen)
    spark.stop()


if __name__ == "__main__":
    main()
