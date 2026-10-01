# Serlog GPS

Proyecto final del curso **Base de Datos Avanzadas y Big Data** (UPN). Serlog es una empresa de transporte de pasajeros con una flota de 100 vehículos que pilotea su propio sistema GPS en 9 unidades. Los datos residen en un clúster administrado de PostgreSQL 14.24 con PostGIS 3.3.5 y btree_gist 1.6 en DigitalOcean; la tabla `gps_data` supera los 21,4 millones de filas.

El proyecto automatiza el cálculo de indicadores operativos sobre esa base y los explota con herramientas de BI y Big Data.

## Arquitectura

| Pilar | Componentes | Resultado |
|---|---|---|
| **1. Automatización en la base de datos** (PL/pgSQL) | `fn_distancia_recorrida`, trigger de auditoría de `device_assignments`, procedimiento `sp_generar_resumen_diario` (CURSOR explícito y COMMIT por vehículo), triggers de alertas en tiempo real (velocidad > 90 km/h y eventos de geocerca) | Métricas calculadas en la propia base, auditoría y tabla `alertas_operativas` |
| **2. Business Intelligence** | Esquema estrella `dim_fecha`, `dim_vehiculo`, `fact_recorrido_diario` + Power BI Desktop | Tablero operativo (`powerbi/reportes/dashboard.pdf`) |
| **3. Big Data** (Apache Spark) | `spark/reportes_operativos.py` (PySpark, lectura JDBC de solo lectura, distancia geodésica con `pyproj`) | Reportes diario, semanal, mensual, de calidad y de geocercas en Parquet/CSV, más un control cruzado contra `fn_distancia_recorrida` |

Flujo de datos:

```
Equipos GPS ──► gps_data / geofence_events (PostgreSQL + PostGIS)
                 │
                 ├─► triggers 012/014 ──► alertas_operativas ──────────┐
                 ├─► sp_generar_resumen_diario ──► fact_recorrido_diario ├─► Power BI (Importar)
                 ├─► fn_distancia_recorrida (on demand) ────────────────┘
                 └─► Spark (JDBC solo lectura) ──► spark/output/{parquet,csv}
```

Los tres pilares aplican las mismas reglas de negocio:

- **Día en hora de Lima:** `gps_data."timestamp"` está en UTC (timestamp sin zona); los días se calculan en `America/Lima`.
- **Vehículo vía `device_assignments`:** rango semiabierto `[assigned_from, assigned_to)` sobre la fecha local; el día de un traspaso cuenta solo para el vehículo nuevo. `gps_data.vehicle_id` no se usa (está vacío en los datos recientes).
- **Puntos `POINT(0 0)` ("null island"):** el GPS los reporta sin señal satelital; se excluyen de todos los cálculos y se cuentan como métrica de calidad.
- **Umbrales:** movimiento ≥ 5 km/h, intervalo máximo entre puntos de 300 s, alerta de velocidad > 90 km/h.

## Estructura del repositorio

```
serlog-gps/
├── README.md
├── .env.example                      Plantilla de credenciales para Spark (copiar a .env)
├── sql/
│   ├── checks/000_preflight_schema.sql        Verificación del esquema (solo lectura)
│   ├── functions/                             fn_distancia_recorrida, función de auditoría y funciones de alertas
│   ├── tables/                                Auditoría, esquema estrella y alertas_operativas
│   ├── procedures/                            ETL diario y backfill de 60 días
│   ├── triggers/                              Triggers de auditoría, velocidad y geocerca
│   └── tests/                                 Pruebas de la función y de los triggers
├── spark/
│   ├── reportes_operativos.py                 Pipeline PySpark de reportes operativos
│   ├── requirements.txt                       Dependencias de Python
│   └── Dockerfile                             Imagen con Java 17, PySpark 3.5 y driver JDBC
└── powerbi/
    ├── queries/                               Consultas nativas usadas por Power BI
    └── reportes/dashboard.pdf                 Exportación del tablero
```

## Objetos de base de datos

| N.º | Script | Objeto | Tipo | Propósito |
|---|---|---|---|---|
| 0 | `sql/checks/000_preflight_schema.sql` | — | Verificación | Reporta el esquema y aborta si difiere del esperado (solo lectura) |
| 1 | `sql/functions/001_fn_distancia_recorrida.sql` | `fn_distancia_recorrida(vehicle_id, fecha[, zona])` | Función | Km recorridos por un vehículo en un día local |
| 2 | `sql/tables/002_audit_device_assignments_table.sql` | `audit_device_assignments` | Tabla | Historial de cambios de asignaciones |
| 3 | `sql/functions/003_fn_audit_device_assignments.sql` | `fn_audit_device_assignments()` | Función de trigger | Registra INSERT/UPDATE/DELETE (`SECURITY DEFINER`) |
| 4 | `sql/triggers/004_trg_audit_device_assignments.sql` | `trg_audit_device_assignments` | Trigger | `AFTER INSERT/UPDATE/DELETE` sobre `device_assignments` |
| 5 | `sql/tables/005_dim_fecha.sql` | `dim_fecha` | Tabla (dimensión) | Día local Lima, precargada 2024–2028 |
| 6 | `sql/tables/006_dim_vehiculo.sql` | `dim_vehiculo` | Tabla (dimensión) | Copia de `vehicles` sincronizada por upsert |
| 7 | `sql/tables/007_fact_recorrido_diario.sql` | `fact_recorrido_diario` | Tabla (hechos) | Métricas diarias por vehículo |
| 8 | `sql/procedures/008_sp_generar_resumen_diario.sql` | `sp_generar_resumen_diario(fecha)` | Procedimiento | ETL con cursor, UPSERT y COMMIT por vehículo |
| 9 | `sql/procedures/009_backfill_fact_recorrido_diario.sql` | — | Script | Carga de los últimos 60 días con datos |
| 10 | `sql/tables/010_alertas_operativas.sql` | `alertas_operativas` | Tabla | Alertas en tiempo real |
| 11 | `sql/functions/011_fn_alerta_exceso_velocidad.sql` | `fn_alerta_exceso_velocidad()` | Función de trigger | Agrupa excesos de velocidad en episodios |
| 12 | `sql/triggers/012_trg_alerta_exceso_velocidad.sql` | `trg_alerta_exceso_velocidad` | Trigger | `AFTER INSERT … WHEN (NEW.speed > 90)` sobre `gps_data` |
| 13 | `sql/functions/013_fn_alerta_evento_geocerca.sql` | `fn_alerta_evento_geocerca()` | Función de trigger | Registra entradas y salidas de geocercas |
| 14 | `sql/triggers/014_trg_alerta_evento_geocerca.sql` | `trg_alerta_evento_geocerca` | Trigger | `AFTER INSERT` sobre `geofence_events` |

La función (001), la auditoría (002→004), el esquema estrella (005→009) y las alertas (010→014) son bloques independientes; dentro de cada bloque el orden es obligatorio. Cada script empieza con una guarda que aborta si falta su prerrequisito, y todos son re-ejecutables. El encabezado de cada script documenta sus reglas y decisiones de diseño.

## Aplicación de los scripts

Las credenciales se definen en el entorno, nunca en archivos del repositorio:

```bash
export DATABASE_URL_READONLY="postgresql://<usuario_lectura>:<password>@<host>:<puerto>/<base>?sslmode=require"
export DATABASE_URL_ADMIN="postgresql://<usuario_escritura>:<password>@<host>:<puerto>/<base>?sslmode=require"
```

```bash
# 0. Verificación previa (solo lectura). Conservar la salida.
psql "$DATABASE_URL_READONLY" -v ON_ERROR_STOP=1 -f sql/checks/000_preflight_schema.sql

# 1-8 y 10-14: -1 ejecuta cada archivo en una sola transacción.
for f in sql/functions/001_fn_distancia_recorrida.sql \
         sql/tables/002_audit_device_assignments_table.sql \
         sql/functions/003_fn_audit_device_assignments.sql \
         sql/triggers/004_trg_audit_device_assignments.sql \
         sql/tables/005_dim_fecha.sql sql/tables/006_dim_vehiculo.sql \
         sql/tables/007_fact_recorrido_diario.sql sql/procedures/008_sp_generar_resumen_diario.sql \
         sql/tables/010_alertas_operativas.sql sql/functions/011_fn_alerta_exceso_velocidad.sql \
         sql/triggers/012_trg_alerta_exceso_velocidad.sql sql/functions/013_fn_alerta_evento_geocerca.sql \
         sql/triggers/014_trg_alerta_evento_geocerca.sql; do
  psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f "$f" || break
done

# 9. Backfill SIN -1: el procedimiento hace COMMIT por vehículo.
psql "$DATABASE_URL_ADMIN" -v ON_ERROR_STOP=1 -f sql/procedures/009_backfill_fact_recorrido_diario.sql
```

Ejemplos de uso:

```sql
SELECT fn_distancia_recorrida(8, DATE '2026-09-20');            -- día en hora de Lima (por defecto)
SELECT fn_distancia_recorrida(8, DATE '2026-09-20', 'UTC');     -- otro huso
CALL public.sp_generar_resumen_diario(DATE '2026-09-26');      -- fuera de una transacción explícita
SELECT * FROM audit_device_assignments ORDER BY audit_id DESC;  -- no por fecha_evento: now() se repite dentro de una transacción
```

## Decisiones técnicas

**`fn_distancia_recorrida`**
- `gps_data.location` es `geography(Point,4326)` y `ST_MakeLine` no acepta geography: la línea se arma con `::geometry` y se mide con `::geography`.
- El día se calcula en hora de Lima a partir de timestamps en UTC; cortar con `"timestamp"::date` partiría el turno de la tarde a las 19:00.
- Rango semiabierto `[assigned_from, assigned_to)`: un `BETWEEN` inclusivo contaría el día de traspaso para **los dos** vehículos (caso real: dispositivo 1, del vehículo 1 al 10 el 2025-08-13, 129 022 km cada uno).
- Los puntos `POINT(0 0)` inflaban la distancia ~8 000 km cada uno: 241 721 km → 192,29 km reales para el vehículo 1 el 2025-08-12.
- Una línea por asignación (no se mezclan dispositivos) y filtro sargable con poda de particiones.
- Vehículo inexistente → error; sin asignación ese día → `NULL` + NOTICE; sin puntos → `0.00`.

**Auditoría**
- Se audita `INSERT`, `UPDATE` y `DELETE`; los `UPDATE` sin cambios no generan filas.
- `SECURITY DEFINER` con `search_path` fijo; se registra `session_user`, `application_name` e IP del cliente.
- `TIMESTAMPTZ`, `IDENTITY`, `CHECK` de coherencia y `CREATE OR REPLACE TRIGGER` (re-ejecutable).

**Esquema estrella y ETL**
- **Misma lógica en todos los pilares.** `km_recorridos` llama a `fn_distancia_recorrida` (001); los tiempos usan los umbrales de Spark: movimiento ≥ 5 km/h e intervalo ≤ 300 s.
- **Sin puntos, sin fila.** Un vehículo sin ningún punto válido ese día no genera fila: ausencia de fila = sin datos, y 0 km = tuvo señal pero no se movió.
- **PROCEDURE y no FUNCTION**, porque necesita COMMIT por vehículo. PostgreSQL impide COMMIT en un procedimiento con `SET search_path` o `SECURITY DEFINER`, y dentro de un bloque con `EXCEPTION`: por eso cada vehículo corre en un sub-bloque con su propio manejo de error y el COMMIT va después.

**Alertas**
- **Umbral de velocidad: > 90 km/h**, elegido según el volumen de alertas resultante (episodios de 5 min, toda la flota). 90 km/h es además el límite habitual para ómnibus en carretera en Perú.

  | Umbral | Puntos | Episodios | Por semana |
  |---|---|---|---|
  | > 80 | 60 857 | 2 170 | ~25 (ruido: operación normal en carretera) |
  | > 85 | 11 020 | 1 256 | ~14 |
  | **> 90** | **1 430** | **251** | **~3 (accionable)** |
  | > 100 | 216 | 31 | ~0,4 (pierde casos reales) |

- La condición va en el `WHEN` del trigger: se evalúa antes de invocar la función, así que para casi todos los puntos el trigger no ejecuta PL/pgSQL.
- **Alertas por episodio.** Un exceso de 2 minutos serían ~24 filas con muestreo de 5 s; se agrupan en un episodio (valor = velocidad máxima). Más de 150 km/h se registra como `VELOCIDAD_IMPLAUSIBLE`.
- **Los triggers nunca bloquean la ingesta.** Si la alerta falla, el punto GPS o el evento de geocerca se guardan igual y solo queda un `WARNING`.
- Distribución de `gps_data.speed` (21 412 543 puntos válidos): 36,55 % en 0 km/h exacto; puntos en movimiento con p99 = 74, p99,9 = 85 y p99,99 = 91 km/h; máximo 204 (una lectura aislada entre 0 y 9 km/h).
- `geofence_events` tiene `UNIQUE (assignment_id, event_type, gps_timestamp)`: un duplicado descartado por `ON CONFLICT DO NOTHING` no dispara el trigger. `geofence_vehicle_state` guarda el estado actual por asignación, no eventos, por lo que no lleva trigger.

## Pruebas

| Script | Qué verifica | Efecto en la base |
|---|---|---|
| `sql/tests/test_001_fn_distancia_recorrida.sql` | La función contra un cálculo independiente (LAG + `ST_Distance`) y sus casos borde | Solo lectura |
| `sql/tests/test_004_audit_device_assignments.sql` | Secuencia INSERT, UPDATE sin cambios, UPDATE y DELETE en la auditoría | `BEGIN … ROLLBACK` (consume 1 id de secuencia) |
| `sql/tests/test_012_014_alertas_operativas.sql` | Umbral, episodios, implausibles, null island, resiliencia y geocercas | `BEGIN … ROLLBACK` |

```bash
psql "$DATABASE_URL_READONLY" -v ON_ERROR_STOP=1 -f sql/tests/test_001_fn_distancia_recorrida.sql
psql "$DATABASE_URL_ADMIN"    -v ON_ERROR_STOP=1 -f sql/tests/test_004_audit_device_assignments.sql   # preferentemente en staging
psql "$DATABASE_URL_ADMIN"    -v ON_ERROR_STOP=1 -f sql/tests/test_012_014_alertas_operativas.sql    # preferentemente en staging
```

Resultados en una réplica local (PostGIS 14 en Docker, mismo DDL y 2,9 M puntos reales desde 2026-07-01):

| Prueba | Resultado |
|---|---|
| Aplicación y re-aplicación de 005–014 | Sin errores (idempotente) |
| `fact_recorrido_diario` contra el `reporte_diario` de Spark | **349/349 filas iguales** en km, minutos en movimiento/detenido, velocidad promedio, eventos y puntos |
| Backfill de 60 días (2026-07-29 → 2026-09-26, 1,94 M puntos) | **6,8 s**; 349 filas y 7 vehículos. La segunda corrida da 0 duplicados y recalcula `calculado_en` |
| Fallo forzado en un vehículo | `WARNING` y continúa; los otros 5 vehículos del día quedan guardados |
| Casos borde del procedimiento | `NULL` y fecha futura dan error claro; un día sin datos (2025-01-10) no carga filas; dentro de `BEGIN` da `invalid transaction termination` |
| Trigger de velocidad | 85 y 90 no disparan; 90,5 → 97 → 93 forman un episodio (máx. 97, 3 puntos); 7 min después se abre otro; 204 = implausible; (0,0) se ignora; con la tabla de alertas caída, el punto GPS igual se guarda |
| Trigger de geocercas | Entrada y salida registradas con dispositivo y geocerca; el duplicado descartado por `ON CONFLICT` no genera alerta |
| Sobrecarga en la ingesta | Un mes real (1 001 886 puntos): 6,06 / 6,15 s sin trigger contra 6,00 / 6,26 s con trigger. Diferencia dentro del ruido |

Estimación sobre la base real (misma carga de lectura del procedimiento): ~0,5 s por día y 4,4 s de búsqueda inicial, es decir **~35 s para el backfill de 60 días**.

## Programación del resumen diario

`sp_generar_resumen_diario()` se ejecuta para el **día anterior** en hora de Lima, después de la medianoche local, para trabajar con días completos. Es idempotente: re-ejecutarlo solo recalcula. Dos opciones:

**Opción A: pg_cron** (la extensión debe estar disponible y precargada en el servidor)

```sql
-- Una sola vez, en la base que indique cron.database_name:
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- pg_cron usa UTC: 06:30 UTC = 01:30 en Lima.
SELECT cron.schedule_in_database(
    'serlog-resumen-diario',
    '30 6 * * *',
    $$CALL public.sp_generar_resumen_diario(((now() AT TIME ZONE 'America/Lima')::date - 1))$$,
    '<base>'
);

-- Opcional: reprocesar D-2 para absorber puntos que llegan tarde. Debe ser un job aparte:
-- dos CALL en un mismo comando corren en una transacción implícita y el COMMIT interno fallaría.
SELECT cron.schedule_in_database(
    'serlog-resumen-diario-d2',
    '45 6 * * *',
    $$CALL public.sp_generar_resumen_diario(((now() AT TIME ZONE 'America/Lima')::date - 2))$$,
    '<base>'
);

-- Monitoreo y baja
SELECT jobid, jobname, schedule, active FROM cron.job;
SELECT jobid, status, return_message, start_time FROM cron.job_run_details ORDER BY start_time DESC LIMIT 20;
SELECT cron.unschedule('serlog-resumen-diario');
```

**Opción B: cron externo** (no requiere extensiones)

```cron
CRON_TZ=America/Lima
# 01:30 hora Lima, todos los días. Sin -1: el procedimiento hace COMMIT por vehículo.
30 1 * * * psql "$DATABASE_URL_ETL" -v ON_ERROR_STOP=1 -c "CALL public.sp_generar_resumen_diario(((now() AT TIME ZONE 'America/Lima')::date - 1))" >> /var/log/serlog/resumen_diario.log 2>&1
```

Los `WARNING` de vehículos que fallen quedan en `cron.job_run_details` (opción A) o en el log (opción B).

## Pipeline de Spark

`spark/reportes_operativos.py` procesa el histórico de `gps_data` y genera reportes de uso por vehículo. Solo lee de PostgreSQL y deja los resultados como archivos locales en `spark/output/parquet/<reporte>/` y `spark/output/csv/<reporte>/` (carpeta ignorada por git).

| Reporte | Grano | Contenido |
|---|---|---|
| `reporte_diario` | vehículo × día (hora Lima) | km, km en movimiento, velocidad promedio / en movimiento / máxima, horas en movimiento / detenido / con señal, % de uso, entradas/salidas de geocercas, métricas de calidad |
| `eventos_geocerca_diario` | vehículo × día × geocerca | entradas y salidas |
| `reporte_semanal` | vehículo × semana (lunes) | totales y tendencias del reporte diario |
| `reporte_mensual` | vehículo × mes | ídem |
| `calidad_datos_diaria` | dispositivo × día | puntos, puntos (0,0), puntos sin asignación, primer/último punto |
| `control_cruzado` | casos vehículo × día | km de Spark contra la misma lógica ejecutada en PostgreSQL |

**Parámetros**

| Parámetro | Valor | Motivo |
|---|---|---|
| `UMBRAL_MOVIMIENTO_KMH` | **5 km/h** | El equipo ya reporta 0 km/h al estar detenido (36 % de los puntos) y casi no hay lecturas entre 0 y 3 km/h; 5 km/h absorbe el jitter residual sin perder maniobras reales. |
| `MAX_INTERVALO_S` | **300 s** | El muestreo normal es de 5 s (p99,9 = 85 s); un hueco mayor es equipo apagado o sin cobertura y no cuenta ni como movimiento ni como detención. |
| `VEL_IMPLICITA_IMPOSIBLE_KMH` | **150 km/h** | Solo métrica de calidad; esos segmentos no se excluyen, para mantener la misma lógica que SQL. |
| `KM_MINIMO_DIA_ACTIVO` | **1 km en movimiento** | Evita que el jitter de un vehículo estacionado cuente como día de uso. |

- **Distancia:** suma de segmentos geodésicos sobre WGS84 con `pyproj.Geod` (GeographicLib, el mismo algoritmo que PostGIS usa para `geography`). Cada segmento une puntos consecutivos de la misma asignación y el mismo día local, igual que `ST_MakeLine … GROUP BY da.id` en SQL.
- `km_recorridos` replica exactamente la función SQL, jitter incluido. `km_en_movimiento` suma solo los segmentos donde alguno de los dos puntos supera el umbral: es la métrica recomendada para medir uso.

**Lectura JDBC**

- Se usa `spark.read.format("jdbc")` con **`partitionColumn = ts`** y `lowerBound`/`upperBound` iguales al rango pedido (`spark.read.jdbc()` no admite límites de tipo timestamp). Con `numPartitions = 8` cada tarea lee su propia ventana de tiempo en paralelo; `numPartitions` es también el máximo de conexiones simultáneas contra la base.
- **`fetchsize = 10000`:** el driver trae las filas en lotes en lugar de cargar el resultado completo en memoria.
- La geografía se convierte a `lon`/`lat` en PostgreSQL, así no se transfiere WKB.
- `sessionInitStatement` abre la sesión con `default_transaction_read_only = on`, como segunda barrera además de los permisos del rol.

**Ejecución con Docker**

La imagen incluye Java 17, PySpark 3.5 y el driver JDBC, que se descarga al construirla y no se versiona.

```bash
docker build -t serlog-spark:local spark/

# Credenciales: DATABASE_URL_READONLY, variables DB_* o un archivo .env (ver .env.example)
docker run --rm -v "$PWD:/app" -e ENV_FILE=/app/.env serlog-spark:local --salida /app/spark/output

# Opciones: --desde/--hasta (fechas locales de Lima, hasta exclusivo), --particiones, --control-cruzado "1:2025-08-12,8:2026-09-20"
docker run --rm -v "$PWD:/app" -e DATABASE_URL_READONLY serlog-spark:local --desde 2026-09-01 --hasta 2026-09-27
```

**Ejecución local o en notebook** (Python 3.11, Java 17)

```bash
pip install -r spark/requirements.txt
export JDBC_JAR=/ruta/postgresql-42.7.4.jar     # driver JDBC de PostgreSQL
python spark/reportes_operativos.py --salida spark/output
```

En un notebook (por ejemplo, Google Colab) basta instalar Java 17 y las dependencias, definir `JDBC_JAR` y `DATABASE_URL_READONLY` como variables de entorno y llamar a `main(["--salida", "<carpeta>"])` tras cargar el script.

**Resultados de la corrida completa** (Docker local, Java 17, PySpark 3.5.3, `local[*]`, driver de 5 GB, 8 particiones JDBC)

| Etapa | Tiempo |
|---|---|
| Lectura de `gps_data` y resolución de vehículo (**21 435 715 puntos**, 2024-09-09 → 2026-09-27) | 133,5 s |
| Reporte diario (segmentos, distancia geodésica, tiempos) | 35,0 s |
| Geocercas, semanal, mensual y calidad | 15,2 s |
| Control cruzado contra PostgreSQL | 19,5 s |
| **Total** | **216 s (~3,6 min)** |

Filas exportadas: diario 4 013, eventos de geocerca 3 028, semanal 632, mensual 158, calidad 4 015.

**Control cruzado Spark vs. SQL:** coincidencia exacta en los 3 casos (1 / 2025-08-12 = 192,29 km; 10 / 2025-08-13 = 199,01 km; 8 / 2026-09-20 = 118,32 km).

**Escalamiento:** el cuello de botella es la transferencia JDBC (~160 000 puntos/s); el cómputo en Spark tardó ~50 s. Con 100 unidades el volumen crece ~11 veces: la lectura escala de forma lineal y el cómputo se reparte agregando núcleos o nodos sin tocar el código. Para uso diario conviene procesar de forma incremental con `--desde`, solo los días nuevos, y conservar el histórico en Parquet.

## Power BI

La capa de BI se construyó en **Power BI Desktop** con el conector nativo de PostgreSQL (**Npgsql**).

- **Modo Importar** (no DirectQuery): snapshot de datos, sin mantener una conexión persistente abierta contra la base de producción.
- **SSL:** el clúster usa un certificado emitido por una CA propia del proveedor. Npgsql valida la cadena de certificados, por lo que el certificado CA del clúster debe importarse en el almacén de Windows "Entidades de certificación raíz de confianza" antes de conectar.
- **Origen de datos:** en "Obtener datos → PostgreSQL → Opciones avanzadas", cada tabla usa una sentencia SQL nativa (`powerbi/queries/`), lo que permite llamar a `fn_distancia_recorrida()` y unir el esquema estrella en la propia consulta.

| Consulta | Qué trae | Pilar que evidencia |
|---|---|---|
| `01_resumen_por_vehiculo.sql` | Km recorridos por vehículo y día (últimos 13 días), con `fn_distancia_recorrida()` | Función on demand |
| `02_esquema_estrella.sql` | `fact_recorrido_diario` unido con `dim_vehiculo` y `dim_fecha`: km, tiempo en movimiento/detenido, velocidad promedio, puntos GPS | Procedimiento batch y esquema estrella |
| `03_alertas_operativas.sql` | Últimas 20 alertas de velocidad y geocerca, con placa en lugar de `vehicle_id` | Triggers de alerta en tiempo real |

El tablero incluye: km recorridos por vehículo, tendencia diaria de km, tiempo en movimiento vs. detenido por vehículo, resumen operativo diario (esquema estrella) y alertas operativas. Su exportación está en `powerbi/reportes/dashboard.pdf`.

El archivo `.pbix` **no se versiona**: en modo Importar embebe una copia de los datos reales de geolocalización del piloto, que son información sensible. Las consultas y la exportación del tablero son evidencia suficiente sin ese riesgo.

## Hallazgos de calidad de datos

1. **Hueco en particiones:** `gps_data_2025` va de `2025-01-01` **a `2025-12-31`** (límite superior exclusivo) y no hay partición `DEFAULT`, por lo que no existe partición para el **31/12/2025** (UTC): 0 filas ese día contra ~44 000 el 29 y el 30. En hora de Lima, ese hueco se reparte en dos días parciales (30/12 desde las 19:00 y 31/12 hasta las 19:00).
2. **86 días sin ningún punto en toda la flota** (hora Lima): del 2024-09-10 al 2024-11-11 (63 días; el 2024-09-09 hay un único punto suelto), del 2024-11-13 al 2024-11-14, y del **2025-01-01 al 2025-01-21 (21 días)**.
3. **`gps_data.vehicle_id`** existe, pero está vacío en los datos recientes.
4. **El dispositivo 1 concentra el 98 % de los puntos `POINT(0 0)`** (29 513 de 30 097, el 0,92 % de sus lecturas); parece un problema de antena o del equipo, no de la plataforma.
5. **Velocidades reportadas imposibles para un bus:** máximo de 204 km/h (VCT-649, 2026-04-30), 156 km/h (VBV-678) y 123 km/h (VBV-487). `vel_maxima_kmh` muestra lo que reporta el equipo; conviene revisar esos días antes de usar ese valor en informes.
6. **8 482 segmentos con saltos físicamente imposibles** (> 150 km/h implícitos): jitter de posición en intervalos de 1 a 5 s.
7. **Eventos de geocerca desbalanceados:** 7 886 entradas contra 7 176 salidas, es decir, 710 entradas sin salida registrada.
8. **Vehículo 5 (placa `AAA-113`)**: su dispositivo tiene solo 133 puntos en todo el histórico, y el 5 % son `(0,0)`; parece una unidad de prueba o con placa provisional.
9. **Jitter con el vehículo detenido:** 2 378 km del total de 550 814 km (0,43 %). Es poco a nivel global, pero en días de un vehículo estacionado genera kilómetros ficticios (12 días con distancia y sin movimiento real); por eso existe `km_en_movimiento`.
10. **`TRUNCATE`** sobre `device_assignments` no queda auditado: los triggers por fila no lo capturan.
