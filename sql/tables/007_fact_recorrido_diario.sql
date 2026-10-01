-- =============================================================================
-- 007_fact_recorrido_diario.sql
-- Tabla de hechos: una fila por vehículo y día local con sus métricas operativas.
-- Requiere 005 (dim_fecha) y 006 (dim_vehiculo). La carga sp_generar_resumen_diario (008).
--
-- Definición de cada métrica (idéntica al pipeline de Spark, reporte_diario):
--  * km_recorridos          = fn_distancia_recorrida(vehicle_id, fecha) (script 001):
--                             día America/Lima, sin POINT(0 0), asignación [from, to).
--  * tiempo_movimiento_min  = suma de intervalos entre puntos consecutivos cuyo punto
--                             inicial tiene speed >= 5 km/h (UMBRAL_MOVIMIENTO_KMH)
--                             y dura <= 300 s (MAX_INTERVALO_S).
--  * tiempo_detenido_min    = ídem con speed < 5 km/h. Los huecos > 300 s no cuentan
--                             en ninguno de los dos (equipo apagado o sin cobertura).
--  * velocidad_promedio_kmh = promedio de speed de los puntos válidos del día
--                             (= vel_promedio_kmh de Spark). NULL si no hubo puntos.
--  * eventos_geocerca       = entradas + salidas (geofence_events) del día local.
--
-- Otras decisiones:
--  * calculado_en es TIMESTAMPTZ: instante inequívoco de la carga.
--  * puntos_gps permite distinguir "0 km con señal" de días con datos escasos.
--
-- Aplicar (requiere un rol con privilegios de escritura):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/tables/007_fact_recorrido_diario.sql
-- =============================================================================

DO $$
BEGIN
    IF to_regclass('public.dim_fecha') IS NULL OR to_regclass('public.dim_vehiculo') IS NULL THEN
        RAISE EXCEPTION 'Faltan dimensiones: aplicar primero 005_dim_fecha.sql y 006_dim_vehiculo.sql';
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS public.fact_recorrido_diario (
    fecha                  DATE          NOT NULL REFERENCES public.dim_fecha (fecha),
    vehicle_id             INTEGER       NOT NULL REFERENCES public.dim_vehiculo (vehicle_id),
    km_recorridos          NUMERIC(10,2) NOT NULL,
    tiempo_movimiento_min  NUMERIC(7,2)  NOT NULL,
    tiempo_detenido_min    NUMERIC(7,2)  NOT NULL,
    velocidad_promedio_kmh NUMERIC(6,2),
    eventos_geocerca       INTEGER       NOT NULL DEFAULT 0,
    puntos_gps             INTEGER       NOT NULL,
    calculado_en           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT fact_recorrido_diario_pk PRIMARY KEY (fecha, vehicle_id),
    CONSTRAINT fact_recorrido_diario_valores_ck CHECK (
            km_recorridos >= 0
        AND tiempo_movimiento_min >= 0
        AND tiempo_detenido_min   >= 0
        AND tiempo_movimiento_min + tiempo_detenido_min <= 1440
        AND eventos_geocerca >= 0
        AND puntos_gps >= 0)
);

-- La PK (fecha, vehicle_id) cubre "todo un día"; este índice cubre "historia de un vehículo".
CREATE INDEX IF NOT EXISTS fact_recorrido_diario_vehiculo_idx
    ON public.fact_recorrido_diario (vehicle_id, fecha);

REVOKE ALL ON public.fact_recorrido_diario FROM PUBLIC;

COMMENT ON TABLE public.fact_recorrido_diario IS
    'Hechos diarios por vehículo (día America/Lima). Umbrales: movimiento >= 5 km/h, intervalo <= 300 s. Carga: sp_generar_resumen_diario.';
