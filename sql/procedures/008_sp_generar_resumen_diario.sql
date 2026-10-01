-- =============================================================================
-- 008_sp_generar_resumen_diario.sql
-- Procedimiento ETL: calcula y carga (UPSERT) fact_recorrido_diario para un día.
-- Requiere 001 (fn_distancia_recorrida) y 005-007 (esquema estrella).
--
-- Flujo por vehículo con asignación vigente ese día (CURSOR explícito):
--   Extract   : puntos GPS del día local, vía device_assignments [from, to).
--   Transform : tiempos en movimiento/detenido, velocidad promedio y eventos de
--               geocerca con los mismos umbrales del pipeline de Spark
--               (UMBRAL_MOVIMIENTO_KMH = 5.0, MAX_INTERVALO_S = 300); km vía
--               fn_distancia_recorrida, sin reescribir su lógica.
--   Load      : UPSERT en fact_recorrido_diario y COMMIT.
-- Si un vehículo falla, RAISE WARNING y sigue con el siguiente; lo ya cargado
-- queda confirmado. Un vehículo sin ningún punto válido ese día NO genera fila:
-- ausencia de fila = sin datos, 0 km = tuvo señal pero no se movió.
--
-- RESTRICCIONES DE POSTGRESQL QUE DEFINEN ESTE DISEÑO
--  * Es PROCEDURE y no FUNCTION porque necesita COMMIT por vehículo.
--  * Un procedimiento con SET (p. ej. search_path) o SECURITY DEFINER no puede
--    ejecutar COMMIT ("invalid transaction termination"). Por eso no los tiene y
--    todos los objetos van calificados con public.
--  * COMMIT no está permitido dentro de un bloque con EXCEPTION: cada vehículo se
--    procesa en un sub-bloque BEGIN/EXCEPTION y el COMMIT va después de él.
--  * El cursor del FOR se convierte automáticamente en "holdable" al primer
--    COMMIT, así que sigue abierto entre transacciones.
--  * Se debe invocar con CALL fuera de una transacción explícita: NO usar
--    psql -1 ni BEGIN; si no, el COMMIT interno falla.
--
-- Aplicar la definición (esto sí puede ir en una transacción):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/procedures/008_sp_generar_resumen_diario.sql
-- Ejecutar para un día:
--   psql "$DATABASE_URL_ADMIN" -v ON_ERROR_STOP=1 -c "CALL public.sp_generar_resumen_diario(DATE '2026-09-26')"
-- =============================================================================

DO $$
BEGIN
    IF to_regprocedure('public.fn_distancia_recorrida(integer,date,text)') IS NULL THEN
        RAISE EXCEPTION 'Falta fn_distancia_recorrida: aplicar primero sql/functions/001_fn_distancia_recorrida.sql';
    END IF;
    IF to_regclass('public.fact_recorrido_diario') IS NULL THEN
        RAISE EXCEPTION 'Falta fact_recorrido_diario: aplicar primero 005, 006 y 007 (sql/tables/)';
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE public.sp_generar_resumen_diario(p_fecha DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Umbrales de negocio: deben coincidir con spark/reportes_operativos.py.
    c_umbral_movimiento_kmh CONSTANT DOUBLE PRECISION := 5.0;
    c_max_intervalo_s       CONSTANT DOUBLE PRECISION := 300;
    c_zona                  CONSTANT TEXT := 'America/Lima';

    -- Vehículos con al menos una asignación vigente ese día (rango semiabierto).
    cur_vehiculos CURSOR (c_fecha DATE) FOR
        SELECT DISTINCT da.vehicle_id
          FROM public.device_assignments da
         WHERE da.vehicle_id IS NOT NULL
           AND da.device_id  IS NOT NULL
           AND da.assigned_from <= c_fecha
           AND (da.assigned_to IS NULL OR da.assigned_to > c_fecha)
         ORDER BY da.vehicle_id;

    v_desde     TIMESTAMP;   -- inicio del día local expresado en UTC
    v_hasta     TIMESTAMP;
    v_puntos    BIGINT;
    v_vel_prom  DOUBLE PRECISION;
    v_mov_s     DOUBLE PRECISION;
    v_det_s     DOUBLE PRECISION;
    v_km        NUMERIC;
    v_eventos   INTEGER;
    v_cargados  INTEGER := 0;
    v_sin_datos INTEGER := 0;
    v_errores   INTEGER := 0;
    v_msg       TEXT;
    v_estado    TEXT;
BEGIN
    IF p_fecha IS NULL THEN
        RAISE EXCEPTION 'sp_generar_resumen_diario: p_fecha no puede ser NULL';
    END IF;
    IF p_fecha > (now() AT TIME ZONE c_zona)::date THEN
        RAISE EXCEPTION 'sp_generar_resumen_diario: % es una fecha futura en %', p_fecha, c_zona;
    END IF;

    v_desde := (p_fecha::timestamp       AT TIME ZONE c_zona) AT TIME ZONE 'UTC';
    v_hasta := ((p_fecha + 1)::timestamp AT TIME ZONE c_zona) AT TIME ZONE 'UTC';

    INSERT INTO public.dim_fecha (fecha) VALUES (p_fecha) ON CONFLICT (fecha) DO NOTHING;
    COMMIT;

    FOR r IN cur_vehiculos(p_fecha) LOOP
        BEGIN
            -- Extract + Transform: puntos válidos del vehículo en el día local.
            -- Mismos segmentos que Spark: puntos consecutivos de la misma
            -- asignación dentro del día; el estado del intervalo lo define el
            -- punto inicial (prev_speed).
            SELECT count(*),
                   avg(p.speed),
                   COALESCE(sum(p.dt_s) FILTER (WHERE p.dt_s <= c_max_intervalo_s
                                                  AND COALESCE(p.prev_speed, 0) >= c_umbral_movimiento_kmh), 0),
                   COALESCE(sum(p.dt_s) FILTER (WHERE p.dt_s <= c_max_intervalo_s
                                                  AND COALESCE(p.prev_speed, 0) <  c_umbral_movimiento_kmh), 0)
              INTO v_puntos, v_vel_prom, v_mov_s, v_det_s
              FROM (
                    SELECT g.speed,
                           lag(g.speed) OVER w AS prev_speed,
                           EXTRACT(EPOCH FROM g."timestamp" - lag(g."timestamp") OVER w) AS dt_s
                      FROM public.device_assignments da
                      JOIN public.gps_data g
                        ON g.device_id = da.device_id
                     WHERE da.vehicle_id = r.vehicle_id
                       AND da.assigned_from <= p_fecha
                       AND (da.assigned_to IS NULL OR da.assigned_to > p_fecha)
                       AND g."timestamp" >= v_desde
                       AND g."timestamp" <  v_hasta
                       AND NOT (ST_X(g.location::geometry) = 0 AND ST_Y(g.location::geometry) = 0)
                    WINDOW w AS (PARTITION BY da.id ORDER BY g."timestamp")
                   ) p;

            IF v_puntos = 0 THEN
                v_sin_datos := v_sin_datos + 1;
            ELSE
                v_km := public.fn_distancia_recorrida(r.vehicle_id, p_fecha, c_zona);

                SELECT count(*)
                  INTO v_eventos
                  FROM public.geofence_events e
                 WHERE e.vehicle_id = r.vehicle_id
                   AND e.gps_timestamp >= v_desde
                   AND e.gps_timestamp <  v_hasta;

                -- Mantiene la dimensión sincronizada con vehicles (placa corregida, etc.).
                INSERT INTO public.dim_vehiculo (vehicle_id, placa, creado_en_utc)
                SELECT v.id, trim(v.plate), v.created_at
                  FROM public.vehicles v
                 WHERE v.id = r.vehicle_id
                ON CONFLICT (vehicle_id) DO UPDATE
                   SET placa = EXCLUDED.placa, creado_en_utc = EXCLUDED.creado_en_utc, actualizado_en = now()
                 WHERE (dim_vehiculo.placa, dim_vehiculo.creado_en_utc)
                       IS DISTINCT FROM (EXCLUDED.placa, EXCLUDED.creado_en_utc);

                -- Load: UPSERT idempotente.
                INSERT INTO public.fact_recorrido_diario AS f
                       (fecha, vehicle_id, km_recorridos, tiempo_movimiento_min, tiempo_detenido_min,
                        velocidad_promedio_kmh, eventos_geocerca, puntos_gps, calculado_en)
                VALUES (p_fecha, r.vehicle_id, COALESCE(v_km, 0),
                        round((v_mov_s / 60.0)::numeric, 2), round((v_det_s / 60.0)::numeric, 2),
                        round(v_vel_prom::numeric, 2), v_eventos, v_puntos, now())
                ON CONFLICT (fecha, vehicle_id) DO UPDATE
                   SET km_recorridos          = EXCLUDED.km_recorridos,
                       tiempo_movimiento_min  = EXCLUDED.tiempo_movimiento_min,
                       tiempo_detenido_min    = EXCLUDED.tiempo_detenido_min,
                       velocidad_promedio_kmh = EXCLUDED.velocidad_promedio_kmh,
                       eventos_geocerca       = EXCLUDED.eventos_geocerca,
                       puntos_gps             = EXCLUDED.puntos_gps,
                       calculado_en           = EXCLUDED.calculado_en;

                v_cargados := v_cargados + 1;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT, v_estado = RETURNED_SQLSTATE;
            RAISE WARNING 'sp_generar_resumen_diario(%): vehículo % falló y se omite [SQLSTATE %]: %',
                          p_fecha, r.vehicle_id, v_estado, v_msg;
            v_errores := v_errores + 1;
        END;
        COMMIT;  -- confirma este vehículo; un fallo posterior no lo revierte.
    END LOOP;

    RAISE NOTICE 'sp_generar_resumen_diario(%): cargados=%, sin_datos=%, errores=%',
                 p_fecha, v_cargados, v_sin_datos, v_errores;
END;
$$;

REVOKE ALL ON PROCEDURE public.sp_generar_resumen_diario(DATE) FROM PUBLIC;

COMMENT ON PROCEDURE public.sp_generar_resumen_diario(DATE) IS
    'ETL diario a fact_recorrido_diario (día America/Lima). COMMIT por vehículo; invocar con CALL fuera de transacción.';
