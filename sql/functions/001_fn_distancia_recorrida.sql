-- =============================================================================
-- 001_fn_distancia_recorrida.sql
-- Crea/reemplaza public.fn_distancia_recorrida(vehicle_id, fecha[, zona]):
-- kilómetros recorridos por un vehículo en un día calendario LOCAL.
--
-- Parámetros:
--   p_vehicle_id  id de vehicles.
--   p_fecha       día local a medir.
--   p_zona        zona horaria del día (por defecto America/Lima).
-- Devuelve NUMERIC con dos decimales (km). Prueba: sql/tests/test_001_fn_distancia_recorrida.sql.
--
-- Esquema relevante:
--   gps_data(device_id int4 NOT NULL, "timestamp" timestamp NOT NULL,
--            location geography(Point,4326) NOT NULL, vehicle_id int4 NULL, ...)
--     * particionada por RANGE("timestamp") en gps_data_2024..2028
--     * PK (device_id, "timestamp"): ese índice cubre el filtro de esta función.
--   device_assignments(id int4 PK, device_id int4 NULL, vehicle_id int4 NULL,
--                      assigned_from date NOT NULL, assigned_to date NULL)
--   vehicles(id int4 PK)
--
-- REGLAS Y DECISIONES
--  1. Tipo espacial: location es GEOGRAPHY y ST_MakeLine solo acepta geometry. La
--     línea se arma con location::geometry y se mide con ::geography (metros sobre
--     el esferoide WGS84).
--  2. Zona horaria: "timestamp" es timestamp sin zona en UTC. p_fecha se interpreta
--     como día local y se convierte a un rango UTC semiabierto [desde, hasta); cortar
--     con "timestamp"::date partiría el día a medianoche UTC (19:00 en Lima).
--  3. Filtro sargable: el rango sobre "timestamp" permite usar el índice y la poda
--     de particiones.
--  4. Traspaso de dispositivo: assigned_from/assigned_to son DATE y se usa el rango
--     semiabierto [assigned_from, assigned_to), de modo que el día del traspaso
--     cuenta solo para el vehículo nuevo y no para ambos. Con fechas sin hora no
--     es posible partir ese día.
--  5. Una línea por asignación (GROUP BY da.id) y suma de longitudes, para no unir
--     en un mismo trazo puntos de dispositivos distintos.
--  6. Puntos "null island": el GPS reporta POINT(0 0) cuando no tiene señal
--     satelital; cada uno agregaría ~8 000 km. Se excluyen.
--  7. gps_data.vehicle_id no se usa (está vacío en los datos recientes): la relación
--     vehículo-GPS se resuelve solo vía device_assignments.
--
-- CASOS BORDE
--  * Algún parámetro obligatorio NULL           -> NULL (función STRICT).
--  * Vehículo inexistente                        -> error P0002 (no_data_found).
--  * Sin asignación vigente ese día             -> NULL + NOTICE (distancia
--    desconocida; no es lo mismo que "no se movió").
--  * Asignación vigente pero sin puntos GPS     -> 0.00
--  * Un solo punto en el día                    -> 0.00
--  * Zona horaria inválida                      -> error de PostgreSQL.
--
-- LIMITACIÓN: el jitter del GPS con el vehículo detenido suma distancias pequeñas.
-- No se filtra por velocidad para no alterar la definición de "distancia recorrida".
--
-- Aplicar (requiere un rol con privilegios de escritura):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/functions/001_fn_distancia_recorrida.sql
-- =============================================================================

-- Guarda: aborta si el esquema difiere del esperado.
DO $$
BEGIN
    PERFORM 1 FROM geography_columns
     WHERE f_table_schema = 'public' AND f_table_name = 'gps_data'
       AND f_geography_column = 'location' AND srid = 4326;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'gps_data.location no es geography(4326). Correr sql/checks/000_preflight_schema.sql';
    END IF;

    IF (SELECT udt_name FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'device_assignments'
           AND column_name = 'assigned_to') IS DISTINCT FROM 'date' THEN
        RAISE EXCEPTION 'device_assignments.assigned_to ya no es DATE. Correr sql/checks/000_preflight_schema.sql';
    END IF;

    IF (SELECT udt_name FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'gps_data'
           AND column_name = 'timestamp') IS DISTINCT FROM 'timestamp' THEN
        RAISE EXCEPTION 'gps_data."timestamp" ya no es timestamp sin zona. Correr sql/checks/000_preflight_schema.sql';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.fn_distancia_recorrida(
    p_vehicle_id INTEGER,
    p_fecha      DATE,
    p_zona       TEXT DEFAULT 'America/Lima'
) RETURNS NUMERIC
LANGUAGE plpgsql
STABLE
STRICT
SET search_path = public, pg_temp
AS $$
DECLARE
    -- Límites del día local expresados en UTC, el mismo huso que gps_data."timestamp".
    v_desde  TIMESTAMP := (p_fecha::timestamp       AT TIME ZONE p_zona) AT TIME ZONE 'UTC';
    v_hasta  TIMESTAMP := ((p_fecha + 1)::timestamp AT TIME ZONE p_zona) AT TIME ZONE 'UTC';
    v_metros DOUBLE PRECISION;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM vehicles v WHERE v.id = p_vehicle_id) THEN
        RAISE EXCEPTION 'fn_distancia_recorrida: el vehículo % no existe', p_vehicle_id
              USING ERRCODE = 'no_data_found';
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM device_assignments da
         WHERE da.vehicle_id = p_vehicle_id
           AND da.device_id IS NOT NULL
           AND da.assigned_from <= p_fecha
           AND (da.assigned_to IS NULL OR da.assigned_to > p_fecha)
    ) THEN
        RAISE NOTICE 'fn_distancia_recorrida: el vehículo % no tiene dispositivo asignado el %',
                     p_vehicle_id, p_fecha;
        RETURN NULL;
    END IF;

    SELECT COALESCE(SUM(ST_Length(t.trayecto::geography)), 0)
      INTO v_metros
      FROM (
            SELECT ST_MakeLine(g.location::geometry ORDER BY g."timestamp") AS trayecto
              FROM device_assignments da
              JOIN gps_data g
                ON g.device_id = da.device_id
             WHERE da.vehicle_id = p_vehicle_id
               AND da.assigned_from <= p_fecha
               AND (da.assigned_to IS NULL OR da.assigned_to > p_fecha)
               AND g."timestamp" >= v_desde
               AND g."timestamp" <  v_hasta
               AND NOT (ST_X(g.location::geometry) = 0 AND ST_Y(g.location::geometry) = 0)
             GROUP BY da.id
           ) t;

    RETURN ROUND((v_metros / 1000.0)::numeric, 2);  -- kilómetros
END;
$$;

COMMENT ON FUNCTION public.fn_distancia_recorrida(INTEGER, DATE, TEXT) IS
    'Km recorridos por un vehículo en un día local (por defecto America/Lima). '
    'NULL si no tiene dispositivo asignado ese día; excluye puntos POINT(0 0). Ver sql/functions/001.';
