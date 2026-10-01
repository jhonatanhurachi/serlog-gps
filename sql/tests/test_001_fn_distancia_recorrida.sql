-- =============================================================================
-- test_001_fn_distancia_recorrida.sql
-- Prueba de solo lectura de fn_distancia_recorrida (ejecutar después de aplicar 001).
--
-- Compara la función contra un cálculo independiente: suma de ST_Distance entre
-- puntos consecutivos (LAG) de cada asignación. Ambos deben coincidir. Si el
-- vehículo no tiene asignación vigente ese día, lo esperado es NULL.
-- Valores de referencia (misma lógica ejecutada como SELECT):
--   (1, 2025-08-12) = 192.29   (10, 2025-08-13) = 199.01   (8, 2026-09-20) = 118.32
--   (1, 2025-08-13) = NULL -> día de traspaso: el dispositivo 1 ya es del vehículo 10
--
-- Uso:
--   psql "$DATABASE_URL_ADMIN" -v ON_ERROR_STOP=1 -f sql/tests/test_001_fn_distancia_recorrida.sql
-- =============================================================================

\echo '== Función vs cálculo independiente (columna ok debe ser true) =='
WITH casos(veh, fecha) AS (
    VALUES (1, DATE '2025-08-12'), (10, DATE '2025-08-13'), (1, DATE '2025-08-13'),
           (8, DATE '2026-09-20'), (4, DATE '2026-09-22'), (2, DATE '2026-09-25')
),
limites AS (
    SELECT veh, fecha,
           (fecha::timestamp       AT TIME ZONE 'America/Lima') AT TIME ZONE 'UTC' AS desde,
           ((fecha + 1)::timestamp AT TIME ZONE 'America/Lima') AT TIME ZONE 'UTC' AS hasta
      FROM casos
),
segmentos AS (
    SELECT l.veh, l.fecha,
           ST_Distance(g.location, lag(g.location) OVER (PARTITION BY da.id ORDER BY g."timestamp")) AS m
      FROM limites l
      JOIN device_assignments da
        ON da.vehicle_id = l.veh
       AND da.assigned_from <= l.fecha
       AND (da.assigned_to IS NULL OR da.assigned_to > l.fecha)
      JOIN gps_data g
        ON g.device_id = da.device_id
       AND g."timestamp" >= l.desde AND g."timestamp" < l.hasta
       AND NOT (ST_X(g.location::geometry) = 0 AND ST_Y(g.location::geometry) = 0)
),
esperado AS (
    SELECT c.veh, c.fecha,
           CASE WHEN EXISTS (SELECT 1 FROM device_assignments da
                              WHERE da.vehicle_id = c.veh
                                AND da.device_id IS NOT NULL
                                AND da.assigned_from <= c.fecha
                                AND (da.assigned_to IS NULL OR da.assigned_to > c.fecha))
                THEN ROUND((COALESCE(SUM(s.m), 0) / 1000.0)::numeric, 2)
           END AS km
      FROM casos c
      LEFT JOIN segmentos s USING (veh, fecha)
     GROUP BY c.veh, c.fecha
)
SELECT e.veh, e.fecha,
       fn_distancia_recorrida(e.veh, e.fecha) AS km_funcion,
       e.km                                   AS km_esperado,
       fn_distancia_recorrida(e.veh, e.fecha) IS NOT DISTINCT FROM e.km AS ok
  FROM esperado e
 ORDER BY e.fecha, e.veh;

\echo '== Casos borde =='
-- Parámetro NULL -> NULL (STRICT)
SELECT fn_distancia_recorrida(NULL, DATE '2026-09-20') IS NULL AS null_vehiculo_ok,
       fn_distancia_recorrida(8, NULL)                IS NULL AS null_fecha_ok;

-- Día sin asignación vigente -> NULL + NOTICE (vehículo 10 empieza el 2025-08-13)
SELECT fn_distancia_recorrida(10, DATE '2025-08-12') IS NULL AS sin_asignacion_ok;

-- Asignación vigente pero sin puntos (fecha futura) -> 0.00
SELECT fn_distancia_recorrida(8, DATE '2027-06-01') = 0 AS sin_puntos_ok;

-- Vehículo inexistente -> error P0002 (se captura para no abortar el script)
DO $$
BEGIN
    PERFORM fn_distancia_recorrida(-1, DATE '2026-09-20');
    RAISE EXCEPTION 'FALLO: se esperaba error para vehículo inexistente';
EXCEPTION
    WHEN no_data_found THEN RAISE NOTICE 'vehiculo_inexistente_ok';
END;
$$;
