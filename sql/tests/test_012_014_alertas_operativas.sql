-- =============================================================================
-- test_012_014_alertas_operativas.sql
-- Prueba de los triggers de alertas (ejecutar después de aplicar 010 a 014).
--
-- Se ejecuta dentro de BEGIN ... ROLLBACK: no persiste ninguna fila de prueba en
-- gps_data, geofence_events ni alertas_operativas. Se usan fechas de 2028-06-01
-- (partición gps_data_2028, sin datos reales) para no chocar con la PK
-- (device_id, "timestamp").
-- Efectos colaterales: se consumen ids de secuencias y el caso 9 toma un lock
-- exclusivo breve sobre alertas_operativas. Conviene ejecutarlo en staging o en
-- un horario sin operación.
--
-- Uso:
--   psql "$DATABASE_URL_ADMIN" -v ON_ERROR_STOP=1 -f sql/tests/test_012_014_alertas_operativas.sql
-- =============================================================================

BEGIN;

-- Dispositivo de prueba: el 3 (asignado al vehículo 8 desde 2024-11-27, sin fecha de fin).
-- Cada fila indica si DEBE o NO DEBE disparar alerta.
INSERT INTO gps_data (device_id, "timestamp", location, speed) VALUES
  (3, '2028-06-01 12:00:00', 'SRID=4326;POINT(-71.53 -16.40)', 85),     -- 1 NO: bajo el umbral
  (3, '2028-06-01 12:00:03', 'SRID=4326;POINT(-71.53 -16.40)', 90),     -- 2 NO: 90 no es > 90
  (3, '2028-06-01 12:00:05', 'SRID=4326;POINT(-71.53 -16.40)', 90.5),   -- 3 SÍ: abre episodio A
  (3, '2028-06-01 12:01:00', 'SRID=4326;POINT(-71.52 -16.40)', 97),     -- 4 SÍ: extiende A (máx 97)
  (3, '2028-06-01 12:03:00', 'SRID=4326;POINT(-71.51 -16.40)', 93),     -- 5 SÍ: extiende A (sigue 97)
  (3, '2028-06-01 12:10:00', 'SRID=4326;POINT(-71.50 -16.40)', 92),     -- 6 SÍ: 7 min después -> episodio B
  (3, '2028-06-01 12:20:00', 'SRID=4326;POINT(-71.49 -16.40)', 204),    -- 7 SÍ: VELOCIDAD_IMPLAUSIBLE
  (3, '2028-06-01 12:30:00', 'SRID=4326;POINT(0 0)',            120);   -- 8 NO: null island

\echo '== Alertas de velocidad generadas (esperado: A 90.5->97 con 3 puntos, B 92, implausible 204)'
SELECT tipo_alerta, device_id, vehicle_id, valor, umbral, puntos,
       fecha_evento AT TIME ZONE 'UTC' AS inicio_utc, fecha_ultimo_evento AT TIME ZONE 'UTC' AS fin_utc
  FROM alertas_operativas
 WHERE fecha_evento >= '2028-06-01 00:00+00' AND fecha_evento < '2028-06-02 00:00+00'
 ORDER BY fecha_evento;

-- 9 Resiliencia: si la alerta falla, el punto GPS igual se guarda (solo un WARNING).
SAVEPOINT resiliencia;
ALTER TABLE alertas_operativas RENAME TO alertas_operativas_oculta;
INSERT INTO gps_data (device_id, "timestamp", location, speed)
VALUES (3, '2028-06-01 12:40:00', 'SRID=4326;POINT(-71.48 -16.40)', 110);
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM gps_data WHERE device_id = 3 AND "timestamp" = '2028-06-01 12:40:00') THEN
        RAISE EXCEPTION 'FALLO: el punto GPS no se guardó cuando la alerta falló';
    END IF;
    RAISE NOTICE 'resiliencia_ok: el punto se guardó aunque la alerta falló';
END;
$$;
ROLLBACK TO SAVEPOINT resiliencia;

-- 10 Geocerca: entrada y salida del vehículo 8; un duplicado descartado por
-- ON CONFLICT DO NOTHING (como hace el backend) no debe generar alerta.
INSERT INTO geofence_events (vehicle_id, geofence_id, event_type, speed, gps_timestamp, assignment_id, distance_from_center)
SELECT 8, 5, t.event_type, t.speed, t.ts, gsa.id, 15.0
  FROM (VALUES ('in', 12.0, TIMESTAMP '2028-06-01 13:00:00'),
               ('out', 18.0, TIMESTAMP '2028-06-01 13:45:00')) AS t(event_type, speed, ts)
 CROSS JOIN (SELECT id FROM geofence_schedule_assignments WHERE vehicle_id = 8 ORDER BY id LIMIT 1) gsa;

INSERT INTO geofence_events (vehicle_id, geofence_id, event_type, speed, gps_timestamp, assignment_id)
SELECT 8, 5, 'in', 12.0, TIMESTAMP '2028-06-01 13:00:00', id
  FROM geofence_schedule_assignments WHERE vehicle_id = 8 ORDER BY id LIMIT 1
ON CONFLICT DO NOTHING;

\echo '== Alertas de geocerca (esperado: 1 ENTRADA y 1 SALIDA, device 3, geocerca Base)'
SELECT tipo_alerta, device_id, vehicle_id, valor, detalle ->> 'geocerca' AS geocerca,
       fecha_evento AT TIME ZONE 'UTC' AS evento_utc
  FROM alertas_operativas
 WHERE tipo_alerta LIKE 'GEOCERCA%'
   AND fecha_evento >= '2028-06-01 00:00+00' AND fecha_evento < '2028-06-02 00:00+00'
 ORDER BY fecha_evento;

DO $$
DECLARE
    v_resumen TEXT;
BEGIN
    SELECT string_agg(format('%s:%s:%s:%s', tipo_alerta, valor, puntos, vehicle_id), ',' ORDER BY fecha_evento)
      INTO v_resumen
      FROM alertas_operativas
     WHERE fecha_evento >= '2028-06-01 00:00+00' AND fecha_evento < '2028-06-02 00:00+00';
    IF v_resumen IS DISTINCT FROM
       'EXCESO_VELOCIDAD:97:3:8,EXCESO_VELOCIDAD:92:1:8,VELOCIDAD_IMPLAUSIBLE:204:1:8,GEOCERCA_ENTRADA:12:1:8,GEOCERCA_SALIDA:18:1:8' THEN
        RAISE EXCEPTION 'FALLO: alertas inesperadas: %', v_resumen;
    END IF;
    RAISE NOTICE 'alertas_ok: %', v_resumen;
END;
$$;

ROLLBACK;
