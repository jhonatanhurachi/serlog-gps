-- =============================================================================
-- 013_fn_alerta_evento_geocerca.sql
-- Función del trigger trg_alerta_evento_geocerca (014) sobre geofence_events.
-- Requiere 010 (alertas_operativas).
--
-- Tabla de origen:
--   geofence_events(id int4 PK, vehicle_id int4 NOT NULL, geofence_id int4 NOT NULL,
--                   event_type text CHECK IN ('in','out'), location geography,
--                   speed float8, angle float8, gps_timestamp timestamp (UTC),
--                   assignment_id int4 -> geofence_schedule_assignments,
--                   distance_from_center float8, created_at timestamptz)
--   UNIQUE (assignment_id, event_type, gps_timestamp): un INSERT ... ON CONFLICT
--   DO NOTHING descartado no dispara triggers AFTER INSERT, así que no se duplican
--   alertas. (geofence_vehicle_state guarda el estado actual por asignación, no
--   eventos, por lo que no lleva trigger de INSERT.)
--
-- Registra cada entrada/salida como GEOCERCA_ENTRADA / GEOCERCA_SALIDA con:
--   vehicle_id del evento (coincide al 100 % con geofence_schedule_assignments),
--   device_id resuelto vía device_assignments, valor = velocidad en el evento,
--   umbral = NULL, detalle = geocerca, id del evento y distancia al centro.
--
-- Igual que 011: nunca hace fallar el INSERT de origen (EXCEPTION + WARNING).
--
-- Aplicar (requiere un rol con privilegios de escritura):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/functions/013_fn_alerta_evento_geocerca.sql
-- =============================================================================

DO $$
BEGIN
    IF to_regclass('public.alertas_operativas') IS NULL THEN
        RAISE EXCEPTION 'Falta alertas_operativas: aplicar primero sql/tables/010_alertas_operativas.sql';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.fn_alerta_evento_geocerca()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    c_zona      CONSTANT TEXT := 'America/Lima';
    -- gps_timestamp es NULL-able: si falta, se usa el instante de registro.
    v_evento    TIMESTAMPTZ := COALESCE(NEW.gps_timestamp AT TIME ZONE 'UTC', NEW.created_at, now());
    v_device_id INTEGER;
    v_geocerca  TEXT;
    v_msg       TEXT;
BEGIN
    BEGIN
        SELECT da.device_id
          INTO v_device_id
          FROM device_assignments da
         WHERE da.vehicle_id = NEW.vehicle_id
           AND da.device_id IS NOT NULL
           AND da.assigned_from <= (v_evento AT TIME ZONE c_zona)::date
           AND (da.assigned_to IS NULL OR da.assigned_to > (v_evento AT TIME ZONE c_zona)::date)
         ORDER BY da.assigned_from DESC
         LIMIT 1;

        SELECT g.name INTO v_geocerca FROM geofences g WHERE g.id = NEW.geofence_id;

        INSERT INTO alertas_operativas
               (tipo_alerta, device_id, vehicle_id, valor, umbral,
                fecha_evento, fecha_ultimo_evento, puntos, detalle)
        VALUES (CASE NEW.event_type WHEN 'in' THEN 'GEOCERCA_ENTRADA' ELSE 'GEOCERCA_SALIDA' END,
                v_device_id, NEW.vehicle_id, NEW.speed, NULL,
                v_evento, v_evento, 1,
                jsonb_build_object('geofence_id', NEW.geofence_id,
                                   'geocerca', v_geocerca,
                                   'geofence_event_id', NEW.id,
                                   'distancia_centro_m', NEW.distance_from_center));
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
        RAISE WARNING 'fn_alerta_evento_geocerca: no se registró la alerta (evento %): %', NEW.id, v_msg;
    END;
    RETURN NULL;
END;
$$;

REVOKE ALL ON FUNCTION public.fn_alerta_evento_geocerca() FROM PUBLIC;

COMMENT ON FUNCTION public.fn_alerta_evento_geocerca() IS
    'Trigger de geofence_events: registra entradas/salidas en alertas_operativas. Nunca bloquea el INSERT de origen.';
