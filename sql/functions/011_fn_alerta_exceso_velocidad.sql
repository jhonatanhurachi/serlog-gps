-- =============================================================================
-- 011_fn_alerta_exceso_velocidad.sql
-- Función del trigger trg_alerta_exceso_velocidad (012) sobre gps_data.
-- Requiere 010 (alertas_operativas).
--
-- Solo se ejecuta para las filas que ya pasaron el WHEN (NEW.speed > 90) del
-- trigger: el 99,99 % de los puntos no llega a invocarla.
--
-- Qué hace con cada punto sobre el umbral:
--  1. Ignora puntos POINT(0 0) (sin señal satelital; su speed no es confiable).
--  2. Clasifica: > 150 km/h = VELOCIDAD_IMPLAUSIBLE (falla de sensor), si no
--     EXCESO_VELOCIDAD.
--  3. Resuelve el vehículo vía device_assignments (día America/Lima, rango
--     [assigned_from, assigned_to)), igual que fn_distancia_recorrida.
--     gps_data.vehicle_id no se usa (está vacío).
--  4. Si el dispositivo tiene un episodio del mismo tipo cuyo último punto está a
--     5 min o menos, lo extiende (valor = máximo, puntos + 1). Si no, abre uno nuevo.
--
-- REGLA CRÍTICA: esta función NUNCA debe hacer fallar el INSERT en gps_data. Si
-- falla, la ingesta del GPS perdería el punto. Todo error se captura, se reporta
-- con RAISE WARNING y el punto se guarda igual. El bloque EXCEPTION crea una
-- subtransacción, pero solo en las filas que pasan el WHEN, que son raras.
--
-- SECURITY DEFINER + search_path fijo: el rol que inserta en gps_data (backend)
-- no necesita permisos sobre alertas_operativas.
--
-- Aplicar (requiere un rol con privilegios de escritura):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/functions/011_fn_alerta_exceso_velocidad.sql
-- =============================================================================

DO $$
BEGIN
    IF to_regclass('public.alertas_operativas') IS NULL THEN
        RAISE EXCEPTION 'Falta alertas_operativas: aplicar primero sql/tables/010_alertas_operativas.sql';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.fn_alerta_exceso_velocidad()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
    -- c_umbral debe coincidir con el WHEN de trg_alerta_exceso_velocidad (012).
    c_umbral       CONSTANT DOUBLE PRECISION := 90;
    c_implausible  CONSTANT DOUBLE PRECISION := 150;
    c_ventana      CONSTANT INTERVAL := INTERVAL '5 minutes';
    c_zona         CONSTANT TEXT := 'America/Lima';
    v_tipo         TEXT;
    v_umbral       DOUBLE PRECISION;
    v_evento       TIMESTAMPTZ := NEW."timestamp" AT TIME ZONE 'UTC';
    v_vehicle_id   INTEGER;
    v_alerta_id    BIGINT;
    v_msg          TEXT;
BEGIN
    BEGIN
        IF ST_X(NEW.location::geometry) = 0 AND ST_Y(NEW.location::geometry) = 0 THEN
            RETURN NULL;
        END IF;

        IF NEW.speed > c_implausible THEN
            v_tipo := 'VELOCIDAD_IMPLAUSIBLE';  v_umbral := c_implausible;
        ELSE
            v_tipo := 'EXCESO_VELOCIDAD';       v_umbral := c_umbral;
        END IF;

        SELECT da.vehicle_id
          INTO v_vehicle_id
          FROM device_assignments da
         WHERE da.device_id = NEW.device_id
           AND da.assigned_from <= (v_evento AT TIME ZONE c_zona)::date
           AND (da.assigned_to IS NULL OR da.assigned_to > (v_evento AT TIME ZONE c_zona)::date)
         ORDER BY da.assigned_from DESC
         LIMIT 1;

        -- Episodio abierto: mismo dispositivo y tipo, último punto dentro de la ventana
        -- (hacia atrás o hacia adelante, por si los puntos llegan desordenados).
        SELECT a.id
          INTO v_alerta_id
          FROM alertas_operativas a
         WHERE a.device_id   = NEW.device_id
           AND a.tipo_alerta = v_tipo
           AND a.fecha_ultimo_evento >= v_evento - c_ventana
           AND a.fecha_evento        <= v_evento + c_ventana
         ORDER BY a.fecha_ultimo_evento DESC
         LIMIT 1
         FOR UPDATE;

        IF v_alerta_id IS NOT NULL THEN
            UPDATE alertas_operativas
               SET valor               = GREATEST(valor, NEW.speed),
                   fecha_evento        = LEAST(fecha_evento, v_evento),
                   fecha_ultimo_evento = GREATEST(fecha_ultimo_evento, v_evento),
                   puntos              = puntos + 1
             WHERE id = v_alerta_id;
        ELSE
            INSERT INTO alertas_operativas
                   (tipo_alerta, device_id, vehicle_id, valor, umbral,
                    fecha_evento, fecha_ultimo_evento, puntos, detalle)
            VALUES (v_tipo, NEW.device_id, v_vehicle_id, NEW.speed, v_umbral,
                    v_evento, v_evento, 1,
                    jsonb_build_object('lon', ST_X(NEW.location::geometry),
                                       'lat', ST_Y(NEW.location::geometry)));
        END IF;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
        RAISE WARNING 'fn_alerta_exceso_velocidad: no se registró la alerta (device %, %): %',
                      NEW.device_id, NEW."timestamp", v_msg;
    END;
    RETURN NULL;  -- AFTER trigger: el valor de retorno se ignora.
END;
$$;

REVOKE ALL ON FUNCTION public.fn_alerta_exceso_velocidad() FROM PUBLIC;

COMMENT ON FUNCTION public.fn_alerta_exceso_velocidad() IS
    'Trigger de gps_data: registra episodios de exceso de velocidad (> 90 km/h) en alertas_operativas. Nunca bloquea la ingesta.';
