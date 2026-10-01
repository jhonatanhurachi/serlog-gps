-- =============================================================================
-- 010_alertas_operativas.sql
-- Tabla de alertas operativas en tiempo real. La escriben los triggers 012
-- (exceso de velocidad sobre gps_data) y 014 (eventos de geocerca).
--
-- Columnas base: id, tipo_alerta, device_id, vehicle_id, valor, umbral,
-- fecha_evento, detectado_en. Columnas adicionales:
--  * fecha_ultimo_evento, puntos: un exceso de velocidad dura minutos y el GPS
--    reporta cada 5 s. Sin agrupar, un solo episodio de 2 min generaría ~24 alertas.
--    La alerta representa un EPISODIO: fecha_evento = primer punto, fecha_ultimo_evento
--    = último punto, puntos = cantidad y valor = velocidad máxima alcanzada.
--  * detalle JSONB: datos propios de cada tipo (geocerca, id del evento de
--    origen) sin crear columnas que solo usa un tipo.
--
-- Tipos:
--  EXCESO_VELOCIDAD       speed > 90 km/h (ver 012 para la justificación del umbral)
--  VELOCIDAD_IMPLAUSIBLE  speed > 150 km/h: falla del sensor, no un exceso real
--                         (p. ej. 204 km/h entre dos lecturas de 0 y 9 km/h)
--  GEOCERCA_ENTRADA / GEOCERCA_SALIDA   eventos de geofence_events
--
-- fecha_evento / fecha_ultimo_evento son TIMESTAMPTZ: el origen (gps_data.timestamp,
-- geofence_events.gps_timestamp) es timestamp sin zona en UTC y se convierte al guardar.
--
-- Aplicar (requiere un rol con privilegios de escritura):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/tables/010_alertas_operativas.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.alertas_operativas (
    id                  BIGINT           GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tipo_alerta         TEXT             NOT NULL,
    device_id           INTEGER,
    vehicle_id          INTEGER,
    valor               DOUBLE PRECISION,
    umbral              DOUBLE PRECISION,
    fecha_evento        TIMESTAMPTZ      NOT NULL,
    fecha_ultimo_evento TIMESTAMPTZ      NOT NULL,
    puntos              INTEGER          NOT NULL DEFAULT 1,
    detalle             JSONB,
    detectado_en        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CONSTRAINT alertas_operativas_tipo_ck CHECK (tipo_alerta IN (
        'EXCESO_VELOCIDAD', 'VELOCIDAD_IMPLAUSIBLE', 'GEOCERCA_ENTRADA', 'GEOCERCA_SALIDA')),
    CONSTRAINT alertas_operativas_fechas_ck CHECK (fecha_ultimo_evento >= fecha_evento),
    CONSTRAINT alertas_operativas_puntos_ck CHECK (puntos >= 1)
);

-- Lo usa el trigger de velocidad para encontrar el episodio abierto del dispositivo.
CREATE INDEX IF NOT EXISTS alertas_operativas_episodio_idx
    ON public.alertas_operativas (device_id, tipo_alerta, fecha_ultimo_evento DESC);

-- Consultas de reportería: alertas de un vehículo por fecha.
CREATE INDEX IF NOT EXISTS alertas_operativas_vehiculo_idx
    ON public.alertas_operativas (vehicle_id, fecha_evento);

REVOKE ALL ON public.alertas_operativas FROM PUBLIC;

COMMENT ON TABLE public.alertas_operativas IS
    'Alertas en tiempo real (exceso de velocidad por episodio y eventos de geocerca). Escrita por triggers 012 y 014.';
