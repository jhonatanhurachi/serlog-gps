-- =============================================================================
-- 009_backfill_fact_recorrido_diario.sql
-- Carga inicial de fact_recorrido_diario: los últimos 60 días con datos en gps_data.
-- Requiere 008 (sp_generar_resumen_diario).
--
-- * Idempotente: sp_generar_resumen_diario hace UPSERT; re-ejecutarlo recalcula y
--   actualiza, nunca duplica filas.
-- * Ventana: 60 días locales (America/Lima) que terminan en el último día con
--   datos, sin fechas futuras y sin el día en curso (incompleto). Los días sin
--   ningún punto no generan filas (ver 008).
-- * Cada día y cada vehículo se confirman por separado (COMMIT dentro del
--   procedimiento): si se interrumpe, lo ya cargado queda y basta re-ejecutarlo.
-- * Para encontrar el último día se escanea solo la última semana de gps_data
--   (gps_data no tiene índice por "timestamp" solo; la PK empieza por device_id).
--
-- Ejecutar SIN -1 (el DO block no puede estar dentro de una transacción explícita,
-- porque el procedimiento hace COMMIT):
--   psql "$DATABASE_URL_ADMIN" -v ON_ERROR_STOP=1 -f sql/procedures/009_backfill_fact_recorrido_diario.sql
-- =============================================================================

DO $$
DECLARE
    c_dias   CONSTANT INTEGER := 60;
    c_zona   CONSTANT TEXT    := 'America/Lima';
    v_ahora_utc TIMESTAMP     := now() AT TIME ZONE 'UTC';
    v_ultimo    DATE;
    v_dia       DATE;
    v_t0        TIMESTAMPTZ   := clock_timestamp();
    v_t_dia     TIMESTAMPTZ;
BEGIN
    IF to_regprocedure('public.sp_generar_resumen_diario(date)') IS NULL THEN
        RAISE EXCEPTION 'Falta sp_generar_resumen_diario: aplicar primero 008_sp_generar_resumen_diario.sql';
    END IF;

    SELECT ((max(g."timestamp") AT TIME ZONE 'UTC') AT TIME ZONE c_zona)::date
      INTO v_ultimo
      FROM public.gps_data g
     WHERE g."timestamp" >= v_ahora_utc - INTERVAL '7 days'
       AND g."timestamp" <= v_ahora_utc;               -- ignora timestamps futuros

    IF v_ultimo IS NULL THEN
        RAISE EXCEPTION 'No hay datos en gps_data en los últimos 7 días; revisar la ingesta antes del backfill.';
    END IF;

    -- El día en curso todavía está incompleto: se excluye.
    v_ultimo := LEAST(v_ultimo, (now() AT TIME ZONE c_zona)::date - 1);

    RAISE NOTICE 'Backfill de % días: % a %', c_dias, v_ultimo - (c_dias - 1), v_ultimo;

    FOR v_dia IN
        SELECT d::date FROM generate_series(v_ultimo - (c_dias - 1), v_ultimo, INTERVAL '1 day') AS d
    LOOP
        v_t_dia := clock_timestamp();
        CALL public.sp_generar_resumen_diario(v_dia);
        RAISE NOTICE '  % listo en % s', v_dia,
                     round(EXTRACT(EPOCH FROM clock_timestamp() - v_t_dia)::numeric, 2);
    END LOOP;

    RAISE NOTICE 'Backfill terminado en % s', round(EXTRACT(EPOCH FROM clock_timestamp() - v_t0)::numeric, 1);
END;
$$;

-- Verificación rápida del resultado
SELECT min(fecha) AS desde, max(fecha) AS hasta, count(*) AS filas,
       count(DISTINCT vehicle_id) AS vehiculos, round(sum(km_recorridos), 2) AS km_total
  FROM public.fact_recorrido_diario;
