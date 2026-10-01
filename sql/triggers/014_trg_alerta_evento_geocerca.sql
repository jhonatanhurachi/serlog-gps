-- =============================================================================
-- 014_trg_alerta_evento_geocerca.sql
-- Trigger AFTER INSERT sobre geofence_events. Requiere 013.
--
-- Sin WHEN: todos los eventos (in/out) son relevantes. El volumen es bajo
-- (~15 000 eventos en 7 meses, unos 70 por día en toda la flota).
-- Si más adelante solo interesan algunas geocercas, se filtra aquí con
-- WHEN (NEW.geofence_id IN (...)) sin tocar la función.
--
-- Aplicar (requiere un rol con privilegios de escritura, dueño de geofence_events):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/triggers/014_trg_alerta_evento_geocerca.sql
-- =============================================================================

DO $$
BEGIN
    IF to_regprocedure('public.fn_alerta_evento_geocerca()') IS NULL THEN
        RAISE EXCEPTION 'Falta fn_alerta_evento_geocerca(): aplicar primero sql/functions/013_fn_alerta_evento_geocerca.sql';
    END IF;
END;
$$;

CREATE OR REPLACE TRIGGER trg_alerta_evento_geocerca
    AFTER INSERT ON public.geofence_events
    FOR EACH ROW
    EXECUTE FUNCTION public.fn_alerta_evento_geocerca();
