-- =============================================================================
-- 004_trg_audit_device_assignments.sql
-- Crea/reemplaza el trigger trg_audit_device_assignments sobre device_assignments.
-- Requiere 002 y 003.
--
-- Notas:
--  * CREATE OR REPLACE TRIGGER (PostgreSQL 14+) hace el script re-ejecutable.
--  * Cubre INSERT, UPDATE y DELETE (ver 003); TRUNCATE no queda auditado.
--  * Nombres calificados con esquema (public.).
--
-- Aplicar (requiere un rol con privilegios de escritura, dueño de la tabla):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/triggers/004_trg_audit_device_assignments.sql
-- =============================================================================

DO $$
BEGIN
    IF to_regprocedure('public.fn_audit_device_assignments()') IS NULL THEN
        RAISE EXCEPTION 'Falta public.fn_audit_device_assignments(): aplicar primero 003_fn_audit_device_assignments.sql';
    END IF;
END;
$$;

CREATE OR REPLACE TRIGGER trg_audit_device_assignments
    AFTER INSERT OR UPDATE OR DELETE ON public.device_assignments
    FOR EACH ROW
    EXECUTE FUNCTION public.fn_audit_device_assignments();
