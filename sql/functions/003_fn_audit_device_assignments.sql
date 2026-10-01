-- =============================================================================
-- 003_fn_audit_device_assignments.sql
-- Crea/reemplaza la función de trigger public.fn_audit_device_assignments().
-- Requiere 002. La usa el trigger de 004.
--
-- DECISIONES DE DISEÑO
--  1. Audita INSERT, UPDATE y DELETE (valor anterior y nuevo en JSONB).
--  2. Un UPDATE sin cambios reales (OLD idéntico a NEW) no se registra. Se decide
--     aquí y no con WHEN en el trigger, porque un trigger que también cubre
--     INSERT/DELETE no puede referirse a OLD y NEW en su WHEN.
--  3. accion = TG_OP.
--  4. RETURN NULL: en un trigger AFTER el valor devuelto se ignora.
--  5. SECURITY DEFINER + search_path fijo: el rol que modifica device_assignments
--     no necesita permiso de INSERT sobre la tabla de auditoría ni puede escribir
--     en ella directamente; el search_path fijo evita que un objeto homónimo en
--     otro esquema suplante a los usados por la función. Por eso la tabla registra
--     session_user y no current_user (ver 002).
--
-- LIMITACIÓN: TRUNCATE no dispara triggers por fila y no queda auditado.
--
-- Aplicar (requiere un rol con privilegios de escritura, preferentemente el dueño
-- de device_assignments):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/functions/003_fn_audit_device_assignments.sql
-- =============================================================================

DO $$
BEGIN
    IF to_regclass('public.audit_device_assignments') IS NULL THEN
        RAISE EXCEPTION 'Falta public.audit_device_assignments: aplicar primero 002_audit_device_assignments_table.sql';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.fn_audit_device_assignments()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_device_assignments
               (assignment_id, device_id, vehicle_id, accion, valor_anterior, valor_nuevo)
        VALUES (NEW.id, NEW.device_id, NEW.vehicle_id, TG_OP, NULL, to_jsonb(NEW));

    ELSIF TG_OP = 'UPDATE' THEN
        IF to_jsonb(OLD) = to_jsonb(NEW) THEN
            RETURN NULL;  -- UPDATE sin cambios: no se audita.
        END IF;
        INSERT INTO audit_device_assignments
               (assignment_id, device_id, vehicle_id, accion, valor_anterior, valor_nuevo)
        VALUES (NEW.id, NEW.device_id, NEW.vehicle_id, TG_OP, to_jsonb(OLD), to_jsonb(NEW));

    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_device_assignments
               (assignment_id, device_id, vehicle_id, accion, valor_anterior, valor_nuevo)
        VALUES (OLD.id, OLD.device_id, OLD.vehicle_id, TG_OP, to_jsonb(OLD), NULL);
    END IF;

    RETURN NULL;  -- AFTER trigger: el valor de retorno se ignora.
END;
$$;

REVOKE ALL ON FUNCTION public.fn_audit_device_assignments() FROM PUBLIC;

COMMENT ON FUNCTION public.fn_audit_device_assignments() IS
    'Función de trigger: registra INSERT/UPDATE/DELETE de device_assignments en audit_device_assignments.';
