-- =============================================================================
-- 002_audit_device_assignments_table.sql
-- Crea la tabla de auditoría public.audit_device_assignments.
-- Prerrequisito de 003 (función) y 004 (trigger).
--
-- Origen auditado:
--   device_assignments(id int4 PK, device_id int4 NULL, vehicle_id int4 NULL, ...)
--
-- DECISIONES DE DISEÑO
--  1. audit_id BIGINT GENERATED ALWAYS AS IDENTITY: estándar SQL, impide insertar
--     ids manuales y admite crecimiento indefinido.
--  2. assignment_id/device_id/vehicle_id son INTEGER, igual que en el origen.
--     assignment_id es NOT NULL (el id de origen es PK); device_id y vehicle_id
--     admiten NULL porque en el origen también lo hacen.
--  3. fecha_evento TIMESTAMPTZ: el instante de auditoría debe ser inequívoco
--     (servidor en GMT, operación en Lima). now() es el inicio de la transacción;
--     el orden exacto dentro de una misma transacción lo da audit_id.
--  4. usuario_bd usa session_user y no current_user: la función del trigger (003)
--     es SECURITY DEFINER, por lo que current_user sería el dueño de la función.
--  5. aplicacion (application_name) e ip_cliente complementan al usuario de BD
--     cuando varias aplicaciones comparten el mismo rol.
--  6. CHECK sobre accion (INSERT/UPDATE/DELETE) y sobre la coherencia de
--     valor_anterior/valor_nuevo según la acción.
--  7. Sin FK hacia device_assignments: la auditoría debe sobrevivir al DELETE de
--     la fila auditada.
--  8. REVOKE ALL FROM PUBLIC: solo el dueño (y la función SECURITY DEFINER) escribe.
--
-- Aplicar (requiere un rol con privilegios de escritura):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/tables/002_audit_device_assignments_table.sql
-- =============================================================================

-- Guarda: CREATE TABLE IF NOT EXISTS no corrige una tabla existente con otra
-- estructura, así que en ese caso se aborta para revisarla manualmente.
DO $$
BEGIN
    IF to_regclass('public.audit_device_assignments') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name   = 'audit_device_assignments'
                          AND column_name  = 'ip_cliente') THEN
        RAISE EXCEPTION 'public.audit_device_assignments ya existe con otra estructura; revisarla antes de continuar.';
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS public.audit_device_assignments (
    audit_id       BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    assignment_id  INTEGER     NOT NULL,
    device_id      INTEGER,
    vehicle_id     INTEGER,
    accion         TEXT        NOT NULL,
    valor_anterior JSONB,
    valor_nuevo    JSONB,
    fecha_evento   TIMESTAMPTZ NOT NULL DEFAULT now(),
    usuario_bd     TEXT        NOT NULL DEFAULT session_user,
    aplicacion     TEXT                 DEFAULT current_setting('application_name', true),
    ip_cliente     INET                 DEFAULT inet_client_addr(),
    CONSTRAINT audit_device_assignments_accion_ck
        CHECK (accion IN ('INSERT', 'UPDATE', 'DELETE')),
    CONSTRAINT audit_device_assignments_valores_ck
        CHECK (   (accion = 'INSERT' AND valor_anterior IS NULL     AND valor_nuevo IS NOT NULL)
               OR (accion = 'UPDATE' AND valor_anterior IS NOT NULL AND valor_nuevo IS NOT NULL)
               OR (accion = 'DELETE' AND valor_anterior IS NOT NULL AND valor_nuevo IS NULL))
);

CREATE INDEX IF NOT EXISTS audit_device_assignments_assignment_idx
    ON public.audit_device_assignments (assignment_id, audit_id);

CREATE INDEX IF NOT EXISTS audit_device_assignments_fecha_idx
    ON public.audit_device_assignments (fecha_evento);

REVOKE ALL ON public.audit_device_assignments FROM PUBLIC;

COMMENT ON TABLE public.audit_device_assignments IS
    'Historial de INSERT/UPDATE/DELETE sobre device_assignments. Lo escribe trg_audit_device_assignments.';
