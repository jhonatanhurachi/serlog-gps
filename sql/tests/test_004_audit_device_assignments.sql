-- =============================================================================
-- test_004_audit_device_assignments.sql
-- Prueba del trigger de auditoría (ejecutar después de aplicar 002, 003 y 004).
--
-- Se ejecuta dentro de una transacción que termina en ROLLBACK: no persiste
-- ningún cambio en device_assignments ni en audit_device_assignments.
-- Toma locks de fila en device_assignments mientras dura; conviene ejecutarlo en
-- staging o en un horario sin operación.
-- Efecto colateral: las secuencias no se revierten con ROLLBACK, por lo que se
-- consume un id de device_assignments_id_seq.
--
-- Uso:
--   psql "$DATABASE_URL_ADMIN" -v ON_ERROR_STOP=1 -f sql/tests/test_004_audit_device_assignments.sql
-- =============================================================================

BEGIN;

-- Fila de prueba: dispositivo y vehículo existentes, fechas lejanas para no
-- interferir con asignaciones reales.
INSERT INTO device_assignments (device_id, vehicle_id, assigned_from, assigned_to)
VALUES ((SELECT min(id) FROM devices), (SELECT min(id) FROM vehicles), DATE '2099-01-01', DATE '2099-01-02');

-- UPDATE sin cambios: NO debe auditarse.
UPDATE device_assignments SET assigned_to = assigned_to WHERE assigned_from = DATE '2099-01-01';

-- UPDATE real: sí debe auditarse.
UPDATE device_assignments SET assigned_to = DATE '2099-01-03' WHERE assigned_from = DATE '2099-01-01';

DELETE FROM device_assignments WHERE assigned_from = DATE '2099-01-01';

\echo '== Esperado: exactamente 3 filas, en orden INSERT, UPDATE, DELETE =='
SELECT audit_id, assignment_id, accion,
       valor_anterior ->> 'assigned_to' AS antes,
       valor_nuevo    ->> 'assigned_to' AS despues,
       usuario_bd, aplicacion
  FROM audit_device_assignments
 WHERE (COALESCE(valor_nuevo, valor_anterior) ->> 'assigned_from') = '2099-01-01'
 ORDER BY audit_id;

DO $$
DECLARE
    v_acciones TEXT;
BEGIN
    SELECT string_agg(accion, ',' ORDER BY audit_id) INTO v_acciones
      FROM audit_device_assignments
     WHERE (COALESCE(valor_nuevo, valor_anterior) ->> 'assigned_from') = '2099-01-01';
    IF v_acciones IS DISTINCT FROM 'INSERT,UPDATE,DELETE' THEN
        RAISE EXCEPTION 'FALLO: se esperaba INSERT,UPDATE,DELETE y se obtuvo %', v_acciones;
    END IF;
    RAISE NOTICE 'trigger_auditoria_ok';
END;
$$;

ROLLBACK;
