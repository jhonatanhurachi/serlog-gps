-- =============================================================================
-- 000_preflight_schema.sql
-- Verificación previa del esquema (solo lectura: consultas SELECT y un bloque DO
-- que únicamente lee el catálogo).
--
-- Reporta versión, extensiones, columnas, restricciones, particiones de gps_data
-- y asignaciones solapadas, y aborta si el esquema difiere del esperado por los
-- scripts 001-004 (PostgreSQL 14+, PostGIS, tipos de columna). Ejecutarlo antes
-- de aplicar 001-004 y conservar su salida.
--
-- Uso (basta un rol de solo lectura):
--   psql "$DATABASE_URL_READONLY" -v ON_ERROR_STOP=1 -f sql/checks/000_preflight_schema.sql
-- =============================================================================

\echo '== Versión del servidor y extensiones =='
SELECT version();
SELECT extname, extversion FROM pg_extension ORDER BY extname;

\echo '== Columnas de las tablas involucradas =='
SELECT table_name, ordinal_position, column_name, udt_name, is_nullable, column_default
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name IN ('gps_data', 'device_assignments', 'vehicles', 'audit_device_assignments')
 ORDER BY table_name, ordinal_position;

\echo '== Constraints =='
SELECT conrelid::regclass AS tabla, conname, contype, pg_get_constraintdef(oid) AS definicion
  FROM pg_constraint
 WHERE conrelid IN ('public.gps_data'::regclass,
                    'public.device_assignments'::regclass,
                    'public.vehicles'::regclass)
 ORDER BY 1, 2;

\echo '== Particiones de gps_data (revisar huecos entre rangos) =='
SELECT c.relname AS particion, pg_get_expr(c.relpartbound, c.oid) AS rango
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
 WHERE i.inhparent = 'public.gps_data'::regclass
 ORDER BY 1;

\echo '== Tipo espacial y SRID de gps_data.location =='
SELECT f_table_name, f_geography_column, srid, type
  FROM geography_columns
 WHERE f_table_schema = 'public' AND f_table_name = 'gps_data';

\echo '== Triggers existentes sobre device_assignments =='
SELECT tgname, pg_get_triggerdef(oid) AS definicion
  FROM pg_trigger
 WHERE tgrelid = 'public.device_assignments'::regclass AND NOT tgisinternal;

\echo '== Objetos que 001-004 van a crear o reemplazar (vacío = aún no existen) =='
SELECT 'function' AS objeto, p.oid::regprocedure::text AS nombre
  FROM pg_proc p
 WHERE p.pronamespace = 'public'::regnamespace
   AND p.proname IN ('fn_distancia_recorrida', 'fn_audit_device_assignments')
UNION ALL
SELECT 'table', to_regclass('public.audit_device_assignments')::text
 WHERE to_regclass('public.audit_device_assignments') IS NOT NULL;

\echo '== Asignaciones que se solapan (mismo dispositivo o mismo vehículo) =='
-- Semántica usada por 001: rango semiabierto [assigned_from, assigned_to).
-- Un traspaso el mismo día (assigned_to de una = assigned_from de otra) NO es solape.
SELECT a.id AS asignacion_a, b.id AS asignacion_b,
       a.device_id AS disp_a, b.device_id AS disp_b,
       a.vehicle_id AS veh_a, b.vehicle_id AS veh_b
  FROM public.device_assignments a
  JOIN public.device_assignments b
    ON a.id < b.id
   AND (a.device_id = b.device_id OR a.vehicle_id = b.vehicle_id)
   AND a.assigned_from < COALESCE(b.assigned_to, 'infinity'::date)
   AND b.assigned_from < COALESCE(a.assigned_to, 'infinity'::date);

\echo '== Aserciones (aborta si alguna suposición ya no se cumple) =='
DO $$
DECLARE
    r RECORD;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
        RAISE EXCEPTION 'PostGIS no está instalado en esta base.';
    END IF;

    -- CREATE OR REPLACE TRIGGER (script 004) requiere PostgreSQL 14 o superior.
    IF current_setting('server_version_num')::int < 140000 THEN
        RAISE EXCEPTION 'Se requiere PostgreSQL 14+; el servidor es %.', current_setting('server_version');
    END IF;

    FOR r IN
        SELECT * FROM (VALUES
            ('gps_data',           'device_id',     'int4'),
            ('gps_data',           'timestamp',     'timestamp'),
            ('gps_data',           'location',      'geography'),
            ('device_assignments', 'id',            'int4'),
            ('device_assignments', 'device_id',     'int4'),
            ('device_assignments', 'vehicle_id',    'int4'),
            ('device_assignments', 'assigned_from', 'date'),
            ('device_assignments', 'assigned_to',   'date'),
            ('vehicles',           'id',            'int4')
        ) AS t(tabla, columna, tipo)
    LOOP
        PERFORM 1
           FROM information_schema.columns c
          WHERE c.table_schema = 'public'
            AND c.table_name   = r.tabla
            AND c.column_name  = r.columna
            AND c.udt_name     = r.tipo;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Esquema distinto al validado: public.%.% no existe o ya no es de tipo %.',
                            r.tabla, r.columna, r.tipo;
        END IF;
    END LOOP;

    IF NOT EXISTS (SELECT 1 FROM geography_columns
                    WHERE f_table_schema = 'public' AND f_table_name = 'gps_data'
                      AND f_geography_column = 'location' AND srid = 4326) THEN
        RAISE EXCEPTION 'gps_data.location ya no es geography con SRID 4326.';
    END IF;

    RAISE NOTICE 'Preflight OK: el esquema coincide con el esperado.';
END;
$$;
