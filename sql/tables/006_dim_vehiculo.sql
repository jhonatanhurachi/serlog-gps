-- =============================================================================
-- 006_dim_vehiculo.sql
-- Dimensión de vehículos del esquema estrella de reportería.
--
-- Origen: vehicles(id int4 PK, plate char(7) NOT NULL UNIQUE, created_at timestamp);
-- se copian sus tres atributos.
--
-- Diseño:
--  * placa se guarda con trim(): plate es char(7) (bpchar) y arrastra espacios.
--  * Sin foreign key hacia vehicles: una dimensión de reportería no debe bloquear
--    cambios en la tabla operativa ni perder historia si un vehículo se elimina.
--    Se sincroniza por upsert aquí y en cada corrida de sp_generar_resumen_diario (008).
--  * vehicles.created_at es timestamp sin zona en UTC; se conserva tal cual
--    (creado_en_utc).
--
-- Aplicar (requiere un rol con privilegios de escritura):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/tables/006_dim_vehiculo.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.dim_vehiculo (
    vehicle_id     INTEGER     PRIMARY KEY,
    placa          TEXT        NOT NULL,
    creado_en_utc  TIMESTAMP,
    actualizado_en TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO public.dim_vehiculo (vehicle_id, placa, creado_en_utc)
SELECT v.id, trim(v.plate), v.created_at
  FROM public.vehicles v
ON CONFLICT (vehicle_id) DO UPDATE
   SET placa          = EXCLUDED.placa,
       creado_en_utc  = EXCLUDED.creado_en_utc,
       actualizado_en = now()
 WHERE (dim_vehiculo.placa, dim_vehiculo.creado_en_utc)
       IS DISTINCT FROM (EXCLUDED.placa, EXCLUDED.creado_en_utc);

REVOKE ALL ON public.dim_vehiculo FROM PUBLIC;

COMMENT ON TABLE public.dim_vehiculo IS
    'Dimensión vehículo del esquema estrella; copia de vehicles sincronizada por upsert (sin FK a propósito).';
