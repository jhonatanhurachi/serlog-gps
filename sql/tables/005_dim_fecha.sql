-- =============================================================================
-- 005_dim_fecha.sql
-- Dimensión de fechas del esquema estrella de reportería (fact_recorrido_diario).
--
-- Diseño:
--  * fecha es un día calendario LOCAL (America/Lima), igual que p_fecha en
--    fn_distancia_recorrida y la columna fecha del pipeline de Spark.
--  * anio, mes, dia, dia_semana y es_fin_semana son columnas GENERATED: se
--    derivan de fecha y no pueden quedar inconsistentes con ella.
--  * dia_semana usa ISO 8601: 1 = lunes ... 7 = domingo (ISODOW), el mismo
--    criterio que usa el reporte semanal de Spark (semanas que empiezan en lunes).
--  * Se precarga 2024-01-01..2028-12-31, el mismo rango de las particiones de
--    gps_data. sp_generar_resumen_diario (008) inserta la fecha si faltara.
--
-- Aplicar (requiere un rol con privilegios de escritura):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/tables/005_dim_fecha.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS public.dim_fecha (
    fecha         DATE     PRIMARY KEY,
    anio          SMALLINT GENERATED ALWAYS AS (EXTRACT(YEAR   FROM fecha)::smallint) STORED,
    mes           SMALLINT GENERATED ALWAYS AS (EXTRACT(MONTH  FROM fecha)::smallint) STORED,
    dia           SMALLINT GENERATED ALWAYS AS (EXTRACT(DAY    FROM fecha)::smallint) STORED,
    dia_semana    SMALLINT GENERATED ALWAYS AS (EXTRACT(ISODOW FROM fecha)::smallint) STORED,
    es_fin_semana BOOLEAN  GENERATED ALWAYS AS (EXTRACT(ISODOW FROM fecha) IN (6, 7)) STORED
);

INSERT INTO public.dim_fecha (fecha)
SELECT d::date
  FROM generate_series(DATE '2024-01-01', DATE '2028-12-31', INTERVAL '1 day') AS d
ON CONFLICT (fecha) DO NOTHING;

REVOKE ALL ON public.dim_fecha FROM PUBLIC;

COMMENT ON TABLE public.dim_fecha IS
    'Dimensión fecha (día local America/Lima) del esquema estrella de reportería GPS.';
COMMENT ON COLUMN public.dim_fecha.dia_semana IS 'ISO 8601: 1 = lunes ... 7 = domingo.';
