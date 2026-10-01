-- =============================================================================
-- 012_trg_alerta_exceso_velocidad.sql
-- Trigger AFTER INSERT sobre gps_data que detecta exceso de velocidad.
-- Requiere 011 (fn_alerta_exceso_velocidad).
--
-- UMBRAL: speed > 90 km/h, elegido según la distribución histórica de speed
-- (21 412 543 puntos válidos, 2024-09-09 a 2026-09-27):
--  * 36,55 % de los puntos están en 0 km/h exacto (vehículo detenido).
--  * Puntos en movimiento (>= 5 km/h): p99 = 74, p99,9 = 85, p99,99 = 91 km/h.
--  * Histograma: 70 564 puntos en 80-89 km/h contra 1 671 en 90-99 (42 veces menos)
--    y 248 por encima de 100. Los 80 km/h son operación normal en carretera; 90
--    es el borde donde la operación real se corta.
--  * Volumen de alertas por umbral (episodios de 5 min, toda la flota):
--        > 80 km/h -> 2 170 episodios (~25 por semana): ruido
--        > 85 km/h -> 1 256 episodios (~14 por semana)
--        > 90 km/h ->   251 episodios (~3 por semana): accionable
--        > 100 km/h ->   31 episodios: pierde casos reales
--  * 90 km/h es también el límite habitual para ómnibus en carretera en Perú
--    (Reglamento Nacional de Tránsito); un límite contractual distinto se
--    ajusta aquí y en 011.
--
-- WHEN en lugar de IF dentro de la función: la condición se evalúa en C antes de
-- invocar PL/pgSQL. Para el 99,99 % de los puntos el trigger no ejecuta nada.
-- WHEN no admite subconsultas ni parámetros: el umbral queda fijo aquí y en la
-- constante c_umbral de 011. Para cambiarlo hay que re-ejecutar ambos scripts.
--
-- gps_data está particionada: el trigger se crea en la tabla padre y PostgreSQL
-- lo clona en todas las particiones, incluidas las que se creen en el futuro.
--
-- Aplicar (requiere un rol con privilegios de escritura, dueño de gps_data):
--   psql "$DATABASE_URL_ADMIN" -1 -v ON_ERROR_STOP=1 -f sql/triggers/012_trg_alerta_exceso_velocidad.sql
-- =============================================================================

DO $$
BEGIN
    IF to_regprocedure('public.fn_alerta_exceso_velocidad()') IS NULL THEN
        RAISE EXCEPTION 'Falta fn_alerta_exceso_velocidad(): aplicar primero sql/functions/011_fn_alerta_exceso_velocidad.sql';
    END IF;
END;
$$;

CREATE OR REPLACE TRIGGER trg_alerta_exceso_velocidad
    AFTER INSERT ON public.gps_data
    FOR EACH ROW
    WHEN (NEW.speed > 90)
    EXECUTE FUNCTION public.fn_alerta_exceso_velocidad();
