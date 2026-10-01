-- Power BI (modo Importar): km recorridos por vehículo y día local de los últimos
-- 13 días cerrados, calculados con fn_distancia_recorrida (America/Lima).
SELECT 
    v.plate AS placa,
    d.fecha,
    fn_distancia_recorrida(v.id, d.fecha, 'America/Lima') AS km_recorridos
FROM vehicles v
CROSS JOIN (
    SELECT generate_series(
        CURRENT_DATE - INTERVAL '13 days',
        CURRENT_DATE - INTERVAL '1 day',
        INTERVAL '1 day'
    )::date AS fecha
) d
ORDER BY v.plate, d.fecha;