-- Power BI (modo Importar): últimas 20 alertas de alertas_operativas (velocidad y
-- geocerca), con la placa del vehículo o "Sin asignar".
SELECT 
    a.tipo_alerta,
    a.device_id,
    COALESCE(dv.placa, 'Sin asignar') AS placa,
    a.valor,
    a.umbral,
    a.fecha_evento,
    a.fecha_ultimo_evento,
    a.puntos,
    a.detalle
FROM alertas_operativas a
LEFT JOIN dim_vehiculo dv ON dv.vehicle_id = a.vehicle_id
ORDER BY a.fecha_evento DESC
LIMIT 20;