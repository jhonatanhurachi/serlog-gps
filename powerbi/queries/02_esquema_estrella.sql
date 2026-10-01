-- Power BI (modo Importar): hechos diarios de fact_recorrido_diario unidos con
-- dim_vehiculo y dim_fecha (km, tiempos en movimiento/detenido, velocidad, puntos).
SELECT 
    dv.placa,
    df.fecha,
    f.km_recorridos,
    f.tiempo_movimiento_min,
    f.tiempo_detenido_min,
    f.velocidad_promedio_kmh,
    f.puntos_gps
FROM fact_recorrido_diario f
JOIN dim_vehiculo dv ON dv.vehicle_id = f.vehicle_id
JOIN dim_fecha df ON df.fecha = f.fecha
ORDER BY dv.placa, df.fecha;