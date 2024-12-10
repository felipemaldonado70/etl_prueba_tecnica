SELECT 
    servicio,
    SUM(total) AS costo_total
FROM 
    reporte_costos_proveedores_cloud
WHERE 
    fecha >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
GROUP BY 
    servicio
ORDER BY 
    costo_total DESC;
