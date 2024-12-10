SELECT 
    nube,
    servicio,
    SUM(total) AS costo_total
FROM 
    reporte_costos_proveedores_cloud
GROUP BY 
    nube, servicio
ORDER BY 
    costo_total DESC
LIMIT 3;
