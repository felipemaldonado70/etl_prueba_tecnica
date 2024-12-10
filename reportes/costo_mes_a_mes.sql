SELECT 
    mes,
    SUM(total) AS costo_total
FROM 
    reporte_costos_proveedores_cloud
GROUP BY 
    mes
ORDER BY 
    mes;
