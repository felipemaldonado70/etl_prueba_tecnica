SELECT 
    mes,
    categoria,
    nube,
    SUM(total) AS costo_total
FROM 
    reporte_costos_proveedores_cloud
GROUP BY 
    mes, categoria, nube
ORDER BY 
    mes, categoria, nube;
