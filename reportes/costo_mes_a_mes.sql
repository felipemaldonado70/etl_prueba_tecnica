SELECT 
    mes,
    SUM(total) AS costo_total
FROM 
    facturacion
GROUP BY 
    mes
ORDER BY 
    mes;
