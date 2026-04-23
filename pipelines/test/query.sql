SELECT route_no, date, COUNT(*)
FROM catalog_iceberg.bus_bronze.buswaypoint_test
WHERE date BETWEEN '2025-04-01' AND '2025-04-20'
GROUP BY route_no, date
ORDER BY date ASC;
