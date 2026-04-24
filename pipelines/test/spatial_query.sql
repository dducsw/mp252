-- Spatial Bounding Box Query
-- Finds buses in a specific region of the city
SELECT vehicle, x, y, timestamp
FROM catalog_iceberg.bus_bronze.buswaypoint_test
WHERE x BETWEEN 106.70 AND 106.80 
  AND y BETWEEN 10.80 AND 10.90
ORDER BY timestamp DESC;
