2025-04-25 analysis: bus route heatmap

```sql

WITH route_list AS (
    SELECT 
        route_no,
        ROW_NUMBER() OVER (ORDER BY route_no) AS rn
    FROM (
        SELECT DISTINCT route_no
        FROM catalog_iceberg.bus_gold.gold_bus_dashboard
        WHERE date = DATE '2025-04-25'
    )
),

route_count AS (
    SELECT COUNT(*) AS total_routes FROM route_list
),

selected_index AS (
    SELECT 
        (CAST(to_unixtime(current_timestamp) / 120 AS BIGINT) % total_routes) + 1 AS rn
    FROM route_count
),

selected_route AS (
    SELECT route_no
    FROM route_list
    WHERE rn = (SELECT rn FROM selected_index)
),

data AS (
    SELECT g.*
    FROM catalog_iceberg.bus_gold.gold_bus_dashboard g
    JOIN selected_route r 
        ON g.route_no = r.route_no
    WHERE g.date = DATE '2025-04-25'
)

SELECT * FROM data