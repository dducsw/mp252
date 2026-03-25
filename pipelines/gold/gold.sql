-- catalog_iceberg.bus_gold

-- Create Namespace
CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_gold;

-- Table: route_bus_performance
-- Hourly performance metrics per bus per route per direction
CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_gold.bus_performance (
    vehicle STRING,
    date DATE,
    hour INT,
    route_id INT,
    route_no STRING,
    route_var_id INT,
    avg_speed DOUBLE,
    max_speed DOUBLE,
    distance_km DOUBLE,
    waypoint_count INT,
    stop_count INT,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (date, route_id);

CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_gold.trip_summary (
    trip_id STRING, -- generated: vehicle + start_time hash

    vehicle STRING,
    date DATE,
    route_id INT,
    route_no STRING,
    route_var_id INT,
    direction STRING,
    
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_minutes INT,

    avg_speed DOUBLE,
    max_speed DOUBLE,
    distance_km DOUBLE,
    waypoint_count INT,
    stop_count INT,

    is_completed BOOLEAN,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (date, route_id);