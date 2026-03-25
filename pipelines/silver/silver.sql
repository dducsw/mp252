-- catalog_iceberg.bus_silver

-- Create Namespace
CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_silver;

-- Table: bus_way_point
-- Cleaned bus waypoint data
CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.buswaypoint (
            vehicle STRING,
            driver STRING,
            timestamp TIMESTAMP,
            date DATE,
            speed DOUBLE,
            lng DOUBLE,
            lat DOUBLE,
            z DOUBLE,
            heading FLOAT,
            ignition BOOLEAN,
            aircon BOOLEAN,
            door_up BOOLEAN,
            door_down BOOLEAN,
            sos BOOLEAN,
            working BOOLEAN,
            analog1 FLOAT,
            analog2 FLOAT,

            route_id INT,
            route_no STRING,
            route_var_id INT,
            
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)

-- Table: route_path
-- Cleaned route path data
CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.route_path (
    route_id INT,
    route_no STRING,
    route_var_id INT,
    route_var_name STRING,
    outbound BOOLEAN,
    path ARRAY<ARRAY<DOUBLE>>,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (route_id);

-- Table: route_info
-- Cleaned route info with direction field
CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.route_info (
    route_id INT,
    route_no STRING,
    route_var_id INT,
    route_var_name STRING,
    outbound BOOLEAN,
    direction STRING,
    distance_km DOUBLE,
    running_time INT,
    start_stop STRING,
    end_stop STRING,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (route_id);

-- Table: route_stop
-- Cleaned bus stop data for each route
CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.route_stop (
    route_id INT,
    route_var_id INT,
    stop_id INT,
    code STRING,
    stop_name STRING,
    stop_type STRING,
    zone STRING,
    ward STRING,
    address_no STRING,
    street STRING,
    lng DOUBLE,
    lat DOUBLE,
    routes STRING,
    updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (route_id);
