# Redis Serving Layer - Real-time Data Access

This document details the implementation and application of Redis within the Data Platform architecture, serving as the **Serving Layer** for real-time applications.

## 1. Role of Redis in Architecture
While Iceberg (Data Lakehouse) serves deep analytical queries (Batch/Analytical), Redis is utilized for:
- **Low Latency**: Providing data with < 10ms latency.
- **Real-time Monitoring**: Instant tracking of bus positions and status.
- **KPI Aggregation**: Displaying aggregated metrics (active vehicles, average speed) per route.

## 2. Data Structures and Implementation

The system flexibly uses Redis data structures to optimize performance:

### 2.1 Redis Stream (`buswaypoint_stream`)
- **Purpose**: Stores the raw event stream from Kafka.
- **Details**: 
    - Uses the `maxlen` feature to limit memory usage (approx. 3.6 million records, equivalent to 1 hour of data at 1000 msg/s).
    - Serves applications that need to re-consume real-time data or display short-term history on maps.

### 2.2 Redis Hashes (`buswaypoint_latest:{vehicle_id}`)
- **Purpose**: Stores the latest state of each vehicle (Latest Known State).
- **Details**:
    - Extremely fast access with $O(1)$ complexity via `vehicle_id`.
    - Contains fields: `speed`, `latitude`, `longitude`, `ignition`, `aircon`, `route_no`, `updated_at`.
    - **TTL**: Automatically expires after 2 hours of inactivity to save memory.

### 2.3 Redis Hashes (`bus_metrics:route:{route_no}`)
- **Purpose**: Stores pre-calculated KPI metrics for each route.
- **Metrics include**:
    - `active_vehicle_count`: Number of running vehicles (ignition=True).
    - `idle_vehicle_count`: Number of stopped vehicles (speed=0).
    - `avg_speed`: Average speed across the entire route.
    - `sos_vehicle_count`: Number of vehicles in emergency status.

### 2.4 Redis Sets (`vehicles_seen`, `routes_active`)
- **Purpose**: Tracks the list of active vehicles and routes in the system.
- **Application**: Provides input data for Grafana variables (Dropdown menus for vehicle/route selection).

## 3. Processing Pipeline: Kafka to Redis

This pipeline is implemented by the `kafka_to_redis.py` Python script using optimized techniques:

1.  **Atomic Batching (Pipelines)**: Uses Redis Pipelines to send a group of commands (batch) to the server in a single network round-trip, significantly increasing throughput.
2.  **Data Enrichment**: At the consumption stage from Kafka, the system joins vehicle IDs with a mapping file (`vehicle_route_mapping.csv`) to add route information before storing in Redis.
3.  **Graceful Commits**: Kafka offsets are only committed after the data has been successfully written to Redis, ensuring integrity (At-least-once delivery).
4.  **Automatic Cleanup**: Supports the `RESET_REDIS` flag to clear old data before starting a new session.

## 4. Real-world Applications

- **Dashboard**: Overall city-wide monitoring panel.
- **Real-time Map**: Smooth display of bus movements on a map.
- **Alert System**: Instant notification if `sos_vehicle_count > 0` or if a vehicle exceeds safety speed limits.
