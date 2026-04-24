# Kafka to Redis Pipeline

This pipeline consumes bus waypoint data from Kafka and streams it to Redis for real-time monitoring and caching.

## Features
- Streams raw events to a Redis Stream (`buswaypoint_stream`).
- Maintains the latest state of each vehicle in Redis Hashes (`buswaypoint_latest:<vehicle_id>`).
- Aggregates real-time metrics per route (`bus_metrics:route:<route_no>`).
- Supports automatic Redis cleanup before starting.

## Prerequisites
- Kafka Broker (default: `localhost:9092`)
- Redis Server (default: `localhost:6379`)
- Python dependencies: `kafka-python`, `redis`, `tqdm`

## Environment Variables
| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_TOPIC` | Kafka topic to consume from | `buswaypoint_json` |
| `REDIS_HOST` | Redis host | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `REDIS_STREAM` | Redis stream key name | `buswaypoint_stream` |
| `MAPPING_CSV` | Path to vehicle-route mapping CSV | `data/vehicle_route_mapping.csv` |
| `RESET_REDIS` | Set to `true` to clear Redis data before starting | `false` |

## Usage

### 1. Standard Run
Run the pipeline normally:
```bash
python pipelines/redis/kafka_to_redis.py
```

### 2. Run with Redis Cleanup
To clear all existing `buswaypoint_stream`, vehicle hashes, and metrics before starting:
```bash
RESET_REDIS=true python pipelines/redis/kafka_to_redis.py
```

### 3. Manual Redis Cleanup (Optional)
If you need to clear data manually inside the Docker container:
```bash
docker exec -it redis redis-cli FLUSHALL
```

## 4. Inspecting Data in Redis

You can use `docker exec` to interact with the data directly from the command line.

### Access Redis CLI
```bash
docker exec -it redis redis-cli
```

### Check Latest Status of a Vehicle
```bash
# Example: Check vehicle 51B-063.15
docker exec -it redis redis-cli HGETALL buswaypoint_latest:51B-063.15
```

### View Route Metrics (KPIs)
```bash
# Example: Check metrics for route 1
docker exec -it redis redis-cli HGETALL bus_metrics:route:1

# Check "ALL" routes aggregate
docker exec -it redis redis-cli HGETALL bus_metrics:route:ALL
```

### Read from the Stream
```bash
# Read the last 5 messages from the stream
docker exec -it redis redis-cli XREAD COUNT 5 STREAMS buswaypoint_stream 0
```

### Check Active Vehicles or Routes
```bash
# List all active vehicle IDs
docker exec -it redis redis-cli SMEMBERS vehicles_seen

# List all active route numbers
docker exec -it redis redis-cli SMEMBERS routes_active
```
