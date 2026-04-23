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
If you only want to clear the data without starting the pipeline, you can use `redis-cli`:
```bash
# Clear the stream
redis-cli DEL buswaypoint_stream

# Clear vehicle sets
redis-cli DEL vehicles_seen routes_active

# Clear hashes (using xargs for patterns)
redis-cli --scan --pattern "buswaypoint_latest:*" | xargs redis-cli DEL
redis-cli --scan --pattern "bus_metrics:route:*" | xargs redis-cli DEL
```
