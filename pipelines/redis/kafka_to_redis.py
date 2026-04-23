import json
import os
import signal
import sys
import logging
import csv
from datetime import datetime, timezone
import redis
from kafka import KafkaConsumer
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaToRedisConsumer:
    """Robust Kafka consumer that syncs latest states and event streams to Redis."""

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        redis_host: str,
        redis_port: int,
        redis_stream_key: str,
        group_id: str = "kafka-redis-streaming-group",
        batch_size: int = 100,
        mapping_file: str = "data/vehicle_route_mapping.csv"
    ):
        self.kafka_topic = kafka_topic
        self.redis_stream_key = redis_stream_key
        self.batch_size = batch_size
        self.message_count = 0
        self.running = True
        
        # Load vehicle to route mapping
        self.mapping_file = mapping_file
        self.vehicle_to_route = self._load_mapping()

        # Handle termination signals gracefully
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

        # Initialize Kafka consumer
        try:
            self.kafka_consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=kafka_bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='latest',  # Process only the latest events as default behavior
                enable_auto_commit=False,  # Manual commit after batch processing
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            logger.info(f"Connected to Kafka broker at {kafka_bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            sys.exit(1)

        # Initialize Redis connection (Single Node)
        try:
            logger.info(f"Connecting to Redis Single Node at {redis_host}:{redis_port}")
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                decode_responses=True,
                socket_keepalive=True,
                socket_timeout=5.0
            )
            self.redis_client.ping()
            logger.info("Connected to Redis successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            sys.exit(1)

    def _load_mapping(self) -> dict:
        """Load vehicle-to-route mapping from CSV."""
        mapping = {}
        if os.path.exists(self.mapping_file):
            try:
                with open(self.mapping_file, mode='r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        mapping[row['vehicle']] = row['route_no']
                logger.info(f"Loaded {len(mapping)} vehicle-to-route mappings from {self.mapping_file}")
            except Exception as e:
                logger.warning(f"Error loading mapping file: {e}")
        else:
            logger.warning(f"Mapping file not found: {self.mapping_file}")
        return mapping

    def _handle_exit(self, signum, frame):
        """Signal handler to trigger graceful shutdown."""
        logger.info("\nTermination signal received. Shutting down gracefully...")
        self.running = False

    def _flatten(self, data: dict) -> dict:
        """Flatten nested data for Redis compatibility."""
        msg_type = data.get("msgType", "Unknown")
        payload = data.get("msgBusWayPoint", {})
        ingested_at = datetime.now(timezone.utc).isoformat()
        
        stream_data = {"msgType": str(msg_type)}
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                stream_data[key] = json.dumps(value)
            else:
                stream_data[key] = str(value)

        event_epoch = payload.get("datetime")
        if event_epoch is not None:
            try:
                stream_data["event_time"] = datetime.fromtimestamp(
                    int(event_epoch), tz=timezone.utc
                ).isoformat()
            except (TypeError, ValueError):
                pass

        # Ingestion timestamp helps debug consumer lag and event freshness in Redis.
        stream_data["ingested_at"] = ingested_at
        return stream_data

    @staticmethod
    def _is_true(value: str) -> bool:
        return str(value).lower() == "true"

    @staticmethod
    def _to_float(value: str) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _refresh_global_metrics(self):
        """
        Build cheap snapshot KPIs from latest-per-vehicle hashes.
        These metrics are better served from Redis hashes than by scanning the event stream.
        """
        vehicle_ids = self.redis_client.smembers("vehicles_seen")
        if not vehicle_ids:
            return

        pipe = self.redis_client.pipeline(transaction=False)
        for vehicle_id in vehicle_ids:
            pipe.hgetall(f"buswaypoint_latest:{vehicle_id}")
        vehicle_states = pipe.execute()

        stale_vehicle_ids = []
        route_metrics = {}

        def ensure_bucket(route_no: str):
            if route_no not in route_metrics:
                route_metrics[route_no] = {
                    "active_vehicle_count": 0,
                    "idle_vehicle_count": 0,
                    "sos_vehicle_count": 0,
                    "aircon_on_count": 0,
                    "speed_sum": 0.0,
                    "speed_count": 0,
                }
            return route_metrics[route_no]

        for vehicle_id, state in zip(vehicle_ids, vehicle_states):
            if not state:
                stale_vehicle_ids.append(vehicle_id)
                continue

            route_no = state.get("route_no") or "UNKNOWN"
            ignition_on = self._is_true(state.get("ignition"))
            aircon_on = self._is_true(state.get("aircon"))
            sos_on = self._is_true(state.get("sos"))
            speed = self._to_float(state.get("speed"))

            all_bucket = ensure_bucket("ALL")
            route_bucket = ensure_bucket(route_no)

            if ignition_on:
                all_bucket["active_vehicle_count"] += 1
                route_bucket["active_vehicle_count"] += 1

                all_bucket["speed_sum"] += speed
                route_bucket["speed_sum"] += speed

                all_bucket["speed_count"] += 1
                route_bucket["speed_count"] += 1

                if speed == 0:
                    all_bucket["idle_vehicle_count"] += 1
                    route_bucket["idle_vehicle_count"] += 1
                if aircon_on:
                    all_bucket["aircon_on_count"] += 1
                    route_bucket["aircon_on_count"] += 1

            if sos_on:
                all_bucket["sos_vehicle_count"] += 1
                route_bucket["sos_vehicle_count"] += 1

        pipe = self.redis_client.pipeline(transaction=False)

        updated_at = datetime.now(timezone.utc).isoformat()
        for route_no, bucket in route_metrics.items():
            avg_speed = (
                round(bucket["speed_sum"] / bucket["speed_count"], 2)
                if bucket["speed_count"] > 0
                else 0.0
            )
            metric_hash = {
                "active_vehicle_count": str(bucket["active_vehicle_count"]),
                "idle_vehicle_count": str(bucket["idle_vehicle_count"]),
                "sos_vehicle_count": str(bucket["sos_vehicle_count"]),
                "aircon_on_count": str(bucket["aircon_on_count"]),
                "avg_speed": f"{avg_speed:.2f}",
                "updated_at": updated_at,
                "route_no": route_no,
            }
            pipe.hset(f"bus_metrics:route:{route_no}", mapping=metric_hash)
            pipe.expire(f"bus_metrics:route:{route_no}", 7200)

        if stale_vehicle_ids:
            pipe.srem("vehicles_seen", *stale_vehicle_ids)
        pipe.execute()

    def _process_batch(self, messages: list):
        """Processes a batch of Kafka messages using Redis pipeline."""
        pipe = self.redis_client.pipeline(transaction=False)
        for message in messages:
            data = message.value
            payload = data.get("msgBusWayPoint", {})
            vehicle_id = payload.get("vehicle", "Unknown")
            stream_data = self._flatten(data)

            # 1. Lookup route mapping and enrich stream_data
            if vehicle_id != "Unknown":
                route_no = self.vehicle_to_route.get(vehicle_id)
                if route_no:
                    stream_data["route_no"] = route_no
                    # Track active routes for dashboard variables
                    pipe.sadd("routes_active", route_no)
                    pipe.expire("routes_active", 1800)
                pipe.sadd("vehicles_seen", vehicle_id)
                pipe.expire("vehicles_seen", 7200)

            # 2. Update standard Redis Stream (Event Tracking)
            # Maxlen set to 3.6M to hold ~1 hour of data at 1000 msg/s
            pipe.xadd(
                self.redis_stream_key, 
                stream_data, 
                maxlen=3600000, 
                approximate=True
            )

            # 3. Update Redis Hash for O(1) Quick Latest Lookup
            if vehicle_id != "Unknown":
                hash_key = f"buswaypoint_latest:{vehicle_id}"
                pipe.hset(hash_key, mapping=stream_data)
                pipe.expire(hash_key, 7200)  # TTL 2 hours

        pipe.execute()
        self._refresh_global_metrics()
        self.message_count += len(messages)

    def run(self):
        """Main execution loop to consume from Kafka and stream to Redis."""
        logger.info("Starting high-throughput stream processing...")
        logger.info(f"Subscribed Topic: {self.kafka_topic} | Redis Stream: {self.redis_stream_key}")

        try:
            with tqdm(desc="Streaming messages", unit=" msg") as pbar:
                while self.running:
                    # Poll for a batch of messages
                    message_pack = self.kafka_consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                    
                    if not message_pack:
                        continue

                    for tp, messages in message_pack.items():
                        if not self.running:
                            break
                        
                        self._process_batch(messages)
                        
                        # Use Kafka's native commit
                        self.kafka_consumer.commit()
                        
                        pbar.update(len(messages))
                        pbar.set_postfix({'last_offset': messages[-1].offset})

        except Exception as e:
            logger.error(f"Streaming error occurred: {e}")
        finally:
            try:
                self.kafka_consumer.close()
                self.redis_client.close()
                logger.info("Connections closed securely.")
            except Exception as e:
                logger.warning(f"Error during cleanup: {e}")
            
            logger.info(f"Total messages successfully streamed: {self.message_count}")


def main():
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "buswaypoint_json")
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_stream = os.getenv("REDIS_STREAM", "buswaypoint_stream")
    mapping_csv = os.getenv("MAPPING_CSV", "data/vehicle_route_mapping.csv")
    
    consumer = KafkaToRedisConsumer(
        kafka_bootstrap_servers=kafka_servers,
        kafka_topic=kafka_topic,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_stream_key=redis_stream,
        batch_size=500,
        mapping_file=mapping_csv
    )
    
    consumer.run()


if __name__ == "__main__":
    main()
