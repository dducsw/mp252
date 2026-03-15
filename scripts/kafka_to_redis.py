"""
Optimized Kafka to Redis Consumer.
Consumes messages from Kafka and streams them to Redis Streams and Hashes.
Combines checkpointing, batching, graceful shutdown, and robust error handling.
"""

import json
import os
import signal
import sys
import logging
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

    CHECKPOINT_FILE = "kafka_to_redis_checkpoint.json"

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        kafka_topic: str,
        redis_host: str,
        redis_port: int,
        redis_stream_key: str,
        group_id: str = "kafka-redis-streaming-group",
        batch_size: int = 100
    ):
        self.kafka_topic = kafka_topic
        self.redis_stream_key = redis_stream_key
        self.batch_size = batch_size
        self.message_count = 0
        self.batch_count = 0
        self.running = True

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
                enable_auto_commit=False,  # Manual commit via checkpointing
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            logger.info(f"Connected to Kafka broker at {kafka_bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            sys.exit(1)

        # Initialize Redis connection
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                decode_responses=True,
                socket_keepalive=True
            )
            self.redis_client.ping()
            logger.info(f"Connected to Redis successfully at {redis_host}:{redis_port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            sys.exit(1)

        self.last_offset = -1
        self._load_checkpoint()

    def _handle_exit(self, signum, frame):
        """Signal handler to trigger graceful shutdown."""
        logger.info("\nTermination signal received. Shutting down gracefully...")
        self.running = False

    def _load_checkpoint(self):
        """Loads the last processed offset from checkpoint file."""
        if os.path.exists(self.CHECKPOINT_FILE):
            try:
                with open(self.CHECKPOINT_FILE, "r") as f:
                    checkpoint = json.load(f)
                    self.last_offset = checkpoint.get("last_offset", -1)
                    logger.info(f"Loaded checkpoint: last_offset={self.last_offset}")
            except Exception as e:
                logger.warning(f"Error loading checkpoint, starting fresh. Details: {e}")

    def _save_checkpoint(self, offset: int):
        """Saves current progress to the checkpoint file."""
        try:
            with open(self.CHECKPOINT_FILE, "w") as f:
                json.dump({"last_offset": offset}, f)
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    def _process_message(self, message) -> str:
        """Processes a single Kafka message and pushes to Redis Stream and Hash."""
        data = message.value
        msg_type = data.get("msgType", "Unknown")
        payload = data.get("msgBusWayPoint", {})
        vehicle_id = payload.get("vehicle", "Unknown")

        # Flatten nested data to strings for Redis Streams/Hashes Compatibility
        stream_data = {"msgType": str(msg_type)}
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                stream_data[key] = json.dumps(value)
            else:
                stream_data[key] = str(value)

        # 1. Update standard Redis Stream (Event Tracking)
        stream_id = self.redis_client.xadd(
            self.redis_stream_key,
            stream_data,
            maxlen=100000,
            approximate=True
        )

        # 2. Update Redis Hash for O(1) Quick Latest Lookup
        if vehicle_id != "Unknown":
            hash_key = f"buswaypoint_latest:{vehicle_id}"
            self.redis_client.hset(hash_key, mapping=stream_data)

        return stream_id

    def run(self):
        """Main execution loop to consume from Kafka and stream to Redis."""
        logger.info("Starting optimized stream processing...")
        logger.info(f"Subscribed Topic: {self.kafka_topic} | Redis Stream: {self.redis_stream_key}")

        try:
            with tqdm(desc="Streaming messages", unit=" msg") as pbar:
                while self.running:
                    # Use poll to ensure the loop can be broken by self.running gracefully
                    messages = self.kafka_consumer.poll(timeout_ms=1000)
                    if not messages:
                        continue

                    for tp, batch in messages.items():
                        for message in batch:
                            if not self.running:
                                break

                            # Skip duplicated offset based on checkpointing
                            if message.offset <= self.last_offset:
                                continue

                            stream_id = self._process_message(message)
                            self.last_offset = message.offset
                            self.message_count += 1
                            self.batch_count += 1
                            
                            pbar.update(1)

                            # Checkpoint handling per batch_size
                            if self.batch_count % self.batch_size == 0:
                                self._save_checkpoint(message.offset)
                                pbar.set_postfix({
                                    'offset': message.offset,
                                    'stream_id': stream_id
                                })

        except Exception as e:
            logger.error(f"Streaming error occurred: {e}")
        finally:
            self._save_checkpoint(self.last_offset)
            logger.info(f"Final checkpoint saved at offset={self.last_offset}")
            
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

    consumer = KafkaToRedisConsumer(
        kafka_bootstrap_servers=kafka_servers,
        kafka_topic=kafka_topic,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_stream_key=redis_stream,
        batch_size=500  # Updated batch size for better throughput
    )
    
    consumer.run()


if __name__ == "__main__":
    main()
