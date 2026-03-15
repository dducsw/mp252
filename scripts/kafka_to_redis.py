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

    def _handle_exit(self, signum, frame):
        """Signal handler to trigger graceful shutdown."""
        logger.info("\nTermination signal received. Shutting down gracefully...")
        self.running = False

    def _flatten(self, data: dict) -> dict:
        """Flatten nested data for Redis compatibility."""
        msg_type = data.get("msgType", "Unknown")
        payload = data.get("msgBusWayPoint", {})
        
        stream_data = {"msgType": str(msg_type)}
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                stream_data[key] = json.dumps(value)
            else:
                stream_data[key] = str(value)
        return stream_data

    def _process_batch(self, messages: list):
        """Processes a batch of Kafka messages using Redis pipeline."""
        pipe = self.redis_client.pipeline(transaction=False)
        for message in messages:
            data = message.value
            payload = data.get("msgBusWayPoint", {})
            vehicle_id = payload.get("vehicle", "Unknown")
            stream_data = self._flatten(data)

            # 1. Update standard Redis Stream (Event Tracking)
            # Maxlen set to 3.6M to hold ~1 hour of data at 1000 msg/s
            pipe.xadd(
                self.redis_stream_key, 
                stream_data, 
                maxlen=3600000, 
                approximate=True
            )

            # 2. Update Redis Hash for O(1) Quick Latest Lookup
            if vehicle_id != "Unknown":
                hash_key = f"buswaypoint_latest:{vehicle_id}"
                pipe.hset(hash_key, mapping=stream_data)
                pipe.expire(hash_key, 7200)  # TTL 2 hours

        pipe.execute()
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
    
    consumer = KafkaToRedisConsumer(
        kafka_bootstrap_servers=kafka_servers,
        kafka_topic=kafka_topic,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_stream_key=redis_stream,
        batch_size=500
    )
    
    consumer.run()


if __name__ == "__main__":
    main()
