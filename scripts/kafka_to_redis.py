import json
import os
import signal
import sys
import logging
import redis
from kafka import KafkaConsumer
from tqdm import tqdm

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
        batch_size: int = 100
    ):
        self.kafka_topic = kafka_topic
        self.redis_stream_key = redis_stream_key
        self.batch_size = batch_size
        self.message_count = 0
        self.running = True

        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

        # Pass topic directly to constructor (same pattern as working script).
        # No group_id — avoids broker-managed offset overriding seek_to_beginning.
        try:
            self.kafka_consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                api_version=(3, 6, 0),
            )
            logger.info(f"Connected to Kafka broker at {kafka_bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            sys.exit(1)

        try:
            logger.info(f"Connecting to Redis at {redis_host}:{redis_port}")
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

            # 1. Redis Stream — event log (~1 hour at 1000 msg/s)
            pipe.xadd(
                self.redis_stream_key,
                stream_data,
                maxlen=3600000,
                approximate=True
            )

            # 2. Redis Hash — O(1) latest state per vehicle
            if vehicle_id != "Unknown":
                hash_key = f"buswaypoint_latest:{vehicle_id}"
                pipe.hset(hash_key, mapping=stream_data)
                pipe.expire(hash_key, 7200)  # TTL 2 hours

        try:
            pipe.execute()
            self.message_count += len(messages)
        except Exception as e:
            logger.error(f"Redis pipeline error: {e}")

    def run(self):
        """Main execution loop."""
        logger.info("Starting stream processing...")
        logger.info(f"Topic: {self.kafka_topic} → Redis Stream: {self.redis_stream_key}")

        try:
            with tqdm(desc="Streaming", unit=" msg") as pbar:
                while self.running:
                    message_pack = self.kafka_consumer.poll(
                        timeout_ms=1000,
                        max_records=self.batch_size
                    )
                    if not message_pack:
                        continue

                    for tp, messages in message_pack.items():
                        if not self.running:
                            break
                        self._process_batch(messages)
                        pbar.update(len(messages))
                        pbar.set_postfix({
                            'partition': tp.partition,
                            'offset': messages[-1].offset,
                            'total': self.message_count
                        })

        except Exception as e:
            logger.error(f"Streaming error: {e}")
        finally:
            try:
                self.kafka_consumer.close()
                self.redis_client.close()
                logger.info("Connections closed.")
            except Exception as e:
                logger.warning(f"Cleanup error: {e}")
            logger.info(f"Total messages streamed: {self.message_count}")


def main():
    consumer = KafkaToRedisConsumer(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "buswaypoint_json"),
        redis_host=os.getenv("REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("REDIS_PORT", 6379)),
        redis_stream_key=os.getenv("REDIS_STREAM", "buswaypoint_stream"),
        batch_size=500
    )
    consumer.run()


if __name__ == "__main__":
    main()