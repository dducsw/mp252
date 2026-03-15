.PHONY: init-kafka-topics list-kafka-topics read-kafka-topic help

KAFKA_CONTAINER = kafka
BOOTSTRAP_SERVER = localhost:9092

help:
	@echo "Available commands:"
	@echo "  make init-kafka-topics           - Create necessary Kafka topics for the project"
	@echo "  make list-kafka-topics           - List all current Kafka topics"
	@echo "  make read-kafka-topic TOPIC=name - Read messages from a specific Kafka topic"

init-kafka-topics:
	@echo "Creating Kafka topics (buswaypoint_json)..."
	@if ! docker ps | grep -q $(KAFKA_CONTAINER); then \
		echo "Error: $(KAFKA_CONTAINER) container is not running! Start docker-compose first."; \
		exit 1; \
	fi
	@docker exec $(KAFKA_CONTAINER) /opt/kafka/bin/kafka-topics.sh \
		--create --if-not-exists \
		--topic buswaypoint_json \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--partitions 2 \
		--replication-factor 2
	@echo "Kafka topics created successfully."

list-kafka-topics:
	@echo "Listing Kafka topics:"
	@docker exec $(KAFKA_CONTAINER) /opt/kafka/bin/kafka-topics.sh \
		--list \
		--bootstrap-server $(BOOTSTRAP_SERVER)

read-kafka-topic:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Error: Please specify a topic using TOPIC=topic_name. Example: make read-kafka-topic TOPIC=buswaypoint_json"; \
		exit 1; \
	fi
	@python scripts/read_topic.py $(TOPIC)
