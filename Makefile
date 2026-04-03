.PHONY: init-kafka-topics list-kafka-topics read-kafka-topic list-consumer-groups run-spark install-deps setup-env download-kaggle-hpclab download-kaggle-hpclab-url help

KAFKA_CONTAINER = kafka
BOOTSTRAP_SERVER = localhost:9092
CONSUMER_GROUP = buswaypoint_consumer_group

help:
	@echo "Available commands:"
	@echo "  make install-deps                - Install Python dependencies from requirements.txt"
	@echo "  make setup-env                   - Setup Python virtual environment and install dependencies"
	@echo "  make download-kaggle-hpclab      - Download HPCLAB dataset from Kaggle (requires credentials)"
	@echo "  make download-kaggle-hpclab-url  - Download from direct URL: make download-kaggle-hpclab-url URL=<link>"
	@echo "  make init-kafka-topics           - Create necessary Kafka topics and consumer groups"
	@echo "  make list-kafka-topics           - List all current Kafka topics"
	@echo "  make list-consumer-groups        - List all consumer groups"
	@echo "  make read-kafka-topic TOPIC=name - Read messages from a specific Kafka topic"
	@echo "  make run-spark FILE=path         - Run Spark application (e.g., pipelines/example/create_example_table.py)"

install-deps:
	@echo "Installing Python dependencies..."
	@pip3 install --upgrade pip
	@pip3 install -r scripts/requirements.txt
	@echo "Dependencies installed successfully."

setup-env:
	@echo "Creating Python virtual environment..."
	@if [ ! -d "venv" ]; then \
		python3 -m venv venv; \
		echo "Virtual environment created."; \
	else \
		echo "Virtual environment already exists."; \
	fi
	@echo "Installing dependencies in virtual environment..."
	@. venv/bin/activate && pip install --upgrade pip && pip install -r scripts/requirements.txt
	@echo "Setup completed. To activate run: source venv/bin/activate"


download-kaggle-hpclab-url:
	@if [ -z "$(URL)" ]; then \
		echo "Error: Please provide URL parameter. Example: make download-kaggle-hpclab-url URL=https://..."; \
		exit 1; \
	fi
	@mkdir -p data/HPCLAB/part2/part2
	@echo "Downloading from: $(URL)"
	@cd data/HPCLAB/part2/part2 && wget "$(URL)" -O dataset.zip && unzip -o dataset.zip && rm dataset.zip
	@echo "Dataset downloaded and extracted successfully!"
	@ls -lh data/HPCLAB/part2/part2/ | head -20

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
		--replication-factor 1
	@echo "Kafka topics created successfully."
	@echo "Creating consumer group ($(CONSUMER_GROUP))..."
	@docker exec $(KAFKA_CONTAINER) /opt/kafka/bin/kafka-consumer-groups.sh \
		--create \
		--group $(CONSUMER_GROUP) \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--topic buswaypoint_json 2>/dev/null || true
	@echo "Consumer group created successfully."

list-kafka-topics:
	@echo "Listing Kafka topics:"
	@docker exec $(KAFKA_CONTAINER) /opt/kafka/bin/kafka-topics.sh \
		--list \
		--bootstrap-server $(BOOTSTRAP_SERVER)

list-consumer-groups:
	@echo "Listing Consumer Groups:"
	@docker exec $(KAFKA_CONTAINER) /opt/kafka/bin/kafka-consumer-groups.sh \
		--list \
		--bootstrap-server $(BOOTSTRAP_SERVER)

read-kafka-topic:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Error: Please specify a topic using TOPIC=topic_name. Example: make read-kafka-topic TOPIC=buswaypoint_json"; \
		exit 1; \
	fi
	@python scripts/read_topic.py $(TOPIC)

run-spark:
	@if [ -z "$(FILE)" ]; then \
		echo "Error: Please specify file using FILE=path. Example: make run-spark FILE=pipelines/example/create_example_table.py"; \
		exit 1; \
	fi
	@docker exec -it spark-master spark-submit /opt/spark/apps/$(FILE)
