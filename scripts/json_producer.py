import json
import pandas as pd
from kafka import KafkaProducer
from tqdm import tqdm

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=10,
    batch_size=16384
)

json_file = "data/HPCLAB/sample.json"
topic_name = "bus_gps_json"

df = pd.read_json(json_file)

print("Total rows:", len(df))

records = df.to_dict(orient="records")

for idx, row in tqdm(enumerate(records), total=len(records), desc="Sending JSON to Kafka"):
    producer.send(topic_name, row)

    if idx % 5000 == 0:
        producer.flush()

producer.flush()

print("Finished sending data to Kafka")