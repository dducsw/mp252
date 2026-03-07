import pandas as pd
from kafka import KafkaProducer
from tqdm import tqdm

from ufms_pb2 import BaseMessage, BusWayPoint, MSG_TYPE_BUS_WAY_POINT

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v,
    linger_ms=10,
    batch_size=16384
)

json_file = "scripts/data/sample.json"
topic_name = "bus_gps_protobuf"

df = pd.read_json(json_file)

print("Total rows:", len(df))

for idx, row in tqdm(df.iterrows(), total=len(df), desc="Sending protobuf to Kafka"):

    wp = row["msgBusWayPoint"]

    waypoint = BusWayPoint(
        vehicle=wp.get("vehicle", ""),
        driver=wp.get("driver", ""),
        speed=float(wp.get("speed", 0)),
        datetime=int(wp.get("datetime", 0)),
        x=float(wp.get("x", 0)),
        y=float(wp.get("y", 0)),
        z=float(wp.get("z", 0)),
        heading=float(wp.get("heading", 0)),
        ignition=bool(wp.get("ignition", False)),
        aircon=bool(wp.get("aircon", False)),
        door_up=bool(wp.get("door_up", False)),
        door_down=bool(wp.get("door_down", False)),
        sos=bool(wp.get("sos", False)),
        working=bool(wp.get("working", False)),
        analog1=float(wp.get("analog1", 0)),
        analog2=float(wp.get("analog2", 0))
    )

    message = BaseMessage()
    message.msgType = MSG_TYPE_BUS_WAY_POINT
    message.bus_way_point.CopyFrom(waypoint)

    raw_bytes = message.SerializeToString()

    producer.send(topic_name, raw_bytes)
    print(f"Sent message {idx + 1}/{len(df)} to Kafka topic '{topic_name}'")
    if idx % 100000 == 0:
        producer.flush()

producer.flush()

print("Finished sending data to Kafka")