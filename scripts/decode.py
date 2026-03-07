import json
import base64
import pandas as pd
from ufms_pb2 import BaseMessage, BusWayPoint, MsgType

json_file = "scripts/data/sample.json"
output_parquet = "scripts/data/parquet/base_part2.parquet"

records = []

with open(json_file, "r", encoding="utf-8") as f:
    data = json.load(f)

for row in data:

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

    msg = BaseMessage(
        msgType=MsgType.MSG_TYPE_BUS_WAY_POINT,
        bus_way_point=waypoint
    )

    raw_bytes = msg.SerializeToString()

    base64_str = base64.b64encode(raw_bytes).decode()

    records.append({
        "raw_base64": base64_str
    })

df = pd.DataFrame(records)

df.to_parquet(output_parquet, index=False)

print("Finished writing parquet with protobuf payload.")