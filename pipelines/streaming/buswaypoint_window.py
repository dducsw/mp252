import os
import math
import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, session_window, avg, count, 
    min, max, to_timestamp, from_unixtime, udf, struct,
    sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, DoubleType, BooleanType, TimestampType
)
from pyspark.sql.streaming import GroupStateTimeout, GroupState

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "buswaypoint_json")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# --- Schema Definition ---
bus_way_point_schema = StructType([
    StructField("vehicle", StringType(), True),
    StructField("driver", StringType(), True),
    StructField("speed", FloatType(), True),
    StructField("datetime", IntegerType(), True),
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True),
    StructField("z", FloatType(), True),
    StructField("heading", FloatType(), True),
    StructField("ignition", BooleanType(), True),
    StructField("aircon", BooleanType(), True),
    StructField("door_up", BooleanType(), True),
    StructField("door_down", BooleanType(), True),
    StructField("sos", BooleanType(), True),
    StructField("working", BooleanType(), True),
    StructField("analog1", FloatType(), True),
    StructField("analog2", FloatType(), True)
])

root_schema = StructType([
    StructField("msgType", StringType(), True),
    StructField("msgBusWayPoint", bus_way_point_schema, True)
])

# --- Helper Functions ---
def haversine(lon1, lat1, lon2, lat2):
    """Calculate the great circle distance between two points on the earth."""
    if lon1 is None or lat1 is None or lon2 is None or lat2 is None:
        return 0.0
    # Convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    # Haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

# --- Stateful Processing for Delta Distance ---
def update_vehicle_state(vehicle_id, inputs, state: GroupState):
    """
    Inputs: Iterator of rows (x, y, timestamp)
    State: Stores (last_x, last_y)
    Returns: Iterator of (vehicle_id, timestamp, delta_distance)
    """
    if state.hasTimedOut:
        state.remove()
        return []

    last_pos = state.get if state.exists else None
    results = []

    for row in inputs:
        curr_x = row.x
        curr_y = row.y
        curr_ts = row.timestamp
        
        delta = 0.0
        if last_pos:
            delta = haversine(last_pos[0], last_pos[1], curr_x, curr_y)
        
        last_pos = (curr_x, curr_y)
        results.append((vehicle_id, curr_ts, delta))
    
    state.update(last_pos)
    return results

# --- Redis Sink function ---
def write_to_redis(batch_df, batch_id):
    """Writes a micro-batch dataframe to Redis using pipelines."""
    def send_to_redis(partition):
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        pipe = r.pipeline(transaction=False)
        for row in partition:
            # Determine metric type and set key
            if "avg_speed" in row:
                key = f"metric:avg_speed:{row.vehicle}"
                pipe.hset(key, mapping={
                    "window_start": str(row.window.start),
                    "window_end": str(row.window.end),
                    "avg_speed": str(row.avg_speed)
                })
                pipe.expire(key, 3600)
            elif "msg_count" in row:
                key = f"metric:msg_density:{row.vehicle}:{row.window.start.isoformat()}"
                pipe.hset(key, mapping={"count": str(row.msg_count)})
                pipe.expire(key, 7200)
            elif "trip_points" in row:
                key = f"metric:active_trip:{row.vehicle}"
                pipe.hset(key, mapping={
                    "start": str(row.session_window.start),
                    "end": str(row.session_window.end),
                    "points": str(row.trip_points)
                })
            elif "hour_distance" in row:
                key = f"metric:distance_1h:{row.vehicle}"
                pipe.hset(key, mapping={"distance_km": str(row.hour_distance)})
                pipe.expire(key, 3600)
        pipe.execute()
        r.close()

    batch_df.foreachPartition(send_to_redis)

# --- Main Application ---
def main():
    spark = SparkSession.builder \
        .appName("BusWayPointWindowPipeline") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON and add Watermark
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), root_schema).alias("data")) \
        .select("data.msgBusWayPoint.*") \
        .withColumn("timestamp", to_timestamp(from_unixtime("datetime"))) \
        .withWatermark("timestamp", "10 minutes")

    # --- Metric 1: Sliding Window (Avg Speed) ---
    speed_df = parsed_stream \
        .groupBy(window("timestamp", "5 minutes", "1 minute"), "vehicle") \
        .agg(avg("speed").alias("avg_speed"))

    # --- Metric 2: Tumbling Window (Msg Density) ---
    density_df = parsed_stream \
        .groupBy(window("timestamp", "10 minutes"), "vehicle") \
        .agg(count("*").alias("msg_count"))

    # --- Metric 3: Session Window (Trip Detection) ---
    session_df = parsed_stream \
        .groupBy(session_window("timestamp", "15 minutes"), "vehicle") \
        .agg(count("*").alias("trip_points"))

    # --- Metric 4: Stateful Distance ---
    # We use flatMapGroupsWithState for the delta distance
    # Output schema for stateful op
    state_schema = "vehicle string, timestamp timestamp, delta_distance double"
    
    delta_stream = parsed_stream \
        .select("vehicle", "x", "y", "timestamp") \
        .groupByKey(lambda r: r.vehicle) \
        .flatMapGroupsWithState(
            update_vehicle_state,
            outputMode="append",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
            stateSchema="last_x double, last_y double", # Simulating pos state
            resultSchema=state_schema
        )

    # Now aggregate delta_distance over a sliding window
    distance_1h_df = delta_stream \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(window("timestamp", "1 hour", "10 minutes"), "vehicle") \
        .agg(spark_sum("delta_distance").alias("hour_distance")) 

    # 3. Write all streams to Redis using foreachBatch
    # Note: In a real app, you might want separate queries or a union for a single sink
    # For demonstration, we union the columns to a common format
    
    combined_df = speed_df.select("vehicle", "window", col("avg_speed")).withColumnRenamed("window", "window") \
        .unionByName(density_df.select("vehicle", "window", col("msg_count")), allowMissingColumns=True) \
        .unionByName(session_df.select("vehicle", col("session_window").alias("window"), col("trip_points")), allowMissingColumns=True) \
        .unionByName(distance_1h_df.select("vehicle", "window", col("hour_distance")), allowMissingColumns=True)

    query = combined_df.writeStream \
        .foreachBatch(write_to_redis) \
        .option("checkpointLocation", "/tmp/spark_checkpoints/bus_window") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
