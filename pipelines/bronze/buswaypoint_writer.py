import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, DoubleType, BooleanType
)

from pyspark.sql.functions import current_timestamp, from_unixtime, to_timestamp, to_date

def write_json_to_iceberg(spark: SparkSession, json_path: str, table_name: str) -> None:
    # Define schema for the nested BusWayPoint based on ufms.proto
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
    
    # Define root schema
    root_schema = StructType([
        StructField("msgType", StringType(), True),
        StructField("msgBusWayPoint", bus_way_point_schema, True)
    ])
    
    # Read the JSON file (it's an array of objects, so multiline=True)
    df = spark.read.schema(root_schema).option("multiline", "true").json(json_path)
    
    # Flatten the struct so it maps well to a tabular Iceberg table
    df_flattened = df.select("msgType", "msgBusWayPoint.*")
    
    # Convert epoch datetime to Spark timestamp and extract Date for partitioning
    df_with_ts = (
        df_flattened
        .withColumn("timestamp", to_timestamp(from_unixtime("datetime")))
        .withColumn("date", to_date(from_unixtime("datetime")))
        .withColumn("load_at", current_timestamp())
    )
    
    # Write to Iceberg with partitioning using the native date field
    (
        df_with_ts
        .write.format("iceberg")
        .mode("overwrite")
        .partitionBy("date")
        .saveAsTable(table_name)
    )

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ReadJsonToMinIO").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Get the project root based on current script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    json_path = os.path.join(project_root, "data", "HPCLAB", "sample.json")
    
    table_name = "catalog_iceberg.bus_bronze.bus_way_point"
    
    write_json_to_iceberg(spark, json_path, table_name)
