from pyspark.sql import SparkSession
import sys
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, DoubleType, BooleanType
)
from pyspark.sql.functions import current_timestamp, from_unixtime, to_timestamp, to_date, col, from_json

def create_table_if_not_exists(spark: SparkSession, table_name: str):
    """Creates the target Iceberg table with manual partitioning for 'before' comparison."""
    
    # Ensure the namespace exists
    namespace = ".".join(table_name.split(".")[:-1])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    
    # Create table with manual 'date' partitioning
    # This represents the 'unoptimized' state before hidden partitioning
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        vehicle STRING,
        driver STRING,
        speed FLOAT,
        datetime INT,
        x DOUBLE,
        y DOUBLE,
        z FLOAT,
        heading FLOAT,
        ignition BOOLEAN,
        aircon BOOLEAN,
        door_up BOOLEAN,
        door_down BOOLEAN,
        sos BOOLEAN,
        working BOOLEAN,
        analog1 FLOAT,
        analog2 FLOAT,
        msgType STRING,
        timestamp TIMESTAMP,
        date DATE,
        load_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (date)
    TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.metadata.compression-codec'='gzip',
        'commit.manifest.merge.enabled'='false' -- Disable merging to keep many small manifest files for testing
    )
    """
    print(f"Ensuring table {table_name} exists...")
    spark.sql(create_table_sql)
    sys.stdout.flush()

def stream_kafka_to_iceberg(spark: SparkSession, table_name: str) -> None:
    # Define schema for the nested BusWayPoint
    buswaypoint_raw_schema = StructType([
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
        StructField("msgBusWayPoint", buswaypoint_raw_schema, True)
    ])
    
    # Read from Kafka topic
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "buswaypoint_json")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "5000")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    df_json = df_kafka.selectExpr("CAST(value AS STRING) as json")

    df = df_json.select(
        from_json(col("json"), root_schema).alias("data")
    ).select("data.*")

    df_filtered = df.filter(col("msgType") == "MsgType_BusWayPoint")
    df_flattened = df_filtered.select("msgType", "msgBusWayPoint.*")

    # Prepare final DataFrame with repartition to FORCE multiple small files per batch
    df_with_ts = (
        df_flattened
        .withColumn("timestamp", to_timestamp(from_unixtime("datetime")))
        .withColumn("date", to_date(from_unixtime("datetime")))
        .withColumn("load_at", current_timestamp())
        .repartition(10) # Forces at least 10 files per micro-batch
    )
    
    def write_batch_to_iceberg(batch_df, batch_id):
        batch_df.writeTo(table_name).append()
        print(f"Batch {batch_id} committed to {table_name}")
        sys.stdout.flush()

    # Write to Iceberg
    query = (
        df_with_ts
        .writeStream
        .foreachBatch(write_batch_to_iceberg)
        .option("checkpointLocation", "s3a://iceberg/lakehouse/checkpoints/bronze/buswaypoint_raw_test")
        .trigger(processingTime="0 seconds") 
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("FastIngestSmallFiles")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .config("spark.sql.catalog.catalog_iceberg.type", "rest")
        .config("spark.sql.catalog.catalog_iceberg.uri", "http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    
    table_name = "catalog_iceberg.bus_bronze.buswaypoint_raw"
    
    # Step 1: Create Schema
    create_table_if_not_exists(spark, table_name)
    
    # Step 2: Start Ingestion
    print(f"Starting ingestion into {table_name}...")
    sys.stdout.flush()
    
    stream_kafka_to_iceberg(spark, table_name)