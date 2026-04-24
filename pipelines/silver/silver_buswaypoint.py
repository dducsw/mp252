from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, row_number
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder.appName("SilverWayPointClean").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Read bronze tables
    df_bus = spark.read.table("catalog_iceberg.bus_bronze.bus_way_point")
    df_map = spark.read.table("catalog_iceberg.bus_bronze.vehicle_bus_mapping")

    # Join vehicle -> route
    df_join = df_bus.join(df_map, on="vehicle", how="left")

    # Step 1 & 2: Filter and Deduplicate (Using Window to get the latest record)
    window_spec = Window.partitionBy("vehicle", "timestamp").orderBy(col("load_at").desc())

    df_filtered = (
        df_join.filter(
            col("vehicle").isNotNull() & 
            col("timestamp").isNotNull() & 
            col("x").isNotNull() & (col("x") != 0) & 
            col("y").isNotNull() & (col("y") != 0)
        )
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Clean + add time columns + Step 5: Sorting
    df_clean = (
        df_filtered.withColumn("date", to_date(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
        .withColumn("updated_at", current_timestamp())
        .select(
            "vehicle",
            "route_id",
            "route_no",
            "driver",
            "timestamp",
            "date",
            "hour",
            "speed",
            "x",
            "y",
            "z",
            "heading",
            "ignition",
            "aircon",
            "door_up",
            "door_down",
            "sos",
            "working",
            "analog1",
            "analog2",
            "updated_at"
        )
        .sortWithinPartitions(
            col("route_id").asc_nulls_first(),
            col("vehicle").asc_nulls_first(),
            col("timestamp").asc_nulls_first()
        )
    )

    # Create Silver table
    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_silver
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.bus_way_point (
            vehicle STRING,
            route_id INT,
            route_no STRING,
            driver STRING,
            timestamp TIMESTAMP,
            date DATE,
            hour INT,
            speed DOUBLE,
            x DOUBLE,
            y DOUBLE,
            z DOUBLE,
            heading FLOAT,
            ignition BOOLEAN,
            aircon BOOLEAN,
            door_up BOOLEAN,
            door_down BOOLEAN,
            sos BOOLEAN,
            working BOOLEAN,
            analog1 FLOAT,
            analog2 FLOAT,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)
    """)

    # Step 4: Write to Silver with overwritePartitions
    df_clean.writeTo("catalog_iceberg.bus_silver.bus_way_point").overwritePartitions()

    print("WRITE bus_silver.bus_way_point SUCCESS (Idempotent, Cleaned, Sorted)")


if __name__ == "__main__":
    main()