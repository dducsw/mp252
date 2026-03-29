from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour

def main():
    spark = SparkSession.builder.appName("SilverWayPointClean").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Read bronze tables
    df_bus = spark.read.table("catalog_iceberg.bus_bronze.bus_way_point")
    df_map = spark.read.table("catalog_iceberg.bus_bronze.vehicle_bus_mapping")

    # Join vehicle -> route
    df_join = df_bus.join(df_map, on="vehicle", how="left")

    # Clean + add time columns
    df_clean = (
        df_join.withColumn("date", to_date(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
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
            "analog2"
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
            analog2 FLOAT
        )
        USING iceberg
        PARTITIONED BY (date)
    """)

    # Write to Silver
    df_clean.writeTo("catalog_iceberg.bus_silver.bus_way_point").append()

    print("WRITE bus_silver.bus_way_point SUCCESS")


if __name__ == "__main__":
    main()