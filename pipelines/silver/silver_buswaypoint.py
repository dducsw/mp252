from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour

def main():
    spark = SparkSession.builder.appName("SilverWayPointClean").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.table("catalog_iceberg.bus_bronze.bus_way_point")

    df_clean = (
        df.withColumn("date", to_date(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
        .select(
            "vehicle",
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

    # Create table with partition
    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.bus_way_point (
            vehicle STRING,
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

    df_clean.writeTo("catalog_iceberg.bus_silver.bus_way_point").append()

    print("WRITE bus_way_point SUCCESS")

if __name__ == "__main__":
    main()