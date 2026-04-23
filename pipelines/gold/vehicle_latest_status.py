from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, current_timestamp, row_number, when


SOURCE_TABLE = "catalog_iceberg.bus_silver.bus_way_point"
TARGET_TABLE = "catalog_iceberg.bus_gold.vehicle_latest_status"


def main():
    spark = SparkSession.builder.appName("GoldVehicleLatestStatus").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.table(SOURCE_TABLE)

    latest_window = Window.partitionBy("vehicle").orderBy(col("timestamp").desc())

    latest_status = (
        df.withColumn("rn", row_number().over(latest_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("is_moving", when(col("speed") > 0, 1).otherwise(0))
        .withColumn("is_stopped", when(col("speed") == 0, 1).otherwise(0))
        .withColumn("updated_at", current_timestamp())
        .select(
            "vehicle",
            "route_id",
            "route_no",
            "driver",
            col("timestamp").alias("last_timestamp"),
            col("date").alias("last_date"),
            col("hour").alias("last_hour"),
            col("speed").alias("last_speed"),
            "is_moving",
            "is_stopped",
            "x",
            "y",
            "heading",
            "ignition",
            "working",
            "aircon",
            "door_up",
            "door_down",
            "sos",
            "updated_at",
        )
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_gold")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_gold.vehicle_latest_status (
            vehicle STRING,
            route_id INT,
            route_no STRING,
            driver STRING,
            last_timestamp TIMESTAMP,
            last_date DATE,
            last_hour INT,
            last_speed DOUBLE,
            is_moving INT,
            is_stopped INT,
            x DOUBLE,
            y DOUBLE,
            heading FLOAT,
            ignition BOOLEAN,
            working BOOLEAN,
            aircon BOOLEAN,
            door_up BOOLEAN,
            door_down BOOLEAN,
            sos BOOLEAN,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (last_date)
        """
    )

    latest_status.writeTo(TARGET_TABLE).overwritePartitions()

    print("WRITE vehicle_latest_status SUCCESS")


if __name__ == "__main__":
    main()
