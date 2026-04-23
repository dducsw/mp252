from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    lag,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    sum as spark_sum,
    when,
)


SOURCE_TABLE = "catalog_iceberg.bus_silver.bus_way_point"
TARGET_TABLE = "catalog_iceberg.bus_gold.vehicle_daily_stats"


def main():
    spark = SparkSession.builder.appName("GoldVehicleDailyStats").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.table(SOURCE_TABLE)

    movement_window = Window.partitionBy("date", "vehicle").orderBy("timestamp")
    latest_window = Window.partitionBy("date", "vehicle").orderBy(col("timestamp").desc())

    df_enriched = (
        df.withColumn("prev_speed", lag("speed").over(movement_window))
        .withColumn("is_moving", when(col("speed") > 0, 1).otherwise(0))
        .withColumn("is_stopped", when(col("speed") == 0, 1).otherwise(0))
        .withColumn(
            "stop_event",
            when(
                (col("speed") == 0)
                & (col("prev_speed").isNotNull())
                & (col("prev_speed") > 0),
                1,
            ).otherwise(0),
        )
    )

    latest_route = (
        df_enriched.withColumn("rn", row_number().over(latest_window))
        .filter(col("rn") == 1)
        .select(
            "date",
            "vehicle",
            col("route_id").alias("latest_route_id"),
            col("route_no").alias("latest_route_no"),
            col("driver").alias("latest_driver"),
        )
    )

    daily_stats = (
        df_enriched.groupBy("date", "vehicle")
        .agg(
            spark_min("timestamp").alias("first_seen_at"),
            spark_max("timestamp").alias("last_seen_at"),
            count(lit(1)).alias("waypoint_count"),
            countDistinct("hour").alias("active_hours"),
            avg("speed").alias("avg_speed"),
            spark_max("speed").alias("max_speed"),
            spark_sum("is_moving").alias("moving_point_count"),
            spark_sum("is_stopped").alias("stopped_point_count"),
            spark_sum("stop_event").alias("stop_event_count"),
        )
        .join(latest_route, on=["date", "vehicle"], how="left")
        .withColumn(
            "moving_ratio",
            when(col("waypoint_count") > 0, col("moving_point_count") / col("waypoint_count"))
            .otherwise(0.0),
        )
        .withColumn(
            "stopped_ratio",
            when(col("waypoint_count") > 0, col("stopped_point_count") / col("waypoint_count"))
            .otherwise(0.0),
        )
        .withColumn("updated_at", current_timestamp())
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_gold")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_gold.vehicle_daily_stats (
            date DATE,
            vehicle STRING,
            latest_route_id INT,
            latest_route_no STRING,
            latest_driver STRING,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            waypoint_count BIGINT,
            active_hours BIGINT,
            avg_speed DOUBLE,
            max_speed DOUBLE,
            moving_point_count BIGINT,
            stopped_point_count BIGINT,
            stop_event_count BIGINT,
            moving_ratio DOUBLE,
            stopped_ratio DOUBLE,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)
        """
    )

    daily_stats.select(
        "date",
        "vehicle",
        "latest_route_id",
        "latest_route_no",
        "latest_driver",
        "first_seen_at",
        "last_seen_at",
        "waypoint_count",
        "active_hours",
        "avg_speed",
        "max_speed",
        "moving_point_count",
        "stopped_point_count",
        "stop_event_count",
        "moving_ratio",
        "stopped_ratio",
        "updated_at",
    ).writeTo(TARGET_TABLE).overwritePartitions()

    print("WRITE vehicle_daily_stats SUCCESS")


if __name__ == "__main__":
    main()
