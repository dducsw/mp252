from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, when, round

def main():
    spark = SparkSession.builder.appName("GoldBusDashboard").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read.table("catalog_iceberg.bus_silver.weekly_bus_analysis")
        .filter(col("date").between("2025-04-22", "2025-04-29"))
    )

    df = df.withColumn("hour", hour("timestamp"))

    df = df.withColumn("day_of_week", dayofweek("date"))

    df = df.withColumn(
        "day_type",
        when(col("day_of_week").isin([1,7]), "Weekend").otherwise("Weekday")
    )

    df = df.withColumn(
        "is_peak_hour",
        when(col("hour").isin([7,8,17,18]), 1).otherwise(0)
    )

    df = df.withColumn(
        "speed_level",
        when(col("speed") == 0, "stopped")
        .when(col("speed") < 20, "slow")
        .when(col("speed") < 60, "normal")
        .otherwise("fast")
    )

    df = df.withColumn(
        "is_moving",
        when(col("speed") > 0, 1).otherwise(0)
    )

    df = df.withColumn(
        "is_stopped",
        when(col("speed") == 0, 1).otherwise(0)
    )

    df.select(
        "vehicle",
        col("RouteNo").alias("route_no"),
        "date",
        "hour",
        "day_of_week",
        "day_type",
        "is_peak_hour",
        "speed",
        "speed_level",
        "is_moving",
        "is_stopped",
        col("x"),
        col("y"),
    ).writeTo("catalog_iceberg.bus_gold.gold_bus_dashboard").createOrReplace()

    print("WRITE GOLD SUCCESS")

if __name__ == "__main__":
    main()