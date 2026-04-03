from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, split, explode, when, broadcast, round, abs

def main():
    spark = SparkSession.builder.appName("SilverWeeklyBus").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    bus_df = spark.read.table("catalog_iceberg.bus_bronze.bus_way_point") \
        .filter(col("date").between("2025-04-22", "2025-04-29")) \
        .withColumn("x_round", round(col("x"), 4)) \
        .withColumn("y_round", round(col("y"), 4))

    stop_df = spark.read.table("catalog_iceberg.bus_bronze.route_stop")
    route_info_df = (
        spark.read.table("catalog_iceberg.bus_bronze.route_info")
        .select("RouteId", "RouteVarId", "Outbound")
        .dropDuplicates(["RouteId", "RouteVarId"])
    )

    stop_expand = (
        stop_df.alias("s")
        .join(
            broadcast(route_info_df).alias("r"),
            on=["RouteId", "RouteVarId"],
            how="left"
        )
        .withColumn("RouteNo", explode(split(col("Routes"), ",")))
        .withColumn("RouteNo", trim(col("RouteNo")))
        .withColumn("Lng_round", round(col("Lng"), 4))
        .withColumn("Lat_round", round(col("Lat"), 4))
        .select("Lng", "Lat", "RouteNo", "Lng_round", "Lat_round", "Outbound")
    )
    joined_df = (
        bus_df.alias("b")
        .join(
            broadcast(stop_expand).alias("s"),
            (col("b.x_round") == col("s.Lng_round")) &
            (col("b.y_round") == col("s.Lat_round")),
            "inner"
        ).filter(
            (abs(col("b.x") - col("s.Lng")) < 0.0001) &
            (abs(col("b.y") - col("s.Lat")) < 0.0001)
        )
    )

    result_df = (
        joined_df
        .withColumn(
            "speed_level",
            when(col("speed") == 0, "stopped")
            .when(col("speed") < 20, "slow")
            .when(col("speed") < 60, "normal")
            .otherwise("fast")
        ).select(
            "vehicle","timestamp","x","y","speed",
            "RouteNo","ignition","aircon","sos","working",
            "speed_level","date", "outbound"
        )
    )

    result_df.writeTo("catalog_iceberg.bus_silver.weekly_bus_analysis").createOrReplace()

    print("WRITE SILVER SUCCESS")

if __name__ == "__main__":
    main()
