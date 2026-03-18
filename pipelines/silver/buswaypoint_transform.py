from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, broadcast, col, explode_outer, expr

def create_spark_session():
    return (
        SparkSession.builder
        .appName("TripDashboardSilver")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    bus = (
        spark.read.format("iceberg")
        .load("catalog_iceberg.bus_bronze.bus_way_point")
        .select("vehicle", "timestamp", "speed", "x", "y")
        .filter(
            col("speed").isNotNull()
            & col("timestamp").isNotNull()
            & col("x").isNotNull()
            & col("y").isNotNull()
        )
        .orderBy(col("timestamp").desc())
    )
    route = (
        spark.read.format("iceberg")
        .load("catalog_iceberg.bus_bronze.route_path")
        .select("RouteId", "RouteNo", "RouteVarName", "lat", "lng")
    )
    stop = (
        spark.read.format("iceberg")
        .load("catalog_iceberg.bus_bronze.route_stop")
        .select(
            col("Name").alias("StopName"),
            col("Lat").alias("StopLat"),
            col("Lng").alias("StopLng")
        )
    )

    route_flat = route.select(
        "RouteId",
        "RouteNo",
        "RouteVarName",
        explode_outer(arrays_zip("lat", "lng")).alias("point")
    ).select(
        "RouteId",
        "RouteNo",
        "RouteVarName",
        col("point.lat").alias("lat"),
        col("point.lng").alias("lng")
    ).filter(
        col("lat").isNotNull() & col("lng").isNotNull()
    )

    df = bus.alias("b").join(
        broadcast(route_flat).alias("r"),
        expr("""
            abs(b.x - r.lng) < 0.0007 AND
            abs(b.y - r.lat) < 0.0007
        """),
        "left"
    )

    df = df.join(
        broadcast(stop).alias("s"),
        expr("""
            abs(b.y - s.StopLat) < 0.001 AND
            abs(b.x - s.StopLng) < 0.001
        """),
        "left"
    )

    df_final = df.select(
        col("b.vehicle"),
        col("b.timestamp"),
        col("b.speed"),
        col("b.y").alias("lat"),
        col("b.x").alias("lng"),

        col("r.RouteId"),
        col("r.RouteNo"),
        col("r.RouteVarName"),

        col("s.StopName"),
        col("s.StopLat"),
        col("s.StopLng")
    )

    spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_silver
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.dashboard_trip (
        vehicle STRING,
        timestamp TIMESTAMP,
        speed FLOAT,
        lat DOUBLE,
        lng DOUBLE,

        RouteId INT,
        RouteNo STRING,
        RouteVarName STRING,

        StopName STRING,
        StopLat DOUBLE,
        StopLng DOUBLE
    )
    USING iceberg
""")

    df_final.writeTo("catalog_iceberg.bus_silver.dashboard_trip").overwrite(expr("true"))

    print("===== FINAL SILVER READY =====")


if __name__ == "__main__":
    main()
