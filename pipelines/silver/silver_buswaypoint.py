from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, hour, current_timestamp, round, 
    abs, broadcast, when, split, explode, trim
)

def main():
    spark = SparkSession.builder.appName("SilverBusWayPointEnrich").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Read Bronze data
    bus_df = spark.read.table("catalog_iceberg.bus_bronze.bus_way_point")
    mapping_df = spark.read.table("catalog_iceberg.bus_bronze.vehicles_mapping")
    route_info_df = spark.read.table("catalog_iceberg.bus_bronze.route_info")
    
    # Pre-deduplicate route_info to avoid waypoint duplication
    route_info_unique = route_info_df.dropDuplicates(["RouteId"])

    # 2. Prepare bus data (cleaning & basic transformations)
    bus_clean = (
        bus_df
        .filter(
            col("speed").isNotNull()
            & col("timestamp").isNotNull()
            & col("x").isNotNull()
            & col("y").isNotNull()
        )
        .withColumn("date", to_date(col("timestamp")))
        .withColumn("updated_at", current_timestamp())
    )

    # 3. Join with Mapping (Bronze)
    bus_with_mapping = bus_clean.join(
        broadcast(mapping_df),
        on="vehicle",
        how="left"
    )

    # 4. Join with Route Info (Bronze)
    bus_enriched = bus_with_mapping.join(
        broadcast(route_info_unique),
        bus_with_mapping.route_id == route_info_unique.RouteId,
        how="left"
    ).select(
        "vehicle",
        "driver",
        "timestamp",
        "date",
        "speed",
        col("x").alias("lng"),
        col("y").alias("lat"),
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

        col("route_id"),
        col("route_no"),
        col("RouteVarId").alias("route_var_id"),
        
        "updated_at"
    )

    # Create Silver table
    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_silver
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.buswaypoint_enriched (
            vehicle STRING,
            driver STRING,
            timestamp TIMESTAMP,
            date DATE,
            speed DOUBLE,
            lng DOUBLE,
            lat DOUBLE,
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

            route_id INT,
            route_no STRING,
            route_var_id INT,
            
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)
    """)

    bus_enriched.writeTo("catalog_iceberg.bus_silver.buswaypoint_enriched").append()

    print("WRITE bus_way_point SILVER SUCCESS")

if __name__ == "__main__":
    main()