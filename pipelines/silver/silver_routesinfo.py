from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverRouteInfo")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.table("catalog_iceberg.bus_bronze.route_info")

    df_clean = (
        df
        .select(
            col("RouteId").cast("int").alias("route_id"),
            col("RouteNo").cast("string").alias("route_no"),
            col("RouteVarId").cast("int").alias("route_var_id"),
            col("RouteVarName").cast("string").alias("route_var_name"),
            col("Outbound").cast("boolean").alias("outbound"),
            col("Distance").cast("double").alias("distance_km"),
            col("RunningTime").cast("int").alias("running_time"),
            col("StartStop").cast("string").alias("start_stop"),
            col("EndStop").cast("string").alias("end_stop")
        )
        .withColumn(
            "direction",
            when(col("outbound") == True, "Outbound")
            .otherwise("Inbound")
        )
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.route_info (
            route_id INT,
            route_no STRING,
            route_var_id INT,
            route_var_name STRING,
            outbound BOOLEAN,
            direction STRING,
            distance_km DOUBLE,
            running_time INT,
            start_stop STRING,
            end_stop STRING
        )
        USING iceberg
        PARTITIONED BY (route_id)
    """)

    df_clean.writeTo("catalog_iceberg.bus_silver.route_info").append()

    print("WRITE route_info SUCCESS")

if __name__ == "__main__":
    main()