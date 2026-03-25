from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverRoutePath")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.table("catalog_iceberg.bus_bronze.route_path")

    df_clean = df.select(
        col("RouteId").cast("int").alias("route_id"),
        col("RouteNo").cast("string").alias("route_no"),
        col("RouteVarId").cast("int").alias("route_var_id"),
        col("RouteVarName").cast("string").alias("route_var_name"),
        col("Outbound").cast("boolean").alias("outbound"),

        # path for deck.gl: [[lng, lat], ...]
        expr("""
            transform(
                sequence(0, size(lat)-1),
                i -> array(
                    cast(lng[i] as double),
                    cast(lat[i] as double)
                )
            )
        """).alias("path"),
        current_timestamp().alias("updated_at")
    )

    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_silver
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.route_path (
            route_id INT,
            route_no STRING,
            route_var_id INT,
            route_var_name STRING,
            outbound BOOLEAN,
            path ARRAY<ARRAY<DOUBLE>>,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (route_id)
    """)

    df_clean.writeTo("catalog_iceberg.bus_silver.route_path").append()

    print("WRITE route_path SILVER SUCCESS")

if __name__ == "__main__":
    main()