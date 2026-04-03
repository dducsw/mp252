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
        col("RouteId").cast("int"),
        col("RouteNo").cast("string"),
        col("RouteVarId").cast("int"),
        col("RouteVarName").cast("string"),
        col("Outbound").cast("boolean"),

        expr("""
            transform(
                sequence(0, size(lat)-1),
                i -> array(
                    cast(lng[i] as double),
                    cast(lat[i] as double)
                )
            )
        """).alias("path")
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.route_path (
            RouteId INT,
            RouteNo STRING,
            RouteVarId INT,
            RouteVarName STRING,
            Outbound BOOLEAN,
            path ARRAY<ARRAY<DOUBLE>>
        )
        USING iceberg
        PARTITIONED BY (RouteId)
    """)

    df_clean.writeTo("catalog_iceberg.bus_silver.route_path").append()

    print("WRITE route_path SILVER SUCCESS")

if __name__ == "__main__":
    main()