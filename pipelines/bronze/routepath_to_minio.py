from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def create_spark_session():
    return (
        SparkSession.builder
        .appName("RoutePathBronzeToIceberg")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .option("multiLine", True)
        .json("/opt/spark/apps/data/MCPT/route_paths.json")
    )

    df_clean = df.select(
        col("RouteId").cast("int").alias("RouteId"),
        col("RouteNo").cast("string").alias("RouteNo"),
        col("RouteVarId").cast("int").alias("RouteVarId"),
        col("RouteVarName").cast("string").alias("RouteVarName"),
        col("Outbound").cast("boolean").alias("Outbound"),
        col("lat").cast("array<double>").alias("lat"),
        col("lng").cast("array<double>").alias("lng")
    )

    spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_bronze
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_bronze.route_path (
        RouteId INT,
        RouteNo STRING,
        RouteVarId INT,
        RouteVarName STRING,
        Outbound BOOLEAN,
        lat ARRAY<DOUBLE>,
        lng ARRAY<DOUBLE>
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='2'
    )
    """)

    df_clean.writeTo("catalog_iceberg.bus_bronze.route_path").overwrite(expr("true"))
    print("===== WRITE SUCCESS =====")


if __name__ == "__main__":
    main()
