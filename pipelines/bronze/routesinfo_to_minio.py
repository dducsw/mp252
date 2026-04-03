from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

def create_spark_session():
    return (
        SparkSession.builder
        .appName("RouteInfoBronzeToIceberg")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .option("multiLine", True)
        .json("/opt/spark/apps/data/MCPT/routes_info.json")
    )

    print("===== RAW SCHEMA =====")
    df.printSchema()

    # 🔥 explode Vars
    df_exploded = df.withColumn("var", explode("Vars"))

    df_clean = df_exploded.select(
        col("RouteId"),
        col("RouteNo"),
        col("var.RouteVarId").alias("RouteVarId"),
        col("var.RouteVarName").alias("RouteVarName"),
        col("var.Outbound").alias("Outbound"),
        col("var.Distance").alias("Distance"),
        col("var.RunningTime").alias("RunningTime"),
        col("var.StartStop").alias("StartStop"),
        col("var.EndStop").alias("EndStop")
    )

    spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_bronze
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_bronze.route_info (
        RouteId INT,
        RouteNo STRING,
        RouteVarId INT,
        RouteVarName STRING,
        Outbound BOOLEAN,
        Distance DOUBLE,
        RunningTime INT,
        StartStop STRING,
        EndStop STRING
    )
    USING iceberg
    PARTITIONED BY (RouteId)
    TBLPROPERTIES ('format-version'='2')
    """)

    df_clean.writeTo("catalog_iceberg.bus_bronze.route_info").append()

    print("===== ROUTE_INFO WRITE SUCCESS =====")


if __name__ == "__main__":
    main()