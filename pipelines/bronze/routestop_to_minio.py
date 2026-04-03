from pyspark.sql import SparkSession

def create_spark_session():
    return (
        SparkSession.builder
        .appName("RouteStopBronzeToIceberg")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    df = (
        spark.read
        .option("multiLine", True)
        .json("/opt/spark/apps/data/MCPT/route_stops.json")
    )

    df_clean = df.select(
        "RouteId",
        "RouteVarId",
        "StopId",
        "Code",
        "Name",
        "StopType",
        "Zone",
        "Ward",
        "AddressNo",
        "Street",
        "Lng",
        "Lat",
        "Routes",
        "Outbound"
    )

    spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_bronze
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_bronze.route_stop (
        RouteId INT,
        RouteVarId INT,
        StopId INT,
        Code STRING,
        Name STRING,
        StopType STRING,
        Zone STRING,
        Ward STRING,
        AddressNo STRING,
        Street STRING,
        Lng DOUBLE,
        Lat DOUBLE,
        Routes STRING,
        Outbound BOOLEAN
    )
    USING iceberg
    PARTITIONED BY (RouteId)
    TBLPROPERTIES (
        'format-version'='2'
    )
    """)

    spark.sql("""
    ALTER TABLE catalog_iceberg.bus_bronze.route_stop
    ADD COLUMN IF NOT EXISTS Outbound BOOLEAN
    """)

    df_clean.writeTo("catalog_iceberg.bus_bronze.route_stop").append()

    print("===== ROUTE_STOP WRITE SUCCESS =====")


if __name__ == "__main__":
    main()
