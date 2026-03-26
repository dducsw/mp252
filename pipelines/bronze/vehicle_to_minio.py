from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
    return (
        SparkSession.builder
        .appName("VehicleBusMappingBronzeToIceberg")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("/opt/spark/apps/data/vehicle_route_mapping.csv")
    )

    print("===== RAW SCHEMA =====")
    df.printSchema()

    spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_bronze.vehicle_bus_mapping (
        vehicle STRING,
        route_id INT,
        route_no STRING
    )
    USING iceberg
    PARTITIONED BY (route_id)
    TBLPROPERTIES ('format-version'='2')
    """)

    df.writeTo("catalog_iceberg.bus_bronze.vehicle_bus_mapping").append()

    print("===== VEHICLE_BUS_MAPPING WRITE SUCCESS =====")


if __name__ == "__main__":
    main()