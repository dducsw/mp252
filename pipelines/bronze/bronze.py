from pyspark.sql import SparkSession

def create_bronze_tables(spark: SparkSession):

    # Tạo namespace cho Bronze layer
    spark.sql("""
    CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_bronze
    """)

    # Tạo bảng bus_way_point đúng schema từ consumer
    spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_bronze.bus_way_point (
        msgType STRING,
        vehicle STRING,
        driver STRING,
        speed FLOAT,
        datetime INT,
        x DOUBLE,
        y DOUBLE,
        z FLOAT,
        heading FLOAT,
        ignition BOOLEAN,
        aircon BOOLEAN,
        door_up BOOLEAN,
        door_down BOOLEAN,
        sos BOOLEAN,
        working BOOLEAN,
        analog1 FLOAT,
        analog2 FLOAT,
        timestamp TIMESTAMP,
        date DATE,
        load_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (date)
    """)

    print("Bronze table bus_way_point created or already exists.")


if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("InitBronzeBusWayPoint")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    create_bronze_tables(spark)