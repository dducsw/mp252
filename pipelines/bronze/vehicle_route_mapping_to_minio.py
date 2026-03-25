import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def main():
    spark = SparkSession.builder \
        .appName("VehicleRouteMappingToBronze") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Path to mapping CSV
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    csv_path = os.path.join(project_root, "data", "HPCLAB", "vehicle_route_mapping.csv")

    # Read CSV
    mapping_df = spark.read.option("header", "true").csv(csv_path)

    # Prepare data and cast route_id
    mapping_enriched = (
        mapping_df
        .select(
            col("vehicle"),
            col("route_id").cast("int"),
            col("route_no"),
            current_timestamp().alias("load_at")
        )
    )

    # Create Namespace if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_bronze")

    # Explicit CREATE TABLE
    spark.sql("""
    CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_bronze.vehicles_mapping (
        vehicle STRING,
        route_id INT,
        route_no STRING,
        load_at TIMESTAMP
    )
    USING iceberg
    """)

    # Write to Bronze Iceberg table
    mapping_enriched.writeTo("catalog_iceberg.bus_bronze.vehicles_mapping").overwrite(expr("true"))

    print("===== VEHICLE_ROUTE_MAPPING WRITE TO BRONZE SUCCESS =====")

if __name__ == "__main__":
    main()
