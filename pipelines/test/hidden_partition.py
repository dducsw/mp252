from pyspark.sql import SparkSession
import sys

def evolve_to_hidden_partition(spark: SparkSession, table_name: str):
    print(f"Starting Partition Evolution for: {table_name}")
    
    # 1. Evolve Partition Spec
    print("Evolving partition spec: removing 'date', adding 'days(timestamp)'...")
    spark.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD date")
    spark.sql(f"ALTER TABLE {table_name} ADD PARTITION FIELD days(timestamp)")
    
    print("Partition Evolution completed!")
    sys.stdout.flush()

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("IcebergPartitionEvolution")
        .config("spark.sql.catalog.catalog_iceberg.type", "rest")
        .config("spark.sql.catalog.catalog_iceberg.uri", "http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg")
        .getOrCreate()
    )
    
    target_table = sys.argv[1] if len(sys.argv) > 1 else "catalog_iceberg.bus_bronze.buswaypoint_test"
    evolve_to_hidden_partition(spark, target_table)
