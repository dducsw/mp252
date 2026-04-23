from pyspark.sql import SparkSession
import sys

def apply_z_order_spatial_optimization(spark: SparkSession, table_name: str):
    print(f"--- Applying Spatial Z-Order Optimization for: {table_name} ---")
    
    # Z-Order on (x, y) coordinates
    # This clusters geographically close points into the same data files
    print("Executing rewrite_data_files with Z-ORDER strategy on (x, y)...")
    
    spark.sql(f"""
        CALL catalog_iceberg.system.rewrite_data_files(
            table => '{table_name}',
            strategy => 'sort',
            sort_order => 'zorder(x, y)'
        )
    """)
    
    print("--- Z-Order Spatial Optimization Completed! ---")
    sys.stdout.flush()

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("IcebergZOrderOptimization")
        .config("spark.sql.catalog.catalog_iceberg.type", "rest")
        .config("spark.sql.catalog.catalog_iceberg.uri", "http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg")
        .getOrCreate()
    )
    
    target_table = sys.argv[1] if len(sys.argv) > 1 else "catalog_iceberg.bus_bronze.buswaypoint_test"
    apply_z_order_spatial_optimization(spark, target_table)
