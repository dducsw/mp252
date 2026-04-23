from pyspark.sql import SparkSession

# ================== CONFIG ==================
APP_NAME = "IcebergSortOrder"

TABLE = "catalog_iceberg.bus_silver.bus_way_point"

ICEBERG_CATALOG_NAME = "iceberg"
ICEBERG_REST_URI = "http://iceberg-rest:8181"
ICEBERG_WAREHOUSE = "s3a://lake/"

S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin123"

# Sort order đề xuất cho workload hiện tại:
# - route_id: gom dữ liệu theo tuyến xe
# - timestamp: giữ dữ liệu theo trục thời gian trong từng tuyến
SORT_ORDER_SQL = "route_id ASC NULLS LAST, timestamp ASC NULLS LAST"

# Bật bước rewrite sau khi set sort order để dữ liệu hiện có cũng được sắp lại layout.
ENABLE_REWRITE_AFTER_SORT = True
TARGET_FILE_SIZE_BYTES = 64 * 1024 * 1024


spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", ICEBERG_REST_URI)
    .config("spark.sql.catalog.iceberg.warehouse", ICEBERG_WAREHOUSE)
    .config("spark.sql.defaultCatalog", ICEBERG_CATALOG_NAME)
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def run(sql: str):
    print("\n=== RUN SQL ===")
    print(sql.strip())
    result = spark.sql(sql)
    print("=== RESULT ===")
    result.show(truncate=False)
    return result


def show_table_info():
    print("\n=== TABLE INFO ===")
    spark.sql(f"DESCRIBE TABLE {TABLE}").show(truncate=False)

    print("\n=== TABLE PROPERTIES ===")
    spark.sql(f"SHOW TBLPROPERTIES {TABLE}").show(truncate=False)


def set_sort_order():
    print("\n>>> [1] Set Iceberg write order")
    run(
        f"""
        ALTER TABLE {TABLE}
        WRITE ORDERED BY {SORT_ORDER_SQL}
        """
    )


def rewrite_data_files_with_sort():
    if not ENABLE_REWRITE_AFTER_SORT:
        print("Skip rewrite_data_files_with_sort (disabled).")
        return

    print("\n>>> [2] Rewrite data files to apply sort order on existing data")
    run(
        f"""
        CALL iceberg.system.rewrite_data_files(
            table => '{TABLE}',
            strategy => 'sort',
            sort_order => '{SORT_ORDER_SQL}',
            options => map(
                'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}'
            )
        )
        """
    )


if __name__ == "__main__":
    try:
        show_table_info()
        set_sort_order()
        rewrite_data_files_with_sort()
        show_table_info()
    finally:
        spark.stop()
        print("\n>>> Spark session stopped.")
