from datetime import datetime, timedelta, timezone
from time import perf_counter

from pyspark.sql import SparkSession

# ================== CONFIG ==================
APP_NAME = "IcebergMaintenance"

# Use a single fully-qualified table name consistently across diagnostics and maintenance.
TABLE = "catalog_iceberg.bus_silver.bus_way_point"

ICEBERG_CATALOG_NAME = "iceberg"
ICEBERG_REST_URI = "http://iceberg-rest:8181"
ICEBERG_WAREHOUSE = "s3a://lake/"

S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin123"

ENABLE_COMPACT_DATA_FILES = True
ENABLE_REWRITE_MANIFESTS = True
ENABLE_EXPIRE_SNAPSHOTS = True
ENABLE_REMOVE_ORPHANS = True

MIN_FILE_SIZE_BYTES = 3 * 1024 * 1024
TARGET_FILE_SIZE_BYTES = 64 * 1024 * 1024
MAX_FILE_SIZE_BYTES = 128 * 1024 * 1024
RETAIN_LAST_SNAPSHOTS = 3
ORPHAN_RETENTION_DAYS = 10

BENCHMARK_QUERIES = [
    (
        "count_all",
        f"SELECT COUNT(*) AS row_count FROM {TABLE}",
    ),
    (
        "latest_7_days",
        f"""
        SELECT date, COUNT(*) AS row_count
        FROM {TABLE}
        WHERE date >= date_sub(current_date(), 7)
        GROUP BY date
        ORDER BY date DESC
        """,
    ),
    (
        "route_daily_agg",
        f"""
        SELECT route_id, date, COUNT(*) AS row_count
        FROM {TABLE}
        GROUP BY route_id, date
        ORDER BY date DESC, row_count DESC
        LIMIT 20
        """,
    ),
]


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


def show_query(title: str, sql: str):
    print(f"\n=== {title} ===")
    spark.sql(sql).show(truncate=False)


def show_environment():
    print("\n=== CHECK ENV ===")
    spark.sql("SHOW CATALOGS").show(truncate=False)
    spark.sql(f"SHOW DATABASES IN {ICEBERG_CATALOG_NAME}").show(truncate=False)


def show_table_health():
    show_query("Table", f"DESCRIBE TABLE {TABLE}")
    show_query("Row count", f"SELECT COUNT(*) AS row_count FROM {TABLE}")
    show_query(
        "Snapshots",
        f"""
        SELECT committed_at, snapshot_id, parent_id, operation
        FROM {TABLE}.snapshots
        ORDER BY committed_at DESC
        """,
    )
    show_query(
        "Snapshot count",
        f"SELECT COUNT(*) AS snapshot_count FROM {TABLE}.snapshots",
    )
    show_query(
        "Manifest count",
        f"SELECT COUNT(*) AS manifest_count FROM {TABLE}.manifests",
    )


def execute_benchmark(name: str, sql: str):
    print(f"\n=== Benchmark: {name} ===")
    print(sql.strip())
    started_at = perf_counter()
    result = spark.sql(sql)
    # Force evaluation so the measured duration reflects query execution time.
    rows = result.collect()
    duration_seconds = perf_counter() - started_at
    print(f"Duration: {duration_seconds:.3f}s")
    spark.createDataFrame(rows, result.schema).show(truncate=False)
    return duration_seconds


def run_benchmarks(stage_label: str):
    print(f"\n=== QUERY BENCHMARKS ({stage_label}) ===")
    benchmark_results = {}
    for name, sql in BENCHMARK_QUERIES:
        benchmark_results[name] = execute_benchmark(name, sql)
    return benchmark_results


def show_benchmark_comparison(before_results, after_results):
    print("\n=== BENCHMARK COMPARISON ===")
    print("query_name | before_s | after_s | improvement_s | improvement_pct")

    for name, _ in BENCHMARK_QUERIES:
        before_seconds = before_results[name]
        after_seconds = after_results[name]
        improvement_seconds = before_seconds - after_seconds
        improvement_pct = (
            (improvement_seconds / before_seconds) * 100 if before_seconds > 0 else 0.0
        )
        print(
            f"{name} | "
            f"{before_seconds:.3f} | "
            f"{after_seconds:.3f} | "
            f"{improvement_seconds:.3f} | "
            f"{improvement_pct:.2f}%"
        )


def compact_data_files():
    """
    Rewrite small data files into fewer larger files first to reduce read overhead.
    """
    if not ENABLE_COMPACT_DATA_FILES:
        print("Skip compact_data_files (disabled).")
        return

    print("\n>>> [1] Compact small data files")
    run(
        f"""
        CALL iceberg.system.rewrite_data_files(
            table => '{TABLE}',
            options => map(
                'min-file-size-bytes',    '{MIN_FILE_SIZE_BYTES}',
                'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                'max-file-size-bytes',    '{MAX_FILE_SIZE_BYTES}'
            )
        )
        """
    )


def rewrite_manifests():
    """
    Rewrite manifests after data file compaction so query planning sees cleaner metadata.
    """
    if not ENABLE_REWRITE_MANIFESTS:
        print("Skip rewrite_manifests (disabled).")
        return

    print("\n>>> [2] Rewrite manifests")
    run(
        f"""
        CALL iceberg.system.rewrite_manifests(
            table => '{TABLE}'
        )
        """
    )


def expire_snapshots():
    """
    Expire old snapshots after metadata/layout rewrites, while retaining a few recent recovery points.
    """
    if not ENABLE_EXPIRE_SNAPSHOTS:
        print("Skip expire_snapshots (disabled).")
        return

    print("\n>>> [3] Expire old snapshots")
    run(
        f"""
        CALL iceberg.system.expire_snapshots(
            table => '{TABLE}',
            retain_last => {RETAIN_LAST_SNAPSHOTS}
        )
        """
    )


def remove_orphan_files():
    """
    Remove orphan files last, after old snapshots have been expired.
    """
    if not ENABLE_REMOVE_ORPHANS:
        print("Skip remove_orphan_files (disabled).")
        return

    print("\n>>> [4] Remove orphan files")
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=ORPHAN_RETENTION_DAYS)
    ).strftime("%Y-%m-%d %H:%M:%S")

    run(
        f"""
        CALL iceberg.system.remove_orphan_files(
            table => '{TABLE}',
            older_than => TIMESTAMP '{cutoff}'
        )
        """
    )


def run_maintenance():
    compact_data_files()
    rewrite_manifests()
    expire_snapshots()
    remove_orphan_files()


if __name__ == "__main__":
    try:
        show_environment()
        show_table_health()
        benchmark_before = run_benchmarks("BEFORE MAINTENANCE")
        run_maintenance()
        show_table_health()
        benchmark_after = run_benchmarks("AFTER MAINTENANCE")
        show_benchmark_comparison(benchmark_before, benchmark_after)
    finally:
        spark.stop()
        print("\n>>> Spark session stopped.")
