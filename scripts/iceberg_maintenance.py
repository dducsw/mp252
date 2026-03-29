from pyspark.sql import SparkSession
from datetime import datetime, timedelta, timezone

# ================== CONFIG ==================
APP_NAME = "IcebergMaintenance"

TABLE = "catalog_iceberg.bus_silver.bus_way_point"  # Thay bằng tên bảng bạn muốn bảo trì

# Iceberg REST catalog endpoint
ICEBERG_REST_URI = "http://iceberg-rest:8181"

ENABLE_COMPACT_DATA_FILES   = True   # gom file nhỏ -> file lớn
ENABLE_EXPIRE_SNAPSHOTS     = True   # xóa snapshot cũ
ENABLE_REMOVE_ORPHANS       = True   # xóa file mồ côi
ENABLE_REWRITE_MANIFESTS    = True   # gom manifest nhỏ

MIN_FILE_SIZE_BYTES     = 3 * 1024 * 1024       
TARGET_FILE_SIZE_BYTES  = 64 * 1024 * 1024     
MAX_FILE_SIZE_BYTES     = 128 * 1024 * 1024   
RETAIN_LAST_SNAPSHOTS   = 1   
ORPHAN_RETENTION_DAYS   = 10 

spark = (
    SparkSession.builder
    .appName(APP_NAME)

    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.iceberg.type", "rest")
    .config(f"spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
    .config(f"spark.sql.catalog.iceberg.warehouse", "s3a://lake/")
    .config("spark.sql.defaultCatalog", "iceberg") 
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def run(sql: str):
    print("\n=== RUN SQL ===")
    print(sql.strip())
    res = spark.sql(sql)
    print("=== RESULT ===")
    res.show(truncate=False)


# ================== MAINTENANCE TASKS ==================
print("\n=== CHECK ENV ===")
spark.sql("SHOW CATALOGS").show()
spark.sql("SHOW DATABASES IN iceberg").show()

print("\n=== Row count hiện tại ===")
spark.sql("SELECT COUNT(*) AS row_count FROM iceberg.testsi.buswaypoint").show()

print("\n=== Snapshots hiện tại ===")
spark.sql("""
    SELECT committed_at, snapshot_id, parent_id, operation
    FROM iceberg.testsi.buswaypoint.snapshots
""").show(truncate=False)
# spark.sql("SHOW TABLES IN iceberg.bronze").show()

# spark.sql("SELECT COUNT(*) AS row_count FROM iceberg.testsi.buswaypoint").show()

spark.sql("""
    SELECT committed_at, snapshot_id, parent_id, operation
    FROM iceberg.testsi.buswaypoint.snapshots
""").show(truncate=False)

# print("\n=== Row count hiện tại ===")
# spark.sql("SHOW CATALOGS").show()
# spark.sql("SELECT COUNT(*) AS row_count FROM iceberg.testsi.buswaypoint").show()

print("\n=== Số lượng snapshot hiện tại ===")
spark.sql("SELECT COUNT(*) AS snapshot_count FROM iceberg.testsi.buswaypoint.snapshots").show()

print("\n=== Số lượng manifest hiện tại ===")
spark.sql("SELECT COUNT(*) AS manifest_count FROM iceberg.testsi.buswaypoint.manifests").show()

print("\n=== Mô tả bảng ===")
spark.sql("DESCRIBE TABLE iceberg.testsi.buswaypoint").show(truncate=False)
# print("\n=== Snapshots hiện tại (nếu có) ===")
# spark.sql(f"""
#     SELECT committed_at, snapshot_id, parent_id, operation
#     FROM iceberg.testsi.buswaypoint.snapshots
# """).show(truncate=False)

def compact_data_files():
    """
    Gom các data file nhỏ thành file lớn hơn.
    """
    if not ENABLE_COMPACT_DATA_FILES:
        print("Skip compact_data_files (disabled).")
        return
    print("\n>>> [1] Compact small data files")
    sql = f"""
        CALL iceberg.system.rewrite_data_files(
            table => '{TABLE}',
            options => map(
                'min-file-size-bytes',    '{MIN_FILE_SIZE_BYTES}',
                'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                'max-file-size-bytes',    '{MAX_FILE_SIZE_BYTES}'
            )
        )
    """
    run(sql)

def rewrite_manifests():
    """
    Gom manifest nhỏ, sắp xếp lại metadata để query nhanh hơn.
    """
    if not ENABLE_REWRITE_MANIFESTS:
        print("Skip rewrite_manifests (disabled).")
        return

    print("\n>>> [2] Rewrite manifests")
    sql = f"""
        CALL iceberg.system.rewrite_manifests(
            table => '{TABLE}'
        )
    """
    run(sql)


def expire_snapshots():
    """
    Xóa snapshot cũ, chỉ giữ lại một số snapshot gần nhất (expire_snapshots).
    """
    if not ENABLE_EXPIRE_SNAPSHOTS:
        print("Skip expire_snapshots (disabled).")
        return

    print("\n>>> [3] Expire old snapshots")
    sql = f"""
        CALL iceberg.system.expire_snapshots(
            table => '{TABLE}',
            retain_last => {RETAIN_LAST_SNAPSHOTS}
        )
    """
    run(sql)


def remove_orphan_files():
    """
    Xóa orphan files – file không còn được snapshot nào tham chiếu.
    """
    if not ENABLE_REMOVE_ORPHANS:
        print("Skip remove_orphan_files (disabled).")
        return

    print("\n>>> [4] Remove orphan files")
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=ORPHAN_RETENTION_DAYS)
    ).strftime("%Y-%m-%d %H:%M:%S")

    sql = f"""
        CALL iceberg.system.remove_orphan_files(
            table => '{TABLE}',
            older_than => TIMESTAMP '{cutoff}'
        )
    """
    run(sql)
# ================== MAIN ==================
if __name__ == "__main__":
    try:

        # compact_data_files()
        # spark.sql("SELECT snapshot_id, operation FROM iceberg.testsi.buswaypoint.snapshots ORDER BY committed_at DESC LIMIT 3;").show()
        # rewrite_manifests()
        expire_snapshots()
        # remove_orphan_files()
    finally:
        spark.stop()
        print("\n>>> Spark session stopped.")
