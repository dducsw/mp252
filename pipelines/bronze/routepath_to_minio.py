from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def create_spark_session():
    return (
        SparkSession.builder
        .appName("RoutePathToMinIO")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = (
        spark.read
        .option("multiLine", True)
        .json("/opt/spark/apps/data/MCPT/route_paths.json")
    )

    df_clean = df.select(
        col("RouteId").cast("int"),
        col("RouteNo").cast("string"),
        col("RouteVarId").cast("int"),
        col("RouteVarName").cast("string"),
        col("Outbound").cast("boolean"),

        # 🔥 path chuẩn cho deck.gl
        expr("""
            transform(
                sequence(0, size(lat)-1),
                i -> array(
                    cast(lng[i] as double),
                    cast(lat[i] as double)
                )
            )
        """).alias("path")
    )

    # ===== WRITE TO MINIO =====
    df_clean.write \
        .format("iceberg") \
        .mode("overwrite") \
        .save("catalog_iceberg.bus_bronze.route_path")

    print("===== WRITE TO MINIO SUCCESS =====")

if __name__ == "__main__":
    main()