from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when

spark = SparkSession.builder \
    .appName("CarbonHive-Batch-Quality-Audit") \
    .getOrCreate()

path = "storage/parquet/telemetry_dynamic_batches"

df = spark.read.parquet(path)

print("\n========== DYNAMIC STREAM QUALITY AUDIT ==========")

print("Total records:", df.count())

print("\nOverall matching status:")
df.groupBy("grid_match_status") \
    .count() \
    .show(truncate=False)

print("\nBatch-wise matching status:")

df.groupBy("batch_id").agg(
    count("*").alias("total"),
    spark_sum(
        when(col("grid_match_status") == "matched", 1).otherwise(0)
    ).alias("matched"),
    spark_sum(
        when(col("grid_match_status") != "matched", 1).otherwise(0)
    ).alias("unmatched")
).orderBy(col("batch_id").desc()).show(50, truncate=False)

spark.stop()
