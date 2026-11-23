from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count,
    min as spark_min,
    max as spark_max
)

spark = SparkSession.builder \
    .appName("CarbonHive-Unmatched-Summary") \
    .getOrCreate()

df = spark.read.parquet(
    "storage/parquet/telemetry_dynamic_batches"
)

unmatched = df.filter(
    col("grid_match_status") == "no_valid_grid_observation"
)

print("\n========== UNMATCHED SUMMARY ==========")
print("Total unmatched:", unmatched.count())

print("\nCounts by batch:")
unmatched.groupBy("batch_id") \
    .agg(count("*").alias("records")) \
    .orderBy("batch_id") \
    .show(100, truncate=False)

print("\nTimestamp range:")
unmatched.select(
    spark_min("event_timestamp").alias("earliest"),
    spark_max("event_timestamp").alias("latest")
).show(truncate=False)

print("\nTimestamp range by batch:")
unmatched.groupBy("batch_id").agg(
    count("*").alias("records"),
    spark_min("event_timestamp").alias("earliest"),
    spark_max("event_timestamp").alias("latest")
).orderBy("batch_id").show(100, truncate=False)

spark.stop()
