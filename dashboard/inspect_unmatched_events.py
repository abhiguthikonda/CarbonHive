from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, min as spark_min, max as spark_max
)

spark = SparkSession.builder \
    .appName("CarbonHive-Unmatched-Event-Audit") \
    .getOrCreate()

path = "storage/parquet/telemetry_dynamic_batches"

df = spark.read.parquet(path)

unmatched = df.filter(
    col("grid_match_status") != "matched"
)

print("\n========== UNMATCHED EVENT AUDIT ==========")

print("Total unmatched:", unmatched.count())

print("\nUnmatched records by batch:")
unmatched.groupBy("batch_id").count() \
    .orderBy("batch_id").show(100, truncate=False)

print("\nEvent timestamp range:")
unmatched.select(
    spark_min("event_timestamp").alias("earliest_event"),
    spark_max("event_timestamp").alias("latest_event")
).show(truncate=False)

print("\nSample unmatched records:")
unmatched.select(
    "event_id",
    "batch_id",
    "event_timestamp",
    "equipment_id",
    "grid_match_status",
    "grid_zone",
    "carbon_emissions_kg"
).orderBy("event_timestamp").show(20, truncate=False)

spark.stop()
