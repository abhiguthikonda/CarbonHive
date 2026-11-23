from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    when, abs as spark_abs, max as spark_max
)

spark = (
    SparkSession.builder
    .appName("CarbonHive-Integrity-Audit")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(
    "/opt/spark-apps/storage/parquet/telemetry_dynamic_batches"
)

total = df.count()

print("\n========== CARBONHIVE DATA INTEGRITY AUDIT ==========")
print("Total records:", total)

print("\n1. EVENT ID UNIQUENESS")
unique_ids = df.select("event_id").distinct().count()
print("Unique event IDs:", unique_ids)
print("Duplicate event IDs:", total - unique_ids)

print("\n2. NULL VALUE CHECK")
critical_columns = [
    "event_id",
    "electricity_kwh",
    "carbon_emissions_kg",
    "grid_match_status",
    "carbon_intensity_kgco2e_per_kwh",
    "grid_observation_timestamp",
    "batch_id"
]

for name in critical_columns:
    null_count = df.filter(col(name).isNull()).count()
    print(f"{name}: {null_count} nulls")

print("\n3. GRID MATCH STATUS")
df.groupBy("grid_match_status").count().show()

print("\n4. GRID FACTOR AGE")
df.select(
    spark_max("grid_factor_age_seconds").alias("maximum_age_seconds")
).show()

print("\n5. CARBON CALCULATION CHECK")
checked = df.withColumn(
    "expected_emissions",
    col("electricity_kwh") *
    col("carbon_intensity_kgco2e_per_kwh")
)

incorrect = checked.filter(
    spark_abs(
        col("carbon_emissions_kg") - col("expected_emissions")
    ) > 0.000001
).count()

print("Incorrect carbon calculations:", incorrect)

print("\n6. NEGATIVE VALUES")
negative = df.filter(
    (col("electricity_kwh") < 0) |
    (col("carbon_emissions_kg") < 0) |
    (col("carbon_intensity_kgco2e_per_kwh") < 0)
).count()

print("Records with negative values:", negative)

print("\n7. BATCH DISTRIBUTION")
df.groupBy("batch_id").count().orderBy("batch_id").show()

print("\n========== AUDIT COMPLETED ==========")

spark.stop()
