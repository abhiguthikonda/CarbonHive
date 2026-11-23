from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    abs,
    max as spark_max
)

PATH = "storage/parquet/telemetry_reconciled"

spark = (
    SparkSession.builder
    .appName("CarbonHive-Reconciliation-Validation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(PATH)

total = df.count()
unique_ids = df.select("event_id").distinct().count()

print("\n========== RECONCILIATION VALIDATION ==========")

print("Total records:", total)
print("Unique event IDs:", unique_ids)
print("Duplicate event IDs:", total - unique_ids)

print("\nMatch status:")
df.groupBy("grid_match_status").count().show()

print("\nGrid zones:")
df.groupBy("grid_zone").count().show()

print("\nReconciliation types:")
df.groupBy("reconciliation_type").count().show()

print("\nNull checks:")
for field in [
    "event_id",
    "grid_zone",
    "grid_observation_timestamp",
    "carbon_intensity_kgco2e_per_kwh",
    "carbon_emissions_kg",
    "grid_retrieved_at"
]:
    print(field, "=", df.filter(col(field).isNull()).count())

incorrect = df.filter(
    abs(
        col("carbon_emissions_kg")
        - (
            col("electricity_kwh")
            * col("carbon_intensity_kgco2e_per_kwh")
        )
    ) > 0.000001
).count()

print("\nIncorrect emission calculations:", incorrect)

print("\nMaximum grid factor age:")
df.select(
    spark_max("grid_factor_age_seconds")
).show()

print("\n========== VALIDATION COMPLETE ==========")

spark.stop()