from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    min as spark_min,
    max as spark_max,
    abs as spark_abs
)

spark = (
    SparkSession.builder
    .appName("CarbonHive-Nov13-Reconciliation-Validation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

PATH = "storage/parquet/telemetry_reconciled_nov13"

df = spark.read.parquet(PATH)

total = df.count()
unique_ids = df.select("event_id").distinct().count()

print("\n========== NOV 13 RECONCILIATION VALIDATION ==========")

print("Total records:", total)
print("Unique event IDs:", unique_ids)
print("Duplicate event IDs:", total - unique_ids)

print("\nGrid match status:")
df.groupBy("grid_match_status").count().show()

print("Grid zones:")
df.groupBy("grid_zone").count().show()

print("Grid observations:")
df.groupBy("grid_observation_timestamp").count().show()

print("Reconciliation types:")
df.groupBy("reconciliation_type").count().show()

print("\nCritical null checks:")

critical_columns = [
    "event_id",
    "electricity_kwh",
    "carbon_emissions_kg",
    "carbon_intensity_kgco2e_per_kwh",
    "grid_observation_timestamp",
    "grid_retrieved_at",
    "grid_zone",
    "grid_factor_age_seconds"
]

for name in critical_columns:
    null_count = df.filter(col(name).isNull()).count()
    print(f"{name}: {null_count}")

incorrect_calculations = df.filter(
    spark_abs(
        col("carbon_emissions_kg") -
        (
            col("electricity_kwh") *
            col("carbon_intensity_kgco2e_per_kwh")
        )
    ) > 0.000001
).count()

print("\nIncorrect carbon calculations:", incorrect_calculations)

invalid_age = df.filter(
    (col("grid_factor_age_seconds") < 0) |
    (col("grid_factor_age_seconds") > 900)
).count()

print("Invalid grid factor ages:", invalid_age)

print("\nGrid factor age range:")

df.select(
    spark_min("grid_factor_age_seconds").alias("minimum_age_seconds"),
    spark_max("grid_factor_age_seconds").alias("maximum_age_seconds")
).show()

print("\nFinal record count:", total)

spark.stop()