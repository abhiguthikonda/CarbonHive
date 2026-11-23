from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CarbonHive-Parquet-Validation")
    .getOrCreate()
)

df = spark.read.parquet(
    "/opt/spark-apps/storage/parquet/telemetry"
)

print("\n========== PARQUET SCHEMA ==========")
df.printSchema()

print("\n========== TOTAL STORED RECORDS ==========")
print("Total records:", df.count())

print("\n========== SAMPLE CARBON ANALYTICS ==========")
df.select(
    "event_id",
    "electricity_kwh",
    "carbon_emissions_kg",
    "calculated_carbon_intensity_kg_per_unit",
    "detected_anomaly",
    "detected_anomaly_type"
).show(10, truncate=False)

print("\n========== ANOMALY SUMMARY ==========")
df.groupBy(
    "detected_anomaly",
    "detected_anomaly_type"
).count().show(truncate=False)

spark.stop()
