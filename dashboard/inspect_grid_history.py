from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("CarbonHive-Grid-History-Audit").getOrCreate()

path = "storage/parquet/grid_carbon_history"

df = spark.read.parquet(path)

print("\n========== GRID HISTORY AUDIT ==========")
print("Total records:", df.count())

print("\nLatest observations:")
df.select(
    "zone",
    "observation_timestamp",
    "carbon_intensity_kgco2e_per_kwh",
    "retrieved_at"
).orderBy(col("observation_timestamp").desc()).show(20, truncate=False)

print("\nRecords grouped by observation timestamp:")
df.groupBy("observation_timestamp").agg(
    count("*").alias("records")
).orderBy(col("observation_timestamp").desc()).show(20, truncate=False)

spark.stop()
