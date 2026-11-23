from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CarbonHive-Grid-History-Audit") \
    .getOrCreate()

path = "storage/parquet/grid_carbon_history"

df = spark.read.parquet(path)

print("\n========== GRID OBSERVATION HISTORY ==========")

df.select(
    "zone",
    "carbon_intensity_kgco2e_per_kwh",
    "observation_timestamp",
    "retrieved_at",
    "is_estimated",
    "temporal_granularity"
).orderBy("observation_timestamp").show(
    100, truncate=False
)

print("Total grid history records:", df.count())

spark.stop()
