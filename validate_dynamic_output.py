from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CarbonHive-Validation")
    .master("local[*]")
    .getOrCreate()
)

path = "/opt/spark-apps/storage/parquet/telemetry_dynamic_batches"

df = spark.read.parquet(path)

print("\n========== SCHEMA ==========")
df.printSchema()

print("\n========== RECORD COUNT ==========")
print(df.count())

print("\n========== SAMPLE RECORDS ==========")
df.show(5, truncate=False)

spark.stop()
