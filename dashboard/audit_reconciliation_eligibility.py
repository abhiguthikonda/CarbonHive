from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, row_number, count,
    min as spark_min,
    max as spark_max
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("CarbonHive-Reconciliation-Eligibility") \
    .getOrCreate()

telemetry = spark.read.parquet(
    "storage/parquet/telemetry_dynamic_batches"
)

grid = spark.read.parquet(
    "storage/parquet/grid_carbon_history"
)

unmatched = telemetry.filter(
    col("grid_match_status") == "no_valid_grid_observation"
).alias("t")

observations = grid.filter(
    (col("zone") == "IN-SO") &
    col("observation_timestamp").isNotNull() &
    col("carbon_intensity_kgco2e_per_kwh").isNotNull()
).alias("g")

candidates = unmatched.join(
    observations,
    expr("""
        t.event_timestamp >=
        to_timestamp(g.observation_timestamp)
        AND t.event_timestamp <=
        to_timestamp(g.observation_timestamp) + INTERVAL 15 MINUTES
    """),
    "left"
)

window = Window.partitionBy(
    col("t.event_id")
).orderBy(
    col("g.observation_timestamp").desc()
)

matched = candidates.withColumn(
    "rank", row_number().over(window)
).filter(
    col("rank") == 1
).withColumn(
    "factor_age_seconds",
    col("t.event_timestamp").cast("long") -
    col("g.observation_timestamp").cast("timestamp").cast("long")
)

print("\n========== RECONCILIATION ELIGIBILITY ==========")

print("Total unmatched events:", unmatched.count())

print("\nEligibility summary:")

matched.select(
    col("t.event_id").alias("event_id"),
    col("t.batch_id").alias("batch_id"),
    col("t.event_timestamp").alias("event_timestamp"),
    col("g.observation_timestamp").alias("observation_timestamp"),
    col("factor_age_seconds")
).groupBy(
    expr("""
        CASE
          WHEN observation_timestamp IS NOT NULL
           AND factor_age_seconds >= 0
           AND factor_age_seconds <= 900
          THEN 'eligible'
          ELSE 'ineligible'
        END
    """).alias("eligibility")
).count().show(truncate=False)

print("\nEligibility by batch:")

matched.withColumn(
    "eligibility",
    expr("""
        CASE
          WHEN observation_timestamp IS NOT NULL
           AND factor_age_seconds >= 0
           AND factor_age_seconds <= 900
          THEN 'eligible'
          ELSE 'ineligible'
        END
    """)
).groupBy(
    col("t.batch_id"), col("eligibility")
).count().orderBy(
    col("t.batch_id")
).show(100, truncate=False)

print("\nFactor age range:")
matched.select(
    spark_min("factor_age_seconds").alias("minimum_age_seconds"),
    spark_max("factor_age_seconds").alias("maximum_age_seconds")
).show(truncate=False)

spark.stop()
