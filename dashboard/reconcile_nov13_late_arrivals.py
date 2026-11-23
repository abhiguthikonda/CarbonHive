from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    row_number,
    current_timestamp,
    to_timestamp
)
from pyspark.sql.window import Window

TELEMETRY_PATH = "storage/parquet/telemetry_dynamic_batches"
GRID_PATH = "storage/parquet/grid_carbon_history"
OUTPUT_PATH = "storage/parquet/telemetry_reconciled_nov13"

spark = (
    SparkSession.builder
    .appName("CarbonHive-Nov13-Late-Arrival-Reconciliation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 1. Read original telemetry and grid history
# --------------------------------------------------

telemetry = spark.read.parquet(TELEMETRY_PATH)

grid = spark.read.parquet(GRID_PATH)

unmatched = telemetry.filter(
    col("grid_match_status") == "no_valid_grid_observation"
).alias("t")

observations = grid.filter(
    (col("zone") == "IN-SO") &
    col("observation_timestamp").isNotNull() &
    col("carbon_intensity_kgco2e_per_kwh").isNotNull()
).alias("g")

# --------------------------------------------------
# 2. Match events to their eligible grid observation
# --------------------------------------------------

candidates = unmatched.join(
    observations,
    expr("""
        t.event_timestamp >=
        to_timestamp(g.observation_timestamp)
        AND t.event_timestamp <=
        to_timestamp(g.observation_timestamp) + INTERVAL 15 MINUTES
    """),
    "inner"
)

# Select the latest preceding eligible observation
window = Window.partitionBy(
    col("t.event_id")
).orderBy(
    to_timestamp(col("g.observation_timestamp")).desc()
)

matched = (
    candidates
    .withColumn("_rank", row_number().over(window))
    .filter(col("_rank") == 1)
    .select(
        "t.*",
        col("g.zone").alias("_grid_zone"),
        col("g.observation_timestamp").alias("_observation_timestamp"),
        col("g.carbon_intensity_gco2e_per_kwh").alias("_grid_intensity_g"),
        col("g.carbon_intensity_kgco2e_per_kwh").alias("_grid_intensity_kg"),
        col("g.source").alias("_grid_source"),
        col("g.is_estimated").alias("_grid_is_estimated"),
        col("g.estimation_method").alias("_grid_estimation_method"),
        col("g.emission_factor_type").alias("_grid_emission_factor_type"),
        col("g.flow_traced").alias("_grid_flow_traced"),
        col("g.temporal_granularity").alias("_grid_temporal_granularity"),
        col("g.retrieved_at").alias("_grid_retrieved_at")
    )
)

# --------------------------------------------------
# 3. Recalculate carbon accounting and add metadata
# --------------------------------------------------

reconciled = (
    matched
    .withColumn(
        "original_grid_match_status",
        col("grid_match_status")
    )
    .withColumn(
        "grid_match_status",
        expr("'matched'")
    )
    .withColumn("grid_zone", col("_grid_zone"))
    .withColumn(
        "grid_observation_timestamp",
        col("_observation_timestamp")
    )
    .withColumn(
        "carbon_intensity_gco2e_per_kwh",
        col("_grid_intensity_g")
    )
    .withColumn(
        "carbon_intensity_kgco2e_per_kwh",
        col("_grid_intensity_kg")
    )
    .withColumn(
        "carbon_emissions_kg",
        col("electricity_kwh") * col("_grid_intensity_kg")
    )
    .withColumn(
        "grid_factor_age_seconds",
        col("event_timestamp").cast("long") -
        to_timestamp(col("_observation_timestamp")).cast("long")
    )
    .withColumn("carbon_factor_source", col("_grid_source"))
    .withColumn("grid_is_estimated", col("_grid_is_estimated"))
    .withColumn(
        "grid_estimation_method",
        col("_grid_estimation_method")
    )
    .withColumn(
        "grid_emission_factor_type",
        col("_grid_emission_factor_type")
    )
    .withColumn("grid_flow_traced", col("_grid_flow_traced"))
    .withColumn(
        "grid_temporal_granularity",
        col("_grid_temporal_granularity")
    )
    .withColumn("grid_retrieved_at", col("_grid_retrieved_at"))
    .withColumn(
        "reconciliation_type",
        expr("'late_arriving_grid_observation'")
    )
    .withColumn("reconciled_at", current_timestamp())
)

# Remove temporary join columns
temporary_columns = [
    "_grid_zone",
    "_observation_timestamp",
    "_grid_intensity_g",
    "_grid_intensity_kg",
    "_grid_source",
    "_grid_is_estimated",
    "_grid_estimation_method",
    "_grid_emission_factor_type",
    "_grid_flow_traced",
    "_grid_temporal_granularity",
    "_grid_retrieved_at"
]

for name in temporary_columns:
    reconciled = reconciled.drop(name)

# --------------------------------------------------
# 4. Safety checks before writing
# --------------------------------------------------

unmatched_count = unmatched.count()
reconciled_count = reconciled.count()
unique_count = reconciled.select("event_id").distinct().count()

print("\n========== RECONCILIATION CHECK ==========")
print("Original unmatched events:", unmatched_count)
print("Reconciled events:", reconciled_count)
print("Unique event IDs:", unique_count)

if reconciled_count > unmatched_count:
    raise RuntimeError(
        "Reconciled count exceeds unmatched count. "
        "Output was not written."
    )

if reconciled_count == 0:
    raise RuntimeError(
        "No eligible records found. Output was not written."
    )

print(
    "Unmatched events not eligible for reconciliation:",
    unmatched_count - reconciled_count
)

if unique_count != reconciled_count:
    raise RuntimeError(
        "Duplicate event IDs detected. Output was not written."
    )

invalid = reconciled.filter(
    (col("grid_match_status") != "matched") |
    col("grid_zone").isNull() |
    col("grid_observation_timestamp").isNull() |
    col("carbon_emissions_kg").isNull() |
    (col("grid_factor_age_seconds") < 0) |
    (col("grid_factor_age_seconds") > 900)
).count()

if invalid:
    raise RuntimeError(
        f"{invalid} invalid records detected. Output was not written."
    )

# --------------------------------------------------
# 5. Write to a NEW directory
# --------------------------------------------------

print("\nAll safety checks passed.")
print("Writing reconciled Parquet data...")

reconciled.write \
    .mode("errorifexists") \
    .parquet(OUTPUT_PATH)

print("\nRECONCILIATION COMPLETED")
print("Output:", OUTPUT_PATH)
print("Records written:", reconciled_count)

spark.stop()