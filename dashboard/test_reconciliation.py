from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    row_number,
    expr
)
from pyspark.sql.window import Window


DYNAMIC_PATH = (
    "storage/parquet/telemetry_dynamic"
)

BATCH_PATH = (
    "storage/parquet/telemetry_dynamic_batches"
)

GRID_PATH = (
    "storage/parquet/grid_carbon_history"
)


spark = (
    SparkSession.builder
    .appName("CarbonHive-Reconciliation-Test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n========== RECONCILIATION TEST ==========\n")

# Read both dynamic output locations separately.
dynamic_old = spark.read.parquet(DYNAMIC_PATH)
dynamic_batches = spark.read.parquet(BATCH_PATH)

print("\n========== DYNAMIC DIRECTORY ==========")
print("Records:", dynamic_old.count())
dynamic_old.groupBy("grid_match_status").count().show()

print("\n========== DYNAMIC BATCHES ==========")
print("Records:", dynamic_batches.count())
dynamic_batches.groupBy("grid_match_status").count().show()

# Combine both datasets for reconciliation inspection.
industrial = dynamic_old.unionByName(
    dynamic_batches,
    allowMissingColumns=True
)

print("\n========== COMBINED DATA ==========")
print("Total records:", industrial.count())
industrial.groupBy("grid_match_status").count().show()

# Select only previously unmatched records.
print("Total industrial records:", industrial.count())

print("\nGrid match status distribution:")
industrial.groupBy("grid_match_status").count().show(
    truncate=False
)

print("\nBatch IDs:")
industrial.select("batch_id").distinct().show(
    50,
    truncate=False
)

# Select only previously unmatched records.
unmatched = industrial.filter(
    col("grid_match_status")
    == "no_valid_grid_observation"
)

print("Previously unmatched events:", unmatched.count())

print("Previously unmatched events:", unmatched.count())

# Read existing grid-carbon history.
grid = (
    spark.read.parquet(GRID_PATH)
    .filter(col("zone") == "IN-SO")
    .dropDuplicates(
        ["zone", "observation_timestamp"]
    )
    .select(
        col("zone").alias("grid_zone"),
        col("grid_observation_timestamp"),
        col("carbon_intensity_kgco2e_per_kwh"),
        col("carbon_intensity_gco2e_per_kwh"),
        col("observation_timestamp"),
        col("retrieved_at"),
        col("is_estimated"),
        col("estimation_method"),
        col("emission_factor_type"),
        col("flow_traced"),
        col("temporal_granularity")
    )
)

# Join events with valid preceding grid observations.
candidates = (
    unmatched.alias("i")
    .join(
        grid.alias("g"),
        (
    (col("g.grid_zone") == "IN-SO")
    & (
        col("g.grid_observation_timestamp")
        <= col("i.event_timestamp")
    )
    & (
        col("g.grid_observation_timestamp")
        >= expr(
            "i.event_timestamp - INTERVAL 15 MINUTES"
        )
    )
),
        "inner"
    )
)

# Select the latest eligible observation per event.
window_spec = (
    Window
    .partitionBy(col("i.event_id"))
    .orderBy(
        col("g.grid_observation_timestamp").desc()
    )
)

matched = (
    candidates
    .withColumn(
        "match_rank",
        row_number().over(window_spec)
    )
    .filter(col("match_rank") == 1)
)
from pyspark.sql.functions import (
    lit,
    unix_timestamp,
    when
)

# Build corrected records while preserving the original
# industrial telemetry fields.
matched_output = (
    matched
    .select(
        "i.*",
        col("g.grid_zone").alias("_new_grid_zone"),
        col("g.grid_observation_timestamp").alias("_new_grid_timestamp"),
        col("g.carbon_intensity_kgco2e_per_kwh").alias("_new_intensity_kg"),
        col("g.carbon_intensity_gco2e_per_kwh").alias("_new_intensity_g"),
        col("g.retrieved_at").alias("_new_retrieved_at"),
        col("g.is_estimated").alias("_new_is_estimated"),
        col("g.estimation_method").alias("_new_estimation_method"),
        col("g.emission_factor_type").alias("_new_factor_type"),
        col("g.flow_traced").alias("_new_flow_traced"),
        col("g.temporal_granularity").alias("_new_granularity")
    )
    .withColumn("grid_match_status", lit("matched"))
    .withColumn("grid_zone", col("_new_grid_zone"))
    .withColumn("grid_observation_timestamp", col("_new_grid_timestamp"))
    .withColumn(
        "carbon_intensity_kgco2e_per_kwh",
        col("_new_intensity_kg")
    )
    .withColumn(
        "carbon_intensity_gco2e_per_kwh",
        col("_new_intensity_g")
    )
    .withColumn(
        "grid_factor_age_seconds",
        unix_timestamp("event_timestamp")
        - unix_timestamp("_new_grid_timestamp")
    )
    .withColumn(
        "carbon_factor_source",
        lit("Electricity Maps")
    )
    .withColumn("grid_is_estimated", col("_new_is_estimated"))
    .withColumn("grid_estimation_method", col("_new_estimation_method"))
    .withColumn("grid_emission_factor_type", col("_new_factor_type"))
    .withColumn("grid_flow_traced", col("_new_flow_traced"))
    .withColumn("grid_temporal_granularity", col("_new_granularity"))
    .withColumn("grid_retrieved_at", col("_new_retrieved_at"))
    .withColumn(
        "carbon_emissions_kg",
        col("electricity_kwh")
        * col("_new_intensity_kg")
    )
    .withColumn(
        "calculated_carbon_intensity_kg_per_unit",
        when(
            col("production_units") > 0,
            col("carbon_emissions_kg") / col("production_units")
        )
    )
    .drop(
        "_new_grid_zone",
        "_new_grid_timestamp",
        "_new_intensity_kg",
        "_new_intensity_g",
        "_new_retrieved_at",
        "_new_is_estimated",
        "_new_estimation_method",
        "_new_factor_type",
        "_new_flow_traced",
        "_new_granularity"
    )
    .withColumn("reconciliation_type", lit("late_arriving_grid_observation"))
)

# Write to a separate location.
RECONCILED_PATH = (
    "storage/parquet/telemetry_reconciled"
)

print("\n========== RECONCILIATION OUTPUT ==========")

print("Records prepared:", matched_output.count())

print("Output schema:")
matched_output.printSchema()

# errorifexists prevents accidental overwriting.
matched_output.write \
    .mode("errorifexists") \
    .parquet(RECONCILED_PATH)

print("Reconciled records saved to:", RECONCILED_PATH)

matched_count = matched.select(
    col("i.event_id")
).distinct().count()

print("Newly matchable events:", matched_count)

print(
    "Still unmatched:",
    unmatched.count() - matched_count
)

print("\n========== MATCHED SAMPLE ==========\n")

matched.select(
    col("i.event_id"),
    col("i.event_timestamp"),
    col("g.observation_timestamp"),
    col("g.carbon_intensity_kgco2e_per_kwh"),
    col("g.retrieved_at")
).show(10, truncate=False)

print("\n========== TEST COMPLETE ==========\n")

spark.stop()