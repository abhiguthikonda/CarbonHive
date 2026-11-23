import os
import glob
import time
import logging
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    lit,
    expr,
    when,
    row_number,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)
from pyspark.sql.window import Window


# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

INDUSTRIAL_TOPIC = "verified-industrial-telemetry"
GRID_TOPIC = "grid-carbon-intensity"

GRID_ZONE = "IN-SO"
MAX_GRID_AGE_MINUTES = 15

GRID_HISTORY_PATH = (
    "/opt/spark-apps/storage/parquet/grid_carbon_history"
)

OUTPUT_PATH = (
    "/opt/spark-apps/storage/parquet/telemetry_dynamic"
)

BATCH_OUTPUT_PATH = (
    "/opt/spark-apps/storage/parquet/telemetry_dynamic_batches"
)

GRID_CHECKPOINT = (
    "/opt/spark-apps/storage/checkpoints/carbon_grid_history"
)

INDUSTRIAL_CHECKPOINT = (
    "/opt/spark-apps/storage/checkpoints/carbon_dynamic"
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("CarbonHive-Dynamic")


# ---------------------------------------------------------
# INDUSTRIAL TELEMETRY SCHEMA
# ---------------------------------------------------------

industrial_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("sequence_number", LongType(), True),
    StructField("timestamp", StringType(), True),
    StructField("facility_id", StringType(), True),
    StructField("equipment_id", StringType(), True),
    StructField("electricity_kwh", DoubleType(), True),
    StructField("fuel_m3", DoubleType(), True),
    StructField("production_units", DoubleType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("pressure_bar", DoubleType(), True),
    StructField("facility_electricity_co2e_kg", DoubleType(), True),
    StructField("carbon_intensity_kg_per_unit", DoubleType(), True),
    StructField("is_synthetic", BooleanType(), True),
    StructField("ground_truth_anomaly", BooleanType(), True),
    StructField("anomaly_type", StringType(), True),
    StructField("hmac_sha256", StringType(), True),
])


# ---------------------------------------------------------
# GRID CARBON SCHEMA
# ---------------------------------------------------------

grid_schema = StructType([
    StructField("source", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("carbon_intensity_gco2e_per_kwh", DoubleType(), True),
    StructField("carbon_intensity_kgco2e_per_kwh", DoubleType(), True),
    StructField("observation_timestamp", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("retrieved_at", StringType(), True),
    StructField("emission_factor_type", StringType(), True),
    StructField("flow_traced", BooleanType(), True),
    StructField("is_estimated", BooleanType(), True),
    StructField("estimation_method", StringType(), True),
    StructField("temporal_granularity", StringType(), True),
    StructField("kafka_ingested_at", StringType(), True),
])


# ---------------------------------------------------------
# GRID STREAM
# ---------------------------------------------------------

def start_grid_history_stream(spark):

    grid_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", GRID_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    grid_parsed = (
        grid_kafka
        .select(
            from_json(
                col("value").cast("string"),
                grid_schema
            ).alias("grid")
        )
        .select("grid.*")
        .filter(
            col("zone").isNotNull()
            & col("observation_timestamp").isNotNull()
            & col("carbon_intensity_kgco2e_per_kwh").isNotNull()
        )
        .filter(col("zone") == GRID_ZONE)
        .withColumn(
            "grid_observation_timestamp",
            to_timestamp(col("observation_timestamp"))
        )
        .filter(col("grid_observation_timestamp").isNotNull())
        .withWatermark(
            "grid_observation_timestamp",
            "1 day"
        )
        .dropDuplicates([
            "zone",
            "observation_timestamp"
        ])
    )

    query = (
        grid_parsed.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", GRID_HISTORY_PATH)
        .option("checkpointLocation", GRID_CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


# ---------------------------------------------------------
# INDUSTRIAL MICRO-BATCH PROCESSING
# ---------------------------------------------------------

def process_industrial_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    batch_started = time.perf_counter()

    if not glob.glob(
        os.path.join(GRID_HISTORY_PATH, "*.parquet")
    ):
        logger.warning(
            "Grid history is not available. Batch %s skipped.",
            batch_id
        )
        return

    grid_history = (
        batch_df.sparkSession.read
        .parquet(GRID_HISTORY_PATH)
        .filter(col("zone") == GRID_ZONE)
        .withColumn(
            "grid_observation_timestamp",
            to_timestamp(col("observation_timestamp"))
        )
        .dropDuplicates([
            "zone",
            "observation_timestamp"
        ])
        .select(
            col("zone").alias("grid_zone"),
            col("grid_observation_timestamp"),
            col("carbon_intensity_gco2e_per_kwh"),
            col("carbon_intensity_kgco2e_per_kwh"),
            col("source").alias("grid_source"),
            col("is_estimated").alias("grid_is_estimated"),
            col("estimation_method").alias("grid_estimation_method"),
            col("emission_factor_type").alias("grid_emission_factor_type"),
            col("flow_traced").alias("grid_flow_traced"),
            col("temporal_granularity").alias("grid_temporal_granularity"),
            col("retrieved_at").alias("grid_retrieved_at"),
        )
    )

    industrial = (
    batch_df
    .withColumn(
        "event_timestamp",
        to_timestamp(col("timestamp"))
    )
    .withColumn("event_grid_zone", lit(GRID_ZONE))
)

    # Match only grid observations at or before the event,
    # and no older than the configured freshness window.
    candidates = industrial.join(
        grid_history,
        (
            (industrial.event_grid_zone == grid_history.grid_zone)
            & (
                grid_history.grid_observation_timestamp
                <= industrial.event_timestamp
            )
            & (
                grid_history.grid_observation_timestamp
                >= industrial.event_timestamp
                - expr(f"INTERVAL {MAX_GRID_AGE_MINUTES} MINUTES")
            )
        ),
        "left"
    )

    # Select the latest valid preceding grid observation.
    ranking = Window.partitionBy(
        "event_id"
    ).orderBy(
        col("grid_observation_timestamp").desc_nulls_last()
    )

    matched = (
        candidates
        .withColumn("_match_rank", row_number().over(ranking))
        .filter(col("_match_rank") == 1)
        .drop("_match_rank")
    )

    enriched = (
        matched
        .withColumn(
            "grid_match_status",
            when(
                col("grid_observation_timestamp").isNotNull(),
                lit("matched")
            ).otherwise(lit("no_valid_grid_observation"))
        )
        .withColumn(
            "grid_factor_age_seconds",
            when(
                col("grid_observation_timestamp").isNotNull(),
                col("event_timestamp").cast("long")
                - col("grid_observation_timestamp").cast("long")
            ).otherwise(lit(None).cast("long"))
        )
        .withColumn(
            "carbon_emissions_kg",
            when(
                col("grid_match_status") == "matched",
                col("electricity_kwh")
                * col("carbon_intensity_kgco2e_per_kwh")
            ).otherwise(lit(None).cast("double"))
        )
        .withColumn(
            "calculated_carbon_intensity_kg_per_unit",
            when(
                (col("production_units") > 0)
                & (col("grid_match_status") == "matched"),
                col("carbon_emissions_kg")
                / col("production_units")
            ).otherwise(lit(None).cast("double"))
        )
        .withColumn(
            "detected_anomaly",
            (
                (col("electricity_kwh") > 25.0)
                | (col("temperature_c") > 95.0)
                | (col("production_units") < 10.0)
            )
        )
        .withColumn(
            "detected_anomaly_type",
            when(
                col("electricity_kwh") > 25.0,
                lit("high_electricity")
            )
            .when(
                col("temperature_c") > 95.0,
                lit("high_temperature")
            )
            .when(
                col("production_units") < 10.0,
                lit("low_production")
            )
            .otherwise(lit(None).cast("string"))
        )
        .withColumn(
            "carbon_factor_source",
            col("grid_source")
        )
        .withColumn(
            "carbon_factor_zone",
            col("grid_zone")
        )
    )

    output = enriched.select(
        "event_id",
        "sequence_number",
        "timestamp",
        "event_timestamp",
        "facility_id",
        "equipment_id",
        "electricity_kwh",
        "fuel_m3",
        "production_units",
        "temperature_c",
        "pressure_bar",
        "carbon_emissions_kg",
        "calculated_carbon_intensity_kg_per_unit",
        "detected_anomaly",
        "detected_anomaly_type",
        "ground_truth_anomaly",
        "anomaly_type",
        "is_synthetic",
        "grid_match_status",
        "grid_zone",
        "carbon_intensity_gco2e_per_kwh",
        "carbon_intensity_kgco2e_per_kwh",
        "grid_observation_timestamp",
        "grid_factor_age_seconds",
        "carbon_factor_source",
        "grid_is_estimated",
        "grid_estimation_method",
        "grid_emission_factor_type",
        "grid_flow_traced",
        "grid_temporal_granularity",
        "grid_retrieved_at",
    )

    cached_output = output.persist()

    try:
        status_rows = (
            cached_output
            .groupBy("grid_match_status")
            .agg(
                expr("count(*) AS count"),
                expr(
                    "min(event_timestamp) AS oldest_event_timestamp"
                ),
                expr(
                    "max(event_timestamp) AS newest_event_timestamp"
                )
            )
            .collect()
        )

        status_counts = {
            row["grid_match_status"]: row["count"]
            for row in status_rows
        }

        oldest_event_timestamp = min(
            row["oldest_event_timestamp"]
            for row in status_rows
            if row["oldest_event_timestamp"] is not None
        )

        newest_event_timestamp = max(
            row["newest_event_timestamp"]
            for row in status_rows
            if row["newest_event_timestamp"] is not None
        )

        matched_count = status_counts.get("matched", 0)
        unmatched_count = status_counts.get(
            "no_valid_grid_observation", 0
        )
        total_count = sum(status_counts.values())

        batch_output_path = (
            f"{BATCH_OUTPUT_PATH}/batch_id={batch_id}"
        )

        cached_output.write \
            .mode("overwrite") \
            .parquet(batch_output_path)

        logger.info(
            "Batch %s | total=%s | matched=%s | unmatched=%s",
            batch_id,
            total_count,
            matched_count,
            unmatched_count
        )

        # Record pipeline throughput after successful Parquet write.
        processing_seconds = time.perf_counter() - batch_started
        metrics_recorded_at = datetime.now(timezone.utc)

        oldest_event_utc = oldest_event_timestamp.replace(
            tzinfo=timezone.utc
        )

        newest_event_utc = newest_event_timestamp.replace(
            tzinfo=timezone.utc
        )

        oldest_event_latency_seconds = max(
            0.0,
            (metrics_recorded_at - oldest_event_utc).total_seconds()
        )

        newest_event_latency_seconds = max(
            0.0,
            (metrics_recorded_at - newest_event_utc).total_seconds()
        )

        throughput = (
            total_count / processing_seconds
            if processing_seconds > 0
            else 0.0
        )

        pipeline_metrics = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "batch_id": int(batch_id),
            "processed_rows": int(total_count),
            "matched_rows": int(matched_count),
            "unmatched_rows": int(unmatched_count),
            "processing_seconds": round(processing_seconds, 4),
            "throughput_rows_per_second": round(throughput, 2),
                        "oldest_event_latency_seconds": round(
                oldest_event_latency_seconds, 2
            ),
            "newest_event_latency_seconds": round(
                newest_event_latency_seconds, 2
            ),
            "metrics_recorded_at": metrics_recorded_at.isoformat(),
        }

        metrics_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "results",
            "pipeline_metrics.jsonl"
        )

        try:
            os.makedirs(os.path.dirname(metrics_path), exist_ok=True)

            with open(metrics_path, "a", encoding="utf-8") as metrics_file:
                metrics_file.write(
                    json.dumps(pipeline_metrics) + "\n"
                )

            logger.info(
                "Pipeline throughput | batch=%s | rows/sec=%.2f",
                batch_id,
                throughput
            )

        except Exception:
            logger.exception(
                "Could not save pipeline metrics for batch %s",
                batch_id
            )
    finally:
        cached_output.unpersist()

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():

    spark = (
        SparkSession.builder
        .appName("CarbonHive-Dynamic-Carbon-Analytics")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("==============================================")
    print("CARBONHIVE DYNAMIC CARBON STREAMING")
    print("==============================================")
    print(f"Industrial topic: {INDUSTRIAL_TOPIC}")
    print(f"Grid topic: {GRID_TOPIC}")
    print(f"Grid zone: {GRID_ZONE}")
    print(f"Maximum grid age: {MAX_GRID_AGE_MINUTES} minutes")
    print(f"Grid history: {GRID_HISTORY_PATH}")
    print(f"Dynamic output: {OUTPUT_PATH}")

    # Start grid ingestion first.
    grid_query = start_grid_history_stream(spark)

    print("Grid history streaming started.")

    # Wait for at least one grid observation to be persisted.
    print("Waiting for initial grid history...")

    grid_files = []

    for attempt in range(60):
        grid_files = glob.glob(
            os.path.join(GRID_HISTORY_PATH, "*.parquet")
        )

        if grid_files:
            break

        time.sleep(2)

    if not grid_files:
        grid_query.stop()
        raise RuntimeError(
            "No grid observations were persisted within 120 seconds. "
            "Check the grid producer and Kafka topic."
        )

    print("Initial grid history is available.")

    industrial_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", INDUSTRIAL_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    industrial_parsed = (
        industrial_kafka
        .select(
            from_json(
                col("value").cast("string"),
                industrial_schema
            ).alias("event")
        )
        .select("event.*")
        .filter(
            col("event_id").isNotNull()
            & col("timestamp").isNotNull()
            & col("electricity_kwh").isNotNull()
            & col("production_units").isNotNull()
        )
    )

    industrial_query = (
        industrial_parsed.writeStream
        .foreachBatch(process_industrial_batch)
        .option(
            "checkpointLocation",
            INDUSTRIAL_CHECKPOINT
        )
        .trigger(processingTime="20 seconds")
        .start()
    )

    print("Industrial streaming started.")
    print("Dynamic carbon accounting is active.")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()