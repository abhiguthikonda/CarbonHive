from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "verified-industrial-telemetry"

# Illustrative factor only. Replace with an authoritative,
# region-appropriate factor before making real-world claims.
ELECTRICITY_EMISSION_FACTOR = 0.70

event_schema = StructType([
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


def main():
    spark = (
        SparkSession.builder
        .appName("CarbonHive-Carbon-Analytics")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("CarbonHive analytics streaming started.")
    print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(
        "Illustrative electricity factor: "
        f"{ELECTRICITY_EMISSION_FACTOR} kg CO2e/kWh"
    )

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        kafka_stream
        .select(
            from_json(
                col("value").cast("string"),
                event_schema
            ).alias("event")
        )
        .select("event.*")
        .filter(
            col("event_id").isNotNull()
            & col("electricity_kwh").isNotNull()
            & col("production_units").isNotNull()
            & col("temperature_c").isNotNull()
        )
    )

    # Independent anomaly detection using simulator operating ranges.
    detected_stream = (
        parsed_stream
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
    )

    # Recalculate electricity-related emissions and carbon intensity.
    enriched_stream = (
        detected_stream
        .withColumn(
            "carbon_emissions_kg",
            col("electricity_kwh") * lit(ELECTRICITY_EMISSION_FACTOR)
        )
        .withColumn(
            "calculated_carbon_intensity_kg_per_unit",
            when(
                col("production_units") > 0,
                col("electricity_kwh")
                * lit(ELECTRICITY_EMISSION_FACTOR)
                / col("production_units")
            ).otherwise(lit(None).cast("double"))
        )
    )

    output_stream = enriched_stream.select(
        "event_id",
        "sequence_number",
        "timestamp",
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
    )

    # Durable historical output.
    parquet_query = (
        output_stream.writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            "/opt/spark-apps/storage/parquet/telemetry"
        )
        .option(
            "checkpointLocation",
            "/opt/spark-apps/storage/checkpoints/carbon_parquet"
        )
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Live console output for demonstration and debugging.
    console_query = (
        output_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option(
            "checkpointLocation",
            "/opt/spark-apps/storage/checkpoints/carbon_console"
        )
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("Parquet output: storage/parquet/telemetry")
    print("Carbon analytics and anomaly detection are active.")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
