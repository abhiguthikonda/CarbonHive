import json
import logging
import os
import random
import time
import uuid

from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from integrity.hmac_verifier import sign_event


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

load_dotenv()
CARBONHIVE_HMAC_SECRET = os.getenv(
    "CARBONHIVE_HMAC_SECRET"
)

if not CARBONHIVE_HMAC_SECRET:
    raise RuntimeError(
        "CARBONHIVE_HMAC_SECRET is missing from .env"
    )

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "industrial-telemetry",
)

EVENT_INTERVAL_SECONDS = float(
    os.getenv("EVENT_INTERVAL_SECONDS", "0.5")
)

ANOMALY_PROBABILITY = float(
    os.getenv("ANOMALY_PROBABILITY", "0.05")
)

FACILITY_ID = "factory-001"

EQUIPMENT_IDS = [
    "compressor-001",
    "boiler-001",
    "production-line-001",
]

ILLUSTRATIVE_ELECTRICITY_FACTOR = 0.70


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Synthetic telemetry generation
# ---------------------------------------------------------

def generate_event(sequence_number):
    """Generate one synthetic industrial telemetry event."""

    equipment_id = random.choice(EQUIPMENT_IDS)

    electricity_kwh = round(
        random.uniform(5.0, 25.0),
        3,
    )

    fuel_m3 = round(
        random.uniform(0.1, 5.0),
        3,
    )

    production_units = round(
        random.uniform(10.0, 100.0),
        2,
    )

    temperature_c = round(
        random.uniform(55.0, 95.0),
        2,
    )

    pressure_bar = round(
        random.uniform(2.0, 8.0),
        2,
    )

    ground_truth_anomaly = (
        random.random() < ANOMALY_PROBABILITY
    )

    anomaly_type = None

    if ground_truth_anomaly:
        anomaly_type = random.choice(
            [
                "high_electricity",
                "high_temperature",
                "low_production",
            ]
        )

        if anomaly_type == "high_electricity":
            electricity_kwh = round(
                random.uniform(35.0, 60.0),
                3,
            )

        elif anomaly_type == "high_temperature":
            temperature_c = round(
                random.uniform(110.0, 140.0),
                2,
            )

        elif anomaly_type == "low_production":
            production_units = round(
                random.uniform(1.0, 5.0),
                2,
            )

    electricity_co2e_kg = round(
        electricity_kwh * ILLUSTRATIVE_ELECTRICITY_FACTOR,
        4,
    )

    carbon_intensity = round(
        electricity_co2e_kg / production_units,
        6,
    )

    return {
        "event_id": str(uuid.uuid4()),
        "sequence_number": sequence_number,
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "facility_id": FACILITY_ID,
        "equipment_id": equipment_id,
        "electricity_kwh": electricity_kwh,
        "fuel_m3": fuel_m3,
        "production_units": production_units,
        "temperature_c": temperature_c,
        "pressure_bar": pressure_bar,
        "facility_electricity_co2e_kg": electricity_co2e_kg,
        "carbon_intensity_kg_per_unit": carbon_intensity,
        "is_synthetic": True,
        "ground_truth_anomaly": ground_truth_anomaly,
        "anomaly_type": anomaly_type,
    }


# ---------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------

def create_producer():
    """Create a Kafka producer with reliable acknowledgements."""

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(
            value,
            separators=(",", ":"),
        ).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000,
    )


# ---------------------------------------------------------
# Main streaming loop
# ---------------------------------------------------------

def main():
    """Continuously publish industrial telemetry to Kafka."""

    if EVENT_INTERVAL_SECONDS <= 0:
        raise ValueError(
            "EVENT_INTERVAL_SECONDS must be greater than zero."
        )

    if not 0 <= ANOMALY_PROBABILITY <= 1:
        raise ValueError(
            "ANOMALY_PROBABILITY must be between 0 and 1."
        )

    producer = None
    sequence_number = 0
    sent_count = 0
    failed_count = 0

    try:
        logger.info(
            "Connecting to Kafka at %s",
            KAFKA_BOOTSTRAP_SERVERS,
        )

        producer = create_producer()

        logger.info(
            "Connected. Publishing to topic '%s'.",
            KAFKA_TOPIC,
        )

        while True:
            sequence_number += 1

            event = generate_event(sequence_number)
            event = sign_event(
    event,
    CARBONHIVE_HMAC_SECRET,
)

            try:
                metadata = producer.send(
                    KAFKA_TOPIC,
                    key=event["equipment_id"],
                    value=event,
                ).get(timeout=35)

                sent_count += 1

                logger.info(
                    "Published event=%s sequence=%d "
                    "partition=%d offset=%d anomaly=%s",
                    event["event_id"],
                    event["sequence_number"],
                    metadata.partition,
                    metadata.offset,
                    event["ground_truth_anomaly"],
                )

            except (KafkaError, TimeoutError) as exc:
                failed_count += 1

                logger.error(
                    "Failed to publish sequence %d: %s",
                    sequence_number,
                    exc,
                )

            time.sleep(EVENT_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Shutdown requested.")

    except Exception:
        logger.exception(
            "Simulator stopped because of an error."
        )

    finally:
        if producer is not None:
            try:
                producer.flush(timeout=10)
            finally:
                producer.close(timeout=10)

        logger.info(
            "Simulator stopped. Published=%d Failed=%d",
            sent_count,
            failed_count,
        )


if __name__ == "__main__":
    main()
