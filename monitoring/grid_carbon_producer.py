import json
import logging
import os
import time

from kafka import KafkaProducer
from monitoring.grid_carbon_collector import fetch_grid_carbon_intensity


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "grid-carbon-intensity"
POLL_INTERVAL_SECONDS = 300

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5
    )


def main():
    producer = create_producer()

    logger.info("Connected to Kafka")
    logger.info("Publishing to topic: %s", KAFKA_TOPIC)

    try:
        while True:
            try:
                grid_data = fetch_grid_carbon_intensity()

                if not isinstance(grid_data, dict):
                    raise TypeError("Collector must return a dictionary")

                grid_data["kafka_ingested_at"] = (
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                )

                zone = grid_data.get("zone") or os.getenv(
                    "ELECTRICITY_MAPS_ZONE", "IN-SO"
                )

                future = producer.send(
                    KAFKA_TOPIC,
                    key=zone,
                    value=grid_data
                )

                metadata = future.get(timeout=30)

                logger.info(
                    "Published | zone=%s | partition=%s | offset=%s",
                    zone,
                    metadata.partition,
                    metadata.offset
                )

            except Exception:
                logger.exception("Grid collection or publishing failed")

            logger.info("Next collection in %s seconds", POLL_INTERVAL_SECONDS)
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Stopping grid carbon producer")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
