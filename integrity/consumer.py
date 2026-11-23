import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

from integrity.hmac_verifier import verify_event


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

load_dotenv(override=True)

BOOTSTRAP_SERVERS = "localhost:9092"

RAW_TOPIC = "industrial-telemetry"
VERIFIED_TOPIC = "verified-industrial-telemetry"

CONSUMER_GROUP = "carbonhive-integrity-group"

HMAC_SECRET = os.getenv("CARBONHIVE_HMAC_SECRET")

if not HMAC_SECRET:
    raise RuntimeError(
        "CARBONHIVE_HMAC_SECRET is missing from .env"
    )

AUDIT_FILE = Path("results/integrity_rejections.jsonl")
AUDIT_FILE.parent.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("carbonhive.integrity")


# --------------------------------------------------
# AUDIT LOGGING
# --------------------------------------------------

def record_rejection(message, reason, event=None):

    event = event if isinstance(event, dict) else {}

    audit_record = {
        "audit_timestamp": __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat(),

        "reason": reason,
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,

        "event_id": event.get("event_id"),
        "equipment_id": event.get("equipment_id"),
        "sequence_number": event.get("sequence_number")
    }

    with AUDIT_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(audit_record) + "\n")


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():

    consumer = None
    producer = None

    verified_count = 0
    rejected_count = 0

    try:

        # Consume raw industrial events
        consumer = KafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
            max_poll_records=1
        )

        # Publish only verified events
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,

            value_serializer=lambda value: json.dumps(
                value,
                separators=(",", ":"),
                allow_nan=False
            ).encode("utf-8"),

            key_serializer=lambda key: key.encode("utf-8"),

            acks="all",
            retries=5
        )

        logger.info("Integrity consumer started.")
        logger.info("Reading from: %s", RAW_TOPIC)
        logger.info("Verified output: %s", VERIFIED_TOPIC)

        while True:

            records = consumer.poll(
                timeout_ms=1000,
                max_records=1
            )

            for topic_partition, messages in records.items():

                for message in messages:

                    event = None

                    try:

                        # Parse JSON
                        event = json.loads(
                            message.value.decode("utf-8")
                        )

                        if not isinstance(event, dict):
                            raise ValueError(
                                "Event payload must be a JSON object"
                            )

                        # Verify HMAC signature
                        if not verify_event(event, HMAC_SECRET):

                            record_rejection(
                                message,
                                "HMAC verification failed",
                                event
                            )

                            rejected_count += 1

                            logger.warning(
                                "REJECTED: HMAC verification failed | "
                                "partition=%s offset=%s",
                                message.partition,
                                message.offset
                            )

                            # Commit only after audit succeeds
                            consumer.commit()
                            continue

                        # Forward verified event
                        event_id = str(event.get("event_id", ""))

                        if not event_id:
                            raise ValueError(
                                "Verified event is missing event_id"
                            )

                        producer.send(
                            VERIFIED_TOPIC,
                            key=event_id,
                            value=event
                        ).get(timeout=10)

                        # Commit source offset only after forwarding
                        consumer.commit()

                        verified_count += 1

                        logger.info(
                            "VERIFIED AND FORWARDED: event=%s | "
                            "partition=%s offset=%s",
                            event_id,
                            message.partition,
                            message.offset
                        )

                    except (UnicodeDecodeError, json.JSONDecodeError,
                            ValueError) as error:

                        record_rejection(
                            message,
                            str(error),
                            event
                        )

                        rejected_count += 1

                        logger.warning(
                            "REJECTED: %s | partition=%s offset=%s",
                            error,
                            message.partition,
                            message.offset
                        )

                        consumer.commit()

        # Normally unreachable
    except KeyboardInterrupt:

        logger.info("Shutdown requested.")

    finally:

        if producer is not None:
            producer.flush()
            producer.close()

        if consumer is not None:
            consumer.close()

        logger.info("Integrity consumer stopped.")
        logger.info("Verified events: %s", verified_count)
        logger.info("Rejected events: %s", rejected_count)


if __name__ == "__main__":
    main()