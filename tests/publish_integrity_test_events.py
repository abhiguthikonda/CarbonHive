import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

from producer.simulator import generate_event
from integrity.hmac_verifier import sign_event


load_dotenv(override=True)

secret = os.getenv("CARBONHIVE_HMAC_SECRET")

if not secret:
    raise RuntimeError("HMAC secret is missing.")

topic = os.getenv(
    "KAFKA_TOPIC",
    "industrial-telemetry",
)

bootstrap_servers = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8"),
    acks="all",
)

try:
    # Test 1: Authentic signed event
    valid_event = generate_event(900001)
    valid_event["integrity_test_case"] = "VALID"
    valid_event = sign_event(valid_event, secret)

    # Test 2: Sign first, then modify the payload
    tampered_event = generate_event(900002)
    tampered_event["integrity_test_case"] = "TAMPERED"
    tampered_event = sign_event(tampered_event, secret)
    tampered_event["electricity_kwh"] += 999

    # Test 3: Event without an HMAC signature
    unsigned_event = generate_event(900003)
    unsigned_event["integrity_test_case"] = "UNSIGNED"

    test_events = [
        valid_event,
        tampered_event,
        unsigned_event,
    ]

    for event in test_events:
        metadata = producer.send(
            topic,
            key=event["equipment_id"],
            value=event,
        ).get(timeout=15)

        print(
            f"Published test={event['integrity_test_case']} "
            f"partition={metadata.partition} "
            f"offset={metadata.offset}"
        )

    producer.flush()
    print("\nALL THREE TEST EVENTS PUBLISHED.")

finally:
    producer.close()
