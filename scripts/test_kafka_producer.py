"""
Test Kafka Producer
Publishes a sample JSON message to the butterfly-observations topic.
Usage: python scripts/test_kafka_producer.py
"""

import json
import os
import sys
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def create_producer(broker: str) -> KafkaProducer:
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def main():
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "butterfly-observations")

    sample_message = {
        "id": 999999,
        "observed_on": "2025-06-15",
        "latitude": 45.72,
        "longitude": 25.66,
        "county": "Brasov",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
    }

    print(f"Connecting to Kafka broker: {broker}")
    try:
        producer = create_producer(broker)
    except NoBrokersAvailable:
        print(f"ERROR: Could not connect to Kafka at {broker}")
        sys.exit(1)

    print(f"Publishing test message to topic '{topic}'...")
    future = producer.send(
        topic,
        key=sample_message["county"],
        value=sample_message,
    )
    result = future.get(timeout=10)
    print(f"Message sent successfully!")
    print(f"  Topic: {result.topic}")
    print(f"  Partition: {result.partition}")
    print(f"  Offset: {result.offset}")
    print(f"  Message: {json.dumps(sample_message, indent=2)}")

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
