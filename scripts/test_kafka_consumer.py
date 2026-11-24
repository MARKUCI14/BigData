"""
Test Kafka Consumer
Reads and prints messages from the butterfly-observations topic.
Usage: python scripts/test_kafka_consumer.py
"""

import json
import os
import sys

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


def main():
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "butterfly-observations")

    print(f"Connecting to Kafka broker: {broker}")
    print(f"Subscribing to topic: {topic}")
    print("Waiting for messages (Ctrl+C to stop)...\n")

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="test-consumer-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            consumer_timeout_ms=30000,  # Stop after 30s of no messages
        )
    except NoBrokersAvailable:
        print(f"ERROR: Could not connect to Kafka at {broker}")
        sys.exit(1)

    message_count = 0
    try:
        for message in consumer:
            message_count += 1
            print(f"--- Message #{message_count} ---")
            print(f"  Partition: {message.partition}")
            print(f"  Offset:    {message.offset}")
            print(f"  Key:       {message.key}")
            print(f"  Value:     {json.dumps(message.value, indent=2)}")
            print()
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        consumer.close()
        print(f"Total messages consumed: {message_count}")


if __name__ == "__main__":
    main()
