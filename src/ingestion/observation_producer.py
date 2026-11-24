"""Replay or synthesize Clouded Apollo observations to Kafka."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import sys
import time
from datetime import date, datetime, timezone
from itertools import cycle
from pathlib import Path
from typing import Iterator

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "data" / "raw" / "Parnassius_obs_cleaned.csv"
COUNTY_COLUMN = "place_county_name"
REQUIRED_COLUMNS = {"id", "observed_on", "latitude", "longitude", COUNTY_COLUMN}
MAX_CONNECTION_ATTEMPTS = 5
RETRY_DELAY_SECONDS = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("replay", "synthetic"), default="replay")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between messages")
    parser.add_argument("--count", type=int, default=None, help="Number of messages (synthetic mode defaults to unlimited)")
    parser.add_argument("--kafka-broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "butterfly-observations"))
    parser.add_argument("--input", type=Path, default=DEFAULT_SOURCE, help="Historical observations CSV")
    args = parser.parse_args()
    if args.delay < 0:
        parser.error("--delay must be zero or greater")
    if args.count is not None and args.count < 1:
        parser.error("--count must be at least 1")
    return args


def utc_timestamp() -> str:
    """Return a UTC timestamp in the ISO-8601 format expected by consumers."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def load_replay_observations(source: Path) -> list[dict[str, object]]:
    """Read and normalize replay records from the source CSV."""
    if not source.is_file():
        raise FileNotFoundError(f"Observation CSV not found: {source}")

    observations = []
    with source.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV is missing required columns: {', '.join(sorted(missing))}")
        for row_number, row in enumerate(reader, start=2):
            try:
                observation_id = int(row["id"])
                latitude = float(row["latitude"])
                longitude = float(row["longitude"])
            except (TypeError, ValueError):
                LOGGER.warning("Skipping row %d with an invalid id or coordinate", row_number)
                continue
            county = (row.get(COUNTY_COLUMN) or "").strip() or "Unknown"
            observed_on = (row.get("observed_on") or "").strip()
            observations.append(
                {
                    "id": observation_id,
                    "observed_on": observed_on,
                    "latitude": latitude,
                    "longitude": longitude,
                    "county": county,
                }
            )
    if not observations:
        raise ValueError(f"No valid observation rows found in {source}")
    LOGGER.info("Loaded %d valid observations from %s", len(observations), source)
    return observations


def load_known_counties(source: Path) -> list[str]:
    """Get the distinct nonempty counties represented in the source CSV."""
    if not source.is_file():
        raise FileNotFoundError(f"Observation CSV not found: {source}")
    with source.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        if COUNTY_COLUMN not in (reader.fieldnames or []):
            raise ValueError(f"CSV is missing required column: {COUNTY_COLUMN}")
        counties = sorted({(row.get(COUNTY_COLUMN) or "").strip() for row in reader} - {""})
    if not counties:
        raise ValueError(f"No county names found in {source}")
    return counties


def replay_messages(observations: list[dict[str, object]], count: int | None) -> Iterator[dict[str, object]]:
    """Yield the requested number of timestamped replay messages."""
    limit = len(observations) if count is None else count
    records = observations if count is None else cycle(observations)
    for observation in records:
        if limit <= 0:
            break
        yield {**observation, "timestamp": utc_timestamp(), "source": "stream"}
        limit -= 1


def synthetic_messages(
    count: int | None,
    counties: list[str],
    rng: random.Random | None = None,
) -> Iterator[dict[str, object]]:
    """Yield synthetic messages with Romanian coordinates and process-unique IDs."""
    generator = rng or random.SystemRandom()
    sent = 0
    used_ids: set[int] = set()
    while count is None or sent < count:
        observation_id = time.time_ns()
        while observation_id in used_ids:
            observation_id += 1
        used_ids.add(observation_id)
        yield {
            "id": observation_id,
            "observed_on": date.today().isoformat(),
            "latitude": round(generator.uniform(43.6, 48.3), 6),
            "longitude": round(generator.uniform(20.2, 29.7), 6),
            "county": generator.choice(counties),
            "timestamp": utc_timestamp(),
            "source": "stream",
        }
        sent += 1


def create_producer(broker: str) -> KafkaProducer:
    """Create a Kafka producer with JSON serialization and delivery retries."""
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        acks="all",
        retries=5,
        request_timeout_ms=15000,
        api_version_auto_timeout_ms=5000,
    )


def connect_with_retries(broker: str) -> KafkaProducer:
    """Retry broker connection a few times before surfacing the failure."""
    for attempt in range(1, MAX_CONNECTION_ATTEMPTS + 1):
        try:
            return create_producer(broker)
        except NoBrokersAvailable:
            if attempt == MAX_CONNECTION_ATTEMPTS:
                raise
            LOGGER.warning(
                "Kafka broker %s unavailable (attempt %d/%d); retrying in %d seconds",
                broker,
                attempt,
                MAX_CONNECTION_ATTEMPTS,
                RETRY_DELAY_SECONDS,
            )
            time.sleep(RETRY_DELAY_SECONDS)
    raise RuntimeError("Unable to connect to Kafka")


def publish_message(producer: KafkaProducer, topic: str, message: dict[str, object]) -> None:
    """Send and wait for broker acknowledgment, using county as the key."""
    producer.send(topic, key=str(message["county"]), value=message).get(timeout=30)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    try:
        if args.mode == "replay":
            messages = replay_messages(load_replay_observations(args.input.resolve()), args.count)
        else:
            messages = synthetic_messages(args.count, load_known_counties(args.input.resolve()))

        LOGGER.info("Connecting to Kafka at %s; topic=%s; mode=%s", args.kafka_broker, args.topic, args.mode)
        producer = connect_with_retries(args.kafka_broker)
        sent = 0
        try:
            for message in messages:
                publish_message(producer, args.topic, message)
                sent += 1
                LOGGER.info(
                    "Published observation id=%s county=%s timestamp=%s",
                    message["id"],
                    message["county"],
                    message["timestamp"],
                )
                if args.delay and (args.count is None or sent < args.count):
                    time.sleep(args.delay)
            producer.flush()
            LOGGER.info("Finished publishing %d observation(s)", sent)
        finally:
            producer.close()
    except NoBrokersAvailable:
        LOGGER.error("Could not connect to Kafka broker %s after retries", args.kafka_broker)
        sys.exit(1)
    except KafkaError as error:
        LOGGER.error("Kafka publish failed: %s", error)
        sys.exit(1)
    except KeyboardInterrupt:
        LOGGER.info("Producer interrupted by user")


if __name__ == "__main__":
    main()
