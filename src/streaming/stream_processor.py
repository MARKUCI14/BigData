"""Consume Kafka observations, validate them, and upsert to MongoDB."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.streaming.realtime_aggregator import update_realtime_statistics


DEFAULT_MAPPING = PROJECT_ROOT / "config" / "county_mapping.json"
DEFAULT_CHECKPOINT = PROJECT_ROOT / "data" / "checkpoints" / "streaming"
DEFAULT_DEAD_LETTER = PROJECT_ROOT / "data" / "bronze" / "dead_letters" / "streaming"
DEFAULT_KAFKA_BROKER = "localhost:9092"
DEFAULT_TOPIC = "butterfly-observations"
DEFAULT_MONGO_URI = "mongodb://localhost:27017"
DEFAULT_DATABASE = "bigdata"
TRIGGER_INTERVAL = "10 seconds"
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,"
    "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1"
)
LATITUDE_RANGE = (43.5, 48.5)
LONGITUDE_RANGE = (20.0, 30.5)
MESSAGE_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("observed_on", DateType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("county", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("source", StringType(), True),
    ]
)


def load_or_create_stream_run_id(checkpoint_path: Path) -> str:
    """Persist a run token alongside Spark offsets so foreachBatch retries stay idempotent."""
    checkpoint_path.mkdir(parents=True, exist_ok=True)
    run_id_path = checkpoint_path / "stream_run_id"
    if run_id_path.exists():
        stream_run_id = run_id_path.read_text(encoding="utf-8").strip()
        if stream_run_id:
            return stream_run_id
    stream_run_id = uuid4().hex
    run_id_path.write_text(stream_run_id, encoding="utf-8")
    return stream_run_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka-broker", default=os.getenv("KAFKA_BROKER", DEFAULT_KAFKA_BROKER))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC))
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", DEFAULT_MONGO_URI))
    parser.add_argument("--database", default=os.getenv("MONGO_DATABASE", DEFAULT_DATABASE))
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT)
    parser.add_argument("--dead-letter-output", type=Path, default=DEFAULT_DEAD_LETTER)
    parser.add_argument("--mapping", type=Path, default=DEFAULT_MAPPING)
    parser.add_argument("--trigger-interval", default=TRIGGER_INTERVAL)
    return parser.parse_args()


def load_county_mapping(path: Path) -> tuple[dict[str, str], list[str]]:
    """Load case-insensitive aliases and the canonical Romanian county names."""
    with path.open("r", encoding="utf-8") as mapping_file:
        document = json.load(mapping_file)
    aliases = document.get("county_mapping", {})
    canonical = document.get("canonical_counties", [])
    if not aliases or not canonical:
        raise ValueError(f"County mapping file must define county_mapping and canonical_counties: {path}")
    return {str(alias).strip().casefold(): name for alias, name in aliases.items()}, list(canonical)


def create_spark_session(app_name: str = "Clouded Apollo Kafka stream") -> SparkSession:
    """Create Spark with the Kafka and MongoDB connector packages available."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.session.timeZone", "UTC")
    )
    master = os.getenv("SPARK_MASTER")
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def parse_and_validate_messages(kafka_batch: DataFrame, aliases: dict[str, str], canonical: list[str]) -> DataFrame:
    """Parse Kafka JSON values, standardize county names, and classify invalid rows."""
    decoded = kafka_batch.select(
        "topic",
        "partition",
        "offset",
        F.col("value").cast("string").alias("raw_value"),
        F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("event"),
    )
    source_county = F.trim(F.col("event.county"))
    county_map = F.create_map(
        *[value for alias, name in aliases.items() for value in (F.lit(alias), F.lit(name))]
    )
    decoded = decoded.withColumn(
        "county",
        F.when(source_county.isNull() | (source_county == ""), F.lit(None).cast("string")).otherwise(
            F.coalesce(F.element_at(county_map, F.lower(source_county)), source_county)
        ),
    )
    county_validated = F.col("county").isin(*canonical)
    coordinates_validated = F.coalesce(
        F.col("event.latitude").between(*LATITUDE_RANGE) & F.col("event.longitude").between(*LONGITUDE_RANGE),
        F.lit(False),
    )
    reason = F.concat_ws(
        "; ",
        F.when(F.col("event").isNull(), F.lit("invalid JSON message")),
        F.when(F.col("event.id").isNull(), F.lit("missing or invalid id")),
        F.when(F.col("event.observed_on").isNull(), F.lit("missing or invalid observed_on date")),
        F.when(F.col("event.latitude").isNull() | F.col("event.longitude").isNull(), F.lit("missing or invalid coordinates")),
        F.when(F.col("county").isNull(), F.lit("missing county")),
        F.when(F.col("county").isNotNull() & ~county_validated, F.lit("unknown county")),
        F.when(~coordinates_validated, F.lit("coordinates outside Romania bounds")),
        F.when(F.col("event.timestamp").isNull(), F.lit("missing or invalid timestamp")),
        F.when(F.col("event.source").isNull() | (F.trim(F.col("event.source")) == ""), F.lit("missing source")),
    )
    return decoded.withColumn("invalid_reason", reason).withColumn("coordinates_validated", coordinates_validated)


def upsert_valid_batch(batch: DataFrame, batch_id: int, mongo_uri: str, database_name: str) -> int:
    """Upsert a valid micro-batch by stable observation ID and return document count."""
    valid = batch.filter(F.col("invalid_reason") == "").select(
        F.col("event.id").alias("observation_id"),
        F.col("event.observed_on").alias("observed_on"),
        F.year(F.col("event.observed_on")).alias("year"),
        F.month(F.col("event.observed_on")).alias("month"),
        F.col("event.latitude").alias("latitude"),
        F.col("event.longitude").alias("longitude"),
        F.col("county").alias("county"),
        F.col("event.timestamp").alias("timestamp"),
        F.col("event.source").alias("source"),
    ).dropDuplicates(["observation_id"])

    processed = valid.count()
    if processed:
        (
            valid.write.format("mongodb")
            .mode("append")
            .option("connection.uri", mongo_uri)
            .option("database", database_name)
            .option("collection", "observations")
            .option("operationType", "update")
            .option("idFieldList", "observation_id")
            .option("upsertDocument", "true")
            .option("maxBatchSize", "500")
            .save()
        )
    LOGGER.info("Kafka batch %d: upserted %d observations", batch_id, processed)
    return processed


def process_micro_batch(
    kafka_batch: DataFrame,
    batch_id: int,
    aliases: dict[str, str],
    canonical: list[str],
    mongo_uri: str,
    database_name: str,
    dead_letter_output: Path,
    stream_run_id: str,
) -> None:
    """Write rejected messages to a retry-safe dead-letter path and upsert valid rows."""
    parsed = parse_and_validate_messages(kafka_batch, aliases, canonical).persist()
    try:
        rejected = parsed.filter(F.col("invalid_reason") != "")
        rejected_count = rejected.count()
        if rejected_count:
            dead_letters = rejected.select(
                "topic",
                "partition",
                "offset",
                "raw_value",
                "invalid_reason",
                F.current_timestamp().alias("processed_at"),
                F.lit(batch_id).alias("stream_batch_id"),
            )
            batch_path = dead_letter_output / f"batch_id={batch_id}"
            dead_letters.write.mode("overwrite").parquet(str(batch_path))
            LOGGER.warning("Kafka batch %d: wrote %d rejected messages to %s", batch_id, rejected_count, batch_path)
        accepted = (
            parsed.filter(F.col("invalid_reason") == "")
            .withColumn("_observation_id", F.col("event.id"))
            .dropDuplicates(["_observation_id"])
            .drop("_observation_id")
            .persist()
        )
        try:
            accepted_count = upsert_valid_batch(accepted, batch_id, mongo_uri, database_name)
            realtime_stats = update_realtime_statistics(
                accepted,
                batch_id,
                stream_run_id,
                mongo_uri,
                database_name,
            )
            LOGGER.info(
                "Kafka batch %d complete: accepted=%d rejected=%d realtime_counties=%d",
                batch_id,
                accepted_count,
                rejected_count,
                realtime_stats["updated_counties"],
            )
        finally:
            accepted.unpersist()
    finally:
        parsed.unpersist()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    aliases, canonical = load_county_mapping(args.mapping.resolve())
    checkpoint_path = args.checkpoint.resolve()
    stream_run_id = load_or_create_stream_run_id(checkpoint_path)
    spark = create_spark_session()
    try:
        kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.kafka_broker)
            .option("subscribe", args.topic)
            .option("startingOffsets", "latest")
            .load()
        )
        query = (
            kafka_stream.writeStream.foreachBatch(
                lambda batch, batch_id: process_micro_batch(
                    batch,
                    batch_id,
                    aliases,
                    canonical,
                    args.mongo_uri,
                    args.database,
                    args.dead_letter_output.resolve(),
                    stream_run_id,
                )
            )
            .option("checkpointLocation", str(checkpoint_path))
            .trigger(processingTime=args.trigger_interval)
            .start()
        )
        LOGGER.info(
            "Streaming consumer started: broker=%s topic=%s checkpoint=%s trigger=%s",
            args.kafka_broker,
            args.topic,
            checkpoint_path,
            args.trigger_interval,
        )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
