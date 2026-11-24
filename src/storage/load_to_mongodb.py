"""Upsert Silver observations and Gold statistics into MongoDB."""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, datetime, time as datetime_time, timezone
from pathlib import Path
from typing import Iterable

from pymongo import MongoClient, ReplaceOne, UpdateOne
from pyspark.sql import SparkSession


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OBSERVATIONS = PROJECT_ROOT / "data" / "silver" / "observations"
DEFAULT_MONTHLY_STATISTICS = PROJECT_ROOT / "data" / "gold" / "monthly_county_stats"
DEFAULT_CORRELATIONS = PROJECT_ROOT / "data" / "gold" / "correlations"
DEFAULT_URI = "mongodb://localhost:27017"
DEFAULT_DATABASE = "bigdata"
DEFAULT_BATCH_SIZE = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", type=Path, default=DEFAULT_OBSERVATIONS)
    parser.add_argument("--monthly-statistics", type=Path, default=DEFAULT_MONTHLY_STATISTICS)
    parser.add_argument("--correlations", type=Path, default=DEFAULT_CORRELATIONS)
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", DEFAULT_URI))
    parser.add_argument("--database", default=os.getenv("MONGO_DATABASE", DEFAULT_DATABASE))
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    return parser.parse_args()


def _as_mongo_datetime(value: date | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return datetime.combine(value, datetime_time.min, tzinfo=timezone.utc)


def _write_batches(collection, operations: Iterable[object], batch_size: int) -> dict[str, int]:
    """Bulk-upsert operations in bounded batches and aggregate MongoDB result counts."""
    inserted = 0
    updated = 0
    processed = 0
    batch = []
    for operation in operations:
        batch.append(operation)
        if len(batch) >= batch_size:
            result = collection.bulk_write(batch, ordered=False)
            inserted += result.upserted_count
            updated += result.modified_count
            processed += len(batch)
            batch.clear()
    if batch:
        result = collection.bulk_write(batch, ordered=False)
        inserted += result.upserted_count
        updated += result.modified_count
        processed += len(batch)
    return {"inserted": inserted, "updated": updated, "processed": processed}


def _observation_operations(rows, batch_loaded_at: datetime):
    """Convert Silver rows to observation documents keyed by observation_id."""
    skipped = 0
    for row in rows:
        observation_id = row["id"]
        if observation_id is None:
            skipped += 1
            continue
        document = {
            "observation_id": int(observation_id),
            "observed_on": _as_mongo_datetime(row["observed_on"]),
            "year": int(row["year"]) if row["year"] is not None else None,
            "month": int(row["month"]) if row["month"] is not None else None,
            "latitude": float(row["latitude"]) if row["latitude"] is not None else None,
            "longitude": float(row["longitude"]) if row["longitude"] is not None else None,
            "county": row["county"],
            "source": "batch",
            "batch_loaded_at": batch_loaded_at,
        }
        yield UpdateOne(
            {"observation_id": document["observation_id"]},
            {"$set": document},
            upsert=True,
        )
    if skipped:
        LOGGER.warning("Skipped %d Silver observations without an id", skipped)


def _monthly_operations(rows, correlation_by_county: dict[str, float | None], batch_loaded_at: datetime):
    """Convert Gold monthly rows to documents keyed by the unique county-year-month index."""
    for row in rows:
        county = row["county"]
        year = int(row["year"])
        month = int(row["month"])
        document = {
            "county": county,
            "year": year,
            "month": month,
            "observation_count": int(row["observation_count"] or 0),
            "avg_temperature": float(row["avg_temperature"]) if row["avg_temperature"] is not None else None,
            "has_observations": bool(row["has_observations"]),
            "temperature_category": row["temperature_category"],
            "correlation_coefficient": correlation_by_county.get(county),
            "batch_loaded_at": batch_loaded_at,
        }
        yield ReplaceOne(
            {"county": county, "year": year, "month": month},
            document,
            upsert=True,
        )


def _correlation_operations(rows, batch_loaded_at: datetime):
    """Convert correlation output to documents keyed by group type and group value."""
    for row in rows:
        document = {
            "group_type": row["group_type"],
            "group_value": row["group_value"],
            "pearson_r": float(row["pearson_r"]) if row["pearson_r"] is not None else None,
            "p_value": float(row["p_value"]) if row["p_value"] is not None else None,
            "sample_size": int(row["sample_size"]),
            "small_sample": bool(row["small_sample"]),
            "batch_loaded_at": batch_loaded_at,
        }
        yield ReplaceOne(
            {"group_type": document["group_type"], "group_value": document["group_value"]},
            document,
            upsert=True,
        )


def run_load(
    observations_path: Path = DEFAULT_OBSERVATIONS,
    monthly_statistics_path: Path = DEFAULT_MONTHLY_STATISTICS,
    correlations_path: Path = DEFAULT_CORRELATIONS,
    mongo_uri: str | None = None,
    database_name: str | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    spark: SparkSession | None = None,
) -> dict[str, dict[str, int]]:
    """Load Silver and Gold data into MongoDB with idempotent, batched upserts."""
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    uri = mongo_uri or os.getenv("MONGO_URI", DEFAULT_URI)
    db_name = database_name or os.getenv("MONGO_DATABASE", DEFAULT_DATABASE)
    started = time.perf_counter()
    batch_loaded_at = datetime.now(timezone.utc)
    owns_spark = spark is None
    session = spark or SparkSession.builder.appName("Load Gold and Silver data into MongoDB").getOrCreate()
    client = None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        database = client[db_name]
        observations_collection = database["observations"]
        monthly_collection = database["monthly_statistics"]
        correlations_collection = database["correlations"]

        observations_collection.create_index("observation_id", unique=True, name="idx_observation_id_unique")
        monthly_collection.create_index(
            [("county", 1), ("year", 1), ("month", 1)],
            unique=True,
            name="idx_county_year_month_unique",
        )
        correlations_collection.create_index(
            [("group_type", 1), ("group_value", 1)],
            unique=True,
            name="idx_group_type_value_unique",
        )

        observations = session.read.parquet(str(observations_path))
        monthly = session.read.parquet(str(monthly_statistics_path))
        correlations = session.read.parquet(str(correlations_path))
        correlation_by_county = {
            row["group_value"]: float(row["pearson_r"])
            for row in correlations.filter(
                (correlations.group_type == "county") & correlations.pearson_r.isNotNull()
            ).select("group_value", "pearson_r").collect()
        }

        observation_stats = _write_batches(
            observations_collection,
            _observation_operations(observations.toLocalIterator(), batch_loaded_at),
            batch_size,
        )
        observation_cleanup = (
            observations_collection.delete_many(
                {"source": {"$ne": "stream"}, "batch_loaded_at": {"$ne": batch_loaded_at}}
            )
            if observation_stats["processed"]
            else None
        )
        monthly_stats = _write_batches(
            monthly_collection,
            _monthly_operations(monthly.toLocalIterator(), correlation_by_county, batch_loaded_at),
            batch_size,
        )
        monthly_cleanup = (
            monthly_collection.delete_many({"batch_loaded_at": {"$ne": batch_loaded_at}})
            if monthly_stats["processed"]
            else None
        )
        correlation_stats = _write_batches(
            correlations_collection,
            _correlation_operations(correlations.toLocalIterator(), batch_loaded_at),
            batch_size,
        )
        correlation_cleanup = (
            correlations_collection.delete_many({"batch_loaded_at": {"$ne": batch_loaded_at}})
            if correlation_stats["processed"]
            else None
        )

        LOGGER.info(
            "MongoDB observations: inserted=%d updated=%d stale_removed=%d",
            observation_stats["inserted"],
            observation_stats["updated"],
            observation_cleanup.deleted_count if observation_cleanup else 0,
        )
        LOGGER.info(
            "MongoDB monthly_statistics: inserted=%d updated=%d stale_removed=%d",
            monthly_stats["inserted"],
            monthly_stats["updated"],
            monthly_cleanup.deleted_count if monthly_cleanup else 0,
        )
        LOGGER.info(
            "MongoDB correlations: inserted=%d updated=%d stale_removed=%d",
            correlation_stats["inserted"],
            correlation_stats["updated"],
            correlation_cleanup.deleted_count if correlation_cleanup else 0,
        )
        return {
            "observations": {**observation_stats, "stale_removed": observation_cleanup.deleted_count if observation_cleanup else 0},
            "monthly_statistics": {**monthly_stats, "stale_removed": monthly_cleanup.deleted_count if monthly_cleanup else 0},
            "correlations": {**correlation_stats, "stale_removed": correlation_cleanup.deleted_count if correlation_cleanup else 0},
        }
    finally:
        if client is not None:
            client.close()
        if owns_spark:
            session.stop()
        LOGGER.info("MongoDB batch load finished in %.2f seconds", time.perf_counter() - started)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run_load(
        args.observations.resolve(),
        args.monthly_statistics.resolve(),
        args.correlations.resolve(),
        args.mongo_uri,
        args.database,
        args.batch_size,
    )


if __name__ == "__main__":
    main()
