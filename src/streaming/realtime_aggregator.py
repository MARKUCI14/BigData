"""Maintain idempotent per-county real-time MongoDB statistics."""

from __future__ import annotations

import logging
from datetime import date, datetime, time as datetime_time, timezone
from typing import Any

from pymongo import MongoClient, UpdateOne
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType


LOGGER = logging.getLogger(__name__)
BULK_BATCH_SIZE = 500
RECENT_LIMIT = 10
EVENT_SCHEMA = StructType(
    [
        StructField("observation_id", LongType(), False),
        StructField("observed_on", DateType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("county", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("associated_temperature", DoubleType(), True),
    ]
)


def _chunks(values: list[Any], size: int):
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _as_utc_datetime(value: date | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return datetime.combine(value, datetime_time.min, tzinfo=timezone.utc)


def _temperature_lookup(monthly_collection, keys: list[tuple[str, int, int]]) -> dict[tuple[str, int, int], float]:
    """Fetch available historical monthly temperatures in bounded query batches."""
    temperatures = {}
    for batch in _chunks(keys, BULK_BATCH_SIZE):
        query = {
            "$or": [
                {"county": county, "year": year, "month": month}
                for county, year, month in batch
            ]
        }
        for document in monthly_collection.find(query, {"county": 1, "year": 1, "month": 1, "avg_temperature": 1}):
            value = document.get("avg_temperature")
            if value is not None:
                key = (document["county"], int(document["year"]), int(document["month"]))
                temperatures[key] = float(value)
    return temperatures


def _aggregate_county_events(events: DataFrame) -> list:
    """Count events within tumbling minute windows and collect the latest ten per county."""
    windowed = events.withColumn("_minute_window", F.window("timestamp", "1 minute"))
    window_counts = windowed.groupBy("county", "_minute_window").agg(
        F.count(F.lit(1)).alias("window_observations")
    )
    totals = window_counts.groupBy("county").agg(
        F.sum("window_observations").cast("long").alias("new_observations")
    )
    latest = events.groupBy("county").agg(F.max("observed_on").alias("latest_observation_date"))

    recent_window = Window.partitionBy("county").orderBy(
        F.col("timestamp").desc(), F.col("observation_id").desc()
    )
    recent = (
        events.withColumn("_recent_rank", F.row_number().over(recent_window))
        .filter(F.col("_recent_rank") <= RECENT_LIMIT)
        .groupBy("county")
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        F.col("timestamp"),
                        F.col("observation_id"),
                        F.col("observed_on"),
                        F.col("latitude"),
                        F.col("longitude"),
                        F.col("associated_temperature"),
                    )
                )
            ).alias("recent_observations")
        )
    )
    return totals.join(latest, "county").join(recent, "county").collect()


def update_realtime_statistics(
    valid_events: DataFrame,
    batch_id: int,
    stream_run_id: str,
    mongo_uri: str,
    database_name: str,
) -> dict[str, int]:
    """Enrich new events and update capped county statistics once per Spark batch."""
    key_rows = valid_events.select(
        F.col("county"),
        F.year(F.col("event.observed_on")).alias("year"),
        F.month(F.col("event.observed_on")).alias("month"),
    ).distinct().collect()
    keys = [(row["county"], int(row["year"]), int(row["month"])) for row in key_rows]
    batch_key = f"{stream_run_id}:{batch_id}"
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        database = client[database_name]
        observation_collection = database["observations"]
        monthly_collection = database["monthly_statistics"]
        realtime_collection = database["realtime_statistics"]
        observation_collection.create_index("observation_id", unique=True, name="idx_observation_id_unique")
        realtime_collection.create_index("county", unique=True, name="idx_county_unique")

        temperature_by_key = _temperature_lookup(monthly_collection, keys)
        event_rows = valid_events.select(
            F.col("event.id").alias("observation_id"),
            F.col("event.observed_on").alias("observed_on"),
            F.col("event.latitude").alias("latitude"),
            F.col("event.longitude").alias("longitude"),
            F.col("county"),
            F.col("event.timestamp").alias("timestamp"),
            F.year(F.col("event.observed_on")).alias("year"),
            F.month(F.col("event.observed_on")).alias("month"),
        ).toLocalIterator()

        observation_updates = []
        observation_ids = []
        missing_temperatures = 0
        for row in event_rows:
            observation_id = int(row["observation_id"])
            key = (row["county"], int(row["year"]), int(row["month"]))
            temperature = temperature_by_key.get(key)
            if temperature is None:
                missing_temperatures += 1
            observation_updates.append(
                UpdateOne(
                    {"observation_id": observation_id},
                    {"$set": {"associated_temperature": temperature}},
                )
            )
            observation_ids.append(observation_id)
            if len(observation_updates) >= BULK_BATCH_SIZE:
                observation_collection.bulk_write(observation_updates, ordered=False)
                observation_updates.clear()
        if observation_updates:
            observation_collection.bulk_write(observation_updates, ordered=False)

        if missing_temperatures:
            LOGGER.info("Kafka batch %d: no historical temperature found for %d observation(s)", batch_id, missing_temperatures)

        # Tag only first-seen IDs. The batch key survives foreachBatch retries, so a partial retry can finish safely.
        for batch_ids in _chunks(observation_ids, BULK_BATCH_SIZE):
            observation_collection.update_many(
                {"observation_id": {"$in": batch_ids}, "realtime_counted": {"$ne": True}},
                {"$set": {"realtime_counted": True, "realtime_batch_key": batch_key}},
            )

        new_documents = observation_collection.find(
            {"realtime_batch_key": batch_key},
            {
                "observation_id": 1,
                "observed_on": 1,
                "latitude": 1,
                "longitude": 1,
                "county": 1,
                "timestamp": 1,
                "associated_temperature": 1,
            },
        )
        event_values = []
        for document in new_documents:
            observed_on = document.get("observed_on")
            if isinstance(observed_on, datetime):
                observed_on = observed_on.date()
            event_values.append(
                (
                    int(document["observation_id"]),
                    observed_on,
                    float(document["latitude"]) if document.get("latitude") is not None else None,
                    float(document["longitude"]) if document.get("longitude") is not None else None,
                    document["county"],
                    document.get("timestamp"),
                    float(document["associated_temperature"])
                    if document.get("associated_temperature") is not None
                    else None,
                )
            )

        if not event_values:
            LOGGER.info("Kafka batch %d: no new observations to aggregate", batch_id)
            return {"new_observations": 0, "updated_counties": 0, "missing_temperatures": missing_temperatures}

        events = valid_events.sparkSession.createDataFrame(event_values, schema=EVENT_SCHEMA)
        county_summaries = _aggregate_county_events(events)
        now = datetime.now(timezone.utc)
        updated_counties = 0
        for summary in county_summaries:
            county = summary["county"]
            recent_documents = []
            for item in summary["recent_observations"]:
                recent_documents.append(
                    {
                        "observation_id": int(item["observation_id"]),
                        "observed_on": _as_utc_datetime(item["observed_on"]),
                        "latitude": float(item["latitude"]) if item["latitude"] is not None else None,
                        "longitude": float(item["longitude"]) if item["longitude"] is not None else None,
                        "timestamp": _as_utc_datetime(item["timestamp"]),
                        "associated_temperature": float(item["associated_temperature"])
                        if item["associated_temperature"] is not None
                        else None,
                    }
                )

            realtime_collection.update_one(
                {"county": county},
                {"$setOnInsert": {"total_observations": 0, "recent_observations": []}},
                upsert=True,
            )
            result = realtime_collection.update_one(
                {
                    "county": county,
                    "$or": [
                        {"stream_run_id": {"$ne": stream_run_id}},
                        {"last_processed_batch": {"$lt": batch_id}},
                    ],
                },
                {
                    "$inc": {"total_observations": int(summary["new_observations"])},
                    "$max": {
                        "latest_observation_date": _as_utc_datetime(summary["latest_observation_date"])
                    },
                    "$push": {"recent_observations": {"$each": recent_documents, "$slice": -RECENT_LIMIT}},
                    "$set": {
                        "last_updated": now,
                        "stream_run_id": stream_run_id,
                        "last_processed_batch": batch_id,
                    },
                },
                upsert=False,
            )
            if result.modified_count:
                updated_counties += 1

        LOGGER.info(
            "Kafka batch %d: new observations=%d; real-time counties updated=%d",
            batch_id,
            len(event_values),
            updated_counties,
        )
        return {
            "new_observations": len(event_values),
            "updated_counties": updated_counties,
            "missing_temperatures": missing_temperatures,
        }
    finally:
        client.close()
