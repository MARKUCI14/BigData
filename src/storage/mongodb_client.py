"""Read-only MongoDB access helpers used by the dashboard."""

from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Any, Callable, TypeVar

import pandas as pd
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError

T = TypeVar("T")
RETRYABLE_ERRORS = (AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError)
DEFAULT_URI = "mongodb://localhost:27017"


class MongoDBClient:
    """Pooled MongoDB reader with bounded retries for transient connection errors."""

    def __init__(self, connection_string: str | None = None, database: str = "bigdata"):
        uri = connection_string or os.getenv("MONGO_URI", DEFAULT_URI)
        self._client = MongoClient(
            uri,
            retryReads=True,
            retryWrites=True,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            maxPoolSize=50,
        )
        self._database = self._client[database]

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> MongoDBClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _retry(self, operation: Callable[[], T], attempts: int = 3) -> T:
        for attempt in range(attempts):
            try:
                return operation()
            except RETRYABLE_ERRORS:
                if attempt == attempts - 1:
                    raise
                time.sleep(0.2 * (2**attempt))
        raise RuntimeError("Retry loop exited unexpectedly")

    def _frame(self, collection: str, query: dict[str, Any], sort: list[tuple[str, int]] | None = None,
               limit: int = 0) -> pd.DataFrame:
        def read() -> list[dict[str, Any]]:
            cursor = self._database[collection].find(query, {"_id": 0})
            if sort:
                cursor = cursor.sort(sort)
            if limit:
                cursor = cursor.limit(limit)
            return list(cursor)

        return pd.DataFrame(self._retry(read))

    def get_monthly_stats(self, county: str | None = None, year: int | None = None,
                          month: int | None = None) -> pd.DataFrame:
        query: dict[str, Any] = {}
        if county is not None:
            query["county"] = county
        if year is not None:
            query["year"] = int(year)
        if month is not None:
            query["month"] = int(month)
        return self._frame("monthly_statistics", query, [("year", 1), ("month", 1), ("county", 1)])

    def get_observations(self, county: str | None = None,
                         year_range: tuple[int, int] | None = None) -> pd.DataFrame:
        query: dict[str, Any] = {}
        if county is not None:
            query["county"] = county
        if year_range is not None:
            start, end = year_range
            query["year"] = {"$gte": int(start), "$lte": int(end)}
        return self._frame("observations", query, [("observed_on", 1)])

    def get_correlations(self, group_type: str | None = None) -> pd.DataFrame:
        query = {"group_type": group_type} if group_type is not None else {}
        return self._frame("correlations", query, [("group_type", 1), ("group_value", 1)])

    def get_realtime_stats(self, county: str | None = None) -> dict[str, Any] | list[dict[str, Any]] | None:
        collection = self._database["realtime_statistics"]

        def read():
            if county is not None:
                return collection.find_one({"county": county}, {"_id": 0})
            return list(collection.find({}, {"_id": 0}).sort("county", 1))

        return self._retry(read)

    def get_latest_observations(self, limit: int = 20) -> pd.DataFrame:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        return self._frame("observations", {}, [("observed_on", -1)], limit)

    def get_observation_count_by_county(self) -> pd.DataFrame:
        def aggregate():
            return list(self._database["observations"].aggregate([
                {"$group": {"_id": "$county", "observation_count": {"$sum": 1}}},
                {"$project": {"_id": 0, "county": "$_id", "observation_count": 1}},
                {"$sort": {"county": 1}},
            ]))

        return pd.DataFrame(self._retry(aggregate))

    def get_available_counties(self) -> list[str]:
        def read():
            return sorted(value for value in self._database["observations"].distinct("county") if value)

        return self._retry(read)

    def get_date_range(self) -> tuple[str, str]:
        def read():
            return list(self._database["observations"].aggregate([
                {"$group": {"_id": None, "minimum": {"$min": "$observed_on"},
                            "maximum": {"$max": "$observed_on"}}}
            ]))

        rows = self._retry(read)
        if not rows or rows[0]["minimum"] is None or rows[0]["maximum"] is None:
            return "", ""

        def iso(value: date | datetime | Any) -> str:
            return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()

        return iso(rows[0]["minimum"]), iso(rows[0]["maximum"])

    def get_collection_stats(self) -> dict[str, int]:
        names = ("observations", "monthly_statistics", "correlations", "realtime_statistics")
        return self._retry(lambda: {name: self._database[name].count_documents({}) for name in names})
