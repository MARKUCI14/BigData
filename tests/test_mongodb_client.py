"""Tests for MongoDBClient query types, filters, and streaming helpers."""

from __future__ import annotations

from datetime import datetime, timezone

import mongomock
import pandas as pd
import pytest

import src.storage.mongodb_client as mongodb_module
from src.storage.mongodb_client import MongoDBClient


@pytest.fixture
def mongo_client(monkeypatch):
    monkeypatch.setattr(mongodb_module, "MongoClient", mongomock.MongoClient)
    client = MongoDBClient("mongodb://localhost", "test")
    database = client._database
    database.monthly_statistics.insert_many([
        {"county": "Brașov", "year": 2020, "month": 1, "avg_temperature": 3.0},
        {"county": "Cluj", "year": 2021, "month": 2, "avg_temperature": 5.0},
    ])
    database.observations.insert_many([
        {"observation_id": 1, "county": "Brașov", "year": 2020, "month": 1,
         "observed_on": datetime(2020, 1, 3, tzinfo=timezone.utc),
         "timestamp": datetime(2020, 1, 3, 10, tzinfo=timezone.utc), "realtime_counted": True},
        {"observation_id": 2, "county": "Cluj", "year": 2021, "month": 2,
         "observed_on": datetime(2021, 2, 4, tzinfo=timezone.utc),
         "timestamp": datetime(2021, 2, 4, 10, tzinfo=timezone.utc), "realtime_counted": False},
    ])
    database.correlations.insert_many([
        {"group_type": "county", "group_value": "Brașov", "pearson_r": 0.7},
        {"group_type": "overall", "group_value": "all", "pearson_r": 0.4},
    ])
    database.realtime_statistics.insert_one({
        "county": "Brașov", "total_observations": 1,
        "last_updated": datetime(2020, 1, 3, tzinfo=timezone.utc),
    })
    yield client
    client.close()


def test_historical_queries_return_dataframes_and_apply_filters(mongo_client):
    monthly = mongo_client.get_monthly_stats(county="Brașov", year=2020)
    observations = mongo_client.get_observations(year_range=(2020, 2020))
    correlations = mongo_client.get_correlations(group_type="county")

    assert isinstance(monthly, pd.DataFrame) and len(monthly) == 1
    assert monthly.iloc[0]["county"] == "Brașov"
    assert isinstance(observations, pd.DataFrame) and observations["observation_id"].tolist() == [1]
    assert isinstance(correlations, pd.DataFrame) and correlations["group_value"].tolist() == ["Brașov"]


def test_realtime_and_metadata_queries_return_native_types(mongo_client):
    stats = mongo_client.get_realtime_stats("Brașov")
    latest = mongo_client.get_latest_observations(limit=1)
    counties = mongo_client.get_available_counties()
    minimum, maximum = mongo_client.get_date_range()
    counts = mongo_client.get_collection_stats()

    assert isinstance(stats, dict) and stats["total_observations"] == 1
    assert isinstance(latest, pd.DataFrame) and latest.iloc[0]["observation_id"] == 2
    assert counties == ["Brașov", "Cluj"]
    assert (minimum, maximum) == ("2020-01-03", "2021-02-04")
    assert counts["observations"] == 2


def test_stream_helpers_filter_to_first_seen_counties_and_timestamp(mongo_client):
    since = datetime(2020, 1, 1, tzinfo=timezone.utc)
    latest = mongo_client.get_latest_observations(limit=20, realtime_only=True, counties=["Brașov"])

    assert latest["observation_id"].tolist() == [1]
    assert mongo_client.count_stream_observations(since=since) == 1
