"""End-to-end batch pipeline test using small slices of the checked-in source data."""

from __future__ import annotations

import csv
import json
import os
import uuid
from collections import Counter
from pathlib import Path

import pytest
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from src.ingestion.ingest_observations import ingest_observations
from src.ingestion.ingest_temperatures import ingest_temperatures
from src.processing.bronze_to_silver_observations import transform_observations
from src.processing.bronze_to_silver_temperatures import transform_temperatures
from src.processing.compute_correlations import run_analytics
from src.processing.silver_to_gold import run_silver_to_gold
from src.storage.load_to_mongodb import run_load


ROOT = Path(__file__).resolve().parents[1]
OBSERVATIONS_CSV = ROOT / "data" / "raw" / "Parnassius_obs_cleaned.csv"
TEMPERATURES_CSV = ROOT / "data" / "raw" / "Romania_County_Monthly_Temperatures_2000_2026.csv"
COUNTY_MAPPING = ROOT / "config" / "county_mapping.json"


@pytest.fixture
def mongo_uri():
    uri = os.getenv("MONGO_TEST_URI", "mongodb://localhost:27018")
    client = MongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError:
        client.close()
        pytest.skip("MongoDB test service is unavailable; start it with docker compose -f docker-compose.test.yml up -d")
    client.close()
    return uri


def _write_real_data_slices(observations_path: Path, temperatures_path: Path) -> tuple[list[str], int]:
    """Select up to eight real observations and matching temperatures for three counties."""
    mapping = json.loads(COUNTY_MAPPING.read_text(encoding="utf-8"))
    aliases = {key.casefold(): value for key, value in mapping["county_mapping"].items()}

    with OBSERVATIONS_CSV.open("r", encoding="utf-8-sig", newline="") as source:
        observation_reader = csv.DictReader(source)
        observation_fields = observation_reader.fieldnames
        observation_rows = list(observation_reader)
    assert observation_fields is not None

    with TEMPERATURES_CSV.open("r", encoding="utf-8-sig", newline="") as source:
        temperature_reader = csv.DictReader(source)
        temperature_fields = temperature_reader.fieldnames
        temperature_rows = list(temperature_reader)
    assert temperature_fields is not None

    county_field = temperature_fields[1]
    temperature_by_county = {
        aliases.get((row.get(county_field) or "").strip().casefold(), (row.get(county_field) or "").strip()): row
        for row in temperature_rows
    }
    def canonical_observation(row: dict[str, str]) -> str:
        county = (row.get("place_county_name") or "").strip()
        return aliases.get(county.casefold(), county)

    common_counts = Counter(
        county for row in observation_rows
        if (county := canonical_observation(row)) in temperature_by_county
    )
    selected_counties = [county for county, _ in common_counts.most_common(3)]
    selected_observations = [
        row for row in observation_rows if canonical_observation(row) in selected_counties
    ][:8]
    if len(selected_observations) < 5:
        pytest.skip("The checked-in source data has fewer than five observations in matching temperature counties")
    selected_counties = sorted({canonical_observation(row) for row in selected_observations})

    with observations_path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=observation_fields)
        writer.writeheader()
        writer.writerows(selected_observations)

    years = {row["observed_on"][:4] for row in selected_observations if row.get("observed_on")}
    month_columns = [
        name for name in temperature_fields[2:]
        if len(name) >= 5 and name[1:5] in years
    ]
    if not month_columns:
        pytest.skip("No temperature columns match the selected observation years")
    selected_temperature_rows = [temperature_by_county[county] for county in selected_counties]
    with temperatures_path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.writer(destination)
        writer.writerow(["", county_field, *month_columns])
        for index, row in enumerate(selected_temperature_rows, start=1):
            writer.writerow([index, row.get(county_field, ""), *(row.get(month) for month in month_columns)])
    return selected_counties, len(selected_observations)


@pytest.mark.integration
def test_real_data_flows_from_csv_through_mongodb(spark, mongo_uri, tmp_path):
    """Run ingestion, Bronze/Silver/Gold, analytics, and MongoDB loading in isolation."""
    raw_observations = tmp_path / "observations.csv"
    raw_temperatures = tmp_path / "temperatures.csv"
    selected_counties, selected_observation_count = _write_real_data_slices(raw_observations, raw_temperatures)
    database_name = f"bigdata_integration_{uuid.uuid4().hex}"

    bronze_observations = tmp_path / "bronze" / "observations"
    bronze_temperatures = tmp_path / "bronze" / "temperatures"
    silver_observations = tmp_path / "silver" / "observations"
    silver_temperatures = tmp_path / "silver" / "temperatures"
    gold_monthly = tmp_path / "gold" / "monthly_statistics"
    gold_correlations = tmp_path / "gold" / "correlations"
    gold_trends = tmp_path / "gold" / "trends"
    gold_seasonal = tmp_path / "gold" / "seasonal"
    metadata = tmp_path / "gold" / "analytics_metadata.json"

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        observations_read, observations_written = ingest_observations(
            spark, raw_observations, bronze_observations
        )
        temperature_count, _, _ = ingest_temperatures(spark, raw_temperatures, bronze_temperatures)
        assert observations_read == selected_observation_count
        assert observations_written == selected_observation_count
        assert temperature_count == len(selected_counties)

        bronze = spark.read.parquet(str(bronze_observations))
        assert {"id", "observed_on", "county", "ingestion_timestamp", "data_source"}.issubset(bronze.columns)
        assert spark.read.parquet(str(bronze_temperatures)).count() == len(selected_counties)

        silver_observation_summary = transform_observations(
            bronze_observations, silver_observations, COUNTY_MAPPING, spark=spark
        )
        silver_temperature_summary = transform_temperatures(
            bronze_temperatures, silver_temperatures, COUNTY_MAPPING, spark=spark
        )
        silver = spark.read.parquet(str(silver_observations))
        assert silver_observation_summary["output_rows"] > 0
        assert {"year", "month", "day", "county_validated", "coordinates_validated"}.issubset(silver.columns)
        assert set(row.county for row in silver.select("county").distinct().collect()).issubset(set(selected_counties))
        assert silver_temperature_summary["county_month_records"] > 0

        gold_summary = run_silver_to_gold(
            silver_observations, silver_temperatures, gold_monthly, spark=spark
        )
        gold_rows = spark.read.parquet(str(gold_monthly))
        assert gold_summary["county_months"] > 0
        assert {"observation_count", "avg_temperature", "temperature_category"}.issubset(gold_rows.columns)
        assert gold_rows.filter("observation_count > 0").count() > 0

        run_analytics(
            gold_monthly, gold_correlations, gold_trends, gold_seasonal, metadata, spark=spark
        )
        expected_correlations = spark.read.parquet(str(gold_correlations)).count()
        loaded = run_load(
            observations_path=silver_observations,
            monthly_statistics_path=gold_monthly,
            correlations_path=gold_correlations,
            mongo_uri=mongo_uri,
            database_name=database_name,
            spark=spark,
        )

        database = client[database_name]
        assert database.observations.count_documents({}) == silver_observation_summary["output_rows"]
        assert database.monthly_statistics.count_documents({}) == gold_summary["county_months"]
        assert database.correlations.count_documents({}) == expected_correlations
        assert database.observations.find_one({"source": "batch", "county": {"$in": selected_counties}})
        assert loaded["observations"]["processed"] == silver_observation_summary["output_rows"]
        assert database.monthly_statistics.find_one({"avg_temperature": {"$ne": None}})
    finally:
        client.drop_database(database_name)
        client.close()
