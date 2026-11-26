"""Tests for CSV observation ingestion and Bronze metadata."""

from __future__ import annotations

import pytest

from src.ingestion.ingest_observations import ingest_observations


def test_ingestion_adds_metadata_and_handles_bad_dates_and_empty_counties(spark, tmp_path):
    source = tmp_path / "observations.csv"
    source.write_text(
        "id,observed_on,latitude,longitude,place_county_name\n"
        "1,2022-05-10,45.2,25.1,Brasov\n"
        "2,not-a-date,46.0,24.0,Cluj\n"
        "3,2023-06-11,44.0,26.0,\n",
        encoding="utf-8",
    )
    destination = tmp_path / "bronze"

    rows_read, rows_written = ingest_observations(spark, source, destination)

    assert (rows_read, rows_written) == (3, 2)
    frame = spark.read.parquet(str(destination))
    assert {"ingestion_timestamp", "source_file", "data_source", "county", "year"}.issubset(frame.columns)
    by_id = {row.id: row for row in frame.collect()}
    assert by_id[1].county == "Brasov"
    assert by_id[1].source_file == source.name
    assert by_id[1].data_source == "inaturalist"
    assert by_id[1].ingestion_timestamp is not None
    assert by_id[3].county == "Unknown"
    assert by_id[3].year == 2023


def test_ingestion_rejects_missing_required_columns(spark, tmp_path):
    source = tmp_path / "invalid.csv"
    source.write_text("id,observed_on\n1,2022-05-10\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing required columns"):
        ingest_observations(spark, source, tmp_path / "bronze", dry_run=True)
