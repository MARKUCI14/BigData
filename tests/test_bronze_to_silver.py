"""Tests for Bronze observation standardization and quality flags."""

from __future__ import annotations

import json

from src.processing.bronze_to_silver_observations import transform_observations


def test_silver_standardizes_counties_validates_coordinates_dates_and_duplicates(spark, tmp_path):
    mapping = tmp_path / "county_mapping.json"
    mapping.write_text(json.dumps({
        "county_mapping": {"brasov": "Brașov", "cluj": "Cluj"},
        "canonical_counties": ["Brașov", "Cluj"],
    }), encoding="utf-8")
    bronze_path = tmp_path / "bronze"
    rows = [
        (1, "2022-04-12", "45.0", "25.0", " brasov ", "2022-04-13", "observations.csv"),
        (1, "2022-04-12", "45.1", "25.1", "Brașov", "2022-04-14", "observations.csv"),
        (2, "2022-05-20", "50.0", "25.0", "cluj", "2022-05-21", "observations.csv"),
        (3, "not-a-date", "46.0", "24.0", "Cluj", "2022-05-21", "observations.csv"),
        (4, "2022-06-01", "46.0", "24.0", "   ", "2022-06-02", "observations.csv"),
    ]
    schema = "id long, observed_on string, latitude string, longitude string, county string, ingestion_timestamp string, source_file string"
    spark.createDataFrame(rows, schema).write.parquet(str(bronze_path))

    summary = transform_observations(bronze_path, tmp_path / "silver", mapping, spark=spark)

    assert summary["input_rows"] == 5
    assert summary["output_rows"] == 2
    assert summary["invalid_date_rows"] == 1
    assert summary["missing_county_rows"] == 1
    assert summary["duplicate_rows"] == 1
    result = spark.read.parquet(str(tmp_path / "silver")).orderBy("id").collect()
    assert result[0].county == "Brașov"
    assert (result[0].year, result[0].month, result[0].day) == (2022, 4, 12)
    assert result[0].coordinates_validated is True
    assert result[1].county == "Cluj"
    assert result[1].coordinates_validated is False
