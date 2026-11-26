"""Tests for Gold monthly aggregation, temperature join, and categories."""

from __future__ import annotations

from src.processing.silver_to_gold import build_gold_dataset


def test_gold_counts_observations_left_joins_temperatures_and_categorizes(spark):
    observations = spark.createDataFrame(
        [("Brașov", 2022, 1), ("Brașov", 2022, 1), ("Cluj", 2022, 2)],
        "county string, year int, month int",
    )
    temperatures = spark.createDataFrame(
        [("Brașov", 2022, 1, 3.5), ("Brașov", 2022, 2, 12.0), ("Cluj", 2022, 2, 27.0)],
        "county string, year int, month int, avg_temperature double",
    )

    rows = {
        (row.county, row.month): row
        for row in build_gold_dataset(observations, temperatures).collect()
    }

    assert rows[("Brașov", 1)].observation_count == 2
    assert rows[("Brașov", 1)].temperature_category == "Cold"
    assert rows[("Brașov", 1)].has_observations is True
    assert rows[("Brașov", 2)].observation_count == 0
    assert rows[("Brașov", 2)].temperature_category == "Mild"
    assert rows[("Brașov", 2)].has_observations is False
    assert rows[("Cluj", 2)].temperature_category == "Hot"
    assert rows[("Cluj", 2)].observation_count == 1
