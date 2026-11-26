"""Tests for Pearson correlation and small-sample handling."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from src.processing.compute_correlations import build_correlation_dataset, pearson_p_value


def test_pearson_correlation_matches_known_linear_values(spark):
    gold = spark.createDataFrame(
        [("Brașov", 2020, 1, 1, 5.0), ("Brașov", 2020, 2, 2, 10.0),
         ("Brașov", 2021, 1, 3, 15.0)],
        "county string, year int, month int, observation_count int, avg_temperature double",
    )

    result = {
        (row.group_type, row.group_value): row
        for row in build_correlation_dataset(gold).collect()
    }

    assert result[("overall", "all")].pearson_r == pytest.approx(1.0)
    assert result[("county", "Brașov")].pearson_r == pytest.approx(1.0)
    assert result[("year", "2020")].pearson_r == pytest.approx(1.0)
    assert result[("overall", "all")].p_value == pytest.approx(0.0)
    assert result[("overall", "all")].small_sample is True


@pytest.mark.parametrize("correlation,size", [(0.5, 2), (None, 10), (0.5, None)])
def test_p_value_is_undefined_for_small_or_missing_samples(correlation, size):
    assert pearson_p_value(correlation, size) is None


def test_constant_input_yields_undefined_spark_correlation(spark):
    gold = spark.createDataFrame(
        [("Cluj", 2020, month, 0, float(month)) for month in (1, 2, 3)],
        "county string, year int, month int, observation_count int, avg_temperature double",
    )
    overall = build_correlation_dataset(gold).filter(F.col("group_type") == "overall").first()

    assert overall.pearson_r is None
    assert overall.p_value is None
