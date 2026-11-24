"""Compute correlation, yearly trend, and seasonal analytics from Gold data."""

from __future__ import annotations

import argparse
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import DoubleType


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"
DEFAULT_SOURCE = GOLD_ROOT / "monthly_county_stats"
DEFAULT_CORRELATIONS = GOLD_ROOT / "correlations"
DEFAULT_TRENDS = GOLD_ROOT / "trends"
DEFAULT_SEASONAL = GOLD_ROOT / "seasonal_patterns"
DEFAULT_METADATA = GOLD_ROOT / "analytics_metadata.json"
SMALL_SAMPLE_THRESHOLD = 10
OBSERVATION_CAVEAT = (
    "Observation counts measure recorded observations in the source dataset; they are not estimates of "
    "Clouded Apollo population abundance. Results may reflect sampling effort and reporting bias, and do not "
    "establish a causal effect of temperature."
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_SOURCE, help="Gold monthly county statistics Parquet directory")
    parser.add_argument("--correlations-output", type=Path, default=DEFAULT_CORRELATIONS)
    parser.add_argument("--trends-output", type=Path, default=DEFAULT_TRENDS)
    parser.add_argument("--seasonal-output", type=Path, default=DEFAULT_SEASONAL)
    parser.add_argument("--metadata-output", type=Path, default=DEFAULT_METADATA)
    parser.add_argument("--dry-run", action="store_true", help="Calculate metrics without writing outputs")
    return parser.parse_args()


def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    """Evaluate the continued fraction used by the regularized incomplete beta."""
    max_iterations = 200
    epsilon = 3.0e-14
    tiny = 1.0e-300
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < tiny:
        d = tiny
    d = 1.0 / d
    result = d
    for iteration in range(1, max_iterations + 1):
        twice = 2 * iteration
        coefficient = iteration * (b - iteration) * x / ((qam + twice) * (a + twice))
        d = 1.0 + coefficient * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + coefficient / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        result *= d * c

        coefficient = -(a + iteration) * (qab + iteration) * x / ((a + twice) * (qap + twice))
        d = 1.0 + coefficient * d
        if abs(d) < tiny:
            d = tiny
        c = 1.0 + coefficient / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        result *= delta
        if abs(delta - 1.0) < epsilon:
            break
    return result


def _regularized_beta(x: float, a: float, b: float) -> float:
    """Compute the regularized incomplete beta function without extra dependencies."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    factor = math.exp(
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log1p(-x)
    )
    if x < (a + 1.0) / (a + b + 2.0):
        return factor * _beta_continued_fraction(a, b, x) / a
    return 1.0 - factor * _beta_continued_fraction(b, a, 1.0 - x) / b


def pearson_p_value(pearson_r: float | None, sample_size: int | None) -> float | None:
    """Return the two-sided p-value for Pearson r, or None when it is undefined."""
    if pearson_r is None or sample_size is None or sample_size < 3:
        return None
    correlation = max(-1.0, min(1.0, float(pearson_r)))
    if abs(correlation) == 1.0:
        return 0.0
    degrees_of_freedom = sample_size - 2
    return _regularized_beta(1.0 - correlation * correlation, degrees_of_freedom / 2.0, 0.5)


def build_correlation_dataset(gold: DataFrame) -> DataFrame:
    """Compute county, year, and overall Pearson correlations and p-values."""
    required = {"county", "year", "observation_count", "avg_temperature"}
    missing = required - set(gold.columns)
    if missing:
        raise ValueError(f"Gold data is missing columns: {', '.join(sorted(missing))}")

    pair_count = F.count(
        F.when(F.col("avg_temperature").isNotNull() & F.col("observation_count").isNotNull(), F.lit(1))
    ).cast("int")
    by_county = gold.groupBy("county").agg(
        F.corr("avg_temperature", "observation_count").alias("pearson_r"),
        pair_count.alias("sample_size"),
    ).select(
        F.lit("county").alias("group_type"),
        F.col("county").cast("string").alias("group_value"),
        "pearson_r",
        "sample_size",
    )
    by_year = gold.groupBy("year").agg(
        F.corr("avg_temperature", "observation_count").alias("pearson_r"),
        pair_count.alias("sample_size"),
    ).select(
        F.lit("year").alias("group_type"),
        F.col("year").cast("string").alias("group_value"),
        "pearson_r",
        "sample_size",
    )
    overall = gold.agg(
        F.corr("avg_temperature", "observation_count").alias("pearson_r"),
        pair_count.alias("sample_size"),
    ).select(
        F.lit("overall").alias("group_type"),
        F.lit("all").alias("group_value"),
        "pearson_r",
        "sample_size",
    )
    correlations = by_county.unionByName(by_year).unionByName(overall)
    p_value_udf = F.udf(pearson_p_value, DoubleType())
    return (
        correlations.withColumn("p_value", p_value_udf("pearson_r", "sample_size"))
        .withColumn("small_sample", F.col("sample_size") < SMALL_SAMPLE_THRESHOLD)
        .select("group_type", "group_value", "pearson_r", "p_value", "sample_size", "small_sample")
    )


def build_trend_dataset(gold: DataFrame) -> DataFrame:
    """Compute yearly observation totals and percentage change per county."""
    yearly = gold.groupBy("county", "year").agg(
        F.sum("observation_count").cast("long").alias("total_observations")
    )
    window = Window.partitionBy("county").orderBy("year")
    yearly = yearly.withColumn("previous_year_observations", F.lag("total_observations").over(window))
    yearly = yearly.withColumn("previous_year", F.lag("year").over(window))
    return yearly.withColumn(
        "year_over_year_change_pct",
        F.when(
            F.col("previous_year_observations").isNull()
            | (F.col("previous_year_observations") == 0)
            | (F.col("previous_year") != F.col("year") - 1),
            F.lit(None).cast("double"),
        ).otherwise(
            (F.col("total_observations") - F.col("previous_year_observations"))
            / F.col("previous_year_observations")
            * 100.0
        ),
    ).select(
        "county",
        "year",
        "total_observations",
        "previous_year_observations",
        "year_over_year_change_pct",
    )


def build_seasonal_dataset(gold: DataFrame) -> DataFrame:
    """Compute mean and total observations by calendar month and flag peak months."""
    monthly = gold.groupBy("month").agg(
        F.avg("observation_count").alias("avg_observations"),
        F.sum("observation_count").cast("long").alias("total_observations"),
        F.count(F.lit(1)).cast("long").alias("county_month_records"),
    )
    full_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    monthly = monthly.withColumn("_peak_average", F.max("avg_observations").over(full_window))
    return monthly.withColumn("is_peak_month", F.col("avg_observations") == F.col("_peak_average")).drop("_peak_average")


def run_analytics(
    source: Path,
    correlations_path: Path = DEFAULT_CORRELATIONS,
    trends_path: Path = DEFAULT_TRENDS,
    seasonal_path: Path = DEFAULT_SEASONAL,
    metadata_path: Path = DEFAULT_METADATA,
    dry_run: bool = False,
    spark: SparkSession | None = None,
) -> dict[str, int | float | None]:
    """Generate all analytical Gold datasets and their methodology metadata."""
    owns_spark = spark is None
    session = spark or SparkSession.builder.appName("Gold correlation and trend analytics").getOrCreate()
    try:
        gold = session.read.parquet(str(source))
        required_columns = {"county", "year", "month", "observation_count", "avg_temperature"}
        missing_columns = required_columns - set(gold.columns)
        if missing_columns:
            raise ValueError(f"Gold monthly statistics are missing columns: {', '.join(sorted(missing_columns))}")
        correlations = build_correlation_dataset(gold)
        trends = build_trend_dataset(gold)
        seasonal = build_seasonal_dataset(gold)

        correlation_summary = correlations.agg(
            F.count(F.lit(1)).alias("correlation_groups"),
            F.sum(F.when(F.col("small_sample"), 1).otherwise(0)).alias("small_sample_groups"),
        ).first()
        trend_years = trends.count()
        seasonal_summary = seasonal.orderBy(F.col("avg_observations").desc(), F.col("month")).first()
        LOGGER.info(
            "Analytics summary: correlation_groups=%d; groups_with_n_lt_%d=%d; yearly_trend_rows=%d; peak_month=%s (mean observations=%.4f)",
            correlation_summary["correlation_groups"] or 0,
            SMALL_SAMPLE_THRESHOLD,
            correlation_summary["small_sample_groups"] or 0,
            trend_years,
            seasonal_summary["month"] if seasonal_summary else None,
            seasonal_summary["avg_observations"] if seasonal_summary else 0.0,
        )

        if not dry_run:
            correlations.write.mode("overwrite").parquet(str(correlations_path))
            trends.write.mode("overwrite").partitionBy("year").parquet(str(trends_path))
            seasonal.write.mode("overwrite").parquet(str(seasonal_path))
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            metadata = {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "source": str(source),
                "correlation_method": "Pearson product-moment correlation, two-sided p-value",
                "p_value_null_when_sample_size_below": 3,
                "small_sample_threshold": SMALL_SAMPLE_THRESHOLD,
                "caveat": OBSERVATION_CAVEAT,
            }
            metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            LOGGER.info(
                "Wrote correlations, trends, seasonal patterns, and methodology metadata under %s",
                metadata_path.parent,
            )
        else:
            LOGGER.info("Dry run: calculated analytics; no files were written")

        return {
            "correlation_groups": correlation_summary["correlation_groups"] or 0,
            "small_sample_groups": correlation_summary["small_sample_groups"] or 0,
            "yearly_trend_rows": trend_years,
            "peak_month": seasonal_summary["month"] if seasonal_summary else None,
        }
    finally:
        if owns_spark:
            session.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run_analytics(
        args.input.resolve(),
        args.correlations_output.resolve(),
        args.trends_output.resolve(),
        args.seasonal_output.resolve(),
        args.metadata_output.resolve(),
        args.dry_run,
    )


if __name__ == "__main__":
    main()
