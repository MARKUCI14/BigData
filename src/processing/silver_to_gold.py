"""Aggregate monthly butterfly observations and temperatures into Gold data."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OBSERVATIONS = PROJECT_ROOT / "data" / "silver" / "observations"
DEFAULT_TEMPERATURES = PROJECT_ROOT / "data" / "silver" / "temperatures"
DEFAULT_DESTINATION = PROJECT_ROOT / "data" / "gold" / "monthly_county_stats"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", type=Path, default=DEFAULT_OBSERVATIONS, help="Silver observations Parquet directory")
    parser.add_argument("--temperatures", type=Path, default=DEFAULT_TEMPERATURES, help="Silver temperatures Parquet directory")
    parser.add_argument("--output", type=Path, default=DEFAULT_DESTINATION, help="Gold output directory")
    parser.add_argument("--dry-run", action="store_true", help="Calculate analytics without writing Parquet")
    return parser.parse_args()


def build_gold_dataset(observations, temperatures):
    """Aggregate observation counts and left-join them to all temperature months."""
    observation_columns = {"county", "year", "month"}
    temperature_columns = {"county", "year", "month", "avg_temperature"}
    missing_observation_columns = observation_columns - set(observations.columns)
    missing_temperature_columns = temperature_columns - set(temperatures.columns)
    if missing_observation_columns:
        raise ValueError(
            f"Silver observations are missing columns: {', '.join(sorted(missing_observation_columns))}"
        )
    if missing_temperature_columns:
        raise ValueError(
            f"Silver temperatures are missing columns: {', '.join(sorted(missing_temperature_columns))}"
        )

    counts = observations.groupBy("county", "year", "month").agg(F.count(F.lit(1)).alias("observation_count"))
    joined = temperatures.alias("temperature").join(
        counts.alias("observations"),
        on=(
            (F.col("temperature.county") == F.col("observations.county"))
            & (F.col("temperature.year") == F.col("observations.year"))
            & (F.col("temperature.month") == F.col("observations.month"))
        ),
        how="left",
    )
    result = joined.select(
        F.col("temperature.county").alias("county"),
        F.col("temperature.year").cast("int").alias("year"),
        F.col("temperature.month").cast("int").alias("month"),
        F.coalesce(F.col("observations.observation_count"), F.lit(0)).cast("int").alias("observation_count"),
        F.col("temperature.avg_temperature").cast("double").alias("avg_temperature"),
    )
    result = result.withColumn("has_observations", F.col("observation_count") > 0)
    result = result.withColumn(
        "temperature_category",
        F.when(F.col("avg_temperature").isNull(), F.lit(None).cast("string"))
        .when(F.col("avg_temperature") < 5.0, F.lit("Cold"))
        .when(F.col("avg_temperature") < 15.0, F.lit("Mild"))
        .when(F.col("avg_temperature") <= 25.0, F.lit("Warm"))
        .otherwise(F.lit("Hot")),
    )
    return result.select(
        "county",
        "year",
        "month",
        "observation_count",
        "avg_temperature",
        "has_observations",
        "temperature_category",
    )


def run_silver_to_gold(
    observations_path: Path,
    temperatures_path: Path,
    destination: Path,
    dry_run: bool = False,
    spark: SparkSession | None = None,
) -> dict[str, int | float | None]:
    """Build Gold monthly statistics, log an analytics summary, and optionally write Parquet."""
    owns_spark = spark is None
    session = spark or SparkSession.builder.appName("Silver to Gold monthly county statistics").getOrCreate()
    try:
        observations = session.read.parquet(str(observations_path))
        temperatures = session.read.parquet(str(temperatures_path))
        gold = build_gold_dataset(observations, temperatures)
        summary = gold.agg(
            F.count(F.lit(1)).alias("county_months"),
            F.sum(F.when(F.col("has_observations"), 1).otherwise(0)).alias("with_observations"),
            F.sum(F.when(~F.col("has_observations"), 1).otherwise(0)).alias("without_observations"),
            F.min("avg_temperature").alias("min_temperature"),
            F.max("avg_temperature").alias("max_temperature"),
        ).first()
        LOGGER.info(
            "Gold analytics: county-months=%d; with_observations=%d; without_observations=%d; temperature_range=%s..%s °C",
            summary["county_months"] or 0,
            summary["with_observations"] or 0,
            summary["without_observations"] or 0,
            summary["min_temperature"],
            summary["max_temperature"],
        )
        if not dry_run:
            gold.write.mode("overwrite").partitionBy("year").parquet(str(destination))
            LOGGER.info("Wrote Gold monthly county statistics to %s", destination)
        else:
            LOGGER.info("Dry run: calculated analytics; no files were written")
        return {
            "county_months": summary["county_months"] or 0,
            "with_observations": summary["with_observations"] or 0,
            "without_observations": summary["without_observations"] or 0,
            "min_temperature": summary["min_temperature"],
            "max_temperature": summary["max_temperature"],
        }
    finally:
        if owns_spark:
            session.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run_silver_to_gold(
        args.observations.resolve(),
        args.temperatures.resolve(),
        args.output.resolve(),
        args.dry_run,
    )


if __name__ == "__main__":
    main()
