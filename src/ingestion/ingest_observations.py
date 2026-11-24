"""Ingest iNaturalist observations into the Bronze Parquet layer."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "data" / "raw" / "Parnassius_obs_cleaned.csv"
DEFAULT_DESTINATION = PROJECT_ROOT / "data" / "bronze" / "observations"
REQUIRED_COLUMNS = {"id", "observed_on", "latitude", "longitude", "place_county_name"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_SOURCE, help="Source CSV path")
    parser.add_argument("--output", type=Path, default=DEFAULT_DESTINATION, help="Bronze output directory")
    parser.add_argument("--dry-run", action="store_true", help="Validate and report without writing Parquet")
    return parser.parse_args()


def ingest_observations(
    spark: SparkSession,
    source: Path,
    destination: Path,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Validate, enrich, and optionally write observations; return read/written counts."""
    if not source.is_file():
        raise FileNotFoundError(f"Observation CSV not found: {source}")

    raw = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(str(source))
    missing = REQUIRED_COLUMNS - set(raw.columns)
    if missing:
        raise ValueError(f"CSV is missing required columns: {', '.join(sorted(missing))}")

    total_read = raw.count()
    cleaned = (
        raw.withColumn("observed_on", F.to_date(F.trim(F.col("observed_on")), "yyyy-MM-dd"))
        .withColumn(
            "county",
            F.when(
                F.col("place_county_name").isNull() | (F.trim(F.col("place_county_name")) == ""),
                F.lit("Unknown"),
            ).otherwise(F.trim(F.col("place_county_name"))),
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(source.name))
        .withColumn("data_source", F.lit("inaturalist"))
        .withColumn("year", F.year("observed_on"))
    )

    invalid_dates = cleaned.filter(F.col("observed_on").isNull()).count()
    if invalid_dates:
        LOGGER.warning("Skipping %d rows with missing or malformed observed_on dates", invalid_dates)
    valid = cleaned.filter(F.col("observed_on").isNotNull()).drop("place_county_name")
    total_written = valid.count()
    unknown_counties = valid.filter(F.col("county") == "Unknown").count()
    if unknown_counties:
        LOGGER.warning("Kept %d rows with missing county names as county='Unknown'", unknown_counties)

    LOGGER.info("Rows read: %d; rows written: %d; rows skipped: %d", total_read, total_written, total_read - total_written)
    if not dry_run:
        valid.write.mode("overwrite").partitionBy("year").parquet(str(destination))
        LOGGER.info("Wrote Bronze observations to %s", destination)
    else:
        LOGGER.info("Dry run: validated data; no files were written")
    return total_read, total_written


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    spark = SparkSession.builder.appName("Ingest iNaturalist observations to Bronze").getOrCreate()
    try:
        ingest_observations(spark, args.input.resolve(), args.output.resolve(), args.dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
