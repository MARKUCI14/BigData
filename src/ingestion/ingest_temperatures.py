"""Ingest the wide Copernicus county temperature CSV into Bronze Parquet."""

from __future__ import annotations

import argparse
import csv
import logging
import re
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "data" / "raw" / "Romania_County_Monthly_Temperatures_2000_2026.csv"
DEFAULT_DESTINATION = PROJECT_ROOT / "data" / "bronze" / "temperatures"
MONTH_COLUMN = re.compile(r"^X\d{4}\.(0[1-9]|1[0-2])$")
MISSING_TEMPERATURES = {"", "nan", "na", "null"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_SOURCE, help="Source CSV path")
    parser.add_argument("--output", type=Path, default=DEFAULT_DESTINATION, help="Bronze output directory")
    parser.add_argument("--dry-run", action="store_true", help="Validate and report without writing Parquet")
    return parser.parse_args()


def read_and_validate_header(source: Path) -> list[str]:
    """Read and validate the CSV header before Spark normalizes blank names."""
    if not source.is_file():
        raise FileNotFoundError(f"Temperature CSV not found: {source}")
    with source.open("r", encoding="utf-8-sig", newline="") as csv_file:
        header = next(csv.reader(csv_file), None)
    if not header or len(header) < 3:
        raise ValueError("Temperature CSV must contain an index, County, and monthly columns")
    if header[0].strip() or header[1].strip() != "County":
        raise ValueError("Expected an unnamed numeric index column followed by a 'County' column")
    invalid = [column for column in header[2:] if not MONTH_COLUMN.fullmatch(column)]
    if invalid:
        raise ValueError(f"Unexpected monthly temperature column format: {', '.join(invalid[:5])}")
    if len(set(header)) != len(header):
        raise ValueError("Temperature CSV contains duplicate column names")
    return header


def ingest_temperatures(
    spark: SparkSession,
    source: Path,
    destination: Path,
    dry_run: bool = False,
) -> tuple[int, str, str]:
    """Validate, normalize missing values, and optionally write the wide dataset."""
    header = read_and_validate_header(source)
    raw = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(str(source))
    if len(raw.columns) != len(header) or raw.columns[1] != "County":
        raise ValueError("Spark could not read the expected temperature CSV structure")

    index_column = raw.columns[0]
    bad_indexes = raw.filter(F.col(index_column).cast("double").isNull()).count()
    if bad_indexes:
        LOGGER.warning("Found %d rows with a nonnumeric index; dropping the source index column", bad_indexes)

    monthly_columns = header[2:]
    date_keys = [column[1:] for column in monthly_columns]
    county_frame = raw.select(F.trim(F.col("County")).alias("County"))
    county_count = county_frame.filter(F.col("County").isNotNull() & (F.col("County") != "")).count()
    if county_frame.filter(F.col("County").isNull() | (F.col("County") == "")).count():
        LOGGER.warning("Found rows with a missing county name")

    prepared = raw.drop(index_column)
    invalid_counts = []
    for position, column in enumerate(monthly_columns):
        source_value = F.lower(F.trim(F.col(column)))
        missing = source_value.isNull() | source_value.isin(*MISSING_TEMPERATURES)
        invalid_counts.append(
            F.sum(F.when(~missing & F.col(column).cast("double").isNull(), 1).otherwise(0)).alias(f"invalid_{position}")
        )

    invalid_row = prepared.agg(*invalid_counts).first()
    invalid_values = sum(value or 0 for value in invalid_row)
    for column in monthly_columns:
        source_value = F.lower(F.trim(F.col(column)))
        missing = source_value.isNull() | source_value.isin(*MISSING_TEMPERATURES)
        prepared = prepared.withColumn(
            column,
            F.when(missing, F.lit(None).cast("double")).otherwise(F.col(column).cast("double")),
        )

    if invalid_values:
        LOGGER.warning("Found %d nonnumeric temperature values; preserving them as null", invalid_values)
    prepared = (
        prepared.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(source.name))
        .withColumn("data_source", F.lit("copernicus"))
    )
    rows = prepared.count()
    LOGGER.info(
        "Ingested %d counties; monthly data covers %s through %s (%d rows)",
        county_count,
        date_keys[0],
        date_keys[-1],
        rows,
    )
    if not dry_run:
        prepared.write.mode("overwrite").parquet(str(destination))
        LOGGER.info("Wrote Bronze temperatures to %s", destination)
    else:
        LOGGER.info("Dry run: validated data; no files were written")
    return county_count, date_keys[0], date_keys[-1]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    spark = SparkSession.builder.appName("Ingest Copernicus temperatures to Bronze").getOrCreate()
    try:
        ingest_temperatures(spark, args.input.resolve(), args.output.resolve(), args.dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
