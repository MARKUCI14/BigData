"""Clean and standardize Bronze butterfly observations into Silver Parquet."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "data" / "bronze" / "observations"
DEFAULT_DESTINATION = PROJECT_ROOT / "data" / "silver" / "observations"
DEFAULT_MAPPING = PROJECT_ROOT / "config" / "county_mapping.json"
LATITUDE_RANGE = (43.5, 48.5)
LONGITUDE_RANGE = (20.0, 30.5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_SOURCE, help="Bronze observations Parquet directory")
    parser.add_argument("--output", type=Path, default=DEFAULT_DESTINATION, help="Silver output directory")
    parser.add_argument("--mapping", type=Path, default=DEFAULT_MAPPING, help="County mapping JSON file")
    parser.add_argument("--dry-run", action="store_true", help="Calculate quality metrics without writing Parquet")
    return parser.parse_args()


def load_county_mapping(path: Path) -> tuple[dict[str, str], list[str]]:
    """Load case-insensitive county aliases and their canonical names."""
    with path.open("r", encoding="utf-8") as mapping_file:
        document = json.load(mapping_file)
    aliases = document.get("county_mapping", {})
    canonical = document.get("canonical_counties", [])
    if not aliases or not canonical:
        raise ValueError(f"County mapping file must define county_mapping and canonical_counties: {path}")
    return {str(alias).strip().casefold(): name for alias, name in aliases.items()}, list(canonical)


def standardize_county(frame: DataFrame, aliases: dict[str, str]) -> DataFrame:
    """Rename the legacy field if needed and map aliases to canonical county names."""
    if "county" not in frame.columns and "place_county_name" in frame.columns:
        frame = frame.withColumnRenamed("place_county_name", "county")
    if "county" not in frame.columns:
        raise ValueError("Bronze observations must contain 'county' or 'place_county_name'")

    trimmed = F.trim(F.col("county").cast("string"))
    mapping_expression = F.create_map(
        *[value for alias, canonical in aliases.items() for value in (F.lit(alias), F.lit(canonical))]
    )
    return frame.withColumn(
        "county",
        F.when(trimmed.isNull() | (trimmed == ""), F.lit(None).cast("string")).otherwise(
            F.coalesce(F.element_at(mapping_expression, F.lower(trimmed)), trimmed)
        ),
    )


def deduplicate_observations(frame: DataFrame) -> tuple[DataFrame, int]:
    """Keep one deterministic row per non-null observation ID and report duplicates."""
    ordering = []
    for column in ("ingestion_timestamp", "observed_on", "county", "latitude", "longitude", "source_file"):
        if column in frame.columns:
            ordering.append(F.col(column).asc_nulls_last())
    if not ordering:
        ordering = [F.col("id").asc()]

    ranked = frame.withColumn(
        "_dedup_rank",
        F.row_number().over(Window.partitionBy("id").orderBy(*ordering)),
    )
    duplicate_count = ranked.filter(F.col("id").isNotNull() & (F.col("_dedup_rank") > 1)).count()
    return ranked.filter(F.col("id").isNull() | (F.col("_dedup_rank") == 1)).drop("_dedup_rank"), duplicate_count


def transform_observations(
    source: Path,
    destination: Path,
    mapping_path: Path = DEFAULT_MAPPING,
    dry_run: bool = False,
    spark: SparkSession | None = None,
) -> dict[str, int | str | None]:
    """Clean Bronze observations, log quality metrics, and optionally write Silver."""
    aliases, canonical_counties = load_county_mapping(mapping_path)
    owns_spark = spark is None
    session = spark or SparkSession.builder.appName("Bronze to Silver observations").getOrCreate()
    try:
        frame = session.read.parquet(str(source))
        required_columns = {"id", "observed_on", "latitude", "longitude"}
        missing_columns = required_columns - set(frame.columns)
        if missing_columns:
            raise ValueError(f"Bronze observations are missing columns: {', '.join(sorted(missing_columns))}")

        input_rows = frame.count()
        frame = standardize_county(frame, aliases)
        frame = (
            frame.withColumn("id", F.col("id").cast("long"))
            .withColumn("observed_on", F.to_date(F.col("observed_on")))
            .withColumn("latitude", F.col("latitude").cast("double"))
            .withColumn("longitude", F.col("longitude").cast("double"))
        )
        invalid_date_rows = frame.filter(F.col("observed_on").isNull()).count()
        no_county_rows = frame.filter(F.col("observed_on").isNotNull() & (F.col("county").isNull() | (F.trim("county") == ""))).count()
        if invalid_date_rows:
            LOGGER.warning("Dropping %d rows with missing or unparseable observation dates", invalid_date_rows)
        if no_county_rows:
            LOGGER.warning("Dropping %d rows with missing or empty county names", no_county_rows)
        valid_base = frame.filter(F.col("observed_on").isNotNull() & F.col("county").isNotNull() & (F.trim("county") != ""))
        valid_base = (
            valid_base.withColumn("year", F.year("observed_on"))
            .withColumn("month", F.month("observed_on"))
            .withColumn("day", F.dayofmonth("observed_on"))
            .withColumn("county_validated", F.col("county").isin(*canonical_counties))
            .withColumn(
                "coordinates_validated",
                F.coalesce(
                    F.col("latitude").between(*LATITUDE_RANGE) & F.col("longitude").between(*LONGITUDE_RANGE),
                    F.lit(False),
                ),
            )
        )
        clean, duplicate_rows = deduplicate_observations(valid_base)
        if duplicate_rows:
            LOGGER.warning("Dropping %d duplicate observations based on id", duplicate_rows)
        output_rows = clean.count()
        summary = clean.agg(
            F.countDistinct("county").alias("county_count"),
            F.min("observed_on").alias("date_min"),
            F.max("observed_on").alias("date_max"),
            F.sum(F.when(~F.col("county_validated"), 1).otherwise(0)).alias("invalid_county_count"),
            F.sum(F.when(~F.col("coordinates_validated"), 1).otherwise(0)).alias("invalid_coordinate_count"),
        ).first()
        dropped_rows = input_rows - output_rows
        LOGGER.info(
            "Observation quality: input=%d output=%d dropped=%d (invalid_date=%d, missing_county=%d, duplicate_id=%d); counties=%d (unrecognized_rows=%d); invalid_coordinate_rows=%d; date_range=%s..%s",
            input_rows,
            output_rows,
            dropped_rows,
            invalid_date_rows,
            no_county_rows,
            duplicate_rows,
            summary["county_count"] or 0,
            summary["invalid_county_count"] or 0,
            summary["invalid_coordinate_count"] or 0,
            summary["date_min"],
            summary["date_max"],
        )
        if not dry_run:
            clean.write.mode("overwrite").partitionBy("year").parquet(str(destination))
            LOGGER.info("Wrote Silver observations to %s", destination)
        else:
            LOGGER.info("Dry run: calculated data quality metrics; no files were written")
        return {
            "input_rows": input_rows,
            "output_rows": output_rows,
            "dropped_rows": dropped_rows,
            "invalid_date_rows": invalid_date_rows,
            "missing_county_rows": no_county_rows,
            "duplicate_rows": duplicate_rows,
            "distinct_counties": summary["county_count"] or 0,
            "date_min": str(summary["date_min"]) if summary["date_min"] is not None else None,
            "date_max": str(summary["date_max"]) if summary["date_max"] is not None else None,
        }
    finally:
        if owns_spark:
            session.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    transform_observations(args.input.resolve(), args.output.resolve(), args.mapping.resolve(), args.dry_run)


if __name__ == "__main__":
    main()
