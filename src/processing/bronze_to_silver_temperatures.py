"""Unpivot Bronze county temperatures into clean Silver Parquet records."""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "data" / "bronze" / "temperatures"
DEFAULT_DESTINATION = PROJECT_ROOT / "data" / "silver" / "temperatures"
DEFAULT_MAPPING = PROJECT_ROOT / "config" / "county_mapping.json"
MONTH_COLUMN = re.compile(r"^X(?P<year>\d{4})\.(?P<month>\d{2})$")
MIN_TEMPERATURE = -40.0
MAX_TEMPERATURE = 50.0
EXPECTED_COUNTIES = 42


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_SOURCE, help="Bronze temperature Parquet directory")
    parser.add_argument("--output", type=Path, default=DEFAULT_DESTINATION, help="Silver output directory")
    parser.add_argument("--mapping", type=Path, default=DEFAULT_MAPPING, help="County mapping JSON file")
    parser.add_argument("--dry-run", action="store_true", help="Calculate quality metrics without writing Parquet")
    return parser.parse_args()


def load_county_mapping(path: Path) -> tuple[dict[str, str], list[str]]:
    """Load case-insensitive county aliases and canonical county names."""
    with path.open("r", encoding="utf-8") as mapping_file:
        document = json.load(mapping_file)
    aliases = document.get("county_mapping", {})
    canonical = document.get("canonical_counties", [])
    if not aliases or not canonical:
        raise ValueError(f"County mapping file must define county_mapping and canonical_counties: {path}")
    return {str(alias).strip().casefold(): name for alias, name in aliases.items()}, list(canonical)


def find_temperature_columns(columns: list[str]) -> list[tuple[str, int, int]]:
    """Validate and order wide monthly temperature columns by year and month."""
    parsed = []
    malformed = []
    for name in columns:
        match = MONTH_COLUMN.fullmatch(name)
        if match:
            month = int(match.group("month"))
            if not 1 <= month <= 12:
                malformed.append(name)
            else:
                parsed.append((name, int(match.group("year")), month))
        elif name.startswith("X"):
            malformed.append(name)
    if malformed:
        raise ValueError(f"Unexpected monthly temperature columns: {', '.join(malformed[:5])}")
    if not parsed:
        raise ValueError("No monthly temperature columns matching X{YYYY}.{MM} were found")
    return sorted(parsed, key=lambda value: (value[1], value[2]))


def standardize_county(frame: DataFrame, column_name: str, aliases: dict[str, str]) -> DataFrame:
    """Trim county values and map known aliases to the configured canonical names."""
    trimmed = F.trim(F.col(column_name).cast("string"))
    mapping_expression = F.create_map(
        *[value for alias, canonical in aliases.items() for value in (F.lit(alias), F.lit(canonical))]
    )
    return frame.withColumn(
        "county",
        F.when(trimmed.isNull() | (trimmed == ""), F.lit(None).cast("string")).otherwise(
            F.coalesce(F.element_at(mapping_expression, F.lower(trimmed)), trimmed)
        ),
    )


def transform_temperatures(
    source: Path,
    destination: Path,
    mapping_path: Path = DEFAULT_MAPPING,
    dry_run: bool = False,
    spark: SparkSession | None = None,
) -> dict[str, int]:
    """Unpivot and validate Bronze temperature data, optionally writing Silver Parquet."""
    aliases, canonical_counties = load_county_mapping(mapping_path)
    owns_spark = spark is None
    session = spark or SparkSession.builder.appName("Bronze to Silver temperatures").getOrCreate()
    try:
        wide = session.read.parquet(str(source))
        county_column = "County" if "County" in wide.columns else "county" if "county" in wide.columns else None
        if county_column is None:
            raise ValueError("Bronze temperatures must contain a 'County' or 'county' column")
        month_columns = find_temperature_columns(wide.columns)

        # Selecting only the county and month columns also discards the unnamed source index and ingestion metadata.
        stack_values = []
        for column_name, year, month in month_columns:
            stack_values.extend((str(year), str(month), f"CAST(`{column_name}` AS DOUBLE)"))
        stack_expression = (
            f"stack({len(month_columns)}, {', '.join(stack_values)}) "
            "as (year, month, avg_temperature)"
        )
        long = wide.select(
            F.col(county_column).alias("_source_county"),
            F.expr(stack_expression),
        )
        long = standardize_county(long, "_source_county", aliases).drop("_source_county")
        long = long.withColumn("avg_temperature", F.col("avg_temperature").cast("double"))

        missing_temperature = F.col("avg_temperature").isNull() | F.isnan("avg_temperature")
        null_temperature_rows = long.filter(missing_temperature).count()
        missing_county_rows = long.filter(F.col("county").isNull()).count()
        if null_temperature_rows:
            LOGGER.info("Dropping %d county-month rows with null temperature values", null_temperature_rows)
        if missing_county_rows:
            LOGGER.warning("Dropping %d county-month rows with missing county names", missing_county_rows)

        nonnull = long.filter(~missing_temperature & F.col("county").isNotNull())
        outlier_condition = (F.col("avg_temperature") < MIN_TEMPERATURE) | (F.col("avg_temperature") > MAX_TEMPERATURE)
        outlier_rows = nonnull.filter(outlier_condition).count()
        prepared = nonnull.withColumn("temperature_validated", ~outlier_condition)
        prepared = prepared.select("county", "year", "month", "avg_temperature", "temperature_validated")

        records = prepared.count()
        county_count = prepared.select("county").distinct().count()
        unknown_counties = prepared.filter(~F.col("county").isin(*canonical_counties)).select("county").distinct().count()
        LOGGER.info(
            "Temperature quality: county-month records=%d; null temperatures dropped=%d; outliers flagged=%d; counties found=%d/%d (unrecognized=%d)",
            records,
            null_temperature_rows,
            outlier_rows,
            county_count,
            EXPECTED_COUNTIES,
            unknown_counties,
        )
        if not dry_run:
            prepared.write.mode("overwrite").partitionBy("year").parquet(str(destination))
            LOGGER.info("Wrote Silver temperatures to %s", destination)
        else:
            LOGGER.info("Dry run: calculated data quality metrics; no files were written")
        return {
            "county_month_records": records,
            "null_temperature_rows": null_temperature_rows,
            "outlier_rows": outlier_rows,
            "counties_found": county_count,
            "unrecognized_counties": unknown_counties,
        }
    finally:
        if owns_spark:
            session.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    transform_temperatures(args.input.resolve(), args.output.resolve(), args.mapping.resolve(), args.dry_run)


if __name__ == "__main__":
    main()
