"""Orchestrate the Spark batch pipeline from raw CSVs through Gold analytics."""

from __future__ import annotations

import argparse
import importlib
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession

from src.ingestion.ingest_observations import ingest_observations
from src.ingestion.ingest_temperatures import ingest_temperatures
from src.processing.bronze_to_silver_observations import transform_observations
from src.processing.bronze_to_silver_temperatures import transform_temperatures
from src.processing.compute_correlations import run_analytics
from src.processing.silver_to_gold import run_silver_to_gold


LOGGER = logging.getLogger(__name__)
RAW_OBSERVATIONS = PROJECT_ROOT / "data" / "raw" / "Parnassius_obs_cleaned.csv"
RAW_TEMPERATURES = PROJECT_ROOT / "data" / "raw" / "Romania_County_Monthly_Temperatures_2000_2026.csv"
BRONZE_OBSERVATIONS = PROJECT_ROOT / "data" / "bronze" / "observations"
BRONZE_TEMPERATURES = PROJECT_ROOT / "data" / "bronze" / "temperatures"
SILVER_OBSERVATIONS = PROJECT_ROOT / "data" / "silver" / "observations"
SILVER_TEMPERATURES = PROJECT_ROOT / "data" / "silver" / "temperatures"
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"
GOLD_MONTHLY = GOLD_ROOT / "monthly_county_stats"
GOLD_CORRELATIONS = GOLD_ROOT / "correlations"
GOLD_TRENDS = GOLD_ROOT / "trends"
GOLD_SEASONAL = GOLD_ROOT / "seasonal_patterns"
GOLD_METADATA = GOLD_ROOT / "analytics_metadata.json"
COUNTY_MAPPING = PROJECT_ROOT / "config" / "county_mapping.json"


class OptionalStageSkipped(Exception):
    """Signal that an optional stage has not been implemented in this checkout."""


@dataclass(frozen=True)
class PipelineStage:
    name: str
    group: str
    action: Callable[[], object]
    outputs: tuple[Path, ...]
    dependencies: tuple[str, ...] = ()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stages",
        choices=("all", "ingest", "bronze-to-silver", "silver-to-gold"),
        default="all",
        help="Pipeline stage group to run",
    )
    parser.add_argument("--force", action="store_true", help="Reprocess stages even when their outputs exist")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue with independent stages after a failure")
    parser.add_argument("--dry-run", action="store_true", help="Run validations and metrics without writing outputs")
    return parser.parse_args()


def _load_mongodb_stage() -> object:
    """Invoke Issue #14's loader when present; leave a clear skip until then."""
    try:
        loader = importlib.import_module("src.storage.load_to_mongodb")
    except ModuleNotFoundError as error:
        if error.name == "src.storage.load_to_mongodb":
            raise OptionalStageSkipped("MongoDB batch loader is not implemented yet (planned in Issue #14)") from error
        raise
    entrypoint = getattr(loader, "run_load", None) or getattr(loader, "main", None)
    if entrypoint is None:
        raise RuntimeError("MongoDB batch loader must expose run_load() or main()")
    return entrypoint()


def build_stages(spark: SparkSession, dry_run: bool) -> list[PipelineStage]:
    """Build ordered stage definitions using one shared Spark session."""
    return [
        PipelineStage(
            "ingest-observations",
            "ingest",
            lambda: ingest_observations(spark, RAW_OBSERVATIONS, BRONZE_OBSERVATIONS, dry_run),
            (BRONZE_OBSERVATIONS,),
        ),
        PipelineStage(
            "ingest-temperatures",
            "ingest",
            lambda: ingest_temperatures(spark, RAW_TEMPERATURES, BRONZE_TEMPERATURES, dry_run),
            (BRONZE_TEMPERATURES,),
        ),
        PipelineStage(
            "silver-observations",
            "bronze-to-silver",
            lambda: transform_observations(
                BRONZE_OBSERVATIONS, SILVER_OBSERVATIONS, COUNTY_MAPPING, dry_run, spark
            ),
            (SILVER_OBSERVATIONS,),
            ("ingest-observations",),
        ),
        PipelineStage(
            "silver-temperatures",
            "bronze-to-silver",
            lambda: transform_temperatures(
                BRONZE_TEMPERATURES, SILVER_TEMPERATURES, COUNTY_MAPPING, dry_run, spark
            ),
            (SILVER_TEMPERATURES,),
            ("ingest-temperatures",),
        ),
        PipelineStage(
            "gold-monthly-statistics",
            "silver-to-gold",
            lambda: run_silver_to_gold(
                SILVER_OBSERVATIONS, SILVER_TEMPERATURES, GOLD_MONTHLY, dry_run, spark
            ),
            (GOLD_MONTHLY,),
            ("silver-observations", "silver-temperatures"),
        ),
        PipelineStage(
            "gold-analytics",
            "silver-to-gold",
            lambda: run_analytics(
                GOLD_MONTHLY,
                GOLD_CORRELATIONS,
                GOLD_TRENDS,
                GOLD_SEASONAL,
                GOLD_METADATA,
                dry_run,
                spark,
            ),
            (GOLD_CORRELATIONS, GOLD_TRENDS, GOLD_SEASONAL, GOLD_METADATA),
            ("gold-monthly-statistics",),
        ),
        PipelineStage(
            "gold-to-mongodb",
            "all",
            _load_mongodb_stage,
            (),
            ("gold-analytics",),
        ),
    ]


def run_pipeline(
    spark: SparkSession,
    stages: str = "all",
    force: bool = False,
    continue_on_error: bool = False,
    dry_run: bool = False,
) -> dict[str, str]:
    """Run selected stages in dependency order and return a status per stage."""
    definitions = build_stages(spark, dry_run)
    by_name = {stage.name: stage for stage in definitions}
    selected = [stage for stage in definitions if stages == "all" or stage.group == stages]
    results: dict[str, str] = {}
    stop_after_failure = False
    pipeline_started = datetime.now(timezone.utc)
    LOGGER.info("Batch pipeline started at %s; stages=%s; force=%s", pipeline_started.isoformat(), stages, force)

    for stage in selected:
        started_at = datetime.now(timezone.utc)
        started = time.perf_counter()
        LOGGER.info("Stage %s started at %s", stage.name, started_at.isoformat())
        try:
            if stop_after_failure:
                results[stage.name] = "SKIPPED (pipeline stopped after failure)"
            else:
                failed_dependencies = [
                    dependency
                    for dependency in stage.dependencies
                    if results.get(dependency, "").startswith(("FAILED", "BLOCKED"))
                ]
                if failed_dependencies:
                    results[stage.name] = f"BLOCKED (failed dependencies: {', '.join(failed_dependencies)})"
                    LOGGER.warning("Skipping stage %s because dependencies failed: %s", stage.name, failed_dependencies)
                    if not continue_on_error:
                        stop_after_failure = True
                else:
                    missing_dependencies = [
                        dependency
                        for dependency in stage.dependencies
                        if dependency not in results
                        and not all(path.exists() for path in by_name[dependency].outputs)
                    ]
                    if missing_dependencies:
                        results[stage.name] = f"BLOCKED (missing prerequisites: {', '.join(missing_dependencies)})"
                        LOGGER.warning("Skipping stage %s because prerequisite outputs are missing", stage.name)
                        if not continue_on_error:
                            stop_after_failure = True
                    elif stage.outputs and not force and not dry_run and all(path.exists() for path in stage.outputs):
                        results[stage.name] = "SKIPPED (outputs already exist; use --force to rebuild)"
                        LOGGER.info("Skipping stage %s because outputs already exist", stage.name)
                    else:
                        stage.action()
                        results[stage.name] = "SUCCESS"
        except OptionalStageSkipped as error:
            results[stage.name] = f"SKIPPED (optional stage: {error})"
            LOGGER.warning("Stage %s skipped: %s", stage.name, error)
        except Exception:
            results[stage.name] = "FAILED"
            LOGGER.exception("Stage %s failed", stage.name)
            if not continue_on_error:
                stop_after_failure = True
        finally:
            duration = time.perf_counter() - started
            ended_at = datetime.now(timezone.utc)
            LOGGER.info(
                "Stage %s ended at %s; duration=%.2fs; status=%s",
                stage.name,
                ended_at.isoformat(),
                duration,
                results[stage.name],
            )

    pipeline_ended = datetime.now(timezone.utc)
    LOGGER.info("Batch pipeline ended at %s", pipeline_ended.isoformat())
    LOGGER.info("Pipeline status summary:")
    for stage in selected:
        LOGGER.info("  %-26s %s", stage.name, results[stage.name])
    LOGGER.info("Pipeline duration: %.2fs", (pipeline_ended - pipeline_started).total_seconds())
    if any(status.startswith(("FAILED", "BLOCKED")) for status in results.values()):
        LOGGER.error("Batch pipeline finished with failures")
    else:
        LOGGER.info("Batch pipeline finished successfully")
    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    spark = SparkSession.builder.appName("Clouded Apollo batch pipeline").getOrCreate()
    try:
        statuses = run_pipeline(spark, args.stages, args.force, args.continue_on_error, args.dry_run)
        if any(status.startswith(("FAILED", "BLOCKED")) for status in statuses.values()):
            sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
