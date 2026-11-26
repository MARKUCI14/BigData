"""Load the bundled batch datasets into MongoDB when serving collections are empty."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
from pathlib import Path

from pymongo import MongoClient


LOGGER = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
COLLECTIONS = ("observations", "monthly_statistics", "correlations")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="reload bundled datasets even if MongoDB has data")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
    database_name = os.getenv("MONGO_DATABASE", "bigdata")

    with MongoClient(uri, serverSelectionTimeoutMS=10000) as client:
        client.admin.command("ping")
        database = client[database_name]
        counts = {name: database[name].count_documents({}) for name in COLLECTIONS}

    if not args.force and all(counts.values()):
        LOGGER.info("MongoDB already has batch data; skipping seed: %s", counts)
        return

    LOGGER.info("MongoDB batch data is missing; running the bundled batch pipeline: %s", counts)
    spark_submit = Path("/opt/spark/bin/spark-submit")
    command = [
        str(spark_submit),
        "--master",
        "spark://spark-master:7077",
        "--conf",
        "spark.jars.packages=",
        "--conf",
        "spark.executorEnv.PYTHONPATH=/opt/spark/app",
        str(PROJECT_ROOT / "src" / "processing" / "run_batch_pipeline.py"),
        "--stages",
        "all",
    ]
    if args.force:
        command.append("--force")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


if __name__ == "__main__":
    main()
