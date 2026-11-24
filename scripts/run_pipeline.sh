#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
SPARK_MASTER_URL="${SPARK_MASTER:-spark://localhost:7077}"

exec spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --properties-file "${PROJECT_ROOT}/config/spark-defaults.conf" \
  "${PROJECT_ROOT}/src/processing/run_batch_pipeline.py" \
  "$@"
