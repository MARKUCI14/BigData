# Spark Configuration

## Cluster

| Property | Value |
|----------|-------|
| Spark Master | `spark://spark-master:7077` |
| Spark Master UI | `http://localhost:8080` |
| Spark Worker UI | `http://localhost:8081` |
| Worker Memory | 2g |
| Worker Cores | 2 |

## Docker image

Compose builds one local image, `clouded-apollo-spark:3.5.9`, for both the master and worker. Its base is the Docker Official Image `spark:3.5.9-scala2.12-java17-python3-ubuntu`; the custom layer installs PyMongo for streaming writes. The Spark master and worker use `/opt/spark` as `SPARK_HOME` and mount the repository data directory at `/opt/spark/data`.

## Required Packages

The following Spark packages must be included via `--packages` when submitting jobs:

```
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9
org.mongodb.spark:mongo-spark-connector_2.12:10.2.1
```

The Kafka connector version matches Spark 3.5.9. The packages are listed in `config/spark-defaults.conf`, which Compose mounts into both Spark containers. The streaming consumer also sets the same package coordinates in its Spark session.

## spark-submit Examples

### Batch Processing (from host)
```bash
spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
  src/processing/run_batch_pipeline.py --stages all
```

### Batch Processing (from Docker)
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/src/processing/run_batch_pipeline.py --stages all
```

### Streaming Job
```bash
spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
  src/streaming/stream_processor.py
```

## Data Directories (inside Docker)

| Layer | Path |
|-------|------|
| Raw | `/opt/spark/data/raw/` |
| Bronze | `/opt/spark/data/bronze/` |
| Silver | `/opt/spark/data/silver/` |
| Gold | `/opt/spark/data/gold/` |
