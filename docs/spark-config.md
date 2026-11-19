# Spark Configuration

## Cluster

| Property | Value |
|----------|-------|
| Spark Master | `spark://spark-master:7077` |
| Spark Master UI | `http://localhost:8080` |
| Spark Worker UI | `http://localhost:8081` |
| Worker Memory | 2g |
| Worker Cores | 2 |

## Required Packages

The following Spark packages must be included via `--packages` when submitting jobs:

```
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
org.mongodb.spark:mongo-spark-connector_2.12:10.2.1
```

## spark-submit Examples

### Batch Processing (from host)
```bash
spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
  src/processing/run_batch_pipeline.py --stages all
```

### Batch Processing (from Docker)
```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
  /opt/bitnami/spark/src/processing/run_batch_pipeline.py --stages all
```

### Streaming Job
```bash
spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
  src/streaming/stream_processor.py
```

## Data Directories (inside Docker)

| Layer | Path |
|-------|------|
| Raw | `/opt/bitnami/spark/data/raw/` |
| Bronze | `/opt/bitnami/spark/data/bronze/` |
| Silver | `/opt/bitnami/spark/data/silver/` |
| Gold | `/opt/bitnami/spark/data/gold/` |
