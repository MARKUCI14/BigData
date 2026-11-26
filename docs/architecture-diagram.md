# Architecture Diagrams

The diagrams show the repository's implemented data paths. Historical temperature data is handled in the batch pipeline. Kafka currently carries Clouded Apollo observations, not weather events.

## End-to-end architecture

```mermaid
flowchart LR
  subgraph Sources[Source data]
    OBS[iNaturalist observation CSV]
    TEMP[Copernicus monthly county temperature CSV]
    REPLAY[Observation replay or synthetic generator]
  end

  subgraph Batch[Historical batch path]
    INGEST[Python and PySpark ingestion]
    BRONZE[(Bronze Parquet)]
    SILVER_JOB[PySpark validation, standardization, temperature unpivot]
    SILVER[(Silver Parquet)]
    GOLD_JOB[PySpark join, counts, trends, seasonal and correlation analytics]
    GOLD[(Gold Parquet)]
    MONGO_LOAD[Python MongoDB batch loader]
    OBS --> INGEST
    TEMP --> INGEST
    INGEST --> BRONZE --> SILVER_JOB --> SILVER --> GOLD_JOB --> GOLD --> MONGO_LOAD
  end

  subgraph Stream[Streaming observation path]
    PRODUCER[Python Kafka producer]
    KAFKA[Apache Kafka<br/>butterfly-observations]
    SPARK_STREAM[Spark Structured Streaming<br/>validation and micro-batch aggregation]
    DEAD[Dead-letter Parquet]
    REPLAY --> PRODUCER --> KAFKA --> SPARK_STREAM
    SPARK_STREAM --> DEAD
  end

  subgraph Serving[Serving and visualization]
    MONGO[(MongoDB<br/>observations, monthly_statistics,<br/>correlations, realtime_statistics)]
    DASH[Streamlit and Plotly dashboard]
  end

  MONGO_LOAD --> MONGO
  SPARK_STREAM --> MONGO
  MONGO --> DASH
  GOLD --> DASH
```

## Streaming flow

```mermaid
flowchart LR
  SOURCE[Observation CSV replay or synthetic events]
  PRODUCER[Python producer<br/>JSON event, county key]
  TOPIC[Kafka topic<br/>butterfly-observations]
  CONSUMER[Spark Structured Streaming<br/>schema validation and county normalization]
  VALID{Valid observation?}
  DEAD[Dead-letter Parquet<br/>reason, raw event, Kafka offset]
  MONGO_OBS[(MongoDB observations<br/>idempotent upsert)]
  AGG[Realtime county aggregation<br/>counts and recent events]
  MONGO_STATS[(MongoDB realtime_statistics)]
  DASH[Streamlit Real-Time Monitoring<br/>periodic MongoDB polling]

  SOURCE --> PRODUCER --> TOPIC --> CONSUMER --> VALID
  VALID -- no --> DEAD
  VALID -- yes --> MONGO_OBS
  VALID -- yes --> AGG --> MONGO_STATS
  MONGO_OBS --> DASH
  MONGO_STATS --> DASH
```

The historical batch path reads local source CSVs into Parquet and then MongoDB. Docker Compose defines the local service topology; see the [quick start](../README.md#quick-start) and [service configuration](../docker-compose.yml).
