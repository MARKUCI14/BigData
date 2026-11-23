# Architecture layer guides

These guides explain the responsibility, boundaries, and planned code locations for each part of the accepted architecture. The repository is still at the documentation stage, so paths below are **planned locations** unless the guide identifies a file as present. Confirm source schemas and implementation choices before treating examples as final contracts.

## Guides

- [Data sources](01-data-sources.md) — historical ecological/climate inputs and the incoming weather stream.
- [Batch ingestion](02-batch-ingestion.md) — loading historical inputs into raw/Bronze storage.
- [Batch storage and lakehouse](03-batch-storage.md) — Bronze, Silver, and Gold responsibilities and formats.
- [Batch processing](04-batch-processing.md) — Spark transformations and orchestration.
- [Streaming ingestion](05-stream-ingestion.md) — producing weather events and transporting them through Kafka.
- [Stream processing](06-stream-processing.md) — validating and aggregating incoming events with Spark Structured Streaming.
- [Speed/serving storage](07-serving-storage.md) — MongoDB's low-latency query role and data ownership.
- [Serving and visualization](08-serving-visualization.md) — dashboard queries, pages, and interpretation requirements.
- [Planned repository file map](file-map.md) — what each anticipated file is for and the issue that owns it.

## Data flow at a glance

```mermaid
flowchart LR
  I[iNaturalist historical observations] --> BI[Python batch ingestion]
  C[Copernicus historical climate] --> BI
  BI --> B[Bronze raw source stage]
  B --> P[Spark batch processing]
  P --> S[Silver validated and standardized]
  S --> G[Gold county-month analytical outputs]
  G --> M[MongoDB serving summaries]
  G --> UI[Streamlit historical analytics]
  W[Weather API or simulated weather events] --> K[Kafka]
  K --> ST[Spark Structured Streaming]
  ST --> M
  ST --> G2[Lakehouse stream outputs/checkpoints]
  M --> UI
```

The accepted proposal and diagram define the streaming input as weather data. Some existing GitHub issues describe a butterfly-observation stream instead; resolve that mismatch before implementing those tickets. The issue file lists are intended to guide implementation and review, not silently change an issue's acceptance criteria.
