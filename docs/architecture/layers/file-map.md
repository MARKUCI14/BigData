# Repository file map

This map connects the project's architecture layers to their current repository paths. The batch data lake uses local Parquet files organized into Bronze, Silver, and Gold layers.

| Area | Path | Purpose |
| --- | --- | --- |
| Raw source data | `data/raw/` | Version-controlled iNaturalist observations and Copernicus county temperature CSVs. |
| Bronze data | `data/bronze/` | Source-oriented ingested Parquet outputs. Generated files are Git-ignored. |
| Silver data | `data/silver/` | Validated and standardized Parquet outputs. Generated files are Git-ignored. |
| Gold data | `data/gold/` | Aggregated analytical Parquet outputs. Generated files are Git-ignored. |
| Batch and streaming ingestion | `src/ingestion/` | CSV ingestion and Kafka observation producer scripts. |
| Batch processing | `src/processing/` | Spark jobs for Bronze-to-Silver and Silver-to-Gold transformations and analytics. |
| Stream processing | `src/streaming/` | Spark Structured Streaming consumer and real-time aggregation. |
| Serving storage | `src/storage/` | MongoDB connection and load utilities. |
| Dashboard | `src/dashboard/` | Streamlit application. |
| Application configuration | `config/` | County mapping and Spark defaults. |
| Container definitions | `docker/` | Custom Spark and Streamlit images and MongoDB initialization. |
| Local orchestration | `docker-compose.yml` | Local Kafka, Spark, MongoDB, and dashboard services. |
| Utility scripts | `scripts/` | Repository helper scripts. |
| Tests | `tests/` | Unit and integration test suite. |
| Project documentation | `docs/` | Architecture, data model, pipeline, configuration, and development guides. |

The proposal's weather-event stream and the currently implemented butterfly-observation stream differ. See [the system overview](../system-overview.md) for that architecture gap and the current component status.
