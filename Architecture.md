# Clouded Apollo Analytics Architecture

This document summarizes the architecture for analyzing the relationship between Romanian county-level temperatures and reported Clouded Apollo (*Parnassius mnemosyne*) observations. The project combines historical batch analytics with a near-real-time data path. Observation counts describe recorded sightings; they are not direct population estimates, and correlation does not establish causation.

## Architecture layers

| Layer | Definition | Project technology |
| --- | --- | --- |
| Data sources | Historical butterfly sightings and monthly climate observations; the target live source is incoming weather data. | iNaturalist CSV, Copernicus CSV, Weather API or realistic simulation |
| Ingestion | Loads historical source files and transports live events. | Python and Apache Kafka |
| Batch storage | Preserves source-aligned, standardized, and analytical datasets for repeatable processing. | Local Parquet data lake organized into Bronze, Silver, and Gold folders |
| Batch processing | Validates, standardizes, aggregates, joins, and analyzes historical data. | PySpark |
| Real-time processing | Reads Kafka events in micro-batches, validates them, and updates recent results. | Spark Structured Streaming |
| Serving storage | Stores query-ready records for low-latency dashboard access. | MongoDB |
| Visualization | Presents county, monthly, trend, correlation, and recent-event results. | Streamlit and Plotly |
| Local deployment | Connects the services for development and demonstration. | Docker Compose |

## Historical path

The observation and temperature CSV files in `data/raw/` are ingested separately. Bronze outputs preserve source-oriented records in Parquet. Silver transformations standardize counties and dates; the temperature transform converts the wide month columns into one county/year/month/temperature record per month. Gold processing joins monthly observation counts to the county/month temperature records, then creates trend, seasonal, and correlation outputs. Gold Parquet files are also loaded into MongoDB for dashboard queries.

## Near-real-time path

The target design in the proposal and architecture image sends incoming weather measurements through Kafka to Spark Structured Streaming, then serves recent results through MongoDB and Streamlit. The current code instead replays or synthesizes butterfly observation events on the `butterfly-observations` topic. This difference is an open architecture gap. The team must either implement the weather event path or revise the proposal and diagram to make butterfly observation events the intended stream.

## Definitions and decisions

- **Bronze**: ingested records kept close to their source form.
- **Silver**: validated and standardized data suitable for integration and analysis.
- **Gold**: curated analytical outputs intended for reporting and visualization.
- **Data lake**: the project’s file-based analytical storage. The current implementation uses Parquet on local folders; Delta Lake and Iceberg appear as alternatives in the proposal diagram, but are not configured.
- **Speed/serving store**: MongoDB collections used to query selected processed records and summaries quickly. It complements the Parquet history rather than replacing it.
- **Streaming micro-batch**: a small set of Kafka events processed together by Structured Streaming on a recurring trigger.
- **Dead letter**: a persisted record of an event that failed parsing or validation, with its rejection reason and Kafka offset metadata.
- **Observation count**: number of records in the supplied citizen-science dataset for a county and month, not a count of all butterflies in nature.

## Implementation status

The batch and streaming modules, MongoDB initialization, and Compose configuration are present. The dashboard is still a placeholder. The Compose stack has not completed startup because the configured Spark image tag `bitnami/spark:3.5` cannot be pulled in the tested environment. Therefore the repository contains the pipeline definition, but the full architecture has not yet been validated end to end.

For detailed component flow, schemas, and decisions, see the [documentation index](docs/README.md), [system architecture](docs/architecture/system-overview.md), [data model](docs/data-model.md), [batch pipeline](docs/pipelines/batch-pipeline.md), and [streaming pipeline](docs/pipelines/streaming-pipeline.md).
