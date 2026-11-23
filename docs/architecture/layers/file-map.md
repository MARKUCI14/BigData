# Planned repository file map

This map connects the issue backlog to planned files and their responsibilities. It is a planning aid, not a claim that these files or components already exist. Current top-level code has not yet been established; check the tree before creating files and update this map if the team chooses a different structure.

## Project and infrastructure

| Planned file/path | Responsibility | Issue |
| --- | --- | --- |
| `README.md` | Project entry point, research scope, architecture pointer, verified quick start. | [#1](https://github.com/MARKUCI14/BigData/issues/1), [#25](https://github.com/MARKUCI14/BigData/issues/25) |
| `requirements.txt` or `pyproject.toml` | Declared Python dependencies and project metadata; select one package workflow. | [#1](https://github.com/MARKUCI14/BigData/issues/1), [#23](https://github.com/MARKUCI14/BigData/issues/23) |
| `.env.example` | Safe placeholder names for required local configuration; never real credentials. | [#1](https://github.com/MARKUCI14/BigData/issues/1) |
| `docker-compose.yml` | Local service topology, networks, volumes, ports, and health checks. | [#2](https://github.com/MARKUCI14/BigData/issues/2) |
| `docker/streamlit/Dockerfile` | Build image for the Streamlit app if containerized UI remains selected. | [#2](https://github.com/MARKUCI14/BigData/issues/2) |
| `docker/mongo/init-mongo.js` | Initialize MongoDB collections/indexes on first service initialization. | [#4](https://github.com/MARKUCI14/BigData/issues/4) |
| `config/kafka-config.md` | Explain topic, broker addresses, serialization, and retention. | [#3](https://github.com/MARKUCI14/BigData/issues/3) |
| `config/spark-config.md` | Document Spark runtime, submit parameters, and connector compatibility. | [#5](https://github.com/MARKUCI14/BigData/issues/5) |

## Data and Python application

| Planned file/path | Responsibility | Issue |
| --- | --- | --- |
| `data/raw/` | Approved source files or local placement instructions. | [#1](https://github.com/MARKUCI14/BigData/issues/1) |
| `data/bronze/`, `data/silver/`, `data/gold/` | Generated lakehouse layers; normally ignored by Git. | [#6](https://github.com/MARKUCI14/BigData/issues/6)–[#12](https://github.com/MARKUCI14/BigData/issues/12) |
| `src/ingestion/ingest_observations.py` | Batch iNaturalist-to-Bronze loader. | [#6](https://github.com/MARKUCI14/BigData/issues/6) |
| `src/ingestion/ingest_temperatures.py` | Batch Copernicus-to-Bronze loader. | [#7](https://github.com/MARKUCI14/BigData/issues/7) |
| `src/ingestion/weather_producer.py` (candidate) | Weather API/simulator-to-Kafka producer; reconcile with current issue name/scope. | [#8](https://github.com/MARKUCI14/BigData/issues/8) |
| `scripts/test_kafka_producer.py`, `scripts/test_kafka_consumer.py` | Minimal infrastructure smoke checks, distinct from production ingestion. | [#3](https://github.com/MARKUCI14/BigData/issues/3) |
| `scripts/test_spark_job.py` | Spark cluster/connectivity smoke job. | [#5](https://github.com/MARKUCI14/BigData/issues/5) |
| `src/processing/bronze_to_silver_observations.py` | Validate and standardize observation rows. | [#9](https://github.com/MARKUCI14/BigData/issues/9) |
| `src/processing/bronze_to_silver_temperatures.py` | Reshape and validate temperature data. | [#10](https://github.com/MARKUCI14/BigData/issues/10) |
| `config/county_mapping.json` | Explicit county aliases to canonical identifiers after source inspection. | [#9](https://github.com/MARKUCI14/BigData/issues/9), [#10](https://github.com/MARKUCI14/BigData/issues/10) |
| `src/processing/silver_to_gold.py` | Build integrated county-month dataset and documented summaries. | [#11](https://github.com/MARKUCI14/BigData/issues/11), [#12](https://github.com/MARKUCI14/BigData/issues/12) |
| `src/processing/compute_correlations.py` (optional separate module) | Implement an agreed statistical analysis without duplicating Gold aggregation. | [#12](https://github.com/MARKUCI14/BigData/issues/12) |
| `src/processing/run_batch_pipeline.py` | Run batch stages in order and report status. | [#13](https://github.com/MARKUCI14/BigData/issues/13) |
| `scripts/run_pipeline.sh` | Convenience wrapper for the agreed Spark submission/runtime configuration. | [#13](https://github.com/MARKUCI14/BigData/issues/13) |
| `src/storage/load_to_mongodb.py` | Idempotently load Gold serving outputs. | [#14](https://github.com/MARKUCI14/BigData/issues/14) |
| `src/streaming/stream_processor.py` | Consume Kafka events, validate, checkpoint, and write stream outputs. | [#15](https://github.com/MARKUCI14/BigData/issues/15) |
| `src/streaming/realtime_aggregator.py` | Maintain idempotent recent stream summaries. | [#16](https://github.com/MARKUCI14/BigData/issues/16) |
| `src/storage/mongodb_client.py` | Shared dashboard read/query layer. | [#17](https://github.com/MARKUCI14/BigData/issues/17) |
| `src/dashboard/app.py` | Streamlit entry point and navigation. | [#18](https://github.com/MARKUCI14/BigData/issues/18) |
| `src/dashboard/pages/historical_analysis.py` | Historical observation dashboard page. | [#19](https://github.com/MARKUCI14/BigData/issues/19) |
| `src/dashboard/pages/temperature_analysis.py` | Climate dashboard page. | [#20](https://github.com/MARKUCI14/BigData/issues/20) |
| `src/dashboard/pages/correlation_analysis.py` | Qualified temperature/observation association page. | [#21](https://github.com/MARKUCI14/BigData/issues/21) |
| `src/dashboard/pages/realtime_monitoring.py` | Stream freshness and recent weather summary page. | [#22](https://github.com/MARKUCI14/BigData/issues/22) |

## Quality, documentation, and delivery

| Planned file/path | Responsibility | Issue |
| --- | --- | --- |
| `tests/test_ingestion.py`, `tests/test_bronze_to_silver.py`, `tests/test_silver_to_gold.py`, `tests/test_correlations.py`, `tests/test_mongodb_client.py` | Focused unit-level checks using small synthetic fixtures. | [#23](https://github.com/MARKUCI14/BigData/issues/23) |
| `tests/test_integration.py`, `tests/fixtures/`, optional `docker-compose.test.yml` | Service-backed end-to-end verification with deterministic, authorized fixtures. | [#24](https://github.com/MARKUCI14/BigData/issues/24) |
| `docs/data-dictionary.md` | Field-level definitions across implemented layers; link to `docs/data-model.md`. | [#25](https://github.com/MARKUCI14/BigData/issues/25) |
| `docs/architecture-diagram.md` or `docs/architecture/overview.md` | Maintain one canonical architecture diagram and link to it from README. | [#26](https://github.com/MARKUCI14/BigData/issues/26) |
| `docs/presentation-notes.md`, `docs/assets/screenshots/` | Demo narrative and approved dashboard screenshots, if required. | [#27](https://github.com/MARKUCI14/BigData/issues/27) |
| `docs/README.md`, `docs/requirements.md`, `docs/architecture/`, `docs/data-model.md`, `docs/technical-decisions.md`, `docs/dev-guide/setup.md` | Central documentation index and design/setup sources of truth. | [#28](https://github.com/MARKUCI14/BigData/issues/28) |

## File ownership rules

- One issue owns a deliverable; related issues link to it rather than restating its full design.
- Prefer modifying an established source-of-truth document over creating a duplicate schema or architecture file.
- A file listed here is a planned path. If implementation uses another path, update the issue, this map, and the relevant layer guide in the same pull request.
- Never mark planned files as implemented until they exist and have been reviewed.
