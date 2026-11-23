# Technical decisions

This log separates accepted project direction from decisions still to be made. Add dated entries as implementation choices are finalized.

Detailed records for the current proposed direction are in [architecture ADRs](architecture/): Python language, medallion data stages, Kafka/Spark streaming, MongoDB serving, and Streamlit visualization.

## Current direction

| Topic | Direction | Rationale/status |
| --- | --- | --- |
| Language | Python | Team-selected implementation language. |
| Research use case | Clouded Apollo observations and Romanian temperature, at county/month grain | Accepted proposal; date range 2000–2026 subject to data completeness. |
| Architecture | Batch plus near-real-time processing | Required to demonstrate historical and streaming workflows for the course. |
| Batch/stream technologies | Spark; Kafka and Spark Structured Streaming | Proposed in accepted architecture; validate feasibility and finalize versions/configuration. |
| Lakehouse | Delta Lake or Apache Iceberg | Proposal leaves table format open. |
| Serving | MongoDB | Proposed speed/serving store; confirm whether needed for actual dashboard query workload. |
| UI | Streamlit | Proposed visualization interface. |
| Weather stream | Weather API or realistic simulation | Source and event contract open. |
| Historical data | iNaturalist and Copernicus Climate Data Store | Accepted proposal sources; obtain and document exact extracts and licenses/terms. |

## Open decisions

| ID | Decision needed | What to evaluate |
| --- | --- | --- |
| D-01 | Local/cloud execution and deployment topology | Course demo constraints, team hardware, reproducibility, and operational complexity. |
| D-02 | Lakehouse table format and storage | Delta Lake vs Iceberg compatibility with selected Spark/runtime and local setup. |
| D-03 | Streaming input and event schema | API availability vs simulation, realistic event fields/rate, timestamp semantics. |
| D-04 | Data source snapshots and acquisition | Exact files, source query/filters, licenses/terms, reproducibility, 2026 partial-year handling. |
| D-05 | County normalization and climate/observation join | Canonical geography, boundary/version assumptions, unmatched records, temporal alignment. |
| D-06 | Statistical analysis | Temperature metric, count treatment, trend/association method, confounders, uncertainty and caveats. |
| D-07 | MongoDB collections, indexes, and Gold synchronization | Actual dashboard query patterns, idempotency, retention, whether separate live and historical collections are needed. |
| D-08 | Dependencies and Python environment | Packaging tool, supported Python version, pinned runtime versions, Spark/Kafka compatibility. |
| D-09 | Testing and quality checks | Validation strategy for transforms, schemas, batch/stream behavior, and reproducibility. |

## Decision record template

For significant choices, add a record containing:

- **Date and status:** proposed, accepted, superseded.
- **Context:** problem and constraints.
- **Decision:** chosen approach.
- **Alternatives:** reasonable options considered.
- **Consequences:** tradeoffs, operational implications, and follow-up work.
- **Evidence:** relevant source files, benchmarks, or compatibility checks.
