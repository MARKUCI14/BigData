# Project guidance for Claude

## Project context

This is Team Work Dream Work's Big Data course project. It studies the association between environmental temperature and reported Clouded Apollo (*Parnassius mnemosyne*) observations in Romania, at monthly and county level for 2000–2026 where data is available. The repository currently contains planning documentation, not an implemented pipeline.

## Source of truth

- Read `docs/README.md` and the relevant topic documents before making project changes.
- The accepted proposal and course brief set the research scope and deliverables. The proposal's architecture is preliminary; implementation decisions can be refined and must be recorded in `docs/technical-decisions.md`.
- Treat code and configuration in the repository as the truth for what is implemented. Keep documentation explicit about planned versus implemented behavior.
- Do not invent existing modules, commands, schemas, data files, services, or dependency versions. If implementation context is missing, inspect the repository and state assumptions.

## Research and data handling

- The main question is association, not causation. Do not describe the result as proof that temperature causes population change.
- iNaturalist observation counts represent reported occurrences and are affected by reporting effort, access, seasonality, and other factors. Never label them as actual population size without a defensible independent measure.
- Preserve source provenance, source time/location fields, and transformation history. Document missing data, coverage limits, cleaning rules, and any aggregation or join assumptions.
- Keep raw source data logically separate from cleaned and analytical outputs. Do not commit credentials, API tokens, or private data.
- Distinguish historical batch processing from incoming stream processing in code, docs, and demonstrations.

## Engineering guidance

- Python is the chosen implementation language. Proposed technologies include Kafka, Spark/Spark Structured Streaming, a Delta Lake or Apache Iceberg data lake, MongoDB, and Streamlit; these are not confirmed as implemented or finalized. Check project files and documented decisions before relying on them.
- Prefer small, clearly named modules with explicit input/output contracts. Make processing repeatable and configuration external to source code.
- Add or update documentation when interfaces, schemas, setup steps, architecture, or decisions change. Record significant choices and alternatives in `docs/technical-decisions.md`.
- Avoid adding dependencies, infrastructure, or services without a project need. Never put secrets in source control.
- Any generative AI assistance used for project work must be acknowledged and documented, as required by the course brief.

## Course outcome

The end-to-end project should demonstrate heterogeneous data sources (including a real or realistically simulated stream), ingestion, historical and near-real-time processing, storage, meaningful analytics, visualizations, reproducible execution, deployment instructions, and team understanding of the architecture. See `docs/requirements.md` and `docs/course-deliverables.md`.
