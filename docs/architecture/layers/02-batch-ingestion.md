# Layer 2: Batch ingestion

## Responsibility

Load the historical iNaturalist and Copernicus inputs into the lake's raw/Bronze stage while preserving source values, provenance, and ingestion metadata. Ingestion should validate the minimum structure needed to read the source; substantive normalization belongs in Silver processing.

## Planned files

| File | Purpose | Owning issue |
| --- | --- | --- |
| `src/ingestion/ingest_observations.py` | Read and validate the observation CSV; attach ingestion metadata; write the Bronze observation dataset. | [#6](https://github.com/MARKUCI14/BigData/issues/6) |
| `src/ingestion/ingest_temperatures.py` | Read the climate CSV and preserve source layout/metadata in Bronze. | [#7](https://github.com/MARKUCI14/BigData/issues/7) |
| `data/raw/` | Team-approved raw inputs or documented local-only placement; avoid committing restricted or large extracts without agreement. | [#1](https://github.com/MARKUCI14/BigData/issues/1), [#6](https://github.com/MARKUCI14/BigData/issues/6), [#7](https://github.com/MARKUCI14/BigData/issues/7) |
| `data/bronze/` | Generated source-preserving Parquet or selected lakehouse tables; should normally be ignored by Git. | [#6](https://github.com/MARKUCI14/BigData/issues/6), [#7](https://github.com/MARKUCI14/BigData/issues/7) |

## Implementation contract

Document required columns, source type/units, invalid-row behavior, idempotency key, write mode, and run summary. Keep county canonicalization and analytical joins out of ingestion unless an ADR explicitly places them there. Use configuration for paths and service endpoints; do not hard-code secrets.

## Handoff

Bronze outputs feed [batch storage and processing](03-batch-storage.md). The downstream transforms must be able to trace rows back to their source and distinguish null/missing values from genuine zero values.
