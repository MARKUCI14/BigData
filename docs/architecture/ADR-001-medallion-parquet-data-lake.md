# ADR 001 Medallion Data Lake with Parquet

- Status: Accepted for the current implementation
- Date: 2026-11-26

## Context

The project combines historical observation and temperature files and needs traceable, repeatable transformations. The supplied architecture diagram names Delta Lake or Apache Iceberg as possibilities, while the repository stores data in local `data/` directories.

## Decision

Use Bronze, Silver, and Gold as logical data-quality stages and Parquet as the current file format. Keep the original CSV sources in `data/raw/`; write ingested, standardized, and analytical outputs to their corresponding folders under `data/`.

## Consequences

Spark can read the columnar outputs and each stage can be recomputed from retained inputs. The current implementation does not provide transactional table semantics, schema evolution, or time travel from Delta Lake or Iceberg. Adopt one of those table formats only through a separate decision and compatible runtime setup.

