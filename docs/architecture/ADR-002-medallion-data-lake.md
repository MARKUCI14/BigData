# ADR-002: Organize historical data into Bronze, Silver, and Gold stages

- **Status:** Proposed
- **Date:** 2026-11-23

## Context

The supplied architecture and accepted proposal call for raw, cleaned/joined, and integrated analytical data stages.

## Decision

Adopt Bronze (source-preserving), Silver (validated and standardized), and Gold (analytical outputs) as the logical organization of lake data. The physical table format and storage engine remain undecided between Delta Lake and Apache Iceberg.

## Consequences

- Retain source provenance and make transformations repeatable.
- Define schemas, formats, paths, partitioning, retention, and recovery behavior during implementation.
- Do not claim a lakehouse technology is implemented until selected and present in the repository/runtime.
