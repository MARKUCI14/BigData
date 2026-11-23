# ADR-001: Use Python as the project implementation language

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

The team selected Python for the project. The proposed platform includes Spark processing and a Streamlit interface, both of which have Python integration options.

## Decision

Use Python for project-owned ingestion, processing orchestration, and visualization code, subject to compatibility with the final selected runtimes and libraries.

## Consequences

- Select and document a supported Python version and dependency manager before implementation setup is considered reproducible.
- Check runtime compatibility for Spark, Kafka clients, and other libraries when versions are chosen.
- This decision does not determine storage formats or service deployment choices.
