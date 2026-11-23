# ADR-004: Use MongoDB as a low-latency serving store

- **Status:** Proposed
- **Date:** 2026-11-23

## Context

The proposed architecture places MongoDB between processing and the Streamlit dashboard for fast access to recent and analytical summaries.

## Decision

Retain MongoDB as the proposed speed/serving store pending validation against actual dashboard queries and deployment constraints.

## Consequences

- Define collection/document grain, indexes, idempotent writes, synchronization, and retention from query needs.
- Decide whether it serves only live summaries or also synchronized historical Gold outputs.
- Reconsider if the extra service does not materially support the course demonstration.
