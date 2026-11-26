# ADR 004 MongoDB Serving Store

- Status: Accepted for the current implementation
- Date: 2026-11-26

## Context

The dashboard needs direct access to processed historical summaries and recent stream results without recomputing the complete Parquet data lake for every interaction.

## Decision

Use MongoDB as a serving store for observation documents, monthly statistics, correlation results, and per-county real-time statistics. Use stable unique identities and indexes for the principal lookup keys. Keep Parquet as the historical analytical record; MongoDB is a query-serving representation.

## Consequences

The dashboard can query precomputed records with low latency, while the batch outputs remain reproducible from their source files. MongoDB data must be reloadable or rebuildable from pipeline outputs. The initializer currently creates four collections, including `correlations`; this is more specific than the three collections in the preliminary proposal.

