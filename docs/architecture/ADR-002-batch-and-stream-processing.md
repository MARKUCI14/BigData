# ADR 002 Shared Spark Platform for Batch and Stream Processing

- Status: Accepted for the current implementation
- Date: 2026-11-26

## Context

The project brief requires historical batch processing and near-real-time processing. The proposal selects Apache Spark for batch workloads and Spark Structured Streaming for Kafka events.

## Decision

Use PySpark for historical ingestion, cleaning, standardization, county/month aggregation, and analytics. Use Spark Structured Streaming with `foreachBatch` for Kafka micro-batches. Keep the two workflows separate at their input boundaries while sharing the Spark ecosystem and data conventions.

## Consequences

One technology family covers both processing modes, which simplifies deployment and knowledge transfer. Kafka and MongoDB connector packages must match the Spark, Scala, and Java runtime. A Spark Compose image must be available and consistent for master and worker. The configured image tag currently prevents deployment and must be corrected before the architecture can be verified.

