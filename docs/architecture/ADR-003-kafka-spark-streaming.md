# ADR-003: Use Kafka and Spark Structured Streaming for the stream demonstration

- **Status:** Proposed
- **Date:** 2026-11-23

## Context

The course requires a real or realistically simulated streaming source and a near-real-time processing workflow. The accepted proposal and diagram show incoming weather events flowing through Kafka and Spark Structured Streaming.

## Decision

Use Kafka as the proposed event transport and Spark Structured Streaming as the proposed event processor, subject to local runtime and course-demo feasibility review.

## Consequences

- Define event schema, topic, serialization, timestamps, checkpointing, delivery/replay behavior, and error handling before declaring the workflow reproducible.
- Select the weather API or simulation and document how it represents realistic incoming data.
- Distinguish live weather summaries from the historical county/month analysis when their measurements or grain are not comparable.
