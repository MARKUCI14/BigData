# ADR 005 Docker Compose Local Runtime

- Status: Implemented in Compose configuration; successful startup not verified
- Date: 2026-11-26

## Context

The team needs a reproducible local environment to demonstrate Kafka, Spark, MongoDB, and Streamlit working together. The project brief encourages containerization and deployment documentation.

## Decision

Use Docker Compose to define Zookeeper, Kafka, topic initialization, MongoDB, Spark master and worker, and Streamlit on a shared network with persistent Kafka and MongoDB volumes.

## Consequences

One Compose project describes service wiring and host ports. The current Spark image tag `bitnami/spark:3.5` is not pullable in the tested environment, so this decision is not validated until a supported image is selected consistently for Spark master and worker and the stack starts successfully.

