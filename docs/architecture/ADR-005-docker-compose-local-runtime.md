# ADR 005 Docker Compose Local Runtime

- Status: Implemented in Compose configuration; successful startup not yet verified with the selected Spark image
- Date: 2026-11-26

## Context

The team needs a reproducible local environment to demonstrate Kafka, Spark, MongoDB, and Streamlit working together. The project brief encourages containerization and deployment documentation.

## Decision

Use Docker Compose to define Zookeeper, Kafka, topic initialization, MongoDB, Spark master and worker, and Streamlit on a shared network with persistent Kafka and MongoDB volumes. Build the Spark master and worker from one custom image based on the Docker Official Image `spark:3.5.9-scala2.12-java17-python3-ubuntu` so their Spark and Scala versions match.

## Consequences

One Compose project describes service wiring and host ports. The selected Spark image replaces the previously unpullable `bitnami/spark:3.5` tag. Docker startup and service health still need to be verified on the target machine; the configuration alone does not establish that the full stack starts successfully.

