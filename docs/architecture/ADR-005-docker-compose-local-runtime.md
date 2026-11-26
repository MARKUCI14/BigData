# ADR 005 Docker Compose Local Runtime

- Status: Implemented in Compose configuration; successful startup not verified
- Date: 2026-11-26

## Context

The team needs a reproducible local environment to demonstrate Kafka, Spark, MongoDB, and Streamlit working together. The project brief encourages containerization and deployment documentation.

## Decision

Use Docker Compose to define Zookeeper, Kafka, topic initialization, MongoDB, Spark master and worker, and Streamlit on a shared network with persistent Kafka and MongoDB volumes.

## Consequences

One Compose project describes service wiring and host ports. Spark master and worker use the Apache-published `apache/spark:3.5.3-python3` base through the same local image build, with explicit standalone master/worker commands. The stack still needs a fresh pull and startup check on the target machine.

