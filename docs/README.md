# Project Documentation

This documentation defines the Clouded Apollo analytics platform, explains how its data moves through the system, and records important implementation decisions. It separates the architecture described by the project proposal from the parts currently present in the repository.

## Start here

| Document | What it defines |
| --- | --- |
| [System architecture](architecture/system-overview.md) | Components, data flows, and the known gap between the target stream and current code |
| [Data model](data-model.md) | Historical records, Kafka events, analytical outputs, and MongoDB collections |
| [Batch pipeline](pipelines/batch-pipeline.md) | Bronze, Silver, and Gold processing for historical data |
| [Streaming pipeline](pipelines/streaming-pipeline.md) | Kafka producer, Spark consumer, validation, and MongoDB writes |
| [Architecture decisions](architecture/) | Reasons for the selected storage, processing, event, serving, and deployment patterns |
| [Development process](dev-guide/development-process.md) | Issue-to-PR steps, branch workflow, validation, and release process |
| [Commit message template](templates/commit-message.txt) | Optional Git commit template |
| [GitHub pull request template](../.github/PULL_REQUEST_TEMPLATE.md) | Automatically suggested PR description |
| [GitHub issue templates](../.github/ISSUE_TEMPLATE/) | Feature/task and bug report forms |
| [Kafka configuration](kafka-config.md) | Topic, message transport, and connection details |
| [Spark configuration](spark-config.md) | Cluster configuration and connector packages |
| [Git flow](Git-Flow.md) | Branch and integration workflow |

## Status vocabulary

- **Target** means required or planned by the proposal, course brief, or architecture diagram.
- **Implemented** means corresponding code or configuration exists in this repository. It does not mean the component has passed an end-to-end run.
- **Gap** means the current implementation differs from the target or still needs verification.

The Docker Compose stack has not yet been verified end to end. Its Spark base image reference currently fails to pull (`bitnami/spark:3.5`), so statements about the stack describe its configuration, not a successful deployment.

