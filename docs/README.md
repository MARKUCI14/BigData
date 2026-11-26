# Project Documentation

This documentation defines the Clouded Apollo analytics platform, explains how its data moves through the system, and records important implementation decisions. It separates the architecture described by the project proposal from the parts currently present in the repository.

## Start here

| Document | What it defines |
| --- | --- |
| [System architecture](architecture/system-overview.md) | Components, data flows, and the known gap between the target stream and current code |
| [Data model](data-model.md) | Historical records, Kafka events, analytical outputs, and MongoDB collections |
| [Architecture diagrams](architecture-diagram.md) | Batch and streaming flow diagrams |
| [Data dictionary](data-dictionary.md) | Field definitions across Bronze, Silver, and Gold |
| [Presentation notes](presentation-notes.md) | Five-minute outline, demo sequence, and anticipated questions |
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

The Docker Compose stack and dashboard pages are implemented. A Compose definition documents configuration; it does not by itself establish that all images build or services pass a live end-to-end run. Validate the stack on the target machine before a demonstration. The Kafka stream currently carries butterfly observations; the proposal's weather-event stream remains an architecture gap.

For project setup, team details, data sources, pipeline stages, and limitations, see the [main README](../README.md). The [architecture diagrams](architecture-diagram.md) and [data dictionary](data-dictionary.md) provide the system flow and processing-layer field definitions.

