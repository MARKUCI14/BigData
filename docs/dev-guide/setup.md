# Setup and development

## Current status

The repository currently has documentation and a Python-oriented `.gitignore`, but no application source, dependency manifest, Docker configuration, or runnable pipeline. Therefore there is no verified project install or run command yet. Avoid installing a guessed stack before the team finalizes dependencies.

## Prerequisites (planned)

- Git.
- A Python version to be selected and documented with the dependency manifest.
- Runtime services required by the final design (potentially Kafka, MongoDB, and Spark). Their versions and local/cloud startup procedure are not yet defined.
- Authorized historical source datasets and any API credentials needed by the selected weather source. Keep credentials in local environment configuration and out of version control.

## Current repository orientation

```text
.
├── CLAUDE.md
├── README.md
└── docs/
```

There are no project-specific setup commands until implementation is added. When a Python package/dependency file and runnable entry points are introduced, document a fresh-clone workflow here, including service startup, environment variables, data placement, batch run, stream run, dashboard run, and cleanup.

## Development workflow to establish

1. Agree on the Python version and dependency manager; commit the manifest and appropriate lock file.
2. Document service versions and a reproducible local topology, preferably through a compose file if the team selects containers.
3. Provide an example environment file with names and safe placeholder values only; never commit real credentials.
4. Document how to acquire/place the source datasets without committing restricted or large data unintentionally.
5. Add exact commands for data validation, batch processing, streaming, dashboard launch, and any quality checks when those commands exist.
6. Record expected outputs and troubleshooting guidance, including how to reset local state safely.

## Configuration checklist for implementation

Document required environment variables, defaults, units, paths, service endpoints, Kafka topics, MongoDB database/collections, lake storage location, and checkpoint location. Validate required settings at startup and provide actionable errors. Do not expose secrets in logs.
