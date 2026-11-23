# Layer 3: Batch storage and lakehouse

## Responsibility

Retain historical inputs and progressively refined datasets so processing can be repeated, audited, and queried for historical analysis.

## Logical stages

| Stage | Meaning | Expected content |
| --- | --- | --- |
| Bronze | Source-preserving | Raw-ish observation and climate records plus source and ingestion metadata. |
| Silver | Validated and standardized | Typed dates/numbers, documented county keys, quality indicators, and long-form county/month temperature rows. |
| Gold | Integrated analytical outputs | County/month observation counts joined with temperature, plus explicitly defined trends/seasonal summaries. |

## Planned locations

- `data/bronze/observations/` and `data/bronze/temperatures/`
- `data/silver/observations/` and `data/silver/temperatures/`
- `data/gold/monthly_county_stats/`, with additional analytics paths only if adopted
- `data/checkpoints/` for stream checkpoints (runtime state; exclude from Git)

These paths come from the issue backlog and remain subject to the final table format and storage design. Delta Lake versus Apache Iceberg is not settled. Do not commit generated datasets/checkpoints by default; document source-data sharing rules separately.

## Design rules

- Keep a reproducible transformation path from Bronze to Silver to Gold.
- Define schemas and keys from actual source files, not example records alone.
- Document partition strategy and why it helps expected query/processing patterns; avoid over-partitioning small datasets.
- Track quality flags and missingness. A county-month with no observation is not equivalent to a missing temperature month.
- Preserve processing/source version metadata where useful for lineage and re-runs.

## Related guides

See [batch ingestion](02-batch-ingestion.md), [batch processing](04-batch-processing.md), [stream processing](06-stream-processing.md), and the [planned file map](file-map.md).
