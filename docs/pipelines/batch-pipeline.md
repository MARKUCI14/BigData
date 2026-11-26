# Historical Batch Pipeline

## Purpose

The batch pipeline processes the historical iNaturalist and Copernicus datasets repeatedly and produces county/month analytical outputs. It is orchestrated by `src/processing/run_batch_pipeline.py`.

## Stages and data layers

| Stage | Input | Output | Definition |
| --- | --- | --- | --- |
| `ingest-observations` | Raw iNaturalist CSV | `data/bronze/observations/` | Read and write source observations into the Bronze layer. |
| `ingest-temperatures` | Raw Copernicus CSV | `data/bronze/temperatures/` | Read and write monthly county temperature records into Bronze. |
| `silver-observations` | Bronze observations | `data/silver/observations/` | Validate and standardize observation dates, coordinates, and county names. |
| `silver-temperatures` | Bronze temperatures | `data/silver/temperatures/` | Validate and standardize temperature records and county names. |
| `gold-monthly-statistics` | Both Silver datasets | `data/gold/monthly_county_stats/` | Join/aggregate at county and month grain for analytical use. |
| `gold-analytics` | Gold monthly statistics | correlations, trends, seasonal outputs, metadata under `data/gold/` | Calculate descriptive and association metrics. |
| `gold-to-mongodb` | Gold outputs | MongoDB serving collections | Load precomputed results for low-latency dashboard queries. |

Parquet is the current file output format. Bronze, Silver, and Gold describe data quality and intended use, not separate storage products. The repository currently uses local filesystem folders rather than Delta Lake or Apache Iceberg tables.

## Running the pipeline

From the repository root, after dependencies and a Spark runtime are available:

```bash
spark-submit src/processing/run_batch_pipeline.py --stages all
```

The orchestrator supports `all`, `ingest`, `bronze-to-silver`, and `silver-to-gold` stage groups, plus `--force`, `--continue-on-error`, and `--dry-run`. Review `docs/spark-config.md` for cluster and connector configuration.

## Data quality and interpretation

- Preserve raw inputs so transformations can be repeated.
- Standardize county aliases through `config/county_mapping.json` before joining datasets.
- Keep missing values distinct from measured zero values.
- Report the covered date range and available county-months for every metric.
- Treat observation totals as reported sightings, not population estimates.
- Treat correlations as associations rather than causal effects.

## Verification status

The code defines these stages and output paths. A successful end-to-end execution has not yet been established in Docker; see [project documentation status](../README.md).

