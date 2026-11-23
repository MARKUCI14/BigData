# Layer 4: Batch processing

## Responsibility

Use Spark to transform historical Bronze datasets into standardized Silver data and integrated Gold analytics. The target grain for the research dataset is county by calendar month, subject to compatible source coverage.

## Planned files

| File | Purpose | Owning issue |
| --- | --- | --- |
| `src/processing/bronze_to_silver_observations.py` | Validate and standardize observation records; apply documented county/date/coordinate rules. | [#9](https://github.com/MARKUCI14/BigData/issues/9) |
| `src/processing/bronze_to_silver_temperatures.py` | Convert the source climate table into a typed county/year/month representation and flag quality issues. | [#10](https://github.com/MARKUCI14/BigData/issues/10) |
| `src/processing/silver_to_gold.py` | Aggregate reported observations and integrate with climate values on defined county/year/month keys. | [#11](https://github.com/MARKUCI14/BigData/issues/11) |
| `src/processing/compute_correlations.py` or documented functions in `silver_to_gold.py` | Produce research summaries only after the method and interpretation limits are agreed. | [#12](https://github.com/MARKUCI14/BigData/issues/12) |
| `src/processing/run_batch_pipeline.py` | Run selected batch stages in dependency order and report stage outcomes. | [#13](https://github.com/MARKUCI14/BigData/issues/13) |

## Processing decisions to settle

- Canonical county key and source-to-canonical mapping.
- Handling of duplicate IDs, missing dates/counties, coordinate outliers, and unmatched counties.
- Whether the Gold data includes a complete county-month grid; if so, define the county universe and distinguish true zero reports from missing source coverage.
- Climate statistic, unit, date alignment, and how partial 2026 coverage is represented.
- Statistical analysis suitable for sparse and unevenly reported counts; avoid causal language.
- Orchestrator stage names, dependencies, failure policy, and idempotent output behavior.

## Output contract

Every output should document grain, schema, units, keys, quality/missingness semantics, and source lineage. The Gold model is described conceptually in [data-model.md](../../data-model.md).
