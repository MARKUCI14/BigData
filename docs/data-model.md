# Database and data model

## Status

No database or schema is implemented in the repository yet. This document describes a **conceptual** model derived from the proposal; revise it against real source files and code before treating it as final.

## Conceptual entities

| Entity | Grain | Candidate attributes |
| --- | --- | --- |
| Butterfly observation | One source observation | Source observation ID, taxon/species, observed-at date/time (as supplied), county or location reference, coordinates if permitted/available, source URL, quality/status fields, ingested-at timestamp. |
| Climate measurement | One source measurement or source aggregate | Source/dataset ID, period, county/location reference, temperature value, unit, variable, source quality/coverage metadata, ingested-at timestamp. |
| Weather event | One incoming stream event | Event ID, event time, received-at time, location, temperature value/unit, source, optional schema/version metadata. |
| County-month analytical record | One county and calendar month | County key/name, year, month, observation count, temperature statistic(s), contributing record counts, missingness/coverage indicators, processing version. |

Candidate fields are not guaranteed to exist in source data. Confirm what the files actually contain; do not fabricate IDs, county mappings, or precision.

## Relationships and integration

- Many observation records can contribute to one county-month count.
- Many climate measurements can contribute to one county-month statistic.
- The analytical dataset joins county-month summaries on a normalized county key and matching year/month.
- Weather events may be aggregated by location and time window for recent operational views. They are not necessarily directly comparable to historical monthly Copernicus values.

County names and boundaries may change or differ between sources. Select a canonical county reference/code and maintain a documented mapping only after inspecting source coverage. Define what happens when a record cannot be assigned to a county.

## Proposed lakehouse layout

Logical layers, not committed table names:

1. `bronze`: immutable/raw source records and ingestion metadata.
2. `silver`: validated records with normalized types, units, time fields, and geographic references.
3. `gold`: county-month integrated analytical datasets, plus any separately defined live summaries.

The table format (Delta Lake or Apache Iceberg), physical storage, file format, partitioning, primary/deduplication keys, and retention policy remain open.

## MongoDB serving model

MongoDB is proposed for low-latency dashboard queries. A candidate document grain is one summary per location and time window, with fields for event window, temperature summary, event count, update time, and pipeline/source version. Historical Gold synchronization may use a separate county-month summary collection. Final collection names, indexes, idempotent upsert key, and retention rules should follow actual query patterns and be recorded in `technical-decisions.md`.

## Data quality and privacy

- Preserve units and original timestamps alongside normalized representations where practical.
- Define duplicate handling using source identifiers where available; avoid assuming approximate matches are duplicates.
- Record missing temperature, missing county, and incomplete time coverage explicitly.
- Treat location precision according to source terms and project privacy requirements; avoid exposing unnecessarily precise sensitive locations in dashboards.
- Track source, ingestion time, transformation version, and quality status through the pipeline.
