# Presentation Notes

## Five-minute outline

| Time | Section | Main point |
| --- | --- | --- |
| 0:00–0:35 | Research question | Explore association between monthly county temperature and recorded Clouded Apollo sightings; sightings are not population counts. |
| 0:35–1:10 | Data | Introduce the iNaturalist observation CSV and Copernicus monthly county temperatures; note the limited observation sample. |
| 1:10–2:05 | Architecture | Show the batch Bronze → Silver → Gold path and the separate Kafka → Spark Structured Streaming → MongoDB path. |
| 2:05–2:45 | Processing and quality | Explain county/date normalization, coordinate flags, duplicate removal, temperature unpivoting, and county-month aggregation. |
| 2:45–4:20 | Dashboard demo | Visit Historical Analysis, Temperature Analysis, Correlation Analysis, then Real-Time Monitoring. |
| 4:20–4:45 | Live stream | Start a finite observation replay and show new records or county totals in the realtime page. |
| 4:45–5:00 | Conclusion | State what the pipeline demonstrates, its limits, and what further data or validation would improve the analysis. |

## Talking points by architecture layer

- **Sources:** The historical inputs are iNaturalist sightings and Copernicus monthly mean temperatures. The supplied observation snapshot is small; it cannot represent all sightings or population abundance.
- **Ingestion:** PySpark validates source columns and records ingestion metadata in Bronze Parquet. A Python producer can replay or synthesize observation events for Kafka.
- **Bronze and Silver:** Bronze retains source-oriented records. Silver standardizes counties and dates, derives calendar fields, flags suspicious coordinates, removes duplicate IDs, and turns wide temperature columns into county/year/month rows.
- **Gold analytics:** The batch job joins monthly sighting counts to county temperatures and derives correlation, yearly trend, and seasonal summaries. Missing temperatures remain null; no matching sighting means a zero recorded count.
- **Streaming:** Kafka transports observation events. Spark Structured Streaming validates them, records rejects in a dead-letter path, upserts accepted observations, and maintains per-county realtime totals in MongoDB.
- **Serving and dashboard:** MongoDB stores dashboard-ready observations and summaries. Streamlit and Plotly provide historical, temperature, correlation, and realtime views with county/year filters.
- **Deployment:** Docker Compose describes the local Kafka, Spark, MongoDB, and dashboard services. Confirm service health and image availability on the presentation machine before relying on a live demonstration.

## Demo flow

1. From the repository root, run `docker compose up --build -d` and `docker compose ps`. Confirm Kafka, Spark, MongoDB, and Streamlit report healthy/running before starting the demo.
2. Open the Spark Master UI at `http://localhost:8080` and show completed batch or streaming jobs. If jobs are not present, submit `spark-submit src/processing/run_batch_pipeline.py --stages all` and wait for completion.
3. Open `http://localhost:8501` and show the global county and year filters.
4. Navigate through the dashboard in this order:
   - **Historical Analysis:** yearly/monthly sighting patterns, county comparison, locations, and source records.
   - **Temperature Analysis:** county/month heatmap, yearly evolution, seasonal range, and selectable county trends.
   - **Correlation Analysis:** county-month scatter, Pearson summary, seasonal overlay, and methodology caveats.
   - **Real-Time Monitoring:** stream status, recent event feed, county totals, and map.
5. Start a finite producer replay in another terminal: `python src/ingestion/observation_producer.py --mode replay --count 10`.
6. Wait for the Spark streaming consumer to process the events, then show the realtime page refreshing and the MongoDB-backed event/count changes.
7. Close with the data and interpretation limitations: the events are observation reports, the stream is not a weather feed, and correlation is not causation.

If the services or Spark job fail to start, switch to the architecture diagram and prepared static data examples rather than implying that a live run succeeded.

## Anticipated questions

### Why use a Lambda-style architecture?

The batch path supports repeatable processing of the full historical CSVs, while the streaming path updates recent event records and county totals with lower latency. They serve different timing needs and share MongoDB as a serving layer. The current streaming demonstration carries butterfly observations; it does not yet implement the proposal's weather-event stream.

### How do you handle data quality issues?

Ingestion checks required columns, parses dates, and records source metadata. Silver standardizes county aliases, removes invalid dates and empty counties, de-duplicates observation IDs, casts coordinates, and flags coordinates outside a broad Romania bounding box. Temperature values are converted to numeric form and implausible values are flagged. The stream validates event fields and writes rejected events with their Kafka offset and reason to a dead-letter path. Missing temperature is kept as null rather than treated as zero.

### What do the correlation results mean?

Pearson r describes the direction and strength of a linear association between temperature and observation counts in county-month aggregates. The p-value describes how compatible the observed coefficient is with a zero-correlation model under its assumptions; it does not address observer bias or prove causation. There are only 71 underlying sightings in the supplied snapshot, and county-month rows are repeated aggregates, so both coefficient and p-value need cautious interpretation.

### How would the system scale with more data?

Increase Kafka partitions and Spark worker capacity, checkpoint streaming offsets to durable storage, and keep transformations distributed instead of collecting large data sets on the driver. Store Parquet in shared object storage and choose partitions based on query patterns and cardinality. Add or tune MongoDB indexes, and consider sharding when measured query/write load requires it. The current local Compose environment is a development/demo setup, not a scale benchmark.

## Team and responsibilities

The proposal records the following team and degree areas. It assigns data acquisition, sorting, and cleaning to the bioinformatics students, and architecture and pipeline planning to the software engineering students; it does not map individual repository files to individual contributors.

| Team member | Degree area | Proposal-level responsibility |
| --- | --- | --- |
| Alixandrescu Elena-Adina | Bioinformatics Applied in Lifesciences | Data acquisition, sorting, and cleaning group |
| Faur Ucu-Mihai | Bioinformatics Applied in Lifesciences | Data acquisition, sorting, and cleaning group |
| Gergely Kinga | Bioinformatics Applied in Lifesciences | Data acquisition, sorting, and cleaning group |
| Portik Márk-Krisztián | Software Engineering | Architecture and pipeline planning group |
| Zoltáni Szabolcs | Software Engineering | Architecture and pipeline planning group |

## Dashboard screenshots

The four pages are implemented, but representative screenshots have not been captured from a running dashboard. Do not present generated mockups as application screenshots. After starting the services and loading data, save one image per page under `docs/assets/screenshots/` using these names, then add them here:

- `historical-analysis.png`
- `temperature-analysis.png`
- `correlation-analysis.png`
- `realtime-monitoring.png`
