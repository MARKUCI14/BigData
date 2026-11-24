# GitHub Issues — Clouded Apollo Big Data Project

> [!NOTE]
> This document contains all planned GitHub issues for the **"The effect of the environmental temperature on the Clouded Apollo (Parnassius mnemosyne) population size in Romania"** project. Issues are organized by milestone and include titles, labels, and full markdown bodies ready for GitHub.

---

## Milestones Overview

| # | Milestone | Description | Target |
|---|-----------|-------------|--------|
| M1 | **Project Setup & Infrastructure** | Repository structure, Docker Compose, CI skeleton | Sprint 1 |
| M2 | **Data Ingestion — Batch** | Load historical CSV data into the Bronze layer | Sprint 1–2 |
| M3 | **Data Lake — Medallion Architecture** | Bronze → Silver → Gold transformations | Sprint 2–3 |
| M4 | **Batch Processing — Spark** | PySpark jobs for cleaning, aggregation, and analytics | Sprint 3–4 |
| M5 | **Streaming Pipeline** | Kafka + Spark Structured Streaming for real-time observations | Sprint 4–5 |
| M6 | **Speed Storage — MongoDB** | Collections, indexes, and data-loading logic | Sprint 3–5 |
| M7 | **Visualization — Streamlit Dashboard** | Interactive analytical dashboard | Sprint 5–6 |
| M8 | **Testing, Documentation & Final Delivery** | Tests, README, architecture diagrams, presentation | Sprint 6 |

---

## M1 — Project Setup & Infrastructure

---

### Issue #1 — Initialize repository structure and Python project skeleton

**Labels:** `setup`, `infrastructure`, `good first issue`
**Milestone:** M1 — Project Setup & Infrastructure

#### Description

Set up the foundational repository structure so that all team members can begin work in an organized manner.

#### Acceptance Criteria

- [ ] Create the following top-level directory layout:
  ```
  BigData/
  ├── data/
  │   ├── raw/                  # Original downloaded datasets (Bronze input)
  │   ├── bronze/               # Raw ingested Parquet files
  │   ├── silver/               # Cleaned & standardized Parquet files
  │   └── gold/                 # Aggregated analytical Parquet files
  ├── src/
  │   ├── ingestion/            # Batch & streaming ingestion scripts
  │   ├── processing/           # Spark batch processing jobs
  │   ├── streaming/            # Spark Structured Streaming jobs
  │   ├── storage/              # MongoDB load/read utilities
  │   └── dashboard/            # Streamlit application
  ├── config/                   # Configuration files (Kafka, Spark, MongoDB)
  ├── docker/                   # Dockerfiles for custom images
  ├── tests/                    # Unit and integration tests
  ├── docs/                     # Architecture diagrams, project docs
  ├── scripts/                  # Utility / helper scripts
  ├── docker-compose.yml
  ├── requirements.txt
  ├── .env.example
  └── README.md
  ```
- [ ] Add a `requirements.txt` with initial dependencies: `pyspark`, `kafka-python`, `pymongo`, `streamlit`, `plotly`, `pandas`, `requests`
- [ ] Create a `.env.example` with placeholder environment variables for Kafka, Spark, and MongoDB connection strings
- [ ] Update `README.md` with project title, brief description, and setup instructions stub
- [ ] Ensure `.gitignore` covers `data/bronze/`, `data/silver/`, `data/gold/`, `.env`, Spark logs, and checkpoint directories

#### Technical Notes

- The `data/raw/` directory should contain (or reference) the two source CSV files already in the repository: `Parnassius_obs_cleaned.csv` and `Romania_County_Monthly_Temperatures_2000_2026.csv`
- Large generated Parquet files in `data/bronze/`, `data/silver/`, and `data/gold/` should be gitignored; only the raw source CSVs are version-controlled

---

### Issue #2 — Create Docker Compose stack for all services

**Labels:** `setup`, `infrastructure`, `docker`
**Milestone:** M1 — Project Setup & Infrastructure

#### Description

Create a `docker-compose.yml` that orchestrates all infrastructure services required by the project. The goal is a single `docker-compose up` command to bring up the full environment.

#### Acceptance Criteria

- [ ] Define the following services in `docker-compose.yml`:
  | Service | Image | Ports |
  |---------|-------|-------|
  | `zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | 2181 |
  | `kafka` | `confluentinc/cp-kafka:7.6.0` | 9092 (external), 29092 (internal) |
  | `spark-master` | `bitnami/spark:3.5` | 8080 (UI), 7077 |
  | `spark-worker` | `bitnami/spark:3.5` | 8081 (UI) |
  | `mongodb` | `mongo:7.0` | 27017 |
  | `streamlit` | Custom Dockerfile | 8501 |
- [ ] Configure a shared Docker network (`bigdata-net`) for inter-service communication
- [ ] Add named volumes for MongoDB data persistence and Kafka log persistence
- [ ] Add health checks for Kafka, MongoDB, and Spark Master
- [ ] Create a `docker/streamlit/Dockerfile` that installs Python dependencies and copies the dashboard source
- [ ] The Spark master/worker should mount the project `data/` directory for direct Parquet access
- [ ] Document startup instructions in `README.md`

#### Technical Notes

- Kafka should auto-create the topic `butterfly-observations` on startup (or via a startup script)
- MongoDB should initialize with an `init-mongo.js` script that creates the `bigdata` database and the three collections: `observations`, `monthly_statistics`, `realtime_statistics`
- Spark workers should have access to the Kafka and MongoDB connectors (JARs) via Spark packages configuration

---

### Issue #3 — Configure Kafka topic and verify producer/consumer connectivity

**Labels:** `infrastructure`, `kafka`
**Milestone:** M1 — Project Setup & Infrastructure

#### Description

Set up the Kafka topic required for the streaming pipeline and verify that messages can be produced and consumed correctly within the Docker environment.

#### Acceptance Criteria

- [ ] Create a Kafka topic `butterfly-observations` with the following configuration:
  - Partitions: 3
  - Replication factor: 1
  - Retention: 7 days
- [ ] Write a minimal Python test producer (`scripts/test_kafka_producer.py`) that publishes a sample JSON message:
  ```json
  {
    "id": 999999,
    "observed_on": "2025-06-15",
    "latitude": 45.72,
    "longitude": 25.66,
    "place_county_name": "Brasov",
    "timestamp": "2025-06-15T10:30:00Z"
  }
  ```
- [ ] Write a minimal Python test consumer (`scripts/test_kafka_consumer.py`) that reads and prints messages from the topic
- [ ] Both scripts should read connection config from environment variables
- [ ] Document the topic configuration in `config/kafka-config.md`

#### Technical Notes

- Use `kafka-python` library for the test scripts
- The message value should be JSON-serialized UTF-8
- The message key should be the county name (for partition-level ordering by county)

---

### Issue #4 — Configure MongoDB collections, indexes, and initialization script

**Labels:** `infrastructure`, `mongodb`, `database`
**Milestone:** M1 — Project Setup & Infrastructure

#### Description

Create the MongoDB initialization script and define the schema expectations (indexes, validation) for each collection used by the project.

#### Acceptance Criteria

- [ ] Create `docker/mongo/init-mongo.js` that runs on first container start and performs:
  1. Create database `bigdata`
  2. Create collection `observations` with indexes:
     - `{ observation_id: 1 }` — unique
     - `{ county: 1, observed_on: 1 }`
     - `{ observed_on: 1 }`
  3. Create collection `monthly_statistics` with indexes:
     - `{ county: 1, year: 1, month: 1 }` — unique compound
  4. Create collection `realtime_statistics` with indexes:
     - `{ county: 1 }` — unique
     - `{ last_updated: 1 }`
- [ ] Document the expected document schema for each collection in `docs/mongodb-schema.md`:
  - `observations`: `observation_id`, `observed_on`, `year`, `month`, `latitude`, `longitude`, `county`, `source` (batch/stream)
  - `monthly_statistics`: `county`, `year`, `month`, `observation_count`, `avg_temperature`, `correlation_coefficient`
  - `realtime_statistics`: `county`, `total_observations`, `latest_observation_date`, `recent_observations[]`, `last_updated`
- [ ] Verify that the init script runs correctly when the MongoDB container starts fresh
- [ ] Add a `scripts/test_mongo_connection.py` utility that connects to MongoDB and prints collection names and counts

---

### Issue #5 — Verify Spark cluster connectivity and basic PySpark job execution

**Labels:** `infrastructure`, `spark`
**Milestone:** M1 — Project Setup & Infrastructure

#### Description

Verify that the Spark cluster (master + worker) starts correctly, can execute a simple PySpark job, and can read Parquet files from the mounted data directory.

#### Acceptance Criteria

- [ ] Submit a test PySpark job (`scripts/test_spark_job.py`) via `spark-submit` that:
  1. Creates a SparkSession connecting to the Spark master
  2. Reads one of the source CSV files
  3. Prints the schema and row count
  4. Writes a small Parquet file to `data/bronze/test/`
  5. Reads the Parquet file back and prints it
- [ ] Verify the Spark UI is accessible at `http://localhost:8080`
- [ ] Document the `spark-submit` command and required Spark packages (Kafka connector, MongoDB connector) in `config/spark-config.md`
- [ ] Ensure the following Spark packages are available:
  - `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`
  - `org.mongodb.spark:mongo-spark-connector_2.12:10.2.1`

---

## M2 — Data Ingestion — Batch

---

### Issue #6 — Ingest iNaturalist butterfly observations CSV into Bronze layer

**Labels:** `data-ingestion`, `batch`, `bronze`
**Milestone:** M2 — Data Ingestion — Batch

#### Description

Implement the batch ingestion process for the iNaturalist Clouded Apollo observation dataset. The raw CSV should be read, minimally validated, and stored as Parquet in the Bronze layer of the Data Lake.

#### Acceptance Criteria

- [ ] Create `src/ingestion/ingest_observations.py` that:
  1. Reads `data/raw/Parnassius_obs_cleaned.csv`
  2. Validates the expected columns exist: `id`, `observed_on`, `latitude`, `longitude`, `place_county_name`
  3. Adds ingestion metadata columns:
     - `ingestion_timestamp` (current datetime)
     - `source_file` (original filename)
     - `data_source` ("inaturalist")
  4. Converts `observed_on` to a proper date type
  5. Writes the result as Parquet to `data/bronze/observations/`
  6. Partitions the output by `year` (extracted from `observed_on`)
  7. Logs a summary: total rows read, total rows written, any skipped rows
- [ ] The script should be idempotent — re-running overwrites existing Bronze data for this source
- [ ] Handle edge cases:
  - Missing or malformed dates → log warning, skip row
  - Missing county names → log warning, keep row with `county = "Unknown"`
- [ ] Add a `--dry-run` flag that validates but does not write

#### Technical Notes

- The source CSV contains 71 observations spanning 2009–2025 across ~18 Romanian counties
- Use PySpark for consistency with the rest of the pipeline (even though Pandas could handle this small file)
- Column `place_county_name` should be renamed to `county` for downstream consistency

#### Data Sample

```
id,observed_on,latitude,longitude,place_county_name
171573651,2009-06-20,45.9483689,26.5774343,Vrancea
130474310,2010-06-17,45.2982275429,23.3869466136,Gorj
```

---

### Issue #7 — Ingest Copernicus temperature dataset CSV into Bronze layer

**Labels:** `data-ingestion`, `batch`, `bronze`
**Milestone:** M2 — Data Ingestion — Batch

#### Description

Implement the batch ingestion process for the Copernicus Climate Data Store monthly temperature dataset. The wide-format CSV must be read and stored as Parquet in the Bronze layer.

#### Acceptance Criteria

- [ ] Create `src/ingestion/ingest_temperatures.py` that:
  1. Reads `data/raw/Romania_County_Monthly_Temperatures_2000_2026.csv`
  2. Validates the expected structure: first column is a numeric index, second column is `County`, remaining columns follow the pattern `X{YYYY}.{MM}`
  3. Adds ingestion metadata columns:
     - `ingestion_timestamp`
     - `source_file`
     - `data_source` ("copernicus")
  4. Writes the result as Parquet to `data/bronze/temperatures/`
  5. Logs a summary: total counties, date range covered, any anomalies detected
- [ ] The script should be idempotent
- [ ] Handle edge cases:
  - Missing temperature values (NaN) → preserve as null in Parquet
  - Unexpected column format → log error, halt ingestion

#### Technical Notes

- The CSV is in **wide format**: 42 rows (counties) × 321+ columns (monthly temperatures from Jan 2000 to Sep 2026)
- Column names like `X2000.01` represent January 2000
- County names contain Romanian diacritics (e.g., `Brașov`, `Călărași`, `Bistrița-Năsăud`)
- The first unnamed column is a numeric row index and should be dropped during ingestion
- At the Bronze level, store the data as-is (wide format); unpivoting happens in Silver

---

### Issue #8 — Implement streaming observation simulator (Kafka producer)

**Labels:** `data-ingestion`, `streaming`, `kafka`
**Milestone:** M2 — Data Ingestion — Batch

#### Description

Create a Python script that simulates a continuous stream of new butterfly observations by replaying historical records (or generating synthetic ones) and publishing them to the Kafka topic.

#### Acceptance Criteria

- [ ] Create `src/ingestion/observation_producer.py` that:
  1. Reads the historical observations from `data/raw/Parnassius_obs_cleaned.csv`
  2. Iterates through the records and publishes each as a JSON message to the `butterfly-observations` Kafka topic
  3. Adds a `timestamp` field set to the current time (simulating real-time arrival)
  4. Adds a `source` field set to `"stream"`
  5. Uses the `county` as the Kafka message key
  6. Introduces a configurable delay between messages (default: 2 seconds) to simulate streaming behavior
  7. Optionally generates synthetic observations by:
     - Randomly selecting a county from the known list
     - Setting `observed_on` to today's date
     - Generating random coordinates within Romania's bounding box
     - Assigning a unique synthetic `id`
- [ ] Support the following CLI arguments:
  - `--mode`: `replay` (default) or `synthetic`
  - `--delay`: seconds between messages (default: 2)
  - `--count`: number of messages to produce (default: unlimited / all historical records)
  - `--kafka-broker`: broker address (default: `localhost:9092`)
  - `--topic`: topic name (default: `butterfly-observations`)
- [ ] Log each published message (observation id, county, timestamp)
- [ ] Handle Kafka connection failures gracefully with retries

#### Message Schema

```json
{
  "id": 171573651,
  "observed_on": "2025-11-19",
  "latitude": 45.9483689,
  "longitude": 26.5774343,
  "county": "Vrancea",
  "timestamp": "2025-11-19T15:30:00Z",
  "source": "stream"
}
```

---

## M3 — Data Lake — Medallion Architecture

---

### Issue #9 — Bronze → Silver: Clean and standardize butterfly observations

**Labels:** `data-processing`, `spark`, `silver`, `data-quality`
**Milestone:** M3 — Data Lake — Medallion Architecture

#### Description

Implement the PySpark transformation that reads Bronze-layer butterfly observations and produces a cleaned, standardized Silver-layer dataset.

#### Acceptance Criteria

- [ ] Create `src/processing/bronze_to_silver_observations.py` that:
  1. Reads Parquet from `data/bronze/observations/`
  2. Applies the following transformations:
     - Rename `place_county_name` → `county` (if not already done in Bronze)
     - Standardize county names: trim whitespace, fix known misspellings, normalize diacritics where needed (e.g., ensure `Brasov` ↔ `Brașov` consistency with the temperature dataset)
     - Parse `observed_on` → extract `year`, `month`, `day` columns
     - Validate `latitude` is in range [43.5, 48.5] and `longitude` is in [20.0, 30.5] (Romania's approximate bounding box)
     - Drop duplicate observations based on `id`
     - Drop rows where `observed_on` is null or `county` is null/empty
  3. Writes the cleaned dataset to `data/silver/observations/` as Parquet, partitioned by `year`
  4. Logs data quality metrics:
     - Total input rows
     - Rows dropped (and reasons)
     - Distinct counties found
     - Date range covered
- [ ] Create a **county name mapping table** (`config/county_mapping.json`) that maps any variant spellings to canonical county names matching the temperature dataset exactly

#### Data Quality Rules

| Check | Action |
|-------|--------|
| `observed_on` is null or unparseable | Drop row, log warning |
| `county` is null or empty | Drop row, log warning |
| `county` not in known Romanian county list | Flag as `county_validated = false` |
| Coordinates outside Romania bounding box | Flag as `coordinates_validated = false`, keep row |
| Duplicate `id` | Keep first occurrence, drop rest |

---

### Issue #10 — Bronze → Silver: Unpivot and clean temperature data

**Labels:** `data-processing`, `spark`, `silver`, `data-quality`
**Milestone:** M3 — Data Lake — Medallion Architecture

#### Description

Transform the wide-format Bronze temperature dataset into a long (unpivoted) Silver-layer dataset where each row represents one county-month-year temperature measurement.

#### Acceptance Criteria

- [ ] Create `src/processing/bronze_to_silver_temperatures.py` that:
  1. Reads Parquet from `data/bronze/temperatures/`
  2. Drops the numeric index column (first unnamed column)
  3. Unpivots the wide-format data: columns `X{YYYY}.{MM}` → rows with `year`, `month`, `avg_temperature`
  4. Parses column names: `X2000.01` → `year=2000`, `month=1`
  5. Standardizes county names to match the canonical mapping from Issue #9
  6. Validates temperature values are within a plausible range (e.g., −40°C to +50°C) — flag outliers
  7. Writes the result to `data/silver/temperatures/` as Parquet, partitioned by `year`
  8. Logs data quality metrics:
     - Total county-month records
     - Null temperature values
     - Counties found vs. expected 42

#### Expected Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `county` | string | Canonical county name |
| `year` | integer | Calendar year (2000–2026) |
| `month` | integer | Calendar month (1–12) |
| `avg_temperature` | double | Monthly average temperature in °C |

#### Technical Notes

- The Spark `stack()` function or manual unpivot logic can be used to convert wide to long format
- Some months in 2026 may have no data (Oct, Nov, Dec) — these will be null and should be dropped
- The output should contain approximately 42 counties × 321 months ≈ 13,482 rows (minus any trailing nulls)

---

### Issue #11 — Silver → Gold: Aggregate monthly observations per county

**Labels:** `data-processing`, `spark`, `gold`, `analytics`
**Milestone:** M3 — Data Lake — Medallion Architecture

#### Description

Create the Gold-layer aggregated dataset that counts butterfly observations per county per month and joins them with average temperatures.

#### Acceptance Criteria

- [ ] Create `src/processing/silver_to_gold.py` that:
  1. Reads Silver observations from `data/silver/observations/`
  2. Reads Silver temperatures from `data/silver/temperatures/`
  3. Aggregates observations: `GROUP BY county, year, month` → `observation_count`
  4. Joins the observation aggregates with temperature data on `(county, year, month)`
  5. Computes additional analytical columns:
     - `has_observations`: boolean flag (true if `observation_count > 0`)
     - `temperature_category`: categorical — "Cold" (< 5°C), "Mild" (5–15°C), "Warm" (15–25°C), "Hot" (> 25°C)
  6. Writes the joined Gold dataset to `data/gold/monthly_county_stats/` as Parquet, partitioned by `year`
  7. Logs analytics summary:
     - Total county-month combinations
     - County-months with observations vs. without
     - Temperature range summary

#### Expected Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `county` | string | County name |
| `year` | integer | Year |
| `month` | integer | Month |
| `observation_count` | integer | Number of butterfly observations (0 if none) |
| `avg_temperature` | double | Monthly average temperature °C |
| `has_observations` | boolean | Whether any observations exist |
| `temperature_category` | string | Cold / Mild / Warm / Hot |

#### Technical Notes

- Use a **left outer join** from temperatures to observations, so every county-month has a row even if there were no observations (most will be 0)
- The Gold dataset will serve as the primary analytical table for the dashboard

---

### Issue #12 — Silver → Gold: Compute correlation metrics and trend analysis

**Labels:** `data-processing`, `spark`, `gold`, `analytics`, `statistics`
**Milestone:** M3 — Data Lake — Medallion Architecture

#### Description

Compute the statistical relationship between temperature and observation counts, producing Gold-layer analytical datasets for the dashboard and research question.

#### Acceptance Criteria

- [ ] Extend `src/processing/silver_to_gold.py` (or create a new `src/processing/compute_correlations.py`) that:
  1. Reads the Gold `monthly_county_stats` dataset
  2. Computes **Pearson correlation coefficient** between `avg_temperature` and `observation_count`:
     - Per county (across all months/years)
     - Per year (across all counties/months)
     - Overall (all data)
  3. Computes observation **trend analysis**:
     - Yearly total observations per county
     - Year-over-year change percentage
  4. Computes **seasonal patterns**:
     - Average observations per month (across all years and counties)
     - Peak observation months
  5. Writes the results to:
     - `data/gold/correlations/` — correlation coefficients per county and per year
     - `data/gold/trends/` — yearly observation trends
     - `data/gold/seasonal_patterns/` — monthly seasonal aggregates

#### Expected Output — Correlations

| Column | Type | Description |
|--------|------|-------------|
| `group_type` | string | "county", "year", or "overall" |
| `group_value` | string | County name, year, or "all" |
| `pearson_r` | double | Pearson correlation coefficient |
| `p_value` | double | Statistical significance (if feasible) |
| `sample_size` | integer | Number of data points used |

#### Technical Notes

- Pearson correlation is the primary metric as specified in the Architecture document
- Given the sparse observation data (71 total observations across 25 years), correlations per county may have very small sample sizes — flag any correlation computed with n < 10
- Consider using `pyspark.ml.stat.Correlation` or computing manually with Spark SQL aggregations
- Document caveats about observation count ≠ population size in the output metadata

---

## M4 — Batch Processing — Spark

---

### Issue #13 — Create unified Spark batch processing pipeline orchestrator

**Labels:** `data-processing`, `spark`, `pipeline`
**Milestone:** M4 — Batch Processing — Spark

#### Description

Create a master orchestration script that runs the complete batch processing pipeline in the correct order: Bronze ingestion → Silver cleaning → Gold aggregation and analytics.

#### Acceptance Criteria

- [ ] Create `src/processing/run_batch_pipeline.py` that:
  1. Accepts CLI arguments for which stages to run (`--stages`: `all`, `ingest`, `bronze-to-silver`, `silver-to-gold`)
  2. Executes the stages in the correct dependency order:
     ```
     Ingest CSVs → Bronze
         ↓
     Bronze → Silver (observations)
     Bronze → Silver (temperatures)
         ↓
     Silver → Gold (monthly stats)
     Silver → Gold (correlations & trends)
         ↓
     Gold → MongoDB (load serving layer)
     ```
  3. Logs start/end time and duration for each stage
  4. Reports overall pipeline status (success/failure per stage)
  5. Supports `--force` flag to re-process even if output already exists
- [ ] Add error handling: if a stage fails, log the error and optionally continue to independent stages
- [ ] Create a convenience script `scripts/run_pipeline.sh` that invokes `spark-submit` with the correct Spark packages and configuration

#### Technical Notes

- This script ties together Issues #6, #7, #9, #10, #11, #12, and #15
- It should be runnable both locally and inside the Docker Spark cluster

---

### Issue #14 — Load Gold-layer results into MongoDB (batch)

**Labels:** `data-processing`, `mongodb`, `batch`
**Milestone:** M4 — Batch Processing — Spark

#### Description

Implement the final step of the batch pipeline: loading processed Gold-layer datasets into MongoDB for fast access by the Streamlit dashboard.

#### Acceptance Criteria

- [ ] Create `src/storage/load_to_mongodb.py` that:
  1. Reads the Gold-layer Parquet files:
     - `data/gold/monthly_county_stats/` → `monthly_statistics` collection
     - `data/gold/correlations/` → embedded within `monthly_statistics` or a separate `correlations` collection
  2. Uses `pymongo` to bulk-upsert documents into MongoDB
  3. Uses the unique compound key `(county, year, month)` for `monthly_statistics` upserts
  4. Clears and reloads the `observations` collection from Silver-layer data (all historical observations)
  5. Logs load statistics:
     - Documents inserted/updated per collection
     - Total load time
- [ ] Alternatively, implement using the Spark MongoDB connector (`mongo-spark-connector`) for direct Spark → MongoDB writes
- [ ] Ensure idempotency: re-running should not duplicate records

#### Technical Notes

- Prefer upsert operations over drop-and-reload to avoid downtime on the dashboard
- For the `observations` collection, each document should use `observation_id` as the unique key
- Consider batch sizes for `pymongo` bulk operations (e.g., 500 documents per batch)

---

## M5 — Streaming Pipeline

---

### Issue #15 — Implement Spark Structured Streaming consumer for Kafka observations

**Labels:** `streaming`, `spark`, `kafka`, `real-time`
**Milestone:** M5 — Streaming Pipeline

#### Description

Implement the Spark Structured Streaming job that continuously consumes butterfly observation messages from Kafka, processes them, and writes results to MongoDB.

#### Acceptance Criteria

- [ ] Create `src/streaming/stream_processor.py` that:
  1. Creates a SparkSession with Kafka and MongoDB connectors
  2. Reads from the `butterfly-observations` Kafka topic as a streaming source
  3. Parses the JSON message payload into a structured DataFrame with the schema:
     ```
     id: long
     observed_on: date
     latitude: double
     longitude: double
     county: string
     timestamp: timestamp
     source: string
     ```
  4. Applies data quality checks:
     - Validate county name against the canonical county list
     - Validate coordinates within Romania's bounding box
     - Drop malformed messages, log to a "dead letter" output
  5. Writes validated observations to MongoDB `observations` collection using `foreachBatch` with upserts (key: `observation_id`) to prevent duplicates on Kafka replay
  6. Implements checkpointing to `data/checkpoints/streaming/` for fault tolerance
  7. Runs continuously until manually stopped

#### Technical Notes

- Use `foreachBatch` sink to perform MongoDB upserts (not append-only writes)
- Use a stable observation ID (`id` from the message) as the MongoDB document `_id` or a unique key to handle Kafka message replays safely
- Configure the streaming trigger interval (e.g., `processingTime='10 seconds'`)
- Ensure the streaming job can be started/stopped independently of the batch pipeline

---

### Issue #16 — Implement real-time aggregation and MongoDB statistics updates

**Labels:** `streaming`, `spark`, `mongodb`, `real-time`
**Milestone:** M5 — Streaming Pipeline

#### Description

Extend the streaming processor to maintain real-time per-county statistics in MongoDB, updated as new observations arrive.

#### Acceptance Criteria

- [ ] Extend `src/streaming/stream_processor.py` (or create `src/streaming/realtime_aggregator.py`) to:
  1. Maintain a window-based aggregation:
     - Tumbling window: 1-minute intervals
     - Count observations per county within each window
  2. On each micro-batch, update the `realtime_statistics` collection in MongoDB:
     - `total_observations`: increment by the count of new observations for each county
     - `latest_observation_date`: update to the most recent `observed_on`
     - `recent_observations`: maintain a capped array of the last 10 observations per county (use MongoDB `$push` with `$slice`)
     - `last_updated`: set to current timestamp
  3. Associate incoming observations with the corresponding historical temperature data:
     - Look up the temperature for the observation's county/year/month from the `monthly_statistics` collection in MongoDB
     - If temperature data exists, enrich the observation document with `associated_temperature`
- [ ] Handle the case where no temperature data exists for a given county-month gracefully

#### Technical Notes

- The `realtime_statistics` collection should be updated via MongoDB upserts (using `$inc`, `$set`, `$push` operators)
- Since temperature data is historical and monthly, streaming observations will be associated with pre-existing temperature records rather than streaming temperature data
- This aligns with the Architecture document's note about focusing streaming on new butterfly observations

---

## M6 — Speed Storage — MongoDB

---

### Issue #17 — Implement MongoDB data access layer (read utilities)

**Labels:** `mongodb`, `backend`, `data-access`
**Milestone:** M6 — Speed Storage — MongoDB

#### Description

Create a reusable Python module for reading data from MongoDB, to be used by the Streamlit dashboard.

#### Acceptance Criteria

- [ ] Create `src/storage/mongodb_client.py` with a `MongoDBClient` class that provides:
  ```python
  class MongoDBClient:
      def __init__(self, connection_string: str, database: str = "bigdata")

      # Historical queries
      def get_monthly_stats(self, county=None, year=None, month=None) -> pd.DataFrame
      def get_observations(self, county=None, year_range=None) -> pd.DataFrame
      def get_correlations(self, group_type=None) -> pd.DataFrame

      # Real-time queries
      def get_realtime_stats(self, county=None) -> dict
      def get_latest_observations(self, limit=20) -> pd.DataFrame
      def get_observation_count_by_county() -> pd.DataFrame

      # Metadata
      def get_available_counties() -> list[str]
      def get_date_range() -> tuple[str, str]
      def get_collection_stats() -> dict
  ```
- [ ] All query methods should return `pandas.DataFrame` or native Python types suitable for Streamlit
- [ ] Add connection pooling and retry logic
- [ ] Read the MongoDB connection string from environment variables
- [ ] Add unit tests for the client using `mongomock` or a test MongoDB container

---

## M7 — Visualization — Streamlit Dashboard

---

### Issue #18 — Create Streamlit dashboard skeleton with navigation and layout

**Labels:** `dashboard`, `streamlit`, `frontend`
**Milestone:** M7 — Visualization — Streamlit Dashboard

#### Description

Set up the Streamlit application structure with a multi-page navigation layout matching the four analytical sections defined in the Architecture document.

#### Acceptance Criteria

- [ ] Create `src/dashboard/app.py` as the main Streamlit entry point
- [ ] Implement a sidebar navigation with four pages:
  1. **📊 Historical Analysis** — Observation counts and trends
  2. **🌡️ Temperature Analysis** — Temperature patterns and evolution
  3. **🔗 Correlation Analysis** — Temperature vs. observations
  4. **📡 Real-Time Monitoring** — Live streaming data
- [ ] Add a project header with:
  - Project title: "Clouded Apollo (Parnassius mnemosyne) in Romania"
  - Brief description
  - Data source attributions (iNaturalist, Copernicus)
- [ ] Add global sidebar filters:
  - County multi-select (populated from MongoDB)
  - Year range slider
- [ ] Configure Streamlit page settings:
  - `page_title="Clouded Apollo Analytics"`
  - `layout="wide"`
  - `page_icon="🦋"`
- [ ] Connect to MongoDB using the `MongoDBClient` from Issue #17
- [ ] Add a footer with team credits

#### Technical Notes

- Use Streamlit's `st.navigation` or multi-page app structure
- Global filters should be passed as state to individual pages
- The dashboard should handle the case where MongoDB is empty (show informative messages)

---

### Issue #19 — Implement Historical Analysis page

**Labels:** `dashboard`, `streamlit`, `visualization`, `analytics`
**Milestone:** M7 — Visualization — Streamlit Dashboard

#### Description

Build the Historical Analysis dashboard page showing butterfly observation patterns from 2000–2026.

#### Acceptance Criteria

- [ ] Create `src/dashboard/pages/historical_analysis.py` with the following visualizations:
  1. **KPI Cards** (top row):
     - Total observations
     - Number of counties with observations
     - Date range of observations
     - Most active county
  2. **Yearly Observation Trend** — Line chart showing total observations per year (2000–2026)
  3. **Monthly Observation Distribution** — Bar chart showing observations per month (all years combined), highlighting the peak flight season
  4. **County-Level Comparison** — Horizontal bar chart ranking counties by total observations
  5. **Observations Map** — Scatter map of Romania showing observation locations (latitude/longitude), colored by county
  6. **Detailed Data Table** — Expandable table showing all individual observations with sorting and filtering
- [ ] All charts should be interactive (Plotly)
- [ ] Charts should respond to the global sidebar filters (county selection, year range)
- [ ] Add explanatory text/captions beneath each chart

---

### Issue #20 — Implement Temperature Analysis page

**Labels:** `dashboard`, `streamlit`, `visualization`, `analytics`
**Milestone:** M7 — Visualization — Streamlit Dashboard

#### Description

Build the Temperature Analysis dashboard page showing climate patterns across Romanian counties.

#### Acceptance Criteria

- [ ] Create `src/dashboard/pages/temperature_analysis.py` with the following visualizations:
  1. **Average Temperature Heatmap** — County × Month heatmap showing average temperatures (all years)
  2. **Temperature Evolution Over Time** — Multi-line chart showing yearly average temperature per county (or selected counties)
  3. **Monthly Temperature Patterns** — Line chart showing average temperature per month (seasonal pattern), with shaded ranges showing min/max across years
  4. **County Temperature Comparison** — Box plot or violin plot comparing temperature distributions across counties
  5. **Temperature Trend by County** — Small multiples or selectable line chart showing temperature trends for individual counties
- [ ] All charts should use Plotly for interactivity
- [ ] Add temperature unit labels (°C) consistently
- [ ] Respond to global sidebar filters

---

### Issue #21 — Implement Correlation Analysis page (Temperature vs. Observations)

**Labels:** `dashboard`, `streamlit`, `visualization`, `analytics`, `statistics`
**Milestone:** M7 — Visualization — Streamlit Dashboard

#### Description

Build the Correlation Analysis page that presents the core research question: the relationship between temperature and butterfly observations.

#### Acceptance Criteria

- [ ] Create `src/dashboard/pages/correlation_analysis.py` with the following visualizations:
  1. **Scatter Plot: Temperature vs. Observation Count** — Interactive scatter plot with:
     - X-axis: average monthly temperature
     - Y-axis: observation count per county-month
     - Color: county
     - Tooltip: county, year, month, temperature, count
     - Optional: trend line (linear regression)
  2. **Correlation Coefficient Summary** — Table or card view showing:
     - Overall Pearson r
     - Per-county Pearson r
     - Per-year Pearson r (where data exists)
     - Sample size for each
  3. **Seasonal Observation vs. Temperature Overlay** — Dual-axis line chart:
     - Left Y-axis: average observations per month
     - Right Y-axis: average temperature per month
     - X-axis: months (Jan–Dec)
  4. **County Comparison Panel** — Side-by-side comparison of 2–3 selected counties showing their observation-temperature patterns
  5. **Limitations & Methodology Note** — Expandable section documenting:
     - Observation ≠ population size
     - Observer bias caveat
     - Correlation ≠ causation
- [ ] Add a statistical interpretation summary with appropriate caveats

#### Technical Notes

- Given only 71 observations, correlations should be interpreted with extreme caution
- Display confidence or significance information where possible
- Use `scipy.stats.pearsonr` for p-value calculation if not already computed in Gold layer

---

### Issue #22 — Implement Real-Time Monitoring page

**Labels:** `dashboard`, `streamlit`, `visualization`, `real-time`
**Milestone:** M7 — Visualization — Streamlit Dashboard

#### Description

Build the Real-Time Monitoring page that displays live streaming observation data from the Kafka → Spark → MongoDB pipeline.

#### Acceptance Criteria

- [ ] Create `src/dashboard/pages/realtime_monitoring.py` with the following components:
  1. **Live KPI Metrics** (auto-refreshing):
     - Total observations received (all-time)
     - Observations received in the last hour
     - Number of active counties (received observations recently)
     - Timestamp of latest processed event
  2. **Recent Observations Feed** — Live-updating table showing the last 20 observations with columns: timestamp, county, coordinates, observation ID
  3. **Observations per County** — Bar chart updated with real-time county totals
  4. **Live Map** — Map showing recent observation locations, with new points appearing as they arrive
  5. **Stream Status Indicator** — Visual indicator (green/red) showing whether the streaming pipeline is active (based on recency of `last_updated` timestamps in `realtime_statistics`)
- [ ] Implement auto-refresh using `st.rerun()` or `st.empty()` with configurable refresh interval (default: 10 seconds)
- [ ] Handle gracefully the case where the streaming pipeline is not running (show "No streaming data available" message)

#### Technical Notes

- Streamlit does not natively support WebSocket push; use periodic polling of MongoDB via `st.rerun()` or Streamlit's `st.fragment` with `run_every` parameter
- The refresh interval should be configurable via a sidebar slider

---

## M8 — Testing, Documentation & Final Delivery

---

### Issue #23 — Write unit tests for data processing functions

**Labels:** `testing`, `quality`
**Milestone:** M8 — Testing, Documentation & Final Delivery

#### Description

Create comprehensive unit tests for the core data processing logic to ensure correctness and prevent regressions.

#### Acceptance Criteria

- [ ] Create tests in `tests/` directory using `pytest`:
  1. `tests/test_ingestion.py`:
     - Test CSV reading and schema validation
     - Test metadata column addition
     - Test edge case handling (missing dates, empty county)
  2. `tests/test_bronze_to_silver.py`:
     - Test county name standardization
     - Test coordinate validation (inside/outside Romania)
     - Test duplicate removal
     - Test date parsing and year/month extraction
  3. `tests/test_silver_to_gold.py`:
     - Test observation aggregation counts
     - Test temperature join logic (left outer join correctness)
     - Test temperature categorization
  4. `tests/test_correlations.py`:
     - Test Pearson correlation computation with known values
     - Test handling of small sample sizes
  5. `tests/test_mongodb_client.py`:
     - Test query methods return correct types
     - Test filtering logic
- [ ] Use PySpark's `SparkSession.builder.master("local[*]")` for Spark tests
- [ ] Create test fixtures with small synthetic datasets
- [ ] Add a `pytest.ini` or `pyproject.toml` configuration
- [ ] All tests should pass with `pytest tests/ -v`

---

### Issue #24 — Write integration test for end-to-end pipeline

**Labels:** `testing`, `quality`, `integration`
**Milestone:** M8 — Testing, Documentation & Final Delivery

#### Description

Create an integration test that validates the complete data flow from raw CSV ingestion through to MongoDB loading.

#### Acceptance Criteria

- [ ] Create `tests/test_integration.py` that:
  1. Uses a small subset of real data (5–10 observation rows, 2–3 counties of temperature data)
  2. Runs the complete pipeline: Ingest → Bronze → Silver → Gold → MongoDB
  3. Validates:
     - Bronze Parquet files exist and have expected schema
     - Silver Parquet files have correct transformations applied
     - Gold Parquet files have correct aggregations
     - MongoDB collections contain the expected documents
  4. Cleans up test artifacts after completion
- [ ] The test should be runnable with `pytest tests/test_integration.py` (requires Docker services running)
- [ ] Add a `docker-compose.test.yml` for spinning up test infrastructure if needed

---

### Issue #25 — Create comprehensive README and project documentation

**Labels:** `documentation`
**Milestone:** M8 — Testing, Documentation & Final Delivery

#### Description

Write complete project documentation covering setup, architecture, usage, and results.

#### Acceptance Criteria

- [ ] Update `README.md` to include:
  1. **Project Overview** — Title, research question, team members
  2. **Architecture Diagram** — Embedded Mermaid or image diagram showing the full data flow
  3. **Technology Stack** — Table of technologies with versions and purposes
  4. **Prerequisites** — Docker, Docker Compose, Python 3.11+
  5. **Quick Start** — Step-by-step instructions:
     ```bash
     git clone <repo>
     cd BigData
     docker-compose up -d
     python src/processing/run_batch_pipeline.py --stages all
     python src/ingestion/observation_producer.py --mode replay
     # Open http://localhost:8501 for the dashboard
     ```
  6. **Project Structure** — Directory tree with descriptions
  7. **Data Sources** — Description of iNaturalist and Copernicus datasets, with citations
  8. **Pipeline Description** — Explanation of each processing stage
  9. **Dashboard Screenshots** — Screenshots of each dashboard page
  10. **Known Limitations** — As documented in Architecture.md
  11. **Bibliography** — Academic references from ProjectProposal.md
- [ ] Create `docs/architecture-diagram.md` with a detailed Mermaid diagram
- [ ] Create `docs/data-dictionary.md` documenting all fields across Bronze, Silver, and Gold layers
- [ ] Ensure all source files have docstrings and inline comments

---

### Issue #26 — Create architecture flow diagram (Mermaid)

**Labels:** `documentation`, `architecture`
**Milestone:** M8 — Testing, Documentation & Final Delivery

#### Description

Create a detailed, visually clear architecture diagram using Mermaid that illustrates the complete data flow through the system.

#### Acceptance Criteria

- [ ] Create `docs/architecture-diagram.md` containing a Mermaid flowchart that shows:
  1. Data sources (iNaturalist CSV, Copernicus CSV, Streaming simulator)
  2. Ingestion layer (Python scripts → Kafka / Bronze Parquet)
  3. Data Lake medallion layers (Bronze → Silver → Gold)
  4. Spark processing (batch + streaming)
  5. MongoDB collections
  6. Streamlit dashboard
  7. Technology labels on each component
- [ ] Create a second diagram showing the streaming data flow specifically:
  ```
  Producer → Kafka → Spark Streaming → MongoDB → Streamlit
  ```
- [ ] Include the diagram(s) in the main `README.md`

---

### Issue #27 — Prepare project presentation materials

**Labels:** `documentation`, `presentation`
**Milestone:** M8 — Testing, Documentation & Final Delivery

#### Description

Prepare materials for the final project presentation/defense.

#### Acceptance Criteria

- [ ] Create `docs/presentation-notes.md` with:
  1. 5-minute presentation outline
  2. Key talking points per architecture layer
  3. Demo flow: which screens to show and in what order
  4. Answers to anticipated questions:
     - "Why Lambda architecture?"
     - "How do you handle data quality issues?"
     - "What do the correlation results actually mean?"
     - "How would this scale with more data?"
  5. Team member responsibilities summary (who built what)
- [ ] Capture dashboard screenshots for all four pages
- [ ] Document demo steps for live presentation:
  1. Show Docker Compose services running
  2. Show Spark UI with completed jobs
  3. Navigate through dashboard pages
  4. Start the streaming producer live
  5. Show real-time page updating

---

## Labels Summary

| Label | Color Suggestion | Description |
|-------|-----------------|-------------|
| `setup` | 🔵 `#0075ca` | Initial project setup tasks |
| `infrastructure` | 🔵 `#0075ca` | Docker, services, infrastructure configuration |
| `docker` | 🐳 `#2496ED` | Docker and Docker Compose related |
| `kafka` | 🟠 `#e36209` | Apache Kafka related |
| `mongodb` | 🟢 `#0e8a16` | MongoDB related |
| `spark` | 🔴 `#d73a49` | Apache Spark related |
| `data-ingestion` | 🟡 `#fbca04` | Data ingestion tasks |
| `batch` | 🟤 `#795548` | Batch processing |
| `streaming` | ⚡ `#7057ff` | Streaming / real-time processing |
| `bronze` | 🟤 `#CD7F32` | Bronze layer of Data Lake |
| `silver` | ⚪ `#C0C0C0` | Silver layer of Data Lake |
| `gold` | 🟡 `#FFD700` | Gold layer of Data Lake |
| `data-processing` | 🟣 `#6f42c1` | Data transformation and processing |
| `data-quality` | ✅ `#1d76db` | Data quality and validation |
| `analytics` | 📊 `#5319e7` | Analytical computations |
| `statistics` | 📈 `#006b75` | Statistical analysis |
| `real-time` | ⚡ `#b60205` | Real-time components |
| `dashboard` | 🎨 `#d876e3` | Streamlit dashboard |
| `frontend` | 🎨 `#d876e3` | Frontend / UI |
| `visualization` | 📊 `#bfd4f2` | Data visualization |
| `backend` | ⚙️ `#333333` | Backend logic |
| `data-access` | 🔑 `#c2e0c6` | Data access layer |
| `pipeline` | 🔄 `#0366d6` | Pipeline orchestration |
| `database` | 💾 `#0e8a16` | Database schema and configuration |
| `testing` | 🧪 `#e4e669` | Tests |
| `quality` | ✅ `#1d76db` | Code and data quality |
| `integration` | 🔗 `#c5def5` | Integration testing |
| `documentation` | 📝 `#0075ca` | Documentation |
| `architecture` | 🏗️ `#333333` | Architecture design |
| `presentation` | 🎤 `#f9d0c4` | Presentation materials |
| `good first issue` | 👋 `#7057ff` | Good for newcomers |
