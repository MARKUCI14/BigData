# Data Model

## Scope and grain

The model combines individual Clouded Apollo sightings with monthly county-level temperatures. An observation is one source sighting; a monthly analytical row represents one Romanian county and calendar month. The monthly grain supports comparison across the historical period while avoiding a claim that reported sightings equal the actual population.

## Historical source records

### Butterfly observations

The source file is `data/raw/Parnassius_obs_cleaned.csv`. The ingestion and streaming code use these source fields:

| Field | Meaning | Handling |
| --- | --- | --- |
| `id` | iNaturalist observation identifier | Required stable identity; represented as an integer in the Kafka event and as `observation_id` in MongoDB. |
| `observed_on` | Date the observation was made | Parsed as a date; year and month are derived for analysis. |
| `latitude`, `longitude` | Observation coordinates | Numeric coordinates; streaming validation checks a broad Romania bounding box. |
| `place_county_name` / `county` | County in the CSV / canonical county in processed events | Standardized with `config/county_mapping.json`; unknown values should be treated as invalid or explicitly mapped. |

Additional CSV columns may be retained during ingestion; the listed fields are the fields currently consumed by the event producer and stream schema.

### Temperature records

The source file is `data/raw/Romania_County_Monthly_Temperatures_2000_2026.csv`. It is a wide CSV with `County` and month columns named `XYYYY.MM` (for example, `X2000.01`). Values are monthly average temperatures in degrees Celsius. The Silver transformation unpivots these columns into one row per county and month. County, year, and month are the integration keys used for comparison with observation summaries.

## Kafka event contract (current implementation)

Topic: `butterfly-observations`. Values are UTF-8 JSON. The producer uses county as the message key so records for a county are partitioned together.

```json
{
  "id": 171573651,
  "observed_on": "2025-06-15",
  "latitude": 45.948369,
  "longitude": 26.577434,
  "county": "Vrancea",
  "timestamp": "2025-06-15T10:30:00Z",
  "source": "stream"
}
```

| Field | Type | Required by the current consumer | Definition |
| --- | --- | --- | --- |
| `id` | integer | Yes | Stable observation identifier. Used for de-duplication and MongoDB upsert identity. |
| `observed_on` | ISO date | Yes | Date attached to the observation. |
| `latitude`, `longitude` | number | Yes | Coordinates; validated against a broad Romania range. |
| `county` | string | Yes | County name; normalized to a canonical Romanian county. |
| `timestamp` | ISO-8601 timestamp | Yes | UTC event/replay processing timestamp. |
| `source` | string | Yes | Producer label, currently `stream`. |

Invalid JSON, missing fields, invalid dates, noncanonical counties, and out-of-range coordinates are written to the streaming dead-letter Parquet path with their Kafka topic, partition, offset, raw value, and rejection reason.

### Target stream contract gap

The proposal and supplied architecture diagram describe incoming weather measurements. The current Kafka contract describes butterfly sightings. A weather event needs a separately agreed schema (for example, event time, county or coordinates, temperature value, unit, and source) and a decision about whether the existing topic is renamed or a distinct topic is introduced. Do not treat the current observation schema as a weather schema.

## Analytical records

Gold outputs are produced by county/month aggregation and analytics stages. The intended monthly summary grain is:

| Field | Meaning |
| --- | --- |
| `county` | Canonical county name |
| `year`, `month` | Calendar month of the observation/temperature records |
| observation count | Number of reported Clouded Apollo records in the group |
| average temperature | Monthly county temperature value from the climate source |
| correlation / trend metrics | Derived statistics with their grouping and period documented alongside the result |

The current Gold join starts with available temperature county-months and fills an absent matching observation group with `observation_count = 0`. This means no record was present in the supplied observation file for that county-month; it does not prove that the species was absent. A missing temperature remains missing and must not be interpreted as zero. Correlation results must record their grouping and sample scope.

## MongoDB serving collections

| Collection | Grain and purpose | Identity / indexing intent |
| --- | --- | --- |
| `observations` | One processed historical or streaming observation | Unique `observation_id`; county and date query indexes. |
| `monthly_statistics` | One county/year/month summary | Unique `(county, year, month)`. |
| `correlations` | One correlation result for a defined group | Unique `(group_type, group_value)`; include metric and period metadata in documents. |
| `realtime_statistics` | Latest aggregate per county | Unique `county`; indexed `last_updated`. |

The initialization script creates the four collections above. This differs from the preliminary proposal text, which named three collections and omitted `correlations`; the implemented initializer is the current schema reference.

