# Data Dictionary

This document describes the fields written by the current Bronze, Silver, and Gold processing stages. Source CSV fields are preserved where stated; generated data files are not checked in.

## Bronze: observations

Written to `data/bronze/observations/` from `Parnassius_obs_cleaned.csv`. CSV values are initially read as strings unless explicitly converted below.

| Field | Type | Description |
| --- | --- | --- |
| `id` | string | Source observation identifier; cast to long in Silver. |
| `observed_on` | date | Parsed source sighting date; malformed values are excluded. |
| `latitude` | string | Source latitude; cast to double in Silver. |
| `longitude` | string | Source longitude; cast to double in Silver. |
| `county` | string | Trimmed county value from `place_county_name`; blank values become `Unknown`. |
| `year` | integer | Calendar year derived from `observed_on`; used for partitioning. |
| `ingestion_timestamp` | timestamp | Spark processing time when the row was ingested. |
| `source_file` | string | Input CSV filename. |
| `data_source` | string | Constant `inaturalist`. |

## Bronze: temperatures

Written to `data/bronze/temperatures/` from `Romania_County_Monthly_Temperatures_2000_2026.csv`.

| Field | Type | Description |
| --- | --- | --- |
| `County` | string | County as named in the source CSV; standardized in Silver. |
| `XYYYY.MM` | double or null | One source column per month, for example `X2020.06`; monthly mean temperature in °C. |
| `ingestion_timestamp` | timestamp | Spark processing time when the data was ingested. |
| `source_file` | string | Input CSV filename. |
| `data_source` | string | Constant `copernicus`. |

The source's unnamed numeric row-index column is discarded during ingestion. Null-like strings become null values; other nonnumeric readings are preserved as null when converted.

## Silver: observations

Written to `data/silver/observations/`, partitioned by `year`.

| Field | Type | Description |
| --- | --- | --- |
| `id` | long | Stable source observation ID; duplicate IDs are reduced to one row. |
| `observed_on` | date | Parsed date of the sighting. |
| `latitude` | double | Parsed latitude. |
| `longitude` | double | Parsed longitude. |
| `county` | string | Trimmed canonical county name from `config/county_mapping.json`. |
| `year` | integer | Year extracted from `observed_on`; partition key. |
| `month` | integer | Calendar month, 1–12. |
| `day` | integer | Day of month, 1–31. |
| `county_validated` | boolean | True if county is in the configured canonical county list. |
| `coordinates_validated` | boolean | True if coordinates are inside the broad Romania bounds (latitude 43.5–48.5, longitude 20.0–30.5). |
| `ingestion_timestamp` | timestamp | Inherited Bronze ingestion time. |
| `source_file` | string | Inherited source filename. |
| `data_source` | string | Inherited source label. |

Rows without a valid date or county are removed. Coordinates outside the configured bounds are flagged, not removed.

## Silver: temperatures

Written to `data/silver/temperatures/`, partitioned by `year`. The wide month columns are unpivoted into one row per county and month.

| Field | Type | Description |
| --- | --- | --- |
| `county` | string | Canonical county name. |
| `year` | integer | Four-digit calendar year parsed from the source column. |
| `month` | integer | Calendar month, 1–12. |
| `avg_temperature` | double | Monthly mean temperature in °C. |
| `temperature_validated` | boolean | False when the value is outside the configured plausible range; such values are retained and flagged. |

## Gold: monthly county statistics

Written to `data/gold/monthly_county_stats/`, partitioned by `year`. Grain: one county/year/month temperature row, left joined to observation counts.

| Field | Type | Description |
| --- | --- | --- |
| `county` | string | Canonical county. |
| `year` | integer | Calendar year. |
| `month` | integer | Calendar month, 1–12. |
| `observation_count` | integer | Number of Silver observations for the county and month; zero means no matching source record. |
| `avg_temperature` | double or null | County's monthly mean temperature in °C. Missing values remain null. |
| `has_observations` | boolean | True when `observation_count` is greater than zero. |
| `temperature_category` | string or null | `Cold` (<5 °C), `Mild` (<15 °C), `Warm` (≤25 °C), or `Hot` (>25 °C); null if temperature is missing. |

## Gold: analytical outputs

### Correlations (`data/gold/correlations/`)

| Field | Type | Description |
| --- | --- | --- |
| `group_type` | string | `county`, `year`, or `overall`. |
| `group_value` | string | County name, year text, or `all`. |
| `pearson_r` | double or null | Pearson correlation between temperature and observation count. |
| `p_value` | double or null | Two-sided p-value; null for undefined or fewer than three paired records. |
| `sample_size` | integer | Number of non-null county-month pairs in the group. |
| `small_sample` | boolean | True when sample size is below the configured threshold of 10. |

### Yearly trends (`data/gold/trends/`)

| Field | Type | Description |
| --- | --- | --- |
| `county` | string | Canonical county. |
| `year` | integer | Calendar year. |
| `total_observations` | long | Sum of monthly observation counts. |
| `previous_year_observations` | long or null | Prior available calendar year's total. |
| `year_over_year_change_pct` | double or null | Percentage change when the previous consecutive year has a nonzero total. |

### Seasonal patterns (`data/gold/seasonal_patterns/`)

| Field | Type | Description |
| --- | --- | --- |
| `month` | integer | Calendar month, 1–12. |
| `avg_observations` | double | Mean county-month observation count for the month. |
| `total_observations` | long | Sum of county-month observation counts for the month. |
| `county_month_records` | long | Number of county-month rows contributing to the summary. |
| `is_peak_month` | boolean | True for the month(s) with the largest mean observation count. |

## MongoDB naming

MongoDB observation documents rename Silver `id` to `observation_id`. Collections and their query schemas are described in [MongoDB schema documentation](mongodb-schema.md). Realtime documents add `realtime_counted`, event timestamps, stream-run metadata, and county-level totals; they are not part of the historical Gold schema.
