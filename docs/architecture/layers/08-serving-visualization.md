# Layer 8: Serving and visualization

## Responsibility

Streamlit presents historical analysis from Gold data and recent weather summaries from the serving path. The dashboard should communicate coverage, units, source, and interpretation limits rather than imply stronger ecological conclusions than the data supports.

## Planned files

| File | Purpose | Owning issue |
| --- | --- | --- |
| `src/dashboard/app.py` | Configure dashboard title/layout, navigation, shared filters, and data access. | [#18](https://github.com/MARKUCI14/BigData/issues/18) |
| `src/dashboard/pages/historical_analysis.py` | Explore reported observation counts and temporal/geographic patterns. | [#19](https://github.com/MARKUCI14/BigData/issues/19) |
| `src/dashboard/pages/temperature_analysis.py` | Explore historical climate coverage and trends. | [#20](https://github.com/MARKUCI14/BigData/issues/20) |
| `src/dashboard/pages/correlation_analysis.py` | Present county-month comparison and carefully qualified association results. | [#21](https://github.com/MARKUCI14/BigData/issues/21) |
| `src/dashboard/pages/realtime_monitoring.py` | Show freshness and recent weather-event summaries from the streaming workflow. | [#22](https://github.com/MARKUCI14/BigData/issues/22); current ticket describes butterfly observations and needs alignment. |
| `src/storage/mongodb_client.py` | Provide query methods used by pages. | [#17](https://github.com/MARKUCI14/BigData/issues/17) |
| `docker/streamlit/Dockerfile` | Build the dashboard image if the team keeps a containerized UI. | [#2](https://github.com/MARKUCI14/BigData/issues/2) |

## Design guidance

- Match charts to the research question and available data; start with a small set of useful views.
- Use shared filters consistently and explain what a selected range excludes.
- Show missing/partial coverage and distinguish zero reports from absent source data.
- Label temperature units, event windows, counties, sample sizes, and data refresh time.
- State that reported observations are not population size; correlation is not causation.
- Provide clear empty/error states when MongoDB or the stream is unavailable.
- Avoid exposing precise observation coordinates unless the source terms and project purpose justify it.
