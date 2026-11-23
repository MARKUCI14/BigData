# Layer 1: Data sources

## Responsibility

Provide the historical and incoming inputs used by the platform, retaining enough source context to reproduce and interpret results.

## Inputs

| Source | Mode | Role | Planned location/reference |
| --- | --- | --- | --- |
| iNaturalist Clouded Apollo records | Historical batch | Reported butterfly observations for Romania, with source date and location fields. | `data/raw/Parnassius_obs_cleaned.csv` is named in issues; verify the real file, schema, and source terms before coding. |
| Copernicus Climate Data Store | Historical batch | Historical Romanian climate/temperature records to integrate at a documented geographic and temporal grain. | `data/raw/Romania_County_Monthly_Temperatures_2000_2026.csv` is named in issues; verify coverage, units, and layout. |
| Weather API or realistic simulator | Incoming stream | Demonstrate continuously arriving weather events. | Source implementation is open; accepted architecture calls this a weather stream. |

## Source contract to document

For each extract or event source, record source URL/API, extraction time, query/filter, source terms, schema/version, unit, time zone, geographic granularity, coverage, missingness, and provenance fields. Do not assume issue examples or sample counts describe the files actually present.

## Interpretation boundary

iNaturalist records count submissions, not individual butterflies or population size. Reporting effort, access, seasonality, and platform behavior can influence counts. The research question concerns association with reported occurrence, not proof of temperature causation.

## Related planned files

- Source extracts or references under `data/raw/` (subject to license, size, and repository policy).
- `.env.example` only if an API credential/configuration is needed; real secrets stay outside source control.
- Source provenance and field definitions in `docs/data-notes.md` and `docs/data-model.md`.
