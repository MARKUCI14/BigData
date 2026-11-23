# Data and analysis notes

## Research framing

The accepted proposal asks whether environmental temperature is associated with changes in reported Clouded Apollo (*Parnassius mnemosyne*) observation counts in Romania. The proposed unit of comparison is county by month over 2000–2026, subject to availability and completeness.

Do not interpret an association as a causal effect. A higher or lower number of iNaturalist submissions may reflect observation effort, season, accessibility, platform adoption, or other changes in addition to species occurrence. The analysis should use the phrase **reported observations** unless an independent population measure is available.

## Sources named in the proposal

- **iNaturalist:** historical Clouded Apollo observations in Romania. Proposal describes a dataset broken down by month and county for 2000–2026.
- **Copernicus Climate Data Store:** historical Romanian temperatures averaged by month and county for 2000–2026.
- **Weather API or simulated stream:** proposed incoming weather records for near-real-time pipeline demonstration.

Exact extracts, variables, units, licensing/terms, spatial resolution, temporal coverage, missing periods, and update dates have not been recorded in this repository. Add them after inspecting the actual datasets.

## Example workbooks inspected (provided 2026-11-23)

The team provided Excel examples in the assignment resources folder. These were inspected read-only; they have not been copied into this repository.

| Workbook | Populated sheet | Observed layout | Initial implications |
| --- | --- | --- | --- |
| `Parnassius_obs_cleaned.xlsx` | `Munka1` (`A1:E72`); `Munka2` and `Munka3` are empty | Header fields: `id`, `observed_on`, `latitude`, `longitude`, `place_county_name`; 71 observation rows. `observed_on` cells are Excel date serial values in the workbook storage and should be read as dates by a workbook-aware reader. | Validate identifier uniqueness, date parsing, county completeness, and coordinate quality against the actual workbook. Do not assume all values are strings because the XML representation is numeric. |
| `Romania_County_Monthly_Temperatures_2000_2026.xlsx` | `Munka1` (`A1:LK43`); `Munka2` and `Munka3` are empty | Header has an unnamed first/index column, `County`, and 321 monthly columns from `X2000.01` through `X2026.09`; 42 county rows. Values are monthly temperatures. | Confirm the source unit and meaning of each monthly value from its source documentation. Drop the export index during processing, then reshape wide months to county/year/month rows. Sep 2026 is the last populated month in this example; do not infer later months are zero. |

These profiles describe the supplied examples only. Confirm that they are the team-approved source snapshots, determine source provenance/terms, and record any replacement/version changes before building ingestion around them. The workbooks currently live outside the repository; setup instructions should explain local placement or acquisition rather than assuming they are committed.

## Integration questions to resolve

- Are the two historical sources measured at compatible county boundaries and spatial granularity?
- Which timezone/calendar and county identifier will be canonical?
- Does the temperature metric represent monthly mean, daily-derived mean, or another measure, and in which unit?
- How will incomplete months, zero observations, and unavailable climate data be distinguished?
- Is 2026 a partial year for either source?
- What data quality and eligibility filters are applied to citizen-science records?
- What statistical method is appropriate for sparse/count-valued observations, temporal trends, and uneven observation effort?

## Minimum provenance to retain

Record source name and extract date, source query/filter, original field names or schema version, units, geographic/time coverage, license or terms reference, transformation version, and known data limitations. Keep raw inputs separate from derived records where allowed by the source terms.
