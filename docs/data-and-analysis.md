# Data and analysis notes

## Research framing

The accepted proposal asks whether environmental temperature is associated with changes in reported Clouded Apollo (*Parnassius mnemosyne*) observation counts in Romania. The proposed unit of comparison is county by month over 2000–2026, subject to availability and completeness.

Do not interpret an association as a causal effect. A higher or lower number of iNaturalist submissions may reflect observation effort, season, accessibility, platform adoption, or other changes in addition to species occurrence. The analysis should use the phrase **reported observations** unless an independent population measure is available.

## Sources named in the proposal

- **iNaturalist:** historical Clouded Apollo observations in Romania. Proposal describes a dataset broken down by month and county for 2000–2026.
- **Copernicus Climate Data Store:** historical Romanian temperatures averaged by month and county for 2000–2026.
- **Weather API or simulated stream:** proposed incoming weather records for near-real-time pipeline demonstration.

Exact extracts, variables, units, licensing/terms, spatial resolution, temporal coverage, missing periods, and update dates have not been recorded in this repository. Add them after inspecting the actual datasets.

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
