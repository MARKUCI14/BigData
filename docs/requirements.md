# Requirements

## Project scope

The project investigates the relationship between environmental temperature and reported Clouded Apollo (*Parnassius mnemosyne*) observations in Romania. The proposal's target grain is county by month over 2000–2026, conditional on source availability and completeness.

Research question: **To what extent is environmental temperature associated with changes in the number of reported Clouded Apollo observations in Romania?**

This is an association study. Citizen-science observation counts do not directly measure the number of individuals in the wild.

## Functional requirements

| ID | Requirement | Status |
| --- | --- | --- |
| F-01 | Ingest historical Clouded Apollo records from iNaturalist data supplied or acquired by the team. | Planned |
| F-02 | Ingest historical Romanian temperature data from Copernicus Climate Data Store, retaining source period and geographic fields. | Planned |
| F-03 | Accept a third input of incoming weather records via a weather API or realistically simulated stream. | Planned; source to confirm |
| F-04 | Preserve raw inputs and produce cleaned/standardized datasets with documented transformations. | Planned |
| F-05 | Integrate butterfly and climate data on an explicitly defined temporal and geographic basis. | Planned; join keys and methodology open |
| F-06 | Support historical batch processing and monthly/county summaries. | Planned |
| F-07 | Process incoming weather records continuously or in micro-batches and make recent summaries available. | Planned |
| F-08 | Provide meaningful descriptive analytics, trend views, and an analysis of association between temperature and reported observations. | Planned; statistical method open |
| F-09 | Present results using charts, tables, and reports in a Streamlit interface or documented alternative. | Planned |
| F-10 | Provide reproducible instructions for setup, data preparation, and execution. | Planned |

## Non-functional requirements

| ID | Requirement |
| --- | --- |
| N-01 | Architectural coherence: show how ingestion, storage, batch processing, streaming, serving, and visualization work together. |
| N-02 | Reproducibility: document source versions/periods, configuration, transformations, and execution steps sufficiently to repeat analysis. |
| N-03 | Data quality: validate required fields and types; make handling of invalid, missing, duplicate, and out-of-range records explicit. |
| N-04 | Traceability: preserve raw data or a recoverable source copy and document lineage into analytical outputs. |
| N-05 | Interpretability: clearly label units, dates, geographic levels, missingness, and the limits of observation-based measures. |
| N-06 | Scalability: explain how the design handles growth in historical records and continuously arriving events; demonstrate appropriate Big Data technologies. |
| N-07 | Maintainability: keep configuration and secrets out of source code; document interfaces and significant decisions. |
| N-08 | Course compliance: prepare documentation and execution/deployment instructions. |

## Constraints and exclusions

- The primary goal is an end-to-end Big Data platform, not a machine-learning model. Machine learning is optional.
- Association does not establish causation.
- iNaturalist reports are influenced by observer effort, accessibility, seasonality, and reporting practices; they are not a population census.
- The date range and coverage depend on the availability and completeness of both datasets.
- The technology diagram is preliminary. Confirm actual selected technologies and their feasibility in the decision log.

## Acceptance evidence

Evidence should include runnable batch and streaming workflows, integrated data, analytics and visualizations, a coherent architecture explanation, and reproducible setup/execution documentation. See [course requirements and deliverables](course/course-requirements.md).
