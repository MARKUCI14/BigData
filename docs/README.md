# Project documentation

This directory describes the Clouded Apollo and climate data platform, its course requirements, proposed architecture, data model, and implementation decisions. Its layout separates architecture records, course materials, and development guidance. The repository is currently in the planning stage; proposed designs are labeled as plans and should be updated as implementation becomes concrete.

## Documents

- [Requirements](requirements.md) — research scope and functional/non-functional expectations.
- [Architecture overview](architecture/overview.md) — proposed components and data flows.
- [Architecture decision records](architecture/) — rationale and status for significant design choices.
- [Data model](data-model.md) — conceptual entities and candidate storage layout.
- [Technical decisions](technical-decisions.md) — open decisions and current direction.
- [Development guides](dev-guide/) — setup and contribution guidance.
- [Course requirements and deliverables](course/course-requirements.md) — mapping from the 2026–2027 brief to project evidence.
- [Data and analysis notes](data-notes.md) — source assumptions, limitations, and analysis principles.

## Project summary

The project asks: to what extent is environmental temperature associated with changes in reported Clouded Apollo (*Parnassius mnemosyne*) observations in Romania? The proposal targets monthly and county-level data for 2000–2026, subject to data availability. Historical butterfly observations and climate records are processed in batch; incoming weather records are planned to demonstrate streaming. Analytics and visualizations are intended to support historical and recent views.

Observation totals are not equivalent to population size. They reflect platform reporting as well as ecological occurrence and must be interpreted accordingly.

## Keeping documentation current

When implementation changes a component, interface, schema, command, dependency, or decision, update the corresponding document in the same change. Label each item as **planned**, **implemented**, or **open** where its status could be misunderstood. Keep course deliverables traceable to implementation and demonstration evidence.
