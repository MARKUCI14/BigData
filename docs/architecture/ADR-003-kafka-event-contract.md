# ADR 003 Kafka Event Contract

- Status: Accepted for current code; target source alignment unresolved
- Date: 2026-11-26

## Context

Kafka decouples event production from Spark stream processing. The course brief requires a real or realistically simulated streaming source. The proposal and diagram specify incoming weather measurements; current producer and consumer code use butterfly observation events.

## Decision

The implemented contract is documented in [the data model](../data-model.md): JSON butterfly observation events on `butterfly-observations`, keyed by county and de-duplicated by stable observation ID. This describes current code only. Before claiming alignment with the proposal, the team must either implement a weather event contract or formally change the proposal and diagram to observation events.

## Consequences

County keys preserve partition affinity for a county. Stable IDs support idempotent persistence when Kafka records are replayed. Weather events need an agreed schema and topic before the stream can serve the project's stated weather-input purpose.

