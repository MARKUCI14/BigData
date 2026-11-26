# Near-Real-Time Pipeline

## Purpose and target

The project proposal and architecture diagram call for a realistic stream of incoming weather measurements. Kafka is the transport layer; Spark Structured Streaming validates and processes events; MongoDB serves recent results to the dashboard.

## Current implementation

The current producer in `src/ingestion/observation_producer.py` replays historical butterfly observations or generates synthetic observation-shaped events. It publishes JSON to Kafka topic `butterfly-observations`, keyed by county. The current Spark consumer in `src/streaming/stream_processor.py` consumes that same observation contract, writes invalid records to a Parquet dead-letter path, upserts valid observations, and updates per-county real-time statistics in MongoDB.

This is a working code path definition, not evidence that the full path has run successfully. Docker startup is currently blocked by the Spark image reference recorded in [project documentation](../README.md).

## Processing semantics

1. A producer sends JSON values to Kafka and awaits broker acknowledgment.
2. Spark reads the configured topic with Structured Streaming and uses checkpoint storage to track progress.
3. Each micro-batch is parsed against the event schema and checked for required fields, date, county, and coordinate validity.
4. Rejected values are written to a dead-letter Parquet directory with a reason and Kafka coordinates.
5. Accepted observations are de-duplicated by stable observation ID and upserted to MongoDB.
6. County-level real-time statistics are updated by the aggregation module.

The consumer starts at `latest` in a fresh checkpoint. Changing or deleting checkpoints changes replay behavior and should be treated as an operational decision.

## Required alignment work

The team needs to resolve the mismatch between the weather stream in the proposal and observation stream in the code. To match the proposal, define the weather event fields and units, choose API versus simulation, decide the Kafka topic, implement the producer and consumer transformations, and update MongoDB/dashboard definitions. If the team instead chooses to stream butterfly observations, revise the proposal and architecture diagram so they describe that choice accurately.

