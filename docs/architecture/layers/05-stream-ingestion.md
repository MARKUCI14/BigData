# Layer 5: Streaming ingestion

## Responsibility

Accept incoming weather events and transport them through Kafka to the stream processor. This is the near-real-time path in the teacher-accepted architecture; historical iNaturalist and Copernicus inputs remain the batch path.

## Planned files

| File | Purpose | Owning issue |
| --- | --- | --- |
| `src/ingestion/weather_producer.py` (candidate; confirm issue naming) | Fetch or simulate weather events and publish the agreed event schema. | Existing [#8](https://github.com/MARKUCI14/BigData/issues/8) currently describes butterfly observations; align its scope before implementation. |
| `scripts/test_kafka_producer.py` | Publish a small known event to verify Kafka connectivity. | [#3](https://github.com/MARKUCI14/BigData/issues/3) |
| `scripts/test_kafka_consumer.py` | Consume a known event to verify topic connectivity and serialization. | [#3](https://github.com/MARKUCI14/BigData/issues/3) |
| `config/kafka-config.md` | Document broker/topic configuration and local versus container addresses. | [#3](https://github.com/MARKUCI14/BigData/issues/3) |
| `docker-compose.yml` | Provide Kafka service/network configuration. | [#2](https://github.com/MARKUCI14/BigData/issues/2) |

## Event contract to define

Specify a stable event ID, event timestamp (with timezone), received/ingestion timestamp, location key/coordinates, temperature variable, numeric value and unit, source, and schema version as applicable. Define malformed-event behavior, duplicate/replay behavior, topic partitions, retention, and serialization. Use the actual weather source fields; do not copy the butterfly-observation schema into a weather event.

## Boundary with the historical study

Live weather summaries can demonstrate the streaming architecture without being silently mixed into historical county-month research data. If the team later streams butterfly observations instead, update the accepted architecture documentation and obtain course approval before changing this boundary.
