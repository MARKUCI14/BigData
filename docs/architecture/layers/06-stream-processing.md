# Layer 6: Stream processing

## Responsibility

Use Spark Structured Streaming to consume Kafka weather events, validate and transform them, calculate recent summaries, and publish results to serving storage and/or a lakehouse stream output.

## Planned files

| File | Purpose | Owning issue |
| --- | --- | --- |
| `src/streaming/stream_processor.py` | Read Kafka events, parse the schema, apply quality rules, checkpoint progress, and write processed records. | [#15](https://github.com/MARKUCI14/BigData/issues/15) currently defines observation events; align the contract with the accepted weather stream. |
| `src/streaming/realtime_aggregator.py` | Calculate recent weather summaries at the chosen location and event-time window grain. | [#16](https://github.com/MARKUCI14/BigData/issues/16) currently describes observation counts; revise with the stream scope. |
| `data/checkpoints/streaming/` | Durable local checkpoint location mounted for the streaming job; not source controlled. | [#15](https://github.com/MARKUCI14/BigData/issues/15) |
| Dead-letter output (location TBD) | Retain or count malformed events with safe diagnostic metadata for review. | Define in the stream issue before implementation. |

## Reliability requirements

- Choose event-time versus processing-time windows and define late/out-of-order handling.
- Keep checkpoints on persistent storage and document reset/recovery behavior.
- Make sinks safe under micro-batch retry and Kafka replay. Increment-only writes can double count; use stable keys/batch IDs or deterministic upserts.
- Define how malformed events are reported and what happens when MongoDB or the lake is unavailable.
- Run the stream independently from the historical batch workflow and document how to stop it cleanly.

## Monitoring outputs

Expose event time, processing/update time, counts, and freshness so the dashboard can distinguish a quiet source from a failed processor. Keep units, locations, and aggregation windows visible.
