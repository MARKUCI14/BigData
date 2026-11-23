# Layer 7: Speed and serving storage

## Responsibility

MongoDB is the proposed low-latency serving store for dashboard queries. It is not the source of truth for raw history; the lakehouse retains reproducible historical outputs.

## Planned files

| File | Purpose | Owning issue |
| --- | --- | --- |
| `docker/mongo/init-mongo.js` | Initialize database/collections and required indexes for a fresh local MongoDB instance. | [#4](https://github.com/MARKUCI14/BigData/issues/4) |
| `src/storage/load_to_mongodb.py` | Idempotently load Gold summaries (and any explicitly required historical records) for serving. | [#14](https://github.com/MARKUCI14/BigData/issues/14) |
| `src/storage/mongodb_client.py` | Encapsulate read queries used by the dashboard. | [#17](https://github.com/MARKUCI14/BigData/issues/17) |
| `docs/mongodb-schema.md` (issue proposal) and `docs/data-model.md` | Document collection grain, keys, indexes, and relationship to lake outputs; avoid maintaining duplicate conflicting definitions. | [#4](https://github.com/MARKUCI14/BigData/issues/4) |

## Data ownership and idempotency

- Keep collection grain and unique keys explicit. Align `id` versus `observation_id` before indexing or upserting.
- Separate county-month historical summaries from live weather aggregates unless a defined query needs them combined.
- Decide whether observations are stored in MongoDB at all; load only what the dashboard needs.
- Use deterministic upserts for repeatable batch loads. Avoid drop/reload behavior that can erase live data or interrupt readers.
- Define indexes from dashboard query patterns and inspect their cost on the chosen deployment.
- Keep credentials in environment configuration and never in initialization scripts or logs.

## Related files

Connection settings belong in `.env.example` as placeholder names/values only; use `.env` locally and keep it ignored. Docker wiring is owned by [#2](https://github.com/MARKUCI14/BigData/issues/2).
