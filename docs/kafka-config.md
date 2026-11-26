# Kafka Configuration

## Topic: butterfly-observations

Compose creates `butterfly-observations` by default after Kafka becomes healthy. Set `KAFKA_TOPIC` in the root `.env` file to use a different topic; the Kafka initializer and application containers receive the same value.

| Property | Value |
|----------|-------|
| Topic Name | `butterfly-observations` |
| Partitions | 3 |
| Replication Factor | 1 |
| Retention | 7 days (604800000 ms) |
| Message Key | County name (string) |
| Message Value | JSON (UTF-8) |

## Connection

| Context | Broker Address |
|---------|---------------|
| From Docker network | `kafka:29092` |
| From host machine | `localhost:9092` |

## Message Schema

```json
{
  "id": 171573651,
  "observed_on": "2025-06-15",
  "latitude": 45.9483689,
  "longitude": 26.5774343,
  "county": "Vrancea",
  "timestamp": "2025-06-15T10:30:00Z",
  "source": "stream"
}
```

## Partitioning Strategy

Messages are keyed by county name to ensure all observations from the same county
are delivered to the same partition, maintaining per-county ordering.
