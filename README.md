# StreamHub

Event Hubs-style streaming platform built locally around a Kafka-compatible Redpanda broker, FastAPI control/producer API and PostgreSQL metadata.

## What is implemented

- Streams/topics with configurable partitions and retention
- Partition-key hashing for ordered keys
- Producer REST API + batch API
- Large-file ingestion: CSV, JSON, JSONL/NDJSON and Parquet
- Incremental file parsing; records are published without loading the complete file into RAM
- Per-stream DLQ for rejected upload records
- Schema registry with versions and backward-compatible evolution checks
- API-key roles: admin, producer, consumer
- Per-key REST rate limiting
- Kafka consumer groups and committed-offset lag inspection
- Kafka-compatible receiver connection metadata
- REST receiver/pull endpoint for clients that cannot use Kafka directly
- Live tail in the UI
- Databricks Structured Streaming → Delta Bronze starter notebook
- Health and basic metrics endpoints
- Swagger/OpenAPI at /docs

## Architecture

File/API producer → FastAPI → Redpanda partitions → Kafka consumer groups / REST receiver → Databricks Structured Streaming → Delta Bronze/Silver/Gold.

PostgreSQL stores control-plane metadata. Redpanda stores the durable live event log and retention. MinIO is included for future object-storage workflows; it is not the broker.

## Run locally

PowerShell:
```powershell
Copy-Item .env.example .env
docker compose down
docker compose up -d --build
docker compose ps
```

WSL/bash:
```bash
cp .env.example .env
docker compose down
docker compose up -d --build
docker compose ps
```

Open:
- UI: http://localhost:8000
- Swagger: http://localhost:8000/docs
- MinIO console: http://localhost:9001

The default development admin key is `sh_live_change_me`. Change it in `.env` before exposing anything outside your machine.

## Create a stream

```bash
curl -X POST http://localhost:8000/v1/streams \
  -H "x-api-key: sh_live_change_me" -H "content-type: application/json" \
  -d '{"name":"orders","partitions":3,"retention_hours":24,"partition_key":"customer_id"}'
```

## Send events

```bash
curl -X POST http://localhost:8000/v1/streams/orders/events \
  -H "x-api-key: sh_live_change_me" -H "content-type: application/json" \
  -d '{"customer_id":123,"amount":4500,"city":"Bengaluru"}'
```

Batch:
```bash
curl -X POST http://localhost:8000/v1/streams/orders/events/batch \
  -H "x-api-key: sh_live_change_me" -H "content-type: application/json" \
  -d '{"events":[{"customer_id":1,"amount":10},{"customer_id":2,"amount":20}]}'
```

## Upload files

Supported: CSV, JSON, JSONL/NDJSON and Parquet.

```bash
curl -X POST http://localhost:8000/v1/streams/orders/upload \
  -H "x-api-key: sh_live_change_me" -F "file=@sales.csv"
```

Each valid record becomes an event. Bad records are sent to `orders-dlq`.

For multi-GB production ingestion, put a resumable multipart upload/object-store layer in front of this API. The local parser itself is incremental, but a production HTTP edge should also support resumable/chunked uploads.

## Receiver options

### Kafka-compatible client

Local bootstrap server:
`localhost:19092`

Topic:
`orders`

Example:
```python
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "localhost:19092",
    "group.id": "my-app",
    "auto.offset.reset": "earliest",
})
c.subscribe(["orders"])

while True:
    msg = c.poll(1.0)
    if msg and not msg.error():
        print(msg.partition(), msg.offset(), msg.value().decode())
```

Consumer groups and offsets are handled by the Kafka protocol.

### REST receiver

For clients that cannot speak Kafka:
```
GET /v1/streams/{stream}/events
  ?partition=0
  &offset=latest
  &limit=20
  &timeout_ms=500
```

The response contains partition/offset plus the event. This endpoint is a convenience pull API and does not commit offsets.

Connection details:
```
GET /v1/streams/{stream}/connection?group=databricks
```

This returns the broker endpoint, topic, group and a Databricks connection example.

## Databricks

Databricks cloud cannot reach the local `localhost:19092` address. Run the broker on a reachable VM/load balancer or use another reachable Kafka-compatible endpoint, then set that address in `databricks/bronze_stream.py`.

The notebook pattern is:

Kafka stream → raw Bronze Delta → Silver deduplication/quality → Gold transformations.

Use a Unity Catalog Volume for checkpoints/auxiliary files; the Volume is not the streaming broker.

## Useful endpoints

```
POST   /v1/streams
GET    /v1/streams
GET    /v1/streams/{name}
DELETE /v1/streams/{name}

POST   /v1/streams/{name}/events
POST   /v1/streams/{name}/events/batch
POST   /v1/streams/{name}/upload
GET    /v1/streams/{name}/events
GET    /v1/streams/{name}/connection
GET    /v1/streams/{name}/peek

POST   /v1/streams/{name}/schemas
GET    /v1/streams/{name}/schemas

POST   /v1/streams/{name}/consumer-groups/{group}
GET    /v1/streams/{name}/consumer-groups
GET    /v1/streams/{name}/consumer-groups/{group}/lag

POST   /v1/keys
GET    /v1/keys
DELETE /v1/keys/{id}

GET    /v1/metrics
GET    /health
```

## Important production gaps

This is an Event Hubs-style engineering project, not a drop-in replacement with Azure Event Hubs SLA. Before public/production use, add multi-broker replication, TLS/SASL/OAuth, tenant isolation, resumable object-storage uploads, stronger schema formats (Avro/Protobuf/JSON Schema), quotas/backpressure, Prometheus/Grafana/OpenTelemetry, backups/disaster recovery, and Kubernetes or managed infrastructure.

Delivery is deliberately at-least-once; events carry `event_id` so downstream systems can deduplicate. Exactly-once should only be claimed after validating the full producer-to-sink pipeline.
