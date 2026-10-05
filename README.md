# StreamHub V1

Event Hubs-style streaming platform on Redpanda (Kafka API) + FastAPI + Postgres + MinIO.

## Run (WSL2 / Docker)
    docker compose up -d --build
    # API docs: http://localhost:8000/docs   | MinIO console: http://localhost:9001

## Try it
    K="x-api-key: sh_live_change_me"
    curl -X POST localhost:8000/v1/streams -H "$K" -H 'content-type: application/json' \
      -d '{"name":"orders","partitions":3,"retention_hours":24,"partition_key":"customer_id"}'

    curl -X POST localhost:8000/v1/streams/orders/events -H "$K" -H 'content-type: application/json' \
      -d '{"customer_id":123,"amount":4500,"city":"Bengaluru"}'

    curl -X POST localhost:8000/v1/streams/orders/upload -H "$K" -F file=@sales.csv

    curl localhost:8000/v1/streams/orders -H "$K"
    curl localhost:8000/v1/streams/orders/consumer-groups/databricks/lag -H "$K"

Read with any Kafka client at `localhost:19092`, e.g. `docker compose exec redpanda rpk topic consume orders`.

## Databricks
See `databricks/bronze_stream.py`. Databricks cloud cannot reach `localhost`; use a tunnel or a hosted broker.

## Not yet built (next phases)
Schema registry, per-user keys/RBAC (V1 uses one admin key), Parquet/Avro upload, retry policy,
rate limiting, dashboard (Next.js), Prometheus/Grafana, TLS/SASL on the broker.

## V1.1 additions
- **Dashboard:** http://localhost:8000 (enter your admin key from `.env`): create/delete streams, publish, upload, schemas, lag, live tail, API keys.
- **Schema registry:** `POST /v1/streams/{n}/schemas` with `{"field":"integer|string|double|boolean"}`; backward-compatibility enforced; invalid events rejected (422) or sent to the DLQ on upload.
- **Roles:** admin / producer / consumer keys via `/v1/keys`; per-key rate limit (`RATE_LIMIT_PER_SEC`, default 200).
- **Upload formats:** CSV, JSONL, NDJSON, Parquet.
