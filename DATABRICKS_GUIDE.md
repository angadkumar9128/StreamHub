# StreamHub + Databricks Integration Guide

## 1. Architecture

StreamHub has four separate responsibilities:

1. **StreamHub API** — creates streams, accepts events/files, validates schemas, exposes receiver APIs.
2. **Redpanda/Kafka** — durable live event transport and consumer groups.
3. **PostgreSQL** — StreamHub control-plane metadata such as streams, schemas and API-key metadata.
4. **Databricks + Unity Catalog** — analytics/storage layer for the data consumed from StreamHub.

Recommended Databricks layout:

```
StreamHub -> Redpanda/Kafka -> Databricks Structured Streaming
                                  |
                                  +-> Bronze Delta table
                                  +-> Silver Delta table
                                  +-> Gold tables
                                  |
                                  +-> Unity Catalog metadata tables
                                  +-> Unity Catalog Volume
                                      - checkpoints
                                      - Auto Loader/schema artifacts
                                      - optional raw file archive
```

A Unity Catalog **table** should be the primary home for tabular analytics data and metadata. A Unity Catalog **Volume** should be used for files and streaming support artifacts. Databricks recommends volumes for non-tabular files and table names for tabular data.

## 2. Important: Databricks cannot use localhost

If StreamHub is running on your Windows/WSL laptop:

```
localhost:19092
```

means the laptop, not Databricks Cloud.

For Databricks Cloud, Redpanda must be reachable from the Databricks compute network.

### Option A — recommended test/production-like setup

Run StreamHub/Redpanda on a reachable Linux VM or cloud host and expose Kafka on TCP 19092.

Set:

```env
KAFKA_EXTERNAL_HOST=<reachable-dns-or-public-ip>
PUBLIC_KAFKA_BOOTSTRAP=<reachable-dns-or-public-ip>:19092
```

The Docker Compose configuration advertises the same host to external Kafka clients.

Do not expose a production Kafka broker as unauthenticated PLAINTEXT. For production use TLS/SASL or another supported authentication mechanism, firewall the port, and use a private network where possible.

### Option B — local development

Keep:

```env
KAFKA_EXTERNAL_HOST=localhost
PUBLIC_KAFKA_BOOTSTRAP=localhost:19092
```

Use this only from clients that run on the same machine/network. It is not a Databricks Cloud endpoint.

## 3. Reconfigure StreamHub for an external broker

Copy the example environment:

```bash
cp .env.example .env
```

For a reachable host:

```env
KAFKA_EXTERNAL_HOST=YOUR_REACHABLE_HOST
PUBLIC_KAFKA_BOOTSTRAP=YOUR_REACHABLE_HOST:19092
KAFKA_BROKERS=redpanda:9092
DATABASE_URL=postgresql://streamhub:streamhub@postgres:5432/streamhub
```

Restart:

```bash
docker compose up -d --build
```

Verify:

```bash
docker exec streamhub-redpanda-1 rpk cluster health
curl http://localhost:8000/health
```

Then verify the advertised address:

```bash
curl -H "x-api-key: sh_live_change_me" \
  "http://localhost:8000/v1/streams/orders/connection?group=databricks"
```

The returned `bootstrap_server` must be the address that Databricks can reach.

## 4. Databricks prerequisites

You need:

- A Databricks workspace.
- Unity Catalog enabled.
- Compute using a supported Databricks Runtime. Unity Catalog Volumes require DBR 13.3 LTS or above.
- Permission to create/use a catalog and schema, or an existing catalog/schema supplied by your administrator.
- Permission to create/use a volume.
- Network connectivity from Databricks compute to the StreamHub Kafka endpoint.
- Kafka connectivity on TCP 19092 (or your configured port).

For streaming checkpoints, use a Unity Catalog-governed storage location. Do not put production checkpoints in the DBFS root.

## 5. Create a Databricks catalog/schema/volume

If you have permission:

```sql
CREATE CATALOG IF NOT EXISTS streamhub;
CREATE SCHEMA IF NOT EXISTS streamhub.streaming;
CREATE VOLUME IF NOT EXISTS streamhub.streaming.streamhub_files;
```

The volume path is:

```
/Volumes/streamhub/streaming/streamhub_files/
```

Recommended directories:

```
/Volumes/streamhub/streaming/streamhub_files/raw/
/Volumes/streamhub/streaming/streamhub_files/checkpoints/
/Volumes/streamhub/streaming/streamhub_files/schemas/
/Volumes/streamhub/streaming/streamhub_files/manifests/
```

Do not create tables inside a volume directory. Volumes and Unity Catalog tables are separate governed objects.

## 6. Databricks metadata tables

The supplied notebook creates these tables:

- `streamhub.streaming.stream_registry`
- `streamhub.streaming.ingestion_runs`
- `streamhub.streaming.file_manifest`
- `streamhub.streaming.event_quality`

These are Delta/Unity Catalog tables and are separate from the StreamHub PostgreSQL control-plane metadata.

The metadata tables let you answer questions such as:

- Which StreamHub stream was consumed?
- When did ingestion start?
- How many records were processed?
- How many records failed?
- What source file/event batch produced the data?
- What was the last observed Kafka offset?
- Which Databricks table contains the processed data?

## 7. Bronze / Silver / Gold

### Bronze

Bronze preserves the Kafka event envelope and ingestion metadata:

```
event_id
stream
partition_key
event_time
kafka_partition
kafka_offset
kafka_timestamp
ingested_at
payload_json
```

Use Bronze for replay/debugging and lineage.

### Silver

Silver parses the payload into useful columns, performs type conversion, validation and deduplication.

A typical rule is:

```
dropDuplicates(["event_id"])
```

Do not blindly cast every field to a string if you know the schema. Use an explicit schema for production pipelines.

### Gold

Gold contains business-ready aggregations, for example:

```
daily_sales
sales_by_city
sales_by_product
customer_metrics
```

## 8. Run the notebook

Open:

```
databricks/streamhub_end_to_end.py
```

Upload/import it into your Databricks workspace.

Set these parameters near the top:

```python
BOOTSTRAP = "YOUR_REACHABLE_HOST:19092"
STREAM = "orders"

CATALOG = "streamhub"
SCHEMA = "streaming"
VOLUME = "streamhub_files"
```

The notebook first checks Kafka connectivity, then creates the Unity Catalog objects, then starts the Bronze stream.

Use a unique checkpoint for each stream:

```
/Volumes/streamhub/streaming/streamhub_files/checkpoints/orders_bronze
```

Never reuse a checkpoint between unrelated streaming queries.

## 9. Test the live stream

Start the notebook stream.

Then from StreamHub send:

```bash
curl -X POST http://localhost:8000/v1/streams/orders/events \
  -H "x-api-key: sh_live_change_me" \
  -H "Content-Type: application/json" \
  -d '{"customer_id":1001,"amount":4999.50,"city":"Bengaluru","product":"Laptop"}'
```

Send several more events.

The Databricks Bronze table should receive the events.

Query:

```sql
SELECT *
FROM streamhub.streaming.bronze_orders
ORDER BY ingested_at DESC
LIMIT 20;
```

## 10. Test a file upload

Upload a CSV/JSONL/NDJSON/Parquet file through the StreamHub UI.

StreamHub converts each valid record into an event and publishes it to the selected stream. Invalid records go to the stream DLQ.

Databricks consumes those events exactly like API-produced events.

This gives you one ingestion path for:

- API events
- CSV
- JSONL/NDJSON
- Parquet

and one downstream streaming path into Databricks.

## 11. Store source files in the Volume

For workloads where you also need the original files, store them under:

```
/Volumes/streamhub/streaming/streamhub_files/raw/
```

Use this for:

- original CSV/JSON/Parquet files
- replay/archive files
- schema files
- manifests
- pipeline support artifacts

Use Unity Catalog tables for the processed tabular data.

## 12. Auto Loader

If files are placed directly into a Unity Catalog Volume and you want file-arrival ingestion, Auto Loader can ingest incrementally.

Conceptually:

```
Volume raw files
      |
      v
Auto Loader
      |
      v
Bronze Delta
      |
      v
Silver
      |
      v
Gold
```

Keep Auto Loader schema/checkpoint state in a Unity Catalog-governed location.

For StreamHub's Kafka path, Structured Streaming from Kafka is the primary live-ingestion pattern. Auto Loader is complementary for file-based ingestion.

## 13. Kafka consumer group

Use a stable group name such as:

```
databricks-orders-bronze
```

Do not create a random group for every notebook run if you expect the stream to resume from its previous position.

Consumer group + checkpoint state together provide the operational resume/restart behavior of the pipeline.

## 14. Starting offsets

For a first test:

```python
.option("startingOffsets", "earliest")
```

This lets the test notebook read existing events.

For a production pipeline, decide deliberately whether the first deployment should replay history or begin from the latest available data.

After the first successful run, checkpoint state controls progress.

## 15. Troubleshooting

### Kafka connection timeout

Check:

1. Databricks compute can reach the host.
2. TCP 19092 is open.
3. The advertised Kafka address is not `localhost`.
4. Firewall/security-group rules allow the Databricks network.
5. Redpanda advertises the same reachable address.

### Stream starts but no records arrive

Check:

```sql
SELECT * FROM streamhub.streaming.bronze_orders
ORDER BY ingested_at DESC
LIMIT 20;
```

Then check StreamHub:

```bash
curl -H "x-api-key: sh_live_change_me" \
  "http://localhost:8000/v1/streams/orders/peek?limit=20"
```

Then inspect the consumer group lag from StreamHub.

### Checkpoint error

Use a new checkpoint directory only when intentionally creating a new independent query. Do not delete a production checkpoint just to make an error disappear.

### Schema errors

First inspect the Bronze raw payload. Then update the explicit Silver parsing schema. Do not silently change the Bronze raw contract.

## 16. Security checklist before production

- Replace the default StreamHub admin key.
- Do not expose Redpanda with unauthenticated PLAINTEXT.
- Use TLS/SASL or a private network.
- Restrict firewall/security-group access.
- Use Unity Catalog permissions for catalogs, schemas, tables and volumes.
- Store secrets in Databricks secret management rather than notebook source.
- Use separate checkpoints per pipeline.
- Use separate Kafka consumer groups for independent applications.
- Add monitoring and alerting.
- Configure backups and disaster recovery.
- Use multiple Redpanda brokers and replication for production.

## 17. Final mental model

Think of the platform this way:

**StreamHub**
= ingestion/control plane

**Redpanda**
= live event transport

**PostgreSQL**
= StreamHub control metadata

**Databricks Delta tables**
= governed analytical data + pipeline metadata

**Unity Catalog Volumes**
= governed files + checkpoints + schema/support artifacts

**Databricks Structured Streaming**
= continuous Kafka-to-Delta processing

This separation keeps transport, operational metadata, files and analytical tables from being mixed together.
