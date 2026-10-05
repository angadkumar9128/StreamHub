# Databricks notebook source
# MAGIC %md
# MAGIC # StreamHub -> Databricks End-to-End Streaming
# MAGIC
# MAGIC This notebook reads the StreamHub Kafka-compatible stream into Delta/Unity Catalog.
# MAGIC It also creates operational metadata tables and stores checkpoints/support files in a Unity Catalog Volume.
# MAGIC
# MAGIC **Before running:** replace BOOTSTRAP with a hostname/IP reachable from Databricks compute. `localhost:19092` only works for local clients.

# COMMAND ----------

# Configuration
BOOTSTRAP = "YOUR_REACHABLE_HOST:19092"
STREAM = "orders"
GROUP_ID = "databricks-orders-bronze"

CATALOG = "streamhub"
SCHEMA = "streaming"
VOLUME = "streamhub_files"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_{STREAM}"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_{STREAM}"
REGISTRY_TABLE = f"{CATALOG}.{SCHEMA}.stream_registry"
RUNS_TABLE = f"{CATALOG}.{SCHEMA}.ingestion_runs"
QUALITY_TABLE = f"{CATALOG}.{SCHEMA}.event_quality"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
CHECKPOINT = f"{VOLUME_ROOT}/checkpoints/{STREAM}_bronze"
RAW_DIR = f"{VOLUME_ROOT}/raw/{STREAM}"
SCHEMA_DIR = f"{VOLUME_ROOT}/schemas/{STREAM}"
MANIFEST_DIR = f"{VOLUME_ROOT}/manifests/{STREAM}"

print("BOOTSTRAP:", BOOTSTRAP)
print("STREAM:", STREAM)
print("BRONZE:", BRONZE_TABLE)
print("CHECKPOINT:", CHECKPOINT)

# COMMAND ----------

# Create governed Unity Catalog objects.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

for path in [f"{RAW_DIR}/", f"{SCHEMA_DIR}/", f"{MANIFEST_DIR}/", f"{CHECKPOINT}/"]:
    dbutils.fs.mkdirs(path)

print("Unity Catalog and Volume ready:", VOLUME_ROOT)

# COMMAND ----------

# Connectivity smoke test.
# This creates a small batch Kafka DataFrame and reads metadata only.
# If this fails, fix network/firewall/advertised Kafka address before starting the stream.

probe = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", STREAM)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .option("kafka.request.timeout.ms", "10000")
    .option("kafka.session.timeout.ms", "10000")
    .load()
)

print("Kafka connection OK")
print("Existing records:", probe.count())
display(probe.select("topic", "partition", "offset", "timestamp").limit(20))

# COMMAND ----------

# StreamHub event envelope schema.
# payload_json is deliberately preserved as JSON text in Bronze so that Bronze
# remains replayable even when source payloads evolve.
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

envelope_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("stream", StringType(), True),
    StructField("partition_key", StringType(), True),
    StructField("event_time", StringType(), True),
])

# COMMAND ----------

# Create operational metadata tables.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
  stream STRING,
  kafka_bootstrap STRING,
  consumer_group STRING,
  bronze_table STRING,
  silver_table STRING,
  checkpoint_path STRING,
  registered_at TIMESTAMP,
  status STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
  stream STRING,
  query_name STRING,
  batch_id BIGINT,
  input_rows BIGINT,
  min_kafka_offset BIGINT,
  max_kafka_offset BIGINT,
  processed_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUALITY_TABLE} (
  stream STRING,
  batch_id BIGINT,
  input_rows BIGINT,
  valid_event_ids BIGINT,
  null_event_ids BIGINT,
  processed_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

# Register this pipeline configuration in the Databricks metadata layer.
spark.sql(f"""
MERGE INTO {REGISTRY_TABLE} t
USING (
  SELECT
    '{STREAM}' AS stream,
    '{BOOTSTRAP}' AS kafka_bootstrap,
    '{GROUP_ID}' AS consumer_group,
    '{BRONZE_TABLE}' AS bronze_table,
    '{SILVER_TABLE}' AS silver_table,
    '{CHECKPOINT}' AS checkpoint_path,
    current_timestamp() AS registered_at,
    'configured' AS status
) s
ON t.stream = s.stream AND t.consumer_group = s.consumer_group
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

display(spark.table(REGISTRY_TABLE))

# COMMAND ----------

# Kafka -> Bronze.
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", STREAM)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("kafka.group.id", GROUP_ID)
    .load()
)

bronze_stream = (
    raw_stream
    .select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
    )
    .withColumn("event", F.from_json("raw_json", envelope_schema))
    .select(
        "topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "kafka_key", "raw_json",
        F.col("event.event_id").alias("event_id"),
        F.col("event.stream").alias("stream"),
        F.col("event.partition_key").alias("partition_key"),
        F.col("event.event_time").alias("event_time"),
        F.get_json_object("raw_json", "$.payload").alias("payload_json"),
        F.current_timestamp().alias("ingested_at"),
    )
)

# COMMAND ----------

# Idempotent micro-batch writer.
# Bronze is keyed by event_id; a replay/restart does not intentionally create
# duplicate event envelopes when the same event_id is seen again.
from delta.tables import DeltaTable

def write_bronze_and_metadata(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df.createOrReplaceTempView("_streamhub_bronze_batch")

    if not spark.catalog.tableExists(BRONZE_TABLE):
        (
            batch_df.limit(0)
            .write
            .format("delta")
            .saveAsTable(BRONZE_TABLE)
        )

    delta_target = DeltaTable.forName(spark, BRONZE_TABLE)

    (
        delta_target.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.event_id = s.event_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    stats = (
        batch_df.agg(
            F.count("*").alias("input_rows"),
            F.count("event_id").alias("valid_event_ids"),
            F.sum(F.when(F.col("event_id").isNull(), 1).otherwise(0)).alias("null_event_ids"),
            F.min("kafka_offset").alias("min_kafka_offset"),
            F.max("kafka_offset").alias("max_kafka_offset"),
        )
        .collect()[0]
    )

    run_row = spark.createDataFrame([(
        STREAM,
        f"streamhub_{STREAM}_bronze",
        int(batch_id),
        int(stats["input_rows"]),
        int(stats["min_kafka_offset"]) if stats["min_kafka_offset"] is not None else None,
        int(stats["max_kafka_offset"]) if stats["max_kafka_offset"] is not None else None,
        None,
    )], "stream STRING, query_name STRING, batch_id BIGINT, input_rows BIGINT, min_kafka_offset BIGINT, max_kafka_offset BIGINT, processed_at TIMESTAMP")         .withColumn("processed_at", F.current_timestamp())

    run_row.write.mode("append").format("delta").saveAsTable(RUNS_TABLE)

    quality_row = spark.createDataFrame([(
        STREAM,
        int(batch_id),
        int(stats["input_rows"]),
        int(stats["valid_event_ids"]),
        int(stats["null_event_ids"]),
        None,
    )], "stream STRING, batch_id BIGINT, input_rows BIGINT, valid_event_ids BIGINT, null_event_ids BIGINT, processed_at TIMESTAMP")         .withColumn("processed_at", F.current_timestamp())

    quality_row.write.mode("append").format("delta").saveAsTable(QUALITY_TABLE)

# COMMAND ----------

# Start the continuous Bronze query.
# The checkpoint is durable in the Unity Catalog Volume.
bronze_query = (
    bronze_stream.writeStream
    .queryName(f"streamhub_{STREAM}_bronze")
    .foreachBatch(write_bronze_and_metadata)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

print("Started:", bronze_query.name)
print("Checkpoint:", CHECKPOINT)

# COMMAND ----------

# Monitor the running query.
display(
    spark.sql(f"""
      SELECT *
      FROM {RUNS_TABLE}
      WHERE stream = '{STREAM}'
      ORDER BY processed_at DESC
      LIMIT 20
    """)
)

# COMMAND ----------

# Send a test event from StreamHub while the previous cell is running:
#
# curl -X POST http://localhost:8000/v1/streams/orders/events \
#   -H "x-api-key: sh_live_change_me" \
#   -H "Content-Type: application/json" \
#   -d '{"customer_id":9001,"amount":9999.50,"city":"Bengaluru","product":"Databricks-Test"}'
#
# Then query Bronze:
display(
    spark.sql(f"""
      SELECT event_id, stream, partition_key, event_time,
             kafka_partition, kafka_offset, kafka_timestamp,
             payload_json, ingested_at
      FROM {BRONZE_TABLE}
      ORDER BY ingested_at DESC
      LIMIT 20
    """)
)

# COMMAND ----------

# Example Silver transformation for an orders stream.
# Update the schema below to match your actual stream contract.
order_payload_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("city", StringType(), True),
    StructField("product", StringType(), True),
])

silver_stream = (
    spark.readStream.table(BRONZE_TABLE)
    .withColumn("payload", F.from_json("payload_json", order_payload_schema))
    .select(
        "event_id", "stream", "partition_key", "event_time",
        "kafka_partition", "kafka_offset", "kafka_timestamp",
        F.col("payload.customer_id").alias("customer_id"),
        F.col("payload.amount").cast("double").alias("amount"),
        F.col("payload.city").alias("city"),
        F.col("payload.product").alias("product"),
        "ingested_at",
    )
    .dropDuplicates(["event_id"])
)

SILVER_CHECKPOINT = f"{VOLUME_ROOT}/checkpoints/{STREAM}_silver"

silver_query = (
    silver_stream.writeStream
    .queryName(f"streamhub_{STREAM}_silver")
    .format("delta")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .outputMode("append")
    .toTable(SILVER_TABLE)
)

print("Started Silver:", silver_query.name)

# COMMAND ----------

# Inspect data and operational metadata.
display(spark.table(SILVER_TABLE).orderBy(F.col("ingested_at").desc()).limit(50))

# COMMAND ----------

display(spark.table(RUNS_TABLE).orderBy(F.col("processed_at").desc()).limit(50))

# COMMAND ----------

display(spark.table(QUALITY_TABLE).orderBy(F.col("processed_at").desc()).limit(50))

# COMMAND ----------

# Volume contents. This should contain checkpoints/support files created by the pipeline.
display(dbutils.fs.ls(VOLUME_ROOT))

# COMMAND ----------

# Optional: save a copy of the current pipeline configuration as JSON in the Volume.
config_json = f'''{{
  "stream": "{STREAM}",
  "bootstrap": "{BOOTSTRAP}",
  "consumer_group": "{GROUP_ID}",
  "bronze_table": "{BRONZE_TABLE}",
  "silver_table": "{SILVER_TABLE}",
  "checkpoint": "{CHECKPOINT}"
}}'''
dbutils.fs.put(f"{SCHEMA_DIR}/pipeline_config.json", config_json, overwrite=True)
print(f"Saved {SCHEMA_DIR}/pipeline_config.json")

# COMMAND ----------

# Stop only when intentionally ending a test session.
# bronze_query.stop()
# silver_query.stop()
#
# Do not delete checkpoints to solve normal restart issues. A checkpoint stores
# source offsets, commits and state needed to resume the streaming query.
