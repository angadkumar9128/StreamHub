# Databricks notebook: StreamHub (Kafka) -> Delta Bronze
# NOTE: Databricks must be able to reach the broker. localhost:19092 is NOT reachable from
# Databricks cloud; expose Redpanda via a tunnel (e.g. ngrok tcp 19092 / cloudflared) or host it on a VM.
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

BOOTSTRAP = "YOUR_PUBLIC_HOST:PORT"
STREAM = "orders"
BRONZE = "main.streamhub.bronze_orders"
CHECKPOINT = "/Volumes/main/streamhub/checkpoints/bronze_orders"

envelope = StructType([
    StructField("event_id", StringType()),
    StructField("stream", StringType()),
    StructField("partition_key", StringType()),
    StructField("event_time", StringType()),
    StructField("payload", StringType()),   # keep raw JSON in bronze; parse in silver
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", STREAM)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

bronze = (raw
    .select(F.col("partition"), F.col("offset"), F.col("timestamp").alias("kafka_ts"),
            F.col("value").cast("string").alias("raw"))
    .withColumn("evt", F.from_json(F.get_json_object("raw", "$"), "event_id STRING, event_time STRING"))
    .withColumn("payload", F.get_json_object("raw", "$.payload"))
    .select("partition", "offset", "kafka_ts", "evt.event_id", "evt.event_time", "payload"))

(bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)   # swap for processingTime="10 seconds" for continuous
    .toTable(BRONZE))

# Silver (batch/stream): dedupe on event_id (at-least-once delivery)
# spark.readStream.table(BRONZE).dropDuplicates(["event_id"]) ...
