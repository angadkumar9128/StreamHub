# Databricks notebook: StreamHub (Kafka) -> Delta Bronze/Silver
BOOTSTRAP = "YOUR_PUBLIC_HOST:PORT"
STREAM = "orders"
BRONZE = "main.streamhub.bronze_orders"
SILVER = "main.streamhub.silver_orders"
CHECKPOINT = "/Volumes/main/streamhub/checkpoints/bronze_orders"

from pyspark.sql import functions as F

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", STREAM)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

bronze = (raw
    .select("partition", "offset", F.col("timestamp").alias("kafka_ts"),
            F.col("value").cast("string").alias("raw"))
    .withColumn("event_json", F.from_json(
        "raw",
        "event_id STRING, stream STRING, partition_key STRING, event_time STRING, payload MAP<STRING,STRING>"))
    .select("partition", "offset", "kafka_ts", "event_json.*"))

(bronze.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .toTable(BRONZE))

# Silver example:
# silver = (spark.readStream.table(BRONZE)
#     .withWatermark("event_time", "10 minutes")
#     .dropDuplicates(["event_id"]))
