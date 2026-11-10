"""Start Silver validation/quarantine and Gold aggregation streams.

Run after the Bronze stream is writing Delta files. Paths are configured through
environment variables so no storage account details are hard-coded.
"""
import os
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("trade-silver-gold").getOrCreate()
from src.transform_silver import parse_and_split
from src.aggregate_gold import build_trade_volume

BRONZE_PATH = os.environ["BRONZE_PATH"]
SILVER_PATH = os.environ["SILVER_PATH"]
QUARANTINE_PATH = os.environ["QUARANTINE_PATH"]
GOLD_PATH = os.environ["GOLD_PATH"]
CHECKPOINT_ROOT = os.environ["CHECKPOINT_ROOT"]

bronze = spark.readStream.format("delta").load(BRONZE_PATH)
valid, invalid = parse_and_split(bronze)

# Watermark and deduplicate on the business event key. Choose the watermark
# based on the source's actual late-arrival SLA.
silver = (
    valid.withWatermark("event_time", "10 minutes")
    .dropDuplicates(["trade_id"])
)

silver_query = (
    silver.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver")
    .start(SILVER_PATH)
)

quarantine = invalid.withColumn(
    "rejection_reason",
    F.lit("Failed required-field or business-rule validation")
)
quarantine_query = (
    quarantine.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/quarantine")
    .start(QUARANTINE_PATH)
)

silver_for_gold = spark.readStream.format("delta").load(SILVER_PATH)
gold = build_trade_volume(silver_for_gold, watermark="10 minutes", window="5 minutes")
gold_query = (
    gold.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_ROOT}/gold")
    .start(GOLD_PATH)
)

for query in (silver_query, quarantine_query, gold_query):
    query.awaitTermination()
