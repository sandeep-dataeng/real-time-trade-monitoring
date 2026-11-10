"""Starter: consume Azure Event Hubs events and persist raw events to Delta.

Validate Event Hubs connector options against the Databricks Runtime in use.
Credentials must be supplied through a secret-backed configuration.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("trade-events-to-bronze").getOrCreate()

connection_string = os.environ["EVENTHUBS_CONNECTION_STRING"]
event_hub_name = os.environ["EVENTHUB_NAME"]
bronze_path = os.environ["BRONZE_PATH"]
checkpoint_path = os.environ["CHECKPOINT_PATH"]

# Connector configuration is version-specific; confirm the required connection
# string format and options for the installed Event Hubs Spark connector.
event_hubs_options = {
    "eventhubs.connectionString": connection_string,
    "eventhubs.name": event_hub_name,
}

raw_events = (
    spark.readStream.format("eventhubs")
    .options(**event_hubs_options)
    .load()
)

bronze = raw_events.selectExpr(
    "CAST(body AS STRING) AS raw_body",
    "CAST(sequenceNumber AS BIGINT) AS sequence_number",
    "CAST(enqueuedTime AS TIMESTAMP) AS enqueued_time",
    "CAST(partition AS STRING) AS partition_id",
    "CAST(offset AS STRING) AS offset_value",
).withColumn("ingested_at", current_timestamp())

query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(bronze_path)
)
query.awaitTermination()
