"""Local substitute for Event Hubs ingestion using Spark's file streaming source.

This validates Structured Streaming and Delta transformations locally. It is
not an implementation of Azure Event Hubs connectivity.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder.appName("local-trade-bronze")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

input_dir = os.getenv("EVENT_INPUT_DIR", "runtime/input")
bronze_path = os.getenv("BRONZE_PATH", "runtime/delta/bronze")
checkpoint = os.getenv("BRONZE_CHECKPOINT", "runtime/checkpoints/bronze")

schema = StructType([
    StructField("trade_id", StringType()),
    StructField("symbol", StringType()),
    StructField("side", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("source", StringType()),
])

events = (
    spark.readStream.schema(schema).option("maxFilesPerTrigger", 20).json(input_dir)
    .withColumn("ingested_at", current_timestamp())
)

query = (
    events.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", checkpoint)
    .start(bronze_path)
)
print(f"Streaming JSON files from {input_dir} to Bronze Delta at {bronze_path}")
query.awaitTermination()
