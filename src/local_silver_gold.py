"""Local Bronze-to-Silver-to-Gold streaming pipeline using Delta Lake."""
import os
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder.appName("local-trade-silver-gold")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze_path = os.getenv("BRONZE_PATH", "runtime/delta/bronze")
silver_path = os.getenv("SILVER_PATH", "runtime/delta/silver")
quarantine_path = os.getenv("QUARANTINE_PATH", "runtime/delta/quarantine")
gold_path = os.getenv("GOLD_PATH", "runtime/delta/gold")
checkpoint_root = os.getenv("CHECKPOINT_ROOT", "runtime/checkpoints")

bronze = spark.readStream.format("delta").load(bronze_path)
valid_condition = (
    F.col("trade_id").isNotNull()
    & (F.length(F.trim("trade_id")) > 0)
    & F.col("symbol").isNotNull()
    & F.col("side").isin("BUY", "SELL")
    & F.col("quantity").isNotNull() & (F.col("quantity") > 0)
    & F.col("price").isNotNull() & (F.col("price") >= 0)
    & F.col("event_time").isNotNull()
)
is_valid = F.coalesce(valid_condition, F.lit(False))
valid = bronze.filter(is_valid)
invalid = bronze.filter(~is_valid).withColumn(
    "rejection_reason", F.lit("Required-field or business-rule validation failed")
)

silver = (
    valid.withWatermark("event_time", "10 minutes")
    .dropDuplicates(["trade_id"])
)
silver_query = (
    silver.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{checkpoint_root}/silver")
    .start(silver_path)
)
quarantine_query = (
    invalid.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{checkpoint_root}/quarantine")
    .start(quarantine_path)
)

silver_for_gold = spark.readStream.format("delta").load(silver_path)
gold = (
    silver_for_gold.withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "5 minutes"), "symbol")
    .agg(
        F.count("*").alias("trade_count"),
        F.sum(F.col("quantity") * F.col("price")).alias("traded_notional"),
    )
)
gold_query = (
    gold.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{checkpoint_root}/gold")
    .start(gold_path)
)

print("Silver, quarantine and Gold streams started.")
for query in (silver_query, quarantine_query, gold_query):
    query.awaitTermination()
