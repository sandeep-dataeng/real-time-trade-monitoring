"""Parsing and basic validation helpers for the Silver layer."""
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

TRADE_SCHEMA = StructType([
    StructField("trade_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("side", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("source", StringType(), True),
])

def parse_and_split(bronze_df):
    """Return (valid, invalid) DataFrames; caller decides where to persist them."""
    parsed = (
        bronze_df
        .withColumn("event", F.from_json("raw_body", TRADE_SCHEMA))
        .select("event.*", "ingested_at", "sequence_number")
    )
    valid_condition = (
        F.col("trade_id").isNotNull()
        & (F.length(F.trim("trade_id")) > 0)
        & F.col("symbol").isNotNull()
        & (F.length(F.trim("symbol")) > 0)
        & F.col("side").isin("BUY", "SELL")
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("price").isNotNull()
        & (F.col("price") >= 0)
        & F.col("event_time").isNotNull()
    )
    valid = parsed.filter(valid_condition)
    invalid = parsed.filter(~valid_condition | F.col("trade_id").isNull())
    return valid, invalid
