"""Build event-time trade aggregates for a Gold monitoring table."""
from pyspark.sql import functions as F

def build_trade_volume(silver_stream, watermark="10 minutes", window="5 minutes"):
    """Aggregate trade count and notional by symbol and event-time window."""
    return (
        silver_stream
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window), F.col("symbol"))
        .agg(
            F.count("*").alias("trade_count"),
            F.sum(F.col("quantity") * F.col("price")).alias("traded_notional"),
            F.sum(
                F.when(F.col("side") == "BUY", F.col("quantity") * F.col("price"))
                 .otherwise(F.lit(0.0))
            ).alias("buy_notional"),
            F.sum(
                F.when(F.col("side") == "SELL", F.col("quantity") * F.col("price"))
                 .otherwise(F.lit(0.0))
            ).alias("sell_notional"),
        )
    )
