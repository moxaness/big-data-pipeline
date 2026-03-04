# spark_streaming.py
# STREAMSIGHT — Real-Time Crypto Market Anomaly Detection
# Spark Structured Streaming job: Kafka -> Windowed VWAP + Z-Score -> Elasticsearch + Console
#
# Run with:
#   C:\spark\spark-3.5.7-bin-hadoop3\bin\spark-submit ^
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4 ^
#     spark_streaming.py
#
# Math:
#   VWAP (Volume-Weighted Average Price) = Σ(price × quantity) / Σ(quantity)
#   Z-Score = (current_vwap - rolling_mean) / rolling_std
#   Anomaly flagged when |Z-Score| > threshold (default 2.0)

import signal
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum as spark_sum, avg, stddev, lit, when, abs as spark_abs,
    current_timestamp, expr, to_json, struct, coalesce, concat_ws, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

# =============================================================================
# CONFIGURATION
# =============================================================================
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "crypto_trades"
ES_HOST = "localhost"
ES_PORT = "9200"
ES_INDEX = "streamsight-trades"
CHECKPOINT_DIR = "C:/Users/niou2/Documents/streamsight/checkpoints"  # Windows path with forward slashes

# Anomaly detection parameters (tunable)
# PRODUCTION SETTINGS (validated 2025-12-19):
#   - Window: 1 minute tumbling windows for VWAP aggregation
#   - Watermark: 2 minutes late data tolerance (ensures complete windows)
#   - First output appears after: window_duration + watermark_delay ≈ 3 minutes
# For dev/testing, temporarily use "10 seconds" each for faster validation.
WINDOW_DURATION = "10 seconds"     # Tumbling window size for VWAP calculation
WATERMARK_DELAY = "5 seconds"      # Late data tolerance
ZSCORE_THRESHOLD = 2.0             # |Z| > 2 flagged as anomaly (≈5% of normal distribution)

# =============================================================================
# SPARK SESSION
# =============================================================================
spark = (
    SparkSession.builder
    .appName("STREAMSIGHT-Kafka-Spark-Streaming")
    .master("local[*]")
    # Elasticsearch Spark connector config
    .config("spark.es.nodes", ES_HOST)
    .config("spark.es.port", ES_PORT)
    .config("spark.es.index.auto.create", "true")
    .config("spark.es.nodes.wan.only", "true")  # Required for localhost
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Graceful shutdown handler for Ctrl+C (Windows compatible)
streaming_query = None

def signal_handler(sig, frame):
    """Handle SIGINT (Ctrl+C) for graceful shutdown."""
    print("\n[STREAMSIGHT] Received shutdown signal. Stopping streaming query...")
    if streaming_query is not None:
        streaming_query.stop()
    spark.stop()
    print("[STREAMSIGHT] Shutdown complete.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# =============================================================================
# KAFKA SOURCE
# =============================================================================
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")  # Don't fail if Kafka topic has gaps
    .load()
)

# Kafka value is bytes -> cast to string
raw_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str")

# =============================================================================
# SCHEMA DEFINITION
# Schema matches the output from binance_to_kafka.py producer:
#   {"symbol": "BTCUSDT", "price": 12345.67, "quantity": 0.123, "event_time": 1702999999999}
# =============================================================================
trade_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("event_time", LongType(), True)
])


# =============================================================================
# PARSE JSON AND EXTRACT FIELDS
# =============================================================================
parsed_df = (
    raw_df
    .select(
        from_json(col("json_str"), trade_schema, {"mode": "PERMISSIVE"}).alias("data")
    )
    .select("data.*")
    .filter(col("symbol").isNotNull())  # Drop malformed records
)

# Convert event_time (ms) to timestamp and prepare for windowing
trades_df = (
    parsed_df
    .withColumn("event_ts", (col("event_time") / 1000).cast(TimestampType()))
    .withColumn("price_qty", col("price") * col("quantity"))  # For VWAP numerator
    .drop("event_time")  # Keep only event_ts
)

# =============================================================================
# WINDOWED AGGREGATION WITH VWAP
# 
# VWAP Formula:
#   VWAP = Σ(price_i × quantity_i) / Σ(quantity_i)
#
# We compute per tumbling window with watermark for late data.
#
# *** WHY ELASTICSEARCH APPEARS EMPTY INITIALLY ***
# Spark Structured Streaming with watermarks delays output until it's confident
# no more late data will arrive for a window. The output appears only AFTER:
#   1. The window closes (WINDOW_DURATION passes), AND
#   2. The watermark advances past window_end + WATERMARK_DELAY
#
# Example with 30s window + 30s watermark:
#   - Window [00:00, 00:30) collects data
#   - At 00:30, window closes but output is held
#   - At 01:00 (watermark = 01:00 - 30s = 00:30), window [00:00, 00:30) emits
#   - Total wait: ~60 seconds from first event to first output
#
# This is NOT a bug - it's exactly-once semantics ensuring complete windows.
# =============================================================================
windowed_df = (
    trades_df
    .withWatermark("event_ts", WATERMARK_DELAY)
    .groupBy(
        window(col("event_ts"), WINDOW_DURATION),
        col("symbol")
    )
    .agg(
        spark_sum("price_qty").alias("sum_price_qty"),      # Numerator for VWAP
        spark_sum("quantity").alias("total_volume"),        # Denominator & volume metric
        spark_sum(lit(1)).alias("trade_count"),             # Number of trades
        avg("price").alias("avg_price"),                    # Simple average (for comparison)
        stddev("price").alias("price_stddev"),              # Price volatility in window
    )
)

# Calculate VWAP
vwap_df = (
    windowed_df
    .withColumn(
        "vwap",
        when(col("total_volume") > 0, col("sum_price_qty") / col("total_volume"))
        .otherwise(lit(0.0))
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end", col("window.end"))
    .drop("window", "sum_price_qty")  # Clean up intermediate columns
)

# =============================================================================
# Z-SCORE ANOMALY DETECTION
#
# Approach: We use the current window's VWAP compared against the simple average
# price and standard deviation within the same window. For a production system,
# you'd use a sliding window of previous N windows to compute rolling stats.
#
# Z-Score Formula:
#   Z = (VWAP - μ) / σ
#   where μ = avg_price, σ = price_stddev (within window)
#
# This detects when VWAP deviates significantly from the mean price,
# indicating unusual volume-weighted price movement (e.g., large orders
# moving the price disproportionately).
#
# Threshold: |Z| > 2.0 → anomaly (~4.6% of normal distribution)
# =============================================================================
anomaly_df = (
    vwap_df
    .withColumn(
        "zscore",
        when(
            (col("price_stddev").isNotNull()) & (col("price_stddev") > 0),
            (col("vwap") - col("avg_price")) / col("price_stddev")
        ).otherwise(lit(0.0))
    )
    .withColumn(
        "is_anomaly",
        when(spark_abs(col("zscore")) > ZSCORE_THRESHOLD, lit(True)).otherwise(lit(False))
    )
    .withColumn(
        "anomaly_type",
        when(col("zscore") > ZSCORE_THRESHOLD, lit("HIGH_VWAP"))
        .when(col("zscore") < -ZSCORE_THRESHOLD, lit("LOW_VWAP"))
        .otherwise(lit("NORMAL"))
    )
)

# =============================================================================
# FINAL OUTPUT SCHEMA FOR SINKS
# =============================================================================
output_df = (
    anomaly_df
    .select(
        col("symbol"),
        col("window_start"),
        col("window_end"),
        col("total_volume"),
        col("trade_count"),
        col("avg_price"),
        col("vwap"),
        col("price_stddev"),
        col("zscore"),
        col("is_anomaly"),
        col("anomaly_type"),
        # Generate deterministic document ID for ES idempotency
        concat_ws("_", 
            col("symbol"), 
            date_format(col("window_start"), "yyyyMMddHHmmss")
        ).alias("doc_id")
    )
)

# =============================================================================
# SINK 1: CONSOLE OUTPUT (for debugging/validation)
# =============================================================================
console_query = (
    output_df
    .writeStream
    .outputMode("append")  # Append mode works with watermark
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/console")
    .trigger(processingTime="10 seconds")  # Process every 10 seconds
    .start()
)

# =============================================================================
# SINK 2: ELASTICSEARCH (using foreachBatch for more control)
#
# We use foreachBatch to write each micro-batch to Elasticsearch.
# This allows us to:
#   1. Use doc_id for idempotent upserts
#   2. Handle errors gracefully
#   3. Transform data if needed
# =============================================================================
def write_to_elasticsearch(batch_df, batch_id):
    """
    Write a micro-batch DataFrame to Elasticsearch.
    Uses the elasticsearch-spark connector with doc_id for idempotency.
    """
    if batch_df.count() == 0:
        return  # Skip empty batches
    
    try:
        # Write to ES with doc_id as the document identifier
        (
            batch_df
            .write
            .format("org.elasticsearch.spark.sql")
            .option("es.resource", ES_INDEX)
            .option("es.mapping.id", "doc_id")          # Use doc_id for upserts
            .option("es.write.operation", "upsert")     # Idempotent writes
            .option("es.nodes", ES_HOST)
            .option("es.port", ES_PORT)
            .option("es.nodes.wan.only", "true")
            .mode("append")
            .save()
        )
        print(f"[STREAMSIGHT] Batch {batch_id}: Wrote {batch_df.count()} records to Elasticsearch")
    except Exception as e:
        print(f"[STREAMSIGHT] Batch {batch_id}: ES write failed - {e}")
        # Don't raise - allow streaming to continue even if ES is down

es_query = (
    output_df
    .writeStream
    .outputMode("append")
    .foreachBatch(write_to_elasticsearch)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/elasticsearch")
    .trigger(processingTime="10 seconds")
    .start()
)

# =============================================================================
# AWAIT TERMINATION
# =============================================================================
print("[STREAMSIGHT] Streaming started. Press Ctrl+C to stop.")
print(f"[STREAMSIGHT] Checkpoints: {CHECKPOINT_DIR}")
print(f"[STREAMSIGHT] Elasticsearch: http://{ES_HOST}:{ES_PORT}/{ES_INDEX}")
print(f"[STREAMSIGHT] Anomaly threshold: |Z| > {ZSCORE_THRESHOLD}")

# Store reference for signal handler
streaming_query = console_query

try:
    # Wait for any query to terminate
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n[STREAMSIGHT] Interrupted. Shutting down...")
finally:
    # Stop all active streams
    for q in spark.streams.active:
        q.stop()
    spark.stop()
    print("[STREAMSIGHT] All streams stopped.")
