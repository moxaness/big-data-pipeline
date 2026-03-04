# STREAMSIGHT: Real-Time Cryptocurrency Market Anomaly Detection Platform

## Executive Summary

STREAMSIGHT is a real-time data engineering pipeline that detects anomalous price movements in cryptocurrency markets. The system ingests live trade data from the Binance exchange via WebSocket, streams it through Apache Kafka for reliable message delivery, processes it using Apache Spark Structured Streaming to calculate Volume-Weighted Average Price (VWAP) and statistical anomaly scores, and stores the results in Elasticsearch for visualization in Kibana. The entire infrastructure runs in Docker containers on a single Windows machine, making it suitable for development, testing, and demonstration purposes.

---

## Glossary of Key Technologies

### Apache Kafka
**What it is:** A distributed event streaming platform that acts as a high-throughput message queue.

**Plain English:** Think of Kafka as a very fast, reliable mailroom. When trade data arrives from Binance, it gets placed in a "topic" (like a mailbox). The Spark job can then read from this mailbox at its own pace. If Spark is temporarily slow or restarting, the messages wait safely in Kafka rather than being lost.

**Why we use it:** Kafka decouples the data producer (Binance WebSocket) from the consumer (Spark). This means if Spark crashes, no data is lost—it's still in Kafka waiting to be processed.

### Apache Spark (Structured Streaming)
**What it is:** A distributed data processing engine with a streaming module that processes data in micro-batches.

**Plain English:** Spark is like a very smart assembly line worker. It takes raw trade data, groups it into time windows (e.g., every 10 seconds), calculates statistics, and decides if anything looks unusual. "Structured Streaming" means it treats streaming data like a continuously growing table, making it easier to write queries.

**Why we use it:** Spark handles complex aggregations (like VWAP) efficiently and has built-in support for exactly-once processing semantics, meaning each trade is counted exactly once even if there are failures.

### Elasticsearch
**What it is:** A distributed search and analytics engine built on Apache Lucene.

**Plain English:** Elasticsearch is like a super-powered filing cabinet that can instantly find any document and perform calculations on millions of records. We store our anomaly detection results here so we can search, filter, and visualize them.

**Why we use it:** Elasticsearch excels at time-series data and integrates seamlessly with Kibana for visualization. It's also horizontally scalable if we need more capacity.

### Docker
**What it is:** A containerization platform that packages applications with their dependencies.

**Plain English:** Docker is like shipping containers for software. Each service (Kafka, Elasticsearch, etc.) runs in its own isolated "container" with everything it needs. This ensures consistent behavior regardless of the host machine's configuration.

**Why we use it:** Docker makes it trivial to spin up a complete data infrastructure with a single command (`docker compose up`). No manual installation of Kafka, Zookeeper, or Elasticsearch required.

### Checkpointing (Spark)
**What it is:** A mechanism where Spark periodically saves its processing state to disk.

**Plain English:** Imagine you're reading a long book and you use bookmarks. If you have to stop reading, you can pick up exactly where you left off. Checkpointing does this for Spark—if the job crashes, it can resume from the last checkpoint instead of reprocessing everything.

**Why it matters:** Checkpointing enables exactly-once semantics and fault tolerance. Without it, a restart would either miss data or double-count it.

### Watermarking (Spark)
**What it is:** A technique for handling late-arriving data in streaming systems.

**Plain English:** In the real world, network delays mean some trade data might arrive "late" (e.g., a trade from 9:00:05 arrives at 9:00:15). Watermarking tells Spark: "Wait up to X seconds for late data before finalizing a time window." After that threshold, late data is dropped to prevent unbounded state growth.

**Why it matters:** Without watermarking, Spark would either drop all late data (losing accuracy) or keep windows open forever (running out of memory).

---

## End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        STREAMSIGHT ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    WebSocket     ┌──────────────────────────────────────┐ │
│  │   BINANCE    │ ═══════════════> │  binance_to_kafka.py                 │ │
│  │  (External)  │   Live Trades    │  (Python Producer)                   │ │
│  └──────────────┘                  │  - Connects to Binance WebSocket     │ │
│                                    │  - Extracts: symbol, price, qty, ts  │ │
│                                    │  - Serializes to JSON                │ │
│                                    │  - Sends to Kafka topic              │ │
│                                    └───────────────┬──────────────────────┘ │
│                                                    │                        │
│                                                    ▼                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                     DOCKER COMPOSE SERVICES                             ││
│  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                ││
│  │  │  Zookeeper  │◄───►│    Kafka    │     │Elasticsearch│                ││
│  │  │  Port 2181  │     │  Port 9092  │     │  Port 9200  │                ││
│  │  │             │     │             │     │             │                ││
│  │  │ Coordinates │     │   Topic:    │     │   Index:    │                ││
│  │  │   Kafka     │     │crypto_trades│     │streamsight- │                ││
│  │  │  cluster    │     │             │     │   trades    │                ││
│  │  └─────────────┘     └──────┬──────┘     └──────▲──────┘                ││
│  │                             │                   │                       ││
│  │                             │                   │                       ││
│  │  ┌─────────────┐            │                   │                       ││
│  │  │   Kibana    │            │                   │                       ││
│  │  │  Port 5601  │────────────┼───────────────────┘                       ││
│  │  │             │            │   Queries ES for                          ││
│  │  │ Dashboards  │            │   visualization                           ││
│  │  └─────────────┘            │                                           ││
│  └─────────────────────────────┼───────────────────────────────────────────┘│
│                                │                                            │
│                                ▼                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  spark_streaming.py (Spark Structured Streaming Job)                    ││
│  │  ────────────────────────────────────────────────────────────────────   ││
│  │                                                                         ││
│  │  1. READ from Kafka topic "crypto_trades"                               ││
│  │     └─► Raw JSON: {"symbol","price","quantity","event_time"}            ││
│  │                                                                         ││
│  │  2. PARSE JSON into structured DataFrame                                ││
│  │     └─► Convert event_time (ms) to Timestamp                            ││
│  │                                                                         ││
│  │  3. APPLY WATERMARK (5 seconds late tolerance)                          ││
│  │                                                                         ││
│  │  4. WINDOW AGGREGATION (10-second tumbling windows)                     ││
│  │     └─► Group by (window, symbol)                                       ││
│  │     └─► Calculate: sum(price*qty), sum(qty), count, avg, stddev         ││
│  │                                                                         ││
│  │  5. COMPUTE VWAP                                                        ││
│  │     └─► VWAP = sum(price*qty) / sum(qty)                                ││
│  │                                                                         ││
│  │  6. COMPUTE Z-SCORE                                                     ││
│  │     └─► zscore = (VWAP - avg_price) / stddev                            ││
│  │     └─► is_anomaly = |zscore| > 2.0                                     ││
│  │                                                                         ││
│  │  7. WRITE to Elasticsearch (foreachBatch with upsert)                   ││
│  │     └─► doc_id = symbol + window_start (for idempotency)                ││
│  │                                                                         ││
│  │  8. WRITE to Console (for debugging)                                    ││
│  │                                                                         ││
│  │  CHECKPOINTS: C:\Users\niou2\Documents\streamsight\checkpoints          ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### File-to-Function Mapping

| File                  | Purpose       | Key Responsibilities                             |
|-----------------------|---------------|--------------------------------------------------|
| `binance_to_kafka.py` | Data Producer | Connects to Binance WebSocket API, receives BTCUSDT trade events, extracts relevant fields, serializes to JSON, publishes to Kafka topic `crypto_trades` |
| `spark_streaming.py` | Stream Processor | Consumes from Kafka, parses JSON, applies watermarking, performs windowed VWAP aggregation, calculates Z-scores, writes results to Elasticsearch and console |
| `docker-compose.yml` | Infrastructure Definition | Defines Zookeeper, Kafka, Elasticsearch, and Kibana services with networking and port mappings |
| `run_producer.bat` | Launch Script | Sets environment, activates venv, runs the producer script |
| `run_spark.bat` | Launch Script | Sets HADOOP_HOME, PYSPARK_PYTHON env vars, runs spark-submit with required packages |
| `checkpoints/` | State Directory | Stores Spark checkpoint data for fault tolerance |

---

## Technology Trade-offs and Alternatives

### Why Kafka instead of Redis Pub/Sub?

| Criteria | Kafka | Redis Pub/Sub |
|----------|-------|---------------|
| **Message Persistence** | ✅ Persists messages to disk | ❌ Messages lost if no subscriber connected |
| **Consumer Replay** | ✅ Can re-read historical messages | ❌ Fire-and-forget only |
| **Backpressure Handling** | ✅ Consumer reads at own pace | ❌ Slow consumers lose messages |
| **Throughput** | ✅ Millions of messages/sec | ⚠️ Lower (memory-bound) |
| **Complexity** | ⚠️ Requires Zookeeper | ✅ Simpler setup |

**Decision:** Kafka's durability and replay capability are essential for a streaming pipeline where we can't afford to lose trades.

### Why Spark Structured Streaming instead of Apache Flink?

| Criteria | Spark Structured Streaming | Apache Flink |
|----------|---------------------------|--------------|
| **Latency** | ⚠️ Micro-batch (100ms-seconds) | ✅ True streaming (ms) |
| **Exactly-Once** | ✅ With checkpointing | ✅ Native |
| **Ecosystem** | ✅ Huge (MLlib, SQL, GraphX) | ⚠️ Smaller |
| **Python API** | ✅ PySpark mature | ⚠️ PyFlink less mature |
| **Learning Curve** | ✅ SQL-like API | ⚠️ Steeper |
| **Windows Support** | ✅ Works with winutils | ⚠️ More Linux-focused |

**Decision:** Spark's mature Python API, familiar SQL semantics, and better Windows compatibility made it the pragmatic choice for this project.

### Why Elasticsearch instead of PostgreSQL (TimescaleDB)?

| Criteria               | Elasticsearch             | TimescaleDB/Postgres          |
|------------------------|---------------------------|-------------------------------|
| **Query Speed**        | ✅ Sub-second on billions | ✅ Fast with proper indexing |
| **Schema Flexibility** | ✅ Schemaless JSON        | ⚠️ Requires migrations       |
| **Full-Text Search**   | ✅ Native                 | ⚠️ Limited                   |
| **Kibana Integration** | ✅ Native                 | ❌ Requires Grafana          |
| **Aggregations**       | ✅ Excellent              | ✅ Excellent                 |
| **ACID Transactions**  | ❌ Eventually consistent  | ✅ Full ACID                 |

**Decision:** Elasticsearch's native Kibana integration and schemaless flexibility accelerate development. For this monitoring use case, eventual consistency is acceptable.

---

## Implementation Deep Dive

### Data Schema

**Kafka Message (Input):**
```json
{
  "symbol": "BTCUSDT",
  "price": 87928.57,
  "quantity": 0.00053,
  "event_time": 1766178316409
}
```

**Elasticsearch Document (Output):**
```json
{
  "symbol": "BTCUSDT",
  "window_start": 1766177700000,
  "window_end": 1766177710000,
  "total_volume": 5.108719999999982,
  "trade_count": 1258,
  "avg_price": 88081.96677265479,
  "vwap": 88078.50566584224,
  "price_stddev": 10.41532523536461,
  "zscore": -0.3323090479018683,
  "is_anomaly": false,
  "anomaly_type": "NORMAL",
  "doc_id": "BTCUSDT_20251219215500"
}
```

### VWAP Formula

The Volume-Weighted Average Price (VWAP) gives more weight to trades with larger volumes:

$$
\text{VWAP} = \frac{\sum_{i=1}^{n} (P_i \times Q_i)}{\sum_{i=1}^{n} Q_i}
$$

Where:
- $P_i$ = Price of trade $i$
- $Q_i$ = Quantity (volume) of trade $i$
- $n$ = Number of trades in the window

**Spark Implementation:**
```python
# In aggregation
spark_sum("price_qty").alias("sum_price_qty"),  # Σ(P × Q)
spark_sum("quantity").alias("total_volume"),    # Σ(Q)

# VWAP calculation
.withColumn(
    "vwap",
    when(col("total_volume") > 0, col("sum_price_qty") / col("total_volume"))
    .otherwise(lit(0.0))
)
```

### Z-Score Formula

The Z-score measures how many standard deviations the VWAP is from the mean price:

$$
Z = \frac{\text{VWAP} - \mu}{\sigma}
$$

Where:
- $\mu$ = Mean price in the window (`avg_price`)
- $\sigma$ = Standard deviation of prices in the window (`price_stddev`)

**Interpretation:**
- $|Z| \leq 2$: Normal price movement (~95% of data)
- $|Z| > 2$: Anomalous—large orders moved price significantly

**Spark Implementation:**
```python
.withColumn(
    "zscore",
    when(
        (col("price_stddev").isNotNull()) & (col("price_stddev") > 0),
        (col("vwap") - col("avg_price")) / col("price_stddev")
    ).otherwise(lit(0.0))
)
.withColumn(
    "is_anomaly",
    when(spark_abs(col("zscore")) > 2.0, lit(True)).otherwise(lit(False))
)
```

### Windowing and Watermark Rationale

**Configuration:**
```python
WINDOW_DURATION = "10 seconds"  # Tumbling window size
WATERMARK_DELAY = "5 seconds"   # Late data tolerance
```

**Why 10-second windows?**
- Crypto markets are highly volatile; 10 seconds captures meaningful price movements
- Short enough to detect rapid anomalies
- Long enough to accumulate sufficient trades for statistical significance

**Why 5-second watermark?**
- Network latency from Binance is typically < 1 second
- 5 seconds provides safety margin without excessive memory usage
- Trade-off: Later events are dropped, but accuracy is acceptable

**When does output appear?**
Output is emitted when:
1. Window closes (10 seconds pass), AND
2. Watermark advances past `window_end + 5 seconds`

Total latency: ~15-20 seconds from first event to output.

---

## Troubleshooting Guide

### Problem: ES count is 0 after running the pipeline

**Diagnostic Commands:**
```powershell
# 1. Check if Kafka has data
docker exec kafka kafka-console-consumer --topic crypto_trades --bootstrap-server localhost:9092 --max-messages 3 --timeout-ms 5000

# Expected: JSON trade messages
# If "0 messages": Producer is not running or not connected

# 2. Check if ES is reachable
curl.exe -s http://localhost:9200/_cluster/health?pretty

# Expected: "status": "yellow" or "green"

# 3. Check ES index exists
curl.exe -s http://localhost:9200/streamsight-trades

# Expected: Index mapping JSON (not 404 error)

# 4. Check if Spark is running
Get-Process -Name java -ErrorAction SilentlyContinue

# Expected: java process with recent StartTime
```

**Common Causes:**
1. **Producer not running**: Start with `run_producer.bat`
2. **Spark not running**: Start with `run_spark.bat`
3. **Checkpoints corrupted**: Delete `checkpoints/` and restart Spark
4. **Window not closed yet**: Wait 15-20 seconds for first output

### Problem: Spark fails with "Failed to find data source: kafka"

**Cause:** Spark packages not loaded.

**Solution:** Always run via `run_spark.bat` or:
```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4 spark_streaming.py
```

### Problem: Producer disconnects randomly

**Cause:** WebSocket timeout or network instability.

**Solution:** The producer has built-in ping/pong (20s interval). If still failing:
1. Check internet connection
2. Restart producer
3. Consider adding reconnection logic (future enhancement)

### Problem: "Address already in use" error

**Cause:** Previous process didn't terminate cleanly.

**Solution:**
```powershell
# Kill all Java processes
Get-Process -Name java | Stop-Process -Force

# Kill all Python processes (careful if other Python apps running)
Get-Process -Name python | Stop-Process -Force

# Restart Docker containers
docker compose restart
```

---

## Quick Reference Commands

```powershell
# Start infrastructure
docker compose up -d

# Start producer (in separate window)
.\run_producer.bat

# Start Spark (in separate window)
.\run_spark.bat

# Check ES document count
curl.exe -s http://localhost:9200/streamsight-trades/_count

# View sample documents
curl.exe -s "http://localhost:9200/streamsight-trades/_search?pretty&size=3"

# Clear checkpoints (reset Spark state)
Remove-Item -Recurse -Force checkpoints

# Stop everything
docker compose down
Get-Process -Name java,python | Stop-Process -Force
```

---

## Authors

- **Farouk BENKHLIFA**
- **Mohammed Anes BOUCETLA**

**Date:** December 19, 2025
