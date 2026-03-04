# STREAMSIGHT Technical Report
# Real-Time Crypto Market Anomaly Detection Platform
# =================================================

## Project Overview

STREAMSIGHT is a real-time data pipeline for detecting anomalies in cryptocurrency market data. The system ingests live trade data from the Binance WebSocket API, processes it using Apache Spark Structured Streaming, and persists aggregated metrics to Elasticsearch for visualization in Kibana.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────┐
│   Binance WS    │────▶│ Python Producer │────▶│    Apache Kafka         │
│  (btcusdt@trade)│     │ (binance_to_    │     │  (crypto_trades topic)  │
└─────────────────┘     │  kafka.py)      │     └───────────┬─────────────┘
                        └─────────────────┘                 │
                                                            ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────┐
│     Kibana      │◀────│  Elasticsearch  │◀────│  Spark Structured       │
│   Dashboard     │     │ (streamsight-   │     │  Streaming              │
│                 │     │  trades index)  │     │  (spark_streaming.py)   │
└─────────────────┘     └─────────────────┘     └─────────────────────────┘
```

## Key Components

### 1. Data Ingestion (binance_to_kafka.py)
- WebSocket client connecting to Binance trade stream
- Extracts: symbol, price, quantity, event_time
- Publishes JSON records to Kafka topic `crypto_trades`
- Graceful shutdown handling via SIGINT

### 2. Stream Processing (spark_streaming.py)
- **Event-Time Processing**: Uses trade timestamp (not processing time) for accurate windowing
- **Watermarking**: 2-minute delay tolerance for late-arriving data
- **Tumbling Windows**: 1-minute aggregation windows

### 3. Metrics Computed

#### VWAP (Volume-Weighted Average Price)
$$VWAP = \frac{\sum_{i=1}^{n} (price_i \times quantity_i)}{\sum_{i=1}^{n} quantity_i}$$

VWAP is a trading benchmark that gives the average price at which a security has traded throughout the day, weighted by volume. It's more representative than simple average price because it accounts for trading activity.

#### Z-Score Anomaly Detection
$$Z = \frac{VWAP - \mu}{\sigma}$$

Where:
- $\mu$ = average price within the window
- $\sigma$ = standard deviation of price within the window

Anomaly thresholds:
- |Z| > 2.0: Flagged as anomaly (~4.6% of normal distribution)
- Z > 2.0: HIGH_VWAP (large buy orders pushing VWAP above average)
- Z < -2.0: LOW_VWAP (large sell orders pushing VWAP below average)

### 4. Fault Tolerance
- **Checkpointing**: Spark saves progress to disk for recovery
- **Exactly-Once Semantics**: Elasticsearch upserts using deterministic document IDs
- **Idempotent Writes**: Document ID = `{symbol}_{window_start_timestamp}`

### 5. Storage (Elasticsearch)
Index: `streamsight-trades`
Fields:
- symbol (keyword)
- window_start, window_end (date)
- total_volume, trade_count (numeric)
- avg_price, vwap, price_stddev (double)
- zscore (double)
- is_anomaly (boolean)
- anomaly_type (keyword: NORMAL, HIGH_VWAP, LOW_VWAP)

### 6. Visualization (Kibana)
- VWAP time series chart
- Volume heatmap
- Anomaly event list
- Z-score distribution histogram

## Technology Stack

| Component | Technology | Version |
|-----------|------------|---------|
| OS | Windows 11 | - |
| Runtime | Python | 3.10.11 |
| Stream Processing | Apache Spark | 3.5.7 |
| Message Queue | Apache Kafka | 7.5.0 (Confluent) |
| Time Series DB | Elasticsearch | 8.11.4 |
| Visualization | Kibana | 8.11.4 |
| Containerization | Docker Compose | - |

## Performance Considerations

1. **Window Duration (1 minute)**: Balance between granularity and computational cost
2. **Watermark Delay (2 minutes)**: Allows late data while preventing unbounded state
3. **Trigger Interval (10 seconds)**: Provides near-real-time output without overwhelming ES
4. **Local[*] Mode**: Uses all available CPU cores for local development

## Limitations & Future Work

1. **Single Symbol**: Currently only processes BTC/USDT; extend to multiple symbols
2. **Local Deployment**: Production would use Kubernetes/YARN cluster
3. **Historical Z-Score**: Current Z-score is intra-window; could implement cross-window rolling statistics
4. **Alerting**: Kibana alerts are basic; consider Apache Flink CEP for complex event patterns

## File Artifacts

| File | Description |
|------|-------------|
| spark_streaming.py | Main Spark Structured Streaming job |
| binance_to_kafka.py | Binance WebSocket to Kafka producer |
| docker-compose.yml | Docker services (Kafka, Zookeeper, ES, Kibana) |
| create_es_index.ps1 | Elasticsearch index creation script |
| runbook.txt | Step-by-step execution guide |
| kibana_setup.md | Kibana dashboard configuration |
| technical_report.md | This document |

## Execution Summary

```powershell
# 1. Start services
docker compose up -d

# 2. Create ES index
.\create_es_index.ps1

# 3. Start producer (Terminal 1)
python binance_to_kafka.py

# 4. Start Spark job (Terminal 2)
$env:HADOOP_HOME = "C:\hadoop"
$env:PYSPARK_PYTHON = ".\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = ".\venv\Scripts\python.exe"

C:\spark\spark-3.5.7-bin-hadoop3\bin\spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4 `
  spark_streaming.py

# 5. View dashboard
# Open http://localhost:5601
```

## Author

STREAMSIGHT - Semester Project 2025
