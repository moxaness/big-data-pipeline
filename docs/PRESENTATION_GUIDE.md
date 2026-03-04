# STREAMSIGHT — Complete Jury Presentation Guide

A step-by-step guide for presenting the STREAMSIGHT project in front of the jury. Follow this document from top to bottom.

---

## Table of Contents

1. [Before the Presentation (30 min before)](#1-before-the-presentation-30-min-before)
2. [Screen Layout Setup](#2-screen-layout-setup)
3. [Presentation Step 1 — Introduction (2 min)](#3-presentation-step-1--introduction-2-min)
4. [Presentation Step 2 — Architecture (3 min)](#4-presentation-step-2--architecture-3-min)
5. [Presentation Step 3 — Live Demo (5-7 min)](#5-presentation-step-3--live-demo-5-7-min)
6. [Presentation Step 4 — Code Walkthrough (3-5 min)](#6-presentation-step-4--code-walkthrough-3-5-min)
7. [Presentation Step 5 — Math & Key Concepts (3 min)](#7-presentation-step-5--math--key-concepts-3-min)
8. [Presentation Step 6 — Limitations & Future Work (2 min)](#8-presentation-step-6--limitations--future-work-2-min)
9. [How to Handle Jury Questions](#9-how-to-handle-jury-questions)
10. [Emergency Troubleshooting During Demo](#10-emergency-troubleshooting-during-demo)

---

## 1. Before the Presentation (30 min before)

You must have the pipeline already running before you stand up to speak. The jury should see live data moving, not waiting screens.

### 1.1 Open Docker Desktop

- Click the Docker Desktop icon from the Start Menu or taskbar
- Wait for the green "Engine running" indicator in the system tray (takes ~30-60 seconds)
- **Verify** by opening PowerShell and running:

```powershell
docker ps
```

If it prints a table header (even empty), Docker is ready. If it errors with "cannot connect to daemon", wait longer.

### 1.2 Start Infrastructure (Terminal 1)

Open **PowerShell** and run:

```powershell
cd C:\Users\niou2\Documents\streamsight
docker compose up -d
```

Wait 30 seconds, then verify all 4 containers are UP:

```powershell
docker compose ps
```

You should see: `zookeeper`, `kafka`, `elasticsearch`, `kibana` all with status "Up".

**If Kafka shows "Exited":** Run this fix (stale ZooKeeper node from previous shutdown):

```powershell
docker compose restart zookeeper
Start-Sleep -Seconds 10
docker compose restart kafka
```

Then verify again with `docker compose ps`.

### 1.3 Create/Reset Elasticsearch Index (Terminal 1)

```powershell
.\create_es_index.ps1
```

This ensures the index mapping is clean for the demo.

### 1.4 Clear Old Checkpoints (Terminal 1)

```powershell
Remove-Item -Recurse -Force checkpoints -ErrorAction SilentlyContinue
```

### 1.5 Start the Producer (Terminal 1)

```powershell
.\venv\Scripts\Activate.ps1
python binance_to_kafka.py
```

You should see lines like:

```
Sent: {'symbol': 'BTCUSDT', 'price': 67614.0, 'quantity': 0.00013, 'event_time': 1770849765928}
```

Leave this running.

### 1.6 Start Spark Streaming (Terminal 2 — new window)

Open a **second PowerShell window** and run:

```powershell
cd C:\Users\niou2\Documents\streamsight

$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
$env:SPARK_HOME = "C:\spark\spark-3.5.7-bin-hadoop3"
$env:HADOOP_HOME = "C:\hadoop"
$env:PYSPARK_PYTHON = "C:\Users\niou2\Documents\streamsight\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Users\niou2\Documents\streamsight\venv\Scripts\python.exe"
$env:PATH = "$env:JAVA_HOME\bin;C:\hadoop\bin;$env:SPARK_HOME\bin;$env:PATH"

spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4 `
  spark_streaming.py
```

Wait until you see batch output tables appearing (takes ~15-30 seconds with dev settings).

### 1.7 Open Browser Tabs

Open **Google Chrome** (or your browser) with these tabs:

| Tab | URL | What For |
|-----|-----|----------|
| **Tab 1** | `http://localhost:5601` | Kibana dashboard |
| **Tab 2** | `http://localhost:9200/streamsight-trades/_count` | ES document count (refresh to show growth) |
| **Tab 3** | `http://localhost:9200/streamsight-trades/_search?pretty&size=3` | Sample ES documents |

In Kibana (Tab 1):
- Navigate to your saved dashboard "STREAMSIGHT Real-Time Crypto Analytics"
- Set the time range to "Last 15 minutes"
- Enable auto-refresh: click the clock icon → select **10 seconds**

### 1.8 Open VS Code

Open VS Code with the project folder. Have these files in tabs:
- `binance_to_kafka.py`
- `spark_streaming.py`
- `docker-compose.yml`

### 1.9 Have a Third Terminal Ready (Terminal 3)

Open a **third PowerShell window** at `C:\Users\niou2\Documents\streamsight` — you'll use this during the demo for live verification commands.

### 1.10 Final Check — Is Everything Working?

In Terminal 3, run:

```powershell
curl.exe -s http://localhost:9200/streamsight-trades/_count
```

If the count is > 0 and increasing every time you run it, **you are ready to present**.

---

## 2. Screen Layout Setup

Arrange your screen so you can switch between views quickly during the demo:

**Option A — Single monitor:**
- Keep browser (Kibana) maximized
- Use Alt+Tab to switch to VS Code and terminals

**Option B — Dual monitor (ideal):**
- **Monitor 1 (audience-facing / projector):** Browser with Kibana, VS Code when showing code
- **Monitor 2 (your screen):** Terminals running producer and Spark

**Recommended window order for Alt+Tab:**
1. Kibana (browser)
2. VS Code (code walkthrough)
3. Terminal 1 — Producer output (live data streaming)
4. Terminal 2 — Spark batch output
5. Terminal 3 — Free terminal for live commands

---

## 3. Presentation Step 1 — Introduction (2 min)

### What to say

> "The project is called STREAMSIGHT. It is a real-time data engineering pipeline that detects anomalous price movements in cryptocurrency markets."

Explain the **problem** first:

> "Cryptocurrency markets operate 24/7 with millions of trades per day. Large sudden orders — from whales or bots — can move prices in unexpected ways. The question is: how do we detect these anomalies in real time, as they happen?"

Then the **solution**:

> "STREAMSIGHT ingests live trade data from the Binance exchange, streams it through Kafka, processes it in Spark using windowed aggregations, and uses statistical Z-scores to flag anomalies. The results are stored in Elasticsearch and visualized in Kibana."

### What to show

Nothing on screen yet, or show the architecture diagram from the README if you have slides. If no slides, you'll show it in the next step.

---

## 4. Presentation Step 2 — Architecture (3 min)

### What to show

Open VS Code or the README and show the architecture diagram:

```
Binance WebSocket → Python Producer → Apache Kafka → Spark Streaming → Elasticsearch → Kibana
   (live trades)    (binance_to_kafka.py)  (crypto_trades)  (spark_streaming.py)  (streamsight-trades)    (dashboards)
```

### What to say for each component

Walk through **left to right**, explaining each box:

**1. Binance WebSocket (data source)**
> "Binance is one of the largest crypto exchanges. They provide a free WebSocket API that pushes every single BTC/USDT trade in real time. Each trade includes the symbol, price, quantity, and timestamp."

**2. Python Producer (`binance_to_kafka.py`)**
> "This is our data ingestion script. It connects to the Binance WebSocket, extracts the fields we need, serializes them to JSON, and publishes each trade to a Kafka topic called `crypto_trades`."

**3. Apache Kafka (message broker)**
> "Kafka acts as a buffer between the ingestion and processing layers. If Spark is slow or restarts, the messages wait safely in Kafka — nothing is lost. This is called decoupling."

*If the jury asks "why not Redis Pub/Sub?":*
> "Redis Pub/Sub is fire-and-forget — if no subscriber is connected, messages are lost. Kafka persists messages to disk and supports consumer replay."

**4. Spark Structured Streaming (`spark_streaming.py`)**
> "Spark reads from Kafka, groups trades into 10-second time windows, calculates the VWAP — Volume-Weighted Average Price — and computes a Z-score. If the Z-score exceeds 2 in absolute value, we flag it as an anomaly."

**5. Elasticsearch (storage)**
> "The aggregated results — one row per window per symbol — are written to Elasticsearch with idempotent upserts using a deterministic document ID."

**6. Kibana (visualization)**
> "Kibana connects to Elasticsearch and renders real-time dashboards: VWAP charts, volume heatmaps, anomaly tables, and Z-score distributions."

### Technology choice justification (have ready if asked)

| Choice | Why | Over what alternative |
|--------|-----|-----------------------|
| Kafka | Durable, replayable, handles backpressure | Redis Pub/Sub (loses messages) |
| Spark Structured Streaming | Mature Python API, exactly-once, SQL-like | Flink (less mature PyFlink, harder on Windows) |
| Elasticsearch | Native Kibana integration, great for time-series | PostgreSQL/TimescaleDB (needs Grafana) |
| Docker | One-command setup, reproducible | Manual installation |

---

## 5. Presentation Step 3 — Live Demo (5-7 min)

**This is the most important part.** The jury wants to see something real moving on screen. Go through these steps in order.

### Demo Step A: Show the Producer (30 sec)

**Switch to Terminal 1** (producer).

> "Here you can see live trades streaming in from Binance right now. Each line is a real BTC/USDT trade happening on the exchange."

Point to a few lines. Show that the `price`, `quantity`, and `event_time` change with every message.

### Demo Step B: Verify Kafka Has Data (30 sec)

**Switch to Terminal 3** (free terminal) and run:

```powershell
docker exec kafka kafka-console-consumer --topic crypto_trades --bootstrap-server localhost:9092 --max-messages 3 --timeout-ms 5000
```

> "Now I'm reading directly from the Kafka topic. These are the same JSON messages that Spark will consume. You can see Kafka is acting as the buffer between our producer and our stream processor."

Wait for 3 messages to appear, then the command exits automatically.

### Demo Step C: Show Spark Processing (1 min)

**Switch to Terminal 2** (Spark).

> "Here is Spark Structured Streaming processing data in micro-batches. Every 10 seconds, it reads new messages from Kafka, groups them into time windows, and computes VWAP and Z-scores."

Point to a batch output table. Explain each column:
- `symbol` — the trading pair (BTCUSDT)
- `window_start` / `window_end` — the 10-second window
- `total_volume` — total BTC traded in this window
- `trade_count` — number of individual trades
- `avg_price` — simple average
- `vwap` — volume-weighted average (the interesting one)
- `zscore` — how far VWAP deviates from the mean
- `is_anomaly` — true if |Z| > 2
- `anomaly_type` — NORMAL, HIGH_VWAP, or LOW_VWAP

### Demo Step D: Show Elasticsearch Growing (30 sec)

**Switch to the browser Tab 2** (ES count) and refresh the page a few times.

> "You can see the document count in Elasticsearch increasing — every time Spark finishes a window, new documents are written here."

Then **switch to browser Tab 3** (ES sample docs):

> "Here is a sample document in Elasticsearch. You can see the exact schema: symbol, VWAP, Z-score, anomaly flag, etc."

### Demo Step E: Show Kibana Dashboard (2-3 min) — THE WOW MOMENT

**Switch to browser Tab 1** (Kibana).

This is the visual payoff. Walk through each panel:

> "This is the real-time Kibana dashboard. Let me walk through each visualization."

**VWAP Time Series (main chart, top):**
> "This line chart shows the Volume-Weighted Average Price over time. You can see the price fluctuating — each point corresponds to one 10-second window."

**Volume Heatmap:**
> "This heatmap shows trading volume intensity. Brighter cells mean more volume was traded in that window."

**Anomaly Events Table:**
> "This table filters only anomalous windows — where |Z-score| exceeded 2. If you see any rows here, it means significant volume-weighted price deviations were detected."

*(If no anomalies are showing:)*
> "Right now the market is relatively calm, so all windows are flagged as NORMAL. Anomalies appear during sudden market moves."

**Z-Score Distribution:**
> "This histogram shows the distribution of Z-scores. Most values cluster around 0 — which is expected for a normal distribution. Anomalies would appear in the tails past ±2."

**Total Trades Metric:**
> "This shows the total number of individual trades we've processed."

**Point out auto-refresh:**
> "Notice the dashboard refreshes every 10 seconds automatically, so you're always seeing the latest data."

---

## 6. Presentation Step 4 — Code Walkthrough (3-5 min)

**Switch to VS Code.**

### 6.1 Show `binance_to_kafka.py` (1 min)

Open the file and highlight:

**Lines 9-11 — Configuration:**
> "We connect to Binance's WebSocket for the BTC/USDT trade stream and publish to Kafka on localhost:9092."

**Lines 21-36 — `on_message` function:**
> "Every time a trade arrives, we extract four fields: symbol, price, quantity, and event_time. We serialize it as JSON and send it to the Kafka topic `crypto_trades`."

**Lines 15-18 — KafkaProducer:**
> "We use the kafka-python library. The producer serializes values to UTF-8 JSON bytes."

### 6.2 Show `spark_streaming.py` (3 min)

This is the core. Show these sections in order:

**Lines 29-34 — Configuration:**
> "These are the tunable parameters. Window duration is 10 seconds, watermark delay is 5 seconds, and the anomaly threshold is a Z-score of 2."

**Lines 48-56 — Spark Session setup:**
> "We create a Spark session configured to talk to Elasticsearch on localhost:9200."

**Lines 77-85 — Kafka Source:**
> "Spark reads from Kafka as a DataFrame. Each row has a binary key and value — we cast the value to a string to get our JSON."

**Lines 94-100 — Schema Definition:**
> "This schema matches exactly what the producer sends: symbol (string), price (double), quantity (double), event_time (long integer in milliseconds)."

**Lines 108-118 — Parse and Prepare:**
> "We parse the JSON into structured columns, convert event_time from milliseconds to a Timestamp, and compute `price × quantity` for the VWAP numerator."

**Lines 131-148 — Windowed Aggregation (KEY SECTION):**
> "This is the heart of the pipeline. We set a 5-second watermark for late data, then group by 10-second tumbling windows and symbol. For each window we compute: sum of price×quantity, total volume, trade count, average price, and standard deviation."

**Lines 152-160 — VWAP Calculation:**
> "VWAP equals the sum of price×quantity divided by total volume. This gives us the volume-weighted average — unlike a simple average, it weights larger trades more heavily."

**Lines 200-215 — Z-Score and Anomaly Detection:**
> "The Z-score is (VWAP minus average price) divided by standard deviation. If the absolute value exceeds 2, we flag it as an anomaly — either HIGH_VWAP for buy pressure or LOW_VWAP for sell pressure."

**Lines 270-290 — Elasticsearch Sink:**
> "We use `foreachBatch` to write each micro-batch to Elasticsearch. The key is the `doc_id` — it's deterministic (symbol + window_start), so if the same window is written twice, it upserts instead of duplicating. This gives us exactly-once semantics."

### 6.3 Briefly Show `docker-compose.yml` (30 sec)

> "Our infrastructure is defined as Docker services: Zookeeper for Kafka coordination, Kafka as the message broker, Elasticsearch for storage, and Kibana for visualization. One `docker compose up` starts everything."

---

## 7. Presentation Step 5 — Math & Key Concepts (3 min)

Explain the two formulas and the streaming concepts. Write on the whiteboard if available, or point to the code.

### VWAP (Volume-Weighted Average Price)

> "VWAP is a standard trading metric. It gives more weight to trades with higher volume."

Formula:

```
VWAP = Σ(price_i × quantity_i) / Σ(quantity_i)
```

> "For example, if there are two trades — one for 0.001 BTC at $67,000 and another for 1.0 BTC at $67,500 — the simple average is $67,250. But VWAP is much closer to $67,500 because the second trade had 1000x more volume."

### Z-Score (Anomaly Detection)

> "The Z-score measures how many standard deviations the VWAP deviates from the simple average price."

Formula:

```
Z = (VWAP - μ) / σ
```

> "Where μ is the average price and σ is the standard deviation within the window."

> **"Why 2 as the threshold?"** In a normal distribution, about 95.4% of values fall within 2 standard deviations. So |Z| > 2 means the observation is in the unusual top ~4.6% — statistically significant."

### Watermarking

> "In real-time systems, data can arrive late due to network delays. Our watermark of 5 seconds tells Spark: wait up to 5 seconds for late-arriving trades before finalizing a window. This balances accuracy vs. memory — without a watermark, Spark would keep every window open forever."

### Checkpointing

> "Spark periodically saves its progress to disk. If the job crashes, it resumes from the last checkpoint instead of reprocessing everything from scratch. Combined with idempotent Elasticsearch writes, this gives us exactly-once processing semantics."

### Exactly-Once Semantics

> "Each trade is counted exactly once in the output. This is achieved through three mechanisms: Kafka offsets tracked via checkpoints, deterministic document IDs in Elasticsearch, and upsert write mode."

---

## 8. Presentation Step 6 — Limitations & Future Work (2 min)

Be honest — juries appreciate self-awareness about limitations.

> "The project works well as a demonstration, but I want to be transparent about its limitations and what I would improve."

| Limitation | What I would do differently |
|------------|----------------------------|
| **Single trading pair** — Only BTC/USDT is monitored | Extend the WebSocket to subscribe to multiple symbols (ETH, SOL, etc.) |
| **Intra-window Z-score** — Current Z-score compares within the same window | Implement a rolling Z-score across the last N windows for more meaningful anomaly detection |
| **Local deployment** — Everything runs on one machine in `local[*]` mode | In production, deploy Spark on a Kubernetes or YARN cluster for horizontal scaling |
| **No alerting** — Anomalies are only visible in the dashboard | Add email/Slack notifications via Kibana alerts or a dedicated alerting service |
| **Java version constraint** — Spark 3.5 requires Java 17 | The Hadoop ecosystem has been slow to adopt newer Java versions; Spark 4.0 should fix this |
| **No ML-based detection** — Only statistical Z-scores | Could add Isolation Forest or autoencoder models for more sophisticated anomaly patterns |

---

## 9. How to Handle Jury Questions

### Most likely questions and how to answer them

**Q: "What happens if Spark crashes mid-processing?"**
> "Spark uses checkpointing — it saves its processing state to disk every micro-batch. When it restarts, it reads the checkpoint and resumes exactly where it left off. Combined with Kafka's message retention, no data is lost. And because we use upsert writes to Elasticsearch with deterministic doc IDs, even if a batch is replayed, it won't create duplicates."

**Q: "What happens if Kafka goes down?"**
> "The producer would fail to send new messages. When Kafka comes back up, the producer reconnects and new trades flow again. During the outage, trades from Binance that weren't sent are lost — to fix this, we could add a local buffer or retry queue in the producer."

**Q: "Why Z-score threshold of 2?"**
> "In a normal distribution, approximately 95.4% of data points fall within 2 standard deviations of the mean. A Z-score above 2 puts the observation in the top 4.6%, which is statistically unusual. It's a standard threshold in anomaly detection. We could also use 3 for stricter detection (99.7% — only 0.3% flagged)."

**Q: "Why not use machine learning for anomaly detection?"**
> "Z-scores are interpretable, fast, and require no training data. You can explain exactly why something was flagged. ML models like Isolation Forest could detect more complex patterns but are harder to explain and require historical training data. Z-scores were the pragmatic choice for a real-time streaming system."

**Q: "What is exactly-once semantics?"**
> "It means every input event produces exactly one output — not zero (data loss) and not two (duplication). We achieve it through: Spark checkpointing to track which Kafka offsets have been processed, and Elasticsearch upserts with deterministic document IDs so reprocessing the same window overwrites rather than duplicates."

**Q: "What is the difference between event time and processing time?"**
> "Event time is when the trade actually happened on Binance. Processing time is when Spark receives and processes it. We use event time for windowing because it's more accurate — if a trade happened at 10:00:05 but arrives at 10:00:08, it should count in the 10:00:00-10:00:10 window, not in a later one."

**Q: "Why Kafka and not just call the Binance API directly from Spark?"**
> "Kafka provides decoupling, durability, and backpressure handling. If Spark is slow, messages queue in Kafka instead of being lost. Kafka also allows multiple consumers — we could add a second consumer for archiving or a different processing job without changing the producer."

**Q: "How much data can this system handle?"**
> "Locally, it handles thousands of trades per second. Kafka can handle millions of messages per second in a cluster. Spark scales horizontally by adding more worker nodes. Elasticsearch can scale by adding more shards and nodes. The architecture is designed to scale — we just run it locally for demonstration."

**Q: "What is a tumbling window?"**
> "A tumbling window is a fixed-size, non-overlapping time window. For example, with a 10-second tumbling window: [00:00-00:10), [00:10-00:20), [00:20-00:30), and so on. Every event falls into exactly one window. The alternative is a sliding window, which overlaps — for example, a 10-second window that slides every 5 seconds."

**Q: "What does Docker Compose do?"**
> "Docker Compose defines and runs multi-container applications. Our `docker-compose.yml` file specifies four services — Zookeeper, Kafka, Elasticsearch, and Kibana — with their images, ports, and dependencies. One command (`docker compose up`) starts the entire infrastructure."

**Q: "What is the role of Zookeeper?"**
> "Zookeeper coordinates the Kafka cluster. It keeps track of which brokers are alive, manages topic metadata, and handles leader election for partitions. In newer Kafka versions (KRaft mode), Zookeeper is being removed — but our Confluent image still uses it."

---

## 10. Emergency Troubleshooting During Demo

Things that might go wrong during the live demo and how to fix them fast.

### Producer stops printing messages

The WebSocket disconnected. In Terminal 1:

```powershell
# Ctrl+C to stop, then restart
python binance_to_kafka.py
```

### Spark shows no new batches

Spark may have stopped. Check Terminal 2. If it exited, restart:

```powershell
spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4 `
  spark_streaming.py
```

If it shows a checkpoint error:

```powershell
Remove-Item -Recurse -Force checkpoints -ErrorAction SilentlyContinue
```

Then restart spark-submit.

### Kibana shows "No data"

1. Check the time range — make sure "Last 15 minutes" is selected
2. Check ES has data: `curl.exe -s http://localhost:9200/streamsight-trades/_count`
3. If count is 0: both producer and Spark need to be running and you need to wait ~15-20 seconds

### Elasticsearch is not reachable

```powershell
docker compose ps
```

If elasticsearch is down:

```powershell
docker compose restart elasticsearch
Start-Sleep -Seconds 30
```

### Kafka crashed

```powershell
docker compose restart zookeeper
Start-Sleep -Seconds 10
docker compose restart kafka
```

### Spark crashes with "getSubject is not supported"

You forgot to set JAVA_HOME. Run the environment setup commands again in Terminal 2:

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;C:\hadoop\bin;C:\spark\spark-3.5.7-bin-hadoop3\bin;$env:PATH"
```

Then re-run spark-submit.

### Pro tip: If everything is broken and you're running out of time

Nuclear reset:

```powershell
# Stop everything
docker compose down
Remove-Item -Recurse -Force checkpoints -ErrorAction SilentlyContinue

# Start fresh
docker compose up -d
Start-Sleep -Seconds 30
docker compose ps       # verify all 4 running
.\create_es_index.ps1   # recreate index

# Then start producer + Spark again as described above
```

---

## Timing Summary

| Step | Duration | What |
|------|----------|------|
| Introduction | 2 min | Problem, solution overview |
| Architecture | 3 min | Diagram, tech choices, why each component |
| **Live Demo** | **5-7 min** | **Producer → Kafka → Spark → ES → Kibana** |
| Code Walkthrough | 3-5 min | binance_to_kafka.py, spark_streaming.py |
| Math & Concepts | 3 min | VWAP, Z-score, watermarking, checkpointing |
| Limitations | 2 min | Honest self-assessment |
| **Total** | **~18-22 min** | + Q&A from jury |

---

## Final Checklist Before Walking Up to Present

- [ ] Docker Desktop running (green icon)
- [ ] All 4 containers UP (`docker compose ps`)
- [ ] Producer running in Terminal 1 (live `Sent:` messages)
- [ ] Spark running in Terminal 2 (batch tables appearing)
- [ ] ES count > 0 and growing (`curl.exe -s http://localhost:9200/streamsight-trades/_count`)
- [ ] Kibana dashboard open with auto-refresh on
- [ ] VS Code open with `binance_to_kafka.py` and `spark_streaming.py` tabs
- [ ] Terminal 3 ready for live commands
- [ ] **Take a deep breath. You built this. You understand it. You've got this.**
