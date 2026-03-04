# STREAMSIGHT - How to Restart After Reboot

This guide explains exactly what to do when you turn on your computer to get the pipeline running again.

> **Last verified:** 2026-02-18 — all steps tested end-to-end on Windows.

---

## Prerequisites

Make sure you have the following installed:

- **Docker Desktop** for Windows
- **Python 3.10+** (venv at `C:\Users\niou2\Documents\streamsight\venv`)
  - Required packages: `kafka-python==2.3.0`, `websocket-client==1.9.0`, `pyspark==3.5.7`
- **Apache Spark 3.5.7** at `C:\spark\spark-3.5.7-bin-hadoop3`
- **Java 17 (Eclipse Temurin)** at `C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot`
  - **IMPORTANT:** Spark 3.5 does NOT work with Java 18+. If your system default is a newer Java, you MUST set `JAVA_HOME` to Java 17 before running Spark (see commands below).
- **Hadoop winutils** at `C:\hadoop\bin` (winutils.exe + hadoop.dll for Hadoop 3.3.x)

---

## Quick Start (Copy-Paste Commands)

### Terminal 1 — Infrastructure + Producer

Open **PowerShell** and run these commands in order:

```powershell
# 1. Go to project folder
cd C:\Users\niou2\Documents\streamsight

# 2. Start Docker containers (Kafka, Elasticsearch, Kibana)
docker compose up -d

# 3. Wait for services to be healthy (~30-60 seconds)
Start-Sleep -Seconds 30

# 4. Verify all 4 containers are running (especially Kafka)
docker compose ps

# 5. If Kafka shows "Exited", restart Zookeeper then Kafka:
#    docker compose restart zookeeper; Start-Sleep -Seconds 10; docker compose restart kafka

# 6. Clear old checkpoints (avoids stale state errors)
Remove-Item -Recurse -Force checkpoints -ErrorAction SilentlyContinue

# 7. Activate Python environment and start the producer (keep this terminal open)
.\venv\Scripts\Activate.ps1
python binance_to_kafka.py
```

### Terminal 2 — Spark Streaming

**Open a NEW PowerShell terminal** and run:

```powershell
# 1. Go to project folder
cd C:\Users\niou2\Documents\streamsight

# 2. Set JAVA_HOME to Java 17 (required — Spark 3.5 fails with Java 18+)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
$env:SPARK_HOME = "C:\spark\spark-3.5.7-bin-hadoop3"
$env:HADOOP_HOME = "C:\hadoop"
$env:PYSPARK_PYTHON = "C:\Users\niou2\Documents\streamsight\venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Users\niou2\Documents\streamsight\venv\Scripts\python.exe"
$env:PATH = "$env:JAVA_HOME\bin;C:\hadoop\bin;$env:SPARK_HOME\bin;$env:PATH"

# 3. Verify Java 17 is active (should print "openjdk version "17.0.18"")
java -version

# 4. Start Spark streaming job
spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4 `
  spark_streaming.py
```

---

## Step-by-Step Explanation

### Step 1: Start Docker Desktop

- Open **Docker Desktop** from Start Menu
- Wait until it says "Docker Desktop is running" (green icon in system tray)
- **Verify the engine is ready** by running `docker ps` in PowerShell — if it errors, wait a bit longer
- This starts the Docker engine needed for Kafka, Elasticsearch, and Kibana

### Step 2: Start Infrastructure Services

Open PowerShell and run:

```powershell
cd C:\Users\niou2\Documents\streamsight
docker compose up -d
```

This starts 4 containers:
- **Zookeeper** (Kafka coordination)
- **Kafka** (message broker)
- **Elasticsearch** (data storage)
- **Kibana** (visualization)

### Step 3: Verify Services Are Running

```powershell
docker compose ps
```

You should see all 4 services with status "Up":

```
NAME            STATUS
elasticsearch   Up (healthy)
kafka           Up
kibana          Up
zookeeper       Up
```

> **If Kafka shows "Exited":** This is a known issue caused by stale ZooKeeper nodes after unclean shutdowns. Fix it by restarting Zookeeper first, then Kafka:
> ```powershell
> docker compose restart zookeeper
> Start-Sleep -Seconds 10
> docker compose restart kafka
> ```

### Step 4: Clear Old Checkpoints

```powershell
Remove-Item -Recurse -Force checkpoints -ErrorAction SilentlyContinue
```

This prevents Spark from failing due to stale checkpoint state from previous runs.

### Step 5: Start the Binance Producer

```powershell
cd C:\Users\niou2\Documents\streamsight
.\venv\Scripts\Activate.ps1
python binance_to_kafka.py
```

You should see output like:
```
Connected to Binance WebSocket
Sent: {'symbol': 'BTCUSDT', 'price': 67614.0, 'quantity': 0.00013, ...}
Sent: {'symbol': 'BTCUSDT', 'price': 67613.54, 'quantity': 0.00148, ...}
```

**Keep this terminal open!** The producer needs to keep running.

### Step 6: Start Spark Streaming (New Terminal)

Open a **new PowerShell window** and run:

```powershell
cd C:\Users\niou2\Documents\streamsight

# CRITICAL: Set Java 17 — Spark 3.5 crashes with Java 18+ ("getSubject is not supported")
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

You should see Spark logs and eventually:
```
[STREAMSIGHT] Streaming started. Press Ctrl+C to stop.
[STREAMSIGHT] Checkpoints: C:/Users/niou2/Documents/streamsight/checkpoints
[STREAMSIGHT] Elasticsearch: http://localhost:9200/streamsight-trades
[STREAMSIGHT] Anomaly threshold: |Z| > 2.0
```

> **Note:** Batch 0 will be **empty** — this is normal. The first batch with actual data appears after the window duration (10s) + watermark delay (5s) = ~15 seconds. Only closed windows emit data.
>
> You may also see `WARN ProcessingTimeExecutor: Current batch is falling behind` — this is harmless during the first 2-3 batches while Spark warms up.

After ~30-60 seconds you should see batches with real data:
```
+-------+-------------------+-------------------+...
|symbol |window_start       |window_end         |...
+-------+-------------------+-------------------+...
|BTCUSDT|2026-02-18 00:38:50|2026-02-18 00:39:00|...
```

**Keep this terminal open too!**

### Step 7: Verify Everything Works

Open a **third PowerShell window** and check:

```powershell
# Check Elasticsearch has data
curl.exe -s http://localhost:9200/streamsight-trades/_count
```

You should see: `{"count":4,...}` (starts small, increases over time as windows close)

---

## Verification Commands

| What to Check | Command |
|---------------|---------|
| Docker containers running | `docker compose ps` |
| Kafka topic exists | `docker exec kafka kafka-topics --list --bootstrap-server localhost:9092` |
| Kafka receiving data | `docker exec kafka kafka-console-consumer --topic crypto_trades --bootstrap-server localhost:9092 --max-messages 3 --timeout-ms 10000` |
| Elasticsearch document count | `curl.exe -s http://localhost:9200/streamsight-trades/_count` |
| View sample ES documents | `curl.exe -s "http://localhost:9200/streamsight-trades/_search?pretty&size=3"` |
| View Kibana dashboard | Open browser: `http://localhost:5601` |

---

## How to Stop Everything

Stop the components in **reverse order** (Spark → Producer → Docker) to ensure a clean shutdown.

### Step 1: Stop Spark Streaming

Go to the **Spark terminal** and press `Ctrl+C`. You should see:

```
[STREAMSIGHT] Received shutdown signal. Stopping streaming query...
[STREAMSIGHT] All streams stopped.
```

> If Spark doesn't stop after a few seconds, press `Ctrl+C` again. The signal handler will force-stop all active streaming queries.

### Step 2: Stop the Binance Producer

Go to the **Producer terminal** and press `Ctrl+C`. You should see:

```
SIGINT received. Shutting down...
Shutdown complete.
```

The producer flushes any remaining messages to Kafka before exiting.

### Step 3: Stop Docker Containers

In **any PowerShell terminal**, run:

```powershell
cd C:\Users\niou2\Documents\streamsight
docker compose down
```

This stops and removes all 4 containers (Zookeeper, Kafka, Elasticsearch, Kibana).

> **Note:** Elasticsearch data is persisted in a Docker volume (`esdata`). Your indexed data will still be there next time you run `docker compose up -d`.

### Full Clean Shutdown (One-Liner)

If both Spark and the producer are already stopped, you can shut down everything with:

```powershell
docker compose down
```

### Nuclear Option: Remove All Data

To completely reset (delete ES data, Kafka topics, and Spark checkpoints):

```powershell
docker compose down -v
Remove-Item -Recurse -Force checkpoints -ErrorAction SilentlyContinue
```

The `-v` flag removes Docker volumes, wiping all Elasticsearch and Kafka data.

---

## Troubleshooting

### "Cannot connect to Docker daemon" / "open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified"
→ Docker Desktop app may be open but the engine isn't ready yet. Wait 30-60 seconds after launching Docker Desktop, then verify with `docker ps`.

### Kafka container shows "Exited" after `docker compose up -d`
→ This is caused by stale ZooKeeper ephemeral nodes after an unclean shutdown. Fix:
```powershell
docker compose restart zookeeper
Start-Sleep -Seconds 10
docker compose restart kafka
```

### Spark crashes with "getSubject is not supported"
→ You are running Spark with Java 18+. Spark 3.5 requires Java 8, 11, or 17. Fix:
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
java -version   # Should print "openjdk version "17.0.18""
```
Then re-run `spark-submit`.

### "Connection refused" on port 9092 (Kafka)
→ Wait longer after `docker compose up -d` (services need ~30-60 seconds to start). Verify with:
```powershell
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### "No data in Elasticsearch"
→ Make sure both producer AND Spark are running. With dev settings (10s window + 5s watermark), first results appear after ~15 seconds.

### Spark fails with checkpoint error
→ Clear checkpoints and restart:
```powershell
Remove-Item -Recurse -Force checkpoints -ErrorAction SilentlyContinue
# Then run spark-submit again
```

### "Current batch is falling behind" warning
→ This is normal during the first 2-3 batches while Spark initializes, downloads JARs, and warms up the JVM. The warning disappears once Spark reaches steady state. No action needed.

### Batch 0 is empty (no data rows)
→ This is expected. Spark uses `startingOffsets = "latest"`, so it only processes messages that arrive *after* it starts. The first non-empty batch appears after one full window duration (10s) + watermark delay (5s) ≈ 15 seconds.

---

## Summary: What Needs to Be Running

| Component | How to Start | Terminal | Notes |
|-----------|--------------|----------|-------|
| Docker Desktop | Open from Start Menu | Background | Wait for engine to be ready |
| Kafka + ES + Kibana | `docker compose up -d` | Can close after running | Check Kafka didn't crash |
| Binance Producer | `python binance_to_kafka.py` | Keep open | Activate venv first |
| Spark Streaming | `spark-submit ... spark_streaming.py` | Keep open | **Must set JAVA_HOME to Java 17** |
