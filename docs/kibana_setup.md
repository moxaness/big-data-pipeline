# STREAMSIGHT — Kibana Dashboard Guide

## Quick Start (Automated)

The fastest way to set up your Kibana dashboard — one command:

```powershell
python setup_kibana.py
```

This creates a **data view**, **5 visualizations**, and a **dashboard** automatically.
Then open: **http://localhost:5601/app/dashboards#/view/streamsight-dashboard**

### Script Options

| Command | What it does |
|---|---|
| `python setup_kibana.py` | Create everything via Kibana API |
| `python setup_kibana.py --reset` | Delete existing objects, then recreate |
| `python setup_kibana.py --export-only` | Only generate `kibana_dashboard.ndjson` (no API calls) |

### Manual Import (Alternative)

If the API method fails, the script also generates a `kibana_dashboard.ndjson` file you can import manually:

1. Open **http://localhost:5601**
2. Go to **Stack Management** (gear icon, left sidebar) → **Saved Objects**
3. Click **Import** (top-right)
4. Select `kibana_dashboard.ndjson`
5. Click **Import**
6. Go to **Dashboard** (left sidebar) → open **STREAMSIGHT Real-Time Crypto Analytics**

---

## What Gets Created

### Data View
- **Name**: STREAMSIGHT Trades
- **Index**: `streamsight-trades`
- **Time field**: `window_start`

### Visualizations

| # | Name | Type | What it shows |
|---|---|---|---|
| 1 | VWAP Over Time by Symbol | Line chart | Volume-Weighted Average Price per crypto, over time |
| 2 | Volume Over Time by Symbol | Stacked bar chart | Total trade volume per crypto, over time |
| 3 | Anomaly Events | Data table | Only rows where `is_anomaly = true` (high Z-scores) |
| 4 | Z-Score Over Time by Symbol | Area chart | Z-score values per crypto (spikes = anomalies) |
| 5 | Total Trades | Metric | Single number — total trade count |

### Dashboard Layout

```
┌──────────────────────────────────────────────────┐
│           VWAP Over Time by Symbol               │
│               (line chart)                       │
├────────────────────────┬─────────────────────────┤
│  Volume Over Time      │    Anomaly Events       │
│  (stacked bars)        │    (table)              │
├────────────────────────────────────┬─────────────┤
│  Z-Score Over Time by Symbol      │  Total       │
│  (area chart)                     │  Trades      │
└────────────────────────────────────┴─────────────┘
```

---

## Using the Dashboard (Beginner Guide)

### Opening the Dashboard

1. Open **http://localhost:5601** in your browser
2. Click **Dashboard** in the left sidebar (or use the hamburger menu ☰)
3. Click **STREAMSIGHT Real-Time Crypto Analytics**

### Setting the Time Range

By default, Kibana may show "Last 15 minutes". To change:

1. Click the **calendar icon** (top-right, next to the search bar)
2. Pick a range:
   - **Last 15 minutes** — good for live monitoring
   - **Last 1 hour** — see more history
   - **Last 24 hours** — full day view
3. Or set a custom range with the date pickers

### Enabling Auto-Refresh

To see live data updates:

1. Click the **calendar icon** (top-right)
2. Under **Refresh every**, toggle it **ON**
3. Set interval to **10 seconds** (or 5s for faster updates)
4. Click **Start**

The dashboard will now poll Elasticsearch every 10 seconds.

### Filtering Data

**Filter by symbol** (e.g., only show BTCUSDT):
1. Click **+ Add filter** (below the search bar)
2. Field: `symbol`
3. Operator: `is`
4. Value: `BTCUSDT`
5. Click **Save**

**Search with KQL** (Kibana Query Language):
Type in the search bar:
- `symbol: BTCUSDT` — show only Bitcoin
- `is_anomaly: true` — show only anomalies
- `zscore > 2` — show high Z-scores
- `symbol: ETHUSDT and zscore > 1.5` — combine conditions

### Interacting with Charts

- **Hover** over a data point → see tooltip with exact values
- **Click and drag** on a chart → zoom into that time range
- **Click a legend item** → hide/show that series
- **Double-click a legend item** → isolate that series

### Resizing/Moving Panels

- **Drag** the title bar of any panel to move it
- **Drag** the bottom-right corner to resize
- Click **Save** (top-right) to keep your layout changes

---

## Troubleshooting

### "No results found" in dashboard
1. **Check time range** — click the calendar and set to "Last 1 hour"
2. **Check data exists**:
   ```powershell
   curl http://localhost:9200/streamsight-trades/_count
   ```
   If count is 0, make sure `binance_to_kafka.py` and `spark_streaming.py` are running.
3. **Wait for first window** — Spark needs ~15-30 seconds (window + watermark) before emitting data

### Dashboard shows old data only
- Enable auto-refresh (see above)
- Or click the **Refresh** button (circular arrow, top-right)

### Visualizations show errors
- Re-run `python setup_kibana.py --reset` to recreate everything
- Make sure the ES index mapping is correct: `.\create_es_index.ps1`

### Script says "Kibana not available"
```powershell
# Check if Docker services are running
docker compose ps

# Start if needed
docker compose up -d

# Wait ~30 seconds for Kibana to initialize, then retry
python setup_kibana.py
```

### Want to start from scratch
```powershell
python setup_kibana.py --reset
```

---

## Prerequisites Checklist

Before running the setup script, make sure:

- [ ] Docker services are running: `docker compose up -d`
- [ ] Elasticsearch index exists: `.\create_es_index.ps1`
- [ ] Kibana is accessible: open http://localhost:5601
- [ ] (For live data) `python binance_to_kafka.py` is running
- [ ] (For live data) `spark-submit ... spark_streaming.py` is running
