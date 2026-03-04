#!/usr/bin/env python3
"""
demo_app.py — STREAMSIGHT Presentation Demo App

A single-file web application for presenting the STREAMSIGHT project.
Includes: project explanation, architecture, math, pipeline controls,
live terminal output streaming, embedded Kibana dashboard, and verification tools.

Usage:
    python demo_app.py
    Open http://localhost:8000

Requirements:
    pip install fastapi uvicorn
"""

import asyncio
import json
import os
import signal
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Dict, Optional

try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import HTMLResponse
except ImportError:
    print("ERROR: FastAPI not installed. Run: pip install fastapi uvicorn")
    sys.exit(1)

# =============================================================================
# CONFIG
# =============================================================================
PROJECT_DIR = Path(__file__).parent.resolve()
HOST = "0.0.0.0"
PORT = 8000

# Environment for Spark subprocess
SPARK_ENV = {
    **os.environ,
    "JAVA_HOME": r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot",
    "SPARK_HOME": r"C:\spark\spark-3.5.7-bin-hadoop3",
    "HADOOP_HOME": r"C:\hadoop",
    "PYSPARK_PYTHON": str(PROJECT_DIR / "venv" / "Scripts" / "python.exe"),
    "PYSPARK_DRIVER_PYTHON": str(PROJECT_DIR / "venv" / "Scripts" / "python.exe"),
}
SPARK_ENV["PATH"] = (
    SPARK_ENV["JAVA_HOME"] + r"\bin;" +
    SPARK_ENV["HADOOP_HOME"] + r"\bin;" +
    SPARK_ENV["SPARK_HOME"] + r"\bin;" +
    SPARK_ENV.get("PATH", "")
)

PYTHON_EXE = str(PROJECT_DIR / "venv" / "Scripts" / "python.exe")
SPARK_SUBMIT = SPARK_ENV["SPARK_HOME"] + r"\bin\spark-submit.cmd"
SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.4"

# =============================================================================
# PIPELINE STEPS
# =============================================================================
STEPS = [
    {
        "id": "infra",
        "num": 1,
        "title": "Start Infrastructure",
        "desc": "Launch Zookeeper, Kafka, Elasticsearch & Kibana via Docker Compose",
        "cmd": ["docker", "compose", "up", "-d"],
        "long_running": False,
        "env": None,
    },
    {
        "id": "es_index",
        "num": 2,
        "title": "Create Elasticsearch Index",
        "desc": "Create the streamsight-trades index with proper field mappings",
        "cmd": ["powershell", "-ExecutionPolicy", "Bypass", "-File", str(PROJECT_DIR / "create_es_index.ps1")],
        "long_running": False,
        "env": None,
    },
    {
        "id": "producer",
        "num": 3,
        "title": "Start Kafka Producer",
        "desc": "Connect to Binance WebSocket and stream live BTC trades to Kafka",
        "cmd": [PYTHON_EXE, str(PROJECT_DIR / "binance_to_kafka.py")],
        "long_running": True,
        "env": None,
    },
    {
        "id": "spark",
        "num": 4,
        "title": "Start Spark Streaming",
        "desc": "Consume from Kafka, compute windowed VWAP & Z-Score, write to Elasticsearch",
        "cmd": [SPARK_SUBMIT, "--packages", SPARK_PACKAGES, str(PROJECT_DIR / "spark_streaming.py")],
        "long_running": True,
        "env": SPARK_ENV,
    },
    {
        "id": "kibana",
        "num": 5,
        "title": "Setup Kibana Dashboard",
        "desc": "Create data view, visualizations, and dashboard via Kibana API",
        "cmd": [PYTHON_EXE, str(PROJECT_DIR / "setup_kibana.py")],
        "long_running": False,
        "env": None,
    },
]

# =============================================================================
# PROCESS MANAGER
# =============================================================================
processes: Dict[str, asyncio.subprocess.Process] = {}
ws_clients: Dict[str, set] = {s["id"]: set() for s in STEPS}
step_status: Dict[str, str] = {s["id"]: "pending" for s in STEPS}  # pending|running|done|error
step_output: Dict[str, list] = {s["id"]: [] for s in STEPS}

MAX_OUTPUT_LINES = 500


async def run_step(step_id: str):
    """Run a pipeline step, streaming output to all connected WebSocket clients."""
    step = next(s for s in STEPS if s["id"] == step_id)
    
    if step_status[step_id] == "running":
        return

    step_status[step_id] = "running"
    step_output[step_id] = []
    await broadcast(step_id, {"type": "status", "status": "running"})

    try:
        env = step.get("env") or os.environ.copy()
        proc = await asyncio.create_subprocess_exec(
            *step["cmd"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(PROJECT_DIR),
            env=env,
            creationflags=getattr(signal, "CREATE_NEW_PROCESS_GROUP", 0) if sys.platform == "win32" else 0,
        )
        processes[step_id] = proc

        async def read_stream():
            assert proc.stdout is not None
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip("\n\r")
                if text:
                    step_output[step_id].append(text)
                    if len(step_output[step_id]) > MAX_OUTPUT_LINES:
                        step_output[step_id] = step_output[step_id][-MAX_OUTPUT_LINES:]
                    await broadcast(step_id, {"type": "output", "text": text})

        await read_stream()
        await proc.wait()

        if proc.returncode == 0 or step["long_running"]:
            step_status[step_id] = "done"
            await broadcast(step_id, {"type": "status", "status": "done"})
        else:
            step_status[step_id] = "error"
            await broadcast(step_id, {"type": "status", "status": "error", "code": proc.returncode})

    except Exception as e:
        step_status[step_id] = "error"
        await broadcast(step_id, {"type": "status", "status": "error", "msg": str(e)})
    finally:
        processes.pop(step_id, None)


async def stop_step(step_id: str):
    """Stop a running step."""
    proc = processes.get(step_id)
    if proc:
        try:
            if sys.platform == "win32":
                proc.kill()
            else:
                proc.terminate()
            await asyncio.wait_for(proc.wait(), timeout=5)
        except Exception:
            proc.kill()
        processes.pop(step_id, None)
    step_status[step_id] = "done"
    await broadcast(step_id, {"type": "status", "status": "done"})


async def stop_all():
    """Stop all running processes and tear down Docker."""
    for step_id in list(processes.keys()):
        await stop_step(step_id)
    # Docker compose down
    proc = await asyncio.create_subprocess_exec(
        "docker", "compose", "down",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(PROJECT_DIR),
    )
    await proc.wait()
    for s in STEPS:
        step_status[s["id"]] = "pending"
        step_output[s["id"]] = []


async def broadcast(step_id: str, msg: dict):
    """Send a message to all WebSocket clients watching a step."""
    dead = set()
    for ws in ws_clients[step_id]:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.add(ws)
    ws_clients[step_id] -= dead


# =============================================================================
# FASTAPI APP
# =============================================================================
app = FastAPI(title="STREAMSIGHT Demo")


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_PAGE


@app.websocket("/ws/{step_id}")
async def websocket_endpoint(websocket: WebSocket, step_id: str):
    if step_id not in ws_clients:
        await websocket.close()
        return
    await websocket.accept()
    ws_clients[step_id].add(websocket)

    # Send current status + buffered output
    await websocket.send_json({"type": "status", "status": step_status[step_id]})
    for line in step_output[step_id]:
        await websocket.send_json({"type": "output", "text": line})

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            if action == "run":
                asyncio.create_task(run_step(step_id))
            elif action == "stop":
                asyncio.create_task(stop_step(step_id))
            elif action == "stop_all":
                asyncio.create_task(stop_all())
    except WebSocketDisconnect:
        ws_clients[step_id].discard(websocket)


@app.get("/api/es_count")
def es_count():
    try:
        req = urllib.request.Request("http://localhost:9200/streamsight-trades/_count")
        with urllib.request.urlopen(req, timeout=2) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/es_sample")
def es_sample():
    try:
        req = urllib.request.Request("http://localhost:9200/streamsight-trades/_search?size=3&pretty")
        with urllib.request.urlopen(req, timeout=2) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/health")
async def health_check():
    """Check services concurrently using threads to avoid slow sequential timeouts."""
    import concurrent.futures
    import socket

    def check_service(name: str, host: str, port: int) -> tuple:
        try:
            sock = socket.create_connection((host, port), timeout=2)
            sock.close()
            return (name, "up")
        except Exception:
            return (name, "down")

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        tasks = [
            loop.run_in_executor(pool, check_service, "elasticsearch", "localhost", 9200),
            loop.run_in_executor(pool, check_service, "kibana", "localhost", 5601),
        ]
        results_list = await asyncio.gather(*tasks)
    results = dict(results_list)
    return results


# =============================================================================
# HTML PAGE (embedded)
# =============================================================================
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>STREAMSIGHT — Live Demo</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.css">
<script src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.js"></script>
<style>
  :root {
    --bg: #ffffff;
    --bg2: #f6f8fa;
    --bg3: #eef1f5;
    --border: #d1d9e0;
    --text: #1f2328;
    --text2: #59636e;
    --accent: #0969da;
    --green: #1a7f37;
    --red: #cf222e;
    --orange: #bf8700;
    --purple: #8250df;
    --cyan: #0550ae;
    --radius: 12px;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.7;
    font-size: 1.1rem;
  }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* NAV */
  nav {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    z-index: 100;
    background: rgba(255,255,255,0.92);
    backdrop-filter: blur(12px);
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    border-bottom: 1px solid var(--border);
    padding: 0 2rem;
    display: flex;
    align-items: center;
    height: 56px;
    gap: 2rem;
  }
  nav .logo {
    font-size: 1.25rem;
    font-weight: 700;
    color: var(--accent);
    letter-spacing: 2px;
  }
  nav .nav-links { display:flex; gap:1.5rem; }
  nav .nav-links a {
    color: var(--text2);
    font-size: 1rem;
    font-weight: 500;
    transition: color 0.2s;
  }
  nav .nav-links a:hover { color: var(--text); text-decoration: none; }

  main { margin-top: 56px; }

  /* SECTION */
  section {
    max-width: 1200px;
    margin: 0 auto;
    padding: 3rem 2rem;
    border-bottom: 1px solid var(--border);
  }
  section:last-child { border-bottom: none; }
  h1 {
    font-size: 3.2rem;
    font-weight: 800;
    margin-bottom: 0.5rem;
    background: linear-gradient(135deg, var(--accent), var(--purple));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }
  h2 {
    font-size: 1.85rem;
    font-weight: 700;
    margin-bottom: 1rem;
    color: var(--text);
    padding-bottom: 0.5rem;
    border-bottom: 2px solid var(--accent);
    display: inline-block;
  }
  h3 {
    font-size: 1.3rem;
    font-weight: 600;
    margin: 1.5rem 0 0.5rem;
    color: var(--accent);
  }
  p, li { color: var(--text2); font-size: 1.1rem; }
  ul { padding-left: 1.5rem; margin: 0.5rem 0; }
  li { margin: 0.3rem 0; }

  /* HERO */
  .hero {
    text-align: center;
    padding: 5rem 2rem 3rem;
  }
  .hero .subtitle {
    font-size: 1.35rem;
    color: var(--text2);
    max-width: 750px;
    margin: 0.5rem auto 1.5rem;
  }
  .hero .authors {
    font-size: 1.05rem;
    color: var(--text2);
  }
  .badge {
    display: inline-block;
    padding: 0.3rem 0.9rem;
    border-radius: 20px;
    font-size: 0.9rem;
    font-weight: 600;
    margin: 0.25rem;
    border: 1px solid var(--border);
    color: var(--text2);
  }
  .badge.blue { border-color: var(--accent); color: var(--accent); }
  .badge.green { border-color: var(--green); color: var(--green); }
  .badge.purple { border-color: var(--purple); color: var(--purple); }
  .badge.orange { border-color: var(--orange); color: var(--orange); }

  /* CARDS */
  .card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.5rem;
    margin: 1rem 0;
  }
  .grid-2 {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1rem;
  }
  @media (max-width: 768px) { .grid-2 { grid-template-columns: 1fr; } }

  /* ARCHITECTURE */
  .arch-box {
    background: var(--bg3);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.5rem;
    font-family: 'Consolas', 'Courier New', monospace;
    font-size: 0.95rem;
    line-height: 1.4;
    overflow-x: auto;
    white-space: pre;
    color: var(--text2);
  }

  /* TECH TABLE */
  table {
    width: 100%;
    border-collapse: collapse;
    margin: 1rem 0;
    font-size: 1.05rem;
  }
  th, td {
    padding: 0.85rem 1.1rem;
    text-align: left;
    border-bottom: 1px solid var(--border);
  }
  th {
    color: var(--accent);
    font-weight: 600;
    background: var(--bg3);
  }
  td { color: var(--text2); }
  tr:hover td { background: var(--bg3); }

  /* MATH */
  .math-block {
    background: var(--bg3);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.5rem 2rem;
    margin: 1rem 0;
    text-align: center;
  }
  .math-block .label {
    font-size: 0.95rem;
    color: var(--text2);
    margin-top: 0.5rem;
  }

  /* PIPELINE STEPS */
  .step-card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    margin: 1rem 0;
    overflow: hidden;
    transition: border-color 0.3s;
  }
  .step-card.running { border-color: var(--orange); }
  .step-card.done { border-color: var(--green); }
  .step-card.error { border-color: var(--red); }

  .step-header {
    display: flex;
    align-items: center;
    padding: 1rem 1.5rem;
    gap: 1rem;
  }
  .step-num {
    width: 42px;
    height: 42px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    font-weight: 700;
    font-size: 1.05rem;
    background: var(--bg3);
    border: 2px solid var(--border);
    flex-shrink: 0;
  }
  .step-card.running .step-num { border-color: var(--orange); color: var(--orange); }
  .step-card.done .step-num { border-color: var(--green); color: var(--green); }
  .step-card.error .step-num { border-color: var(--red); color: var(--red); }

  .step-info { flex: 1; }
  .step-info .title { font-weight: 600; font-size: 1.15rem; color: var(--text); }
  .step-info .desc { font-size: 0.95rem; color: var(--text2); }

  .step-actions { display: flex; gap: 0.5rem; }

  .btn {
    padding: 0.55rem 1.35rem;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: var(--bg3);
    color: var(--text);
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s;
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }
  .btn:hover { background: var(--border); }
  .btn.primary { background: var(--accent); color: #fff; border-color: var(--accent); }
  .btn.primary:hover { opacity: 0.85; }
  .btn.danger { background: var(--red); color: #fff; border-color: var(--red); }
  .btn.danger:hover { opacity: 0.85; }
  .btn:disabled { opacity:0.4; cursor: not-allowed; }

  .status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
    background: var(--text2);
  }
  .status-dot.running { background: var(--orange); animation: pulse 1s infinite; }
  .status-dot.done { background: var(--green); }
  .status-dot.error { background: var(--red); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

  /* TERMINAL */
  .terminal {
    background: #1e1e2e;
    border-top: 1px solid var(--border);
    padding: 0.85rem 1.1rem;
    font-family: 'Consolas', 'Courier New', monospace;
    font-size: 0.9rem;
    line-height: 1.5;
    max-height: 280px;
    overflow-y: auto;
    color: #a6e3a1;
    display: none;
  }
  .terminal.visible { display: block; }
  .terminal .line { white-space: pre-wrap; word-break: break-all; }
  .terminal .line.error { color: #f38ba8; }

  /* KIBANA EMBED */
  .iframe-container {
    border: 1px solid var(--border);
    border-radius: var(--radius);
    overflow: hidden;
    margin: 1rem 0;
    background: var(--bg2);
  }
  .iframe-container iframe {
    width: 100%;
    height: 700px;
    border: none;
    background: #fff;
  }
  .iframe-placeholder {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 300px;
    color: var(--text2);
    gap: 1rem;
  }

  /* VERIFY PANEL */
  .verify-result {
    background: #1e1e2e;
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem;
    margin-top: 0.75rem;
    font-family: 'Consolas', monospace;
    font-size: 0.95rem;
    color: #89b4fa;
    max-height: 220px;
    overflow-y: auto;
    white-space: pre-wrap;
    display: none;
  }
  .verify-result.visible { display: block; }

  /* SHUTDOWN */
  .shutdown-section {
    text-align: center;
    padding: 4rem 2rem;
  }

  /* FLOW */
  .flow-container {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.25rem;
    margin: 1.5rem 0;
    flex-wrap: wrap;
  }
  .flow-node {
    background: var(--bg3);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.75rem 1.25rem;
    text-align: center;
    min-width: 120px;
  }
  .flow-node .fname { font-size: 1.05rem; font-weight: 600; color: var(--text); }
  .flow-node .fsub { font-size: 0.85rem; color: var(--text2); }
  .flow-arrow { color: var(--accent); font-size: 1.75rem; font-weight: bold; }

  /* SCROLLBAR */
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: var(--bg2); }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--text2); }
</style>
</head>
<body>

<!-- NAVIGATION -->
<nav>
  <div class="logo">STREAMSIGHT</div>
  <div class="nav-links">
    <a href="#about">About</a>
    <a href="#architecture">Architecture</a>
    <a href="#math">Math</a>
    <a href="#pipeline">Pipeline</a>
    <a href="#dashboard">Dashboard</a>
    <a href="#verify">Verify</a>
  </div>
</nav>

<main>

<!-- ===================== HERO ===================== -->
<section class="hero" id="top">
  <h1>STREAMSIGHT</h1>
  <p class="subtitle">
    Real-Time Cryptocurrency Market Anomaly Detection Platform
  </p>
  <div style="margin: 1rem 0;">
    <span class="badge blue">Apache Kafka</span>
    <span class="badge purple">Apache Spark</span>
    <span class="badge green">Elasticsearch</span>
    <span class="badge orange">Kibana</span>
  </div>
  <p class="authors">
    Farouk BENKHLIFA &amp; Mohammed Anes BOUCETLA
  </p>
</section>

<!-- ===================== ABOUT ===================== -->
<section id="about">
  <h2>What is STREAMSIGHT?</h2>

  <div class="card">
    <h3>The Problem</h3>
    <p>
      Cryptocurrency markets operate 24/7 with millions of trades per day.
      Large sudden orders — from whales or bots — can move prices in unexpected ways.
      How do we detect these anomalies <strong>in real time</strong>, as they happen?
    </p>
  </div>

  <div class="card">
    <h3>The Solution</h3>
    <p>
      STREAMSIGHT ingests <strong>live trade data</strong> from the Binance exchange via WebSocket,
      streams it through <strong>Apache Kafka</strong> for reliable delivery,
      processes it in <strong>Spark Structured Streaming</strong> using windowed VWAP aggregation and
      statistical Z-scores to flag anomalies, and stores results in <strong>Elasticsearch</strong> for
      visualization in <strong>Kibana</strong>.
    </p>
  </div>

  <h3>Technology Stack</h3>
  <table>
    <tr><th>Component</th><th>Technology</th><th>Why?</th></tr>
    <tr>
      <td><strong>Message Queue</strong></td>
      <td>Apache Kafka 7.5</td>
      <td>Durable, replayable message streaming. If Spark crashes, no data is lost.</td>
    </tr>
    <tr>
      <td><strong>Stream Processing</strong></td>
      <td>Spark Structured Streaming 3.5</td>
      <td>SQL-like API, exactly-once semantics, windowed aggregation, mature Python support.</td>
    </tr>
    <tr>
      <td><strong>Storage</strong></td>
      <td>Elasticsearch 8.11</td>
      <td>Sub-second queries, schemaless JSON, native Kibana integration.</td>
    </tr>
    <tr>
      <td><strong>Visualization</strong></td>
      <td>Kibana 8.11</td>
      <td>Real-time dashboards with auto-refresh, no-code chart builder.</td>
    </tr>
    <tr>
      <td><strong>Infrastructure</strong></td>
      <td>Docker Compose</td>
      <td>All services start with one command. Reproducible on any machine.</td>
    </tr>
    <tr>
      <td><strong>Data Source</strong></td>
      <td>Binance WebSocket API</td>
      <td>Free, real-time, high-frequency trade data for BTC/USDT.</td>
    </tr>
  </table>
</section>

<!-- ===================== ARCHITECTURE ===================== -->
<section id="architecture">
  <h2>Architecture</h2>

  <div class="flow-container">
    <div class="flow-node">
      <div class="fname">Binance</div>
      <div class="fsub">WebSocket API</div>
    </div>
    <div class="flow-arrow">→</div>
    <div class="flow-node">
      <div class="fname">Producer</div>
      <div class="fsub">binance_to_kafka.py</div>
    </div>
    <div class="flow-arrow">→</div>
    <div class="flow-node" style="border-color: var(--accent);">
      <div class="fname">Kafka</div>
      <div class="fsub">crypto_trades topic</div>
    </div>
    <div class="flow-arrow">→</div>
    <div class="flow-node" style="border-color: var(--purple);">
      <div class="fname">Spark</div>
      <div class="fsub">VWAP + Z-Score</div>
    </div>
    <div class="flow-arrow">→</div>
    <div class="flow-node" style="border-color: var(--green);">
      <div class="fname">Elasticsearch</div>
      <div class="fsub">streamsight-trades</div>
    </div>
    <div class="flow-arrow">→</div>
    <div class="flow-node" style="border-color: var(--orange);">
      <div class="fname">Kibana</div>
      <div class="fsub">Dashboard</div>
    </div>
  </div>

  <div class="arch-box">┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────┐
│   Binance WS    │────▶│ Python Producer │────▶│    Apache Kafka         │
│  (btcusdt@trade)│     │ (binance_to_    │     │  (crypto_trades topic)  │
└─────────────────┘     │  kafka.py)      │     └───────────┬─────────────┘
                        └─────────────────┘                 │
                                                            ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────┐
│     Kibana      │◀────│  Elasticsearch  │◀────│  Spark Structured       │
│   Dashboard     │     │ (streamsight-   │     │  Streaming              │
│   (Port 5601)   │     │  trades index)  │     │  (spark_streaming.py)   │
└─────────────────┘     └─────────────────┘     └─────────────────────────┘

Data Flow:
  1. Binance streams raw trades via WebSocket (symbol, price, quantity, timestamp)
  2. Python producer serializes to JSON and publishes to Kafka topic
  3. Spark consumes from Kafka, applies watermarking for late data
  4. 10-second tumbling windows aggregate trades → VWAP, Z-Score, anomaly flags
  5. Results upserted to Elasticsearch with deterministic doc IDs (idempotent)
  6. Kibana auto-refreshes dashboard every 10 seconds</div>

  <h3>Key Design Decisions</h3>
  <div class="grid-2">
    <div class="card">
      <h3>Why Kafka over Redis Pub/Sub?</h3>
      <p>Kafka persists messages to disk. If Spark restarts, it can replay from where it left off. Redis Pub/Sub is fire-and-forget — messages lost if no subscriber is connected.</p>
    </div>
    <div class="card">
      <h3>Why Spark over Flink?</h3>
      <p>Spark has a mature Python API (PySpark), SQL-like syntax, better Windows support, and a huge ecosystem. Flink has lower latency but a steeper learning curve.</p>
    </div>
    <div class="card">
      <h3>Why Elasticsearch over PostgreSQL?</h3>
      <p>Native Kibana integration, schemaless JSON, sub-second queries on time-series data. No ORM or migrations needed.</p>
    </div>
    <div class="card">
      <h3>Why Tumbling Windows?</h3>
      <p>Non-overlapping 10-second windows keep state small and output deterministic. Each trade belongs to exactly one window.</p>
    </div>
  </div>
</section>

<!-- ===================== MATH ===================== -->
<section id="math">
  <h2>Mathematical Foundation</h2>

  <div class="grid-2">
    <div class="math-block">
      <h3 style="margin-top:0;">VWAP — Volume-Weighted Average Price</h3>
      <div id="katex-vwap" style="margin: 1rem 0;"></div>
      <p class="label">
        Gives more weight to trades with larger volume.<br>
        P<sub>i</sub> = price of trade i, Q<sub>i</sub> = quantity of trade i
      </p>
    </div>
    <div class="math-block">
      <h3 style="margin-top:0;">Z-Score — Anomaly Detection</h3>
      <div id="katex-zscore" style="margin: 1rem 0;"></div>
      <p class="label">
        Measures how many standard deviations VWAP is from mean price.<br>
        |Z| &gt; 2.0 → Anomaly (~4.6% of normal distribution)
      </p>
    </div>
  </div>

  <div class="card">
    <h3>Interpretation</h3>
    <table>
      <tr><th>Z-Score</th><th>Meaning</th><th>Flag</th></tr>
      <tr><td>|Z| ≤ 2.0</td><td>Normal price movement — VWAP is close to average</td><td>NORMAL</td></tr>
      <tr><td>Z &gt; 2.0</td><td>Large buy orders pushed VWAP above average</td><td style="color:var(--red);">HIGH_VWAP</td></tr>
      <tr><td>Z &lt; -2.0</td><td>Large sell orders pushed VWAP below average</td><td style="color:var(--red);">LOW_VWAP</td></tr>
    </table>
  </div>

  <div class="card">
    <h3>Windowing &amp; Watermark</h3>
    <p>
      <strong>Window:</strong> 10-second tumbling windows — each trade belongs to exactly one window.<br>
      <strong>Watermark:</strong> 5-second tolerance for late-arriving data.<br>
      <strong>Output delay:</strong> ~15 seconds from first event to first result (window closes + watermark advances).
    </p>
    <p style="margin-top:0.5rem;">
      This is not a bug — it's <strong>exactly-once semantics</strong>. Spark waits until it's confident
      all data for a window has arrived before emitting results.
    </p>
  </div>
</section>

<!-- ===================== PIPELINE ===================== -->
<section id="pipeline">
  <h2>Pipeline Control</h2>
  <p style="margin-bottom:1rem;">Click each step in order to start the pipeline. Terminal output streams in real-time.</p>

  <div id="steps-container"></div>

  <div style="text-align:center; margin-top:2rem;">
    <button class="btn danger" onclick="stopAll()" id="btn-stop-all" style="display:none;">
      🛑 Stop Everything
    </button>
  </div>
</section>

<!-- ===================== KIBANA DASHBOARD ===================== -->
<section id="dashboard">
  <h2>Live Dashboard</h2>
  <p style="margin-bottom:0.5rem;">
    Kibana dashboard updates every 10 seconds with live data from Elasticsearch.
  </p>
  <div style="display:flex; gap:0.5rem; margin-bottom:0.75rem; flex-wrap:wrap;">
    <button class="btn primary" onclick="loadKibana()">📊 Load Kibana Dashboard</button>
    <button class="btn" onclick="openKibanaPopup()">⧉ Open in Popup Window</button>
    <a class="btn" href="http://localhost:5601/app/dashboards#/view/streamsight-dashboard" target="_blank">
      Open in New Tab ↗
    </a>
  </div>
  <p id="kibana-status" style="margin:0 0 0.75rem; font-weight:600; font-size:1rem;"></p>
  <div class="iframe-container" id="kibana-container">
    <div class="iframe-placeholder" id="kibana-placeholder">
      <div style="font-size:2rem;">📊</div>
      <p>Click "Load Kibana Dashboard" after completing steps 1-5</p>
    </div>
  </div>
</section>

<!-- ===================== VERIFY ===================== -->
<section id="verify">
  <h2>Live Verification</h2>
  <p style="margin-bottom:1rem;">Run these checks to prove data is flowing through the pipeline.</p>

  <div class="grid-2">
    <div class="card">
      <h3 style="margin-top:0;">📈 Elasticsearch Document Count</h3>
      <p>Shows how many aggregated records are stored.</p>
      <button class="btn primary" onclick="checkEsCount()" style="margin-top:0.75rem;">Check Count</button>
      <div class="verify-result" id="verify-count"></div>
    </div>
    <div class="card">
      <h3 style="margin-top:0;">📄 Sample Documents</h3>
      <p>View the latest records written by Spark.</p>
      <button class="btn primary" onclick="checkEsSample()" style="margin-top:0.75rem;">Fetch Sample</button>
      <div class="verify-result" id="verify-sample"></div>
    </div>
    <div class="card">
      <h3 style="margin-top:0;">🔗 Service Health</h3>
      <p>Check if Elasticsearch and Kibana are running.</p>
      <button class="btn primary" onclick="checkHealth()" style="margin-top:0.75rem;">Check Health</button>
      <div class="verify-result" id="verify-health"></div>
    </div>
    <div class="card">
      <h3 style="margin-top:0;">🔄 Auto-Refresh Count</h3>
      <p>Watch the document count grow every 5 seconds.</p>
      <button class="btn primary" onclick="toggleAutoCount(this)" style="margin-top:0.75rem;">Start Auto-Count</button>
      <div class="verify-result visible" id="verify-auto" style="font-size:2.5rem; text-align:center; color: var(--green); height: 70px; display:flex; align-items:center; justify-content:center;">—</div>
    </div>
  </div>
</section>

<!-- ===================== SHUTDOWN ===================== -->
<section class="shutdown-section">
  <h2>Shutdown</h2>
  <p style="margin-bottom:1.5rem; color:var(--text2);">
    Stop all processes and tear down Docker containers.
  </p>
  <button class="btn danger" onclick="stopAll()" style="font-size:1.1rem; padding: 0.75rem 2rem;">
    🛑 Stop Everything &amp; Clean Up
  </button>
</section>

</main>

<script>
// =============================================================================
// RENDER KATEX
// =============================================================================
document.addEventListener('DOMContentLoaded', () => {
  try {
    katex.render(String.raw`\text{VWAP} = \frac{\displaystyle\sum_{i=1}^{n} P_i \times Q_i}{\displaystyle\sum_{i=1}^{n} Q_i}`,
      document.getElementById('katex-vwap'), {displayMode: true, throwOnError: false});
    katex.render(String.raw`Z = \frac{\text{VWAP} - \mu}{\sigma}`,
      document.getElementById('katex-zscore'), {displayMode: true, throwOnError: false});
  } catch(e) { console.warn('KaTeX render error', e); }

  buildSteps();
});

// =============================================================================
// STEP CARDS
// =============================================================================
const STEPS = [
  {id:"infra",  num:1, title:"Start Infrastructure",       desc:"Launch Zookeeper, Kafka, Elasticsearch & Kibana via Docker Compose", long:false},
  {id:"es_index",num:2, title:"Create Elasticsearch Index",  desc:"Create the streamsight-trades index with proper field mappings",     long:false},
  {id:"producer",num:3, title:"Start Kafka Producer",        desc:"Connect to Binance WebSocket and stream live BTC trades into Kafka",long:true},
  {id:"spark",   num:4, title:"Start Spark Streaming",       desc:"Consume from Kafka → windowed VWAP & Z-Score → write to Elasticsearch",long:true},
  {id:"kibana",  num:5, title:"Setup Kibana Dashboard",      desc:"Create data view, visualizations, and dashboard via Kibana API",   long:false},
];

const sockets = {};
const stepEls = {};

function buildSteps() {
  const container = document.getElementById('steps-container');
  STEPS.forEach(s => {
    const div = document.createElement('div');
    div.className = 'step-card';
    div.id = 'step-' + s.id;
    div.innerHTML = `
      <div class="step-header">
        <div class="step-num">${s.num}</div>
        <div class="step-info">
          <div class="title">${s.title}</div>
          <div class="desc">${s.desc}</div>
        </div>
        <div class="step-actions">
          <span class="status-dot" id="dot-${s.id}"></span>
          <button class="btn primary" id="btn-run-${s.id}" onclick="runStep('${s.id}')">▶ Run</button>
          ${s.long ? `<button class="btn danger" id="btn-stop-${s.id}" onclick="stopStep('${s.id}')" style="display:none;">⏹ Stop</button>` : ''}
        </div>
      </div>
      <div class="terminal" id="term-${s.id}"></div>
    `;
    container.appendChild(div);
    stepEls[s.id] = div;
    connectWs(s.id);
  });
}

function connectWs(stepId) {
  const ws = new WebSocket(`ws://${location.host}/ws/${stepId}`);
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'status') updateStatus(stepId, msg.status);
    if (msg.type === 'output') appendOutput(stepId, msg.text);
  };
  ws.onclose = () => { setTimeout(() => connectWs(stepId), 2000); };
  sockets[stepId] = ws;
}

function runStep(stepId) {
  const ws = sockets[stepId];
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify({action: 'run'}));
  }
  document.getElementById('btn-stop-all').style.display = 'inline-flex';
}

function stopStep(stepId) {
  const ws = sockets[stepId];
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({action: 'stop'}));
}

function stopAll() {
  // Send stop_all through any connected socket
  for (const ws of Object.values(sockets)) {
    if (ws.readyState === 1) {
      ws.send(JSON.stringify({action: 'stop_all'}));
      break;
    }
  }
  document.getElementById('btn-stop-all').style.display = 'none';
}

function updateStatus(stepId, status) {
  const card = stepEls[stepId];
  const dot = document.getElementById('dot-' + stepId);
  const btnRun = document.getElementById('btn-run-' + stepId);
  const btnStop = document.getElementById('btn-stop-' + stepId);
  const term = document.getElementById('term-' + stepId);
  const step = STEPS.find(s => s.id === stepId);

  card.className = 'step-card ' + (status === 'running' ? 'running' : status === 'done' ? 'done' : status === 'error' ? 'error' : '');
  dot.className = 'status-dot ' + status;

  if (status === 'running') {
    btnRun.disabled = true;
    if (btnStop) btnStop.style.display = 'inline-flex';
    term.classList.add('visible');
  } else {
    btnRun.disabled = false;
    if (btnStop) btnStop.style.display = 'none';
    if (status === 'pending') {
      term.classList.remove('visible');
      term.innerHTML = '';
    }
  }
}

function appendOutput(stepId, text) {
  const term = document.getElementById('term-' + stepId);
  term.classList.add('visible');
  const line = document.createElement('div');
  line.className = 'line';
  if (text.toLowerCase().includes('error') || text.toLowerCase().includes('exception')) {
    line.classList.add('error');
  }
  line.textContent = text;
  term.appendChild(line);
  // Auto-scroll
  term.scrollTop = term.scrollHeight;
}

// =============================================================================
// KIBANA IFRAME
// =============================================================================
const KIBANA_URL = 'http://localhost:5601';
const KIBANA_DASH = KIBANA_URL + '/app/dashboards#/view/streamsight-dashboard?embed=true&_g=(refreshInterval:(pause:!f,value:10000),time:(from:now-30m,to:now))';
const KIBANA_DASH_FULL = KIBANA_URL + '/app/dashboards#/view/streamsight-dashboard?_g=(refreshInterval:(pause:!f,value:10000),time:(from:now-30m,to:now))';

function loadKibana() {
  const container = document.getElementById('kibana-container');
  const placeholder = document.getElementById('kibana-placeholder');
  const status = document.getElementById('kibana-status');
  status.textContent = 'Loading Kibana...';
  status.style.color = 'var(--orange)';

  // First check if Kibana is up
  fetch('/api/health').then(r => r.json()).then(d => {
    if (d.kibana !== 'up') {
      status.textContent = '\u274c Kibana is not running. Complete Step 1 first.';
      status.style.color = 'var(--red)';
      return;
    }
    if (placeholder) placeholder.remove();
    const old = container.querySelector('iframe');
    if (old) old.remove();
    const iframe = document.createElement('iframe');
    iframe.src = KIBANA_DASH;
    iframe.allow = 'fullscreen';
    iframe.onload = () => {
      status.textContent = '\u2705 Dashboard loaded';
      status.style.color = 'var(--green)';
    };
    iframe.onerror = () => {
      status.textContent = '\u26a0\ufe0f Iframe blocked by Kibana CSP. Use "Open in New Tab" instead.';
      status.style.color = 'var(--orange)';
    };
    container.appendChild(iframe);
    // Fallback: if iframe looks empty after 5s, show hint
    setTimeout(() => {
      if (status.textContent.includes('Loading')) {
        status.textContent = '\u2705 Loaded (if blank, try "Open in Popup Window")';
        status.style.color = 'var(--green)';
      }
    }, 5000);
  }).catch(() => {
    status.textContent = '\u274c Cannot reach demo server. Is it running?';
    status.style.color = 'var(--red)';
  });
}

function openKibanaTab() {
  window.open(KIBANA_DASH_FULL, '_blank');
}

function openKibanaPopup() {
  window.open(KIBANA_DASH_FULL, 'KibanaDashboard', 'width=1400,height=900,scrollbars=yes,resizable=yes');
}

// =============================================================================
// VERIFICATION
// =============================================================================
async function checkEsCount() {
  const el = document.getElementById('verify-count');
  el.classList.add('visible');
  el.textContent = 'Loading...';
  try {
    const r = await fetch('/api/es_count');
    const d = await r.json();
    if (d.error) { el.textContent = 'Error: ' + d.error; return; }
    el.textContent = `Document count: ${d.count}\n\nIndex: streamsight-trades\nShards: ${JSON.stringify(d._shards)}`;
  } catch(e) { el.textContent = 'Failed: ' + e.message; }
}

async function checkEsSample() {
  const el = document.getElementById('verify-sample');
  el.classList.add('visible');
  el.textContent = 'Loading...';
  try {
    const r = await fetch('/api/es_sample');
    const d = await r.json();
    if (d.error) { el.textContent = 'Error: ' + d.error; return; }
    const hits = d.hits?.hits || [];
    if (hits.length === 0) { el.textContent = 'No documents found. Pipeline may not be running yet.'; return; }
    el.textContent = hits.map(h => JSON.stringify(h._source, null, 2)).join('\n\n---\n\n');
  } catch(e) { el.textContent = 'Failed: ' + e.message; }
}

async function checkHealth() {
  const el = document.getElementById('verify-health');
  el.classList.add('visible');
  el.textContent = 'Checking...';
  try {
    const r = await fetch('/api/health');
    const d = await r.json();
    const lines = Object.entries(d).map(([k,v]) => {
      const icon = v === 'up' ? '✅' : '❌';
      return `${icon}  ${k}: ${v}`;
    });
    el.textContent = lines.join('\n');
  } catch(e) { el.textContent = 'Failed: ' + e.message; }
}

let autoCountInterval = null;
function toggleAutoCount(btn) {
  if (autoCountInterval) {
    clearInterval(autoCountInterval);
    autoCountInterval = null;
    btn.textContent = 'Start Auto-Count';
    return;
  }
  btn.textContent = 'Stop Auto-Count';
  const el = document.getElementById('verify-auto');
  async function tick() {
    try {
      const r = await fetch('/api/es_count');
      const d = await r.json();
      el.textContent = d.count !== undefined ? d.count.toLocaleString() : 'Error';
      el.style.color = 'var(--green)';
    } catch(e) {
      el.textContent = '—';
      el.style.color = 'var(--red)';
    }
  }
  tick();
  autoCountInterval = setInterval(tick, 5000);
}
</script>
</body>
</html>"""


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    import uvicorn

    print("=" * 60)
    print("  STREAMSIGHT — Demo App")
    print("=" * 60)
    print(f"\n  Open: http://localhost:{PORT}\n")
    print("  Press Ctrl+C to stop the demo server.\n")

    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
