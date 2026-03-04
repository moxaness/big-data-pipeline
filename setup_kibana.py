#!/usr/bin/env python3
"""
setup_kibana.py — Automatically create STREAMSIGHT Kibana dashboard

Creates data view, 5 visualizations, and a dashboard via the Kibana API.
Also generates an NDJSON file for manual import as a backup.

Usage:
    python setup_kibana.py                # Create everything via API
    python setup_kibana.py --reset        # Delete existing objects first, then recreate
    python setup_kibana.py --export-only  # Only generate NDJSON file (no API calls)

Prerequisites:
    - Docker services running:  docker compose up -d
    - ES index created:         .\\create_es_index.ps1
    - Data flowing:             python binance_to_kafka.py  (terminal 1)
                                spark-submit ... spark_streaming.py  (terminal 2)
"""

import json
import urllib.request
import urllib.error
import sys
import time
import os

# =============================================================================
# CONFIGURATION
# =============================================================================
KIBANA_URL = "http://localhost:5601"
DATA_VIEW_ID = "streamsight-dv"
INDEX_PATTERN = "streamsight-trades"

HEADERS = {
    "Content-Type": "application/json",
    "kbn-xsrf": "true",
}

# Saved object IDs (stable so re-runs are idempotent)
IDS = {
    "data_view": DATA_VIEW_ID,
    "vwap": "streamsight-vwap",
    "volume": "streamsight-volume",
    "anomaly": "streamsight-anomaly",
    "zscore": "streamsight-zscore",
    "trades": "streamsight-trades-metric",
    "dashboard": "streamsight-dashboard",
}


# =============================================================================
# HTTP HELPERS
# =============================================================================
def kibana_request(method, path, body=None):
    """Make a request to the Kibana API. Returns parsed JSON or None."""
    url = f"{KIBANA_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=HEADERS, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
            if not raw.strip():
                return {}
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        if e.code == 409:
            print(f"    [SKIP] Already exists (use --reset to overwrite)")
            return None
        print(f"    [ERROR] HTTP {e.code}: {error_body[:200]}")
        return None
    except urllib.error.URLError as e:
        print(f"    [ERROR] Connection failed: {e}")
        return None


def wait_for_kibana(timeout=60):
    """Wait for Kibana to become available."""
    print(f"Checking Kibana at {KIBANA_URL} ...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            req = urllib.request.Request(
                f"{KIBANA_URL}/api/status", headers=HEADERS, method="GET"
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                level = data.get("status", {}).get("overall", {}).get("level", "")
                if level == "available":
                    print("  Kibana is ready!\n")
                    return True
        except Exception:
            pass
        time.sleep(2)
    print("  [ERROR] Kibana not available. Run: docker compose up -d")
    return False


def delete_saved_object(obj_type, obj_id):
    """Delete a saved object (ignore if not found)."""
    try:
        kibana_request("DELETE", f"/api/saved_objects/{obj_type}/{obj_id}?force=true")
    except Exception:
        pass


# =============================================================================
# DATA VIEW
# =============================================================================
def create_data_view():
    print("[1/2] Creating data view ...")
    body = {
        "data_view": {
            "id": DATA_VIEW_ID,
            "title": INDEX_PATTERN,
            "timeFieldName": "window_start",
            "name": "STREAMSIGHT Trades",
        },
        "override": True,
    }
    result = kibana_request("POST", "/api/data_views/data_view", body)
    if result:
        print("    OK — Data view created")
    return result


# =============================================================================
# LENS VISUALIZATION BUILDER
# =============================================================================
def _build_lens_body(title, vis_type, state, layer_ids):
    """Build the request body for a Lens saved object."""
    references = [
        {
            "type": "index-pattern",
            "id": DATA_VIEW_ID,
            "name": f"indexpattern-datasource-layer-{lid}",
        }
        for lid in layer_ids
    ]
    return {
        "attributes": {
            "title": title,
            "description": "",
            "visualizationType": vis_type,
            "state": state,
            "references": references,
        },
        "references": references,
    }


def create_lens(obj_id, title, vis_type, state, layer_ids):
    """Create a single Lens visualization."""
    body = _build_lens_body(title, vis_type, state, layer_ids)
    result = kibana_request(
        "POST", f"/api/saved_objects/lens/{obj_id}?overwrite=true", body
    )
    if result:
        print(f"    OK — {title}")
    return result


# =============================================================================
# INDIVIDUAL VISUALIZATIONS
# =============================================================================

def create_vwap_chart():
    """VWAP Over Time — line chart split by symbol."""
    print("[2/7] Creating VWAP Over Time chart ...")
    layer_id = "vwap-layer"
    state = {
        "datasourceStates": {
            "indexpattern": {
                "layers": {
                    layer_id: {
                        "columnOrder": ["col-symbol", "col-time", "col-vwap"],
                        "columns": {
                            "col-time": {
                                "dataType": "date",
                                "isBucketed": True,
                                "label": "Time",
                                "operationType": "date_histogram",
                                "params": {"interval": "auto"},
                                "scale": "interval",
                                "sourceField": "window_start",
                            },
                            "col-vwap": {
                                "dataType": "number",
                                "isBucketed": False,
                                "label": "Average VWAP",
                                "operationType": "average",
                                "scale": "ratio",
                                "sourceField": "vwap",
                            },
                            "col-symbol": {
                                "dataType": "string",
                                "isBucketed": True,
                                "label": "Symbol",
                                "operationType": "terms",
                                "scale": "ordinal",
                                "sourceField": "symbol",
                                "params": {
                                    "size": 10,
                                    "orderBy": {
                                        "type": "column",
                                        "columnId": "col-vwap",
                                    },
                                    "orderDirection": "desc",
                                },
                            },
                        },
                        "incompleteColumns": {},
                    }
                }
            }
        },
        "visualization": {
            "layers": [
                {
                    "accessors": ["col-vwap"],
                    "layerId": layer_id,
                    "layerType": "data",
                    "seriesType": "line",
                    "xAccessor": "col-time",
                    "splitAccessor": "col-symbol",
                }
            ],
            "legend": {"isVisible": True, "position": "right"},
            "preferredSeriesType": "line",
            "valueLabels": "hide",
        },
        "query": {"language": "kuery", "query": ""},
        "filters": [],
    }
    return create_lens(IDS["vwap"], "VWAP Over Time by Symbol", "lnsXY", state, [layer_id])


def create_volume_chart():
    """Volume Over Time — stacked bar chart split by symbol."""
    print("[3/7] Creating Volume Over Time chart ...")
    layer_id = "volume-layer"
    state = {
        "datasourceStates": {
            "indexpattern": {
                "layers": {
                    layer_id: {
                        "columnOrder": ["col-symbol", "col-time", "col-volume"],
                        "columns": {
                            "col-time": {
                                "dataType": "date",
                                "isBucketed": True,
                                "label": "Time",
                                "operationType": "date_histogram",
                                "params": {"interval": "auto"},
                                "scale": "interval",
                                "sourceField": "window_start",
                            },
                            "col-volume": {
                                "dataType": "number",
                                "isBucketed": False,
                                "label": "Total Volume",
                                "operationType": "sum",
                                "scale": "ratio",
                                "sourceField": "total_volume",
                            },
                            "col-symbol": {
                                "dataType": "string",
                                "isBucketed": True,
                                "label": "Symbol",
                                "operationType": "terms",
                                "scale": "ordinal",
                                "sourceField": "symbol",
                                "params": {
                                    "size": 10,
                                    "orderBy": {
                                        "type": "column",
                                        "columnId": "col-volume",
                                    },
                                    "orderDirection": "desc",
                                },
                            },
                        },
                        "incompleteColumns": {},
                    }
                }
            }
        },
        "visualization": {
            "layers": [
                {
                    "accessors": ["col-volume"],
                    "layerId": layer_id,
                    "layerType": "data",
                    "seriesType": "bar_stacked",
                    "xAccessor": "col-time",
                    "splitAccessor": "col-symbol",
                }
            ],
            "legend": {"isVisible": True, "position": "right"},
            "preferredSeriesType": "bar_stacked",
            "valueLabels": "hide",
        },
        "query": {"language": "kuery", "query": ""},
        "filters": [],
    }
    return create_lens(IDS["volume"], "Volume Over Time by Symbol", "lnsXY", state, [layer_id])


def create_anomaly_table():
    """Anomaly Events — data table filtered to is_anomaly: true."""
    print("[4/7] Creating Anomaly Events table ...")
    layer_id = "anomaly-layer"
    state = {
        "datasourceStates": {
            "indexpattern": {
                "layers": {
                    layer_id: {
                        "columnOrder": [
                            "col-time",
                            "col-symbol",
                            "col-vwap",
                            "col-zscore",
                            "col-anomaly",
                            "col-volume",
                        ],
                        "columns": {
                            "col-time": {
                                "dataType": "date",
                                "isBucketed": True,
                                "label": "Time",
                                "operationType": "date_histogram",
                                "params": {"interval": "auto"},
                                "scale": "interval",
                                "sourceField": "window_start",
                            },
                            "col-symbol": {
                                "dataType": "string",
                                "isBucketed": True,
                                "label": "Symbol",
                                "operationType": "terms",
                                "scale": "ordinal",
                                "sourceField": "symbol",
                                "params": {
                                    "size": 50,
                                    "orderBy": {
                                        "type": "column",
                                        "columnId": "col-zscore",
                                    },
                                    "orderDirection": "desc",
                                },
                            },
                            "col-vwap": {
                                "dataType": "number",
                                "isBucketed": False,
                                "label": "VWAP",
                                "operationType": "average",
                                "scale": "ratio",
                                "sourceField": "vwap",
                            },
                            "col-zscore": {
                                "dataType": "number",
                                "isBucketed": False,
                                "label": "Z-Score",
                                "operationType": "average",
                                "scale": "ratio",
                                "sourceField": "zscore",
                            },
                            "col-anomaly": {
                                "dataType": "string",
                                "isBucketed": True,
                                "label": "Anomaly Type",
                                "operationType": "terms",
                                "scale": "ordinal",
                                "sourceField": "anomaly_type",
                                "params": {
                                    "size": 10,
                                    "orderBy": {"type": "alphabetical"},
                                    "orderDirection": "asc",
                                },
                            },
                            "col-volume": {
                                "dataType": "number",
                                "isBucketed": False,
                                "label": "Volume",
                                "operationType": "sum",
                                "scale": "ratio",
                                "sourceField": "total_volume",
                            },
                        },
                        "incompleteColumns": {},
                    }
                }
            }
        },
        "visualization": {
            "columns": [
                {"columnId": "col-time"},
                {"columnId": "col-symbol"},
                {"columnId": "col-vwap"},
                {"columnId": "col-zscore"},
                {"columnId": "col-anomaly"},
                {"columnId": "col-volume"},
            ],
            "layerId": layer_id,
            "layerType": "data",
        },
        "query": {"language": "kuery", "query": "is_anomaly : true"},
        "filters": [],
    }
    return create_lens(IDS["anomaly"], "Anomaly Events", "lnsDatatable", state, [layer_id])


def create_zscore_chart():
    """Z-Score Over Time — area chart split by symbol."""
    print("[5/7] Creating Z-Score Over Time chart ...")
    layer_id = "zscore-layer"
    state = {
        "datasourceStates": {
            "indexpattern": {
                "layers": {
                    layer_id: {
                        "columnOrder": ["col-symbol", "col-time", "col-zscore"],
                        "columns": {
                            "col-time": {
                                "dataType": "date",
                                "isBucketed": True,
                                "label": "Time",
                                "operationType": "date_histogram",
                                "params": {"interval": "auto"},
                                "scale": "interval",
                                "sourceField": "window_start",
                            },
                            "col-zscore": {
                                "dataType": "number",
                                "isBucketed": False,
                                "label": "Average Z-Score",
                                "operationType": "average",
                                "scale": "ratio",
                                "sourceField": "zscore",
                            },
                            "col-symbol": {
                                "dataType": "string",
                                "isBucketed": True,
                                "label": "Symbol",
                                "operationType": "terms",
                                "scale": "ordinal",
                                "sourceField": "symbol",
                                "params": {
                                    "size": 10,
                                    "orderBy": {
                                        "type": "column",
                                        "columnId": "col-zscore",
                                    },
                                    "orderDirection": "desc",
                                },
                            },
                        },
                        "incompleteColumns": {},
                    }
                }
            }
        },
        "visualization": {
            "layers": [
                {
                    "accessors": ["col-zscore"],
                    "layerId": layer_id,
                    "layerType": "data",
                    "seriesType": "area",
                    "xAccessor": "col-time",
                    "splitAccessor": "col-symbol",
                }
            ],
            "legend": {"isVisible": True, "position": "right"},
            "preferredSeriesType": "area",
            "valueLabels": "hide",
        },
        "query": {"language": "kuery", "query": ""},
        "filters": [],
    }
    return create_lens(IDS["zscore"], "Z-Score Over Time by Symbol", "lnsXY", state, [layer_id])


def create_trades_metric():
    """Total Trades — single metric visualization."""
    print("[6/7] Creating Total Trades metric ...")
    layer_id = "trades-layer"
    state = {
        "datasourceStates": {
            "indexpattern": {
                "layers": {
                    layer_id: {
                        "columnOrder": ["col-trades"],
                        "columns": {
                            "col-trades": {
                                "dataType": "number",
                                "isBucketed": False,
                                "label": "Total Trades",
                                "operationType": "sum",
                                "scale": "ratio",
                                "sourceField": "trade_count",
                            }
                        },
                        "incompleteColumns": {},
                    }
                }
            }
        },
        "visualization": {
            "layerId": layer_id,
            "layerType": "data",
            "metricAccessor": "col-trades",
        },
        "query": {"language": "kuery", "query": ""},
        "filters": [],
    }
    return create_lens(IDS["trades"], "Total Trades", "lnsMetric", state, [layer_id])


# =============================================================================
# DASHBOARD
# =============================================================================
def create_dashboard():
    """Create the main dashboard with inline (by-value) Lens panels.

    By-value panels embed the full Lens config in embeddableConfig.attributes,
    avoiding the need for separate Lens saved objects (and their mapping issues).
    """
    print("[2/2] Creating dashboard with inline panels ...")

    vis_configs = [
        ("p1", "VWAP Over Time by Symbol", "lnsXY", _vwap_state(), ["vwap-layer"], {"x": 0, "y": 0, "w": 48, "h": 15}),
        ("p2", "Volume Over Time by Symbol", "lnsXY", _volume_state(), ["volume-layer"], {"x": 0, "y": 15, "w": 24, "h": 15}),
        ("p3", "Anomaly Events", "lnsDatatable", _anomaly_state(), ["anomaly-layer"], {"x": 24, "y": 15, "w": 24, "h": 15}),
        ("p4", "Z-Score Over Time by Symbol", "lnsXY", _zscore_state(), ["zscore-layer"], {"x": 0, "y": 30, "w": 36, "h": 12}),
        ("p5", "Total Trades", "lnsMetric", _trades_state(), ["trades-layer"], {"x": 36, "y": 30, "w": 12, "h": 12}),
    ]

    panels = []
    for panel_id, title, vis_type, state, layer_ids, grid in vis_configs:
        refs = [
            {"type": "index-pattern", "id": DATA_VIEW_ID, "name": f"indexpattern-datasource-layer-{lid}"}
            for lid in layer_ids
        ]
        panels.append({
            "version": "8.11.4",
            "type": "lens",
            "gridData": {**grid, "i": panel_id},
            "panelIndex": panel_id,
            "embeddableConfig": {
                "attributes": {
                    "title": title,
                    "description": "",
                    "visualizationType": vis_type,
                    "state": state,
                    "references": refs,
                },
                "enhancements": {},
            },
            "title": title,
        })

    options = {
        "useMargins": True,
        "syncColors": False,
        "syncCursor": True,
        "syncTooltips": False,
        "hidePanelTitles": False,
    }

    search_source = {
        "query": {"query": "", "language": "kuery"},
        "filter": [],
    }

    body = {
        "attributes": {
            "title": "STREAMSIGHT Real-Time Crypto Analytics",
            "description": "Real-time cryptocurrency analytics: VWAP, volume, Z-score anomaly detection.",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps(options),
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-1h",
            "refreshInterval": {"pause": False, "value": 10000},
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(search_source)
            },
        },
        "references": [],
    }

    result = kibana_request(
        "POST",
        f"/api/saved_objects/dashboard/{IDS['dashboard']}?overwrite=true",
        body,
    )
    if result:
        print("    OK — Dashboard created")
    return result


# =============================================================================
# NDJSON EXPORT (for manual import via Kibana UI)
# =============================================================================
def generate_ndjson(filepath="kibana_dashboard.ndjson"):
    """Generate an NDJSON file that can be imported via Kibana UI.

    Kibana UI path: Stack Management > Saved Objects > Import
    """
    print(f"\nGenerating {filepath} ...")

    objects = []

    # 1. Index pattern / data view
    objects.append({
        "id": DATA_VIEW_ID,
        "type": "index-pattern",
        "attributes": {
            "title": INDEX_PATTERN,
            "timeFieldName": "window_start",
            "name": "STREAMSIGHT Trades",
        },
        "references": [],
        "managed": False,
    })

    # 2-6. All Lens visualizations
    vis_configs = [
        (IDS["vwap"], "VWAP Over Time by Symbol", "lnsXY", _vwap_state(), ["vwap-layer"]),
        (IDS["volume"], "Volume Over Time by Symbol", "lnsXY", _volume_state(), ["volume-layer"]),
        (IDS["anomaly"], "Anomaly Events", "lnsDatatable", _anomaly_state(), ["anomaly-layer"]),
        (IDS["zscore"], "Z-Score Over Time by Symbol", "lnsXY", _zscore_state(), ["zscore-layer"]),
        (IDS["trades"], "Total Trades", "lnsMetric", _trades_state(), ["trades-layer"]),
    ]

    for vis_id, title, vis_type, state, layer_ids in vis_configs:
        refs = [
            {"type": "index-pattern", "id": DATA_VIEW_ID, "name": f"indexpattern-datasource-layer-{lid}"}
            for lid in layer_ids
        ]
        objects.append({
            "id": vis_id,
            "type": "lens",
            "typeMigrationVersion": "8.9.0",
            "attributes": {
                "title": title,
                "description": "",
                "visualizationType": vis_type,
                "state": state,
                "references": refs,
            },
            "references": refs,
        })

    # 7. Dashboard
    panels = [
        {"version": "8.11.4", "type": "lens", "gridData": {"x": 0, "y": 0, "w": 48, "h": 15, "i": "p1"}, "panelIndex": "p1", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p1", "title": "VWAP Over Time by Symbol"},
        {"version": "8.11.4", "type": "lens", "gridData": {"x": 0, "y": 15, "w": 24, "h": 15, "i": "p2"}, "panelIndex": "p2", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p2", "title": "Volume Over Time by Symbol"},
        {"version": "8.11.4", "type": "lens", "gridData": {"x": 24, "y": 15, "w": 24, "h": 15, "i": "p3"}, "panelIndex": "p3", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p3", "title": "Anomaly Events"},
        {"version": "8.11.4", "type": "lens", "gridData": {"x": 0, "y": 30, "w": 36, "h": 12, "i": "p4"}, "panelIndex": "p4", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p4", "title": "Z-Score Over Time by Symbol"},
        {"version": "8.11.4", "type": "lens", "gridData": {"x": 36, "y": 30, "w": 12, "h": 12, "i": "p5"}, "panelIndex": "p5", "embeddableConfig": {"enhancements": {}}, "panelRefName": "panel_p5", "title": "Total Trades"},
    ]
    dash_refs = [
        {"id": IDS["vwap"], "name": "panel_p1", "type": "lens"},
        {"id": IDS["volume"], "name": "panel_p2", "type": "lens"},
        {"id": IDS["anomaly"], "name": "panel_p3", "type": "lens"},
        {"id": IDS["zscore"], "name": "panel_p4", "type": "lens"},
        {"id": IDS["trades"], "name": "panel_p5", "type": "lens"},
    ]
    objects.append({
        "id": IDS["dashboard"],
        "type": "dashboard",
        "attributes": {
            "title": "STREAMSIGHT Real-Time Crypto Analytics",
            "description": "Real-time cryptocurrency analytics: VWAP, volume, Z-score anomaly detection.",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": False, "syncCursor": True, "syncTooltips": False, "hidePanelTitles": False}),
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-1h",
            "refreshInterval": {"pause": False, "value": 10000},
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})
            },
        },
        "references": dash_refs,
    })

    with open(filepath, "w", encoding="utf-8") as f:
        for obj in objects:
            f.write(json.dumps(obj, separators=(",", ":")) + "\n")

    print(f"    Saved to {filepath}")
    print(f"    Manual import: Kibana > Stack Management > Saved Objects > Import")


# -- State builders (reused by both API and NDJSON paths) ---------------------

def _vwap_state():
    lid = "vwap-layer"
    return {
        "datasourceStates": {"indexpattern": {"layers": {lid: {
            "columnOrder": ["col-symbol", "col-time", "col-vwap"],
            "columns": {
                "col-time": {"dataType": "date", "isBucketed": True, "label": "Time", "operationType": "date_histogram", "params": {"interval": "auto"}, "scale": "interval", "sourceField": "window_start"},
                "col-vwap": {"dataType": "number", "isBucketed": False, "label": "Average VWAP", "operationType": "average", "scale": "ratio", "sourceField": "vwap"},
                "col-symbol": {"dataType": "string", "isBucketed": True, "label": "Symbol", "operationType": "terms", "scale": "ordinal", "sourceField": "symbol", "params": {"size": 10, "orderBy": {"type": "column", "columnId": "col-vwap"}, "orderDirection": "desc"}},
            }, "incompleteColumns": {}}}}},
        "visualization": {"layers": [{"accessors": ["col-vwap"], "layerId": lid, "layerType": "data", "seriesType": "line", "xAccessor": "col-time", "splitAccessor": "col-symbol"}], "legend": {"isVisible": True, "position": "right"}, "preferredSeriesType": "line", "valueLabels": "hide"},
        "query": {"language": "kuery", "query": ""}, "filters": [],
    }

def _volume_state():
    lid = "volume-layer"
    return {
        "datasourceStates": {"indexpattern": {"layers": {lid: {
            "columnOrder": ["col-symbol", "col-time", "col-volume"],
            "columns": {
                "col-time": {"dataType": "date", "isBucketed": True, "label": "Time", "operationType": "date_histogram", "params": {"interval": "auto"}, "scale": "interval", "sourceField": "window_start"},
                "col-volume": {"dataType": "number", "isBucketed": False, "label": "Total Volume", "operationType": "sum", "scale": "ratio", "sourceField": "total_volume"},
                "col-symbol": {"dataType": "string", "isBucketed": True, "label": "Symbol", "operationType": "terms", "scale": "ordinal", "sourceField": "symbol", "params": {"size": 10, "orderBy": {"type": "column", "columnId": "col-volume"}, "orderDirection": "desc"}},
            }, "incompleteColumns": {}}}}},
        "visualization": {"layers": [{"accessors": ["col-volume"], "layerId": lid, "layerType": "data", "seriesType": "bar_stacked", "xAccessor": "col-time", "splitAccessor": "col-symbol"}], "legend": {"isVisible": True, "position": "right"}, "preferredSeriesType": "bar_stacked", "valueLabels": "hide"},
        "query": {"language": "kuery", "query": ""}, "filters": [],
    }

def _anomaly_state():
    lid = "anomaly-layer"
    return {
        "datasourceStates": {"indexpattern": {"layers": {lid: {
            "columnOrder": ["col-time", "col-symbol", "col-vwap", "col-zscore", "col-anomaly", "col-volume"],
            "columns": {
                "col-time": {"dataType": "date", "isBucketed": True, "label": "Time", "operationType": "date_histogram", "params": {"interval": "auto"}, "scale": "interval", "sourceField": "window_start"},
                "col-symbol": {"dataType": "string", "isBucketed": True, "label": "Symbol", "operationType": "terms", "scale": "ordinal", "sourceField": "symbol", "params": {"size": 50, "orderBy": {"type": "column", "columnId": "col-zscore"}, "orderDirection": "desc"}},
                "col-vwap": {"dataType": "number", "isBucketed": False, "label": "VWAP", "operationType": "average", "scale": "ratio", "sourceField": "vwap"},
                "col-zscore": {"dataType": "number", "isBucketed": False, "label": "Z-Score", "operationType": "average", "scale": "ratio", "sourceField": "zscore"},
                "col-anomaly": {"dataType": "string", "isBucketed": True, "label": "Anomaly Type", "operationType": "terms", "scale": "ordinal", "sourceField": "anomaly_type", "params": {"size": 10, "orderBy": {"type": "alphabetical"}, "orderDirection": "asc"}},
                "col-volume": {"dataType": "number", "isBucketed": False, "label": "Volume", "operationType": "sum", "scale": "ratio", "sourceField": "total_volume"},
            }, "incompleteColumns": {}}}}},
        "visualization": {"columns": [{"columnId": "col-time"}, {"columnId": "col-symbol"}, {"columnId": "col-vwap"}, {"columnId": "col-zscore"}, {"columnId": "col-anomaly"}, {"columnId": "col-volume"}], "layerId": lid, "layerType": "data"},
        "query": {"language": "kuery", "query": "is_anomaly : true"}, "filters": [],
    }

def _zscore_state():
    lid = "zscore-layer"
    return {
        "datasourceStates": {"indexpattern": {"layers": {lid: {
            "columnOrder": ["col-symbol", "col-time", "col-zscore"],
            "columns": {
                "col-time": {"dataType": "date", "isBucketed": True, "label": "Time", "operationType": "date_histogram", "params": {"interval": "auto"}, "scale": "interval", "sourceField": "window_start"},
                "col-zscore": {"dataType": "number", "isBucketed": False, "label": "Average Z-Score", "operationType": "average", "scale": "ratio", "sourceField": "zscore"},
                "col-symbol": {"dataType": "string", "isBucketed": True, "label": "Symbol", "operationType": "terms", "scale": "ordinal", "sourceField": "symbol", "params": {"size": 10, "orderBy": {"type": "column", "columnId": "col-zscore"}, "orderDirection": "desc"}},
            }, "incompleteColumns": {}}}}},
        "visualization": {"layers": [{"accessors": ["col-zscore"], "layerId": lid, "layerType": "data", "seriesType": "area", "xAccessor": "col-time", "splitAccessor": "col-symbol"}], "legend": {"isVisible": True, "position": "right"}, "preferredSeriesType": "area", "valueLabels": "hide"},
        "query": {"language": "kuery", "query": ""}, "filters": [],
    }

def _trades_state():
    lid = "trades-layer"
    return {
        "datasourceStates": {"indexpattern": {"layers": {lid: {
            "columnOrder": ["col-trades"],
            "columns": {
                "col-trades": {"dataType": "number", "isBucketed": False, "label": "Total Trades", "operationType": "sum", "scale": "ratio", "sourceField": "trade_count"},
            }, "incompleteColumns": {}}}}},
        "visualization": {"layerId": lid, "layerType": "data", "metricAccessor": "col-trades"},
        "query": {"language": "kuery", "query": ""}, "filters": [],
    }


# =============================================================================
# RESET (delete all STREAMSIGHT objects)
# =============================================================================
def reset_all():
    """Delete all STREAMSIGHT saved objects so they can be recreated."""
    print("Resetting existing objects ...")
    delete_saved_object("dashboard", IDS["dashboard"])
    for key in ["vwap", "volume", "anomaly", "zscore", "trades"]:
        delete_saved_object("lens", IDS[key])
    # Delete data view via its own API
    kibana_request("DELETE", f"/api/data_views/data_view/{DATA_VIEW_ID}")
    print("    Done — old objects removed\n")


# =============================================================================
# NDJSON IMPORT (reliable method for Kibana 8.x)
# =============================================================================
def import_ndjson(filepath):
    """Import saved objects from NDJSON file via Kibana import API.
    
    This is more reliable than individual saved object creation because
    the import API handles field serialization properly (avoids the
    lens.state object mapping conflict in .kibana index).
    """
    print(f"\nImporting objects from {os.path.basename(filepath)} ...")

    with open(filepath, "rb") as f:
        file_data = f.read()

    # Build multipart/form-data body manually (no external deps)
    boundary = "----KibanaNDJSONImport"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="kibana_dashboard.ndjson"\r\n'
        f"Content-Type: application/ndjson\r\n"
        f"\r\n"
    ).encode("utf-8") + file_data + f"\r\n--{boundary}--\r\n".encode("utf-8")

    url = f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true"
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("kbn-xsrf", "true")
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            success = result.get("success", False)
            count = result.get("successCount", 0)
            errors = result.get("errors", [])

            if success:
                print(f"    OK — {count} objects imported successfully")
            else:
                print(f"    Partial — {count} objects imported, {len(errors)} errors:")
                for err in errors[:5]:
                    err_id = err.get("id", "?")
                    err_type = err.get("type", "?")
                    err_msg = err.get("error", {}).get("message", str(err.get("error", "")))[:120]
                    print(f"      {err_type}/{err_id}: {err_msg}")
            return result

    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        print(f"    [ERROR] HTTP {e.code}: {error_body[:300]}")
        return None
    except urllib.error.URLError as e:
        print(f"    [ERROR] Connection failed: {e}")
        return None


# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("  STREAMSIGHT — Kibana Dashboard Setup")
    print("=" * 60)

    export_only = "--export-only" in sys.argv
    do_reset = "--reset" in sys.argv

    # Always generate the NDJSON file
    ndjson_path = os.path.join(os.path.dirname(__file__) or ".", "kibana_dashboard.ndjson")
    generate_ndjson(ndjson_path)

    if export_only:
        print("\n--export-only: NDJSON file generated. Skipping API calls.")
        print("Import it in Kibana: Stack Management > Saved Objects > Import")
        return

    # Check Kibana is ready
    if not wait_for_kibana():
        print("\nKibana is not available. You can still import the NDJSON file manually:")
        print("  1. Open http://localhost:5601")
        print("  2. Go to Stack Management > Saved Objects > Import")
        print("  3. Select kibana_dashboard.ndjson")
        sys.exit(1)

    if do_reset:
        reset_all()

    # Create data view (uses its own API)
    create_data_view()

    # Create dashboard with inline (by-value) Lens panels
    # No separate Lens saved objects needed — avoids .kibana mapping issues
    create_dashboard()

    print("\n" + "=" * 60)
    print("  DONE!")
    print("=" * 60)
    print(f"\n  Open your dashboard:")
    print(f"  {KIBANA_URL}/app/dashboards#/view/{IDS['dashboard']}")
    print()
    print("  Quick tips:")
    print("  - Time range: click the calendar icon (top-right) > 'Last 15 minutes'")
    print("  - Auto-refresh: click calendar > 'Refresh every' > 10 seconds")
    print("  - Make sure binance_to_kafka.py + spark_streaming.py are running")
    print("  - First data appears ~15-30s after spark starts (window + watermark)")
    print()


if __name__ == "__main__":
    main()
