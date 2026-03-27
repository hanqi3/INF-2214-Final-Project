"""
dashboard/server.py -- Real-Time Peak Total Revenue Dashboard Server
====================================================================
Consumes sliding-window revenue insights from Kafka and serves a
live dashboard over HTTP.

Unlike a static report, this server is a real Kafka consumer. Window
aggregates and recent raw rideshare events arrive continuously from the
streaming pipeline and are exposed to the browser through a Flask API.

Endpoints
---------
GET /             Dashboard HTML page
GET /api/stats    Aggregated stats plus recent raw rideshare events as JSON

Run:
    python dashboard/server.py
    http://localhost:8050
"""

import json
import os
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from threading import Lock

from flask import Flask, jsonify
from kafka import KafkaConsumer


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INSIGHTS_TOPIC = os.getenv("INSIGHTS_TOPIC", "revenue-insights")
RIDESHARE_TOPIC = os.getenv("RIDESHARE_TOPIC", "rideshare-events")
LATE_EVENTS_TOPIC = os.getenv("LATE_EVENTS_TOPIC", "late-rideshare-events")
MAX_INSIGHTS_IN_MEMORY = 5000
MAX_EVENTS_IN_MEMORY = 120
MAX_LATE_EVENTS_IN_MEMORY = 120


# -----------------------------------------------------------------------------
# Thread-safe in-memory stores
# -----------------------------------------------------------------------------

insight_store = deque(maxlen=MAX_INSIGHTS_IN_MEMORY)
event_store = deque(maxlen=MAX_EVENTS_IN_MEMORY)
late_event_store = deque(maxlen=MAX_LATE_EVENTS_IN_MEMORY)
store_lock = Lock()


# -----------------------------------------------------------------------------
# Formatting helpers
# -----------------------------------------------------------------------------

def time_label(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M")


def window_label(start_ms: int, end_ms: int) -> str:
    return f"{time_label(start_ms)} - {time_label(end_ms)}"


# -----------------------------------------------------------------------------
# Kafka consumer threads
# -----------------------------------------------------------------------------

def consume_topic(topic: str, target_store: deque, group_id: str, label: str):
    """
    Background thread: consumes messages from one Kafka topic and pushes them
    into the corresponding in-memory deque.

    This consumer retries automatically if Kafka is temporarily unavailable
    at startup.
    """
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=group_id,
                auto_offset_reset="earliest",
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                session_timeout_ms=10_000,
                heartbeat_interval_ms=3_000
            )

            print(f"[Dashboard] Connected to {label} topic: {topic}")

            while True:
                for message in consumer:
                    with store_lock:
                        target_store.append(message.value)
                time.sleep(1)

        except Exception as exc:
            print(f"[Dashboard] Kafka consumer error on {label}: {exc}")
            time.sleep(3)


def consume_insights():
    """Consume window-level revenue aggregates from the output topic."""
    consume_topic(
        topic=INSIGHTS_TOPIC,
        target_store=insight_store,
        group_id="rideshare-dashboard-group",
        label="insights",
    )


def consume_events():
    """Consume recent raw rideshare events from the input topic."""
    consume_topic(
        topic=RIDESHARE_TOPIC,
        target_store=event_store,
        group_id="rideshare-dashboard-events-group",
        label="rideshare events",
    )


def consume_late_events():
    """Consume events routed to the late-event side output topic."""
    consume_topic(
        topic=LATE_EVENTS_TOPIC,
        target_store=late_event_store,
        group_id="rideshare-dashboard-late-events-group",
        label="late rideshare events",
    )


# -----------------------------------------------------------------------------
# Statistics aggregation
# -----------------------------------------------------------------------------

def deduplicated_insights() -> list:
    """
    Keep only the latest aggregate per (window_end, zone_id).

    Sliding windows may emit updated results for the same region/window pair
    when late events arrive. We keep only the newest version for display.
    """
    with store_lock:
        records = list(insight_store)

    latest_by_key = {}
    for record in records:
        try:
            key = (int(record["window_end"]), str(record["zone_id"]))
            emitted_at = int(record.get("emitted_at", 0))
        except (KeyError, TypeError, ValueError):
            continue

        previous = latest_by_key.get(key)
        if previous is None or emitted_at >= int(previous.get("emitted_at", 0)):
            latest_by_key[key] = record

    return list(latest_by_key.values())


def recent_events() -> list:
    """
    Return the most recent raw rideshare events for dashboard inspection.

    This gives the UI a small live sample of individual events so the user can
    compare raw stream records with the aggregated window results.
    """
    with store_lock:
        records = list(event_store)

    latest = []
    for record in records[-12:][::-1]:
        try:
            latest.append({
                "zone_id": str(record["zone_id"]),
                "final_fare": round(float(record["final_fare"]), 2),
                "surge_multiplier": round(float(record.get("surge_multiplier", 1.0)), 2),
                "weather": str(record.get("weather", "-")),
                "event_time_label": time_label(int(record["event_time"])),
                "ingest_time_label": time_label(int(record["ingest_time"])),
            })
        except (KeyError, TypeError, ValueError):
            continue

    return latest


def recent_late_events() -> list:
    """
    Return the most recent events that were emitted to the late side output.

    These are useful as a compact proof that watermark + allowed lateness are
    actively filtering and routing overly delayed records.
    """
    with store_lock:
        records = list(late_event_store)

    latest = []
    for record in records[-8:][::-1]:
        try:
            latest.append({
                "zone_id": str(record["zone_id"]),
                "final_fare": round(float(record["final_fare"]), 2),
                "event_time_label": time_label(int(record["event_time"])),
                "ingest_time_label": time_label(int(record["ingest_time"])),
                "late_reason": str(record.get("late_reason", "late")),
            })
        except (KeyError, TypeError, ValueError):
            continue

    return latest


def compute_stats() -> dict:
    """
    Compute aggregated statistics for all dashboard charts and tables.

    Called on every /api/stats request. The browser polls this endpoint every
    2 seconds to keep the UI synchronized with the latest streaming results.
    """
    insights = deduplicated_insights()
    events = recent_events()
    late_events = recent_late_events()
    if not insights:
        return {
            "latest_window_label": "Waiting for data",
            "windows_tracked": 0,
            "current_peak": None,
            "latest_rankings": [],
            "peak_series": [],
            "leader_counts": [],
            "recent_windows": [],
            "recent_events": events,
            "recent_late_events": late_events,
            "late_event_count": len(late_events),
            "updated_at": None,
        }

    by_window = defaultdict(list)
    for insight in insights:
        by_window[int(insight["window_end"])].append(insight)

    ordered_window_ends = sorted(by_window.keys())
    latest_window_end = ordered_window_ends[-1]
    latest_window_records = sorted(
        by_window[latest_window_end],
        key=lambda item: float(item.get("total_revenue", 0)),
        reverse=True,
    )
    current_peak = latest_window_records[0] if latest_window_records else None

    peak_series = []
    peak_leaders = []
    for window_end in ordered_window_ends[-18:]:
        ranked = sorted(
            by_window[window_end],
            key=lambda item: float(item.get("total_revenue", 0)),
            reverse=True,
        )
        if not ranked:
            continue

        top = ranked[0]
        peak_series.append({
            "window_label": window_label(int(top["window_start"]), int(top["window_end"])),
            "zone_id": top["zone_id"],
            "total_revenue": round(float(top["total_revenue"]), 2),
        })
        peak_leaders.append(top["zone_id"])

    leader_counts = [
        {"zone_id": zone, "count": count}
        for zone, count in Counter(peak_leaders).most_common()
    ]

    recent_windows = []
    for window_end in ordered_window_ends[-8:][::-1]:
        ranked = sorted(
            by_window[window_end],
            key=lambda item: float(item.get("total_revenue", 0)),
            reverse=True,
        )
        if ranked:
            top = ranked[0]
            recent_windows.append({
                "window_label": window_label(int(top["window_start"]), int(top["window_end"])),
                "zone_id": top["zone_id"],
                "total_revenue": round(float(top["total_revenue"]), 2),
                "avg_revenue": round(float(top["avg_revenue"]), 2),
                "trip_count": int(top["trip_count"]),
            })

    return {
        "latest_window_label": (
            window_label(int(current_peak["window_start"]), int(current_peak["window_end"]))
            if current_peak else "Waiting for data"
        ),
        "windows_tracked": len(ordered_window_ends),
        "current_peak": current_peak,
        "latest_rankings": latest_window_records[:10],
        "peak_series": peak_series,
        "leader_counts": leader_counts,
        "recent_windows": recent_windows,
        "recent_events": events,
        "recent_late_events": late_events,
        "late_event_count": len(late_events),
        "updated_at": datetime.now(tz=timezone.utc).strftime("%H:%M:%S UTC"),
    }


# -----------------------------------------------------------------------------
# Flask app
# -----------------------------------------------------------------------------

app = Flask(__name__)


@app.route("/")
def index():
    """Serve the dashboard HTML page."""
    dashboard_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(dashboard_path, "r") as f:
        return f.read()


@app.route("/api/stats")
def get_stats():
    """Return aggregated stats and recent events for the dashboard."""
    return jsonify(compute_stats())


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    import threading

    insight_thread = threading.Thread(target=consume_insights, daemon=True)
    event_thread = threading.Thread(target=consume_events, daemon=True)
    late_event_thread = threading.Thread(target=consume_late_events, daemon=True)
    insight_thread.start()
    event_thread.start()
    late_event_thread.start()

    print("=" * 56)
    print("  Peak Total Revenue Dashboard")
    print("  http://localhost:8050")
    print("=" * 56)

    app.run(host="0.0.0.0", port=8050, debug=False)
