"""
data_generator.py -- Rideshare CSV Replay Stream (Kafka Producer)
=================================================================
Reads the pre-generated rideshare CSV dataset and replays it as a
Kafka event stream on the topic "rideshare-events".

Each message is a JSON-encoded rideshare event. The PyFlink revenue
aggregator reads from this topic via KafkaSource and computes sliding-
window revenue metrics in real time.


Why replay from CSV?
    - The dataset already contains both event_time and ingest_time
    - That lets us simulate delayed / out-of-order arrival
    - Flink can then apply event time + watermark correctly

Usage:
    python src/data_generator.py
    python src/data_generator.py --csv /app/rideshare_window_agg.csv --topic rideshare-events --speedup 360
"""

import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC           = os.getenv("RIDESHARE_TOPIC", "rideshare-events")
DEFAULT_CSV     = os.getenv("RIDESHARE_CSV", "/app/rideshare_window_agg.csv")
REPLAY_SPEEDUP  = float(os.getenv("REPLAY_SPEEDUP", "360"))


# -----------------------------------------------------------------------------
# Timestamp Helpers
# -----------------------------------------------------------------------------

def parse_ts(ts: str) -> int:
    """Convert YYYY-mm-dd HH:MM:SS into Unix milliseconds."""
    dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# -----------------------------------------------------------------------------
# CSV Loader
# -----------------------------------------------------------------------------

def load_rows(csv_path: str) -> list:
    """
    Load the rideshare dataset from CSV and enrich each row for replay.

    - event_time and ingest_time are converted to Unix milliseconds
    - numeric fields are cast to floats
    - rows are sorted by ingest_time so Kafka replay follows arrival order
    """
    rows = []
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["event_time_ms"] = parse_ts(row["event_time"])
            row["ingest_time_ms"] = parse_ts(row["ingest_time"])
            row["final_fare"] = round(float(row["final_fare"]), 2)
            row["surge_multiplier"] = round(float(row["surge_multiplier"]), 2)
            rows.append(row)

    rows.sort(key=lambda r: r["ingest_time_ms"])
    return rows


# -----------------------------------------------------------------------------
# Kafka Producer
# -----------------------------------------------------------------------------

def connect_producer(bootstrap: str, retries: int = 15) -> KafkaProducer:
    """
    Connect to Kafka with retries.
    Waits up to retries * 3 seconds for the broker to be available.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            if not producer.bootstrap_connected():
                raise NoBrokersAvailable()
            print(f"[Generator] Connected to Kafka at {bootstrap}")
            return producer
        except NoBrokersAvailable:
            print(f"[Generator] Kafka not ready, retrying ({attempt}/{retries})...")
            time.sleep(3)

    raise RuntimeError(
        f"Could not connect to Kafka at {bootstrap} after {retries} attempts"
    )


# -----------------------------------------------------------------------------
# Main Replay Loop
# -----------------------------------------------------------------------------

def run(csv_path: str = DEFAULT_CSV,
        topic: str = TOPIC,
        speedup: float = REPLAY_SPEEDUP):
    """
    Replay rideshare CSV rows to Kafka once.

    Replay behavior:
    - rows are published in ingest_time order
    - the original ingest-time gaps are compressed by the speedup factor
    - timestamps are shifted forward so the replayed stream looks current

    Each rideshare record becomes one Kafka message on the "rideshare-events"
    topic. Flink consumes these messages via KafkaSource as an unbounded stream.
    """
    rows = load_rows(csv_path)
    producer = connect_producer(KAFKA_BOOTSTRAP)

    min_event_ms = min(row["event_time_ms"] for row in rows)
    replay_shift_ms = int(time.time() * 1000) - min_event_ms
    total_sent = 0

    print(f"[Generator] Publishing to topic '{topic}' from CSV '{csv_path}'")
    print(f"[Generator] Replay speed: {speedup}x")
    print("[Generator] Press Ctrl+C to stop\n")

    try:
        previous_ingest_ms = None
        print("[Generator] Starting replay")

        for index, row in enumerate(rows, start=1):
            if previous_ingest_ms is not None:
                delta_ms = row["ingest_time_ms"] - previous_ingest_ms
                sleep_seconds = max(0.0, min(delta_ms / 1000.0 / speedup, 1.5))
                time.sleep(sleep_seconds)

            event = {
                "request_id": row["request_id"],
                "zone_id": row["zone_id"],
                "event_time": row["event_time_ms"] + replay_shift_ms,
                "ingest_time": row["ingest_time_ms"] + replay_shift_ms,
                "final_fare": row["final_fare"],
                "surge_multiplier": row["surge_multiplier"],
                "weather": row["weather"],
                "source_event_time": row["event_time"],
                "source_ingest_time": row["ingest_time"],
            }

            producer.send(topic, value=event)
            previous_ingest_ms = row["ingest_time_ms"]
            total_sent += 1

            if index % 200 == 0:
                print(
                    f"  [{datetime.now().strftime('%H:%M:%S')}] "
                    f"Sent {index} events, {total_sent:,} total"
                )

        producer.flush()
        print("[Generator] Replay completed\n")

    except KeyboardInterrupt:
        print(f"\n[Generator] Stopped. Total published: {total_sent:,}")
        producer.flush()
    finally:
        producer.close()


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka rideshare CSV replay generator")
    parser.add_argument("--csv", default=DEFAULT_CSV)
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--speedup", type=float, default=REPLAY_SPEEDUP)
    args = parser.parse_args()

    KAFKA_BOOTSTRAP = args.bootstrap
    run(args.csv, args.topic, args.speedup)
