"""
peak_total_revenue.py -- PyFlink Sliding Window Revenue Aggregation
===================================================================
Consumes rideshare events from Kafka, applies event-time semantics, and
computes total revenue per region over a sliding window.

This job is submitted to a running Flink cluster via:
    flink run --python src/peak_total_revenue.py

Concepts demonstrated
---------------------
- KafkaSource: unbounded stream source for rideshare events
- Event time + bounded out-of-orderness watermarks
- SlidingEventTimeWindows: overlapping hourly windows with 10-minute slides
- allowed_lateness: updates windows when slightly late events arrive
- side output: captures events that arrive after watermark + allowed lateness
- ProcessWindowFunction: computes per-region revenue metrics per window
- KafkaSink: writes revenue insights to "revenue-insights" for the dashboard

Configuration
-------------
KAFKA_BOOTSTRAP         Kafka broker address (default: kafka:9092)
RIDESHARE_TOPIC         Input topic  (default: rideshare-events)
INSIGHTS_TOPIC          Output topic (default: revenue-insights)
LATE_EVENTS_TOPIC       Side output topic (default: late-rideshare-events)
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.window import SlidingEventTimeWindows


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
RIDESHARE_TOPIC = os.getenv("RIDESHARE_TOPIC", "rideshare-events")
INSIGHTS_TOPIC = os.getenv("INSIGHTS_TOPIC", "revenue-insights")
LATE_EVENTS_TOPIC = os.getenv("LATE_EVENTS_TOPIC", "late-rideshare-events")

WINDOW_SIZE_MINUTES = 60
WINDOW_SLIDE_MINUTES = 10
WATERMARK_MINUTES = 1
ALLOWED_LATENESS_SECONDS = 30


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# -----------------------------------------------------------------------------
# SECTION 1: PARSING
# -----------------------------------------------------------------------------

class ParseRideEvent(MapFunction):
    """
    MapFunction: raw Kafka message (JSON string) --> validated JSON string.

    Malformed or incomplete messages are dropped (return None) and filtered
    downstream. This keeps the streaming job resilient to bad input.
    """

    REQUIRED_FIELDS = {
        "request_id",
        "zone_id",
        "event_time",
        "ingest_time",
        "final_fare",
        "surge_multiplier",
        "weather",
    }

    def map(self, raw: str) -> str:
        try:
            record = json.loads(raw.strip())
        except json.JSONDecodeError:
            return None

        if not self.REQUIRED_FIELDS.issubset(record.keys()):
            return None

        try:
            normalized = {
                "request_id": str(record["request_id"]),
                "zone_id": str(record["zone_id"]),
                "event_time": int(record["event_time"]),
                "ingest_time": int(record["ingest_time"]),
                "final_fare": float(record["final_fare"]),
                "surge_multiplier": float(record["surge_multiplier"]),
                "weather": str(record["weather"]),
            }
        except (TypeError, ValueError):
            return None

        return json.dumps(normalized)


# -----------------------------------------------------------------------------
# SECTION 2: WATERMARK STRATEGY
# -----------------------------------------------------------------------------

class RideEventTimestampAssigner(TimestampAssigner):
    """
    Extracts event_time from each rideshare record for Flink's event-time clock.

    Why event time matters
    ----------------------
    In this project, rides are replayed in ingest-time order, but windowing
    should be based on when the ride actually happened. Using event_time lets
    Flink place events into the correct sliding window even if they arrive
    slightly out of order.

    Watermark formula:
        watermark = max(event_time_seen) - 1 minute

    This means:
      - Events arriving up to 1 minute late are still on-time for watermarking
      - Events can still update an existing window for 30 more seconds via
        allowed lateness
      - Windows are emitted when the watermark passes their end boundary
    """

    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            return int(json.loads(value)["event_time"])
        except Exception:
            return record_timestamp


# -----------------------------------------------------------------------------
# SECTION 3: WINDOW AGGREGATION
# -----------------------------------------------------------------------------

class SlidingRevenueWindowFunction(ProcessWindowFunction):
    """
    Compute revenue metrics for one region inside one sliding window.

    Because the stream is keyed by zone_id before windowing, each invocation
    of this function sees only the rides for a single region and a single
    window interval.
    """

    def process(self, key: str, context: ProcessWindowFunction.Context, elements):
        total_revenue = 0.0
        total_surge = 0.0
        trip_count = 0
        weather_mix = defaultdict(int)
        max_event_time = 0

        for elem in elements:
            try:
                record = json.loads(elem)
            except Exception:
                continue

            total_revenue += float(record["final_fare"])
            total_surge += float(record["surge_multiplier"])
            trip_count += 1
            weather_mix[record.get("weather", "unknown")] += 1
            max_event_time = max(max_event_time, int(record["event_time"]))

        if trip_count == 0:
            return []

        window_start = context.window().start
        window_end = context.window().end
        average_revenue = round(total_revenue / trip_count, 2)
        average_surge = round(total_surge / trip_count, 2)

        insight = {
            "metric_type": "region_window_total",
            "zone_id": key,
            "window_start": window_start,
            "window_end": window_end,
            "window_start_iso": to_iso(window_start),
            "window_end_iso": to_iso(window_end),
            "window_duration_minutes": WINDOW_SIZE_MINUTES,
            "slide_minutes": WINDOW_SLIDE_MINUTES,
            "allowed_lateness_seconds": ALLOWED_LATENESS_SECONDS,
            "avg_revenue": average_revenue,
            "total_revenue": round(total_revenue, 2),
            "trip_count": trip_count,
            "avg_surge_multiplier": average_surge,
            "weather_mix": dict(weather_mix),
            "latest_event_time": max_event_time,
            "emitted_at": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        }
        return [json.dumps(insight)]


# -----------------------------------------------------------------------------
# SECTION 4: LATE-EVENT SIDE OUTPUT
# -----------------------------------------------------------------------------

class TagLateRideEvent(MapFunction):
    """
    Normalize a too-late event before writing it to the side-output topic.

    These are events that arrived after the window had already exceeded both
    the watermark bound and the configured allowed lateness.
    """

    def map(self, value: str) -> str:
        try:
            record = json.loads(value)
        except Exception:
            return None

        event_time = int(record.get("event_time", 0))
        ingest_time = int(record.get("ingest_time", 0))

        return json.dumps({
            "metric_type": "late_rideshare_event",
            "late_reason": "beyond_allowed_lateness",
            "request_id": str(record.get("request_id", "")),
            "zone_id": str(record.get("zone_id", "")),
            "event_time": event_time,
            "event_time_iso": to_iso(event_time),
            "ingest_time": ingest_time,
            "ingest_time_iso": to_iso(ingest_time),
            "final_fare": round(float(record.get("final_fare", 0.0)), 2),
            "surge_multiplier": round(float(record.get("surge_multiplier", 1.0)), 2),
            "weather": str(record.get("weather", "unknown")),
            "watermark_minutes": WATERMARK_MINUTES,
            "allowed_lateness_seconds": ALLOWED_LATENESS_SECONDS,
            "emitted_at": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        })


# -----------------------------------------------------------------------------
# SECTION 5: PIPELINE ASSEMBLY
# -----------------------------------------------------------------------------

def build_pipeline():
    """
    Build and execute the complete PyFlink streaming pipeline.

    Stages:
      1. Read raw rideshare events from Kafka
      2. Parse and validate JSON
      3. Assign event timestamps and watermarks
      4. Key the stream by region
      5. Apply sliding event-time windows with allowed lateness
      6. Emit aggregated window insights back to Kafka
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(RIDESHARE_TOPIC)
        .set_group_id("rideshare-sliding-window-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "KafkaSource[rideshare-events]",
    )

    parsed_stream = (
        raw_stream
        .map(ParseRideEvent(), output_type=Types.STRING())
        .filter(lambda value: value is not None)
    )

    # Allow bounded out-of-orderness in the replayed event stream.
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(WATERMARK_MINUTES))
        .with_timestamp_assigner(RideEventTimestampAssigner())
    )

    timed_stream = parsed_stream.assign_timestamps_and_watermarks(watermark_strategy)

    keyed_stream = timed_stream.key_by(
        lambda value: json.loads(value)["zone_id"],
        key_type=Types.STRING(),
    )

    late_output_tag = OutputTag("late-rideshare-events", Types.STRING())

    windowed_stream = (
        keyed_stream
        .window(
            SlidingEventTimeWindows.of(
                Time.minutes(WINDOW_SIZE_MINUTES),
                Time.minutes(WINDOW_SLIDE_MINUTES),
            )
        )
        # Late events can still revise an already-created window for a short period.
        .allowed_lateness(Time.seconds(ALLOWED_LATENESS_SECONDS).to_milliseconds())
        .side_output_late_data(late_output_tag)
    )

    revenue_insights = (
        windowed_stream
        .process(SlidingRevenueWindowFunction(), output_type=Types.STRING())
    )

    late_events = (
        revenue_insights
        .get_side_output(late_output_tag)
        .map(TagLateRideEvent(), output_type=Types.STRING())
        .filter(lambda value: value is not None)
    )

    insight_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(INSIGHTS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    late_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(LATE_EVENTS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    revenue_insights.sink_to(insight_sink)
    late_events.sink_to(late_sink)
    revenue_insights.print()
    late_events.print()

    print("=" * 60)
    print("  Peak Total Revenue by Region (Sliding Window)")
    print("=" * 60)
    print(f"Kafka source: {KAFKA_BOOTSTRAP} / {RIDESHARE_TOPIC}")
    print(f"Kafka sink:   {KAFKA_BOOTSTRAP} / {INSIGHTS_TOPIC}")
    print(f"Late sink:    {KAFKA_BOOTSTRAP} / {LATE_EVENTS_TOPIC}")
    print(f"Window:       {WINDOW_SIZE_MINUTES} minutes")
    print(f"Slide:        {WINDOW_SLIDE_MINUTES} minutes")
    print(f"Watermark:    {WATERMARK_MINUTES} minutes")
    print(f"Lateness:     {ALLOWED_LATENESS_SECONDS} seconds")

    env.execute("Peak Total Revenue by Region")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    build_pipeline()
