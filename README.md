# INF 2214 Final Project, Group 9

## Team Members
- Hanqi Yao
- Kaylee Li
- Xiaoen Hu
- Yihang Liang

## Project Overview
This project simulates a ride-sharing analytics pipeline built with Apache Kafka, PyFlink, and a live dashboard. Synthetic rideshare events are generated into a CSV file, replayed into Kafka in ingest-time order, processed by a PyFlink sliding-window job using event-time semantics, and visualized in a Flask dashboard.

The pipeline focuses on:
- real-time revenue monitoring by region
- event-time windowed analytics
- out-of-order and late-event handling
- live inspection of raw events, aggregated insights, and side-output late events

## Use Case
Ride-sharing systems produce high-volume event streams whose arrival order may not match the time when rides actually happened. Demand changes across regions, peak commuting periods, and weather conditions. This project models that environment so we can study how stream-processing systems behave when data is delayed or arrives out of order.

The main business question is: how can a platform continuously identify high-revenue regions and revenue trends while still handling delayed data correctly?

## Architecture
The current project runs as a multi-container pipeline with Docker Compose.

Flow:
1. `CSVGenerator.py` creates `rideshare_window_agg.csv`
2. `src/data_generator.py` replays that CSV into Kafka topic `rideshare-events`
3. `src/peak_total_revenue.py` consumes the stream with PyFlink
4. Flink computes sliding-window revenue metrics per region
5. Flink writes aggregates to `revenue-insights`
6. Flink writes too-late events to `late-rideshare-events`
7. `dashboard/server.py` consumes Kafka topics and serves the dashboard at `http://localhost:8050`
8. `sample output/Screenshot of the output.png` displays the sample output of the dashboard

Main services in [docker-compose.yml](docker-compose.yml):
- `kafka`: Apache Kafka 3.7 in KRaft mode
- `init-kafka`: creates required topics
- `jobmanager`: Flink JobManager
- `taskmanager`: Flink TaskManager
- `aggregator`: submits the PyFlink job
- `generator`: replays the CSV into Kafka
- `dashboard`: Flask dashboard

## Repository Structure
- [CSVGenerator.py](CSVGenerator.py): synthetic dataset generator
- [rideshare_window_agg.csv](rideshare_window_agg.csv): generated rideshare dataset
- [src/data_generator.py](src/data_generator.py): Kafka replay producer
- [src/peak_total_revenue.py](src/peak_total_revenue.py): PyFlink streaming job
- [dashboard/server.py](dashboard/server.py): dashboard backend and Kafka consumers
- [dashboard/index.html](dashboard/index.html): live dashboard UI
- [Dockerfile.flink](Dockerfile.flink): Flink + Python runtime
- [Dockerfile.generator](Dockerfile.generator): Kafka replay producer image
- [Dockerfile.dashboard](Dockerfile.dashboard): dashboard image

## Dataset
The dataset is synthetically generated to represent ride requests over a 24-hour period.

Each row contains:

| Field | Type | Description |
|---|---|---|
| `request_id` | string | unique ride identifier |
| `zone_id` | string | region where the ride originated |
| `event_time` | timestamp | when the ride request actually occurred |
| `ingest_time` | timestamp | when the event arrives in the stream |
| `final_fare` | float | fare after surge pricing |
| `surge_multiplier` | float | pricing multiplier |
| `weather` | string | simulated weather condition |

The generated CSV is sorted by `ingest_time`, because replay order should mimic arrival order in the streaming system.

### Data Generation Logic
The generator models:
- morning peak demand from 7 AM to 9 AM
- evening peak demand from 5 PM to 7 PM
- lower late-night demand
- regional variability across Toronto-style zones
- weather-driven surge changes
- delayed and out-of-order arrival patterns

### Current Delay Distribution
The current dataset generator uses weighted ingest delays so the stream includes both realistic out-of-order records and a small set of definitely late records:

- `70.0%` of events: delay `0-60` seconds
- `20.0%` of events: delay `60-180` seconds
- `9.8%` of events: delay `180-600` seconds
- `0.2%` of events: delay greater than `3730` seconds

This distribution is intentionally aligned with the current Flink lateness settings so the dashboard can surface late-event behavior more clearly.

## Kafka Topics
The project uses three Kafka topics:

| Topic | Purpose |
|---|---|
| `rideshare-events` | raw rideshare event stream |
| `revenue-insights` | aggregated window outputs from Flink |
| `late-rideshare-events` | side-output stream for events beyond allowed lateness |

These topics are created automatically by the `init-kafka` service.

## Time Semantics
This project uses event time rather than processing time.

- `event_time` determines window assignment
- `ingest_time` determines replay order into Kafka
- replay order is preserved by sorting records by `ingest_time`

In [src/data_generator.py](src/data_generator.py), both timestamps are converted to Unix milliseconds before publishing to Kafka. In [src/peak_total_revenue.py](src/peak_total_revenue.py), Flink extracts `event_time` and uses it for watermarking and sliding windows.

## Windowing and Late Data Handling
The PyFlink job currently uses:
- sliding event-time windows of `1 hour`
- slide interval of `10 minutes`
- bounded out-of-orderness watermark of `1 minute`
- allowed lateness of `30 seconds`

This means:
- mildly delayed events can still contribute to their windows
- slightly later events can still revise an existing window during the allowed-lateness period
- events arriving after the window end plus watermark plus allowed lateness are routed to `late-rideshare-events`

Important note:
Because this is a sliding-window job, an event may be late for one overlapping window and still be valid for another overlapping window. The `late-rideshare-events` topic is intended for records that are beyond the accepted lateness threshold for the relevant window instance.

## Dashboard
The dashboard is served by Flask and consumes Kafka continuously in the background. It displays:
- The top summary cards display the latest completed-window metrics, including Peak Total Revenue, Leading Region, Trip Count, Average Revenue, and the number of Completed Windows currently retained for display.
- Peak Revenue Over Time shows how the highest regional revenue changes across recent completed sliding windows.
- Latest Window Breakdown visualizes how total revenue is distributed across regions in the newest completed window.
- Latest Region Windows lists the latest window results ranked by total revenue, including total revenue, average revenue, trip count, and average surge multiplier for each region.
- Top Regions provides a compact leaderboard of the highest-performing regions in the latest completed window.
- Recent Live Rides shows recently consumed raw rideshare events from the Kafka input topic, including region, fare, surge multiplier, weather, event time, and ingest time.
- Late Events shows records routed to the Flink side-output topic after arriving beyond the watermark and allowed-lateness threshold.

Access points:
- Dashboard: `http://localhost:8050`
- Flink Web UI: `http://localhost:8081`

## Environment Setup
Recommended setup: Docker Desktop with Linux containers enabled.

You do not need to install Kafka or Flink manually if you run the project through Docker Compose.

Requirements:
- Docker Desktop
- Docker Compose

## How to Run
### 1. Generate or regenerate the dataset
If you want a fresh CSV locally, run:

```powershell
python CSVGenerator.py
```

If your machine uses the Windows launcher instead:

```powershell
py CSVGenerator.py
```

### 2. Start the full pipeline
From the project root, run:

```powershell
docker compose up --build
```

This will:
- start Kafka
- create topics
- start the Flink cluster
- submit the PyFlink aggregation job
- replay the CSV into Kafka
- start the dashboard

### 3. Open the interfaces
- Dashboard: `http://localhost:8050`
- Flink UI: `http://localhost:8081`

## Useful Commands
Start only Kafka and topic creation:

```powershell
docker compose up kafka init-kafka
```

Follow Flink job logs:

```powershell
docker compose logs -f aggregator
```

Follow dashboard logs:

```powershell
docker compose logs -f dashboard
```

Stop everything:

```powershell
docker compose down
```
## Sample Output
![Sample Output](sample%20output/Screenshot%20of%20the%20output.png)

Sample output of the real-time dashboard for our PyFlink + Kafka streaming pipeline. It shows the peak total revenue by region in the latest sliding event-time window, recent revenue trends, ranked regional results, raw incoming ride events, and late events captured through Flink side output.

## Summary
This project demonstrates an end-to-end streaming analytics workflow:
- synthetic rideshare data generation
- Kafka-based event replay
- PyFlink event-time sliding-window aggregation
- explicit late-event handling
- live dashboard visualization

It is designed both as a functional demo and as a learning exercise in Kafka, Flink, event-time processing, and late-data behavior.
