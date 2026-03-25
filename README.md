# INF 2214 Final Project, Group 9

## Team Members
Hanqi Yao, Kaylee Li, Xiaoen Hu, Yihang Liang
## Project Overview

### 1. Use Case

### 2. Data Source

The dataset used in this project is **synthetically generated** using a custom Python script (`CSVGenerator.py`) to simulate ride-sharing demand in an urban environment. The data is designed to mimic real-world ride request patterns over a **24-hour period**, enabling controlled experimentation with streaming analytics concepts in Apache Flink.

#### Data Characteristics

- Simulates **thousands of ride request events** over one full day  
- Captures **temporal demand patterns**, including:
  - Morning peak hours (7–9 AM)
  - Evening peak hours (5–7 PM)
  - Reduced demand during late-night hours  
- Models **geographic variation** across multiple predefined zones (e.g., Downtown, Airport, transit hubs, residential areas)  
- Includes **dynamic pricing effects** through surge multipliers influenced by demand and weather conditions  
- Introduces **late and out-of-order events** to reflect real-world streaming conditions  

The dataset is sorted by `ingest_time`, which simulates the order in which events arrive at the streaming system.

#### Schema

The dataset follows the schema below:

| Field              | Type       | Description |
|-------------------|-----------|------------|
| request_id        | string    | Unique identifier for each ride request |
| zone_id           | string    | Geographic zone where the request originates |
| event_time        | timestamp | Time when the ride request actually occurred |
| ingest_time       | timestamp | Time when the event arrives in the system (may be delayed) |
| final_fare        | float     | Total fare after applying surge pricing |
| surge_multiplier  | float     | Dynamic pricing factor reflecting demand and supply conditions |
| weather           | string    | Weather condition at the time of request (clear, rain, snow) |

#### Data Generation Logic

The synthetic data generator incorporates several mechanisms to approximate real-world ride-sharing behavior:

- **Temporal Demand Modeling**  
  Ride request frequency varies by time of day, with higher volumes during commuting hours and lower activity overnight.

- **Geographic Distribution**  
  Demand is distributed unevenly across zones, with higher activity in central business districts, transit hubs, and high-traffic areas.

- **Dynamic Pricing (Surge)**  
  The `surge_multiplier` increases during periods of high demand, peak hours, and adverse weather conditions (e.g., rain or snow), resulting in higher `final_fare` values.

- **Weather Effects**  
  Weather conditions influence both demand and pricing, allowing analysis of how external factors impact ride-sharing systems.

- **Late and Out-of-Order Events**  
  Each event is assigned both an `event_time` and an `ingest_time`. Random delays are introduced so that some events arrive later than others, resulting in out-of-order data. This enables testing of **event-time processing and watermark strategies** in Flink.

#### Purpose in Streaming Pipeline

This synthetic dataset is specifically designed to support **window-based aggregation tasks** in Apache Flink, including:

- Identifying **peak demand hours**  
- Detecting **high-demand zones**  
- Computing **revenue trends using sliding windows**  
- Analyzing the impact of **weather on surge pricing**  

By incorporating realistic temporal patterns and delayed event arrivals, the dataset allows for meaningful evaluation of **stateful stream processing, time semantics, and late data handling**.
### 3. System Design
### 4. Time Semantics
### 5. Late Data Handling

### 6. Environment Setup
### 7. How to Run
### 8. Sample Output
