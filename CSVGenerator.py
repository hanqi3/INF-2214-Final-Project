import csv
import random
import uuid
from datetime import datetime, timedelta

NUM_EVENTS = 2000
OUTPUT_FILE = "rideshare_window_agg.csv"

START = datetime(2026, 3, 1, 0, 0, 0)
END = datetime(2026, 3, 2, 0, 0, 0)

ZONES = [
    "Downtown", "UnionStation", "Waterfront", "Midtown", "NorthYork", 
    "Scarborough", "Etobicoke", "UofT", "Airport", "Yorkdale"
    ]
WEATHER = ["clear", "rain", "snow"]


def rand_time():
    delta = int((END - START).total_seconds())
    return START + timedelta(seconds=random.randint(0, delta))


def demand(hour):
    if 7 <= hour <= 9:
        return 3
    if 17 <= hour <= 19:
        return 4
    if 0 <= hour < 6:
        return 0.5
    return 1.5


def delay_seconds():
    """
    Return an ingest delay using the requested lateness distribution.

    Buckets:
    - 70.0%: 0 to 60 seconds
    - 20.0%: 60 to 180 seconds
    - 9.8%: 180 to 600 seconds
    - 0.2%: greater than 3730 seconds
    """
    bucket = random.random()
    if bucket < 0.70:
        return random.randint(0, 60)
    if bucket < 0.90:
        return random.randint(60, 180)
    if bucket < 0.998:
        return random.randint(180, 600)
    return random.randint(3731, 5400)


def generate():
    rows = []

    for _ in range(NUM_EVENTS):
        t = rand_time()
        zone = random.choice(ZONES)
        weather = random.choices(WEATHER, weights=[0.7, 0.2, 0.1])[0]

        # demand spike logic
        multiplier = demand(t.hour)

        # base demand
        base = random.randint(1, 5)

        # fare
        base_fare = random.uniform(8, 25)

        # weather increases surge
        surge = 1.0
        if weather == "rain":
            surge += 0.3
        elif weather == "snow":
            surge += 0.6

        # peak hour bonus
        if multiplier > 2:
            surge += 0.2

        final_fare = round(base_fare * surge, 2)

        # Replay rows in ingest-time order so the stream includes a controlled
        # mix of on-time, moderately late, and definitely late records.
        ingest = t + timedelta(seconds=delay_seconds())

        rows.append({
            "request_id": str(uuid.uuid4()),
            "zone_id": zone,
            "event_time": t.strftime("%Y-%m-%d %H:%M:%S"),
            "ingest_time": ingest.strftime("%Y-%m-%d %H:%M:%S"),
            "final_fare": final_fare,
            "surge_multiplier": round(surge, 2),
            "weather": weather
        })

    rows.sort(key=lambda x: x["ingest_time"])

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print("Generated dataset:", OUTPUT_FILE)


if __name__ == "__main__":
    random.seed(42)
    generate()
