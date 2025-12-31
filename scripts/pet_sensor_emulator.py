#!/usr/bin/env python3
"""
Pet Sensor Collar Emulator

Simulates pet sensor data from smart collars and publishes to Kafka.
Each collar sends activity and enjoyment data based on what the pet is doing.
"""

import json
import random
import time
from datetime import datetime, timedelta
from typing import Generator

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Pet collar IDs (matching the database)
COLLAR_IDS = [f"COLLAR-{str(i).zfill(3)}" for i in range(1, 16)]

# Activity types pets can engage in
ACTIVITIES = [
    {"type": "sleeping", "base_enjoyment": 70, "variance": 10},
    {"type": "eating", "base_enjoyment": 85, "variance": 10},
    {"type": "playing", "base_enjoyment": 95, "variance": 5},
    {"type": "walking", "base_enjoyment": 88, "variance": 12},
    {"type": "running", "base_enjoyment": 92, "variance": 8},
    {"type": "resting", "base_enjoyment": 75, "variance": 15},
    {"type": "grooming", "base_enjoyment": 60, "variance": 20},
    {"type": "socializing", "base_enjoyment": 90, "variance": 10},
    {"type": "exploring", "base_enjoyment": 85, "variance": 15},
    {"type": "training", "base_enjoyment": 78, "variance": 18},
]


def generate_sensor_event(collar_id: str) -> dict:
    """Generate a single sensor event for a pet collar."""
    activity = random.choice(ACTIVITIES)

    # Calculate enjoyment score with some randomness
    enjoyment = activity["base_enjoyment"] + random.uniform(
        -activity["variance"], activity["variance"]
    )
    enjoyment = max(0, min(100, enjoyment))  # Clamp to 0-100

    # Generate additional sensor metrics
    heart_rate = random.randint(60, 180)  # BPM varies by activity
    if activity["type"] in ["running", "playing"]:
        heart_rate = random.randint(120, 180)
    elif activity["type"] in ["sleeping", "resting"]:
        heart_rate = random.randint(50, 80)

    # Temperature (body temp in Celsius)
    temperature = round(random.uniform(37.5, 39.5), 1)

    # Activity intensity (0-100)
    intensity = random.randint(10, 100)
    if activity["type"] in ["sleeping", "resting"]:
        intensity = random.randint(0, 20)
    elif activity["type"] in ["running", "playing"]:
        intensity = random.randint(70, 100)

    # Duration of current activity in minutes
    duration_minutes = random.randint(5, 120)

    return {
        "collar_id": collar_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "activity_type": activity["type"],
        "enjoyment_score": round(enjoyment, 2),
        "heart_rate_bpm": heart_rate,
        "body_temp_celsius": temperature,
        "activity_intensity": intensity,
        "duration_minutes": duration_minutes,
        "battery_level": random.randint(20, 100),
        "gps_latitude": round(random.uniform(37.0, 48.0), 6),
        "gps_longitude": round(random.uniform(-122.0, -74.0), 6),
    }


def generate_batch_events(num_events: int = 100) -> Generator[dict, None, None]:
    """Generate a batch of sensor events."""
    for _ in range(num_events):
        collar_id = random.choice(COLLAR_IDS)
        yield generate_sensor_event(collar_id)


def create_kafka_producer(
    bootstrap_servers: str = "localhost:9092",
    max_retries: int = 5,
    retry_delay: int = 5
) -> KafkaProducer:
    """Create a Kafka producer with retry logic."""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            print(f"Connected to Kafka at {bootstrap_servers}")
            return producer
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"Kafka not available, retrying in {retry_delay}s... ({attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                raise


def run_emulator(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "pet-sensor-events",
    events_per_batch: int = 50,
    batch_interval_seconds: float = 5.0,
    total_batches: int = None,  # None = run forever
):
    """
    Run the pet sensor emulator.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic: Kafka topic to publish to
        events_per_batch: Number of events to generate per batch
        batch_interval_seconds: Time between batches
        total_batches: Number of batches to generate (None = infinite)
    """
    print(f"Starting Pet Sensor Emulator")
    print(f"  Topic: {topic}")
    print(f"  Events per batch: {events_per_batch}")
    print(f"  Batch interval: {batch_interval_seconds}s")
    print(f"  Total batches: {'infinite' if total_batches is None else total_batches}")
    print()

    producer = create_kafka_producer(bootstrap_servers)

    batch_count = 0
    total_events = 0

    try:
        while total_batches is None or batch_count < total_batches:
            batch_count += 1
            batch_events = 0

            for event in generate_batch_events(events_per_batch):
                producer.send(
                    topic,
                    key=event["collar_id"],
                    value=event
                )
                batch_events += 1
                total_events += 1

            producer.flush()

            print(f"Batch {batch_count}: Sent {batch_events} events "
                  f"(Total: {total_events}) - {datetime.now().strftime('%H:%M:%S')}")

            if total_batches is None or batch_count < total_batches:
                time.sleep(batch_interval_seconds)

    except KeyboardInterrupt:
        print(f"\nStopping emulator. Total events sent: {total_events}")
    finally:
        producer.close()


def generate_historical_data(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "pet-sensor-events",
    days_back: int = 7,
    events_per_day: int = 1000,
):
    """Generate historical data for testing."""
    print(f"Generating {days_back} days of historical data...")

    producer = create_kafka_producer(bootstrap_servers)
    total_events = 0

    try:
        for day_offset in range(days_back, 0, -1):
            target_date = datetime.utcnow() - timedelta(days=day_offset)

            for _ in range(events_per_day):
                collar_id = random.choice(COLLAR_IDS)
                event = generate_sensor_event(collar_id)

                # Adjust timestamp to historical date
                random_hour = random.randint(0, 23)
                random_minute = random.randint(0, 59)
                event_time = target_date.replace(
                    hour=random_hour, minute=random_minute, second=0
                )
                event["timestamp"] = event_time.isoformat() + "Z"

                producer.send(topic, key=collar_id, value=event)
                total_events += 1

            producer.flush()
            print(f"  Generated {events_per_day} events for {target_date.date()}")

        print(f"Done! Total historical events: {total_events}")

    finally:
        producer.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pet Sensor Emulator")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--topic",
        default="pet-sensor-events",
        help="Kafka topic name"
    )
    parser.add_argument(
        "--events-per-batch",
        type=int,
        default=50,
        help="Events per batch"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds between batches"
    )
    parser.add_argument(
        "--batches",
        type=int,
        default=None,
        help="Number of batches (default: infinite)"
    )
    parser.add_argument(
        "--historical",
        action="store_true",
        help="Generate historical data instead of real-time"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Days of historical data to generate"
    )

    args = parser.parse_args()

    if args.historical:
        generate_historical_data(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            days_back=args.days,
        )
    else:
        run_emulator(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            events_per_batch=args.events_per_batch,
            batch_interval_seconds=args.interval,
            total_batches=args.batches,
        )
