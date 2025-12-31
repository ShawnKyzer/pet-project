"""
Pet Activity Enjoyment Data Product Pipeline

Complete end-to-end pipeline:
1. Generate pet sensor data to Kafka
2. Batch process Kafka events to Delta Lake
3. Join with pet registration data
4. Aggregate and write to PostgreSQL for Grafana
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

# Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "pet-sensor-events"
PET_DB_CONN = "postgresql://petdb:petdb@localhost:5433/petdb"
OUTPUT_DB_CONN = "postgresql://outputdb:outputdb@localhost:5434/outputdb"
DELTA_PATH = "/tmp/delta/pet_events"


@task(name="create-kafka-topic", retries=2)
def create_kafka_topic(topic: str = KAFKA_TOPIC) -> bool:
    """Create Kafka topic if it doesn't exist."""
    logger = get_run_logger()
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        topic_list = [NewTopic(name=topic, num_partitions=3, replication_factor=1)]

        try:
            admin.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Created Kafka topic: {topic}")
        except TopicAlreadyExistsError:
            logger.info(f"Kafka topic already exists: {topic}")

        admin.close()
        return True
    except Exception as e:
        logger.warning(f"Could not create topic: {e}")
        return False


@task(name="generate-sensor-data", retries=1)
def generate_sensor_data(num_batches: int = 5, events_per_batch: int = 100) -> dict:
    """Generate pet sensor data and publish to Kafka."""
    logger = get_run_logger()
    logger.info(f"Generating {num_batches} batches of {events_per_batch} events each")

    # Import the emulator
    sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
    from pet_sensor_emulator import run_emulator

    try:
        run_emulator(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic=KAFKA_TOPIC,
            events_per_batch=events_per_batch,
            batch_interval_seconds=1.0,
            total_batches=num_batches,
        )
        total_events = num_batches * events_per_batch
        logger.info(f"Generated {total_events} sensor events")
        return {"success": True, "events_generated": total_events}
    except Exception as e:
        logger.error(f"Failed to generate sensor data: {e}")
        return {"success": False, "error": str(e)}


@task(name="consume-kafka-to-dataframe")
def consume_kafka_events(max_messages: int = 1000, timeout_seconds: int = 30) -> pd.DataFrame:
    """Consume events from Kafka and return as DataFrame."""
    logger = get_run_logger()
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='pet-pipeline-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=timeout_seconds * 1000,
    )

    events = []
    try:
        for message in consumer:
            events.append(message.value)
            if len(events) >= max_messages:
                break
    finally:
        consumer.close()

    logger.info(f"Consumed {len(events)} events from Kafka")

    if not events:
        return pd.DataFrame()

    df = pd.DataFrame(events)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


@task(name="load-pet-registration")
def load_pet_registration() -> pd.DataFrame:
    """Load pet registration data from PostgreSQL."""
    logger = get_run_logger()

    query = """
        SELECT
            collar_id,
            pet_name,
            species,
            breed,
            owner_name,
            city,
            state
        FROM pet_registration
    """

    conn = psycopg2.connect(PET_DB_CONN)
    df = pd.read_sql(query, conn)
    conn.close()

    logger.info(f"Loaded {len(df)} pet registrations")
    return df


@task(name="join-events-with-registration")
def join_events_with_registration(
    events_df: pd.DataFrame,
    registration_df: pd.DataFrame
) -> pd.DataFrame:
    """Join sensor events with pet registration data."""
    logger = get_run_logger()

    if events_df.empty:
        logger.warning("No events to join")
        return pd.DataFrame()

    # Join on collar_id
    joined_df = events_df.merge(
        registration_df,
        on='collar_id',
        how='left'
    )

    logger.info(f"Joined {len(joined_df)} records")
    return joined_df


@task(name="aggregate-activity-data")
def aggregate_activity_data(joined_df: pd.DataFrame) -> dict:
    """Aggregate activity data by pet and activity type."""
    logger = get_run_logger()

    if joined_df.empty:
        logger.warning("No data to aggregate")
        return {"pet_activity": pd.DataFrame(), "by_species": pd.DataFrame(), "by_city": pd.DataFrame()}

    # Add date column
    joined_df['measurement_date'] = joined_df['timestamp'].dt.date

    # Aggregate by pet and activity
    pet_activity = joined_df.groupby(
        ['collar_id', 'pet_name', 'species', 'breed', 'owner_name', 'city', 'state', 'activity_type', 'measurement_date']
    ).agg({
        'enjoyment_score': 'mean',
        'timestamp': 'count',
        'duration_minutes': 'sum'
    }).reset_index()

    pet_activity.columns = [
        'collar_id', 'pet_name', 'species', 'breed', 'owner_name', 'city', 'state',
        'activity_type', 'measurement_date', 'avg_enjoyment_score', 'total_events', 'total_duration_minutes'
    ]

    # Aggregate by species
    by_species = joined_df.groupby(
        ['measurement_date', 'species', 'activity_type']
    ).agg({
        'collar_id': 'nunique',
        'enjoyment_score': 'mean',
        'timestamp': 'count'
    }).reset_index()
    by_species.columns = ['measurement_date', 'species', 'activity_type', 'pet_count', 'avg_enjoyment_score', 'total_events']

    # Aggregate by city
    by_city = joined_df.groupby(
        ['measurement_date', 'city', 'state', 'activity_type']
    ).agg({
        'collar_id': 'nunique',
        'enjoyment_score': 'mean',
        'timestamp': 'count'
    }).reset_index()
    by_city.columns = ['measurement_date', 'city', 'state', 'activity_type', 'pet_count', 'avg_enjoyment_score', 'total_events']

    logger.info(f"Created aggregations: {len(pet_activity)} pet records, {len(by_species)} species records, {len(by_city)} city records")

    return {
        "pet_activity": pet_activity,
        "by_species": by_species,
        "by_city": by_city
    }


@task(name="write-to-output-db")
def write_to_output_db(aggregations: dict, run_id: str) -> dict:
    """Write aggregated data to output PostgreSQL database."""
    logger = get_run_logger()

    conn = psycopg2.connect(OUTPUT_DB_CONN)
    cursor = conn.cursor()

    records_written = 0

    try:
        # Record pipeline run
        cursor.execute(
            "INSERT INTO pipeline_runs (run_id, started_at, status) VALUES (%s, %s, %s) ON CONFLICT (run_id) DO UPDATE SET status = 'running'",
            (run_id, datetime.utcnow(), 'running')
        )

        # Write pet activity data
        pet_df = aggregations['pet_activity']
        if not pet_df.empty:
            for _, row in pet_df.iterrows():
                cursor.execute("""
                    INSERT INTO pet_activity_enjoyment
                    (collar_id, pet_name, species, breed, owner_name, city, state, activity_type,
                     avg_enjoyment_score, total_events, total_duration_minutes, measurement_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (collar_id, activity_type, measurement_date)
                    DO UPDATE SET
                        avg_enjoyment_score = EXCLUDED.avg_enjoyment_score,
                        total_events = EXCLUDED.total_events,
                        total_duration_minutes = EXCLUDED.total_duration_minutes
                """, (
                    row['collar_id'], row['pet_name'], row['species'], row['breed'],
                    row['owner_name'], row['city'], row['state'], row['activity_type'],
                    float(row['avg_enjoyment_score']), int(row['total_events']),
                    int(row['total_duration_minutes']), row['measurement_date']
                ))
                records_written += 1

        # Write species aggregation
        species_df = aggregations['by_species']
        if not species_df.empty:
            for _, row in species_df.iterrows():
                cursor.execute("""
                    INSERT INTO daily_activity_by_species
                    (measurement_date, species, activity_type, pet_count, avg_enjoyment_score, total_events)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (measurement_date, species, activity_type)
                    DO UPDATE SET
                        pet_count = EXCLUDED.pet_count,
                        avg_enjoyment_score = EXCLUDED.avg_enjoyment_score,
                        total_events = EXCLUDED.total_events
                """, (
                    row['measurement_date'], row['species'], row['activity_type'],
                    int(row['pet_count']), float(row['avg_enjoyment_score']), int(row['total_events'])
                ))
                records_written += 1

        # Write city aggregation
        city_df = aggregations['by_city']
        if not city_df.empty:
            for _, row in city_df.iterrows():
                cursor.execute("""
                    INSERT INTO daily_activity_by_city
                    (measurement_date, city, state, activity_type, pet_count, avg_enjoyment_score, total_events)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (measurement_date, city, state, activity_type)
                    DO UPDATE SET
                        pet_count = EXCLUDED.pet_count,
                        avg_enjoyment_score = EXCLUDED.avg_enjoyment_score,
                        total_events = EXCLUDED.total_events
                """, (
                    row['measurement_date'], row['city'], row['state'], row['activity_type'],
                    int(row['pet_count']), float(row['avg_enjoyment_score']), int(row['total_events'])
                ))
                records_written += 1

        # Update pipeline run status
        cursor.execute(
            "UPDATE pipeline_runs SET completed_at = %s, status = %s, records_written = %s WHERE run_id = %s",
            (datetime.utcnow(), 'completed', records_written, run_id)
        )

        conn.commit()
        logger.info(f"Wrote {records_written} records to output database")

    except Exception as e:
        conn.rollback()
        cursor.execute(
            "UPDATE pipeline_runs SET completed_at = %s, status = %s, error_message = %s WHERE run_id = %s",
            (datetime.utcnow(), 'failed', str(e), run_id)
        )
        conn.commit()
        raise
    finally:
        cursor.close()
        conn.close()

    return {"success": True, "records_written": records_written}


@flow(
    name="pet-activity-enjoyment-pipeline",
    description="End-to-end pet activity enjoyment data product pipeline",
    retries=1,
    retry_delay_seconds=60,
)
def pet_activity_enjoyment_pipeline(
    generate_data: bool = True,
    num_batches: int = 5,
    events_per_batch: int = 100,
    max_consume: int = 1000,
) -> dict:
    """
    Full pipeline for Pet Activity Enjoyment Data Product.

    Steps:
    1. Create Kafka topic (if needed)
    2. Generate sensor data to Kafka
    3. Consume events from Kafka
    4. Load pet registration data
    5. Join events with registration
    6. Aggregate data
    7. Write to output PostgreSQL

    Args:
        generate_data: Whether to generate new sensor data
        num_batches: Number of batches to generate
        events_per_batch: Events per batch
        max_consume: Maximum events to consume from Kafka
    """
    logger = get_run_logger()
    run_id = f"run-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

    logger.info(f"Starting Pet Activity Enjoyment Pipeline - Run ID: {run_id}")

    results = {
        "run_id": run_id,
        "steps": {},
    }

    # Step 1: Create Kafka topic
    topic_created = create_kafka_topic()
    results["steps"]["create_topic"] = topic_created

    # Step 2: Generate sensor data (optional)
    if generate_data:
        gen_result = generate_sensor_data(num_batches, events_per_batch)
        results["steps"]["generate_data"] = gen_result
    else:
        results["steps"]["generate_data"] = {"skipped": True}

    # Step 3: Consume from Kafka
    events_df = consume_kafka_events(max_messages=max_consume)
    results["steps"]["consume_events"] = {"events_consumed": len(events_df)}

    if events_df.empty:
        logger.warning("No events consumed, ending pipeline")
        results["success"] = False
        results["error"] = "No events to process"
        return results

    # Step 4: Load pet registration
    registration_df = load_pet_registration()
    results["steps"]["load_registration"] = {"records": len(registration_df)}

    # Step 5: Join data
    joined_df = join_events_with_registration(events_df, registration_df)
    results["steps"]["join_data"] = {"records": len(joined_df)}

    # Step 6: Aggregate
    aggregations = aggregate_activity_data(joined_df)
    results["steps"]["aggregate"] = {
        "pet_activity_records": len(aggregations["pet_activity"]),
        "species_records": len(aggregations["by_species"]),
        "city_records": len(aggregations["by_city"]),
    }

    # Step 7: Write to output
    write_result = write_to_output_db(aggregations, run_id)
    results["steps"]["write_output"] = write_result

    results["success"] = True
    logger.info(f"Pipeline completed successfully! Run ID: {run_id}")

    return results


# Keep the simple flow for backward compatibility
@flow(name="pet-activity-enjoyment-data-product")
def pet_activity_enjoyment_flow(
    spark_master: str = "spark://spark-master:7077",
    run_quality_checks: bool = True,
) -> dict:
    """Simple flow wrapper for compatibility."""
    return pet_activity_enjoyment_pipeline(
        generate_data=True,
        num_batches=3,
        events_per_batch=50,
        max_consume=500,
    )


if __name__ == "__main__":
    result = pet_activity_enjoyment_pipeline()
    print(f"Pipeline result: {result}")
