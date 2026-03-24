import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from logger import get_logger
from models.seismic import earthquake_deserializer, ts_to_str

load_dotenv()

SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_SEISMIC", "earthquake-feeds")
GROUP_ID = "earthquakes"

POSTGRES_HOST = os.getenv("SUPABASE_HOST", "localhost")
POSTGRES_PORT = os.getenv("SUPABASE_PORT", "5432")
POSTGRES_USER = os.getenv("SUPABASE_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("SUPABASE_PASSWORD", "postgres")
POSTGRES_DATABASE = os.getenv("SUPABASE_DATABASE", "postgres")

log = get_logger(__name__)


def get_connection():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DATABASE,
        sslmode="require",
    )
    conn.autocommit = True
    return conn


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[SERVER],
        auto_offset_reset="earliest",
        group_id=GROUP_ID,
        value_deserializer=earthquake_deserializer,
    )


def insert(cursor, earthquake):
    # Create in Supabase before running:
    # CREATE TABLE seismics (
    #     id SERIAL PRIMARY KEY,
    #     mag DOUBLE PRECISION,
    #     mag_type TEXT,
    #     place TEXT,
    #     tsunami INTEGER,
    #     event_time TIMESTAMP,
    #     event_type TEXT,
    #     title TEXT,
    #     sig INTEGER,
    #     lat DOUBLE PRECISION,
    #     lon DOUBLE PRECISION,
    #     depth DOUBLE PRECISION
    # );

    event_time = datetime.fromtimestamp(earthquake.event_time / 1000)
    cursor.execute(
        """INSERT INTO SEISMICS
           (mag, mag_type, place,
           tsunami, event_time, event_type,
           title, sig, lat,
           lon, depth)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            earthquake.mag,
            earthquake.mag_type,
            earthquake.place,
            earthquake.tsunami,
            event_time,
            earthquake.event_type,
            earthquake.title,
            earthquake.sig,
            earthquake.lat,
            earthquake.lon,
            earthquake.depth,
        ),
    )
    return


def run():
    log.info(f"connecting to topic: {TOPIC_NAME}")

    conn = get_connection()
    cur = conn.cursor()

    consumer = build_consumer()
    count = 0

    try:
        for message in consumer:
            earthquake = message.value

            if earthquake is None:
                log.warning(f"skipping malformed message at offset {message.offset}")
                continue

            insert(cur, earthquake)

            count += 1
            log.info(
                f"""[{count}] {ts_to_str(earthquake.event_time)} |
                {earthquake.place} mag={earthquake.mag}"""
            )

    except KafkaError as e:
        log.error(f"kafka error: {e}")
    except KeyboardInterrupt:
        log.info("stopped by user")
    finally:
        consumer.close()
        cur.close()
        conn.close()
        log.info(f"consumer closed — total messages processed: {count}")


if __name__ == "__main__":
    run()
