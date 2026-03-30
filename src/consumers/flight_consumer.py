import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import time
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from psycopg2.extras import execute_values

from logger import get_logger
from models.flight import flight_deserializer

load_dotenv()

SERVER = os.getenv("REDPANDA_SERVER", "localhost:9092")
REDPANDA_USERNAME = os.getenv("REDPANDA_USERNAME", "")
REDPANDA_PASSWORD = os.getenv("REDPANDA_PASSWORD", "")
TOPIC_NAME = os.getenv("TOPIC_FLIGHTS", "flight-feeds")
GROUP_ID = "flights"

BATCH_SIZE = 9000
FLUSH_INTERVAL = 10

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
        value_deserializer=flight_deserializer,
        consumer_timeout_ms=10000,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=REDPANDA_USERNAME,
        sasl_plain_password=REDPANDA_PASSWORD,
    )


def create_temp_table(cursor):
    cursor.execute("""
        CREATE TEMP TABLE IF NOT EXISTS current_flights (
            icao24 TEXT PRIMARY KEY
        );
    """)


def insert_current_icaos(cursor, icaos):
    cursor.execute("TRUNCATE current_flights;")

    execute_values(
        cursor,
        "INSERT INTO current_flights (icao24) VALUES %s",
        [(icao,) for icao in icaos],
    )


def delete_missing_flights(cursor):
    cursor.execute("""
        DELETE FROM flights f
        WHERE NOT EXISTS (
            SELECT 1 FROM current_flights c
            WHERE c.icao24 = f.icao24
        )
    """)


def insert_batch(cursor, records):
    # Create in Supabase before running:
    # CREATE TABLE flights (
    #     icao24 TEXT PRIMARY KEY,
    #     callsign TEXT,
    #     origin_country TEXT,
    #     time_position TIMESTAMP,
    #     last_contact TIMESTAMP,
    #     longitude DOUBLE PRECISION,
    #     latitude DOUBLE PRECISION,
    #     baro_altitude DOUBLE PRECISION,
    #     on_ground BOOLEAN,
    #     velocity DOUBLE PRECISION,
    #     true_track DOUBLE PRECISION,
    #     category INTEGER
    # );

    query = """
    INSERT INTO flights
    (icao24, callsign, origin_country, time_position, last_contact,
     longitude, latitude, baro_altitude, on_ground,
     velocity, true_track, category)
    VALUES %s
    ON CONFLICT (icao24) DO UPDATE SET
        callsign = EXCLUDED.callsign,
        origin_country = EXCLUDED.origin_country,
        time_position = EXCLUDED.time_position,
        last_contact = EXCLUDED.last_contact,
        longitude = EXCLUDED.longitude,
        latitude = EXCLUDED.latitude,
        baro_altitude = EXCLUDED.baro_altitude,
        on_ground = EXCLUDED.on_ground,
        velocity = EXCLUDED.velocity,
        true_track = EXCLUDED.true_track,
        category = EXCLUDED.category
    """

    execute_values(cursor, query, records)


def get_record(flight):
    return (
        flight.icao24,
        flight.callsign,
        flight.origin_country,
        datetime.fromtimestamp(flight.time_position) if flight.time_position else None,
        datetime.fromtimestamp(flight.last_contact) if flight.last_contact else None,
        flight.longitude,
        flight.latitude,
        flight.baro_altitude,
        flight.on_ground,
        flight.velocity,
        flight.true_track,
        flight.category,
    )


def run():
    log.info(f"connecting to topic: {TOPIC_NAME}")

    conn = get_connection()
    cur = conn.cursor()

    consumer = build_consumer()
    count = 0

    buffer = []
    current_icaos = set()
    last_message_time = time.time()

    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)

            # 📭 No messages → flush por tiempo
            if not msg_pack:
                if buffer and (time.time() - last_message_time > FLUSH_INTERVAL):
                    log.info(f"flush by timeout — inserting {len(buffer)} records")

                    create_temp_table(cur)
                    insert_current_icaos(cur, current_icaos)

                    insert_batch(cur, buffer)

                    log.info("deleting flights not in current snapshot")
                    delete_missing_flights(cur)

                    buffer.clear()
                    current_icaos.clear()

                continue

            # 📦 Hay mensajes
            for tp, messages in msg_pack.items():
                for message in messages:
                    flight = message.value

                    if flight is None:
                        log.warning(
                            f"skipping malformed message at offset {message.offset}"
                        )
                        continue

                    record = get_record(flight)
                    buffer.append(record)

                    # 🔥 trackear snapshot
                    current_icaos.add(flight.icao24)

                    last_message_time = time.time()

                    # 📊 flush por tamaño
                    if len(buffer) >= BATCH_SIZE:
                        log.info(f"flush by size — inserting {len(buffer)} records")

                        create_temp_table(cur)
                        insert_current_icaos(cur, current_icaos)

                        insert_batch(cur, buffer)

                        log.info("deleting flights not in current snapshot")
                        delete_missing_flights(cur)

                        buffer.clear()
                        current_icaos.clear()

                    count += 1

                    log.info(
                        f"[{count}] {flight.callsign or 'N/A'} | "
                        f"{flight.origin_country} | "
                        f"lat={flight.latitude} lon={flight.longitude} "
                        f"alt={flight.baro_altitude}m"
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
