import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import time
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer
from psycopg2.extras import execute_values

from logger import get_logger
from models.weather import weather_deserializer

load_dotenv()

SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_WEATHER", "weather-feeds")
GROUP_ID = "weather"

BATCH_SIZE = 257
FLUSH_INTERVAL = 10  # segundos

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
        value_deserializer=weather_deserializer,
    )


def insert_batch(cursor, records):
    # Create in Supabase before running:
    # CREATE TABLE weather (
    #     latitude DOUBLE PRECISION,
    #     longitude DOUBLE PRECISION,
    #     region_name TEXT,
    #     elevation_m DOUBLE PRECISION,
    #     weathercode INTEGER,
    #     interval_s INTEGER,
    #     windspeed_ms DOUBLE PRECISION,
    #     winddirection_deg DOUBLE PRECISION,
    #     windgusts_ms DOUBLE PRECISION,
    #     precipitation_mm DOUBLE PRECISION,
    #     rain_mm DOUBLE PRECISION,
    #     snowfall_cm DOUBLE PRECISION,
    #     showers_mm DOUBLE PRECISION,
    #     snow_depth_m DOUBLE PRECISION,
    #     cloudcover_pct DOUBLE PRECISION,
    #     cloudcover_low_pct DOUBLE PRECISION,
    #     temperature_c DOUBLE PRECISION,
    #     apparent_temperature_c DOUBLE PRECISION,
    #     humidity_pct DOUBLE PRECISION,
    #     visibility_m DOUBLE PRECISION,
    #     pressure_hpa DOUBLE PRECISION,
    #     snapshot_ts TIMESTAMP,
    #     PRIMARY KEY (latitude, longitude)
    # );

    query = """
    INSERT INTO weather
    (latitude, longitude, region_name, elevation_m,
     weathercode, interval_s, windspeed_ms, winddirection_deg,
     windgusts_ms, precipitation_mm, rain_mm, snowfall_cm,
     showers_mm, snow_depth_m, cloudcover_pct, cloudcover_low_pct,
     temperature_c, apparent_temperature_c, humidity_pct,
     visibility_m, pressure_hpa, snapshot_ts)
    VALUES %s
    ON CONFLICT (latitude, longitude) DO UPDATE SET
        region_name = EXCLUDED.region_name,
        elevation_m = EXCLUDED.elevation_m,
        weathercode = EXCLUDED.weathercode,
        interval_s = EXCLUDED.interval_s,
        windspeed_ms = EXCLUDED.windspeed_ms,
        winddirection_deg = EXCLUDED.winddirection_deg,
        windgusts_ms = EXCLUDED.windgusts_ms,
        precipitation_mm = EXCLUDED.precipitation_mm,
        rain_mm = EXCLUDED.rain_mm,
        snowfall_cm = EXCLUDED.snowfall_cm,
        showers_mm = EXCLUDED.showers_mm,
        snow_depth_m = EXCLUDED.snow_depth_m,
        cloudcover_pct = EXCLUDED.cloudcover_pct,
        cloudcover_low_pct = EXCLUDED.cloudcover_low_pct,
        temperature_c = EXCLUDED.temperature_c,
        apparent_temperature_c = EXCLUDED.apparent_temperature_c,
        humidity_pct = EXCLUDED.humidity_pct,
        visibility_m = EXCLUDED.visibility_m,
        pressure_hpa = EXCLUDED.pressure_hpa,
        snapshot_ts = EXCLUDED.snapshot_ts
    """
    execute_values(cursor, query, records)


def get_record(weather):
    snapshot_ts = (
        datetime.fromtimestamp(weather.snapshot_ts) if weather.snapshot_ts else None
    )

    return (
        weather.latitude,
        weather.longitude,
        weather.region_name,
        weather.elevation_m,
        weather.weathercode,
        weather.interval_s,
        weather.windspeed_ms,
        weather.winddirection_deg,
        weather.windgusts_ms,
        weather.precipitation_mm,
        weather.rain_mm,
        weather.snowfall_cm,
        weather.showers_mm,
        weather.snow_depth_m,
        weather.cloudcover_pct,
        weather.cloudcover_low_pct,
        weather.temperature_c,
        weather.apparent_temperature_c,
        weather.humidity_pct,
        weather.visibility_m,
        weather.pressure_hpa,
        snapshot_ts,
    )


def run():
    log.info(f"connecting to topic: {TOPIC_NAME}")

    conn = get_connection()
    cur = conn.cursor()

    consumer = build_consumer()

    buffer = []
    last_message_time = time.time()
    count = 0

    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)

            if not msg_pack:
                if buffer and (time.time() - last_message_time > FLUSH_INTERVAL):
                    log.info(f"flush by timeout — inserting {len(buffer)} records")
                    insert_batch(cur, buffer)
                    conn.commit()
                    buffer.clear()
                continue

            for tp, messages in msg_pack.items():
                for message in messages:
                    weather = message.value

                    if weather is None:
                        log.warning(
                            f"skipping malformed message at offset {message.offset}"
                        )
                        continue

                    record = get_record(weather)
                    buffer.append(record)
                    last_message_time = time.time()

                    if len(buffer) >= BATCH_SIZE:
                        insert_batch(cur, buffer)
                        buffer.clear()
                        log.info(f"flush by size — inserting {len(buffer)} records")

                    count += 1

                    log.info(
                        f"[{count}] {weather.region_name} | "
                        f"temp={weather.temperature_c}°C | "
                        f"wind={weather.windspeed_ms}m/s"
                    )

    except KeyboardInterrupt:
        log.info("stopped by user")

    finally:
        if buffer:
            log.info(f"final flush — inserting {len(buffer)} records")
            insert_batch(cur, buffer)

        consumer.close()
        cur.close()
        conn.close()

        log.info(f"consumer closed — total messages processed: {count}")


if __name__ == "__main__":
    run()
