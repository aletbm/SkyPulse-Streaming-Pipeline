"""
Flink job: weather_tumbling

Tumbling window aggregation over weather-feeds topic.
Averages weather variables per region every 5 minutes.

Uses the same env vars as weather_consumer.py.
Uses snapshot_ts as event time — Open-Meteo updates every 15 min
so a 60s watermark gives enough slack.
"""

import os
from dotenv import load_dotenv
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

load_dotenv()

SERVER = os.getenv("REDPANDA_SERVER", "localhost:9092")
REDPANDA_USERNAME = os.getenv("REDPANDA_USERNAME", "")
REDPANDA_PASSWORD = os.getenv("REDPANDA_PASSWORD", "")
TOPIC_NAME = os.getenv("TOPIC_WEATHER", "weather-feeds")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")
SUPABASE_USER = os.getenv("SUPABASE_USER", "postgres")
SUPABASE_PASS = os.getenv("SUPABASE_PASSWORD")
SUPABASE_DB = os.getenv("SUPABASE_DATABASE", "postgres")
JDBC_URL = f"jdbc:postgresql://{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"


def create_source(t_env):
    t_env.execute_sql(f"""
        CREATE TABLE weather_source (
            snapshot_id            VARCHAR,
            region_name            VARCHAR,
            latitude               DOUBLE,
            longitude              DOUBLE,
            elevation_m            DOUBLE,
            weathercode            INTEGER,
            interval_s             INTEGER,
            windspeed_ms           DOUBLE,
            winddirection_deg      DOUBLE,
            windgusts_ms           DOUBLE,
            precipitation_mm       DOUBLE,
            rain_mm                DOUBLE,
            snowfall_cm            DOUBLE,
            showers_mm             DOUBLE,
            snow_depth_m           DOUBLE,
            cloudcover_pct         DOUBLE,
            cloudcover_low_pct     DOUBLE,
            temperature_c          DOUBLE,
            apparent_temperature_c DOUBLE,
            humidity_pct           DOUBLE,
            visibility_m           DOUBLE,
            pressure_hpa           DOUBLE,
            snapshot_ts            BIGINT,
            event_timestamp AS TO_TIMESTAMP_LTZ(snapshot_ts, 0),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '60' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'properties.bootstrap.servers' = '{SERVER}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism'     = 'SCRAM-SHA-256',
            'properties.sasl.jaas.config'   = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="{REDPANDA_USERNAME}" password="{REDPANDA_PASSWORD}";',
            'topic'                        = '{TOPIC_NAME}',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)
    return "weather_source"


def create_sink(t_env):
    # Create in Supabase before running:
    # CREATE TABLE IF NOT EXISTS weather_tumbling (
    #     window_start      TIMESTAMP,
    #     window_end        TIMESTAMP,
    #     region_name       TEXT,
    #     avg_temperature_c DOUBLE PRECISION,
    #     min_temperature_c DOUBLE PRECISION,
    #     max_temperature_c DOUBLE PRECISION,
    #     avg_windspeed_ms  DOUBLE PRECISION,
    #     avg_windgusts_ms  DOUBLE PRECISION,
    #     avg_visibility_m  DOUBLE PRECISION,
    #     avg_humidity_pct  DOUBLE PRECISION,
    #     total_precip_mm   DOUBLE PRECISION,
    #     snapshot_count    BIGINT,
    #     PRIMARY KEY (window_start, window_end, region_name)
    # );

    t_env.execute_sql(f"""
        CREATE TABLE weather_tumbling_sink (
            window_start      TIMESTAMP(3),
            window_end        TIMESTAMP(3),
            region_name       VARCHAR,
            avg_temperature_c DOUBLE,
            min_temperature_c DOUBLE,
            max_temperature_c DOUBLE,
            avg_windspeed_ms  DOUBLE,
            avg_windgusts_ms  DOUBLE,
            avg_visibility_m  DOUBLE,
            avg_humidity_pct  DOUBLE,
            total_precip_mm   DOUBLE,
            snapshot_count    BIGINT,
            PRIMARY KEY (window_start, window_end, region_name) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{JDBC_URL}',
            'table-name' = 'weather_tumbling',
            'username'   = '{SUPABASE_USER}',
            'password'   = '{SUPABASE_PASS}',
            'driver'     = 'org.postgresql.Driver'
        )
    """)
    return "weather_tumbling_sink"


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source = create_source(t_env)
        sink = create_sink(t_env)

        t_env.execute_sql(f"""
            INSERT INTO {sink}
            SELECT
                window_start,
                window_end,
                region_name,
                AVG(temperature_c)    AS avg_temperature_c,
                MIN(temperature_c)    AS min_temperature_c,
                MAX(temperature_c)    AS max_temperature_c,
                AVG(windspeed_ms)     AS avg_windspeed_ms,
                AVG(windgusts_ms)     AS avg_windgusts_ms,
                AVG(visibility_m)     AS avg_visibility_m,
                AVG(humidity_pct)     AS avg_humidity_pct,
                SUM(precipitation_mm) AS total_precip_mm,
                COUNT(*)              AS snapshot_count
            FROM TABLE(
                TUMBLE(TABLE {source}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
            )
            GROUP BY window_start, window_end, region_name
        """).print()

    except Exception as e:
        print(f"weather tumbling job failed: {e}")


if __name__ == "__main__":
    run()

# docker exec -it deploy-jobmanager-1 ./bin/flink run -py /opt/src/jobs/weather_tumbling.py --pyFiles /opt/src
