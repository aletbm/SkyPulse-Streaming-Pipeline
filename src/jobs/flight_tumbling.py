"""
Flink job: flights_tumbling

Tumbling window aggregation over flight-feeds topic.
Counts active flights by origin country every 5 minutes.

Uses the same env vars as flights_consumer.py.
Uses PROCTIME() since OpenSky snapshot_ts is the same for all
flights in a batch — not suitable as event time.
"""

import os
from dotenv import load_dotenv
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

load_dotenv()

SERVER = os.getenv("REDPANDA_SERVER", "localhost:9092")
REDPANDA_USERNAME = os.getenv("REDPANDA_USERNAME", "")
REDPANDA_PASSWORD = os.getenv("REDPANDA_PASSWORD", "")
TOPIC_NAME = os.getenv("TOPIC_FLIGHTS", "flight-feeds")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")
SUPABASE_USER = os.getenv("SUPABASE_USER", "postgres")
SUPABASE_PASS = os.getenv("SUPABASE_PASSWORD")
SUPABASE_DB = os.getenv("SUPABASE_DATABASE", "postgres")
JDBC_URL = f"jdbc:postgresql://{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"


def create_source(t_env):
    t_env.execute_sql(f"""
        CREATE TABLE flights_source (
            icao24         VARCHAR,
            callsign       VARCHAR,
            origin_country VARCHAR,
            time_position  BIGINT,
            last_contact   BIGINT,
            longitude      DOUBLE,
            latitude       DOUBLE,
            baro_altitude  DOUBLE,
            on_ground      BOOLEAN,
            velocity       DOUBLE,
            true_track     DOUBLE,
            category       INTEGER,
            proc_time      AS PROCTIME()
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
    return "flights_source"


def create_sink(t_env):
    # Create in Supabase before running:
    # CREATE TABLE IF NOT EXISTS flights_tumbling (
    #     window_start    TIMESTAMP,
    #     window_end      TIMESTAMP,
    #     origin_country  TEXT,
    #     flight_count    BIGINT,
    #     airborne_count  BIGINT,
    #     avg_altitude_m  DOUBLE PRECISION,
    #     avg_velocity_ms DOUBLE PRECISION,
    #     PRIMARY KEY (window_start, window_end, origin_country)
    # );
    t_env.execute_sql(f"""
        CREATE TABLE flights_tumbling_sink (
            window_start    TIMESTAMP(3),
            window_end      TIMESTAMP(3),
            origin_country  VARCHAR,
            flight_count    BIGINT,
            airborne_count  BIGINT,
            avg_altitude_m  DOUBLE,
            avg_velocity_ms DOUBLE,
            PRIMARY KEY (window_start, window_end, origin_country) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{JDBC_URL}',
            'table-name' = 'flights_tumbling',
            'username'   = '{SUPABASE_USER}',
            'password'   = '{SUPABASE_PASS}',
            'driver'     = 'org.postgresql.Driver'
        )
    """)
    return "flights_tumbling_sink"


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
                origin_country,
                COUNT(*)                                  AS flight_count,
                COUNT(*) FILTER (WHERE on_ground = FALSE) AS airborne_count,
                AVG(baro_altitude)                        AS avg_altitude_m,
                AVG(velocity)                             AS avg_velocity_ms
            FROM TABLE(
                TUMBLE(TABLE {source}, DESCRIPTOR(proc_time), INTERVAL '5' MINUTE)
            )
            GROUP BY window_start, window_end, origin_country
        """).print()

    except Exception as e:
        print(f"flights tumbling job failed: {e}")


if __name__ == "__main__":
    run()

# docker exec -it deploy-jobmanager-1 ./bin/flink run -py /opt/src/jobs/flight_tumbling.py --pyFiles /opt/src
