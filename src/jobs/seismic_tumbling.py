"""
Flink job: seismic_tumbling

Tumbling window aggregation over earthquake-feeds topic.
Groups earthquakes by region every 5 minutes and writes to Supabase.

Uses the same env vars as seismic_consumer.py.
"""

import os
from dotenv import load_dotenv
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

load_dotenv()

SERVER = os.getenv("REDPANDA_SERVER", "localhost:9092")
REDPANDA_USERNAME = os.getenv("REDPANDA_USERNAME", "")
REDPANDA_PASSWORD = os.getenv("REDPANDA_PASSWORD", "")
TOPIC_NAME = os.getenv("TOPIC_SEISMIC", "earthquake-feeds")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")
SUPABASE_USER = os.getenv("SUPABASE_USER", "postgres")
SUPABASE_PASS = os.getenv("SUPABASE_PASSWORD")
SUPABASE_DB = os.getenv("SUPABASE_DATABASE", "postgres")
JDBC_URL = f"jdbc:postgresql://{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"


def create_source(t_env):
    t_env.execute_sql(f"""
        CREATE TABLE seismic_source (
            mag        DOUBLE,
            mag_type   VARCHAR,
            place      VARCHAR,
            tsunami    INTEGER,
            event_time BIGINT,
            event_type VARCHAR,
            title      VARCHAR,
            sig        INTEGER,
            lat        DOUBLE,
            lon        DOUBLE,
            depth      DOUBLE,
            event_timestamp AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '30' SECOND
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
    return "seismic_source"


def create_sink(t_env):
    # Create in Supabase before running:
    # CREATE TABLE IF NOT EXISTS seismics_tumbling (
    #     window_start  TIMESTAMP,
    #     window_end    TIMESTAMP,
    #     region        TEXT,
    #     avg_magnitude DOUBLE PRECISION,
    #     max_magnitude DOUBLE PRECISION,
    #     event_count   BIGINT,
    #     tsunami_count BIGINT,
    #     avg_depth     DOUBLE PRECISION,
    #     PRIMARY KEY (window_start, window_end, region)
    # );
    t_env.execute_sql(f"""
        CREATE TABLE seismic_tumbling_sink (
            window_start  TIMESTAMP(3),
            window_end    TIMESTAMP(3),
            region        VARCHAR,
            avg_magnitude DOUBLE,
            max_magnitude DOUBLE,
            event_count   BIGINT,
            tsunami_count BIGINT,
            avg_depth     DOUBLE,
            PRIMARY KEY (window_start, window_end, region) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{JDBC_URL}',
            'table-name' = 'seismics_tumbling',
            'username'   = '{SUPABASE_USER}',
            'password'   = '{SUPABASE_PASS}',
            'driver'     = 'org.postgresql.Driver'
        )
    """)
    return "seismic_tumbling_sink"


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
                CASE
                    WHEN place LIKE '%, %'
                    THEN TRIM(SUBSTRING(place FROM POSITION(',' IN place) + 1))
                    ELSE place
                END AS region,
                AVG(mag)     AS avg_magnitude,
                MAX(mag)     AS max_magnitude,
                COUNT(*)     AS event_count,
                SUM(tsunami) AS tsunami_count,
                AVG(depth)   AS avg_depth
            FROM TABLE(
                TUMBLE(TABLE {source}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
            )
            GROUP BY
                window_start, window_end,
                CASE
                    WHEN place LIKE '%, %'
                    THEN TRIM(SUBSTRING(place FROM POSITION(',' IN place) + 1))
                    ELSE place
                END
        """).print()

    except Exception as e:
        print(f"seismic tumbling job failed: {e}")


if __name__ == "__main__":
    run()

# docker exec -it deploy-jobmanager-1 ./bin/flink run -py /opt/src/jobs/seismic_tumbling.py --pyFiles /opt/src
