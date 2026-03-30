"""
Flink job: flight_context_tumbling

Tumbling window join across all three topics every 5 minutes.
Groups events into 10-degree lat/lon grid cells and joins flights
with nearby seismic and weather conditions.

Proximity logic:
  - Events are considered "nearby" if they share the same
    10-degree lat/lon grid cell (roughly 1000x1000 km).
  - This avoids a costly cross join while still capturing
    meaningful geographic proximity.

Uses the same env vars as all three consumers.
"""

import os
from dotenv import load_dotenv
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

load_dotenv()

SERVER = os.getenv("REDPANDA_SERVER", "localhost:9092")
REDPANDA_USERNAME = os.getenv("REDPANDA_USERNAME", "")
REDPANDA_PASSWORD = os.getenv("REDPANDA_PASSWORD", "")

TOPIC_FLIGHTS = os.getenv("TOPIC_FLIGHTS", "flight-feeds")
TOPIC_SEISMIC = os.getenv("TOPIC_SEISMIC", "earthquake-feeds")
TOPIC_WEATHER = os.getenv("TOPIC_WEATHER", "weather-feeds")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")
SUPABASE_USER = os.getenv("SUPABASE_USER", "postgres")
SUPABASE_PASS = os.getenv("SUPABASE_PASSWORD")
SUPABASE_DB = os.getenv("SUPABASE_DATABASE", "postgres")
JDBC_URL = f"jdbc:postgresql://{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"


def create_sources(t_env):
    t_env.execute_sql(f"""
        CREATE TABLE fc_flights (
            icao24         VARCHAR,
            origin_country VARCHAR,
            latitude       DOUBLE,
            longitude      DOUBLE,
            baro_altitude  DOUBLE,
            on_ground      BOOLEAN,
            velocity       DOUBLE,
            proc_time      AS PROCTIME()
        ) WITH (
            'connector'                    = 'kafka',
            'properties.bootstrap.servers' = '{SERVER}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism'     = 'SCRAM-SHA-256',
            'properties.sasl.jaas.config'   = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="{REDPANDA_USERNAME}" password="{REDPANDA_PASSWORD}";',
            'topic'                        = '{TOPIC_FLIGHTS}',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE fc_seismic (
            mag        DOUBLE,
            place      VARCHAR,
            event_time BIGINT,
            tsunami    INTEGER,
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
            'topic'                        = '{TOPIC_SEISMIC}',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE fc_weather (
            latitude       DOUBLE,
            longitude      DOUBLE,
            temperature_c  DOUBLE,
            windspeed_ms   DOUBLE,
            windgusts_ms   DOUBLE,
            visibility_m   DOUBLE,
            weathercode    INTEGER,
            snapshot_ts    BIGINT,
            event_timestamp AS TO_TIMESTAMP_LTZ(snapshot_ts, 0),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '60' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'properties.bootstrap.servers' = '{SERVER}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism'     = 'SCRAM-SHA-256',
            'properties.sasl.jaas.config'   = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="{REDPANDA_USERNAME}" password="{REDPANDA_PASSWORD}";',
            'topic'                        = '{TOPIC_WEATHER}',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """)


def create_sink(t_env):
    # Create in Supabase before running:
    # CREATE TABLE IF NOT EXISTS flight_context (
    #     window_start      TIMESTAMP,
    #     window_end        TIMESTAMP,
    #     grid_lat          INTEGER,
    #     grid_lon          INTEGER,
    #     flight_count      BIGINT,
    #     airborne_count    BIGINT,
    #     avg_altitude_m    DOUBLE PRECISION,
    #     nearby_eq_count   BIGINT,
    #     max_eq_magnitude  DOUBLE PRECISION,
    #     tsunami_count     BIGINT,
    #     avg_temperature_c DOUBLE PRECISION,
    #     avg_windgusts_ms  DOUBLE PRECISION,
    #     avg_visibility_m  DOUBLE PRECISION,
    #     PRIMARY KEY (window_start, window_end, grid_lat, grid_lon)
    # );
    t_env.execute_sql(f"""
        CREATE TABLE flight_context_sink (
            window_start      TIMESTAMP(3),
            window_end        TIMESTAMP(3),
            grid_lat          INTEGER,
            grid_lon          INTEGER,
            flight_count      BIGINT,
            airborne_count    BIGINT,
            avg_altitude_m    DOUBLE,
            nearby_eq_count   BIGINT,
            max_eq_magnitude  DOUBLE,
            tsunami_count     BIGINT,
            avg_temperature_c DOUBLE,
            avg_windgusts_ms  DOUBLE,
            avg_visibility_m  DOUBLE,
            PRIMARY KEY (window_start, window_end, grid_lat, grid_lon) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{JDBC_URL}',
            'table-name' = 'flight_context',
            'username'   = '{SUPABASE_USER}',
            'password'   = '{SUPABASE_PASS}',
            'driver'     = 'org.postgresql.Driver'
        )
    """)
    return "flight_context_sink"


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        create_sources(t_env)
        sink = create_sink(t_env)

        # Pre-aggregate each source into 10-degree grid cells per window
        # then LEFT JOIN on (window, grid_cell) — avoids a cross join.
        t_env.execute_sql("""
            CREATE VIEW flights_agg AS
            SELECT
                window_start,
                window_end,
                CAST(FLOOR(latitude  / 10) * 10 AS INTEGER) AS grid_lat,
                CAST(FLOOR(longitude / 10) * 10 AS INTEGER) AS grid_lon,
                COUNT(*)                                      AS flight_count,
                COUNT(*) FILTER (WHERE on_ground = FALSE)     AS airborne_count,
                AVG(baro_altitude)                            AS avg_altitude_m
            FROM TABLE(
                TUMBLE(TABLE fc_flights, DESCRIPTOR(proc_time), INTERVAL '5' MINUTE)
            )
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY
                window_start, window_end,
                CAST(FLOOR(latitude  / 10) * 10 AS INTEGER),
                CAST(FLOOR(longitude / 10) * 10 AS INTEGER)
        """)

        t_env.execute_sql("""
            CREATE VIEW seismic_agg AS
            SELECT
                window_start,
                window_end,
                CAST(FLOOR(lat / 10) * 10 AS INTEGER) AS grid_lat,
                CAST(FLOOR(lon / 10) * 10 AS INTEGER) AS grid_lon,
                COUNT(*)     AS nearby_eq_count,
                MAX(mag)     AS max_eq_magnitude,
                SUM(tsunami) AS tsunami_count
            FROM TABLE(
                TUMBLE(TABLE fc_seismic, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
            )
            GROUP BY
                window_start, window_end,
                CAST(FLOOR(lat / 10) * 10 AS INTEGER),
                CAST(FLOOR(lon / 10) * 10 AS INTEGER)
        """)

        t_env.execute_sql("""
            CREATE VIEW weather_agg AS
            SELECT
                window_start,
                window_end,
                CAST(FLOOR(latitude  / 10) * 10 AS INTEGER) AS grid_lat,
                CAST(FLOOR(longitude / 10) * 10 AS INTEGER) AS grid_lon,
                AVG(temperature_c) AS avg_temperature_c,
                AVG(windgusts_ms)  AS avg_windgusts_ms,
                AVG(visibility_m)  AS avg_visibility_m
            FROM TABLE(
                TUMBLE(TABLE fc_weather, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
            )
            GROUP BY
                window_start, window_end,
                CAST(FLOOR(latitude  / 10) * 10 AS INTEGER),
                CAST(FLOOR(longitude / 10) * 10 AS INTEGER)
        """)

        t_env.execute_sql(f"""
            INSERT INTO {sink}
            SELECT
                f.window_start,
                f.window_end,
                f.grid_lat,
                f.grid_lon,
                f.flight_count,
                f.airborne_count,
                f.avg_altitude_m,
                COALESCE(s.nearby_eq_count,   0)   AS nearby_eq_count,
                COALESCE(s.max_eq_magnitude,  0.0) AS max_eq_magnitude,
                COALESCE(s.tsunami_count,     0)   AS tsunami_count,
                COALESCE(w.avg_temperature_c, 0.0) AS avg_temperature_c,
                COALESCE(w.avg_windgusts_ms,  0.0) AS avg_windgusts_ms,
                COALESCE(w.avg_visibility_m,  0.0) AS avg_visibility_m
            FROM flights_agg f
            LEFT JOIN seismic_agg s
                ON  f.window_start = s.window_start
                AND f.grid_lat     = s.grid_lat
                AND f.grid_lon     = s.grid_lon
            LEFT JOIN weather_agg w
                ON  f.window_start = w.window_start
                AND f.grid_lat     = w.grid_lat
                AND f.grid_lon     = w.grid_lon
        """).print()

    except Exception as e:
        print(f"flight context job failed: {e}")


if __name__ == "__main__":
    run()

# docker exec -it deploy-jobmanager-1 ./bin/flink run -py /opt/src/jobs/flight_context_tumbling.py --pyFiles /opt/src
