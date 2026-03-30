import json
import os
import pathlib
import subprocess
import sys
import time

import psycopg2


def get_terraform_outputs() -> dict:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    terraform_dir = os.path.join(script_dir, "..", "terraform")
    result = subprocess.run(
        ["terraform", f"-chdir={terraform_dir}", "output", "-json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return {k: v["value"] for k, v in json.loads(result.stdout).items()}


outputs = get_terraform_outputs()

POSTGRES_HOST = f"aws-0-{outputs['supabase_region']}.pooler.supabase.com"
POSTGRES_PORT = 5432
POSTGRES_USER = f"postgres.{outputs['project_ref']}"
POSTGRES_PASSWORD = outputs["db_password"]
POSTGRES_DATABASE = "postgres"


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


conn = get_connection()
cur = conn.cursor()


def wait_for_db(retries=10, delay=15):
    global cur
    for attempt in range(1, retries + 1):
        try:
            cur.execute("SELECT 1")
            print("✅ DB ready")
            return
        except Exception as e:
            print(f"⏳ [{attempt}/{retries}] Not ready yet: {e}")
            time.sleep(delay)
    print("❌ DB unreachable after all retries")
    sys.exit(1)


def run_migrations():
    global cur

    statements = [
        # ── Extensions ────────────────────────────────────────────────
        "CREATE EXTENSION IF NOT EXISTS postgis",
        "CREATE EXTENSION IF NOT EXISTS postgis_topology",
        # ── Schemas ───────────────────────────────────────────────────
        "CREATE SCHEMA IF NOT EXISTS staging",
        "CREATE SCHEMA IF NOT EXISTS intermediate",
        "CREATE SCHEMA IF NOT EXISTS mart",
        # ── Permisos PostgREST ────────────────────────────────────────
        "GRANT USAGE ON SCHEMA staging      TO anon, authenticated, service_role",
        "GRANT USAGE ON SCHEMA intermediate TO anon, authenticated, service_role",
        "GRANT USAGE ON SCHEMA mart         TO anon, authenticated, service_role",
        # ── public: flights ───────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS public.flights (
            icao24          TEXT             PRIMARY KEY,
            callsign        TEXT,
            origin_country  TEXT,
            time_position   TIMESTAMP,
            last_contact    TIMESTAMP,
            longitude       DOUBLE PRECISION,
            latitude        DOUBLE PRECISION,
            baro_altitude   DOUBLE PRECISION,
            on_ground       BOOLEAN,
            velocity        DOUBLE PRECISION,
            true_track      DOUBLE PRECISION,
            category        INTEGER
        )
        """,
        # ── public: seismics ──────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS public.seismics (
            id          SERIAL           PRIMARY KEY,
            mag         DOUBLE PRECISION,
            mag_type    TEXT,
            place       TEXT,
            tsunami     INTEGER,
            event_time  TIMESTAMP,
            event_type  TEXT,
            title       TEXT,
            sig         INTEGER,
            lat         DOUBLE PRECISION,
            lon         DOUBLE PRECISION,
            depth       DOUBLE PRECISION
        )
        """,
        # ── public: weather ───────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS public.weather (
            latitude                DOUBLE PRECISION,
            longitude               DOUBLE PRECISION,
            region_name             TEXT,
            elevation_m             DOUBLE PRECISION,
            weathercode             INTEGER,
            interval_s              INTEGER,
            windspeed_ms            DOUBLE PRECISION,
            winddirection_deg       DOUBLE PRECISION,
            windgusts_ms            DOUBLE PRECISION,
            precipitation_mm        DOUBLE PRECISION,
            rain_mm                 DOUBLE PRECISION,
            snowfall_cm             DOUBLE PRECISION,
            showers_mm              DOUBLE PRECISION,
            snow_depth_m            DOUBLE PRECISION,
            cloudcover_pct          DOUBLE PRECISION,
            cloudcover_low_pct      DOUBLE PRECISION,
            temperature_c           DOUBLE PRECISION,
            apparent_temperature_c  DOUBLE PRECISION,
            humidity_pct            DOUBLE PRECISION,
            visibility_m            DOUBLE PRECISION,
            pressure_hpa            DOUBLE PRECISION,
            snapshot_ts             TIMESTAMP,
            PRIMARY KEY (latitude, longitude)
        )
        """,
        # ── public: flights_tumbling ──────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS public.flights_tumbling (
            window_start    TIMESTAMP,
            window_end      TIMESTAMP,
            origin_country  TEXT,
            flight_count    BIGINT,
            airborne_count  BIGINT,
            avg_altitude_m  DOUBLE PRECISION,
            avg_velocity_ms DOUBLE PRECISION,
            PRIMARY KEY (window_start, window_end, origin_country)
        )
        """,
        # ── public: seismics_tumbling ─────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS public.seismics_tumbling (
            window_start    TIMESTAMP,
            window_end      TIMESTAMP,
            region          TEXT,
            avg_magnitude   DOUBLE PRECISION,
            max_magnitude   DOUBLE PRECISION,
            event_count     BIGINT,
            tsunami_count   BIGINT,
            avg_depth       DOUBLE PRECISION,
            PRIMARY KEY (window_start, window_end, region)
        )
        """,
        # ── public: weather_tumbling ──────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS public.weather_tumbling (
            window_start        TIMESTAMP,
            window_end          TIMESTAMP,
            region_name         TEXT,
            avg_temperature_c   DOUBLE PRECISION,
            min_temperature_c   DOUBLE PRECISION,
            max_temperature_c   DOUBLE PRECISION,
            avg_windspeed_ms    DOUBLE PRECISION,
            avg_windgusts_ms    DOUBLE PRECISION,
            avg_visibility_m    DOUBLE PRECISION,
            avg_humidity_pct    DOUBLE PRECISION,
            total_precip_mm     DOUBLE PRECISION,
            snapshot_count      BIGINT,
            PRIMARY KEY (window_start, window_end, region_name)
        )
        """,
        # ── public: flight_context ────────────────────────────────────
        # Column order matches flight_context_tumbling.py sink DDL exactly
        # (Flink JDBC writes by position, not by name)
        """
        CREATE TABLE IF NOT EXISTS public.flight_context (
            window_start        TIMESTAMP,
            window_end          TIMESTAMP,
            grid_lat            INTEGER,
            grid_lon            INTEGER,
            flight_count        BIGINT,
            avg_altitude_m      DOUBLE PRECISION,
            airborne_count      BIGINT,
            nearby_eq_count     BIGINT,
            max_eq_magnitude    DOUBLE PRECISION,
            tsunami_count       BIGINT,
            avg_temperature_c   DOUBLE PRECISION,
            avg_windgusts_ms    DOUBLE PRECISION,
            avg_visibility_m    DOUBLE PRECISION,
            PRIMARY KEY (window_start, window_end, grid_lat, grid_lon)
        )
        """,
        # ── public: raw_airports ──────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS public.raw_airports (
            airport_id      INTEGER          PRIMARY KEY,
            name            TEXT,
            city            TEXT,
            country         TEXT,
            iata            TEXT,
            icao            TEXT,
            latitude        DOUBLE PRECISION,
            longitude       DOUBLE PRECISION,
            altitude_ft     INTEGER,
            timezone_offset DOUBLE PRECISION,
            dst             TEXT,
            tz_database     TEXT,
            airport_type    TEXT,
            source          TEXT
        )
        """,
        # ── public: raw_airlines ──────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS public.raw_airlines (
            airline_id  INTEGER PRIMARY KEY,
            name        TEXT,
            alias       TEXT,
            iata        TEXT,
            icao        TEXT,
            callsign    TEXT,
            country     TEXT,
            active      TEXT
        )
        """,
        # ── public: raw_planes ────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS public.raw_planes (
            name    TEXT,
            iata    TEXT,
            icao    TEXT
        )
        """,
        # ── public: raw_routes ────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS public.raw_routes (
            airline         TEXT,
            airline_id      INTEGER,
            src_airport     TEXT,
            src_airport_id  INTEGER,
            dst_airport     TEXT,
            dst_airport_id  INTEGER,
            codeshare       TEXT,
            stops           INTEGER,
            equipment       TEXT
        )
        """,
        # ── staging: stg_flights ──────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS staging.stg_flights (
            icao24                  TEXT                PRIMARY KEY,
            callsign                TEXT,
            origin_country          TEXT,
            time_position           TIMESTAMP,
            last_contact            TIMESTAMP,
            longitude               DOUBLE PRECISION,
            latitude                DOUBLE PRECISION,
            geog                    GEOGRAPHY(Point, 4326),
            baro_altitude           DOUBLE PRECISION,
            on_ground               BOOLEAN,
            velocity                DOUBLE PRECISION,
            true_track              DOUBLE PRECISION,
            category                INTEGER,
            baro_altitude_m         NUMERIC,
            velocity_knots          NUMERIC,
            continent               TEXT,
            grid_lat                INTEGER,
            grid_lon                INTEGER,
            _loaded_at              TIMESTAMP
        )
        """,
        # ── staging: stg_seismics ─────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS staging.stg_seismics (
            id                      INTEGER             PRIMARY KEY,
            mag                     NUMERIC,
            mag_type                TEXT,
            place                   TEXT,
            tsunami                 INTEGER,
            event_time              TIMESTAMP,
            event_type              TEXT,
            title                   TEXT,
            sig                     INTEGER,
            lat                     DOUBLE PRECISION,
            lon                     DOUBLE PRECISION,
            depth_km                NUMERIC,
            region                  TEXT,
            magnitude_class         TEXT,
            is_tsunami_related      BOOLEAN,
            grid_lat                INTEGER,
            grid_lon                INTEGER,
            _loaded_at              TIMESTAMP
        )
        """,
        # ── staging: stg_weather ──────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS staging.stg_weather (
            latitude                DOUBLE PRECISION,
            longitude               DOUBLE PRECISION,
            region_name             TEXT,
            elevation_m             NUMERIC,
            weathercode             INTEGER,
            interval_s              INTEGER,
            windspeed_ms            NUMERIC,
            winddirection_deg       NUMERIC,
            windgusts_ms            NUMERIC,
            precipitation_mm        NUMERIC,
            rain_mm                 NUMERIC,
            snowfall_cm             NUMERIC,
            showers_mm              NUMERIC,
            snow_depth_m            NUMERIC,
            cloudcover_pct          NUMERIC,
            cloudcover_low_pct      NUMERIC,
            temperature_c           NUMERIC,
            apparent_temperature_c  NUMERIC,
            humidity_pct            NUMERIC,
            visibility_m            NUMERIC,
            pressure_hpa            NUMERIC,
            snapshot_ts             TIMESTAMP,
            weather_label           TEXT,
            wind_severity           TEXT,
            grid_lat                INTEGER,
            grid_lon                INTEGER,
            _loaded_at              TIMESTAMP,
            PRIMARY KEY (latitude, longitude)
        )
        """,
        # ── staging: stg_airports ─────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS staging.stg_airports (
            airport_id              INTEGER             PRIMARY KEY,
            airport_name            TEXT,
            city                    TEXT,
            country                 TEXT,
            iata_code               TEXT,
            icao_code               TEXT,
            latitude                NUMERIC,
            longitude               NUMERIC,
            altitude_ft             INTEGER,
            altitude_m              NUMERIC,
            timezone_offset         DOUBLE PRECISION,
            tz_database             TEXT,
            dst                     TEXT,
            geog                    GEOGRAPHY(Point, 4326),
            continent               TEXT,
            airport_type            TEXT,
            source                  TEXT,
            _loaded_at              TIMESTAMP
        )
        """,
        # ── staging: stg_airlines ─────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS staging.stg_airlines (
            airline_id              INTEGER             PRIMARY KEY,
            airline_name            TEXT,
            alias                   TEXT,
            iata_code               TEXT,
            icao_code               TEXT,
            callsign                TEXT,
            country                 TEXT,
            is_active               BOOLEAN,
            _loaded_at              TIMESTAMP
        )
        """,
        # ── staging: stg_routes ───────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS staging.stg_routes (
            airline_code                TEXT,
            airline_name                TEXT,
            airline_country             TEXT,
            airline_is_active           BOOLEAN,
            origin_code                 TEXT,
            origin_airport_name         TEXT,
            origin_city                 TEXT,
            origin_country              TEXT,
            origin_continent            TEXT,
            origin_lat                  DOUBLE PRECISION,
            origin_lon                  DOUBLE PRECISION,
            origin_timezone             TEXT,
            destination_code            TEXT,
            destination_airport_name    TEXT,
            destination_city            TEXT,
            destination_country         TEXT,
            destination_continent       TEXT,
            destination_lat             DOUBLE PRECISION,
            destination_lon             DOUBLE PRECISION,
            destination_timezone        TEXT,
            is_codeshare                BOOLEAN,
            stops                       INTEGER,
            aircraft_codes              TEXT,
            distance_km                 NUMERIC,
            route_type                  TEXT,
            _loaded_at                  TIMESTAMP
        )
        """,
        # ── intermediate: int_airport_grid ────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS intermediate.int_airport_grid (
            grid_lat        DOUBLE PRECISION,
            grid_lon        DOUBLE PRECISION,
            airport_name    TEXT,
            iata_code       TEXT,
            icao_code       TEXT,
            city            TEXT,
            country         TEXT,
            continent       TEXT,
            tz_database     TEXT,
            altitude_m      NUMERIC,
            geog            GEOGRAPHY(Point, 4326),
            PRIMARY KEY (grid_lat, grid_lon)
        )
        """,
        # ── intermediate: int_flights_enriched ───────────────────────
        """
        CREATE TABLE IF NOT EXISTS intermediate.int_flights_enriched (
            icao24                      TEXT                PRIMARY KEY,
            callsign                    TEXT,
            origin_country              TEXT,
            time_position               TIMESTAMP,
            last_contact                TIMESTAMP,
            longitude                   DOUBLE PRECISION,
            latitude                    DOUBLE PRECISION,
            geog                        GEOGRAPHY(Point, 4326),
            baro_altitude_m             NUMERIC,
            on_ground                   BOOLEAN,
            velocity_ms                 NUMERIC,
            velocity_knots              NUMERIC,
            flight_phase                TEXT,
            current_continent           TEXT,
            grid_lat                    INTEGER,
            grid_lon                    INTEGER,
            true_track                  DOUBLE PRECISION,
            category                    INTEGER,
            nearest_airport_name        TEXT,
            nearest_airport_iata        TEXT,
            nearest_airport_icao        TEXT,
            nearest_airport_city        TEXT,
            nearest_airport_country     TEXT,
            nearest_airport_continent   TEXT,
            nearest_airport_timezone    TEXT,
            nearest_airport_altitude_m  NUMERIC,
            near_airport                BOOLEAN,
            airline_name                TEXT,
            airline_iata                TEXT,
            airline_icao                TEXT,
            _loaded_at                  TIMESTAMP
        )
        """,
        # ── mart: mart_flight_activity ────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS mart.mart_flight_activity (
            origin_country          TEXT                PRIMARY KEY,
            continent               TEXT,
            airline_name            TEXT,
            airline_iata            TEXT,
            airline_icao            TEXT,
            live_flight_count       BIGINT,
            live_airborne_count     BIGINT,
            live_on_ground_count    BIGINT,
            cruising_count          BIGINT,
            climb_descend_count     BIGINT,
            takeoff_landing_count   BIGINT,
            avg_altitude_m          NUMERIC,
            avg_speed_knots         NUMERIC,
            max_speed_knots         NUMERIC,
            max_altitude_m          NUMERIC,
            flights_near_airport    BIGINT,
            pct_near_airport        NUMERIC,
            most_common_airport     TEXT,
            most_common_city        TEXT,
            active_grid_cells       BIGINT,
            last_window_start       TIMESTAMP,
            last_window_end         TIMESTAMP,
            window_flight_count     BIGINT,
            window_avg_velocity_ms  NUMERIC,
            peak_flights_2h         BIGINT,
            min_flights_2h          BIGINT,
            avg_flights_2h          NUMERIC,
            refreshed_at            TIMESTAMP
        )
        """,
        # ── mart: mart_weather_conditions ─────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS mart.mart_weather_conditions (
            region_name                 TEXT                PRIMARY KEY,
            station_count               BIGINT,
            avg_temp_c                  NUMERIC,
            min_temp_c                  NUMERIC,
            max_temp_c                  NUMERIC,
            avg_humidity_pct            NUMERIC,
            avg_windspeed_ms            NUMERIC,
            max_windgusts_ms            NUMERIC,
            total_precip_mm             NUMERIC,
            total_snowfall_cm           NUMERIC,
            avg_visibility_m            NUMERIC,
            avg_pressure_hpa            NUMERIC,
            avg_cloudcover_pct          NUMERIC,
            dominant_weather            TEXT,
            dominant_wind_severity      TEXT,
            stations_with_precip        BIGINT,
            stations_with_snow          BIGINT,
            latest_snapshot             TIMESTAMP,
            last_window_start           TIMESTAMP,
            last_window_snapshot_count  BIGINT,
            condition_summary           TEXT,
            refreshed_at                TIMESTAMP
        )
        """,
        # ── mart: mart_seismic_activity ───────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS mart.mart_seismic_activity (
            region                  TEXT                PRIMARY KEY,
            event_count_24h         BIGINT,
            avg_magnitude           NUMERIC,
            max_magnitude           NUMERIC,
            min_magnitude           NUMERIC,
            avg_depth_km            NUMERIC,
            tsunami_events          BIGINT,
            micro_count             BIGINT,
            minor_count             BIGINT,
            light_count             BIGINT,
            moderate_count          BIGINT,
            strong_count            BIGINT,
            major_great_count       BIGINT,
            risk_level              TEXT,
            last_window_start       TIMESTAMP,
            last_window_event_count BIGINT,
            last_window_max_mag     NUMERIC,
            refreshed_at            TIMESTAMP
        )
        """,
        # ── mart: mart_flight_context ─────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS mart.mart_flight_context (
            window_start                TIMESTAMP,
            window_end                  TIMESTAMP,
            grid_lat                    INTEGER,
            grid_lon                    INTEGER,
            continent                   TEXT,
            primary_airport_name        TEXT,
            primary_airport_iata        TEXT,
            primary_airport_city        TEXT,
            primary_airport_country     TEXT,
            primary_airport_timezone    TEXT,
            distinct_airlines           BIGINT,
            dominant_airline            TEXT,
            flight_count                BIGINT,
            airborne_count              BIGINT,
            ground_count                BIGINT,
            avg_altitude_m              NUMERIC,
            cruising_count              BIGINT,
            climb_descend_count         BIGINT,
            takeoff_landing_count       BIGINT,
            near_airport_count          BIGINT,
            avg_speed_knots             NUMERIC,
            max_altitude_m              NUMERIC,
            nearby_eq_count             INTEGER,
            max_eq_magnitude            DOUBLE PRECISION,
            tsunami_count               INTEGER,
            eq_count_24h                BIGINT,
            max_mag_24h                 NUMERIC,
            tsunami_count_24h           BIGINT,
            avg_temperature_c           NUMERIC,
            avg_windgusts_ms            NUMERIC,
            avg_visibility_m            NUMERIC,
            wind_severity               TEXT,
            weather_label               TEXT,
            avg_precip_mm               NUMERIC,
            risk_score                  NUMERIC,
            refreshed_at                TIMESTAMP,
            PRIMARY KEY (window_start, window_end, grid_lat, grid_lon)
        )
        """,
    ]

    index_statements = [
        """
        CREATE INDEX IF NOT EXISTS idx_stg_airports_geog
            ON staging.stg_airports USING gist (geog)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_airport_grid_lookup
            ON intermediate.int_airport_grid (grid_lat, grid_lon)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_stg_flights_grid
            ON staging.stg_flights (grid_lat, grid_lon)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_stg_flights_grid_computed
            ON staging.stg_flights ((FLOOR(latitude * 2) / 2),
            (FLOOR(longitude * 2) / 2))
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_stg_airlines_country
            ON staging.stg_airlines (country) WHERE is_active = TRUE
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_stg_airports_iata
            ON staging.stg_airports (iata_code)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_stg_airports_icao
            ON staging.stg_airports (icao_code)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_stg_airlines_iata
            ON staging.stg_airlines (iata_code)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_stg_airlines_icao
            ON staging.stg_airlines (icao_code)
        """,
    ]

    for stmt in statements:
        label = stmt.strip().splitlines()[0].strip()
        print(f"  -> {label[:-2]}")
        cur.execute(stmt)

    print("✅ Tables created")

    for stmt in index_statements:
        label = stmt.strip().splitlines()[0].strip()
        try:
            cur.execute(stmt)
            print(f"  -> {label}")
        except Exception as e:
            print(f"  ⚠️  Skipped (run again after Bruin): {label} — {e}")

    print("✅ Migrations complete")


def run_rls():
    global cur

    tables = [
        "public.flights",
        "public.seismics",
        "public.weather",
        "public.flights_tumbling",
        "public.seismics_tumbling",
        "public.weather_tumbling",
        "public.flight_context",
        "public.raw_airports",
        "public.raw_airlines",
        "public.raw_planes",
        "public.raw_routes",
        "staging.stg_flights",
        "staging.stg_seismics",
        "staging.stg_weather",
        "staging.stg_airports",
        "staging.stg_airlines",
        "staging.stg_routes",
        "intermediate.int_airport_grid",
        "intermediate.int_flights_enriched",
        "mart.mart_flight_activity",
        "mart.mart_weather_conditions",
        "mart.mart_seismic_activity",
        "mart.mart_flight_context",
    ]

    def policy_exists(schema, tname, policy_name):
        cur.execute(
            """
            SELECT 1 FROM pg_policies
            WHERE schemaname = %s
              AND tablename  = %s
              AND policyname = %s
        """,
            (schema, tname, policy_name),
        )
        return cur.fetchone() is not None

    for table in tables:
        schema, tname = table.split(".")

        cur.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")

        policy_sr = f"{tname}_service_role_all"
        if not policy_exists(schema, tname, policy_sr):
            cur.execute(f"""
                CREATE POLICY {policy_sr}
                ON {table}
                TO service_role
                USING (true)
                WITH CHECK (true)
            """)

        policy_ro = f"{tname}_read_only"
        if not policy_exists(schema, tname, policy_ro):
            cur.execute(f"""
                CREATE POLICY {policy_ro}
                ON {table}
                FOR SELECT
                TO anon, authenticated
                USING (true)
            """)

        print(f"  -> RLS enabled: {table}")

    print("✅ RLS configured")


def update_env(env_path: str):
    updates = {
        "SUPABASE_HOST": POSTGRES_HOST,
        "SUPABASE_PORT": str(POSTGRES_PORT),
        "SUPABASE_USER": POSTGRES_USER,
        "SUPABASE_PASSWORD": POSTGRES_PASSWORD,
        "SUPABASE_DATABASE": POSTGRES_DATABASE,
    }

    env_file = pathlib.Path(env_path)
    if env_file.exists():
        lines = env_file.read_text(encoding="utf-8").splitlines()
    else:
        lines = []

    existing_keys = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            new_lines.append(line)
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in updates:
            new_lines.append(f"{key}={updates[key]}")
            existing_keys.add(key)
        else:
            new_lines.append(line)

    for key, value in updates.items():
        if key not in existing_keys:
            new_lines.append(f"{key}={value}")

    env_file.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    print(f"✅ .env updated: {env_path}")
    for key, value in updates.items():
        masked = value if "PASSWORD" not in key else "***"
        print(f"   {key}={masked}")


def run_realtime():
    global cur

    tables = [
        "public.flights",
        "public.seismics",
        "public.weather",
        "public.flights_tumbling",
        "public.seismics_tumbling",
        "public.weather_tumbling",
        "public.flight_context",
        "public.raw_airports",
        "public.raw_airlines",
        "public.raw_planes",
        "public.raw_routes",
        "staging.stg_flights",
        "staging.stg_seismics",
        "staging.stg_weather",
        "staging.stg_airports",
        "staging.stg_airlines",
        "staging.stg_routes",
        "intermediate.int_airport_grid",
        "intermediate.int_flights_enriched",
        "mart.mart_flight_activity",
        "mart.mart_weather_conditions",
        "mart.mart_seismic_activity",
        "mart.mart_flight_context",
    ]

    for table in tables:
        _, tname = table.split(".")

        cur.execute(
            """
            SELECT 1 FROM pg_publication_tables
            WHERE pubname = 'supabase_realtime'
              AND schemaname = 'public'
              AND tablename = %s
        """,
            (tname,),
        )

        if cur.fetchone() is None:
            cur.execute(f"ALTER PUBLICATION supabase_realtime ADD TABLE {table}")
            print(f"  -> Realtime enabled: {table}")
        else:
            print(f"  -> Already realtime: {table}")

    print("✅ Realtime configured")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(script_dir, "..", "..", ".env")

    wait_for_db()
    run_migrations()
    run_rls()
    run_realtime()
    update_env(env_path)

    cur.close()
    conn.close()
