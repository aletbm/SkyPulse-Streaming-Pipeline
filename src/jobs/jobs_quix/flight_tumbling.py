import os
import math
import psycopg2
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

BROKER         = os.getenv("REDPANDA_SERVER", "localhost:9092")
USERNAME       = os.getenv("REDPANDA_USERNAME", "")
PASSWORD       = os.getenv("REDPANDA_PASSWORD", "")
TOPIC_IN       = os.getenv("TOPIC_FLIGHTS", "flight-feeds")

PG_HOST        = os.getenv("SUPABASE_HOST")
PG_PORT        = int(os.getenv("SUPABASE_PORT", "5432"))
PG_USER        = os.getenv("SUPABASE_USER", "postgres")
PG_PASS        = os.getenv("SUPABASE_PASSWORD")
PG_DB          = os.getenv("SUPABASE_DATABASE", "postgres")

WINDOW_SECONDS = 5 * 60

def upsert_window(rows: list[dict]) -> None:
    """Batch-upsert aggregated window rows into flights_tumbling."""
    if not rows:
        return
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASS, dbname=PG_DB
    )
    conn.autocommit = True
    sql = """
        INSERT INTO flights_tumbling
            (window_start, window_end, origin_country,
             flight_count, airborne_count, avg_altitude_m, avg_velocity_ms)
        VALUES
            (%(window_start)s, %(window_end)s, %(origin_country)s,
             %(flight_count)s, %(airborne_count)s, %(avg_altitude_m)s, %(avg_velocity_ms)s)
        ON CONFLICT (window_start, window_end, origin_country)
        DO UPDATE SET
            flight_count    = EXCLUDED.flight_count,
            airborne_count  = EXCLUDED.airborne_count,
            avg_altitude_m  = EXCLUDED.avg_altitude_m,
            avg_velocity_ms = EXCLUDED.avg_velocity_ms
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        print(f"[flights_tumbling] upserted {len(rows)} rows")
    finally:
        conn.close()


def init_agg(msg: dict) -> dict:
    acc = {"origin_country": msg.get("origin_country", "unknown"),
           "flight_count": 0, "airborne_count": 0,
           "alt_sum": 0.0, "alt_n": 0,
           "vel_sum": 0.0, "vel_n": 0}
    return reduce_agg(acc, msg)


def reduce_agg(acc: dict, msg: dict) -> dict:
    if not acc.get("origin_country"):
        acc["origin_country"] = msg.get("origin_country", "unknown")
    acc["flight_count"] += 1
    if not msg.get("on_ground", True):
        acc["airborne_count"] += 1
    alt = msg.get("baro_altitude")
    if alt is not None and not math.isnan(float(alt)):
        acc["alt_sum"] += float(alt)
        acc["alt_n"]   += 1
    vel = msg.get("velocity")
    if vel is not None and not math.isnan(float(vel)):
        acc["vel_sum"] += float(vel)
        acc["vel_n"]   += 1
    return acc


def _ms_to_ts(ms: int):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def run():
    app = Application(
        broker_address=BROKER,
        consumer_group="flights-tumbling-qx",
        auto_offset_reset="latest",
        producer_extra_config={
            "security.protocol": "SASL_SSL",
            "sasl.mechanism":    "SCRAM-SHA-256",
            "sasl.username":     USERNAME,
            "sasl.password":     PASSWORD,
            "enable.idempotence": False,
            "acks": "1",
        },
        consumer_extra_config={
            "security.protocol": "SASL_SSL",
            "sasl.mechanism":    "SCRAM-SHA-256",
            "sasl.username":     USERNAME,
            "sasl.password":     PASSWORD,
        },
    )

    topic = app.topic(TOPIC_IN, value_deserializer="json")

    sdf = app.dataframe(topic)

    sdf = (
        sdf
        .filter(lambda m: m.get("origin_country") is not None)
        .group_by(lambda m: m["origin_country"], name="origin_country")
        .tumbling_window(duration_ms=WINDOW_SECONDS * 1000)
        .reduce(reducer=reduce_agg, initializer=init_agg)
        .current()
    )

    def emit(result):
        start_ms = result["start"]
        end_ms   = result["end"]
        acc      = result["value"]
        upsert_window([{
            "window_start":    _ms_to_ts(start_ms),
            "window_end":      _ms_to_ts(end_ms),
            "origin_country":  acc["origin_country"],
            "flight_count":    acc["flight_count"],
            "airborne_count":  acc["airborne_count"],
            "avg_altitude_m":  acc["alt_sum"] / acc["alt_n"] if acc["alt_n"] else None,
            "avg_velocity_ms": acc["vel_sum"] / acc["vel_n"] if acc["vel_n"] else None,
        }])

    sdf.update(emit)

    print(f"[flights_tumbling] starting — broker={BROKER} topic={TOPIC_IN}")
    app.run(sdf)


if __name__ == "__main__":
    run()
