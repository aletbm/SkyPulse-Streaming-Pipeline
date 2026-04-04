import os
import math
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

BROKER         = os.getenv("REDPANDA_SERVER", "localhost:9092")
USERNAME       = os.getenv("REDPANDA_USERNAME", "")
PASSWORD       = os.getenv("REDPANDA_PASSWORD", "")
TOPIC_IN       = os.getenv("TOPIC_SEISMIC", "earthquake-feeds")

PG_HOST        = os.getenv("SUPABASE_HOST")
PG_PORT        = int(os.getenv("SUPABASE_PORT", "5432"))
PG_USER        = os.getenv("SUPABASE_USER", "postgres")
PG_PASS        = os.getenv("SUPABASE_PASSWORD")
PG_DB          = os.getenv("SUPABASE_DATABASE", "postgres")

WINDOW_SECONDS = 5 * 60
WATERMARK_S    = 30


def upsert_window(rows: list[dict]) -> None:
    if not rows:
        return
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASS, dbname=PG_DB
    )
    conn.autocommit = True
    sql = """
        INSERT INTO seismics_tumbling
            (window_start, window_end, region,
             avg_magnitude, max_magnitude, event_count, tsunami_count, avg_depth)
        VALUES
            (%(window_start)s, %(window_end)s, %(region)s,
             %(avg_magnitude)s, %(max_magnitude)s, %(event_count)s,
             %(tsunami_count)s, %(avg_depth)s)
        ON CONFLICT (window_start, window_end, region)
        DO UPDATE SET
            avg_magnitude = EXCLUDED.avg_magnitude,
            max_magnitude = EXCLUDED.max_magnitude,
            event_count   = EXCLUDED.event_count,
            tsunami_count = EXCLUDED.tsunami_count,
            avg_depth     = EXCLUDED.avg_depth
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        print(f"[seismic_tumbling] upserted {len(rows)} rows")
    finally:
        conn.close()


def extract_region(place: str) -> str:
    """Mirror Flink CASE: text after last comma, or full place."""
    if place and ", " in place:
        return place.split(", ")[-1].strip()
    return place or "unknown"


def _safe_float(val, default=0.0) -> float:
    try:
        v = float(val)
        return v if not math.isnan(v) else default
    except (TypeError, ValueError):
        return default


def _ms_to_ts(ms: int):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def init_agg(msg: dict) -> dict:
    acc = {
        "region": extract_region(msg.get("place", "")),
        "mag_sum": 0.0, "mag_max": 0.0, "mag_n": 0,
        "tsunami_count": 0,
        "depth_sum": 0.0, "depth_n": 0,
        "event_count": 0,
    }
    return reduce_agg(acc, msg)


def reduce_agg(acc: dict, msg: dict) -> dict:
    acc["event_count"] += 1

    mag = _safe_float(msg.get("mag"))
    acc["mag_sum"] += mag
    acc["mag_max"]  = max(acc["mag_max"], mag)
    acc["mag_n"]   += 1

    acc["tsunami_count"] += int(msg.get("tsunami", 0) or 0)

    depth = _safe_float(msg.get("depth"))
    acc["depth_sum"] += depth
    acc["depth_n"]   += 1

    return acc


def run():
    app = Application(
        broker_address=BROKER,
        consumer_group="seismic-tumbling-qx",
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

    topic = app.topic(
        TOPIC_IN,
        value_deserializer="json",
        timestamp_extractor=lambda val, *_: int(val.get("event_time", 0)),
    )

    sdf = app.dataframe(topic)

    sdf = (
        sdf
        .group_by(lambda m: extract_region(m.get("place", "")), name="region")
        .tumbling_window(
            duration_ms=WINDOW_SECONDS * 1000,
            grace_ms=WATERMARK_S * 1000,
        )
        .reduce(reducer=reduce_agg, initializer=init_agg)
        .current()
    )

    def emit(result):
        acc = result["value"]
        upsert_window([{
            "window_start":  _ms_to_ts(result["start"]),
            "window_end":    _ms_to_ts(result["end"]),
            "region":        acc["region"],
            "avg_magnitude": acc["mag_sum"] / acc["mag_n"] if acc["mag_n"] else None,
            "max_magnitude": acc["mag_max"],
            "event_count":   acc["event_count"],
            "tsunami_count": acc["tsunami_count"],
            "avg_depth":     acc["depth_sum"] / acc["depth_n"] if acc["depth_n"] else None,
        }])

    sdf.update(emit)

    print(f"[seismic_tumbling] starting — broker={BROKER} topic={TOPIC_IN}")
    app.run(sdf)


if __name__ == "__main__":
    run()
