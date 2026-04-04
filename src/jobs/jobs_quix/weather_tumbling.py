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
TOPIC_IN       = os.getenv("TOPIC_WEATHER", "weather-feeds")

PG_HOST        = os.getenv("SUPABASE_HOST")
PG_PORT        = int(os.getenv("SUPABASE_PORT", "5432"))
PG_USER        = os.getenv("SUPABASE_USER", "postgres")
PG_PASS        = os.getenv("SUPABASE_PASSWORD")
PG_DB          = os.getenv("SUPABASE_DATABASE", "postgres")

WINDOW_SECONDS = 5 * 60
WATERMARK_S    = 60

def upsert_window(rows: list[dict]) -> None:
    if not rows:
        return
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASS, dbname=PG_DB
    )
    conn.autocommit = True
    sql = """
        INSERT INTO weather_tumbling
            (window_start, window_end, region_name,
             avg_temperature_c, min_temperature_c, max_temperature_c,
             avg_windspeed_ms, avg_windgusts_ms, avg_visibility_m,
             avg_humidity_pct, total_precip_mm, snapshot_count)
        VALUES
            (%(window_start)s, %(window_end)s, %(region_name)s,
             %(avg_temperature_c)s, %(min_temperature_c)s, %(max_temperature_c)s,
             %(avg_windspeed_ms)s, %(avg_windgusts_ms)s, %(avg_visibility_m)s,
             %(avg_humidity_pct)s, %(total_precip_mm)s, %(snapshot_count)s)
        ON CONFLICT (window_start, window_end, region_name)
        DO UPDATE SET
            avg_temperature_c = EXCLUDED.avg_temperature_c,
            min_temperature_c = EXCLUDED.min_temperature_c,
            max_temperature_c = EXCLUDED.max_temperature_c,
            avg_windspeed_ms  = EXCLUDED.avg_windspeed_ms,
            avg_windgusts_ms  = EXCLUDED.avg_windgusts_ms,
            avg_visibility_m  = EXCLUDED.avg_visibility_m,
            avg_humidity_pct  = EXCLUDED.avg_humidity_pct,
            total_precip_mm   = EXCLUDED.total_precip_mm,
            snapshot_count    = EXCLUDED.snapshot_count
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        print(f"[weather_tumbling] upserted {len(rows)} rows")
    finally:
        conn.close()


def _safe_float(val, default=0.0) -> float:
    try:
        v = float(val)
        return v if not math.isnan(v) else default
    except (TypeError, ValueError):
        return default


def init_agg(msg: dict) -> dict:
    acc = {
        "region_name": msg.get("region_name", "unknown"),
        "temp_sum": 0.0, "temp_min": float("inf"), "temp_max": float("-inf"), "temp_n": 0,
        "wind_sum": 0.0, "wind_n": 0,
        "gust_sum": 0.0, "gust_n": 0,
        "vis_sum":  0.0, "vis_n":  0,
        "hum_sum":  0.0, "hum_n":  0,
        "precip_total": 0.0,
        "count": 0,
    }
    return reduce_agg(acc, msg)


def reduce_agg(acc: dict, msg: dict) -> dict:
    acc["count"] += 1

    t = _safe_float(msg.get("temperature_c"))
    acc["temp_sum"] += t
    acc["temp_min"]  = min(acc["temp_min"], t)
    acc["temp_max"]  = max(acc["temp_max"], t)
    acc["temp_n"]   += 1

    for key, field in [("wind_sum", "windspeed_ms"), ("gust_sum", "windgusts_ms"),
                       ("vis_sum", "visibility_m"),  ("hum_sum", "humidity_pct")]:
        n_key = key.replace("sum", "n")
        v = _safe_float(msg.get(field))
        acc[key]   += v
        acc[n_key] += 1

    acc["precip_total"] += _safe_float(msg.get("precipitation_mm"))
    return acc


def _ms_to_ts(ms: int):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def run():
    app = Application(
        broker_address=BROKER,
        consumer_group="weather-tumbling-qx",
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
        timestamp_extractor=lambda val, *_: int(val.get("snapshot_ts", 0)) * 1000,
    )

    sdf = app.dataframe(topic)

    sdf = (
        sdf
        .filter(lambda m: m.get("region_name") is not None)
        .group_by(lambda m: m["region_name"], name="region_name")
        .tumbling_window(
            duration_ms=WINDOW_SECONDS * 1000,
            grace_ms=WATERMARK_S * 1000,
        )
        .reduce(reducer=reduce_agg, initializer=init_agg)
        .current()
    )

    def emit(result):
        acc = result["value"]
        n   = acc["temp_n"]
        upsert_window([{
            "window_start":     _ms_to_ts(result["start"]),
            "window_end":       _ms_to_ts(result["end"]),
            "region_name":      acc["region_name"],
            "avg_temperature_c": acc["temp_sum"] / n if n else None,
            "min_temperature_c": acc["temp_min"] if n else None,
            "max_temperature_c": acc["temp_max"] if n else None,
            "avg_windspeed_ms":  acc["wind_sum"] / acc["wind_n"] if acc["wind_n"] else None,
            "avg_windgusts_ms":  acc["gust_sum"] / acc["gust_n"] if acc["gust_n"] else None,
            "avg_visibility_m":  acc["vis_sum"]  / acc["vis_n"]  if acc["vis_n"]  else None,
            "avg_humidity_pct":  acc["hum_sum"]  / acc["hum_n"]  if acc["hum_n"]  else None,
            "total_precip_mm":   acc["precip_total"],
            "snapshot_count":    acc["count"],
        }])

    sdf.update(emit)

    print(f"[weather_tumbling] starting — broker={BROKER} topic={TOPIC_IN}")
    app.run(sdf)


if __name__ == "__main__":
    run()
