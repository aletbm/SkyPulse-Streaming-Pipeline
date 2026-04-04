import os
import math
import time
import psycopg2
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()

BROKER        = os.getenv("REDPANDA_SERVER", "localhost:9092")
USERNAME      = os.getenv("REDPANDA_USERNAME", "")
PASSWORD      = os.getenv("REDPANDA_PASSWORD", "")
TOPIC_FLIGHTS = os.getenv("TOPIC_FLIGHTS", "flight-feeds")
TOPIC_SEISMIC = os.getenv("TOPIC_SEISMIC", "earthquake-feeds")
TOPIC_WEATHER = os.getenv("TOPIC_WEATHER", "weather-feeds")

PG_HOST = os.getenv("SUPABASE_HOST")
PG_PORT = int(os.getenv("SUPABASE_PORT", "5432"))
PG_USER = os.getenv("SUPABASE_USER", "postgres")
PG_PASS = os.getenv("SUPABASE_PASSWORD")
PG_DB   = os.getenv("SUPABASE_DATABASE", "postgres")

WINDOW_SECONDS = 5 * 60

_seismic_agg = defaultdict(lambda: {"eq_count": 0, "mag_max": 0.0,
                                     "tsunami_count": 0})
_weather_agg = defaultdict(lambda: {"temp_sum": 0.0, "gust_sum": 0.0,
                                     "vis_sum": 0.0, "n": 0})


def _grid(lat, lon):
    return (int(math.floor(float(lat) / 10) * 10),
            int(math.floor(float(lon) / 10) * 10))

def _now_bucket() -> int:
    ts_s = int(time.time())
    return (ts_s // WINDOW_SECONDS) * WINDOW_SECONDS

def _event_bucket(ts_ms: int) -> int:
    ts_s = ts_ms // 1000
    return (ts_s // WINDOW_SECONDS) * WINDOW_SECONDS

def _ms_to_ts(ms: int):
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc)

def _safe_float(val, default=0.0) -> float:
    try:
        v = float(val)
        return v if not math.isnan(v) else default
    except (TypeError, ValueError):
        return default


def acc_seismic(msg: dict):
    lat = msg.get("lat")
    lon = msg.get("lon")
    if lat is None or lon is None:
        return
    ts  = int(msg.get("event_time", 0))
    bucket = _event_bucket(ts) if ts else _now_bucket()
    key = (bucket, *_grid(lat, lon))
    a = _seismic_agg[key]
    a["eq_count"]      += 1
    a["mag_max"]        = max(a["mag_max"], _safe_float(msg.get("mag")))
    a["tsunami_count"] += int(msg.get("tsunami", 0) or 0)

def acc_weather(msg: dict):
    lat = msg.get("latitude")
    lon = msg.get("longitude")
    if lat is None or lon is None:
        return
    ts  = int(msg.get("snapshot_ts", 0))
    bucket = _event_bucket(ts * 1000) if ts else _now_bucket()
    key = (bucket, *_grid(lat, lon))
    a = _weather_agg[key]
    a["temp_sum"] += _safe_float(msg.get("temperature_c"))
    a["gust_sum"] += _safe_float(msg.get("windgusts_ms"))
    a["vis_sum"]  += _safe_float(msg.get("visibility_m"))
    a["n"]        += 1


def init_flight(msg: dict) -> dict:
    return reduce_flight({"flight_count": 0, "airborne_count": 0,
                          "alt_sum": 0.0, "alt_n": 0,
                          "grid": _grid(msg["latitude"], msg["longitude"]),
                          }, msg)

def reduce_flight(acc: dict, msg: dict) -> dict:
    if not acc.get("grid"):
        acc["grid"] = _grid(msg["latitude"], msg["longitude"])
    acc["flight_count"] += 1
    if not msg.get("on_ground", True):
        acc["airborne_count"] += 1
    alt = _safe_float(msg.get("baro_altitude"))
    if alt:
        acc["alt_sum"] += alt
        acc["alt_n"]   += 1
    return acc

def upsert_window(rows: list[dict]) -> None:
    if not rows:
        return
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASS, dbname=PG_DB
    )
    conn.autocommit = True
    sql = """
        INSERT INTO flight_context
            (window_start, window_end, grid_lat, grid_lon,
             flight_count, airborne_count, avg_altitude_m,
             nearby_eq_count, max_eq_magnitude, tsunami_count,
             avg_temperature_c, avg_windgusts_ms, avg_visibility_m)
        VALUES
            (%(window_start)s, %(window_end)s, %(grid_lat)s, %(grid_lon)s,
             %(flight_count)s, %(airborne_count)s, %(avg_altitude_m)s,
             %(nearby_eq_count)s, %(max_eq_magnitude)s, %(tsunami_count)s,
             %(avg_temperature_c)s, %(avg_windgusts_ms)s, %(avg_visibility_m)s)
        ON CONFLICT (window_start, window_end, grid_lat, grid_lon)
        DO UPDATE SET
            flight_count      = EXCLUDED.flight_count,
            airborne_count    = EXCLUDED.airborne_count,
            avg_altitude_m    = EXCLUDED.avg_altitude_m,
            nearby_eq_count   = EXCLUDED.nearby_eq_count,
            max_eq_magnitude  = EXCLUDED.max_eq_magnitude,
            tsunami_count     = EXCLUDED.tsunami_count,
            avg_temperature_c = EXCLUDED.avg_temperature_c,
            avg_windgusts_ms  = EXCLUDED.avg_windgusts_ms,
            avg_visibility_m  = EXCLUDED.avg_visibility_m
    """
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        print(f"[flight_context] upserted {len(rows)} rows")
    finally:
        conn.close()

def run():
    app = Application(
        broker_address=BROKER,
        consumer_group="flight-context-qx",
        auto_offset_reset="latest",
        producer_extra_config={
            "security.protocol":  "SASL_SSL",
            "sasl.mechanism":     "SCRAM-SHA-256",
            "sasl.username":      USERNAME,
            "sasl.password":      PASSWORD,
            "enable.idempotence": False,
            "acks":               "1",
        },
        consumer_extra_config={
            "security.protocol": "SASL_SSL",
            "sasl.mechanism":    "SCRAM-SHA-256",
            "sasl.username":     USERNAME,
            "sasl.password":     PASSWORD,
        },
    )

    t_flights = app.topic(TOPIC_FLIGHTS, value_deserializer="json")
    t_seismic = app.topic(TOPIC_SEISMIC, value_deserializer="json")
    t_weather = app.topic(TOPIC_WEATHER, value_deserializer="json")

    sdf_s = app.dataframe(t_seismic)
    sdf_s.update(acc_seismic)

    sdf_w = app.dataframe(t_weather)
    sdf_w.update(acc_weather)

    sdf_f = app.dataframe(t_flights)
    sdf_f = (
        sdf_f
        .filter(lambda m: m.get("latitude") is not None and m.get("longitude") is not None)
        .group_by(lambda m: _grid(m["latitude"], m["longitude"]), name="grid_cell")
        .tumbling_window(duration_ms=WINDOW_SECONDS * 1000, grace_ms=60 * 1000,)
        .reduce(reducer=reduce_flight, initializer=init_flight)
        .current()
    )

    def emit(result):
        acc      = result["value"]
        grid_lat, grid_lon = acc["grid"]

        win_start_ms = result["start"]
        win_end_ms   = result["end"]
        bucket       = win_start_ms // 1000

        sa = _seismic_agg.pop((bucket, grid_lat, grid_lon), {})
        wa = _weather_agg.pop((bucket, grid_lat, grid_lon), {})

        upsert_window([{
            "window_start":      _ms_to_ts(win_start_ms),
            "window_end":        _ms_to_ts(win_end_ms),
            "grid_lat":          grid_lat,
            "grid_lon":          grid_lon,
            "flight_count":      acc["flight_count"],
            "airborne_count":    acc["airborne_count"],
            "avg_altitude_m":    acc["alt_sum"] / acc["alt_n"] if acc["alt_n"] else None,
            "nearby_eq_count":   sa.get("eq_count", 0),
            "max_eq_magnitude":  sa.get("mag_max", 0.0),
            "tsunami_count":     sa.get("tsunami_count", 0),
            "avg_temperature_c": wa["temp_sum"] / wa["n"] if wa.get("n") else None,
            "avg_windgusts_ms":  wa["gust_sum"] / wa["n"] if wa.get("n") else None,
            "avg_visibility_m":  wa["vis_sum"]  / wa["n"] if wa.get("n") else None,
        }])

    sdf_f.update(emit)

    print(f"[flight_context] starting — broker={BROKER}")
    app.run()


if __name__ == "__main__":
    run()
