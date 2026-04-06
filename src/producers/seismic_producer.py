import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import os
import time

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

from logger import get_logger
from models.seismic import earthquake_serializer, parse_earthquake, safe_int, ts_to_str

load_dotenv()

DAY_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
HOUR_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
INTERVAL = 900
SERVER = os.getenv("REDPANDA_SERVER", "localhost:9092")
REDPANDA_USERNAME = os.getenv("REDPANDA_USERNAME", "")
REDPANDA_PASSWORD = os.getenv("REDPANDA_PASSWORD", "")

TOPIC_NAME = os.getenv("TOPIC_SEISMIC", "earthquake-feeds")

STATE_DIR = Path(__file__).parent / "misc"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "state_seismic.json"

log = get_logger("seismic_producer")


def load_last_ts() -> int:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())["last_ts"]
    return 0


def save_last_ts(ts: int):
    STATE_FILE.write_text(json.dumps({"last_ts": ts}))


def get_earthquake(url: str) -> list | None:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()["features"]
    except requests.exceptions.Timeout:
        log.warning("request timeout — skipping cycle")
    except requests.exceptions.HTTPError as e:
        log.warning(f"HTTP error: {e} — skipping cycle")
    except Exception as e:
        log.error(f"unexpected error: {e}")
    return None


def send_features(features: list, last_ts: int) -> tuple[int, int]:
    count = 0

    features_sorted = sorted(
        features, key=lambda f: safe_int(f["properties"].get("time"))
    )

    for feature in features_sorted:
        event_ts = safe_int(feature["properties"].get("time"))
        if event_ts <= last_ts:
            continue
        earthquake = parse_earthquake(feature)
        if earthquake is None:
            continue
        producer.send(TOPIC_NAME, value=earthquake)
        last_ts = max(last_ts, event_ts)
        count += 1

    producer.flush()
    return last_ts, count


producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=earthquake_serializer,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=REDPANDA_USERNAME,
    sasl_plain_password=REDPANDA_PASSWORD,
)


def run():
    last_ts = load_last_ts()
    log.info(f"starting — last_ts: {last_ts}")

    features = get_earthquake(DAY_URL)
    if features:
        max_ts = max(safe_int(f["properties"].get("time")) for f in features)
        log.info(f"day feed — most recent: {ts_to_str(max_ts)}")
        last_ts, count = send_features(features, last_ts)
        save_last_ts(last_ts)
        log.info(f"initial load — {count} earthquakes sent")

    while True:
        features = get_earthquake(HOUR_URL)
        if features:
            max_ts = max(safe_int(f["properties"].get("time")) for f in features)
            log.info(f"hour feed — most recent: {ts_to_str(max_ts)}")
            last_ts, count = send_features(features, last_ts)
            save_last_ts(last_ts)
            log.info(f"cycle done — {count} new earthquakes sent")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    run()
