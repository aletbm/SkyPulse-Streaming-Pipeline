import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

from logger import get_logger
from models.flight import flight_serializer, parse_flight

load_dotenv()

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
USE_AUTH = os.getenv("USE_AUTH", "false").lower() == "true"

SERVER = os.getenv("REDPANDA_SERVER", "localhost:9092")
REDPANDA_USERNAME = os.getenv("REDPANDA_USERNAME", "")
REDPANDA_PASSWORD = os.getenv("REDPANDA_PASSWORD", "")
TOPIC_NAME = os.getenv("TOPIC_FLIGHTS", "flight-feeds")

INTERVAL = 120
MAX_POSITION_AGE = 60
TOKEN_REFRESH_MARGIN = 60

log = get_logger("flight_producer")


class TokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self) -> str:
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self) -> str:
        retries = 6

        for attempt in range(retries):
            try:
                r = requests.post(
                    TOKEN_URL,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": CLIENT_ID,
                        "client_secret": CLIENT_SECRET,
                    },
                    timeout=30,
                )
                r.raise_for_status()
                data = r.json()

                self.token = data["access_token"]
                expires_in = data.get("expires_in", 1800)
                self.expires_at = datetime.now() + timedelta(
                    seconds=expires_in - TOKEN_REFRESH_MARGIN
                )

                log.info("token refreshed")
                return self.token

            except Exception as e:
                wait = 2**attempt
                log.warning(f"retry {attempt + 1} failed — waiting {wait}s: {e}")
                time.sleep(wait)

        raise Exception("failed to refresh token after retries")

    def headers(self) -> dict:
        return {"Authorization": f"Bearer {self.get_token()}"}


def get_flights(tokens: TokenManager) -> dict | None:
    url = "https://opensky-network.org/api/states/all"

    params = {
        "extended": 1,
    }

    try:
        if USE_AUTH:
            log.info("using AUTH mode")

            response = requests.get(
                url,
                params=params,
                headers=tokens.headers(),
                timeout=120,
            )

        else:
            log.info("using NO-AUTH mode")

            response = requests.get(
                url,
                params=params,
                timeout=120,
            )

        if response.status_code == 429:
            retry_after = int(
                response.headers.get("X-Rate-Limit-Retry-After-Seconds", 60)
            )
            log.warning(f"rate limit — sleeping {retry_after}s")
            time.sleep(retry_after)
            return None

        response.raise_for_status()
        return response.json()

    except requests.exceptions.Timeout:
        log.warning("request timeout — skipping cycle")

    except requests.exceptions.HTTPError as e:
        log.warning(f"HTTP error: {e} — skipping cycle")

    except Exception as e:
        log.error(f"unexpected error: {e}")

    return None


producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=flight_serializer,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=REDPANDA_USERNAME,
    sasl_plain_password=REDPANDA_PASSWORD,
)


def run():
    tokens = TokenManager()
    log.info(f"starting flights producer — topic: {TOPIC_NAME}")

    while True:
        data = get_flights(tokens)

        if data is None or data.get("states") is None:
            log.warning("no data received — skipping cycle")
            time.sleep(INTERVAL)
            continue

        snapshot_ts = data["time"]
        cycle_count = 0
        skipped = 0

        for state in data["states"]:
            time_position = state[3]

            if time_position is None:
                skipped += 1
                continue

            if snapshot_ts - time_position > MAX_POSITION_AGE:
                skipped += 1
                continue

            flight = parse_flight(state)
            if flight is None:
                skipped += 1
                continue

            flight.last_seen = snapshot_ts

            producer.send(TOPIC_NAME, value=flight)
            cycle_count += 1

        producer.flush()
        log.info(f"cycle done — sent: {cycle_count} flights, skipped: {skipped}")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    run()
