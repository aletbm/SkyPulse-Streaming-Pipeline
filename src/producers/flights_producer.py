import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import time
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer

from logger import get_logger
from models.flight import parse_flight, flight_serializer

from dotenv import load_dotenv

load_dotenv()

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_FLIGHTS", "flight-feeds")
INTERVAL = 90
MAX_POSITION_AGE = 60
TOKEN_REFRESH_MARGIN = 30

log = get_logger(__name__)


class TokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self) -> str:
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self) -> str:
        try:
            r = requests.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                },
                timeout=10,
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
            log.error(f"failed to refresh token: {e}")
            raise

    def headers(self) -> dict:
        return {"Authorization": f"Bearer {self.get_token()}"}


def get_flights(tokens: TokenManager) -> dict | None:
    try:
        response = requests.get(
            "https://opensky-network.org/api/states/all",
            params={"extended": 1},
            headers=tokens.headers(),
            timeout=15,
        )

        if response.status_code == 429:
            retry_after = int(
                response.headers.get("X-Rate-Limit-Retry-After-Seconds", 3600)
            )
            log.warning(f"rate limit reached — sleeping {retry_after}s")
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
)


if __name__ == "__main__":
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

            producer.send(TOPIC_NAME, value=flight)
            cycle_count += 1

        producer.flush()
        log.info(f"cycle done — sent: {cycle_count} flights, skipped: {skipped}")
        time.sleep(INTERVAL)
