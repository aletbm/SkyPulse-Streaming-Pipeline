import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import time
import asyncio
import httpx
from dotenv import load_dotenv
from kafka import KafkaProducer

from logger import get_logger
from models.weather import parse_weather, weather_serializer

load_dotenv()

SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_WEATHER", "weather-feeds")
INTERVAL = 120
CONCURRENCY = 50

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CURRENT_VARS = (
    "weathercode,windspeed_10m,winddirection_10m,windgusts_10m,"
    "precipitation,rain,snowfall,showers,snow_depth,"
    "cloudcover,cloudcover_low,temperature_2m,apparent_temperature,"
    "relativehumidity_2m,visibility,surface_pressure"
)

log = get_logger(__name__)


def generate_grid() -> list[dict]:
    points = []

    for lat in range(-60, 76, 10):
        for lon in range(-180, 181, 10):
            points.append(
                {"name": f"grid_{lat}_{lon}", "lat": float(lat), "lon": float(lon)}
            )

    for lat in [-80, -70, 80, 85]:
        for lon in range(-180, 181, 20):
            points.append(
                {"name": f"grid_{lat}_{lon}", "lat": float(lat), "lon": float(lon)}
            )

    return points


async def fetch_point(
    client: httpx.AsyncClient, point: dict
) -> tuple[dict, dict | None]:
    try:
        response = await client.get(
            OPEN_METEO_URL,
            params={
                "latitude": point["lat"],
                "longitude": point["lon"],
                "current": CURRENT_VARS,
                "wind_speed_unit": "ms",
                "timezone": "UTC",
            },
        )
        response.raise_for_status()
        return point, response.json()
    except httpx.TimeoutException:
        log.warning(f"timeout for ({point['lat']}, {point['lon']}) — skipping")
    except httpx.HTTPStatusError as e:
        log.warning(
            f"HTTP error for ({point['lat']}, {point['lon']}): {e.response.status_code}"
        )
    except Exception as e:
        log.error(f"unexpected error for ({point['lat']}, {point['lon']}): {e}")
    return point, None


async def fetch_all(grid: list[dict]) -> list[tuple[dict, dict | None]]:
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def bounded_fetch(client, point):
        async with semaphore:
            return await fetch_point(client, point)

    async with httpx.AsyncClient(timeout=10) as client:
        tasks = [bounded_fetch(client, point) for point in grid]
        return await asyncio.gather(*tasks)


producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=weather_serializer,
)

GRID = generate_grid()

# --- Main ---
if __name__ == "__main__":
    log.info(
        f"starting weather producer — topic: {TOPIC_NAME} | grid: {len(GRID)} points | concurrency: {CONCURRENCY}"
    )

    while True:
        t0 = time.time()

        results = asyncio.run(fetch_all(GRID))

        cycle_count = 0
        errors = 0

        for point, data in results:
            if data is None:
                errors += 1
                continue

            weather = parse_weather(point["lat"], point["lon"], point["name"], data)
            if weather is None:
                errors += 1
                continue

            producer.send(TOPIC_NAME, value=weather)
            cycle_count += 1

        producer.flush()
        elapsed = round(time.time() - t0, 1)
        log.info(
            f"cycle done in {elapsed}s — sent: {cycle_count} | "
            f"errors: {errors} | total points: {len(GRID)}"
        )
        time.sleep(INTERVAL)
