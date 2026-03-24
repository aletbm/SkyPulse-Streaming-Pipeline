import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import time

import openmeteo_requests
import requests_cache
from dotenv import load_dotenv
from kafka import KafkaProducer
from retry_requests import retry

from logger import get_logger
from models.weather import parse_weather, weather_serializer

load_dotenv()

SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_WEATHER", "weather-feeds")
INTERVAL = 600

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

CURRENT_VARS = [
    "weathercode",
    "windspeed_10m",
    "winddirection_10m",
    "windgusts_10m",
    "precipitation",
    "rain",
    "snowfall",
    "showers",
    "snow_depth",
    "cloudcover",
    "cloudcover_low",
    "temperature_2m",
    "apparent_temperature",
    "relativehumidity_2m",
    "visibility",
    "surface_pressure",
]

log = get_logger(__name__)

CACHE_DIR = Path(__file__).parent / "misc"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "cache"

cache_session = requests_cache.CachedSession(CACHE_FILE, expire_after=300)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


def generate_grid():
    points = []

    # Base grid (medium density global coverage)
    for lat in range(-60, 76, 20):
        for lon in range(-180, 181, 20):
            points.append(
                {"name": f"grid_{lat}_{lon}", "lat": float(lat), "lon": float(lon)}
            )

    # High-density regions (areas with more air traffic / activity)
    high_density_regions = [
        (-50, 60, -130, -60),  # Americas
        (30, 70, -10, 40),  # Europe
        (10, 60, 60, 140),  # Asia
    ]

    for lat_min, lat_max, lon_min, lon_max in high_density_regions:
        for lat in range(lat_min, lat_max, 10):
            for lon in range(lon_min, lon_max, 10):
                points.append(
                    {"name": f"dense_{lat}_{lon}", "lat": float(lat), "lon": float(lon)}
                )

    # Polar regions (minimal coverage)
    for lat in [-80, 80]:
        for lon in range(-180, 181, 60):
            points.append(
                {"name": f"polar_{lat}_{lon}", "lat": float(lat), "lon": float(lon)}
            )

    unique_points = list({(p["lat"], p["lon"]): p for p in points}.values())

    return unique_points


def fetch_weather(point):
    try:
        params = {
            "latitude": point["lat"],
            "longitude": point["lon"],
            "current": CURRENT_VARS,
            "wind_speed_unit": "ms",
            "timezone": "UTC",
        }

        responses = openmeteo.weather_api(OPEN_METEO_URL, params=params)
        response = responses[0]

        current = response.Current()

        data = {
            "latitude": response.Latitude(),
            "longitude": response.Longitude(),
            "elevation": response.Elevation(),
            "current": {},
        }

        for i, var in enumerate(CURRENT_VARS):
            data["current"][var] = current.Variables(i).Value()

        data["current"]["time"] = current.Time()

        return data

    except Exception as e:
        log.warning(f"error for ({point['lat']}, {point['lon']}): {e}")
        return None


def run():

    log.info(
        f"starting weather producer — topic: {TOPIC_NAME} | grid: {len(GRID)} points"
    )

    while True:
        t0 = time.time()
        cycle_count = 0
        errors = 0

        for point in GRID:
            data = fetch_weather(point)

            if data is None:
                errors += 1
                continue

            weather = parse_weather(point["lat"], point["lon"], point["name"], data)

            if weather is None:
                errors += 1
                continue

            producer.send(TOPIC_NAME, value=weather)
            cycle_count += 1

            log.info(
                f"[{cycle_count}] {weather.region_name} | "
                f"temp={weather.temperature_c}°C | "
                f"wind={weather.windspeed_ms}m/s"
            )

            time.sleep(0.1)

        producer.flush()
        elapsed = round(time.time() - t0, 1)
        log.info(
            f"cycle done in {elapsed}s — sent: {cycle_count} | "
            f"errors: {errors} | total points: {len(GRID)}"
        )
        time.sleep(INTERVAL)


GRID = generate_grid()

producer = KafkaProducer(
    bootstrap_servers=[SERVER],
    value_serializer=weather_serializer,
)

if __name__ == "__main__":
    run()
