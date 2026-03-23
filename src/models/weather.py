import json
import hashlib
import dataclasses
from dataclasses import dataclass
from datetime import datetime, timezone
import pandas as pd


@dataclass
class Weather:
    snapshot_id: str
    region_name: str
    latitude: float
    longitude: float
    elevation_m: float
    weathercode: int
    interval_s: int
    windspeed_ms: float
    winddirection_deg: float
    windgusts_ms: float
    precipitation_mm: float
    rain_mm: float
    snowfall_cm: float
    showers_mm: float
    snow_depth_m: float
    cloudcover_pct: float
    cloudcover_low_pct: float
    temperature_c: float
    apparent_temperature_c: float
    humidity_pct: float
    visibility_m: float
    pressure_hpa: float
    snapshot_ts: int


def safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def make_snapshot_id(lat: float, lon: float, ts: int) -> str:
    raw = f"{lat:.1f}:{lon:.1f}:{ts // 900}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def dict2series(lat: float, lon: float, region_name: str, data: dict) -> pd.Series:
    current = data["current"]
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    return pd.Series(
        {
            "latitude": lat,
            "longitude": lon,
            "region_name": region_name,
            "elevation_m": data.get("elevation"),
            "weathercode": current.get("weathercode"),
            "interval_s": current.get("interval"),
            "windspeed_ms": current.get("windspeed_10m"),
            "winddirection_deg": current.get("winddirection_10m"),
            "windgusts_ms": current.get("windgusts_10m"),
            "precipitation_mm": current.get("precipitation"),
            "rain_mm": current.get("rain"),
            "snowfall_cm": current.get("snowfall"),
            "showers_mm": current.get("showers"),
            "snow_depth_m": current.get("snow_depth"),
            "cloudcover_pct": current.get("cloudcover"),
            "cloudcover_low_pct": current.get("cloudcover_low"),
            "temperature_c": current.get("temperature_2m"),
            "apparent_temperature_c": current.get("apparent_temperature"),
            "humidity_pct": current.get("relativehumidity_2m"),
            "visibility_m": current.get("visibility"),
            "pressure_hpa": current.get("surface_pressure"),
            "snapshot_ts": now_ts,
        }
    )


def weather_from_serie(serie: pd.Series) -> Weather:
    now_ts = safe_int(serie["snapshot_ts"])
    lat = safe_float(serie["latitude"])
    lon = safe_float(serie["longitude"])

    return Weather(
        snapshot_id=make_snapshot_id(lat, lon, now_ts),
        region_name=str(serie["region_name"]),
        latitude=lat,
        longitude=lon,
        elevation_m=safe_float(serie["elevation_m"]),
        weathercode=safe_int(serie["weathercode"]),
        interval_s=safe_int(serie["interval_s"]),
        windspeed_ms=safe_float(serie["windspeed_ms"]),
        winddirection_deg=safe_float(serie["winddirection_deg"]),
        windgusts_ms=safe_float(serie["windgusts_ms"]),
        precipitation_mm=safe_float(serie["precipitation_mm"]),
        rain_mm=safe_float(serie["rain_mm"]),
        snowfall_cm=safe_float(serie["snowfall_cm"]),
        showers_mm=safe_float(serie["showers_mm"]),
        snow_depth_m=safe_float(serie["snow_depth_m"]),
        cloudcover_pct=safe_float(serie["cloudcover_pct"]),
        cloudcover_low_pct=safe_float(serie["cloudcover_low_pct"]),
        temperature_c=safe_float(serie["temperature_c"]),
        apparent_temperature_c=safe_float(serie["apparent_temperature_c"]),
        humidity_pct=safe_float(serie["humidity_pct"]),
        visibility_m=safe_float(serie["visibility_m"]),
        pressure_hpa=safe_float(serie["pressure_hpa"]),
        snapshot_ts=now_ts,
    )


def parse_weather(
    lat: float, lon: float, region_name: str, data: dict
) -> Weather | None:
    try:
        serie = dict2series(lat, lon, region_name, data)
        return weather_from_serie(serie)
    except Exception as e:
        from logger import get_logger

        get_logger(__name__).warning(f"failed to parse weather for ({lat}, {lon}): {e}")
        return None


def weather_serializer(snapshot: Weather) -> bytes:
    return json.dumps(dataclasses.asdict(snapshot)).encode("utf-8")


def weather_deserializer(data: bytes) -> Weather | None:
    try:
        return Weather(**json.loads(data.decode("utf-8")))
    except Exception as e:
        from logger import get_logger

        get_logger(__name__).warning(f"failed to deserialize weather snapshot: {e}")
        return None
