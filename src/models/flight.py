import json
import dataclasses
from dataclasses import dataclass
import pandas as pd


@dataclass
class Flight:
    icao24: str
    callsign: str
    origin_country: str
    time_position: int
    last_contact: int
    longitude: float
    latitude: float
    baro_altitude: float
    on_ground: bool
    velocity: float
    true_track: float
    category: int


def safe_int(val, default=0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def safe_float(val, default=0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_bool(val, default=False) -> bool:
    try:
        return bool(val)
    except (ValueError, TypeError):
        return default


def dict2series(state: list) -> pd.Series:
    return pd.Series(
        {
            "icao24": state[0],
            "callsign": state[1],
            "origin_country": state[2],
            "time_position": state[3],
            "last_contact": state[4],
            "longitude": state[5],
            "latitude": state[6],
            "baro_altitude": state[7],
            "on_ground": state[8],
            "velocity": state[9],
            "true_track": state[10],
            "category": state[17] if len(state) > 17 else None,
        }
    )


def flight_from_serie(serie: pd.Series) -> Flight:
    return Flight(
        icao24=str(serie["icao24"]),
        callsign=str(serie["callsign"] or "").strip(),
        origin_country=str(serie["origin_country"] or "unknown"),
        time_position=safe_int(serie["time_position"]),
        last_contact=safe_int(serie["last_contact"]),
        longitude=safe_float(serie["longitude"]),
        latitude=safe_float(serie["latitude"]),
        baro_altitude=safe_float(serie["baro_altitude"]),
        on_ground=safe_bool(serie["on_ground"]),
        velocity=safe_float(serie["velocity"]),
        true_track=safe_float(serie["true_track"]),
        category=safe_int(serie["category"]),
    )


def parse_flight(state: list) -> Flight | None:
    try:
        serie = dict2series(state)
        return flight_from_serie(serie)
    except Exception as e:
        from logger import get_logger

        get_logger(__name__).warning(
            f"failed to parse flight {state[0] if state else '?'}: {e}"
        )
        return None


def flight_serializer(flight: Flight) -> bytes:
    return json.dumps(dataclasses.asdict(flight)).encode("utf-8")


def flight_deserializer(data: bytes) -> Flight | None:
    try:
        return Flight(**json.loads(data.decode("utf-8")))
    except Exception as e:
        from logger import get_logger

        get_logger(__name__).warning(f"failed to deserialize flight: {e}")
        return None
