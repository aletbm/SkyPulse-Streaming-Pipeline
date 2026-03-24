import dataclasses
import json
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class Earthquake:
    mag: float
    mag_type: str
    place: str
    tsunami: int
    event_time: int
    event_type: str
    title: str
    sig: int
    lat: float
    lon: float
    depth: float


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


def earthquake_deserializer(data: bytes) -> Earthquake | None:
    try:
        return Earthquake(**json.loads(data.decode("utf-8")))
    except Exception as e:
        from logger import get_logger

        get_logger(__name__).warning(f"failed to deserialize message: {e}")
        return None


def parse_earthquake(feature: dict) -> Earthquake | None:
    try:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]
        return Earthquake(
            mag=safe_float(props.get("mag")),
            mag_type=str(props.get("magType") or "unknown"),
            place=str(props.get("place") or "unknown"),
            tsunami=safe_int(props.get("tsunami")),
            event_time=safe_int(props.get("time")),
            event_type=str(props.get("type") or "unknown"),
            title=str(props.get("title") or ""),
            sig=safe_int(props.get("sig")),
            lon=safe_float(coords[0]),
            lat=safe_float(coords[1]),
            depth=safe_float(coords[2]),
        )
    except Exception as e:
        from logger import get_logger

        get_logger(__name__).warning(f"failed to parse {feature.get('id')}: {e}")
        return None


def earthquake_serializer(earthquake: Earthquake) -> bytes:
    return json.dumps(dataclasses.asdict(earthquake)).encode("utf-8")


def ts_to_str(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
