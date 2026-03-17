"""
shared/schema_mapping.py
Maps raw OpenSky API state-vector arrays to named, typed dictionaries.

OpenSky returns states as lists:
  [icao24, callsign, origin_country, time_position, last_contact,
   longitude, latitude, baro_altitude, on_ground, velocity,
   true_track, vertical_rate, sensors, geo_altitude, squawk,
   spi, position_source]
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# Field index constants (OpenSky REST API v2)
ICAO24          = 0
CALLSIGN        = 1
ORIGIN_COUNTRY  = 2
TIME_POSITION   = 3
LAST_CONTACT    = 4
LONGITUDE       = 5
LATITUDE        = 6
BARO_ALTITUDE   = 7
ON_GROUND       = 8
VELOCITY        = 9
TRUE_TRACK      = 10
VERTICAL_RATE   = 11
SENSORS         = 12   # array of integers — ignored
GEO_ALTITUDE    = 13
SQUAWK          = 14
SPI             = 15
POSITION_SOURCE = 16


# ─────────────────────────────────────────────────────────────────────────────
# Classification helpers
# ─────────────────────────────────────────────────────────────────────────────

def classify_altitude(altitude_m: Optional[float]) -> str:
    """Return a human-readable altitude band."""
    if altitude_m is None:
        return "unknown"
    if altitude_m <= 0:
        return "ground"
    if altitude_m < 1_000:
        return "low"
    if altitude_m < 6_000:
        return "medium"
    if altitude_m < 12_000:
        return "high"
    return "very_high"


def classify_flight_region(lon: Optional[float], lat: Optional[float]) -> str:
    """Coarse geographic region from lon/lat."""
    if lon is None or lat is None:
        return "unknown"
    if lat > 60:
        return "arctic"
    if lat < -60:
        return "antarctic"
    if -30 < lon < 60 and 0 < lat < 60:
        return "europe_africa"
    if 60 < lon < 150 and 0 < lat < 60:
        return "asia_pacific"
    if -180 < lon < -30 and 0 < lat < 60:
        return "north_america"
    if -90 < lon < -30 and -60 < lat < 0:
        return "south_america"
    return "other"


# ─────────────────────────────────────────────────────────────────────────────
# Main mapping function
# ─────────────────────────────────────────────────────────────────────────────

def state_vector_to_dict(state: List[Any], event_timestamp: int) -> Dict[str, Any]:
    """
    Convert one OpenSky state-vector list into a clean dictionary
    suitable for JSON serialisation and downstream processing.
    """
    lon = _safe_float(state, LONGITUDE)
    lat = _safe_float(state, LATITUDE)
    baro = _safe_float(state, BARO_ALTITUDE)
    vel = _safe_float(state, VELOCITY)

    return {
        "icao24":          _safe_str(state, ICAO24),
        "callsign":        _safe_str(state, CALLSIGN, strip=True),
        "origin_country":  _safe_str(state, ORIGIN_COUNTRY),
        "time_position":   _safe_int(state, TIME_POSITION),
        "last_contact":    _safe_int(state, LAST_CONTACT),
        "longitude":       lon,
        "latitude":        lat,
        "baro_altitude":   baro,
        "geo_altitude":    _safe_float(state, GEO_ALTITUDE),
        "on_ground":       bool(state[ON_GROUND]) if len(state) > ON_GROUND else None,
        "velocity":        vel,
        "velocity_kmh":    round(vel * 3.6, 2) if vel is not None else None,
        "true_track":      _safe_float(state, TRUE_TRACK),
        "vertical_rate":   _safe_float(state, VERTICAL_RATE),
        "squawk":          _safe_str(state, SQUAWK),
        "spi":             bool(state[SPI]) if len(state) > SPI else None,
        "position_source": _safe_int(state, POSITION_SOURCE),
        "altitude_level":  classify_altitude(baro),
        "flight_region":   classify_flight_region(lon, lat),
        "event_timestamp": event_timestamp,
    }


def batch_state_vectors(
    states: List[List[Any]], event_timestamp: int
) -> List[Dict[str, Any]]:
    """Convert a full batch of state vectors, skipping nulls and ground-only."""
    records = []
    for s in states:
        if s is None or s[ICAO24] is None:
            continue
        records.append(state_vector_to_dict(s, event_timestamp))
    return records


# ─────────────────────────────────────────────────────────────────────────────
# Safe accessors
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(lst: List, idx: int) -> Optional[float]:
    try:
        val = lst[idx]
        return float(val) if val is not None else None
    except (IndexError, TypeError, ValueError):
        return None


def _safe_int(lst: List, idx: int) -> Optional[int]:
    try:
        val = lst[idx]
        return int(val) if val is not None else None
    except (IndexError, TypeError, ValueError):
        return None


def _safe_str(lst: List, idx: int, strip: bool = False) -> Optional[str]:
    try:
        val = lst[idx]
        if val is None:
            return None
        s = str(val)
        return s.strip() if strip else s
    except (IndexError, TypeError):
        return None
