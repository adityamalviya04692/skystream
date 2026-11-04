"""Pure transformation logic used by the Spark job (no Spark imports, so it is unit-testable)."""
from __future__ import annotations

import csv
import math
import re

# ICAO airline prefix of the callsign -> airline (Indian domestic operators)
AIRLINES = {"IGO": "IndiGo", "AIC": "Air India", "AXB": "Air India Express", "AKJ": "Akasa Air",
            "SEJ": "SpiceJet", "LLR": "Alliance Air"}
SM_TO_M = 1609.344
MAX_VISIBILITY_M = 9999.0  # METAR reports "6+" statute miles, i.e. 10 km or more
# metric visibility group in the raw report: the 4 digits after the wind (and optional variable-wind) group
RAW_VISIBILITY = re.compile(r"\d{2,3}(?:G\d{2,3})?(?:KT|MPS)\s+(?:\d{3}V\d{3}\s+)?(\d{4})\b")


def airline_of(callsign: str | None) -> str | None:
    prefix = (callsign or "")[:3].upper()
    return prefix if prefix in AIRLINES else None


def load_airports(path: str) -> list[dict]:
    with open(path, newline="") as f:
        return [{**row, "lat": float(row["lat"]), "lon": float(row["lon"])} for row in csv.DictReader(f)]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = math.radians
    h = math.sin(r(lat2 - lat1) / 2) ** 2 + math.cos(r(lat1)) * math.cos(r(lat2)) * math.sin(r(lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(h))


def nearest_airport(lat: float | None, lon: float | None, airports: list[dict], max_km: float = 15.0) -> str | None:
    """IATA code of the closest tracked airport within max_km, else None."""
    if lat is None or lon is None:
        return None
    best = min(airports, key=lambda a: haversine_km(lat, lon, a["lat"], a["lon"]))
    return best["iata"] if haversine_km(lat, lon, best["lat"], best["lon"]) <= max_km else None


def visibility_m(visib) -> float | None:
    """AviationWeather 'visib' is statute miles: a number, a fraction like '1/2', or '6+'."""
    if visib in (None, ""):
        return None
    s = str(visib).strip()
    if s.endswith("+"):
        return MAX_VISIBILITY_M
    try:
        miles = float(s.split("/")[0]) / float(s.split("/")[1]) if "/" in s else float(s)
    except (ValueError, ZeroDivisionError):
        return None
    return min(round(miles * SM_TO_M), MAX_VISIBILITY_M)


def metar_row(obs: dict) -> dict:
    """Flatten one AviationWeather METAR JSON object into the airport_weather columns."""
    wdir = obs.get("wdir")
    # Indian METARs report visibility in metres; the JSON 'visib' is that value rounded through statute miles
    raw_vis = RAW_VISIBILITY.search(obs.get("rawOb") or "")
    return {
        "icao": obs.get("icaoId"),
        "obs_time": int(obs["obsTime"]) if obs.get("obsTime") is not None else None,
        "temp_c": obs.get("temp"),
        "dewpoint_c": obs.get("dewp"),
        "wind_dir": None if wdir is None else str(wdir),  # degrees, or "VRB" for variable
        "wind_kt": obs.get("wspd"),
        "visibility_m": float(raw_vis.group(1)) if raw_vis else visibility_m(obs.get("visib")),
        "wx": obs.get("wxString"),
        "raw": obs.get("rawOb"),
    }


def _ts(rec: dict) -> int:
    return int(rec.get("time_position") or rec.get("last_contact") or rec["capture_ts"])


def detect_events(prev: dict, rows: list[dict], airports: list[dict], max_gap_s: int = 1800):
    """Find takeoffs and landings by comparing each aircraft's on_ground flag with its previous capture.

    prev maps icao24 -> {"on_ground", "ts", "lat", "lon"} from the last known state. Rows are processed in time
    order, so a batch holding several captures still catches every flip. A flip after a gap longer than max_gap_s
    is ignored, because the aircraft may have flown a whole leg out of receiver coverage.
    Returns (events, latest_row_per_aircraft).
    """
    state, latest, events = dict(prev), {}, []
    for r in sorted(rows, key=_ts):
        ts, p, now_ground = _ts(r), state.get(r["icao24"]), r.get("on_ground")
        if (p and p["on_ground"] is not None and now_ground is not None and p["on_ground"] != now_ground
                and (not p["ts"] or ts - p["ts"] <= max_gap_s)):
            # a takeoff happened where the aircraft was last seen on the ground; a landing where it is now
            kind, lat, lon = ("takeoff", p["lat"], p["lon"]) if p["on_ground"] else ("landing", r["latitude"], r["longitude"])
            events.append({"icao24": r["icao24"], "callsign": r.get("callsign"), "airline_icao": airline_of(r.get("callsign")),
                           "event": kind, "ts": ts, "airport": nearest_airport(lat, lon, airports),
                           "latitude": lat, "longitude": lon})
        state[r["icao24"]] = {"on_ground": now_ground, "ts": ts, "lat": r.get("latitude"), "lon": r.get("longitude")}
        latest[r["icao24"]] = r
    return events, list(latest.values())
