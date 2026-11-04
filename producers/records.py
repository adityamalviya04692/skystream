"""Pure record-shaping helpers for the producers (no Kafka or network imports, so they are unit-testable)."""
from __future__ import annotations

# OpenSky /states/all returns each aircraft as a positional list in this order
STATE_FIELDS = ("icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude", "latitude",
                "baro_altitude", "on_ground", "velocity", "true_track", "vertical_rate", "sensors", "geo_altitude",
                "squawk", "spi", "position_source")

# output field -> accepted CSV header names (OpenSky renamed headers between database releases)
AIRCRAFT_FIELDS = {
    "icao24": ("icao24",),
    "registration": ("registration",),
    "typecode": ("typecode",),
    "model": ("model",),
    "manufacturer": ("manufacturername",),
    "operator": ("operator",),
    "operator_icao": ("operatoricao",),
    "built": ("built",),
}


def state_to_record(state: list, capture_ts: int) -> dict:
    """Turn one OpenSky state vector into a named record stamped with the capture time."""
    rec = dict(zip(STATE_FIELDS, state))
    rec.pop("sensors", None)
    rec["callsign"] = (rec.get("callsign") or "").strip() or None
    rec["capture_ts"] = capture_ts
    return rec


def aircraft_row(row: dict) -> dict | None:
    """Normalise one aircraft-database CSV row; None unless the aircraft is Indian-registered (VT-)."""
    low = {k.strip().strip("'").lower(): (v or "").strip().strip("'")
           for k, v in row.items() if isinstance(k, str) and isinstance(v, (str, type(None)))}
    out = {name: next((low[h] for h in heads if low.get(h)), None) for name, heads in AIRCRAFT_FIELDS.items()}
    if not out["icao24"] or not (out["registration"] or "").upper().startswith("VT-"):
        return None
    out["icao24"] = out["icao24"].lower()
    out["registration"] = out["registration"].upper()
    return out
