"""Read-only JSON API over the SkyStream Postgres serving tables, consumed by the dashboard. Times are epoch seconds."""
import os

import psycopg
from fastapi import FastAPI, Query
from psycopg.rows import dict_row

CONNINFO = (f"host={os.getenv('PGHOST', 'postgres')} dbname={os.getenv('PGDATABASE', 'skystream')} "
            f"user={os.getenv('PGUSER', 'skystream')} password={os.getenv('PGPASSWORD', 'skystream')}")
LIVE_WINDOW_S = 900  # an aircraft counts as live if seen in the last 15 minutes (three captures)
TODAY_IST = "extract(epoch FROM date_trunc('day', now() AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata')"

app = FastAPI(title="SkyStream API", version="0.1.0")


def query(sql: str, params: tuple = ()) -> list:
    with psycopg.connect(CONNINFO, row_factory=dict_row) as conn:
        return conn.execute(sql, params).fetchall()


@app.get("/api/health")
def health():
    row = query("SELECT max(capture_ts) AS last_capture FROM v_captures")[0]
    return {"ok": True, "last_capture": row["last_capture"]}


@app.get("/api/live")
def live():
    """Every aircraft seen in the last 15 minutes, with its type and engine when the aircraft database knows it."""
    aircraft = query(
        "SELECT p.icao24, p.callsign, p.airline_icao, p.latitude, p.longitude, p.baro_altitude, p.velocity, "
        "p.true_track, p.vertical_rate, p.on_ground, p.capture_ts, a.registration, a.typecode, e.engine, e.maker "
        "FROM live_positions p LEFT JOIN aircraft a USING (icao24) LEFT JOIN v_aircraft_engine e USING (icao24) "
        "WHERE p.capture_ts > extract(epoch FROM now()) - %s", (LIVE_WINDOW_S,))
    return {"capture_ts": max((a["capture_ts"] for a in aircraft), default=None), "aircraft": aircraft}


@app.get("/api/summary")
def summary():
    return query(
        f"SELECT (SELECT count(*) FROM live_positions WHERE NOT on_ground "
        f"         AND capture_ts > extract(epoch FROM now()) - %s) AS airborne, "
        f"       (SELECT count(*) FROM flight_events WHERE event = 'takeoff' AND ts >= {TODAY_IST}) AS takeoffs_today, "
        f"       (SELECT count(*) FROM flight_events WHERE event = 'landing' AND ts >= {TODAY_IST}) AS landings_today, "
        f"       (SELECT count(*) FROM v_takeoff_weather WHERE hot_day AND ts >= {TODAY_IST}) AS hot_day_takeoffs_today, "
        f"       (SELECT count(*) FROM v_captures WHERE capture_ts >= {TODAY_IST}) AS captures_today, "
        f"       (SELECT max(capture_ts) FROM v_captures) AS last_capture", (LIVE_WINDOW_S,))[0]


@app.get("/api/weather")
def weather():
    return query("SELECT ap.iata, ap.city, w.icao, w.obs_time, w.temp_c, w.wind_dir, w.wind_kt, w.visibility_m, "
                 "w.wx, w.raw FROM airport_weather w JOIN airports ap ON ap.icao = w.icao ORDER BY ap.iata")


@app.get("/api/events")
def events(limit: int = Query(30, ge=1, le=200)):
    return query("SELECT e.*, a.registration, a.typecode FROM flight_events e LEFT JOIN aircraft a USING (icao24) "
                 "ORDER BY e.ts DESC LIMIT %s", (limit,))


@app.get("/api/engines")
def engines():
    return query("SELECT engine, maker, cycles, flight_hours::float AS flight_hours FROM v_engine_utilisation_today")


@app.get("/api/captures")
def captures(limit: int = Query(36, ge=1, le=288)):
    rows = query("SELECT capture_ts, aircraft, airborne FROM v_captures ORDER BY capture_ts DESC LIMIT %s", (limit,))
    return rows[::-1]
