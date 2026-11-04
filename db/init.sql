-- SkyStream serving schema. Times are Unix epoch seconds (UTC).

CREATE TABLE airports (
    iata text PRIMARY KEY, icao text UNIQUE NOT NULL, city text, lat double precision, lon double precision
);
COPY airports FROM '/reference/airports.csv' WITH (FORMAT csv, HEADER true);

-- engine family by operator + ICAO type code; operator '*' is the default for a type
CREATE TABLE engine_map (
    operator_icao text, typecode text, engine text, maker text, PRIMARY KEY (operator_icao, typecode)
);
COPY engine_map FROM '/reference/engine_map.csv' WITH (FORMAT csv, HEADER true);

-- latest known state per aircraft, upserted every capture
CREATE TABLE live_positions (
    icao24 text PRIMARY KEY, callsign text, airline_icao text, origin_country text,
    latitude double precision, longitude double precision, baro_altitude double precision,
    geo_altitude double precision, velocity double precision, true_track double precision,
    vertical_rate double precision, on_ground boolean, squawk text,
    time_position bigint, last_contact bigint, capture_ts bigint, updated_at timestamptz DEFAULT now()
);

CREATE TABLE flight_events (
    icao24 text, callsign text, airline_icao text, event text CHECK (event IN ('takeoff', 'landing')),
    ts bigint, airport text, latitude double precision, longitude double precision,
    PRIMARY KEY (icao24, event, ts)
);
CREATE INDEX ON flight_events (ts);

CREATE TABLE airport_weather (
    icao text PRIMARY KEY, obs_time bigint, temp_c real, dewpoint_c real, wind_dir text, wind_kt real,
    visibility_m real, wx text, raw text, updated_at timestamptz DEFAULT now()
);
CREATE TABLE airport_weather_obs (LIKE airport_weather, PRIMARY KEY (icao, obs_time));

-- one row per Spark batch and capture; keyed by batch_id so a replayed batch is not counted twice
CREATE TABLE capture_batches (
    batch_id bigint, capture_ts bigint, aircraft int, airborne int, PRIMARY KEY (batch_id, capture_ts)
);
CREATE VIEW v_captures AS
SELECT capture_ts, sum(aircraft)::int AS aircraft, sum(airborne)::int AS airborne
FROM capture_batches GROUP BY capture_ts;

CREATE TABLE aircraft (
    icao24 text PRIMARY KEY, registration text, typecode text, model text, manufacturer text,
    operator text, operator_icao text, built text
);

-- each landing paired with the same aircraft's previous takeoff = one flight leg
CREATE VIEW v_flights AS
SELECT icao24, callsign, airline_icao, dep_airport, airport AS arr_airport, takeoff_ts, ts AS landing_ts,
       (ts - takeoff_ts) / 3600.0 AS air_hours
FROM (SELECT e.*, lag(event) OVER w AS prev_event, lag(ts) OVER w AS takeoff_ts, lag(airport) OVER w AS dep_airport
      FROM flight_events e WINDOW w AS (PARTITION BY icao24 ORDER BY ts)) x
WHERE event = 'landing' AND prev_event = 'takeoff' AND ts - takeoff_ts < 18 * 3600;

CREATE VIEW v_aircraft_engine AS
SELECT a.icao24, a.registration, a.typecode, a.operator_icao,
       COALESCE(own.engine, dflt.engine) AS engine, COALESCE(own.maker, dflt.maker) AS maker
FROM aircraft a
LEFT JOIN engine_map own ON own.typecode = a.typecode AND own.operator_icao = a.operator_icao
LEFT JOIN engine_map dflt ON dflt.typecode = a.typecode AND dflt.operator_icao = '*';

-- weather at the departure airport at takeoff: the latest METAR issued in the 90 minutes before
CREATE VIEW v_takeoff_weather AS
SELECT t.icao24, t.callsign, t.airline_icao, t.airport, t.ts, w.temp_c, w.visibility_m, w.wx,
       w.temp_c >= 30 AS hot_day
FROM flight_events t
JOIN airports ap ON ap.iata = t.airport
LEFT JOIN LATERAL (
    SELECT * FROM airport_weather_obs o
    WHERE o.icao = ap.icao AND o.obs_time BETWEEN t.ts - 5400 AND t.ts
    ORDER BY o.obs_time DESC LIMIT 1) w ON true
WHERE t.event = 'takeoff';

CREATE VIEW v_engine_utilisation_today AS
SELECT e.engine, e.maker, count(*) AS cycles, round(sum(f.air_hours)::numeric, 1) AS flight_hours
FROM v_flights f JOIN v_aircraft_engine e USING (icao24)
WHERE f.landing_ts >= extract(epoch FROM date_trunc('day', now() AT TIME ZONE 'Asia/Kolkata') AT TIME ZONE 'Asia/Kolkata')
GROUP BY e.engine, e.maker
ORDER BY flight_hours DESC;

CREATE VIEW v_airborne_now AS
SELECT COALESCE(airline_icao, 'other') AS airline_icao, count(*) AS airborne
FROM live_positions
WHERE NOT on_ground AND capture_ts > extract(epoch FROM now()) - 900
GROUP BY 1;
