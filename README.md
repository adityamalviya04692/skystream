# SkyStream

Real-time domestic air traffic over India, built as a streaming lakehouse. Every 5 minutes, live aircraft positions
(OpenSky Network) and airport weather (AviationWeather.gov METAR) stream through Kafka into Spark Structured
Streaming, land in Delta Lake as bronze, silver and gold tables, and are served from Postgres to a dashboard.
Everything is free: free public APIs, open-source software, runs on a laptop.

```
OpenSky /states/all ─┐                                      ┌─ bronze (raw, replayable)
AviationWeather METAR┼─► Python producers ─► Kafka ─► Spark ┼─ silver (parsed, validated, deduped)
Aircraft DB CSV drop ┘    every 5 min        topics   stream├─ gold   (takeoff / landing events)
                                              │             └─► Postgres views ─► dashboard
                                              └─ dlq (records that failed validation)
```

## What it shows

- **Streaming ingestion** from REST APIs and a file drop, with captures aligned to the clock (:00, :05, :10…).
- **Kafka**: keyed topics, a compacted reference topic (`ref.aircraft`), a dead-letter topic (`dlq`).
- **Spark Structured Streaming** with `foreachBatch`: medallion layers in Delta Lake, idempotent `MERGE` writes
  so a replayed batch never duplicates rows, checkpoints for restart safety.
- **Event detection**: a takeoff or landing is an aircraft's `on_ground` flag flipping between captures.
- **Temporal join**: each takeoff is joined to the departure airport's latest METAR (hot-day takeoffs, low visibility).
- **Engine analytics**: aircraft → engine family → maker (Rolls-Royce, CFM, Pratt & Whitney, GE), giving flight
  hours and cycles per engine family per day.

## Run it

You need Docker (Docker Desktop or the free [Colima](https://github.com/abiosoft/colima)) with about 4 GB of memory.

```bash
cp .env.example .env          # optionally add OpenSky API credentials
docker compose up -d --build
```

| What | Where |
|---|---|
| Dashboard (switches to live data once the API answers) | http://localhost:8080 |
| API (FastAPI, interactive docs at `/docs`) | http://localhost:8000 |
| Kafka UI: topics, messages, consumer lag | http://localhost:8081 |
| Spark UI | http://localhost:4040 |
| Postgres | `localhost:5432`, database/user `skystream` |

Check data is arriving:

```bash
docker compose logs -f opensky-producer metar-producer spark
docker compose exec postgres psql -U skystream -c "SELECT * FROM v_airborne_now ORDER BY airborne DESC;"
docker compose exec postgres psql -U skystream -c "SELECT * FROM v_engine_utilisation_today;"
```

### OpenSky credentials

Anonymous access gives 400 credits a day. The India box (larger than 400 square degrees) costs 4 credits per call,
so that's only about 100 captures. A free OpenSky account gives 4,000 credits a day, enough for a capture every
5 minutes (about 1,150 credits). Create an account, open **Account → API client**, and put the client id and secret
in `.env`.

### Aircraft database (optional, for engine analytics)

Download the aircraft database CSV from https://opensky-network.org/data/aircraft and drop it into
`data/aircraft/`. The `aircraft-loader` service publishes the Indian-registered (VT-) aircraft and moves the file
to `data/aircraft/processed/`.

## Project layout

```
producers/            OpenSky, METAR and aircraft-CSV producers (Python, confluent-kafka)
processing/           Spark Structured Streaming job; logic.py holds the pure, unit-tested transforms
db/init.sql           Postgres serving tables and analytics views
api/                  FastAPI read-only JSON API over the views (/api/live, /summary, /weather, /events, /engines, /captures)
dashboard/nginx.conf  serves the dashboard and proxies /api to the API on the same origin
reference/            airports.csv (IATA/ICAO/coordinates), engine_map.csv (type code -> engine -> maker)
mockup/               dashboard design (simulated data for now)
tests/                unit tests for the parsing and event-detection logic
```

Tests run without Docker or Spark:

```bash
python3 -m unittest discover -s tests
```

## Design notes

- **Why every 5 minutes?** It keeps a full day of India captures within OpenSky's free daily credits and matches the
  METAR update rate. Change `CAPTURE_SECONDS` in `.env`.
- **Why collect to the driver in `foreachBatch`?** One capture over India is a few hundred aircraft. Event detection
  needs each aircraft's previous state, which lives in Postgres. Small, keyed, stateful work like this is simpler and
  cheaper on the driver than as a distributed join. The Delta writes stay distributed.
- **Engine mapping is an approximation.** The ICAO type code doesn't always fix the engine: IndiGo flies A320neos
  with both PW1100G and LEAP-1A. `reference/engine_map.csv` holds an operator-specific row where it matters and a
  default row otherwise.

## Known limitations and next steps

- **Ground coverage is sparse.** In a live test only 22 of 349 aircraft over India reported `on_ground`, near Delhi
  and Kolkata. Takeoffs and landings are therefore caught mainly at airports with good receiver coverage. Next step:
  also infer a takeoff when an aircraft is first seen climbing below 1,500 m near an airport, and a landing when it
  descends below 600 m near an airport and then disappears.
- **Live and simulated parts of the dashboard.** On load the dashboard calls `/api/health`. If the pipeline is
  running, the map, flight card, airline counts, weather, events, engines and pipeline pages switch to real
  captures, refreshed every 30 seconds. Free ADS-B data has no routes or schedules, so the airport boards, delay
  insights and flight-phase page stay simulated, and a banner says so. Opened without the pipeline, the whole page
  falls back to the simulation.
- **Always-on demo:** a scheduled GitHub Actions job that captures once, computes the gold views and publishes a
  snapshot to GitHub Pages.

## Data sources and credits

- Aircraft positions: [The OpenSky Network](https://opensky-network.org). Cite them if you publish results.
- Weather: [AviationWeather.gov](https://aviationweather.gov/data/api/) (NOAA), public domain.
- Aircraft photos in the mockup: Wikimedia Commons, credited on each card.
