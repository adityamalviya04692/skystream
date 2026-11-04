"""Captures the latest METAR for every tracked airport each CAPTURE_SECONDS and publishes to raw.metar."""
import csv
import logging
import os
import time

import requests

from common import CAPTURE_SECONDS, USER_AGENT, make_producer, run_every, send

log = logging.getLogger("metar")
METAR_URL = "https://aviationweather.gov/api/data/metar"
AIRPORTS_CSV = os.getenv("AIRPORTS_CSV", "/reference/airports.csv")
TOPIC = "raw.metar"


def main() -> None:
    with open(AIRPORTS_CSV, newline="") as f:
        icaos = [row["icao"] for row in csv.DictReader(f)]
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    producer = make_producer()

    def capture() -> None:
        # one request for all airports, well inside AviationWeather's 1 request/min guidance
        r = session.get(METAR_URL, params={"ids": ",".join(icaos), "format": "json"}, timeout=30)
        r.raise_for_status()
        observations = r.json() if r.content else []  # 204 with an empty body when nothing matches
        now = int(time.time())
        for obs in observations:
            send(producer, TOPIC, obs.get("icaoId"), {**obs, "capture_ts": now})
        producer.flush(30)
        log.info("captured %d METARs for %d airports", len(observations), len(icaos))

    run_every(CAPTURE_SECONDS, capture, log)


if __name__ == "__main__":
    main()
