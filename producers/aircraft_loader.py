"""Watches AIRCRAFT_DIR for OpenSky aircraft-database CSV drops and publishes Indian-registered aircraft to ref.aircraft."""
import csv
import logging
import os
import shutil

from common import make_producer, run_every, send
from records import aircraft_row

log = logging.getLogger("aircraft")
AIRCRAFT_DIR = os.getenv("AIRCRAFT_DIR", "/data/aircraft")
TOPIC = "ref.aircraft"  # compacted: the latest record per icao24 wins


def main() -> None:
    producer = make_producer()
    done_dir = os.path.join(AIRCRAFT_DIR, "processed")
    os.makedirs(done_dir, exist_ok=True)

    def scan() -> None:
        for name in sorted(os.listdir(AIRCRAFT_DIR)):
            if not name.endswith(".csv"):
                continue
            path, sent = os.path.join(AIRCRAFT_DIR, name), 0
            with open(path, newline="", encoding="utf-8", errors="replace") as f:
                quote = "'" if f.readline().startswith("'") else '"'  # newer OpenSky releases quote with '
                f.seek(0)
                for row in csv.DictReader(f, quotechar=quote):
                    rec = aircraft_row(row)
                    if rec:
                        send(producer, TOPIC, rec["icao24"], rec)
                        sent += 1
            producer.flush(60)
            shutil.move(path, os.path.join(done_dir, name))
            log.info("%s: published %d Indian-registered aircraft", name, sent)

    run_every(int(os.getenv("SCAN_SECONDS", "30")), scan, log)


if __name__ == "__main__":
    main()
