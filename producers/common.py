"""Kafka producer, config and capture-loop helpers shared by every SkyStream producer."""
import json
import logging
import os
import time

from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
CAPTURE_SECONDS = int(os.getenv("CAPTURE_SECONDS", "300"))
USER_AGENT = "SkyStream/0.1 (open-source flight data pipeline)"


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True,
                     "linger.ms": 50, "compression.type": "lz4"})


def send(producer: Producer, topic: str, key, value: dict) -> None:
    producer.produce(topic, key=key.encode() if key else None,
                     value=json.dumps(value, separators=(",", ":")).encode())
    producer.poll(0)


def run_every(seconds: int, fn, log: logging.Logger) -> None:
    """Run fn now, then on wall-clock boundaries (:00, :05, :10 for 300 s) so all producers capture together."""
    while True:
        try:
            fn()
        except Exception:
            log.exception("capture failed; retrying next cycle")
        time.sleep(seconds - time.time() % seconds)
