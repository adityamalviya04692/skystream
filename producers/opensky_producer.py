"""Captures every aircraft over India from the OpenSky Network each CAPTURE_SECONDS and publishes to raw.states."""
import logging
import os
import time

import requests

from common import CAPTURE_SECONDS, USER_AGENT, make_producer, run_every, send
from records import state_to_record

log = logging.getLogger("opensky")
STATES_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
INDIA_BBOX = {"lamin": 6.0, "lomin": 68.0, "lamax": 36.0, "lomax": 98.0}  # >400 sq deg: 4 credits per call
TOPIC = "raw.states"


class Token:
    """OAuth2 client-credentials token, refreshed a minute before expiry. No credentials means anonymous access."""

    def __init__(self, session: requests.Session):
        self.session, self.value, self.expires = session, None, 0.0
        self.client_id = os.getenv("OPENSKY_CLIENT_ID")
        self.secret = os.getenv("OPENSKY_CLIENT_SECRET")

    def header(self) -> dict:
        if not self.client_id:
            return {}
        if time.time() > self.expires - 60:
            r = self.session.post(TOKEN_URL, timeout=20, data={
                "grant_type": "client_credentials", "client_id": self.client_id, "client_secret": self.secret})
            r.raise_for_status()
            body = r.json()
            self.value, self.expires = body["access_token"], time.time() + body.get("expires_in", 1800)
        return {"Authorization": f"Bearer {self.value}"}


def main() -> None:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    token, producer = Token(session), make_producer()
    if not token.client_id:
        log.warning("OPENSKY_CLIENT_ID not set: anonymous access allows 400 credits/day (about 100 captures)")

    def capture() -> None:
        r = session.get(STATES_URL, params=INDIA_BBOX, headers=token.header(), timeout=30)
        if r.status_code == 429:
            log.warning("rate limited; OpenSky says retry after %s s", r.headers.get("X-Rate-Limit-Retry-After-Seconds"))
            return
        r.raise_for_status()
        body = r.json()
        states = body.get("states") or []
        for s in states:
            send(producer, TOPIC, s[0], state_to_record(s, body["time"]))
        producer.flush(30)
        log.info("captured %d aircraft (credits left today: %s)", len(states), r.headers.get("X-Rate-Limit-Remaining"))

    run_every(CAPTURE_SECONDS, capture, log)


if __name__ == "__main__":
    main()
