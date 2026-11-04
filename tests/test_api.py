import os
import sys
import unittest

try:
    import psycopg  # noqa: F401
    from fastapi.testclient import TestClient
except ImportError:  # the API's dependencies are only installed in CI and in the api container
    TestClient = None

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))


@unittest.skipIf(TestClient is None, "fastapi not installed")
class ApiTest(unittest.TestCase):
    def setUp(self):
        import main
        self.main, self.calls = main, []

        def fake_query(sql, params=()):
            self.calls.append((sql, params))
            if "FROM live_positions p" in sql:
                return [{"icao24": "800abc", "callsign": "IGO2134", "capture_ts": 1793700000, "on_ground": False}]
            if "FROM v_captures ORDER BY" in sql:
                return [{"capture_ts": 1793700300, "aircraft": 340, "airborne": 320},
                        {"capture_ts": 1793700000, "aircraft": 349, "airborne": 327}]
            return [{"last_capture": 1793700300, "airborne": 327}]

        self._orig, main.query = main.query, fake_query
        self.client = TestClient(main.app)

    def tearDown(self):
        self.main.query = self._orig

    def test_live_reports_latest_capture(self):
        body = self.client.get("/api/live").json()
        self.assertEqual(body["capture_ts"], 1793700000)
        self.assertEqual(body["aircraft"][0]["callsign"], "IGO2134")
        self.assertEqual(self.calls[0][1], (900,))

    def test_captures_are_returned_oldest_first(self):
        rows = self.client.get("/api/captures?limit=2").json()
        self.assertEqual([r["capture_ts"] for r in rows], [1793700000, 1793700300])

    def test_limit_is_validated(self):
        self.assertEqual(self.client.get("/api/events?limit=0").status_code, 422)
        self.assertEqual(self.client.get("/api/events?limit=500").status_code, 422)

    def test_health(self):
        self.assertEqual(self.client.get("/api/health").json(), {"ok": True, "last_capture": 1793700300})


if __name__ == "__main__":
    unittest.main()
