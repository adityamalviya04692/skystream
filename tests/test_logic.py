import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "processing"))
import logic  # noqa: E402

AIRPORTS = logic.load_airports(os.path.join(os.path.dirname(__file__), "..", "reference", "airports.csv"))
DEL = next(a for a in AIRPORTS if a["iata"] == "DEL")
BOM = next(a for a in AIRPORTS if a["iata"] == "BOM")


def pos(icao24, ts, on_ground, lat, lon, callsign="IGO2134"):
    return {"icao24": icao24, "callsign": callsign, "time_position": ts, "last_contact": ts, "capture_ts": ts,
            "on_ground": on_ground, "latitude": lat, "longitude": lon}


class AirlineTest(unittest.TestCase):
    def test_known_and_unknown_prefixes(self):
        self.assertEqual(logic.airline_of("IGO2134"), "IGO")
        self.assertEqual(logic.airline_of("akj1102"), "AKJ")
        self.assertIsNone(logic.airline_of("UAE512"))
        self.assertIsNone(logic.airline_of(None))


class AirportTest(unittest.TestCase):
    def test_nearest_within_range(self):
        self.assertEqual(logic.nearest_airport(DEL["lat"] + 0.02, DEL["lon"], AIRPORTS), "DEL")

    def test_nothing_nearby(self):
        self.assertIsNone(logic.nearest_airport(23.0, 80.0, AIRPORTS))  # central India, no tracked airport
        self.assertIsNone(logic.nearest_airport(None, 77.0, AIRPORTS))


class MetarTest(unittest.TestCase):
    def test_visibility_forms(self):
        self.assertEqual(logic.visibility_m("6+"), 9999.0)
        self.assertEqual(logic.visibility_m(1), 1609)
        self.assertEqual(logic.visibility_m("1/2"), 805)
        self.assertIsNone(logic.visibility_m(""))
        self.assertIsNone(logic.visibility_m("M"))

    def test_metar_row(self):
        row = logic.metar_row({"icaoId": "VIDP", "obsTime": 1793700000, "temp": 24, "dewp": 12, "wdir": "VRB",
                               "wspd": 3, "visib": 1, "wxString": "HZ", "rawOb": "VIDP 040830Z VRB03KT 1600 HZ"})
        self.assertEqual(row["icao"], "VIDP")
        self.assertEqual(row["wind_dir"], "VRB")
        self.assertEqual(row["visibility_m"], 1600)  # exact metres from the raw report, not the rounded miles
        self.assertEqual(row["wx"], "HZ")

    def test_raw_visibility_with_variable_wind_and_fallback(self):
        raw = "METAR VABB 040400Z 27008KT 240V300 2500 FU FEW100 32/20 Q1015"
        self.assertEqual(logic.metar_row({"icaoId": "VABB", "obsTime": 1, "visib": 1.55, "rawOb": raw})["visibility_m"], 2500)
        self.assertEqual(logic.metar_row({"icaoId": "VABB", "obsTime": 1, "visib": "6+", "rawOb": "CAVOK"})["visibility_m"], 9999.0)


class DetectEventsTest(unittest.TestCase):
    def test_takeoff_uses_last_ground_position(self):
        prev = {"800abc": {"on_ground": True, "ts": 1000, "lat": DEL["lat"], "lon": DEL["lon"]}}
        events, latest = logic.detect_events(prev, [pos("800abc", 1300, False, 28.9, 77.3)], AIRPORTS)
        self.assertEqual([(e["event"], e["airport"], e["airline_icao"]) for e in events], [("takeoff", "DEL", "IGO")])
        self.assertEqual(len(latest), 1)

    def test_landing_uses_new_position(self):
        prev = {"800abc": {"on_ground": False, "ts": 1000, "lat": 19.5, "lon": 73.0}}
        events, _ = logic.detect_events(prev, [pos("800abc", 1300, True, BOM["lat"], BOM["lon"])], AIRPORTS)
        self.assertEqual([(e["event"], e["airport"]) for e in events], [("landing", "BOM")])

    def test_two_captures_in_one_batch_and_out_of_order(self):
        rows = [pos("800abc", 1600, False, 28.9, 77.3), pos("800abc", 1300, True, DEL["lat"], DEL["lon"])]
        prev = {"800abc": {"on_ground": True, "ts": 1000, "lat": DEL["lat"], "lon": DEL["lon"]}}
        events, latest = logic.detect_events(prev, rows, AIRPORTS)
        self.assertEqual([e["event"] for e in events], ["takeoff"])
        self.assertEqual(latest[0]["time_position"], 1600)

    def test_no_event_without_history_or_after_long_gap(self):
        self.assertEqual(logic.detect_events({}, [pos("800abc", 1300, False, 28.9, 77.3)], AIRPORTS)[0], [])
        prev = {"800abc": {"on_ground": True, "ts": 1000, "lat": DEL["lat"], "lon": DEL["lon"]}}
        self.assertEqual(logic.detect_events(prev, [pos("800abc", 1000 + 3600, False, 25.0, 80.0)], AIRPORTS)[0], [])

    def test_unknown_ground_flag_is_ignored(self):
        prev = {"800abc": {"on_ground": None, "ts": 1000, "lat": 20.0, "lon": 78.0}}
        self.assertEqual(logic.detect_events(prev, [pos("800abc", 1300, False, 20.1, 78.1)], AIRPORTS)[0], [])


if __name__ == "__main__":
    unittest.main()
