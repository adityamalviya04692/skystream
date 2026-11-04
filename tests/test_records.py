import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producers"))
from records import aircraft_row, state_to_record  # noqa: E402

# synthetic state vector in OpenSky's positional order
STATE = ["800abc", "IGO2134 ", "India", 1793700000, 1793700001, 72.86, 19.09, 10972.8, False, 230.1, 41.0,
         0.0, None, 11277.6, "4213", False, 0]


class StateTest(unittest.TestCase):
    def test_named_fields(self):
        rec = state_to_record(STATE, 1793700005)
        self.assertEqual(rec["icao24"], "800abc")
        self.assertEqual(rec["callsign"], "IGO2134")
        self.assertEqual((rec["latitude"], rec["longitude"]), (19.09, 72.86))
        self.assertFalse(rec["on_ground"])
        self.assertEqual(rec["capture_ts"], 1793700005)
        self.assertNotIn("sensors", rec)

    def test_blank_callsign_becomes_none(self):
        self.assertIsNone(state_to_record(["800abc", "        "] + STATE[2:], 1)["callsign"])


class AircraftTest(unittest.TestCase):
    def test_old_header_style(self):
        rec = aircraft_row({"icao24": "800ABC", "registration": "vt-ijd", "typecode": "A20N", "model": "A320-271N",
                            "manufacturername": "Airbus", "operator": "IndiGo", "operatoricao": "IGO", "built": "2018-03-01"})
        self.assertEqual((rec["icao24"], rec["registration"], rec["typecode"], rec["operator_icao"]),
                         ("800abc", "VT-IJD", "A20N", "IGO"))

    def test_new_quoted_header_style(self):
        rec = aircraft_row({"'icao24'": "'800abc'", "'registration'": "'VT-YAO'", "'typecode'": "'B38M'",
                            "'manufacturerName'": "'Boeing'", "'operatorIcao'": "'AKJ'"})
        self.assertEqual((rec["registration"], rec["manufacturer"], rec["operator_icao"]), ("VT-YAO", "Boeing", "AKJ"))

    def test_non_indian_aircraft_skipped(self):
        self.assertIsNone(aircraft_row({"icao24": "a1b2c3", "registration": "N12345", "typecode": "B738"}))
        self.assertIsNone(aircraft_row({"icao24": "", "registration": "VT-ABC"}))


if __name__ == "__main__":
    unittest.main()
