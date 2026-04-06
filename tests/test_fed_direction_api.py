import importlib.util
import json
import pathlib
import unittest
from unittest.mock import patch

MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "fetch_data.py"
spec = importlib.util.spec_from_file_location("fetch_data", MODULE_PATH)
fetch_data = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_data)


class FedDirectionApiTests(unittest.TestCase):
    def test_rateprobability_cut2_when_cut_odds_above_50(self):
        payload = {
            "today": {
                "midpoint": 4.625,
                "rows": [
                    {
                        "meeting": "May 1, 2026",
                        "prob_move_pct": 68,
                        "prob_is_cut": True,
                        "implied_rate_post_meeting": 4.375,
                        "change_bps": -25,
                    }
                ]
            }
        }
        with patch.object(fetch_data, "http_get", return_value=json.dumps(payload)):
            result = fetch_data.fetch_fed_direction_rateprob()
        self.assertEqual(result["fed_direction"], "cut2")
        self.assertEqual(result["source"], "rateprobability_api")

    def test_rateprobability_hike_when_upside_move_priced(self):
        payload = {
            "today": {
                "midpoint": 3.625,
                "rows": [
                    {
                        "meeting": "Sep 16, 2026",
                        "prob_move_pct": 71,
                        "prob_is_cut": False,
                        "implied_rate_post_meeting": 3.875,
                        "change_bps": 25,
                    }
                ]
            }
        }
        with patch.object(fetch_data, "http_get", return_value=json.dumps(payload)):
            result = fetch_data.fetch_fed_direction_rateprob()
        self.assertEqual(result["fed_direction"], "hike")

    def test_rateprobability_defaults_to_cut1_for_hold_or_small_move(self):
        payload = {
            "today": {
                "midpoint": 3.625,
                "rows": [
                    {
                        "meeting": "Apr 29, 2026",
                        "prob_move_pct": 4,
                        "prob_is_cut": False,
                        "implied_rate_post_meeting": 3.63,
                        "change_bps": 0.5,
                    }
                ]
            }
        }
        with patch.object(fetch_data, "http_get", return_value=json.dumps(payload)):
            result = fetch_data.fetch_fed_direction_rateprob()
        self.assertEqual(result["fed_direction"], "cut1")
        self.assertIn("定价", result["reasoning"])


if __name__ == "__main__":
    unittest.main()
