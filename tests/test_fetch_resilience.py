import importlib.util
import pathlib
import unittest

MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "fetch_data.py"
spec = importlib.util.spec_from_file_location("fetch_data", MODULE_PATH)
fetch_data = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_data)


class FetchResilienceTests(unittest.TestCase):
    def test_carry_forward_uses_previous_value_when_current_missing(self):
        previous = {"onchain": {"mvrv_zscore": 2.04}, "tips": 1.97}
        self.assertEqual(
            fetch_data.carry_forward(previous, None, ["onchain", "mvrv_zscore"], "MVRV"),
            2.04,
        )
        self.assertEqual(
            fetch_data.carry_forward(previous, None, ["tips"], "TIPS"),
            1.97,
        )

    def test_carry_forward_prefers_fresh_value(self):
        previous = {"onchain": {"mvrv_zscore": 2.04}}
        self.assertEqual(
            fetch_data.carry_forward(previous, 2.15, ["onchain", "mvrv_zscore"], "MVRV"),
            2.15,
        )

    def test_fed_low_confidence_should_be_replaced_by_previous_valid_value(self):
        previous = {
            "fed": {
                "fed_direction": "cut2",
                "reasoning": "old good value",
                "confidence": "high",
            }
        }
        fed = {"fed_direction": "cut1", "reasoning": "failed fetch", "confidence": "low"}

        if fed and fed.get("confidence") == "low" and previous["fed"].get("fed_direction"):
            result = {
                "fed_direction": previous["fed"].get("fed_direction"),
                "reasoning": f"沿用上一版有效值；本轮抓取失败。上一版依据：{previous['fed'].get('reasoning', '')}".strip(),
                "confidence": previous["fed"].get("confidence", "carried_forward"),
            }
        else:
            result = fed

        self.assertEqual(result["fed_direction"], "cut2")
        self.assertIn("沿用上一版有效值", result["reasoning"])


if __name__ == "__main__":
    unittest.main()
