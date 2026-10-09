"""Synthetic controls for the existing bot-filter construction mechanism."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scripts import audit_polymarket_wallet_flag_mechanism as demo


class WalletFlagMechanismTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.evidence = demo.build_demonstration()

    def test_source_timing_fixed_buy_actions_and_distinct_tokens(self):
        for name, spacing in demo.SCENARIOS:
            with self.subTest(name=name):
                records = self.evidence["cases"][name]["source_records"]
                self.assertEqual(len(records), 10)
                self.assertEqual([row["timestamp"] for row in records], [demo.START+i*spacing for i in range(10)])
                self.assertEqual(len({row["token_id"] for row in records}), 10)
                self.assertTrue(all(row["maker_side"] == "BUY" for row in records))
                self.assertTrue(all(row["maker"] != row["taker"] for row in records))
                self.assertTrue(all(row["cash"] == 1.0 and row["price"] == 0.5 for row in records))

    def test_full_fixture_parity_except_wallet_fields_and_counts(self):
        for case in self.evidence["cases"].values():
            correct = case["exact_expanded_fixtures"]["correct_swapped"]
            copied = case["exact_expanded_fixtures"]["copied_pair"]
            demo.require_conservation(case["source_records"], correct, copied)
            fields = tuple(field for field in demo.FIELDS if field not in {"proxyWallet", "counterparty"})
            self.assertEqual(Counter(tuple(row[field] for field in fields) for row in correct),
                             Counter(tuple(row[field] for field in fields) for row in copied))
            self.assertEqual(Counter(row["is_maker"] for row in correct), {True: 10, False: 10})
            self.assertEqual(sum(row["usdcSize"] for row in correct), 20.0)
            self.assertEqual(sum(row["usdcSize"] for row in copied), 20.0)

    def test_fast_correct_wallets_have_ten_rows_and_median_one_hundred(self):
        result = self.evidence["cases"]["fast_100_seconds"]["results"]["correct_swapped"]
        self.assertEqual(result["summary"]["total_wallets"], 2)
        for wallet in result["wallets"].values():
            self.assertTrue(wallet["present_in_wallet_flags"])
            self.assertEqual(wallet["n_trades"], 10)
            self.assertEqual(wallet["median_iti"], 100.0)
            self.assertEqual(wallet["approx_mean_iti_seconds"], 100.0)
            self.assertTrue(wallet["candidate_gate_passed"])
            self.assertFalse(wallet["is_nonhuman"])
            self.assertTrue(all(wallet[flag] is False for flag in demo.FLAGS))

    def test_fast_copied_wallet_has_twenty_rows_zero_median_and_only_a_definite(self):
        result = self.evidence["cases"]["fast_100_seconds"]["results"]["copied_pair"]
        a, b = result["wallets"][demo.WALLET_A], result["wallets"][demo.WALLET_B]
        self.assertEqual(a["n_trades"], 20)
        self.assertEqual(a["fixture_interval_count"], 19)
        self.assertEqual(a["fixture_zero_interval_count"], 10)
        self.assertEqual(a["median_iti"], 0.0)
        self.assertEqual(a["approx_mean_iti_seconds"], 900/19)
        self.assertTrue(a["flag_a_definite"])
        self.assertTrue(a["is_nonhuman"])
        self.assertTrue(all(a[flag] is False for flag in demo.FLAGS if flag not in {"flag_a_definite", "is_nonhuman"}))
        self.assertFalse(b["present_in_wallet_flags"])
        self.assertEqual(b["n_trades"], 0)
        self.assertIsNone(b["median_iti"])
        self.assertTrue(all(b[flag] is None for flag in demo.FLAGS))
        self.assertEqual(result["summary"]["total_wallets"], 1)
        self.assertEqual(result["summary"]["nonhuman_trades"], 20)

    def test_slow_control_preserves_uncomputed_medians_even_with_zero_intervals(self):
        case = self.evidence["cases"]["slow_300_seconds"]
        self.assertGreater(case["spacing_seconds"], 240)
        for construction, result in case["results"].items():
            for wallet in result["wallets"].values():
                self.assertIsNone(wallet["median_iti"])
                self.assertFalse(wallet["candidate_gate_passed"])
                if wallet["present_in_wallet_flags"]:
                    self.assertFalse(wallet["is_nonhuman"])
                    self.assertGreater(wallet["approx_mean_iti_seconds"], 120)
        copied = case["results"]["copied_pair"]["wallets"][demo.WALLET_A]
        self.assertEqual(copied["approx_mean_iti_seconds"], 2700/19)
        self.assertEqual(copied["fixture_zero_interval_count"], 10)
        self.assertFalse(copied["flag_a_definite"])

    def test_current_source_hash_and_conditional_scope_are_explicit(self):
        expected = hashlib.sha256((demo.ROOT/"analysis/bot_filter.py").read_bytes()).hexdigest()
        self.assertEqual(self.evidence["source_sha256"]["analysis/bot_filter.py"], expected)
        self.assertEqual(self.evidence["status"], "synthetic_mechanism_complete")
        self.assertFalse(self.evidence["data_certified"])
        self.assertIn("does not prove historical flags", self.evidence["interpretation"])
        self.assertEqual(self.evidence["resource_contract"]["rows_per_connection"], 20)
        self.assertEqual(self.evidence["resource_contract"]["spill"], "0B")

    def test_conservation_refuses_altered_source_roles_and_no_extra_spacing(self):
        records = demo.source_records(100)
        correct, copied = demo.expand(records, copied=False), demo.expand(records, copied=True)
        copied[0] = {**copied[0], "timestamp": copied[0]["timestamp"]+1}
        with self.assertRaisesRegex(ValueError, "beyond their wallet fields"):
            demo.require_conservation(records, correct, copied)
        with self.assertRaises(ValueError):
            demo.source_records(240)

    def test_immutable_complete_json_no_overwrite_and_no_null_coercion(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory)/"fixture_output"
            demo.write_immutable(destination, self.evidence)
            encoded = (destination/"evidence.json").read_bytes()
            self.assertLess(len(encoded), demo.MAX_OUTPUT_BYTES)
            reopened = json.loads(encoded)
            self.assertEqual(reopened, self.evidence)
            self.assertIsNone(reopened["cases"]["slow_300_seconds"]["results"]["copied_pair"]["wallets"][demo.WALLET_A]["median_iti"])
            with self.assertRaises(FileExistsError):
                demo.write_immutable(destination, self.evidence)


if __name__ == "__main__":
    unittest.main()
