"""Synthetic records only: no compiler, files, APIs, EC2 or production scans."""
from __future__ import annotations

import ast
import copy
from pathlib import Path
import unittest

from pipeline import normalize_polymarket_v3_archive_v1 as adapter


class ArchiveAdapterTests(unittest.TestCase):
    def setUp(self):
        self.admission = {"schema_version": adapter.SCHEMA,
            "status": "root_accepted_exchange_builds_and_era", "chain_id": 137,
            "exchange_address": adapter.EXCHANGE,
            "build_receipt": {"path": "/mnt/data/runs/fixture_build/RESULT.json", "bytes": 1024, "sha256": "a"*64},
            "builds": copy.deepcopy(adapter.BUILDS), "upgrade": copy.deepcopy(adapter.UPGRADE)}
        self.token_id = (1 << 248) | 0x123400
        self.event = {"exchange_address": adapter.EXCHANGE, "transaction_hash": "0x"+"11"*32,
            "transaction_index": 1, "log_index": 10, "block_number": 93452331,
            "block_hash": "0x"+"22"*32, "order_hash": "0x"+"33"*32,
            "maker": "0x"+"44"*20, "taker": "0x"+"55"*20, "side": 0,
            "token_id": self.token_id, "maker_amount_filled": 250000, "taker_amount_filled": 1000000,
            "fee": 1000, "builder": "0x"+"66"*32, "metadata": "0x"+"77"*32,
            "removed": False, "block_timestamp": 1788883530}
        self.token = {"token_id": str(self.token_id), "complement_token_id": str(self.token_id+1),
            "market_id": "fixture_market", "outcome": "Red", "winning_outcome": "Red",
            "event_slug": None, "resolved": True}
        self.complement = {**self.token, "token_id": str(self.token_id+1),
                           "complement_token_id": str(self.token_id), "outcome": "Blue"}
        self.header = {"block_number": self.event["block_number"], "block_hash": self.event["block_hash"],
                       "timestamp": self.event["block_timestamp"]}

    def execute(self, *, event=None, admission=None, token_rows=None, complement_rows=None, headers=None):
        return adapter.normalize_event(self.event if event is None else event,
            self.admission if admission is None else admission,
            [self.token] if token_rows is None else token_rows,
            [self.complement] if complement_rows is None else complement_rows,
            [self.header] if headers is None else headers)

    def quarantine(self, reason, **kwargs):
        result = self.execute(**kwargs)
        self.assertEqual((result["status"], result["reason"]), ("quarantined", reason), result)
        self.assertEqual(result["rows"], [])
        self.assertEqual(adapter.primary_taker_rows(result), [])
        return result

    def move(self, block, tx=1, log=10, *, upgrade_tx=False):
        self.event.update(block_number=block, transaction_index=tx, log_index=log,
            transaction_hash=adapter.UPGRADE["transaction_hash"] if upgrade_tx else "0x"+"11"*32,
            block_hash=adapter.UPGRADE["block_hash"] if block == adapter.UPGRADE["block_number"] else "0x"+"22"*32)
        self.header.update(block_number=block, block_hash=self.event["block_hash"])

    def module(self, number):
        self.token_id = (number << 248) | 0x123400
        self.event["token_id"] = self.token_id
        self.token.update(token_id=str(self.token_id), complement_token_id=str(self.token_id+1))
        self.complement.update(token_id=str(self.token_id+1), complement_token_id=str(self.token_id))

    def test_four_native_side_and_archive_role_cases(self):
        for native_side in (0, 1):
            with self.subTest(native_side=native_side):
                self.event.update(side=native_side, maker_amount_filled=250000 if native_side == 0 else 1000000,
                                  taker_amount_filled=1000000 if native_side == 0 else 250000)
                result = self.execute()
                self.assertEqual(result["reason"], "eligible")
                self.assertEqual(len(result["rows"]), 2)
                self.assertEqual(result["gross_cash_integer"], 250000)
                self.assertEqual(result["gross_quantity_integer"], 1000000)
                for row in result["rows"]:
                    expected_side = "BUY" if (row["is_maker"] == (native_side == 0)) else "SELL"
                    self.assertEqual(row["side"], expected_side)
                    self.assertEqual(row["price"], 0.25)
                    self.assertEqual(row["usdcSize"], 0.25)
                    self.assertEqual(row["P"], 0.25 if expected_side == "BUY" else 0.75)
                    self.assertEqual(row["Y"], 1 if expected_side == "BUY" else 0)
                    self.assertEqual(row["claim_id"], self.token["token_id"] if expected_side == "BUY" else self.complement["token_id"])
                    self.assertEqual(row["conditionId"], self.token["token_id"])
                primary = adapter.primary_taker_rows(result)
                self.assertEqual(len(primary), 1)
                self.assertIs(primary[0]["is_maker"], False)
                self.assertEqual(primary[0]["proxyWallet"], self.event["taker"])
                self.assertEqual(primary[0]["counterparty"], self.event["maker"])

    def test_fresh_wallet_staging_cannot_alias_counterparty_projection(self):
        result = self.execute()
        maker, taker = result["rows"]
        self.assertEqual((maker["proxyWallet"], maker["counterparty"]), (self.event["maker"], self.event["taker"]))
        self.assertEqual((taker["proxyWallet"], taker["counterparty"]), (self.event["taker"], self.event["maker"]))
        self.event["maker"] = "0x"+"88"*20
        maker["counterparty"] = "independent mutation"
        primary = adapter.primary_taker_rows(result)
        primary[0]["proxyWallet"] = "another independent mutation"
        self.assertEqual(taker["counterparty"], "0x"+"44"*20)
        self.assertEqual(taker["proxyWallet"], "0x"+"55"*20)
        self.assertEqual(result["native"]["maker"], "0x"+"44"*20)

    def test_fees_and_uint256_raw_fields_are_preserved_not_net_priced(self):
        self.event["fee"] = 5000
        result = self.execute()
        self.assertEqual(result["native"], self.event)
        self.assertEqual(result["native"]["token_id"], self.token_id)
        self.assertGreater(self.token_id, 2**128)
        self.assertEqual(result["rows"][0]["price"], 0.25)
        self.assertEqual(result["rows"][0]["usdcSize"], 0.25)

    def test_source_era_is_old_and_new_outside_upgrade_block(self):
        for block, era in ((93452331, "old"), (93452333, "new")):
            self.move(block)
            result = self.execute()
            self.assertEqual(result["source_era"], era)
            self.assertEqual(result["implementation_address"], adapter.BUILDS[era]["implementation_address"])

    def test_upgrade_uses_exact_transaction_and_log_boundary(self):
        cases = [(61, 751, False, "old"), (62, 751, True, "old"),
                 (62, 753, True, "new"), (63, 753, False, "new")]
        for tx, log, same_tx, era in cases:
            with self.subTest(tx=tx, log=log):
                self.move(93452332, tx, log, upgrade_tx=same_tx)
                self.assertEqual(self.execute()["source_era"], era)

    def test_impossible_or_missing_upgrade_locators_quarantine(self):
        for tx, log, same_tx in ((62, 752, True), (62, 751, False), (61, 753, False), (63, 751, False), (61, 751, True)):
            with self.subTest(tx=tx, log=log):
                self.move(93452332, tx, log, upgrade_tx=same_tx)
                self.quarantine("impossible_upgrade_locator")
        self.move(93452331, upgrade_tx=True)
        self.quarantine("impossible_upgrade_locator")
        self.event["transaction_index"] = None
        self.quarantine("invalid_native_locator")

    def test_upgrade_block_hash_must_equal_bound_evidence(self):
        self.move(93452332, 61, 751)
        self.event["block_hash"] = self.header["block_hash"] = "0x"+"99"*32
        self.quarantine("upgrade_block_hash_differs")

    def test_only_explicit_root_build_admission_not_http_source_status(self):
        for mutate in (lambda a: a.update(status="exact_match"), lambda a: a.pop("build_receipt"),
                       lambda a: a["builds"]["old"].update(full_runtime_exact_match_after_declared_immutables=1),
                       lambda a: a["builds"]["new"].update(runtime_bytes=22255),
                       lambda a: a["builds"]["old"].update(runtime_sha256="0"*64),
                       lambda a: a["upgrade"].update(log_index=751), lambda a: a.update(chain_id=True)):
            changed = copy.deepcopy(self.admission); mutate(changed)
            with self.subTest(changed=changed), self.assertRaises(adapter.AdmissionError): self.execute(admission=changed)

    def test_receipt_is_syntactic_binding_not_a_filesystem_probe(self):
        for field, value in (("bytes", True), ("bytes", 0), ("sha256", "bad"),
                             ("path", "/mnt/data/runs/a/../untrusted.json"), ("path", "/tmp/not_a_production_receipt")):
            changed = copy.deepcopy(self.admission); changed["build_receipt"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(adapter.AdmissionError): self.execute(admission=changed)
        # No such file exists or is opened. Production must prove it independently.
        self.assertEqual(self.execute()["reason"], "eligible")

    def test_normal_wallets_emitted_by_v3_are_not_blanket_filtered(self):
        result = self.execute()
        self.assertEqual(result["status"], "classified")
        self.assertEqual(result["exchange_wallet_matches"], {"historical": [], "v3_proxy": []})

    def test_both_wallet_fields_exclude_v3_aggregate_and_historical_contracts(self):
        for field in ("maker", "taker"):
            for address in (*adapter.HISTORICAL_EXCHANGES, adapter.EXCHANGE):
                with self.subTest(field=field, address=address):
                    changed = {**self.event, field: address.upper().replace("0X", "0x")}
                    result = self.execute(event=changed)
                    expected = "exchange_wallet_v3_proxy" if address == adapter.EXCHANGE else "exchange_wallet_historical"
                    self.assertEqual((result["status"], result["reason"]), ("excluded", expected))
                    self.assertEqual(result["rows"], [])
                    self.assertTrue(result["exchange_wallet_matches"]["v3_proxy" if address == adapter.EXCHANGE else "historical"])

    def test_implementation_address_is_not_implicitly_an_exchange_wallet(self):
        self.event["taker"] = adapter.BUILDS["old"]["implementation_address"]
        self.assertEqual(self.execute()["reason"], "eligible")

    def test_negrisk_module2_retained_but_module3_and_unknown_are_separate(self):
        self.module(2)
        self.assertEqual(self.execute()["reason"], "eligible")
        self.module(3)
        result = self.execute(token_rows=[], complement_rows=[])
        self.assertEqual((result["status"], result["reason"]), ("excluded", "combinatorial_module3"))
        self.module(4)
        self.quarantine("unknown_position_module")

    def test_missing_ambiguous_and_nonreciprocal_pairs_quarantine(self):
        self.quarantine("token_mapping_missing_or_ambiguous", token_rows=[])
        self.quarantine("token_mapping_missing_or_ambiguous", token_rows=[self.token, self.token])
        self.quarantine("complement_mapping_missing_or_ambiguous", complement_rows=[])
        self.quarantine("complement_mapping_missing_or_ambiguous", complement_rows=[self.complement, self.complement])
        self.complement["complement_token_id"] = str(self.token_id+99)
        self.quarantine("binary_complement_or_outcome_conflict")

    def test_no_prefix_condition_or_numeric_fuzzy_token_match(self):
        self.token["token_id"] = str(self.token_id+2)
        self.quarantine("exact_full_token_id_mismatch")
        self.token["token_id"] = "0"+str(self.token_id)
        self.quarantine("invalid_spine_schema")
        self.token["token_id"] = self.token_id
        self.quarantine("invalid_spine_schema")

    def test_resolutions_are_metadata_driven_not_assumed_yes_or_no(self):
        self.token["winning_outcome"] = self.complement["winning_outcome"] = "Blue"
        rows = self.execute()["rows"]
        self.assertEqual([row["Y"] for row in rows], [0, 1])
        self.token["winning_outcome"] = self.complement["winning_outcome"] = "Unknown"
        self.quarantine("binary_complement_or_outcome_conflict")
        self.token["resolved"] = False
        self.quarantine("unresolved_token_pair")

    def test_pair_market_outcome_winner_and_module_conflicts_quarantine(self):
        for field, value in (("market_id", "other_market"), ("outcome", "Red"),
                             ("winning_outcome", "Blue"), ("event_slug", "other_event")):
            changed = {**self.complement, field: value}
            self.quarantine("binary_complement_or_outcome_conflict", complement_rows=[changed])
        changed = {**self.complement, "token_id": str((2 << 248) | 1)}
        recorded = {**self.token, "complement_token_id": changed["token_id"]}
        self.quarantine("binary_pair_module_conflict", token_rows=[recorded], complement_rows=[changed])

    def test_zero_quantity_cash_and_invalid_prices_quarantine(self):
        for field in ("maker_amount_filled", "taker_amount_filled"):
            self.quarantine("nonpositive_gross_cash_or_quantity", event={**self.event, field: 0})
        self.quarantine("invalid_recorded_price", event={**self.event, "maker_amount_filled": 2000000})

    def test_endpoint_recorded_price_stays_archive_but_not_paper_eligible(self):
        result = self.execute(event={**self.event, "maker_amount_filled": 1000000})
        self.assertEqual(result["status"], "classified")
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual([row["eligibility_reason"] for row in result["rows"]], ["invalid_recorded_price"]*2)
        self.assertEqual(adapter.primary_taker_rows(result), [])

    def test_extreme_sell_preserves_valid_primary_buy_when_maker_complement_rounds_to_one(self):
        event = {**self.event, "side": 1, "maker_amount_filled": adapter.UINT256, "taker_amount_filled": 1, "fee": 0}
        result = self.execute(event=event)
        self.assertEqual(result["status"], "classified")
        maker, taker = result["rows"]
        self.assertEqual(maker["P"], 1.0)
        self.assertEqual(maker["eligibility_reason"], "invalid_normalized_price")
        self.assertGreater(taker["P"], 0)
        self.assertLess(taker["P"], 1)
        self.assertEqual(taker["eligibility_reason"], "eligible")
        self.assertEqual(len(adapter.primary_taker_rows(result)), 1)
        summary = adapter.coverage_summary([result])
        self.assertEqual((summary["archive_rows"], summary["primary_taker_rows"]), (2, 1))
        self.assertIn({"is_maker": True, "reason": "invalid_normalized_price", "rows": 1}, summary["role_eligibility_counts"])

    def test_extreme_buy_mirror_keeps_gross_roles_but_excludes_only_primary_sell(self):
        result = self.execute(event={**self.event, "maker_amount_filled": 1, "taker_amount_filled": adapter.UINT256, "fee": 0})
        self.assertEqual(result["status"], "classified")
        self.assertEqual([row["eligibility_reason"] for row in result["rows"]], ["eligible", "invalid_normalized_price"])
        self.assertEqual(adapter.primary_taker_rows(result), [])
        summary = adapter.coverage_summary([result])
        self.assertEqual((summary["archive_rows"], summary["primary_taker_rows"]), (2, 0))
        self.assertIn({"is_maker": False, "reason": "invalid_normalized_price", "rows": 1}, summary["role_eligibility_counts"])

    def test_binary64_two_to_55_rounding_keeps_role_level_eligibility(self):
        for native_side in (0, 1):
            with self.subTest(native_side=native_side):
                event = {**self.event, "side": native_side, "fee": 0,
                    "maker_amount_filled": 1 if native_side == 0 else 2**55,
                    "taker_amount_filled": 2**55 if native_side == 0 else 1}
                result = self.execute(event=event)
                self.assertEqual(result["status"], "classified")
                self.assertEqual(len(result["rows"]), 2)
                for row in result["rows"]:
                    self.assertEqual(row["eligibility_reason"], "eligible" if row["side"] == "BUY" else "invalid_normalized_price")
                self.assertEqual(len(adapter.primary_taker_rows(result)), native_side)

    def test_native_schema_rejects_bool_float_dirty_side_removed_and_overflow(self):
        for field, value, reason in (("side", True, "invalid_native_side"), ("side", 256, "invalid_native_side"),
                ("fee", 1.0, "invalid_native_uint256"), ("token_id", str(self.token_id), "invalid_native_uint256"),
                ("maker_amount_filled", -1, "invalid_native_uint256"), ("fee", 2**256, "invalid_native_uint256"),
                ("block_number", True, "invalid_native_locator"), ("removed", True, "invalid_native_locator"),
                ("maker", " "+self.event["maker"], "invalid_native_wallet")):
            with self.subTest(field=field, value=value): self.quarantine(reason, event={**self.event, field: value})
        self.quarantine("invalid_native_schema", event={**self.event, "fallback_timestamp": 123})
        self.quarantine("unsupported_emitting_exchange", event={**self.event, "exchange_address": self.event["maker"]})

    def test_exact_header_required_no_fanout_or_timestamp_fallback(self):
        self.quarantine("exact_header_not_singleton", headers=[])
        self.quarantine("exact_header_not_singleton", headers=[self.header, self.header])
        for field, value in (("block_number", self.header["block_number"]+1), ("block_hash", "0x"+"aa"*32),
                             ("timestamp", None), ("timestamp", True), ("timestamp", 0)):
            self.quarantine("exact_header_key_or_clock_differs", headers=[{**self.header, field: value}])
        self.quarantine("exact_timestamp_assertion_differs", headers=[{**self.header, "timestamp": self.header["timestamp"]+1}])
        # A missing raw assertion uses the already-admitted exact header, never approximation.
        result = self.execute(event={**self.event, "block_timestamp": None})
        self.assertEqual(result["rows"][0]["timestamp"], self.header["timestamp"])

    def test_duplicate_native_records_are_not_automatically_dropped(self):
        first, second = self.execute(), self.execute()
        summary = adapter.coverage_summary([first, second])
        self.assertEqual((summary["input_records"], summary["archive_rows"], summary["primary_taker_rows"]), (2, 4, 2))
        self.assertEqual(adapter.native_relation(self.event, dict(self.event)), "equal_native_record")
        self.assertEqual(adapter.native_relation(self.event, {**self.event, "fee": 1001}), "conflicting_native_record")

    def test_different_native_fills_can_share_full_published_value_distinct_key(self):
        changed = {**self.event, "log_index": 11, "order_hash": "0x"+"99"*32, "fee": 2000}
        first, second = self.execute(), self.execute(event=changed)
        self.assertNotEqual(first["native_identity"], second["native_identity"])
        self.assertEqual(adapter.native_relation(self.event, changed), "different_native_identity")
        self.assertEqual(adapter.value_distinct_key(first["rows"][1]), adapter.value_distinct_key(second["rows"][1]))
        self.assertEqual(len(set(adapter.value_distinct_key(row) for result in (first, second) for row in result["rows"])), 2)

    def test_full_value_key_exactly_matches_existing_lineage_fields(self):
        path = Path(__file__).resolve().parents[1]/"scripts/audit_polymarket_lineage.py"
        tree = ast.parse(path.read_text())
        statement = next(node for node in tree.body if isinstance(node, ast.Assign)
                         and any(isinstance(target, ast.Name) and target.id == "VALUE_FIELDS" for target in node.targets))
        self.assertEqual(adapter.VALUE_FIELDS, ast.literal_eval(statement.value))
        row = self.execute()["rows"][1]
        self.assertEqual(len(adapter.value_distinct_key(row)), 11)
        self.assertEqual(adapter.value_distinct_key(row), adapter.value_distinct_key({**row, "P": 999, "Y": 999}))

    def test_coverage_reconciles_statuses_without_admission_claim(self):
        good = self.execute()
        missing = self.execute(token_rows=[])
        aggregate = self.execute(event={**self.event, "taker": adapter.EXCHANGE})
        self.module(3); combo = self.execute()
        summary = adapter.coverage_summary([good, missing, aggregate, combo])
        self.assertEqual(summary["input_records"], sum(item["records"] for item in summary["counts"]))
        self.assertEqual((summary["archive_rows"], summary["primary_taker_rows"]), (2, 1))
        self.assertTrue(all(summary[field] is False for field in adapter.SCOPE))
        self.assertTrue(all(good[field] is False for field in adapter.SCOPE))


if __name__ == "__main__":
    unittest.main()
