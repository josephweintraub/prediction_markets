"""Small, entirely local published-view integrity fixtures; no real data."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import audit_polymarket_clean_integrity as audit


TYPES = {"string": pa.string(), "large_string": pa.large_string(),
         "double": pa.float64(), "int64": pa.int64(), "bool": pa.bool_()}


def write(path: Path, schema: tuple[tuple[str, str], ...], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([
        pa.field(name, TYPES[kind]) for name, kind in schema])), path)


def trade(month: str = "2022-11", **updates) -> dict:
    row = {"proxyWallet": "private-wallet-id", "timestamp": int(datetime.strptime(month, "%Y-%m").replace(tzinfo=timezone.utc).timestamp()),
           "conditionId": "losing-token", "usdcSize": 12.25, "price": .25,
           "side": "BUY", "outcome": "No", "eventSlug": "", "is_maker": True,
           "counterparty": "private-counterparty-id", "year_month": month}
    return {**row, **updates}


class CleanIntegrityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.clean = self.root / "clean"
        self.flags = self.root / "flags.parquet"
        self.tokens = self.root / "tokens.parquet"
        self.run = self.root / "audit-run"
        self.months = ("2022-11",)
        self.rows = [trade()]
        self.flag_rows = [
            {"token_id": "losing-token", "market_id": "market-a", "winning_outcome": "Yes", "is_updown": False, "question": "q"},
            {"token_id": "winning-token", "market_id": "market-a", "winning_outcome": "Yes", "is_updown": False, "question": "q"},
        ]
        self.token_rows = [
            {"token_id": "losing-token", "condition_id": "market-a", "outcome": "No"},
            {"token_id": "winning-token", "condition_id": "market-a", "outcome": "Yes"},
        ]

    def publish(self) -> None:
        for month in self.months:
            write(self.clean / ("year_month=" + month) / "data.parquet", audit.CLEAN_SCHEMA,
                  [row for row in self.rows if row.get("partition", row["year_month"]) == month])
        write(self.flags, audit.FLAGS_SCHEMA, self.flag_rows)
        write(self.tokens, audit.TOKEN_SCHEMA, self.token_rows)

    def execute(self, **kwargs) -> dict:
        return audit.run_audit(self.clean, self.flags, self.tokens, self.run,
                               expected_months=self.months, expected_rows=len(self.rows), **kwargs)

    def part(self, month: str = "2022-11") -> dict:
        return json.loads((self.run / (month + ".json")).read_text())

    def test_losing_label_and_blank_event_slug_are_legitimate(self) -> None:
        self.publish()
        result = self.execute()
        self.assertEqual(result["status"], "published_integrity_complete")
        part = self.part()
        self.assertEqual(part["groups"][0]["resolved_label_status"], "losing")
        self.assertEqual(part["checks"]["blank_event_slug"], 1)
        self.assertEqual(part["checks"]["token_outcome_mismatch"], 0)
        self.assertEqual(part["checks"]["row_count"], 1)
        self.assertEqual(part["checks"]["positive_finite_cash"], 12.25)

    def test_utc_boundaries_physical_partition_and_zero_one_price(self) -> None:
        self.months = ("2022-11", "2022-12")
        december = trade("2022-12")
        self.rows = [trade(timestamp=december["timestamp"] - 1, price=1), december]
        self.publish()
        result = self.execute()
        self.assertEqual(result["status"], "published_integrity_complete")
        self.assertEqual(result["scanned_rows"], 2)
        self.assertEqual(result["positive_finite_cash"], 24.5)
        self.assertTrue(self.part()["checks"]["group_cash_matches"])

    def test_stored_partition_is_checked_without_hive_override(self) -> None:
        self.rows = [trade(year_month="2022-12", partition="2022-11")]
        self.publish()
        result = self.execute()
        self.assertEqual(result["status"], "blocked_integrity")
        self.assertEqual(self.part()["checks"]["stored_partition_mismatch"], 1)

    def test_timestamp_on_next_month_boundary_fails(self) -> None:
        self.rows = [trade(timestamp=trade("2022-12")["timestamp"])]
        self.publish()
        self.assertEqual(self.execute()["status"], "blocked_integrity")
        self.assertEqual(self.part()["checks"]["timestamp_outside_month"], 1)

    def test_required_null_blank_nonfinite_and_direction_gates(self) -> None:
        for field, value, counter in (
            ("proxyWallet", None, "invalid_wallet"), ("counterparty", " ", "invalid_counterparty"),
            ("conditionId", "", "invalid_token"), ("outcome", None, "invalid_outcome"),
            ("timestamp", None, "invalid_timestamp"), ("side", "SELLER", "invalid_side"),
            ("is_maker", None, "invalid_maker"), ("usdcSize", float("inf"), "invalid_cash"),
            ("usdcSize", float("nan"), "invalid_cash"), ("usdcSize", -1, "invalid_cash"),
            ("price", float("nan"), "invalid_price"), ("price", float("inf"), "invalid_price"),
            ("price", 0, "invalid_price"), ("price", 1.01, "invalid_price"),
        ):
            with self.subTest(field=field, value=value):
                self.rows = [trade(**{field: value})]
                self.publish()
                self.run = self.root / ("audit-" + str(len(list(self.root.glob("audit-*")))))
                result = self.execute()
                self.assertEqual(result["status"], "blocked_integrity")
                self.assertEqual(self.part()["checks"][counter], 1)
                payload = "".join(path.read_text() for path in self.run.glob("*.json"))
                self.assertNotIn("private-wallet-id", payload)
                self.assertNotIn("private-counterparty-id", payload)

    def test_metadata_mismatch_and_market_key_do_not_pass_token_join(self) -> None:
        for row, counter in ((trade(outcome="Yes"), "token_outcome_mismatch"),
                             (trade(conditionId="market-a"), "missing_token_map_rows")):
            with self.subTest(counter=counter):
                self.rows = [row]
                self.publish()
                self.run = self.root / ("audit-" + counter)
                self.assertEqual(self.execute()["status"], "blocked_integrity")
                self.assertEqual(self.part()["checks"][counter], 1)

    def test_duplicate_flags_fail_before_any_clean_row_scan(self) -> None:
        self.flag_rows.append(dict(self.flag_rows[0]))
        self.publish()
        with patch.object(audit, "scan_partition", side_effect=AssertionError("must not scan")):
            result = self.execute()
        self.assertEqual(result["status"], "blocked_integrity")
        self.assertEqual(result["failure_stage"], "spine_preflight")
        self.assertEqual(result["scanned_rows"], 0)
        checks = json.loads((self.run / "spine_checks.json").read_text())
        self.assertEqual(checks["duplicate_flag_keys"], 1)

    def test_valid_duplicate_tokens_and_inconsistent_market_flags_fail(self) -> None:
        self.token_rows.append(dict(self.token_rows[0]))
        self.publish()
        self.assertEqual(self.execute()["status"], "blocked_integrity")
        self.token_rows.pop()
        self.flag_rows[1]["winning_outcome"] = "No"
        self.publish()
        self.run = self.root / "conflicting-flags-run"
        self.assertEqual(self.execute()["status"], "blocked_integrity")
        checks = json.loads((self.run / "spine_checks.json").read_text())
        self.assertEqual(checks["conflicting_market_flag_groups"], 1)

    def test_blank_and_null_cache_tokens_are_inventoried_and_excluded(self) -> None:
        self.token_rows.extend(({"token_id": " "}, {"token_id": None}))
        self.publish()
        self.assertEqual(self.execute()["status"], "published_integrity_complete")
        checks = json.loads((self.run / "spine_checks.json").read_text())
        self.assertEqual(checks["valid_token_rows"], 2)
        self.assertEqual(checks["token_map_blank_token_rows"], 1)
        self.assertEqual(checks["token_map_null_token_rows"], 1)

    def test_winner_must_exist_among_that_markets_outcome_labels(self) -> None:
        for row in self.flag_rows:
            row["winning_outcome"] = "unrecognized-outcome"
        self.publish()
        self.assertEqual(self.execute()["status"], "blocked_integrity")
        checks = json.loads((self.run / "spine_checks.json").read_text())
        self.assertEqual(checks["flags_winner_absent_from_token_map"], 2)

    def test_invalid_unpublished_source_metadata_remains_blocked_with_impact_counts(self) -> None:
        self.token_rows.append({"token_id": "unpublished-token", "condition_id": "inactive-market", "outcome": ""})
        self.publish()
        with patch.object(audit, "scan_partition", side_effect=AssertionError("must not scan")):
            result = self.execute(spine_only=True)
        self.assertEqual(result["status"], "blocked_integrity")
        self.assertEqual(result["blocking_gate_counts"], {"invalid_valid_token_metadata": 1})
        checks = json.loads((self.run / "spine_checks.json").read_text())
        self.assertEqual(checks["invalid_token_metadata_affected_flag_rows"], 0)
        self.assertEqual(checks["invalid_token_metadata_unpublished_rows"], 1)
        self.assertEqual(result["scanned_rows"], 0)

    def test_spine_only_is_explicitly_unscanned_and_impacted_published_metadata_counted(self) -> None:
        self.publish()
        with patch.object(audit, "scan_partition", side_effect=AssertionError("must not scan")):
            self.assertEqual(self.execute(spine_only=True)["status"], "spine_complete_clean_rows_unscanned")
        self.assertFalse((self.run / "2022-11.json").exists())
        self.token_rows[0]["outcome"] = ""
        self.publish()
        self.run = self.root / "affected-source-run"
        self.assertEqual(self.execute(spine_only=True)["status"], "blocked_integrity")
        checks = json.loads((self.run / "spine_checks.json").read_text())
        self.assertEqual(checks["invalid_token_metadata_affected_flag_rows"], 1)
        self.assertEqual(checks["invalid_token_metadata_affected_flag_keys"], 1)

    def test_pilot_explicitly_does_not_certify_full_universe(self) -> None:
        self.months = ("2022-11", "2022-12")
        self.rows = [trade(), trade("2022-12")]
        self.publish()
        result = self.execute(months=("2022-11",))
        self.assertEqual(result["status"], "pilot_complete_full_scan_incomplete")
        self.assertEqual(result["certification_status"], "not_certified_pilot_only")
        self.assertFalse(result["full_universe_scan_completed"])
        self.assertEqual(result["scanned_rows"], 1)

    def test_schema_inventory_and_execution_failures_remain_incomplete(self) -> None:
        self.publish()
        with patch.object(audit, "scan_partition", side_effect=RuntimeError("private-wallet-id")):
            result = self.execute()
        self.assertEqual(result["status"], "incomplete_execution_error")
        self.assertEqual(result["error_class"], "RuntimeError")
        self.assertNotIn("private-wallet-id", (self.run / "summary.json").read_text())
        self.run = self.root / "footer-failure-run"
        write(self.clean / "year_month=2022-11" / "data.parquet", audit.CLEAN_SCHEMA[:-1], self.rows)
        self.assertEqual(self.execute()["status"], "blocked_integrity")
        self.assertFalse((self.run / "2022-11.json").exists())

    def test_changed_spine_is_detected_before_completion(self) -> None:
        self.publish()
        original = audit.scan_partition
        def scan_and_mutate(con, item):
            result = original(con, item)
            self.flag_rows[0]["question"] = "changed"
            write(self.flags, audit.FLAGS_SCHEMA, self.flag_rows)
            return result
        with patch.object(audit, "scan_partition", side_effect=scan_and_mutate):
            result = self.execute()
        self.assertEqual(result["status"], "blocked_integrity")
        self.assertEqual(result["failure_stage"], "final_inventory_reopen")
        self.assertEqual(result["certification_status"], "not_certified")

    def test_spine_only_reopens_frozen_inputs_before_success(self) -> None:
        self.publish()
        original = audit.prepare_spines
        def preflight_and_mutate(con, flags, token_map):
            result = original(con, flags, token_map)
            self.flag_rows[0]["question"] = "changed"
            write(self.flags, audit.FLAGS_SCHEMA, self.flag_rows)
            return result
        with patch.object(audit, "prepare_spines", side_effect=preflight_and_mutate):
            result = self.execute(spine_only=True)
        self.assertEqual(result["status"], "blocked_integrity")
        self.assertEqual(result["failure_stage"], "spine_inventory_reopen")
        self.assertEqual(result["certification_status"], "not_certified")

    def test_no_overwrite_and_metadata_only_never_scan_rows(self) -> None:
        self.publish()
        with patch.object(audit, "prepare_spines", side_effect=AssertionError("must not read bodies")):
            result = self.execute(inventory_only=True)
        self.assertEqual(result["status"], "metadata_complete_rows_unscanned")
        prior = (self.run / "summary.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.execute()
        self.assertEqual((self.run / "summary.json").read_bytes(), prior)

    def test_help_and_local_startup_require_no_heavy_imports(self) -> None:
        script = Path(audit.__file__)
        helped = subprocess.run([sys.executable, "-S", str(script), "--help"], capture_output=True, text=True)
        self.assertEqual(helped.returncode, 0, helped.stderr)
        refused = subprocess.run([sys.executable, "-S", str(script), "--run-dir", str(self.run)],
                                 capture_output=True, text=True, cwd=self.root)
        self.assertEqual(refused.returncode, 2, refused.stderr)
        self.assertIn("production host", refused.stderr)
        self.assertNotIn("ModuleNotFoundError", refused.stderr)


if __name__ == "__main__":
    unittest.main()
