"""Tiny synthetic archive fixtures; production entrypoints stay guarded."""
from datetime import datetime, timezone
import copy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_polymarket_replication import build_inputs as b
from scripts import build_kaushik_polymarket_replication as runner


def seconds(day):
    return int(datetime(2026, 3, day, tzinfo=timezone.utc).timestamp())


def write_rows(path, rows, schema=None):
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return str(path)


def metadata(tmp_path):
    rows = {
        "token_map": [{"token_id": "yes", "condition_id": "0x1", "outcome": "Yes"},
                      {"token_id": "no", "condition_id": "0x1", "outcome": "No"},
                      {"token_id": "other_yes", "condition_id": "0x2", "outcome": "Yes"},
                      {"token_id": "other_no", "condition_id": "0x2", "outcome": "No"}],
        "spine": [{"token_id": token, "market_id": market, "winning_outcome": payout}
                  for token, market, payout in (("yes", "0x1", "Yes"), ("no", "0x1", "Yes"),
                                                ("other_yes", "0x2", "No"), ("other_no", "0x2", "No"))],
        "native": [{"condition_id": "0x1", "n_outcomes": 2, "created_at": "2026-03-01T00:00:00Z",
                    "end_date": "2026-03-20T00:00:00Z", "event_slug": "event-a"},
                   {"condition_id": "0x2", "n_outcomes": 2, "created_at": None,
                    "end_date": None, "event_slug": ""}],
        "categories": [{"mkt": "0x1", "prim": "Sports"}],
    }
    paths = {name: write_rows(tmp_path / (name + ".parquet"), values) for name, values in rows.items()}
    return paths, rows


def trade(**updates):
    row = {"proxyWallet": "wallet", "timestamp": seconds(10), "conditionId": "yes", "usdcSize": 3.0,
           "price": .3, "side": "BUY", "outcome": "Yes", "eventSlug": "wrong-archive-slug",
           "is_maker": False, "counterparty": "other", "year_month": "2026-03"}
    row.update(updates)
    return row


def annotate(con, tmp_path, rows):
    schema = pa.schema([("proxyWallet", pa.string()), ("timestamp", pa.int64()), ("conditionId", pa.string()),
                        ("usdcSize", pa.float64()), ("price", pa.float64()), ("side", pa.string()),
                        ("outcome", pa.string()), ("eventSlug", pa.string()), ("is_maker", pa.bool_()),
                        ("counterparty", pa.string()), ("year_month", pa.string())])
    path = write_rows(tmp_path / "trades.parquet", rows, schema)
    b.annotate_month(con, path, "2026-03")
    return path


def check_complement_preserves_role_multiplicity_and_recorded_price(metadata, tmp_path):
    con = duckdb.connect()
    health = b.prepare_metadata(con, metadata[0])
    assert health["admitted_claims"] == 4
    rows = [trade(), trade(side="SELL"), trade(is_maker=True), trade(is_maker=True),
            trade(conditionId="other_no", outcome="No")]
    annotate(con, tmp_path, rows)
    assert sum(item["rows"] for item in b.month_counts(con, len(rows), "2026-03")) == 5
    result = con.execute("SELECT source_ordinal,side,is_maker,P,Y,claim_code,recorded_token_code,eligibility_reason FROM annotated_month ORDER BY source_ordinal").fetchall()
    assert result[0][3:5] == (.3, 1)
    assert result[1][3:5] == (.7, 0)
    assert result[0][5] == result[0][6]
    assert result[1][5] != result[1][6]
    assert all(row[-1] == "eligible" for row in result)
    # Materialize a tiny stage exactly as the production schema; fixture-only I/O.
    folder = tmp_path / "stage"
    (folder / "year_month=2026-03").mkdir(parents=True)
    con.execute(f"COPY claims TO {b.literal(folder / 'claims.parquet')} (FORMAT PARQUET)")
    con.execute(f"COPY (SELECT source_ordinal,year_month source_month,claim_code,recorded_token_code,timestamp,price recorded_price,P,Y,usdcSize,side,is_maker,duration_clock_valid duration_eligible FROM annotated_month) TO {b.literal(folder / 'year_month=2026-03' / 'base.parquet')} (FORMAT PARQUET)")
    b.create_analysis_view(con, folder)
    sell = con.execute("SELECT claim_id,recorded_token_id,recorded_price,quantity,capital,payoff,roi FROM analysis_base WHERE side='SELL'").fetchone()
    assert sell[:3] == ("no", "yes", .3)
    assert sell[3] == 10.0  # Uses .3, not the unequal binary64 1-.7.
    for value, expected in zip(sell[4:], (7.0, -70.0, -100.0)):
        assert abs(value-expected) < 1e-12
    assert con.execute("SELECT event_cluster,category FROM analysis_base WHERE recorded_token_id='other_no'").fetchone() == ("market:0x2", "Unclassified")
    assert con.execute("SELECT DISTINCT event_cluster FROM analysis_base WHERE recorded_token_id='yes'").fetchall() == [("event:event-a",)]
    con.close()


def check_cutoff_price_size_label_and_duration_gates(metadata, tmp_path):
    con = duckdb.connect()
    b.prepare_metadata(con, metadata[0])
    rows = [trade(timestamp=b.CUTOFF_SECONDS-1), trade(timestamp=b.CUTOFF_SECONDS),
            trade(price=0), trade(price=1), trade(price=float("nan")), trade(usdcSize=0),
            trade(usdcSize=float("inf")), trade(outcome="yes"), trade(is_maker=None),
            trade(side="BAD"), trade(conditionId="missing"),
            trade(conditionId="other_no", outcome="No"), trade(timestamp=seconds(20)),
            trade(timestamp=seconds(21))]
    annotate(con, tmp_path, rows)
    reasons = con.execute("SELECT eligibility_reason,duration_clock_valid FROM annotated_month ORDER BY source_ordinal").fetchall()
    assert reasons[0] == ("eligible", False)  # After endpoint remains in baseline.
    assert reasons[1][0] == "at_or_after_cutoff"
    assert [row[0] for row in reasons[2:11]] == ["invalid_recorded_price"]*3 + ["invalid_size"]*2 + ["outcome_label_disagreement", "missing_archive_role", "invalid_archive_action", "unadmitted_token_pair"]
    assert reasons[11] == ("eligible", False)  # Missing clocks remain in baseline.
    assert reasons[12] == ("eligible", True)  # Endpoint included.
    assert reasons[13] == ("eligible", False)
    assert b.CUTOFF_SECONDS == 1774396800
    assert sum(item["rows"] for item in b.month_counts(con, len(rows), "2026-03")) == len(rows)
    con.close()


def check_ambiguous_pairs_fail_closed_without_fanout(metadata, tmp_path, corruption):
    paths, rows = metadata
    if corruption == "duplicate_token":
        rows["token_map"].append(dict(rows["token_map"][0]))
    elif corruption == "duplicate_spine":
        rows["spine"].append(dict(rows["spine"][0]))
    elif corruption == "market_disagreement":
        rows["spine"][0]["market_id"] = "different"
    elif corruption == "winner_disagreement":
        rows["spine"][1]["winning_outcome"] = "No"
    elif corruption == "third_token":
        rows["token_map"].append({"token_id": "third", "condition_id": "0x1", "outcome": "Draw"})
    elif corruption == "duplicate_native":
        rows["native"].append(dict(rows["native"][0]))
    elif corruption == "duplicate_category":
        rows["categories"].append(dict(rows["categories"][0]))
    for name, values in rows.items():
        write_rows(Path(paths[name]), values)
    con = duckdb.connect()
    b.prepare_metadata(con, paths)
    assert con.execute("SELECT count(*) FROM claims WHERE market_id='0x1'").fetchone()[0] == 0
    annotate(con, tmp_path, [trade(), trade()])
    assert con.execute("SELECT count(*) FROM annotated_month").fetchone()[0] == 2
    assert con.execute("SELECT DISTINCT eligibility_reason FROM annotated_month").fetchall() == [("unadmitted_token_pair",)]
    b.month_counts(con, 2, "2026-03")
    con.close()


def check_exact_binary64_bins_and_invalid_opening_clock(metadata, tmp_path):
    paths, rows = metadata
    rows["native"][0]["created_at"] = "2026-03-11T00:00:00Z"
    write_rows(Path(paths["native"]), rows["native"])
    con = duckdb.connect()
    b.prepare_metadata(con, paths)
    prices = [.09999999999999999, .1, .8999999999999999, .9, .9000000000000001]
    annotate(con, tmp_path, [trade(price=value) for value in prices])
    result = con.execute("SELECT P,duration_clock_valid FROM annotated_month ORDER BY source_ordinal").fetchall()
    assert [row[0] for row in result] == prices
    assert all(row[1] is False for row in result)
    assert con.execute("SELECT count(*) FROM annotated_month WHERE P<.1 OR P>=.9").fetchone()[0] == 3
    con.close()


def check_footer_schema_and_drift_checks(metadata, tmp_path):
    con = duckdb.connect()
    b.prepare_metadata(con, metadata[0])
    path = annotate(con, tmp_path, [trade()])
    info = b.footer(path, "trades")
    assert info["rows"] == 1
    assert len(info["footer_sha256"]) == 64
    assert b.stat_identity(path) == info["stat"]
    with unittest.TestCase().assertRaisesRegex(b.InputBlocked, "no-fanout"):
        b.month_counts(con, 2, "2026-03")
    with unittest.TestCase().assertRaisesRegex(b.InputBlocked, "locator/month"):
        b.month_counts(con, 1, "2026-02")
    con.close()


def check_atomic_publication_never_replaces(tmp_path):
    first = tmp_path / "first"
    first.mkdir()
    target = tmp_path / "published"
    b.atomic_publish(first, target)
    assert target.is_dir() and not first.exists()
    second = tmp_path / "second"
    second.mkdir()
    with unittest.TestCase().assertRaisesRegex(b.InputBlocked, "immutable destination"):
        b.atomic_publish(second, target)
    assert target.is_dir() and second.is_dir()


def check_guards_precede_any_contract_or_production_io():
    argv = ["--contract", "/missing/contract.json", "--expected-head", "a"*40, "--run-dir", "/mnt/data/runs/new"]
    with patch.object(runner, "require_production_host", side_effect=RuntimeError("guard blocked")), patch.object(b, "preflight") as unopened:
        with unittest.TestCase().assertRaisesRegex(RuntimeError, "guard blocked"):
            runner.main(argv)
        unopened.assert_not_called()
    with patch("production_guard.require_production_host", side_effect=RuntimeError("guard blocked")):
        with unittest.TestCase().assertRaisesRegex(RuntimeError, "guard blocked"):
            b.build_stage({}, {}, [])


class ReplicationInputTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix="kaushik-fixture-")
        self.addCleanup(self.folder.cleanup)
        self.tmp_path = Path(self.folder.name)
        self.metadata = metadata(self.tmp_path)

    def test_complement_and_multiplicity(self):
        check_complement_preserves_role_multiplicity_and_recorded_price(self.metadata, self.tmp_path)

    def test_cutoff_price_size_label_duration(self):
        check_cutoff_price_size_label_and_duration_gates(self.metadata, self.tmp_path)

    def test_ambiguous_pair_gates(self):
        original = copy.deepcopy(self.metadata[1])
        for corruption in ("duplicate_token", "duplicate_spine", "market_disagreement", "winner_disagreement", "third_token", "duplicate_native", "duplicate_category"):
            with self.subTest(corruption=corruption):
                current = (self.metadata[0], copy.deepcopy(original))
                check_ambiguous_pairs_fail_closed_without_fanout(current, self.tmp_path, corruption)

    def test_exact_prices_and_opening_clock(self):
        check_exact_binary64_bins_and_invalid_opening_clock(self.metadata, self.tmp_path)

    def test_footer_and_reconciliation(self):
        check_footer_schema_and_drift_checks(self.metadata, self.tmp_path)

    def test_atomic_publication(self):
        check_atomic_publication_never_replaces(self.tmp_path)

    def test_production_guards(self):
        check_guards_precede_any_contract_or_production_io()

    def test_retry_resource_caps(self):
        self.assertEqual(b.CAPS, {
            "memory_limit": "64GB", "threads": 4, "spill_bytes": 16_000_000_000,
            "minimum_free_bytes": 20_000_000_000, "maximum_output_bytes": 32_000_000_000,
            "maximum_month_file_bytes": 4_000_000_000, "maximum_metadata_file_bytes": 1_000_000_000,
            "maximum_read_bytes": 8_000_000_000_000, "maximum_manifest_bytes": 16_000_000,
            "maximum_metadata_rows": 4_000_000})

    def test_retry_preflight_capacity_boundaries(self):
        con = duckdb.connect()
        b.prepare_metadata(con, self.metadata[0])
        path = annotate(con, self.tmp_path, [trade()])
        con.close()
        info = b.footer(path, "trades")
        contract = {"metadata": {name: {"path": value, "sha256": b.sha256(value)}
                                  for name, value in self.metadata[0].items()}}
        files = [{"path": path, "rows": 1, "bytes": info["stat"]["bytes"],
                  "month": "2026-03", "sha256": b.sha256(path)}]
        binding = {"repair_manifest": {"path": str(self.tmp_path / "fixture-manifest.json")}}
        required_disk = 68_000_000_000  # Output 32GB + spill 16GB + free floor 20GB.
        with patch.object(b, "load_contract", return_value=(contract, files, binding)), \
             patch.object(b, "source_snapshot", return_value={"head": "a"*40}), \
             patch.object(b, "validate_destination"), patch.object(b.os, "cpu_count", return_value=4), \
             patch.object(b.shutil, "disk_usage", return_value=SimpleNamespace(free=required_disk)) as disk, \
             patch.object(Path, "read_text", return_value="MemAvailable: 68359375 kB\n") as memory:
            result = b.preflight(self.tmp_path / "contract.json", self.tmp_path / "stage", "a"*40)
            self.assertEqual(result["required_free_bytes"], required_disk)
            self.assertEqual(result["observed_free_bytes"], required_disk)
            disk.return_value = SimpleNamespace(free=required_disk-1)
            with self.assertRaisesRegex(b.InputBlocked, "output/spill/free-floor"):
                b.preflight(self.tmp_path / "contract.json", self.tmp_path / "stage", "a"*40)
            disk.return_value = SimpleNamespace(free=required_disk)
            memory.return_value = "MemAvailable: 68359374 kB\n"
            with self.assertRaisesRegex(b.InputBlocked, "64GB DuckDB"):
                b.preflight(self.tmp_path / "contract.json", self.tmp_path / "stage", "a"*40)

    def test_retry_copy_output_and_disk_reserves(self):
        ceiling = 4_000_000_000
        # An exact 32GB stage allowance and 40GB free bytes both pass.
        with patch.object(b, "_directory_bytes", return_value=28_000_000_000) as used, \
             patch.object(b.shutil, "disk_usage", return_value=SimpleNamespace(free=40_000_000_000)) as disk:
            b._reserve_output(self.tmp_path, ceiling)
            used.return_value = 28_000_000_001
            with self.assertRaisesRegex(b.InputBlocked, "total output budget"):
                b._reserve_output(self.tmp_path, ceiling)
            used.return_value = 28_000_000_000
            disk.return_value = SimpleNamespace(free=39_999_999_999)
            with self.assertRaisesRegex(b.InputBlocked, "disk floor/spill"):
                b._reserve_output(self.tmp_path, ceiling)

    def test_arbitrary_native_winner_labels(self):
        paths, rows = self.metadata
        rows["token_map"][0]["outcome"] = "Arsenal"
        rows["token_map"][1]["outcome"] = "Chelsea"
        rows["spine"][0]["winning_outcome"] = "Arsenal"
        rows["spine"][1]["winning_outcome"] = "Arsenal"
        for name, values in rows.items():
            write_rows(Path(paths[name]), values)
        con = duckdb.connect()
        b.prepare_metadata(con, paths)
        assert con.execute("SELECT token_id,Y,winning_outcome_label FROM claims WHERE market_id='0x1' ORDER BY token_id").fetchall() == [("no", 0, "Arsenal"), ("yes", 1, "Arsenal")]
        rows["spine"][0]["winning_outcome"] = "Unknown"
        rows["spine"][1]["winning_outcome"] = "Unknown"
        write_rows(Path(paths["spine"]), rows["spine"])
        b.prepare_metadata(con, paths)
        assert con.execute("SELECT count(*) FROM claims WHERE market_id='0x1'").fetchone()[0] == 0
        con.close()

    def test_complete_fixture_stage_reopens_and_reconciles(self):
        con = duckdb.connect()
        b.prepare_metadata(con, self.metadata[0])
        rows = [trade(), trade(side="SELL", price=.9000000000000001), trade(is_maker=True),
                trade(timestamp=b.CUTOFF_SECONDS), trade(conditionId="other_no", outcome="No")]
        path = annotate(con, self.tmp_path, rows)
        con.close()
        metadata_info = {name: {**b.footer(value, name), "expected_sha256": b.sha256(value)}
                         for name, value in self.metadata[0].items()}
        trade_info = {**b.footer(path, "trades"), "month": "2026-03", "expected_sha256": b.sha256(path)}
        source = {"head": "a"*40, "files": {}}
        fresh = {"schema_version": "kaushik_replication_input_preflight_v1", "status": "preflight_complete",
                 "target": str(self.tmp_path / "published-stage"), "source": source, "binding": {},
                 "contract": {}, "caps": b.CAPS, "metadata": metadata_info, "trades": [trade_info],
                 "planned_read_bytes": 100_000, "required_free_bytes": 1_000_000}
        review_path = self.tmp_path / "reviewed-preflight.json"
        b.write_json(review_path, fresh)
        reviewed, reviewed_identity = b.read_json(review_path)
        # Only tiny files created by this test are admitted; production guard is
        # tested separately and no canonical /mnt/data input is touched here.
        with patch("production_guard.require_production_host"), patch.object(b, "validate_destination"), \
             patch.object(b, "source_snapshot", return_value=source), patch.object(b, "_reserve_output"):
            manifest = b.build_stage(reviewed, fresh, ["fixture-only"], reviewed_identity)
        assert manifest["rows"] == {"source": 5, "baseline_all_roles": 4, "excluded": 1, "primary_taker": 3}
        assert manifest["status"] == "inputs_complete"
        output = Path(fresh["target"])
        saved, _ = b.read_json(output / "manifest.json")
        assert saved["rows"] == manifest["rows"]
        acceptance, _ = b.read_json(output / "acceptance.json")
        assert acceptance["status"] == "inputs_reopened_accepted"
        assert acceptance["manifest_sha256"] == b.sha256(output / "manifest.json")
        con = duckdb.connect()
        inventory = [output / name for name in sorted(saved["outputs"]) if name.endswith("/base.parquet")]
        b.create_analysis_view(con, output, explicit_month_paths=inventory)
        with self.assertRaisesRegex(b.InputBlocked, "month paths differ"):
            b.create_analysis_view(con, output, explicit_month_paths=[])
        assert con.execute("SELECT count(DISTINCT source_month||':'||source_ordinal) FROM analysis_base").fetchone()[0] == 4
        assert con.execute("SELECT P FROM analysis_base WHERE side='SELL'").fetchone()[0] == 1-.9000000000000001
        con.close()


if __name__ == "__main__":
    unittest.main()
