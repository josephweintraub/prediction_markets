"""Independent tiny artifact oracle, never importing the production rebuild."""
from __future__ import annotations

import copy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import duckdb

SOURCE = Path(__file__).resolve().parents[1] / "scripts/audit_mlb_unfiltered_samples.py"
spec = importlib.util.spec_from_file_location("mlb_saved_qa", SOURCE)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
START = 1_750_000_000
END = START + 100
EXACT_TYPES = ("VARCHAR", "VARCHAR", "BIGINT", "BIGINT", "VARCHAR", "INTEGER", "VARCHAR",
               "VARCHAR", "VARCHAR", "BOOLEAN", "VARCHAR", "VARCHAR", "DOUBLE", "DOUBLE")
EXTRA_TYPES = ("BIGINT", "DATE", "TIMESTAMPTZ", "TIMESTAMPTZ", "BOOLEAN", "BOOLEAN", "DOUBLE", "DATE", "DOUBLE")


def fixture_records():
    records = []
    definitions = [(.5, START-100_000, "human", "m"), (.5, START, "missing", "m"),
                   (.5, END, "nullflag", "m"), (.5, END+1, "human", "m"),
                   (.01, START, "human", "m"), (.99, START, "human", "m"),
                   (1., START, "human", "m"), (1.1, START, "human", "m"),
                   (.5, START, "bot", "m"), (.001, START, "bot", "m"),
                   (.5, START-100_000, "human", "m"), (.5, START, "human", "outside")]
    for index, (price, timestamp, wallet, market) in enumerate(definitions):
        records.append(dict(zip(audit.EXACT, (market, "token", 1000+index, timestamp, f"tx{index}",
                       index, "exchange", wallet, "seller", bool(index % 2), "home", "home", price, 1.))))
    return records


def oracle_samples(records):
    flags = {"human": False, "bot": True, "nullflag": None}
    samples = {"all": [], "filtered": []}
    for row in records:
        if row["market_id"] != "m" or row["timestamp"] > END or not 0 < row["price"] < 1:
            continue
        stamp = datetime.fromtimestamp(row["timestamp"], timezone.utc)
        enriched = dict(row, game_pk=1, official_date=stamp.date(),
                        actual_start_utc=datetime.fromtimestamp(START, timezone.utc),
                        actual_end_utc=datetime.fromtimestamp(END, timezone.utc),
                        buyer_is_flagged_nonhuman=bool(flags.get(row["proxyWallet"])),
                        won=True, calibration_error=1.-row["price"], trade_day=stamp.date(),
                        realized_time=(row["timestamp"]-START)/(END-START))
        # official_date is the event date, not the earlier trade date.
        enriched["official_date"] = datetime.fromtimestamp(START, timezone.utc).date()
        samples["all"].append(enriched)
        if .01 < row["price"] < .99 and not enriched["buyer_is_flagged_nonhuman"]:
            samples["filtered"].append(enriched)
    return samples


class IndependentArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.rows = fixture_records()
        self.old = [copy.deepcopy(self.rows[index]) for index in (0, 1, 8)]
        self.samples = oracle_samples(self.rows)
        self.metadata = [{"market_id": "m", "game_pk": 1,
                          "official_date": datetime.fromtimestamp(START, timezone.utc).date(), "winning_outcome": "home",
                          "actual_start_utc": datetime.fromtimestamp(START, timezone.utc),
                          "actual_end_utc": datetime.fromtimestamp(END, timezone.utc)}]
        self.flags = [{"proxyWallet": wallet, "is_nonhuman": flag} for wallet, flag in
                      (("human", False), ("bot", True), ("nullflag", None))]
        self.cache = [{"block_number": row["block_number"], "timestamp": row["timestamp"]} for row in self.rows]
        self.candidates = [{"market_id": "m"}, {"market_id": "outside"}]

    def tearDown(self):
        self.temp.cleanup()

    def write(self, name, rows, columns, types):
        con = duckdb.connect()
        path = self.base / (name + ".parquet")
        try:
            con.execute("CREATE TABLE fixture(" + ",".join(f'"{key}" {kind}' for key, kind in zip(columns, types)) + ")")
            if rows:
                con.executemany("INSERT INTO fixture VALUES (" + ",".join("?" for _ in columns) + ")",
                                [tuple(row[key] for key in columns) for row in rows])
            con.execute(f"COPY fixture TO {audit.sql_path(path)} (FORMAT PARQUET)")
        finally:
            con.close()
        return path

    def paths(self):
        return {"exact": self.write("exact_trades", self.rows, audit.EXACT, EXACT_TYPES),
                "old": self.write("old", self.old, audit.EXACT, EXACT_TYPES),
                "all": self.write("all_trades", self.samples["all"], audit.ENRICHED, EXACT_TYPES+EXTRA_TYPES),
                "filtered": self.write("filtered_trades", self.samples["filtered"], audit.ENRICHED, EXACT_TYPES+EXTRA_TYPES),
                "phase": self.write("phase", self.metadata, audit.META,
                                    ("VARCHAR", "BIGINT", "DATE", "VARCHAR", "TIMESTAMPTZ", "TIMESTAMPTZ")),
                "flags": self.write("flags", self.flags, ("proxyWallet", "is_nonhuman"), ("VARCHAR", "BOOLEAN")),
                "cache": self.write("cache", self.cache, ("block_number", "timestamp"), ("BIGINT", "BIGINT")),
                "candidates": self.write("candidates", self.candidates, ("market_id",), ("VARCHAR",))}

    def run_check(self):
        con = duckdb.connect()
        try:
            return audit.audit_relations(con, self.paths())
        finally:
            con.close()

    def test_python_oracle_exact_end_all_pregame_endpoints_and_null_missing_flags(self):
        result = self.run_check()
        self.assertEqual(result["counts"], {"prefilter_exact_rows": 12, "old_exact_rows": 3,
            "new_accepted_all_rows": 8, "new_accepted_filtered_rows": 4,
            "old_accepted_all_rows": 3, "old_accepted_filtered_rows": 2,
            "outside_accepted_market_rows": 1, "post_end_rows": 1, "invalid_sample_price_rows": 2,
            "filtered_extreme_price_exclusions": 3, "filtered_flagged_interior_exclusions": 1,
            "accepted_missing_flag_rows": 1, "accepted_null_flag_rows": 1,
            "accepted_valid_price_rows": 8, "restored_all_rows": 5, "restored_filtered_rows": 2,
            "candidate_markets": 2, "accepted_metadata_markets": 1, "accepted_metadata_events": 1,
            "new_all_markets": 1, "new_all_events": 1, "new_filtered_markets": 1, "new_filtered_events": 1,
            "old_all_markets": 1, "old_all_events": 1, "old_filtered_markets": 1, "old_filtered_events": 1,
            "source_blocks": 12, "matched_blocks": 12, "missing_blocks": 0})
        self.assertEqual(result["gross_recorded_cash"], {"exact": 12., "all": 8., "filtered": 4., "old": 3.})
        self.assertTrue(all(result["gates"].values()))
        self.assertEqual(result["support"]["candidate_markets"], 2)
        self.assertEqual(result["support"]["accepted_metadata_markets"], 1)
        self.assertEqual(result["support"]["accepted_metadata_events"], 1)
        self.assertEqual(result["support"]["prefilter_exact"], {"markets": 2, "accepted_events": 1})
        self.assertTrue(all(result["support"][key] == {"markets": 1, "accepted_events": 1}
                            for key in ("old_exact", "new_all", "new_filtered", "old_all", "old_filtered")))
        self.assertEqual(self.samples["all"][0]["realized_time"], -1000.)
        self.assertIn(END, [row["timestamp"] for row in self.samples["filtered"]])

    def test_duplicate_and_contradictory_native_id_refuse(self):
        original = copy.deepcopy(self.rows)
        for change in ({}, {"price": .6}):
            self.rows = copy.deepcopy(original) + [original[0] | change]
            with self.subTest(change=change), self.assertRaisesRegex(audit.AuditBlocked, "duplicate native identity"):
                self.run_check()

    def test_missing_duplicate_zero_cache_and_altered_timestamp_refuse(self):
        original = copy.deepcopy(self.cache)
        for variant in ([], original[1:], original+[original[0]],
                        [original[0] | {"timestamp": 0}] + original[1:],
                        [original[0] | {"timestamp": original[0]["timestamp"]+1}] + original[1:]):
            self.cache = variant
            with self.subTest(variant=variant), self.assertRaises(audit.AuditBlocked):
                self.run_check()

    def test_null_blank_and_conflicting_casefold_flag_keys_refuse(self):
        original = copy.deepcopy(self.flags)
        for item in ({"proxyWallet": None, "is_nonhuman": False}, {"proxyWallet": " ", "is_nonhuman": False},
                     {"proxyWallet": "HUMAN", "is_nonhuman": True}, {"proxyWallet": "human", "is_nonhuman": False}):
            self.flags = original + [item]
            with self.subTest(item=item), self.assertRaises(audit.AuditBlocked):
                self.run_check()

    def test_exact_duplicate_phase_metadata_allowed_but_conflict_or_null_refuses(self):
        original = copy.deepcopy(self.metadata)
        self.metadata = original + original
        self.run_check()
        for variant in (original + [original[0] | {"game_pk": 2}], [original[0] | {"actual_end_utc": None}],
                        [original[0] | {"winning_outcome": "away"}], [original[0] | {"actual_start_utc": "infinity"}],
                        [original[0] | {"actual_end_utc": "infinity"}]):
            self.metadata = variant
            with self.subTest(variant=variant), self.assertRaises(audit.AuditBlocked):
                self.run_check()

    def test_market_token_outcome_collision_refuses(self):
        self.rows[5]["outcome"] = "away"
        with self.assertRaisesRegex(audit.AuditBlocked, "token/outcome"):
            self.run_check()

    def test_old_payload_or_sample_payload_enrichment_and_membership_changes_refuse(self):
        for target, change in (("old", {"price": .6}), ("all", {"usdcSize": 2.}),
                               ("filtered", {"buyer_is_flagged_nonhuman": True}),
                               ("filtered", {"realized_time": None}), ("all", {"won": False})):
            original_old, original_samples = copy.deepcopy(self.old), copy.deepcopy(self.samples)
            values = self.old if target == "old" else self.samples[target]
            values[0].update(change)
            with self.subTest(target=target, change=change), self.assertRaises(audit.AuditBlocked):
                self.run_check()
            self.old, self.samples = original_old, original_samples
        self.samples["all"].pop(3)
        with self.assertRaisesRegex(audit.AuditBlocked, "missing eligible"):
            self.run_check()

    def test_zero_negative_null_native_fields_refuse(self):
        original = copy.deepcopy(self.rows)
        for field, value in (("price", 0.), ("usdcSize", 0.), ("timestamp", 0),
                             ("log_index", -1), ("exchange_address", ""), ("transaction_hash", None)):
            self.rows = copy.deepcopy(original)
            self.rows[0][field] = value
            with self.subTest(field=field), self.assertRaises(audit.AuditBlocked):
                self.run_check()

    def test_saved_manifest_fingerprints_counts_and_no_overwrite(self):
        paths = self.paths()
        con = duckdb.connect()
        try:
            computed = audit.audit_relations(con, paths)
            counts = computed["counts"]
        finally:
            con.close()
        head = "f" * 40
        manifest = {"schema_version": 1, "status": audit.STATUS, "data_certified": False,
                    "scientific_estimators_rerun": False, "source": {"expected_head": head, "head_before": head, "head_after": head,
                    "files": {SOURCE.name: audit.fingerprint(SOURCE)}}, "caps": {"memory_limit": "96GB", "threads": 8,
                    "spill_bytes": 4_000_000_000, "output_bytes": 4_000_000_000, "minimum_free_bytes": 12_000_000_000,
                    "disabled_optimizers": "common_subplan"},
                    "reconciliation": {name: True for name in audit.RECONCILIATION}, "counts": dict(counts,
                    raw_candidate_rows=13, distinct_candidate_fills=12, duplicate_payload_rows=1), "inputs": {}, "outputs": {}}
        for name, label in (("old", "old_exact"), ("phase", "phase"), ("flags", "wallet_flags"), ("cache", "cache"),
                            ("candidates", "candidates")):
            manifest["inputs"][label] = audit.fingerprint(paths[name])
        for name in ("exact", "all", "filtered"):
            item = audit.fingerprint(paths[name]); item["path"] = paths[name].name
            field = {"exact": "prefilter_exact_rows", "all": "new_accepted_all_rows", "filtered": "new_accepted_filtered_rows"}[name]
            item.update(rows=counts[field], schema=computed["artifact_schemas"][name])
            manifest["outputs"][paths[name].name] = item
        manifest_path = self.base / "manifest.json"
        declaration = {"schema_version": 1, "method": "polygon_rpc_block_timestamp", "timestamp_unit": "unix_seconds",
            "cache": {"path": str(paths["cache"]), "sha256": audit.fingerprint(paths["cache"])["sha256"], "format": "parquet", "rows": 12},
            "build_metadata": {"used_exact_cache": True, "source_distinct_blocks": 12, "cache_distinct_blocks": 12,
                               "missing_blocks": 0, "fallback_rows": 0}}
        declaration_path = self.base / "timestamp_provenance.json"
        declaration_path.write_text(json.dumps(declaration))
        manifest["inputs"]["timestamp_provenance"] = audit.fingerprint(declaration_path)
        def save_manifest():
            manifest.pop("summary_artifact", None)
            summary = self.base / "summary.json"
            summary.write_text(json.dumps(manifest))
            manifest["summary_artifact"] = audit.fingerprint(summary) | {"path": "summary.json"}
            manifest_path.write_text(json.dumps(manifest))
        save_manifest()
        fake_disk = mock.Mock(free=20*1024**3)
        expected = {name: counts[name] for name in audit.EXPECTED}
        with mock.patch.dict(audit.EXPECTED, expected, clear=True), mock.patch.object(audit.shutil, "disk_usage", return_value=fake_disk), \
                mock.patch.object(audit, "read_head", return_value=head):
            result = audit.audit_saved(manifest_path, self.base / "qa", head)
            self.assertEqual(result["counts"], counts)
            self.assertFalse(result["data_certified"])
            self.assertLess((self.base / "qa/receipt.json").stat().st_size, audit.CAPS["maximum_receipt_bytes"])
            with self.assertRaisesRegex(audit.AuditBlocked, "immutable"):
                audit.audit_saved(manifest_path, self.base / "qa", head)
            manifest["outputs"]["all_trades.parquet"]["sha256"] = "0"*64
            save_manifest()
            with self.assertRaisesRegex(audit.AuditBlocked, "fingerprint"):
                audit.audit_saved(manifest_path, self.base / "bad", head)
            self.assertFalse((self.base / "bad").exists())


if __name__ == "__main__":
    unittest.main()
