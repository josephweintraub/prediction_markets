"""Small synthetic overlap fixtures; no production data, RPC or EC2 access."""
from contextlib import redirect_stderr
import copy
import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import audit_polymarket_native_overlap as overlap
from scripts import collect_polymarket_native_increment as collector
from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner
from scripts import probe_polymarket_increment_capacity as pilot

INVENTORY_HEAD, PLAN_HEAD, COLLECTOR_HEAD, AUDITOR_HEAD = (letter * 40 for letter in "abcd")
CUTOFF_BLOCK = 94_796_039


def code(head):
    return {"status": "inspected", "head": head, "tracked_worktree_clean": True,
            "tracked_dirty_entries": 0, "untracked_files_checked": False}


def header(number):
    return {"number": number, "hash": "0x" + f"{number + 1:064x}", "parentHash": "0x" + f"{number:064x}",
            "timestamp": planner.utc_cutoff(planner.END_EXCLUSIVE) - 1 + number - CUTOFF_BLOCK}


def native_logs():
    result = []
    for index, address in enumerate(pilot.ADDRESSES):
        block = overlap.FIRST_BLOCK + 5 + index
        # Independent fixture expectations: first two legacy records pay cash;
        # V2 regular is BUY, V2 neg-risk is SELL. V3 is only opaque raw source.
        words = [0, 100 + index, 10, 20, 1] if index < 2 else [index % 2, 100 + index, 10, 20, 1, 0, 0]
        result.append({"address": address, "blockNumber": hex(block), "blockHash": header(block)["hash"],
            "transactionHash": "0x" + f"{index + 1000:064x}", "transactionIndex": hex(index),
            "logIndex": hex(index + 10), "removed": False,
            "topics": [pilot.EXPECTED_TOPICS[address], "0x" + f"{index + 2000:064x}",
                       "0x" + "00" * 12 + "ab" * 20, "0x" + "00" * 12 + "cd" * 20],
            "data": "0x" + "".join(f"{word:064x}" for word in words)})
    return result


def old_rows():
    result = []
    for index, log in enumerate(native_logs()[:4]):
        maker_asset, taker_asset = (str(100 + index), "0") if index == 3 else ("0", str(100 + index))
        result.append({"order_hash": "0x" + f"{index + 2000:064x}", "maker": "0x" + "ab" * 20,
            "taker": "0x" + "cd" * 20, "maker_asset_id": maker_asset, "taker_asset_id": taker_asset,
            "maker_amount_filled": 10, "taker_amount_filled": 20, "fee": 1,
            "block_number": overlap.FIRST_BLOCK + 5 + index, "transaction_hash": "0x" + f"{index + 1000:064x}",
            "log_index": index + 10, "exchange_address": pilot.ADDRESSES[index]})
    return result


def old_table(rows):
    schema = pa.schema([(name, pa.string() if name in overlap.STRINGS else pa.int64()) for name in overlap.FIELDS])
    return pa.Table.from_pylist(rows, schema=schema)


class FakeRPC:
    def __call__(self, endpoint, payload):
        if payload["method"] == "eth_chainId":
            value = "0x89"
        elif payload["method"] == "eth_getBlockByNumber":
            number = int(payload["params"][0], 16)
            value = header(number)
            value = {**value, "number": hex(number), "timestamp": hex(value["timestamp"])}
        else:
            value = native_logs()
        return json.dumps({"jsonrpc": "2.0", "id": payload["id"], "result": value}).encode()


class NativeOverlapTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.paths = {name: self.root / (name + ".parquet") for name in inventory.DEFAULT_PATHS}
        rows = old_rows()
        below = {**rows[0], "block_number": overlap.FIRST_BLOCK - 500,
                 "transaction_hash": "0x" + "ee" * 32}
        tail = {**below, "block_number": overlap.RAW_TAIL - 100}
        # An excluded group, then intersecting groups with one below-window row
        # and supported overlap records including one physical duplicate.
        self.original_rows = [below, below, tail, *rows, rows[0]]
        # Source footer and independently generated RPC share the frozen tail.
        self.logs = native_logs()
        self.logs[3].update(blockNumber=hex(overlap.RAW_TAIL), blockHash=header(overlap.RAW_TAIL)["hash"])
        self.original_rows[6]["block_number"] = overlap.RAW_TAIL
        pq.write_table(old_table(self.original_rows), self.paths["raw_events"], row_group_size=2)
        for name, path in self.paths.items():
            if name != "raw_events":
                pq.write_table(pa.table({"block_number": [1, overlap.RAW_TAIL]}), path)
        self.bind_source_and_collect()

    def bind_source_and_collect(self):
        with patch.object(inventory, "code_state", return_value=code(INVENTORY_HEAD)):
            original = inventory.build_inventory(self.paths, disk_path=self.root)
        self.inventory_path = self.root / "inventory.json"
        self.inventory_path.write_text(json.dumps(original))
        _, binding = planner.read_inventory(self.inventory_path,
            hashlib.sha256(self.inventory_path.read_bytes()).hexdigest(), INVENTORY_HEAD)
        self.plan = {"schema_version": "polymarket_refresh_source_plan_v1", "status": "source_plan_ready_for_review",
            "planner_source": code(PLAN_HEAD), "final_input_identity_unchanged": True,
            "cutoff_headers_reopened_unchanged": True, "end_exclusive_utc": planner.END_EXCLUSIVE,
            "end_exclusive_unix_seconds": planner.utc_cutoff(planner.END_EXCLUSIVE),
            "last_included_header": header(CUTOFF_BLOCK), "first_excluded_header": header(CUTOFF_BLOCK + 1),
            "provisional_append_start": planner.raw_append_start(original, None),
            "provisional_append_blocks": CUTOFF_BLOCK - overlap.RAW_TAIL,
            "raw_footer_tail_header": header(overlap.RAW_TAIL), "inventory_binding": binding}
        self.plan_path = self.root / "plan.json"
        self.plan_path.write_text(json.dumps(self.plan))
        self.plan_digest = collector.digest(self.plan_path.read_bytes())
        self.collection = self.root / "collection"
        self.output = self.root / "overlap.json"
        rpc = FakeRPC()
        def transport(endpoint, payload):
            if payload["method"] == "eth_getLogs":
                return json.dumps({"jsonrpc": "2.0", "id": payload["id"], "result": self.logs}).encode()
            return rpc(endpoint, payload)
        with patch.object(inventory, "code_state", return_value=code(COLLECTOR_HEAD)), \
                patch.object(inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET", "fixture")), \
                patch.object(shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 10**12)):
            result = collector.run_collection(self.plan_path, self.plan_digest, PLAN_HEAD, COLLECTOR_HEAD, self.collection,
                mode="endpoint-overlap", first_block=overlap.FIRST_BLOCK, last_block=overlap.RAW_TAIL,
                max_rpc_calls=100, max_output_bytes=128_000_000, max_runtime_seconds=1200, max_rows=200_000,
                max_ram_bytes=8 * 1024**3, max_total_response_bytes=256 * 1024**2, transport=transport)
        self.assertEqual(result["status"], "raw_collection_complete", result)
        self.manifest_digest = collector.digest((self.collection / "manifest.json").read_bytes())
        self.complete_digest = collector.digest((self.collection / "COMPLETE.json").read_bytes())

    def execute(self, **kwargs):
        with patch.object(inventory, "code_state", return_value=code(AUDITOR_HEAD)), \
                patch.object(inventory, "load_rpc_endpoint", side_effect=AssertionError("must not load RPC")):
            return overlap.audit_overlap(self.plan_path, self.plan_digest, PLAN_HEAD, self.collection,
                kwargs.pop("manifest_digest", self.manifest_digest), kwargs.pop("complete_digest", self.complete_digest),
                kwargs.pop("collector_head", COLLECTOR_HEAD), kwargs.pop("expected_head", AUDITOR_HEAD),
                kwargs.pop("output", self.output), **kwargs)

    def test_complete_overlap_preserves_duplicates_and_reports_uninterpreted_v3(self):
        result = self.execute()
        self.assertEqual(result["status"], "overlap_reconciled", result)
        comparison = result["comparison"]
        self.assertTrue(comparison["all_overlap_checks_passed"])
        self.assertEqual(comparison["old_physical_rows"], 5)
        self.assertEqual(comparison["old_distinct_native_keys"], 4)
        self.assertEqual(comparison["new_old_format_distinct_keys"], 4)
        self.assertEqual(comparison["new_v3_raw_rows_not_economically_decoded"], 1)
        self.assertEqual(comparison["by_address"][0]["old_identical_physical_duplicate_rows"], 1)
        self.assertEqual(result["filter_lineage"], {"selected_body_rows": 6, "below_window": 1,
                                                  "inside_window": 5, "above_window": 0})
        self.assertEqual(result["old_projection"]["row_groups"], [1, 2, 3])
        self.assertTrue(result["final_input_identity_unchanged"])
        self.assertFalse(result["analysis_admitted"])
        self.assertFalse(result["v3_economic_decoding"])
        for forbidden in ("SECRET", "0x" + "ab" * 20, "0x" + "cd" * 20):
            self.assertNotIn(forbidden, json.dumps(result))

    def test_pure_legacy_and_v2_decode_matches_independent_expected_rows(self):
        for log, expected in zip(native_logs()[:4], old_rows()):
            row = {**log, "exchange_address": log["address"], "block_number": int(log["blockNumber"], 16),
                   "log_index": int(log["logIndex"], 16)}
            self.assertEqual(overlap.decode_old_representation(row), tuple(expected[name] for name in overlap.FIELDS))
        with self.assertRaisesRegex(overlap.OverlapBlocked, "economic_decode_prohibited"):
            overlap.decode_old_representation({"exchange_address": overlap.V3_ADDRESS})

    def test_primary_comparison_gates_missing_extra_conflicting_and_mismatched(self):
        raw = native_logs()
        window = {"window": "fixture", "first_block": overlap.FIRST_BLOCK, "last_block": overlap.RAW_TAIL}
        new, _ = pilot.validate_logs(raw, window, {}, 1)
        cases = [(old_rows()[1:], new, "no_extra_new_old_format_keys"),
                 (old_rows(), new[1:], "no_old_supported_keys_missing"),
                 ([*old_rows(), {**old_rows()[0], "fee": 2}], new, "no_old_payload_conflicts"),
                 ([{**old_rows()[0], "maker_amount_filled": 99}, *old_rows()[1:]], new, "all_shared_payloads_agree")]
        for old, fresh, gate in cases:
            with self.subTest(gate=gate):
                result = overlap.compare(iter(old), fresh)
                self.assertFalse(result["checks"][gate])
                self.assertFalse(result["all_overlap_checks_passed"])

    def test_hex_case_normalization_does_not_conflate_physical_duplicate_counts(self):
        rows = old_rows()
        changed_case = {**rows[0], "maker": "0x" + "AB" * 20}
        raw, _ = pilot.validate_logs(native_logs(), {"window": "fixture", "first_block": overlap.FIRST_BLOCK,
                                                   "last_block": overlap.RAW_TAIL}, {}, 1)
        result = overlap.compare([*rows, changed_case], raw)
        self.assertTrue(result["all_overlap_checks_passed"])
        self.assertEqual(result["by_address"][0]["old_canonical_duplicate_rows"], 1)
        self.assertEqual(result["by_address"][0]["old_identical_physical_duplicate_rows"], 0)

    def test_missing_statistics_and_projection_caps_fail_before_body_reader(self):
        path = self.root / "no_stats.parquet"
        pq.write_table(old_table(old_rows()), path, write_statistics=False)
        metadata = pq.read_metadata(path)
        with self.assertRaisesRegex(overlap.OverlapBlocked, "statistics_required"):
            overlap.select_old_row_groups(metadata)
        for field in ("MAX_PROJECTED_BYTES", "MAX_SELECTED_ROWS"):
            original = pq.ParquetFile
            def reader(source, *args, **kwargs):
                if isinstance(source, Path) and source.resolve() == self.paths["raw_events"].resolve():
                    raise AssertionError("must not open old body")
                return original(source, *args, **kwargs)
            with self.subTest(field=field), patch.object(overlap, field, 1), \
                    patch.object(pq, "ParquetFile", side_effect=reader):
                result = self.execute()
                self.assertEqual(result["status"], "blocked")
                self.assertEqual(result["error_code"], "old_projection_budget_exceeded")
                metadata = pq.read_metadata(self.paths["raw_events"])
                with self.assertRaisesRegex(overlap.OverlapBlocked, "projection_budget_exceeded"):
                    overlap.select_old_row_groups(metadata)

    def test_all_twelve_compressed_columns_charged_and_null_block_stats_block(self):
        metadata = pq.read_metadata(self.paths["raw_events"])
        projection = overlap.select_old_row_groups(metadata)
        expected = sum(metadata.row_group(group).column(column).total_compressed_size
                       for group in (1, 2, 3) for column in range(12))
        self.assertEqual(projection["projected_compressed_bytes"], expected)
        self.assertEqual(projection["selected_rows_declared"], 6)
        nulls = self.root / "null_blocks.parquet"
        pq.write_table(old_table([{**old_rows()[0], "block_number": None}]), nulls)
        with self.assertRaisesRegex(overlap.OverlapBlocked, "statistics_required"):
            overlap.select_old_row_groups(pq.read_metadata(nulls))

    def test_exact_projection_columns_row_groups_and_batch_size(self):
        original = pq.ParquetFile
        calls = []
        def parquet(source, *args, **kwargs):
            value = original(source, *args, **kwargs)
            if not isinstance(source, Path) or source.resolve() != self.paths["raw_events"].resolve():
                return value
            class Reader:
                def iter_batches(self, **options):
                    calls.append(options)
                    return value.iter_batches(**options)
            return Reader()
        with patch.object(pq, "ParquetFile", side_effect=parquet):
            result = self.execute()
        self.assertEqual(result["status"], "overlap_reconciled", result)
        self.assertEqual(calls, [{"batch_size": 8192, "row_groups": [1, 2, 3],
                                  "columns": list(overlap.FIELDS), "use_threads": False}])

    def test_bad_complete_manifest_heads_and_collector_hash_fail_closed(self):
        for changes in ({"manifest_digest": "0" * 64}, {"complete_digest": "0" * 64},
                        {"collector_head": "f" * 40}, {"expected_head": "f" * 40}):
            with self.subTest(changes=changes):
                self.assertEqual(self.execute(**changes)["status"], "blocked")
        with patch.object(overlap, "COLLECTOR_SHA256", "0" * 64):
            self.assertEqual(self.execute()["error_code"], "reviewed_collector_source_differs")

    def test_rehashed_wrong_contract_predicates_still_block(self):
        path = self.collection / "manifest.json"
        manifest = json.loads(path.read_bytes())
        manifest["contract"]["addresses"] = list(pilot.ADDRESSES[:4])
        manifest["contract_sha256"] = overlap.sha(overlap.encoded(manifest["contract"]))
        path.write_bytes(overlap.encoded(manifest))
        self.manifest_digest = overlap.sha(path.read_bytes())
        complete_path = self.collection / "COMPLETE.json"
        complete = json.loads(complete_path.read_bytes())
        complete.update(manifest_sha256=self.manifest_digest, contract_sha256=manifest["contract_sha256"])
        complete_path.write_bytes(overlap.encoded(complete))
        self.complete_digest = overlap.sha(complete_path.read_bytes())
        self.assertEqual(self.execute()["error_code"], "collection_predicates_or_scope_differ")

    def test_final_bound_control_reopen_catches_mutation(self):
        original = overlap.compare
        def mutate(*args):
            result = original(*args)
            path = self.collection / "COMPLETE.json"
            path.write_bytes(path.read_bytes() + b" ")
            return result
        with patch.object(overlap, "compare", side_effect=mutate):
            self.assertEqual(self.execute()["error_code"], "bound_file_changed_during_audit")

    def test_unknown_old_emitter_is_counted_and_blocks_and_v2_unknown_side_blocks(self):
        raw, _ = pilot.validate_logs(native_logs(), {"window": "fixture", "first_block": overlap.FIRST_BLOCK,
                                                   "last_block": overlap.RAW_TAIL}, {}, 1)
        unsupported = {**old_rows()[0], "exchange_address": "0x" + "fe" * 20}
        result = overlap.compare([*old_rows(), unsupported], raw)
        self.assertFalse(result["checks"]["no_unsupported_old_emitters"])
        self.assertEqual(result["unsupported_old_physical_rows"], 1)
        self.assertEqual(result["by_address"][-1]["old_physical_rows"], 1)
        row = {**native_logs()[2], "exchange_address": pilot.ADDRESSES[2], "block_number": overlap.FIRST_BLOCK,
               "log_index": 1}
        row["data"] = "0x" + f"{2:064x}" + row["data"][66:]
        with self.assertRaisesRegex(overlap.OverlapBlocked, "side_or_word_count"):
            overlap.decode_old_representation(row)

    def test_changed_old_and_new_inputs_block_not_partial_success(self):
        payload = next((self.collection / "attempts").glob("*/payload.parquet"))
        raw = payload.read_bytes()
        payload.write_bytes(raw + b"x")
        self.assertEqual(self.execute()["error_code"], "bound_file_hash_differs")
        payload.write_bytes(raw)
        self.paths["raw_events"].touch()
        self.assertEqual(self.execute()["status"], "blocked")

    def test_safe_relative_references_and_output_immutability(self):
        for path in ("../escape", "/absolute", "a/../escape", "a//b", "a/./b"):
            with self.subTest(path=path), self.assertRaises(overlap.OverlapBlocked):
                overlap.child(self.collection, path)
        link = self.collection / "linked"
        link.symlink_to(self.root)
        with self.assertRaisesRegex(overlap.OverlapBlocked, "symlink"):
            overlap.child(self.collection, "linked/plan.json")
        result = self.execute()
        overlap.publish(self.output, result)
        before = self.output.read_bytes()
        self.assertEqual(json.loads(before)["status"], "overlap_reconciled")
        with self.assertRaises(overlap.OverlapBlocked):
            overlap.publish(self.output, {"status": "blocked"})
        self.assertEqual(self.output.read_bytes(), before)
        with self.assertRaisesRegex(overlap.OverlapBlocked, "output_already_exists"):
            self.execute()

    def test_destination_inside_immutable_collection_is_not_even_blocked_published(self):
        path = self.collection / "do_not_write.json"
        with self.assertRaisesRegex(overlap.OverlapBlocked, "output_overlaps_input"):
            self.execute(output=path)
        self.assertFalse(path.exists())

    def test_output_size_and_runtime_fail_closed(self):
        with patch.object(overlap, "MAX_OUTPUT_BYTES", 1):
            with self.assertRaisesRegex(overlap.OverlapBlocked, "byte_limit"):
                overlap.publish(self.output, {"status": "blocked"})
        self.assertFalse(self.output.exists())
        state = {"value": 0}
        def clock():
            state["value"] += overlap.RUNTIME_SECONDS + 1
            return state["value"]
        self.assertEqual(self.execute(clock=clock)["error_code"], "audit_runtime_limit")

    def test_guard_before_plan_collection_or_arrow_access(self):
        argv = ["--source-plan", "/never/read", "--source-plan-sha256", "x", "--plan-source-head", "x",
                "--collection-dir", "/never/read", "--collection-manifest-sha256", "x",
                "--collection-complete-sha256", "x", "--collector-source-head", "x",
                "--expected-head", "x", "--output", "/never/write"]
        with patch("production_guard.require_production_host", side_effect=RuntimeError("fixture")), \
                patch.object(overlap, "audit_overlap") as audit, redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                overlap.main(argv)
        audit.assert_not_called()

    def test_production_main_sets_ram_address_space_and_runtime_before_audit(self):
        argv = ["--source-plan", "/fixture/plan", "--source-plan-sha256", "x", "--plan-source-head", "x",
                "--collection-dir", "/fixture/collection", "--collection-manifest-sha256", "x",
                "--collection-complete-sha256", "x", "--collector-source-head", "x",
                "--expected-head", "x", "--output", "/mnt/data/runs/fixture_result.json"]
        def audit(*args):
            address_space.assert_called_once_with(overlap.resource.RLIMIT_AS, (8 * 1024**3, 8 * 1024**3))
            alarm.assert_called_once_with(1200)
            return {"status": "overlap_reconciled"}
        with patch("production_guard.require_production_host"), \
                patch.object(inventory, "resource_state", return_value={"ram_bytes": {"MemAvailable": 16 * 1024**3}}), \
                patch.object(overlap.resource, "setrlimit") as address_space, \
                patch.object(overlap.signal, "signal"), patch.object(overlap.signal, "alarm") as alarm, \
                patch.object(overlap, "audit_overlap", side_effect=audit), patch.object(overlap, "publish"), \
                patch("builtins.print"):
            self.assertEqual(overlap.main(argv), 0)


if __name__ == "__main__":
    unittest.main()
