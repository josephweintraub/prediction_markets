"""Synthetic adaptive trees and byte-limit proof fixtures; no real API use."""
import json
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

import test_collect_polymarket_native_increment as fixtures
from scripts import collect_polymarket_native_increment_v2 as adaptive


class AdaptiveCollectorTests(unittest.TestCase):
    def setUp(self):
        fixtures.CollectorTests.setUp(self)
        self.threshold = 25
        self.rpc = fixtures.FakeRPC()
        self.interrupt_first_leaf = False

    def transport(self, endpoint, payload):
        if payload["method"] == "eth_getLogs":
            query = payload["params"][0]
            width = int(query["toBlock"], 16) - int(query["fromBlock"], 16) + 1
            if width > self.threshold:
                self.rpc.calls.append(payload)
                return b"x" * (adaptive.pilot.MAX_RESPONSE_BYTES + 1)
            if self.interrupt_first_leaf:
                self.interrupt_first_leaf = False
                self.rpc.calls.append(payload)
                raise KeyboardInterrupt("fixture stopped after split")
        return self.rpc(endpoint, payload)

    def execute(self, **changes):
        args = dict(first_block=1000, last_block=1049, max_rpc_calls=100, max_output_bytes=32 * 1024**2,
                    max_runtime_seconds=120, max_rows=1000, max_ram_bytes=512 * 1024**2,
                    max_total_response_bytes=256 * 1024**2, transport=self.transport)
        args.update(changes)
        with patch.object(adaptive.inventory, "code_state", return_value=fixtures.code(fixtures.COLLECTOR_HEAD)), \
                patch.object(adaptive.inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET", "fixture")), \
                patch.object(shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 10**12)):
            return adaptive.run_collection(self.plan_path, self.plan_digest, fixtures.PLAN_HEAD, fixtures.COLLECTOR_HEAD,
                                            self.output, **args)

    def test_proven_oversize_splits_and_only_leaves_count_coverage(self):
        result = self.execute()
        self.assertEqual(result["status"], "raw_collection_complete")
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(len(manifest["splits"]), 1)
        self.assertEqual(len(manifest["windows"]), 2)
        self.assertEqual(result["rows"], 10)
        self.assertEqual(result["rpc_calls_charged"], 14)
        self.assertGreaterEqual(result["response_bytes_charged"], adaptive.pilot.MAX_RESPONSE_BYTES + 1)
        adaptive.verify_tree(manifest["roots"], manifest["splits"], manifest["windows"])

    def test_nested_splits_have_canonical_disjoint_leaf_coverage(self):
        self.threshold = 12
        result = self.execute()
        self.assertEqual(result["status"], "raw_collection_complete")
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(len(manifest["windows"]), 6)
        self.assertEqual(len(manifest["splits"]), 5)
        self.assertEqual(sum(item["window"]["blocks"] for item in manifest["windows"]), 50)

    def test_successful_zero_leaf_receipts_are_explicit(self):
        self.rpc.empty = True
        result = self.execute()
        self.assertEqual(result["status"], "raw_collection_complete")
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertTrue(all(interval["status"] == "success_zero_logs" for leaf in manifest["windows"]
                            for interval in leaf["contract_intervals"]))

    def test_singleton_overflow_is_fatal_without_false_split(self):
        self.threshold = 0
        result = self.execute(last_block=1000)
        self.assertEqual(result["error_code"], "singleton_or_invalid_split")
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertFalse(list((self.output / "nodes").glob("*/SPLIT.json")))

    def interrupt_after_split(self):
        self.interrupt_first_leaf = True
        with self.assertRaises(KeyboardInterrupt):
            self.execute()

    def test_resume_preserves_failed_parent_charge_without_requerying_parent(self):
        self.interrupt_after_split()
        old_calls = len(self.rpc.calls)
        result = self.execute(resume=True)
        self.assertEqual(result["status"], "raw_collection_complete")
        self.assertEqual(result["rpc_calls_charged"], len(self.rpc.calls))
        new_queries = [item["params"][0] for item in self.rpc.calls[old_calls:] if item["method"] == "eth_getLogs"]
        self.assertTrue(all(int(query["toBlock"], 16) - int(query["fromBlock"], 16) + 1 <= 25 for query in new_queries))
        self.assertGreaterEqual(result["response_bytes_charged"], 2 * (adaptive.pilot.MAX_RESPONSE_BYTES + 1))

    def test_tampered_split_children_fail_before_new_network(self):
        self.interrupt_after_split()
        path = next((self.output / "nodes").glob("*/SPLIT.json"))
        value = json.loads(path.read_text())
        value["children"][0]["last_block"] += 1
        path.write_text(json.dumps(value))
        old_calls = len(self.rpc.calls)
        result = self.execute(resume=True)
        self.assertEqual(result["error_code"], "adaptive_split_proof_differs")
        self.assertEqual(len(self.rpc.calls), old_calls)

    def test_removed_overflow_proof_cannot_be_replaced_by_split_claim(self):
        self.interrupt_after_split()
        failure = self.output / "nodes/b000000001000-000000001049/a1/FAILURE.json"
        failure.write_text('{"error_code":"rpc_transport_failure"}')
        result = self.execute(resume=True)
        self.assertEqual(result["error_code"], "adaptive_split_proof_differs")

    def test_malformed_payload_does_not_authorize_smaller_requests(self):
        self.threshold = 100
        def mutate(payload, reply):
            if payload["method"] == "eth_getLogs":
                reply["result"][0]["unknown"] = "field"
            return reply
        self.rpc.mutate = mutate
        result = self.execute()
        self.assertEqual(result["error_code"], "native_log_schema_differs")
        self.assertFalse(list((self.output / "nodes").glob("*/SPLIT.json")))

    def test_unchanged_call_and_response_caps_block_incomplete_tree(self):
        result = self.execute(max_rpc_calls=7)
        self.assertEqual(result["error_code"], "stage_rpc_call_limit")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_missing_or_duplicate_terminal_leaf_blocks_coverage(self):
        self.execute()
        manifest = json.loads((self.output / "manifest.json").read_text())
        with self.assertRaisesRegex(adaptive.pilot.PilotBlocked, "adaptive_missing_leaf"):
            adaptive.verify_tree(manifest["roots"], manifest["splits"], manifest["windows"][:-1])
        with self.assertRaisesRegex(adaptive.pilot.PilotBlocked, "duplicate_or_split_success_node"):
            adaptive.verify_tree(manifest["roots"], manifest["splits"], manifest["windows"] * 2)

    def test_earlier_payload_change_before_finalization_blocks_child_complete(self):
        normal = self.transport
        def mutate_after_left_leaf(endpoint, payload):
            if payload["method"] == "eth_getLogs" and int(payload["params"][0]["fromBlock"], 16) == 1025:
                earlier = next((self.output / "nodes").glob("*/a*/payload.parquet"))
                with earlier.open("ab") as stream:
                    stream.write(b"corrupt")
            return normal(endpoint, payload)
        result = self.execute(transport=mutate_after_left_leaf)
        self.assertEqual(result["error_code"], "adaptive_final_payload_hash_differs")
        self.assertFalse((self.output / "COMPLETE.json").exists())


if __name__ == "__main__":
    unittest.main()
