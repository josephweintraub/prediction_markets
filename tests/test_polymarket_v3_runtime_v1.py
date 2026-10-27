"""Public-runtime fixtures only: no network or production inputs."""
from __future__ import annotations

from contextlib import contextmanager
import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import time
import unittest
from unittest.mock import patch

import test_polymarket_v3_history_v2 as fixtures
from scripts import probe_polymarket_v3_runtime_v1 as runtime

PM, COLLATERAL, COMBO, FEE = ("0x" + value * 20 for value in ("66", "77", "88", "99"))
RUNTIME_HEX = {runtime.prior.OLD_IMPLEMENTATION: "0x6001aabb0002", runtime.prior.NEW_IMPLEMENTATION: "0x6002ccdd0002"}
OBSERVATIONS = {address: {"bytes": len(bytes.fromhex(raw[2:])), "sha256": hashlib.sha256(bytes.fromhex(raw[2:])).hexdigest()}
                for address, raw in RUNTIME_HEX.items()}


def word(value):
    return "0x" + (value[2:].zfill(64) if type(value) is str else f"{value:064x}")


class FakeRPC(fixtures.FakeRPC):
    def __call__(self, endpoint, payload, *, byte_limit, timeout):
        method, params = payload["method"], payload["params"]
        if method not in {"eth_call", "eth_getCode"} and not (method == "eth_getStorageAt" and params[0] != runtime.ADDRESS):
            return super().__call__(endpoint, payload, byte_limit=byte_limit, timeout=timeout)
        self.calls.append(copy.deepcopy(payload))
        if method == "eth_getCode":
            value = RUNTIME_HEX[params[0]]
        elif method == "eth_getStorageAt":
            value = word("0x" + "11" * 20)
        else:
            data, target = params[0]["data"], params[0]["to"]
            if data.startswith(runtime.SELECTORS["moduleById(uint256)"]):
                value = word("0x" + f"{int(data[10:], 16):02x}" * 20)
            elif data == runtime.SELECTORS["decimals()"]:
                value = word(6)
            else:
                values = {runtime.SELECTORS["POSITION_MANAGER()"]: PM,
                    runtime.SELECTORS["COLLATERAL_TOKEN()"]: COLLATERAL,
                    runtime.SELECTORS["COMBINATORIAL_MODULE()"]: COMBO,
                    runtime.SELECTORS["FEE_RECEIVER()"]: FEE, runtime.SELECTORS["MAX_FEE_RATE()"]: 200}
                value = word(values[data])
        if method in self.overrides:
            value = self.overrides[method](value, payload)
        reply, status = {"jsonrpc": "2.0", "id": payload["id"], "result": value}, 200
        if self.reply_override:
            replacement = self.reply_override(reply, payload, byte_limit)
            if replacement is not None:
                reply, status = replacement
        raw = reply if type(reply) is bytes else json.dumps(reply).encode()
        return runtime.HTTPResponse(raw[:byte_limit], status)


@contextmanager
def fixture_limits():
    yield {"started_monotonic": time.monotonic(), "address_space_soft_bytes": runtime.MAX_RAM_BYTES,
           "wall_alarm_seconds": runtime.MAX_RUNTIME_SECONDS}


class RuntimeTests(unittest.TestCase):
    def save_plan(self):
        fixtures.SparseHistoryTests.save_plan(self)

    def setUp(self):
        fixtures.SparseHistoryTests.setUp(self)
        self.rpc = FakeRPC()

    def execute(self, *, transport=None, snapshot=None, resources=None):
        self.run_number += 1
        self.run_dir = self.root / f"runtime-{self.run_number}"
        frozen_headers = {block: {key: fixtures.header(block)[key] for key in ("hash", "timestamp")}
                          for block in runtime.FROZEN_UPGRADE_HEADERS}
        with patch.object(runtime, "require_production_host"), patch.object(runtime, "runtime_limits", fixture_limits), \
                patch.object(runtime.shared, "RUN_ROOT", self.root), \
                patch.object(runtime, "source_snapshot", side_effect=snapshot or (lambda _: {"head": fixtures.PROBE_HEAD})), \
                patch.object(runtime.inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET", "fixture")), \
                patch.object(runtime.inventory, "resource_state", return_value=resources or {
                    "ram_bytes": {"MemAvailable": 10**12}, "disk_bytes": {"free": 10**12}}), \
                patch.object(runtime.shared.shutil, "disk_usage", return_value=fixtures.DISK), \
                patch.object(runtime, "OBSERVED_RUNTIME", OBSERVATIONS), \
                patch.object(runtime, "FROZEN_UPGRADE_HEADERS", frozen_headers):
            return runtime.probe(self.plan_path, self.digest, fixtures.PLAN_HEAD, fixtures.PROBE_HEAD,
                                 run_dir=self.run_dir, transport=transport or self.rpc)

    def blocked(self, expected, **kwargs):
        result = self.execute(**kwargs)
        self.assertEqual(result["status"], "blocked", result)
        self.assertEqual(result["error_code"], expected)
        self.assertFalse((self.run_dir / "COMPLETE.json").exists())
        self.assertNotIn("SECRET", json.dumps(result))
        return result

    def test_complete48_fixed_attempts_no_economic_or_source_admission(self):
        result = self.execute()
        self.assertEqual(result["status"], "bounded_v3_runtime_observations_complete", result)
        self.assertEqual(result["rpc_calls_attempted"], 48)
        self.assertEqual(len(result["runtime_observations"]), 4)
        self.assertEqual(len(result["runtime_blobs"]), 2)
        self.assertEqual(len(result["getters"]), 20)
        self.assertEqual(len(result["dependency_checks"]), 11)
        for flag in ("analysis_admitted", "collection_approved", "source_equivalence_admitted", "economic_decoding_admitted",
                     "token_mapping_admitted", "outcome_mapping_admitted", "exchange_abi_admitted", "first_deployment_certified",
                     "cbor_decoding_performed", "offchain_source_http_requested", "cid_content_verified", "trade_logs_requested"):
            self.assertFalse(result[flag], flag)
        self.assertEqual(result["unknown_getter_observations"], 0)
        self.assertLessEqual(result["response_bytes_charged"], runtime.MAX_TOTAL_RESPONSE_BYTES)
        self.assertEqual(result["response_bytes_received"], result["response_bytes_charged"])
        for item in result["runtime_observations"]:
            retained = bytes.fromhex(result["runtime_blobs"][item["runtime_blob_key"]][2:])
            self.assertEqual(len(retained), item["runtime"]["bytes"])
            self.assertEqual(hashlib.sha256(retained).hexdigest(), item["runtime"]["sha256"])
            self.assertFalse(item["suffix_evidence"]["cbor_decoded"])
            self.assertFalse(item["suffix_evidence"]["metadata_presence_certified"])
            self.assertIsNone(item["suffix_evidence"]["cid"])
        marker = json.loads((self.run_dir / "COMPLETE.json").read_text())
        self.assertEqual(marker["result_sha256"], hashlib.sha256((self.run_dir / "RESULT.json").read_bytes()).hexdigest())
        journal = (self.run_dir / "REQUESTS.jsonl").read_bytes()
        self.assertEqual(result["request_journal_sha256"], hashlib.sha256(journal).hexdigest())
        self.assertEqual(result["request_journal_records"], len(journal.splitlines()))
        self.assertLessEqual(sum(path.stat().st_size for path in self.run_dir.iterdir()), runtime.MAX_OUTPUT_BYTES)
        self.assertFalse((self.run_dir / ".COMPLETE.pending").exists())
        self.assertEqual({item["method"] for item in self.rpc.calls},
                         {"eth_chainId", "eth_getBlockByNumber", "eth_getStorageAt", "eth_getCode", "eth_call"})

    def test_guard_precedes_all_inputs_network_and_limits(self):
        with patch.object(runtime, "require_production_host", side_effect=RuntimeError("guard")), \
                patch.object(runtime.activity, "read_plan") as read, patch.object(runtime, "runtime_limits") as limit:
            with self.assertRaises(RuntimeError):
                runtime.probe(self.plan_path, self.digest, fixtures.PLAN_HEAD, fixtures.PROBE_HEAD, run_dir=self.root / "never")
        read.assert_not_called()
        limit.assert_not_called()
        self.assertFalse((self.root / "never").exists())

    def test_runtime_mismatch_empty_and_malformed_runtime_fail(self):
        for value, code in (("0x6003", "runtime_differs_from_prior_observation"),
                            ("0x", "runtime_differs_from_prior_observation"), ("0x1", "runtime_hex_invalid")):
            self.rpc = FakeRPC()
            self.rpc.overrides["eth_getCode"] = lambda *_: value
            with self.subTest(value=value):
                self.blocked(code)

    def test_frozen_proxy_slots_and_canonical_slot_words(self):
        for value, code in ((word(runtime.prior.NEW_IMPLEMENTATION), "frozen_proxy_implementation_slot_differs"),
                            ("0x0", "invalid_implementation_storage_word"),
                            ("0x" + "11" * 32, "implementation_slot_not_address_word")):
            self.rpc = FakeRPC()
            self.rpc.overrides["eth_getStorageAt"] = lambda *_: value
            with self.subTest(code=code):
                self.blocked(code)

    def test_headers_source_binding_upgrade_link_and_reopening(self):
        self.rpc.overrides["eth_getBlockByNumber"] = lambda value, payload: (
            {**value, "hash": "0x" + "11" * 32} if int(payload["params"][0], 16) == runtime.prior.RAW_TAIL_BLOCK else value)
        self.blocked("source_or_reopened_header_changed")
        self.rpc = FakeRPC()
        self.rpc.overrides["eth_getBlockByNumber"] = lambda value, payload: (
            {**value, "timestamp": hex(int(value["timestamp"], 16) + 1)} if
            int(payload["params"][0], 16) == runtime.prior.UPGRADE_BLOCK - 1 else value)
        self.blocked("header_differs_from_prior_upgrade_observation")
        self.rpc = FakeRPC()
        counts = {}
        def changed(value, payload):
            number = int(payload["params"][0], 16)
            counts[number] = counts.get(number, 0) + 1
            return {**value, "hash": "0x" + "22" * 32} if counts[number] > 1 else value
        self.rpc.overrides["eth_getBlockByNumber"] = changed
        self.blocked("source_or_reopened_header_changed")

    def test_execution_reverted_getters_are_unknown_not_zero_or_equivalent(self):
        def reverted(reply, payload, _limit):
            if payload["method"] == "eth_call":
                return {"jsonrpc": "2.0", "id": payload["id"], "error": {
                    "code": 3, "message": "execution reverted: SECRET", "data": "0x"}}, 200
        self.rpc.reply_override = reverted
        result = self.execute()
        self.assertEqual(result["status"], "bounded_v3_runtime_observations_complete", result)
        self.assertEqual(result["rpc_calls_attempted"], 37)
        self.assertEqual(result["unknown_getter_observations"], 20)
        for getter in result["getters"]:
            self.assertEqual(getter["status"], "unknown_execution_reverted")
            self.assertIsNone(getter["value"])
        self.assertTrue(all(item["status"] == "unknown_missing_getter_observation" for item in result["configuration_checks"]))
        self.assertNotIn("SECRET", json.dumps(result))

    def test_nonrevert_rpc_errors_are_blockers_not_missing_getters(self):
        for code, message in ((-32005, "rate limit SECRET"), (-32000, "missing trie node SECRET"),
                              (3, "invalid opcode SECRET"), (-32602, "execution reverted SECRET")):
            self.rpc = FakeRPC()
            def fail(reply, payload, _limit):
                if payload["method"] == "eth_call":
                    return {"jsonrpc": "2.0", "id": payload["id"], "error": {"code": code, "message": message}}, 200
            self.rpc.reply_override = fail
            with self.subTest(code=code):
                self.blocked("rpc_error_not_admitted")

    def test_empty_wrong_shape_and_noncanonical_words_are_explicit_unknown(self):
        cases = [("0x", "unknown_empty_return"), ("0x01", "unknown_abi_return_shape"),
                 ("0x" + "11" * 32, "unknown_noncanonical_address_word")]
        for value, status in cases:
            self.rpc = FakeRPC()
            self.rpc.overrides["eth_call"] = lambda original, payload: (value if
                payload["params"][0]["data"] == runtime.SELECTORS["POSITION_MANAGER()"] else original)
            with self.subTest(status=status):
                result = self.execute()
                self.assertEqual(result["status"], "bounded_v3_runtime_observations_complete", result)
                missing = [item for item in result["getters"] if item["signature"] == "POSITION_MANAGER()"]
                self.assertTrue(all(item["status"] == status and item["value"] is None for item in missing))
        self.rpc = FakeRPC()
        self.rpc.overrides["eth_call"] = lambda original, payload: word(256) if (
            payload["params"][0]["data"] == runtime.SELECTORS["decimals()"] ) else original
        result = self.execute()
        self.assertEqual(result["status"], "bounded_v3_runtime_observations_complete")
        self.assertTrue(all(item["status"] == "unknown_noncanonical_uint8_word" for item in result["dependency_checks"]
                            if item["check"].endswith("_decimals")))

    def test_observed_zero_word_is_not_confused_with_unknown(self):
        self.rpc.overrides["eth_call"] = lambda value, payload: word(0) if (
            payload["params"][0]["data"] == runtime.SELECTORS["FEE_RECEIVER()"] ) else value
        result = self.execute()
        self.assertEqual(result["status"], "bounded_v3_runtime_observations_complete")
        values = [item for item in result["getters"] if item["signature"] == "FEE_RECEIVER()"]
        self.assertTrue(all(item["status"] == "observed" and item["value"] == "0x" + "00" * 20 for item in values))

    def test_proxy_direct_or_dependency_collateral_mismatches_block(self):
        self.rpc.overrides["eth_call"] = lambda value, payload: word(PM) if (
            payload["params"][0]["to"] == runtime.ADDRESS and
            payload["params"][0]["data"] == runtime.SELECTORS["COLLATERAL_TOKEN()"] ) else value
        self.blocked("proxy_and_implementation_getter_differs")
        self.rpc = FakeRPC()
        self.rpc.overrides["eth_call"] = lambda value, payload: word(PM) if (
            payload["params"][0]["to"] == PM and
            payload["params"][0]["data"] == runtime.SELECTORS["COLLATERAL_TOKEN()"] ) else value
        self.blocked("position_manager_collateral_differs")

    def test_optional_dependencies_preserve_final_header_budget(self):
        with patch.object(runtime, "MAX_RPC_CALLS", 37):
            result = self.execute()
        self.assertEqual(result["status"], "bounded_v3_runtime_observations_complete", result)
        self.assertEqual(result["rpc_calls_attempted"], 37)
        self.assertTrue(all(item["status"] == "not_attempted_call_budget_reserved" for item in result["dependency_checks"]))
        self.assertEqual([int(call["params"][0], 16) for call in self.rpc.calls[-4:]], list(runtime.BLOCKS))

    def test_fixed_selectors_not_sha3_and_only_roster_arguments(self):
        golden = {"POSITION_MANAGER()": "0x1bea83fe", "COLLATERAL_TOKEN()": "0xf5f1f1a7",
            "COMBINATORIAL_MODULE()": "0x5d85fba8", "FEE_RECEIVER()": "0xd3e78e4d", "MAX_FEE_RATE()": "0x92f6576e",
            "decimals()": "0x313ce567", "moduleById(uint256)": "0x4d51cdf1"}
        self.assertEqual(runtime.SELECTORS, golden)
        for signature, selector in golden.items():
            self.assertNotEqual(selector, "0x" + hashlib.sha3_256(signature.encode()).hexdigest()[:8])
        result = self.execute()
        self.assertEqual(result["status"], "bounded_v3_runtime_observations_complete")
        for call in self.rpc.calls:
            if call["method"] == "eth_call":
                transaction, block = call["params"]
                self.assertEqual(set(transaction), {"to", "data", "gas"})
                self.assertEqual(transaction["gas"], hex(runtime.GETTER_GAS))
                self.assertIn(transaction["data"][:10], golden.values())
                self.assertIn(int(block, 16), (runtime.prior.RAW_TAIL_BLOCK, runtime.prior.CUTOFF_BLOCK))

    def test_keccak_recomputes_golden_selectors_when_fixture_library_available(self):
        try:
            from Crypto.Hash import keccak
        except ImportError:
            self.skipTest("Optional fixture crypto library is unavailable; hardcoded golden vectors remain tested.")
        for signature, selector in runtime.SELECTORS.items():
            value = keccak.new(digest_bits=256)
            value.update(signature.encode())
            self.assertEqual(selector, "0x" + value.hexdigest()[:8])

    def test_suffix_remains_unparsed_for_valid_looking_or_unsupported_lengths(self):
        for raw in (b"", b"\x00", b"\xa1\x61a\x01\x00\x04", b"\x60\x00\xff\xff"):
            observed = runtime.suffix_evidence(raw)
            self.assertFalse(observed["cbor_decoded"])
            self.assertFalse(observed["metadata_presence_certified"])
            self.assertFalse(observed["cid_content_verified"])
            self.assertIsNone(observed["cid"])
            self.assertLessEqual(len(observed["suffix_hex"]), 2 + 514 * 2)

    def test_attempt_total_and_wire_caps_stop_without_overshoot(self):
        with patch.object(runtime, "MAX_RPC_CALLS", 5):
            result = self.blocked("rpc_attempt_limit")
            self.assertEqual(result["rpc_calls_attempted"], 5)
        self.rpc = FakeRPC()
        self.rpc.reply_override = lambda reply, payload, limit: (b"x" * limit, 200)
        result = self.blocked("rpc_response_size_refused")
        self.assertEqual(result["response_bytes_received"], runtime.MAX_RESPONSE_BYTES)
        self.assertEqual(result["response_bytes_charged"], runtime.MAX_RESPONSE_BYTES)
        self.rpc = FakeRPC()
        with patch.object(runtime, "MAX_TOTAL_RESPONSE_BYTES", 2):
            result = self.blocked("rpc_response_size_refused")
            self.assertLessEqual(result["response_bytes_charged"], 2)

    def test_unknown_transport_and_partial_read_charge_reserved_maximum(self):
        def broken(*args, **kwargs):
            raise OSError("SECRET partial body")
        result = self.blocked("bounded_input_transport_or_runtime_failure", transport=broken)
        self.assertEqual(result["response_bytes_charged"], runtime.MAX_RESPONSE_BYTES)
        self.assertEqual(result["response_bytes_received"], 0)
        entries = [json.loads(line) for line in (self.run_dir / "REQUESTS.jsonl").read_text().splitlines()]
        self.assertEqual([entry["event"] for entry in entries], ["start", "finish"])
        self.assertTrue(entries[1]["unobserved_body_charged_at_full_reservation"])
        class Response:
            status = 200
            def __enter__(self):
                return self
            def __exit__(self, *_):
                return False
            def read(self, size):
                self.size = size
                raise OSError("SECRET partial read")
        response = Response()
        with patch.object(runtime.shared.urllib.request, "urlopen", return_value=response):
            result = self.blocked("bounded_input_transport_or_runtime_failure", transport=runtime.http_response)
        self.assertEqual(result["response_bytes_charged"], runtime.MAX_RESPONSE_BYTES)
        self.assertEqual(response.size, runtime.MAX_RESPONSE_BYTES)

    def test_rpc_duplicate_keys_wrong_ids_error_envelopes_and_secrets(self):
        replies = [b'{"jsonrpc":"2.0","id":true,"result":"0x89"}',
                   b'{"jsonrpc":"2.0","id":1,"id":1,"result":"0x89"}',
                   b'not JSON SECRET', b'{"jsonrpc":"2.0","id":1,"result":NaN}']
        for raw in replies:
            self.rpc = FakeRPC()
            self.rpc.reply_override = lambda *_: (raw, 200)
            result = self.execute()
            self.assertEqual(result["status"], "blocked")
            self.assertNotIn("SECRET", json.dumps(result))
            self.assertFalse((self.run_dir / "COMPLETE.json").exists())

    def test_same_length_control_mutations_block_completion(self):
        for name in ("OWNER.json", "REQUESTS.jsonl"):
            self.rpc, changed = FakeRPC(), False
            def corrupt(endpoint, payload, **kwargs):
                nonlocal changed
                answer = self.rpc(endpoint, payload, **kwargs)
                if not changed:
                    path = self.run_dir / name
                    raw = path.read_bytes()
                    path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
                    changed = True
                return answer
            with self.subTest(name=name):
                self.blocked("owned_control_content_changed", transport=corrupt)

    def test_source_digest_source_reopen_and_own_uncommitted_blob(self):
        self.digest = "0" * 64
        self.assertEqual(self.blocked("source_plan_digest_differs")["rpc_calls_attempted"], 0)
        self.save_plan()
        calls = 0
        def changed(_head):
            nonlocal calls
            calls += 1
            return {"head": fixtures.PROBE_HEAD, "files": {"fixture": calls}}
        self.blocked("probe_source_changed_during_probe", snapshot=changed)
        scripts = self.root / "scripts"
        scripts.mkdir()
        (scripts / "probe_polymarket_v3_runtime_v1.py").write_bytes(b"runtime fixture")
        with patch.object(runtime, "REPO", self.root), patch.object(runtime.shared, "source_snapshot", return_value={"files": {}}), \
                patch.object(runtime.inventory, "code_state", return_value=fixtures.state(fixtures.PROBE_HEAD)), \
                patch.object(runtime.subprocess, "run", return_value=type("R", (), {"returncode": 128, "stdout": b""})()):
            with self.assertRaisesRegex(runtime.HistoryBlocked, "runtime_source_uncommitted_or_changed"):
                runtime.source_snapshot(fixtures.PROBE_HEAD)

    def test_completion_fsync_reopen_and_directory_failure_leave_only_capped_pending(self):
        original_write, original_fsync, original_open = runtime.Controls.write, runtime.shared.os.fsync, Path.open
        for failure in ("file", "directory", "reopen"):
            def fail_marker(controls, name, value, **kwargs):
                if name != "COMPLETE.json":
                    return original_write(controls, name, value, **kwargs)
                def fsync(fd):
                    is_dir = stat.S_ISDIR(os.fstat(fd).st_mode)
                    if (failure == "file" and not is_dir) or (failure == "directory" and is_dir):
                        raise OSError("marker fault")
                    return original_fsync(fd)
                def open_path(path, *args, **options):
                    if failure == "reopen" and path.name == ".COMPLETE.pending":
                        raise OSError("marker reopen fault")
                    return original_open(path, *args, **options)
                with patch.object(runtime.shared.os, "fsync", side_effect=fsync), patch.object(Path, "open", open_path):
                    return original_write(controls, name, value, **kwargs)
            with self.subTest(failure=failure), patch.object(runtime.Controls, "write", fail_marker):
                with self.assertRaises(OSError):
                    self.execute()
                self.assertFalse((self.run_dir / "COMPLETE.json").exists())
                self.assertTrue((self.run_dir / ".COMPLETE.pending").exists())
                self.assertLessEqual(sum(path.stat().st_size for path in self.run_dir.iterdir()), runtime.MAX_OUTPUT_BYTES)

    def test_resource_and_deadline_admission_and_prohibited_methods(self):
        value = self.blocked("available_ram_below_admission", resources={"ram_bytes": {"MemAvailable": 1}})
        self.assertEqual(value["rpc_calls_attempted"], 0)
        value = self.blocked("available_disk_below_admission", resources={
            "ram_bytes": {"MemAvailable": 10**12}, "disk_bytes": {"free": 1}})
        self.assertEqual(value["rpc_calls_attempted"], 0)
        with patch.object(runtime.shared, "RUN_ROOT", self.root), patch.object(runtime.shared.shutil, "disk_usage", return_value=fixtures.DISK):
            controls = runtime.Controls(self.root / "clock", {"owner": True})
            rpc = runtime.RuntimeRPC("SECRET", self.rpc, [], controls, 0, clock=lambda: runtime.MAX_RUNTIME_SECONDS)
            with self.assertRaisesRegex(runtime.HistoryBlocked, "runtime_deadline_exceeded"):
                rpc.call("eth_chainId", [])
            with self.assertRaisesRegex(runtime.HistoryBlocked, "rpc_method_not_permitted"):
                rpc.call("eth_getLogs", [])
            self.assertEqual(rpc.ledger, [])

    def test_inflight_deadline_retains_observed_response_charge_and_attempt(self):
        with patch.object(runtime.shared, "RUN_ROOT", self.root), patch.object(runtime.shared.shutil, "disk_usage", return_value=fixtures.DISK):
            controls = runtime.Controls(self.root / "inflight", {"owner": True})
            clock = iter((0, runtime.MAX_RUNTIME_SECONDS))
            rpc = runtime.RuntimeRPC("SECRET", self.rpc, [], controls, 0, clock=lambda: next(clock))
            with self.assertRaisesRegex(runtime.HistoryBlocked, "runtime_deadline_exceeded"):
                rpc.call("eth_chainId", [])
            self.assertEqual(len(rpc.ledger), 1)
            self.assertEqual(rpc.ledger[0]["status"], "failed")
            self.assertGreater(rpc.response_bytes_received, 0)
            self.assertEqual(rpc.response_bytes_received, rpc.response_bytes_charged)

    def test_actual_runtime_limit_context_installs480_seconds_and_restores(self):
        changes = []
        previous = (runtime.resource.RLIM_INFINITY, runtime.resource.RLIM_INFINITY)
        def current(_kind):
            return (runtime.MAX_RAM_BYTES, runtime.resource.RLIM_INFINITY) if changes else previous
        with patch.object(runtime.resource, "getrlimit", side_effect=current), \
                patch.object(runtime.resource, "setrlimit", side_effect=lambda *args: changes.append(args)), \
                patch.object(runtime.signal, "getsignal", return_value="old"), \
                patch.object(runtime.signal, "getitimer", return_value=(0.0, 0.0)), \
                patch.object(runtime.signal, "signal") as handler, patch.object(runtime.signal, "setitimer") as alarm:
            with runtime.runtime_limits() as effective:
                self.assertEqual(effective["wall_alarm_seconds"], 480)
                self.assertEqual(effective["address_space_soft_bytes"], 8 * 1024**3)
            self.assertEqual(changes[-1][1], previous)
            self.assertEqual(alarm.call_args_list[0].args, (runtime.signal.ITIMER_REAL, 480))
            self.assertEqual(alarm.call_args_list[-1].args, (runtime.signal.ITIMER_REAL, 0))
            self.assertEqual(handler.call_args_list[-1].args, (runtime.signal.SIGALRM, "old"))
        self.assertEqual(runtime.MAX_OUTPUT_BYTES, runtime.shared.MAX_OUTPUT_BYTES)

    def test_changed_plan_and_wrong_chain_preclude_completion(self):
        self.rpc.overrides["eth_chainId"] = lambda *_: "0x1"
        result = self.blocked("rpc_wrong_chain")
        self.assertEqual(result["rpc_calls_attempted"], 1)
        self.rpc, changed = FakeRPC(), False
        def mutate(endpoint, payload, **kwargs):
            nonlocal changed
            reply = self.rpc(endpoint, payload, **kwargs)
            if not changed:
                self.plan_path.write_bytes(self.plan_path.read_bytes() + b" ")
                changed = True
            return reply
        self.blocked("source_plan_changed_during_probe", transport=mutate)


if __name__ == "__main__":
    unittest.main()
