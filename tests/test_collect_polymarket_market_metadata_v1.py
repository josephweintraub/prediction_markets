"""Synthetic public-metadata pilot fixtures. Never request a live API."""
import base64
import io
import http.client
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts import collect_polymarket_market_metadata_v1 as pilot


HEAD = "1" * 40
SOURCE = {"head": HEAD, "files": {"fixture.py": {"bytes": 1, "sha256": "2" * 64}}}
FREE = shutil._ntuple_diskusage(10**12, 0, 10**12)


def page(cursor="MTAwMA==", number=1, **changes):
    value = {"limit": 1000, "count": 1, "next_cursor": cursor,
             "data": [{"condition_id": "0x" + f"{number:064x}", "tokens": [
                 {"token_id": str(number), "price": .99999, "winner": False}]}]}
    value.update(changes)
    return json.dumps(value, separators=(",", ":")).encode()


class Response:
    def __init__(self, raw, *, status=200, headers=None, url=None, read_error=False):
        self.raw, self.status, self.url, self.closed = raw, status, url, False
        self.headers = {"Content-Type": "application/json", "Content-Length": str(len(raw))}
        if headers:
            self.headers.update(headers)
        self.stream, self.read_error, self.read_bytes = io.BytesIO(raw), read_error, 0

    def read(self, size):
        if self.read_error:
            raise OSError("https://secret.invalid/?api_key=DO_NOT_DISCLOSE")
        raw = self.stream.read(size)
        self.read_bytes += len(raw)
        return raw

    def geturl(self):
        return self.url

    def close(self):
        self.closed = True


class PilotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.parent = Path(self.temp.name).resolve()
        self.output = self.parent / "run"
        self.calls, self.responses = [], []

    def transport(self, spec, timeout):
        self.calls.append((spec, timeout))
        value = self.responses.pop(0)
        if isinstance(value, BaseException):
            raise value
        value.url = value.url or spec["url"]
        return value

    def run_fixture(self, responses=None, *, source_effect=None):
        self.responses = responses if responses is not None else [Response(page()), Response(page("MjAwMA==", 2))]
        with patch.object(pilot, "source_snapshot", side_effect=source_effect, return_value=SOURCE), \
                patch.object(pilot, "available_ram", return_value=10**12), \
                patch.object(pilot.shutil, "disk_usage", return_value=FREE):
            return pilot.run_pilot(self.output, HEAD, transport=self.transport)

    def read(self, relative):
        return json.loads((self.output / relative).read_text())

    def assert_failed(self, result, code):
        self.assertEqual(result["status"], "failed_unaccepted")
        self.assertEqual(result["error_code"], code)
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertFalse(result["analysis_admitted"])

    def test_two_pages_are_pilot_complete_not_universe_complete(self):
        result = self.run_fixture()
        self.assertEqual(result["status"], "pilot_complete_universe_incomplete")
        self.assertEqual(result["successful_pages"], 2)
        self.assertEqual(len(self.calls), 2)
        self.assertIn("next_cursor=MA%3D%3D", self.calls[0][0]["url"])
        self.assertIn("next_cursor=MTAwMA%3D%3D", self.calls[1][0]["url"])
        self.assertFalse(result["universe_complete"])
        manifest = self.read("manifest.json")
        self.assertFalse(manifest["normalized_labels_emitted"])
        self.assertFalse(manifest["snapshot_consistency_certified"])
        self.assertEqual(manifest["markets_in_validated_pages"], 2)
        self.assertEqual((self.output / "responses/a01.body").read_bytes(), page())
        self.assertFalse(json.loads(page())["data"][0]["tokens"][0]["winner"])
        self.assertFalse(any("payout" in key for key in manifest))
        self.assertEqual(manifest["response_bytes"], len(page()) + len(page("MjAwMA==", 2)))
        complete = self.read("COMPLETE.json")
        self.assertEqual(complete["manifest"]["sha256"], pilot.digest((self.output / "manifest.json").read_bytes()))
        for relative, bound in manifest["artifacts"].items():
            raw = (self.output / relative).read_bytes()
            self.assertEqual(bound, {"bytes": len(raw), "sha256": pilot.digest(raw)})
        receipt = self.read("attempts/a01.FINISH.json")
        self.assertTrue(receipt["body_complete"])
        self.assertTrue(receipt["retrieved_utc"])
        self.assertEqual(receipt["response_sha256"], pilot.digest(page()))

    def test_explicit_terminal_cursor_is_distinct_from_page_cap(self):
        result = self.run_fixture([Response(page(pilot.END_CURSOR))])
        self.assertEqual(result["status"], "pilot_complete_terminal_cursor_observed")
        self.assertTrue(self.read("manifest.json")["requested_pagination_complete"])
        self.assertTrue(result["terminal_cursor_observed"])
        self.assertFalse(result["universe_complete"])
        self.assertEqual(len(self.calls), 1)

    def test_full_sized_1000_market_pages_fit_bounded_control_receipts(self):
        markets = [{"condition_id": "0x" + f"{index:064x}", "tokens": [{"winner": False, "price": 1}]}
                   for index in range(1, 2001)]
        first = page(count=1000, data=markets[:1000])
        second = page("MjAwMA==", count=1000, data=markets[1000:])
        result = self.run_fixture([Response(first), Response(second)])
        self.assertEqual(result["status"], "pilot_complete_universe_incomplete")
        self.assertEqual(self.read("manifest.json")["markets_in_validated_pages"], 2000)
        self.assertEqual((self.output / "responses/a01.body").read_bytes(), first)
        self.assertLess((self.output / "manifest.json").stat().st_size, pilot.MAX_CONTROL)

    def test_literal_raw_winner_absence_is_not_payout_inference(self):
        raw = page(pilot.END_CURSOR, data=[{"condition_id": "0x" + "a" * 64,
                                          "tokens": [{"price": 1.0}]}])
        result = self.run_fixture([Response(raw)])
        self.assertEqual(result["successful_pages"], 1)
        self.assertEqual((self.output / "responses/a01.body").read_bytes(), raw)
        self.assertNotIn(b'"winner"', raw)

    def test_empty_terminal_page_is_allowed_but_empty_nonterminal_fails(self):
        raw = page(pilot.END_CURSOR, count=0, data=[])
        self.assertEqual(self.run_fixture([Response(raw)])["successful_pages"], 1)
        self.output = self.parent / "other"
        self.assert_failed(self.run_fixture([Response(page(count=0, data=[]))]), "empty_nonterminal_page")

    def test_missing_repeated_malformed_or_cyclic_cursor_fails(self):
        for cursor in (None, "", "MA==", "not base64", "Zg===", 1):
            with self.subTest(cursor=cursor):
                self.output = self.parent / ("run_" + str(len(list(self.parent.iterdir()))))
                result = self.run_fixture([Response(page(cursor))])
                self.assert_failed(result, "repeated_cursor" if cursor == "MA==" else "invalid_cursor")
        self.output = self.parent / "cycle"
        self.assert_failed(self.run_fixture([Response(page()), Response(page("MA==", 2))]), "repeated_cursor")
        self.output = self.parent / "missing"
        missing = json.loads(page()); del missing["next_cursor"]
        self.assert_failed(self.run_fixture([Response(json.dumps(missing).encode())]), "page_envelope_schema")

    def test_envelope_counts_objects_and_literal_winner_types_are_strict(self):
        cases = [(page(count=True), "page_count_or_data_schema"),
                 (page(limit=False), "page_count_or_data_schema"),
                 (page(count=2), "page_count_or_data_schema"),
                 (page(data={}), "page_count_or_data_schema"),
                 (page(error="DO_NOT_LOG_ERROR_TEXT"), "page_envelope_schema"),
                 (page(data=["not an object"]), "market_object_schema"),
                 (page(data=[{"condition_id": ""}]), "market_object_schema"),
                 (page(data=[{"condition_id": "0x" + "1" * 64, "tokens": None}]), "market_tokens_schema"),
                 (page(data=[{"condition_id": "0x" + "1" * 64, "tokens": [{"winner": 1}]}]), "literal_winner_schema")]
        for index, (raw, code) in enumerate(cases):
            with self.subTest(code=code, index=index):
                self.output = self.parent / f"schema_{index}"
                self.assert_failed(self.run_fixture([Response(raw)]), code)

    def test_duplicate_conditions_inside_or_across_pages_fail(self):
        market = json.loads(page())["data"][0]
        self.assert_failed(self.run_fixture([Response(page(count=2, data=[market, market]))]), "duplicate_market_condition")
        self.output = self.parent / "crosspage"
        self.assert_failed(self.run_fixture([Response(page()), Response(page("MjAwMA==", 1))]), "duplicate_market_condition")

    def test_json_duplicate_keys_nonfinite_and_non_utf8_fail(self):
        for index, (raw, code) in enumerate([(b'{"data":[],"data":[]}', "duplicate_json_key"),
                                            (b'{"x":NaN}', "nonfinite_json"),
                                            (b'{"x":1e999}', "nonfinite_json"),
                                            (b'\xff', "invalid_json"), (b'<html/>', "invalid_json")]):
            self.output = self.parent / f"json_{index}"
            self.assert_failed(self.run_fixture([Response(raw)]), code)

    def test_retryable_failures_are_charged_and_retry_the_same_cursor(self):
        result = self.run_fixture([Response(b'{"error":"busy"}', status=429),
                                   OSError("api_key=DO_NOT_DISCLOSE"), Response(page(pilot.END_CURSOR))])
        self.assertEqual(result["http_attempts"], 3)
        self.assertEqual(len({call[0]["url"] for call in self.calls}), 1)
        self.assertEqual(self.read("manifest.json")["response_bytes_charged"],
                         pilot.CAPS["individual_response_bytes"] + len(b'{"error":"busy"}') + len(page(pilot.END_CURSOR)))
        self.assertEqual(self.read("manifest.json")["response_bytes_observed"],
                         len(b'{"error":"busy"}') + len(page(pilot.END_CURSOR)))
        self.assertFalse(self.read("attempts/a02.FINISH.json")["body_complete"])

    def test_attempt_exhaustion_never_completes_or_leaks_exception_text(self):
        result = self.run_fixture([OSError("https://secret.invalid/?api_key=DO_NOT_DISCLOSE")] +
                                  [Response(b'{"error":"busy"}', status=503) for _ in range(4)])
        self.assert_failed(result, "http_attempt_limit")
        self.assertEqual(len(self.calls), 5)
        self.assertEqual(len(list((self.output / "attempts").glob("*.START.json"))), 5)
        for file in self.output.rglob("*"):
            if file.is_file():
                self.assertNotIn(b"DO_NOT_DISCLOSE", file.read_bytes())
        self.assertNotIn("DO_NOT_DISCLOSE", json.dumps(result))

    def test_status_encoding_type_redirect_and_transport_contract_fail_closed(self):
        for index, (response, code) in enumerate([
            (Response(page(), status=401), "http_status_not_200"),
            (Response(page(), status="200"), "invalid_http_status"),
            (Response(page(), headers={"Content-Type": "text/html"}), "unexpected_content_type"),
            (Response(page(), headers={"Content-Encoding": "gzip"}), "unexpected_content_encoding"),
            (Response(page(), url="https://evil.invalid/"), "redirect_or_response_url_changed"),
            (Response(page(), read_error=True), "response_transport_error"),
            (Response(page(), headers={"Content-Length": "-1"}), "invalid_content_length"),
            (Response(page(), headers={"Content-Length": str(len(page()) + 1)}), "truncated_response"),
            (Response(page(), headers={"Content-Length": "1"}), "content_length_differs"),
        ]):
            self.output = self.parent / f"http_{index}"
            self.assert_failed(self.run_fixture([response]), code)

    def test_individual_response_cap_prevents_read_and_unknown_length_never_overreads(self):
        big = Response(b"x" * (pilot.CAPS["individual_response_bytes"] + 1))
        self.assert_failed(self.run_fixture([big]), "response_byte_limit")
        self.assertEqual(big.read_bytes, 0)
        self.output = self.parent / "unknown_length"
        big = Response(b"x" * (pilot.CAPS["individual_response_bytes"] + 1))
        del big.headers["Content-Length"]
        self.assert_failed(self.run_fixture([big]), "response_byte_limit")
        self.assertEqual(big.read_bytes, pilot.CAPS["individual_response_bytes"])
        self.assertLessEqual(sum(p.stat().st_size for p in self.output.rglob("*") if p.is_file()), pilot.CAPS["all_artifact_bytes"])

    def test_cumulative_body_budget_is_shared_by_failed_attempts(self):
        raw = b"x" * 1024
        with patch.dict(pilot.CAPS, {"total_response_bytes": 2048}):
            result = self.run_fixture([Response(raw, status=503), Response(raw, status=503)])
        self.assert_failed(result, "total_response_byte_limit")
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(result["response_bytes"], 2048)

    def test_opaque_transport_failures_reserve_full_body_allowance_and_stop_at_total_cap(self):
        result = self.run_fixture([OSError("api_key=DO_NOT_DISCLOSE")] * 5)
        self.assert_failed(result, "total_response_byte_limit")
        self.assertEqual(len(self.calls), 4)
        self.assertEqual(result["response_bytes_charged"], 16 * 1024**2)
        self.assertEqual(result["response_bytes_observed"], 0)
        self.assertTrue(self.read("attempts/a01.FINISH.json")["charge_is_conservative"])

    def test_deadline_during_transport_charges_attempt_reservation(self):
        result = self.run_fixture([pilot.PilotError("runtime_limit")])
        self.assert_failed(result, "runtime_limit")
        self.assertEqual(result["response_bytes_charged"], pilot.CAPS["individual_response_bytes"])
        receipt = self.read("attempts/a01.FINISH.json")
        self.assertTrue(receipt["transport_dispatched"])
        self.assertTrue(receipt["charge_is_conservative"])

    def test_close_failure_after_full_body_is_charged_and_receipted(self):
        class CloseFault(Response):
            def close(self):
                raise OSError("api_key=DO_NOT_DISCLOSE")
        result = self.run_fixture([CloseFault(page())])
        self.assert_failed(result, "response_close_error")
        self.assertEqual(result["response_bytes_observed"], len(page()))
        self.assertEqual(result["response_bytes_charged"], pilot.CAPS["individual_response_bytes"])
        receipt = self.read("attempts/a01.FINISH.json")
        self.assertEqual(receipt["response_bytes"], len(page()))
        self.assertTrue(receipt["charge_is_conservative"])
        self.assertEqual((self.output / "responses/a01.body").read_bytes(), page())

    def test_incomplete_read_partial_bytes_are_retained_and_charged(self):
        class IncompleteFault(Response):
            def read(self, size):
                raise http.client.IncompleteRead(b"x" * 123)
        result = self.run_fixture([IncompleteFault(page())])
        self.assert_failed(result, "incomplete_response")
        self.assertEqual(result["response_bytes_observed"], 123)
        self.assertEqual(result["response_bytes_charged"], pilot.CAPS["individual_response_bytes"])
        self.assertEqual((self.output / "responses/a01.body").read_bytes(), b"x" * 123)
        self.assertFalse(self.read("attempts/a01.FINISH.json")["body_complete"])

    def test_opaque_mid_read_failure_keeps_observed_prefix_and_conservative_charge(self):
        class MidReadFault(Response):
            def read(self, size):
                if self.read_bytes:
                    raise OSError("unknown internally consumed bytes; api_key=DO_NOT_DISCLOSE")
                self.read_bytes += 3
                return b"abc"
        result = self.run_fixture([MidReadFault(page())])
        self.assert_failed(result, "response_transport_error")
        self.assertEqual(result["response_bytes_observed"], 3)
        self.assertEqual(result["response_bytes_charged"], pilot.CAPS["individual_response_bytes"])
        self.assertEqual((self.output / "responses/a01.body").read_bytes(), b"abc")
        self.assertNotIn(b"DO_NOT_DISCLOSE", (self.output / "attempts/a01.FINISH.json").read_bytes())

    def test_existing_output_is_never_overwritten(self):
        self.output.mkdir(); keep = self.output / "keep"; keep.write_bytes(b"unchanged")
        with self.assertRaisesRegex(pilot.PilotError, "destination_exists"):
            self.run_fixture()
        self.assertEqual(keep.read_bytes(), b"unchanged")
        self.assertEqual(self.calls, [])

    def test_completion_publication_failure_leaves_only_unaccepted_pending_marker(self):
        with patch.object(pilot, "atomic_completion", side_effect=pilot.PilotError("atomic_completion_refused")):
            result = self.run_fixture([Response(page(pilot.END_CURSOR))])
        self.assert_failed(result, "atomic_completion_refused")
        self.assertTrue((self.output / ".COMPLETE.pending").exists())

    def test_atomic_completion_never_replaces_existing_destination(self):
        temporary, destination = self.parent / "temporary", self.parent / "destination"
        temporary.write_bytes(b"new"); destination.write_bytes(b"original")
        with self.assertRaisesRegex(pilot.PilotError, "atomic_completion_refused"):
            pilot.atomic_completion(temporary, destination)
        self.assertEqual(destination.read_bytes(), b"original")
        self.assertEqual(temporary.read_bytes(), b"new")

    def test_bootstrap_source_and_resource_failure_precedes_output_and_network(self):
        with self.assertRaisesRegex(pilot.PilotError, "source_not_clean"):
            self.run_fixture(source_effect=pilot.PilotError("source_not_clean"))
        self.assertFalse(self.output.exists()); self.assertFalse(self.calls)
        with patch.object(pilot, "source_snapshot", return_value=SOURCE), \
                patch.object(pilot, "available_ram", return_value=1):
            with self.assertRaisesRegex(pilot.PilotError, "available_ram_below_floor"):
                pilot.run_pilot(self.output, HEAD, transport=self.transport)
        self.assertFalse(self.output.exists())

    def test_source_mutation_during_request_blocks_completion(self):
        other = {**SOURCE, "head": "3" * 40}
        result = self.run_fixture([Response(page())], source_effect=[SOURCE, SOURCE, other])
        self.assert_failed(result, "source_changed")
        self.assertEqual(len(self.calls), 1)
        self.assertTrue((self.output / "responses/a01.body").exists())

    def test_source_mutation_at_final_gate_withholds_completion_marker(self):
        other = {**SOURCE, "head": "3" * 40}
        result = self.run_fixture([Response(page(pilot.END_CURSOR))], source_effect=[SOURCE] * 4 + [other])
        self.assert_failed(result, "source_changed")
        self.assertTrue((self.output / "manifest.json").exists())

    def test_unexpected_transport_failure_is_sanitized_and_interrupt_is_unaccepted(self):
        result = self.run_fixture([RuntimeError("password=DO_NOT_DISCLOSE")])
        self.assert_failed(result, "request_runtime_error")
        self.assertNotIn(b"DO_NOT_DISCLOSE", (self.output / "FAILURE.json").read_bytes())
        self.output = self.parent / "interrupted"
        with self.assertRaises(KeyboardInterrupt):
            self.run_fixture([KeyboardInterrupt()])
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertEqual(self.read("FAILURE.json")["error_code"], "interrupted")

    def test_runtime_artifact_cap_and_disk_floor_are_enforced(self):
        with patch.dict(pilot.CAPS, {"runtime_seconds": 0}):
            self.assert_failed(self.run_fixture(), "runtime_limit")
        self.assertFalse(self.calls)
        self.output = self.parent / "tiny_artifacts"
        with patch.dict(pilot.CAPS, {"all_artifact_bytes": 10}):
            self.assert_failed(self.run_fixture(), "artifact_byte_limit")
        self.assertFalse(self.calls)
        self.output = self.parent / "disk"
        with patch.object(pilot, "source_snapshot", return_value=SOURCE), \
                patch.object(pilot, "available_ram", return_value=10**12), \
                patch.object(pilot.shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 1)):
            with self.assertRaisesRegex(pilot.PilotError, "disk_floor"):
                pilot.run_pilot(self.output, HEAD, transport=self.transport)
        self.assertFalse(self.output.exists())

    def test_output_or_body_mutation_fails_reopening(self):
        def mutate(spec, timeout):
            response = self.transport(spec, timeout)
            if len(self.calls) == 2:
                (self.output / "responses/a01.body").write_bytes(b"drift")
            return response
        self.responses = [Response(page()), Response(page("MjAwMA==", 2))]
        with patch.object(pilot, "source_snapshot", return_value=SOURCE), \
                patch.object(pilot, "available_ram", return_value=10**12), \
                patch.object(pilot.shutil, "disk_usage", return_value=FREE):
            result = pilot.run_pilot(self.output, HEAD, transport=mutate)
        self.assert_failed(result, "artifact_reopen_differs")

    def test_cli_production_guard_precedes_source_destination_limits_or_network(self):
        with patch.object(pilot, "require_production_host", side_effect=RuntimeError("guard")), \
                patch.object(pilot, "source_snapshot") as source, \
                patch.object(pilot, "process_limits") as limits, \
                patch.object(pilot, "public_transport") as network, \
                patch.object(Path, "mkdir") as mkdir:
            with self.assertRaisesRegex(RuntimeError, "guard"):
                pilot.main(["--run-dir", "/mnt/data/runs/pilot", "--expected-head", HEAD])
            source.assert_not_called(); limits.assert_not_called(); network.assert_not_called(); mkdir.assert_not_called()

    def test_help_is_local_and_has_no_endpoint_or_cap_overrides(self):
        result = subprocess.run([sys.executable, str(Path(pilot.__file__)), "--help"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn("--expected-head", result.stdout)
        self.assertNotIn("--endpoint", result.stdout)
        self.assertNotIn("--max-", result.stdout)

    def test_real_source_snapshot_requires_tracked_clean_files_and_expected_head(self):
        root = self.parent / "source"; root.mkdir()
        for name in pilot.SOURCE_FILES:
            file = root / name; file.parent.mkdir(parents=True, exist_ok=True); file.write_text("fixture\n")
        subprocess.run(["git", "init", "--quiet", str(root)], check=True)
        subprocess.run(["git", "-C", str(root), "add", *pilot.SOURCE_FILES], check=True)
        subprocess.run(["git", "-C", str(root), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "commit", "--quiet", "-m", "Fixture source"], check=True)
        head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
        with patch.object(pilot, "REPO", root):
            snapshot = pilot.source_snapshot(head)
            self.assertEqual(snapshot["head"], head)
            self.assertEqual(set(snapshot["files"]), set(pilot.SOURCE_FILES))
            with self.assertRaisesRegex(pilot.PilotError, "source_head_differs"):
                pilot.source_snapshot(HEAD)
            with self.assertRaisesRegex(pilot.PilotError, "invalid_expected_head"):
                pilot.source_snapshot("api_key=DO_NOT_DISCLOSE")
            (root / "untracked.txt").write_text("fixture")
            with self.assertRaisesRegex(pilot.PilotError, "source_not_clean"):
                pilot.source_snapshot(head)
            (root / "untracked.txt").unlink()
            subprocess.run(["git", "-C", str(root), "update-index", "--assume-unchanged", "production_guard.py"], check=True)
            (root / "production_guard.py").write_text("changed\n")
            with self.assertRaisesRegex(pilot.PilotError, "source_bytes_differ_from_commit"):
                pilot.source_snapshot(head)

    def test_memory_limit_and_alarm_are_effective_and_restored(self):
        with patch.object(pilot.resource, "getrlimit", side_effect=[(-1, -1), (pilot.CAPS["address_space_bytes"], -1)]), \
                patch.object(pilot.resource, "setrlimit") as limits, \
                patch.object(pilot.signal, "getsignal", return_value=pilot.signal.SIG_DFL), \
                patch.object(pilot.signal, "signal") as handlers, patch.object(pilot.signal, "alarm") as alarm:
            with pilot.process_limits():
                pass
        self.assertEqual(limits.call_args_list[0].args[1], (8 * 1024**3, -1))
        self.assertEqual(limits.call_args_list[-1].args[1], (-1, -1))
        self.assertEqual(alarm.call_args_list[0].args, (300,))
        self.assertEqual(alarm.call_args_list[-1].args, (0,))
        self.assertEqual(handlers.call_args_list[-1].args[1], pilot.signal.SIG_DFL)


if __name__ == "__main__":
    unittest.main()
