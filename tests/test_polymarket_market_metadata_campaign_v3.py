"""Synthetic full-campaign fixtures. No live service or production data."""
import gzip
import http.client
from contextlib import nullcontext
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts import collect_polymarket_market_metadata_campaign_v3 as campaign
from tests.test_polymarket_raw_metadata_probe_v2 import (
    Response, clob_page, clob_row, gamma_page, gamma_row, capped_responses)

HEAD, PRODUCER = "a" * 40, "b" * 40
FREE = shutil._ntuple_diskusage(10**12, 0, 10**12)


def full_responses():
    result = [Response(clob_page()), Response(clob_page("LTE=", [clob_row(2)]))]
    for stream in campaign.STREAMS[1:]:
        result += [Response(json.dumps({"$schema": campaign.GAMMA_SCHEMA_URI,
                    "markets": [gamma_row(stream, n) for n in range(1, 101)],
                    "next_cursor": "opaque-cursor_-"}).encode()),
                   Response(gamma_page(stream, rows=[gamma_row(stream, 101)]))]
    return result


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.output, self.pilot_root = self.root / "campaign", self.root / "probe"
        self.admission_path = self.root / "admission.json"
        self.blobs = {name: (campaign.REPO / name).read_bytes() for name in campaign.SOURCE_FILES}
        self.source = {"head": HEAD, "files": {name: {"bytes": len(raw), "sha256": campaign.digest(raw)}
                                             for name, raw in self.blobs.items()}}
        producer = {"head": PRODUCER, "files": {name: self.source["files"][name] for name in campaign.probe.SOURCE_FILES}}
        responses = capped_responses()
        for response in responses[2:]:
            value = json.loads(response.raw)
            value["$schema"] = campaign.GAMMA_SCHEMA_URI
            response.raw = json.dumps(value).encode()
            response.stream = __import__("io").BytesIO(response.raw)
            response.headers["Content-Length"] = str(len(response.raw))
        def transport(spec, timeout):
            response = responses.pop(0); response.url = spec["url"]
            return response
        with patch.object(campaign.probe, "source_snapshot", return_value=producer), \
                patch.object(campaign.probe, "available_ram", return_value=10**12), \
                patch.object(campaign.probe.shutil, "disk_usage", return_value=FREE):
            result = campaign.probe.run_probe(self.pilot_root, PRODUCER, transport=transport)
        self.assertNotEqual(result["status"], "failed_unaccepted")
        self.pilot_manifest = json.loads((self.pilot_root / "manifest.json").read_bytes())
        files = [self.bound(path) | {"category": "raw" if path.name.endswith(".body.gz") else "control"}
                 for path in sorted(self.pilot_root.rglob("*")) if path.is_file()]
        self.admission = {"schema_version": campaign.SCHEMA, "status": "root_metadata_campaign_admitted",
            "run_dir": str(self.output), "consumer_head": HEAD, "caps": campaign.CAPS,
            "scope_sha256": campaign.digest(campaign.encode(campaign.SCOPE)),
            "approved_probe": {"schema_continuation_accepted": True, "producer_head": PRODUCER,
                "producer_blobs": producer["files"], "complete": self.bound(self.pilot_root / "COMPLETE.json"),
                "manifest": self.bound(self.pilot_root / "manifest.json")},
            "prior_ledger": {"all_prior_metadata_roots_included": True, "roots": [str(self.pilot_root)],
                "standalone_control_files": [], "files": files,
                "http_attempts": self.pilot_manifest["http_attempts"],
                "response_bytes_charged": self.pilot_manifest["response_bytes_charged"], "runtime_seconds": 100}}
        self.calls, self.responses, self.tick = [], [], 0.0

    def bound(self, path):
        raw = path.read_bytes()
        return {"path": str(path), "bytes": len(raw), "sha256": campaign.digest(raw)}

    def save_admission(self):
        raw = campaign.encode(self.admission)
        self.admission_path.write_bytes(raw)
        return campaign.digest(raw)

    def producer_git(self, args, **kwargs):
        self.assertEqual(args[:2], ["git", "cat-file"])
        head, name = args[3].split(":", 1)
        self.assertEqual(head, PRODUCER)
        raw = self.blobs[name]
        return (str(len(raw)) + "\n").encode() if args[2] == "-s" else raw

    def transport(self, spec, timeout):
        self.calls.append((spec, timeout, self.tick))
        response = self.responses.pop(0)
        if isinstance(response, BaseException): raise response
        response.url = response.url or spec["url"]
        return response

    def sleep(self, amount):
        self.assertLessEqual(amount, .25)
        self.tick += amount

    def execute(self, responses=None, *, transport=None, source_effect=None):
        sha = self.save_admission()
        self.responses = full_responses() if responses is None else responses
        with patch.object(campaign, "RUN_ROOT", self.root), \
                patch.object(campaign, "source_snapshot", side_effect=source_effect, return_value=self.source), \
                patch.object(campaign.subprocess, "check_output", side_effect=self.producer_git), \
                patch.object(campaign.probe, "available_ram", return_value=10**12), \
                patch.object(campaign.shutil, "disk_usage", return_value=FREE), \
                patch.object(campaign.time, "monotonic", side_effect=lambda: self.tick):
            return campaign.run_campaign(self.output, HEAD, self.admission_path, sha,
                                         transport=transport or self.transport, sleeper=self.sleep)

    def read(self, relative):
        return json.loads((self.output / relative).read_bytes())

    def lines(self, relative):
        return [json.loads(line) for line in (self.output / relative).read_bytes().splitlines()]

    def body(self, call=1):
        return gzip.decompress((self.output / f"responses/a{call:05d}.body.gz").read_bytes())

    def assert_failed(self, value, code):
        self.assertEqual(value["status"], "failed_unaccepted")
        self.assertEqual(value["error_code"], code)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_five_streams_finish_only_at_actual_terminal_not_page_cap(self):
        value = self.execute()
        self.assertEqual(value["status"], campaign.SUCCESS)
        manifest = self.read("manifest.json")
        self.assertTrue(all(s["pagination_terminal_observed"] for s in manifest["streams"]))
        self.assertEqual(manifest["waterfall"]["raw_rows_in_accepted_pages"], 406)
        self.assertEqual(manifest["waterfall"]["accepted_pages"], 10)
        self.assertEqual(len(self.calls), 10)
        self.assertTrue(all(self.calls[n][2] - self.calls[n-1][2] >= .5 for n in range(1, len(self.calls))))
        for field in ("universe_complete", "analysis_admitted", "family_or_payout_admitted",
                      "normalized_labels_emitted", "schema_provider_or_record_universe_certified"):
            self.assertFalse(manifest[field])
        self.assertIn("limit=100", self.calls[2][0]["url"])
        self.assertIn("after_cursor=opaque-cursor_-", self.calls[3][0]["url"])
        self.assertEqual(self.body(), clob_page())
        complete = self.read("COMPLETE.json")
        self.assertEqual(complete["manifest"]["sha256"], campaign.digest((self.output / "manifest.json").read_bytes()))
        self.assertEqual(manifest["usage"]["external_control_reservation_not_observed_bytes"], 2 * 1024**2)

    def test_more_than_two_pages_are_collected_without_truncating_scope(self):
        responses = full_responses()
        responses[1:2] = [Response(clob_page("MjAwMA==", [clob_row(2)])),
                          Response(clob_page("LTE=", [clob_row(3)]))]
        value = self.execute(responses)
        self.assertEqual(value["status"], campaign.SUCCESS)
        self.assertEqual(value["streams"][0]["accepted_pages"], 3)
        self.assertEqual(len(self.calls), 11)

    def test_real_blank_ids_and_scalar_rows_remain_counted_with_implicit_all_row_locators(self):
        rows = [clob_row(index + 1) for index in range(1000)]
        for index in range(54):
            rows[index]["condition_id"] = ""
            for token in rows[index]["tokens"]: token["token_id"] = ""
        for token in rows[54]["tokens"]: token["token_id"] = ""
        rows += [None, "unrecognized", 1, False, []]
        raw = clob_page("LTE=", rows, limit=2000)
        value = self.execute([Response(raw), *[Response(gamma_page(s)) for s in campaign.STREAMS[1:]]])
        self.assertEqual(value["status"], campaign.SUCCESS)
        self.assertEqual(self.body(), raw)
        summary = self.lines("pages.jsonl")[0]["row_summary"]
        self.assertEqual(summary["raw_row_count"], 1005)
        self.assertEqual(summary["identity_issue_counts"]["condition_id_empty"], 54)
        self.assertEqual(summary["identity_issue_counts"]["token_id_empty"], 110)
        self.assertEqual(summary["identity_issue_counts"]["row_not_object"], 5)
        self.assertEqual(len(summary["issue_rows"]), 60)
        issue_bits = {code: 1 << index for index, code in enumerate(summary["identity_issue_bit_order"])}
        self.assertTrue(dict(summary["issue_rows"])[0] & issue_bits["token_id_empty"])
        self.assertEqual(dict(summary["issue_rows"])[54], issue_bits["token_id_empty"])
        self.assertIn("ordinal", summary["all_row_locators"])
        self.assertNotIn("records", summary)

    def test_unknown_gamma_scope_is_not_confused_with_observed_terminal(self):
        responses = [Response(clob_page("LTE=")), Response(gamma_page(campaign.STREAMS[1], rows=[{}, None]))]
        responses += [Response(gamma_page(s)) for s in campaign.STREAMS[2:]]
        value = self.execute(responses)
        self.assertEqual(value["status"], campaign.SUCCESS)
        self.assertTrue(value["streams"][1]["pagination_terminal_observed"])
        self.assertFalse(value["streams"][1]["returned_scope_fields_verified"])

    def test_explicit_scope_conflict_retains_body_and_failed_page_waterfall(self):
        row = gamma_row(campaign.STREAMS[1]) | {"version": "v2"}
        raw = gamma_page(campaign.STREAMS[1], rows=[row])
        value = self.execute([Response(clob_page("LTE=")), Response(raw)])
        self.assert_failed(value, "gamma_returned_scope_conflict")
        self.assertEqual(self.body(2), raw)
        page = self.lines("pages.jsonl")[-1]
        self.assertFalse(page["accepted"])
        self.assertTrue(page["pagination_terminal_observed"])
        self.assertEqual(value["waterfall"]["explicit_scope_conflict_occurrences_seen"], 1)

    def test_gamma_exact_schema_uri_and_provider_specific_cursors(self):
        stream = campaign.STREAMS[1]
        for extra, code in (({"$schema": "https://wrong.invalid/schema"}, "gamma_schema_uri_differs"),
                            ({"$schema": None}, "gamma_schema_uri_differs"),
                            ({"next_cursor": None}, "invalid_cursor"),
                            ({"next_cursor": "opaque"}, "gamma_short_page_has_cursor")):
            raw = campaign.encode({"markets": []} | extra)
            with self.assertRaisesRegex(campaign.Error, code):
                campaign.page_envelope(raw, stream, None, {None}, set())
        with self.assertRaisesRegex(campaign.Error, "gamma_full_page_missing_cursor"):
            campaign.page_envelope(campaign.encode({"markets": [{}] * 100}), stream, None, {None}, set())
        value = campaign.page_envelope(campaign.encode({"markets": [{}] * 100, "next_cursor": "LTE="}),
                                       stream, None, {None}, set())
        self.assertFalse(value["pagination_terminal_observed"])

    def test_missing_repeated_cursor_and_repeated_rows_cannot_complete(self):
        self.assert_failed(self.execute([Response(clob_page("MA=="))]), "repeated_cursor")
        self.output = self.root / "repeat"
        self.admission["run_dir"] = str(self.output)
        self.assert_failed(self.execute([Response(clob_page()), Response(clob_page("MjAwMA=="))]), "repeated_page_rows")

    def test_three_attempts_per_exact_page_are_shared_not_reset(self):
        value = self.execute([Response(b'{}', status=503) for _ in range(3)])
        self.assert_failed(value, "http_status_not_200")
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(len({spec["url"] for spec, _, _ in self.calls}), 1)
        starts = [row for row in self.lines("attempts.jsonl") if row["event"] == "START"]
        self.assertEqual([row["exact_page_attempt"] for row in starts], [1, 2, 3])

    def test_retry_after_and_two_requests_per_second_are_preserved(self):
        busy = Response(b'{}', status=429, headers={"Retry-After": "3"})
        value = self.execute([busy, *full_responses()])
        self.assertEqual(value["status"], campaign.SUCCESS)
        self.assertGreaterEqual(self.calls[1][2] - self.calls[0][2], 3)
        finish = [r for r in self.lines("attempts.jsonl") if r["event"] == "FINISH"][0]
        self.assertEqual(finish["retry_after_header"], "3")
        self.assertEqual(finish["retry_delay_seconds"], 3)
        self.assertEqual(campaign.retry_delay("Thu, 01 Jan 1970 00:00:05 GMT", now=0), 5)
        with self.assertRaisesRegex(campaign.Error, "retry_after_exceeds_bound"):
            campaign.retry_delay("61")

    def test_excessive_retry_after_fails_without_ignoring_provider_wait(self):
        value = self.execute([Response(b'{}', status=429, headers={"Retry-After": "61"})])
        self.assert_failed(value, "retry_after_exceeds_bound")
        self.assertEqual(len(self.calls), 1)

    def test_opaque_failure_conservative_charge_and_exception_redaction(self):
        value = self.execute([OSError("api_key=DO_NOT_DISCLOSE"), *full_responses()])
        self.assertEqual(value["status"], campaign.SUCCESS)
        self.assertEqual(self.body(), b"")
        first = self.lines("attempts.jsonl")[1]
        self.assertTrue(first["charge_is_conservative"])
        self.assertEqual(first["response_bytes_charged"], 4 * 1024**2)
        for path in self.output.rglob("*"):
            if path.is_file(): self.assertNotIn(b"DO_NOT_DISCLOSE", path.read_bytes())

    def test_incomplete_read_close_failure_and_deadline_are_receipted(self):
        class Partial(Response):
            def read(self, size): raise http.client.IncompleteRead(b"partial")
        class Close(Response):
            def close(self): raise OSError("secret")
        for index, (response, code, retained) in enumerate(((Partial(clob_page()), "incomplete_response", b"partial"),
                                                           (Close(clob_page()), "response_close_error", clob_page()),
                                                           (campaign.Error("runtime_limit"), "runtime_limit", b""))):
            self.output = self.root / f"fault_{index}"; self.admission["run_dir"] = str(self.output)
            self.calls = []
            value = self.execute([response])
            self.assert_failed(value, code)
            self.assertEqual(self.body(), retained)
            self.assertEqual(self.lines("attempts.jsonl")[-1]["response_bytes_charged"], 4 * 1024**2)

    def test_keyboard_interrupt_retains_observed_prefix_and_full_charge_before_reraise(self):
        prefix = b"observed-prefix"
        class Interrupted(Response):
            count = 0
            def read(self, size):
                self.count += 1
                if self.count == 1: return prefix
                raise KeyboardInterrupt()
        response = Interrupted(clob_page())
        with patch.object(campaign.probe, "read_response", side_effect=AssertionError("frozen helper called")):
            with self.assertRaises(KeyboardInterrupt): self.execute([response])
        self.assertTrue(response.closed)
        self.assertEqual(self.body(), prefix)
        finish = self.lines("attempts.jsonl")[-1]
        self.assertEqual(finish["error_code"], "interrupted")
        self.assertTrue(finish["interrupted"])
        self.assertEqual(finish["response_bytes_observed"], len(prefix))
        self.assertEqual(finish["response_bytes_charged"], 4 * 1024**2)
        self.assertFalse(finish["body_complete"])
        self.assertEqual(self.read("FAILURE.json")["usage"]["response_bytes_observed_this_run"], len(prefix))
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_system_exit_and_opaque_read_fault_preserve_prefix_with_close_fault(self):
        prefix = b"observed-prefix"
        for index, exception in enumerate((SystemExit(9), OSError("secret exception"))):
            self.output = self.root / f"read_fault_{index}"; self.admission["run_dir"] = str(self.output)
            self.calls = []
            class Fault(Response):
                count = 0
                def read(self, size):
                    self.count += 1
                    if self.count == 1: return prefix
                    raise exception
                def close(self): raise OSError("secret close")
            if isinstance(exception, SystemExit):
                with self.assertRaises(KeyboardInterrupt): self.execute([Fault(clob_page())])
            else:
                self.assert_failed(self.execute([Fault(clob_page())]), "response_close_error")
            self.assertEqual(self.body(), prefix)
            finish = self.lines("attempts.jsonl")[-1]
            self.assertEqual(finish["response_bytes_observed"], len(prefix))
            self.assertEqual(finish["response_bytes_charged"], 4 * 1024**2)

    def test_hard_response_cap_never_becomes_successful_truncation(self):
        response = Response(b"x" * (4 * 1024**2 + 1))
        value = self.execute([response])
        self.assert_failed(value, "response_byte_limit")
        self.assertEqual(response.read_bytes, 0)
        self.assertEqual(self.lines("attempts.jsonl")[-1]["response_bytes_charged"], 4 * 1024**2)

    def test_existing_global_attempt_and_response_charges_are_not_recycled(self):
        self.admission["prior_ledger"]["http_attempts"] = campaign.CAPS["http_attempts"] - 1
        self.assert_failed(self.execute([Response(clob_page())]), "http_attempt_limit")
        self.assertEqual(len(self.calls), 1)
        self.output = self.root / "response_budget"; self.admission["run_dir"] = str(self.output)
        self.admission["prior_ledger"]["http_attempts"] = 10
        self.admission["prior_ledger"]["response_bytes_charged"] = campaign.CAPS["total_response_bytes"] - 4 * 1024**2
        self.calls = []
        value = self.execute([OSError("unknown body")])
        self.assert_failed(value, "total_response_byte_limit")
        self.assertEqual(len(self.calls), 1)

    def test_existing_runtime_is_not_reset_by_new_campaign(self):
        self.admission["prior_ledger"]["runtime_seconds"] = campaign.CAPS["runtime_seconds"] - .2
        value = self.execute([Response(clob_page("LTE="))])
        self.assert_failed(value, "runtime_limit")
        self.assertEqual(len(self.calls), 1)

    def test_raw_control_other_and_external_reservations_enforce_existing_8gb(self):
        prior = {"bytes": {"raw": 4_000_000_000, "control": 0, "other": 0},
                 "http_attempts": 0, "response_bytes_charged": 0, "runtime_seconds": 0}
        self.output.mkdir()
        ledger = campaign.Ledger(self.output, prior, 0, 0)
        with patch.object(campaign.time, "monotonic", return_value=0), patch.object(campaign.shutil, "disk_usage", return_value=FREE):
            with self.assertRaisesRegex(campaign.Error, "metadata_aggregate_or_subcap"):
                ledger.reserve("raw", 1)
            ledger.prior["bytes"] = {"raw": 0, "control": 500_000_000 - 2 * 1024**2, "other": 0}
            with self.assertRaisesRegex(campaign.Error, "metadata_aggregate_or_subcap"):
                ledger.reserve("control", 1, failure=True)
        self.assertFalse(self.calls)

    def test_standalone_controls_in_mixed_root_do_not_inventory_growing_v3_files(self):
        mixed = self.root / "mixed"; mixed.mkdir()
        standalone = mixed / "metadata_launch.json"; standalone.write_bytes(b'{"metadata":true}')
        unrelated = mixed / "v3-live-file"; unrelated.write_bytes(b"growing")
        self.admission["prior_ledger"]["standalone_control_files"] = [str(standalone)]
        self.admission["prior_ledger"]["files"].append(self.bound(standalone) | {"category": "control"})
        def transport(spec, timeout):
            unrelated.write_bytes(unrelated.read_bytes() + b"more")
            return self.transport(spec, timeout)
        self.assertEqual(self.execute(transport=transport)["status"], campaign.SUCCESS)

    def test_standalone_control_omission_overlap_symlink_and_wrong_class_fail_before_network(self):
        mixed = self.root / "mixed"; mixed.mkdir()
        item = mixed / "metadata_exit.json"; item.write_bytes(b'{}')
        self.admission["prior_ledger"]["standalone_control_files"] = [str(item)]
        with self.assertRaisesRegex(campaign.Error, "prior_roster_not_complete"):
            self.execute()
        self.admission["prior_ledger"]["files"].append(self.bound(item) | {"category": "raw"})
        with self.assertRaisesRegex(campaign.Error, "standalone_control_misclassified"):
            self.execute()
        self.admission["prior_ledger"]["standalone_control_files"] = [str(self.pilot_root / "START.json")]
        with self.assertRaisesRegex(campaign.Error, "standalone_control_overlap"):
            self.execute()
        item.unlink(); item.symlink_to(self.pilot_root / "START.json")
        self.admission["prior_ledger"]["standalone_control_files"] = [str(item)]
        with self.assertRaises(campaign.Error): self.execute()
        self.assertFalse(self.calls)

    def test_prior_roster_and_numeric_minima_cannot_omit_failed_probe_attempts(self):
        failed = self.root / "failed"; failed.mkdir()
        value = {"status": "failed_unaccepted", "http_attempts": 3,
                 "response_bytes_charged": 1000, "elapsed_seconds": 2}
        path = failed / "FAILURE.json"; path.write_bytes(campaign.encode(value))
        start = failed / "START.json"
        start.write_bytes(campaign.encode({"schema_version": "polymarket_raw_metadata_probe_v2"}))
        self.admission["prior_ledger"]["roots"].append(str(failed))
        self.admission["prior_ledger"]["files"] += [self.bound(file) | {"category": "control"} for file in (path, start)]
        with self.assertRaisesRegex(campaign.Error, "prior_usage_below_durable_minima"):
            self.execute()
        self.assertFalse(self.calls)

    def test_crashed_prior_collection_attempts_without_summary_block(self):
        failed = self.root / "crashed"; failed.mkdir()
        path = failed / "attempts.jsonl"
        path.write_bytes(campaign.encode({"event": "START", "call": 1}) +
                         campaign.encode({"event": "FINISH", "call": 1, "response_bytes_charged": 4 * 1024**2}))
        self.admission["prior_ledger"]["roots"].append(str(failed))
        self.admission["prior_ledger"]["files"].append(self.bound(path) | {"category": "control"})
        with self.assertRaisesRegex(campaign.Error, "prior_collection_summary_missing"):
            self.execute()
        self.assertFalse(self.calls)

    def test_unknown_prior_summary_format_cannot_supply_zero_usage(self):
        failed = self.root / "unknown"; failed.mkdir()
        path = failed / "manifest.json"
        path.write_bytes(campaign.encode({"schema_version": "unreviewed_collector", "http_attempts": 0,
                                         "response_bytes_charged": 0, "elapsed_seconds": 0}))
        self.admission["prior_ledger"]["roots"].append(str(failed))
        self.admission["prior_ledger"]["files"].append(self.bound(path) | {"category": "control"})
        with self.assertRaisesRegex(campaign.Error, "prior_summary_format_unrecognized"):
            self.execute()
        self.assertFalse(self.calls)

    def test_exact_source_lookup_summary_attempt_charges_are_included(self):
        lookup = self.root / "lookup"; (lookup / "controls").mkdir(parents=True)
        path = lookup / "controls/RESULT.json"
        summary = {"schema_version": "polymarket_v3_source_lookup_result_v1", "status": "blocked",
                   "http_attempts": 1, "response_bytes_charged": 8 * 1024**2, "elapsed_seconds": 2,
                   "attempts": [{"id": 1, "charged_response_bytes": 8 * 1024**2}]}
        path.write_bytes(campaign.encode(summary))
        self.admission["prior_ledger"]["roots"].append(str(lookup))
        self.admission["prior_ledger"]["files"].append(self.bound(path) | {"category": "control"})
        self.admission["prior_ledger"]["http_attempts"] += 1
        self.admission["prior_ledger"]["response_bytes_charged"] += 8 * 1024**2
        self.assertEqual(self.execute()["status"], campaign.SUCCESS)
        self.output = self.root / "lookup_understated"; self.admission["run_dir"] = str(self.output)
        self.calls = []
        summary["response_bytes_charged"] = 0
        path.write_bytes(campaign.encode(summary))
        self.admission["prior_ledger"]["files"][-1] = self.bound(path) | {"category": "control"}
        with self.assertRaisesRegex(campaign.Error, "prior_lookup_summary_below_attempts"):
            self.execute()
        self.assertFalse(self.calls)

    def test_body_cannot_be_reclassified_into_reserved_other_metadata_pool(self):
        item = next(x for x in self.admission["prior_ledger"]["files"] if x["category"] == "raw")
        item["category"] = "other"
        with self.assertRaisesRegex(campaign.Error, "prior_raw_body_misclassified"):
            self.execute()
        self.assertFalse(self.calls)

    def test_source_lookup_raw_and_partial_response_files_debit_raw_pool(self):
        for index, name in enumerate(("response_001.raw", ".response_001.pending")):
            root = self.root / f"raw_lookup_{index}"; (root / "responses").mkdir(parents=True)
            path = root / "responses" / name; path.write_bytes(b"retained response bytes")
            self.admission["prior_ledger"]["roots"].append(str(root))
            self.admission["prior_ledger"]["files"].append(self.bound(path) | {"category": "other"})
            with self.assertRaisesRegex(campaign.Error, "prior_raw_body_misclassified"):
                self.execute()
            self.admission["prior_ledger"]["files"].pop()
            self.admission["prior_ledger"]["roots"].pop()
        self.assertFalse(self.calls)

    def test_run_bound_admission_cannot_be_replayed_at_another_path(self):
        self.output = self.root / "different"
        with self.assertRaisesRegex(campaign.Error, "root_admission_not_exact"):
            self.execute()
        self.assertFalse(self.calls)

    def test_no_root_or_approved_probe_admission_means_no_network(self):
        self.admission["approved_probe"]["schema_continuation_accepted"] = False
        with self.assertRaisesRegex(campaign.Error, "approved_probe_schema"):
            self.execute()
        self.assertFalse(self.output.exists())
        self.assertFalse(self.calls)

    def test_same_helper_bytes_accept_unrelated_new_consumer_head_but_not_helper_drift(self):
        self.assertNotEqual(HEAD, PRODUCER)
        self.assertEqual(self.execute()["status"], campaign.SUCCESS)
        self.output = self.root / "helper_drift"; self.admission["run_dir"] = str(self.output)
        self.admission["approved_probe"]["producer_blobs"] = {}
        self.calls = []
        with self.assertRaisesRegex(campaign.Error, "probe_consumer_helper_bytes_differ"):
            self.execute()
        self.assertFalse(self.calls)

    def test_producer_committed_blob_mismatch_fails_before_output_or_network(self):
        original = self.producer_git
        def changed(args, **kwargs):
            result = original(args, **kwargs)
            return b"x" * len(result) if args[2] == "blob" else result
        self.producer_git = changed
        with self.assertRaisesRegex(campaign.Error, "probe_producer_blob_differs"):
            self.execute()
        self.assertFalse(self.output.exists())
        self.assertFalse(self.calls)

    def test_all_prior_roots_and_standalone_files_are_exact_and_disjoint(self):
        for roots, standalone, code in (([str(self.pilot_root)] * 2, [], "overlapping_prior_roots"),
                                        ([str(self.pilot_root)], [str(self.pilot_root / "START.json")],
                                         "standalone_control_overlap_or_invalid")):
            self.admission["prior_ledger"]["roots"] = roots
            self.admission["prior_ledger"]["standalone_control_files"] = standalone
            with self.assertRaisesRegex(campaign.Error, code): self.execute()
        self.assertFalse(self.calls)

    def test_unknown_root_file_cannot_be_omitted_from_inventory(self):
        (self.pilot_root / "unlisted.raw").write_bytes(b"not in ledger")
        with self.assertRaisesRegex(campaign.Error, "prior_roster_not_complete"):
            self.execute()
        self.assertFalse(self.calls)

    def test_same_size_raw_control_prior_and_admission_mutations_block_complete(self):
        for index, target in enumerate(("raw", "control", "prior", "admission")):
            self.output = self.root / f"mutate_{index}"; self.admission["run_dir"] = str(self.output)
            self.calls = []
            prior_path = self.pilot_root / "responses/a01.body.gz"
            old_prior = prior_path.read_bytes()
            def transport(spec, timeout):
                response = self.transport(spec, timeout)
                if len(self.calls) == 2:
                    path = self.output / "responses/a00001.body.gz" if target == "raw" else \
                           self.output / "attempts.jsonl" if target == "control" else \
                           prior_path if target == "prior" else self.admission_path
                    raw = bytearray(path.read_bytes()); raw[-1] ^= 1; path.write_bytes(raw)
                return response
            result = self.execute(transport=transport)
            self.assertEqual(result["status"], "failed_unaccepted")
            self.assertFalse((self.output / "COMPLETE.json").exists())
            if target == "prior": prior_path.write_bytes(old_prior)

    def test_source_changed_during_pending_marker_blocks_final_publication(self):
        def source(_):
            return self.source | {"head": "c" * 40} if (self.output / ".COMPLETE.pending").exists() else self.source
        value = self.execute(source_effect=source)
        self.assert_failed(value, "source_changed")
        self.assertTrue((self.output / ".COMPLETE.pending").exists())

    def test_prior_inclusive_deadline_after_final_external_reopens_blocks_commit(self):
        def advance_only_final(_):
            if (self.output / ".COMPLETE.pending").exists(): self.tick = 86400
            return self.source
        value = self.execute(source_effect=advance_only_final)
        self.assert_failed(value, "runtime_limit")
        self.assertGreater(value["usage"]["runtime_seconds_charged"], campaign.CAPS["runtime_seconds"])
        self.assertTrue((self.output / ".COMPLETE.pending").exists())

    def test_final_disk_floor_gate_is_after_final_reopens(self):
        original = campaign.Ledger.reopen
        low = False
        def reopen(ledger):
            nonlocal low
            value = original(ledger)
            if (ledger.output / ".COMPLETE.pending").exists(): low = True
            return value
        def disk(_):
            return shutil._ntuple_diskusage(10**12, 10**12, 1) if low else FREE
        with patch.object(campaign.Ledger, "reopen", reopen), \
                patch.object(campaign.shutil, "disk_usage", side_effect=disk):
            # execute's own disk fixture is replaced only after the final reopen.
            def source(_):
                if (self.output / ".COMPLETE.pending").exists():
                    campaign.shutil.disk_usage.side_effect = disk
                return self.source
            value = self.execute(source_effect=source)
        self.assert_failed(value, "disk_floor")
        self.assertTrue((self.output / ".COMPLETE.pending").exists())

    def test_outer_timer_debits_prior_usage_and_current_preflight_time(self):
        with patch.object(campaign.resource, "getrlimit", side_effect=[(-1, -1), (8 * 1024**3, -1)]), \
                patch.object(campaign.resource, "setrlimit"), patch.object(campaign.signal, "getsignal", return_value=None), \
                patch.object(campaign.signal, "signal"), patch.object(campaign.signal, "setitimer") as timer, \
                patch.object(campaign.time, "monotonic", return_value=20):
            with campaign.process_limits() as debit_prior:
                debit_prior(100, 0)
        self.assertEqual(timer.call_args_list[0].args, (campaign.signal.ITIMER_REAL, 86400))
        self.assertEqual(timer.call_args_list[1].args, (campaign.signal.ITIMER_REAL, 86280))
        self.assertEqual(timer.call_args_list[2].args, (campaign.signal.ITIMER_REAL, 0))

    def test_marker_fsync_and_directory_fsync_faults_never_publish(self):
        original = campaign.os.fsync
        for index, mode in enumerate(("file", "directory")):
            self.output = self.root / f"marker_{index}"; self.admission["run_dir"] = str(self.output)
            self.calls = []
            def fsync(fd):
                pending = self.output / ".COMPLETE.pending"
                if pending.exists() and ((mode == "file" and os.fstat(fd).st_ino == pending.stat().st_ino)
                                         or (mode == "directory" and stat.S_ISDIR(os.fstat(fd).st_mode))):
                    raise OSError("injected publication fault")
                return original(fd)
            with patch.object(campaign.os, "fsync", side_effect=fsync):
                value = self.execute()
            self.assert_failed(value, "internal_runtime_error")
            self.assertTrue((self.output / ".COMPLETE.pending").exists())

    def test_marker_partial_write_flush_close_and_reopen_faults_never_publish(self):
        original = Path.open
        for mode in ("partial", "flush", "close", "reopen"):
            self.output = self.root / f"marker_{mode}"; self.admission["run_dir"] = str(self.output)
            self.calls = []
            class Marker:
                def __init__(self, stream): self.stream = stream
                def __enter__(self): self.stream.__enter__(); return self
                def __exit__(self, *args):
                    value = self.stream.__exit__(*args)
                    if mode == "close": raise OSError("close fault")
                    return value
                def write(self, raw):
                    return self.stream.write(raw[:len(raw)//2] if mode == "partial" else raw)
                def flush(self):
                    if mode == "flush": raise OSError("flush fault")
                    return self.stream.flush()
                def __getattr__(self, name): return getattr(self.stream, name)
            def open_path(path, *args, **kwargs):
                if path.name == ".COMPLETE.pending" and args == ("rb",) and mode == "reopen":
                    raise OSError("reopen fault")
                stream = original(path, *args, **kwargs)
                return Marker(stream) if path.name == ".COMPLETE.pending" and args == ("xb",) else stream
            with patch.object(Path, "open", open_path): value = self.execute()
            self.assert_failed(value, "artifact_short_write" if mode == "partial" else "internal_runtime_error")
            self.assertTrue((self.output / ".COMPLETE.pending").exists())

    def test_same_size_pending_marker_mutation_during_directory_fsync_fails(self):
        original, changed = campaign.os.fsync, False
        def fsync(fd):
            nonlocal changed
            if not changed and stat.S_ISDIR(os.fstat(fd).st_mode):
                path = self.output / ".COMPLETE.pending"
                raw = bytearray(path.read_bytes()); raw[-1] ^= 1; path.write_bytes(raw)
                changed = True
            return original(fd)
        with patch.object(campaign.os, "fsync", side_effect=fsync): value = self.execute()
        self.assertTrue(changed)
        self.assert_failed(value, "artifact_reopen_differs")

    def test_atomic_publication_refusal_preserves_competing_owner_bytes(self):
        publish = campaign.probe.atomic_completion
        def compete(pending, destination):
            destination.write_bytes(b"competing owner")
            return publish(pending, destination)
        with patch.object(campaign.probe, "atomic_completion", side_effect=compete):
            value = self.execute()
        self.assertEqual(value["status"], "failed_unaccepted")
        self.assertEqual(value["error_code"], "atomic_completion_refused")
        self.assertEqual((self.output / "COMPLETE.json").read_bytes(), b"competing owner")
        self.assertTrue((self.output / ".COMPLETE.pending").exists())

    def test_floor_or_available_ram_failure_occurs_before_output_and_network(self):
        sha = self.save_admission()
        with patch.object(campaign, "RUN_ROOT", self.root), \
                patch.object(campaign, "source_snapshot", return_value=self.source), \
                patch.object(campaign.subprocess, "check_output", side_effect=self.producer_git), \
                patch.object(campaign.probe, "available_ram", return_value=1), \
                patch.object(campaign.shutil, "disk_usage", return_value=FREE):
            with self.assertRaisesRegex(campaign.Error, "available_ram_below_floor"):
                campaign.run_campaign(self.output, HEAD, self.admission_path, sha, transport=self.transport)
        self.assertFalse(self.output.exists()); self.assertFalse(self.calls)

    def test_cli_sanitizes_unexpected_preflight_input_errors(self):
        with patch.object(campaign, "require_production_host"), \
                patch.object(campaign.sys, "executable", "/home/ubuntu/venv/bin/python"), \
                patch.object(campaign, "process_limits", return_value=nullcontext()), \
                patch.object(campaign, "run_campaign", side_effect=OSError("api_key=never disclose")), \
                patch("builtins.print") as printed:
            result = campaign.main(["--admission", "/never", "--admission-sha256", "0" * 64,
                                    "--expected-head", HEAD, "--run-dir", "/mnt/data/runs/never"])
        self.assertEqual(result, 1)
        self.assertEqual(json.loads(printed.call_args.args[0])["error_code"], "admission_or_environment_error")
        self.assertNotIn("never disclose", printed.call_args.args[0])

    def test_no_overwrite_or_resume_and_final_publish_is_last_artifact_io(self):
        original_open, publish = Path.open, campaign.probe.atomic_completion
        published = False
        def atomic(*args):
            nonlocal published
            result = publish(*args); published = True
            return result
        def open_path(path, *args, **kwargs):
            if published: raise AssertionError("I/O after commit")
            return original_open(path, *args, **kwargs)
        with patch.object(campaign.probe, "atomic_completion", side_effect=atomic), patch.object(Path, "open", open_path):
            value = self.execute()
        self.assertTrue(published)
        self.assertEqual(value["status"], campaign.SUCCESS)
        with self.assertRaisesRegex(campaign.Error, "destination_not_fresh"):
            self.execute()

    def test_production_guard_precedes_all_inputs_outputs_and_network(self):
        with patch.object(campaign, "require_production_host", side_effect=RuntimeError("guard")), \
                patch.object(campaign, "run_campaign") as run, patch.object(Path, "read_bytes") as read, \
                patch.object(campaign, "process_limits") as limits:
            with self.assertRaisesRegex(RuntimeError, "guard"):
                campaign.main(["--admission", "/never", "--admission-sha256", "0" * 64,
                               "--expected-head", HEAD, "--run-dir", "/mnt/data/runs/never"])
        run.assert_not_called(); read.assert_not_called(); limits.assert_not_called()

    def test_help_has_no_source_caps_auth_or_resume_override(self):
        result = subprocess.run([sys.executable, campaign.__file__, "--help"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        for flag in ("--endpoint", "--max-", "--resume", "--api-key", "--limit", "--version"):
            self.assertNotIn(flag, result.stdout)

    def test_real_source_snapshot_checks_committed_own_and_helper_bytes(self):
        repo = self.root / "source"; repo.mkdir()
        for name in campaign.SOURCE_FILES:
            path = repo / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(b"fixture\n")
        subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
        subprocess.run(["git", "-C", str(repo), "add", *campaign.SOURCE_FILES], check=True)
        subprocess.run(["git", "-C", str(repo), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "commit", "--quiet", "-m", "Fixture"], check=True)
        head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
        with patch.object(campaign, "REPO", repo):
            self.assertEqual(set(campaign.source_snapshot(head)["files"]), set(campaign.SOURCE_FILES))
            subprocess.run(["git", "-C", str(repo), "update-index", "--assume-unchanged", "production_guard.py"], check=True)
            (repo / "production_guard.py").write_bytes(b"changed\n")
            with self.assertRaisesRegex(campaign.Error, "source_bytes_differ_from_commit"):
                campaign.source_snapshot(head)


if __name__ == "__main__":
    unittest.main()
