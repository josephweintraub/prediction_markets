"""Synthetic compact metadata pilot/full fixtures; no real API or production inputs."""
import copy
import gzip
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import time
import unittest
from unittest.mock import patch

from scripts import collect_polymarket_market_metadata_campaign_v4 as campaign
from tests import test_polymarket_market_metadata_campaign_v3 as old_fixtures
from tests.test_polymarket_raw_metadata_probe_v2 import Response, clob_page, clob_row, gamma_page, gamma_row

HEAD, FAILED_HEAD = "a"*40, "c"*40


def write(path, value, raw=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    content = value if raw else campaign.encode(value)
    path.write_bytes(content)
    return {"path": str(path), "bytes": len(content), "sha256": campaign.digest(content)}


def bound(path):
    raw = path.read_bytes()
    return {"path": str(path), "bytes": len(raw), "sha256": campaign.digest(raw)}


class CompactMetadataTests(unittest.TestCase):
    def setUp(self):
        # Reuse the already fixture-generated ten-page Gamma schema proof.
        old = old_fixtures.CampaignTests(); old.setUp(); self.addCleanup(old.doCleanups)
        self.old, self.root = old, old.root
        self.blobs = {name: (campaign.REPO/name).read_bytes() for name in campaign.SOURCE_FILES}
        self.source = {"head": HEAD, "files": {name: {"bytes": len(raw), "sha256": campaign.digest(raw)}
                        for name, raw in self.blobs.items()}}
        self.output = self.root/"compact-pilot"
        self.admission_path, self.inventory_path = self.root/"compact-admission.json", self.root/"compact-inventory.json"
        self.roots = [str(old.pilot_root)]
        for index in range(3):
            root = self.root/f"old-{index}"; self.roots.append(str(root))
            if index == 2:
                write(root/"controls/RESULT.json", {"schema_version": "polymarket_v3_source_lookup_result_v1",
                    "status": "bounded_sourcify_raw_lookup_complete_not_source_equivalence", "attempts": [],
                    "http_attempts": 0, "response_bytes_charged": 0, "elapsed_seconds": 0})
            else:
                write(root/"START.json", {"schema_version": "polymarket_market_metadata_pilot_v1"})
                write(root/"FAILURE.json", {"status": "failed_unaccepted", "usage": {"this_run_http_attempts": 0,
                    "this_run_response_bytes_charged": 0, "this_run_runtime_seconds": 0}})
        old_files = self.inventory_files(self.roots, [])
        self.parent_path = self.root/"old-full-admission.json"
        self.parent = {"schema_version": campaign.legacy.SCHEMA, "consumer_head": FAILED_HEAD,
            "caps": campaign.CAPS, "scope_sha256": campaign.digest(campaign.encode(campaign.legacy.SCOPE)),
            "prior_ledger": {"all_prior_metadata_roots_included": True, "roots": list(self.roots), "files": old_files}}
        parent_ref = write(self.parent_path, self.parent)
        self.failed = self.root/"old-full-failed"; self.roots.append(str(self.failed))
        write(self.failed/"START.json", {"schema_version": "polymarket_market_metadata_campaign_v3",
            "source": {"head": FAILED_HEAD}, "caps": campaign.CAPS, "scope": campaign.legacy.SCOPE,
            "admission": parent_ref})
        cursor = "MTAwMA=="
        raw_first, raw_failed = clob_page(cursor), b'{"incomplete":'
        records = []
        charges = 0
        for index, body_raw in enumerate((raw_first, raw_failed), 1):
            relative = f"responses/a{index:05d}.body.gz"
            compressed = campaign.probe.gzip_bytes(body_raw)
            ref = write(self.failed/relative, compressed, raw=True)
            body = {"path": relative, "bytes": ref["bytes"], "sha256": ref["sha256"], "category": "raw",
                "uncompressed_bytes": len(body_raw), "uncompressed_sha256": campaign.digest(body_raw),
                "complete_received_body": index == 1}
            spec = campaign.legacy.request_spec(campaign.legacy.STREAMS[0], "MA==" if index == 1 else cursor)
            start = {"event": "START", "call": index, "stream": "clob", "page_index": index-1,
                "request": spec, "request_sha256": campaign.digest(campaign.encode(spec)),
                "response_byte_reservation": campaign.CAPS["individual_response_bytes"]}
            charge = len(body_raw) if index == 1 else campaign.CAPS["individual_response_bytes"]
            finish = {"event": "FINISH", "call": index, "stream": "clob", "page_index": index-1,
                "request_sha256": start["request_sha256"], "body": body, "response_bytes_observed": len(body_raw),
                "response_bytes_charged": charge, "accepted": index == 1, "body_complete": index == 1,
                "error_code": None if index == 1 else "response_byte_limit"}
            records.extend((start, finish)); charges += charge
        write(self.failed/"attempts.jsonl", b"".join(campaign.encode(row) for row in records), raw=True)
        write(self.failed/"pages.jsonl", b"", raw=True)
        failure_ref = write(self.failed/"FAILURE.json", {"status": "failed_unaccepted", "error_code": "response_byte_limit",
            "streams": [{"id": "clob", "accepted_pages": 1, "raw_rows": 1, "next_cursor": cursor,
                "pagination_terminal_observed": False}], "usage": {"this_run_http_attempts": 2,
                "this_run_response_bytes_charged": charges, "this_run_runtime_seconds": 10},
            "universe_complete": False, "analysis_admitted": False})
        self.exit_path, self.drain_path = self.root/"old-full.EXIT.json", self.root/"old-root-drain.json"
        exit_ref = write(self.exit_path, {"exit_code": 1, "launch": {"pid": 37980, "linux_birth_ticks": 1771282,
            "boot_id": campaign.FAILED_PIN["boot_id"], "source_head": FAILED_HEAD}})
        drain_ref = write(self.drain_path, {"all_owned_processes_drained": True})
        self.pin = dict(campaign.FAILED_PIN, root=str(self.failed), accepted_pages=1, raw_rows=1,
            this_run_http_attempts=2, cursor=cursor, producer_head=FAILED_HEAD)
        for key, ref in (("failure", failure_ref), ("exit", exit_ref), ("drain", drain_ref)):
            self.pin[key+"_bytes"], self.pin[key+"_sha256"] = ref["bytes"], ref["sha256"]
        fp = patch.object(campaign, "FAILED_PIN", self.pin); fp.start(); self.addCleanup(fp.stop)
        self.standalone = [str(self.parent_path), str(self.exit_path), str(self.drain_path)]
        self.admission = {"schema_version": campaign.SCHEMA, "status": "root_compact_metadata_campaign_admitted",
            "root_checked_prior_producers_drained": True,
            "mode": "pilot", "run_dir": str(self.output), "consumer_head": HEAD,
            "caps": campaign.CAPS, "pilot_caps": campaign.PILOT, "scope_sha256": campaign.digest(campaign.encode(campaign.SCOPE)),
            "prior_ledger": {"all_prior_metadata_roots_included": True, "roots": self.roots,
                "standalone_control_files": self.standalone, **campaign.PRIOR_MINIMUM,
                "retained_bytes_charged": dict(campaign.RETAINED_MINIMUM)},
            "failed_campaign": {"root": str(self.failed), "failure": failure_ref, "exit": exit_ref, "drain": drain_ref},
            "legacy_gamma_probe": old.admission["approved_probe"], "pilot_acceptance": None}
        self.calls, self.tick, self.responses = [], 0.0, []

    def inventory_files(self, roots, standalone):
        paths = [path for name in roots for path in Path(name).rglob("*") if path.is_file()]
        paths += [Path(name) for name in standalone]
        return [bound(path) | {"category": "raw" if "responses" in path.parts else "other" if path.suffix == ".part" else "control"}
                for path in sorted(paths)]

    def save(self):
        files = self.inventory_files(self.admission["prior_ledger"]["roots"], self.admission["prior_ledger"]["standalone_control_files"])
        self.admission["prior_ledger"]["inventory"] = write(self.inventory_path,
            {"schema_version": "polymarket_metadata_prior_inventory_v4", "files": files})
        for key in campaign.RETAINED_MINIMUM:
            observed = sum(item["bytes"] for item in files if item["category"] == key)
            self.admission["prior_ledger"]["retained_bytes_charged"][key] = max(
                self.admission["prior_ledger"]["retained_bytes_charged"][key], observed)
        return write(self.admission_path, self.admission)["sha256"]

    def git(self, args, **kwargs):
        self.assertEqual(args[:2], ["git", "cat-file"])
        _, name = args[3].split(":", 1)
        raw = self.blobs[name]
        return str(len(raw)).encode() if args[2] == "-s" else raw

    def http(self, spec, timeout):
        self.calls.append((spec, timeout, self.tick))
        value = self.responses.pop(0)
        if isinstance(value, BaseException):
            raise value
        value.url = value.url or spec["url"]
        return value

    def sleep(self, value):
        self.tick += value

    def pilot_responses(self):
        return [Response(clob_page("MTAwMA==")), Response(clob_page("MjAwMA==", [clob_row(2)])),
                Response(clob_page("MzAwMA==", [clob_row(3)]))]

    def execute(self, responses=None, *, source_effect=None, transport=None, real_clock=False):
        sha = self.save()
        self.responses = self.pilot_responses() if responses is None else responses
        with patch.object(campaign, "RUN_ROOT", self.root), \
                patch.object(campaign, "source_snapshot", side_effect=source_effect, return_value=self.source), \
                patch.object(campaign.subprocess, "check_output", side_effect=self.git), \
                patch.object(campaign.probe, "available_ram", return_value=10**12), \
                patch.object(campaign.shutil, "disk_usage", return_value=old_fixtures.FREE):
            if real_clock:
                return campaign.run_campaign(self.output, HEAD, self.admission_path, sha, self.admission["mode"],
                                             transport=transport or self.http, sleeper=lambda _:None)
            with patch.object(campaign.time, "monotonic", side_effect=lambda:self.tick):
                return campaign.run_campaign(self.output, HEAD, self.admission_path, sha, self.admission["mode"],
                                             transport=transport or self.http, sleeper=self.sleep)

    def assert_failure(self, result, code=None):
        self.assertEqual(result["status"], "failed_unaccepted", result)
        self.assertFalse((self.output/"COMPLETE.json").exists())
        if code:
            self.assertEqual(result["error_code"], code)

    def prepare_full(self):
        result = self.execute(); self.assertEqual(result["status"], campaign.PILOT_SUCCESS, result)
        pilot_root, pilot_admission, pilot_inventory = self.output, self.admission_path, self.inventory_path
        manifest = json.loads((pilot_root/"manifest.json").read_bytes())
        self.output = self.root/"compact-full"
        self.admission_path, self.inventory_path = self.root/"full-admission.json", self.root/"full-inventory.json"
        self.roots.append(str(pilot_root)); self.standalone.extend((str(pilot_admission), str(pilot_inventory)))
        self.admission.update(mode="full", run_dir=str(self.output), pilot_acceptance={"accepted": True,
            "producer_head": HEAD, "producer_blobs": self.source["files"], "complete": bound(pilot_root/"COMPLETE.json"),
            "manifest": bound(pilot_root/"manifest.json")})
        for key in campaign.PRIOR_MINIMUM:
            usage_key = {"http_attempts":"http_attempts_charged", "response_bytes_charged":"response_bytes_charged",
                         "runtime_seconds":"runtime_seconds_charged"}[key]
            self.admission["prior_ledger"][key] = manifest["usage"][usage_key]
        for key in campaign.RETAINED_MINIMUM:
            self.admission["prior_ledger"]["retained_bytes_charged"][key] = manifest["usage"]["retained_bytes_charged"][key]
        self.admission["prior_ledger"]["retained_bytes_charged"]["control"] += 1_000_000
        self.calls = []; self.tick = 0
        return pilot_root

    def full_responses(self):
        responses = [Response(clob_page("MTAwMA==")), Response(clob_page("LTE=", [clob_row(2)]))]
        for stream in campaign.STREAMS[1:]:
            responses.append(Response(json.dumps({"$schema": campaign.GAMMA_SCHEMA_URI,
                "markets": [gamma_row(stream, index+1) for index in range(100)], "next_cursor":"next-scope"}).encode()))
            responses.append(Response(gamma_page(stream, rows=[gamma_row(stream, 101)])))
        return responses

    def test_three_observation_pilot_is_not_a_spliced_or_terminal_universe(self):
        result = self.execute(); self.assertEqual(result["status"], campaign.PILOT_SUCCESS, result)
        manifest = json.loads((self.output/"manifest.json").read_bytes())
        self.assertEqual(len(self.calls), 3)
        self.assertEqual([row["coverage_role"] for row in manifest["observations"]],
                         ["fresh_prefix", "fresh_prefix", "old_cursor_diagnostic"])
        self.assertEqual([row["requested_cursor"] for row in manifest["observations"]], ["MA==", "MTAwMA==", self.pin["cursor"]])
        self.assertEqual(result["streams"][0]["accepted_pages"], 2)
        self.assertFalse(manifest["universe_complete"]); self.assertFalse(manifest["analysis_admitted"])
        self.assertFalse(manifest["old_prefix_is_new_stream_coverage"])
        self.assertTrue(all("simplified-markets?next_cursor=" in call[0]["url"] for call in self.calls))
        self.assertTrue(all(set(call[0]["parameters"]) == {"next_cursor"} for call in self.calls))

    def test_raw_scalar_blank_tokens_and_unknown_fields_are_retained(self):
        row = clob_row(); row.update(condition_id="", future={"x":[False,None]})
        row["tokens"][0]["token_id"] = ""
        raw = clob_page("MTAwMA==", [row, None, 42])
        responses = self.pilot_responses(); responses[0] = Response(raw)
        result = self.execute(responses); self.assertEqual(result["status"], campaign.PILOT_SUCCESS, result)
        self.assertEqual(gzip.decompress((self.output/"responses/a00001.body.gz").read_bytes()), raw)
        manifest = json.loads((self.output/"manifest.json").read_bytes())
        self.assertEqual(manifest["observations"][0]["count"], 3)

    def test_frozen_prior_call_response_runtime_and_retained_floors_cannot_refund(self):
        for key, value in campaign.PRIOR_MINIMUM.items():
            original = self.admission["prior_ledger"][key]; self.admission["prior_ledger"][key] = value-1
            with self.assertRaises(campaign.Error):self.execute()
            self.admission["prior_ledger"][key] = original
        self.assertEqual(self.calls, [])
        self.admission["prior_ledger"]["retained_bytes_charged"]["control"] = campaign.RETAINED_MINIMUM["control"]-1
        with self.assertRaises(campaign.Error):self.execute()

    def test_no_overwrite_or_admission_mode_and_endpoint_override(self):
        self.admission["mode"] = "full"
        with self.assertRaises(campaign.Error):self.execute()
        self.assertEqual(self.calls, [])
        self.admission["mode"] = "pilot"; self.execute()
        with self.assertRaises(campaign.Error):self.execute()

    def test_response_limit_retains_prefix_and_failure_never_publishes_complete(self):
        response = Response(b"x"*campaign.CAPS["individual_response_bytes"], headers={"Content-Type":"application/json"})
        response.headers.pop("Content-Length")
        result = self.execute([response]); self.assert_failure(result, "response_byte_limit")
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"], campaign.CAPS["individual_response_bytes"])
        self.assertEqual(gzip.decompress((self.output/"responses/a00001.body.gz").read_bytes()), b"x"*campaign.CAPS["individual_response_bytes"])

    def test_full_walk_requires_pilot_and_starts_fresh_before_all_gamma_terminals(self):
        self.prepare_full()
        result = self.execute(self.full_responses())
        self.assertEqual(result["status"], campaign.FULL_SUCCESS, result)
        self.assertEqual(len(self.calls), 10)
        self.assertEqual(self.calls[0][0]["parameters"], {"next_cursor":"MA=="})
        self.assertEqual(len(result["streams"]), 5)
        self.assertTrue(all(row["pagination_terminal_observed"] for row in result["streams"]))
        self.assertTrue(all(call[0]["parameters"]["limit"] == "100" for call in self.calls[2:]))
        self.assertFalse(result["universe_complete"])
        manifest = json.loads((self.output/"manifest.json").read_bytes())
        self.assertEqual(manifest["observations"], [])
        self.assertEqual(manifest["waterfall"]["raw_rows_in_accepted_pages"], 406)

    def test_full_cannot_skip_or_fake_pilot_acceptance(self):
        self.admission.update(mode="full", run_dir=str(self.output))
        with self.assertRaises(campaign.Error):self.execute()
        self.assertEqual(self.calls, [])

    def test_first_pilot_page_terminal_is_not_replaced_by_diagnostic_coverage(self):
        result = self.execute([Response(clob_page("LTE="))])
        self.assert_failure(result, "pilot_first_page_terminal")
        self.assertEqual(len(self.calls), 1)

    def test_gamma_explicit_scope_conflicts_block_but_missing_fields_are_retained(self):
        self.prepare_full(); responses = self.full_responses()
        raw = json.loads(responses[2].raw); raw["markets"][0]["closed"] = not campaign.STREAMS[1]["closed"]
        responses[2] = Response(json.dumps(raw).encode())
        result = self.execute(responses); self.assert_failure(result, "gamma_returned_scope_conflict")
        self.assertEqual(len(self.calls), 3)

    def test_repeated_cursor_and_malformed_json_are_not_retryable(self):
        result = self.execute([Response(clob_page("MA=="))]); self.assert_failure(result, "repeated_cursor")
        self.assertEqual(len(self.calls), 1)

    def test_pilot_request_ceiling_counts_transient_failures(self):
        responses = [OSError("private-exception-text"), OSError("private-exception-text"), *self.pilot_responses()]
        result = self.execute(responses); self.assertEqual(result["status"], campaign.PILOT_SUCCESS, result)
        self.assertEqual(len(self.calls), 5)
        journal = (self.output/"attempts.jsonl").read_text()
        self.assertNotIn("private-exception-text", journal)
        self.assertEqual(result["usage"]["this_run_http_attempts"], 5)
        self.assertGreaterEqual(result["usage"]["this_run_response_bytes_charged"], 2*campaign.CAPS["individual_response_bytes"])

    def test_exact_page_retry_ceiling_never_restarts_the_campaign_budget(self):
        result = self.execute([OSError("hidden") for _ in range(3)])
        self.assert_failure(result, "request_transport_error")
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"], 3*campaign.CAPS["individual_response_bytes"])

    def test_close_fault_and_incomplete_read_keep_prefix_and_full_charge(self):
        class Partial(Response):
            def read(self, size):
                raise http.client.IncompleteRead(b"observed-prefix")
        result = self.execute([Partial(b"unused")]); self.assert_failure(result, "incomplete_response")
        self.assertEqual(gzip.decompress((self.output/"responses/a00001.body.gz").read_bytes()), b"observed-prefix")
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"], campaign.CAPS["individual_response_bytes"])

    def test_external_prior_inventory_exceeds_old_inline_limit_without_new_allowance(self):
        original = self.save
        def padded():
            original()
            self.inventory_path.write_bytes(b" "*(campaign.MAX_INPUT+1)+self.inventory_path.read_bytes())
            self.admission["prior_ledger"]["inventory"] = bound(self.inventory_path)
            return write(self.admission_path, self.admission)["sha256"]
        with patch.object(self, "save", side_effect=padded):result = self.execute()
        self.assertEqual(result["status"], campaign.PILOT_SUCCESS, result)
        self.assertGreater(result["usage"]["external_prior_inventory_control_bytes"], campaign.MAX_INPUT)

    def test_prior_tree_extra_files_and_standalone_collection_receipts_block(self):
        def added(*_):
            write(Path(self.roots[1])/"unlisted.json", {"new":True})
            return self.source
        with self.assertRaises(campaign.Error):self.execute(source_effect=added)
        self.assertEqual(self.calls, [])

    def test_legacy_parent_files_and_failed_drain_pins_cannot_be_omitted(self):
        self.admission["failed_campaign"]["drain"]["sha256"] = "0"*64
        with self.assertRaises(campaign.Error):self.execute()
        self.assertEqual(self.calls, [])

    def test_owned_body_mutation_after_pending_marker_blocks_publication(self):
        def changed(*_):
            if (self.output/".COMPLETE.pending").exists():
                path = self.output/"responses/a00001.body.gz"
                raw = path.read_bytes(); path.write_bytes(bytes([raw[0]^1])+raw[1:])
            return self.source
        result = self.execute(source_effect=changed); self.assert_failure(result)

    def test_same_size_admission_mutation_after_manifest_blocks_publication(self):
        done = [False]
        def changed(*_):
            if (self.output/"manifest.json").exists() and not done[0]:
                raw = self.admission_path.read_bytes(); self.admission_path.write_bytes(raw.replace(HEAD.encode(), b"f"*40, 1)); done[0] = True
            return self.source
        result = self.execute(source_effect=changed); self.assert_failure(result)

    def test_source_mutation_and_late_cumulative_deadline_fail_closed(self):
        def late(*_):
            if (self.output/".COMPLETE.pending").exists():self.tick = campaign.CAPS["runtime_seconds"]
            return self.source
        result = self.execute(source_effect=late); self.assert_failure(result, "runtime_limit")

    def test_keyboard_interrupt_retains_observed_prefix_and_full_charge(self):
        class Interrupted(Response):
            def read(self, size):
                if self.read_bytes:
                    raise KeyboardInterrupt()
                self.read_bytes = len(b"observed-prefix")
                return b"observed-prefix"
        with self.assertRaises(KeyboardInterrupt):self.execute([Interrupted(b"unused", headers={"Content-Length":"100"})])
        self.assertFalse((self.output/"COMPLETE.json").exists())
        self.assertEqual(gzip.decompress((self.output/"responses/a00001.body.gz").read_bytes()), b"observed-prefix")
        failure = json.loads((self.output/"FAILURE.json").read_bytes())
        self.assertEqual(failure["usage"]["this_run_response_bytes_charged"], campaign.CAPS["individual_response_bytes"])

    def test_absolute_attempt_deadline_keeps_returned_prefix_and_skips_expired_close(self):
        case = self
        class Late(Response):
            def read(self, size):
                case.tick = 31
                return b"observed-prefix"
            def close(self):
                raise AssertionError("expired close must not start")
        result = self.execute([Late(b"unused", headers={"Content-Length":"100"})])
        self.assert_failure(result, "http_attempt_timeout")
        self.assertEqual(gzip.decompress((self.output/"responses/a00001.body.gz").read_bytes()), b"observed-prefix")
        lines = [json.loads(line) for line in (self.output/"attempts.jsonl").read_bytes().splitlines()]
        self.assertFalse(lines[-1]["response_close_attempted"])

    def test_hard_alarm_interrupts_real_blocking_open_read_close_and_chained_cleanup(self):
        for mode in ("open", "read", "close", "chained"):
            with self.subTest(mode=mode):
                self.output = self.root/("hard-"+mode); self.admission["run_dir"] = str(self.output)
                self.calls = []; self.tick = 0
                class Slow(Response):
                    def read(self, size):
                        if mode in {"read", "chained"}:
                            if mode == "chained" and self.read_bytes == 0:
                                self.read_bytes = 3
                                return b"abc"
                            time.sleep(1)
                        return super().read(size)
                    def close(self):
                        if mode in {"close", "chained"}:time.sleep(1)
                        return super().close()
                def slow_http(spec, timeout):
                    self.calls.append((spec, timeout, 0))
                    if mode == "open":time.sleep(1)
                    return Slow(clob_page(), url=spec["url"])
                started = time.monotonic()
                with patch.object(campaign, "HTTP_SECONDS", .04):
                    result = self.execute(transport=slow_http, real_clock=True)
                self.assertLess(time.monotonic()-started, .7)
                self.assert_failure(result, "http_attempt_timeout")
                self.assertEqual(result["usage"]["this_run_response_bytes_charged"], campaign.CAPS["individual_response_bytes"])

    def test_parse_and_normal_receipt_stage_share_original_attempt_deadline(self):
        original = campaign.probe.inspect_rows
        def late(rows, stream, page):
            result = original(rows, stream, page)
            if stream["id"] == "clob_simplified":self.tick = 31
            return result
        with patch.object(campaign.probe, "inspect_rows", side_effect=late):result = self.execute()
        self.assert_failure(result, "http_attempt_timeout")
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"], campaign.CAPS["individual_response_bytes"])

    def test_marker_fsync_failure_never_publishes_complete(self):
        original = campaign.os.fsync
        def fail(fd):
            if (self.output/".COMPLETE.pending").exists():raise OSError("private fsync exception")
            return original(fd)
        with patch.object(campaign.os, "fsync", side_effect=fail):result = self.execute()
        self.assert_failure(result)

    def add_compiler(self, unclosed=False):
        root = self.root/"compiler-v2"
        files = self.inventory_files(self.roots, self.standalone)
        parent = {"schema_version":"polymarket_solc_acquisition_admission_v2", "status":"root_terminal_metadata_solc_admitted",
            "consumer_head":"d"*40, "run_dir":str(root), "prior_ledger":{"files":files,
                "retained_bytes_charged":dict(campaign.RETAINED_MINIMUM)}}
        parent_ref = write(self.root/"compiler-admission.json", parent)
        write(root/"attempt01.START.json", {"attempt":1,"url":"https://binaries.soliditylang.org/linux-amd64/solc-linux-amd64-v0.8.34+commit.80d5c536",
            "allowance":32*1024**2+1})
        part = write(root/"attempt01.part", b"compiler fixture never executed", raw=True)
        attempt = {"response_bytes_charged":32*1024**2+1 if unclosed else 123,
            "response_bytes_observed":part["bytes"],"body":{**part,"path":"attempt01.part"},"error_code":None,
            "response_close_completed":True}
        if not unclosed:write(root/"attempt01.FINISH.json", attempt)
        prior_usage = dict(campaign.PRIOR_MINIMUM)
        own = {"http_attempts":1,"response_bytes_charged":attempt["response_bytes_charged"],"runtime_seconds":300}
        summary = {"schema_version":"polymarket_solc_binary_acquisition_v2", "status":"blocked" if unclosed else "authenticated_solc_binary_not_executed",
            "evidence_class":"public_toolchain_binary", "compiler_executed":False,"sample_admitted":False,
            "source":{"head":"d"*40,"files":{name:self.source["files"][name]["sha256"] for name in campaign.COMPILER_FILES}},
            "admission":parent_ref,"caps":{"http_attempts":2,"total_response_bytes_charged":64*1024**2+2,"runtime_seconds":300},
            "attempts":[] if unclosed else [attempt],"http_attempts":1,"response_bytes_charged":own["response_bytes_charged"],
            "runtime_seconds_charged":300,"prior_metadata_usage":prior_usage,
            "cumulative_metadata_usage":{key:prior_usage[key]+own[key] for key in own},
            "prior_metadata_bytes":dict(campaign.RETAINED_MINIMUM)}
        write(root/"START.json", summary)
        write(root/("FAILED.json" if unclosed else "RESULT.json"), summary)
        self.roots.append(str(root)); self.standalone.append(parent_ref["path"])
        self.admission["prior_ledger"].update(summary["cumulative_metadata_usage"])
        self.admission["prior_ledger"]["retained_bytes_charged"]["control"] += 4*1024**2
        self.admission["prior_ledger"]["retained_bytes_charged"]["other"] = part["bytes"]
        return root

    def test_compiler_own_usage_and_retained_reservation_are_carried_once(self):
        self.add_compiler()
        result = self.execute(); self.assertEqual(result["status"], campaign.PILOT_SUCCESS, result)
        self.assertEqual(result["usage"]["http_attempts_charged"], campaign.PRIOR_MINIMUM["http_attempts"]+1+3)
        self.assertEqual(result["usage"]["retained_bytes_charged"]["other"], len(b"compiler fixture never executed"))

    def test_compiler_unclosed_start_requires_full_allowance_charge(self):
        self.add_compiler(unclosed=True)
        result = self.execute(); self.assertEqual(result["status"], campaign.PILOT_SUCCESS, result)

    def test_compiler_understated_usage_or_reserved_bytes_blocks_before_http(self):
        self.add_compiler()
        self.admission["prior_ledger"]["runtime_seconds"] = campaign.PRIOR_MINIMUM["runtime_seconds"]
        with self.assertRaises(campaign.Error):self.execute()
        self.assertEqual(self.calls, [])

    def test_compiler_uses_literal_sha_string_source_map_not_metadata_nested_shape(self):
        root = self.add_compiler()
        for name in ("START.json", "RESULT.json"):
            path = root/name; value = json.loads(path.read_bytes())
            value["source"]["files"] = {name:self.source["files"][name] for name in campaign.COMPILER_FILES}
            write(path, value)
        with self.assertRaisesRegex(campaign.Error, "compiler_producer_binding_differs"):self.execute()
        self.assertEqual(self.calls, [])

    def test_root_explicit_producer_drain_attestation_required(self):
        self.admission["root_checked_prior_producers_drained"] = False
        with self.assertRaises(campaign.Error):self.execute()
        self.assertEqual(self.calls, [])

    def test_normal_body_durability_is_inside_hard_attempt_timer(self):
        original = campaign.Ledger.write
        def slow(ledger, relative, raw, category="control", **kwargs):
            if relative.startswith("responses/") and not kwargs.get("failure", False):time.sleep(1)
            return original(ledger, relative, raw, category, **kwargs)
        started = time.monotonic()
        with patch.object(campaign, "HTTP_SECONDS", .04), patch.object(campaign.Ledger, "write", slow):
            result = self.execute(real_clock=True)
        self.assertLess(time.monotonic()-started, .7)
        self.assert_failure(result, "http_attempt_timeout")

    def test_default_transport_sends_the_exact_recorded_headers_without_proxy_or_redirect(self):
        seen = []
        class Opener:
            def open(self, outgoing, timeout):
                seen.append((outgoing, timeout)); return object()
        spec = campaign.request_spec(campaign.STREAMS[0], "MA==")
        with patch.object(campaign.request, "build_opener", return_value=Opener()) as build:
            campaign.public_transport(spec, 30)
        outgoing, timeout = seen[0]
        self.assertEqual(outgoing.full_url, spec["url"])
        self.assertEqual({k.lower():v for k,v in outgoing.header_items()}, {k.lower():v for k,v in spec["headers"].items()})
        self.assertEqual(timeout, 30)
        self.assertEqual(build.call_args.args[0].proxies, {})
        self.assertIsInstance(build.call_args.args[1], campaign.probe.NoRedirect)

    def test_invalid_retry_after_still_seals_failed_attempt_receipt(self):
        response = Response(b"{}", status=429, headers={"Retry-After":"private-invalid-retry-header"})
        result = self.execute([response]); self.assert_failure(result)
        lines = [json.loads(line) for line in (self.output/"attempts.jsonl").read_bytes().splitlines()]
        self.assertEqual([row["event"] for row in lines], ["START", "FINISH"])
        self.assertFalse(lines[-1]["accepted"])
        self.assertNotIn("private-invalid-retry-header", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
