"""Synthetic two-prefix continuation, never a provider or production request."""
import copy
from contextlib import ExitStack
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import time
import unittest
from unittest.mock import patch

from scripts import collect_polymarket_market_metadata_campaign_v6 as campaign
from tests import test_polymarket_market_metadata_campaign_v5 as old
from tests.test_polymarket_raw_metadata_probe_v2 import Response, gamma_page, gamma_row

HEAD = "e" * 40


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None): return cls(2026, 10, 28, 12, 20, tzinfo=timezone.utc)


class BudgetContinuationTests(unittest.TestCase):
    def setUp(self):
        parent = old.RecoveryTests(); parent.setUp(); self.addCleanup(parent.doCleanups)
        self.parent = parent; self.root = parent.root; self.ops = parent.ops
        # The real frozen v5 implementation produces the accepted page and the
        # final rejected partial. No historical cap is patched or increased.
        for name in campaign.previous.SOURCE_FILES[len(campaign.v4.SOURCE_FILES):]:
            raw = (campaign.REPO / name).read_bytes(); parent.fixture.blobs[name] = raw
            parent.source["files"][name] = {"bytes": len(raw), "sha256": campaign.digest(raw)}
        stream = campaign.STREAMS[2]
        first = gamma_page(stream, cursor="fixture-v5-next", rows=[gamma_row(stream, i) for i in range(5000, 5100)])
        parent.admission["prior_ledger"]["response_bytes_charged"] = campaign.previous.CAPS["total_response_bytes"] - len(first) - 3
        partial_response = Response(b"rejected-partial-prefix"); del partial_response.headers["Content-Length"]
        result = parent.execute([Response(first), partial_response])
        self.assertEqual(result["error_code"], "response_byte_limit", result)
        self.assertEqual(len(parent.calls), 2)
        self.v5root = parent.output; self.v5failure = json.loads((self.v5root / "FAILURE.json").read_bytes())
        start = json.loads((self.v5root / "START.json").read_bytes())
        self.source = {"head": HEAD, "files": copy.deepcopy(parent.source["files"])}
        for name in campaign.SOURCE_FILES[len(campaign.previous.SOURCE_FILES):]:
            raw = (campaign.REPO / name).read_bytes()
            self.source["files"][name] = {"bytes": len(raw), "sha256": campaign.digest(raw)}
        child = {"hostname": "fixture", "boot_id": "1ad746ce-103e-4e8f-a2f5-ba4df810e899", "pid": 40, "birth_ticks": 50, "process_group": 40, "session": 40}
        caller = child | {"pid": 30, "birth_ticks": 40, "process_group": 30, "session": 30}
        launch = {"schema_version": "polymarket_oct1_stage_launcher_v3", "stage": "metadata", "status": "producer_dispatched_owned_session",
            "source": start["source"], "admission": start["admission"], "command": ["fixture"], "output_dir": str(self.v5root),
            "child": child, "caller": caller, "ownership_unknown": False, "retry_or_resume": False}
        launch_ref = old.v4.write(self.ops / "full_metadata_v5.LAUNCH.json", launch)
        exited = launch | {"status": "producer_exited", "error_code": None, "producer_exit_code": 1,
            "owned_group_drained": True, "children_still_running": False, "stdout_eof_observed": True, "source_and_admission_unchanged": True}
        exit_ref = old.v4.write(self.ops / "full_metadata_v5.EXIT.json", exited)
        prefix = {"root": str(self.v5root), "producer_head": old.HEAD,
            "admission": old.v4.bound(parent.admission_path), "start": old.v4.bound(self.v5root / "START.json"),
            "failure": old.v4.bound(self.v5root / "FAILURE.json"), "launch": launch_ref, "exit": exit_ref,
            "attempts": old.v4.bound(self.v5root / "attempts.jsonl"), "pages": old.v4.bound(self.v5root / "pages.jsonl")}
        attempts = [json.loads(line) for line in (self.v5root / "attempts.jsonl").read_bytes().splitlines()]
        partial = attempts[-1]["body"]
        state = self.v5failure["streams"][2]
        drain = {"schema_version": "polymarket_metadata_v5_root_terminal_drain_v1", "status": "root_verified_terminal_failed_metadata_stage_drained",
            "run_dir": str(self.v5root), "source_head": old.HEAD, "admission": prefix["admission"],
            "bindings": {"producer_start": prefix["start"], "failure": prefix["failure"], "launch": launch_ref, "exit": exit_ref},
            "child": child, "caller": caller, "producer_pid_absent": True, "caller_pid_absent": True,
            "owned_group_and_session_empty": True, "transfers_drained": True, "relevant_consumers_remaining": 0,
            "universe_admitted": False, "final_sample_admitted": False, "metadata_runtime_seconds_floor_for_next_stage": 40200,
            "reconciliation": {"new_accepted_pages": 1, "new_http_attempts": 2, "new_raw_rows": 100,
                "new_response_bytes_charged": result["usage"]["this_run_response_bytes_charged"],
                "failed_page_index": state["accepted_pages"], "next_cursor": state["next_cursor"],
                "final_partial_body": {"path": str(self.v5root / partial["path"]), "bytes": partial["bytes"], "sha256": partial["sha256"]}}}
        prefix["drain"] = old.v4.write(self.ops / "full_metadata_v5_root_terminal_drain_v1.json", drain)
        self.pin = copy.deepcopy(campaign.PIN)
        self.pin.update(root=str(self.v5root), producer_head=old.HEAD, pages=1, calls=2, raw_rows=100,
            streams=tuple((s["accepted_pages"], s["raw_rows"], s["pagination_terminal_observed"]) for s in self.v5failure["streams"]))
        for key in ("admission", "start", "failure", "launch", "exit", "drain"):
            self.pin[key] = (prefix[key]["bytes"], prefix[key]["sha256"])
        approval = {"schema": "polymarket_metadata_v6_human_budget_approval_v1",
            "status": "human_authorized_budget_change_not_production_admission", "human_reply": "yes sure",
            "production_launch_admitted": False, "fresh_preparation_must_be_added_to_prior_runtime": True,
            "fresh_preparation_conservative_start_utc": "2026-10-28T12:20:00Z"}
        approval_ref = old.v4.write(self.ops / "full_metadata_v6_human_budget_approval_v1.json", approval)
        self.approval_pin = (approval_ref["bytes"], approval_ref["sha256"])
        controls = [approval_ref]
        amounts = {key: max(campaign.RETAINED_MINIMUM[key], self.v5failure["usage"]["retained_bytes_charged"][key]) for key in campaign.RETAINED_MINIMUM}
        amounts["control"] += prefix["failure"]["bytes"] + campaign.LATE_V5_CONTROLS + sum(r["bytes"] for r in controls) + 1000
        self.output = self.root / campaign.OUTPUT_NAME
        self.admission_path = self.ops / "full_metadata_v6_admission_v1.json"
        self.admission = {"schema_version": campaign.SCHEMA, "status": campaign.STATUS, "mode": "full", "consumer_head": HEAD,
            "run_dir": str(self.output), "caps": campaign.CAPS, "scope_sha256": campaign.digest(campaign.encode(campaign.SCOPE)),
            "v4_prefix": parent.admission["v4_prefix"], "v5_prefix": prefix, "approval": approval_ref,
            "prior_ledger": {**campaign.PRIOR_MINIMUM, "retained_bytes_charged": amounts, "root_controls": controls},
            "root_checked_prior_producers_drained": True}
        self.tick = 0.; self.calls = []

    def responses(self): return [Response(gamma_page(s, rows=[gamma_row(s, 9000)])) for s in campaign.STREAMS[2:]]

    def execute(self, responses=None, *, source_effect=None, transport=None, real_clock=False):
        ref = old.v4.write(self.admission_path, self.admission); queue = self.responses() if responses is None else responses
        def http(spec, timeout):
            self.calls.append(spec); value = queue.pop(0)
            if isinstance(value, BaseException): raise value
            value.url = spec["url"]; return value
        def sleep(value): self.assertGreaterEqual(value, 0); self.tick += value
        with patch.object(campaign, "RUN_ROOT", self.root), patch.object(campaign, "OPERATIONS", self.ops), \
             patch.object(campaign, "PIN", self.pin), patch.object(campaign, "APPROVAL_PIN", self.approval_pin), \
             patch.object(campaign, "datetime", FixedDatetime), \
             patch.object(campaign.previous, "RUN_ROOT", self.root), patch.object(campaign.previous, "OPERATIONS", self.ops), \
             patch.object(campaign.previous, "PIN", self.parent.pin), patch.object(campaign.v4, "RUN_ROOT", self.root), \
             patch.object(campaign, "source_snapshot", return_value=self.source, side_effect=source_effect), \
             patch.object(campaign.subprocess, "check_output", side_effect=self.parent.fixture.git), \
             patch.object(campaign.probe, "available_ram", return_value=10**12), \
             patch.object(campaign.shutil, "disk_usage", return_value=old.v4.old_fixtures.FREE), ExitStack() as stack:
            if not real_clock: stack.enter_context(patch.object(campaign.time, "monotonic", side_effect=lambda:self.tick))
            return campaign.run_campaign(self.output, HEAD, self.admission_path, ref["sha256"], transport=transport or http,
                sleeper=time.sleep if real_clock else sleep)

    def rebind(self, key):
        new = old.v4.bound(Path(self.admission["v5_prefix"][key]["path"])); self.admission["v5_prefix"][key] = new
        if key not in {"attempts", "pages"}: self.pin[key] = (new["bytes"], new["sha256"])

    def blocked(self, pattern=".*"):
        with self.assertRaisesRegex(Exception, pattern): self.execute()
        self.assertEqual(self.calls, []); self.assertFalse(self.output.exists())

    def test_full_two_prefix_replay_preserves_partial_and_resumes_attempt_two(self):
        original = (self.v5root / "responses/a00002.body.gz").read_bytes()
        result = self.execute(); self.assertEqual(result["status"], campaign.SUCCESS, result)
        starts = [json.loads(line) for line in (self.output / "attempts.jsonl").read_bytes().splitlines() if json.loads(line)["event"] == "START"]
        self.assertEqual([s["exact_page_attempt"] for s in starts], [2, 1, 1])
        self.assertEqual(self.calls[0]["parameters"]["after_cursor"], "fixture-v5-next")
        self.assertEqual([s["stream"] for s in self.calls], [s["id"] for s in campaign.STREAMS[2:]])
        self.assertEqual((self.v5root / "responses/a00002.body.gz").read_bytes(), original)
        manifest = json.loads((self.output / "manifest.json").read_bytes())
        self.assertEqual(manifest["replay"]["accepted_pages"], 6)
        self.assertFalse(manifest["replay"]["failed_partial_accepted"])
        self.assertFalse(manifest["universe_complete"]); self.assertFalse(manifest["analysis_admitted"])
        self.assertTrue((self.output / "COMPLETE.json").is_file())

    def test_budget_change_is_exact_and_historical_caps_unchanged(self):
        self.assertEqual(campaign.CAPS["http_attempts"], 100000)
        self.assertEqual(campaign.CAPS["total_response_bytes"], 103079215104)
        self.assertEqual(campaign.CAPS["aggregate_metadata_bytes"], 20_000_000_000)
        self.assertEqual(campaign.previous.CAPS["http_attempts"], 40000)
        self.assertEqual(campaign.v4.MAX_CONTROL, 16*1024**2)
        self.assertIn("10_000", __import__("inspect").getsource(campaign.v4.inventory_files))

    def test_no_approval_or_changed_human_approval_blocks_before_http(self):
        self.admission["approval"]["sha256"] = "0"*64; self.blocked("human_approval_pin_differs")

    def test_missing_or_conflicting_parent_source_and_drain_are_rejected(self):
        path = Path(self.admission["v5_prefix"]["drain"]["path"]); value = json.loads(path.read_bytes())
        value["owned_group_and_session_empty"] = False; old.v4.write(path, value); self.rebind("drain")
        self.blocked("v5_prefix_drain_differs")

    def test_rebound_wrong_v5_source_head_is_not_a_valid_prefix(self):
        self.admission["v5_prefix"]["producer_head"] = "0"*40; self.blocked("v5_prefix_contract_differs")

    def test_v4_body_mutation_is_rejected(self):
        path = self.parent.old_root / "responses/a00001.body.gz"; path.write_bytes(path.read_bytes()[:-1]+b"x")
        self.blocked("reference_bytes_or_hash_differs")

    def test_v5_body_mutation_is_rejected(self):
        path = self.v5root / "responses/a00001.body.gz"; path.write_bytes(path.read_bytes()[:-1]+b"x")
        self.blocked("reference_bytes_or_hash_differs")

    def test_final_failed_partial_is_mandatory_and_never_adopted(self):
        (self.v5root / "responses/a00002.body.gz").unlink(); self.blocked()

    def test_dropped_v5_page_even_with_new_journal_hash_is_rejected(self):
        (self.v5root / "pages.jsonl").write_bytes(b""); self.rebind("pages"); self.blocked()

    def test_orphan_attempt_or_invalidation_cannot_be_imported(self):
        path = self.v5root / "attempts.jsonl"; path.write_bytes(path.read_bytes()+campaign.encode({"event":"INVALIDATE"}))
        self.rebind("attempts"); self.blocked("v5_counts_or_orphan_attempts")

    def test_cross_prefix_cursor_and_page_hashes_cannot_repeat(self):
        responses = self.responses()
        responses[0] = Response(gamma_page(campaign.STREAMS[2], cursor="next-scope", rows=[gamma_row(campaign.STREAMS[2], i) for i in range(7000,7100)]))
        result=self.execute(responses);self.assertEqual(result["error_code"],"repeated_cursor")
        self.assertEqual(len(self.calls),1);self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_prior_row_hashes_survive_both_prefixes(self):
        responses=self.responses();stream=campaign.STREAMS[2]
        responses[0]=Response(gamma_page(stream,cursor="never-seen",rows=[gamma_row(stream,i) for i in range(5000,5100)]))
        result=self.execute(responses);self.assertEqual(result["error_code"],"repeated_page_rows")
        self.assertEqual(len(self.calls),1)

    def test_page_index_and_attempt_role_are_crossjoined_not_inferred(self):
        path=self.v5root/"attempts.jsonl";records=[json.loads(line) for line in path.read_bytes().splitlines()]
        records[0]["page_index"]+=1
        old.v4.write(path,b"".join(campaign.encode(r) for r in records),raw=True);self.rebind("attempts")
        self.blocked("v5_attempt_pair_differs")

    def test_final_partial_receipt_cannot_refund_the_start_reservation(self):
        path=self.v5root/"attempts.jsonl";records=[json.loads(line) for line in path.read_bytes().splitlines()]
        records[-1]["response_bytes_charged"]=0
        old.v4.write(path,b"".join(campaign.encode(r) for r in records),raw=True);self.rebind("attempts")
        self.blocked("v5_failed_attempt_differs")

    def test_same_size_rebound_start_must_match_historical_writer_reservation(self):
        path=self.v5root/"attempts.jsonl";before=path.read_bytes();records=[json.loads(line) for line in before.splitlines()]
        records[0]["response_byte_reservation"]-=1
        changed=b"".join(campaign.encode(r) for r in records);self.assertEqual(len(before),len(changed))
        path.write_bytes(changed);self.rebind("attempts");self.blocked("v5_historical_reservation_differs")

    def test_unclosed_historical_start_is_not_free_or_adoptable(self):
        path=self.v5root/"attempts.jsonl";lines=path.read_bytes().splitlines(keepends=True)
        path.write_bytes(b"".join(lines[:-1]));self.rebind("attempts");self.blocked("v5_attempt_pair_differs")

    def test_raw_irregular_rows_are_preserved_not_filtered(self):
        responses=self.responses();rows=[None,54,{"id":"","version":"v1","closed":True,"unknown":"literal"}]
        responses[0]=Response(gamma_page(campaign.STREAMS[2],rows=rows))
        result=self.execute(responses);self.assertEqual(result["status"],campaign.SUCCESS,result)
        saved=json.loads(gzip.decompress((self.output/"responses/a00001.body.gz").read_bytes()))
        self.assertEqual(saved["markets"],rows)
        manifest=json.loads((self.output/"manifest.json").read_bytes())
        self.assertEqual(manifest["waterfall_new_run"]["raw_rows_in_accepted_pages"],5)

    def test_wrong_scope_stays_rejected_with_charge_and_raw_body(self):
        stream=campaign.STREAMS[2];row=gamma_row(stream,9999);row["closed"]=False
        result=self.execute([Response(gamma_page(stream,rows=[row]))])
        self.assertEqual(result["error_code"],"gamma_returned_scope_conflict")
        self.assertTrue((self.output/"responses/a00001.body.gz").exists())

    def test_inherited_failed_attempt_leaves_only_two_attempts(self):
        result=self.execute([OSError("private-secret")]*2)
        self.assertEqual(result["status"],"failed_unaccepted");self.assertEqual(len(self.calls),2)
        starts=[json.loads(line) for line in (self.output/"attempts.jsonl").read_bytes().splitlines() if json.loads(line)["event"]=="START"]
        self.assertEqual([s["exact_page_attempt"] for s in starts],[2,3])
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"],2*campaign.CAPS["individual_response_bytes"])
        self.assertNotIn("private-secret",(self.output/"attempts.jsonl").read_text())

    def test_recovered_attempt_three_keeps_identical_cursor_and_resets_next_page(self):
        result=self.execute([OSError("private"),*self.responses()])
        self.assertEqual(result["status"],campaign.SUCCESS,result)
        starts=[json.loads(line) for line in (self.output/"attempts.jsonl").read_bytes().splitlines() if json.loads(line)["event"]=="START"]
        self.assertEqual([s["exact_page_attempt"] for s in starts],[2,3,1,1]);self.assertEqual(self.calls[0],self.calls[1])

    def test_cumulative_counter_floor_cannot_reset(self):
        self.admission["prior_ledger"]["http_attempts"]-=1;self.blocked("prior_usage_below_floor")

    def test_response_floor_cannot_refund_prior_failure(self):
        self.admission["prior_ledger"]["response_bytes_charged"]-=1;self.blocked("prior_usage_below_floor")

    def test_control_floor_keeps_unwritten_failure_late_union_and_new_controls(self):
        self.admission["prior_ledger"]["retained_bytes_charged"]["control"]=campaign.RETAINED_MINIMUM["control"]
        self.blocked("prior_retained_floor_refunded")

    def test_preparation_clock_is_added_not_reset(self):
        class Later(FixedDatetime):
            @classmethod
            def now(cls,tz=None):return cls(2026,10,28,12,21,tzinfo=timezone.utc)
        with patch.object(FixedDatetime,"now",Later.now):self.blocked("preparation_runtime_floor_not_charged")

    def test_http_limit_is_global_after_both_prefixes(self):
        self.admission["prior_ledger"]["http_attempts"]=campaign.CAPS["http_attempts"]
        result=self.execute();self.assertEqual(result["error_code"],"http_attempt_limit");self.assertEqual(self.calls,[])

    def test_wire_limit_has_no_refund_and_small_remaining_prefix_is_saved(self):
        self.admission["prior_ledger"]["response_bytes_charged"]=campaign.CAPS["total_response_bytes"]-3
        responses=self.responses();del responses[0].headers["Content-Length"]
        result=self.execute(responses);self.assertEqual(result["error_code"],"response_byte_limit")
        self.assertEqual(result["usage"]["response_bytes_charged"],campaign.CAPS["total_response_bytes"])
        self.assertEqual(len(gzip.decompress((self.output/"responses/a00001.body.gz").read_bytes())),3)

    def test_raw_subcap_cannot_borrow_mapping_or_aggregate_budget(self):
        self.admission["prior_ledger"]["retained_bytes_charged"]["raw"]=campaign.CAPS["raw_body_bytes"]-1
        result=self.execute();self.assertEqual(result["error_code"],"metadata_aggregate_or_subcap");self.assertEqual(self.calls,[])

    def test_new_source_change_after_replay_blocks_before_network(self):
        calls=0
        def source(*_):
            nonlocal calls
            calls+=1;return self.source if calls==1 else self.source|{"head":"f"*40}
        with self.assertRaisesRegex(Exception,"source_changed_before_dispatch"):self.execute(source_effect=source)
        self.assertEqual(self.calls,[])

    def test_durable_start_signal_boundary_has_full_attempt_charge(self):
        original=campaign.Ledger.append
        def append(ledger,name,value,**kwargs):
            result=original(ledger,name,value,**kwargs)
            if value.get("event")=="START":raise campaign.Error("runtime_limit")
            return result
        with patch.object(campaign.Ledger,"append",append):result=self.execute()
        self.assertEqual(result["error_code"],"runtime_limit");self.assertEqual(self.calls,[])
        self.assertEqual(result["usage"]["this_run_http_attempts"],1)
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"],campaign.CAPS["individual_response_bytes"])

    def test_interrupted_read_retains_prefix_and_full_charge(self):
        class Interrupted(Response):
            def read(self,size):
                if self.read_bytes:return (_ for _ in ()).throw(KeyboardInterrupt())
                self.read_bytes=1;return b"observed-prefix"
        with self.assertRaises(KeyboardInterrupt):self.execute([Interrupted(b"unused",headers={"Content-Length":"100"})])
        self.assertEqual(gzip.decompress((self.output/"responses/a00001.body.gz").read_bytes()),b"observed-prefix")
        result=json.loads((self.output/"FAILURE.json").read_bytes())
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"],campaign.CAPS["individual_response_bytes"])
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_pending_marker_late_input_mutation_blocks_completion(self):
        original=campaign.source_snapshot
        def source(*_):
            if (self.output/".COMPLETE.pending").exists():
                path=self.v5root/"responses/a00002.body.gz";path.write_bytes(path.read_bytes()[:-1]+b"x")
            return self.source
        result=self.execute(source_effect=source)
        self.assertEqual(result["status"],"failed_unaccepted");self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_last_owned_body_verifier_cannot_change_admission_then_publish(self):
        pending_seen=False;mutated=False;original=campaign.probe.verify_body
        def source(*_):
            nonlocal pending_seen
            if (self.output/".COMPLETE.pending").exists():pending_seen=True
            return self.source
        def verify(root,body):
            nonlocal mutated
            if pending_seen and not mutated:
                raw=self.admission_path.read_bytes();self.admission_path.write_bytes(raw[:-1]+b" ");mutated=True
            return original(root,body)
        with patch.object(campaign.probe,"verify_body",verify):result=self.execute(source_effect=source)
        self.assertTrue(mutated);self.assertEqual(result["status"],"failed_unaccepted")
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_late_prior_inclusive_deadline_blocks_final_marker(self):
        def source(*_):
            if (self.output/".COMPLETE.pending").exists():self.tick=86400
            return self.source
        result=self.execute(source_effect=source)
        self.assertEqual(result["error_code"],"runtime_limit");self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_competing_marker_is_never_overwritten(self):
        original=campaign.probe.atomic_completion
        def publish(source,destination):
            destination.write_bytes(b"competitor");return original(source,destination)
        with patch.object(campaign.probe,"atomic_completion",publish):result=self.execute()
        self.assertEqual(result["status"],"failed_unaccepted");self.assertEqual((self.output/"COMPLETE.json").read_bytes(),b"competitor")

    def test_directory_fsync_failure_cannot_leave_complete(self):
        import os,stat
        original=os.fsync
        def fsync(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):raise OSError("private-directory-fsync")
            return original(fd)
        with patch.object(campaign.os,"fsync",fsync):result=self.execute()
        self.assertEqual(result["status"],"failed_unaccepted");self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_inventory_segments_are_deterministic_and_single_controls_bounded(self):
        with patch.object(campaign,"INVENTORY_SEGMENT_BYTES",512):result=self.execute()
        self.assertEqual(result["status"],campaign.SUCCESS,result)
        index=json.loads((self.output/"artifact_inventory.json").read_bytes())
        self.assertGreater(len(index["segments"]),1)
        self.assertTrue(all(ref["bytes"]<=512 for ref in index["segments"]))
        records=[]
        for ref in index["segments"]:records.extend(json.loads(line) for line in (self.output/ref["path"]).read_bytes().splitlines())
        self.assertEqual(len(records),index["record_count"]);self.assertEqual([r["path"] for r in records],sorted(r["path"] for r in records))

    def test_collection_payload_cannot_be_hidden_as_operational_control(self):
        self.admission["prior_ledger"]["root_controls"].append(old.v4.bound(self.v5root/"attempts.jsonl"))
        self.blocked("root_control_invalid_or_duplicate")

    def test_duplicate_operational_controls_cannot_consume_an_unbilled_slot(self):
        self.admission["prior_ledger"]["root_controls"]*=2
        self.blocked("root_control_invalid_or_duplicate")

    def test_unknown_exception_text_never_enters_failure_diagnostics(self):
        result=self.execute([campaign.Error("private_token_query")])
        self.assertEqual(result["error_code"],"contract_validation_error")
        self.assertNotIn("private_token_query",(self.output/"FAILURE.json").read_text())

    def test_actual_cli_guard_is_before_source_or_admission(self):
        with patch.object(campaign,"require_production_host",side_effect=RuntimeError("guard")),patch.object(campaign,"run_campaign") as run:
            with self.assertRaisesRegex(RuntimeError,"guard"):
                campaign.main(["--mode","full","--run-dir","/invalid","--expected-head",HEAD,"--admission","/missing","--admission-sha256","0"*64])
            run.assert_not_called()


if __name__ == "__main__":unittest.main()
