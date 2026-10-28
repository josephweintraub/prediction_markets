"""Synthetic recovery only. Frozen v4 produces the tiny failed-prefix fixture."""
import copy
from contextlib import ExitStack
import gzip
import json
from pathlib import Path
import signal
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from scripts import collect_polymarket_market_metadata_campaign_v5 as campaign
from tests import test_polymarket_market_metadata_campaign_v4 as v4
from tests.test_polymarket_raw_metadata_probe_v2 import Response, gamma_page, gamma_row

HEAD = "d" * 40


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        fixture = v4.CompactMetadataTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        fixture.prepare_full(); self.fixture = fixture; self.root = fixture.root
        self.ops = self.root / "operations"; self.ops.mkdir()
        fixture.admission_path = self.ops / "full_metadata_v4_admission_v3.json"
        fixture.inventory_path = self.ops / "full_metadata_v4_prior_inventory_v3.json"
        def stop_after_five(*_):
            if len(fixture.calls) == 5: raise ValueError("private-failure-text")
            return fixture.source
        result = fixture.execute(fixture.full_responses()[:5], source_effect=stop_after_five)
        self.assertEqual(result["error_code"], "internal_runtime_error")
        self.old_root = fixture.output; self.old_admission = fixture.admission_path
        self.source = {"head": HEAD, "files": copy.deepcopy(fixture.source["files"])}
        self.source["files"].update({name: {"bytes": 4, "sha256": campaign.digest(b"test")} for name in campaign.SOURCE_FILES[len(campaign.previous.SOURCE_FILES):]})
        start = json.loads((self.old_root / "START.json").read_bytes())
        self.full_source = start["source"]
        child = {"hostname": "fixture", "boot_id": "1ad746ce-103e-4e8f-a2f5-ba4df810e899", "pid": 20, "birth_ticks": 30, "process_group": 20, "session": 20}
        caller = child | {"pid": 10, "birth_ticks": 15, "process_group": 10, "session": 10}
        launch = {"schema_version": "polymarket_oct1_stage_launcher_v1", "stage": "metadata", "status": "producer_dispatched_owned_session",
            "source": self.full_source, "admission": start["admission"], "command": ["fixture"], "output_dir": str(self.old_root),
            "child": child, "caller": caller, "ownership_unknown": False, "retry_or_resume": False}
        launch_ref = v4.write(self.ops / "full_metadata_v4.LAUNCH.json", launch)
        exited = launch | {"status": "producer_exited", "error_code": None, "producer_exit_code": 1,
            "owned_group_drained": True, "children_still_running": False, "stdout_eof_observed": True, "source_and_admission_unchanged": True}
        exit_ref = v4.write(self.ops / "full_metadata_v4.EXIT.json", exited)
        prefix = {"root": str(self.old_root), "producer_head": v4.HEAD,
            "admission": v4.bound(self.old_admission), "start": v4.bound(self.old_root / "START.json"),
            "failure": v4.bound(self.old_root / "FAILURE.json"), "launch": launch_ref, "exit": exit_ref,
            "attempts": v4.bound(self.old_root / "attempts.jsonl"), "pages": v4.bound(self.old_root / "pages.jsonl")}
        drain = {"schema_version": "polymarket_metadata_full_v4_stage_drain_v1", "status": "root_verified_bound_metadata_stage_drained",
            "run_dir": str(self.old_root), "source_head": v4.HEAD, "launch": launch_ref, "producer_start": prefix["start"],
            "caller_exit": exit_ref, "terminal_summary": prefix["failure"], "complete": None, "child": child, "caller": caller,
            "producer_pid_absent": True, "caller_pid_absent": True, "owned_group_and_session_empty": True,
            "transfers_drained": True, "host_idle_claimed": False, "universe_admitted": False}
        prefix["drain"] = v4.write(self.ops / "full_metadata_v4_root_drain_v1.json", drain)
        failure = json.loads((self.old_root / "FAILURE.json").read_bytes())
        self.pin = {key: copy.deepcopy(value) for key, value in campaign.PIN.items()}
        self.pin.update(root=str(self.old_root), producer_head=v4.HEAD, pages=5, raw_rows=sum(row["raw_rows"] for row in failure["streams"]),
            streams=tuple((row["accepted_pages"], row["raw_rows"], row["pagination_terminal_observed"]) for row in failure["streams"]))
        for name in ("admission", "start", "failure", "launch", "exit", "drain", "attempts"):
            self.pin[name] = (prefix[name]["bytes"], prefix[name]["sha256"])
        self.pin["pages_journal"] = (prefix["pages"]["bytes"], prefix["pages"]["sha256"])
        controls = [prefix[key] for key in ("launch", "exit", "drain")]
        retained = {key: max(campaign.RETAINED_MINIMUM[key], value) for key, value in failure["usage"]["retained_bytes_charged"].items()}
        retained["control"] += prefix["failure"]["bytes"] + sum(ref["bytes"] for ref in controls) + 10000
        self.output = self.root / "recovery"; self.admission_path = self.ops / "full_metadata_v5_admission_v1.json"
        self.admission = {"schema_version": campaign.SCHEMA, "status": campaign.STATUS, "mode": "full", "consumer_head": HEAD,
            "run_dir": str(self.output), "caps": campaign.CAPS, "scope_sha256": campaign.digest(campaign.encode(campaign.SCOPE)),
            "v4_prefix": prefix, "prior_ledger": {**campaign.PRIOR_MINIMUM, "retained_bytes_charged": retained, "root_controls": controls},
            "root_checked_prior_producers_drained": True}
        self.calls = []; self.tick = 0.0

    def responses(self):
        return [Response(gamma_page(stream, rows=[gamma_row(stream, 1001)])) for stream in campaign.STREAMS[2:]]

    def execute(self, responses=None, *, source_effect=None, transport=None, sleeper=None, real_clock=False):
        ref = v4.write(self.admission_path, self.admission); self.queue = self.responses() if responses is None else responses
        def http(spec, timeout):
            self.calls.append(spec); value = self.queue.pop(0)
            if isinstance(value, BaseException): raise value
            value.url = spec["url"]; return value
        def sleep(value):
            self.assertGreaterEqual(value, 0); self.tick += value
        with patch.object(campaign, "RUN_ROOT", self.root), patch.object(campaign, "OPERATIONS", self.ops), \
             patch.object(campaign, "PIN", self.pin), patch.object(campaign.previous, "RUN_ROOT", self.root), \
             patch.object(campaign, "source_snapshot", return_value=self.source, side_effect=source_effect), \
             patch.object(campaign.subprocess, "check_output", side_effect=self.fixture.git), \
             patch.object(campaign.probe, "available_ram", return_value=10**12), \
             patch.object(campaign.shutil, "disk_usage", return_value=v4.old_fixtures.FREE), ExitStack() as stack:
            if not real_clock: stack.enter_context(patch.object(campaign.time, "monotonic", side_effect=lambda:self.tick))
            return campaign.run_campaign(self.output, HEAD, self.admission_path, ref["sha256"], "full",
                transport=transport or http, sleeper=sleeper or (time.sleep if real_clock else sleep))

    def rebind(self, name):
        ref = self.admission["v4_prefix"][name]; new = v4.bound(Path(ref["path"]))
        self.admission["v4_prefix"][name] = new
        self.pin["pages_journal" if name == "pages" else name] = (new["bytes"], new["sha256"])

    def assert_blocked_before_http(self, pattern=None):
        with self.assertRaisesRegex(Exception, pattern or ".*"): self.execute()
        self.assertEqual(self.calls, []); self.assertFalse(self.output.exists())

    def test_replay_and_exact_gamma_continuation_no_completed_stream_recollection(self):
        result = self.execute(); self.assertEqual(result["status"], campaign.SUCCESS, result)
        self.assertEqual([item["stream"] for item in self.calls], [stream["id"] for stream in campaign.STREAMS[2:]])
        old_state = json.loads((self.old_root/"FAILURE.json").read_bytes())["streams"][2]
        self.assertEqual(self.calls[0]["parameters"]["after_cursor"], old_state["next_cursor"])
        self.assertNotIn("after_cursor", self.calls[1]["parameters"])
        self.assertTrue(all(row["pagination_terminal_observed"] for row in result["streams"]))
        manifest = json.loads((self.output/"manifest.json").read_bytes())
        self.assertEqual(manifest["replay"]["accepted_pages"], 5)
        self.assertEqual(len(list((self.output/"responses").iterdir())), 3)
        self.assertEqual(result["usage"]["http_attempts_charged"], campaign.PRIOR_MINIMUM["http_attempts"]+3)
        self.assertFalse(manifest["universe_complete"]); self.assertFalse(manifest["snapshot_consistency_certified"])
        self.assertTrue((self.output/"COMPLETE.json").is_file())

    def test_original_ten_thousand_and_sixteen_mib_limits_unchanged(self):
        self.assertEqual(campaign.previous.MAX_CONTROL, 16*1024**2)
        self.assertEqual(campaign.CAPS, campaign.previous.CAPS)
        self.assertIn("10_000", __import__("inspect").getsource(campaign.previous.inventory_files))

    def test_pin_mismatch_or_raw_body_mutation_blocks_before_http(self):
        path = self.old_root/"responses/a00001.body.gz"; data = path.read_bytes(); path.write_bytes(data[:-1]+b"x")
        self.assert_blocked_before_http("reference_bytes_or_hash_differs")

    def test_dropped_page_even_rebound_does_not_skip_coverage(self):
        path = self.old_root/"pages.jsonl"; lines = path.read_bytes().splitlines(keepends=True); path.write_bytes(b"".join(lines[1:])); self.rebind("pages")
        self.assert_blocked_before_http("prefix_page_sequence_differs")

    def test_extra_orphan_attempt_even_rebound_blocks(self):
        path = self.old_root/"attempts.jsonl"; path.write_bytes(path.read_bytes()+campaign.encode({"event":"START","call":6})); self.rebind("attempts")
        self.assert_blocked_before_http("prefix_counts_or_orphan_attempts")

    def test_invalidated_or_incomplete_finish_blocks(self):
        path = self.old_root/"attempts.jsonl"; records = [json.loads(line) for line in path.read_bytes().splitlines()]
        records[1]["body_complete"] = False; path.write_bytes(b"".join(campaign.encode(row) for row in records)); self.rebind("attempts")
        self.assert_blocked_before_http("prefix_attempt_binding_differs")

    def test_crossjoined_page_index_and_role_must_match(self):
        path = self.old_root/"pages.jsonl"; records = [json.loads(line) for line in path.read_bytes().splitlines()]
        records[0]["page_index"] = 9; path.write_bytes(b"".join(campaign.encode(row) for row in records)); self.rebind("pages")
        self.assert_blocked_before_http("prefix_cursor_or_index_differs")

    def test_unknown_extra_tree_file_not_silently_imported(self):
        (self.old_root/"unexpected").write_bytes(b"unbilled")
        self.assert_blocked_before_http("prefix_tree_roster_differs")

    def test_all_cumulative_floors_and_extra_control_additive_charges(self):
        self.admission["prior_ledger"]["http_attempts"] -= 1
        self.assert_blocked_before_http("prior_usage_below_floor")

    def test_response_floor_and_control_reservation_cannot_be_refunded(self):
        self.admission["prior_ledger"]["response_bytes_charged"] -= 1
        self.assert_blocked_before_http("prior_usage_below_floor")

    def test_extra_control_bytes_remain_additive_to_old_floor(self):
        self.admission["prior_ledger"]["retained_bytes_charged"]["control"] = campaign.RETAINED_MINIMUM["control"]
        self.assert_blocked_before_http("prior_retained_floor_refunded")

    def test_http_ceiling_cannot_reset_after_replay(self):
        self.admission["prior_ledger"]["http_attempts"]=campaign.CAPS["http_attempts"]
        result=self.execute();self.assertEqual(result["error_code"],"http_attempt_limit")
        self.assertEqual(self.calls,[]);self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_response_reservation_uses_only_remaining_cumulative_bytes(self):
        self.admission["prior_ledger"]["response_bytes_charged"]=campaign.CAPS["total_response_bytes"]-3
        result=self.execute();self.assertEqual(result["error_code"],"response_byte_limit")
        self.assertEqual(result["usage"]["response_bytes_charged"],campaign.CAPS["total_response_bytes"])
        self.assertEqual(len(self.calls),1)

    def test_raw_partition_cannot_borrow_unused_mapping_budget(self):
        self.admission["prior_ledger"]["retained_bytes_charged"]["raw"]=campaign.CAPS["raw_body_bytes"]-1
        result=self.execute();self.assertEqual(result["error_code"],"metadata_aggregate_or_subcap")
        self.assertEqual(self.calls,[]);self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_collection_receipt_cannot_be_hidden_as_extra_operational_control(self):
        ref=v4.write(self.ops/"rpc.jsonl",{"calls":1})
        self.admission["prior_ledger"]["root_controls"].append(ref)
        self.assert_blocked_before_http("root_control_invalid_or_duplicate")

    def test_runtime_floor_and_exhaustion_block_before_replay_or_network(self):
        self.admission["prior_ledger"]["runtime_seconds"] = 86400
        self.assert_blocked_before_http("runtime_limit")

    def test_root_drain_ownership_cannot_be_unknown(self):
        ref = self.admission["v4_prefix"]["drain"]; value = json.loads(Path(ref["path"]).read_bytes())
        value["owned_group_and_session_empty"] = False; v4.write(Path(ref["path"]), value); self.rebind("drain")
        self.assert_blocked_before_http("prefix_drain_differs")

    def test_wrong_scope_in_new_page_is_rejected_with_body_and_charge(self):
        responses = self.responses(); stream = campaign.STREAMS[2]; bad = gamma_row(stream, 1001); bad["closed"] = not stream["closed"]
        responses[0] = Response(gamma_page(stream, rows=[bad]))
        result = self.execute(responses); self.assertEqual(result["error_code"], "gamma_returned_scope_conflict")
        self.assertEqual(len(self.calls), 1); self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_retry_and_spacing_use_safe_wait_and_keep_full_failed_charge(self):
        responses = [OSError("private-secret-query"), *self.responses()]
        result = self.execute(responses); self.assertEqual(result["status"], campaign.SUCCESS, result)
        self.assertGreaterEqual(result["usage"]["this_run_response_bytes_charged"], campaign.CAPS["individual_response_bytes"])
        self.assertEqual(self.calls[0], self.calls[1])
        self.assertNotIn("private-secret-query", (self.output/"attempts.jsonl").read_text())

    def test_three_exact_failures_exhaust_attempts_without_reset(self):
        result = self.execute([OSError("secret")]*3)
        self.assertEqual(result["status"], "failed_unaccepted"); self.assertEqual(len(self.calls), 3)
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"], 3*campaign.CAPS["individual_response_bytes"])

    def test_durable_start_signal_boundary_retains_full_response_reservation(self):
        original=campaign.Ledger.append
        def append(ledger,name,value,**kwargs):
            result=original(ledger,name,value,**kwargs)
            if value.get("event")=="START":raise campaign.Error("runtime_limit")
            return result
        with patch.object(campaign.Ledger,"append",append):result=self.execute()
        self.assertEqual(result["error_code"],"runtime_limit");self.assertEqual(self.calls,[])
        self.assertEqual(result["usage"]["this_run_http_attempts"],1)
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"],campaign.CAPS["individual_response_bytes"])
        records=[json.loads(line) for line in (self.output/"attempts.jsonl").read_bytes().splitlines()]
        self.assertEqual([row["event"] for row in records],["START"])
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_unknown_error_text_never_echoes_even_if_probe_error(self):
        result = self.execute([campaign.Error("private_token_value")])
        self.assertEqual(result["error_code"], "contract_validation_error")
        self.assertNotIn("private_token_value", (self.output/"attempts.jsonl").read_text())
        self.assertNotIn("private_token_value", (self.output/"FAILURE.json").read_text())

    def test_partial_read_and_close_fault_keep_prefix_and_full_allowance(self):
        class Partial(Response):
            def read(self, _): raise campaign.http.client.IncompleteRead(b"observed-prefix")
            def close(self): raise OSError("private-close-secret")
        result = self.execute([Partial(b"unused")])
        self.assertEqual(result["status"], "failed_unaccepted")
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"], campaign.CAPS["individual_response_bytes"])
        self.assertEqual(gzip.decompress((self.output/"responses/a00001.body.gz").read_bytes()), b"observed-prefix")

    def test_actual_hard_attempt_alarm_interrupts_blocking_read(self):
        class Blocked(Response):
            def read(self, _): time.sleep(1); return b""
        started=time.monotonic()
        with patch.object(campaign,"HTTP_SECONDS",.03): result=self.execute([Blocked(b"unused")],real_clock=True)
        self.assertEqual(result["error_code"],"http_attempt_timeout")
        self.assertLess(time.monotonic()-started,1.)
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_actual_hard_attempt_alarm_covers_blocking_close(self):
        class Blocked(Response):
            def close(self):time.sleep(1)
        started=time.monotonic()
        with patch.object(campaign,"HTTP_SECONDS",.03):result=self.execute([Blocked(b"{}")],real_clock=True)
        self.assertEqual(result["error_code"],"http_attempt_timeout");self.assertLess(time.monotonic()-started,1.)
        self.assertEqual(result["usage"]["this_run_response_bytes_charged"],campaign.CAPS["individual_response_bytes"])

    def test_no_duplicate_prefix_body_or_old_record_price_inference(self):
        result=self.execute(); self.assertEqual(result["status"],campaign.SUCCESS)
        old=json.loads((self.old_root/"FAILURE.json").read_bytes())
        self.assertEqual(result["streams"][0],old["streams"][0]); self.assertEqual(result["streams"][1],old["streams"][1])

    def test_source_change_before_first_dispatch_blocks_without_network(self):
        count=0
        def snapshot(*_):
            nonlocal count
            count+=1
            return self.source if count==1 else self.source|{"head":"f"*40}
        self.assert_blocked_before_http_with_source(snapshot)

    def assert_blocked_before_http_with_source(self,effect):
        with self.assertRaisesRegex(campaign.Error,"source_changed_before_dispatch"): self.execute(source_effect=effect)
        self.assertEqual(self.calls,[]); self.assertFalse(self.output.exists())

    def test_original_prior_or_new_admission_late_mutation_blocks_marker(self):
        original=campaign.Ledger.json
        def write(ledger,name,value,**kwargs):
            result=original(ledger,name,value,**kwargs)
            if name==".COMPLETE.pending":
                content=self.admission_path.read_bytes(); self.admission_path.write_bytes(content[:-1]+b" ")
            return result
        with patch.object(campaign.Ledger,"json",write): result=self.execute()
        self.assertEqual(result["status"],"failed_unaccepted");self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_expired_final_reopens_never_publish_marker(self):
        original=campaign.Ledger.json
        def write(ledger,name,value,**kwargs):
            result=original(ledger,name,value,**kwargs)
            if name==".COMPLETE.pending":self.tick=86400
            return result
        with patch.object(campaign.Ledger,"json",write): result=self.execute()
        self.assertEqual(result["status"],"failed_unaccepted");self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_late_extra_prefix_file_is_not_hidden_by_saved_hashes(self):
        original=campaign.Ledger.json
        def write(ledger,name,value,**kwargs):
            result=original(ledger,name,value,**kwargs)
            if name==".COMPLETE.pending":(self.old_root/"unbilled").write_bytes(b"new")
            return result
        with patch.object(campaign.Ledger,"json",write):result=self.execute()
        self.assertEqual(result["status"],"failed_unaccepted");self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_source_change_before_dispatch_or_late_publication_blocks(self):
        original = campaign.Ledger.json
        def write(ledger, name, value, **kwargs):
            result = original(ledger, name, value, **kwargs)
            if name == ".COMPLETE.pending":
                path = self.old_root/"responses/a00001.body.gz"; data=path.read_bytes(); path.write_bytes(data[:-1]+b"x")
            return result
        with patch.object(campaign.Ledger, "json", write):
            result = self.execute()
        self.assertEqual(result["status"], "failed_unaccepted"); self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_existing_output_is_never_reused(self):
        self.execute()
        with self.assertRaisesRegex(campaign.Error,"destination_not_fresh"): self.execute()

    def test_fixed_journal_pins_cannot_be_substituted_without_contract_change(self):
        self.admission["v4_prefix"]["pages"]["sha256"]="0"*64
        self.assert_blocked_before_http("prefix_journal_pin_differs")

    def test_source_snapshot_freezes_new_source_and_tests_against_commit(self):
        original=campaign.source_snapshot
        data={name:(campaign.REPO/name).read_bytes() for name in campaign.SOURCE_FILES[len(campaign.previous.SOURCE_FILES):]}
        parent={"head":HEAD,"files":{name:self.source["files"][name] for name in campaign.previous.SOURCE_FILES}}
        def git(args,**_):
            name=args[-1].split(":",1)[1]
            return str(len(data[name])).encode() if "-s" in args else data[name]
        with patch.object(campaign.previous,"source_snapshot",side_effect=lambda _:copy.deepcopy(parent)),patch.object(campaign.subprocess,"check_output",side_effect=git):
            snapshot=original(HEAD)
            self.assertEqual(snapshot["files"][next(iter(data))]["sha256"],campaign.digest(next(iter(data.values()))))
        broken={**data};broken[next(iter(data))]=b"x"*len(next(iter(data.values())))
        def badgit(args,**_):
            name=args[-1].split(":",1)[1]
            return str(len(broken[name])).encode() if "-s" in args else broken[name]
        with patch.object(campaign.previous,"source_snapshot",side_effect=lambda _:copy.deepcopy(parent)),patch.object(campaign.subprocess,"check_output",side_effect=badgit),self.assertRaisesRegex(campaign.Error,"own_source_blob_differs"):
            original(HEAD)

    def test_main_guard_before_any_input_or_source_read(self):
        args = ["--mode","full","--admission",str(self.admission_path),"--admission-sha256","a"*64,"--expected-head",HEAD,"--run-dir",str(self.output)]
        with patch.object(campaign,"require_production_host",side_effect=RuntimeError("guard")), patch.object(campaign,"source_snapshot") as source, self.assertRaises(RuntimeError): campaign.main(args)
        source.assert_not_called()


class WaitAndDiagnosticTests(unittest.TestCase):
    def test_legacy_race_reproduced_and_new_wait_returns_without_negative_sleep(self):
        ledger = SimpleNamespace(remaining_time=lambda:100.)
        with patch.object(campaign.legacy.time,"monotonic",side_effect=[.9,1.1]), self.assertRaises(ValueError):
            campaign.legacy.wait_until(1.,ledger,time.sleep)
        sleeps=[]
        with patch.object(campaign.time,"monotonic",side_effect=[.9,1.1]): campaign.wait_until(1.,ledger,sleeps.append)
        self.assertEqual(len(sleeps),1); self.assertGreater(sleeps[0],0)

    def test_past_target_and_clock_jump_never_sleep_zero_or_negative(self):
        sleeper=Mock()
        with patch.object(campaign.time,"monotonic",return_value=2.): campaign.wait_until(1.,SimpleNamespace(remaining_time=lambda:100),sleeper)
        sleeper.assert_not_called()

    def test_wait_cannot_extend_global_deadline_or_swallow_interrupt(self):
        with patch.object(campaign.time,"monotonic",return_value=0), self.assertRaisesRegex(campaign.Error,"runtime_limit"):
            campaign.wait_until(1.,SimpleNamespace(remaining_time=lambda:.5),Mock())
        with patch.object(campaign.time,"monotonic",return_value=0), self.assertRaises(KeyboardInterrupt):
            campaign.wait_until(1.,SimpleNamespace(remaining_time=lambda:2),Mock(side_effect=KeyboardInterrupt))

    def test_diagnostic_never_serializes_exception_text_locals_or_custom_type(self):
        secret="secret-token-in-local"
        try: raise ValueError(secret)
        except ValueError as exc: result=campaign.diagnostic(exc,"spacing_wait")
        self.assertEqual(result["exception_class"],"ValueError"); self.assertNotIn(secret,campaign.encode(result).decode())
        custom=type("private_secret_type",(Exception,),{})(secret)
        self.assertEqual(campaign.diagnostic(custom,"untrusted-secret")["exception_class"],"unlisted")
        self.assertNotIn("secret",campaign.encode(campaign.diagnostic(custom,"untrusted-secret")).decode())

    def test_alarm_boundary_never_arms_negative_interval_or_grants_new_deadline(self):
        ledger=SimpleNamespace(remaining_time=lambda:100.,check=lambda:None)
        armed=[]
        def timer(_kind,delay):
            self.assertGreaterEqual(delay,0);armed.append(delay)
        with patch.object(campaign.signal,"getitimer",return_value=(0.,0.)),patch.object(campaign.signal,"getsignal",return_value=signal.SIG_DFL), \
             patch.object(campaign.signal,"signal"),patch.object(campaign.signal,"setitimer",side_effect=timer), \
             patch.object(campaign.time,"monotonic",side_effect=[0.,.2]),self.assertRaisesRegex(campaign.Error,"http_attempt_timeout"):
            with campaign.attempt_alarm(ledger,.1):pass
        self.assertTrue(all(value>=0 for value in armed))

    def test_alarm_restores_outer_timer_with_elapsed_time_debited(self):
        ledger=SimpleNamespace(remaining_time=lambda:100.,check=lambda:None);tick=0.;armed=[]
        with patch.object(campaign.signal,"getitimer",return_value=(10.,0.)),patch.object(campaign.signal,"getsignal",return_value=signal.SIG_DFL), \
             patch.object(campaign.signal,"signal"),patch.object(campaign.signal,"setitimer",side_effect=lambda _,delay:armed.append(delay)), \
             patch.object(campaign.time,"monotonic",side_effect=lambda:tick):
            with campaign.attempt_alarm(ledger,1.):tick=.2
        self.assertAlmostEqual(armed[-1],9.8)


if __name__ == "__main__": unittest.main()
