"""Small fake-HTTP toolchain fixtures; no network or executable invocation."""
from __future__ import annotations
import contextlib
import copy
import http.client
import io
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import tempfile
import time
import unittest
from unittest.mock import patch
import urllib.error

from scripts import collect_polymarket_solc_binary_v2 as collect

HEAD="a"*40
SOURCE={"head":HEAD,"files":{"fixture":"b"*64}}
FREE=shutil._ntuple_diskusage(10**12,0,10**12)
BINARY=b"\x7fELFpublic fixture compiler bytes, never executed\n"
META_CAPS={"aggregate_metadata_bytes":8_000_000_000,"raw_body_bytes":4_000_000_000,
           "control_bytes":500_000_000,"other_metadata_bytes":3_500_000_000,
           "http_attempts":40000,"total_response_bytes":32*1024**3,"runtime_seconds":86400}
SOURCE_SNAPSHOT=collect.source_snapshot


def ref(path):
    raw=path.read_bytes();return {"path":str(path),"bytes":len(raw),"sha256":collect.digest(raw)}


def write(path,value,raw=False):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(value if raw else collect.encode(value));return ref(path)


class Response:
    def __init__(self,body=BINARY,status=200,headers=None,reads=None,close_error=None,url=None):
        self.body,self.status,self.offset=body,status,0
        self.headers={"Content-Length":str(len(body))} if headers is None else headers
        self.reads=list(reads) if reads is not None else None;self.close_error=close_error;self.url=url or collect.URL
    def geturl(self):return self.url
    def read(self,size):
        if self.reads is not None:
            value=self.reads.pop(0) if self.reads else b""
            if isinstance(value,BaseException):raise value
            return value
        raw=self.body[self.offset:self.offset+size];self.offset+=len(raw);return raw
    def close(self):
        if self.close_error is not None:raise self.close_error


class CompilerAcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.output=self.root/"new-run";self.admission=self.root/"new-admission.json"
        rp=patch.object(collect,"RUN_ROOT",self.root);rp.start();self.addCleanup(rp.stop)
        fp=patch.object(collect,"FAILED_ROOT",self.root/"metadata-full");fp.start();self.addCleanup(fp.stop)
        tp=patch.object(collect,"TERMINAL_PINS",{});tp.start();self.addCleanup(tp.stop)
        bp=patch.object(collect,"COMPILER_SHA",collect.digest(BINARY));bp.start();self.addCleanup(bp.stop)
        sp=patch.object(collect,"source_snapshot",return_value=copy.deepcopy(SOURCE));self.snapshot=sp.start();self.addCleanup(sp.stop)
        dp=patch.object(collect.shutil,"disk_usage",return_value=FREE);self.disk=dp.start();self.addCleanup(dp.stop)
        self.old_roots=[self.root/f"metadata-old-{i}" for i in range(4)];self.full=self.root/"metadata-full"
        self.old_files=[]
        for index,root in enumerate(self.old_roots):
            control=root/"STATE.json";body=root/"responses/a01.body.gz"
            self.old_files.extend([("control",write(control,{"i":index})),("raw",write(body,b"market raw bytes not to be rehashed",raw=True))])
        self.operation=self.root/"prior-operation.json";self.old_files.append(("control",write(self.operation,{"finished":True})))
        self.parent_path=self.root/"full-metadata-admission.json"
        self.parent={"schema_version":"polymarket_market_metadata_campaign_admission_v3","status":"root_metadata_campaign_admitted",
            "run_dir":str(self.full),"consumer_head":collect.PRODUCER_HEAD,"caps":META_CAPS,
            "prior_ledger":{"all_prior_metadata_roots_included":True,"roots":[str(x) for x in self.old_roots],
                "standalone_control_files":[str(self.operation)],"files":[{**binding,"category":category} for category,binding in self.old_files],
                "http_attempts":4857,"response_bytes_charged":11970589074,"runtime_seconds":4200}}
        self.new_operation=self.root/"post-full-operation.json";write(self.new_operation,{"drained":True})
        self.start=write(self.full/"START.json",{"source":"frozen"})
        self.full_raw=write(self.full/"responses/a00001.body.gz",b"finished metadata response",raw=True)
        self.failure={"status":"failed_unaccepted","error_code":"response_byte_limit","universe_complete":False,"analysis_admitted":False,
            "usage":{"http_attempts_charged":4859,"response_bytes_charged":11970589082,"this_run_http_attempts":2,
                "this_run_response_bytes_charged":8,"response_bytes_observed_this_run":8,"runtime_seconds_charged":7122.941926235002,
                "retained_bytes_charged":{"raw":0,"control":0,"other":0}},
            "waterfall":{"http_attempts_this_run":2,"accepted_pages":1,"failed_attempts":1}}
        self.drain_overrides={};self.journal_edit=lambda rows:None;self.entry_edit=lambda entries:None;self.refresh()
        self.calls=[]

    def refresh(self):
        parent_ref=write(self.parent_path,self.parent)
        self.start=write(self.full/"START.json",{"schema_version":"polymarket_market_metadata_campaign_v3",
            "source":{"head":collect.PRODUCER_HEAD},"admission":parent_ref,"caps":META_CAPS,
            "universe_complete":False,"analysis_admitted":False})
        bodies=[self.full_raw,write(self.full/"responses/a00002.body.gz",b"partial response prefix",raw=True)]
        rows=[]
        for index,body in enumerate(bodies):
            call=index+1;accepted=index==0;request={"url":"https://public.invalid/markets","query":{"cursor":str(index)}}
            request_sha=collect.digest(collect.encode(request))
            rows.append({"event":"START","call":call,"stream":"clob","page_index":index,"exact_page_attempt":1,
                "request":request,"request_sha256":request_sha,"response_byte_reservation":4})
            rows.append({"event":"FINISH","call":call,"stream":"clob","page_index":index,"request_sha256":request_sha,
                "response_bytes_charged":4,"response_bytes_observed":4,"body_complete":accepted,"transport_dispatched":True,
                "interrupted":False,"accepted":accepted,"error_code":None if accepted else "response_byte_limit",
                "body":{**body,"path":f"responses/a{call:05d}.body.gz","category":"raw","uncompressed_bytes":4,
                    "uncompressed_sha256":"d"*64,"complete_received_body":accepted}})
        self.journal_edit(rows)
        journal_ref=write(self.full/"attempts.jsonl",b"".join(collect.encode(x) for x in rows),raw=True)
        pages_ref=write(self.full/"pages.jsonl",b"{}\\n",raw=True)
        failure_ref=write(self.full/"FAILURE.json",self.failure)
        self.launch_path=self.root/"metadata.LAUNCH.json"
        launch={"command":["/home/ubuntu/venv/bin/python","scripts/collect_polymarket_market_metadata_campaign_v3.py",
            "--admission",str(self.parent_path),"--admission-sha256",parent_ref["sha256"],
            "--expected-head",collect.PRODUCER_HEAD,"--run-dir",str(self.full)],
            "source_head":collect.PRODUCER_HEAD,"pid":123,"linux_birth_ticks":456,"boot_id":"fixture-boot"}
        launch_ref=write(self.launch_path,launch)
        exit_ref=write(self.root/"metadata.EXIT.json",{"exit_code":1,"launch":launch})
        drain={"schema_version":"polymarket_oct1_terminal_shutdown_drain_v1","source_head":collect.PRODUCER_HEAD,
            "source_clean":True,"metadata_failure":failure_ref,"boot_id":launch["boot_id"],
            "agents_and_transfers_released":True,"shutdown_ready":True,
            "live_receipt_births":[],"unknown_process_receipts":[],"owned_live_groups_or_sessions":[],
            "relevant_compute_notebook_transfer_consumers":[],
            "metadata_conservative_runtime_reservation_seconds":7800,
            "process_receipts":[{"binding":launch_ref,"pid":123,"birth_ticks":456,"receipt_boot_id":"fixture-boot",
                                 "matching_live_birth":False}]}
        drain.update(self.drain_overrides)
        drain_ref=write(self.root/"drain.json",drain)
        pins={"start":self.start,"failure":failure_ref,"admission":parent_ref,"launch":launch_ref,"exit":exit_ref,"drain":drain_ref}
        collect.TERMINAL_PINS.clear();collect.TERMINAL_PINS.update(pins)
        producer=[*self.old_files,("control",parent_ref),("control",ref(self.new_operation)),
            ("control",self.start),*[("raw",b) for b in bodies],("control",journal_ref),("control",pages_ref),
            ("control",failure_ref),("control",launch_ref),("control",exit_ref),("control",drain_ref)]
        entries=[{**binding,"category":category,"identity":collect.identity(Path(binding["path"]))} for category,binding in producer]
        self.entry_edit(entries)
        totals={k:sum(e["bytes"] for e in entries if e["category"]==k) for k in ("raw","control","other")}
        self.value={"schema_version":"polymarket_solc_acquisition_admission_v2","status":"root_terminal_metadata_solc_admitted",
            "consumer_head":HEAD,"run_dir":str(self.output),"caps":copy.deepcopy(collect.CAPS),
            "metadata_terminal":{"root":str(self.full),**pins},
            "prior_usage":copy.deepcopy(collect.META_MINIMA),
            "prior_ledger":{"all_prior_metadata_roots_included":True,"roots":[str(x) for x in self.old_roots]+[str(self.full)],
                "standalone_control_files":[str(self.operation),str(self.new_operation),str(self.parent_path),
                    str(self.launch_path),exit_ref["path"],drain_ref["path"]],
                "files":entries,"retained_bytes_charged":totals}}
        self.admission_ref=write(self.admission,self.value)

    def fake_http(self,timeout):self.calls.append(timeout);return Response()
    def execute(self,http=None,**kwargs):
        return collect.run_collection(self.admission,self.admission_ref["sha256"],HEAD,self.output,http=http or self.fake_http,**kwargs)
    def blocked(self,code=None,**kwargs):
        try:result=self.execute(**kwargs)
        except collect.Blocked as exc:
            self.assertFalse((self.output/"COMPLETE.json").exists())
            if code:self.assertEqual(str(exc),code)
            return None
        self.assertEqual(result["status"],"blocked",result);self.assertFalse((self.output/"COMPLETE.json").exists())
        if code:self.assertEqual(result["error_code"],code)
        return result

    def test_authenticated_binary_remains_nonexecutable_and_scope_is_toolchain(self):
        result=self.execute();self.assertEqual(result["status"],"authenticated_solc_binary_not_executed",result)
        self.assertEqual(result["http_attempts"],1);self.assertEqual(len(self.calls),1);self.assertLessEqual(self.calls[0],30)
        binary=self.output/result["binary"]["path"];self.assertEqual(binary.read_bytes(),BINARY)
        self.assertFalse(binary.stat().st_mode&0o111);self.assertFalse(result["compiler_executed"])
        self.assertFalse(result["prior_raw_body_sha_rehashed"]);self.assertFalse(result["sample_admitted"])
        self.assertEqual(result["evidence_class"],"public_toolchain_binary")
        marker=json.loads((self.output/"COMPLETE.json").read_bytes());self.assertEqual(marker["result"]["sha256"],ref(self.output/"RESULT.json")["sha256"])

    def test_prior_raw_bodies_are_never_opened(self):
        bodies={Path(item["path"]) for item in self.value["prior_ledger"]["files"] if item["category"]=="raw"}
        original=Path.open
        def opened(path,*args,**kwargs):
            if path in bodies:raise AssertionError("prior raw body read")
            return original(path,*args,**kwargs)
        with patch.object(Path,"open",opened):result=self.execute()
        self.assertEqual(result["status"],"authenticated_solc_binary_not_executed",result)

    def test_terminal_failure_is_required_but_successful_pagination_is_not(self):
        self.assertEqual(self.execute()["status"],"authenticated_solc_binary_not_executed")
        shutil.rmtree(self.output);self.failure["status"]="complete";self.refresh()
        self.blocked("metadata_terminal_not_expected_failure")
        self.assertEqual(len(self.calls),1)

    def save_admission(self):
        self.admission_ref=write(self.admission,self.value)

    def test_v2_carries_only_own_new_charges_and_preserves_inherited_minima(self):
        result=self.execute()
        self.assertEqual(result["schema_version"],"polymarket_solc_binary_acquisition_v2")
        self.assertEqual(result["prior_metadata_usage"],collect.META_MINIMA)
        self.assertEqual(result["http_attempts"],1)
        self.assertEqual(result["response_bytes_charged"],len(BINARY))
        self.assertEqual(result["runtime_seconds_charged"],300)
        self.assertEqual(result["cumulative_metadata_usage"],{"http_attempts":4860,
            "response_bytes_charged":11970589082+len(BINARY),"runtime_seconds":8100})
        self.assertFalse(result["metadata_pagination_succeeded"])
        self.assertTrue(result["terminal_drain_is_historical_not_current_host_idle"])

    def test_all_three_inherited_usage_minima_are_mandatory_before_http(self):
        for key in collect.META_MINIMA:
            self.refresh();self.value["prior_usage"][key]=collect.META_MINIMA[key]-1;self.save_admission()
            self.blocked("prior_usage_below_terminal_minimum")
        self.assertEqual(self.calls,[])

    def test_global_metadata_reservation_includes_fixed_new_call_body_and_runtime_limits(self):
        for key in collect.META_MINIMA:
            self.refresh();self.value["prior_usage"][key]=collect.META_CEILINGS[key];self.save_admission()
            self.blocked("metadata_cumulative_reservation")
        self.assertEqual(self.calls,[])

    def test_retained_charges_cannot_refund_old_reservations_or_observed_files(self):
        self.failure["usage"]["retained_bytes_charged"]["control"]=1000000
        self.refresh();self.blocked("prior_retained_charge_understated")
        self.failure["usage"]["retained_bytes_charged"]["control"]=0;self.refresh()
        self.value["prior_ledger"]["retained_bytes_charged"]["raw"]=0;self.save_admission()
        self.blocked("prior_retained_charge_understated");self.assertEqual(self.calls,[])

    def test_drain_unknown_live_or_missing_launch_binding_blocks(self):
        for changes in ({"unknown_process_receipts":[{}]},{"live_receipt_births":[{}]},
                        {"owned_live_groups_or_sessions":[{}]},{"agents_and_transfers_released":False},
                        {"process_receipts":[]}):
            self.drain_overrides=changes;self.refresh();self.blocked("metadata_terminal_drain_binding")
        self.assertEqual(self.calls,[])

    def test_root_runtime_reservation_may_not_be_refunded(self):
        self.drain_overrides={"metadata_conservative_runtime_reservation_seconds":7900}
        self.refresh();self.blocked("prior_runtime_below_root_drain");self.assertEqual(self.calls,[])

    def test_published_metadata_complete_or_manifest_never_relabels_failed_run(self):
        for name in ("COMPLETE.json","manifest.json"):
            path=self.full/name;write(path,{"status":"not actually complete"})
            self.blocked("metadata_terminal_not_expected_failure");path.unlink()
        self.assertEqual(self.calls,[])

    def test_journal_missing_duplicate_or_unclosed_records_block(self):
        def missing(rows):rows.pop()
        def duplicate(rows):rows[2]["call"]=1
        def unclosed(rows):rows.append(copy.deepcopy(rows[-2]))
        for edit in (missing,duplicate,unclosed):
            self.journal_edit=edit;self.refresh();self.blocked()
        self.assertEqual(self.calls,[])

    def test_journal_response_usage_and_body_bindings_must_reconcile(self):
        def wrong_body(rows):rows[-1]["body"]["sha256"]="e"*64
        def wrong_request(rows):rows[-1]["request_sha256"]="f"*64
        def wrong_observed(rows):rows[-1]["response_bytes_observed"]=3
        def wrong_locator(rows):rows[-1]["page_index"]=0
        for edit in (wrong_body,wrong_request,wrong_observed,wrong_locator):
            self.journal_edit=edit;self.refresh();self.blocked()
        self.assertEqual(self.calls,[])

    def test_failure_may_not_understate_or_invent_journal_charges(self):
        for key in ("this_run_http_attempts","this_run_response_bytes_charged","response_bytes_charged",
                    "response_bytes_observed_this_run","http_attempts_charged"):
            saved=self.failure["usage"][key];self.failure["usage"][key]=saved-1
            self.refresh();self.blocked("failed_journal_usage_differs")
            self.failure["usage"][key]=saved
        self.assertEqual(self.calls,[])

    def test_usage_booleans_cannot_masquerade_as_integer_or_runtime_charge(self):
        for key in collect.META_MINIMA:
            self.refresh();self.value["prior_usage"][key]=True;self.save_admission()
            self.blocked("usage_number_schema")
        self.assertEqual(self.calls,[])

    def test_extra_failed_raw_body_or_unlisted_partial_is_not_implicitly_admitted(self):
        extra=self.full/"responses/extra.body.gz";write(extra,b"orphan",raw=True)
        self.blocked("prior_ledger_coverage");self.assertEqual(self.calls,[])

    def test_unexpected_other_root_bytes_cannot_hide_source_response_body(self):
        for item in self.value["prior_ledger"]["files"]:
            if item["path"]==self.full_raw["path"]:item["category"]="other"
        self.save_admission();self.blocked("prior_raw_body_misclassified");self.assertEqual(self.calls,[])

    def test_four_prior_roots_and_comprehensive_files_are_required(self):
        self.parent["prior_ledger"]["roots"].pop();self.refresh();self.blocked("four_prior_metadata_roots_missing")

    def test_missing_file_misclassified_body_and_wrong_identity_fail_preflight(self):
        for mode in ("missing","category","size","ctime","duplicate"):
            def edit(entries):
                if mode=="missing":entries.pop()
                elif mode=="category":next(x for x in entries if x["category"]=="raw")["category"]="other"
                elif mode=="size":entries[0]["bytes"]-=1
                elif mode=="ctime":entries[0]["identity"]["ctime_ns"]+=1
                else:entries.append(copy.deepcopy(entries[0]))
            self.entry_edit=edit;self.refresh();self.blocked();self.assertEqual(self.calls,[])

    def test_unlisted_file_or_symlink_blocks_hierarchy(self):
        extra=self.old_roots[0]/"unexpected";extra.write_bytes(b"unexpected");self.blocked("prior_ledger_coverage")
        extra.unlink();extra.symlink_to(self.operation);self.blocked("prior_tree_symlink")

    def test_metadata_partition_and_global_reservations_gate_before_http(self):
        self.entry_edit=lambda entries:None
        with patch.dict(collect.CAPS,other_metadata_bytes=100):
            self.refresh();self.blocked("metadata_partition_reservation")
        self.assertEqual(self.calls,[])

    def test_control_read_caps_include_final_reopen(self):
        with patch.dict(collect.CAPS,all_small_control_read_bytes=1):self.refresh();self.blocked("small_control_read_limit")
        self.assertEqual(self.calls,[])

    def test_sha_mismatch_blocks_without_retry(self):
        calls=[]
        def http(timeout):calls.append(timeout);return Response(b"wrong binary")
        result=self.blocked("compiler_sha_mismatch",http=http)
        self.assertEqual(len(calls),1);self.assertEqual((self.output/"attempt01.part").read_bytes(),b"wrong binary")
        self.assertFalse(result["compiler_executed"])

    def test_transient_503_retries_once_and_keeps_failed_body(self):
        responses=iter([Response(b"unavailable",503),Response()])
        result=self.execute(http=lambda _:next(responses))
        self.assertEqual(result["status"],"authenticated_solc_binary_not_executed",result)
        self.assertEqual(result["http_attempts"],2);self.assertEqual((self.output/"attempt01.part").read_bytes(),b"unavailable")

    def test_opaque_failures_exhaust_exact_two_attempt_ceiling(self):
        calls=[]
        def http(timeout):calls.append(timeout);raise urllib.error.URLError("fixture network failure")
        result=self.blocked("response_transport_or_read_failure",http=http)
        self.assertEqual(len(calls),2);self.assertEqual(result["response_bytes_charged"],collect.CAPS["total_response_bytes_charged"])

    def test_failed_reads_retain_prefix_and_charge_full_allowance(self):
        responses=iter([Response(headers={},reads=[b"prefix",OSError("read failed")]),Response()])
        result=self.execute(http=lambda _:next(responses))
        self.assertEqual(result["status"],"authenticated_solc_binary_not_executed",result)
        self.assertEqual((self.output/"attempt01.part").read_bytes(),b"prefix")
        self.assertEqual(result["attempts"][0]["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])

    def test_incomplete_read_partial_is_retained(self):
        responses=iter([Response(headers={},reads=[http.client.IncompleteRead(b"partial",10)]),Response()])
        result=self.execute(http=lambda _:next(responses));self.assertEqual(result["status"],"authenticated_solc_binary_not_executed",result)
        self.assertEqual((self.output/"attempt01.part").read_bytes(),b"partial")

    def test_close_failure_preserves_body_and_conservatively_charges(self):
        result=self.blocked("response_close_failed",http=lambda _:Response(close_error=OSError("close failed")))
        self.assertEqual((self.output/"attempt01.part").read_bytes(),BINARY)
        self.assertEqual(result["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])

    def test_unsealed_body_failure_is_fully_charged_and_never_retried(self):
        original=collect.Artifacts.seal
        def seal(writer,name):
            if name.endswith(".part"):raise OSError("staging hash fault")
            return original(writer,name)
        with patch.object(collect.Artifacts,"seal",seal):result=self.blocked()
        self.assertEqual(result["http_attempts"],1)
        self.assertEqual(result["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])
        self.assertFalse(result["attempts"][0]["body_integrity_verified"])
        self.assertEqual((self.output/"attempt01.part").read_bytes(),BINARY)

    def test_redirect_changed_url_encoding_and_oversize_block_without_followup(self):
        for response in (Response(b"redirect",302),Response(url="https://other.invalid"),
                         Response(headers={"Content-Encoding":"gzip"}),Response(headers={"Content-Length":str(collect.CAPS["body_bytes_per_attempt"]+1)})):
            if self.output.exists():shutil.rmtree(self.output)
            calls=[]
            def http(timeout):calls.append(timeout);return response
            self.blocked(http=http);self.assertEqual(len(calls),1)

    def test_body_overflow_detector_never_stored(self):
        with patch.dict(collect.CAPS,body_bytes_per_attempt=8,response_bytes_per_attempt_including_detector=9,total_response_bytes_charged=18):
            self.refresh();result=self.blocked("response_byte_limit",http=lambda _:Response(headers={},reads=[b"123456789"]))
        self.assertEqual((self.output/"attempt01.part").read_bytes(),b"12345678")
        self.assertEqual(result["attempts"][0]["response_bytes_observed"],9)

    def test_transport_uses_only_fixed_get_with_no_proxies_or_redirects(self):
        class Opener:
            def open(inner,request,timeout):
                self.assertEqual(request.full_url,collect.URL);self.assertEqual(request.get_method(),"GET")
                self.assertEqual(request.get_header("Accept-encoding"),"identity");return Response()
        with patch.object(collect.urllib.request,"build_opener",return_value=Opener()) as opener:
            collect.transport(30)
        handlers=opener.call_args.args
        self.assertEqual(handlers[0].proxies,{})
        self.assertIsInstance(handlers[1],collect.NoRedirect)
        self.assertIsNone(handlers[1].redirect_request(None,None,None,None,None,None))

    def test_deadline_includes_read_close_and_final_admission(self):
        clock=[0]
        class Slow(Response):
            def close(self):clock[0]=301
        result=self.blocked("runtime_limit",http=lambda _:Slow(),clock=lambda:clock[0])
        self.assertEqual(result["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])

    def test_absolute_attempt_deadline_includes_open_read_eof_and_close(self):
        for phase in ("open","read","eof","close"):
            with self.subTest(phase=phase):
                if self.output.exists():shutil.rmtree(self.output)
                tick=[0];calls=[];closed=[]
                class Slow(Response):
                    def read(inner,size):
                        raw=super().read(size)
                        if phase=="read" or (phase=="eof" and not raw):tick[0]=31
                        return raw
                    def close(inner):
                        closed.append(True)
                        if phase=="close":tick[0]=31
                def http(timeout):
                    calls.append(timeout)
                    if phase=="open":tick[0]=31
                    return Slow()
                result=self.blocked("http_attempt_timeout",http=http,clock=lambda:tick[0])
                self.assertEqual(len(calls),1);self.assertEqual(closed,[True])
                self.assertEqual(result["http_attempts"],1)
                self.assertFalse(result["attempts"][0]["transient"])
                self.assertEqual(result["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])
                expected=b"" if phase=="open" else BINARY
                self.assertEqual((self.output/"attempt01.part").read_bytes(),expected)
                self.assertEqual(result["attempts"][0]["response_bytes_observed"],len(expected))

    def test_consumed_prefix_is_retained_before_whole_or_attempt_deadline_failure(self):
        prefix=b"observed-prefix"
        for elapsed,code in ((30,"http_attempt_timeout"),(301,"runtime_limit")):
            with self.subTest(elapsed=elapsed):
                if self.output.exists():shutil.rmtree(self.output)
                tick=[0]
                class LatePrefix(Response):
                    def read(inner,size):tick[0]=elapsed;return prefix
                result=self.blocked(code,http=lambda _:LatePrefix(headers={}),clock=lambda:tick[0])
                self.assertEqual((self.output/"attempt01.part").read_bytes(),prefix)
                attempt=result["attempts"][0]
                self.assertEqual(attempt["response_bytes_observed"],len(prefix))
                self.assertEqual(attempt["body"]["bytes"],len(prefix))
                self.assertEqual(attempt["body"]["sha256"],collect.digest(prefix))
                self.assertEqual(attempt["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])
                self.assertFalse(attempt["transient"]);self.assertEqual(result["http_attempts"],1)

    def test_incomplete_read_consumed_partial_survives_expired_deadlines(self):
        partial=b"consumed-incomplete-prefix"
        for elapsed,code in ((31,"http_attempt_timeout"),(301,"runtime_limit")):
            with self.subTest(elapsed=elapsed):
                if self.output.exists():shutil.rmtree(self.output)
                tick=[0]
                class LateIncomplete(Response):
                    def read(inner,size):
                        tick[0]=elapsed;raise http.client.IncompleteRead(partial,10)
                result=self.blocked(code,http=lambda _:LateIncomplete(headers={}),clock=lambda:tick[0])
                self.assertEqual((self.output/"attempt01.part").read_bytes(),partial)
                self.assertEqual(result["attempts"][0]["response_bytes_observed"],len(partial))
                self.assertEqual(result["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])
                self.assertEqual(result["http_attempts"],1)

    def test_absolute_attempt_deadline_accumulates_across_reads(self):
        tick=[0]
        class SlowChunks(Response):
            def read(inner,size):
                tick[0]+=16
                return super().read(size)
        result=self.blocked("http_attempt_timeout",http=lambda _:SlowChunks(headers={},reads=[BINARY[:10],BINARY[10:]]),clock=lambda:tick[0])
        self.assertEqual((self.output/"attempt01.part").read_bytes(),BINARY)
        self.assertEqual(result["attempts"][0]["response_bytes_observed"],len(BINARY))
        self.assertEqual(result["http_attempts"],1)

    def test_hard_attempt_alarm_interrupts_blocking_open_read_close_and_chained_cleanup(self):
        for phase in ("open","read","close","read_then_close"):
            with self.subTest(phase=phase):
                if self.output.exists():shutil.rmtree(self.output)
                calls=[];reads=[];prefix=b"already-returned-prefix"
                class Blocking(Response):
                    def read(inner,size):
                        if phase=="read_then_close" and not reads:
                            reads.append(True);return prefix
                        if phase in ("read","read_then_close"):time.sleep(1)
                        return super().read(size)
                    def close(inner):
                        if phase in ("close","read_then_close"):time.sleep(1)
                def http(timeout):
                    calls.append(timeout)
                    if phase=="open":time.sleep(1)
                    return Blocking(headers={})
                with patch.dict(collect.CAPS,http_timeout_seconds=0.04):
                    self.refresh();began=time.monotonic();result=self.blocked("http_attempt_timeout",http=http)
                    elapsed=time.monotonic()-began
                self.assertLess(elapsed,0.35,(phase,elapsed))
                self.assertEqual(len(calls),1);self.assertEqual(result["http_attempts"],1)
                self.assertEqual(result["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])
                expected=prefix if phase=="read_then_close" else BINARY if phase=="close" else b""
                self.assertEqual((self.output/"attempt01.part").read_bytes(),expected)
                self.assertEqual(result["attempts"][0]["response_bytes_observed"],len(expected))
                self.assertFalse(result["attempts"][0]["transient"])
                if phase=="read_then_close":self.assertFalse(result["attempts"][0]["response_close_attempted"])
                if phase=="close":self.assertTrue(result["attempts"][0]["response_close_attempted"])
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))

    def test_hard_normal_attempt_alarm_includes_fsync_and_body_hash_seal(self):
        for phase in ("fsync","seal"):
            with self.subTest(phase=phase):
                if self.output.exists():shutil.rmtree(self.output)
                closed=[False];blocked_once=[False]
                original_fsync,original_seal=collect.os.fsync,collect.Artifacts.seal
                class Closed(Response):
                    def close(inner):closed[0]=True
                def fsync(fd):
                    if phase=="fsync" and closed[0] and not blocked_once[0]:
                        blocked_once[0]=True;time.sleep(1)
                    return original_fsync(fd)
                def seal(writer,name):
                    if phase=="seal" and name=="attempt01.part" and not blocked_once[0]:
                        blocked_once[0]=True;time.sleep(1)
                    return original_seal(writer,name)
                with patch.dict(collect.CAPS,http_timeout_seconds=0.04):
                    self.refresh();began=time.monotonic()
                    with patch.object(collect.os,"fsync",fsync),patch.object(collect.Artifacts,"seal",seal):
                        result=self.blocked("http_attempt_timeout",http=lambda _:Closed())
                    elapsed=time.monotonic()-began
                self.assertTrue(blocked_once[0]);self.assertLess(elapsed,0.35,(phase,elapsed))
                self.assertEqual((self.output/"attempt01.part").read_bytes(),BINARY)
                attempt=result["attempts"][0]
                self.assertEqual(attempt["body"]["sha256"],collect.digest(BINARY))
                self.assertEqual(attempt["response_bytes_observed"],len(BINARY))
                self.assertEqual(attempt["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])
                self.assertEqual(result["http_attempts"],1);self.assertFalse(attempt["transient"])
                self.assertTrue(attempt["response_close_completed"])
                self.assertTrue((self.output/"FAILED.json").exists())
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))

    def test_hard_alarm_is_armed_before_attempt_prefix_creation(self):
        original=collect.Artifacts.create
        def create(writer,name):
            if name=="attempt01.part":time.sleep(1)
            return original(writer,name)
        with patch.dict(collect.CAPS,http_timeout_seconds=0.04):
            self.refresh();began=time.monotonic()
            with patch.object(collect.Artifacts,"create",create):result=self.blocked()
            elapsed=time.monotonic()-began
        self.assertLess(elapsed,0.35);self.assertEqual(self.calls,[])
        self.assertEqual(result["http_attempts"],1)
        self.assertEqual(result["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])
        self.assertFalse(result["attempts"][0]["body_integrity_verified"])
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))

    def test_nested_outer_timer_is_restored_with_elapsed_debited_on_all_exits(self):
        for phase in ("success","timeout","interrupt"):
            with self.subTest(phase=phase):
                if self.output.exists():shutil.rmtree(self.output)
                prior_handler=signal.getsignal(signal.SIGALRM)
                prior_timer=signal.getitimer(signal.ITIMER_REAL);saved_at=time.monotonic()
                def outer(*_):raise AssertionError("fixture outer deadline unexpectedly consumed")
                class Blocking(Response):
                    def read(inner,size):time.sleep(1);return b""
                def http(timeout):
                    if phase=="interrupt":time.sleep(0.01);raise KeyboardInterrupt()
                    if phase=="success":time.sleep(0.01)
                    return Blocking(headers={}) if phase=="timeout" else Response()
                try:
                    signal.signal(signal.SIGALRM,outer);signal.setitimer(signal.ITIMER_REAL,1.5)
                    began=time.monotonic()
                    with patch.dict(collect.CAPS,http_timeout_seconds=0.04):
                        self.refresh()
                        if phase=="interrupt":
                            with self.assertRaises(KeyboardInterrupt):self.execute(http=http)
                        elif phase=="timeout":self.blocked("http_attempt_timeout",http=http)
                        else:self.assertEqual(self.execute(http=http)["status"],"authenticated_solc_binary_not_executed")
                    elapsed=time.monotonic()-began;remaining,interval=signal.getitimer(signal.ITIMER_REAL)
                    self.assertIs(signal.getsignal(signal.SIGALRM),outer);self.assertEqual(interval,0)
                    self.assertGreater(remaining,0);self.assertLessEqual(remaining,1.5-elapsed+0.005)
                    self.assertGreater(remaining,1.5-elapsed-0.02)
                finally:
                    signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,prior_handler)
                    if prior_timer[0]:signal.setitimer(signal.ITIMER_REAL,max(0.000001,prior_timer[0]-(time.monotonic()-saved_at)),prior_timer[1])

    def test_whole_stage_remaining_takes_precedence_over_attempt_alarm(self):
        class Writer:
            now=staticmethod(time.monotonic)
            remaining=staticmethod(lambda:0.02)
        began=time.monotonic()
        with self.assertRaisesRegex(collect.Blocked,"^runtime_limit$"):
            with collect.attempt_alarm(Writer(),time.monotonic()+1):time.sleep(1)
        self.assertLess(time.monotonic()-began,0.25)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))

    def test_deadline_prefix_retention_does_not_relax_byte_caps(self):
        tick=[0];prefix=b"123456789"
        class LatePrefix(Response):
            def read(inner,size):tick[0]=301;return prefix
        with patch.dict(collect.CAPS,body_bytes_per_attempt=8,response_bytes_per_attempt_including_detector=9,total_response_bytes_charged=18):
            self.refresh();result=self.blocked("runtime_limit",http=lambda _:LatePrefix(headers={}),clock=lambda:tick[0])
        self.assertEqual((self.output/"attempt01.part").read_bytes(),b"12345678")
        self.assertEqual(result["attempts"][0]["response_bytes_observed"],9)
        self.assertEqual(result["response_bytes_charged"],9)

    def test_deadline_prefix_retention_does_not_relax_disk_floor(self):
        tick=[0];prefix=b"consumed-but-disk-unavailable"
        case=self
        class LatePrefix(Response):
            def read(inner,size):
                tick[0]=301;case.disk.return_value=shutil._ntuple_diskusage(10**12,10**12,0)
                return prefix
        result=self.blocked("free_space_floor",http=lambda _:LatePrefix(headers={}),clock=lambda:tick[0])
        self.assertEqual((self.output/"attempt01.part").read_bytes(),b"")
        self.assertEqual(result["attempts"][0]["response_bytes_observed"],len(prefix))
        self.assertEqual(result["response_bytes_charged"],collect.CAPS["response_bytes_per_attempt_including_detector"])

    def test_late_input_raw_source_binary_and_hierarchy_mutation_block(self):
        original_json,original_fsync=collect.Artifacts.json,collect.os.fsync
        for phase in ("result","pending"):
            for target in ("admission","raw","source","binary","newfile","drain","journal","failure"):
                with self.subTest(phase=phase,target=target):
                    if self.output.exists():shutil.rmtree(self.output)
                    self.snapshot.return_value=copy.deepcopy(SOURCE);self.refresh();changed=[False]
                    extra=self.old_roots[0]/"extra"
                    def mutate():
                        if changed[0]:return
                        changed[0]=True
                        if target=="source":self.snapshot.return_value={"head":"f"*40};return
                        if target=="newfile":extra.write_bytes(b"extra");return
                        path={"admission":self.admission,"raw":Path(self.full_raw["path"]),"binary":self.output/"attempt01.part",
                              "drain":Path(collect.TERMINAL_PINS["drain"]["path"]),
                              "journal":self.full/"attempts.jsonl","failure":self.full/"FAILURE.json"}[target]
                        raw=path.read_bytes();path.write_bytes(raw[:-1]+(b" " if raw[-1:]!=b" " else b"\n"))
                    def written(writer,name,value,**kwargs):
                        result=original_json(writer,name,value,**kwargs)
                        if phase=="result" and name=="RESULT.json":mutate()
                        return result
                    def fsync(fd):
                        result=original_fsync(fd)
                        if phase=="pending" and (self.output/".COMPLETE.json.pending").exists():mutate()
                        return result
                    with patch.object(collect.Artifacts,"json",written),patch.object(collect.os,"fsync",fsync):self.blocked()
                    self.assertTrue(changed[0])
                    if extra.exists():extra.unlink()
                    if target=="raw":Path(self.full_raw["path"]).write_bytes(b"finished metadata response");self.full_raw=ref(Path(self.full_raw["path"]))

    def test_complete_fsync_directory_rename_faults_withhold_marker(self):
        for mode in ("file","directory","rename"):
            if self.output.exists():shutil.rmtree(self.output)
            original_fsync,original_commit=collect.os.fsync,collect.atomic_commit
            def fsync(fd):
                if (self.output/".COMPLETE.json.pending").exists() and mode in {"file","directory"}:
                    if (mode=="directory")==stat.S_ISDIR(os.fstat(fd).st_mode):raise OSError("fault")
                return original_fsync(fd)
            def commit(pending,target):
                if mode=="rename" and target.name=="COMPLETE.json":raise OSError("fault")
                return original_commit(pending,target)
            with patch.object(collect.os,"fsync",fsync),patch.object(collect,"atomic_commit",commit):self.blocked()

    def test_interrupt_keeps_prefix_and_never_executes(self):
        with self.assertRaises(KeyboardInterrupt):self.execute(http=lambda _:Response(headers={},reads=[b"prefix",KeyboardInterrupt()]))
        self.assertEqual((self.output/"attempt01.part").read_bytes(),b"prefix");self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_guard_precedes_reads_and_no_url_sha_or_cap_override(self):
        args=["--admission","/missing","--admission-sha256","f"*64,"--expected-head",HEAD,"--output-dir","/missing-output"]
        with patch.object(collect,"require_production_host",side_effect=RuntimeError("guard")),patch.object(collect,"run_collection",side_effect=AssertionError("input read")):
            with self.assertRaisesRegex(RuntimeError,"guard"):collect.main(args)
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):collect.main(args+["--url","https://other.invalid"])

    def test_owned_json_reopens_are_charged_within_same_control_read_ceiling(self):
        original=collect.Controls.charge_owned;charges=[];instances=[]
        def charge(reader,amount):
            before=reader.read_bytes;original(reader,amount)
            self.assertEqual(reader.read_bytes-before,amount);charges.append(amount);instances.append(reader)
        with patch.object(collect.Controls,"charge_owned",charge):result=self.execute()
        self.assertEqual(result["status"],"authenticated_solc_binary_not_executed",result)
        self.assertGreater(len(charges),5);self.assertLessEqual(instances[-1].read_bytes,collect.CAPS["all_small_control_read_bytes"])
        self.assertEqual(result["reserved_owned_control_reopens"],3*1024**2)

    def test_late_owned_result_and_pending_marker_mutations_block(self):
        original_json,original_fsync=collect.Artifacts.json,collect.os.fsync
        for target in ("result","marker"):
            if self.output.exists():shutil.rmtree(self.output)
            changed=[False]
            def mutate(path):
                if changed[0]:return
                changed[0]=True;raw=path.read_bytes();path.write_bytes(raw[:-1]+b" ")
            def written(writer,name,value,**kwargs):
                result=original_json(writer,name,value,**kwargs)
                if target=="result" and name=="RESULT.json":mutate(self.output/name)
                return result
            def fsync(fd):
                result=original_fsync(fd)
                if target=="marker" and (self.output/".COMPLETE.json.pending").exists():mutate(self.output/".COMPLETE.json.pending")
                return result
            with patch.object(collect.Artifacts,"json",written),patch.object(collect.os,"fsync",fsync):self.blocked()
            self.assertTrue(changed[0])

    def test_existing_output_and_strict_json_are_fail_closed(self):
        self.output.mkdir();(self.output/"user").write_text("keep")
        self.blocked("output_not_new_literal_directory");self.assertEqual((self.output/"user").read_text(),"keep")
        for raw in (b'{"x":1,"x":2}',b'{"x":NaN}',b'{"x":1e9999}'):
            with self.assertRaises(collect.Blocked):collect.decode(raw)

    def test_response_reservation_cannot_overrun_all_artifact_ceiling(self):
        with patch.dict(collect.CAPS,all_new_artifact_bytes=32*1024**2):
            self.refresh();self.blocked("artifact_byte_limit")
        self.assertEqual(self.calls,[])

    def test_source_files_are_bound_to_committed_blobs(self):
        state={"status":"inspected","head":HEAD,"tracked_worktree_clean":True};stale=[False]
        def git(argv,**kwargs):
            raw=(collect.REPO/argv[-1].split(":",1)[1]).read_bytes()
            if argv[2]=="-s":return str(len(raw)).encode()
            return raw[:-1]+b" " if stale[0] else raw
        with patch.object(collect.shared,"code_state",return_value=state),patch.object(collect.subprocess,"check_output",side_effect=git):
            self.assertEqual(set(SOURCE_SNAPSHOT(HEAD)["files"]),set(collect.SOURCE_FILES));stale[0]=True
            with self.assertRaisesRegex(collect.Blocked,"source_blob_differs"):SOURCE_SNAPSHOT(HEAD)

    def test_complete_is_last_fallible_artifact_io(self):
        original_commit,original_open=collect.atomic_commit,Path.open;done=[False]
        def commit(pending,target):
            result=original_commit(pending,target)
            if target.name=="COMPLETE.json":done[0]=True
            return result
        def opened(path,*args,**kwargs):
            if done[0]:raise AssertionError("artifact read after COMPLETE")
            return original_open(path,*args,**kwargs)
        with patch.object(collect,"atomic_commit",commit),patch.object(Path,"open",opened):result=self.execute()
        self.assertEqual(result["status"],"authenticated_solc_binary_not_executed",result)


if __name__=="__main__":unittest.main()
