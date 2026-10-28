#!/usr/bin/env python3
"""Paced, source-versioned continuation of the unchanged exact-header inventory.

Numeric per-item 429 is a bounded retry policy, not proof of provider quota or
failure cause. Old failed runs and all charges remain immutable. No unsealed
headers are adopted, and raw clocks do not admit the final research sample.
"""
from __future__ import annotations

import argparse
from datetime import datetime,timezone
import math
from pathlib import Path
import subprocess
import sys
import time

REPO=Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:sys.path.insert(0,str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_exact_header_campaign_v4 as previous

legacy,scale,pilot,inventory=previous.legacy,previous.scale,previous.pilot,previous.inventory
CAPS=previous.CAPS
SOURCE_FILES=(*previous.SOURCE_FILES,"scripts/collect_polymarket_exact_header_campaign_v5.py",
    "tests/test_polymarket_exact_header_campaign_v5.py")
SCHEMA="polymarket_exact_header_campaign_admission_v5"
SUCCESS,RUN_ROOT=previous.SUCCESS,previous.RUN_ROOT
MAX_JSON,MAX_LEDGER,FINAL_RESERVE=previous.MAX_JSON,previous.MAX_LEDGER,previous.FINAL_RESERVE
Blocked,Deadline,require=previous.Blocked,previous.Deadline,previous.require
encode,decode,digest=previous.encode,previous.decode,previous.digest
identity,bound,reference,read_json,ref=previous.identity,previous.bound,previous.reference,previous.read_json,previous.ref
Budget,owner_identity=previous.Budget,previous.owner_identity
key_chunks,read_headers,verify_chunk,journal_usage=previous.key_chunks,previous.read_headers,previous.verify_chunk,previous.journal_usage
request_deadline,http_response,process_limits=previous.request_deadline,previous.http_response,previous.process_limits
diagnostic=previous.diagnostic
TRANSIENT_RPC_CODES=previous.TRANSIENT_RPC_CODES|{429}
RETRY_DELAYS=previous.RETRY_DELAYS
RATE_LIMIT_DELAYS=(2.0,4.0)
METHOD_ITEMS_PER_SECOND=150.0
FORECAST_RESERVE_SECONDS=3600.0
V4_HEAD="d20f23a0b8ab71a4107bd3535d37b6afaae6e0e1"
V4_ROOT=RUN_ROOT/"2026-10-27_polymarket_exact_header_campaign_v4"
OP_ROOT=RUN_ROOT/"2026-10-27_polymarket_oct1_finalization_v1"
V4_REFS={
    "owner":{"path":str(V4_ROOT/"OWNER.json"),"bytes":6654,"sha256":"9947b185e98f58c17336b51679f546b07ccaa6b2311d177d2790df051ea0b851"},
    "failed":{"path":str(V4_ROOT/"FAILED.json"),"bytes":576,"sha256":"00f67a27a8a6495a00da74c20034880be904f30523d470cf59bad17626c362bd"},
    "root_journal":{"path":str(V4_ROOT/"rpc.jsonl"),"bytes":356,"sha256":"c1f989a41a007ed8b9c15af1ccb37830c9cc66f0c8a5e527f9374f2201e0c986"},
    "chunk_journal":{"path":str(V4_ROOT/"chunks/c000000/a02/rpc.jsonl"),"bytes":41633,"sha256":"5c82392650db81c124920b8736a8fc515d7229413c8ed958194c0aa3095b3805"},
    "diagnostic":{"path":str(V4_ROOT/"chunks/c000000/a02/error_r000000318.json"),"bytes":426,"sha256":"754b96e3992fb21228818c660fad7134063363757120a821d52a11be4af40933"},
    "admission":{"path":str(OP_ROOT/"full_header_v4_admission_v3.json"),"bytes":138467,"sha256":"f5bef298c5d0b918bdac912728f1f7fb027958da2008240f732113d110ec3ab2"},
    "stdout":{"path":str(OP_ROOT/"full_header_v4.log"),"bytes":576,"sha256":"053f5f0cac9db3b443453820439c6b9dd475d8874e4c656776b3edfe69d03d7b"},
    "exit":{"path":str(OP_ROOT/"full_header_v4.EXIT.json"),"bytes":2813,"sha256":"af1fd00735bf78a293529b7bd94f86cff40011230aa441b4adeb7adbfc65ecf7"},
    "launch":{"path":str(OP_ROOT/"full_header_v4.LAUNCH.json"),"bytes":2371,"sha256":"baec4dbcd3cbc5d2724a6ef7fb324b5a6b1d000f1dbc7eb1d8cc1150ca5bd322"},
    "root_drain":{"path":str(OP_ROOT/"full_header_v4_root_drain_v1.json"),"bytes":1323,"sha256":"d5aed27c83ccde78455f7bb35791ff3de4beb56e5516173bda3a29eef8dc243a"}}
V4_INHERITED_RPC={"http_attempts":220,"method_items":12309,"response_bytes":101435357,"elapsed_seconds":13200}
V4_CUMULATIVE_RPC={"http_attempts":318,"method_items":22010,"response_bytes":174515481,"elapsed_seconds":14400}
V4_NEW_RPC={"http_attempts":98,"method_items":9701,"response_bytes":73080124}


def source_snapshot(head):
    source=previous.source_snapshot(head);files=dict(source["files"]);own={}
    for name in SOURCE_FILES[len(previous.SOURCE_FILES):]:
        actual=bound(REPO/name,MAX_JSON);raw=scale.git_blob(head,name)
        require(len(raw)==actual["bytes"] and digest(raw)==actual["sha256"],"source_blob_differs")
        own[name]=actual
        files[name]={key:actual[key] for key in ("bytes","sha256")}
    require(previous.source_snapshot(head)==source,"source_changed_during_snapshot")
    for name,capture in own.items():
        require(bound(REPO/name,MAX_JSON)==capture,"source_reopen_changed")
    return {"head":head,"files":files}


def previous_source(source):
    return {"head":source["head"],"files":{name:source["files"][name] for name in previous.SOURCE_FILES}}


def validate_inputs(value,source,actual_keys):
    return previous.validate_inputs(value,previous_source(source),actual_keys)


def validate_reboot(certificate_ref,prior_value,inputs,private_ref):
    return previous.validate_reboot(certificate_ref,prior_value,inputs,private_ref)


def forecast_seconds(keys,batch_size,observed_max):
    require(type(keys) is int and 0<=keys<=CAPS["keys"] and type(batch_size) is int and
        0<batch_size<=CAPS["initial_batch_size"],"forecast_key_or_batch_invalid")
    legacy.elapsed(observed_max,CAPS["request_seconds"])
    return math.ceil(keys/batch_size)*max(batch_size/METHOD_ITEMS_PER_SECOND,observed_max)+FORECAST_RESERVE_SECONDS


def wait_for_pace(budget):
    delay=max(0,budget.next_dispatch_at-time.monotonic())
    if delay:
        require(budget.remaining()>delay,"runtime_limit")
        if budget.chunk is not None:
            require(budget.chunk_elapsed()+delay<CAPS["chunk_runtime_seconds"],"chunk_runtime_limit")
        time.sleep(delay);budget.check()


def paced_start(budget,items):
    """Mark the actual dispatch, after any source checks and journal fsync."""
    now=time.monotonic()
    budget.next_dispatch_at=max(now,budget.next_dispatch_at)+items/METHOD_ITEMS_PER_SECOND


def validate_prior(value,output,source,inputs,reboot):
    require(type(value) is dict and set(value)=={"complete_roster","baseline_cache_excluded","roots",
        "standalone_control_files","original_pilot_input","files","rpc","campaigns","observations",
        "v4_charge_attestation"},"prior_v5_ledger_schema")
    require(value["roots"].count(str(V4_ROOT))==1,"v4_failed_root_not_exact")
    files={item["path"]:item for item in value["files"]}
    require(len(files)==len(value["files"]),"prior_file_duplicate_or_missing")
    captures={}
    owned={item["path"] for key,item in V4_REFS.items() if key in
        {"owner","failed","root_journal","chunk_journal","diagnostic"}}
    actual=set()
    for path in V4_ROOT.rglob("*"):
        require(not path.is_symlink() and (path.is_dir() or path.is_file()),"v4_failed_tree_invalid")
        if path.is_file():actual.add(str(path))
        require(len(actual)<=5,"v4_failed_tree_extra")
    require(actual==owned,"v4_failed_tree_not_exact")
    for name,expected in V4_REFS.items():
        require(expected["path"] in files and ref(files[expected["path"]])==expected and
            files[expected["path"]]["category"]=="control" and files[expected["path"]]["chunk"]==
            (0 if name in {"chunk_journal","diagnostic"} else None),"v4_artifact_not_rostered")
        if name not in {"owner","failed","root_journal","chunk_journal","diagnostic"}:
            require(value["standalone_control_files"].count(expected["path"])==1,"v4_control_not_exact_once")
        captures[name]=reference(expected)
    owner=read_json(V4_REFS["owner"])[0];failed=read_json(V4_REFS["failed"])[0]
    old_admission=read_json(V4_REFS["admission"],MAX_LEDGER)[0]
    require(owner.get("schema_version")=="polymarket_exact_header_owner_v4" and owner.get("run_dir")==str(V4_ROOT)
        and owner.get("caps")==CAPS and owner.get("inputs")==inputs and owner.get("raw_clocks_only") is True
        and owner.get("final_sample_admitted") is False and ref(owner.get("admission",{}))==V4_REFS["admission"]
        and owner.get("source",{}).get("head")==V4_HEAD,"v4_owner_or_inputs_differs")
    require(legacy.owner_state(owner.get("process",{}))=="dead","v4_owner_live_or_unknown")
    legacy.producer_source(owner["source"],previous_source(source),previous.SOURCE_FILES)
    require(old_admission.get("schema_version")==previous.SCHEMA and old_admission.get("consumer_head")==V4_HEAD
        and old_admission.get("run_dir")==str(V4_ROOT) and old_admission.get("caps")==CAPS and
        old_admission.get("inputs")=={name:ref(item) for name,item in inputs.items()} and
        old_admission.get("prior",{}).get("rpc")==V4_INHERITED_RPC,"v4_admission_differs")
    require(failed.get("schema_version")=="polymarket_exact_header_campaign_v4" and failed.get("status")=="blocked"
        and failed.get("error_code")=="rpc_item_error_not_retryable" and failed.get("accepted_keys")==0
        and failed.get("raw_clocks_only") is True and failed.get("final_sample_admitted") is False,
        "v4_failure_status_differs")
    usage=failed.get("usage",{})
    require(usage.get("prior_rpc")==V4_INHERITED_RPC and all(usage.get(key)==V4_CUMULATIVE_RPC[key]
        for key in V4_NEW_RPC),"v4_failure_counters_differs")
    charged=V4_REFS["admission"]["bytes"]+sum(files[path]["bytes"] for path in owned if path!=V4_REFS["failed"]["path"])
    require(usage.get("new_retained_bytes")=={"baseline":0,"accepted":0,"failed":0,"control":charged},
        "v4_failure_retained_charge_differs")
    printed=read_json(V4_REFS["stdout"])[0];expected=decode(encode(failed))
    expected["usage"]["new_retained_bytes"]["control"]+=V4_REFS["failed"]["bytes"]
    require(encode(printed)==encode(expected),"v4_stdout_postwrite_alias_differs")
    root_usage,root_starts,root_finishes=journal_usage(Path(V4_REFS["root_journal"]["path"]))
    keys=legacy.key_chunk_at(inputs["keys"]["path"],0)
    chunk_usage,starts,finishes=journal_usage(Path(V4_REFS["chunk_journal"]["path"]),(0,keys))
    require(len(root_starts)==len(root_finishes)==1 and len(starts)==len(finishes)==97 and
        all(root_usage[key]+chunk_usage[key]==V4_NEW_RPC[key] for key in V4_NEW_RPC),"v4_durable_usage_differs")
    last=max(starts);finish=finishes[last]
    require(last==V4_CUMULATIVE_RPC["http_attempts"] and starts[last]["k"]==9600 and starts[last]["n"]==100 and
        sum(item["r"]=="validated" for item in finishes.values())==96 and finish["r"]=="failed" and
        finish["p"] is None and finish["s"]==200 and finish["b"]==finish["z"] and finish["h"]==finish["x"],
        "v4_last_error_journal_differs")
    error=read_json(V4_REFS["diagnostic"])[0]
    require(error=={"schema_version":"polymarket_exact_header_error_v4","call":last,
        "reason":"rpc_item_error_not_retryable","http_status":200,"error_code_counts":{"429":12},
        "observed_body_bytes":finish["z"],"observed_body_sha256":finish["x"],
        "complete_body_sha256":finish["h"],"charged_response_bytes":finish["b"],"provider_text_retained":False},
        "v4_error_diagnostic_differs")
    observed_max=max((finishes[call]["t"]-item["t"])/1000 for call,item in starts.items())
    exit_value=read_json(V4_REFS["exit"])[0];launch=read_json(V4_REFS["launch"])[0]
    require(exit_value.get("schema_version")=="polymarket_oct1_stage_launcher_v1" and
        exit_value.get("stage")=="headers" and exit_value.get("output_dir")==str(V4_ROOT) and
        exit_value.get("producer_exit_code")==1 and exit_value.get("owned_group_drained") is True and
        exit_value.get("ownership_unknown") is False and exit_value.get("children_still_running") is False and
        exit_value.get("source_and_admission_unchanged") is True and exit_value.get("stdout_eof_observed") is True and
        exit_value.get("stdout_truncated") is False and exit_value.get("stdout_sha256")==V4_REFS["stdout"]["sha256"] and
        exit_value.get("stdout_bytes_observed")==V4_REFS["stdout"]["bytes"] and
        ref(exit_value.get("admission",{}))==V4_REFS["admission"] and
        exit_value.get("child",{}).get("pid")==owner["process"]["pid"] and
        launch.get("child")==exit_value.get("child") and launch.get("caller")==exit_value.get("caller") and
        launch.get("command")==exit_value.get("command"),"v4_launcher_drain_differs")
    require(all(legacy.owner_state({key:exit_value[name][key] for key in
        ("hostname","boot_id","pid","birth_ticks")})=="dead" for name in ("caller","child")),
        "v4_caller_or_child_live_or_unknown")
    drain=read_json(V4_REFS["root_drain"])[0]
    require(drain.get("schema_version")=="polymarket_header_v4_root_drain_v1" and drain.get("source_head")==V4_HEAD
        and drain.get("header_owned_processes_drained") is True and drain.get("remaining_owned_group_members")==[] and
        drain.get("final_sample_admitted") is False and drain.get("process_receipts_checked")==0 and
        drain.get("references")==[V4_REFS[name] for name in ("failed","exit","diagnostic")] and
        drain.get("owners")==[{"current":None,"owner":exit_value[name]} for name in ("caller","child")],
        "v4_root_drain_differs")
    charge,charge_binding=read_json(value["v4_charge_attestation"])
    require(set(charge)=={"schema_version","status","source_head","run_dir","refs",
        "global_new_runtime_seconds","chunk_runtime_seconds","inherited_rpc",
        "cumulative_rpc","raw_clocks_only","final_sample_admitted","checked_utc",
        "observed_caller_elapsed_seconds","observed_chunk_journal_span_seconds"} and
        charge["schema_version"]=="polymarket_header_v4_charge_attestation_v1" and
        charge["status"]=="root_verified_v4_failure_charges" and charge["source_head"]==V4_HEAD and
        charge["run_dir"]==str(V4_ROOT) and charge["refs"]=={name:V4_REFS[name] for name in
            ("owner","failed","root_drain","exit","root_journal","chunk_journal")} and
        charge["global_new_runtime_seconds"]==1200 and charge["chunk_runtime_seconds"]=={"0":120} and
        charge["inherited_rpc"]==V4_INHERITED_RPC and charge["cumulative_rpc"]==V4_CUMULATIVE_RPC and
        charge["raw_clocks_only"] is True and charge["final_sample_admitted"] is False,"v4_charge_attestation_differs")
    require(type(charge["checked_utc"]) is str and len(charge["checked_utc"])<=64 and
        datetime.fromisoformat(charge["checked_utc"]).utcoffset()==timezone.utc.utcoffset(None) and
        type(charge["observed_caller_elapsed_seconds"]) in (int,float) and
        charge["observed_caller_elapsed_seconds"]==exit_value.get("elapsed_seconds") and
        type(charge["observed_chunk_journal_span_seconds"]) in (int,float) and
        charge["observed_chunk_journal_span_seconds"]==
        (max(item["t"] for item in finishes.values())-min(item["t"] for item in starts.values()))/1000,
        "v4_charge_observation_differs")
    require(legacy.elapsed(exit_value.get("elapsed_seconds"))+legacy.elapsed(drain.get("elapsed_seconds"))<=1200 and
        legacy.elapsed(usage.get("elapsed_seconds"))-V4_INHERITED_RPC["elapsed_seconds"]<=1200 and
        max(item["t"] for item in finishes.values())-min(item["t"] for item in starts.values())<=120000,
        "v4_runtime_attestation_understated")
    require(charge_binding["path"] in files and ref(files[charge_binding["path"]])==ref(charge_binding) and
        files[charge_binding["path"]]["category"]=="control" and files[charge_binding["path"]]["chunk"] is None and
        value["standalone_control_files"].count(charge_binding["path"])==1,"v4_charge_not_rostered")
    removed={item["path"] for item in V4_REFS.values()}|{charge_binding["path"]}
    base={key:part for key,part in value.items() if key!="v4_charge_attestation"}
    base["roots"]=[path for path in value["roots"] if path!=str(V4_ROOT)]
    base["files"]=[item for item in value["files"] if item["path"] not in removed]
    base["standalone_control_files"]=[path for path in value["standalone_control_files"] if path not in removed]
    base["rpc"]=dict(V4_INHERITED_RPC)
    prior=previous.validate_prior(base,output,previous_source(source),inputs,reboot)
    for key,minimum in V4_CUMULATIVE_RPC.items():
        actual_rpc=value["rpc"][key]
        require(type(actual_rpc) is int if key!="elapsed_seconds" else type(actual_rpc) in (int,float),
            "prior_v5_rpc_schema")
        require(math.isfinite(actual_rpc) and minimum<=actual_rpc<=CAPS[key if key!="elapsed_seconds" else "runtime_seconds"],
            "prior_v5_rpc_understated")
    prior["rpc"]=dict(value["rpc"])
    for path in removed:
        item=files[path];capture=reference(ref(item),MAX_LEDGER)
        prior["files"][path]=capture|{"category":"control","chunk":item["chunk"]}
        prior["bytes"]["control"]+=item["bytes"]
    require(all(prior["bytes"][key]<=CAPS[key+"_bytes"] for key in prior["bytes"]) and
        sum(prior["bytes"].values())<=CAPS["global_header_bytes"],"prior_disk_allocation")
    prior["attempts"].setdefault(0,set()).add(str(V4_ROOT/"chunks/c000000/a02"))
    aggregate=prior["chunk_usage"].setdefault(0,dict.fromkeys((*V4_NEW_RPC,"elapsed_seconds"),0))
    for key,count in chunk_usage.items():aggregate[key]+=count
    aggregate["elapsed_seconds"]+=120
    require(len(prior["attempts"][0])<CAPS["attempts_per_chunk"] and all(aggregate[key]<=CAPS["chunk_"+key]
        for key in V4_NEW_RPC) and
        aggregate["elapsed_seconds"]<CAPS["chunk_runtime_seconds"] and
        sum(item["bytes"] for item in prior["files"].values() if item["chunk"]==0)<=CAPS["chunk_artifact_bytes"],
        "v4_prior_chunk_cap")
    prior["observed_max_call_seconds"]=observed_max
    return prior


class RPC(previous.RPC):
    def call(self, keys=None, offset=0, *, terminal=False):
        b = self.budget; n = 0 if keys is None else len(keys)
        for attempt in range(CAPS["attempts_per_batch"]):
            b.check();wait_for_pace(b)
            if hasattr(b,"before_request"): b.before_request()
            for key,extra in (("http_attempts",1),("method_items",max(1,n))):
                require(b.rpc[key]+extra+(0 if terminal else 1) <= CAPS[key],key+"_limit")
                if b.chunk is not None:
                    require(b.chunk_base[key]+b.rpc[key]-b.chunk_start_rpc[key]+extra <= CAPS["chunk_"+key],
                        "chunk_"+key+"_limit")
            allowance = min(CAPS["individual_response_bytes"],CAPS["response_bytes"]-b.rpc["response_bytes"])
            if b.chunk is not None:
                allowance = min(allowance,CAPS["chunk_response_bytes"]-b.chunk_base["response_bytes"]
                    -b.rpc["response_bytes"]+b.chunk_start_rpc["response_bytes"])
            require(allowance>0,"response_bytes_limit")
            call,first_id = b.rpc["http_attempts"]+1,b.rpc["method_items"]+1
            requests = [{"jsonrpc":"2.0","id":first_id+i,"method":"eth_chainId" if keys is None else "eth_getBlockByNumber",
                "params":[] if keys is None else [hex(key["block_number"]),False]}
                for i,key in enumerate([None] if keys is None else keys)]
            payload = requests[0] if keys is None else requests
            b.append(self.journal,{"e":"s","i":call,"k":-1 if keys is None else self.key_offset+offset,"n":n,
                "j":first_id,"b":allowance,"t":int(time.monotonic()*1000),"h":digest(encode(payload))})
            b.rpc["http_attempts"]+=1; b.rpc["method_items"]+=max(1,n)
            reply = {"raw":b"","charged_bytes":allowance,"complete":False,"status":None,
                "error_code":"transport_failure","proven_oversize":False}
            code,split,retry,rows,codes,projection = None,False,False,None,[],None
            stage = None; interrupted = None; response_digest = None
            try:
                remaining = min(CAPS["request_seconds"],b.remaining(),
                    CAPS["chunk_runtime_seconds"]-b.chunk_elapsed() if b.chunk is not None else b.remaining())
                with request_deadline(remaining,"request_deadline" if remaining == CAPS["request_seconds"] else
                    "chunk_runtime_limit" if b.chunk is not None and CAPS["chunk_runtime_seconds"]-b.chunk_elapsed() <= b.remaining() else "runtime_limit"):
                    paced_start(b,max(1,n))
                    reply = self.transport(self.endpoint,payload,byte_limit=allowance,timeout=remaining)
                    require(type(reply) is dict and type(reply.get("raw")) is bytes and len(reply["raw"]) <= allowance and
                        type(reply.get("charged_bytes")) is int and len(reply["raw"]) <= reply["charged_bytes"] <= allowance and
                        type(reply.get("complete")) is bool and type(reply.get("proven_oversize")) is bool,"transport_contract")
                    if reply["complete"] is False: reply["charged_bytes"] = allowance
                    else: require(reply["charged_bytes"] == len(reply["raw"]),"transport_contract")
                    response_digest = digest(reply["raw"])
                    code = reply.get("error_code")
                    split = (reply["proven_oversize"] is True and code == "response_declared_oversize") or (reply.get("status") == 413 and code is None)
                    require(not split,"proven_batch_size_refusal"); require(code is None,code or "transport_failure")
                    if reply.get("status") == 429 or type(reply.get("status")) is int and 500 <= reply["status"] <= 599:
                        retry=True; raise Blocked("retryable_http_failure")
                    require(reply.get("status") == 200 and reply["complete"] is True,"http_or_body_incomplete")
                    values = decode(reply["raw"])
                    if keys is not None and type(values) is dict and set(values) == {"jsonrpc","id","error"}:
                        err = values["error"]
                        split = (values["jsonrpc"] == "2.0" and values["id"] is None and type(err) is dict and
                            type(err.get("code")) is int and err["code"] in (-32600,-32005) and
                            type(err.get("message")) is str and err["message"].lower() in {
                                "batch requests are not supported","batch size limit exceeded","batch request limit exceeded"})
                        require(not split,"proven_batch_size_refusal")
                    values = [values] if keys is None else values
                    require(type(values) is list and len(values) == len(requests),"rpc_batch_shape")
                    replies = {}
                    for item in values:
                        require(type(item) is dict and set(item) in ({"jsonrpc","id","result"},{"jsonrpc","id","error"}) and
                            item["jsonrpc"] == "2.0" and type(item["id"]) is int and item["id"] not in replies,"rpc_reply_shape")
                        replies[item["id"]] = item
                    require(set(replies) == {item["id"] for item in requests},"rpc_reply_ids")
                    partial = []
                    for req,key in zip(requests,[None] if keys is None else keys):
                        item = replies[req["id"]]
                        if "error" in item:
                            err = item["error"]
                            require(type(err) is dict and {"code","message"} <= set(err) <= {"code","message","data"} and
                                type(err["code"]) is int and -(2**31) <= err["code"] < 2**31 and type(err["message"]) is str,
                                "rpc_error_schema")
                            codes.append(err["code"])
                        elif keys is None: require(pilot.quantity(item["result"]) == 137,"wrong_chain")
                        else: partial.append(pilot.validate_header(item["result"],key))
                    if partial: pilot.validate_clock(partial)
                    if codes:
                        retry = all(code in TRANSIENT_RPC_CODES for code in codes)
                        raise Blocked("retryable_rpc_item_error" if retry else "rpc_item_error_not_retryable")
                    rows = None if keys is None else partial
                    if rows is not None: projection = digest(scale.projection_bytes(rows))
            except Deadline as exc:
                returned = reply.get("raw",b"") if type(reply) is dict else b""
                observed = exc.raw or (returned if type(returned) is bytes and len(returned) <= allowance else b"")
                status = exc.status if exc.status is not None else reply.get("status") if type(reply) is dict else None
                reply = {"raw":observed,"charged_bytes":allowance,"complete":False,"status":status,
                    "error_code":exc.code,"proven_oversize":False}
                code=exc.code; stage = code if code != "request_deadline" else None
            except (KeyboardInterrupt,SystemExit) as exc:
                interrupted=exc; code="interrupted"
                observed=getattr(exc,"raw",b"")
                reply={"raw":observed if type(observed) is bytes and len(observed)<=allowance else b"",
                    "charged_bytes":allowance,"complete":False,"status":getattr(exc,"status",None),"proven_oversize":False}
                response_digest=None
            except (Blocked,pilot.PilotBlocked) as exc: code=str(exc)
            except BaseException: code="transport_failure"
            if not (type(reply) is dict and type(reply.get("raw")) is bytes and len(reply["raw"]) <= allowance and
                    type(reply.get("charged_bytes")) is int and len(reply["raw"]) <= reply["charged_bytes"] <= allowance and
                    type(reply.get("complete")) is bool and type(reply.get("proven_oversize")) is bool):
                observed = reply.get("raw",b"") if type(reply) is dict else b""
                observed = observed[:allowance] if type(observed) is bytes else b""
                reply={"raw":observed,"charged_bytes":allowance,"complete":False,"status":None}
                response_digest = None
                code="transport_contract"
            if reply.get("status") is not None and not (type(reply["status"]) is int and 100 <= reply["status"] <= 599):
                reply.update(status=None,complete=False);code="transport_contract"
            if code == "transport_contract":reply["complete"]=False
            if reply["complete"] is False: reply["charged_bytes"]=allowance
            b.rpc["response_bytes"] += reply["charged_bytes"]
            response_digest = response_digest or digest(reply["raw"])
            finish = {"e":"f","i":call,"b":reply["charged_bytes"],"z":len(reply["raw"]),
                "h":response_digest if reply["complete"] else None,"x":response_digest,
                "p":projection if code is None else None,"r":"validated" if code is None else "split" if split else "failed",
                "s":reply.get("status"),"t":int(time.monotonic()*1000)}
            b.append(self.journal,finish,failure=code is not None)
            if code is None: return rows
            diagnostic(b,self.journal,finish,code,codes)
            if interrupted is not None: raise interrupted
            if stage is not None: raise Blocked(stage)
            if split:
                require(keys is not None and len(keys)>1,"singleton_size_refusal")
                reduced=min(self.batch_size,max(1,len(keys)//2))
                require(forecast_seconds(b.remaining_keys,reduced,b.prior["observed_max_call_seconds"])<b.remaining(),
                    "reduced_batch_runtime_not_fit")
                self.batch_size=reduced;return None
            retry = retry or code in {"transport_failure","request_deadline","response_incomplete","content_length_differs","response_close_failure"}
            require(retry and attempt+1 < CAPS["attempts_per_batch"],code or "rpc_failure")
            delay=(RATE_LIMIT_DELAYS if 429 in codes or reply.get("status")==429 else RETRY_DELAYS)[attempt]
            require(b.remaining()>delay,"runtime_limit")
            if b.chunk is not None:require(b.chunk_elapsed()+delay<CAPS["chunk_runtime_seconds"],"chunk_runtime_limit")
            time.sleep(delay);b.check()
        raise Blocked("batch_attempts_exhausted")


def run_campaign(admission_path, admission_sha, head, output, private, *, transport=http_response, arm_prior=None):
    started = time.monotonic(); output, private = Path(output), Path(private)
    require(output.is_absolute() and output.parent == RUN_ROOT and output.parent.resolve() == output.parent
            and output.parent.is_dir() and not output.exists() and not output.is_symlink(), "destination_not_fresh")
    admission, admission_binding = read_json({"path": str(admission_path), "bytes": identity(admission_path)["bytes"], "sha256": admission_sha}, MAX_LEDGER)
    require(type(admission) is dict and set(admission) == {"schema_version", "status", "caps", "consumer_head", "run_dir",
        "raw_clocks_only", "final_sample_admitted", "inputs", "private_file", "actual_keys", "conservative_new_runtime_seconds", "prior",
        "reboot_equivalence"}
        and admission["schema_version"] == SCHEMA and admission["status"] == "root_exact_header_campaign_admitted"
        and admission["caps"] == CAPS and admission["consumer_head"] == head and admission["run_dir"] == str(output)
        and admission["raw_clocks_only"] is True and admission["final_sample_admitted"] is False, "root_admission_not_exact")
    source = source_snapshot(head)
    # Tighten the outer alarm before large SHA/Parquet admission reads. The
    # subsequent ledger validation independently checks this declared charge.
    declared_runtime = legacy.elapsed(admission["prior"]["rpc"]["elapsed_seconds"])
    require(declared_runtime + time.monotonic() - started < CAPS["runtime_seconds"], "runtime_limit")
    if arm_prior: arm_prior(declared_runtime, started)
    K = legacy.integer(admission["actual_keys"], CAPS["keys"]); require(K > 0, "empty_key_sample")
    inputs, measured = validate_inputs(admission["inputs"], source, K)
    reboot=validate_reboot(admission["reboot_equivalence"],admission["prior"],inputs,admission["private_file"])
    prior = validate_prior(admission["prior"], output, source, inputs,reboot)
    for value in inputs.values():
        require(value["path"] in prior["files"] and ref(prior["files"][value["path"]]) == ref(value), "input_absent_from_global_ledger")
    projection = legacy.elapsed(admission["conservative_new_runtime_seconds"])
    require(projection >= 2 * measured["seconds_per_key"] * K and projection >= prior["observation_elapsed_seconds"]*math.ceil(K/CAPS["initial_batch_size"]) and projection >= forecast_seconds(K,CAPS["initial_batch_size"],prior["observed_max_call_seconds"]) and projection + prior["rpc"]["elapsed_seconds"] + time.monotonic()-started < CAPS["runtime_seconds"], "runtime_feasibility_not_admitted")
    require(max(80, 1.25 * measured["bytes_per_key"]) * K <= CAPS["accepted_bytes"], "header_disk_projection_not_fit")
    private_binding = reference(admission["private_file"])
    require(private_binding["path"] == str(private) and private.stat().st_size <= 16 * 1024, "private_configuration_binding")
    endpoint, configuration = inventory.load_rpc_endpoint(private)
    require(configuration == "private_file", "private_rpc_authority_required")
    require(reboot["private_current"]==pilot.identity(private) and private_binding==reboot["private_binding"],
        "rpc_authority_differs_from_reboot")
    for path in [admission_binding["path"], private_binding["path"], *prior["files"]]:
        require(Path(path) != output and output not in Path(path).parents and Path(path) not in output.parents, "input_output_overlap")
    resources = inventory.resource_state(output.parent)
    require(resources["ram_bytes"]["MemAvailable"] >= CAPS["minimum_available_ram_bytes"], "available_ram_floor")
    require(resources["disk_bytes"]["free"] >= CAPS["minimum_free_bytes"] + CAPS["chunk_artifact_bytes"] + FINAL_RESERVE,
            "disk_floor")
    require(prior["rpc"]["elapsed_seconds"] == declared_runtime, "prior_runtime_changed")
    output.mkdir(mode=0o700)
    b = Budget(output, prior, admission_binding["bytes"], started)
    b.next_dispatch_at=time.monotonic();b.remaining_keys=K
    new_active_seconds,new_accepted_keys=0.0,0
    source_identities = {name: identity(REPO / name) for name in SOURCE_FILES}
    def before_request():
        for value in (admission_binding, private_binding, inputs["keys"],reboot["binding"],reboot["original_reference"]):
            require(identity(Path(value["path"])) == value["identity"], "request_input_changed")
        require(all(identity(REPO / name) == value for name, value in source_identities.items()), "request_source_changed")
    b.before_request = before_request
    completed, last, accepted = {}, None, 0
    def external(full=False):
        require(source_snapshot(head) == source, "source_changed")
        for value in [admission_binding, private_binding, *inputs.values(), *prior["files"].values()]:
            require(identity(Path(value["path"])) == value["identity"], "input_identity_changed")
        require(inventory.load_rpc_endpoint(private) == (endpoint, configuration), "private_authority_changed")
        require(identity(Path(reboot["binding"]["path"]))==reboot["binding"]["identity"] and
            previous.reboot_environment()==reboot["environment"],"reboot_certificate_or_environment_changed")
        if full:
            require(read_json(ref(admission_binding), MAX_LEDGER)[1] == admission_binding, "admission_changed")
            require(reference(admission["private_file"]) == private_binding, "private_authority_changed")
            for value in inputs.values(): require(bound(Path(value["path"]), CAPS["baseline_bytes"]) == value, "input_changed")
            require(validate_reboot(admission["reboot_equivalence"],admission["prior"],inputs,admission["private_file"])==reboot,
                "reboot_certificate_changed")
            require(validate_prior(admission["prior"], output, source, inputs,reboot) == prior, "prior_artifacts_or_roster_changed")
        b.check()
    try:
        b.write("OWNER.json", encode({"schema_version": "polymarket_exact_header_owner_v5", "run_dir": str(output),
            "source": source, "process": owner_identity(), "inputs": inputs, "caps": CAPS, "admission": admission_binding,
            "reboot_equivalence":reboot["binding"],
            "raw_clocks_only": True, "final_sample_admitted": False}))
        external()
        RPC(endpoint, b, transport, "rpc.jsonl").call()
        for chunk, keys in enumerate(key_chunks(inputs["keys"]["path"])):
            b.remaining_keys=K-accepted
            if new_accepted_keys:
                require(max(forecast_seconds(b.remaining_keys,CAPS["initial_batch_size"],prior["observed_max_call_seconds"]),
                    b.remaining_keys*new_active_seconds/new_accepted_keys+FORECAST_RESERVE_SECONDS)<b.remaining(),
                    "remaining_runtime_not_fit")
            offset = chunk * CAPS["keys_per_chunk"]
            if chunk in prior["sealed"]:
                root = Path(prior["sealed"][chunk]["root"])
                rows, header_binding = verify_chunk(root, keys, offset)
            else:
                attempt = len(prior["attempts"].get(chunk, set())) + 1
                require(attempt <= CAPS["attempts_per_chunk"], "chunk_attempts_exhausted")
                b.chunk, b.chunk_started = chunk, time.monotonic()
                b.chunk_base = prior["chunk_usage"].get(chunk, dict.fromkeys(("http_attempts", "method_items", "response_bytes", "elapsed_seconds"), 0))
                b.chunk_start_rpc = dict(b.rpc)
                base = f"chunks/c{chunk:06d}/a{attempt:02d}"
                rows = RPC(endpoint, b, transport, base + "/rpc.jsonl", offset).fetch(keys)
                payload = scale.compact_parquet(rows)
                require(len(payload) <= CAPS["chunk_artifact_bytes"], "chunk_parquet_byte_limit")
                b.check(len(payload), "accepted")  # Accepted allocation must fit before staging.
                headers = b.write(base + "/headers.parquet", payload, "failed")
                require(read_headers(output / headers["path"], keys) == rows, "chunk_header_reopen_differs")
                usage, starts, finishes = journal_usage(output / (base + "/rpc.jsonl"))
                require(len(starts) == len(finishes), "chunk_open_request")
                manifest = {"schema_version": "polymarket_exact_header_chunk_v3", "status": "exact_header_chunk_complete_raw_clocks_only",
                    "chunk": chunk, "attempt": attempt, "offset": offset, "keys": len(keys), "keys_sha256": digest(encode(keys)),
                    "headers": {**headers, "path": "headers.parquet"}, "rpc_usage": usage,
                    "active_seconds_charged": b.chunk_elapsed(), "raw_clocks_only": True, "final_sample_admitted": False}
                mb = b.write(base + "/manifest.json", encode(manifest))
                b.seal(base + "/COMPLETE.json", {"status": manifest["status"], "manifest": {**mb, "path": "manifest.json"}}, external)
                b.files[headers["path"]]["category"] = "accepted"
                b.used["failed"] -= len(payload); b.used["accepted"] += len(payload)
                root = output / base
                rows, header_binding = verify_chunk(root, keys, offset)
                b.check()
                new_active_seconds+=time.monotonic()-b.chunk_started;new_accepted_keys+=len(keys)
                b.chunk = None
            if last: pilot.validate_clock([last, rows[0]])
            last = rows[-1]; accepted += len(rows)
            completed[chunk] = {"root": str(root), "headers": header_binding, "keys": len(keys)}
        require(accepted == K and len(completed) == math.ceil(K / CAPS["keys_per_chunk"]), "exact_key_coverage_failed")
        RPC(endpoint, b, transport, "rpc.jsonl").call(terminal=True)
        external(full=True); b.reopen()
        observed = dict.fromkeys(("http_attempts", "method_items", "response_bytes"), 0)
        for name in b.files:
            if Path(name).name != "rpc.jsonl": continue
            usage, starts, finishes = journal_usage(output / name)
            require(len(starts) == len(finishes), "campaign_unclosed_request")
            for key in observed: observed[key] += usage[key]
        require(all(b.rpc[key] - prior["rpc"][key] == observed[key] for key in observed), "campaign_charge_reconciliation")
        # Independent ordered final pass catches late mutations of every sealed
        # chunk, with native expected hashes and adjacent cross-chunk parents.
        last, reopened_count = None, 0
        for chunk, keys in enumerate(key_chunks(inputs["keys"]["path"])):
            rows, header_binding = verify_chunk(Path(completed[chunk]["root"]), keys, chunk * CAPS["keys_per_chunk"])
            require(header_binding == completed[chunk]["headers"], "final_chunk_changed")
            if last: pilot.validate_clock([last, rows[0]])
            last = rows[-1]; reopened_count += len(rows)
        require(reopened_count == K, "final_key_coverage_failed")
        inventory_binding = b.write("artifact_inventory.json", encode(b.files))
        manifest = {"schema_version": "polymarket_exact_header_campaign_v5", "status": SUCCESS, "source": source,
            "recovery_policy": {"transient_rpc_codes":sorted(TRANSIENT_RPC_CODES),"diagnostic_record_bytes":previous.DIAGNOSTIC_RECORD_BYTES,
                "diagnostic_chunk_bytes":previous.DIAGNOSTIC_CHUNK_BYTES,"diagnostic_total_bytes":previous.DIAGNOSTIC_TOTAL_BYTES,
                "retry_delays_seconds":RETRY_DELAYS,"rate_limit_delays_seconds":RATE_LIMIT_DELAYS,
                "method_items_per_second":METHOD_ITEMS_PER_SECOND,"pacing":"shared_start_to_start_no_catchup",
                "forecast_reserve_seconds":FORECAST_RESERVE_SECONDS},
            "inputs": inputs, "admission": admission_binding, "caps": CAPS, "requested_keys": K, "accepted_keys": K,
            "reboot_equivalence":reboot["binding"],
            "missing_keys": 0, "duplicate_keys": 0, "fallback_rows": 0, "exact_key_coverage": True,
            "chunks": completed, "artifact_inventory": inventory_binding, "usage": b.usage(),
            "raw_clocks_only": True, "final_sample_admitted": False, "history_completeness_certified": False,
            "cryptographic_header_verification": False, "timestamp_semantics": "RPC-reported integer-second block time shared by fills in the same block"}
        mb = b.write("manifest.json", encode(manifest))
        b.seal("COMPLETE.json", {"schema_version": "polymarket_exact_header_campaign_completion_v5", "status": SUCCESS,
            "manifest": mb, "raw_clocks_only": True, "final_sample_admitted": False}, lambda: external(full=True))
        return {"status": SUCCESS, "run_dir": str(output), "accepted_keys": K, "usage": manifest["usage"], "final_sample_admitted": False}
    except BaseException as exc:
        code = exc.code if isinstance(exc,Deadline) else str(exc) if isinstance(exc, (Blocked, pilot.PilotBlocked)) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "header_campaign_runtime_failure"
        result = {"schema_version": "polymarket_exact_header_campaign_v5", "status": "blocked", "error_code": code,
                  "usage": b.usage(), "accepted_keys": accepted, "raw_clocks_only": True, "final_sample_admitted": False}
        try: b.write("FAILED.json", encode(result), failure=True)
        except BaseException: pass
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
        return result



def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admission",type=Path,required=True);parser.add_argument("--admission-sha256",required=True)
    parser.add_argument("--expected-head",required=True);parser.add_argument("--run-dir",type=Path,required=True)
    parser.add_argument("--rpc-private-file",type=Path,required=True)
    args=parser.parse_args(argv);require_production_host()
    try:
        require(Path(sys.executable)==Path("/home/ubuntu/venv/bin/python"),"production_interpreter_required")
        with process_limits() as debit:
            result=run_campaign(args.admission,args.admission_sha256,args.expected_head,args.run_dir,
                args.rpc_private_file,arm_prior=debit)
    except Deadline as exc:result={"status":"blocked","error_code":exc.code}
    except (Blocked,pilot.PilotBlocked) as exc:result={"status":"blocked","error_code":str(exc)}
    except (OSError,ValueError,TypeError,KeyError,subprocess.SubprocessError):
        result={"status":"blocked","error_code":"admission_or_environment_failure"}
    print(encode(result).decode().strip());return 0 if result["status"]==SUCCESS else 1


if __name__=="__main__":raise SystemExit(main())


