#!/usr/bin/env python3
"""Versioned recovery of the same literal exact-header inventory.

The v3 failure and single observation remain immutable and fully charged.
Well-formed, potentially transient per-item RPC errors retry a complete batch;
no partial result is accepted. Fixed-code diagnostics contain no provider prose.
A future reuse of this v4 campaign requires separately reviewed admission.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import http.client
import math
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import threading
import time
from urllib import error, request

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_exact_header_campaign_v3 as legacy

CAPS = legacy.CAPS
SOURCE_FILES = (*legacy.SOURCE_FILES, "scripts/collect_polymarket_exact_header_campaign_v4.py",
                "tests/test_polymarket_exact_header_campaign_v4.py")
SCHEMA = "polymarket_exact_header_campaign_admission_v4"
SUCCESS = legacy.SUCCESS
RUN_ROOT, MAX_JSON, MAX_LEDGER, FINAL_RESERVE = legacy.RUN_ROOT, legacy.MAX_JSON, legacy.MAX_LEDGER, legacy.FINAL_RESERVE
Blocked, require, digest, encode, decode = legacy.Blocked, legacy.require, legacy.digest, legacy.encode, legacy.decode
pilot, scale, inventory, Budget = legacy.pilot, legacy.scale, legacy.inventory, legacy.Budget
identity, bound, reference, read_json, ref = legacy.identity, legacy.bound, legacy.reference, legacy.read_json, legacy.ref
owner_identity, owner_state = legacy.owner_identity, legacy.owner_state
key_chunks, read_headers, verify_chunk, journal_usage = legacy.key_chunks, legacy.read_headers, legacy.verify_chunk, legacy.journal_usage
TRANSIENT_RPC_CODES = frozenset((-32603, -32002, -32005))
RETRY_DELAYS = (0.5,1.0)
DIAGNOSTIC_RECORD_BYTES, DIAGNOSTIC_CHUNK_BYTES, DIAGNOSTIC_TOTAL_BYTES = 4096, 256*1024, 32*1024**2
PRIOR_SOURCE_HEAD = "1bbeac146d22b2ca0ec290180f6dc7bf89318b6c"
PRIOR_FLOOR = {"http_attempts":220, "method_items":12309, "response_bytes":101435357, "elapsed_seconds":7680}
OBSERVATION_ACCEPTANCE = {
    "path":"/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1/header_error_observation_v3.ROOT_ACCEPTANCE.json",
    "bytes":2576, "sha256":"f3cf41e30d6bf6bbeba35c8e276d4de42fc9a286376250a0e256a2b7298e15af"}


class Deadline(BaseException):
    def __init__(self, code):
        self.code, self.raw, self.status = code, b"", None


def source_snapshot(head):
    source = legacy.source_snapshot(head)
    files = dict(source["files"])
    for name in SOURCE_FILES[len(legacy.SOURCE_FILES):]:
        actual = bound(REPO/name, MAX_JSON); raw = scale.git_blob(head, name)
        require(len(raw) == actual["bytes"] and digest(raw) == actual["sha256"], "source_blob_differs")
        files[name] = {key:actual[key] for key in ("bytes","sha256")}
    require(legacy.source_snapshot(head) == source, "source_changed_during_snapshot")
    return {"head":head, "files":files}


def legacy_source(source):
    return {"head":source["head"], "files":{name:source["files"][name] for name in legacy.SOURCE_FILES}}


def validate_inputs(value, source, actual_keys):
    return legacy.validate_inputs(value, legacy_source(source), actual_keys)


def validate_prior(value, output, source, inputs):
    require(type(value) is dict and set(value) == {"complete_roster","baseline_cache_excluded","roots",
        "standalone_control_files","original_pilot_input","files","rpc","campaigns","observations"},
        "prior_ledger_schema")
    observations = value["observations"]
    require(type(observations) is list and len(observations) == 1 and
        type(observations[0]) is dict and set(observations[0]) == {"run_dir","root_acceptance"} and
        observations[0]["root_acceptance"] == OBSERVATION_ACCEPTANCE, "observation_admission_not_exact")
    accepted, acceptance_binding = read_json(OBSERVATION_ACCEPTANCE)
    root = Path(observations[0]["run_dir"])
    require(accepted.get("schema_version") == "polymarket_single_header_observation_root_acceptance_v1" and
        accepted.get("http_requests_sent") == 1 and accepted.get("method_items_sent") == 100 and
        accepted.get("timestamps_adopted") == 0 and accepted.get("collection_restarted") is False and
        accepted.get("final_sample_admitted") is False and accepted.get("original_failure_cause_established") is False and
        accepted.get("producer_and_curl_pids_absent") is True and accepted.get("owned_group_and_session_empty") is True,
        "observation_acceptance_invalid")
    require(accepted.get("cumulative_http_attempts") == PRIOR_FLOOR["http_attempts"] and
        accepted.get("cumulative_method_items") == PRIOR_FLOOR["method_items"] and
        accepted.get("cumulative_response_bytes") == PRIOR_FLOOR["response_bytes"] and
        accepted.get("authoritative_cumulative_header_runtime_reservation_seconds") == PRIOR_FLOOR["elapsed_seconds"],
        "observation_charge_differs")
    require(root.is_absolute() and root.resolve() == root and RUN_ROOT in root.parents and
        str(root) in value["roots"] and not root.is_symlink() and root != output and
        root not in output.parents and output not in root.parents, "observation_root_invalid")
    files = {item["path"]:item for item in value["files"]}
    require(len(files) == len(value["files"]), "prior_file_duplicate_or_missing")
    require(acceptance_binding["path"] in files and ref(files[acceptance_binding["path"]]) == ref(acceptance_binding) and
        files[acceptance_binding["path"]]["category"] == "control" and
        acceptance_binding["path"] in value["standalone_control_files"], "observation_acceptance_not_rostered")
    observed = {}
    for name, field in (("request.json","request"),("response.raw","raw_body"),("RESULT.json","result")):
        expected = accepted[field]
        require(expected["path"] == str(root/name) and expected["path"] in files and
            ref(files[expected["path"]]) == expected, "observation_artifact_not_rostered")
        observed[name] = reference(expected, CAPS["individual_response_bytes"])
    result = read_json(ref(observed["RESULT.json"]))[0]
    require(result.get("schema_version") == "polymarket_header_error_observation_v2" and
        result.get("status") == "provider_observation_only" and result.get("body_complete") is True and
        result.get("http_attempts") == 1 and result.get("method_items") == 100 and
        result.get("timestamps_adopted") == 0 and result.get("campaign_retry_or_resume") is False and
        result.get("final_sample_admitted") is False and result.get("bindings_unchanged") is True and
        result.get("child",{}).get("owned_child_reaped") is True and
        result.get("response_sha256") == observed["response.raw"]["sha256"] and
        result.get("response_bytes_received") == result.get("response_bytes_charged") == observed["response.raw"]["bytes"],
        "observation_result_invalid")
    require(str(root/"START.json") in files,"observation_start_missing")
    start = reference(ref(files[str(root/"START.json")]))
    for name,capture in (("START.json",start),("request.json",observed["request.json"]),("response.raw",observed["response.raw"])):
        require(result.get("artifacts",{}).get(name) == {"bytes":capture["bytes"],"sha256":capture["sha256"]},
            "observation_result_artifact_differs")
    for name in ("launch","exit","root_admission"):
        expected = accepted[name]
        require(expected["path"] in files and ref(files[expected["path"]]) == expected and
            files[expected["path"]]["category"] == "control","observation_control_missing")
        reference(expected)
    expected_paths = {str(root/name) for name in ("START.json","request.json","response.raw","RESULT.json")}
    actual = set()
    for path in root.iterdir():
        require(path.is_file() and not path.is_symlink(), "observation_tree_invalid")
        actual.add(str(path))
    require(actual == expected_paths and all(files[path]["chunk"] is None and
        files[path]["category"] == ("failed" if path.endswith("/response.raw") else "control")
        for path in expected_paths), "observation_roster_or_category")
    removed = expected_paths
    base_value = {key:part for key,part in value.items() if key != "observations"}
    base_value["roots"] = [name for name in value["roots"] if name != str(root)]
    base_value["files"] = [item for item in value["files"] if item["path"] not in removed]
    prior = legacy.validate_prior(base_value, output, legacy_source(source), inputs)
    require(bool(value["campaigns"]), "failed_v3_campaign_missing")
    for campaign in value["campaigns"]:
        owner = read_json(ref(prior["files"][str(Path(campaign["run_dir"])/"OWNER.json")]))[0]
        require(owner["source"]["head"] == PRIOR_SOURCE_HEAD, "prior_producer_epoch_not_admitted")
    for key,floor in PRIOR_FLOOR.items():
        require((type(value["rpc"][key]) is int if key != "elapsed_seconds" else
            type(value["rpc"][key]) in (int,float) and math.isfinite(value["rpc"][key])) and
            value["rpc"][key] >= floor, "prior_authoritative_charge_understated")
    for path in removed:
        item = files[path]; actual_binding = reference(ref(item), CAPS["individual_response_bytes"])
        prior["files"][path] = actual_binding | {"category":item["category"],"chunk":None}
        prior["bytes"][item["category"]] += item["bytes"]
    require(all(prior["bytes"][key] <= CAPS[key+"_bytes"] for key in prior["bytes"]) and
        sum(prior["bytes"].values()) <= CAPS["global_header_bytes"], "prior_disk_allocation")
    prior["observation_elapsed_seconds"] = legacy.elapsed(result.get("elapsed_seconds"))
    return prior


@contextmanager
def request_deadline(seconds, kind="request_deadline"):
    require(threading.current_thread() is threading.main_thread(), "main_thread_deadline_required")
    began = time.monotonic(); handler = signal.getsignal(signal.SIGALRM)
    previous, interval = signal.getitimer(signal.ITIMER_REAL)
    require(interval == 0 and seconds > 0, "request_deadline_invalid")
    prior_end = began+previous if previous else None
    delay = min(seconds, previous) if previous else seconds
    code = "runtime_limit" if previous and previous < seconds else kind
    try:
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(Deadline(code)))
        signal.setitimer(signal.ITIMER_REAL, delay)
        yield
        if time.monotonic()-began >= delay: raise Deadline(code)
    finally:
        signal.setitimer(signal.ITIMER_REAL,0); signal.signal(signal.SIGALRM,handler)
        if prior_end is not None:
            signal.setitimer(signal.ITIMER_REAL,max(0.000001,prior_end-time.monotonic()))


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): return None


def http_response(endpoint, payload, *, byte_limit, timeout):
    response, raw, status, code, complete, length = None, bytearray(), None, None, False, None
    began = time.monotonic(); aborted = False; oversize = False
    def keep(part):
        require(type(part) is bytes, "transport_read_contract")
        raw.extend(part[:max(0,byte_limit-len(raw))])
    try:
        opener = request.build_opener(request.ProxyHandler({}), NoRedirect())
        req = request.Request(endpoint, data=encode(payload), method="POST",
            headers={"Content-Type":"application/json","Accept-Encoding":"identity"})
        try: response = opener.open(req,timeout=timeout)
        except error.HTTPError as returned: response = returned
        status = response.status
        require(type(status) is int and 100 <= status <= 599, "invalid_http_status")
        require(response.geturl() == endpoint and response.headers.get("Content-Encoding","identity").lower() == "identity",
            "redirect_or_encoding_changed")
        header = response.headers.get("Content-Length")
        if header is not None:
            require(type(header) is str and header.isdecimal() and str(int(header)) == header, "invalid_content_length")
            length = int(header)
        if length is not None and length > byte_limit:
            code,oversize = "response_declared_oversize",True
        else:
            while len(raw) < byte_limit:
                asked = min(65536,byte_limit-len(raw))
                part = response.read(asked); keep(part)
                require(len(part) <= asked,"transport_read_contract")
                if not part: complete = True; break
            else: complete = length == len(raw)
            require(complete,"response_at_limit_without_terminal_proof")
            require(length is None or len(raw) == length,"content_length_differs")
    except http.client.IncompleteRead as exc:
        keep(exc.partial); code = "response_incomplete"
    except Deadline as exc:
        aborted = True; exc.raw,exc.status = bytes(raw),status; raise
    except (KeyboardInterrupt,SystemExit) as exc:
        aborted = True; exc.raw,exc.status = bytes(raw),status; raise
    except Blocked as exc: code = str(exc)
    except (OSError,ValueError,TypeError): code = "transport_failure"
    finally:
        if response is not None and not aborted:
            if time.monotonic()-began >= timeout:
                exc=Deadline("request_deadline"); exc.raw,exc.status=bytes(raw),status; raise exc
            try: response.close()
            except Deadline as exc:
                exc.raw,exc.status = bytes(raw),status; raise
            except (KeyboardInterrupt,SystemExit) as exc:
                exc.raw,exc.status=bytes(raw),status;raise
            except BaseException: code,complete = "response_close_failure",False
    return {"raw":bytes(raw),"status":status,"complete":complete and code is None,
        "charged_bytes":len(raw) if complete and code is None else byte_limit,
        "error_code":code,"proven_oversize":oversize}


def diagnostic(budget,journal,finish,reason,codes):
    counts = {}
    for code in codes: counts[str(code)] = counts.get(str(code),0)+1
    value = {"schema_version":"polymarket_exact_header_error_v4","call":finish["i"],"reason":reason,
        "http_status":finish["s"],"error_code_counts":counts,"observed_body_bytes":finish["z"],
        "observed_body_sha256":finish["x"],"complete_body_sha256":finish["h"],"charged_response_bytes":finish["b"],
        "provider_text_retained":False}
    raw = encode(value); require(len(raw) <= DIAGNOSTIC_RECORD_BYTES,"diagnostic_record_limit")
    previous = [item for name,item in budget.files.items() if Path(name).name.startswith("error_r")]
    require(sum(item["bytes"] for item in previous)+len(raw) <= DIAGNOSTIC_TOTAL_BYTES,"diagnostic_total_limit")
    require(sum(item["bytes"] for item in previous if item["chunk"] == budget.chunk)+len(raw)
        <= DIAGNOSTIC_CHUNK_BYTES,"diagnostic_chunk_limit")
    target = str(Path(journal).parent/("error_r"+str(finish["i"]).zfill(9)+".json"))
    budget.write(target,raw,failure=True)


class RPC(legacy.RPC):
    def call(self, keys=None, offset=0, *, terminal=False):
        b = self.budget; n = 0 if keys is None else len(keys)
        for attempt in range(CAPS["attempts_per_batch"]):
            b.check()
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
                self.batch_size=min(self.batch_size,max(1,len(keys)//2)); return None
            retry = retry or code in {"transport_failure","request_deadline","response_incomplete","content_length_differs","response_close_failure"}
            require(retry and attempt+1 < CAPS["attempts_per_batch"],code or "rpc_failure")
            delay=RETRY_DELAYS[attempt]
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
        "raw_clocks_only", "final_sample_admitted", "inputs", "private_file", "actual_keys", "conservative_new_runtime_seconds", "prior"}
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
    prior = validate_prior(admission["prior"], output, source, inputs)
    for value in inputs.values():
        require(value["path"] in prior["files"] and ref(prior["files"][value["path"]]) == ref(value), "input_absent_from_global_ledger")
    projection = legacy.elapsed(admission["conservative_new_runtime_seconds"])
    require(projection >= 2 * measured["seconds_per_key"] * K and projection >= prior["observation_elapsed_seconds"]*math.ceil(K/CAPS["initial_batch_size"]) and projection + prior["rpc"]["elapsed_seconds"] < CAPS["runtime_seconds"], "runtime_feasibility_not_admitted")
    require(max(80, 1.25 * measured["bytes_per_key"]) * K <= CAPS["accepted_bytes"], "header_disk_projection_not_fit")
    private_binding = reference(admission["private_file"])
    require(private_binding["path"] == str(private) and private.stat().st_size <= 16 * 1024, "private_configuration_binding")
    endpoint, configuration = inventory.load_rpc_endpoint(private)
    require(configuration == "private_file", "private_rpc_authority_required")
    scale_contract = read_json(admission["inputs"]["scale_contract"])[0]
    require(scale_contract.get("rpc_authority_private_file_identity") == pilot.identity(private), "rpc_authority_differs_from_scale")
    for path in [admission_binding["path"], private_binding["path"], *prior["files"]]:
        require(Path(path) != output and output not in Path(path).parents and Path(path) not in output.parents, "input_output_overlap")
    resources = inventory.resource_state(output.parent)
    require(resources["ram_bytes"]["MemAvailable"] >= CAPS["minimum_available_ram_bytes"], "available_ram_floor")
    require(resources["disk_bytes"]["free"] >= CAPS["minimum_free_bytes"] + CAPS["chunk_artifact_bytes"] + FINAL_RESERVE,
            "disk_floor")
    require(prior["rpc"]["elapsed_seconds"] == declared_runtime, "prior_runtime_changed")
    output.mkdir(mode=0o700)
    b = Budget(output, prior, admission_binding["bytes"], started)
    source_identities = {name: identity(REPO / name) for name in SOURCE_FILES}
    def before_request():
        for value in (admission_binding, private_binding, inputs["keys"]):
            require(identity(Path(value["path"])) == value["identity"], "request_input_changed")
        require(all(identity(REPO / name) == value for name, value in source_identities.items()), "request_source_changed")
    b.before_request = before_request
    completed, last, accepted = {}, None, 0
    def external(full=False):
        require(source_snapshot(head) == source, "source_changed")
        for value in [admission_binding, private_binding, *inputs.values(), *prior["files"].values()]:
            require(identity(Path(value["path"])) == value["identity"], "input_identity_changed")
        require(inventory.load_rpc_endpoint(private) == (endpoint, configuration), "private_authority_changed")
        if full:
            require(read_json(ref(admission_binding), MAX_LEDGER)[1] == admission_binding, "admission_changed")
            require(reference(admission["private_file"]) == private_binding, "private_authority_changed")
            for value in inputs.values(): require(bound(Path(value["path"]), CAPS["baseline_bytes"]) == value, "input_changed")
            require(validate_prior(admission["prior"], output, source, inputs) == prior, "prior_artifacts_or_roster_changed")
        b.check()
    try:
        b.write("OWNER.json", encode({"schema_version": "polymarket_exact_header_owner_v4", "run_dir": str(output),
            "source": source, "process": owner_identity(), "inputs": inputs, "caps": CAPS, "admission": admission_binding,
            "raw_clocks_only": True, "final_sample_admitted": False}))
        external()
        RPC(endpoint, b, transport, "rpc.jsonl").call()
        for chunk, keys in enumerate(key_chunks(inputs["keys"]["path"])):
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
        manifest = {"schema_version": "polymarket_exact_header_campaign_v4", "status": SUCCESS, "source": source,
            "recovery_policy": {"transient_rpc_codes":sorted(TRANSIENT_RPC_CODES),"diagnostic_record_bytes":DIAGNOSTIC_RECORD_BYTES,
                "diagnostic_chunk_bytes":DIAGNOSTIC_CHUNK_BYTES,"diagnostic_total_bytes":DIAGNOSTIC_TOTAL_BYTES,
                "retry_delays_seconds":RETRY_DELAYS},
            "inputs": inputs, "admission": admission_binding, "caps": CAPS, "requested_keys": K, "accepted_keys": K,
            "missing_keys": 0, "duplicate_keys": 0, "fallback_rows": 0, "exact_key_coverage": True,
            "chunks": completed, "artifact_inventory": inventory_binding, "usage": b.usage(),
            "raw_clocks_only": True, "final_sample_admitted": False, "history_completeness_certified": False,
            "cryptographic_header_verification": False, "timestamp_semantics": "RPC-reported integer-second block time shared by fills in the same block"}
        mb = b.write("manifest.json", encode(manifest))
        b.seal("COMPLETE.json", {"schema_version": "polymarket_exact_header_campaign_completion_v4", "status": SUCCESS,
            "manifest": mb, "raw_clocks_only": True, "final_sample_admitted": False}, lambda: external(full=True))
        return {"status": SUCCESS, "run_dir": str(output), "accepted_keys": K, "usage": manifest["usage"], "final_sample_admitted": False}
    except BaseException as exc:
        code = exc.code if isinstance(exc,Deadline) else str(exc) if isinstance(exc, (Blocked, pilot.PilotBlocked)) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "header_campaign_runtime_failure"
        result = {"schema_version": "polymarket_exact_header_campaign_v4", "status": "blocked", "error_code": code,
                  "usage": b.usage(), "accepted_keys": accepted, "raw_clocks_only": True, "final_sample_admitted": False}
        try: b.write("FAILED.json", encode(result), failure=True)
        except BaseException: pass
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
        return result



@contextmanager
def process_limits():
    old,handler = resource.getrlimit(resource.RLIMIT_AS),signal.getsignal(signal.SIGALRM)
    previous,interval = signal.getitimer(signal.ITIMER_REAL); began=time.monotonic()
    require(interval == 0,"unsupported_existing_interval_timer")
    try:
        resource.setrlimit(resource.RLIMIT_AS,(min(old[0],CAPS["address_space_bytes"]) if old[0]>=0 else CAPS["address_space_bytes"],old[1]))
        require(0 < resource.getrlimit(resource.RLIMIT_AS)[0] <= CAPS["address_space_bytes"],"address_space_not_enforced")
        signal.signal(signal.SIGALRM,lambda *_: (_ for _ in ()).throw(Deadline("runtime_limit")))
        signal.setitimer(signal.ITIMER_REAL,min(CAPS["runtime_seconds"],previous) if previous else CAPS["runtime_seconds"])
        def debit(prior,started):
            left=CAPS["runtime_seconds"]-prior-(time.monotonic()-started)
            require(left>0,"runtime_limit")
            if previous:left=min(left,max(0.000001,previous-(time.monotonic()-began)))
            signal.setitimer(signal.ITIMER_REAL,left)
        yield debit
    finally:
        signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,handler)
        if previous:signal.setitimer(signal.ITIMER_REAL,max(0.000001,previous-(time.monotonic()-began)))
        resource.setrlimit(resource.RLIMIT_AS,old)


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

