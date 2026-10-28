#!/usr/bin/env python3
"""Same-volume reboot continuation of frozen header-v5; no new RPC policy."""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import math
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
import uuid
REPO=Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:sys.path.insert(0,str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_exact_header_campaign_v5 as previous
legacy,scale,pilot,inventory=previous.legacy,previous.scale,previous.pilot,previous.inventory
CAPS=previous.CAPS
SOURCE_FILES=(*previous.SOURCE_FILES,"scripts/collect_polymarket_exact_header_campaign_v6.py",
    "tests/test_polymarket_exact_header_campaign_v6.py")
SCHEMA="polymarket_exact_header_campaign_admission_v6"
SUCCESS,RUN_ROOT=previous.SUCCESS,previous.RUN_ROOT
MAX_JSON,MAX_LEDGER,FINAL_RESERVE=previous.MAX_JSON,previous.MAX_LEDGER,previous.FINAL_RESERVE
Blocked,Deadline,require=previous.Blocked,previous.Deadline,previous.require
encode,decode,digest=previous.encode,previous.decode,previous.digest
identity,bound,reference,read_json,ref=previous.identity,previous.bound,previous.reference,previous.read_json,previous.ref
Budget,owner_identity=previous.Budget,previous.owner_identity
key_chunks,read_headers,verify_chunk,journal_usage=previous.key_chunks,previous.read_headers,previous.verify_chunk,previous.journal_usage
request_deadline,http_response,process_limits=previous.request_deadline,previous.http_response,previous.process_limits
RPC=previous.RPC
TRANSIENT_RPC_CODES,RETRY_DELAYS,RATE_LIMIT_DELAYS=previous.TRANSIENT_RPC_CODES,previous.RETRY_DELAYS,previous.RATE_LIMIT_DELAYS
METHOD_ITEMS_PER_SECOND,FORECAST_RESERVE_SECONDS=previous.METHOD_ITEMS_PER_SECOND,previous.FORECAST_RESERVE_SECONDS
forecast_seconds=previous.forecast_seconds
V4_REFS=previous.V4_REFS
OLD_CERTIFICATE={"path":str(previous.OP_ROOT/"header_reboot_equivalence_v4_v2.json"),
    "bytes":8377,"sha256":"89efc5894cb2e60671242c1abe348178e09b9c7826170441e85a71e9a614da7d"}
DATA_UUID,DATA_SERIAL,ROOT_UUID=previous.previous.DATA_UUID,previous.previous.DATA_SERIAL,previous.previous.ROOT_UUID
CERT_SCHEMA="polymarket_header_reboot_equivalence_v6"
CERT_STATUS="root_verified_same_volume_reboot"
CERT_KEYS={"schema_version","status","previous_certificate","old_v3_owner","old_v3_drain","old_v4_owner",
    "old_v4_drain","current_environment","inputs","original_pilot_request","private_authority"}


def reboot_environment():
    """Read the actual mounted source; NVMe enumeration is not a volume ID."""
    def command(argv):
        reply=subprocess.run(argv,capture_output=True,check=True,timeout=10)
        require(len(reply.stdout)<=65536 and len(reply.stderr)<=65536,"reboot_mount_reply_limit")
        return reply.stdout.decode().strip().split()
    mounted=command(["findmnt","-n","-o","SOURCE,UUID,FSTYPE","--target","/mnt/data"])
    require(len(mounted)==3 and re.fullmatch(r"/dev/nvme[0-9]+n[0-9]+(?:p[0-9]+)?",mounted[0]),"mounted_block_source_invalid")
    source=Path(mounted[0]);device=source.lstat();data_device=Path("/mnt/data").stat().st_dev
    require(stat.S_ISBLK(device.st_mode) and device.st_rdev==data_device,"mounted_block_device_differs")
    volume=command(["lsblk","-b","-n","-d","-o","SERIAL,SIZE",str(source)])
    root=command(["findmnt","-n","-o","UUID","--target","/"])
    require(len(volume)==2 and len(root)==1,"reboot_mount_shape")
    value={"boot_id":Path("/proc/sys/kernel/random/boot_id").read_text().strip(),"uuid":mounted[1],"fstype":mounted[2],
        "serial":volume[0],"device_bytes":volume[1],"data_device":data_device,"root_uuid":root[0],"root_device":Path("/").stat().st_dev}
    environment_valid(value);return value


def source_snapshot(head):
    source=previous.source_snapshot(head);files=dict(source["files"]);captures={}
    for name in SOURCE_FILES[len(previous.SOURCE_FILES):]:
        value=bound(REPO/name,MAX_JSON);raw=scale.git_blob(head,name)
        require(len(raw)==value["bytes"] and digest(raw)==value["sha256"],"source_blob_differs")
        captures[name]=value;files[name]={key:value[key] for key in ("bytes","sha256")}
    require(previous.source_snapshot(head)==source,"source_changed_during_snapshot")
    for name,expected in files.items():
        current=bound(REPO/name,MAX_JSON)
        require({key:current[key] for key in ("bytes","sha256")}==expected,"source_final_content_changed")
    for name,value in captures.items():require(bound(REPO/name,MAX_JSON)==value,"source_reopen_changed")
    return {"head":head,"files":files}


def previous_source(source):
    return {"head":source["head"],"files":{name:source["files"][name] for name in previous.SOURCE_FILES}}


def validate_inputs(value,source,actual_keys):
    return previous.validate_inputs(value,previous_source(source),actual_keys)


def environment_valid(value):
    require(type(value) is dict and set(value)=={"boot_id","uuid","fstype","serial","device_bytes",
        "data_device","root_uuid","root_device"},"reboot_environment_schema")
    try:valid=str(uuid.UUID(value["boot_id"]))==value["boot_id"] and uuid.UUID(value["boot_id"]).int>0
    except (ValueError,TypeError,AttributeError):valid=False
    require(valid and value["uuid"]==DATA_UUID and value["fstype"]=="ext4" and
        value["serial"]==DATA_SERIAL and value["device_bytes"]==str(750*1024**3) and value["root_uuid"]==ROOT_UUID and
        type(value["data_device"]) is int and value["data_device"]>0 and
        type(value["root_device"]) is int and value["root_device"]>0,"reboot_environment_identity")


def only_device(old,current,old_device,current_device):
    require(type(old) is dict and type(current) is dict and set(old)==set(current) and
        type(old.get("device")) is int and old["device"]==old_device and
        type(current.get("device")) is int and current["device"]==current_device,"reboot_identity_schema")
    expected=dict(old);expected["device"]=current_device
    require(encode(expected)==encode(current),"reboot_nondevice_identity_differs")


def certificate_value(inputs,private_ref):
    """Build a candidate, never an authority bypass: validate_reboot reopens it."""
    old,_=read_json(OLD_CERTIFICATE)
    environment=reboot_environment();environment_valid(environment)
    keys,request=pilot.read_requests(Path(legacy.ORIGINAL_PILOT_INPUT["path"]),legacy.ORIGINAL_PILOT_INPUT["sha256"])
    require(len(keys)==1000,"original_request_key_count")
    return {"schema_version":CERT_SCHEMA,"status":CERT_STATUS,"previous_certificate":OLD_CERTIFICATE,
        "old_v3_owner":old["old_owner"],"old_v3_drain":old["old_drain"],
        "old_v4_owner":V4_REFS["owner"],"old_v4_drain":V4_REFS["root_drain"],"current_environment":environment,
        "inputs":{name:{"v3":pair["old"],"v4":pair["current"],"current":inputs[name]} for name,pair in old["inputs"].items()},
        "original_pilot_request":{"reference":old["original_pilot_request"]["reference"],
            "v3":old["original_pilot_request"]["old"],"v4":old["original_pilot_request"]["current"],"current":request},
        "private_authority":{"reference":private_ref,"original":old["private_authority"]["old"],
            "v4":old["private_authority"]["current"],"current":pilot.identity(Path(private_ref["path"])),"root_uuid":ROOT_UUID}}


def validate_reboot(certificate_ref,prior_value,inputs,private_ref):
    value,binding=read_json(certificate_ref)
    require(type(value) is dict and set(value)==CERT_KEYS and value["schema_version"]==CERT_SCHEMA and
        value["status"]==CERT_STATUS and value["previous_certificate"]==OLD_CERTIFICATE,"reboot_v6_certificate_schema")
    old,old_binding=read_json(OLD_CERTIFICATE)
    require(set(old)=={"schema_version","status","old_boot_id","current_boot_id","data_volume","old_owner","old_drain",
        "inputs","private_authority","original_pilot_request"} and old["schema_version"]=="polymarket_header_reboot_equivalence_v4"
        and old["status"]=="root_verified_same_volume_device_reassignment" and old["old_boot_id"]==previous.previous.OLD_BOOT_ID
        and old["current_boot_id"]==previous.previous.CURRENT_BOOT_ID,"historical_certificate_schema")
    require(old["data_volume"]=={"uuid":DATA_UUID,"serial":DATA_SERIAL,"device_bytes":750*1024**3,
        "old_device":previous.previous.OLD_DATA_DEVICE,"current_device":previous.previous.CURRENT_DATA_DEVICE},
        "historical_volume_differs")
    environment=reboot_environment();environment_valid(environment)
    require(value["current_environment"]==environment,"current_boot_or_mount_differs")
    require(value["old_v3_owner"]==old["old_owner"] and value["old_v3_drain"]==old["old_drain"] and
        value["old_v4_owner"]==V4_REFS["owner"] and value["old_v4_drain"]==V4_REFS["root_drain"],"historical_proof_refs_differs")
    owner3,ob3=read_json(value["old_v3_owner"]);drain3,db3=read_json(value["old_v3_drain"])
    owner4,ob4=read_json(V4_REFS["owner"]);drain4,db4=read_json(V4_REFS["root_drain"])
    require(owner3.get("schema_version")=="polymarket_exact_header_owner_v3" and
        owner3.get("source",{}).get("head")==previous.previous.PRIOR_SOURCE_HEAD and
        owner3.get("process",{}).get("boot_id")==old["old_boot_id"] and
        drain3.get("owner_sha256")==ob3["sha256"] and drain3.get("run_dir")==owner3.get("run_dir") and
        owner4.get("schema_version")=="polymarket_exact_header_owner_v4" and
        owner4.get("source",{}).get("head")==previous.V4_HEAD and
        owner4.get("process",{}).get("boot_id")==old["current_boot_id"] and
        drain4.get("header_owned_processes_drained") is True,"historical_owner_or_drain_differs")
    require(all(legacy.owner_state(owner["process"])=="dead" for owner in (owner3,owner4)),"historical_owner_live_or_unknown")
    require(prior_value.get("campaigns")==[{"run_dir":owner3["run_dir"],"drain":value["old_v3_drain"]}],
        "historical_campaign_reference_differs")
    require(type(value["inputs"]) is dict and set(value["inputs"])==set(inputs)==set(old["inputs"]) and len(inputs)==9,
        "reboot_input_roster_differs")
    v3_inputs={};v4_inputs={}
    for name,current in inputs.items():
        pair=value["inputs"][name]
        require(type(pair) is dict and set(pair)=={"v3","v4","current"} and pair["v3"]==old["inputs"][name]["old"] and
            pair["v4"]==old["inputs"][name]["current"] and pair["current"]==current and
            ref(pair["v3"])==ref(pair["v4"])==ref(current),"reboot_input_binding_differs")
        only_device(pair["v3"]["identity"],pair["v4"]["identity"],previous.previous.OLD_DATA_DEVICE,
            previous.previous.CURRENT_DATA_DEVICE)
        only_device(pair["v4"]["identity"],current["identity"],previous.previous.CURRENT_DATA_DEVICE,environment["data_device"])
        require(reference(ref(current),CAPS["baseline_bytes"])==current,"reboot_input_rehash_differs")
        v3_inputs[name]=pair["v3"];v4_inputs[name]=pair["v4"]
    require(owner3.get("inputs")==v3_inputs and owner4.get("inputs")==v4_inputs,"historical_owner_inputs_differs")
    original=value["original_pilot_request"]
    require(type(original) is dict and set(original)=={"reference","v3","v4","current"} and
        original["reference"]==legacy.ORIGINAL_PILOT_INPUT==old["original_pilot_request"]["reference"] and
        prior_value.get("original_pilot_input")==original["reference"] and
        original["v3"]==old["original_pilot_request"]["old"] and original["v4"]==old["original_pilot_request"]["current"],
        "reboot_original_reference_differs")
    original_reference=reference(original["reference"],MAX_JSON)
    keys,request=pilot.read_requests(Path(original_reference["path"]),original_reference["sha256"])
    require(len(keys)==1000 and original["current"]==request,"reboot_original_current_differs")
    contract=read_json(ref(inputs["scale_contract"]))[0]
    require(original["v3"]==contract.get("request") and type(request.get("keys")) is int and request["keys"]==1000 and
        all(original["v3"][key]==original["v4"][key]==request[key] for key in ("sha256","keys","normalized_keys_sha256")),
        "reboot_original_content_differs")
    only_device(original["v3"]["identity"],original["v4"]["identity"],previous.previous.OLD_DATA_DEVICE,
        previous.previous.CURRENT_DATA_DEVICE)
    only_device(original["v4"]["identity"],request["identity"],previous.previous.CURRENT_DATA_DEVICE,environment["data_device"])
    authority=value["private_authority"]
    require(type(authority) is dict and set(authority)=={"reference","original","v4","current","root_uuid"} and
        authority["reference"]==private_ref==old["private_authority"]["reference"] and authority["root_uuid"]==ROOT_UUID and
        authority["original"]==old["private_authority"]["old"]==contract.get("rpc_authority_private_file_identity") and
        authority["v4"]==old["private_authority"]["current"],"reboot_private_reference_differs")
    actual_private=reference(private_ref);current_private=pilot.identity(Path(private_ref["path"]))
    require(authority["current"]==current_private,"reboot_private_current_differs")
    only_device(authority["original"],authority["v4"],previous.previous.OLD_PRIVATE_DEVICE,previous.previous.CURRENT_PRIVATE_DEVICE)
    only_device(authority["v4"],current_private,previous.previous.CURRENT_PRIVATE_DEVICE,environment["root_device"])
    old_admission=read_json(ref(owner3["admission"]),MAX_LEDGER)[0]
    require(old_admission.get("private_file")==private_ref,"historical_private_reference_differs")
    files={item["path"]:item for item in prior_value["files"]}
    require(len(files)==len(prior_value["files"]),"prior_duplicate")
    for capture in (binding,old_binding,ob3,db3,ob4,db4):
        require(capture["path"] in files and ref(files[capture["path"]])==ref(capture) and
            files[capture["path"]]["category"]=="control" and files[capture["path"]]["chunk"] is None,"reboot_proof_not_rostered")
    require(prior_value["standalone_control_files"].count(binding["path"])==1,"reboot_certificate_not_exact_once")
    return {"binding":binding,"old_inputs":v3_inputs,"historical_v4_inputs":v4_inputs,
        "private_binding":actual_private,"private_current":current_private,"environment":environment,
        "original_request":{"old":original["v3"],"current":request},"original_reference":original_reference}


def validate_prior(value,output,source,inputs,reboot):
    # Frozen v5 sees only certified historical-v4 OWNER identities. Every file
    # reference is rehashed by that validator; returned prior bindings are fresh.
    require(all(ref(inputs[name])==ref(old) for name,old in reboot["historical_v4_inputs"].items()) and
        set(inputs)==set(reboot["historical_v4_inputs"]),"historical_projection_not_certified")
    return previous.validate_prior(value,output,previous_source(source),reboot["historical_v4_inputs"],reboot)


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
            reboot_environment()==reboot["environment"],"reboot_certificate_or_environment_changed")
        if full:
            require(read_json(ref(admission_binding), MAX_LEDGER)[1] == admission_binding, "admission_changed")
            require(reference(admission["private_file"]) == private_binding, "private_authority_changed")
            for value in inputs.values(): require(bound(Path(value["path"]), CAPS["baseline_bytes"]) == value, "input_changed")
            require(validate_reboot(admission["reboot_equivalence"],admission["prior"],inputs,admission["private_file"])==reboot,
                "reboot_certificate_changed")
            require(validate_prior(admission["prior"], output, source, inputs,reboot) == prior, "prior_artifacts_or_roster_changed")
            # These callbacks can inspect complex historical evidence. Reopen
            # current bindings after all of them, before the atomic seal.
            require(source_snapshot(head)==source,"source_changed")
            for value in [admission_binding,private_binding,*inputs.values(),*prior["files"].values(),
                    reboot["binding"],reboot["original_reference"]]:
                require(bound(Path(value["path"]),CAPS["global_header_bytes"])==
                    {key:value[key] for key in ("path","bytes","sha256","identity")},"final_binding_changed")
            require(all(identity(REPO/name)==value for name,value in source_identities.items()),"final_source_identity_changed")
        b.check()
    try:
        b.write("OWNER.json", encode({"schema_version": "polymarket_exact_header_owner_v6", "run_dir": str(output),
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
        manifest = {"schema_version": "polymarket_exact_header_campaign_v6", "status": SUCCESS, "source": source,
            "recovery_policy": {"transient_rpc_codes":sorted(TRANSIENT_RPC_CODES),"diagnostic_record_bytes":previous.previous.DIAGNOSTIC_RECORD_BYTES,
                "diagnostic_chunk_bytes":previous.previous.DIAGNOSTIC_CHUNK_BYTES,"diagnostic_total_bytes":previous.previous.DIAGNOSTIC_TOTAL_BYTES,
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
        b.seal("COMPLETE.json", {"schema_version": "polymarket_exact_header_campaign_completion_v6", "status": SUCCESS,
            "manifest": mb, "raw_clocks_only": True, "final_sample_admitted": False}, lambda: external(full=True))
        return {"status": SUCCESS, "run_dir": str(output), "accepted_keys": K, "usage": manifest["usage"], "final_sample_admitted": False}
    except BaseException as exc:
        code = exc.code if isinstance(exc,Deadline) else str(exc) if isinstance(exc, (Blocked, pilot.PilotBlocked)) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "header_campaign_runtime_failure"
        result = {"schema_version": "polymarket_exact_header_campaign_v6", "status": "blocked", "error_code": code,
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
