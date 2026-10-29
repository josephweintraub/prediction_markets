"""Version-seven fixed caller for the reviewed header-v8 stage only.

Root starts this caller detached, with stdin/stdout/stderr disconnected from SSH.
It launches one fixed producer, never retries/resumes, and certifies only its own
process-session drainage. It does not certify host idleness or scientific data.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import selectors
import signal
import socket
import stat
import subprocess
import sys
import time
import uuid

REPO=Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:sys.path.insert(0,str(REPO))
from production_guard import require_production_host

PYTHON=Path("/home/ubuntu/venv/bin/python")
RUN_ROOT=Path("/mnt/data/runs")
PRIVATE=Path("/home/ubuntu/.polygon_rpc_url")
STAGES={"headers":("scripts/collect_polymarket_exact_header_campaign_v8.py",72*3600)}
ADMISSIONS={"headers":("polymarket_exact_header_campaign_admission_v8","root_exact_header_campaign_admitted")}
SOURCE_FILES=("production_guard.py","scripts/launch_polymarket_oct1_stage_v1.py",
              "tests/test_launch_polymarket_oct1_stage_v1.py","scripts/launch_polymarket_oct1_stage_v2.py",
              "tests/test_launch_polymarket_oct1_stage_v2.py","scripts/launch_polymarket_oct1_stage_v3.py",
              "tests/test_launch_polymarket_oct1_stage_v3.py","scripts/launch_polymarket_oct1_stage_v4.py",
              "tests/test_launch_polymarket_oct1_stage_v4.py",
              "scripts/launch_polymarket_oct1_stage_v5.py",
              "tests/test_launch_polymarket_oct1_stage_v5.py",
              "scripts/launch_polymarket_oct1_stage_v6.py",
              "tests/test_launch_polymarket_oct1_stage_v6.py",
              "scripts/launch_polymarket_oct1_stage_v7.py",
              "tests/test_launch_polymarket_oct1_stage_v7.py",
              "scripts/collect_polymarket_exact_header_campaign_v7.py",
              "tests/test_polymarket_exact_header_campaign_v8.py",
              "scripts/collect_polymarket_exact_header_campaign_v6.py",
              "tests/test_polymarket_exact_header_campaign_v7.py",
              "scripts/collect_polymarket_exact_header_campaign_v4.py",
              "scripts/collect_polymarket_exact_header_campaign_v5.py",
              "tests/test_polymarket_exact_header_campaign_v6.py",
              "scripts/collect_polymarket_market_metadata_campaign_v4.py",
              "scripts/collect_polymarket_market_metadata_campaign_v5.py",
              "scripts/collect_polymarket_market_metadata_campaign_v6.py",
              "tests/test_polymarket_market_metadata_campaign_v5.py",
              "tests/test_polymarket_market_metadata_campaign_v6.py",
              "tests/test_polymarket_exact_header_campaign_v5.py",*(x[0] for x in STAGES.values()))
CONTROL_LIMIT,LOG_LIMIT,JSON_LIMIT=2*1024**2,512*1024,8192
CLEANUP_SECONDS=30
SCHEMA="polymarket_oct1_stage_launcher_v7"
SHA=re.compile(r"[0-9a-f]{64}")
HEAD=re.compile(r"[0-9a-f]{40}")


class Blocked(ValueError):pass
class Stop(BaseException):
    def __init__(self,code):self.code=code


def require(value,code):
    if not value:raise Blocked(code)


def encode(value):return (json.dumps(value,sort_keys=True,separators=(",",":"),allow_nan=False)+"\n").encode()
def digest(raw):return hashlib.sha256(raw).hexdigest()


def identity(path):
    path=Path(path);s=path.lstat()
    require(path.is_absolute() and path.resolve()==path and stat.S_ISREG(s.st_mode) and s.st_nlink==1,
            "not_literal_single_regular_file")
    return {"device":s.st_dev,"inode":s.st_ino,"bytes":s.st_size,"mode":s.st_mode,
            "mtime_ns":s.st_mtime_ns,"ctime_ns":s.st_ctime_ns}


def bound(path,maximum):
    before=identity(path);require(before["bytes"]<=maximum,"file_byte_limit")
    h=hashlib.sha256();count=0
    with Path(path).open("rb") as f:
        while raw:=f.read(65536):
            count+=len(raw);require(count<=maximum,"file_byte_limit");h.update(raw)
    require(identity(path)==before,"file_changed_during_hash")
    return {"path":str(path),"bytes":count,"sha256":h.hexdigest(),"identity":before}


def admission_prior(stage,path,binding):
    raw=Path(path).read_bytes()
    require(len(raw)==binding["bytes"] and digest(raw)==binding["sha256"]
            and identity(path)==binding["identity"],"admission_changed_during_read")
    def pairs(items):
        value={}
        for key,item in items:
            require(key not in value,"admission_duplicate_key");value[key]=item
        return value
    def nonfinite(_):raise Blocked("admission_nonfinite_json")
    try:value=json.loads(raw,object_pairs_hook=pairs,parse_constant=nonfinite)
    except (ValueError,UnicodeError) as exc:raise Blocked("admission_json_invalid") from exc
    require(type(value) is dict and value.get("schema_version")==ADMISSIONS[stage][0]
            and value.get("status")==ADMISSIONS[stage][1],"admission_schema_differs")
    try:prior=value["prior"]["rpc"]["elapsed_seconds"] if stage=="headers" else value["prior_ledger"]["runtime_seconds"]
    except (KeyError,TypeError) as exc:raise Blocked("admission_prior_runtime_missing") from exc
    require(type(prior) in (int,float) and math.isfinite(prior) and prior>=0,"admission_prior_runtime_invalid")
    require(prior<STAGES[stage][1]-CLEANUP_SECONDS,"admission_prior_runtime_exhausted")
    return prior


def source_snapshot(head):
    require(type(head) is str and HEAD.fullmatch(head),"invalid_expected_head")
    def git(*args):
        return subprocess.check_output(["git",*args],cwd=REPO,stderr=subprocess.DEVNULL,timeout=10)
    require(git("rev-parse","HEAD").decode().strip()==head
            and not git("status","--porcelain","--untracked-files=normal").strip(),"source_not_clean_expected_head")
    files={}
    for name in SOURCE_FILES:
        b=bound(REPO/name,1024**2);size=git("cat-file","-s",head+":"+name).strip()
        require(size.isdigit() and int(size)==b["bytes"],"source_blob_size_differs")
        raw=git("cat-file","blob",head+":"+name)
        require(len(raw)==b["bytes"] and digest(raw)==b["sha256"]
                and identity(REPO/name)==b["identity"],"source_blob_differs")
        files[name]={k:b[k] for k in ("bytes","sha256")}
    require(git("rev-parse","HEAD").decode().strip()==head
            and not git("status","--porcelain","--untracked-files=normal").strip(),"source_changed_during_snapshot")
    return {"head":head,"files":files}


def process_identity(pid):
    require(type(pid) is int and pid>0,"process_pid_invalid")
    boot=Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    require(str(uuid.UUID(boot))==boot and uuid.UUID(boot).int>0,"boot_identity_invalid")
    raw=Path(f"/proc/{pid}/stat").read_text();fields=raw[raw.rfind(")")+2:].split()
    require(len(fields)>19 and int(raw.split("(",1)[0])==pid,"process_stat_invalid")
    result={"hostname":socket.gethostname(),"boot_id":boot,"pid":pid,"birth_ticks":int(fields[19]),
            "process_group":int(fields[2]),"session":int(fields[3])}
    validate_identity(result);return result


def validate_identity(value):
    require(type(value) is dict and set(value)=={"hostname","boot_id","pid","birth_ticks","process_group","session"}
            and type(value["hostname"]) is str and 0<len(value["hostname"])<=255
            and type(value["boot_id"]) is str and str(uuid.UUID(value["boot_id"]))==value["boot_id"]
            and uuid.UUID(value["boot_id"]).int>0
            and all(type(value[k]) is int and value[k]>0 for k in ("pid","birth_ticks","process_group","session")),
            "process_identity_schema_invalid")


def group_members(owner):
    # The unreaped leader keeps its PID reserved, preventing PGID/session reuse.
    require(process_identity(owner["pid"])==owner,"owned_leader_identity_changed")
    result={};count=0
    with os.scandir("/proc") as entries:
        for entry in entries:
            count+=1;require(count<=65536,"process_inventory_limit")
            if not entry.name.isdecimal():continue
            try:raw=Path(entry.path,"stat").read_text()
            except (FileNotFoundError,ProcessLookupError):continue
            fields=raw[raw.rfind(")")+2:].split()
            require(len(fields)>19,"process_stat_invalid")
            if int(fields[2])==owner["process_group"] or int(fields[3])==owner["session"]:
                result[int(entry.name)]={"birth_ticks":int(fields[19]),"state":fields[0]}
    require(owner["pid"] in result and result[owner["pid"]]["birth_ticks"]==owner["birth_ticks"],
            "owned_leader_missing")
    return result


def exited_unreaped(proc):
    # Never use poll/communicate before session drainage: those reap the leader.
    value=os.waitid(os.P_PID,proc.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT)
    return value is not None and value.si_pid==proc.pid


def atomic_publish(pending,target):
    lib=ctypes.CDLL(None,use_errno=True)
    if hasattr(lib,"renameat2"):r=lib.renameat2(-100,os.fsencode(pending),-100,os.fsencode(target),1)
    elif hasattr(lib,"renamex_np"):r=lib.renamex_np(os.fsencode(pending),os.fsencode(target),4)
    else:raise Blocked("no_replace_publication_unavailable")
    require(r==0,"exclusive_publication_refused")
    # Caller success requires namespace durability, not merely visible rename.
    fd=os.open(Path(target).parent,os.O_RDONLY|getattr(os,"O_DIRECTORY",0))
    try:os.fsync(fd)
    finally:os.close(fd)


class Receipts:
    def __init__(self,prefix):
        self.prefix=Path(prefix);self.files={}
        require(self.prefix.is_absolute() and self.prefix.parent.resolve()==self.prefix.parent
                and self.prefix.parent.is_dir() and RUN_ROOT in self.prefix.parents
                and re.fullmatch(r"[a-z][a-z0-9_-]{0,63}",self.prefix.name),"receipt_prefix_invalid")
        for name in ("START.json","LAUNCH.json","EXIT.json","log"):
            p=self.path(name)
            require(not p.exists() and not p.is_symlink() and not self.path("."+name+".pending").exists(),
                    "receipt_prefix_already_used")

    def path(self,name):return self.prefix.parent/(self.prefix.name+"."+name)

    def total(self,extra=0):
        amount=extra
        for name in self.files:amount+=identity(self.path(name))["bytes"]
        require(amount<=CONTROL_LIMIT,"external_control_byte_limit")

    def publish(self,name,value,*,terminal=False):
        raw=encode(value);require(len(raw)<=JSON_LIMIT,"receipt_json_limit")
        pending="."+name+".pending";self.total(len(raw))
        self.files[pending]=None
        with self.path(pending).open("xb") as f:
            os.chmod(self.path(pending),0o600)
            require(f.write(raw)==len(raw),"short_control_write");f.flush();os.fsync(f.fileno())
        require(self.path(pending).read_bytes()==raw,"control_reopen_differs")
        fd=os.open(self.prefix.parent,os.O_RDONLY|getattr(os,"O_DIRECTORY",0))
        try:os.fsync(fd)
        finally:os.close(fd)
        self.total();atomic_publish(self.path(pending),self.path(name))
        self.files.pop(pending)
        if not terminal:self.files[name]=bound(self.path(name),JSON_LIMIT)

    def verify(self):
        self.total()
        for name,binding in self.files.items():
            require(binding is not None and bound(self.path(name),CONTROL_LIMIT)==binding,"caller_artifact_changed")


def command(stage,admission,sha,head,output):
    require(stage in STAGES and type(sha) is str and SHA.fullmatch(sha)
            and type(head) is str and HEAD.fullmatch(head),"stage_or_binding_invalid")
    args=[str(PYTHON),STAGES[stage][0],"--admission",str(admission),"--admission-sha256",sha,
          "--expected-head",head,"--run-dir",str(output)]
    if stage=="headers":args += ["--rpc-private-file",str(PRIVATE)]
    else:args += ["--mode","full"]
    return args


def drain(proc,owner,ends,*,identities=process_identity,members=group_members,terminate=False,pump=lambda _:None):
    require(identities(proc.pid)==owner,"owned_leader_identity_changed")
    if terminate:os.killpg(owner["process_group"],signal.SIGTERM)
    killed=False
    while time.monotonic()<ends:
        pump(min(.05,max(0,ends-time.monotonic())))
        require(identities(proc.pid)==owner,"owned_leader_identity_changed")
        group=members(owner)
        if exited_unreaped(proc) and set(group)=={proc.pid}:
            proc.wait(timeout=max(.001,ends-time.monotonic()))
            return True
        if not killed and time.monotonic()>=ends-2:
            require(identities(proc.pid)==owner,"owned_leader_identity_changed")
            os.killpg(owner["process_group"],signal.SIGKILL);killed=True
    raise Blocked("owned_session_drain_unproven")


def _run_stage(stage,admission,sha,head,output,prefix,*,_began,popen_type=subprocess.Popen,
              identities=process_identity,members=group_members):
    require(stage in STAGES,"stage_or_binding_invalid")
    began=_began;ends=began+STAGES[stage][1];active_ends=ends-CLEANUP_SECONDS
    require(active_ends>began,"stage_timeout_contract")
    admission,output=Path(admission),Path(output)
    args=command(stage,admission,sha,head,output)
    require(output.is_absolute() and output.parent==RUN_ROOT and output.parent.resolve()==output.parent
            and not output.exists() and not output.is_symlink(),"producer_output_not_fresh")
    require(admission.is_absolute() and RUN_ROOT in admission.parents and output not in admission.parents,
            "admission_path_invalid")
    source=source_snapshot(head);input_binding=bound(admission,64*1024**2)
    require(input_binding["sha256"]==sha,"admission_sha_differs")
    prior=admission_prior(stage,admission,input_binding)
    # Debit inherited runtime and this preflight from one original start. The
    # launcher cannot keep a hung producer alive beyond the aggregate ceiling.
    ends=began+STAGES[stage][1]-prior;active_ends=ends-CLEANUP_SECONDS
    require(active_ends>time.monotonic(),"caller_remaining_runtime_exhausted")
    signal.setitimer(signal.ITIMER_REAL,active_ends-time.monotonic())
    caller=identities(os.getpid());validate_identity(caller);receipts=Receipts(prefix)
    require(output not in receipts.prefix.parents and receipts.prefix.parent!=output,"control_output_overlap")
    base={"schema_version":SCHEMA,"stage":stage,"command":args,"source":source,"admission":input_binding,
          "output_dir":str(output),"caller":caller,"timeout_seconds":STAGES[stage][1],
          "admission_prior_runtime_seconds":prior,"remaining_wall_seconds_at_start":STAGES[stage][1]-prior,
          "cleanup_seconds_included":CLEANUP_SECONDS,"retry_or_resume":False,"host_idle_claimed":False,
          "final_sample_admitted":False}
    receipts.publish("START.json",{**base,"status":"caller_started_producer_not_yet_dispatched"})
    proc=None;owner=None;unknown=False;drained=None;failure=None;log=None;selector=None
    observed=retained=0;all_hash=hashlib.sha256();prefix_hash=hashlib.sha256();eof=False
    launch_written=False;constructor_entered=False
    def publish_launch(status):
        nonlocal launch_written
        receipts.publish("LAUNCH.json",{**base,"status":status,"child":owner,
            "child_pid_observed":getattr(proc,"pid",None),"ownership_unknown":unknown})
        launch_written=True
    def pump(seconds):
        nonlocal observed,retained,eof
        if selector is None:return
        for key,_ in selector.select(seconds):
            try:raw=os.read(key.fd,65536)
            except BlockingIOError:continue
            if not raw:selector.unregister(key.fileobj);eof=True;continue
            observed+=len(raw);all_hash.update(raw)
            prefix=raw[:max(0,LOG_LIMIT-retained)]
            if prefix:
                require(log.write(prefix)==len(prefix),"short_log_write")
                retained+=len(prefix);prefix_hash.update(prefix)
    def expire(*_):raise Stop("caller_timeout")
    def interrupted(*_):raise Stop("caller_interrupted")
    try:
        signal.signal(signal.SIGALRM,expire);signal.signal(signal.SIGTERM,interrupted)
        signal.setitimer(signal.ITIMER_REAL,max(.001,active_ends-time.monotonic()))
        require(source_snapshot(head)==source and bound(admission,64*1024**2)==input_binding,"launch_input_changed")
        require(resource.getrlimit(resource.RLIMIT_FSIZE)==(resource.RLIM_INFINITY,resource.RLIM_INFINITY),
                "producer_must_not_inherit_file_size_limit")
        receipts.files["log"]=None;log=receipts.path("log").open("xb");os.chmod(receipts.path("log"),0o600)
        # Retain the native instance before its constructor can create a child.
        proc=popen_type.__new__(popen_type)
        constructor_entered=True
        popen_type.__init__(proc,args,cwd=REPO,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
        require(getattr(proc,"_child_created",False) is True and type(proc.pid) is int and proc.pid>0,
                "producer_handle_unknown")
        owner=identities(proc.pid)
        validate_identity(owner)
        require(owner["pid"]==owner["process_group"]==owner["session"]==proc.pid
                and owner["boot_id"]==caller["boot_id"],"producer_session_identity_invalid")
        publish_launch("producer_dispatched_owned_session")
        selector=selectors.DefaultSelector();os.set_blocking(proc.stdout.fileno(),False)
        selector.register(proc.stdout,selectors.EVENT_READ)
        while not exited_unreaped(proc):pump(.1)
        drained=drain(proc,owner,min(ends,time.monotonic()+CLEANUP_SECONDS),identities=identities,members=members,pump=pump)
        while not eof and time.monotonic()<ends:pump(.05)
        require(eof,"producer_output_eof_unproven")
    except BaseException as exc:
        failure=exc.code if isinstance(exc,Stop) else str(exc) if isinstance(exc,Blocked) else "caller_startup_or_io_failure"
        # The original overall end is never extended by constructor/drain errors.
        signal.setitimer(signal.ITIMER_REAL,max(.001,ends-time.monotonic()))
        if constructor_entered:
            pid=getattr(proc,"pid",None)
            try:
                require(getattr(proc,"_child_created",False) is True and type(pid) is int and pid>0,
                        "producer_handle_unknown")
                if owner is None:
                    candidate=identities(pid)
                    validate_identity(candidate)
                    require(candidate["pid"]==candidate["process_group"]==candidate["session"]==pid
                            and candidate["boot_id"]==caller["boot_id"],"producer_session_identity_invalid")
                    owner=candidate
                require(identities(pid)==owner,"owned_leader_identity_changed")
                drained=drain(proc,owner,min(ends,time.monotonic()+CLEANUP_SECONDS),identities=identities,
                              members=members,terminate=True,pump=pump)
            except BaseException as cleanup_error:
                unknown=True;drained=None
                if isinstance(cleanup_error,Stop) and time.monotonic()>=ends:raise
        if not launch_written:
            publish_launch("producer_ownership_unknown" if unknown else "producer_not_started" if not constructor_entered
                           else "producer_failed_after_dispatch")
    finally:
        if selector is not None:selector.close()
        stream=getattr(proc,"stdout",None)
        if stream is not None:stream.close()
        if log is not None:
            log.flush();os.fsync(log.fileno());log.close()
            captured=bound(receipts.path("log"),LOG_LIMIT)
            require(captured["bytes"]==retained and captured["sha256"]==prefix_hash.hexdigest(),"log_prefix_changed")
            receipts.files["log"]=captured
    source_unchanged=source_snapshot(head)==source and bound(admission,64*1024**2)==input_binding
    receipts.verify()
    result={**base,"status":"producer_exited" if failure is None else "caller_failed_closed",
        "error_code":failure,"child":owner,"child_pid_observed":getattr(proc,"pid",None),
        "producer_exit_code":getattr(proc,"returncode",None),"ownership_unknown":unknown,
        "owned_group_drained":drained,"children_still_running":False if drained is True else None,
        "source_and_admission_unchanged":source_unchanged,"elapsed_seconds":time.monotonic()-began,
        "stdout_bytes_observed":observed,"stdout_sha256":all_hash.hexdigest(),
        "stdout_prefix_bytes_retained":retained,"stdout_prefix_sha256":prefix_hash.hexdigest(),
        "stdout_truncated":observed>retained,"stdout_eof_observed":eof}
    require(time.monotonic()<ends,"caller_total_timeout")
    receipts.publish("EXIT.json",result,terminal=True)
    return result


def run_stage(stage,admission,sha,head,output,prefix,**kwargs):
    previous_alarm=signal.getsignal(signal.SIGALRM);previous_term=signal.getsignal(signal.SIGTERM)
    require(signal.getitimer(signal.ITIMER_REAL)==(0.0,0.0),"unexpected_existing_timer")
    require(stage in STAGES,"stage_or_binding_invalid")
    began=time.monotonic();active_ends=began+STAGES[stage][1]-CLEANUP_SECONDS
    require(active_ends>began,"stage_timeout_contract")
    def expire(*_):raise Stop("caller_timeout")
    def interrupted(*_):raise Stop("caller_interrupted")
    try:
        signal.signal(signal.SIGALRM,expire);signal.signal(signal.SIGTERM,interrupted)
        signal.setitimer(signal.ITIMER_REAL,max(.001,active_ends-time.monotonic()))
        return _run_stage(stage,admission,sha,head,output,prefix,_began=began,**kwargs)
    finally:
        signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous_alarm)
        signal.signal(signal.SIGTERM,previous_term)


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--stage",choices=tuple(STAGES),required=True)
    p.add_argument("--admission",type=Path,required=True);p.add_argument("--admission-sha256",required=True)
    p.add_argument("--expected-head",required=True);p.add_argument("--run-dir",type=Path,required=True)
    p.add_argument("--receipt-prefix",type=Path,required=True);a=p.parse_args(argv)
    require_production_host()
    try:
        require(Path(sys.executable)==PYTHON and Path.cwd()==REPO,"production_interpreter_or_checkout")
        result=run_stage(a.stage,a.admission,a.admission_sha256,a.expected_head,a.run_dir,a.receipt_prefix)
        print(json.dumps({"exit_receipt":str(a.receipt_prefix)+".EXIT.json","producer_exit_code":result["producer_exit_code"],
                          "owned_group_drained":result["owned_group_drained"]},sort_keys=True))
        return 0 if result["producer_exit_code"]==0 and result["owned_group_drained"] is True \
            and result["source_and_admission_unchanged"] and result["error_code"] is None else 2
    except BaseException:
        print('{"status":"caller_failed_closed","host_idle_claimed":false,"ownership_requires_receipt_or_root_check":true}')
        return 2


if __name__=="__main__":raise SystemExit(main())
