"""Literal synthetic packages and a fake compiler seam; no compiler execution."""
from __future__ import annotations

import copy
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import verify_polymarket_v3_source_build_v1 as build

HEAD = "a"*40
SOURCE = {"head": HEAD, "files": {"fixture": "b"*64}}
SOURCE_SNAPSHOT = build.source_snapshot
FREE = shutil._ntuple_diskusage(10**12, 0, 10**12)
PROXY = "0xe3333700ca9d93003f00f0f71f8515005f6c00aa"


def word(number): return number.to_bytes(32, "big")
def address(number): return "0x"+f"{number:040x}"


class SourceBuildTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.output = self.root/"out"
        self.binary = self.root/"solc"; self.binary.write_bytes(b"fixture executable never executed\n"); self.binary.chmod(0o700)
        self.binary_pin = patch.object(build, "COMPILER_SHA", build.digest(self.binary.read_bytes())); self.binary_pin.start(); self.addCleanup(self.binary_pin.stop)
        sp = patch.object(build, "source_snapshot", return_value=copy.deepcopy(SOURCE)); self.snapshot = sp.start(); self.addCleanup(sp.stop)
        dp = patch.object(build.shutil, "disk_usage", return_value=FREE); self.disk = dp.start(); self.addCleanup(dp.stop)
        self.pin_patch = patch.object(build, "PINS", copy.deepcopy(build.PINS)); self.pin_patch.start(); self.addCleanup(self.pin_patch.stop)
        self.packages, self.compiled, self.calls = {}, {}, []
        self.runtime = {"schema_version": "polymarket_v3_runtime_probe_v1", "chain_id": 137, "address": PROXY,
            "source_equivalence_admitted": False, "runtime_blobs": {}, "runtime_observations": [], "getters": []}
        self.old, self.new, self.runtime_path = self.root/"old.json", self.root/"new.json", self.root/"runtime.json"
        for era in ("old", "new"): self.make_package(era)
        self.reseal()

    def make_package(self, era):
        pin = build.PINS[era]
        sources = {name: {"content": "// fixture "+name+"\npragma solidity 0.8.34;\n"} for name in sorted(build.SOURCE_NAMES)}
        settings = {"viaIR": False, "metadata": {"appendCBOR": era == "old", "bytecodeHash": "ipfs" if era == "old" else "none", "useLiteralContent": False},
            "optimizer": {"runs": 1000000, "enabled": True}, "evmVersion": "cancun", "remappings": [f"fixture{i}/=literal{i}/" for i in range(12 if era == "old" else 21)]}
        standard = {"sources": sources, "language": "Solidity", "settings": settings}
        args = b"".join((word(111), word(222), word(333), word(1000), word(444), word(555), word(666), word(777)))
        values = {name: args[index*32:(index+1)*32] for name,index in build.ARG_IMMUTABLES.items()}
        values.update({"COLLATERAL_TOKEN": word(888), "__self": word(int(pin["address"],16)), "_cachedThis": word(int(pin["address"],16)),
            "_cachedChainId": word(137), "_cachedNameHash": bytes.fromhex("f30041e9aac4c4d3a1481d2941dfb0a844a72040e9bbc79a810d1ec5b5d6c7af"),
            "_cachedVersionHash": bytes.fromhex("2a80e1ef1d7842f27f2e6be0972bb708b9a135c38860dbe73c27c3486c34f4de"),
            "_cachedDomainSeparator": bytes.fromhex(pin["domain_separator"])})
        counts = [2,2,2,2,2,2,11,12,2,1,1,1,1,12,5]
        refs, transformations, provider_values = {}, [], {}; cursor = 10
        code = bytearray(b"\x60"*2200); observed = bytearray(code)
        asts = {name: {"id": index, "ast": {"nodeType": "SourceUnit", "nodes": []}} for index,name in enumerate(sources)}
        self.names = list(build.IMMUTABLE_TYPES)
        for index, (name,count) in enumerate(zip(self.names,counts),1):
            src,kind = build.IMMUTABLE_TYPES[name]; ident = str(index); locations = []
            asts[src]["ast"]["nodes"].append({"nodeType": "VariableDeclaration", "id": index, "name": name,
                "mutability": "immutable", "stateVariable": True, "typeDescriptions": {"typeString": kind}})
            for _ in range(count):
                locations.append({"start": cursor, "length": 32}); code[cursor:cursor+32] = bytes(32)
                observed[cursor:cursor+32] = values[name]; transformations.append({"id": ident,"type":"replace","offset":cursor,"reason":"immutable"}); cursor += 33
            refs[ident] = locations; provider_values[ident] = "0x"+values[name].hex()
        # Different, literal observed runtimes for the two fixture eras.
        code[-1] = observed[-1] = 1 if era == "old" else 2
        abi = [{"type":"constructor","inputs":[{"name":name,"type":kind} for name,kind in zip(build.ARG_NAMES,build.ARG_TYPES)]}]
        init = b"\x60\x00"+bytes(code)
        package = {"chainId":"137","address":pin["address"], "compilation":{"compilerVersion":build.VERSION,"fullyQualifiedName":build.TARGET+":"+build.CONTRACT},
            "stdJsonInput":standard, "abi":abi,
            "runtimeBytecode":{"onchainBytecode":"0x"+observed.hex(),"recompiledBytecode":"0x"+code.hex(),"immutableReferences":refs,
                "linkReferences":{},"transformations":transformations,"transformationValues":{"immutables":provider_values}},
            "creationBytecode":{"onchainBytecode":"0x"+(init+args).hex(),"recompiledBytecode":"0x"+init.hex(),"linkReferences":{},
                "transformations":[{"type":"insert","offset":len(init),"reason":"constructorArguments"}],"transformationValues":{"constructorArguments":"0x"+args.hex()}}}
        compiled = {"sources":asts,"contracts":{build.TARGET:{build.CONTRACT:{"abi":abi,"metadata":json.dumps({"compiler":{"version":build.VERSION}}),
            "evm":{"bytecode":{"object":init.hex(),"linkReferences":{}},"deployedBytecode":{"object":code.hex(),"linkReferences":{},"immutableReferences":copy.deepcopy(refs)},
                "methodIdentifiers":{"POSITION_MANAGER()":"1bea83fe"}}}}},"errors":[]}
        self.packages[era], self.compiled[era] = package, compiled
        rhash = build.digest(observed); pin.update(runtime_bytes=len(observed), runtime_sha256=rhash)
        self.runtime["runtime_blobs"][rhash] = "0x"+observed.hex()
        self.runtime["runtime_observations"].append({"implementation_address":pin["address"],"runtime_blob_key":rhash,"runtime":{"bytes":len(observed),"sha256":rhash}})
        for name in ("POSITION_MANAGER","COLLATERAL_TOKEN","COMBINATORIAL_MODULE","FEE_RECEIVER","MAX_FEE_RATE"):
            value = int.from_bytes(values[name],"big") if name == "MAX_FEE_RATE" else "0x"+values[name][-20:].hex()
            for kind,target in (("implementation",pin["address"]),("proxy",PROXY)):
                self.runtime["getters"].append({"era":era,"signature":name+"()","target_kind":kind,"target":target,
                    "block_number":100 if era == "old" else 200,"response_sha256":"d"*64,"status":"observed","value":value})

    def reseal(self):
        for era,path in (("old",self.old),("new",self.new)):
            raw = build.encode(self.packages[era]); path.write_bytes(raw)
            build.PINS[era].update(bytes=len(raw),sha256=build.digest(raw),standard_input_sha256=build.digest(build.canonical(self.packages[era]["stdJsonInput"])))
        raw = build.encode(self.runtime); self.assertLess(len(raw),143415)
        self.runtime_path.write_bytes(raw+b" "*(143415-len(raw)))
        pin = patch.object(build,"RUNTIME_SHA",build.digest(self.runtime_path.read_bytes())); pin.start(); self.addCleanup(pin.stop)

    def executor(self, argv, stdin, emit_out, emit_err, timeout, check, process_receipt):
        self.calls.append((argv,stdin,timeout)); check()
        birth = {"pid":123,"start_ticks":456,"pgid":123,"session":123,"boot_id":"12345678-1234-1234-1234-123456789abc"}
        process_receipt(birth)
        if stdin is None: emit_out(build.VERSION_STDOUT)
        else:
            value = json.loads(stdin.read_bytes()); self.assertEqual(value["settings"]["outputSelection"],build.OUTPUT_SELECTION)
            self.assertEqual(set(value["sources"]),build.SOURCE_NAMES); self.assertEqual(argv,[str(self.binary),"--standard-json","--no-import-callback"])
            era = stdin.name.split(".",1)[0]; emit_out(build.encode(self.compiled[era]))
        return {"exit_code":0,"owned_group_drained":True,"birth":birth}

    def execute(self, **kwargs):
        return build.run_verification(self.old,self.new,self.runtime_path,self.binary,HEAD,self.output,executor=kwargs.pop("executor",self.executor),**kwargs)

    def blocked(self, code=None, **kwargs):
        try: result = self.execute(**kwargs)
        except build.Blocked as exc:
            self.assertFalse((self.output/"COMPLETE.json").exists())
            if code: self.assertEqual(str(exc),code)
            return
        self.assertEqual(result["status"],"blocked",result); self.assertFalse((self.output/"COMPLETE.json").exists())
        if code: self.assertEqual(result["error_code"],code)

    def output_contract(self, era="old"):
        return self.compiled[era]["contracts"][build.TARGET][build.CONTRACT]

    def test_two_builds_exact_immutable_match_scope_and_only_fixed_invocations(self):
        result = self.execute(); self.assertEqual(result["status"],"verified_v3_exchange_runtime_source_build_only",result)
        self.assertEqual(len(self.calls),3); self.assertEqual([x[2] for x in self.calls],[10,600,600])
        self.assertEqual(self.calls[0][0],[str(self.binary),"--version"])
        self.assertFalse(result["dependencies_admitted"]); self.assertFalse(result["token_mapping_admitted"])
        self.assertFalse(result["outcome_mapping_admitted"]); self.assertFalse(result["analysis_admitted"]); self.assertFalse(result["onchain_creation_authenticated"])
        self.assertFalse(result["source_content_keccak_independently_verified_here"])
        self.assertIn("SHA256",result["source_binding_basis"])
        for row in result["builds"]:
            self.assertEqual(len(row["typed_immutables"]),15); self.assertEqual(sum(len(v["locations"]) for v in row["typed_immutables"]),58)
            self.assertEqual(len(row["literal_source_hashes"]),26); self.assertTrue(row["full_runtime_exact_match_after_declared_immutables"])
        marker = json.loads((self.output/"COMPLETE.json").read_bytes())
        self.assertEqual(marker["result"]["sha256"],build.digest((self.output/"RESULT.json").read_bytes()))
        self.assertLessEqual(sum(p.stat().st_size for p in self.output.iterdir()),build.CAPS["all_artifact_bytes"])

    def test_wrong_binary_hash_blocks_before_any_execution(self):
        self.binary.write_bytes(b"wrong executable")
        self.blocked("compiler_sha_or_identity_changed"); self.assertEqual(self.calls,[])

    def test_nonexecutable_or_symlink_binary_is_not_executed(self):
        self.binary.chmod(0o600); self.blocked("compiler_not_bounded_executable")
        self.binary.chmod(0o700); actual=self.root/"actual"; self.binary.rename(actual); self.binary.symlink_to(actual)
        self.blocked("input_not_literal_absolute"); self.assertEqual(self.calls,[])

    def test_wrong_response_hash_blocks_before_execution(self):
        raw=self.old.read_bytes(); self.old.write_bytes(raw[:-1]+b" ")
        self.blocked("input_sha_or_identity_changed"); self.assertEqual(self.calls,[])

    def test_missing_literal_imports_rejected(self):
        self.packages["old"]["stdJsonInput"]["sources"].pop(next(iter(build.SOURCE_NAMES)))
        self.reseal(); self.blocked("literal_source_roster"); self.assertEqual(self.calls,[])

    def test_url_only_imports_rejected(self):
        self.packages["old"]["stdJsonInput"]["sources"][build.TARGET]={"urls":["https://never-follow.invalid"]}
        self.reseal(); self.blocked("source_not_literal_content"); self.assertEqual(self.calls,[])

    def test_settings_extra_flags_or_optimizer_change_rejected(self):
        self.packages["old"]["stdJsonInput"]["settings"]["optimizer"]["runs"]=200
        self.reseal(); self.blocked("compiler_settings_differ")

    def test_wrong_version_stops_before_builds(self):
        def fake(argv,stdin,out,err,timeout,check,process):
            process({"fixture":True});out(b"Version: unexpected\n");return {"exit_code":0,"owned_group_drained":True}
        self.blocked("compiler_version_differs",executor=fake)

    def test_solc_json_errors_block_even_with_zero_exit(self):
        self.compiled["old"]["errors"]=[{"severity":"error","message":"missing import"}]
        self.blocked("compiler_json_error"); self.assertEqual(len(self.calls),2)
        self.assertIn(b"missing import",(self.output/"old.stdout").read_bytes())

    def test_nonzero_or_undrained_child_blocks(self):
        def fake(*args):
            result=self.executor(*args);result["owned_group_drained"]=False;return result
        self.blocked("compiler_exit_or_owned_group_not_drained",executor=fake)

    def test_compiler_link_references_rejected(self):
        self.output_contract()["evm"]["deployedBytecode"]["linkReferences"]={"library.sol":{"L":[{"start":1,"length":20}]}}
        self.blocked("library_linking_not_admitted")

    def test_independent_bytecode_difference_rejected(self):
        self.output_contract()["evm"]["deployedBytecode"]["object"]="61"+self.output_contract()["evm"]["deployedBytecode"]["object"][2:]
        self.blocked("independent_bytecode_differs")

    def test_undeclared_nonimmutable_difference_not_masked(self):
        value="61"+self.output_contract()["evm"]["deployedBytecode"]["object"][2:]
        self.output_contract()["evm"]["deployedBytecode"]["object"]=value
        self.packages["old"]["runtimeBytecode"]["recompiledBytecode"]="0x"+value
        self.reseal(); self.blocked("full_runtime_comparison_differs")

    def test_extra_transformation_rejected(self):
        self.packages["old"]["runtimeBytecode"]["transformations"].append({"id":"1","offset":0,"type":"replace","reason":"cborAuxdata"})
        self.reseal(); self.blocked("undeclared_runtime_transformation")

    def test_missing_changed_or_overlapping_immutable_references_rejected(self):
        self.output_contract()["evm"]["deployedBytecode"]["immutableReferences"]["1"][0]["start"]+=1
        self.blocked("independent_immutable_references_differ")

    def test_wrong_ast_declaration_type_rejected(self):
        node=self.compiled["old"]["sources"][build.SELF_SOURCE]["ast"]["nodes"][0]
        node["typeDescriptions"]["typeString"]="address"
        self.blocked("immutable_declaration_or_type")

    def test_missing_ast_blocks(self):
        self.compiled["old"]["sources"][build.SELF_SOURCE].pop("ast")
        self.blocked("compiled_ast_missing")

    def test_getter_mismatch_blocks(self):
        self.runtime["getters"][0]["value"]=address(999)
        self.reseal(); self.blocked("immutable_getter_differs")

    def test_constructor_provider_inconsistency_blocks_without_claiming_deployment(self):
        self.packages["old"]["creationBytecode"]["transformations"][0]["offset"]-=1
        self.reseal(); self.blocked("reported_creation_consistency_differs")

    def test_stdout_and_stderr_limits_keep_only_bounded_prefixes(self):
        def fake(argv,stdin,out,err,timeout,check,process):
            process({"fixture":True});out(b"x"*20);return {"exit_code":0,"owned_group_drained":True}
        with patch.dict(build.CAPS,version_stdout_bytes=10): self.blocked("compiler_stdout_limit",executor=fake)
        self.assertEqual((self.output/"version.stdout").stat().st_size,10)
        shutil.rmtree(self.output)
        def fake_err(argv,stdin,out,err,timeout,check,process):
            process({"fixture":True});err(b"x"*20);return {"exit_code":0,"owned_group_drained":True}
        with patch.dict(build.CAPS,stderr_bytes=10): self.blocked("compiler_stderr_limit",executor=fake_err)
        self.assertEqual((self.output/"version.stderr").stat().st_size,10)

    def test_aggregate_artifact_cap_and_free_floor(self):
        with patch.dict(build.CAPS,all_artifact_bytes=100): self.blocked("artifact_byte_limit")
        shutil.rmtree(self.output)
        self.disk.return_value=shutil._ntuple_diskusage(1,0,1)
        self.blocked("free_space_floor");self.assertFalse(self.output.exists())

    def test_timeout_and_interrupt_preserve_exit_and_partial_stdout(self):
        def timed(argv,stdin,out,err,timeout,check,process):
            process({"fixture":True});out(b"partial")
            exc=build.Blocked("compiler_child_timeout");exc.compiler_receipt={"exit_code":-15,"owned_group_drained":True,"birth":{"fixture":True}};raise exc
        self.blocked("compiler_child_timeout",executor=timed)
        self.assertEqual((self.output/"version.stdout").read_bytes(),b"partial")
        self.assertTrue(json.loads((self.output/"version.EXIT.json").read_bytes())["owned_group_drained"])
        shutil.rmtree(self.output)
        def interrupted(argv,stdin,out,err,timeout,check,process):
            process({"fixture":True});out(b"partial")
            exc=KeyboardInterrupt();exc.compiler_receipt={"exit_code":None,"owned_group_drained":False,"birth":None};raise exc
        with self.assertRaises(KeyboardInterrupt): self.execute(executor=interrupted)
        self.assertFalse((self.output/"COMPLETE.json").exists());self.assertTrue((self.output/"FAILED.json").exists())

    def test_strict_json_duplicates_nonfinite_and_invalid_payload(self):
        for raw in (b'{"x":1,"x":2}',b'{"x":NaN}',b'{"x":1e9999}',b'compiler status\n{}'):
            with self.subTest(raw=raw),self.assertRaises(build.Blocked):build.decode(raw)

    def test_existing_output_preserved(self):
        self.output.mkdir();(self.output/"keep").write_text("user")
        self.blocked("output_not_new_literal_directory");self.assertEqual((self.output/"keep").read_text(),"user")

    def test_late_inputs_compiler_source_outputs_and_marker_withhold_complete(self):
        original_write,original_fsync=build.Artifacts.write,build.os.fsync
        for phase in ("result","pending"):
            for target in ("input","runtime","compiler","source","proof","marker"):
                with self.subTest(phase=phase,target=target):
                    if self.output.exists():shutil.rmtree(self.output)
                    self.binary.write_bytes(b"fixture executable never executed\n");self.binary.chmod(0o700)
                    self.reseal();self.snapshot.return_value=copy.deepcopy(SOURCE);changed=[False]
                    def mutate():
                        if changed[0]:return
                        changed[0]=True
                        if target=="source":self.snapshot.return_value={"head":"f"*40};return
                        path={"input":self.old,"runtime":self.runtime_path,"compiler":self.binary,
                              "proof":self.output/"old.proof.json","marker":self.output/".COMPLETE.json.pending"}[target]
                        raw=path.read_bytes();path.write_bytes(raw[:-1]+(b" " if raw[-1:]!=b" " else b"\n"))
                    def written(writer,name,raw,**kwargs):
                        result=original_write(writer,name,raw,**kwargs)
                        if phase=="result" and name=="RESULT.json" and target!="marker":mutate()
                        return result
                    def fsync(fd):
                        result=original_fsync(fd)
                        if (phase=="pending" or target=="marker") and (self.output/".COMPLETE.json.pending").exists():mutate()
                        return result
                    with patch.object(build.Artifacts,"write",written),patch.object(build.os,"fsync",fsync):self.blocked()
                    self.assertTrue(changed[0]);self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_pending_file_directory_fsync_and_rename_faults_withhold_complete(self):
        for op in ("file","dir","rename"):
            with self.subTest(op=op):
                if self.output.exists():shutil.rmtree(self.output)
                original_fsync,original_commit=build.os.fsync,build.atomic_commit
                def fsync(fd):
                    if (self.output/".COMPLETE.json.pending").exists() and op in {"file","dir"}:
                        if (op=="dir")==stat.S_ISDIR(os.fstat(fd).st_mode):raise OSError("fault")
                    return original_fsync(fd)
                def commit(pending,target):
                    if op=="rename" and target.name=="COMPLETE.json":raise OSError("fault")
                    return original_commit(pending,target)
                with patch.object(build.os,"fsync",fsync),patch.object(build,"atomic_commit",commit):self.blocked()
                self.assertTrue((self.output/".COMPLETE.json.pending").exists())

    def test_no_fallible_artifact_io_after_complete(self):
        original_commit,original_open=build.atomic_commit,Path.open;done=[False]
        def commit(pending,target):
            result=original_commit(pending,target)
            if target.name=="COMPLETE.json":done[0]=True
            return result
        def opened(path,*args,**kwargs):
            if done[0]:raise AssertionError("artifact I/O after COMPLETE")
            return original_open(path,*args,**kwargs)
        with patch.object(build,"atomic_commit",commit),patch.object(Path,"open",opened):result=self.execute()
        self.assertEqual(result["status"],"verified_v3_exchange_runtime_source_build_only",result)

    def test_staged_marker_whole_deadline(self):
        clock=lambda:1801 if (self.output/".COMPLETE.json.pending").exists() else 0
        self.blocked("whole_runtime_limit",clock=clock)

    def test_cli_guard_precedes_inputs_and_no_extra_flags(self):
        args=["--old-source-json","/missing-old","--new-source-json","/missing-new","--runtime-observation","/missing-runtime",
              "--solc","/missing-solc","--expected-head",HEAD,"--output-dir","/missing-output"]
        with patch.object(build,"require_production_host",side_effect=RuntimeError("guard")),patch.object(build,"run_verification",side_effect=AssertionError("input read")):
            with self.assertRaisesRegex(RuntimeError,"guard"):build.main(args)
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):build.main(args+["--max-runtime","99999"])

    @contextlib.contextmanager
    def mocked_native_process(self, *, invalid_birth=False, constructor_error=False, changed_birth=False,
                              advances=None, empty_selector=False):
        """Exercise ownership logic without spawning or signalling anything."""
        advances=advances or {};clock=[0.0]
        class Pipe:
            def fileno(self):return 10
            def close(self):pass
        class Process:
            pid=123
            returncode=None
            stdout,stderr=Pipe(),Pipe()
            def poll(self):return self.returncode
            def wait(self,timeout):
                clock[0]+=advances.get("wait",0);self.returncode=-15;return self.returncode
        class Selector:
            def __init__(self):self.items={};clock[0]+=advances.get("selector",0)
            def register(self,stream,event,emit):self.items[id(stream)]=(stream,emit);clock[0]+=advances.get("register",0)
            def get_map(self):return {} if empty_selector else self.items
            def select(self,timeout):
                clock[0]+=advances.get("select",0)
                return [(SimpleNamespace(fd=stream.fileno(),data=emit,fileobj=stream),1) for stream,emit in self.items.values()]
            def unregister(self,stream):self.items.pop(id(stream))
            def close(self):pass
        process=Process();birth={"pid":123,"start_ticks":456,"pgid":999 if invalid_birth else 123,"session":123,
            "boot_id":"12345678-1234-1234-1234-123456789abc"}
        process.fixture_clock=clock;birth_calls=[0]
        def births(_):
            birth_calls[0]+=1;clock[0]+=advances.get("birth",0)
            return {**birth,"start_ticks":999} if changed_birth and birth_calls[0]>1 else birth
        def spawn(*args,**kwargs):
            clock[0]+=advances.get("spawn",0)
            if constructor_error:raise OSError("constructor")
            return process
        def absent(_):clock[0]+=advances.get("drain",0);return True
        def read(fd,size):clock[0]+=advances.get("read",0);return b"x"
        def read_text(path,*args,**kwargs):
            if str(path)=="/proc/self/statm":return "1 2 3"
            raise AssertionError("unexpected /proc read")
        with (patch.object(build.subprocess,"Popen",side_effect=spawn) as popen,
              patch.object(build,"process_birth",side_effect=births),
              patch.object(build,"group_absent",side_effect=absent),
              patch.object(build.time,"monotonic",side_effect=lambda:clock[0]),
              patch.object(build.resource,"getrlimit",return_value=(build.CAPS["address_space_bytes"],-1)),
              patch.object(build.resource,"setrlimit") as limits,
              patch.object(Path,"read_text",read_text),
              patch.object(build.os,"set_blocking"),patch.object(build.os,"killpg") as signals,
              patch.object(build.os,"read",side_effect=read),
              patch.object(build.selectors,"DefaultSelector",Selector)):
            yield process,popen,limits,signals

    def test_native_unknown_birth_never_signals_and_keeps_parent_reservation(self):
        with self.mocked_native_process(invalid_birth=True) as (_,_,limits,signals):
            with self.assertRaises(build.Blocked) as captured:
                build.real_executor([str(self.binary),"--version"],None,lambda _:None,lambda _:None,10,lambda:None,lambda _:None)
            self.assertEqual(str(captured.exception),"compiler_birth_not_bound")
            self.assertIsNone(captured.exception.compiler_receipt["birth"])
            self.assertFalse(captured.exception.compiler_receipt["owned_group_drained"])
            signals.assert_not_called();self.assertEqual(limits.call_count,1)
            self.assertEqual(limits.call_args.args[1][0],build.CAPS["parent_live_bytes"])

    def test_native_constructor_failure_is_unknown_not_idle(self):
        with self.mocked_native_process(constructor_error=True) as (_,_,limits,signals):
            with self.assertRaises(OSError) as captured:
                build.real_executor([str(self.binary)],None,lambda _:None,lambda _:None,10,lambda:None,lambda _:None)
            self.assertFalse(captured.exception.compiler_receipt["owned_group_drained"])
            signals.assert_not_called();self.assertEqual(limits.call_count,1)

    def test_native_timeout_signals_only_reopened_owned_group_and_drains(self):
        with self.mocked_native_process() as (_,popen,limits,signals):
            def timeout():
                if popen.called:raise build.Blocked("compiler_child_timeout")
            with self.assertRaisesRegex(build.Blocked,"compiler_child_timeout") as captured:
                build.real_executor([str(self.binary)],None,lambda _:None,lambda _:None,10,timeout,lambda _:None)
            signals.assert_called_once_with(123,build.signal.SIGTERM)
            self.assertTrue(captured.exception.compiler_receipt["owned_group_drained"])
            self.assertEqual(limits.call_args.args[1][0],build.CAPS["address_space_bytes"])
            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            self.assertIs(popen.call_args.kwargs["preexec_fn"],build.child_limits)

    def test_native_changed_birth_blocks_signalling(self):
        with self.mocked_native_process(changed_birth=True) as (_,popen,limits,signals):
            def timeout():
                if popen.called:raise build.Blocked("compiler_child_timeout")
            with self.assertRaises(build.Blocked) as captured:
                build.real_executor([str(self.binary)],None,lambda _:None,lambda _:None,10,timeout,lambda _:None)
            signals.assert_not_called();self.assertFalse(captured.exception.compiler_receipt["owned_group_drained"])
            self.assertEqual(limits.call_count,1)

    def test_constructor_elapsed_time_is_inclusive_for_build_and_version(self):
        for timeout in (600,10):
            with self.subTest(timeout=timeout),self.mocked_native_process(advances={"spawn":timeout+1},empty_selector=True) as (_,_,_,signals):
                with self.assertRaisesRegex(build.Blocked,"compiler_child_timeout") as captured:
                    build.real_executor([str(self.binary)],None,lambda _:None,lambda _:None,timeout,lambda:None,lambda _:None)
                self.assertGreater(captured.exception.compiler_receipt["elapsed_seconds"],timeout)
                self.assertTrue(captured.exception.compiler_receipt["owned_group_drained"])
                self.assertIsNotNone(captured.exception.compiler_receipt["birth"])
                signals.assert_called_once_with(123,build.signal.SIGTERM)

    def test_birth_receipt_selector_wait_and_drain_elapsed_time_are_inclusive(self):
        for stage in ("birth","receipt","selector","register","wait","drain"):
            advances={stage:601} if stage!="receipt" else {}
            with self.subTest(stage=stage),self.mocked_native_process(advances=advances,empty_selector=True) as (process,_,_,_):
                def receipt(_):
                    if stage=="receipt":process.fixture_clock[0]=601
                with self.assertRaisesRegex(build.Blocked,"compiler_child_timeout") as captured:
                    build.real_executor([str(self.binary)],None,lambda _:None,lambda _:None,600,lambda:None,receipt)
                self.assertTrue(captured.exception.compiler_receipt["owned_group_drained"])

    def test_expired_constructor_with_unknown_birth_still_never_signals(self):
        with self.mocked_native_process(invalid_birth=True,advances={"spawn":601},empty_selector=True) as (_,_,limits,signals):
            with self.assertRaisesRegex(build.Blocked,"compiler_birth_not_bound") as captured:
                build.real_executor([str(self.binary)],None,lambda _:None,lambda _:None,600,lambda:None,lambda _:None)
            signals.assert_not_called();self.assertFalse(captured.exception.compiler_receipt["owned_group_drained"])
            self.assertEqual(limits.call_count,1)

    def test_pipe_select_read_and_emit_time_are_inclusive(self):
        for stage in ("select","read","emit"):
            advances={stage:601} if stage!="emit" else {}
            with self.subTest(stage=stage),self.mocked_native_process(advances=advances) as (process,_,_,signals):
                def emitted(_):
                    if stage=="emit":process.fixture_clock[0]=601
                with self.assertRaisesRegex(build.Blocked,"compiler_child_timeout") as captured:
                    build.real_executor([str(self.binary)],None,emitted,emitted,600,lambda:None,lambda _:None)
                self.assertTrue(captured.exception.compiler_receipt["owned_group_drained"])
                signals.assert_called_once_with(123,build.signal.SIGTERM)

    def test_inclusive_child_ram_and_file_ceiling(self):
        with patch.object(build.resource,"setrlimit") as limits:build.child_limits()
        expected=build.CAPS["address_space_bytes"]-build.CAPS["parent_live_bytes"]
        self.assertIn(((build.resource.RLIMIT_AS,(expected,expected)),),[(x.args,) for x in limits.call_args_list])
        self.assertEqual(expected+build.CAPS["parent_live_bytes"],8*1024**3)
        self.assertIn(((build.resource.RLIMIT_FSIZE,(16*1024**2,16*1024**2)),),[(x.args,) for x in limits.call_args_list])

    def test_source_blob_checks_not_just_clean_state(self):
        state={"status":"inspected","head":HEAD,"tracked_worktree_clean":True}
        stale=[False]
        def git(argv,**kwargs):
            name=argv[-1].split(":",1)[1];raw=(build.REPO/name).read_bytes()
            if argv[2]=="-s":return str(len(raw)).encode()
            return raw[:-1]+b" " if stale[0] else raw
        with patch.object(build.shared,"code_state",return_value=state),patch.object(build.subprocess,"check_output",side_effect=git):
            result=SOURCE_SNAPSHOT(HEAD);self.assertEqual(set(result["files"]),set(build.SOURCE_FILES))
            stale[0]=True
            with self.assertRaisesRegex(build.Blocked,"source_blob_differs"):SOURCE_SNAPSHOT(HEAD)


if __name__=="__main__":unittest.main()
