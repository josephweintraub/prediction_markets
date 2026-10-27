"""Reproduce two pinned V3 Exchange builds; no dependency or sample admission.

The production CLI executes only the authenticated literal solc binary, with
literal standard JSON and import callbacks disabled. Fixture callers inject a
compiler seam; this module never downloads compilers, sources or chain data.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
import gc
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import inventory_polymarket_refresh as shared

SOURCE_FILES = ("production_guard.py", "scripts/inventory_polymarket_refresh.py",
                "scripts/verify_polymarket_v3_source_build_v1.py", "tests/test_polymarket_v3_source_build_v1.py")
COMPILER_SHA = "d40adc6f9fdbb22a97d32a02fa05688bf2ee7886affc48c9851b0afd4a726b39"
VERSION = "0.8.34+commit.80d5c536"
VERSION_STDOUT = ("solc, the solidity compiler commandline interface\nVersion: " + VERSION + ".Linux.g++\n").encode()
RUNTIME_SHA = "761eb05565361d5303e14a4b0fceb21dbc5b18b6181879ce3aea3f53cf1f156b"
PINS = {
    "old": {"bytes": 764915, "sha256": "2fa08be23b657b7071abbf89d0e482146a93b521e97142d29320094f174241c0",
            "standard_input_sha256": "555634593c6f3cbcbcd33f3255bec9dc4b1908a698207cde3209584cec47d388",
            "address": "0x7345c6842b244926125ed4054905cac49620b5dc", "runtime_bytes": 22204,
            "runtime_sha256": "dda5fcc0be82f96ae766bf269b5c8f87871cf458fac18423b265ac57123901fb",
            "domain_separator": "d54af932f4b26d9ec1db0c9bc2150cd8e8aff0c0cadc9549e81c72ec9764f9e5"},
    "new": {"bytes": 789270, "sha256": "47e25bfebfe8082f8edf183e401c28307c8ef9eadf748c16494d2dbf1fdc5bf9",
            "standard_input_sha256": "bc24c00475b87a587508eb2848de7e0fe6d46bdb957dbea15969a22eeaa19872",
            "address": "0x641b40ec414a076b9e79e703fc7bf4ebec248bb7", "runtime_bytes": 22256,
            "runtime_sha256": "2dfc1d2e867a7fa851cb195cc7887b614442f4f2de04d3081155a99bd502c6d6",
            "domain_separator": "a5d3dd1900288edb5cccae90b117ef1aa2a910eb05703ab4509fd88fdaa58a4b"}}
SOURCE_NAMES = {
    "src/libraries/Ids.sol", "src/exchange/Exchange.sol", "src/libraries/ModuleIds.sol", "src/exchange/OrderStructs.sol",
    "lib/solady/src/auth/Ownable.sol", "lib/solady/src/tokens/ERC20.sol", "lib/solady/src/utils/EIP712.sol",
    "src/auth/InitializableRoles.sol", "lib/solady/src/tokens/ERC1155.sol", "src/collateral/CollateralToken.sol",
    "src/modules/CombinatorialModule.sol", "src/modules/abstract/BaseModule.sol", "lib/solady/src/auth/OwnableRoles.sol",
    "src/abstract/ERC1155TokenReceiver.sol", "src/modules/abstract/ModuleErrors.sol", "src/modules/abstract/OracleModule.sol",
    "lib/solady/src/utils/Initializable.sol", "src/positionManager/PositionManager.sol", "lib/solady/src/utils/SafeTransferLib.sol",
    "lib/solady/src/utils/UUPSUpgradeable.sol", "src/oracle/interfaces/IBinaryReporter.sol",
    "lib/solady/src/utils/FixedPointMathLib.sol", "lib/solady/src/utils/CallContextChecker.sol",
    "src/collateral/abstract/CollateralErrors.sol", "src/collateral/interfaces/ICollateralToken.sol",
    "src/positionManager/IPositionManagerModule.sol"}
TARGET, CONTRACT = "src/exchange/Exchange.sol", "Exchange"
OUTPUT_SELECTION = {"*": {"": ["ast"]}, TARGET: {CONTRACT: ["abi", "metadata", "evm.bytecode.object",
    "evm.bytecode.linkReferences", "evm.deployedBytecode.object", "evm.deployedBytecode.linkReferences",
    "evm.deployedBytecode.immutableReferences", "evm.methodIdentifiers"]}}
CAPS = {"all_artifact_bytes": 64 * 1024**2, "control_reserve_bytes": 1024**2,
        "stdout_bytes": 16 * 1024**2, "stderr_bytes": 64 * 1024, "version_stdout_bytes": 64 * 1024,
        "compiler_invocations": 2, "version_checks": 1, "child_seconds": 600, "version_seconds": 10,
        "runtime_seconds": 1800, "address_space_bytes": 8 * 1024**3, "parent_live_bytes": 512 * 1024**2,
        "minimum_free_bytes": 20_000_000_000, "spill_bytes": 0, "api_calls": 0}
SHA = re.compile(r"[0-9a-f]{64}")
HEX = re.compile(r"(?:[0-9a-fA-F]{2})*")
HEAD = re.compile(r"[0-9a-f]{40}")
UUID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")
STAT_KEYS = ("device", "inode", "mode", "uid", "gid", "bytes", "mtime_ns", "ctime_ns")
EIP_SOURCE = "lib/solady/src/utils/EIP712.sol"
SELF_SOURCE = "lib/solady/src/utils/CallContextChecker.sol"
IMMUTABLE_TYPES = {"__self": (SELF_SOURCE, "uint256"), "_cachedThis": (EIP_SOURCE, "uint256"),
    "_cachedChainId": (EIP_SOURCE, "uint256"), "_cachedNameHash": (EIP_SOURCE, "bytes32"),
    "_cachedVersionHash": (EIP_SOURCE, "bytes32"), "_cachedDomainSeparator": (EIP_SOURCE, "bytes32"),
    "POSITION_MANAGER": (TARGET, "contract PositionManager"), "COLLATERAL_TOKEN": (TARGET, "address"),
    "COMBINATORIAL_MODULE": (TARGET, "address"), "PROXY_FACTORY": (TARGET, "address"),
    "SAFE_FACTORY": (TARGET, "address"), "PROXY_BYTECODE_HASH": (TARGET, "bytes32"),
    "SAFE_BYTECODE_HASH": (TARGET, "bytes32"), "FEE_RECEIVER": (TARGET, "address"), "MAX_FEE_RATE": (TARGET, "uint256")}
ARG_NAMES = ("_positionManager", "_combinatorialModule", "_feeReceiver", "_maxFeeRate", "_proxyFactory",
             "_safeFactory", "_proxyBytecodeHash", "_safeBytecodeHash")
ARG_TYPES = ("address", "address", "address", "uint256", "address", "address", "bytes32", "bytes32")
ARG_IMMUTABLES = {"POSITION_MANAGER": 0, "COMBINATORIAL_MODULE": 1, "FEE_RECEIVER": 2, "MAX_FEE_RATE": 3,
                  "PROXY_FACTORY": 4, "SAFE_FACTORY": 5, "PROXY_BYTECODE_HASH": 6, "SAFE_BYTECODE_HASH": 7}


class Blocked(ValueError):
    pass


def require(ok, code):
    if not ok: raise Blocked(code)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def encode(value):
    return canonical(value) + b"\n"


def decode(raw):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            require(key not in value, "duplicate_json_key"); value[key] = item
        return value
    def finite(value):
        result = float(value); require(math.isfinite(result), "nonfinite_json"); return result
    try:
        return json.loads(raw.decode(), object_pairs_hook=unique, parse_float=finite,
            parse_constant=lambda _: (_ for _ in ()).throw(Blocked("nonfinite_json")))
    except Blocked: raise
    except (ValueError, UnicodeDecodeError, RecursionError, OverflowError): raise Blocked("invalid_json") from None


def identity(path):
    require(path.is_absolute() and path.resolve() == path, "input_not_literal_absolute")
    s = path.lstat()
    require(stat.S_ISREG(s.st_mode) and s.st_nlink == 1, "input_not_single_regular_file")
    return dict(zip(STAT_KEYS, (s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_gid, s.st_size, s.st_mtime_ns, s.st_ctime_ns)))


def bound_read(path, sha, maximum=1024**2, size=None):
    require(type(sha) is str and SHA.fullmatch(sha), "invalid_input_sha")
    before = identity(path)
    require(0 < before["bytes"] <= maximum and (size is None or before["bytes"] == size), "input_byte_limit")
    with path.open("rb") as stream: raw = stream.read(before["bytes"] + 1)
    require(len(raw) == before["bytes"] and digest(raw) == sha and identity(path) == before, "input_sha_or_identity_changed")
    return raw, {"path": str(path), "sha256": sha, "identity": before}


def compiler_binding(path, check):
    before = identity(path)
    require(0 < before["bytes"] <= 64 * 1024**2 and before["mode"] & 0o111, "compiler_not_bounded_executable")
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        while raw := stream.read(64 * 1024): check(); hasher.update(raw)
    require(hasher.hexdigest() == COMPILER_SHA and identity(path) == before, "compiler_sha_or_identity_changed")
    return {"path": str(path), "sha256": COMPILER_SHA, "identity": before}


def source_snapshot(head):
    require(type(head) is str and HEAD.fullmatch(head), "source_head_invalid")
    state = shared.code_state()
    require(state.get("status") == "inspected" and state.get("head") == head
            and state.get("tracked_worktree_clean") is True, "source_not_clean_bound_head")
    files = {}
    for name in SOURCE_FILES:
        path = REPO / name; before = identity(path)
        require(0 < before["bytes"] <= 1024**2, "source_size_limit")
        size = subprocess.check_output(["git", "cat-file", "-s", head + ":" + name], cwd=REPO,
                                       stderr=subprocess.DEVNULL, timeout=10).strip()
        require(size.isdigit() and int(size) == before["bytes"], "source_blob_size_differs")
        raw = subprocess.check_output(["git", "cat-file", "blob", head + ":" + name], cwd=REPO,
                                      stderr=subprocess.DEVNULL, timeout=10)
        require(raw == path.read_bytes() and identity(path) == before, "source_blob_differs")
        files[name] = digest(raw)
    require(shared.code_state() == state, "source_changed_during_snapshot")
    return {"head": head, "files": files}


def hex_bytes(value, *, prefix=True):
    require(type(value) is str and (not prefix or value.startswith("0x")), "bytecode_hex_schema")
    value = value[2:] if prefix else value
    require(HEX.fullmatch(value) is not None and len(value) <= 128 * 1024, "bytecode_hex_schema")
    return bytes.fromhex(value)


def package_input(package, era):
    pin = PINS[era]; value = package.get("stdJsonInput")
    require(package.get("chainId") == "137" and package.get("address", "").lower() == pin["address"]
            and package.get("compilation", {}).get("compilerVersion") == VERSION
            and package["compilation"].get("fullyQualifiedName") == TARGET + ":" + CONTRACT, "source_package_identity")
    require(type(value) is dict and set(value) == {"sources", "language", "settings"}
            and value["language"] == "Solidity" and digest(canonical(value)) == pin["standard_input_sha256"], "standard_input_binding")
    require(type(value["sources"]) is dict and set(value["sources"]) == SOURCE_NAMES and len(SOURCE_NAMES) == 26,
            "literal_source_roster")
    sources = []
    for name, item in sorted(value["sources"].items()):
        require(type(item) is dict and set(item) == {"content"} and type(item["content"]) is str, "source_not_literal_content")
        raw = item["content"].encode(); sources.append({"path": name, "bytes": len(raw), "sha256": digest(raw)})
    require(sum(row["bytes"] for row in sources) <= 512 * 1024, "literal_source_bytes")
    settings = value["settings"]
    require(set(settings) == {"viaIR", "metadata", "optimizer", "evmVersion", "remappings"}
            and settings["viaIR"] is False and settings["optimizer"] == {"runs": 1000000, "enabled": True}
            and settings["evmVersion"] == "cancun" and settings["metadata"] == {"appendCBOR": era == "old",
                "bytecodeHash": "ipfs" if era == "old" else "none", "useLiteralContent": False}
            and len(settings["remappings"]) == (12 if era == "old" else 21), "compiler_settings_differ")
    augmented = {**value, "settings": {**settings, "outputSelection": OUTPUT_SELECTION}}
    return augmented, sources


def declarations(compiled):
    require(type(compiled.get("sources")) is dict and set(compiled["sources"]) == SOURCE_NAMES, "compiled_source_roster")
    result = {}
    for name, source in compiled["sources"].items():
        require(type(source.get("ast")) is dict and source["ast"].get("nodeType") == "SourceUnit", "compiled_ast_missing")
        stack = [source["ast"]]
        while stack:
            node = stack.pop()
            if type(node) is dict:
                if node.get("nodeType") == "VariableDeclaration" and node.get("mutability") == "immutable":
                    ident = node.get("id")
                    require(type(ident) is int and ident >= 0 and str(ident) not in result, "immutable_ast_id")
                    result[str(ident)] = {"source_path": name, "name": node.get("name"), "type": node.get("typeDescriptions", {}).get("typeString"),
                                          "state_variable": node.get("stateVariable")}
                stack.extend(node.values())
            elif type(node) is list: stack.extend(node)
    return result


def verify_build(compiled, package, runtime, era):
    errors = compiled.get("errors", [])
    require(type(errors) is list and all(type(row) is dict and row.get("severity") in {"warning", "info", "error"} for row in errors), "compiler_error_schema")
    require(not any(row["severity"] == "error" for row in errors), "compiler_json_error")
    output = compiled["contracts"][TARGET][CONTRACT]
    require(output.get("abi") == package.get("abi"), "compiled_abi_differs")
    evm = output["evm"]; bytecode, deployed = evm["bytecode"], evm["deployedBytecode"]
    require(bytecode.get("linkReferences") == deployed.get("linkReferences") == package["creationBytecode"].get("linkReferences")
            == package["runtimeBytecode"].get("linkReferences") == {}, "library_linking_not_admitted")
    init = hex_bytes(bytecode["object"], prefix=False); code = hex_bytes(deployed["object"], prefix=False)
    require(init == hex_bytes(package["creationBytecode"]["recompiledBytecode"])
            and code == hex_bytes(package["runtimeBytecode"]["recompiledBytecode"]), "independent_bytecode_differs")
    pin = PINS[era]; observed = hex_bytes(runtime["runtime_blobs"][pin["runtime_sha256"]])
    require(len(observed) == pin["runtime_bytes"] and digest(observed) == pin["runtime_sha256"]
            and observed == hex_bytes(package["runtimeBytecode"]["onchainBytecode"]), "rpc_runtime_differs")
    require(any(row.get("implementation_address") == pin["address"] and row.get("runtime_blob_key") == pin["runtime_sha256"]
                and row.get("runtime") == {"bytes": len(observed), "sha256": pin["runtime_sha256"]}
                for row in runtime.get("runtime_observations", [])), "runtime_observation_binding_missing")
    refs = deployed.get("immutableReferences")
    require(type(refs) is dict and len(refs) == 15 and refs == package["runtimeBytecode"].get("immutableReferences"), "independent_immutable_references_differ")
    ast = declarations(compiled); result, used_names, sites, transformed, transformations = [], set(), set(), bytearray(code), set()
    provider_values = package["runtimeBytecode"]["transformationValues"]["immutables"]
    require(set(provider_values) == set(refs), "provider_immutable_value_roster")
    constructor = [item for item in output["abi"] if item.get("type") == "constructor"]
    require(len(constructor) == 1 and [(v.get("name"), v.get("type")) for v in constructor[0]["inputs"]]
            == list(zip(ARG_NAMES, ARG_TYPES)), "constructor_schema")
    constructor_args = hex_bytes(package["creationBytecode"]["transformationValues"]["constructorArguments"])
    require(len(constructor_args) == 256 and package["creationBytecode"].get("transformations") == [
        {"type": "insert", "offset": len(init), "reason": "constructorArguments"}]
        and init + constructor_args == hex_bytes(package["creationBytecode"]["onchainBytecode"]), "reported_creation_consistency_differs")
    for ident, locations in refs.items():
        require(type(ident) is str and ident.isdecimal() and str(int(ident)) == ident and ident in ast
                and type(locations) is list and locations, "immutable_id_or_locations")
        declaration = ast[ident]; name = declaration["name"]
        require(name in IMMUTABLE_TYPES and name not in used_names and declaration["state_variable"] is True
                and (declaration["source_path"], declaration["type"]) == IMMUTABLE_TYPES[name], "immutable_declaration_or_type")
        used_names.add(name); values = []
        for location in locations:
            require(type(location) is dict and set(location) == {"start", "length"}
                    and type(location["start"]) is int and location["length"] == 32
                    and 0 <= location["start"] <= len(code) - 32, "immutable_location_schema")
            start = location["start"]
            require(all(i not in sites for i in range(start, start+32)) and code[start:start+32] == bytes(32), "immutable_overlap_or_nonzero_placeholder")
            sites.update(range(start, start+32)); value = observed[start:start+32]; values.append(value)
            transformed[start:start+32] = value; transformations.add((ident, start))
        require(len(set(values)) == 1 and values[0] == hex_bytes(provider_values[ident]) and len(values[0]) == 32, "immutable_values_inconsistent")
        value = values[0]; address_type = declaration["type"] in {"address", "contract PositionManager"}
        require(not address_type or value[:12] == bytes(12), "immutable_address_dirty_bits")
        if name in ARG_IMMUTABLES:
            index = ARG_IMMUTABLES[name]
            require(value == constructor_args[index*32:(index+1)*32], "constructor_immutable_binding_differs")
        self_word = bytes(12) + bytes.fromhex(pin["address"][2:])
        expected = {"__self": self_word, "_cachedThis": self_word, "_cachedChainId": (137).to_bytes(32, "big"),
            "_cachedNameHash": bytes.fromhex("f30041e9aac4c4d3a1481d2941dfb0a844a72040e9bbc79a810d1ec5b5d6c7af"),
            "_cachedVersionHash": bytes.fromhex("2a80e1ef1d7842f27f2e6be0972bb708b9a135c38860dbe73c27c3486c34f4de"),
            "_cachedDomainSeparator": bytes.fromhex(pin["domain_separator"])}
        require(name not in expected or value == expected[name], "immutable_self_domain_binding_differs")
        typed = "0x" + value[-20:].hex() if address_type else int.from_bytes(value, "big") if declaration["type"] == "uint256" else "0x" + value.hex()
        getter_checks = []
        if name in {"POSITION_MANAGER", "COLLATERAL_TOKEN", "COMBINATORIAL_MODULE", "FEE_RECEIVER", "MAX_FEE_RATE"}:
            selected = [row for row in runtime.get("getters", []) if row.get("era") == era and row.get("signature") == name + "()"]
            require(len(selected) == 2 and {row.get("target_kind") for row in selected} == {"implementation", "proxy"}
                    and all(row.get("status") == "observed" and row.get("value") == typed
                        and row.get("target") == (pin["address"] if row["target_kind"] == "implementation" else runtime["address"])
                        and type(row.get("block_number")) is int and row["block_number"] > 0
                        and type(row.get("response_sha256")) is str and SHA.fullmatch(row["response_sha256"])
                        for row in selected), "immutable_getter_differs")
            getter_checks = [{"target": row["target"], "block_number": row["block_number"], "response_sha256": row["response_sha256"]} for row in selected]
        result.append({"ast_id": ident, **declaration, "word": "0x"+value.hex(), "typed_value": typed,
                       "locations": locations, "getter_observation_checks": getter_checks})
    require(used_names == set(IMMUTABLE_TYPES) and len(transformations) == 58, "immutable_count_or_roster")
    declared = package["runtimeBytecode"].get("transformations")
    require(type(declared) is list and len(declared) == 58 and all(type(v) is dict and set(v) == {"id", "type", "offset", "reason"}
            and v["type"] == "replace" and v["reason"] == "immutable" for v in declared)
            and {(v["id"], v["offset"]) for v in declared} == transformations, "undeclared_runtime_transformation")
    require(bytes(transformed) == observed, "full_runtime_comparison_differs")
    methods = evm.get("methodIdentifiers")
    require(type(methods) is dict and methods and all(type(v) is str and re.fullmatch(r"[0-9a-f]{8}", v) for v in methods.values()), "method_identifiers_schema")
    require(type(output.get("metadata")) is str and decode(output["metadata"].encode()).get("compiler", {}).get("version") == VERSION,
            "compiled_metadata_version")
    return {"era": era, "implementation_address": pin["address"], "runtime_bytes": len(observed), "runtime_sha256": digest(observed),
            "independent_unpatched_runtime_sha256": digest(code), "independent_creation_sha256": digest(init),
            "metadata_sha256": digest(output["metadata"].encode()), "method_identifiers": methods,
            "typed_immutables": result, "full_runtime_exact_match_after_declared_immutables": True,
            "constructor_arguments_sha256": digest(constructor_args), "creation_consistency_provider_report_only": True,
            "onchain_creation_authenticated": False, "dependencies_admitted": False, "token_mapping_admitted": False}


def atomic_commit(pending, target):
    lib = ctypes.CDLL(None, use_errno=True)
    if hasattr(lib, "renameat2"): result = lib.renameat2(-100, os.fsencode(pending), -100, os.fsencode(target), 1)
    elif hasattr(lib, "renamex_np"): result = lib.renamex_np(os.fsencode(pending), os.fsencode(target), 4)
    else: raise Blocked("atomic_publication_unavailable")
    require(result == 0, "atomic_publication_refused")


class Artifacts:
    def __init__(self, root, clock):
        self.root, self.clock, self.started, self.paths = root, clock, clock(), {}

    def check(self, extra=0, *, terminal=False, failure=False):
        if not failure: require(self.clock()-self.started < CAPS["runtime_seconds"], "whole_runtime_limit")
        total = 0
        for path in self.root.iterdir():
            require(path.name in self.paths and identity(path)["bytes"] >= 0, "unowned_artifact")
            total += path.stat().st_size
        reserve = 0 if terminal else CAPS["control_reserve_bytes"]
        require(total+extra+reserve <= CAPS["all_artifact_bytes"], "artifact_byte_limit")
        require(shutil.disk_usage(self.root).free >= CAPS["minimum_free_bytes"]+extra+reserve, "free_space_floor")

    def create(self, name):
        require(Path(name).name == name and name not in self.paths and not (self.root/name).exists(), "artifact_exists")
        self.paths[name] = None
        return (self.root/name).open("xb")

    def finish(self, name):
        path = self.root/name; before = identity(path); hasher = hashlib.sha256()
        require(before["bytes"] <= CAPS["all_artifact_bytes"], "artifact_byte_limit")
        remaining = before["bytes"]
        with path.open("rb") as stream:
            while remaining:
                chunk = stream.read(min(remaining, 64*1024))
                require(chunk, "artifact_short_reopen")
                hasher.update(chunk); remaining -= len(chunk)
            require(stream.read(1) == b"", "artifact_grew_during_reopen")
        require(identity(path) == before, "artifact_changed_during_reopen")
        bound = {"path": name, "bytes": before["bytes"], "sha256": hasher.hexdigest()}
        self.paths[name] = bound
        return bound

    def write(self, name, raw, *, final_check=None, failure=False):
        self.check(len(raw), terminal=final_check is not None, failure=failure)
        pending = "."+name+".pending"
        with self.create(pending) as stream:
            require(stream.write(raw) == len(raw), "artifact_short_write"); stream.flush(); os.fsync(stream.fileno())
        bound = self.finish(pending)
        require(bound["bytes"] == len(raw) and bound["sha256"] == digest(raw), "artifact_reopen_differs")
        fd = os.open(self.root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try: os.fsync(fd)
        finally: os.close(fd)
        if final_check is not None:
            final_check()
            require(self.finish(pending) == bound, "pending_marker_changed")
        self.check(terminal=final_check is not None, failure=failure)
        atomic_commit(self.root/pending, self.root/name)
        self.paths.pop(pending); self.paths[name] = {**bound, "path": name}
        return self.paths[name]

    def reopen(self):
        for name, bound in list(self.paths.items()):
            require(bound is not None and self.finish(name) == bound, "published_artifact_changed")


def process_birth(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    return {"pid": pid, "start_ticks": int(fields[19]), "pgid": int(fields[2]), "session": int(fields[3]), "boot_id": boot}


def group_absent(pid):
    try: os.killpg(pid, 0)
    except ProcessLookupError: return True
    return False


def child_limits():
    resource.setrlimit(resource.RLIMIT_AS, (CAPS["address_space_bytes"]-CAPS["parent_live_bytes"],)*2)
    resource.setrlimit(resource.RLIMIT_FSIZE, (CAPS["stdout_bytes"],)*2)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def real_executor(argv, stdin_path, emit_out, emit_err, timeout, check, process_receipt):
    """Bounded pipes; preserve the handle even if post-spawn admission fails."""
    began = time.monotonic()
    ends_at = began + timeout
    def child_check():
        check()
        current = time.monotonic()
        require(current < ends_at, "compiler_child_timeout")
        return current
    process = birth = None; failure = None; drained = False; dispatched = False
    old_as = resource.getrlimit(resource.RLIMIT_AS)
    try:
        child_check()
        vm = int(Path("/proc/self/statm").read_text().split()[0])*os.sysconf("SC_PAGE_SIZE")
        require(vm < CAPS["parent_live_bytes"], "parent_live_address_space_limit")
        resource.setrlimit(resource.RLIMIT_AS, (CAPS["parent_live_bytes"], old_as[1]))
        with (stdin_path.open("rb") if stdin_path is not None else open(os.devnull, "rb")) as source:
            child_check()
            dispatched = True
            process = subprocess.Popen(argv, stdin=source, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=REPO, start_new_session=True, preexec_fn=child_limits, bufsize=0)
        try:
            candidate = process_birth(process.pid)
            require(UUID.fullmatch(candidate["boot_id"]) and candidate["start_ticks"] > 0
                    and candidate["pid"] == candidate["pgid"] == candidate["session"], "compiler_birth_not_bound")
            birth = candidate
        except BaseException:
            # Unknown birth is never permission to signal a numeric PID.
            failure = "compiler_birth_not_bound"
            raise
        # Establish ownership before handling an expired constructor deadline,
        # so the retained handle can still be safely drained on this failure.
        child_check()
        process_receipt(birth)
        child_check()
        selector = selectors.DefaultSelector()
        try:
            child_check()
            for stream, emit in ((process.stdout, emit_out), (process.stderr, emit_err)):
                os.set_blocking(stream.fileno(), False); selector.register(stream, selectors.EVENT_READ, emit)
                child_check()
            while selector.get_map():
                child_check()
                for key, _ in selector.select(min(0.1, max(0, ends_at-time.monotonic()))):
                    child_check()
                    data = os.read(key.fd, 64*1024)
                    child_check()
                    if data: key.data(data)
                    else: selector.unregister(key.fileobj); key.fileobj.close()
                    child_check()
            child_check()
            process.wait(timeout=min(1, max(0, ends_at-time.monotonic())))
            child_check()
            drained = group_absent(process.pid)
            finished = child_check()
            return {"exit_code": process.returncode, "owned_group_drained": drained, "birth": birth,
                    "elapsed_seconds": finished-began, "timeout_seconds": timeout}
        finally: selector.close()
    except BaseException as exc:
        if process is not None and birth is not None and process.poll() is None:
            try:
                require(process_birth(process.pid) == birth, "compiler_birth_changed")
                os.killpg(process.pid, signal.SIGTERM)
                try: process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    require(process_birth(process.pid) == birth, "compiler_birth_changed")
                    os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=1)
            except (OSError, Blocked, subprocess.TimeoutExpired): pass
        if process is not None:
            for stream in (process.stdout, process.stderr):
                if stream is not None: stream.close()
            drained = birth is not None and process.poll() is not None and group_absent(process.pid)
        # The invoking layer persists this even for interrupts and unknown births.
        receipt = {"exit_code": None if process is None else process.poll(), "owned_group_drained": drained,
                   "birth": birth, "pid_if_spawned": None if process is None else process.pid,
                   "elapsed_seconds": time.monotonic()-began, "timeout_seconds": timeout}
        exc.compiler_receipt = receipt
        if failure is not None:
            error = Blocked(failure); error.compiler_receipt = receipt; raise error from None
        raise
    finally:
        # An unproven/live child keeps the parent's smaller reservation until
        # this command exits; never raise it while owned work may remain.
        if not dispatched or drained:
            resource.setrlimit(resource.RLIMIT_AS, old_as)


def invoke(label, argv, stdin_path, writer, timeout, executor, compiler, check):
    require(compiler_binding(Path(compiler["path"]), check) == compiler, "compiler_changed_before_execution")
    writer.write(label+".START.json", encode({"argv": argv, "stdin": None if stdin_path is None else stdin_path.name,
                 "timeout_seconds": timeout, "stdout_limit": CAPS["version_stdout_bytes"] if label == "version" else CAPS["stdout_bytes"],
                 "stderr_limit": CAPS["stderr_bytes"], "compiler": compiler}))
    streams = {"stdout": writer.create(label+".stdout"), "stderr": writer.create(label+".stderr")}
    sizes = {"stdout": 0, "stderr": 0}; limits = {"stdout": CAPS["version_stdout_bytes"] if label == "version" else CAPS["stdout_bytes"], "stderr": CAPS["stderr_bytes"]}
    process_written = False; receipt = {"exit_code": None, "owned_group_drained": False, "birth": None}
    def emit(kind, data):
        require(type(data) is bytes, "compiler_stream_type")
        allowed = min(len(data), max(0, limits[kind]-sizes[kind])); writer.check(allowed)
        if allowed:
            require(streams[kind].write(data[:allowed]) == allowed, "compiler_stream_short_write")
            streams[kind].flush(); sizes[kind] += allowed
        require(allowed == len(data), "compiler_"+kind+"_limit")
    def process_record(birth):
        nonlocal process_written
        require(not process_written, "duplicate_compiler_process_receipt")
        writer.write(label+".PROCESS.json", encode(birth)); process_written = True
    error = None
    try:
        receipt = executor(argv, stdin_path, lambda data: emit("stdout", data), lambda data: emit("stderr", data), timeout, check, process_record)
    except BaseException as exc:
        receipt = getattr(exc, "compiler_receipt", receipt); error = exc
    finally:
        for stream in streams.values(): stream.flush(); os.fsync(stream.fileno()); stream.close()
        stdout = writer.finish(label+".stdout"); stderr = writer.finish(label+".stderr")
        writer.write(label+".EXIT.json", encode({**receipt, "stdout": stdout, "stderr": stderr,
            "error_code": str(error) if isinstance(error, Blocked) else "compiler_interrupted" if error else None}), failure=error is not None)
    if error is not None: raise error
    require(process_written and receipt.get("owned_group_drained") is True and receipt.get("exit_code") == 0,
            "compiler_exit_or_owned_group_not_drained")
    check()
    return stdout, stderr


def run_verification(old_path, new_path, runtime_path, solc, head, output, *, executor=real_executor, clock=time.monotonic):
    output, solc = Path(output), Path(solc)
    started = clock()
    def deadline(): require(clock()-started < CAPS["runtime_seconds"], "whole_runtime_limit")
    require(output.is_absolute() and output.parent.is_dir() and output.parent.resolve() == output.parent
            and not output.exists() and not output.is_symlink(), "output_not_new_literal_directory")
    source = source_snapshot(head); compiler = compiler_binding(solc, deadline)
    packages, bindings, augmented = {}, [], {}
    for era, path in (("old", Path(old_path)), ("new", Path(new_path))):
        raw, bound = bound_read(path, PINS[era]["sha256"], size=PINS[era]["bytes"])
        packages[era] = decode(raw); augmented[era] = package_input(packages[era], era); bindings.append(bound)
    raw, bound = bound_read(Path(runtime_path), RUNTIME_SHA, size=143415); runtime = decode(raw); bindings.append(bound)
    require(runtime.get("chain_id") == 137 and runtime.get("schema_version") == "polymarket_v3_runtime_probe_v1"
            and runtime.get("source_equivalence_admitted") is False, "runtime_receipt_scope")
    for bound in bindings:
        path = Path(bound["path"])
        require(path != solc and output not in path.parents and path not in output.parents, "output_or_inputs_overlap")
    require(shutil.disk_usage(output.parent).free >= CAPS["minimum_free_bytes"]+CAPS["all_artifact_bytes"], "free_space_floor")
    output.mkdir(mode=0o700); writer = Artifacts(output, clock)
    result = {"schema_version": "polymarket_v3_exchange_source_build_v1", "status": "blocked", "source": source,
        "compiler": compiler, "inputs": bindings, "caps": CAPS, "api_calls": 0, "compiler_executed": False,
        "source_content_keccak_independently_verified_here": False,
        "source_binding_basis": "Complete-response SHA256 pins, literal-source SHA256 and authenticated compiler/runtime comparison",
        "dependencies_admitted": False, "token_mapping_admitted": False, "outcome_mapping_admitted": False,
        "analysis_admitted": False, "final_sample_admitted": False, "onchain_creation_authenticated": False}
    try:
        writer.write("START.json", encode(result)); check = lambda: (deadline(), writer.check())
        out, err = invoke("version", [str(solc), "--version"], None, writer, CAPS["version_seconds"], executor, compiler, check)
        require((output/out["path"]).read_bytes() == VERSION_STDOUT and err["bytes"] == 0, "compiler_version_differs")
        builds = []
        for era in ("old", "new"):
            value, sources = augmented[era]
            stdin = writer.write(era+".standard-input.json", encode(value)); deadline(); gc.collect()
            stdout, stderr = invoke(era, [str(solc), "--standard-json", "--no-import-callback"], output/stdin["path"],
                writer, CAPS["child_seconds"], executor, compiler, check)
            result["compiler_executed"] = True
            compiled = decode((output/stdout["path"]).read_bytes())
            proof = verify_build(compiled, packages[era], runtime, era); del compiled; gc.collect()
            proof.update(literal_source_hashes=sources, standard_input=stdin, compiler_stdout=stdout, compiler_stderr=stderr)
            writer.write(era+".proof.json", encode(proof)); builds.append(proof)
        result.update(status="verified_v3_exchange_runtime_source_build_only", builds=builds,
                      version_checks=1, compiler_invocations=2, runtime_source_build_verified=True,
                      owned_children_drained=True, evidence_bytes_before_result=sum((output/name).stat().st_size for name in writer.paths))
        bound_result = writer.write("RESULT.json", encode(result))
        def final_gate():
            writer.reopen()
            for binding in bindings:
                _, current = bound_read(Path(binding["path"]), binding["sha256"], size=binding["identity"]["bytes"])
                require(current == binding, "final_input_identity_changed")
            require(compiler_binding(solc, deadline) == compiler and source_snapshot(head) == source, "final_compiler_or_source_changed")
            deadline()
        writer.write("COMPLETE.json", encode({"status": result["status"], "result": bound_result,
            "runtime_source_build_verified": True, "dependencies_admitted": False, "token_mapping_admitted": False,
            "outcome_mapping_admitted": False, "analysis_admitted": False, "final_sample_admitted": False}), final_check=final_gate)
    except BaseException as exc:
        result.update(status="blocked", error_code=str(exc) if isinstance(exc, Blocked) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit))
                      else "source_build_io_or_resource_failure", owned_children_drained=getattr(exc, "compiler_receipt", {}).get("owned_group_drained"))
        try: writer.write("FAILED.json", encode(result), failure=True)
        except BaseException: result["failure_receipt_published"] = False
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
    return result


@contextmanager
def process_limits():
    old_as = resource.getrlimit(resource.RLIMIT_AS); old_handler = signal.getsignal(signal.SIGALRM)
    try:
        value = min(old_as[0], CAPS["address_space_bytes"]) if old_as[0] >= 0 else CAPS["address_space_bytes"]
        resource.setrlimit(resource.RLIMIT_AS, (value, old_as[1]))
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(Blocked("whole_runtime_limit")))
        signal.setitimer(signal.ITIMER_REAL, CAPS["runtime_seconds"])
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, old_handler)
        if resource.getrlimit(resource.RLIMIT_AS)[0] != CAPS["parent_live_bytes"]:
            resource.setrlimit(resource.RLIMIT_AS, old_as)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("old-source-json", "new-source-json", "runtime-observation", "solc", "output-dir"):
        parser.add_argument("--"+name, type=Path, required=True)
    parser.add_argument("--expected-head", required=True); args = parser.parse_args(argv)
    require_production_host()  # Before input, executable or source reads.
    try:
        require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python") and args.output_dir.parent == Path("/mnt/data/runs"), "production_paths")
        require(shared.resource_state()["ram_bytes"]["MemAvailable"] >= CAPS["address_space_bytes"], "available_ram_floor")
        with process_limits():
            result = run_verification(args.old_source_json, args.new_source_json, args.runtime_observation,
                                      args.solc, args.expected_head, args.output_dir)
    except (Blocked, OSError, KeyError): result = {"status": "blocked", "error_code": "source_build_admission_failed"}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "verified_v3_exchange_runtime_source_build_only" else 2


if __name__ == "__main__":
    raise SystemExit(main())
