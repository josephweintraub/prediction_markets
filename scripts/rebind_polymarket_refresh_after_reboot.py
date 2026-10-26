"""One explicit same-EBS reboot bridge, never a general identity exception.

Create fresh ordinary v1 inventory/plan files; preserve every old byte. Only
26 named input-identity device leaves may change 66304 -> 66309. Native endpoint
planning is repeated. The terminal certificate is not data/sample admission.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner
from scripts import probe_polymarket_increment_capacity as pilot

OLD_DEVICE, NEW_DEVICE = 66304, 66309
VOLUME = "vol-0acbba4565a64f792"
UUID = "d0cd087b-94c4-428c-bae9-ae28929059f6"
SERIAL = "vol0acbba4565a64f792"  # AWS NVMe serial is the volume ID without its hyphen.
DEVICE = "/dev/nvme1n1"
CAPACITY = 750 * 1024**3
MOUNT = Path("/mnt/data")
OLD_INVENTORY_SHA = "869dbef6c7cc72c1a44caba81725bef5339a6db23f4bb1f678b375f54045d48e"
OLD_PLAN_SHA = "8dc0aa67cae304072ccaa3be3660c4e06e13e75db68e91403ddcde336fe383ce"
OLD_CAMPAIGN_SHA = "c4fba53915536077f3619ac3fae352edbd509722d6d223f54b4c2f4973917526"
OLD_TERMINAL_SHA = "96fb0dd73491fc78d72810bfb2fc0cd43b976c5503244c86d47f0e6cce4193e4"
VOLUME_RECEIPT_SHA = "568ba4ef26fa24ba2e0559c8a78f40a40fea55520181ee761592fbb1dac11559"
INVENTORY_HELPER_SHA = "d8d142cba970c993bdc462a03a10bd368e6298dafc807fcf3514d9bbec3551a7"
PLANNER_HELPER_SHA = "e63c873e9393d1c9a3184fd807528c1031d2c40ce49ed019e66425cc1a90b621"
NAMES = tuple(sorted(set(inventory.DEFAULT_PATHS) - {"deduped_events"}))
MAX_CALLS, MAX_RESPONSES, MAX_SECONDS = 100, 256 * 1024**2, 1200
MAX_OUTPUT, MAX_RAM, FREE_FLOOR = 8_000_000, 8 * 1024**3, 20_000_000_000
FILES = {"OWNER.json", "fresh_inventory.json", "fresh_source_plan.json", "equivalence.json", "COMPLETE.json"}


class RebindBlocked(ValueError):
    """Fixed non-sensitive errors only."""


def require(ok, code):
    if not ok:
        raise RebindBlocked(code)


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def same(left, right):
    """JSON equality that never equates integers, floats and booleans."""
    return encoded(left) == encoded(right)


def read_bound(path, expected=None, maximum=MAX_OUTPUT):
    before = inventory.path_identity(path)
    require(before["status"] == "present" and not before["is_symlink"] and path.is_file()
            and 0 < before["target"]["bytes"] <= maximum, "bridge_control_not_regular_or_bounded")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    require(len(raw) == before["target"]["bytes"] and inventory.path_identity(path) == before,
            "bridge_control_changed_during_read")
    digest = sha(raw)
    require(expected is None or digest == expected, "bridge_control_hash_differs")
    value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(RebindBlocked("bridge_nonfinite_json")))
    require(type(value) is dict, "bridge_control_not_object")
    return value, {"path": str(path), "sha256": digest, "bytes": len(raw), "identity": before}


def device_equivalent(old, current):
    expected = copy.deepcopy(old)
    require(old.get("status") == "present" and old.get("is_symlink") is False,
            "bridge_identity_not_regular")
    for role in ("entry", "target"):
        require(type(expected.get(role)) is dict and type(expected[role].get("device")) is int
                and expected[role]["device"] == OLD_DEVICE,
                "bridge_old_device_differs")
        expected[role]["device"] = NEW_DEVICE
    require(same(expected, current), "bridge_nondevice_identity_differs")


def migrate_inputs(old):
    """The only allowed JSON leaf edits, including repeated topology identities."""
    value = copy.deepcopy(old)
    require(set(value) == set(inventory.DEFAULT_PATHS), "bridge_input_roster_differs")
    changed = []
    for name in NAMES:
        entry = value[name]
        require(entry.get("status") == "inspected" and entry.get("file_count") == 1
                and len(entry.get("files", [])) == 1 and entry.get("topology", {}).get("directories") == [],
                "bridge_source_not_fixed_single_file")
        for label, identity in ((f"inputs/{name}/files/0/identity", entry["files"][0]["identity"]),
                                (f"inputs/{name}/topology/input", entry["topology"]["input"])):
            require(identity.get("status") == "present" and identity.get("is_symlink") is False,
                    "bridge_source_identity_not_regular")
            for role in ("entry", "target"):
                require(type(identity[role]["device"]) is int and identity[role]["device"] == OLD_DEVICE,
                        "bridge_old_device_differs")
                identity[role]["device"] = NEW_DEVICE
                changed.append(label + "/" + role + "/device")
    require(value["deduped_events"].get("status") == "missing"
            and value["deduped_events"].get("identity") == {"status": "missing"}, "bridge_missing_source_changed")
    return value, changed


def compare_inventories(old, fresh):
    expected_inputs, leaves = migrate_inputs(old["inputs"])
    require(same(fresh["inputs"], expected_inputs), "bridge_input_metadata_or_identity_differs")
    repair = copy.deepcopy(old["repair_manifest"])
    for role in ("entry", "target"):
        require(type(repair["identity"][role]["device"]) is int and repair["identity"][role]["device"] == OLD_DEVICE,
                "bridge_old_repair_device_differs")
        repair["identity"][role]["device"] = NEW_DEVICE
        leaves.append("repair_manifest/identity/" + role + "/device")
    require(same(fresh["repair_manifest"], repair) and len(leaves) == 26, "bridge_repair_manifest_differs")
    # Only envelope freshness/provenance may differ. Scientific metadata, limits,
    # producer declarations, status and footer consumption stay exactly equal.
    envelope = {"created_utc", "code_before", "code_after", "rpc_readiness", "resources"}
    require(same({key: value for key, value in old.items() if key not in envelope | {"inputs", "repair_manifest"}},
                 {key: value for key, value in fresh.items() if key not in envelope | {"inputs", "repair_manifest"}}),
            "bridge_inventory_nonfreshness_fields_differ")
    require(same(fresh["code_before"], fresh["code_after"]) and fresh["code_before"].get("tracked_worktree_clean") is True,
            "bridge_fresh_inventory_code_not_clean")
    return sorted(leaves)


def compare_plans(old, fresh):
    fields = ("end_exclusive_utc", "end_exclusive_unix_seconds", "raw_footer_tail_header", "last_included_header",
              "first_excluded_header", "cutoff_proof", "provisional_append_start", "provisional_append_blocks",
              "provisional_append_range_empty", "exchange_code_presence", "trade_logs_requested",
              "parquet_body_reads", "collection_approved", "exchange_abi_admitted")
    require(all(field in old and same(old[field], fresh.get(field)) for field in fields),
            "bridge_scientific_plan_or_native_endpoint_differs")
    require(old.get("status") == fresh.get("status") == "source_plan_ready_for_review"
            and fresh.get("cutoff_headers_reopened_unchanged") is True
            and fresh.get("final_input_identity_unchanged") is True, "bridge_fresh_plan_not_ready")


def capture_storage():
    def command(argv):
        result = subprocess.run(argv, capture_output=True, text=True, check=True, timeout=10)
        require(len(result.stdout.encode()) <= 65536 and len(result.stderr.encode()) <= 65536,
                "bridge_storage_command_byte_limit")
        return result.stdout.strip().split()
    mounted = command(["findmnt", "-n", "-o", "SOURCE,UUID,FSTYPE", "--target", str(MOUNT)])
    block = command(["lsblk", "-b", "-n", "-d", "-o", "PATH,SERIAL,SIZE", DEVICE])
    require(len(mounted) == len(block) == 3 and mounted == [DEVICE, UUID, "ext4"]
            and block[:2] == [DEVICE, SERIAL] and block[2] == str(CAPACITY), "bridge_live_volume_differs")
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    require(bool(re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", boot)), "bridge_boot_id_invalid")
    usage = shutil.disk_usage(MOUNT)
    return {"mount": str(MOUNT), "device": DEVICE, "uuid": UUID, "volume_id": VOLUME, "serial": SERIAL,
            "device_bytes": CAPACITY, "filesystem_bytes": usage.total, "linux_device": MOUNT.stat().st_dev,
            "boot_id": boot}


def check_storage(previous, current, old_inventory):
    require(previous.get("schema_version") == "polymarket_data_filesystem_expansion_v1"
            and previous.get("volume_id") == VOLUME and previous.get("uuid") == UUID
            and previous.get("device") == DEVICE and previous.get("device_bytes") == CAPACITY
            and previous.get("exit_code") == 0 and previous.get("protected_input_count") == 6
            and previous.get("protected_input_identities_unchanged") is True, "bridge_prior_volume_receipt_differs")
    identities = {entry["path"]: entry["files"][0]["identity"] for name, entry in old_inventory["inputs"].items() if name in NAMES}
    require(same(previous.get("protected_input_identities"), identities), "bridge_prior_volume_inputs_differ")
    expected = {"mount": str(MOUNT), "device": DEVICE, "uuid": UUID, "volume_id": VOLUME,
                "serial": SERIAL, "device_bytes": CAPACITY, "linux_device": NEW_DEVICE}
    require(same({key: current.get(key) for key in expected}, expected)
            and type(current.get("filesystem_bytes")) is int
            and same(current["filesystem_bytes"], previous["after"]["filesystem_bytes"])
            and bool(re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", current.get("boot_id", ""))),
            "bridge_current_storage_not_admitted")


def source_controls(plan_path, campaign_path, terminal_path, volume_path):
    old_plan, plan_ref = read_bound(plan_path, OLD_PLAN_SHA)
    binding = old_plan["inventory_binding"]
    require(binding.get("sha256") == OLD_INVENTORY_SHA, "bridge_old_inventory_pin_differs")
    old_inventory, current_binding = planner.read_inventory(Path(binding["path"]), OLD_INVENTORY_SHA, binding["source_head"])
    device_equivalent(binding["identity"], current_binding["identity"])
    campaign, campaign_ref = read_bound(campaign_path, OLD_CAMPAIGN_SHA)
    terminal, terminal_ref = read_bound(terminal_path, OLD_TERMINAL_SHA)
    frozen = campaign.get("plan", {})
    old_binding = frozen.get("source_plan", {})
    require(old_binding.get("path") == str(plan_path) and old_binding.get("sha256") == OLD_PLAN_SHA
            and old_binding.get("source_head") == old_plan.get("planner_source", {}).get("head"),
            "bridge_old_campaign_source_binding_differs")
    device_equivalent(old_binding["identity"], plan_ref["identity"])
    require(sha(encoded(frozen)) == campaign.get("plan_sha256") == terminal.get("plan_sha256")
            and campaign.get("run_id") == terminal.get("run_id")
            and terminal.get("status") == "failed_raw_campaign" and terminal.get("children_still_running") is False,
            "bridge_old_campaign_not_sealed")
    prior, volume_ref = read_bound(volume_path, VOLUME_RECEIPT_SHA)
    # Stat every bound input before any refreshed footer can be opened. No
    # semantic/file replacement is permitted merely because it is on this EBS.
    migrate_inputs(old_inventory["inputs"])
    for name in NAMES:
        entry = old_inventory["inputs"][name]
        device_equivalent(entry["files"][0]["identity"], inventory.path_identity(Path(entry["files"][0]["path"])))
        device_equivalent(entry["topology"]["input"], inventory.path_identity(Path(entry["path"])))
    require(same(inventory.path_identity(Path(old_inventory["inputs"]["deduped_events"]["path"])),
                 old_inventory["inputs"]["deduped_events"]["identity"]), "bridge_missing_source_changed")
    device_equivalent(old_inventory["repair_manifest"]["identity"],
                      inventory.path_identity(Path(old_inventory["repair_manifest"]["path"])))
    refs = {"old_plan": plan_ref, "old_inventory": {**current_binding, "bytes": Path(binding["path"]).stat().st_size},
            "old_campaign": campaign_ref, "old_terminal": terminal_ref, "volume_receipt": volume_ref}
    return old_plan, old_binding, old_inventory, prior, refs


class Budget:
    def __init__(self, output, clock=time.monotonic):
        self.output, self.clock, self.started = output, clock, clock()
        self.calls = self.responses = self.bytes = 0

    def check(self, extra=0):
        require(self.clock() - self.started < MAX_SECONDS, "bridge_runtime_limit")
        require(self.bytes + extra <= MAX_OUTPUT, "bridge_output_byte_limit")
        require(shutil.disk_usage(self.output.parent).free - extra >= FREE_FLOOR, "bridge_free_floor")

    def write(self, name, value):
        raw = encoded(value)
        self.check(len(raw))
        inventory.write_exclusive(self.output / name, value)
        self.bytes += len(raw)
        return read_bound(self.output / name, sha(raw))[1]

    def transport(self, transport):
        def call(endpoint, payload):
            self.check()
            require(self.calls < MAX_CALLS, "bridge_rpc_call_limit")
            reserved = inventory.LIMITS["maximum_rpc_response_bytes"] + 1
            require(self.responses + reserved <= MAX_RESPONSES, "bridge_rpc_response_budget")
            self.calls += 1
            self.responses += reserved  # Includes failed/truncated transport attempts.
            result = transport(endpoint, payload)
            require(len(encoded(result)) <= inventory.LIMITS["maximum_rpc_response_bytes"], "bridge_rpc_reply_byte_limit")
            self.check()
            return result
        return call


def produce(plan_path, campaign_path, terminal_path, volume_path, output, expected_head, *,
            private_file=None, transport=inventory._http_rpc, storage=capture_storage, clock=time.monotonic):
    require(not output.exists() and not output.is_symlink(), "bridge_output_already_exists")
    budget = Budget(output, clock)
    budget.check(MAX_OUTPUT)
    require(sha(Path(inventory.__file__).read_bytes()) == INVENTORY_HELPER_SHA
            and sha(Path(planner.__file__).read_bytes()) == PLANNER_HELPER_SHA, "bridge_legacy_helper_hash_differs")
    old_plan, old_binding, old, prior, refs = source_controls(plan_path, campaign_path, terminal_path, volume_path)
    current = storage()
    check_storage(prior, current, old)
    source = inventory.code_state()
    require(source.get("head") == expected_head and source.get("tracked_worktree_clean") is True
            and bool(re.fullmatch(r"[0-9a-f]{40}", expected_head)), "bridge_source_not_clean")
    forbidden = [Path(ref["path"]) for ref in refs.values()]
    forbidden.extend(Path(entry["path"]) for entry in old["inputs"].values())
    forbidden.append(Path(old["repair_manifest"]["path"]))
    for path in forbidden:
        require(output.resolve() != path.resolve() and path.resolve() not in output.resolve().parents
                and output.resolve() not in path.resolve().parents, "bridge_output_overlaps_input")
    output.mkdir(mode=0o700)
    owner_ref = budget.write("OWNER.json", {"schema_version": "polymarket_reboot_rebind_owner_v1", "source": source,
                 "created_utc": datetime.now(timezone.utc).isoformat(), "old_controls": refs,
                 "storage": current, "limits": {"rpc_calls": MAX_CALLS, "response_bytes": MAX_RESPONSES,
                     "runtime_seconds": MAX_SECONDS, "output_bytes": MAX_OUTPUT, "ram_bytes": MAX_RAM,
                     "spill_bytes": 0, "free_floor_bytes": FREE_FLOOR}})
    fresh = inventory.build_inventory({name: Path(item["path"]) for name, item in old["inputs"].items()},
                                      repair_manifest=Path(old["repair_manifest"]["path"]), disk_path=MOUNT)
    leaves = compare_inventories(old, fresh)
    budget.check()
    fresh_inv_ref = budget.write("fresh_inventory.json", fresh)
    new_plan = planner.plan_refresh(output / "fresh_inventory.json", fresh_inv_ref["sha256"], expected_head, expected_head,
                                    rpc_private_file=private_file, transport=budget.transport(transport))
    compare_plans(old_plan, new_plan)
    new_plan_ref = budget.write("fresh_source_plan.json", new_plan)
    # Normal strict source admission must work without an exception or override.
    admitted, new_binding, admitted_inventory = pilot.read_plan(output / "fresh_source_plan.json", new_plan_ref["sha256"], expected_head)
    require(same(admitted, new_plan) and same(admitted_inventory, fresh) and same(storage(), current)
            and same(inventory.code_state(), source), "bridge_final_fresh_binding_differs")
    witness = {"schema_version": "polymarket_reboot_equivalence_v1", "status": "equivalence_verified",
               "old_binding": old_binding, "fresh_binding": new_binding, "old_controls": refs,
               "fresh_inventory": fresh_inv_ref, "fresh_plan": new_plan_ref, "storage": current,
               "allowed_device_transition": {"old": OLD_DEVICE, "new": NEW_DEVICE, "input_leaves": leaves},
               "actual_preparation": {"rpc_calls": budget.calls, "response_bytes_reserved": budget.responses},
               "whole_file_body_hashes_verified": False, "trade_body_reads": 0,
               "scope": "Same-volume identity/footer and exact-endpoint equivalence only, not data/sample admission."}
    witness_ref = budget.write("equivalence.json", witness)
    # Reopen old immutable controls and new inputs once more before sealing.
    require(same(source_controls(plan_path, campaign_path, terminal_path, volume_path)[4], refs),
            "bridge_old_controls_changed")
    planner.reopen_inputs(fresh)
    for reference in (owner_ref, fresh_inv_ref, new_plan_ref, witness_ref):
        require(same(read_bound(Path(reference["path"]), reference["sha256"])[1], reference),
                "bridge_final_local_output_differs")
    require(same(pilot.read_plan(output / "fresh_source_plan.json", new_plan_ref["sha256"], expected_head),
                 (new_plan, new_binding, fresh)) and same(storage(), current)
            and same(inventory.code_state(), source), "bridge_final_reopen_differs")
    budget.check()
    terminal = {"schema_version": "polymarket_reboot_rebind_complete_v1", "status": "rebind_complete_not_analysis_admission",
                "witness": witness_ref, "owner": owner_ref,
                "fresh_plan": new_plan_ref, "fresh_inventory": fresh_inv_ref,
                "conservative_preparation_charge": {"rpc_calls": MAX_CALLS, "response_bytes": MAX_RESPONSES,
                                                     "runtime_seconds": MAX_SECONDS}}
    budget.write("COMPLETE.json", terminal)
    return {"status": terminal["status"], "bridge": read_bound(output / "COMPLETE.json")[1],
            "fresh_plan": new_plan_ref, "fresh_plan_head": expected_head}


def validate_bridge(path, expected_sha256, fresh_plan_path, fresh_plan_sha, fresh_plan_head):
    """Reopen a terminal certificate; no legacy readers are weakened or patched."""
    complete, bridge_ref = read_bound(path, expected_sha256)
    root = path.parent
    require(not root.is_symlink() and root.is_dir() and {item.name for item in root.iterdir()} == FILES,
            "bridge_admission_tree_differs")
    require(complete.get("schema_version") == "polymarket_reboot_rebind_complete_v1"
            and complete.get("status") == "rebind_complete_not_analysis_admission", "bridge_not_complete")
    def local(role, name):
        ref = complete[role]
        require(ref.get("path") == str(root / name), "bridge_local_reference_differs")
        value, actual = read_bound(root / name, ref["sha256"])
        require(same(actual, ref), "bridge_local_identity_differs")
        return value, actual
    witness, witness_ref = local("witness", "equivalence.json")
    owner, _ = local("owner", "OWNER.json")
    fresh_inv, _ = local("fresh_inventory", "fresh_inventory.json")
    fresh_plan, _ = local("fresh_plan", "fresh_source_plan.json")
    require(Path(fresh_plan_path) == root / "fresh_source_plan.json" and fresh_plan_sha == complete["fresh_plan"]["sha256"]
            and owner.get("source", {}).get("head") == fresh_plan_head, "bridge_requested_fresh_plan_differs")
    oldrefs = witness["old_controls"]
    old_plan, old_binding, old_inv, prior, reopened = source_controls(
        Path(oldrefs["old_plan"]["path"]), Path(oldrefs["old_campaign"]["path"]),
        Path(oldrefs["old_terminal"]["path"]), Path(oldrefs["volume_receipt"]["path"]))
    require(same(reopened, oldrefs) and same(oldrefs, owner.get("old_controls")), "bridge_old_control_reopen_differs")
    current = capture_storage()
    check_storage(prior, current, old_inv)
    leaves = compare_inventories(old_inv, fresh_inv)
    compare_plans(old_plan, fresh_plan)
    admitted, binding, admitted_inv = pilot.read_plan(Path(fresh_plan_path), fresh_plan_sha, fresh_plan_head)
    require(same(admitted, fresh_plan) and same(admitted_inv, fresh_inv) and witness.get("status") == "equivalence_verified"
            and same(witness.get("old_binding"), old_binding) and same(witness.get("fresh_binding"), binding)
            and same(witness.get("fresh_plan"), complete["fresh_plan"]) and same(witness.get("fresh_inventory"), complete["fresh_inventory"])
            and same(current, witness.get("storage")) and same(current, owner.get("storage"))
            and same(witness.get("allowed_device_transition"), {"old": OLD_DEVICE, "new": NEW_DEVICE, "input_leaves": leaves}),
            "bridge_equivalence_witness_differs")
    charge = {"rpc_calls": MAX_CALLS, "response_bytes": MAX_RESPONSES, "runtime_seconds": MAX_SECONDS}
    require(same(complete.get("conservative_preparation_charge"), charge), "bridge_preparation_charge_differs")
    files = []
    for item in sorted(root.iterdir()):
        _, ref = read_bound(item)
        files.append({"path": item.name, "bytes": ref["bytes"], "sha256": ref["sha256"]})
    total = sum(item["bytes"] for item in files)
    require(total <= MAX_OUTPUT, "bridge_admission_tree_byte_limit")
    return {"old_plan": old_plan, "old_binding": old_binding, "fresh_plan": fresh_plan, "fresh_binding": binding,
            "new_plan": fresh_plan, "new_binding": binding, "bridge_binding": bridge_ref, "witness_binding": witness_ref,
            "admission_tree_path": str(root), "admission_tree_inventory": {"path": str(root), "files": files, "bytes": total},
            "prep_charge": {**charge, "output_bytes": total}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("old-source-plan", "old-campaign-plan", "old-parent-result", "volume-receipt", "output-dir"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--rpc-private-file", type=Path, default=Path.home() / ".polygon_rpc_url")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        parser.error("Reboot admission requires the canonical mounted EC2 checkout.")
    require(args.output_dir.is_absolute() and Path("/mnt/data/runs") in args.output_dir.resolve().parents,
            "bridge_output_outside_runs")
    for name in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[name] = "1"
    require(inventory.resource_state()["ram_bytes"]["MemAvailable"] >= MAX_RAM, "bridge_minimum_available_ram")
    resource.setrlimit(resource.RLIMIT_AS, (MAX_RAM, MAX_RAM))
    # Helpers catch ordinary Exception; SystemExit is deliberately unswallowable.
    previous = signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(SystemExit("bridge_runtime_limit")))
    signal.alarm(MAX_SECONDS)
    try:
        result = produce(args.old_source_plan, args.old_campaign_plan, args.old_parent_result, args.volume_receipt,
                         args.output_dir, args.expected_head, private_file=args.rpc_private_file)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
