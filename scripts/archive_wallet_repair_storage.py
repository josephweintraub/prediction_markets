#!/usr/bin/env python3
"""Archive exactly three approved storage objects; removal is a separate phase.

Run only on the canonical EC2 host. The archive phase never removes source
files. Removal requires both a verified remote archive and an explicit
confirmation that the root operator checked for active jobs and transfers.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys


DATA_ROOT = Path("/mnt/data")
TARGETS = (
    "pipeline_root_output/trades_no_event_slug.parquet",
    "pipeline_root_output/trades_snap20260624.parquet",
    "telonex/quotes_ticks",
)
EXPECTED_UUID = "d0cd087b-94c4-428c-bae9-ae28929059f6"
RUN_DIR = DATA_ROOT / "runs/2026-10-09_disk_cleanup_v1"
REMOTE_ROOT = "dropbox:Polymarket Data and Code/Archives/2026-10-09_disk_cleanup_v1"
MANIFEST_NAME = "VERIFIED.json"
RESTORE_NAME = "RESTORE.txt"
ADMISSION_NAME = "ADMISSION.json"
HASH_PATTERN = re.compile(r"[0-9a-f]{64}")
COMMAND_RECORDS = []
EXPECTED_COUNTS = {
    TARGETS[0]: (44, 45, 39481253920),
    TARGETS[1]: (44, 45, 38759321999),
    TARGETS[2]: (80, 11, 70073437771),
}
STAT_FIELDS = ("dev", "inode", "size", "mtime_ns", "ctime_ns", "mode", "allocated_bytes")


def execute(arguments, log=None):
    """Invoke commands without a shell; copy logs are created exclusively."""
    if log is None:
        return subprocess.run(arguments, text=True, capture_output=True, check=False)
    with log.open("x", encoding="utf-8") as stream:
        result = subprocess.run(arguments, text=True, stdout=stream,
                                stderr=subprocess.STDOUT, check=False)
    return result


def checked(arguments, log=None):
    result = command(arguments, log)
    if result.returncode:
        detail = result.stderr if log is None else f"See {log}"
        raise RuntimeError(f"Command failed ({result.returncode}): {arguments!r}; {detail}")
    return result.stdout


def command(arguments, log=None):
    started = datetime.now(timezone.utc).isoformat()
    result = execute(arguments, log)
    COMMAND_RECORDS.append({"started_utc": started,
                            "finished_utc": datetime.now(timezone.utc).isoformat(),
                            "arguments": arguments, "returncode": result.returncode,
                            "log": str(log) if log is not None else None})
    return result


def environment_gate():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from production_guard import require_production_host

    require_production_host()
    actual = checked(["findmnt", "-no", "UUID", str(DATA_ROOT)]).strip()
    if actual != EXPECTED_UUID:
        raise RuntimeError(f"Unexpected data mount UUID: {actual!r}")


def fingerprint(info):
    return {"dev": info.st_dev, "inode": info.st_ino, "size": info.st_size,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns,
            "mode": info.st_mode, "allocated_bytes": info.st_blocks * 512}


def identity(record):
    return {key: record[key] for key in ("dev", "inode", "mode")}


def relative_path(value):
    if not isinstance(value, str):
        raise RuntimeError(f"Invalid relative archive path: {value!r}")
    path = PurePosixPath(value)
    if (not value or path.is_absolute()
            or any(part in (".", "..") for part in value.split("/"))
            or str(path) != value):
        raise RuntimeError(f"Invalid relative archive path: {value!r}")
    return value


def approved_path(value):
    relative_path(value)
    if not any(value == target or value.startswith(target + "/") for target in TARGETS):
        raise RuntimeError(f"Path is outside the approved targets: {value}")
    return DATA_ROOT / value


def anchors():
    """Record stable parent identities, refusing links above target roots too."""
    result = {}
    base = DATA_ROOT.lstat()
    if not stat.S_ISDIR(base.st_mode):
        raise RuntimeError("The data root is not a real directory")
    result[""] = fingerprint(base)
    for target in TARGETS:
        parent = PurePosixPath(target).parent
        while str(parent) != ".":
            name = str(parent)
            info = (DATA_ROOT / name).lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_dev != base.st_dev:
                raise RuntimeError(f"Linked or foreign-device parent: {name}")
            result[name] = fingerprint(info)
            parent = parent.parent
    return result


def inventory(allow_missing=False):
    parent_records = anchors()
    device = parent_records[""]["dev"]
    entries = {}
    folded = {}

    def visit(path, name):
        info = path.lstat()
        if info.st_dev != device:
            raise RuntimeError(f"Foreign-device node: {name}")
        if stat.S_ISDIR(info.st_mode):
            kind = "directory"
        elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
            kind = "file"
        else:
            raise RuntimeError(f"Link, hardlinked file, or unsupported node: {name}")
        collision = folded.setdefault(name.casefold(), name)
        if collision != name:
            raise RuntimeError(f"Dropbox case-insensitive path collision: {collision}, {name}")
        entries[name] = {"kind": kind, "stat": fingerprint(info)}
        if kind == "directory":
            with os.scandir(path) as children:
                for child in sorted(children, key=lambda item: item.name):
                    visit(Path(child.path), name + "/" + child.name)

    for name in TARGETS:
        path = DATA_ROOT / name
        if not os.path.lexists(path):
            if allow_missing:
                continue
            raise RuntimeError(f"Missing approved target: {path}")
        visit(path, name)
    return {"anchors": parent_records, "entries": dict(sorted(entries.items()))}


def dropbox_hash(data):
    block_hashes = b"".join(hashlib.sha256(data[start:start + 4 * 1024 * 1024]).digest()
                            for start in range(0, len(data), 4 * 1024 * 1024))
    return hashlib.sha256(block_hashes).hexdigest()


def strict_json(data):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise RuntimeError(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    def invalid_constant(value):
        raise RuntimeError(f"Invalid JSON numeric constant: {value}")

    return json.loads(data, object_pairs_hook=unique_object, parse_constant=invalid_constant)


def nonnegative_integer(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def validate_inventory(frozen):
    if not isinstance(frozen, dict) or set(frozen) != {"anchors", "entries"}:
        raise RuntimeError("Invalid frozen inventory structure")
    entries, parents = frozen["entries"], frozen["anchors"]
    if not isinstance(entries, dict) or not isinstance(parents, dict):
        raise RuntimeError("Invalid frozen inventory mappings")
    expected_parents = {""} | {str(PurePosixPath(target).parent) for target in TARGETS}
    if set(parents) != expected_parents:
        raise RuntimeError("Unexpected inventory parent paths")
    folded = set()
    for name, record in entries.items():
        approved_path(name)
        if (not isinstance(record, dict) or set(record) != {"kind", "stat"}
                or record["kind"] not in ("file", "directory")):
            raise RuntimeError("Invalid manifest node type")
        if name.casefold() in folded:
            raise RuntimeError("Case collision in frozen inventory")
        folded.add(name.casefold())
    for name, info, kind in ([(name, record["stat"], record["kind"]) for name, record in entries.items()]
                             + [(name, info, "directory") for name, info in parents.items()]):
        if (not isinstance(info, dict) or set(info) != set(STAT_FIELDS)
                or not all(nonnegative_integer(info[key]) for key in STAT_FIELDS)
                or info["inode"] == 0 or info["dev"] == 0):
            raise RuntimeError(f"Invalid stat fingerprint: {name}")
        if not (stat.S_ISREG(info["mode"]) if kind == "file" else stat.S_ISDIR(info["mode"])):
            raise RuntimeError(f"Stat fingerprint mode differs from node type: {name}")
        if info["dev"] != parents[""]["dev"]:
            raise RuntimeError("Foreign device in frozen inventory")
    for target in TARGETS:
        if entries.get(target, {}).get("kind") != "directory":
            raise RuntimeError("Manifest lacks an approved target root")
        descendants = {name: record for name, record in entries.items()
                       if name == target or name.startswith(target + "/")}
        counts = (sum(item["kind"] == "file" for item in descendants.values()),
                  sum(item["kind"] == "directory" for item in descendants.values()),
                  sum(item["stat"]["size"] for item in descendants.values() if item["kind"] == "file"))
        if counts != EXPECTED_COUNTS[target]:
            raise RuntimeError(f"Approved initial file/directory/byte counts changed: {target}")
        for name in descendants:
            if name != target and entries.get(str(PurePosixPath(name).parent), {}).get("kind") != "directory":
                raise RuntimeError(f"Missing directory in frozen inventory: {name}")


def listing_command(location):
    return ["rclone", "lsjson", location, "--recursive", "--files-only",
            "--hash", "--hash-type", "dropbox", "--checkers", "8"]


def hash_listing(location):
    raw = strict_json(checked(listing_command(location)))
    if not isinstance(raw, list):
        raise RuntimeError("Unexpected rclone listing")
    result = {}
    folded = set()
    for item in raw:
        if not isinstance(item, dict):
            raise RuntimeError("Invalid rclone file record")
        name = relative_path(item["Path"])
        if item.get("IsDir") or name in result or name.casefold() in folded:
            raise RuntimeError(f"Duplicate, directory, or case collision in listing: {name}")
        hashes = item.get("Hashes", {})
        if not isinstance(hashes, dict):
            raise RuntimeError(f"Invalid hash mapping: {name}")
        candidates = [value.lower() for key, value in hashes.items()
                      if key.casefold() in ("dropbox", "dropboxhash") and isinstance(value, str)]
        size = item.get("Size")
        if (len(candidates) != 1 or not HASH_PATTERN.fullmatch(candidates[0])
                or not nonnegative_integer(size)):
            raise RuntimeError(f"Missing or invalid Dropbox content hash/size: {name}")
        result[name] = {"size": size, "dropbox_hash": candidates[0]}
        folded.add(name.casefold())
    return dict(sorted(result.items()))


def source_hashes(frozen):
    result = {}
    for target in TARGETS:
        for name, record in hash_listing(str(DATA_ROOT / target)).items():
            result[target + "/" + name] = record
    expected = {name: record["stat"]["size"] for name, record in frozen["entries"].items()
                if record["kind"] == "file"}
    if {name: record["size"] for name, record in result.items()} != expected:
        raise RuntimeError("Local hash listing differs from the frozen inventory")
    return dict(sorted(result.items()))


def fresh_remote_gate():
    result = command(["rclone", "lsjson", REMOTE_ROOT, "--stat"])
    if result.returncode == 0:
        raise RuntimeError("Archive destination already exists; refusing reuse")
    if result.returncode != 3 or "directory not found" not in (result.stderr or "").lower():
        raise RuntimeError(f"Cannot establish a fresh archive destination: {result.stderr}")


def write_immutable(path, data):
    with path.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(0o444)


def control_record(data):
    return {"size": len(data), "dropbox_hash": dropbox_hash(data)}


def restore_text():
    lines = ["Verified storage archive: " + REMOTE_ROOT, "",
             "Restore the required target before running any workflow that consumes it.",
             "In particular, restore quotes_ticks before rebuilding quotes_daily.parquet",
             "or resuming a Telonex acquisition/merge driver. The archive preserves the",
             "existing tick tree; acquisition completeness was not inferred from names.",
             "The two trade directories preserve historical pipeline states.", "",
             "After confirming the destination is absent and no job is using it:"]
    for target in TARGETS:
        remote = REMOTE_ROOT + "/" + target
        local = str(DATA_ROOT / target)
        lines += [f'rclone copy "{remote}" "{local}" --immutable --checksum --create-empty-src-dirs',
                  f'rclone check "{local}" "{remote}"']
    lines += ["", "Require matching file paths, sizes, and Dropbox hashes for every file.",
              "VERIFIED.json records original file and directory identities and hashes."]
    return ("\n".join(lines) + "\n").encode()


def archive():
    if os.path.lexists(RUN_DIR):
        raise RuntimeError("Run directory already exists; refusing overwrite")
    fresh_remote_gate()
    source_head = checked(["git", "-C", str(Path(__file__).resolve().parents[1]),
                           "rev-parse", "HEAD"]).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", source_head):
        raise RuntimeError("Cannot bind archive provenance to a canonical source commit")
    frozen = inventory()
    validate_inventory(frozen)
    RUN_DIR.mkdir()
    admission_data = (json.dumps({"expected_mount_uuid": EXPECTED_UUID, "source_head": source_head,
                                 "frozen_utc": datetime.now(timezone.utc).isoformat(),
                                 "inventory": frozen}, indent=2, sort_keys=True) + "\n").encode()
    write_immutable(RUN_DIR / ADMISSION_NAME, admission_data)
    for index, target in enumerate(TARGETS, start=1):
        checked(["rclone", "copy", str(DATA_ROOT / target), REMOTE_ROOT + "/" + target,
                 "--immutable", "--checksum", "--create-empty-src-dirs", "--stats", "1m",
                 "--stats-one-line", "--log-level", "INFO", "--transfers", "4",
                 "--checkers", "8"], RUN_DIR / f"copy_{index}.log")
    data_hashes = source_hashes(frozen)
    if hash_listing(REMOTE_ROOT) != data_hashes:
        raise RuntimeError("Archive file names, sizes, or hashes differ from source")
    if inventory() != frozen:
        raise RuntimeError("Source inventory changed during archiving or verification")
    restore_data = restore_text()
    restore_record = control_record(restore_data)
    uploads = [["rclone", "copyto", str(RUN_DIR / name), REMOTE_ROOT + "/" + name,
                "--immutable", "--checksum", "--stats", "1m", "--log-level", "INFO",
                "--transfers", "4", "--checkers", "8"] for name in (RESTORE_NAME, MANIFEST_NAME)]
    manifest = {
        "status": "VERIFIED", "created_utc": datetime.now(timezone.utc).isoformat(),
        "data_root": str(DATA_ROOT), "run_dir": str(RUN_DIR), "remote_root": REMOTE_ROOT,
        "expected_mount_uuid": EXPECTED_UUID, "targets": list(TARGETS),
        "inventory": frozen, "archive_files": data_hashes, "restore_file": restore_record,
        "rclone_version": checked(["rclone", "version"]).strip(),
        "source_head": source_head,
        "source_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "commands": list(COMMAND_RECORDS),
        "completion_commands": uploads + [listing_command(REMOTE_ROOT)],
        "completion_gate": "Successful archive-phase exit after control-file hash verification and final source identity check; removal rechecks both controls independently",
        "admission_file_sha256": hashlib.sha256(admission_data).hexdigest(),
        "totals": {"files": len(data_hashes),
                   "directories": sum(item["kind"] == "directory" for item in frozen["entries"].values()),
                   "apparent_file_bytes": sum(item["size"] for item in data_hashes.values()),
                   "allocated_bytes": sum(item["stat"]["allocated_bytes"] for item in frozen["entries"].values())},
        "per_target": {target: {
            "file_count": sum(name.startswith(target + "/") for name in data_hashes),
            "directory_count": sum(record["kind"] == "directory" and
                                   (name == target or name.startswith(target + "/"))
                                   for name, record in frozen["entries"].items()),
            "files": {name[len(target) + 1:]: record for name, record in data_hashes.items()
                      if name.startswith(target + "/")},
        } for target in TARGETS},
        "path_mappings": [{"source": str(DATA_ROOT / target),
                           "archive": REMOTE_ROOT + "/" + target} for target in TARGETS],
        "verification": "Exact file paths, byte sizes, and nonempty Dropbox content hashes; source identity unchanged",
    }
    manifest_data = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    write_immutable(RUN_DIR / RESTORE_NAME, restore_data)
    write_immutable(RUN_DIR / MANIFEST_NAME, manifest_data)
    for arguments, name in zip(uploads, (RESTORE_NAME, MANIFEST_NAME)):
        checked(arguments, RUN_DIR / f"upload_{name}.log")
    expected = dict(data_hashes, **{RESTORE_NAME: restore_record,
                                  MANIFEST_NAME: control_record(manifest_data)})
    if hash_listing(REMOTE_ROOT) != dict(sorted(expected.items())):
        raise RuntimeError("Remote manifest/restore instructions or archive content failed verification")
    if inventory() != frozen:
        raise RuntimeError("Source changed before archive phase completed")
    return {"status": "VERIFIED", "files": len(data_hashes),
            "bytes": sum(item["size"] for item in data_hashes.values()),
            "manifest": str(RUN_DIR / MANIFEST_NAME), "remote_root": REMOTE_ROOT}


def load_verified():
    manifest_data = (RUN_DIR / MANIFEST_NAME).read_bytes()
    manifest = strict_json(manifest_data)
    if not isinstance(manifest, dict):
        raise RuntimeError("Invalid verified manifest structure")
    expected_contract = {"status": "VERIFIED", "data_root": str(DATA_ROOT),
                         "run_dir": str(RUN_DIR), "remote_root": REMOTE_ROOT,
                         "expected_mount_uuid": EXPECTED_UUID, "targets": list(TARGETS)}
    if any(manifest.get(key) != value for key, value in expected_contract.items()):
        raise RuntimeError("Verified manifest does not match the fixed approved contract")
    frozen = manifest["inventory"]
    validate_inventory(frozen)
    admission_data = (RUN_DIR / ADMISSION_NAME).read_bytes()
    if (hashlib.sha256(admission_data).hexdigest() != manifest["admission_file_sha256"]
            or strict_json(admission_data)["inventory"] != frozen):
        raise RuntimeError("Frozen admission checkpoint differs from the verified manifest")
    expected_files = {name: record["stat"]["size"] for name, record in frozen["entries"].items()
                      if record["kind"] == "file"}
    archive_files = manifest["archive_files"]
    if not isinstance(archive_files, dict):
        raise RuntimeError("Invalid manifest hash mapping")
    for record in archive_files.values():
        if (not isinstance(record, dict) or set(record) != {"size", "dropbox_hash"}
                or not nonnegative_integer(record["size"])
                or not isinstance(record["dropbox_hash"], str)
                or not HASH_PATTERN.fullmatch(record["dropbox_hash"])):
            raise RuntimeError("Invalid manifest content hash/size")
    if {name: record["size"] for name, record in archive_files.items()} != expected_files:
        raise RuntimeError("Manifest file hashes do not cover the frozen file inventory")
    for name, record in archive_files.items():
        approved_path(name)
        if not HASH_PATTERN.fullmatch(record["dropbox_hash"]):
            raise RuntimeError("Manifest contains an invalid content hash")
    restore_data = (RUN_DIR / RESTORE_NAME).read_bytes()
    if control_record(restore_data) != manifest["restore_file"]:
        raise RuntimeError("Local restore instructions changed")
    expected = dict(archive_files, **{RESTORE_NAME: control_record(restore_data),
                                     MANIFEST_NAME: control_record(manifest_data)})
    if hash_listing(REMOTE_ROOT) != dict(sorted(expected.items())):
        raise RuntimeError("Current remote archive or control files differ from the verified manifest")
    return manifest


def remaining_gate(frozen):
    current = inventory(allow_missing=True)
    if {name: identity(info) for name, info in current["anchors"].items()} != {
            name: identity(info) for name, info in frozen["anchors"].items()}:
        raise RuntimeError("An approved target parent changed identity")
    if not set(current["entries"]).issubset(frozen["entries"]):
        raise RuntimeError("Unrecorded local nodes appeared after verification")
    complete = current["entries"].keys() == frozen["entries"].keys()
    for name, record in current["entries"].items():
        old = frozen["entries"][name]
        if record["kind"] != old["kind"]:
            raise RuntimeError(f"Local node type changed: {name}")
        # Unlinks change directory timestamps/size. During a partial-removal
        # resume retain stable directory identity and full identity for files.
        if (complete or record["kind"] == "file"):
            matches = record == old
        else:
            matches = identity(record["stat"]) == identity(old["stat"])
        if not matches:
            raise RuntimeError(f"Remaining local identity changed: {name}")
    return current


def remove_node(name, record, frozen):
    path = approved_path(name)
    parent_name = str(PurePosixPath(name).parent)
    parent_record = frozen["entries"].get(parent_name)
    expected_parent = (parent_record["stat"] if parent_record else frozen["anchors"][parent_name])
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if identity(fingerprint(os.fstat(descriptor))) != identity(expected_parent):
            raise RuntimeError(f"Parent changed identity before removal: {name}")
        actual = fingerprint(os.stat(path.name, dir_fd=descriptor, follow_symlinks=False))
        expected = record["stat"]
        matches = actual == expected if record["kind"] == "file" else identity(actual) == identity(expected)
        if not matches:
            raise RuntimeError(f"Node changed immediately before removal: {name}")
        if record["kind"] == "file":
            os.unlink(path.name, dir_fd=descriptor)
        else:
            os.rmdir(path.name, dir_fd=descriptor)
    finally:
        os.close(descriptor)


def remove():
    manifest = load_verified()
    frozen = manifest["inventory"]
    current = remaining_gate(frozen)
    for name, record in current["entries"].items():
        if record["kind"] == "file":
            remove_node(name, record, frozen)
    directories = [(name, record) for name, record in current["entries"].items()
                   if record["kind"] == "directory"]
    for name, record in sorted(directories, key=lambda item: (-item[0].count("/"), item[0])):
        remove_node(name, record, frozen)
    if inventory(allow_missing=True)["entries"]:
        raise RuntimeError("Approved targets still have remaining nodes")
    return {"status": "REMOVED", "targets": list(TARGETS),
            "archive_files": len(manifest["archive_files"]), "remote_root": REMOTE_ROOT}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("archive", "remove"))
    parser.add_argument("--confirm-no-active-jobs", action="store_true",
                        help="Root operator has checked no process/notebook/transfer needs these targets")
    args = parser.parse_args(argv)
    environment_gate()
    if args.phase == "remove" and not args.confirm_no_active_jobs:
        raise RuntimeError("Removal requires the root operator's --confirm-no-active-jobs")
    result = archive() if args.phase == "archive" else remove()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
