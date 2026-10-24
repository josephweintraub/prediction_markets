#!/usr/bin/env python3
"""Build and verify an explicitly enumerated, immutable research handoff.

This checks file inventory and byte integrity, not credential contents, archive
contents, scientific validity, or completeness of a reproducible environment.
No source tree or archive is recursively discovered or unpacked during a build.
Parquet is rejected except for explicitly asserted ``aggregate_summary`` or
``bounded_metadata`` roles at no more than 8 MiB. These roles are operator
assertions, not evidence of aggregate grain: a separate schema/row-count audit
is required. Parquet source/destination suffixes must agree.

Plan schema (no additional keys)::

    {"release_name": "joseph_code_v1", "files": [
      {"source": "/absolute/source.py", "destination": "code/source.py",
       "sha256": "<64 lowercase hexadecimal characters>", "role": "source"}
    ]}

Usage::

    python scripts/build_research_handoff.py build --plan plan.json --output NEW_DIR
    python scripts/build_research_handoff.py verify --bundle NEW_DIR
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys


MANIFEST = "manifest.json"
COMPLETE = "COMPLETE.json"
CHUNK_SIZE = 1024 * 1024
MAX_EVIDENCE_PARQUET_BYTES = 8 * 1024 * 1024
EVIDENCE_PARQUET_ROLES = {"aggregate_summary", "bounded_metadata"}
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
FORBIDDEN_SUFFIXES = (
    ".pem", ".key", ".duckdb", ".duckdb.wal", ".sqlite",
    ".sqlite3", ".pkl", ".pickle", ".feather", ".arrow", ".h5", ".hdf5",
)


class HandoffError(ValueError):
    """The explicit handoff contract was not satisfied."""


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")


def _exact_keys(value: object, keys: set[str], label: str) -> dict:
    if not isinstance(value, dict) or set(value) != keys:
        raise HandoffError(f"{label} must contain exactly {sorted(keys)}")
    return value


def _destination(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        raise HandoffError(f"Not a portable relative destination: {value!r}")
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise HandoffError(f"Not a portable relative destination: {value!r}")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise HandoffError(f"Control character in destination: {value!r}")
    if PurePosixPath(value).is_absolute():
        raise HandoffError(f"Absolute destination: {value!r}")
    return value


def _check_file_type(path: str, role: str) -> None:
    parts = PurePosixPath(path.replace("\\", "/")).parts
    lowered = [part.casefold() for part in parts]
    name = lowered[-1]
    if (
        any(part in (".aws", ".ssh") for part in lowered)
        or name in {"credentials", "id_rsa", "id_dsa", "id_ecdsa", "id_ed25519"}
        or name == ".env" or name.startswith(".env.")
        or name.startswith("rclone.conf")
        or ("rclone" in lowered and name in {"config", "config.conf"})
        or name.endswith(FORBIDDEN_SUFFIXES)
        or (name.endswith(".parquet") and role not in EVIDENCE_PARQUET_ROLES)
    ):
        raise HandoffError(f"Credential-like or raw-data file is not allowed: {path}")


def check_file_admission(path: str, role: str, size: int) -> None:
    """Shared admission gate for explicit plan generators, builds and verification.

    A qualifying Parquet role is an operator assertion. This gate does not
    inspect columns/rows and cannot establish that a file is really aggregate
    evidence rather than a small trade sample. Audit that separately.
    """
    if not isinstance(role, str) or not role.strip():
        raise HandoffError("Empty or invalid file role")
    if type(size) is not int or size < 0:
        raise HandoffError("File size must be a nonnegative integer")
    _check_file_type(path, role)
    if path.casefold().endswith(".parquet") and size > MAX_EVIDENCE_PARQUET_BYTES:
        raise HandoffError(f"Explicit evidence Parquet exceeds the 8 MiB cap: {path}")


def _no_symlinks(path: Path) -> None:
    """Check every existing component, without resolving away a symlink."""
    if not path.is_absolute() or ".." in path.parts:
        raise HandoffError(f"Expected an absolute path without '..': {path}")
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if stat.S_ISLNK(current.lstat().st_mode):
            raise HandoffError(f"Symlink is not allowed: {current}")


def _open_source(path: Path):
    _no_symlinks(path)
    if not stat.S_ISREG(path.lstat().st_mode):
        raise HandoffError(f"Expected a regular file: {path}")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise HandoffError(f"Expected a regular file: {path}")
    return os.fdopen(descriptor, "rb")


def _fingerprint(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with _open_source(path) as handle:
        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _validate_entries(entries: object, *, plan: bool) -> list[dict]:
    if not isinstance(entries, list) or not entries:
        raise HandoffError("files must be a nonempty list")
    expected = {"source", "destination", "sha256", "role"} if plan else {
        "destination", "sha256", "size", "role"
    }
    result = []
    destinations = {MANIFEST.casefold(), COMPLETE.casefold(), ".complete.tmp"}
    prefix_spellings = {name.casefold(): name for name in (MANIFEST, COMPLETE, ".complete.tmp")}
    for index, entry in enumerate(entries):
        entry = _exact_keys(entry, expected, f"files[{index}]")
        destination = _destination(entry["destination"])
        if not isinstance(entry["role"], str) or not entry["role"].strip():
            raise HandoffError(f"Empty or invalid role for {destination}")
        _check_file_type(destination, entry["role"])
        folded = destination.casefold()
        if folded in destinations:
            raise HandoffError(f"Duplicate, case-colliding, or reserved destination: {destination}")
        destinations.add(folded)
        # Directory spellings must also agree on case-insensitive filesystems.
        parts = PurePosixPath(destination).parts
        for count in range(1, len(parts) + 1):
            prefix = "/".join(parts[:count])
            earlier = prefix_spellings.setdefault(prefix.casefold(), prefix)
            if earlier != prefix:
                raise HandoffError(f"Case-colliding destination component: {prefix}")
        if not isinstance(entry["sha256"], str) or not SHA256_PATTERN.fullmatch(entry["sha256"]):
            raise HandoffError(f"Invalid SHA-256 for {destination}")
        if plan:
            source = entry["source"]
            if not isinstance(source, str) or not Path(source).is_absolute():
                raise HandoffError(f"Source must be an absolute file path: {source!r}")
            _check_file_type(source, entry["role"])
            if source.casefold().endswith(".parquet") != destination.casefold().endswith(".parquet"):
                raise HandoffError("Parquet source and destination suffixes must agree")
        elif type(entry["size"]) is not int or entry["size"] < 0:
            raise HandoffError(f"Invalid size for {destination}")
        else:
            check_file_admission(destination, entry["role"], entry["size"])
        result.append(dict(entry))
    # A payload file must not also serve as another payload's parent directory.
    for destination in destinations:
        for parent in PurePosixPath(destination).parents:
            if str(parent) != "." and str(parent) in destinations:
                raise HandoffError(f"File/directory destination collision: {destination}")
    return sorted(result, key=lambda entry: entry["destination"])


def _release_name(value: object) -> str:
    value = _destination(value)
    if "/" in value:
        raise HandoffError("release_name must be one portable path component")
    return value


def copy_checked_file(source: Path, destination: Path, size: int, sha256: str) -> None:
    """Copy exclusively and recheck the bytes actually copied against the plan."""
    digest = hashlib.sha256()
    copied = 0
    with _open_source(source) as reader, destination.open("xb") as writer:
        for chunk in iter(lambda: reader.read(CHUNK_SIZE), b""):
            copied += len(chunk)
            if copied > size:
                raise HandoffError(f"Source changed while copying: {source}")
            writer.write(chunk)
            digest.update(chunk)
        writer.flush()
        os.fsync(writer.fileno())
    if copied != size or digest.hexdigest() != sha256:
        raise HandoffError(f"Source changed while copying: {source}")


def _write_exclusive(path: Path, contents: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(contents)
        handle.flush()
        os.fsync(handle.fileno())


def build_handoff(plan: dict, output: Path | str) -> dict:
    """Validate the plan, then create a new bundle; never overwrite or clean up."""
    plan = _exact_keys(plan, {"release_name", "files"}, "plan")
    name = _release_name(plan["release_name"])
    entries = _validate_entries(plan["files"], plan=True)
    output = Path(os.path.abspath(output))
    _no_symlinks(output.parent)
    if output.exists() or output.is_symlink():
        raise HandoffError(f"Output already exists; choose a new directory: {output}")
    validated = []
    for entry in entries:
        source = Path(entry["source"])
        _no_symlinks(source)
        check_file_admission(entry["source"], entry["role"], source.lstat().st_size)
        size, actual_hash = _fingerprint(source)
        check_file_admission(entry["source"], entry["role"], size)
        check_file_admission(entry["destination"], entry["role"], size)
        if actual_hash != entry["sha256"]:
            raise HandoffError(f"Source hash does not match the plan: {entry['source']}")
        validated.append((entry, size))

    output.mkdir(mode=0o700, exist_ok=False)
    manifest_entries = []
    for entry, size in validated:
        target = output.joinpath(*PurePosixPath(entry["destination"]).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        copy_checked_file(Path(entry["source"]), target, size, entry["sha256"])
        manifest_entries.append({
            "destination": entry["destination"], "size": size,
            "sha256": entry["sha256"], "role": entry["role"],
        })
    manifest = {"schema_version": 1, "release_name": name, "files": manifest_entries}
    manifest_bytes = _json_bytes(manifest)
    _write_exclusive(output / MANIFEST, manifest_bytes)
    marker = {
        "schema_version": 1, "release_name": name,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "file_count": len(manifest_entries),
        "total_bytes": sum(entry["size"] for entry in manifest_entries),
    }
    # A failed build remains inspectable but never receives a completion marker.
    temporary_marker = output / ".complete.tmp"
    _write_exclusive(temporary_marker, _json_bytes(marker))
    temporary_marker.rename(output / COMPLETE)
    return manifest


def verify_handoff(bundle: Path | str) -> dict:
    """Rehash every inventory member and reject missing, extra, or linked files."""
    bundle = Path(os.path.abspath(bundle))
    _no_symlinks(bundle)
    if not bundle.is_dir():
        raise HandoffError(f"Expected a bundle directory: {bundle}")
    files = set()
    directories = set()

    def inventory(directory: Path) -> None:
        with os.scandir(directory) as children:
            for child in children:
                relative = Path(child.path).relative_to(bundle).as_posix()
                if child.is_symlink():
                    raise HandoffError(f"Symlink in bundle: {relative}")
                if child.is_dir(follow_symlinks=False):
                    directories.add(relative)
                    inventory(Path(child.path))
                elif child.is_file(follow_symlinks=False):
                    files.add(relative)
                else:
                    raise HandoffError(f"Nonregular bundle member: {relative}")

    inventory(bundle)
    if MANIFEST not in files or COMPLETE not in files:
        raise HandoffError("Bundle is incomplete: manifest or completion marker is missing")
    manifest_bytes = (bundle / MANIFEST).read_bytes()
    manifest = _exact_keys(json.loads(manifest_bytes), {"schema_version", "release_name", "files"}, "manifest")
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
        raise HandoffError("Unsupported manifest schema_version")
    name = _release_name(manifest["release_name"])
    entries = _validate_entries(manifest["files"], plan=False)
    marker = json.loads((bundle / COMPLETE).read_bytes())
    _exact_keys(marker, {"schema_version", "release_name", "manifest_sha256", "file_count", "total_bytes"}, "completion marker")
    if any(type(marker[key]) is not int for key in ("schema_version", "file_count", "total_bytes")):
        raise HandoffError("Invalid completion marker numeric field")
    expected_marker = {
        "schema_version": 1, "release_name": name,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "file_count": len(entries), "total_bytes": sum(entry["size"] for entry in entries),
    }
    if marker != expected_marker:
        raise HandoffError("Completion marker does not match the manifest")
    expected_files = {entry["destination"] for entry in entries} | {MANIFEST, COMPLETE}
    if files != expected_files:
        raise HandoffError(f"Inventory mismatch: missing={sorted(expected_files - files)}, extra={sorted(files - expected_files)}")
    expected_directories = {
        str(parent)
        for entry in entries
        for parent in PurePosixPath(entry["destination"]).parents
        if str(parent) != "."
    }
    if directories != expected_directories:
        raise HandoffError("Unexpected or missing bundle directory")
    for entry in entries:
        path = bundle / entry["destination"]
        check_file_admission(entry["destination"], entry["role"], path.lstat().st_size)
        size, sha256 = _fingerprint(path)
        check_file_admission(entry["destination"], entry["role"], size)
        if size != entry["size"] or sha256 != entry["sha256"]:
            raise HandoffError(f"Size/hash mismatch: {entry['destination']}")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build", help="Create a new bundle from an explicit hash-bound plan")
    build.add_argument("--plan", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    verify = commands.add_parser("verify", help="Verify the exact bundle inventory and hashes")
    verify.add_argument("--bundle", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            with args.plan.open(encoding="utf-8") as handle:
                manifest = build_handoff(json.load(handle), args.output)
        else:
            manifest = verify_handoff(args.bundle)
    except (OSError, ValueError) as error:
        print(f"Handoff check failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps({"release_name": manifest["release_name"], "file_count": len(manifest["files"]), "status": "ok"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
