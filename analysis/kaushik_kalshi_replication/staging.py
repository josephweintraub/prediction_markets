"""Serial, verified Dropbox staging; no manifest-format or research assumptions.

The caller supplies normalized, pinned entries and a callback that completes and
durably saves its outputs before returning JSON provenance. Production staging
always uses the canonical-host guard. Tests replace the guard and transfer call
on tiny temporary fixtures; there is no local-production bypass flag.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import tempfile
from typing import Callable, Iterable

import production_guard


RUN_ROOT = Path("/mnt/data/runs")
FREE_SPACE_FLOOR_BYTES = 20_000_000_000
MAX_RECEIPT_BYTES = 1_048_576


class StagingBlocked(RuntimeError):
    """A source, ownership, capacity or publication gate failed."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise StagingBlocked(message)


def _relative_path(value: str) -> str:
    _require(isinstance(value, str) and bool(value), "nonempty manifest path required")
    _require(not any(ord(char) < 32 for char in value), "control character in source path")
    _require("\\" not in value and ":" not in value, "invalid source path separator")
    path = PurePosixPath(value)
    _require(not path.is_absolute() and all(part not in ("", ".", "..") for part in value.split("/")),
             "source path traversal or empty component")
    _require(path.as_posix() == value, "source path must already be normalized")
    return value


@dataclass(frozen=True)
class ManifestEntry:
    """A path relative to remote_root, its byte size, and lowercase SHA-256."""

    path: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        _relative_path(self.path)
        _require(type(self.size_bytes) is int and self.size_bytes > 0, "positive integer source size required")
        _require(isinstance(self.sha256, str) and re.fullmatch(r"[0-9a-f]{64}", self.sha256) is not None,
                 "pinned lowercase SHA-256 required")


def _identity(path: Path) -> dict:
    info = path.lstat()
    _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
             "staged source must be a regular file with one link")
    _require(info.st_uid == os.getuid(), "staged source is not owned by this process user")
    return {"device": info.st_dev, "inode": info.st_ino, "bytes": info.st_size,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}


def _verify_source(path: Path, entry: ManifestEntry, expected_identity: dict | None = None) -> dict:
    _require(path.resolve(strict=True) == path, "staged source path contains a symlink")
    before = _identity(path)
    _require(before["bytes"] == entry.size_bytes, "staged source size mismatch: " + entry.path)
    if expected_identity is not None:
        _require(before == expected_identity, "staged source changed during callback")
    digest = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        _require((opened.st_dev, opened.st_ino) == (before["device"], before["inode"]),
                 "staged source replaced while opening")
        for block in iter(lambda: stream.read(1_048_576), b""):
            digest.update(block)
    _require(_identity(path) == before, "staged source changed during verification")
    _require(digest.hexdigest() == entry.sha256, "staged source SHA-256 mismatch: " + entry.path)
    return before


def _directory_identity(path: Path) -> tuple[int, int]:
    _require(path.resolve(strict=True) == path, "run directory contains a symlink")
    info = path.lstat()
    _require(stat.S_ISDIR(info.st_mode), "run directory is not a directory")
    return info.st_dev, info.st_ino


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _capacity(path: Path, required_bytes: int) -> int:
    available = shutil.disk_usage(path).free
    _require(available >= required_bytes,
             f"insufficient free space: {available} bytes available; {required_bytes} required")
    return available


def _write_receipt(path: Path, receipt: dict) -> None:
    raw = (json.dumps(receipt, sort_keys=True, indent=2, allow_nan=False) + "\n").encode("utf-8")
    _require(len(raw) <= MAX_RECEIPT_BYTES, "stage receipt exceeds bounded size")
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    _fsync_directory(path.parent)


def stage_and_process(
    entries: Iterable[ManifestEntry],
    *,
    remote_root: str,
    run_dir: Path,
    output_spill_reserve_bytes: int,
    process: Callable[[Path, ManifestEntry], dict],
) -> tuple[Path, ...]:
    """Stage, verify, process, receipt, and remove one temporary shard at a time.

    ``run_dir`` must be an existing immutable run below /mnt/data/runs. This call
    reserves its new ``source_stage_v1`` directory and refuses any existing one;
    retries therefore require a new run/stage rather than an implicit resume.
    The nonnegative reserve is an explicit upper allowance for callback outputs
    and spill. Before each transfer, free space must cover the shard, that full
    reserve, one byte of transfer EOF headroom, and the fixed 20 GB floor.
    An exact-file metadata stat must match the pinned size before copyto. The
    transfer uses no file filters: copyto rejects filters on a single-file
    source. Its hard byte limit is pinned size plus one byte; metadata can change
    after the stat, so this limit and the post-transfer size/SHA check protect
    admission rather than the stat alone. The floor is checked after callback
    and receipt publication. The callback must not alter or move its input and
    must durably publish its own outputs before returning JSON provenance.

    Only the exact, unchanged, verified temporary source is unlinked after a
    successful callback and fsynced immutable receipt. Directories and all files
    present on error are preserved; no source or preexisting dataset is removed.
    """
    production_guard.require_production_host()
    pinned = tuple(entries)
    _require(bool(pinned) and all(type(entry) is ManifestEntry for entry in pinned),
             "nonempty normalized ManifestEntry sequence required")
    _require(len({entry.path for entry in pinned}) == len(pinned), "duplicate manifest source paths")
    _require(type(output_spill_reserve_bytes) is int and output_spill_reserve_bytes >= 0,
             "nonnegative integer output/spill reserve required")
    _require(callable(process), "explicit processing callback required")
    _require(isinstance(remote_root, str) and remote_root.startswith("dropbox:"),
             "explicit Dropbox remote root required")
    remote_path = remote_root[len("dropbox:"):].removesuffix("/")
    _relative_path(remote_path)
    remote_root = "dropbox:" + remote_path
    run_dir = Path(run_dir)
    _require(run_dir.is_absolute(), "absolute run directory required")
    _directory_identity(run_dir)
    _require(run_dir != RUN_ROOT.resolve() and run_dir.is_relative_to(RUN_ROOT.resolve()),
             "run directory must be below the canonical run root")
    stage_root = run_dir / "source_stage_v1"
    _require(not os.path.lexists(stage_root), "immutable stage destination already exists")
    reserved = FREE_SPACE_FLOOR_BYTES + output_spill_reserve_bytes
    _capacity(run_dir, reserved + max(entry.size_bytes for entry in pinned) + 1)
    stage_root.mkdir(mode=0o700)
    receipts_dir = stage_root / "receipts"
    receipts_dir.mkdir(mode=0o700)
    owned_directories = {path: _directory_identity(path) for path in (run_dir, stage_root, receipts_dir)}
    _fsync_directory(stage_root)
    _fsync_directory(run_dir)
    receipts = []
    for index, entry in enumerate(pinned):
        for path, identity in owned_directories.items():
            _require(_directory_identity(path) == identity, "owned stage directory changed")
        _capacity(run_dir, reserved + entry.size_bytes + 1)
        remote = remote_root + "/" + entry.path
        stat_command = ["rclone", "lsjson", remote, "--stat", "--no-mimetype"]
        result = subprocess.run(stat_command, check=True, capture_output=True, text=True)
        _require(len(result.stdout.encode("utf-8")) <= 65_536, "remote stat exceeds metadata size bound")
        remote_stat = json.loads(result.stdout)
        _require(type(remote_stat) is dict and remote_stat.get("IsDir") is False,
                 "remote stat must identify one file")
        _require(type(remote_stat.get("Size")) is int and remote_stat["Size"] == entry.size_bytes,
                 "remote stat size differs from pinned manifest: " + entry.path)
        _require(remote_stat.get("Name") == PurePosixPath(entry.path).name,
                 "remote stat name differs from pinned manifest: " + entry.path)
        temporary_dir = Path(tempfile.mkdtemp(prefix=f"shard-{index:06d}-", dir=stage_root))
        temporary_identity = _directory_identity(temporary_dir)
        staged_path = temporary_dir / PurePosixPath(entry.path).name
        _require(not os.path.lexists(staged_path), "temporary destination already exists")
        command = ["rclone", "copyto", remote, str(staged_path), "--immutable",
                   "--transfers", "1", "--checkers", "1", "--multi-thread-streams", "0",
                   "--max-transfer", f"{entry.size_bytes + 1}B", "--cutoff-mode", "HARD"]
        subprocess.run(command, check=True)
        verified_identity = _verify_source(staged_path, entry)
        _capacity(run_dir, reserved)
        callback_provenance = process(staged_path, entry)
        _require(type(callback_provenance) is dict, "callback must return JSON provenance object")
        _capacity(run_dir, FREE_SPACE_FLOOR_BYTES)
        _verify_source(staged_path, entry, verified_identity)
        for path, identity in owned_directories.items():
            _require(_directory_identity(path) == identity, "owned stage directory changed")
        _require(_directory_identity(temporary_dir) == temporary_identity, "temporary directory changed")
        receipt_path = receipts_dir / f"{index:06d}.json"
        receipt = {"schema_version": "kalshi_source_stage_receipt_v1", "status": "processed_verified",
                   "completed_utc": datetime.now(timezone.utc).isoformat(),
                   "entry": asdict(entry), "remote": remote, "temporary_source": str(staged_path),
                   "verified_identity": verified_identity, "transfer_command": command,
                   "remote_stat_command": stat_command,
                   "remote_stat": {key: remote_stat[key] for key in ("Name", "Path", "Size", "IsDir", "ID", "ModTime") if key in remote_stat},
                   "free_space_floor_bytes": FREE_SPACE_FLOOR_BYTES,
                   "output_spill_reserve_bytes": output_spill_reserve_bytes,
                   "callback_provenance": callback_provenance}
        _write_receipt(receipt_path, receipt)
        _capacity(run_dir, FREE_SPACE_FLOOR_BYTES)
        _require(_directory_identity(temporary_dir) == temporary_identity, "temporary directory changed")
        _require(staged_path.resolve(strict=True) == staged_path and _identity(staged_path) == verified_identity,
                 "verified temporary source changed before cleanup")
        staged_path.unlink()
        receipts.append(receipt_path)
    return tuple(receipts)
