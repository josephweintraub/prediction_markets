"""Create and finalize immutable analysis-run directories and manifests."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import platform
import shutil
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_SUBDIRECTORIES = ("config", "intermediates", "tables", "figures", "report", "logs")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def git_state(repository: Path) -> dict[str, Any]:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=repository, check=True, capture_output=True, text=True
        ).stdout.strip()

    status = git("status", "--porcelain")
    return {
        "commit": git("rev-parse", "HEAD"),
        "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "dirty": bool(status),
        "status_porcelain": status.splitlines(),
    }


def file_fingerprint(path: str | Path, hash_limit_bytes: int = 512 * 1024 * 1024) -> dict:
    item = Path(path)
    stat = item.stat()
    result = {"path": str(item), "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    if item.is_file() and stat.st_size <= hash_limit_bytes:
        digest = hashlib.sha256()
        with item.open("rb") as handle:
            for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
                digest.update(block)
        result["sha256"] = digest.hexdigest()
    return result


def environment_versions() -> dict[str, str]:
    versions = {"python": platform.python_version(), "platform": platform.platform()}
    for package in ("duckdb", "numpy", "pandas", "pyarrow"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = "not-installed"
    return versions


def create_run(
    run_root: str | Path,
    analysis: str,
    analysis_state: str,
    repository: Path,
    command: list[str],
    vintage_path: str | Path,
    parameters: dict[str, Any],
    run_id: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    git = git_state(repository)
    if analysis_state == "confirmatory" and git["dirty"]:
        raise RuntimeError("Confirmatory runs require a clean Git worktree")
    now = datetime.now(timezone.utc)
    run_id = run_id or f"{now.strftime('%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    directory = Path(run_root) / f"{now.date().isoformat()}_{analysis}_{run_id}"
    directory.mkdir(parents=True, exist_ok=False)
    for name in RUN_SUBDIRECTORIES:
        (directory / name).mkdir()
    vintage_copy = directory / "config" / Path(vintage_path).name
    shutil.copy2(vintage_path, vintage_copy)
    manifest = {
        "schema_version": 1,
        "analysis": analysis,
        "analysis_state": analysis_state,
        "run_id": run_id,
        "run_directory": str(directory),
        "status": "running",
        "started_at": utc_now(),
        "command": command,
        "parameters": parameters,
        "git": git,
        "environment": environment_versions(),
        "configuration": {"data_vintage": str(vintage_copy)},
        "inputs": {},
        "outputs": [],
        "validations": {},
    }
    write_manifest(directory, manifest)
    return directory, manifest


def write_manifest(directory: Path, manifest: dict[str, Any]) -> None:
    temporary = directory / "manifest.json.tmp"
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")
    temporary.replace(directory / "manifest.json")


def finalize_run(
    directory: Path,
    manifest: dict[str, Any],
    status: str,
    error: str | None = None,
) -> None:
    manifest["status"] = status
    manifest["finished_at"] = utc_now()
    if error:
        manifest["error"] = error
    write_manifest(directory, manifest)
