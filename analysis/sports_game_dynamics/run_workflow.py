"""Run one immutable NBA or NFL game-dynamics workflow sequentially."""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from .artifacts import fingerprint, resolved


STAGE_MODULES = {
    "nba": {
        "market": "analysis.nba_game_dynamics.build_market_universe",
        "timing": "analysis.nba_game_dynamics.build_game_timing",
        "validated": "analysis.nba_game_dynamics.build_validated_universe",
        "handoff": "analysis.nba_game_dynamics.build_downstream_handoff",
    },
    "nfl": {
        "market": "analysis.nfl_game_dynamics.build_market_universe",
        "timing": "analysis.nfl_game_dynamics.build_game_timing",
        "validated": "analysis.nfl_game_dynamics.build_validated_universe",
        "handoff": "analysis.nfl_game_dynamics.build_downstream_handoff",
    },
}
SHARED_MODULES = {
    "timestamp": "analysis.sports_game_dynamics.timestamp_provenance",
    "exact": "analysis.sports_game_dynamics.build_exact_trades",
    "phase": "analysis.sports_game_dynamics.build_phase_dataset",
    "closes": "analysis.sports_game_dynamics.build_dual_closes",
    "calibration": "analysis.sports_game_dynamics.estimate_calibration",
    "tails": "analysis.sports_game_dynamics.estimate_flb_tails",
    "report": "analysis.sports_game_dynamics.render_flb_report",
}
PACKAGE_NAMES = ("duckdb", "numpy", "pandas", "pyarrow", "requests")
WORK_STATES = ("exploration", "candidate", "confirmatory")


class WorkflowError(ValueError):
    """Raised when an immutable workflow cannot be launched or reconciled."""


def _overlaps(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _git_state(repository: Path) -> dict[str, Any]:
    def git(*arguments: str) -> str:
        return subprocess.run(
            ["git", *arguments], cwd=repository, check=True,
            capture_output=True, text=True,
        ).stdout.strip()

    status = git("status", "--porcelain")
    return {
        "repository": str(repository),
        "commit": git("rev-parse", "HEAD"),
        "branch": git("branch", "--show-current"),
        "dirty": bool(status),
        "status_porcelain": status.splitlines(),
    }


def _input_record(path: Path) -> dict[str, Any]:
    value = fingerprint(path)
    value["mtime_ns"] = path.stat().st_mtime_ns
    return value


def _cache_inventory(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if not path.is_dir():
        raise WorkflowError(f"Provider cache is not a directory: {path}")
    return [_input_record(item) for item in sorted(path.rglob("*")) if item.is_file()]


def _package_versions() -> dict[str, str | None]:
    values: dict[str, str | None] = {}
    for name in PACKAGE_NAMES:
        try:
            values[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            values[name] = None
    return values


def _interpreter_path(value: str) -> Path:
    """Return an absolute executable path without dereferencing venv symlinks."""

    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        located = shutil.which(value)
        if located is None:
            raise WorkflowError(f"Project Python interpreter is unavailable: {value}")
        candidate = Path(located)
    return Path(os.path.abspath(candidate))


def _validate_project_interpreter(value: str) -> dict[str, Any]:
    path = _interpreter_path(value)
    if not path.is_file() or not os.access(path, os.X_OK):
        raise WorkflowError(f"Project Python interpreter is unavailable: {path}")
    probe = (
        "import importlib.metadata,json,platform,sys;"
        f"names={PACKAGE_NAMES!r};"
        "print(json.dumps({'reported_executable':sys.executable,'prefix':sys.prefix,"
        "'version':platform.python_version(),'packages':"
        "{n:importlib.metadata.version(n) for n in names}},sort_keys=True))"
    )
    try:
        completed = subprocess.run(
            [str(path), "-c", probe], check=True, capture_output=True, text=True
        )
        observed = json.loads(completed.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError, OSError) as exc:
        raise WorkflowError(
            f"Project Python interpreter cannot load the required environment: {path}"
        ) from exc
    expected_packages = _package_versions()
    if (
        not isinstance(observed, dict)
        or not isinstance(observed.get("packages"), dict)
        or observed.get("version") != platform.python_version()
        or observed.get("packages") != expected_packages
        or any(value is None for value in expected_packages.values())
    ):
        raise WorkflowError(
            "Project Python interpreter does not match the runner's Python/package environment"
        )
    return {"path": str(path), **observed}


def _stage(
    python: str, key: str, module: str, run_dir: Path, *arguments: str
) -> dict[str, Any]:
    return {
        "key": key,
        "module": module,
        "run_dir": str(run_dir),
        "command": [python, "-m", module, *arguments, "--run-dir", str(run_dir)],
        "status": "pending",
        "started_at_utc": None,
        "ended_at_utc": None,
        "return_code": None,
    }


def build_command_plan(args: argparse.Namespace, target: Path) -> list[dict[str, Any]]:
    sport = args.sport
    modules = STAGE_MODULES[sport]
    python = str(_interpreter_path(args.python_executable))
    contract = str(resolved(args.phase_contract))
    candidates = target / "01_universe" / "candidate_markets.parquet"
    timing = target / "02_timing"
    validated = target / "03_validated"
    handoff = target / "03_handoff"
    eligible = handoff / "eligible_moneylines.parquet"
    adapter = handoff / "adapter_provenance.json"
    timestamp = target / "04_timestamp"
    exact = target / "05_exact"
    phase = target / "06_phase"
    closes = target / "07_closes"
    calibration = target / "08_calibration"
    tails = target / "09_tails"
    timing_arguments = [
        "--candidates", str(candidates),
        "--cache-dir", str(resolved(args.provider_cache)),
    ]
    if sport == "nba":
        timing_arguments.extend(("--phase-contract", contract))
    return [
        _stage(python, "01_market_universe", modules["market"], target / "01_universe",
               "--markets", str(resolved(args.markets))),
        _stage(python, "02_game_timing", modules["timing"], timing, *timing_arguments),
        _stage(python, "03_validated_universe", modules["validated"], validated,
               "--candidates", str(candidates), "--timing-run-dir", str(timing),
               "--universe-tokens", str(resolved(args.universe_tokens)),
               "--token-map", str(resolved(args.token_map))),
        _stage(python, "03_downstream_handoff", modules["handoff"], handoff,
               "--validated-run-dir", str(validated), "--phase-contract", contract),
        _stage(python, "04_timestamp_provenance", SHARED_MODULES["timestamp"], timestamp,
               "--sport", sport, "--raw-trades", str(resolved(args.raw_trades)),
               "--eligible", str(eligible), "--cache", str(resolved(args.block_timestamps)),
               "--adapter-provenance", str(adapter), "--phase-contract", contract),
        _stage(python, "05_exact_trades", SHARED_MODULES["exact"], exact,
               "--sport", sport, "--raw-trades", str(resolved(args.raw_trades)),
               "--eligible", str(eligible), "--cache", str(resolved(args.block_timestamps)),
               "--timestamp-declaration", str(timestamp / "timestamp_provenance.json"),
               "--adapter-provenance", str(adapter), "--phase-contract", contract,
               "--wallet-flags", str(resolved(args.wallet_flags))),
        _stage(python, "06_phase_dataset", SHARED_MODULES["phase"], phase,
               "--sport", sport, "--eligible", str(eligible),
               "--exact-trades", str(exact / "exact_trades.parquet"),
               "--phase-contract", contract),
        _stage(python, "07_dual_closes", SHARED_MODULES["closes"], closes,
               "--sport", sport, "--eligible", str(eligible),
               "--exact-trades", str(exact / "exact_trades.parquet")),
        _stage(python, "08_calibration", SHARED_MODULES["calibration"], calibration,
               "--sport", sport, "--game-closes", str(closes / "game_closes.parquet"),
               "--phase-trades", str(phase / "phase_trades.parquet"),
               "--phase-contract", contract),
        _stage(python, "09_flb_tails", SHARED_MODULES["tails"], tails,
               "--sport", sport, "--calibration-run-dir", str(calibration),
               "--game-closes", str(closes / "game_closes.parquet"),
               "--phase-trades", str(phase / "phase_trades.parquet"),
               "--phase-contract", contract),
        _stage(python, "10_report", SHARED_MODULES["report"], target / "10_report",
               "--sport", sport, "--calibration-run-dir", str(calibration),
               "--tail-run-dir", str(tails),
               "--timestamp-declaration", str(timestamp / "timestamp_provenance.json"),
               "--phase-contract", contract),
    ]


def _output_inventory(target: Path) -> list[dict[str, Any]]:
    records = []
    for path in sorted(target.rglob("*")):
        if path.is_file() and path != target / "manifest.json":
            record = _input_record(path)
            record["path"] = str(path.relative_to(target))
            records.append(record)
    return records


def _capture_postmortem_inventories(
    manifest: dict[str, Any], provider_cache: Path, target: Path
) -> None:
    """Record best-effort diagnostics without replacing the primary outcome."""

    diagnostics = manifest.setdefault("secondary_diagnostics", [])
    for field, operation in (
        ("provider_cache_after", lambda: _cache_inventory(provider_cache)),
        ("outputs", lambda: _output_inventory(target)),
    ):
        try:
            manifest[field] = operation()
        except Exception as exc:
            diagnostics.append({
                "operation": field,
                "type": type(exc).__name__,
                "message": str(exc),
            })


def run_workflow(
    args: argparse.Namespace,
    *,
    command_runner: Callable[..., Any] = subprocess.run,
    now: Callable[[], str] = _utc_now,
) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", args.run_id):
        raise WorkflowError("Run ID must contain only letters, digits, dot, underscore, or hyphen")
    run_root = resolved(args.run_root)
    target = run_root / args.run_id
    if target.exists():
        raise FileExistsError(f"Immutable workflow run already exists: {target}")

    repository = resolved(args.repository)
    provider_cache = resolved(args.provider_cache)
    named_inputs = {
        "markets": resolved(args.markets),
        "universe_tokens": resolved(args.universe_tokens),
        "token_map": resolved(args.token_map),
        "raw_trades": resolved(args.raw_trades),
        "block_timestamps": resolved(args.block_timestamps),
        "wallet_flags": resolved(args.wallet_flags),
        "phase_contract": resolved(args.phase_contract),
    }
    if not repository.is_dir():
        raise WorkflowError(f"Repository is not a directory: {repository}")
    if not provider_cache.is_dir():
        raise WorkflowError(f"Provider cache is not a directory: {provider_cache}")
    protected = {"repository": repository, "provider_cache": provider_cache, **named_inputs}
    for name, path in protected.items():
        if _overlaps(run_root, path):
            raise WorkflowError(f"Run root overlaps workflow {name}: {path}")
    for name, path in named_inputs.items():
        if not path.is_file():
            raise FileNotFoundError(f"Workflow input {name} does not exist: {path}")
    git = _git_state(repository)
    if args.work_state == "confirmatory" and git["dirty"]:
        raise WorkflowError("Confirmatory workflow requires a clean Git worktree")
    stage_interpreter = _validate_project_interpreter(args.python_executable)
    environment = {
        "python": platform.python_version(),
        "python_executable": stage_interpreter["path"],
        "platform": platform.platform(),
        "packages": stage_interpreter["packages"],
        "runner_executable": sys.executable,
        "stage_interpreter": stage_interpreter,
    }
    input_records = {name: _input_record(path) for name, path in named_inputs.items()}
    provider_cache_before = _cache_inventory(provider_cache)
    plan = build_command_plan(args, target)

    target.parent.mkdir(parents=True, exist_ok=True)
    target.mkdir()
    manifest_path = target / "manifest.json"
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "analysis": f"{args.sport}_game_dynamics_v1",
        "sport": args.sport,
        "run_id": args.run_id,
        "run_root": str(run_root),
        "run_dir": str(target),
        "work_state": args.work_state,
        "data_vintage": args.data_vintage,
        "invocation": list(args.invocation),
        "started_at_utc": now(),
        "ended_at_utc": None,
        "status": "initializing",
        "git": None,
        "environment": None,
        "inputs": None,
        "provider_cache_before": None,
        "provider_cache_after": None,
        "command_plan": [],
        "outputs": [],
        "failure": None,
        "secondary_diagnostics": [],
    }
    _atomic_json(manifest_path, manifest)
    active_stage: dict[str, Any] | None = None
    try:
        manifest.update({
            "status": "running",
            "git": git,
            "environment": environment,
            "inputs": input_records,
            "provider_cache_before": provider_cache_before,
            "command_plan": plan,
        })
        _atomic_json(manifest_path, manifest)
        for stage in plan:
            active_stage = stage
            stage["status"] = "running"
            stage["started_at_utc"] = now()
            _atomic_json(manifest_path, manifest)
            completed = command_runner(
                stage["command"], cwd=repository, check=True
            )
            stage["return_code"] = int(getattr(completed, "returncode", 0))
            output = Path(stage["run_dir"])
            if not output.is_dir() or not any(output.iterdir()):
                raise WorkflowError(f"Stage did not publish a nonempty run directory: {stage['key']}")
            stage["status"] = "completed"
            stage["ended_at_utc"] = now()
            _atomic_json(manifest_path, manifest)
        manifest.update({
            "status": "completed",
            "ended_at_utc": now(),
            "provider_cache_after": _cache_inventory(provider_cache),
            "outputs": _output_inventory(target),
        })
        _atomic_json(manifest_path, manifest)
        return target
    except KeyboardInterrupt as exc:
        if active_stage is not None and active_stage["status"] == "running":
            active_stage["status"] = "interrupted"
            active_stage["ended_at_utc"] = now()
        manifest.update({
            "status": "interrupted", "ended_at_utc": now(),
            "failure": {"type": type(exc).__name__, "message": "workflow interrupted"},
        })
        _capture_postmortem_inventories(manifest, provider_cache, target)
        _atomic_json(manifest_path, manifest)
        raise
    except Exception as exc:
        if active_stage is not None and active_stage["status"] == "running":
            active_stage["status"] = "failed"
            active_stage["ended_at_utc"] = now()
            if isinstance(exc, subprocess.CalledProcessError):
                active_stage["return_code"] = exc.returncode
        manifest.update({
            "status": "failed", "ended_at_utc": now(),
            "failure": {"type": type(exc).__name__, "message": str(exc)},
        })
        _capture_postmortem_inventories(manifest, provider_cache, target)
        _atomic_json(manifest_path, manifest)
        raise


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sport", choices=tuple(STAGE_MODULES), required=True)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--work-state", choices=WORK_STATES, required=True)
    parser.add_argument("--data-vintage", required=True)
    parser.add_argument("--repository", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--python-executable", default=sys.executable)
    parser.add_argument("--markets", required=True)
    parser.add_argument("--provider-cache", required=True)
    parser.add_argument("--universe-tokens", required=True)
    parser.add_argument("--token-map", required=True)
    parser.add_argument("--raw-trades", required=True)
    parser.add_argument("--block-timestamps", required=True)
    parser.add_argument("--wallet-flags", required=True)
    parser.add_argument("--phase-contract", required=True)
    args = parser.parse_args(argv)
    args.invocation = [sys.executable, "-m", __name__, *(argv if argv is not None else sys.argv[1:])]
    return args


def main(argv: Sequence[str] | None = None) -> None:
    print(run_workflow(parse_args(argv)))


if __name__ == "__main__":
    main()
