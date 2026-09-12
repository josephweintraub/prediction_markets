from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from analysis.nba_game_dynamics.build_game_timing import parse_args as parse_nba_timing_args
from analysis.nba_game_dynamics.build_validated_universe import (
    parse_args as parse_nba_validated_args,
)
from analysis.sports_game_dynamics.run_workflow import (
    WorkflowError,
    build_command_plan,
    parse_args,
    run_workflow,
)
from analysis.sports_game_dynamics.artifacts import write_parquet
from analysis.nba_game_dynamics.nba_api import parse_live_data_play_by_play


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
RAW_SCHEMA = (
    ("maker", "VARCHAR"), ("taker", "VARCHAR"),
    ("maker_asset_id", "VARCHAR"), ("taker_asset_id", "VARCHAR"),
    ("maker_amount_filled", "BIGINT"), ("taker_amount_filled", "BIGINT"),
    ("block_number", "BIGINT"), ("transaction_hash", "VARCHAR"),
    ("log_index", "INTEGER"), ("exchange_address", "VARCHAR"),
    ("condition_id", "VARCHAR"), ("outcome_token_side", "VARCHAR"),
)


def _repository(path: Path) -> Path:
    path.mkdir()
    (path / "tracked.txt").write_text("fixture\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "fixture@example.test"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Fixture"], cwd=path, check=True)
    subprocess.run(["git", "add", "tracked.txt"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=path, check=True)
    return path


def _runtime_repository(path: Path) -> Path:
    path.mkdir()
    os.symlink(ROOT / "analysis", path / "analysis", target_is_directory=True)
    (path / "tracked.txt").write_text("fixture\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.email", "fixture@example.test"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Fixture"], cwd=path, check=True)
    subprocess.run(["git", "add", "analysis", "tracked.txt"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=path, check=True)
    return path


def _write_frame(path: Path, rows: list[dict]) -> None:
    frame = pd.DataFrame(rows)
    con = duckdb.connect()
    try:
        con.register("rows", frame)
        con.execute(f"COPY rows TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


def _args(tmp_path: Path, run_id: str = "fixture"):
    inputs = tmp_path / "inputs"
    inputs.mkdir(exist_ok=True)
    paths = {}
    for name in (
        "markets", "universe_tokens", "token_map", "raw_trades",
        "block_timestamps", "wallet_flags",
    ):
        path = inputs / f"{name}.parquet"
        path.write_bytes(f"{name}\n".encode())
        paths[name] = path
    repository = _repository(tmp_path / f"repository-{run_id}")
    provider_cache = tmp_path / f"provider-cache-{run_id}"
    provider_cache.mkdir()
    arguments = [
        "--sport", "nba", "--run-root", str(tmp_path / "runs"),
        "--run-id", run_id, "--work-state", "candidate",
        "--data-vintage", "synthetic-v1", "--repository", str(repository),
        "--markets", str(paths["markets"]),
        "--provider-cache", str(provider_cache),
        "--universe-tokens", str(paths["universe_tokens"]),
        "--token-map", str(paths["token_map"]),
        "--raw-trades", str(paths["raw_trades"]),
        "--block-timestamps", str(paths["block_timestamps"]),
        "--wallet-flags", str(paths["wallet_flags"]),
        "--phase-contract", str(ROOT / "configs/game_dynamics/nba_phase_contract_v1.json"),
    ]
    return parse_args(arguments)


class _Clock:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self) -> str:
        self.value += 1
        return f"2026-09-11T00:00:{self.value:02d}Z"


class _Runner:
    def __init__(self, fail_at: int | None = None, interrupt_at: int | None = None) -> None:
        self.commands: list[list[str]] = []
        self.fail_at = fail_at
        self.interrupt_at = interrupt_at

    def __call__(self, command, *, cwd, check):
        self.commands.append(list(command))
        index = len(self.commands)
        if index == self.interrupt_at:
            raise KeyboardInterrupt()
        if index == self.fail_at:
            raise subprocess.CalledProcessError(7, command)
        run_dir = Path(command[command.index("--run-dir") + 1])
        run_dir.mkdir(parents=True)
        (run_dir / "fixture-output.txt").write_text("ok\n", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0)


def test_workflow_records_immutable_complete_sequential_run(tmp_path: Path) -> None:
    args = _args(tmp_path)
    runner = _Runner()
    run = run_workflow(args, command_runner=runner, now=_Clock())
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))

    assert manifest["status"] == "completed"
    assert manifest["run_id"] == "fixture"
    assert manifest["work_state"] == "candidate"
    assert manifest["data_vintage"] == "synthetic-v1"
    assert manifest["git"]["dirty"] is False
    assert len(manifest["git"]["commit"]) == 40
    assert manifest["environment"]["python"]
    assert manifest["environment"]["stage_interpreter"]["version"] == (
        manifest["environment"]["python"]
    )
    assert set(manifest["environment"]["packages"]) == {
        "duckdb", "numpy", "pandas", "pyarrow", "requests",
    }
    assert set(manifest["inputs"]) == {
        "markets", "universe_tokens", "token_map", "raw_trades",
        "block_timestamps", "wallet_flags", "phase_contract",
    }
    assert len(manifest["command_plan"]) == 11
    assert [row["status"] for row in manifest["command_plan"]] == ["completed"] * 11
    assert manifest["command_plan"][3]["key"] == "03_downstream_handoff"
    assert manifest["command_plan"][3]["module"].endswith("build_downstream_handoff")
    assert all(command[1] == "-m" for command in runner.commands)
    assert all("--refresh" not in command for command in runner.commands)
    assert len(manifest["outputs"]) == 11
    assert not list(run.glob(".manifest.json.*"))

    before = (run / "manifest.json").read_bytes()
    with pytest.raises(FileExistsError):
        run_workflow(args, command_runner=_Runner(), now=_Clock())
    assert (run / "manifest.json").read_bytes() == before


def test_workflow_preserves_venv_interpreter_symlink_for_child_stages(
    tmp_path: Path,
) -> None:
    args = _args(tmp_path, "venv-symlink")
    console_python = Path(sys.argv[0]).absolute().parent / "python"
    project_python = (
        console_python if console_python.is_file() else Path(sys.executable).absolute()
    )
    if not project_python.is_symlink():
        pytest.skip("The active project interpreter is not a venv-style symlink")
    args.python_executable = str(project_python)
    runner = _Runner()

    run = run_workflow(args, command_runner=runner, now=_Clock())
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))

    assert project_python.is_symlink()
    assert {command[0] for command in runner.commands} == {str(project_python.absolute())}
    assert manifest["environment"]["python_executable"] == str(project_python.absolute())
    assert manifest["environment"]["stage_interpreter"]["path"] == str(
        project_python.absolute()
    )


def test_workflow_rejects_unavailable_project_interpreter_before_writing(
    tmp_path: Path,
) -> None:
    args = _args(tmp_path, "missing-interpreter")
    args.python_executable = str(tmp_path / "missing-venv" / "bin" / "python")

    with pytest.raises(WorkflowError, match="interpreter is unavailable"):
        run_workflow(args, command_runner=_Runner(), now=_Clock())
    assert not (Path(args.run_root) / args.run_id).exists()


def test_workflow_records_failed_stage_and_preserves_pending_plan(tmp_path: Path) -> None:
    args = _args(tmp_path, "failed")
    with pytest.raises(subprocess.CalledProcessError):
        run_workflow(args, command_runner=_Runner(fail_at=3), now=_Clock())
    manifest = json.loads(
        (tmp_path / "runs" / "failed" / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["status"] == "failed"
    assert manifest["failure"]["type"] == "CalledProcessError"
    assert [row["status"] for row in manifest["command_plan"][:4]] == [
        "completed", "completed", "failed", "pending",
    ]
    assert manifest["command_plan"][2]["return_code"] == 7


def test_workflow_records_interruption(tmp_path: Path) -> None:
    args = _args(tmp_path, "interrupted")
    with pytest.raises(KeyboardInterrupt):
        run_workflow(args, command_runner=_Runner(interrupt_at=2), now=_Clock())
    manifest = json.loads(
        (tmp_path / "runs" / "interrupted" / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["status"] == "interrupted"
    assert manifest["failure"] == {
        "type": "KeyboardInterrupt", "message": "workflow interrupted",
    }
    assert manifest["command_plan"][1]["status"] == "interrupted"


def test_workflow_rejects_provider_cache_file_before_writing(tmp_path: Path) -> None:
    args = _args(tmp_path, "cache-file")
    cache = Path(args.provider_cache)
    cache.rmdir()
    cache.write_text("not a directory\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Provider cache is not a directory"):
        run_workflow(args, command_runner=_Runner(), now=_Clock())
    assert not (Path(args.run_root) / args.run_id).exists()


@pytest.mark.parametrize("outcome", ("failed", "interrupted"))
def test_workflow_preserves_primary_outcome_when_postmortem_inventory_fails(
    tmp_path: Path, outcome: str,
) -> None:
    args = _args(tmp_path, f"inventory-{outcome}")
    cache = Path(args.provider_cache)

    def break_inventory(command, *, cwd, check):
        cache.rmdir()
        cache.write_text("became a file\n", encoding="utf-8")
        if outcome == "interrupted":
            raise KeyboardInterrupt()
        raise subprocess.CalledProcessError(9, command)

    expected = KeyboardInterrupt if outcome == "interrupted" else subprocess.CalledProcessError
    with pytest.raises(expected):
        run_workflow(args, command_runner=break_inventory, now=_Clock())
    manifest = json.loads(
        (Path(args.run_root) / args.run_id / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["status"] == outcome
    assert manifest["ended_at_utc"] is not None
    assert manifest["failure"]["type"] == (
        "KeyboardInterrupt" if outcome == "interrupted" else "CalledProcessError"
    )
    assert manifest["secondary_diagnostics"] == [{
        "operation": "provider_cache_after",
        "type": "WorkflowError",
        "message": f"Provider cache is not a directory: {cache.resolve()}",
    }]


@pytest.mark.parametrize(
    "relationship",
    (
        "inside_cache", "contains_cache", "inside_repository",
        "contains_repository", "contains_input",
    ),
)
def test_workflow_rejects_run_root_ancestry_before_writing(
    tmp_path: Path, relationship: str,
) -> None:
    args = _args(tmp_path, relationship)
    if relationship == "inside_cache":
        args.run_root = str(Path(args.provider_cache) / "runs")
    elif relationship == "contains_cache":
        container = tmp_path / "containing-run-root"
        cache = container / "provider-cache"
        cache.mkdir(parents=True)
        args.run_root = str(container)
        args.provider_cache = str(cache)
    elif relationship == "inside_repository":
        args.run_root = str(Path(args.repository) / "runs")
    elif relationship == "contains_repository":
        container = tmp_path / "repository-containing-run-root"
        container.mkdir()
        args.repository = str(_repository(container / "repository"))
        args.run_root = str(container)
    else:
        args.run_root = str(Path(args.markets).parent)

    target = Path(args.run_root) / args.run_id
    with pytest.raises(ValueError, match="Run root overlaps workflow"):
        run_workflow(args, command_runner=_Runner(), now=_Clock())
    assert not target.exists()


def test_nba_cli_aliases_standardize_run_and_timing_names() -> None:
    timing = parse_nba_timing_args([
        "--candidates", "c.parquet", "--cache-dir", "cache", "--run-dir", "timing",
    ])
    legacy_timing = parse_nba_timing_args([
        "--candidates", "c.parquet", "--cache-dir", "cache", "--output-dir", "timing",
    ])
    assert timing.output_dir == legacy_timing.output_dir == "timing"

    common = [
        "--candidates", "c.parquet", "--universe-tokens", "u.parquet",
        "--token-map", "t.parquet",
    ]
    validated = parse_nba_validated_args([
        *common, "--timing-run-dir", "timing", "--run-dir", "validated",
    ])
    legacy_validated = parse_nba_validated_args([
        *common, "--timing-run", "timing", "--output-dir", "validated",
    ])
    assert validated.timing_run == legacy_validated.timing_run == "timing"
    assert validated.output_dir == legacy_validated.output_dir == "validated"


def test_runbook_freezes_automated_production_gate_and_nba_provider_limits() -> None:
    runbook = (
        ROOT / "docs/analysis_specs/sports_game_dynamics_runbook.md"
    ).read_text(encoding="utf-8")
    nba_spec = (
        ROOT / "docs/analysis_specs/nba_game_dynamics_v1.md"
    ).read_text(encoding="utf-8")

    assert "first production run for each sport and data vintage must be executed" in runbook
    assert "manual Stage 01--10 commands" in runbook
    assert "separate Stage 01--02 adapter/cache audit" in runbook
    assert "same frozen Stage-01 inputs, provider cache, and phase contract" in runbook
    assert "native_lineage.source_evidence" in runbook
    automated = runbook.split("## Automated immutable workflow command", 1)[1].split(
        "## Stages 01--03", 1
    )[0]
    assert "--refresh" not in automated

    for required in (
        "data.nba.com",
        "nba-prod-us-east-1-mediaops-stats.s3.amazonaws.com/NBA/liveData",
        "HTTP 403",
        "timeActual",
        "never interpolates or extrapolates",
    ):
        assert required in nba_spec


def test_nfl_workflow_plan_uses_supported_module_interfaces(tmp_path: Path) -> None:
    args = _args(tmp_path, "nfl-plan")
    args.sport = "nfl"
    args.phase_contract = str(
        ROOT / "configs/game_dynamics/nfl_phase_contract_v1.json"
    )
    plan = build_command_plan(args, tmp_path / "runs" / "nfl-plan")

    timing = plan[1]["command"]
    validated = plan[2]["command"]
    assert plan[1]["module"] == "analysis.nfl_game_dynamics.build_game_timing"
    assert "--phase-contract" not in timing
    assert "--timing-run-dir" in validated
    assert all(command["command"][1] == "-m" for command in plan)


def test_workflow_runs_real_cached_nba_stage01_to_10_offline(tmp_path: Path) -> None:
    inputs = tmp_path / "real-inputs"
    inputs.mkdir()
    markets = inputs / "markets.parquet"
    _write_frame(markets, [{
        "market_id": "nba-market", "event_slug": "nba-nyk-por-2025-03-12",
        "question": "Knicks vs. Trail Blazers", "n_tokens": 2,
        "n_trades_raw": 100.0, "n_buy_filtered": 40.0,
        "usd_buy_filtered": 500.0,
        "first_trade_at": pd.Timestamp("2025-03-01T00:00:00Z"),
        "last_trade_at": pd.Timestamp("2025-03-13T00:00:00Z"),
    }])
    universe = inputs / "universe_tokens.parquet"
    token_map = inputs / "token_map.parquet"
    _write_frame(universe, [
        {"market_id": "nba-market", "token_id": "nba-away", "winning_outcome": "Knicks"},
        {"market_id": "nba-market", "token_id": "nba-home", "winning_outcome": "Knicks"},
    ])
    _write_frame(token_map, [
        {"token_id": "nba-away", "condition_id": "nba-market", "outcome": "Knicks",
         "event_slug": "nba-nyk-por-2025-03-12", "question": "Knicks vs. Trail Blazers"},
        {"token_id": "nba-home", "condition_id": "nba-market", "outcome": "Trail Blazers",
         "event_slug": "nba-nyk-por-2025-03-12", "question": "Knicks vs. Trail Blazers"},
    ])
    provider_cache = tmp_path / "provider-cache-real"
    (provider_cache / "playbyplay").mkdir(parents=True)
    shutil.copyfile(
        FIXTURES / "nba_legacy_schedule.json", provider_cache / "schedule_2024.json"
    )
    shutil.copyfile(
        FIXTURES / "nba_live_data_pbp.json",
        provider_cache / "playbyplay" / "0022400953.json",
    )

    timing = parse_live_data_play_by_play(json.loads(
        (FIXTURES / "nba_live_data_pbp.json").read_text(encoding="utf-8")
    ))
    bases = (
        timing.actual_start_utc - timedelta(minutes=5),
        timing.actual_start_utc + timedelta(minutes=1),
        timing.phase_windows[1].start_utc + timedelta(minutes=1),
        timing.phase_windows[2].start_utc + timedelta(minutes=1),
        timing.phase_windows[3].start_utc + timedelta(minutes=1),
    )
    raw_rows = []
    cache_rows = []
    block = 20_000
    for base in bases:
        for decile in range(1, 11):
            price = (decile - .5) / 10
            raw_rows.append((
                "buyer", "seller", "0", "nba-home", int(price * 1_000_000),
                1_000_000, block, f"0x{block:064x}", 0, "0xexchange",
                "nba-market", "taker",
            ))
            cache_rows.append((block, int((base + timedelta(seconds=decile)).timestamp())))
            block += 1
    raw = inputs / "raw.parquet"
    block_cache = inputs / "block_timestamps.parquet"
    flags = inputs / "wallet_flags.parquet"
    write_parquet(raw, RAW_SCHEMA, raw_rows, ("block_number", "log_index"))
    write_parquet(
        block_cache, (("block_number", "BIGINT"), ("timestamp", "BIGINT")),
        cache_rows, ("block_number",),
    )
    write_parquet(
        flags, (("proxyWallet", "VARCHAR"), ("is_nonhuman", "BOOLEAN")),
        [("unrelated", True)], ("proxyWallet",),
    )
    repository = _runtime_repository(tmp_path / "runtime-repository")
    args = parse_args([
        "--sport", "nba", "--run-root", str(tmp_path / "real-runs"),
        "--run-id", "nba-offline", "--work-state", "candidate",
        "--data-vintage", "offline-fixture-v1", "--repository", str(repository),
        "--markets", str(markets), "--provider-cache", str(provider_cache),
        "--universe-tokens", str(universe), "--token-map", str(token_map),
        "--raw-trades", str(raw), "--block-timestamps", str(block_cache),
        "--wallet-flags", str(flags), "--phase-contract",
        str(ROOT / "configs/game_dynamics/nba_phase_contract_v1.json"),
    ])

    run = run_workflow(args)
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "completed"
    assert all(row["status"] == "completed" for row in manifest["command_plan"])
    assert (run / "03_handoff" / "eligible_moneylines.parquet").is_file()
    assert (run / "03_handoff" / "adapter_provenance.json").is_file()
    assert (run / "10_report" / "sports_flb_report.html").is_file()
