from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from analysis.nba_game_dynamics.build_downstream_handoff import (
    build_downstream_handoff as build_nba_handoff,
)
from analysis.nba_game_dynamics.build_game_timing import (
    build_game_timing_audit,
    cache_inventory_fingerprint,
)
from analysis.nba_game_dynamics.artifact_manifest import file_fingerprint
from analysis.nba_game_dynamics.build_validated_universe import (
    build_validated_universe as build_nba_validated,
)
from analysis.nba_game_dynamics.nba_api import (
    ACTUAL_END_EVENT as NBA_END_EVENT,
    ACTUAL_START_EVENT as NBA_START_EVENT,
    PERIOD_BOUNDARY_EVENT as NBA_PERIOD_EVENT,
    parse_legacy_schedule,
    parse_live_data_play_by_play,
)
from analysis.nfl_game_dynamics.build_downstream_handoff import (
    build_downstream_handoff as build_nfl_handoff,
)
from analysis.nfl_game_dynamics.build_game_timing import build_from_payloads
from analysis.nfl_game_dynamics.build_market_universe import build_market_universe
from analysis.nfl_game_dynamics.build_validated_universe import (
    build_validated_universe as build_nfl_validated,
)
from analysis.sports_game_dynamics.artifacts import write_parquet
from analysis.sports_game_dynamics.adapter_handoff import (
    build_adapter_handoff,
    load_and_verify_adapter_handoff,
)
from analysis.sports_game_dynamics.artifacts import (
    ArtifactError,
    artifact_fingerprint,
    write_json,
)
from analysis.sports_game_dynamics.build_dual_closes import build_dual_closes
from analysis.sports_game_dynamics.build_exact_trades import build_exact_trades
from analysis.sports_game_dynamics.build_phase_dataset import build_phase_dataset
from analysis.sports_game_dynamics.estimate_calibration import estimate_calibration
from analysis.sports_game_dynamics.estimate_flb_tails import estimate_flb_tails
from analysis.sports_game_dynamics.render_flb_report import render_flb_report
from analysis.sports_game_dynamics.schemas import ELIGIBLE_SCHEMA
from analysis.sports_game_dynamics.timestamp_provenance import (
    build_timestamp_declaration,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
CONTRACTS = {
    "nba": ROOT / "configs" / "game_dynamics" / "nba_phase_contract_v2.json",
    "nfl": ROOT / "configs" / "game_dynamics" / "nfl_phase_contract_v1.json",
}
RAW_SCHEMA = (
    ("maker", "VARCHAR"), ("taker", "VARCHAR"),
    ("maker_asset_id", "VARCHAR"), ("taker_asset_id", "VARCHAR"),
    ("maker_amount_filled", "BIGINT"), ("taker_amount_filled", "BIGINT"),
    ("block_number", "BIGINT"), ("transaction_hash", "VARCHAR"),
    ("log_index", "INTEGER"), ("exchange_address", "VARCHAR"),
    ("condition_id", "VARCHAR"), ("outcome_token_side", "VARCHAR"),
)
CACHE_SCHEMA = (("block_number", "BIGINT"), ("timestamp", "BIGINT"))
FLAG_SCHEMA = (("proxyWallet", "VARCHAR"), ("is_nonhuman", "BOOLEAN"))


def _payload(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _write_frame(path: Path, rows: list[dict]) -> None:
    frame = pd.DataFrame(rows)
    con = duckdb.connect()
    try:
        con.register("rows", frame)
        con.execute(f"COPY rows TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


class _NbaClient:
    def __init__(self, cache_dir: Path) -> None:
        cache_dir.mkdir(parents=True)
        self.cache_dir = cache_dir
        self.schedule_cache = cache_dir / "schedule_2024.json"
        self.pbp_cache = cache_dir / "playbyplay" / "0022400953.json"
        self.pbp_cache.parent.mkdir()
        self.schedule_cache.write_bytes(
            json.dumps(_payload("nba_legacy_schedule.json"), sort_keys=True).encode()
        )
        self.pbp_cache.write_bytes(
            json.dumps(_payload("nba_live_data_pbp.json"), sort_keys=True).encode()
        )

    def schedule_games(self, start: date, end: date, *, refresh: bool = False):
        assert start == end == date(2025, 3, 12)
        return (parse_legacy_schedule(_payload("nba_legacy_schedule.json"), 2024)[0],)

    def game_timing(self, game_id: str, *, refresh: bool = False):
        return parse_live_data_play_by_play(
            _payload("nba_live_data_pbp.json"), expected_game_id=game_id
        )

    def provenance_manifest(self) -> dict:
        resources = []
        for url, path in (
            ("https://data.nba.com/2024/schedule.json", self.schedule_cache),
            ("https://official.example/playbyplay_0022400953.json", self.pbp_cache),
        ):
            payload = path.read_bytes()
            resources.append({
                "url": url, "cache_path": str(path.resolve()), "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(), "source": "cache",
            })
        return {
            "schema_version": 1,
            "schedule_provider": "official_nba_data_nba_com_historical_schedule",
            "timing_provider": "official_nba_livedata_s3_origin",
            "actual_start_event": NBA_START_EVENT,
            "actual_end_event": NBA_END_EVENT,
            "period_boundary_event": NBA_PERIOD_EVENT,
            "resources": resources,
        }


def _nba_test_contract(root: Path, candidates: Path, cache: Path) -> Path:
    payload = json.loads(CONTRACTS["nba"].read_text(encoding="utf-8"))
    inventory = cache_inventory_fingerprint(cache)
    payload["audit_scope"].update({
        "cache_inventory_name": inventory["name"],
        "cache_inventory_sha256": inventory["sha256"],
        "candidate_artifact_sha256": file_fingerprint(candidates)["sha256"],
        "candidate_date_from": "2025-03-12",
        "candidate_date_to": "2025-03-12",
        "candidate_market_rows": 1,
        "matched_completed_date_from": "2025-03-12",
        "matched_completed_date_to": "2025-03-12",
        "schedule_resources": 1,
        "play_by_play_resources": 1,
        "matched_completed_games": 1,
        "opening_rule_passes": 1,
        "opening_rule_failures": 0,
        "opening_failure_game_id": "0022400887",
        "schedule_score_mismatches": 0,
        "schedule_score_mismatch_game_id": "0022400072",
        "reconciled_timing_games": 1,
        "overtime_games": 1,
        "single_overtime_games": 1,
        "double_overtime_games": 0,
        "spurious_period_5_games": 0,
    })
    path = root / "nba-test-contract-v2.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def _nba_stage03(root: Path) -> Path:
    candidates = root / "nba_candidates.parquet"
    _write_frame(candidates, [{
        "market_id": "nba-market", "event_slug": "nba-nyk-por-2025-03-12",
        "date": date(2025, 3, 12), "away": "nyk", "home": "por",
        "question": "Knicks vs. Trail Blazers", "n_tokens": 2,
        "n_trades_raw": 100.0, "n_buy_filtered": 40.0,
        "usd_buy_filtered": 500.0,
        "first_trade_at": pd.Timestamp("2025-03-01T00:00:00Z"),
        "last_trade_at": pd.Timestamp("2025-03-13T00:00:00Z"),
    }])
    timing = root / "nba_timing"
    cache = root / "nba_cache"
    client = _NbaClient(cache)
    build_game_timing_audit(
        candidates, cache, timing, client=client,
        phase_contract_path=_nba_test_contract(root, candidates, cache),
    )
    universe = root / "nba_tokens.parquet"
    token_map = root / "nba_token_map.parquet"
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
    validated = root / "nba_stage03"
    build_nba_validated(candidates, timing, universe, token_map, validated)
    return validated


def _nfl_stage03(root: Path) -> Path:
    source = root / "nfl_source.parquet"
    _write_frame(source, [{
        "market_id": "nfl-market", "event_slug": "nfl-la-lv-2025-01-05",
        "question": "Rams vs. Raiders", "n_tokens": 2,
        "n_trades_raw": 100, "n_buy_filtered": 60,
        "usd_buy_filtered": 1200.0,
        "first_trade_at": pd.Timestamp("2025-01-01T00:00:00Z"),
        "last_trade_at": pd.Timestamp("2025-01-06T05:00:00Z"),
    }])
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW markets AS SELECT * FROM read_parquet('{source}')")
        universe_run = root / "nfl_universe"
        build_market_universe(con, "markets", source, universe_run)
        candidates = universe_run / "candidate_markets.parquet"
        candidate_rows = con.execute(
            f"SELECT * FROM read_parquet('{candidates}')"
        ).fetchdf().to_dict("records")
    finally:
        con.close()
    scoreboard_source = root / "nfl_scoreboard_source.json"
    summary_source = root / "nfl_summary_source.json"
    scoreboard_source.write_bytes((FIXTURES / "nfl_scoreboard.json").read_bytes())
    summary_source.write_bytes((FIXTURES / "nfl_summary.json").read_bytes())
    timing = root / "nfl_timing"
    build_from_payloads(
        candidate_rows, [_payload("nfl_scoreboard.json")],
        {"9001": _payload("nfl_summary.json")}, timing,
        candidate_source=candidates,
        scoreboard_sources=[scoreboard_source],
        summary_sources={"9001": summary_source},
    )
    universe = root / "nfl_tokens.parquet"
    token_map = root / "nfl_token_map.parquet"
    _write_frame(universe, [
        {"token_id": "nfl-away", "market_id": "nfl-market", "winning_outcome": "Rams"},
        {"token_id": "nfl-home", "market_id": "nfl-market", "winning_outcome": "Rams"},
    ])
    _write_frame(token_map, [
        {"token_id": "nfl-away", "condition_id": "nfl-market", "outcome": "Raiders"},
        {"token_id": "nfl-home", "condition_id": "nfl-market", "outcome": "Rams"},
    ])
    validated = root / "nfl_stage03"
    build_nfl_validated(candidates, timing, universe, token_map, validated)
    return validated


def _schema(path: Path) -> tuple[tuple[str, str], ...]:
    con = duckdb.connect()
    try:
        return tuple((row[0], row[1]) for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).fetchall())
    finally:
        con.close()


def _stage03_contract(stage03: Path, sport: str) -> Path:
    if sport != "nba":
        return CONTRACTS[sport]
    manifest = json.loads(
        (stage03 / "validated_manifest.json").read_text(encoding="utf-8")
    )
    return Path(manifest["inputs"]["phase_contract"]["path"])


def _run_stage04_to_10(
    root: Path, sport: str, handoff: Path, contract: Path
) -> str:
    root.mkdir(parents=True)
    eligible = handoff / "eligible_moneylines.parquet"
    con = duckdb.connect()
    try:
        row = con.execute(f"SELECT * FROM read_parquet('{eligible}')").fetchdf().iloc[0]
    finally:
        con.close()
    raw_rows: list[tuple] = []
    cache_rows: list[tuple[int, int]] = []
    block = 10_000
    bases = (
        row["actual_start_utc"].to_pydatetime() - timedelta(minutes=5),
        row["actual_start_utc"].to_pydatetime() + timedelta(minutes=1),
        row["period_2_start_utc"].to_pydatetime() + timedelta(minutes=1),
        row["period_3_start_utc"].to_pydatetime() + timedelta(minutes=1),
        row["period_4_start_utc"].to_pydatetime() + timedelta(minutes=1),
    )
    for base in bases:
        for decile in range(1, 11):
            price = (decile - 0.5) / 10
            usdc = int(round(price * 1_000_000))
            raw_rows.append((
                "buyer", "seller", "0", row["home_token_id"], usdc, 1_000_000,
                block, f"0x{block:064x}", 0, "0xexchange", row["market_id"], "taker",
            ))
            cache_rows.append((block, int((base + timedelta(seconds=decile)).timestamp())))
            block += 1
    raw = root / "resolved.parquet"
    cache = root / "timestamps.parquet"
    flags = root / "flags.parquet"
    write_parquet(raw, RAW_SCHEMA, raw_rows, ("block_number", "log_index"))
    write_parquet(cache, CACHE_SCHEMA, cache_rows, ("block_number",))
    write_parquet(flags, FLAG_SCHEMA, [("unrelated-wallet", True)], ("proxyWallet",))
    stage04 = root / "04_timestamp"
    declaration = build_timestamp_declaration(
        sport, raw, eligible, cache, handoff / "adapter_provenance.json", contract, stage04
    )
    assert declaration["missing_blocks"] == declaration["fallback_rows"] == 0
    stage05 = root / "05_exact"
    build_exact_trades(
        sport, raw, eligible, cache, stage04 / "timestamp_provenance.json",
        handoff / "adapter_provenance.json", contract, flags, stage05,
    )
    stage06 = root / "06_phase"
    build_phase_dataset(sport, eligible, stage05 / "exact_trades.parquet", contract, stage06)
    stage07 = root / "07_close"
    build_dual_closes(sport, eligible, stage05 / "exact_trades.parquet", stage07)
    stage08 = root / "08_calibration"
    estimate_calibration(
        sport, stage07 / "game_closes.parquet", stage06 / "phase_trades.parquet",
        contract, stage08,
    )
    stage09 = root / "09_tails"
    estimate_flb_tails(
        sport, stage08, stage07 / "game_closes.parquet",
        stage06 / "phase_trades.parquet", contract, stage09,
    )
    stage10 = root / "10_report"
    render_flb_report(
        sport, stage08, stage09, stage04 / "timestamp_provenance.json",
        contract, stage10,
    )
    con = duckdb.connect()
    try:
        assert con.execute(
            f"SELECT count(*) FROM read_parquet('{stage08 / 'trade_phase_calibration.parquet'}')"
        ).fetchone()[0] == 100
        assert con.execute(
            f"SELECT count(*) FROM read_parquet('{stage09 / 'flb_tail_summary.parquet'}')"
        ).fetchone()[0] == 12
    finally:
        con.close()
    return (stage10 / "sports_flb_report.html").read_text(encoding="utf-8")


@pytest.mark.parametrize("sport", ("nba", "nfl"))
def test_real_adapter_stage03_handoff_runs_shared_stage04_to_10(
    tmp_path: Path, sport: str
) -> None:
    stage03 = _nba_stage03(tmp_path) if sport == "nba" else _nfl_stage03(tmp_path)
    contract = _stage03_contract(stage03, sport)
    handoff = tmp_path / f"{sport}_handoff"
    builder = build_nba_handoff if sport == "nba" else build_nfl_handoff
    provenance = builder(stage03, contract, handoff)

    eligible = handoff / "eligible_moneylines.parquet"
    assert {path.name for path in handoff.iterdir()} == {
        "eligible_moneylines.parquet", "adapter_provenance.json",
    }
    assert _schema(eligible) == ELIGIBLE_SCHEMA
    assert provenance["sport"] == sport
    assert provenance["schema_version"] == 2
    assert "reconciliation" not in provenance
    assert provenance["native_lineage"]["stage"] == f"{sport}_validated_universe"
    assert provenance["native_lineage"]["manifest"]["path"] == str(
        (stage03 / "validated_manifest.json").resolve()
    )
    assert provenance["native_lineage"]["summary"]["path"] == str(
        (stage03 / "summary.json").resolve()
    )
    assert provenance["native_lineage"]["native_eligible"]["path"] == str(
        (stage03 / "eligible_moneylines.parquet").resolve()
    )
    assert provenance["native_lineage"]["source_evidence"]
    assert provenance["phase_contract"]["sha256"] == hashlib.sha256(
        contract.read_bytes()
    ).hexdigest()
    expected_provider = (
        "NBA official data API" if sport == "nba"
        else "ESPN site API (third-party undocumented endpoint)"
    )
    expected_status = "official" if sport == "nba" else "third_party_undocumented"
    assert provenance["source_provider"] == expected_provider
    assert provenance["source_status"] == expected_status

    html = _run_stage04_to_10(
        tmp_path / f"{sport}_downstream", sport, handoff, contract
    )
    assert f"Timing provider: <code>{expected_provider}</code>" in html
    assert f"source status: <code>{expected_status}</code>" in html
    assert html.count('<section class="panel">') == 5


@pytest.mark.parametrize("sport", ("nba", "nfl"))
@pytest.mark.parametrize("mutation", ("manifest", "summary", "source_evidence"))
def test_adapter_reopen_rejects_mutated_native_lineage(
    tmp_path: Path, sport: str, mutation: str
) -> None:
    stage03 = _nba_stage03(tmp_path) if sport == "nba" else _nfl_stage03(tmp_path)
    contract = _stage03_contract(stage03, sport)
    handoff = tmp_path / f"{sport}_handoff"
    builder = build_nba_handoff if sport == "nba" else build_nfl_handoff
    provenance = builder(stage03, contract, handoff)

    if mutation == "manifest":
        value = json.loads((handoff / "adapter_provenance.json").read_text())
        value["native_lineage"]["manifest"]["sha256"] = "0" * 64
        write_json(handoff / "adapter_provenance.json", value)
    elif mutation == "summary":
        summary_path = Path(provenance["native_lineage"]["summary"]["path"])
        summary_path.write_bytes(summary_path.read_bytes() + b"\n")
    else:
        evidence_path = Path(
            provenance["native_lineage"]["source_evidence"][0]["fingerprint"]["path"]
        )
        evidence_path.write_bytes(evidence_path.read_bytes() + b"\n")

    with pytest.raises(
        (ArtifactError, ValueError),
        match="lineage|Fingerprint|fingerprint|cache inventory changed",
    ):
        load_and_verify_adapter_handoff(
            handoff / "adapter_provenance.json",
            sport,
            handoff / "eligible_moneylines.parquet",
            contract,
        )


def test_generic_handoff_rejects_unverified_provider_claims(tmp_path: Path) -> None:
    eligible = tmp_path / "eligible_moneylines.parquet"
    eligible.write_bytes(b"not consulted before lineage rejection")
    with pytest.raises(ArtifactError, match="requires verified native Stage-03 lineage"):
        build_adapter_handoff("self-asserted provider", eligible, CONTRACTS["nfl"], tmp_path / "out")


def test_nba_reopen_rejects_tampered_projected_team_name_with_updated_hash(
    tmp_path: Path,
) -> None:
    stage03 = _nba_stage03(tmp_path)
    contract = _stage03_contract(stage03, "nba")
    handoff = tmp_path / "nba_handoff"
    build_nba_handoff(stage03, contract, handoff)
    eligible = handoff / "eligible_moneylines.parquet"
    replacement = tmp_path / "tampered_eligible.parquet"
    con = duckdb.connect()
    try:
        con.execute(
            f"""COPY (
                    SELECT * REPLACE ('Tampered Team'::VARCHAR AS home_team_name)
                    FROM read_parquet('{eligible}')
                ) TO '{replacement}' (FORMAT PARQUET)"""
        )
    finally:
        con.close()
    replacement.replace(eligible)
    provenance_path = handoff / "adapter_provenance.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    provenance["eligible_moneylines"] = artifact_fingerprint(eligible)
    write_json(provenance_path, provenance)

    with pytest.raises(ArtifactError, match="exact native Stage-03 projection"):
        load_and_verify_adapter_handoff(
            provenance_path, "nba", eligible, contract
        )


@pytest.mark.parametrize(
    "module",
    (
        "analysis.sports_game_dynamics.adapter_handoff",
        "analysis.sports_game_dynamics.timestamp_provenance",
        "analysis.nba_game_dynamics.build_downstream_handoff",
        "analysis.nfl_game_dynamics.build_downstream_handoff",
    ),
)
def test_pipeline_entry_points_support_module_invocation(module: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", module, "--help"], cwd=ROOT,
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--run-dir" in result.stdout
