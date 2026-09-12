from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from analysis.sports_game_dynamics.adapter_handoff import _contract_record, build_adapter_handoff
from analysis.sports_game_dynamics.artifacts import (
    ArtifactError, artifact_fingerprint, fingerprint, write_json, write_parquet,
)
from analysis.sports_game_dynamics.build_dual_closes import CLOSE_SCHEMA, build_dual_closes
from analysis.sports_game_dynamics.build_exact_trades import build_exact_trades
from analysis.sports_game_dynamics.build_phase_dataset import build_phase_dataset
from analysis.sports_game_dynamics.estimate_calibration import (
    CLOSING_SCHEMA, PHASE_PROFILE_SCHEMA, estimate_calibration,
)
from analysis.sports_game_dynamics.estimate_flb_tails import TAIL_SCHEMA, estimate_flb_tails
from analysis.sports_game_dynamics.render_flb_report import render_flb_report
from analysis.sports_game_dynamics.schemas import ELIGIBLE_SCHEMA, EXACT_TRADE_SCHEMA, PHASE_TRADE_SCHEMA
from analysis.sports_game_dynamics.phase_contract import phase_contract_fingerprint
from analysis.sports_game_dynamics.timestamp_provenance import build_timestamp_declaration


ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = {sport: ROOT/"configs"/"game_dynamics"/f"{sport}_phase_contract_v1.json"
             for sport in ("nfl", "nba")}
RAW_SCHEMA = (
    ("maker", "VARCHAR"), ("taker", "VARCHAR"),
    ("maker_asset_id", "VARCHAR"), ("taker_asset_id", "VARCHAR"),
    ("maker_amount_filled", "BIGINT"), ("taker_amount_filled", "BIGINT"),
    ("block_number", "BIGINT"), ("transaction_hash", "VARCHAR"),
    ("log_index", "INTEGER"), ("exchange_address", "VARCHAR"),
    ("condition_id", "VARCHAR"), ("outcome_token_side", "VARCHAR"),
)
FLAG_SCHEMA = (("proxyWallet", "VARCHAR"), ("is_nonhuman", "BOOLEAN"))
CACHE_SCHEMA = (("block_number", "BIGINT"), ("timestamp", "BIGINT"))


def _schema(path: Path) -> tuple[tuple[str, str], ...]:
    con = duckdb.connect()
    try:
        return tuple((row[0], row[1]) for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')"
        ).fetchall())
    finally:
        con.close()


def _rows(path: Path, query: str = "*", suffix: str = "") -> list[tuple]:
    con = duckdb.connect()
    try:
        return con.execute(f"SELECT {query} FROM read_parquet('{path}') {suffix}").fetchall()
    finally:
        con.close()


def _make_sources(root: Path, games: int = 60, sport: str = "nfl", *, fractional_start: bool = False) -> dict[str, Path]:
    eligible_rows = []
    raw_rows = []
    cache_rows = []
    block = 1_000

    def add_fill(market: str, token: str, buyer: str, when: datetime, price: float, *, side: str = "taker") -> None:
        nonlocal block
        usdc = int(round(price*1_000_000))
        if side == "taker":
            raw_rows.append((buyer, "seller", "0", token, usdc, 1_000_000, block,
                             f"0x{block:064x}", block % 17, "0xexchange", market, side))
        else:
            raw_rows.append(("seller", buyer, token, "0", 1_000_000, usdc, block,
                             f"0x{block:064x}", block % 17, "0xexchange", market, side))
        cache_rows.append((block, int(when.timestamp())))
        block += 1

    for index in range(games):
        game_day = date(2025, 1, 1)+timedelta(days=index)
        start = datetime.combine(game_day, datetime.min.time(), tzinfo=timezone.utc)+timedelta(hours=20)
        if fractional_start:
            start += timedelta(microseconds=500_000)
        boundaries = [start, start+timedelta(hours=1), start+timedelta(hours=2),
                      start+timedelta(hours=3), start+timedelta(hours=4)]
        market, away, home = f"market-{index:03}", f"away-{index:03}", f"home-{index:03}"
        home_wins = index % 2 == 0
        eligible_rows.append((
            sport, market, f"game-{index:03}", game_day, "away-id", "Away Team",
            "home-id", "Home Team", away, home, "home-id" if home_wins else "away-id",
            home if home_wins else away, start-timedelta(minutes=10), *boundaries,
        ))
        phase_bases = (start-timedelta(minutes=30), start+timedelta(minutes=5),
                       boundaries[1]+timedelta(minutes=5), boundaries[2]+timedelta(minutes=5),
                       boundaries[3]+timedelta(minutes=5))
        for phase_index, base in enumerate(phase_bases):
            for decile in range(1, 11):
                price = (decile-.5)/10
                add_fill(market, home, f"wallet-{index%7}", base+timedelta(seconds=decile), price)
        # A is this later flagged-buyer fill; C remains the preceding D10 non-bot fill.
        add_fill(market, home, "bot-wallet", start-timedelta(minutes=1), .80)
        if fractional_start:
            add_fill(market, home, "wallet-fractional", start-timedelta(microseconds=500_000), .70)
        if index == 0:
            add_fill(market, away, "wallet-away", boundaries[-1], .20, side="maker")
            add_fill(market, home, "wallet-post", boundaries[-1]+timedelta(seconds=1), .50)
    # One exact duplicate replay must collapse without deleting a distinct economic fill.
    raw_rows.append(raw_rows[0])
    eligible = root/"eligible.parquet"
    raw = root/"resolved.parquet"
    cache = root/"cache.parquet"
    flags = root/"flags.parquet"
    write_parquet(eligible, ELIGIBLE_SCHEMA, eligible_rows, ("market_id",))
    write_parquet(raw, RAW_SCHEMA, raw_rows, ("block_number", "log_index"))
    write_parquet(cache, CACHE_SCHEMA, cache_rows, ("block_number",))
    write_parquet(flags, FLAG_SCHEMA, [("bot-wallet", True)], ("proxyWallet",))
    return {"eligible": eligible, "raw": raw, "cache": cache, "flags": flags}


def _install_verified_fixture_lineage(
    monkeypatch: pytest.MonkeyPatch,
    sport: str,
    eligible: Path,
    contract: Path,
    adapter: Path,
) -> None:
    """Install a test-only loader that verifies an explicit synthetic lineage."""

    providers = {
        "nfl": ("ESPN site API", "third_party_undocumented"),
        "nba": ("NBA official data API", "official"),
    }
    provider, status = providers[sport]
    adapter.mkdir()
    value = {
        "schema_version": "verified_synthetic_fixture_v1",
        "sport": sport,
        "source_provider": provider,
        "source_status": status,
        "eligible_moneylines": artifact_fingerprint(eligible),
        "phase_contract": _contract_record(contract),
        "fixture_evidence": artifact_fingerprint(eligible),
    }
    sidecar = adapter / "adapter_provenance.json"
    write_json(sidecar, value)

    def verify_fixture(
        path: str | Path,
        requested_sport: str,
        requested_eligible: str | Path,
        requested_contract: str | Path,
    ) -> dict:
        observed = json.loads(Path(path).read_text())
        if set(observed) != set(value) or observed.get("schema_version") != value["schema_version"]:
            raise ArtifactError("Synthetic adapter fixture schema mismatch")
        if observed.get("sport") != requested_sport or requested_sport != sport:
            raise ArtifactError("Synthetic adapter fixture sport mismatch")
        if observed.get("eligible_moneylines") != artifact_fingerprint(requested_eligible):
            raise ArtifactError("Synthetic adapter fixture eligible lineage mismatch")
        if observed.get("fixture_evidence") != artifact_fingerprint(requested_eligible):
            raise ArtifactError("Synthetic adapter fixture evidence mismatch")
        if observed.get("phase_contract") != _contract_record(Path(requested_contract).resolve()):
            raise ArtifactError("Adapter provenance phase-contract SHA/semantics mismatch")
        if (observed.get("source_provider"), observed.get("source_status")) != providers[sport]:
            raise ArtifactError("Synthetic adapter fixture provider mismatch")
        return observed

    monkeypatch.setattr(
        "analysis.sports_game_dynamics.timestamp_provenance.load_and_verify_adapter_handoff",
        verify_fixture,
    )


def _pipeline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sport: str = "nfl",
    *,
    fractional_start: bool = False,
) -> dict[str, Path]:
    paths = _make_sources(tmp_path, sport=sport, fractional_start=fractional_start)
    contract = CONTRACTS[sport]
    adapter = tmp_path/"03_adapter"
    _install_verified_fixture_lineage(monkeypatch, sport, paths["eligible"], contract, adapter)
    provenance = tmp_path/"04_timestamp"
    declaration = build_timestamp_declaration(
        sport, paths["raw"], paths["eligible"], paths["cache"],
        adapter/"adapter_provenance.json", contract, provenance,
    )
    assert declaration["missing_blocks"] == declaration["fallback_rows"] == 0
    exact = tmp_path/"05_exact"
    audit = build_exact_trades(sport, paths["raw"], paths["eligible"], paths["cache"],
                               provenance/"timestamp_provenance.json",
                               adapter/"adapter_provenance.json", contract, paths["flags"], exact)
    assert audit["counts"]["duplicate_replays"] == 1
    phase = tmp_path/"06_phase"
    build_phase_dataset(sport, paths["eligible"], exact/"exact_trades.parquet", contract, phase)
    closes = tmp_path/"07_closes"
    build_dual_closes(sport, paths["eligible"], exact/"exact_trades.parquet", closes)
    calibration = tmp_path/"08_calibration"
    estimate_calibration(sport, closes/"game_closes.parquet", phase/"phase_trades.parquet",
                         contract, calibration)
    tails = tmp_path/"09_tails"
    estimate_flb_tails(sport, calibration, closes/"game_closes.parquet",
                       phase/"phase_trades.parquet", contract, tails)
    return {**paths, "adapter": adapter, "contract": contract,
            "provenance": provenance, "exact": exact, "phase": phase,
            "closes": closes, "calibration": calibration, "tails": tails}


def test_exact_cache_coverage_fails_without_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _make_sources(tmp_path, games=1)
    rows = _rows(paths["cache"])[1:]
    incomplete = tmp_path/"incomplete.parquet"
    write_parquet(incomplete, CACHE_SCHEMA, rows, ("block_number",))
    adapter = tmp_path/"adapter"
    _install_verified_fixture_lineage(
        monkeypatch, "nfl", paths["eligible"], CONTRACTS["nfl"], adapter
    )
    with pytest.raises(ArtifactError, match="coverage is incomplete"):
        build_timestamp_declaration(
            "nfl", paths["raw"], paths["eligible"], incomplete,
            adapter/"adapter_provenance.json", CONTRACTS["nfl"], tmp_path/"rejected",
        )
    assert not (tmp_path/"rejected").exists()


@pytest.mark.parametrize("sport", ("nfl", "nba"))
def test_standard_schemas_boundaries_bot_semantics_and_complete_grids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sport: str
) -> None:
    paths = _pipeline(tmp_path, monkeypatch, sport)
    assert _schema(paths["exact"]/"exact_trades.parquet") == EXACT_TRADE_SCHEMA
    assert _schema(paths["phase"]/"phase_trades.parquet") == PHASE_TRADE_SCHEMA
    assert _schema(paths["closes"]/"game_closes.parquet") == CLOSE_SCHEMA
    assert _schema(paths["calibration"]/"closing_calibration.parquet") == CLOSING_SCHEMA
    assert _schema(paths["calibration"]/"trade_phase_calibration.parquet") == PHASE_PROFILE_SCHEMA
    assert _schema(paths["tails"]/"flb_tail_summary.parquet") == TAIL_SCHEMA

    closes = _rows(paths["closes"]/"game_closes.parquet",
                   "primary_buyer_is_flagged_nonhuman,primary_home_probability,sensitivity_home_probability")
    assert set(closes) == {(True, .8, .95)}
    boundary = _rows(
        paths["phase"]/"phase_trades.parquet",
        "phase,analysis_eligible,exclude_within_30s,home_probability",
        "WHERE market_id='market-000' AND timestamp IN "
        "(epoch(actual_end_utc)::BIGINT,epoch(actual_end_utc)::BIGINT+1) ORDER BY timestamp",
    )
    assert boundary == [("quarter_4_plus", True, True, .8), ("post_final", False, True, .5)]
    assert _rows(paths["calibration"]/"closing_calibration.parquet", "count(*)") == [(22,)]
    assert _rows(paths["calibration"]/"trade_phase_calibration.parquet", "count(*)") == [(100,)]
    tails = _rows(paths["tails"]/"flb_tail_summary.parquet",
                  "analysis_scope,suppressed,count(*) OVER(PARTITION BY analysis_scope)")
    assert len(tails) == 12
    assert sum(scope == "trade_phase" and not suppressed for scope, suppressed, _ in tails) == 10
    assert sum(scope == "closing" and suppressed for scope, suppressed, _ in tails) == 2


@pytest.mark.parametrize("sport", ("nfl", "nba"))
def test_report_is_offline_deterministic_complete_and_immutable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sport: str
) -> None:
    paths = _pipeline(tmp_path, monkeypatch, sport)
    reports = []
    manifests = []
    for name in ("10_report_a", "10_report_b"):
        target = tmp_path/name
        manifest = render_flb_report(
            sport, paths["calibration"], paths["tails"],
            paths["provenance"]/"timestamp_provenance.json", paths["contract"], target,
        )
        assert manifest["counts"] == {"closing_profile_rows": 22, "phase_profile_rows": 100,
                                      "tail_rows": 12, "phase_panels": 5}
        reports.append((target/"sports_flb_report.html").read_bytes())
        manifests.append((target/"report_manifest.json").read_bytes())
    assert reports[0] == reports[1]
    assert manifests[0] == manifests[1]
    text = reports[0].decode()
    assert text.count('<section class="panel">') == 5
    assert "All 12 frozen D1/D10 tail rows" in text
    assert "http://" not in text and "https://" not in text and "<script" not in text
    expected_provider = "ESPN site API" if sport == "nfl" else "NBA official data API"
    expected_status = "third_party_undocumented" if sport == "nfl" else "official"
    assert f"Timing provider: <code>{expected_provider}</code>" in text
    assert f"source status: <code>{expected_status}</code>" in text
    assert ("ESPN" in text) is (sport == "nfl")
    before = reports[0]
    with pytest.raises(FileExistsError):
        render_flb_report(sport, paths["calibration"], paths["tails"],
                          paths["provenance"]/"timestamp_provenance.json", paths["contract"],
                          tmp_path/"10_report_a")
    assert (tmp_path/"10_report_a"/"sports_flb_report.html").read_bytes() == before


def test_adapter_handoff_rejects_legacy_source_claim_api(tmp_path: Path) -> None:
    paths = _make_sources(tmp_path, games=1)
    broken = tmp_path/"broken.parquet"
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT * EXCLUDE(period_4_start_utc) FROM read_parquet('{paths['eligible']}')) "
            f"TO '{broken}' (FORMAT PARQUET)"
        )
    finally:
        con.close()
    declaration = tmp_path / "declaration"
    with pytest.raises(TypeError):
        build_adapter_handoff("nfl", "ESPN site API", "third_party_undocumented",
                              broken, CONTRACTS["nfl"], declaration)
    assert not declaration.exists()


def test_adapter_handoff_rejects_unsealed_lineage(tmp_path: Path) -> None:
    paths = _make_sources(tmp_path, games=1)
    target = tmp_path / "adapter_pair"
    with pytest.raises(ArtifactError, match="requires verified native Stage-03 lineage"):
        build_adapter_handoff(
            "nfl", paths["eligible"], CONTRACTS["nfl"], target,
        )
    assert not target.exists()


def test_stage04_requires_matching_adapter_sidecar_and_contract_sha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _make_sources(tmp_path, games=1)
    with pytest.raises(FileNotFoundError):
        build_timestamp_declaration(
            "nfl", paths["raw"], paths["eligible"], paths["cache"],
            tmp_path/"missing.json", CONTRACTS["nfl"], tmp_path/"missing_run",
        )
    adapter = tmp_path/"adapter"
    _install_verified_fixture_lineage(
        monkeypatch, "nfl", paths["eligible"], CONTRACTS["nfl"], adapter
    )
    value = json.loads((adapter/"adapter_provenance.json").read_text())
    value["phase_contract"]["sha256"] = "0"*64
    write_json(adapter/"adapter_provenance.json", value)
    with pytest.raises(ArtifactError, match="phase-contract SHA/semantics mismatch"):
        build_timestamp_declaration(
            "nfl", paths["raw"], paths["eligible"], paths["cache"],
            adapter/"adapter_provenance.json", CONTRACTS["nfl"], tmp_path/"wrong_sha",
        )


def test_fractional_actual_start_keeps_prior_whole_second_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _pipeline(tmp_path, monkeypatch, "nfl", fractional_start=True)
    rows = _rows(
        paths["closes"]/"game_closes.parquet",
        "primary_home_probability,sensitivity_home_probability,primary_close_timestamp",
        "WHERE market_id='market-000'",
    )
    expected_timestamp = int((datetime(2025, 1, 1, 20, 0, 0, 500_000,
                                       tzinfo=timezone.utc)-timedelta(microseconds=500_000)).timestamp())
    assert rows == [(.7, .7, expected_timestamp)]


def test_stage09_recomputes_profile_even_if_mutated_fingerprint_is_updated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _pipeline(tmp_path, monkeypatch)
    profile = paths["calibration"]/"trade_phase_calibration.parquet"
    replacement = tmp_path/"mutated_profile.parquet"
    con = duckdb.connect()
    try:
        con.execute(
            f"""COPY (SELECT * REPLACE (
                   CASE WHEN boundary_sample='literal' AND phase='quarter_1' AND price_decile=1
                        THEN mean_calibration+0.123 ELSE mean_calibration END AS mean_calibration)
                 FROM read_parquet('{profile}')) TO '{replacement}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
        )
    finally:
        con.close()
    replacement.replace(profile)
    estimator_summary_path = paths["calibration"]/"estimator_summary.json"
    summary = json.loads(estimator_summary_path.read_text())
    summary["outputs"]["trade_phase_calibration"] = artifact_fingerprint(profile)
    write_json(estimator_summary_path, summary)
    with pytest.raises(ArtifactError, match="does not recompute"):
        estimate_flb_tails("nfl", paths["calibration"], paths["closes"]/"game_closes.parquet",
                           paths["phase"]/"phase_trades.parquet", paths["contract"],
                           tmp_path/"rejected_tails")
    assert not (tmp_path/"rejected_tails").exists()


def test_stage10_rejects_same_sport_declaration_disconnected_from_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _pipeline(tmp_path, monkeypatch)
    original = json.loads((paths["provenance"]/"timestamp_provenance.json").read_text())
    fake_source = tmp_path/"fake_source.txt"
    fake_source.write_text("internally fingerprinted but unrelated\n")
    for key in ("raw_trades", "eligible_moneylines", "cache"):
        original[key] = fingerprint(fake_source)
    fake = tmp_path/"fake_timestamp_provenance.json"
    write_json(fake, original)
    with pytest.raises(ArtifactError, match="Report summaries do not match"):
        render_flb_report("nfl", paths["calibration"], paths["tails"], fake,
                          paths["contract"], tmp_path/"rejected_report")
    assert not (tmp_path/"rejected_report").exists()
