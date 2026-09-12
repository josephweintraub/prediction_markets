from __future__ import annotations

import json
import hashlib
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest


FIXTURE_DIR = Path(__file__).parent / "fixtures"

from analysis.nba_game_dynamics.build_game_timing import (
    DEFAULT_PHASE_CONTRACT,
    build_game_timing_audit,
    cache_inventory_fingerprint,
    verify_game_timing_run,
)
from analysis.nba_game_dynamics.artifact_manifest import file_fingerprint
from analysis.nba_game_dynamics.build_validated_universe import build_validated_universe
from analysis.nba_game_dynamics.nba_api import (
    ACTUAL_END_EVENT,
    ACTUAL_START_EVENT,
    PERIOD_BOUNDARY_EVENT,
    parse_legacy_schedule,
    parse_live_data_play_by_play,
)
from analysis.sports_game_dynamics.phase_contract import (
    classify_timestamp,
    load_phase_contract,
    phase_contract_fingerprint,
)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _write(path: Path, rows: list[dict]) -> None:
    frame = pd.DataFrame(rows)
    con = duckdb.connect()
    con.register("rows", frame)
    con.execute(f"COPY rows TO '{path}' (FORMAT PARQUET)")
    con.close()


class FakeClient:
    def __init__(
        self, cache_dir: Path, fail_timing: bool = False, payload: dict | None = None
    ) -> None:
        self.fail_timing = fail_timing
        self.payload = payload or _fixture("nba_live_data_pbp.json")
        self.cache_dir = cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)
        self.schedule_cache = cache_dir / "schedule_2024.json"
        self.pbp_cache = cache_dir / "playbyplay" / "0022400953.json"
        self.pbp_cache.parent.mkdir()
        self.schedule_cache.write_bytes(
            json.dumps(_fixture("nba_legacy_schedule.json"), sort_keys=True).encode()
        )
        self.pbp_cache.write_bytes(json.dumps(self.payload, sort_keys=True).encode())

    def schedule_games(self, start: date, end: date, *, refresh: bool = False):
        assert start == end == date(2025, 3, 12)
        return (parse_legacy_schedule(_fixture("nba_legacy_schedule.json"), 2024)[0],)

    def game_timing(self, game_id: str, *, refresh: bool = False):
        if self.fail_timing:
            raise ValueError("action.timeActual missing")
        return parse_live_data_play_by_play(
            self.payload, expected_game_id=game_id
        )

    def provenance_manifest(self) -> dict:
        return {
            "schema_version": 1,
            "schedule_provider": "official_nba_data_nba_com_historical_schedule",
            "timing_provider": "official_nba_livedata_s3_origin",
            "actual_start_event": ACTUAL_START_EVENT,
            "actual_end_event": ACTUAL_END_EVENT,
            "period_boundary_event": PERIOD_BOUNDARY_EVENT,
            "resources": [
                {
                    "url": "https://data.nba.com/2024/schedule.json",
                    "cache_path": str(self.schedule_cache.resolve()),
                    "bytes": len(self.schedule_cache.read_bytes()),
                    "sha256": hashlib.sha256(self.schedule_cache.read_bytes()).hexdigest(),
                    "source": "cache",
                },
                {
                    "url": "https://official.example/playbyplay_0022400953.json",
                    "cache_path": str(self.pbp_cache.resolve()),
                    "bytes": len(self.pbp_cache.read_bytes()),
                    "sha256": hashlib.sha256(self.pbp_cache.read_bytes()).hexdigest(),
                    "source": "cache",
                },
            ],
        }


def _candidate(path: Path) -> None:
    _write(path, [{
        "market_id": "market-1", "event_slug": "nba-nyk-por-2025-03-12",
        "date": date(2025, 3, 12), "away": "nyk", "home": "por",
        "question": "Knicks vs. Trail Blazers", "n_tokens": 2,
        "n_trades_raw": 100.0, "n_buy_filtered": 40.0,
        "usd_buy_filtered": 500.0,
        "first_trade_at": pd.Timestamp("2025-03-01T00:00:00Z"),
        "last_trade_at": pd.Timestamp("2025-03-13T00:00:00Z"),
    }])


def _bound_contract(
    tmp_path: Path,
    candidates: Path,
    cache_dir: Path,
    *,
    opening_passes: int = 1,
    reconciled_timing_games: int = 1,
) -> Path:
    payload = json.loads(Path(DEFAULT_PHASE_CONTRACT).read_text(encoding="utf-8"))
    inventory = cache_inventory_fingerprint(cache_dir)
    scope = payload["audit_scope"]
    scope.update({
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
        "opening_rule_passes": opening_passes,
        "opening_rule_failures": 1 - opening_passes,
        "opening_failure_game_id": "0022400953",
        "schedule_score_mismatches": 0,
        "schedule_score_mismatch_game_id": "0022400072",
        "reconciled_timing_games": reconciled_timing_games,
        "overtime_games": opening_passes,
        "single_overtime_games": opening_passes,
        "double_overtime_games": 0,
        "spurious_period_5_games": 0,
    })
    contract = tmp_path / "nba-test-phase-contract-v2.json"
    contract.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return contract


def _tokens(tmp_path: Path) -> tuple[Path, Path]:
    universe_tokens = tmp_path / "tokens.parquet"
    token_map = tmp_path / "token_map.parquet"
    _write(universe_tokens, [
        {"market_id": "market-1", "token_id": "away-token", "winning_outcome": "Knicks"},
        {"market_id": "market-1", "token_id": "home-token", "winning_outcome": "Knicks"},
    ])
    _write(token_map, [
        {"token_id": "away-token", "condition_id": "market-1", "outcome": "Knicks",
         "event_slug": "nba-nyk-por-2025-03-12", "question": "Knicks vs. Trail Blazers"},
        {"token_id": "home-token", "condition_id": "market-1", "outcome": "Trail Blazers",
         "event_slug": "nba-nyk-por-2025-03-12", "question": "Knicks vs. Trail Blazers"},
    ])
    return universe_tokens, token_map


def _replace_parquet(path: Path, assignment: str) -> None:
    temporary = path.with_suffix(".new.parquet")
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT * REPLACE ({assignment}) FROM read_parquet('{path}')) "
        f"TO '{temporary}' (FORMAT PARQUET)"
    )
    con.close()
    temporary.replace(path)


def test_timing_then_validated_universe_full_offline_path(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    timing_run = tmp_path / "timing"
    cache = tmp_path / "cache"
    client = FakeClient(cache)
    contract = _bound_contract(tmp_path, candidates, cache)
    timing_summary = build_game_timing_audit(
        candidates, cache, timing_run, client=client,
        phase_contract_path=contract,
    )
    assert timing_summary["exact_final_matches"] == 1
    assert timing_summary["timing_games_written"] == 1
    assert timing_summary["espn_fallback_used"] is False
    assert timing_summary["analysis_phases"] == [
        "pregame", "quarter_1", "quarter_2", "quarter_3", "quarter_4_plus"
    ]
    assert timing_summary["phase_contract_sha256"] == phase_contract_fingerprint(
        contract
    )["sha256"]
    assert load_phase_contract(DEFAULT_PHASE_CONTRACT).contract_version == 2
    assert verify_game_timing_run(timing_run) == timing_summary

    universe_tokens, token_map = _tokens(tmp_path)
    output = tmp_path / "validated"
    summary = build_validated_universe(
        candidates, timing_run, universe_tokens, token_map, output
    )
    assert summary["counts"] == {
        "candidate_markets": 1, "moneyline_valid": 1, "timing_valid": 1,
        "standard_timing_eligible": 1, "eligible_moneylines": 1,
    }
    assert summary["exclusion_counts"] == {"moneyline": {}, "standard_timing": {}}
    con = duckdb.connect()
    eligible = con.execute(
        f"SELECT * FROM read_parquet('{output / 'eligible_moneylines.parquet'}')"
    ).fetchdf().iloc[0]
    con.close()
    assert eligible["away_token_id"] == "away-token"
    assert eligible["home_token_id"] == "home-token"
    assert bool(eligible["home_won"]) is False
    assert eligible["game_type_code"] == "002"
    assert eligible["final_period"] == 5
    assert pd.isna(eligible["expected_final_period"])
    assert eligible["timestamp_source"] == "official_nba_livedata_timeActual"
    assert eligible["actual_start_event"] == ACTUAL_START_EVENT
    assert eligible["actual_end_event"] == ACTUAL_END_EVENT
    assert eligible["phase_contract_sha256"] == timing_summary["phase_contract_sha256"]
    assert eligible["winner_team_id"] == 1610612752
    assert bool(eligible["away_is_winner"]) is True
    con = duckdb.connect()
    assert con.execute(
        f"SELECT pbp_away_final_score, pbp_home_final_score "
        f"FROM read_parquet('{timing_run / 'game_timing.parquet'}')"
    ).fetchone() == (114, 113)
    con.close()
    boundaries = {
        name: eligible[name].to_pydatetime()
        for name in load_phase_contract(DEFAULT_PHASE_CONTRACT).required_boundaries
    }
    assert classify_timestamp(
        load_phase_contract(DEFAULT_PHASE_CONTRACT), boundaries,
        eligible["period_2_start_utc"].to_pydatetime(),
    ) == "quarter_2"


def test_timing_failure_is_retained_and_no_timing_row_is_written(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    output = tmp_path / "timing"
    cache = tmp_path / "cache"
    client = FakeClient(cache, fail_timing=True)
    summary = build_game_timing_audit(
        candidates, cache, output, client=client,
        phase_contract_path=_bound_contract(
            tmp_path, candidates, cache, opening_passes=0,
            reconciled_timing_games=0,
        ),
    )
    assert summary["timing_games_written"] == 0
    assert summary["timing_exclusions"] == {"timing_fetch_or_parse_failure": 1}
    con = duckdb.connect()
    row = con.execute(
        f"SELECT timing_status, timing_error_message FROM read_parquet('{output / 'match_audit.parquet'}')"
    ).fetchone()
    assert row == ("failed", "action.timeActual missing")
    assert con.execute(
        f"SELECT count(*) FROM read_parquet('{output / 'game_timing.parquet'}')"
    ).fetchone()[0] == 0
    con.close()

    tokens, token_map = _tokens(tmp_path)
    validated = tmp_path / "validated"
    validated_summary = build_validated_universe(
        candidates, output, tokens, token_map, validated
    )
    assert validated_summary["counts"]["moneyline_valid"] == 1
    assert validated_summary["counts"]["timing_valid"] == 0
    con = duckdb.connect()
    audit = con.execute(
        f"SELECT moneyline_valid, timing_valid, standard_timing_exclusion_reason "
        f"FROM read_parquet('{validated / 'candidate_validation_audit.parquet'}')"
    ).fetchone()
    assert audit == (True, False, "upstream_timing_timing_fetch_or_parse_failure")
    assert con.execute(
        f"SELECT count(*) FROM read_parquet('{validated / 'eligible_moneylines.parquet'}')"
    ).fetchone()[0] == 0
    eligible_columns = {
        row[0] for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{validated / 'eligible_moneylines.parquet'}')"
        ).fetchall()
    }
    con.close()
    assert {"game_id", "actual_start_utc", "winner_team_id"} <= eligible_columns


def test_validated_builder_hard_fails_incomplete_token_coverage(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    timing_run = tmp_path / "timing"
    cache = tmp_path / "cache"
    client = FakeClient(cache)
    build_game_timing_audit(
        candidates, cache, timing_run, client=client,
        phase_contract_path=_bound_contract(tmp_path, candidates, cache),
    )
    tokens = tmp_path / "tokens.parquet"
    token_map = tmp_path / "token_map.parquet"
    _write(tokens, [
        {"market_id": "market-1", "token_id": "only", "winning_outcome": "Knicks"}
    ])
    _write(token_map, [
        {"token_id": "only", "condition_id": "market-1", "outcome": "Knicks",
         "event_slug": "nba-nyk-por-2025-03-12", "question": "Knicks vs. Trail Blazers"}
    ])
    with pytest.raises(ValueError, match="exactly two canonical token rows"):
        build_validated_universe(
            candidates, timing_run, tokens, token_map, tmp_path / "validated"
        )
    assert not (tmp_path / "validated").exists()


@pytest.mark.parametrize(
    ("artifact", "assignment", "message"),
    [
        ("schedule_audit.parquet", "999::BIGINT AS winner_team_id", "Fingerprint mismatch|canonical winner"),
        ("match_audit.parquet", "'2025-03-13'::DATE AS schedule_official_date", "Fingerprint mismatch|schedule_official_date"),
        ("game_timing.parquet", "'2025-03-13 03:30:00+00'::TIMESTAMPTZ AS period_2_start_utc", "Fingerprint mismatch|phase boundaries"),
        ("game_timing.parquet", "3::INTEGER AS final_period", "Fingerprint mismatch|final-period"),
        ("game_timing.parquet", "113::INTEGER AS pbp_away_final_score", "Fingerprint mismatch|PBP away final score"),
        ("game_timing.parquet", "'0' AS provider_provenance_sha256", "Fingerprint mismatch|provenance"),
        ("game_timing.parquet", "'period start' AS actual_start_event", "Fingerprint mismatch|actual start event"),
    ],
)
def test_validated_builder_rejects_cross_artifact_mutations(
    tmp_path: Path, artifact: str, assignment: str, message: str
) -> None:
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    timing = tmp_path / "timing"
    cache = tmp_path / "cache"
    client = FakeClient(cache)
    build_game_timing_audit(
        candidates, cache, timing, client=client,
        phase_contract_path=_bound_contract(tmp_path, candidates, cache),
    )
    _replace_parquet(timing / artifact, assignment)
    tokens, token_map = _tokens(tmp_path)

    with pytest.raises(ValueError, match=message):
        build_validated_universe(
            candidates, timing, tokens, token_map, tmp_path / "validated"
        )
    assert not (tmp_path / "validated").exists()
    assert not list(tmp_path.glob(".validated.staging-*"))


def test_provenance_manifest_hash_and_summary_are_mandatory(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    timing = tmp_path / "timing"
    cache = tmp_path / "cache"
    client = FakeClient(cache)
    build_game_timing_audit(
        candidates, cache, timing, client=client,
        phase_contract_path=_bound_contract(tmp_path, candidates, cache),
    )
    manifest = timing / "provider_provenance.json"
    payload = json.loads(manifest.read_text())
    payload["resources"][0]["sha256"] = "b" * 64
    manifest.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    tokens, token_map = _tokens(tmp_path)

    with pytest.raises(ValueError, match="Fingerprint mismatch|provider_provenance_sha256"):
        build_validated_universe(
            candidates, timing, tokens, token_map, tmp_path / "validated"
        )


def test_schedule_final_ot_reconciliation_failure_is_retained(tmp_path: Path) -> None:
    payload = _fixture("nba_live_data_pbp.json")
    payload["game"]["actions"] = [
        row for row in payload["game"]["actions"] if row["period"] <= 4
    ]
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    timing = tmp_path / "timing"
    cache = tmp_path / "cache"
    client = FakeClient(cache, payload=payload)
    summary = build_game_timing_audit(
        candidates, cache, timing, client=client,
        phase_contract_path=_bound_contract(
            tmp_path, candidates, cache, opening_passes=0,
            reconciled_timing_games=0,
        ),
    )

    assert summary["timing_games_written"] == 0
    con = duckdb.connect()
    error = con.execute(
        f"SELECT timing_error_message FROM read_parquet('{timing / 'match_audit.parquet'}')"
    ).fetchone()[0]
    con.close()
    assert "requires exactly one game/end action" in error


def test_validated_publication_rechecks_provider_cache_bytes(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    cache = tmp_path / "cache"
    client = FakeClient(cache)
    timing = tmp_path / "timing"
    build_game_timing_audit(
        candidates, cache, timing, client=client,
        phase_contract_path=_bound_contract(tmp_path, candidates, cache),
    )
    client.pbp_cache.write_bytes(b"changed after timing publication")
    tokens, token_map = _tokens(tmp_path)

    with pytest.raises(
        ValueError,
        match="Fingerprint mismatch|fingerprint mismatch|cache inventory changed",
    ):
        build_validated_universe(
            candidates, timing, tokens, token_map, tmp_path / "validated"
        )
    assert not (tmp_path / "validated").exists()


def test_exact_candidate_schema_rejects_extra_columns(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    extra = tmp_path / "extra.parquet"
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT *, 1 AS extra FROM read_parquet('{candidates}')) "
        f"TO '{extra}' (FORMAT PARQUET)"
    )
    con.close()
    with pytest.raises(ValueError, match="schema mismatch"):
        build_game_timing_audit(
            extra, tmp_path / "cache", tmp_path / "timing",
            client=FakeClient(tmp_path / "provider")
        )


def test_v2_scope_rejects_different_candidate_artifact(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    cache = tmp_path / "cache"
    client = FakeClient(cache)
    contract = _bound_contract(tmp_path, candidates, cache)
    changed = tmp_path / "changed-candidates.parquet"
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT * REPLACE (101.0::DOUBLE AS n_trades_raw) "
        f"FROM read_parquet('{candidates}')) TO '{changed}' (FORMAT PARQUET)"
    )
    con.close()

    with pytest.raises(ValueError, match="audit_scope input mismatch"):
        build_game_timing_audit(
            changed, cache, tmp_path / "timing", client=client,
            phase_contract_path=contract,
        )
    assert not (tmp_path / "timing").exists()


def test_v2_scope_rejects_different_cache_inventory(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    cache = tmp_path / "cache"
    client = FakeClient(cache)
    contract = _bound_contract(tmp_path, candidates, cache)
    client.pbp_cache.write_bytes(client.pbp_cache.read_bytes() + b"\n")

    with pytest.raises(ValueError, match="audit_scope input mismatch"):
        build_game_timing_audit(
            candidates, cache, tmp_path / "timing", client=client,
            phase_contract_path=contract,
        )
    assert not (tmp_path / "timing").exists()


def test_nba_contract_rejects_ten_minute_regulation_periods(tmp_path: Path) -> None:
    payload = json.loads(Path(DEFAULT_PHASE_CONTRACT).read_text(encoding="utf-8"))
    payload["regulation_period_minutes"] = 10
    contract = tmp_path / "ten-minute-contract.json"
    contract.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    with pytest.raises(ValueError, match="12-minute regulation periods"):
        build_game_timing_audit(
            tmp_path / "unused-candidates.parquet",
            tmp_path / "cache",
            tmp_path / "timing",
            client=FakeClient(tmp_path / "provider"),
            phase_contract_path=contract,
        )


def test_nba_timing_rejects_superseded_v1_contract(tmp_path: Path) -> None:
    v1 = Path(DEFAULT_PHASE_CONTRACT).with_name("nba_phase_contract_v1.json")
    with pytest.raises(ValueError, match="NBA phase contract"):
        build_game_timing_audit(
            tmp_path / "unused-candidates.parquet",
            tmp_path / "cache",
            tmp_path / "timing",
            client=FakeClient(tmp_path / "provider"),
            phase_contract_path=v1,
        )


def test_reopen_and_validated_gates_reject_post_final_as_analysis_phase(
    tmp_path: Path,
) -> None:
    candidates = tmp_path / "candidates.parquet"
    _candidate(candidates)
    timing = tmp_path / "timing"
    cache = tmp_path / "cache"
    client = FakeClient(cache)
    build_game_timing_audit(
        candidates, cache, timing, client=client,
        phase_contract_path=_bound_contract(tmp_path, candidates, cache),
    )
    summary_path = timing / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["analysis_phases"].append("post_final")
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    manifest_path = timing / "timing_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload = summary_path.read_bytes()
    manifest["outputs"]["summary.json"].update(
        bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest()
    )
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    with pytest.raises(ValueError, match="eligible phases mismatch"):
        verify_game_timing_run(timing)
    tokens, token_map = _tokens(tmp_path)
    with pytest.raises(ValueError, match="eligible phases mismatch"):
        build_validated_universe(
            candidates, timing, tokens, token_map, tmp_path / "validated"
        )
