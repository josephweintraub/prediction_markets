from __future__ import annotations

import json
import sys
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import duckdb
import pandas as pd
import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
sys.path.insert(0, str(MODULE_DIR))

import build_validated_universe as validated_module  # noqa: E402
from build_game_timing import (  # noqa: E402
    MATCH_SCHEMA,
    SCHEDULE_SCHEMA,
    TIMING_SCHEMA,
    _match_row,
    _timing_row,
)
from build_validated_universe import (  # noqa: E402
    AUDIT_OUTPUT,
    CANDIDATE_SCHEMA,
    ELIGIBLE_OUTPUT,
    SUMMARY_OUTPUT,
    ValidatedUniverseBuildError,
    build_validated_universe,
)
from match_games import GameMatchAudit, MLB_TEAMS, MlbTeam  # noqa: E402
from mlb_api import GameTiming, InningHalfBoundary, PhaseWindow, ScheduleGame  # noqa: E402
from validate_moneylines import (  # noqa: E402
    MLB_OUTCOME_LABELS_BY_SLUG,
    MoneylineTokenCompositionError,
)


TOKEN_UNIVERSE_SCHEMA = (
    ("token_id", "VARCHAR"),
    ("market_id", "VARCHAR"),
    ("winning_outcome", "VARCHAR"),
)
TOKEN_MAP_SCHEMA = (
    ("token_id", "VARCHAR"),
    ("condition_id", "VARCHAR"),
    ("outcome", "VARCHAR"),
)


def _write_typed(
    path: Path,
    schema: tuple[tuple[str, str], ...],
    rows: list[dict[str, Any]],
) -> None:
    con = duckdb.connect()
    columns = [name for name, _ in schema]
    try:
        declarations = ", ".join(f'"{name}" {kind}' for name, kind in schema)
        con.execute(f"CREATE TABLE fixture ({declarations})")
        if rows:
            placeholders = ", ".join("?" for _ in columns)
            con.executemany(
                f"INSERT INTO fixture VALUES ({placeholders})",
                [[row.get(column) for column in columns] for row in rows],
            )
        con.execute(f"COPY fixture TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        con.close()


def _candidate(
    market_id: str,
    game_date: date,
    away: str = "nyy",
    home: str = "bos",
) -> dict[str, Any]:
    return {
        "market_id": market_id,
        "event_slug": f"mlb-{away}-{home}-{game_date.isoformat()}",
        "date": game_date,
        "away": away,
        "home": home,
        "question": f"{MLB_TEAMS[away].name} vs. {MLB_TEAMS[home].name}",
    }


def _game(
    game_pk: int,
    game_date: date,
    away: str = "nyy",
    home: str = "bos",
    **changes: Any,
) -> ScheduleGame:
    base = ScheduleGame(
        game_pk=game_pk,
        official_date=game_date,
        scheduled_start_utc=datetime.combine(
            game_date, datetime.min.time(), tzinfo=timezone.utc
        )
        + timedelta(hours=17),
        game_type="R",
        season=game_date.year,
        away_team_id=MLB_TEAMS[away].team_id,
        away_team_name=MLB_TEAMS[away].name,
        home_team_id=MLB_TEAMS[home].team_id,
        home_team_name=MLB_TEAMS[home].name,
        status_abstract="Final",
        status_detailed="Final",
        status_code="F",
        is_completed=True,
        doubleheader="N",
        game_number=1,
        series_game_number=1,
        reschedule_date_utc=None,
        rescheduled_from_date=None,
        resume_date_utc=None,
        resumed_from_date=None,
        away_final_score=3,
        home_final_score=5,
        away_is_winner=False,
        home_is_winner=True,
    )
    return replace(base, **changes)


def _timing(game_pk: int, game_date: date, final_inning: int = 9) -> GameTiming:
    start = datetime.combine(game_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
        hours=17, minutes=5
    )
    inning_4 = start + timedelta(hours=1)
    inning_7 = start + timedelta(hours=2)
    end = start + timedelta(hours=3)
    last_half = "bottom" if final_inning >= 9 else "top"
    halves = (
        InningHalfBoundary(1, "top", "innings_1_3", start, start + timedelta(minutes=5), 0, 0),
        InningHalfBoundary(
            4, "top", "innings_4_6", inning_4, inning_4 + timedelta(minutes=5), 1, 1
        ),
        InningHalfBoundary(
            7, "top", "innings_7_plus", inning_7, inning_7 + timedelta(minutes=5), 2, 2
        ),
        InningHalfBoundary(
            final_inning,
            last_half,
            "innings_7_plus",
            end - timedelta(minutes=5),
            end,
            3,
            3,
        ),
    )
    return GameTiming(
        game_pk=game_pk,
        actual_start_utc=start,
        actual_end_utc=end,
        play_count=4,
        inning_halves=halves,
        phase_windows=(
            PhaseWindow("innings_1_3", start, inning_4),
            PhaseWindow("innings_4_6", inning_4, inning_7),
            PhaseWindow("innings_7_plus", inning_7, end),
        ),
    )


def _exact_match(candidate: dict[str, Any], game: ScheduleGame) -> GameMatchAudit:
    first_team_id = MLB_TEAMS[candidate["away"]].team_id
    second_team_id = MLB_TEAMS[candidate["home"]].team_id
    orientation = (
        "official"
        if (first_team_id, second_team_id)
        == (game.away_team_id, game.home_team_id)
        else "reversed"
    )
    return GameMatchAudit(
        market_id=candidate["market_id"],
        market_date=candidate["date"],
        away_slug=candidate["away"],
        home_slug=candidate["home"],
        slug_orientation=orientation,
        away_team=MlbTeam(game.away_team_id, game.away_team_name),
        home_team=MlbTeam(game.home_team_id, game.home_team_name),
        matched_game_pk=game.game_pk,
        exclusion_reason=None,
        schedule_matches=(game,),
    )


def _unmatched(candidate: dict[str, Any], reason: str) -> GameMatchAudit:
    return GameMatchAudit(
        market_id=candidate["market_id"],
        market_date=candidate["date"],
        away_slug=candidate["away"],
        home_slug=candidate["home"],
        slug_orientation=None,
        away_team=None,
        home_team=None,
        matched_game_pk=None,
        exclusion_reason=reason,
        schedule_matches=(),
    )


def _token_rows(candidates: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    universe: list[dict[str, str]] = []
    token_map: list[dict[str, str]] = []
    for candidate in candidates:
        away_labels = MLB_OUTCOME_LABELS_BY_SLUG[candidate["away"]]
        home_labels = MLB_OUTCOME_LABELS_BY_SLUG[candidate["home"]]
        label_index = 0 if candidate["date"].year == 2025 else 1
        away_label = away_labels[label_index]
        home_label = home_labels[label_index]
        for side, label in (("away", away_label), ("home", home_label)):
            token_id = f"{candidate['market_id']}-{side}"
            universe.append(
                {
                    "token_id": token_id,
                    "market_id": candidate["market_id"],
                    "winning_outcome": home_label,
                }
            )
            token_map.append(
                {
                    "token_id": token_id,
                    "condition_id": candidate["market_id"],
                    "outcome": label,
                }
            )
    return universe, token_map


def _write_fixture(
    root: Path,
    candidates: list[dict[str, Any]],
    games: list[ScheduleGame],
    audits: list[GameMatchAudit],
    timings: dict[str, GameTiming | None | Exception],
    *,
    mutate_timing_row: Callable[[dict[str, Any]], None] | None = None,
    omit_token_map_market: str | None = None,
    match_rows_override: list[dict[str, Any]] | None = None,
) -> dict[str, Path]:
    root.mkdir()
    timing_run = root / "timing-run"
    timing_run.mkdir()
    candidates_path = root / "candidates.parquet"
    universe_path = root / "universe_tokens.parquet"
    token_map_path = root / "token_map.parquet"
    output = root / "validated-run"

    _write_typed(candidates_path, CANDIDATE_SCHEMA, candidates)
    _write_typed(
        timing_run / "schedule_audit.parquet",
        SCHEDULE_SCHEMA,
        [asdict(game) for game in games],
    )
    match_rows: list[dict[str, Any]] = []
    timing_rows: list[dict[str, Any]] = []
    for audit in audits:
        match_row = _match_row(audit)
        timing_value = timings.get(audit.market_id)
        if audit.is_matched and isinstance(timing_value, Exception):
            match_row["timing_status"] = "failed"
            match_row["timing_exclusion_reason"] = "timing_fetch_or_parse_failure"
            match_row["timing_error_type"] = type(timing_value).__name__
            match_row["timing_error_message"] = str(timing_value)
        elif audit.is_matched:
            match_row["timing_status"] = "passed"
            if timing_value is not None:
                timing_row = _timing_row(audit, timing_value)
                if mutate_timing_row is not None:
                    mutate_timing_row(timing_row)
                timing_rows.append(timing_row)
        match_rows.append(match_row)
    _write_typed(
        timing_run / "match_audit.parquet",
        MATCH_SCHEMA,
        match_rows_override if match_rows_override is not None else match_rows,
    )
    _write_typed(timing_run / "game_timing.parquet", TIMING_SCHEMA, timing_rows)

    universe_rows, map_rows = _token_rows(candidates)
    if omit_token_map_market is not None:
        map_rows = [
            row for row in map_rows if row["condition_id"] != omit_token_map_market
        ][:-1]
    _write_typed(universe_path, TOKEN_UNIVERSE_SCHEMA, universe_rows)
    _write_typed(token_map_path, TOKEN_MAP_SCHEMA, map_rows)
    return {
        "candidates": candidates_path,
        "timing_run": timing_run,
        "universe": universe_path,
        "token_map": token_map_path,
        "output": output,
    }


def _build(paths: dict[str, Path], output: Path | None = None) -> dict[str, Any]:
    return build_validated_universe(
        paths["candidates"],
        paths["timing_run"],
        paths["universe"],
        paths["token_map"],
        output or paths["output"],
    )


def _read(path: Path, order_by: str = "market_id") -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT * FROM read_parquet('{path}') ORDER BY {order_by}"
        ).fetchdf()
    finally:
        con.close()


def test_full_valid_path_writes_phase_ready_dimension(tmp_path: Path) -> None:
    candidate = _candidate("valid-2026", date(2026, 7, 4), away="lad", home="sf")
    game = _game(2001, candidate["date"], away="lad", home="sf")
    paths = _write_fixture(
        tmp_path / "fixture", [candidate], [game], [_exact_match(candidate, game)],
        {candidate["market_id"]: _timing(game.game_pk, candidate["date"])}
    )

    summary = _build(paths)
    audit = _read(paths["output"] / AUDIT_OUTPUT)
    eligible = _read(paths["output"] / ELIGIBLE_OUTPUT)

    assert summary["counts"] == {
        "candidate_markets": 1,
        "moneyline_valid_markets": 1,
        "core_timing_eligible_markets": 1,
        "eligible_moneylines": 1,
    }
    assert summary["exclusion_counts"] == {
        "upstream_match": {},
        "upstream_timing": {},
        "moneyline_validation": {},
        "core_timing": {},
    }
    assert summary["definition"] == "validated_mlb_moneyline_standard_timing_core_v1"
    assert bool(audit.loc[0, "moneyline_valid"])
    assert bool(audit.loc[0, "core_timing_eligible"])
    assert bool(audit.loc[0, "eligible_for_core_analysis"])
    assert audit.loc[0, "slug_orientation"] == "official"
    assert audit.loc[0, "game_type"] == "R"
    assert eligible.loc[0, "slug_orientation"] == "official"
    assert eligible.loc[0, "game_type"] == "R"
    assert eligible.loc[0, "home_token_id"] == "valid-2026-home"
    assert eligible.loc[0, "winning_outcome"] == "San Francisco Giants"
    assert eligible.loc[0, "actual_start_utc"] < eligible.loc[0, "inning_4_start_utc"]
    assert sorted(path.name for path in paths["output"].iterdir()) == [
        AUDIT_OUTPUT,
        ELIGIBLE_OUTPUT,
        SUMMARY_OUTPUT,
    ]
    assert json.loads((paths["output"] / SUMMARY_OUTPUT).read_text()) == summary


def test_reversed_slug_order_preserves_observation_and_uses_official_sides(
    tmp_path: Path,
) -> None:
    candidate = _candidate(
        "reversed-2025", date(2025, 7, 4), away="bos", home="nyy"
    )
    game = _game(
        2001,
        candidate["date"],
        away="nyy",
        home="bos",
        away_final_score=5,
        home_final_score=3,
        away_is_winner=True,
        home_is_winner=False,
    )
    paths = _write_fixture(
        tmp_path / "fixture",
        [candidate],
        [game],
        [_exact_match(candidate, game)],
        {candidate["market_id"]: _timing(game.game_pk, candidate["date"])},
    )

    _build(paths)
    audit = _read(paths["output"] / AUDIT_OUTPUT)
    eligible = _read(paths["output"] / ELIGIBLE_OUTPUT)

    assert (audit.loc[0, "away_slug"], audit.loc[0, "home_slug"]) == (
        "bos",
        "nyy",
    )
    assert audit.loc[0, "slug_orientation"] == "reversed"
    assert audit.loc[0, "away_team_id"] == MLB_TEAMS["nyy"].team_id
    assert audit.loc[0, "home_team_id"] == MLB_TEAMS["bos"].team_id
    assert eligible.loc[0, "slug_orientation"] == "reversed"
    assert eligible.loc[0, "away_token_id"] == "reversed-2025-home"
    assert eligible.loc[0, "home_token_id"] == "reversed-2025-away"
    assert eligible.loc[0, "winning_team_id"] == MLB_TEAMS["nyy"].team_id


def test_2025_short_team_labels_are_valid(tmp_path: Path) -> None:
    candidate = _candidate("valid-2025", date(2025, 7, 4))
    game = _game(2001, candidate["date"])
    paths = _write_fixture(
        tmp_path / "fixture", [candidate], [game], [_exact_match(candidate, game)],
        {candidate["market_id"]: _timing(game.game_pk, candidate["date"])}
    )

    _build(paths)
    eligible = _read(paths["output"] / ELIGIBLE_OUTPUT)

    assert eligible.loc[0, "winning_outcome"] == "Red Sox"
    assert bool(_read(paths["output"] / AUDIT_OUTPUT).loc[0, "moneyline_valid"])


@pytest.mark.parametrize("game_type", ["D", "S"])
def test_competition_stage_is_visible_but_not_a_timing_exclusion(
    tmp_path: Path, game_type: str
) -> None:
    candidate = _candidate(f"valid-{game_type.lower()}", date(2026, 10, 4))
    game = _game(2100, candidate["date"], game_type=game_type)
    paths = _write_fixture(
        tmp_path / "fixture", [candidate], [game], [_exact_match(candidate, game)],
        {candidate["market_id"]: _timing(game.game_pk, candidate["date"])}
    )

    _build(paths)
    audit = _read(paths["output"] / AUDIT_OUTPUT)
    eligible = _read(paths["output"] / ELIGIBLE_OUTPUT)

    assert bool(audit.loc[0, "core_timing_eligible"])
    assert audit.loc[0, "core_timing_exclusion_reason"] is None
    assert audit.loc[0, "game_type"] == game_type
    assert eligible.loc[0, "game_type"] == game_type


def test_upstream_timing_and_all_frozen_irregular_exclusions_are_retained(
    tmp_path: Path,
) -> None:
    labels = [
        "no-match",
        "timing-failed",
        "missing-timing",
        "rescheduled",
        "resumed",
        "suspended",
        "doubleheader",
        "shortened",
    ]
    candidates = [
        _candidate(label, date(2025, 7, 1) + timedelta(days=index))
        for index, label in enumerate(labels)
    ]
    games: list[ScheduleGame] = []
    audits: list[GameMatchAudit] = [_unmatched(candidates[0], "no_schedule_match")]
    timings: dict[str, GameTiming | None | Exception] = {}
    for index, candidate in enumerate(candidates[1:], start=1):
        changes: dict[str, Any] = {}
        if candidate["market_id"] == "rescheduled":
            changes["rescheduled_from_date"] = candidate["date"] - timedelta(days=1)
        elif candidate["market_id"] == "resumed":
            changes["resumed_from_date"] = candidate["date"] - timedelta(days=1)
        elif candidate["market_id"] == "suspended":
            changes["status_detailed"] = "Final (Suspended Game)"
        elif candidate["market_id"] == "doubleheader":
            changes["doubleheader"] = "Y"
        game = _game(3000 + index, candidate["date"], **changes)
        games.append(game)
        audits.append(_exact_match(candidate, game))
        if candidate["market_id"] == "timing-failed":
            timings[candidate["market_id"]] = ValueError("synthetic timing failure")
        elif candidate["market_id"] == "missing-timing":
            timings[candidate["market_id"]] = None
        else:
            final_inning = 7 if candidate["market_id"] == "shortened" else 9
            timings[candidate["market_id"]] = _timing(
                game.game_pk, candidate["date"], final_inning
            )
    paths = _write_fixture(
        tmp_path / "fixture", candidates, games, audits, timings
    )

    summary = _build(paths)
    audit = _read(paths["output"] / AUDIT_OUTPUT).set_index("market_id")

    expected = {
        "no-match": "upstream_match_no_schedule_match",
        "timing-failed": "upstream_timing_timing_fetch_or_parse_failure",
        "missing-timing": "missing_timing_row",
        "rescheduled": "irregular_rescheduled_game",
        "resumed": "irregular_resumed_game",
        "suspended": "irregular_suspended_game",
        "doubleheader": "irregular_doubleheader",
        "shortened": "irregular_shortened_game",
    }
    assert audit["core_timing_exclusion_reason"].to_dict() == expected
    assert not audit["core_timing_eligible"].any()
    assert audit.loc["no-match", "moneyline_exclusion_reason"] == "no_schedule_match"
    assert audit.loc["timing-failed", "timing_error_type"] == "ValueError"
    assert "synthetic timing failure" in audit.loc["timing-failed", "timing_error_message"]
    assert summary["exclusion_counts"]["core_timing"] == {
        reason: 1 for reason in sorted(expected.values())
    }
    assert summary["exclusion_counts"]["moneyline_validation"] == {
        "no_schedule_match": 1
    }
    assert summary["exclusion_counts"]["upstream_match"] == {"no_schedule_match": 1}
    assert summary["exclusion_counts"]["upstream_timing"] == {
        "not_exact_final_match": 1,
        "timing_fetch_or_parse_failure": 1,
    }
    assert len(_read(paths["output"] / ELIGIBLE_OUTPUT)) == 0


def test_token_composition_failure_leaves_no_output(tmp_path: Path) -> None:
    candidate = _candidate("bad-token-map", date(2025, 7, 4))
    game = _game(2001, candidate["date"])
    paths = _write_fixture(
        tmp_path / "fixture", [candidate], [game], [_exact_match(candidate, game)],
        {candidate["market_id"]: _timing(game.game_pk, candidate["date"])},
        omit_token_map_market=candidate["market_id"],
    )

    with pytest.raises(MoneylineTokenCompositionError, match="token_map is missing"):
        _build(paths)

    assert not paths["output"].exists()


def test_cross_artifact_score_inconsistency_fails_hard(tmp_path: Path) -> None:
    candidate = _candidate("bad-score", date(2025, 7, 4))
    game = _game(2001, candidate["date"])
    paths = _write_fixture(
        tmp_path / "fixture", [candidate], [game], [_exact_match(candidate, game)],
        {candidate["market_id"]: _timing(game.game_pk, candidate["date"])},
        mutate_timing_row=lambda row: row.update({"schedule_home_final_score": 99}),
    )

    with pytest.raises(ValidatedUniverseBuildError, match="schedule_home_final_score"):
        _build(paths)

    assert not paths["output"].exists()


def test_cross_artifact_slug_orientation_inconsistency_fails_hard(
    tmp_path: Path,
) -> None:
    candidate = _candidate("bad-orientation", date(2025, 7, 4))
    game = _game(2001, candidate["date"])
    audit = _exact_match(candidate, game)
    match_row = _match_row(audit)
    match_row["slug_orientation"] = "reversed"
    match_row["timing_status"] = "passed"
    paths = _write_fixture(
        tmp_path / "fixture",
        [candidate],
        [game],
        [audit],
        {candidate["market_id"]: _timing(game.game_pk, candidate["date"])},
        match_rows_override=[match_row],
    )

    with pytest.raises(ValidatedUniverseBuildError, match="slug_orientation"):
        _build(paths)

    assert not paths["output"].exists()


def test_cross_artifact_partial_doubleheader_selection_fails_hard(
    tmp_path: Path,
) -> None:
    candidate = _candidate("ranked-doubleheader", date(2025, 7, 4))
    first = _game(2001, candidate["date"], doubleheader="S", game_number=1)
    second = _game(2002, candidate["date"], doubleheader="S", game_number=2)
    paths = _write_fixture(
        tmp_path / "fixture",
        [candidate],
        [first, second],
        [_exact_match(candidate, first)],
        {candidate["market_id"]: _timing(first.game_pk, candidate["date"])},
    )

    with pytest.raises(ValidatedUniverseBuildError, match="selected schedule matches"):
        _build(paths)

    assert not paths["output"].exists()


@pytest.mark.parametrize("mode", ["missing", "duplicate"])
def test_candidate_match_rows_reconcile_one_to_one(tmp_path: Path, mode: str) -> None:
    candidate = _candidate("candidate", date(2025, 7, 4))
    game = _game(2001, candidate["date"])
    audit = _exact_match(candidate, game)
    match_row = _match_row(audit)
    match_row["timing_status"] = "passed"
    override = [] if mode == "missing" else [match_row, dict(match_row)]
    paths = _write_fixture(
        tmp_path / "fixture", [candidate], [game], [audit],
        {candidate["market_id"]: _timing(game.game_pk, candidate["date"])},
        match_rows_override=override,
    )

    with pytest.raises(ValidatedUniverseBuildError, match="match"):
        _build(paths)

    assert not paths["output"].exists()


def test_atomic_cleanup_and_deterministic_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = _candidate("deterministic", date(2025, 7, 4))
    game = _game(2001, candidate["date"])
    paths = _write_fixture(
        tmp_path / "fixture", [candidate], [game], [_exact_match(candidate, game)],
        {candidate["market_id"]: _timing(game.game_pk, candidate["date"])}
    )
    first = tmp_path / "run-first"
    second = tmp_path / "run-second"

    _build(paths, first)
    _build(paths, second)

    for filename in (AUDIT_OUTPUT, ELIGIBLE_OUTPUT, SUMMARY_OUTPUT):
        assert (first / filename).read_bytes() == (second / filename).read_bytes()

    failing = tmp_path / "run-failing"
    original_write = validated_module._write_rows
    calls = 0

    def fail_on_second_write(*args: Any, **kwargs: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("synthetic staging failure")
        original_write(*args, **kwargs)

    monkeypatch.setattr(validated_module, "_write_rows", fail_on_second_write)
    with pytest.raises(RuntimeError, match="synthetic staging failure"):
        _build(paths, failing)

    assert not failing.exists()
    assert list(tmp_path.glob(".run-failing.staging-*")) == []
