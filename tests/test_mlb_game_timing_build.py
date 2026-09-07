from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
sys.path.insert(0, str(MODULE_DIR))

import build_game_timing as timing_build  # noqa: E402
from build_game_timing import (  # noqa: E402
    MATCH_OUTPUT,
    SCHEDULE_OUTPUT,
    SUMMARY_OUTPUT,
    TIMING_OUTPUT,
    build_game_timing_audit,
)
from match_games import MLB_TEAMS  # noqa: E402
from mlb_api import GameTiming, InningHalfBoundary, PhaseWindow, ScheduleGame  # noqa: E402


def _candidate(
    market_id: str,
    away: str = "nyy",
    home: str = "bos",
    game_date: str = "2025-07-04",
) -> dict[str, object]:
    return {
        "market_id": market_id,
        "date": game_date,
        "away": away,
        "home": home,
        "question": f"{away} vs. {home}",
    }


def _write_candidates(path: Path, rows: list[dict[str, object]]) -> None:
    con = duckdb.connect()
    try:
        con.register("candidate_rows", pd.DataFrame(rows))
        con.execute(f"COPY candidate_rows TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()


def _game(
    game_pk: int,
    away: str = "nyy",
    home: str = "bos",
    *,
    game_date: date = date(2025, 7, 4),
    final: bool = True,
    doubleheader: str = "N",
    game_number: int = 1,
    away_score: int = 3,
    home_score: int = 5,
) -> ScheduleGame:
    return ScheduleGame(
        game_pk=game_pk,
        official_date=game_date,
        scheduled_start_utc=datetime.combine(
            game_date, datetime.min.time(), tzinfo=timezone.utc
        )
        + timedelta(hours=17),
        game_type="R",
        season=2025,
        away_team_id=MLB_TEAMS[away].team_id,
        away_team_name=MLB_TEAMS[away].name,
        home_team_id=MLB_TEAMS[home].team_id,
        home_team_name=MLB_TEAMS[home].name,
        status_abstract="Final" if final else "Preview",
        status_detailed="Final" if final else "Postponed",
        status_code="F" if final else "DR",
        is_completed=final,
        doubleheader=doubleheader,
        game_number=game_number,
        series_game_number=game_number,
        reschedule_date_utc=None,
        rescheduled_from_date=None,
        resume_date_utc=None,
        resumed_from_date=None,
        away_final_score=away_score if final else None,
        home_final_score=home_score if final else None,
        away_is_winner=(away_score > home_score) if final else None,
        home_is_winner=(home_score > away_score) if final else None,
    )


def _timing(game_pk: int, day: date = date(2025, 7, 4)) -> GameTiming:
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
        hours=17, minutes=5
    )
    inning_4 = start + timedelta(hours=1)
    inning_7 = start + timedelta(hours=2)
    end = start + timedelta(hours=3)
    halves = (
        InningHalfBoundary(1, "top", "innings_1_3", start, start + timedelta(minutes=5), 0, 0),
        InningHalfBoundary(
            4, "top", "innings_4_6", inning_4, inning_4 + timedelta(minutes=5), 1, 1
        ),
        InningHalfBoundary(
            7, "top", "innings_7_plus", inning_7, inning_7 + timedelta(minutes=5), 2, 2
        ),
        InningHalfBoundary(9, "bottom", "innings_7_plus", end - timedelta(minutes=5), end, 3, 3),
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


class _FakeClient:
    def __init__(
        self,
        schedules: list[ScheduleGame],
        timings: dict[int, GameTiming | Exception] | None = None,
    ) -> None:
        self.schedules = schedules
        self.timings = timings or {}
        self.schedule_calls: list[tuple[date, date, bool, bool]] = []
        self.timing_calls: list[tuple[int, bool]] = []

    def schedule_games(
        self,
        start_date: date,
        end_date: date,
        *,
        include_nonfinal_for_audit: bool = False,
        refresh: bool = False,
    ) -> tuple[ScheduleGame, ...]:
        self.schedule_calls.append(
            (start_date, end_date, include_nonfinal_for_audit, refresh)
        )
        return tuple(self.schedules)

    def game_timing(self, game_pk: int, *, refresh: bool = False) -> GameTiming:
        self.timing_calls.append((game_pk, refresh))
        result = self.timings[game_pk]
        if isinstance(result, Exception):
            raise result
        return result


def _read(path: Path, order_by: str) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT * FROM read_parquet('{path}') ORDER BY {order_by}"
        ).fetchdf()
    finally:
        con.close()


def test_full_success_writes_schedule_match_timing_and_summary(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _write_candidates(candidates, [_candidate("market-a")])
    game = _game(2001)
    client = _FakeClient([game], {2001: _timing(2001)})
    output = tmp_path / "output"

    summary = build_game_timing_audit(
        candidates, tmp_path / "cache", output, refresh=True, client=client
    )

    assert client.schedule_calls == [
        (date(2025, 7, 4), date(2025, 7, 4), True, True)
    ]
    assert client.timing_calls == [(2001, True)]
    schedule = _read(output / SCHEDULE_OUTPUT, "game_pk")
    match = _read(output / MATCH_OUTPUT, "market_id")
    timing = _read(output / TIMING_OUTPUT, "game_pk")
    assert schedule["game_pk"].tolist() == [2001]
    assert schedule.loc[0, "away_final_score"] == 3
    assert schedule.loc[0, "home_final_score"] == 5
    assert bool(schedule.loc[0, "away_is_winner"]) is False
    assert bool(schedule.loc[0, "home_is_winner"]) is True
    assert match.loc[0, "timing_status"] == "passed"
    assert pd.isna(match.loc[0, "match_exclusion_reason"])
    assert match.loc[0, "schedule_away_final_score"] == 3
    assert match.loc[0, "schedule_home_final_score"] == 5
    assert bool(match.loc[0, "schedule_away_is_winner"]) is False
    assert bool(match.loc[0, "schedule_home_is_winner"]) is True
    assert timing["game_pk"].tolist() == [2001]
    assert timing.loc[0, "schedule_away_final_score"] == 3
    assert timing.loc[0, "schedule_home_final_score"] == 5
    assert bool(timing.loc[0, "schedule_away_is_winner"]) is False
    assert bool(timing.loc[0, "schedule_home_is_winner"]) is True
    assert timing.loc[0, "inning_4_start_utc"] == pd.Timestamp(
        "2025-07-04T18:05:00Z"
    )
    assert timing.loc[0, "inning_7_start_utc"] == pd.Timestamp(
        "2025-07-04T19:05:00Z"
    )
    assert summary["candidate_markets"] == 1
    assert summary["timing_games_written"] == 1
    assert summary["espn_fallback_used"] is False
    assert json.loads((output / SUMMARY_OUTPUT).read_text()) == summary


def test_reversed_slug_order_is_audited_with_canonical_schedule_sides(
    tmp_path: Path,
) -> None:
    candidates = tmp_path / "candidates.parquet"
    _write_candidates(
        candidates, [_candidate("reversed", away="bos", home="nyy")]
    )
    game = _game(2001, away="nyy", home="bos")
    client = _FakeClient([game], {2001: _timing(2001)})
    output = tmp_path / "output"

    summary = build_game_timing_audit(
        candidates, tmp_path / "cache", output, client=client
    )

    match = _read(output / MATCH_OUTPUT, "market_id")
    assert (match.loc[0, "away_slug"], match.loc[0, "home_slug"]) == (
        "bos",
        "nyy",
    )
    assert match.loc[0, "slug_orientation"] == "reversed"
    assert match.loc[0, "away_team_id"] == MLB_TEAMS["nyy"].team_id
    assert match.loc[0, "away_team_name"] == MLB_TEAMS["nyy"].name
    assert match.loc[0, "home_team_id"] == MLB_TEAMS["bos"].team_id
    assert match.loc[0, "home_team_name"] == MLB_TEAMS["bos"].name
    assert client.timing_calls == [(2001, False)]
    assert summary["exact_final_matches"] == 1


def test_ambiguous_nonfinal_and_no_match_are_all_retained(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _write_candidates(
        candidates,
        [
            _candidate("ambiguous"),
            _candidate("nonfinal", away="lad", home="sf"),
            _candidate("no-match", away="mia", home="atl"),
        ],
    )
    schedules = [
        _game(2001, doubleheader="Y", game_number=1),
        _game(2002, doubleheader="Y", game_number=2),
        _game(2003, away="lad", home="sf", final=False),
    ]
    client = _FakeClient(schedules)
    output = tmp_path / "output"

    summary = build_game_timing_audit(
        candidates, tmp_path / "cache", output, client=client
    )

    match = _read(output / MATCH_OUTPUT, "market_id").set_index("market_id")
    assert match.loc["ambiguous", "match_exclusion_reason"] == "multiple_schedule_matches"
    assert match.loc["nonfinal", "match_exclusion_reason"] == "nonfinal_game"
    assert match.loc["no-match", "match_exclusion_reason"] == "no_schedule_match"
    assert set(match["timing_status"]) == {"not_eligible"}
    assert client.timing_calls == []
    assert len(_read(output / TIMING_OUTPUT, "game_pk")) == 0
    assert summary["match_exclusions"] == {
        "multiple_schedule_matches": 1,
        "no_schedule_match": 1,
        "nonfinal_game": 1,
    }


def test_timing_failure_remains_on_candidate_audit(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _write_candidates(
        candidates,
        [
            _candidate("good"),
            _candidate("bad", away="lad", home="sf"),
        ],
    )
    schedules = [_game(2001), _game(2002, away="lad", home="sf")]
    client = _FakeClient(
        schedules,
        {2001: _timing(2001), 2002: ValueError("missing top-of-inning boundary")},
    )
    output = tmp_path / "output"

    summary = build_game_timing_audit(
        candidates, tmp_path / "cache", output, client=client
    )

    match = _read(output / MATCH_OUTPUT, "market_id").set_index("market_id")
    assert match.loc["bad", "matched_game_pk"] == 2002
    assert match.loc["bad", "timing_status"] == "failed"
    assert match.loc["bad", "timing_exclusion_reason"] == "timing_fetch_or_parse_failure"
    assert match.loc["bad", "timing_error_type"] == "ValueError"
    assert "missing top-of-inning" in match.loc["bad", "timing_error_message"]
    assert match.loc["good", "timing_status"] == "passed"
    assert _read(output / TIMING_OUTPUT, "game_pk")["game_pk"].tolist() == [2001]
    assert summary["exact_final_matches"] == 2
    assert summary["timing_games_written"] == 1
    assert summary["timing_failures"] == 1


def test_duplicate_candidate_to_game_mapping_fails_before_timing_or_output(
    tmp_path: Path,
) -> None:
    candidates = tmp_path / "candidates.parquet"
    _write_candidates(
        candidates, [_candidate("market-a"), _candidate("market-b")]
    )
    client = _FakeClient([_game(2001)], {2001: _timing(2001)})
    output = tmp_path / "output"

    with pytest.raises(ValueError, match="not one-to-one"):
        build_game_timing_audit(
            candidates, tmp_path / "cache", output, client=client
        )

    assert client.timing_calls == []
    assert not output.exists()


def test_existing_output_directory_is_rejected_before_api_work(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _write_candidates(candidates, [_candidate("market-a")])
    client = _FakeClient([_game(2001)], {2001: _timing(2001)})
    output = tmp_path / "output"
    output.mkdir()

    with pytest.raises(FileExistsError, match="already exists"):
        build_game_timing_audit(
            candidates, tmp_path / "cache", output, client=client
        )

    assert client.schedule_calls == []
    assert list(output.iterdir()) == []


def test_raw_cache_cannot_be_nested_in_published_output(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _write_candidates(candidates, [_candidate("market-a")])
    client = _FakeClient([_game(2001)], {2001: _timing(2001)})
    output = tmp_path / "output"

    with pytest.raises(ValueError, match="must not overlap"):
        build_game_timing_audit(
            candidates, output / "raw-cache", output, client=client
        )

    assert client.schedule_calls == []
    assert not output.exists()


def test_published_output_cannot_be_nested_in_raw_cache(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.parquet"
    _write_candidates(candidates, [_candidate("market-a")])
    client = _FakeClient([_game(2001)], {2001: _timing(2001)})
    cache = tmp_path / "raw-cache"
    output = cache / "published-output"

    with pytest.raises(ValueError, match="must not overlap"):
        build_game_timing_audit(candidates, cache, output, client=client)

    assert client.schedule_calls == []
    assert not cache.exists()
    assert not output.exists()


def test_failed_write_removes_staging_without_publishing_partial_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidates = tmp_path / "candidates.parquet"
    _write_candidates(candidates, [_candidate("market-a")])
    cache = tmp_path / "raw-cache"
    cache.mkdir()
    cache_marker = cache / "existing-raw-response.json"
    cache_marker.write_text("{}\n", encoding="utf-8")
    output = tmp_path / "output"
    client = _FakeClient([_game(2001)], {2001: _timing(2001)})
    original_write = timing_build._write_parquet
    call_count = 0

    def fail_on_second_write(*args: object, **kwargs: object) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("synthetic output failure")
        original_write(*args, **kwargs)

    monkeypatch.setattr(timing_build, "_write_parquet", fail_on_second_write)
    with pytest.raises(RuntimeError, match="synthetic output failure"):
        build_game_timing_audit(candidates, cache, output, client=client)

    assert not output.exists()
    assert list(tmp_path.glob(".output.staging-*")) == []
    assert cache_marker.read_text(encoding="utf-8") == "{}\n"


def test_outputs_are_deterministic_across_input_orders(tmp_path: Path) -> None:
    rows = [
        _candidate("market-b", away="lad", home="sf"),
        _candidate("market-a"),
    ]
    first_candidates = tmp_path / "candidates-first.parquet"
    second_candidates = tmp_path / "candidates-second.parquet"
    _write_candidates(first_candidates, rows)
    _write_candidates(second_candidates, list(reversed(rows)))
    games = [_game(2002, away="lad", home="sf"), _game(2001)]
    timings = {2001: _timing(2001), 2002: _timing(2002)}
    first_output = tmp_path / "first"
    second_output = tmp_path / "second"

    build_game_timing_audit(
        first_candidates,
        tmp_path / "cache-first",
        first_output,
        client=_FakeClient(games, timings),
    )
    build_game_timing_audit(
        second_candidates,
        tmp_path / "cache-second",
        second_output,
        client=_FakeClient(list(reversed(games)), timings),
    )

    for filename in (SCHEDULE_OUTPUT, MATCH_OUTPUT, TIMING_OUTPUT, SUMMARY_OUTPUT):
        assert (first_output / filename).read_bytes() == (second_output / filename).read_bytes()
