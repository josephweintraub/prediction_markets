"""Build deterministic MLB schedule, market-match, and game-timing audits.

This runner composes the official MLB client and the exact matcher. It does
not select Polymarket candidates, guess among doubleheaders, or implement an
ESPN fallback. A failure to fetch or parse one matched game's live feed is
retained on that candidate's audit row while other games continue.
"""
from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from collections import Counter
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import duckdb

from match_games import GameMatchAudit, assert_one_to_one_matches, match_market_candidates
from mlb_api import GameTiming, MlbApiClient, ScheduleGame


SCHEDULE_OUTPUT = "schedule_audit.parquet"
MATCH_OUTPUT = "match_audit.parquet"
TIMING_OUTPUT = "game_timing.parquet"
SUMMARY_OUTPUT = "summary.json"

SCHEDULE_SCHEMA = (
    ("game_pk", "BIGINT"),
    ("official_date", "DATE"),
    ("scheduled_start_utc", "TIMESTAMPTZ"),
    ("game_type", "VARCHAR"),
    ("season", "INTEGER"),
    ("away_team_id", "INTEGER"),
    ("away_team_name", "VARCHAR"),
    ("home_team_id", "INTEGER"),
    ("home_team_name", "VARCHAR"),
    ("status_abstract", "VARCHAR"),
    ("status_detailed", "VARCHAR"),
    ("status_code", "VARCHAR"),
    ("is_completed", "BOOLEAN"),
    ("doubleheader", "VARCHAR"),
    ("game_number", "INTEGER"),
    ("series_game_number", "INTEGER"),
    ("reschedule_date_utc", "TIMESTAMPTZ"),
    ("rescheduled_from_date", "DATE"),
    ("resume_date_utc", "TIMESTAMPTZ"),
    ("resumed_from_date", "DATE"),
    ("away_final_score", "INTEGER"),
    ("home_final_score", "INTEGER"),
    ("away_is_winner", "BOOLEAN"),
    ("home_is_winner", "BOOLEAN"),
)

MATCH_SCHEMA = (
    ("market_id", "VARCHAR"),
    ("market_date", "DATE"),
    # These retain the observed event-slug positions, despite their legacy names.
    ("away_slug", "VARCHAR"),
    ("home_slug", "VARCHAR"),
    ("slug_orientation", "VARCHAR"),
    # These are canonical official-schedule sides, never inferred from slug order.
    ("away_team_id", "INTEGER"),
    ("away_team_name", "VARCHAR"),
    ("home_team_id", "INTEGER"),
    ("home_team_name", "VARCHAR"),
    ("matched_game_pk", "BIGINT"),
    ("match_exclusion_reason", "VARCHAR"),
    ("schedule_match_count", "INTEGER"),
    ("schedule_match_game_pks_json", "VARCHAR"),
    ("scheduled_start_utc", "TIMESTAMPTZ"),
    ("schedule_status_abstract", "VARCHAR"),
    ("schedule_status_detailed", "VARCHAR"),
    ("schedule_status_code", "VARCHAR"),
    ("schedule_is_completed", "BOOLEAN"),
    ("schedule_doubleheader", "VARCHAR"),
    ("schedule_game_number", "INTEGER"),
    ("schedule_series_game_number", "INTEGER"),
    ("schedule_reschedule_date_utc", "TIMESTAMPTZ"),
    ("schedule_rescheduled_from_date", "DATE"),
    ("schedule_resume_date_utc", "TIMESTAMPTZ"),
    ("schedule_resumed_from_date", "DATE"),
    ("schedule_away_final_score", "INTEGER"),
    ("schedule_home_final_score", "INTEGER"),
    ("schedule_away_is_winner", "BOOLEAN"),
    ("schedule_home_is_winner", "BOOLEAN"),
    ("timing_status", "VARCHAR"),
    ("timing_exclusion_reason", "VARCHAR"),
    ("timing_error_type", "VARCHAR"),
    ("timing_error_message", "VARCHAR"),
)

TIMING_SCHEMA = (
    ("market_id", "VARCHAR"),
    ("game_pk", "BIGINT"),
    ("official_date", "DATE"),
    ("away_team_id", "INTEGER"),
    ("away_team_name", "VARCHAR"),
    ("home_team_id", "INTEGER"),
    ("home_team_name", "VARCHAR"),
    ("scheduled_start_utc", "TIMESTAMPTZ"),
    ("actual_start_utc", "TIMESTAMPTZ"),
    ("inning_4_start_utc", "TIMESTAMPTZ"),
    ("inning_7_start_utc", "TIMESTAMPTZ"),
    ("actual_end_utc", "TIMESTAMPTZ"),
    ("final_inning", "INTEGER"),
    ("play_count", "INTEGER"),
    ("status_abstract", "VARCHAR"),
    ("status_detailed", "VARCHAR"),
    ("status_code", "VARCHAR"),
    ("doubleheader", "VARCHAR"),
    ("game_number", "INTEGER"),
    ("series_game_number", "INTEGER"),
    ("reschedule_date_utc", "TIMESTAMPTZ"),
    ("rescheduled_from_date", "DATE"),
    ("resume_date_utc", "TIMESTAMPTZ"),
    ("resumed_from_date", "DATE"),
    ("schedule_away_final_score", "INTEGER"),
    ("schedule_home_final_score", "INTEGER"),
    ("schedule_away_is_winner", "BOOLEAN"),
    ("schedule_home_is_winner", "BOOLEAN"),
)


def _quote_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve()).replace("'", "''")


def _read_candidates(path: str | Path) -> tuple[list[dict[str, Any]], date, date]:
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Candidate-market Parquet does not exist: {source}")
    con = duckdb.connect()
    try:
        relation = f"read_parquet('{_quote_path(source)}')"
        columns = {
            row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
        }
        missing = sorted({"market_id", "date", "away", "home"} - columns)
        if missing:
            raise ValueError(f"Candidate-market Parquet is missing columns: {missing}")
        frame = con.execute(
            f"""
            SELECT *
            FROM {relation}
            ORDER BY TRY_CAST(date AS DATE) NULLS LAST, away, home, market_id
            """
        ).fetchdf()
        if frame.empty:
            raise ValueError("Candidate-market Parquet contains no rows")
        start_date, end_date = con.execute(
            f"SELECT MIN(TRY_CAST(date AS DATE)), MAX(TRY_CAST(date AS DATE)) FROM {relation}"
        ).fetchone()
    finally:
        con.close()
    if start_date is None or end_date is None:
        raise ValueError("Candidate-market Parquet contains no valid dates")
    return frame.to_dict(orient="records"), start_date, end_date


def _write_parquet(
    rows: Iterable[dict[str, Any]],
    schema: tuple[tuple[str, str], ...],
    path: Path,
    order_by: str,
) -> None:
    columns = [name for name, _ in schema]
    con = duckdb.connect()
    try:
        declarations = ", ".join(f'"{name}" {kind}' for name, kind in schema)
        con.execute(f"CREATE TABLE output_rows ({declarations})")
        materialized = list(rows)
        if materialized:
            placeholders = ", ".join("?" for _ in columns)
            con.executemany(
                f"INSERT INTO output_rows VALUES ({placeholders})",
                [[row.get(column) for column in columns] for row in materialized],
            )
        con.execute(
            f"COPY (SELECT * FROM output_rows ORDER BY {order_by}) "
            f"TO '{_quote_path(path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()


def _schedule_rows(games: Iterable[ScheduleGame]) -> list[dict[str, Any]]:
    return [
        asdict(game)
        for game in sorted(games, key=lambda item: (item.official_date, item.game_pk))
    ]


def _match_row(audit: GameMatchAudit) -> dict[str, Any]:
    unique = audit.schedule_matches[0] if len(audit.schedule_matches) == 1 else None
    return {
        "market_id": audit.market_id,
        "market_date": audit.market_date,
        "away_slug": audit.away_slug,
        "home_slug": audit.home_slug,
        "slug_orientation": audit.slug_orientation,
        "away_team_id": audit.away_team.team_id if audit.away_team else None,
        "away_team_name": audit.away_team.name if audit.away_team else None,
        "home_team_id": audit.home_team.team_id if audit.home_team else None,
        "home_team_name": audit.home_team.name if audit.home_team else None,
        "matched_game_pk": audit.matched_game_pk,
        "match_exclusion_reason": audit.exclusion_reason,
        "schedule_match_count": len(audit.schedule_matches),
        "schedule_match_game_pks_json": json.dumps(
            [game.game_pk for game in audit.schedule_matches], separators=(",", ":")
        ),
        "scheduled_start_utc": unique.scheduled_start_utc if unique else None,
        "schedule_status_abstract": unique.status_abstract if unique else None,
        "schedule_status_detailed": unique.status_detailed if unique else None,
        "schedule_status_code": unique.status_code if unique else None,
        "schedule_is_completed": unique.is_completed if unique else None,
        "schedule_doubleheader": unique.doubleheader if unique else None,
        "schedule_game_number": unique.game_number if unique else None,
        "schedule_series_game_number": unique.series_game_number if unique else None,
        "schedule_reschedule_date_utc": unique.reschedule_date_utc if unique else None,
        "schedule_rescheduled_from_date": unique.rescheduled_from_date if unique else None,
        "schedule_resume_date_utc": unique.resume_date_utc if unique else None,
        "schedule_resumed_from_date": unique.resumed_from_date if unique else None,
        "schedule_away_final_score": unique.away_final_score if unique else None,
        "schedule_home_final_score": unique.home_final_score if unique else None,
        "schedule_away_is_winner": unique.away_is_winner if unique else None,
        "schedule_home_is_winner": unique.home_is_winner if unique else None,
        "timing_status": "not_eligible" if audit.exclusion_reason else "pending",
        "timing_exclusion_reason": (
            "not_exact_final_match" if audit.exclusion_reason else None
        ),
        "timing_error_type": None,
        "timing_error_message": None,
    }


def _timing_row(audit: GameMatchAudit, timing: GameTiming) -> dict[str, Any]:
    schedule = audit.schedule_matches[0]
    boundaries = {
        (boundary.inning, boundary.half): boundary.start_utc
        for boundary in timing.inning_halves
    }
    return {
        "market_id": audit.market_id,
        "game_pk": timing.game_pk,
        "official_date": schedule.official_date,
        "away_team_id": schedule.away_team_id,
        "away_team_name": schedule.away_team_name,
        "home_team_id": schedule.home_team_id,
        "home_team_name": schedule.home_team_name,
        "scheduled_start_utc": schedule.scheduled_start_utc,
        "actual_start_utc": timing.actual_start_utc,
        "inning_4_start_utc": boundaries.get((4, "top")),
        "inning_7_start_utc": boundaries.get((7, "top")),
        "actual_end_utc": timing.actual_end_utc,
        "final_inning": max(boundary.inning for boundary in timing.inning_halves),
        "play_count": timing.play_count,
        "status_abstract": schedule.status_abstract,
        "status_detailed": schedule.status_detailed,
        "status_code": schedule.status_code,
        "doubleheader": schedule.doubleheader,
        "game_number": schedule.game_number,
        "series_game_number": schedule.series_game_number,
        "reschedule_date_utc": schedule.reschedule_date_utc,
        "rescheduled_from_date": schedule.rescheduled_from_date,
        "resume_date_utc": schedule.resume_date_utc,
        "resumed_from_date": schedule.resumed_from_date,
        "schedule_away_final_score": schedule.away_final_score,
        "schedule_home_final_score": schedule.home_final_score,
        "schedule_away_is_winner": schedule.away_is_winner,
        "schedule_home_is_winner": schedule.home_is_winner,
    }


def build_game_timing_audit(
    candidate_path: str | Path,
    cache_dir: str | Path,
    output_dir: str | Path,
    *,
    refresh: bool = False,
    client: MlbApiClient | None = None,
) -> dict[str, Any]:
    """Fetch official records and write complete matching/timing audits."""

    output = Path(output_dir).expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"MLB timing output directory already exists: {output}")
    cache = Path(cache_dir).expanduser().resolve()
    if (
        cache == output
        or cache.is_relative_to(output)
        or output.is_relative_to(cache)
    ):
        raise ValueError("MLB raw cache and published output directory must not overlap")
    output.parent.mkdir(parents=True, exist_ok=True)

    candidates, start_date, end_date = _read_candidates(candidate_path)
    api = client or MlbApiClient(cache)
    schedules = tuple(
        api.schedule_games(
            start_date,
            end_date,
            include_nonfinal_for_audit=True,
            refresh=refresh,
        )
    )
    audits = match_market_candidates(candidates, schedules)
    if len(audits) != len(candidates):
        raise ValueError("Matcher did not return exactly one audit row per candidate")
    assert_one_to_one_matches(audits)

    match_rows = {_audit.market_id: _match_row(_audit) for _audit in audits}
    timing_rows: list[dict[str, Any]] = []
    for audit in audits:
        if not audit.is_matched:
            continue
        try:
            timing = api.game_timing(audit.matched_game_pk, refresh=refresh)
            if timing.game_pk != audit.matched_game_pk:
                raise ValueError(
                    f"Requested game {audit.matched_game_pk}, but timing returned "
                    f"game {timing.game_pk}"
                )
            timing_rows.append(_timing_row(audit, timing))
        except Exception as exc:
            row = match_rows[audit.market_id]
            row["timing_status"] = "failed"
            row["timing_exclusion_reason"] = "timing_fetch_or_parse_failure"
            row["timing_error_type"] = type(exc).__name__
            row["timing_error_message"] = str(exc)
        else:
            match_rows[audit.market_id]["timing_status"] = "passed"

    match_exclusions = Counter(
        audit.exclusion_reason for audit in audits if audit.exclusion_reason is not None
    )
    timing_exclusions = Counter(
        row["timing_exclusion_reason"]
        for row in match_rows.values()
        if row["timing_exclusion_reason"] is not None
    )
    summary = {
        "source": "MLB Stats API",
        "espn_fallback_used": False,
        "refresh": refresh,
        "candidate_date_min": start_date.isoformat(),
        "candidate_date_max": end_date.isoformat(),
        "candidate_markets": len(candidates),
        "schedule_records": len(schedules),
        "schedule_final_records": sum(game.is_completed for game in schedules),
        "exact_final_matches": sum(audit.is_matched for audit in audits),
        "timing_games_written": len(timing_rows),
        "timing_failures": sum(
            row["timing_status"] == "failed" for row in match_rows.values()
        ),
        "match_exclusions": dict(sorted(match_exclusions.items())),
        "timing_exclusions": dict(sorted(timing_exclusions.items())),
        "outputs": {
            "schedule": SCHEDULE_OUTPUT,
            "match": MATCH_OUTPUT,
            "timing": TIMING_OUTPUT,
        },
    }

    staging: Path | None = None
    try:
        staging = Path(
            tempfile.mkdtemp(prefix=f".{output.name}.staging-", dir=output.parent)
        )
        paths = {
            "schedule": staging / SCHEDULE_OUTPUT,
            "match": staging / MATCH_OUTPUT,
            "timing": staging / TIMING_OUTPUT,
            "summary": staging / SUMMARY_OUTPUT,
        }
        _write_parquet(
            _schedule_rows(schedules),
            SCHEDULE_SCHEMA,
            paths["schedule"],
            "official_date, game_pk",
        )
        _write_parquet(
            match_rows.values(),
            MATCH_SCHEMA,
            paths["match"],
            "market_date NULLS LAST, away_slug, home_slug, market_id",
        )
        _write_parquet(
            timing_rows,
            TIMING_SCHEMA,
            paths["timing"],
            "official_date, game_pk, market_id",
        )
        with paths["summary"].open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True)
            handle.write("\n")

        if output.exists():
            raise FileExistsError(
                f"MLB timing output directory appeared during the run: {output}"
            )
        staging.rename(output)
        staging = None
        return summary
    finally:
        if staging is not None and staging.exists():
            shutil.rmtree(staging)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", required=True, help="Candidate-market Parquet")
    parser.add_argument("--cache-dir", required=True, help="Raw MLB response-cache directory")
    parser.add_argument("--output-dir", required=True, help="New audit output directory")
    parser.add_argument("--refresh", action="store_true", help="Refresh cached MLB responses")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    summary = build_game_timing_audit(
        args.candidates,
        args.cache_dir,
        args.output_dir,
        refresh=args.refresh,
    )
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
