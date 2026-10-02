"""Audit frozen ATP clocks and estimate an exploratory Grand Slam restriction.

The original ATP clock is scheduled-start plus archived duration.  Optional
official Australian Open evidence supplies provider-recorded actual start and
final competitive point times.  That start has minute precision and provider
latency is unquantified; exact first-serve verification remains unavailable.
This stage never downloads data or overwrites a completed run.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import duckdb

from analysis.multisport_game_dynamics.provider_extractors import (
    CompetitionRecord,
    flatten_espn_tennis,
    match_name_pair,
    names_match,
    normalize_name,
)
from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,
    fingerprint,
    fresh_run,
    matches_artifact_fingerprint,
    quoted,
    require_columns,
    write_json,
    write_parquet,
)


MIN_CELL_N = 500
CLOCK = "scheduled_start_plus_archived_duration"
ACTUAL_CLOCK = "ao_provider_actual_start_and_terminal_point"
ACTUAL_QUALIFICATION = ("minute_precision_provider_start;final_competitive_point_record;"
                        "provider_latency_unquantified;not_second_exact_first_serve")
COHORTS = ("all_atp", "grand_slam", "ao_provider_actual", "ao_same_cohort_scheduled")
SAMPLES = {
    "filtered_trades": "price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman",
    "all_trades": "price>0 AND price<1",
}
SLAM_ALIASES = {
    "australian open": "Australian Open",
    "french open": "Roland-Garros",
    "roland garros": "Roland-Garros",
    "wimbledon": "Wimbledon",
    "us open": "US Open",
    "u s open": "US Open",
}
EVENT_SCHEMA = (
    ("event_slug", "VARCHAR"), ("market_date", "DATE"),
    ("provider_competition_id", "VARCHAR"), ("provider_event_name", "VARCHAR"),
    ("archive_tourney_id", "VARCHAR"), ("archive_match_num", "VARCHAR"),
    ("archive_tourney_name", "VARCHAR"), ("archive_tourney_level", "VARCHAR"),
    ("archive_round", "VARCHAR"), ("archive_best_of", "VARCHAR"),
    ("archive_score", "VARCHAR"), ("archive_minutes", "INTEGER"),
    ("grand_slam_name", "VARCHAR"), ("is_grand_slam", "BOOLEAN"),
    ("classification_exclusion_reason", "VARCHAR"),
    ("scheduled_start_utc", "TIMESTAMP WITH TIME ZONE"),
    ("synthetic_end_utc", "TIMESTAMP WITH TIME ZONE"),
    ("clock_basis", "VARCHAR"), ("exact_firstserve_verified", "BOOLEAN"),
    ("exact_firstserve_exclusion_reason", "VARCHAR"),
)
PROVIDER_EVENT_SCHEMA = EVENT_SCHEMA + (
    ("provider_actual_eligible", "BOOLEAN"), ("provider_actual_exclusion_reason", "VARCHAR"),
    ("ao_match_id", "VARCHAR"), ("provider_actual_start_utc", "TIMESTAMP WITH TIME ZONE"),
    ("provider_actual_end_utc", "TIMESTAMP WITH TIME ZONE"),
    ("start_precision_seconds", "INTEGER"), ("end_precision_seconds", "INTEGER"),
    ("provider_actual_qualification", "VARCHAR"),
)
PROFILE_SCHEMA = (
    ("cohort", "VARCHAR"), ("sample", "VARCHAR"), ("clock_basis", "VARCHAR"),
    ("weighting", "VARCHAR"), ("time_bin", "INTEGER"),
    ("time_low", "DOUBLE"), ("time_high", "DOUBLE"),
    ("price_bin", "INTEGER"), ("n_fills", "BIGINT"), ("n_events", "BIGINT"),
    ("dollars", "DOUBLE"), ("mean_price", "DOUBLE"),
    ("win_rate", "DOUBLE"), ("mean_calibration", "DOUBLE"),
    ("suppressed", "BOOLEAN"), ("uncertainty_status", "VARCHAR"),
)
TAIL_SCHEMA = (
    ("cohort", "VARCHAR"), ("sample", "VARCHAR"), ("clock_basis", "VARCHAR"),
    ("weighting", "VARCHAR"), ("time_bin", "INTEGER"),
    ("time_low", "DOUBLE"), ("time_high", "DOUBLE"),
    ("d1_n", "BIGINT"), ("d10_n", "BIGINT"),
    ("d1_events", "BIGINT"), ("d10_events", "BIGINT"),
    ("paired_event_count", "BIGINT"), ("d1_error", "DOUBLE"),
    ("d10_error", "DOUBLE"), ("spread_d10_minus_d1", "DOUBLE"),
    ("suppressed", "BOOLEAN"), ("uncertainty_status", "VARCHAR"),
)


def grand_slam_name(name: str) -> str | None:
    """Use exact normalized tournament aliases, not dates or player slugs."""
    return SLAM_ALIASES.get(normalize_name(name))


def classify_grand_slam(
    archive: Mapping[str, Any], provider_name: str
) -> tuple[bool | None, str | None]:
    archived = grand_slam_name(str(archive.get("tourney_name") or ""))
    provider = grand_slam_name(provider_name)
    level = str(archive.get("tourney_level") or "").strip().upper()
    if not level:
        return None, "missing_archive_tourney_level"
    if level == "G":
        if archived is None or provider is None:
            return None, "unrecognized_grand_slam_name"
        if archived != provider:
            return None, "grand_slam_provider_archive_disagreement"
        return True, None
    if archived is not None or provider is not None:
        return None, "grand_slam_level_name_disagreement"
    return False, None


def strict_clock_evidence(timing: Mapping[str, Any]) -> tuple[bool, str]:
    """Reject fields named actual without an audited literal-source adapter.

    The frozen ESPN scoreboard and duration archive can identify a match, but
    neither source is an audited record of both actual competitive boundaries.
    Even an unfamiliar quality label cannot self-certify those semantics.
    """
    if timing.get("timing_quality") == "elapsed_thirds_from_espn_start_and_archived_duration":
        return False, "scheduled_start_and_synthetic_end"
    return False, "no_audited_literal_first_serve_final_point_source"


def load_archive(paths: list[Path]) -> list[dict[str, Any]]:
    rows = []
    required = {"tourney_id", "tourney_name", "tourney_level", "tourney_date", "match_num",
                "winner_name", "loser_name", "minutes", "round", "best_of", "score"}
    for path in paths:
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            missing = required - set(reader.fieldnames or ())
            if missing:
                raise ValueError(f"Archive schema missing {sorted(missing)}: {path}")
            for raw in reader:
                row = dict(raw)
                try:
                    row["tournament_date"] = datetime.strptime(row["tourney_date"], "%Y%m%d").date()
                    raw_minutes = float(row["minutes"] or "")
                    if not raw_minutes.is_integer() or raw_minutes <= 0:
                        continue
                    row["minutes"] = int(raw_minutes)
                except (ValueError, TypeError):
                    continue
                if any(marker in str(row["score"]).upper() for marker in ("RET", "W/O", "DEF", "ABD")):
                    continue
                rows.append(row)
    return rows


def match_archive_metadata(
    market_date: date, result: str, record: CompetitionRecord,
    archive: list[dict[str, Any]], duration_seconds: float,
) -> tuple[dict[str, Any] | None, str | None]:
    pair = tuple(item.name for item in record.competitors)
    possible = [row for row in archive
                if 0 <= (market_date-row["tournament_date"]).days <= 21
                and match_name_pair(pair, (row["winner_name"], row["loser_name"])) is not None
                and names_match(result, row["winner_name"])]
    if len(possible) > 1:
        provider_slam = grand_slam_name(record.event_name)
        named = [row for row in possible
                 if (provider_slam is not None and grand_slam_name(row["tourney_name"]) == provider_slam)
                 or names_match(record.event_name, row["tourney_name"])]
        if len(named) == 1:
            possible = named
    if len(possible) != 1:
        return None, "missing_archive_match" if not possible else "ambiguous_archive_match"
    selected = possible[0]
    if selected["minutes"]*60 != duration_seconds:
        return None, "archive_duration_frozen_clock_disagreement"
    return selected, None


def _rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    cursor = con.execute(query)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def provider_record_exclusion(evidence: Mapping[str, Any], frozen: Mapping[str, Any]) -> str | None:
    """Validate the collector contract without equating the old date to reality."""
    required_values = {
        "sport": "atp", "clock_basis": ACTUAL_CLOCK,
        "clock_status": "verified_provider_observation", "start_source_field": "actual_start_time",
        "end_source_field": "commentary[type=match].timestamp", "source_timezone": "Australia/Melbourne",
        "start_precision_seconds": 60, "end_precision_seconds": 1,
        "qualification": ACTUAL_QUALIFICATION,
        "competitive_chronology_valid": True, "terminal_is_last_logical_point": True,
        "competitive_timestamp_reversal_count": 0, "competitive_duplicate_id_count": 0,
        "competitive_conflicting_duplicate_id_count": 0, "competitive_missing_timestamp_count": 0,
    }
    if any(evidence.get(key) != value for key, value in required_values.items()):
        return "unsupported_provider_clock_contract"
    if evidence.get("exclusion_reason") is not None:
        return "collector_record_excluded"
    if frozen.get("is_grand_slam") is not True or frozen.get("grand_slam_name") != "Australian Open":
        return "provider_evidence_not_classified_australian_open"
    if (evidence.get("market_date") != frozen.get("market_date")
            or match_name_pair((frozen["participant_1"], frozen["participant_2"]),
                               (evidence["participant_1"], evidence["participant_2"])) is None
            or not names_match(frozen["result_label"], evidence["result_label"])):
        return "provider_evidence_frozen_identity_disagreement"
    start, end = evidence.get("actual_start_utc"), evidence.get("actual_end_utc")
    if (not isinstance(start, datetime) or not isinstance(end, datetime)
            or start.tzinfo is None or end.tzinfo is None or end <= start):
        return "invalid_provider_actual_boundaries"
    official_date = evidence.get("official_match_date")
    if not isinstance(official_date, date) or official_date.year != 2026:
        return "unsupported_official_tournament_year"
    # The official actual date may disagree with the frozen scheduled date.
    from zoneinfo import ZoneInfo
    if start.astimezone(ZoneInfo("Australia/Melbourne")).date() != official_date:
        return "official_actual_start_date_disagreement"
    for key in ("ao_match_id", "actual_start_literal", "terminal_point_id",
                "results_source_url", "match_source_url", "evidence_results_cache", "evidence_match_cache"):
        if not isinstance(evidence.get(key), str) or not evidence[key].strip():
            return "missing_provider_literal_evidence"
    terminal_timestamp = evidence.get("terminal_point_timestamp")
    if not isinstance(terminal_timestamp, int) or terminal_timestamp != int(end.timestamp()):
        return "terminal_literal_timestamp_disagreement"
    return None


def _load_provider_evidence(
    con: duckdb.DuckDBPyConnection, actual_path: Path | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any] | None]:
    if actual_path is None:
        return {}, None
    manifest_path = actual_path.parent/"actual_timing_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (manifest.get("status") != "complete" or not matches_artifact_fingerprint(
            manifest.get("outputs", {}).get(actual_path.name), actual_path)):
        raise ValueError("Provider actual evidence manifest/hash mismatch")
    inventory = manifest.get("source_cache", {}).get("files")
    if not isinstance(inventory, list):
        raise ValueError("Missing provider source-cache inventory")
    by_path = {item["path"]: item for item in inventory}
    con.execute(f"CREATE VIEW provider_evidence AS SELECT * FROM read_parquet('{quoted(actual_path)}')")
    require_columns(con, "provider_evidence", (
        "sport", "event_slug", "market_date", "ao_match_id", "official_match_date", "participant_1", "participant_2",
        "result_label", "actual_start_utc", "actual_end_utc", "clock_basis", "clock_status", "start_source_field",
        "end_source_field", "source_timezone", "start_precision_seconds", "end_precision_seconds", "qualification",
        "actual_start_literal", "terminal_point_id", "terminal_point_timestamp", "results_source_url", "match_source_url",
        "evidence_results_cache", "evidence_match_cache", "exclusion_reason",
        "competitive_chronology_valid", "terminal_is_last_logical_point", "competitive_timestamp_reversal_count",
        "competitive_duplicate_id_count", "competitive_conflicting_duplicate_id_count", "competitive_missing_timestamp_count",
    ), "Provider actual evidence")
    if con.execute("SELECT count(*)-count(DISTINCT event_slug),count(*)-count(DISTINCT ao_match_id) "
                   "FROM provider_evidence").fetchone() != (0, 0):
        raise ValueError("Duplicate or null provider actual evidence identities")
    if con.execute("SELECT count(*) FROM provider_evidence p ANTI JOIN timing t USING(event_slug)").fetchone()[0]:
        raise ValueError("Provider actual evidence outside frozen ATP cohort")
    evidence = _rows(con, "SELECT * FROM provider_evidence ORDER BY event_slug")
    checked = set()
    for row in evidence:
        for key in ("evidence_results_cache", "evidence_match_cache"):
            relative = row[key]
            cache_path = (actual_path.parent/relative).resolve()
            if not cache_path.is_relative_to(actual_path.parent.resolve()):
                raise ValueError("Provider evidence cache path escapes its immutable run")
            if relative in checked:
                continue
            metadata = by_path.get(relative)
            value = fingerprint(cache_path)
            if metadata is None or metadata.get("bytes") != value["bytes"] or metadata.get("sha256") != value["sha256"]:
                raise ValueError("Provider raw cache fingerprint disagreement")
            checked.add(relative)
    return {row["event_slug"]: row for row in evidence}, fingerprint(manifest_path)


def _event_rows(
    con: duckdb.DuckDBPyConnection, archives: list[dict[str, Any]], scoreboard_dir: Path,
    evidence: dict[str, dict[str, Any]],
) -> tuple[list[tuple[Any, ...]], list[Path]]:
    rows, scoreboard_paths = [], {}
    cache: dict[date, tuple[CompetitionRecord, ...]] = {}
    for event in _rows(con, "SELECT t.*,m.result_label,m.participant_1,m.participant_2 "
                       "FROM timing t JOIN matches m USING(event_slug) ORDER BY t.event_slug"):
        day = event["market_date"]
        path = scoreboard_dir/f"{day.strftime('%Y%m%d')}.json"
        if day not in cache:
            if not path.is_file():
                raise FileNotFoundError(path)
            payload = json.loads(path.read_text(encoding="utf-8"))
            cache[day] = flatten_espn_tennis(payload)
            scoreboard_paths[path] = None
        records = [record for record in cache[day]
                   if record.competition_id == str(event["game_id"])
                   and (record.grouping_name or "").casefold() == "men's singles"]
        selected, reason = None, None
        if len(records) != 1:
            reason = "missing_or_ambiguous_frozen_scoreboard_competition"
        else:
            record = records[0]
            winners = [competitor for competitor in record.competitors if competitor.winner is True]
            if (record.status_state != "post" or len(winners) != 1
                    or not names_match(event["result_label"], winners[0].name)
                    or match_name_pair((event["participant_1"], event["participant_2"]),
                                       tuple(item.name for item in record.competitors)) is None):
                reason = "frozen_provider_identity_result_disagreement"
            elif (record.scheduled_start_utc != event["actual_start_utc"]
                  or record.event_name != event["provider_event_name"]):
                reason = "frozen_provider_clock_or_tournament_disagreement"
            else:
                seconds = (event["actual_end_utc"]-event["actual_start_utc"]).total_seconds()
                selected, reason = match_archive_metadata(day, event["result_label"], record, archives, seconds)
        is_slam = None
        if selected is not None:
            is_slam, reason = classify_grand_slam(selected, event["provider_event_name"])
        strict, strict_reason = strict_clock_evidence(event)
        actual = evidence.get(event["event_slug"])
        actual_reason = "missing_official_australian_open_evidence"
        if actual is not None:
            actual_reason = provider_record_exclusion(actual, {
                **event, "is_grand_slam": is_slam,
                "grand_slam_name": grand_slam_name(event["provider_event_name"]) if is_slam else None,
            })
            if actual_reason is None:
                strict_reason = "provider_start_minute_precision_and_latency_unquantified"
        archive_values = [selected.get(key) if selected else None for key in
                          ("tourney_id", "match_num", "tourney_name", "tourney_level", "round",
                           "best_of", "score", "minutes")]
        rows.append((event["event_slug"], day, str(event["game_id"]), event["provider_event_name"],
                     *archive_values, grand_slam_name(event["provider_event_name"]) if is_slam else None,
                     is_slam, reason, event["actual_start_utc"], event["actual_end_utc"], CLOCK,
                     strict, strict_reason, actual_reason is None, actual_reason,
                     actual.get("ao_match_id") if actual else None,
                     actual.get("actual_start_utc") if actual and actual_reason is None else None,
                     actual.get("actual_end_utc") if actual and actual_reason is None else None,
                     actual.get("start_precision_seconds") if actual else None,
                     actual.get("end_precision_seconds") if actual else None,
                     actual.get("qualification") if actual else None))
    return rows, sorted(scoreboard_paths)


def summary_rows(con: duckdb.DuckDBPyConnection) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    """Summarize the small scoped relation without smoothing sparse cells."""
    profiles, tails = [], []
    for cohort in COHORTS:
        for sample, predicate in SAMPLES.items():
            con.execute("DROP VIEW IF EXISTS sample_fills")
            con.execute(f"CREATE TEMP VIEW sample_fills AS SELECT * FROM observations "
                        f"WHERE cohort='{cohort}' AND {predicate} AND live_time>=0 AND live_time<=1")
            aggregates = {
                (r["time_bin"], r["price_bin"]): r for r in _rows(con, """
                SELECT time_bin,price_bin,count(*)::BIGINT n,count(DISTINCT event_slug)::BIGINT events,
                       sum(usdc)::DOUBLE dollars,avg(price)::DOUBLE mean_price,
                       avg(won)::DOUBLE win_rate,avg(won-price)::DOUBLE calibration
                FROM sample_fills GROUP BY 1,2
                """)
            }
            equal = {
                (r["time_bin"], r["price_bin"]): r for r in _rows(con, """
                SELECT time_bin,price_bin,avg(mean_price)::DOUBLE mean_price,
                       avg(win_rate)::DOUBLE win_rate,avg(calibration)::DOUBLE calibration
                FROM (SELECT time_bin,price_bin,event_slug,avg(price) mean_price,
                             avg(won) win_rate,avg(won-price) calibration
                      FROM sample_fills GROUP BY 1,2,3) GROUP BY 1,2
                """)
            }
            paired = {r["time_bin"]: r for r in _rows(con, """
                SELECT time_bin,sum(d1_n)::BIGINT d1_n,sum(d10_n)::BIGINT d10_n,
                       count(*)::BIGINT events,avg(d1_error)::DOUBLE d1_error,
                       avg(d10_error)::DOUBLE d10_error
                FROM (SELECT time_bin,event_slug,
                      count(*) FILTER(WHERE price_bin=1) d1_n,
                      count(*) FILTER(WHERE price_bin=10) d10_n,
                      avg(won-price) FILTER(WHERE price_bin=1) d1_error,
                      avg(won-price) FILTER(WHERE price_bin=10) d10_error
                      FROM sample_fills GROUP BY 1,2 HAVING d1_n>0 AND d10_n>0)
                GROUP BY 1
                """)}
            clock = ACTUAL_CLOCK if cohort == "ao_provider_actual" else CLOCK
            for time_bin in range(1, 11):
                low, high = (time_bin-1)/10, time_bin/10
                for price_bin in range(1, 11):
                    raw = aggregates.get((time_bin, price_bin), {})
                    n, events = int(raw.get("n", 0)), int(raw.get("events", 0))
                    for weighting in ("equal_fill", "equal_event"):
                        estimate = raw if weighting == "equal_fill" else equal.get((time_bin, price_bin), {})
                        suppressed = n < MIN_CELL_N
                        means = tuple(None if suppressed else estimate[key] for key in
                                      ("mean_price", "win_rate", "calibration"))
                        profiles.append((cohort, sample, clock, weighting, time_bin, low, high,
                                         price_bin, n, events, float(raw.get("dollars", 0)), *means,
                                         suppressed, "point_estimate_only"))
                d1, d10 = aggregates.get((time_bin, 1), {}), aggregates.get((time_bin, 10), {})
                pair = paired.get(time_bin, {})
                for weighting in ("equal_fill", "paired_equal_event"):
                    if weighting == "equal_fill":
                        n1, n10 = int(d1.get("n", 0)), int(d10.get("n", 0))
                        e1, e10 = int(d1.get("events", 0)), int(d10.get("events", 0))
                        err1, err10 = d1.get("calibration"), d10.get("calibration")
                    else:
                        n1, n10 = int(pair.get("d1_n", 0)), int(pair.get("d10_n", 0))
                        e1 = e10 = int(pair.get("events", 0))
                        err1, err10 = pair.get("d1_error"), pair.get("d10_error")
                    suppressed = n1 < MIN_CELL_N or n10 < MIN_CELL_N
                    values = (None, None, None) if suppressed else (err1, err10, err10-err1)
                    tails.append((cohort, sample, clock, weighting, time_bin, low, high,
                                  n1, n10, e1, e10, int(pair.get("events", 0)), *values,
                                  suppressed, "point_estimate_only"))
    return profiles, tails


def kernel_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    """One grouped range join computes the descriptive, phase-separated curve."""
    rows = _rows(con, """
        WITH sampled AS (
          SELECT *, 'all_trades' sample_kind FROM observations WHERE price>0 AND price<1
          UNION ALL SELECT *, 'filtered_trades' sample_kind FROM observations
          WHERE price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman
        ), weighted AS (
          SELECT s.*,g.range/50.0 evaluation_time,
                 .75*(1-pow((s.live_time-g.range/50.0)/.10,2)) kernel_weight
          FROM sampled s JOIN range(51) g ON abs(s.live_time-g.range/50.0)<.10
          WHERE s.live_time>=0 AND s.live_time<=1 AND s.price_bin IN (1,10)
        )
        SELECT cohort,sample_kind,evaluation_time,price_bin,count(*)::BIGINT n,
               count(DISTINCT event_slug)::BIGINT events,
               sum(kernel_weight*(won-price))/sum(kernel_weight) calibration
        FROM weighted GROUP BY 1,2,3,4
    """)
    indexed = {(r["cohort"], r["sample_kind"], round(r["evaluation_time"], 10), r["price_bin"]): r for r in rows}
    output = []
    for cohort in COHORTS:
        for sample in SAMPLES:
            for point in range(51):
                time = point/50
                d1 = indexed.get((cohort, sample, time, 1), {})
                d10 = indexed.get((cohort, sample, time, 10), {})
                n1, n10 = int(d1.get("n", 0)), int(d10.get("n", 0))
                suppressed = n1 < MIN_CELL_N or n10 < MIN_CELL_N
                values = (None, None, None) if suppressed else (
                    d1["calibration"], d10["calibration"], d10["calibration"]-d1["calibration"])
                output.append((cohort, sample, ACTUAL_CLOCK if cohort == "ao_provider_actual" else CLOCK,
                               "equal_fill", time, .10, n1, n10, int(d1.get("events", 0)),
                               int(d10.get("events", 0)), *values, suppressed, "point_estimate_only"))
    return output


def _write_clock_comparisons(con: duckdb.DuckDBPyConnection, staging: Path) -> None:
    con.execute("""CREATE TEMP VIEW clock_pair_fills AS
        SELECT b.*, CASE WHEN b.timestamp<epoch(e.scheduled_start_utc) THEN 'pregame'
                        WHEN b.timestamp<=epoch(e.synthetic_end_utc) THEN 'live' ELSE 'post' END scheduled_phase,
                    CASE WHEN b.timestamp<epoch(e.provider_actual_start_utc) THEN 'pregame'
                        WHEN b.timestamp<=epoch(e.provider_actual_end_utc) THEN 'live' ELSE 'post' END provider_phase
        FROM buys b JOIN event_cohort e USING(event_slug) WHERE e.provider_actual_eligible
    """)
    phases, support = [], []
    for sample, predicate in {"unfiltered_exact": "true", **SAMPLES}.items():
        counts = con.execute(f"""SELECT count(*),count(DISTINCT event_slug),
                count(*) FILTER(WHERE scheduled_phase='pregame'),count(*) FILTER(WHERE scheduled_phase='live'),
                count(*) FILTER(WHERE scheduled_phase='post'),count(*) FILTER(WHERE provider_phase='pregame'),
                count(*) FILTER(WHERE provider_phase='live'),count(*) FILTER(WHERE provider_phase='post'),
                count(*) FILTER(WHERE scheduled_phase<>provider_phase)
            FROM clock_pair_fills WHERE {predicate}""").fetchone()
        if counts[0] != sum(counts[2:5]) or counts[0] != sum(counts[5:8]):
            raise ValueError("Same-cohort clock assignment reconciliation failed")
        scheduled_n = con.execute(f"SELECT count(*) FROM observations WHERE "
                                 f"cohort='ao_same_cohort_scheduled' AND {predicate}").fetchone()[0]
        actual_n = con.execute(f"SELECT count(*) FROM observations WHERE "
                              f"cohort='ao_provider_actual' AND {predicate}").fetchone()[0]
        if scheduled_n != counts[0] or actual_n != counts[0]:
            raise ValueError("Actual/scheduled comparison changed scoped fill membership")
        support.append((sample, *counts, scheduled_n, actual_n, True))
        indexed = {(row[0], row[1]): row[2:] for row in con.execute(f"""
            SELECT scheduled_phase,provider_phase,count(*)::BIGINT,count(DISTINCT event_slug)::BIGINT,
                   coalesce(sum(usdc),0)::DOUBLE FROM clock_pair_fills WHERE {predicate} GROUP BY 1,2
        """).fetchall()}
        for scheduled in ("pregame", "live", "post"):
            for actual in ("pregame", "live", "post"):
                phases.append((sample, scheduled, actual, *indexed.get((scheduled, actual), (0, 0, 0.0))))
    write_parquet(staging/"clock_comparison_support.parquet", (
        ("sample", "VARCHAR"), ("n_scoped_fills", "BIGINT"), ("n_events_with_fills", "BIGINT"),
        ("scheduled_pregame_n", "BIGINT"), ("scheduled_live_n", "BIGINT"), ("scheduled_post_n", "BIGINT"),
        ("provider_pregame_n", "BIGINT"), ("provider_live_n", "BIGINT"), ("provider_post_n", "BIGINT"),
        ("n_phase_changed", "BIGINT"), ("scheduled_comparison_n", "BIGINT"),
        ("provider_comparison_n", "BIGINT"), ("same_scoped_fill_membership", "BOOLEAN"),
    ), support, ("sample",))
    write_parquet(staging/"clock_phase_assignments.parquet", (
        ("sample", "VARCHAR"), ("scheduled_phase", "VARCHAR"), ("provider_phase", "VARCHAR"),
        ("n_fills", "BIGINT"), ("n_events", "BIGINT"), ("dollars", "DOUBLE"),
    ), phases, ("sample", "scheduled_phase", "provider_phase"))
    con.execute("""CREATE TEMP VIEW clock_offsets AS SELECT event_slug,ao_match_id,
        scheduled_start_utc,synthetic_end_utc,provider_actual_start_utc,provider_actual_end_utc,
        epoch(provider_actual_start_utc)-epoch(scheduled_start_utc) start_offset_seconds,
        epoch(provider_actual_end_utc)-epoch(synthetic_end_utc) end_offset_seconds,
        epoch(provider_actual_end_utc)-epoch(provider_actual_start_utc) provider_elapsed_seconds,
        start_precision_seconds,end_precision_seconds,provider_actual_qualification
        FROM event_cohort WHERE provider_actual_eligible""")
    con.execute(f"COPY (SELECT * FROM clock_offsets ORDER BY event_slug) "
                f"TO '{quoted(staging/'provider_clock_offsets.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
    con.execute(f"""COPY (SELECT count(*)::BIGINT n_events,
        min(start_offset_seconds) min_start_offset_seconds,median(start_offset_seconds) median_start_offset_seconds,
        avg(start_offset_seconds) mean_start_offset_seconds,max(start_offset_seconds) max_start_offset_seconds,
        quantile_cont(abs(start_offset_seconds),.9) p90_absolute_start_offset_seconds,
        min(end_offset_seconds) min_end_offset_seconds,median(end_offset_seconds) median_end_offset_seconds,
        avg(end_offset_seconds) mean_end_offset_seconds,max(end_offset_seconds) max_end_offset_seconds,
        quantile_cont(abs(end_offset_seconds),.9) p90_absolute_end_offset_seconds
        FROM clock_offsets) TO '{quoted(staging/'clock_offset_summary.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)""")


def build_audit(event_timing: str | Path, match_audit: str | Path, archive_dir: str | Path,
                scoreboard_dir: str | Path, exact_buys: str | Path, run_dir: str | Path,
                actual_evidence: str | Path | None = None) -> dict[str, Any]:
    timing_path, match_path, buys_path = [Path(p).expanduser().resolve()
                                         for p in (event_timing, match_audit, exact_buys)]
    archive_root, scoreboard_root = [Path(p).expanduser().resolve() for p in (archive_dir, scoreboard_dir)]
    actual_path = Path(actual_evidence).expanduser().resolve() if actual_evidence is not None else None
    archive_paths = sorted(archive_root.glob("*.csv"))
    if not archive_paths:
        raise FileNotFoundError(f"No frozen ATP CSVs: {archive_root}")
    inputs = (timing_path, match_path, buys_path, archive_root, scoreboard_root)
    if actual_path is not None:
        inputs += (actual_path.parent,)
    with fresh_run(run_dir, inputs) as staging:
        con = duckdb.connect()
        try:
            con.execute(f"CREATE VIEW timing AS SELECT * FROM read_parquet('{quoted(timing_path)}') WHERE sport='atp'")
            con.execute(f"CREATE VIEW matches AS SELECT * FROM read_parquet('{quoted(match_path)}') WHERE sport='atp' AND eligible")
            require_columns(con, "timing", ("event_slug", "game_id", "market_date", "provider_event_name",
                                             "actual_start_utc", "actual_end_utc", "timing_quality"), "ATP timing")
            require_columns(con, "matches", ("event_slug", "result_label", "participant_1", "participant_2"), "Match audit")
            for relation in ("timing", "matches"):
                if con.execute(f"SELECT count(*)-count(DISTINCT event_slug) FROM {relation}").fetchone()[0]:
                    raise ValueError(f"Duplicate event rows: {relation}")
            if con.execute("SELECT count(*) FROM timing FULL JOIN matches USING(event_slug) "
                           "WHERE timing.event_slug IS NULL OR matches.event_slug IS NULL").fetchone()[0]:
                raise ValueError("Accepted timing/match-audit cohort disagreement")
            if con.execute("SELECT count(*) FROM timing WHERE actual_start_utc IS NULL "
                           "OR actual_end_utc IS NULL OR actual_end_utc<=actual_start_utc").fetchone()[0]:
                raise ValueError("Invalid frozen ATP boundaries")
            if con.execute("SELECT count(*) FROM timing WHERE timing_quality IS DISTINCT FROM "
                           "'elapsed_thirds_from_espn_start_and_archived_duration'").fetchone()[0]:
                raise ValueError("Unsupported ATP clock basis; preserve literal-source semantics in a separate adapter")
            evidence, evidence_manifest = _load_provider_evidence(con, actual_path)
            events, scoreboard_paths = _event_rows(con, load_archive(archive_paths), scoreboard_root, evidence)
            write_parquet(staging/"event_cohort.parquet", PROVIDER_EVENT_SCHEMA, events, ("event_slug",))
            con.execute(f"CREATE VIEW event_cohort AS SELECT * FROM read_parquet('{quoted(staging/'event_cohort.parquet')}')")
            con.execute(f"CREATE VIEW buys AS SELECT * FROM read_parquet('{quoted(buys_path)}') WHERE sport='atp'")
            require_columns(con, "buys", ("event_slug", "timestamp", "price", "won", "usdc", "buyer_is_flagged_nonhuman",
                                           "actual_start_utc", "actual_end_utc"), "Exact buys")
            if con.execute("SELECT count(*) FROM buys b LEFT JOIN event_cohort e USING(event_slug) "
                           "WHERE e.event_slug IS NULL OR b.actual_start_utc IS DISTINCT FROM e.scheduled_start_utc "
                           "OR b.actual_end_utc IS DISTINCT FROM e.synthetic_end_utc").fetchone()[0]:
                raise ValueError("Exact-buy timing lineage disagreement")
            if con.execute('SELECT count(*) FROM buys WHERE "timestamp" IS NULL OR price IS NULL OR NOT isfinite(price) '
                           "OR won IS NULL OR won NOT IN (0,1) OR usdc IS NULL OR usdc<=0 OR NOT isfinite(usdc) "
                           "OR buyer_is_flagged_nonhuman IS NULL").fetchone()[0]:
                raise ValueError("Invalid exact ATP fills")
            con.execute("""CREATE VIEW observations AS
                SELECT c.cohort,b.event_slug,b.price,b.won::DOUBLE won,b.usdc,b.buyer_is_flagged_nonhuman,
                       (b.timestamp-epoch(CASE WHEN c.cohort='ao_provider_actual'
                           THEN e.provider_actual_start_utc ELSE e.scheduled_start_utc END))/
                       (epoch(CASE WHEN c.cohort='ao_provider_actual' THEN e.provider_actual_end_utc
                           ELSE e.synthetic_end_utc END)-epoch(CASE WHEN c.cohort='ao_provider_actual'
                           THEN e.provider_actual_start_utc ELSE e.scheduled_start_utc END)) live_time,
                       least(floor(live_time*10)::INTEGER+1,10) time_bin,
                       least(floor(b.price*10)::INTEGER+1,10) price_bin
                FROM buys b JOIN event_cohort e USING(event_slug)
                CROSS JOIN (VALUES ('all_atp'),('grand_slam'),('ao_provider_actual'),
                                    ('ao_same_cohort_scheduled')) c(cohort)
                WHERE c.cohort='all_atp' OR (c.cohort='grand_slam' AND e.is_grand_slam)
                   OR (c.cohort IN ('ao_provider_actual','ao_same_cohort_scheduled') AND e.provider_actual_eligible)
            """)
            profiles, tails = summary_rows(con)
            write_parquet(staging/"calibration_profile.parquet", PROFILE_SCHEMA, profiles,
                          ("cohort", "sample", "weighting", "time_bin", "price_bin"))
            write_parquet(staging/"tail_contrasts.parquet", TAIL_SCHEMA, tails,
                          ("cohort", "sample", "weighting", "time_bin"))
            kernel = kernel_rows(con)
            write_parquet(staging/"kernel_tail_curves.parquet", (
                ("cohort", "VARCHAR"), ("sample", "VARCHAR"), ("clock_basis", "VARCHAR"),
                ("weighting", "VARCHAR"), ("evaluation_time", "DOUBLE"), ("bandwidth", "DOUBLE"),
                ("d1_n", "BIGINT"), ("d10_n", "BIGINT"), ("d1_events", "BIGINT"), ("d10_events", "BIGINT"),
                ("d1_error", "DOUBLE"), ("d10_error", "DOUBLE"), ("spread_d10_minus_d1", "DOUBLE"),
                ("suppressed", "BOOLEAN"), ("uncertainty_status", "VARCHAR"),
            ), kernel, ("cohort", "sample", "evaluation_time"))
            _write_clock_comparisons(con, staging)
            for table, condition in (("classification_exclusions", "classification_exclusion_reason IS NOT NULL"),
                                     ("exact_firstserve_exclusions", "NOT exact_firstserve_verified"),
                                     ("provider_actual_exclusions", "NOT provider_actual_eligible")):
                con.execute(f"COPY (SELECT * FROM event_cohort WHERE {condition} ORDER BY event_slug) "
                            f"TO '{quoted(staging/(table+'.parquet'))}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            coverage = []
            for cohort in COHORTS:
                condition = {"all_atp": "true", "grand_slam": "is_grand_slam",
                             "ao_provider_actual": "provider_actual_eligible",
                             "ao_same_cohort_scheduled": "provider_actual_eligible"}[cohort]
                event_count, exact_count = con.execute(
                    f"SELECT count(*),count(*) FILTER(WHERE exact_firstserve_verified) "
                    f"FROM event_cohort WHERE {condition}").fetchone()
                for sample, predicate in SAMPLES.items():
                    counts = con.execute(f"SELECT count(*),count(*) FILTER(WHERE live_time<0),"
                                         f"count(*) FILTER(WHERE live_time>=0 AND live_time<=1),"
                                         f"count(*) FILTER(WHERE live_time>1) FROM observations "
                                         f"WHERE cohort='{cohort}' AND {predicate}").fetchone()
                    if counts[0] != sum(counts[1:]):
                        raise ValueError("Pregame/live/post fill reconciliation failed")
                    coverage.append((cohort, sample, ACTUAL_CLOCK if cohort == "ao_provider_actual" else CLOCK,
                                     event_count, *counts, exact_count))
            write_parquet(staging/"cohort_coverage.parquet", (
                ("cohort", "VARCHAR"), ("sample", "VARCHAR"), ("clock_basis", "VARCHAR"),
                ("accepted_events", "BIGINT"),
                ("n_fills", "BIGINT"), ("n_pregame", "BIGINT"),
                ("n_live", "BIGINT"), ("n_post_end", "BIGINT"),
                ("exact_firstserve_verified_events", "BIGINT"),
            ), coverage, ("cohort", "sample"))
            for name, expected in (("calibration_profile.parquet", 1600), ("tail_contrasts.parquet", 160)):
                if con.execute(f"SELECT count(*) FROM read_parquet('{quoted(staging/name)}')").fetchone()[0] != expected:
                    raise ValueError(f"Incomplete output grid: {name}")
            manifest = {
                "schema_version": 2, "stage": "atp_timing_cohort_audit_v2", "status": "complete",
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "command": [sys.executable, "-m", "analysis.diagnostics.tennis_timing_cohort",
                            "--event-timing", str(timing_path), "--match-audit", str(match_path),
                            "--archive-dir", str(archive_root), "--scoreboard-dir", str(scoreboard_root),
                            "--exact-buys", str(buys_path), "--run-dir", str(Path(run_dir).expanduser().resolve())]
                           + ([] if actual_path is None else ["--actual-evidence", str(actual_path)]),
                "definitions": {
                    "calibration": "eventual bought-contract outcome minus purchase price",
                    "clock_basis": CLOCK, "provider_actual_clock": ACTUAL_CLOCK,
                    "provider_actual_qualification": ACTUAL_QUALIFICATION,
                    "provider_actual_quality_gate": "all competitive point IDs unique, logical point order and timestamps nondecreasing, terminal point last; exact source chronology gate required",
                    "exact_firstserve_verification": "unavailable; no second-exact first serve or quantified provider latency",
                    "same_cohort_comparison": "ao_provider_actual and ao_same_cohort_scheduled use identical events and source fills before phase assignment",
                    "grand_slam": "unique archived completed match with tourney_level G and agreed tournament identity",
                    "bins": "10 fixed live-time bins and 10 fixed bought-price bins; endpoints 0 and 1 included",
                    "suppression": "fewer than 500 fills in a price cell or either tail",
                    "paired_equal_event": "average event-specific D10 minus D1 among events with both tails in the time bin",
                    "kernel": "Epanechnikov, live h=.10, 51 grid points; only 0<=T<=1 fills; 500 positive-weight fills in each tail",
                    "uncertainty": "point estimates only; no inferential claim", "resolved_market_censoring": "inherited",
                },
                "counts": {"accepted_atp_events": len(events),
                           "grand_slam_events": sum(row[13] is True for row in events),
                           "exact_firstserve_verified_events": sum(row[18] is True for row in events),
                           "provider_actual_events": sum(row[20] is True for row in events),
                           "classification_exclusions": dict(Counter(row[14] for row in events if row[14])),
                           "exact_firstserve_exclusions": dict(Counter(row[19] for row in events)),
                           "provider_actual_exclusions": dict(Counter(row[21] for row in events if row[21])),
                           "calibration_profile": len(profiles), "tail_contrasts": len(tails),
                           "kernel_tail_curves": len(kernel)},
                "inputs": {"event_timing": fingerprint(timing_path), "match_audit": fingerprint(match_path),
                           "exact_buys": fingerprint(buys_path),
                           "archive_csvs": [fingerprint(p) for p in archive_paths],
                           "scoreboards": [fingerprint(p) for p in scoreboard_paths],
                           "actual_evidence": fingerprint(actual_path) if actual_path else None,
                           "actual_evidence_manifest": evidence_manifest},
                "code": fingerprint(Path(__file__)),
                "environment": {"duckdb": duckdb.__version__, "python": sys.version},
                "outputs": {p.name: artifact_fingerprint(p) for p in sorted(staging.glob("*.parquet"))},
            }
            write_json(staging/"manifest.json", manifest)
        finally:
            con.close()
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("event-timing", "match-audit", "archive-dir", "scoreboard-dir", "exact-buys", "run-dir"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--actual-evidence", help="Accepted official AO actual_timing.parquet with frozen manifest/raw cache")
    args = parser.parse_args()
    result = build_audit(**vars(args))
    print(json.dumps(result["counts"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
