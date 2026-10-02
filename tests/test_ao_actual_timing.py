from __future__ import annotations

import copy
import json
from datetime import date, datetime, timezone

import duckdb
import pytest

from analysis.diagnostics.collect_ao_actual_timing import (
    BASE_URL,
    CLOCK_BASIS,
    QUALIFICATION,
    TIMEZONE_EVIDENCE_URL,
    TimingError,
    build_collection,
    chronology_summary,
    result_records,
    select_result,
    verify_clock,
)


def _fixture():
    frozen = {
        "sport": "atp", "event_slug": "ao-final", "market_date": date(2026, 1, 18),
        "participant_1": "Alcaraz", "participant_2": "Djokovic", "result_label": "Alcaraz",
        "provider_participants": "Carlos Alcaraz | Novak Djokovic",
        "provider_start_utc": datetime(2026, 1, 20, 8, 30, tzinfo=timezone.utc),
        "provider_event_name": "Australian Open", "eligible": True,
    }
    scores = [
        [{"set": i, "game": str(a), "winner": a > b} for i, (a, b) in enumerate(((2, 6), (6, 2), (6, 3), (7, 5)), 1)],
        [{"set": i, "game": str(b), "winner": b > a} for i, (a, b) in enumerate(((2, 6), (6, 2), (6, 3), (7, 5)), 1)],
    ]
    result = {
        "date": "2026-02-01", "actual_start_time": "19:45", "match_id": "MS701",
        "match_state": "Complete", "match_status": {"code": "C"}, "team_substituted": False,
        "duration": "3:02", "event_uuid": "men", "uuid": "258364",
        "teams": [{"team_id": "a", "status": "Winner", "score": scores[0]},
                  {"team_id": "b", "score": scores[1]}],
    }
    payload = {
        # Mutable current branding must not override the versioned year and row identities.
        "tournament": {"name": "AusOpen 2027"}, "year": {"year": "2026"}, "matches": [result],
        "teams": [{"uuid": "a", "players": ["pa"]}, {"uuid": "b", "players": ["pb"]}],
        "players": [{"uuid": "pa", "full_name": "Carlos Alcaraz", "gender": "M"},
                    {"uuid": "pb", "full_name": "Novak Djokovic", "gender": "M"}],
        "events": [{"uuid": "men", "name": "Men's Singles"}],
    }
    selected = result_records(payload, 2026, "source_cache/results/day15.json", "result-url")[0]
    detail = copy.deepcopy(result)
    detail["event"] = {"event_name": "Men's Singles", "title": "2026 Men's Singles", "tournament_period": "Main Draw"}
    detail["teams"][0]["players"] = [payload["players"][0]]
    detail["teams"][1]["players"] = [payload["players"][1]]
    detail["commentary"] = [
        {"id": "MS701-004-012-005", "timestamp": 1769946482, "type": "match", "set": 4,
         "winner": 1, "score": "Game", "games_score": "7 - 5", "is_game_complete": True},
        {"id": "MS701-001-001-001", "timestamp": 1769935581, "type": "point", "set": 1,
         "winner": 2, "score": "15 - 0", "games_score": "0 - 0", "is_game_complete": False},
        {"id": "MS701-001-001-000", "timestamp": None, "type": "serve", "set": 1,
         "score": "", "games_score": "0 - 0", "is_game_complete": False},
    ]
    return frozen, payload, selected, detail


def test_literal_actual_start_and_terminal_point_are_independent_of_duration_and_old_date():
    frozen, _, result, detail = _fixture()
    detail["duration"] = "99:99"
    observed = verify_clock(frozen, result, detail, 2026, True)
    assert observed["actual_start_utc"] == datetime(2026, 2, 1, 8, 45, tzinfo=timezone.utc)
    assert observed["actual_end_utc"] == datetime(2026, 2, 1, 11, 48, 2, tzinfo=timezone.utc)
    assert observed["first_completed_point_utc"] == datetime(2026, 2, 1, 8, 46, 21, tzinfo=timezone.utc)
    assert observed["first_point_start_delta_seconds"] == 81
    assert observed["start_precision_seconds"] == 60 and observed["end_precision_seconds"] == 1
    assert observed["official_minus_scheduled_date_days"] == 12
    assert observed["official_minus_market_date_days"] == 14
    assert observed["qualification"] == QUALIFICATION
    assert select_result(frozen, [result]) == result


def test_start_delay_is_saved_diagnostic_not_selected_out_and_end_can_cross_midnight():
    frozen, _, result, detail = _fixture()
    result["actual_start_time"] = detail["actual_start_time"] = "21:22"
    detail["commentary"][1]["timestamp"] = int(datetime(2026, 2, 1, 10, 32, tzinfo=timezone.utc).timestamp())
    detail["commentary"][0]["timestamp"] = int(datetime(2026, 2, 1, 14, 31, 48, tzinfo=timezone.utc).timestamp())
    observed = verify_clock(frozen, result, detail, 2026, True)
    assert observed["first_point_near_start"] is False
    assert observed["first_point_start_delta_seconds"] == 600
    assert observed["actual_end_utc"].hour == 14


@pytest.mark.parametrize(("mutation", "reason"), [
    (lambda d: d.update(actual_start_time=None), "missing_or_invalid_actual_start"),
    (lambda d: d.update(actual_start_time="19:46"), "results_detail_actual_start_conflict"),
    (lambda d: d.update(date="2027-02-01"), "official_year_or_date_conflict"),
    (lambda d: d.update(match_id="MS601"), "detail_match_id_conflict"),
    (lambda d: d.update(match_state="Retired"), "noncomplete_or_irregular_detail"),
    (lambda d: d["teams"][1]["players"][0].update(full_name="Jannik Sinner"), "detail_player_identity_conflict"),
    (lambda d: d["commentary"][0].update(score="10 - 8"), "unknown_terminal_score_type"),
    (lambda d: d["commentary"][0].update(winner=2), "terminal_result_conflict"),
    (lambda d: d["commentary"][0].update(games_score="5 - 7"), "terminal_final_score_conflict"),
    (lambda d: d["commentary"][0].update(timestamp=None), "invalid_competitive_timestamp"),
    (lambda d: d["commentary"].append(copy.deepcopy(d["commentary"][0])), "missing_or_ambiguous_terminal_point"),
    (lambda d: d["commentary"].append({"id": "MS701-004-012-004", "type": "point", "timestamp": 1769946483}), "terminal_not_last_competitive_timestamp"),
    (lambda d: d["commentary"][1].update(timestamp=None), "missing_first_completed_point"),
])
def test_identity_and_clock_conflicts_fail_closed(mutation, reason):
    frozen, _, result, detail = _fixture()
    mutation(detail)
    with pytest.raises(TimingError, match=reason):
        verify_clock(frozen, result, detail, 2026, True)


def test_timezone_missing_ambiguous_pair_or_wrong_winner_rejected():
    frozen, payload, result, detail = _fixture()
    with pytest.raises(TimingError, match="missing_verified_source_timezone"):
        verify_clock(frozen, result, detail, 2026, False)
    with pytest.raises(TimingError, match="ambiguous_official_pair_match"):
        select_result(frozen, [result, copy.deepcopy(result)])
    frozen["result_label"] = "Djokovic"
    with pytest.raises(TimingError, match="official_frozen_winner_conflict"):
        select_result(frozen, [result])
    payload["year"]["year"] = "2027"
    with pytest.raises(TimingError, match="results_year_conflict"):
        result_records(payload, 2026, "cache", "url")


def test_full_chronology_rejects_internal_reversals_even_when_extrema_pass():
    frozen, _, result, detail = _fixture()
    detail["commentary"].extend([
        {"id": "MS701-002-001-001", "timestamp": 1769940000, "type": "point"},
        {"id": "MS701-002-001-002", "timestamp": 1769939000, "type": "point"},
    ])
    summary = chronology_summary(detail)
    assert summary["competitive_chronology_valid"] is False
    assert summary["competitive_timestamp_reversal_count"] == 1
    assert summary["competitive_max_reversal_seconds"] == 1000
    assert summary["terminal_is_last_logical_point"] is True
    with pytest.raises(TimingError, match="internal_competitive_timestamp_reversal"):
        verify_clock(frozen, result, detail, 2026, True)


def test_full_chronology_rejects_duplicate_ids_and_terminal_logical_conflict():
    frozen, _, result, detail = _fixture()
    duplicate = copy.deepcopy(detail["commentary"][1])
    detail["commentary"].append(duplicate)
    summary = chronology_summary(detail)
    assert summary["competitive_duplicate_id_count"] == 1
    assert summary["competitive_conflicting_duplicate_id_count"] == 0
    assert summary["competitive_chronology_valid"] is False
    detail["commentary"][-1]["timestamp"] += 1
    assert chronology_summary(detail)["competitive_conflicting_duplicate_id_count"] == 1
    detail["commentary"].pop()
    detail["commentary"].append({"id": "MS701-005-001-001", "timestamp": 1769946481, "type": "point"})
    assert chronology_summary(detail)["terminal_is_last_logical_point"] is False
    with pytest.raises(TimingError, match="terminal_not_last_logical_point"):
        verify_clock(frozen, result, detail, 2026, True)


def test_numeric_point_order_accepts_equal_timestamps_and_ignores_untimed_serve():
    _, _, _, detail = _fixture()
    detail["commentary"].extend([
        {"id": "MS701-002-001-001", "timestamp": 1769940000, "type": "point"},
        {"id": "MS701-002-001-002", "timestamp": 1769940000, "type": "game"},
    ])
    summary = chronology_summary(detail)
    assert summary["competitive_chronology_valid"] is True
    assert summary["competitive_timestamp_reversal_count"] == 0
    assert summary["competitive_missing_timestamp_count"] == 0


def _audit_file(tmp_path, frozen):
    path = tmp_path/"match_audit.parquet"
    con = duckdb.connect()
    try:
        con.execute("""CREATE TABLE frozen(sport VARCHAR,event_slug VARCHAR,market_date DATE,
            participant_1 VARCHAR,participant_2 VARCHAR,result_label VARCHAR,
            provider_participants VARCHAR,provider_start_utc TIMESTAMPTZ,
            provider_event_name VARCHAR,eligible BOOLEAN)""")
        con.execute("INSERT INTO frozen VALUES (?,?,?,?,?,?,?,?,?,?)", list(frozen.values()))
        con.execute("COPY frozen TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        con.close()
    return path


def test_bounded_collection_raw_cache_fingerprints_and_immutable_offline_replay(tmp_path):
    frozen, payload, _, detail = _fixture()
    source = _audit_file(tmp_path, frozen)
    calls = []

    def fetch(url):
        calls.append(url)
        if url == TIMEZONE_EVIDENCE_URL:
            return b'"Australia/Melbourne";a.utc(1e3*e)'
        if url == BASE_URL + "/match-centre/MS701":
            return json.dumps(detail).encode()
        if url.endswith("/day/15/results"):
            return json.dumps(payload).encode()
        return json.dumps({"year": {"year": "2026"}, "matches": [], "players": [], "teams": [], "events": []}).encode()

    run = tmp_path/"source_run"
    manifest = build_collection(source, run, fetch=fetch)
    assert len(calls) == 17  # one official timezone client, 15 result days, one matched detail
    assert manifest["counts"]["actual_timing_events"] == 1
    assert manifest["counts"]["excluded_events"] == 0
    assert manifest["collection_complete"] is True
    assert manifest["outputs"]["actual_timing.parquet"]["sha256"]
    assert len(manifest["source_cache"]["files"]) == 17
    assert json.loads((run/"source_cache/match_centre/MS701.json").read_text()) == detail
    con = duckdb.connect()
    try:
        assert con.execute("SELECT event_slug,clock_basis,exclusion_reason FROM read_parquet(?)",
                           [str(run/"actual_timing.parquet")]).fetchall() == [("ao-final", CLOCK_BASIS, None)]
    finally:
        con.close()
    with pytest.raises(FileExistsError, match="Immutable run already exists"):
        build_collection(source, run, fetch=fetch)
    replay = build_collection(source, tmp_path/"replay", cache_dir=run/"source_cache",
                              fetch=lambda _: pytest.fail("Offline replay called network"))
    assert replay["counts"] == manifest["counts"]
    assert replay["outputs"]["actual_timing.parquet"]["sha256"] == manifest["outputs"]["actual_timing.parquet"]["sha256"]


def test_failed_results_day_preserves_raw_and_excludes_all_candidates(tmp_path):
    frozen, payload, _, detail = _fixture()
    source = _audit_file(tmp_path, frozen)

    def fetch(url):
        if url == TIMEZONE_EVIDENCE_URL:
            return b'"Australia/Melbourne";a.utc(1e3*e)'
        if url.endswith("/day/3/results"):
            raise OSError("Unavailable")
        if url.endswith("/day/15/results"):
            return json.dumps(payload).encode()
        return json.dumps({"year": {"year": "2026"}, "matches": []}).encode()

    run = tmp_path/"partial"
    manifest = build_collection(source, run, fetch=fetch)
    assert manifest["collection_complete"] is False
    assert manifest["counts"]["actual_timing_events"] == 0
    assert manifest["counts"]["exclusion_reasons"] == {"incomplete_or_invalid_results_collection": 1}
    assert (run/"source_cache/results/day15.json").exists()
    assert not (run/"source_cache/match_centre/MS701.json").exists()
