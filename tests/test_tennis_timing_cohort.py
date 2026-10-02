from __future__ import annotations

import json
from datetime import date, datetime, timezone

import duckdb
import pytest

from analysis.diagnostics.tennis_timing_cohort import (
    CLOCK,
    ACTUAL_CLOCK,
    ACTUAL_QUALIFICATION,
    build_audit,
    classify_grand_slam,
    grand_slam_name,
    kernel_rows,
    match_archive_metadata,
    provider_record_exclusion,
    strict_clock_evidence,
    summary_rows,
)
from analysis.multisport_game_dynamics.provider_extractors import CompetitorRecord, CompetitionRecord
from analysis.sports_game_dynamics.artifacts import artifact_fingerprint, fingerprint


def test_grand_slam_requires_level_and_agreed_tournament_identity() -> None:
    assert classify_grand_slam({"tourney_level": "G", "tourney_name": "Roland Garros"}, "French Open") == (True, None)
    assert grand_slam_name("US Open") == "US Open"
    assert grand_slam_name("Australian Open") == "Australian Open"
    assert grand_slam_name("Wimbledon") == "Wimbledon"
    assert classify_grand_slam({"tourney_level": "A", "tourney_name": "Auckland"}, "ASB Classic") == (False, None)
    assert classify_grand_slam({"tourney_level": "G", "tourney_name": "Australian Open"}, "Roland Garros") == (
        None, "grand_slam_provider_archive_disagreement")
    assert classify_grand_slam({"tourney_level": "A", "tourney_name": "Australian Open"}, "Australian Open") == (
        None, "grand_slam_level_name_disagreement")
    assert classify_grand_slam({"tourney_name": "Australian Open"}, "Australian Open") == (
        None, "missing_archive_tourney_level")
    assert grand_slam_name("Australian Open qualifying") is None


def test_columns_named_actual_and_clean_price_proxy_cannot_verify_clock() -> None:
    timing = {"actual_start_utc": "2026-01-20T00:00:00Z", "actual_end_utc": "2026-01-20T02:00:00Z",
              "clock_clean_30m": True,
              "timing_quality": "elapsed_thirds_from_espn_start_and_archived_duration"}
    assert strict_clock_evidence(timing) == (False, "scheduled_start_and_synthetic_end")
    assert strict_clock_evidence({**timing, "timing_quality": "literal_actual_timestamps"}) == (
        False, "no_audited_literal_first_serve_final_point_source")


def test_archive_match_requires_unique_pair_result_and_frozen_duration() -> None:
    record = CompetitionRecord(
        event_id="ao", competition_id="m1", event_name="Australian Open",
        scheduled_start_utc=datetime(2026, 1, 20, tzinfo=timezone.utc),
        competitors=(CompetitorRecord("1", "Alpha One", None, True), CompetitorRecord("2", "Beta Two", None, False)),
        status_state="post", status_detail="Final", grouping_name="Men's Singles")
    row = {"tournament_date": date(2026, 1, 19), "winner_name": "Alpha One", "loser_name": "Beta Two",
           "minutes": 120, "tourney_name": "Australian Open", "tourney_level": "G"}
    assert match_archive_metadata(date(2026, 1, 20), "Alpha One", record, [row], 7200) == (row, None)
    assert match_archive_metadata(date(2026, 1, 20), "Beta Two", record, [row], 7200)[1] == "missing_archive_match"
    assert match_archive_metadata(date(2026, 1, 20), "Alpha One", record, [row, row], 7200)[1] == "ambiguous_archive_match"
    assert match_archive_metadata(date(2026, 1, 20), "Alpha One", record, [row], 7260)[1] == "archive_duration_frozen_clock_disagreement"


def test_paired_equal_event_contrast_and_tail_suppression() -> None:
    con = duckdb.connect()
    try:
        con.execute("""CREATE TABLE observations(cohort VARCHAR,event_slug VARCHAR,price DOUBLE,won DOUBLE,
            usdc DOUBLE,buyer_is_flagged_nonhuman BOOLEAN,live_time DOUBLE,time_bin INTEGER,price_bin INTEGER)""")
        for event, price, won, count in (("a", .05, 1, 900), ("b", .05, 0, 100),
                                        ("a", .95, 1, 100), ("b", .95, 0, 900)):
            con.execute("INSERT INTO observations SELECT 'all_atp',?,?,?,1,false,.05,1,"
                        "least(floor(?*10)::INTEGER+1,10) FROM range(?)", [event, price, won, price, count])
        # A cell at the next time has 500 longshots and only 499 favorites.
        for price, count in ((.05, 500), (.95, 499)):
            con.execute("INSERT INTO observations SELECT 'all_atp','c',?,1,1,false,.15,2,"
                        "least(floor(?*10)::INTEGER+1,10) FROM range(?)", [price, price, count])
        profiles, tails = summary_rows(con)
    finally:
        con.close()
    fill = next(row for row in tails if row[0] == "all_atp" and row[1] == "filtered_trades"
                and row[3] == "equal_fill" and row[4] == 1)
    paired = next(row for row in tails if row[0] == "all_atp" and row[1] == "filtered_trades"
                  and row[3] == "paired_equal_event" and row[4] == 1)
    assert fill[14] == pytest.approx(-1.7)
    assert paired[14] == pytest.approx(-.9)
    assert paired[11] == 2
    sparse = next(row for row in tails if row[0] == "all_atp" and row[1] == "filtered_trades"
                  and row[3] == "equal_fill" and row[4] == 2)
    assert sparse[7:9] == (500, 499)
    assert sparse[12:16] == (None, None, None, True)
    strict = [row for row in profiles if row[0] == "ao_provider_actual"]
    assert len(profiles) == 1600 and len(tails) == 160
    assert all(row[8:10] == (0, 0) and row[11:15] == (None, None, None, True) for row in strict)


def _frozen_fixture(tmp_path):
    archive_dir, scoreboard_dir = tmp_path/"archive", tmp_path/"scoreboards"
    archive_dir.mkdir(); scoreboard_dir.mkdir()
    (archive_dir/"2026.csv").write_text(
        "tourney_id,tourney_name,tourney_level,tourney_date,match_num,winner_name,loser_name,minutes,round,best_of,score\n"
        "2026-580,Australian Open,G,20260119,1,Alpha One,Beta Two,120,R128,5,6-4 6-4 6-4\n", encoding="utf-8")
    (scoreboard_dir/"20260120.json").write_text(json.dumps({"events": [{
        "id": "ao", "name": "Australian Open", "groupings": [{"grouping": {"name": "Men's Singles"},
        "competitions": [{"id": "m1", "startDate": "2026-01-20T00:00:00Z",
                          "status": {"type": {"state": "post", "detail": "Final"}},
                          "competitors": [{"athlete": {"id": "1", "displayName": "Alpha One"}, "winner": True},
                                          {"athlete": {"id": "2", "displayName": "Beta Two"}, "winner": False}]}]}]}]}),
                                                      encoding="utf-8")
    con = duckdb.connect()
    try:
        con.execute("""CREATE TABLE timing AS SELECT 'atp' sport,'e1' event_slug,'m1' game_id,
            DATE '2026-01-20' market_date,'Australian Open' provider_event_name,
            TIMESTAMPTZ '2026-01-20 00:00:00+00' actual_start_utc,
            TIMESTAMPTZ '2026-01-20 02:00:00+00' actual_end_utc,
            'elapsed_thirds_from_espn_start_and_archived_duration' timing_quality""")
        con.execute("""CREATE TABLE matches AS SELECT 'atp' sport,'e1' event_slug,true eligible,
            'Alpha One' participant_1,'Beta Two' participant_2,'Alpha One' result_label""")
        con.execute("""CREATE TABLE buys AS SELECT t.*,epoch(t.actual_start_utc)::BIGINT AS "timestamp",
            .05::DOUBLE price,false won,1::DOUBLE usdc,false buyer_is_flagged_nonhuman
            FROM timing t CROSS JOIN range(500)
            UNION ALL SELECT t.*,epoch(t.actual_end_utc)::BIGINT,.95::DOUBLE,true,1::DOUBLE,false
            FROM timing t CROSS JOIN range(500)""")
        for name in ("timing", "matches", "buys"):
            con.execute(f"COPY {name} TO '{tmp_path/(name+'.parquet')}' (FORMAT PARQUET)")
    finally:
        con.close()
    return tmp_path/"timing.parquet", tmp_path/"matches.parquet", archive_dir, scoreboard_dir, tmp_path/"buys.parquet"


def test_immutable_build_publishes_empty_strict_clock_and_exact_boundaries(tmp_path) -> None:
    inputs = _frozen_fixture(tmp_path)
    run = tmp_path/"audit"
    result = build_audit(*inputs, run)
    assert result["counts"]["accepted_atp_events"] == 1
    assert result["counts"]["grand_slam_events"] == 1
    assert result["counts"]["exact_firstserve_verified_events"] == 0
    assert result["counts"]["provider_actual_events"] == 0
    con = duckdb.connect()
    try:
        coverage = con.execute("SELECT accepted_events,n_fills,n_live FROM read_parquet(?) "
                               "WHERE cohort='grand_slam' AND sample='filtered_trades'", [str(run/"cohort_coverage.parquet")]).fetchone()
        assert coverage == (1, 1000, 1000)
        observed = con.execute("SELECT time_bin,price_bin,n_fills,mean_calibration FROM read_parquet(?) "
                               "WHERE cohort='grand_slam' AND sample='filtered_trades' AND weighting='equal_fill' "
                               "AND n_fills>0 ORDER BY time_bin", [str(run/"calibration_profile.parquet")]).fetchall()
        assert observed[0][:3] == (1, 1, 500)
        assert observed[1][:3] == (10, 10, 500)
        assert observed[0][3] == pytest.approx(-.05)
        assert observed[1][3] == pytest.approx(.05)
        assert con.execute("SELECT DISTINCT clock_basis FROM read_parquet(?)", [str(run/"event_cohort.parquet")]).fetchall() == [(CLOCK,)]
    finally:
        con.close()
    with pytest.raises(FileExistsError, match="Immutable run already exists"):
        build_audit(*inputs, run)


def test_frozen_exact_clock_disagreement_prevents_publication(tmp_path) -> None:
    inputs = _frozen_fixture(tmp_path)
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE bad AS SELECT * REPLACE(actual_end_utc+INTERVAL 1 SECOND AS actual_end_utc) "
                    "FROM read_parquet(?)", [str(inputs[-1])])
        con.execute(f"COPY bad TO '{tmp_path/'bad_buys.parquet'}' (FORMAT PARQUET)")
    finally:
        con.close()
    run = tmp_path/"rejected"
    with pytest.raises(ValueError, match="timing lineage"):
        build_audit(*inputs[:-1], tmp_path/"bad_buys.parquet", run)
    assert not run.exists()


def _provider_fixture(tmp_path):
    root = tmp_path/"provider_evidence"
    cache = root/"source_cache"
    cache.mkdir(parents=True)
    for name in ("results.json", "match.json"):
        (cache/name).write_text("{}", encoding="utf-8")
    accepted = {
        "sport": "atp", "event_slug": "e1", "market_date": date(2026, 1, 20), "ao_match_id": "MS101",
        "official_match_date": date(2026, 1, 20), "participant_1": "Alpha One", "participant_2": "Beta Two",
        "result_label": "Alpha One", "actual_start_utc": datetime(2026, 1, 20, 0, 1, tzinfo=timezone.utc),
        "actual_end_utc": datetime(2026, 1, 20, 2, 1, tzinfo=timezone.utc), "clock_basis": ACTUAL_CLOCK,
        "clock_status": "verified_provider_observation", "start_source_field": "actual_start_time",
        "end_source_field": "commentary[type=match].timestamp", "source_timezone": "Australia/Melbourne",
        "start_precision_seconds": 60, "end_precision_seconds": 1, "qualification": ACTUAL_QUALIFICATION,
        "actual_start_literal": "11:01", "terminal_point_id": "point99",
        "terminal_point_timestamp": int(datetime(2026, 1, 20, 2, 1, tzinfo=timezone.utc).timestamp()),
        "results_source_url": "https://example.invalid/official/results", "match_source_url": "https://example.invalid/official/match",
        "evidence_results_cache": "source_cache/results.json", "evidence_match_cache": "source_cache/match.json",
        "exclusion_reason": None,
        "competitive_chronology_valid": True, "terminal_is_last_logical_point": True,
        "competitive_timestamp_reversal_count": 0, "competitive_duplicate_id_count": 0,
        "competitive_conflicting_duplicate_id_count": 0, "competitive_missing_timestamp_count": 0,
    }
    con = duckdb.connect()
    try:
        columns = []
        for key, value in accepted.items():
            kind = "TIMESTAMPTZ" if isinstance(value, datetime) else "DATE" if isinstance(value, date) else "BOOLEAN" if isinstance(value, bool) else "BIGINT" if isinstance(value, int) else "VARCHAR"
            columns.append(f"{key} {kind}")
        con.execute("CREATE TABLE evidence("+",".join(columns)+")")
        con.execute("INSERT INTO evidence VALUES ("+",".join("?" for _ in accepted)+")", list(accepted.values()))
        path = root/"actual_timing.parquet"
        con.execute(f"COPY evidence TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()
    manifest = {"status": "complete", "outputs": {path.name: artifact_fingerprint(path)},
                "source_cache": {"files": [{**fingerprint(cache/name), "path": f"source_cache/{name}"}
                                           for name in ("results.json", "match.json")]}}
    (root/"actual_timing_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return path, accepted


def test_provider_clock_contract_preserves_precision_and_rejects_bad_identity(tmp_path) -> None:
    _, accepted = _provider_fixture(tmp_path)
    frozen = {"market_date": date(2026, 1, 20), "participant_1": "Alpha One", "participant_2": "Beta Two",
              "result_label": "Alpha One", "is_grand_slam": True, "grand_slam_name": "Australian Open"}
    assert provider_record_exclusion(accepted, frozen) is None
    assert provider_record_exclusion({**accepted, "start_precision_seconds": 1}, frozen) == "unsupported_provider_clock_contract"
    assert provider_record_exclusion({**accepted, "competitive_chronology_valid": False}, frozen) == "unsupported_provider_clock_contract"
    assert provider_record_exclusion({**accepted, "competitive_timestamp_reversal_count": 1}, frozen) == "unsupported_provider_clock_contract"
    assert provider_record_exclusion({**accepted, "participant_2": "Gamma Three"}, frozen) == "provider_evidence_frozen_identity_disagreement"
    assert provider_record_exclusion({**accepted, "terminal_point_timestamp": 0}, frozen) == "terminal_literal_timestamp_disagreement"
    assert provider_record_exclusion({**accepted, "actual_end_utc": accepted["actual_start_utc"]}, frozen) == "invalid_provider_actual_boundaries"
    # A wrong old scheduled date is precisely what the new clock can correct.
    changed_old_date = {**frozen, "scheduled_start_utc": datetime(2026, 1, 19, tzinfo=timezone.utc)}
    assert provider_record_exclusion(accepted, changed_old_date) is None


def test_provider_actual_and_scheduled_comparison_share_scope_and_reassign_phase(tmp_path) -> None:
    inputs = _frozen_fixture(tmp_path)
    evidence, _ = _provider_fixture(tmp_path)
    run = tmp_path/"with_actual"
    result = build_audit(*inputs, run, actual_evidence=evidence)
    assert result["counts"]["provider_actual_events"] == 1
    assert result["counts"]["exact_firstserve_verified_events"] == 0
    assert result["counts"]["kernel_tail_curves"] == 408
    con = duckdb.connect()
    try:
        rows = con.execute("SELECT cohort,accepted_events,n_fills,n_pregame,n_live,n_post_end," 
                           "exact_firstserve_verified_events FROM read_parquet(?) "
                           "WHERE sample='all_trades' AND cohort LIKE 'ao_%' ORDER BY cohort",
                           [str(run/"cohort_coverage.parquet")]).fetchall()
        assert rows == [("ao_provider_actual", 1, 1000, 500, 500, 0, 0),
                        ("ao_same_cohort_scheduled", 1, 1000, 0, 1000, 0, 0)]
        paired = con.execute("SELECT n_scoped_fills,n_phase_changed,scheduled_comparison_n," 
                             "provider_comparison_n,same_scoped_fill_membership FROM read_parquet(?) "
                             "WHERE sample='unfiltered_exact'", [str(run/"clock_comparison_support.parquet")]).fetchone()
        assert paired == (1000, 500, 1000, 1000, True)
        offsets = con.execute("SELECT median_start_offset_seconds,median_end_offset_seconds "
                              "FROM read_parquet(?)", [str(run/"clock_offset_summary.parquet")]).fetchone()
        assert offsets == (60., 60.)
    finally:
        con.close()


def test_provider_cache_tampering_and_duplicate_ids_prevent_publication(tmp_path) -> None:
    inputs = _frozen_fixture(tmp_path)
    evidence, _ = _provider_fixture(tmp_path)
    cache = evidence.parent/"source_cache/match.json"
    cache.write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="raw cache fingerprint"):
        build_audit(*inputs, tmp_path/"tampered", actual_evidence=evidence)
    cache.write_text("{}", encoding="utf-8")
    con = duckdb.connect()
    try:
        duplicated = evidence.parent/"duplicates.parquet"
        con.execute(f"COPY (SELECT * FROM read_parquet('{evidence}') UNION ALL "
                    f"SELECT * FROM read_parquet('{evidence}')) TO '{duplicated}' (FORMAT PARQUET)")
    finally:
        con.close()
    manifest_path = evidence.parent/"actual_timing_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["outputs"][duplicated.name] = artifact_fingerprint(duplicated)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="Duplicate or null"):
        build_audit(*inputs, tmp_path/"duplicates", actual_evidence=duplicated)
    assert not (tmp_path/"tampered").exists() and not (tmp_path/"duplicates").exists()


def test_live_kernel_cannot_borrow_pregame_fills() -> None:
    con = duckdb.connect()
    try:
        con.execute("""CREATE TABLE observations AS SELECT 'all_atp' cohort,'e1' event_slug,
            .05::DOUBLE price,0::DOUBLE won,1::DOUBLE usdc,false buyer_is_flagged_nonhuman,
            -.01::DOUBLE live_time,0 time_bin,1 price_bin FROM range(1000)
            UNION ALL SELECT 'all_atp','e1',.95::DOUBLE,1::DOUBLE,1::DOUBLE,false,0::DOUBLE,1,10 FROM range(500)
            UNION ALL SELECT 'all_atp','e1',.05::DOUBLE,0::DOUBLE,1::DOUBLE,false,.05::DOUBLE,1,1 FROM range(499)""")
        rows = kernel_rows(con)
    finally:
        con.close()
    endpoint = next(row for row in rows if row[0] == "all_atp" and row[1] == "all_trades" and row[4] == 0)
    assert endpoint[6:8] == (499, 500)
    assert endpoint[10:14] == (None, None, None, True)
