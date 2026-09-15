from __future__ import annotations

from datetime import date, datetime, timezone

import duckdb

from analysis.multisport_game_dynamics.build_candidates import build_candidates
from analysis.multisport_game_dynamics.build_timing import (
    _candidate_match,
    _phase_rows,
    _scoreboard_url,
)
from analysis.multisport_game_dynamics.provider_extractors import CompetitorRecord, CompetitionRecord


UTC = timezone.utc


def _write(con: duckdb.DuckDBPyConnection, path: str, query: str) -> None:
    con.execute(f"COPY ({query}) TO '{path}' (FORMAT PARQUET)")


def test_candidate_build_accepts_binary_nhl_and_three_way_epl(tmp_path) -> None:
    con = duckdb.connect()
    try:
        con.execute("""CREATE TABLE meta(
            condition_id VARCHAR,event_slug VARCHAR,question VARCHAR,group_item_title VARCHAR,
            neg_risk BOOLEAN,sports_market_type VARCHAR,market_slug VARCHAR)""")
        rows = [
            ("nhl-m","nhl-ana-bos-2026-01-01","Ducks vs. Bruins",None,False,"moneyline","nhl-x"),
            ("epl-a","epl-ars-bou-2026-01-02","Will Arsenal FC win on 2026-01-02?","Arsenal FC",True,"moneyline","a"),
            ("epl-b","epl-ars-bou-2026-01-02","Will AFC Bournemouth win on 2026-01-02?","AFC Bournemouth",True,"moneyline","b"),
            ("epl-d","epl-ars-bou-2026-01-02","Will Arsenal FC vs. AFC Bournemouth end in a draw?","Draw (Arsenal FC vs. AFC Bournemouth)",True,"moneyline","d"),
        ]
        con.executemany("INSERT INTO meta VALUES (?,?,?,?,?,?,?)",rows)
        con.execute("CREATE TABLE token_map(condition_id VARCHAR,token_id VARCHAR,outcome VARCHAR)")
        con.execute("CREATE TABLE universe(token_id VARCHAR,market_id VARCHAR,winning_outcome VARCHAR)")
        token_rows = [
            ("nhl-m","nhl-1","Ducks","Ducks"),("nhl-m","nhl-2","Bruins","Ducks"),
            ("epl-a","a-y","Yes","No"),("epl-a","a-n","No","No"),
            ("epl-b","b-y","Yes","No"),("epl-b","b-n","No","No"),
            ("epl-d","d-y","Yes","Yes"),("epl-d","d-n","No","Yes"),
        ]
        for market,token,outcome,winner in token_rows:
            con.execute("INSERT INTO token_map VALUES (?,?,?)",[market,token,outcome])
            con.execute("INSERT INTO universe VALUES (?,?,?)",[token,market,winner])
        meta_path=tmp_path/"meta.parquet"; token_path=tmp_path/"tokens.parquet"; universe_path=tmp_path/"universe.parquet"
        _write(con,str(meta_path),"SELECT * FROM meta")
        _write(con,str(token_path),"SELECT * FROM token_map")
        _write(con,str(universe_path),"SELECT * FROM universe")
    finally:
        con.close()
    run=tmp_path/"run"
    result=build_candidates(meta_path,token_path,universe_path,run)
    assert result["counts"]["events"] == 2
    check=duckdb.connect()
    try:
        observed=check.execute(
            "SELECT sport,result_label,market_count FROM read_parquet(?) ORDER BY sport",
            [str(run/"candidate_events.parquet")],
        ).fetchall()
    finally:
        check.close()
    assert observed == [("epl","draw",3),("nhl","Ducks",1)]


def test_provider_match_requires_unique_pair_final_and_result() -> None:
    candidate={
        "sport":"nhl","participant_1":"Ducks","participant_2":"Bruins",
        "result_label":"Ducks","market_date":date(2026,1,1),
    }
    record=CompetitionRecord(
        event_id="1",competition_id="1",event_name="Anaheim at Boston",
        scheduled_start_utc=datetime(2026,1,1,18,tzinfo=UTC),
        competitors=(
            CompetitorRecord("a","Anaheim Ducks","away",True),
            CompetitorRecord("b","Boston Bruins","home",False),
        ),status_state="post",status_detail="Final",
    )
    assert _candidate_match(candidate,[record]) == (record,None)
    wrong={**candidate,"result_label":"Bruins"}
    assert _candidate_match(wrong,[record])[1] == "provider_result_mismatch_or_nonfinal"


def test_tennis_match_allows_tournament_listing_lead_but_caps_it() -> None:
    candidate={
        "sport":"wta","participant_1":"Clara Tauson","participant_2":"Maya Joint",
        "result_label":"Joint","market_date":date(2025,9,18),
    }
    record=CompetitionRecord(
        event_id="811-2025",competition_id="162854",event_name="Seoul",
        scheduled_start_utc=datetime(2025,9,20,3,5,tzinfo=UTC),
        competitors=(
            CompetitorRecord("1","Maya Joint","away",True),
            CompetitorRecord("2","Clara Tauson","home",False),
        ),status_state="post",status_detail="Final",grouping_id="2",
        grouping_name="Women's Singles",
    )
    assert _candidate_match(candidate,[record]) == (record,None)
    too_early={**candidate,"market_date":date(2025,9,10)}
    assert _candidate_match(too_early,[record])[1] == "no_provider_pair_match"


def test_college_scoreboards_request_full_division_slates() -> None:
    assert _scoreboard_url("cbb", "20251103").endswith(
        "?dates=20251103&limit=200&groups=50"
    )
    assert _scoreboard_url("cfb", "20250913").endswith(
        "?dates=20250913&limit=200&groups=80"
    )
    assert _scoreboard_url("nhl", "20260101").endswith("?dates=20260101&limit=200")


def test_cbb_literal_phases_use_two_observed_halves() -> None:
    starts={
        1:datetime(2026,1,1,18,tzinfo=UTC),
        2:datetime(2026,1,1,19,tzinfo=UTC),
    }
    rows=_phase_rows("cbb","cbb-a-b-2026-01-01",starts,datetime(2026,1,1,20,tzinfo=UTC))
    assert [row[2] for row in rows] == ["half_1","half_2_plus"]
    assert rows[0][-1] == rows[1][-2]


def test_ufc_early_finish_keeps_observed_rounds_only() -> None:
    start=datetime(2026,1,1,18,tzinfo=UTC)
    rows=_phase_rows("ufc","ufc-a-b-2026-01-01",{1:start},start.replace(minute=4))
    assert [row[2] for row in rows] == ["round_1"]
    assert rows[0][-2:] == (start,start.replace(minute=4))
