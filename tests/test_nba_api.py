from __future__ import annotations

import json
import hashlib
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import analysis.nba_game_dynamics.nba_api as nba_api_module


FIXTURE_DIR = Path(__file__).parent / "fixtures"

from analysis.nba_game_dynamics.nba_api import (
    ACTUAL_END_EVENT,
    ACTUAL_START_EVENT,
    NBA_SCHEDULE_URL,
    NbaApiClient,
    PERIOD_BOUNDARY_EVENT,
    ScheduleGame,
    classify_timestamp,
    is_boundary_sensitive,
    nba_season_start_for_date,
    parse_legacy_schedule,
    parse_live_data_play_by_play,
    validate_schedule_timing,
    winning_team_id,
)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _two_overtime_payload() -> dict:
    payload = _fixture("nba_live_data_pbp.json")
    actions = payload["game"]["actions"]
    actions[-2].update(scoreAway="113", scoreHome="113")
    actions.pop()
    actions.extend([
        {
            "actionNumber": 761, "orderNumber": 7420000,
            "clock": "PT05M00.00S", "timeActual": "2025-03-13T04:57:00.0Z",
            "period": 6, "actionType": "period", "subType": "start",
        },
        {
            "actionNumber": 820, "orderNumber": 8010000,
            "clock": "PT00M00.00S", "timeActual": "2025-03-13T05:11:00.0Z",
            "period": 6, "actionType": "period", "subType": "end",
            "scoreAway": "114", "scoreHome": "113",
        },
        {
            "actionNumber": 821, "orderNumber": 8020000,
            "clock": "PT00M00.00S", "timeActual": "2025-03-13T05:11:02.0Z",
            "period": 6, "actionType": "game", "subType": "end",
            "scoreAway": "114", "scoreHome": "113",
        },
    ])
    return payload


def test_legacy_schedule_keeps_local_date_utc_start_result_and_postponement() -> None:
    final, postponed = parse_legacy_schedule(_fixture("nba_legacy_schedule.json"), 2024)

    assert final.game_id == "0022400953"
    assert final.official_date == date(2025, 3, 12)
    assert final.scheduled_start_utc == datetime(2025, 3, 13, 2, tzinfo=timezone.utc)
    assert (final.away_team_id, final.home_team_id) == (1610612752, 1610612757)
    assert (final.away_final_score, final.home_final_score) == (114, 113)
    assert final.game_type_code == "002"
    assert final.is_completed is True
    assert final.expected_final_period is None
    assert final.winner_team_id == 1610612752
    assert (final.away_is_winner, final.home_is_winner) == (True, False)
    assert winning_team_id(final) == 1610612752
    assert postponed.is_completed is False
    assert postponed.away_final_score is None
    assert postponed.postponement_reason == "Postponed"


def test_schedule_preserves_only_explicit_overtime_period_declarations() -> None:
    payload = _fixture("nba_legacy_schedule.json")
    game = payload["lscd"][0]["mscd"]["g"][0]
    game["stt"] = "Final/OT"
    assert parse_legacy_schedule(payload, 2024)[0].expected_final_period == 5
    game["stt"] = "Final/2OT"
    assert parse_legacy_schedule(payload, 2024)[0].expected_final_period == 6
    game["stt"] = "Final/0OT"
    with pytest.raises(ValueError, match="Invalid overtime count"):
        parse_legacy_schedule(payload, 2024)


def test_live_data_uses_absolute_period_actions_and_folds_overtime_into_q4_plus() -> None:
    timing = parse_live_data_play_by_play(_fixture("nba_live_data_pbp.json"))

    assert timing.game_id == "0022400953"
    assert timing.action_count == 12
    assert timing.final_period == 5
    assert timing.actual_start_utc == datetime(
        2025, 3, 13, 2, 13, 27, 700000, tzinfo=timezone.utc
    )
    assert timing.actual_end_utc == datetime(
        2025, 3, 13, 4, 54, 47, 300000, tzinfo=timezone.utc
    )
    assert [row.phase for row in timing.phase_windows] == [
        "quarter_1", "quarter_2", "quarter_3", "quarter_4_plus"
    ]
    assert timing.phase_windows[-1].start_utc == datetime(
        2025, 3, 13, 3, 55, 30, 200000, tzinfo=timezone.utc
    )
    assert timing.phase_windows[-1].end_utc == timing.actual_end_utc
    assert timing.actual_start_event == ACTUAL_START_EVENT
    assert timing.actual_end_event == ACTUAL_END_EVENT
    assert timing.period_boundary_event == PERIOD_BOUNDARY_EVENT
    assert timing.actual_start_action_number == 3
    assert timing.actual_end_action_number == 760
    assert (timing.pbp_away_final_score, timing.pbp_home_final_score) == (114, 113)


def test_actual_start_uses_only_the_first_audited_post_start_action() -> None:
    payload = _fixture("nba_live_data_pbp.json")
    timing = parse_live_data_play_by_play(payload)
    period_start = timing.periods[0].start_utc

    assert timing.actual_start_utc > period_start
    without_tip = deepcopy(payload)
    without_tip["game"]["actions"][1]["actionType"] = "shot"
    without_tip["game"]["actions"].insert(2, {
        "actionNumber": 4, "orderNumber": 40000,
        "clock": "PT11M56.00S", "timeActual": "2025-03-13T02:13:28.0Z",
        "period": 1, "actionType": "jumpball", "subType": "recovered",
        "descriptor": "startperiod", "scoreAway": "0", "scoreHome": "0",
    })
    with pytest.raises(ValueError, match="first post-start action"):
        parse_live_data_play_by_play(without_tip)

    production_failure = _fixture("nba_live_data_pbp.json")
    production_failure["game"]["gameId"] = "0022400887"
    production_failure["game"]["actions"][1].update(
        actionType="foul", subType="personal", descriptor="loose ball",
        description="A. Drummond loose ball personal FOUL (1 PF)",
        clock="PT12M00.00S",
    )
    with pytest.raises(ValueError, match="first post-start action"):
        parse_live_data_play_by_play(production_failure)


@pytest.mark.parametrize(
    ("action_type", "subtype", "descriptor", "clock", "description"),
    (
        ("jumpball", "recovered", "startperiod", "PT11M45.00S", "Jump Ball A vs. B"),
        ("jumpball", "recovered", "outofbounds", "PT11M53.00S", "Jump Ball A vs. B"),
        ("jumpball", "recovered", "heldball", "PT11M54.00S", "Jump Ball A vs. B"),
        ("jumpball", "recovered", "unclearpass", "PT12M00.00S", "Jump Ball A vs. B"),
        ("violation", "jumpball", "", "PT12M00.00S", "A. Player jumpball VIOLATION"),
    ),
)
def test_observed_opening_signatures_are_exact(
    action_type: str, subtype: str, descriptor: str, clock: str,
    description: str,
) -> None:
    payload = _fixture("nba_live_data_pbp.json")
    payload["game"]["actions"][1].update(
        actionType=action_type, subType=subtype, descriptor=descriptor, clock=clock,
        description=description,
    )

    assert parse_live_data_play_by_play(payload).actual_start_action_number == 3


def test_only_exact_opening_delay_of_game_rows_are_skipped() -> None:
    payload = _fixture("nba_live_data_pbp.json")
    administrative = {
        "actionNumber": 11, "orderNumber": 25000,
        "clock": "PT12M00.00S", "timeActual": "2025-03-13T02:13:20.0Z",
        "period": 1, "actionType": "violation", "subType": "delay-of-game",
        "description": "TEAM delay-of-game VIOLATION",
        "scoreAway": "0", "scoreHome": "0",
    }
    second = {**administrative, "actionNumber": 12, "orderNumber": 27500,
              "timeActual": "2025-03-13T02:13:21.0Z"}
    payload["game"]["actions"][1:1] = [administrative, second]
    assert parse_live_data_play_by_play(payload).actual_start_action_number == 3

    for field, value in (
        ("clock", "PT11M59.00S"), ("scoreHome", "1"),
        ("description", "different admin"),
    ):
        changed = _fixture("nba_live_data_pbp.json")
        invalid = {**administrative, field: value}
        changed["game"]["actions"].insert(1, invalid)
        with pytest.raises(ValueError, match="first post-start action"):
            parse_live_data_play_by_play(changed)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("clock", "PT11M44.99S"),
        ("scoreAway", "1"),
        ("descriptor", "unknown"),
        ("subType", ""),
        ("description", "not an opening tip"),
    ),
)
def test_unobserved_or_out_of_range_opening_action_fails(
    field: str, value: object
) -> None:
    payload = _fixture("nba_live_data_pbp.json")
    payload["game"]["actions"][1][field] = value
    with pytest.raises(ValueError, match="first post-start action"):
        parse_live_data_play_by_play(payload)


def test_schedule_ot_and_two_ot_must_reconcile_with_terminal_period() -> None:
    game = parse_legacy_schedule(_fixture("nba_legacy_schedule.json"), 2024)[0]
    timing = parse_live_data_play_by_play(_fixture("nba_live_data_pbp.json"))
    validate_schedule_timing(game, timing)

    explicit_ot = ScheduleGame(**{
        **game.__dict__, "status_text": "Final/OT", "expected_final_period": 5,
    })
    validate_schedule_timing(explicit_ot, timing)

    two_ot = ScheduleGame(**{
        **game.__dict__, "status_text": "Final/2OT", "expected_final_period": 6,
    })
    with pytest.raises(ValueError, match="expects final period 6"):
        validate_schedule_timing(two_ot, timing)

    two_ot_timing = parse_live_data_play_by_play(_two_overtime_payload())
    assert two_ot_timing.final_period == 6
    validate_schedule_timing(game, two_ot_timing)
    validate_schedule_timing(two_ot, two_ot_timing)


def test_observed_overtime_chain_must_be_tied_until_decisive_final_period() -> None:
    decisive_regulation = _fixture("nba_live_data_pbp.json")
    decisive_regulation["game"]["actions"][-4].update(
        scoreAway="105", scoreHome="104"
    )
    with pytest.raises(ValueError, match="decisive period 4"):
        parse_live_data_play_by_play(decisive_regulation)

    decisive_first_ot = _two_overtime_payload()
    decisive_first_ot["game"]["actions"][-4].update(
        scoreAway="114", scoreHome="113"
    )
    with pytest.raises(ValueError, match="decisive period 5"):
        parse_live_data_play_by_play(decisive_first_ot)

    tied_final = _fixture("nba_live_data_pbp.json")
    tied_final["game"]["actions"][-2].update(scoreAway="113", scoreHome="113")
    tied_final["game"]["actions"][-1].update(scoreAway="113", scoreHome="113")
    with pytest.raises(ValueError, match="tied final PBP"):
        parse_live_data_play_by_play(tied_final)


@pytest.mark.parametrize("mutation", ("missing", "duplicate", "wrong_period", "reordered", "score"))
def test_game_end_is_unique_ordered_same_period_terminal_evidence(mutation: str) -> None:
    payload = _fixture("nba_live_data_pbp.json")
    game_end = payload["game"]["actions"][-1]
    if mutation == "missing":
        payload["game"]["actions"].pop()
    elif mutation == "duplicate":
        payload["game"]["actions"].append({
            **game_end, "actionNumber": 762, "orderNumber": 7430000,
            "timeActual": "2025-03-13T04:54:51.3Z",
        })
    elif mutation == "wrong_period":
        game_end["period"] = 4
    elif mutation == "reordered":
        game_end["timeActual"] = "2025-03-13T04:54:46.3Z"
    else:
        game_end["scoreAway"] = "115"
    with pytest.raises(ValueError, match="game/end|terminal scores"):
        parse_live_data_play_by_play(payload)


def test_schedule_and_pbp_final_scores_must_match_exactly() -> None:
    game = parse_legacy_schedule(_fixture("nba_legacy_schedule.json"), 2024)[0]
    mismatch = ScheduleGame(**{**game.__dict__, "away_final_score": 115})
    with pytest.raises(ValueError, match="schedule/PBP final scores disagree"):
        validate_schedule_timing(
            mismatch, parse_live_data_play_by_play(_fixture("nba_live_data_pbp.json"))
        )

    for value in ("114.0", -1, True):
        payload = _fixture("nba_live_data_pbp.json")
        payload["game"]["actions"][-2]["scoreAway"] = value
        with pytest.raises(ValueError, match="scoreAway"):
            parse_live_data_play_by_play(payload)


def test_production_score_discrepancy_0022400072_fails_closed() -> None:
    game = parse_legacy_schedule(_fixture("nba_legacy_schedule.json"), 2024)[0]
    game = ScheduleGame(**{
        **game.__dict__, "game_id": "0022400072",
        "away_final_score": 140, "home_final_score": 104,
    })
    payload = _fixture("nba_live_data_pbp.json")
    payload["game"]["gameId"] = "0022400072"
    payload["game"]["actions"][-2].update(scoreAway="139", scoreHome="104")
    payload["game"]["actions"][-1].update(scoreAway="139", scoreHome="104")

    with pytest.raises(ValueError, match="schedule/PBP final scores disagree"):
        validate_schedule_timing(game, parse_live_data_play_by_play(payload))


def test_normalized_period_and_action_fields_and_exact_season_selection() -> None:
    payload = _fixture("nba_live_data_pbp.json")
    for row in payload["game"]["actions"]:
        row["period"] = str(row["period"])
        row["actionType"] = row["actionType"].upper()
        row["subType"] = row["subType"].upper()
        row["clock"] = row["clock"].lower()
    assert parse_live_data_play_by_play(payload).final_period == 5
    assert nba_season_start_for_date(date(2025, 6, 30)) == 2024
    assert nba_season_start_for_date(date(2025, 7, 1)) == 2025


def test_client_fetches_only_exact_requested_seasons(monkeypatch, tmp_path: Path) -> None:
    client = NbaApiClient(tmp_path / "cache")
    urls: list[str] = []

    def empty_schedule(cache_path: Path, url: str, refresh: bool) -> dict:
        urls.append(url)
        return {"lscd": []}

    monkeypatch.setattr(client, "_read_or_fetch", empty_schedule)
    assert client.schedule_games(date(2024, 10, 1), date(2026, 6, 30)) == ()
    assert urls == [NBA_SCHEDULE_URL.format(season=2024), NBA_SCHEDULE_URL.format(season=2025)]


def test_cache_is_immutable_and_provenance_fingerprints_raw_bytes(tmp_path: Path) -> None:
    class Response:
        status_code = 200

        def __init__(self, content: bytes) -> None:
            self.content = content

        def raise_for_status(self) -> None:
            return None

    class Session:
        def __init__(self, content: bytes) -> None:
            self.content = content

        def get(self, *args, **kwargs) -> Response:
            return Response(self.content)

    original = b'{"lscd":[]}'
    session = Session(original)
    client = NbaApiClient(tmp_path / "cache", session=session, max_attempts=1)
    cache_path = tmp_path / "cache" / "schedule_2024.json"
    url = NBA_SCHEDULE_URL.format(season=2024)

    assert client._read_or_fetch(cache_path, url, False) == {"lscd": []}
    resource = client.provenance_manifest()["resources"][0]
    assert resource["bytes"] == len(original)
    assert resource["sha256"] == hashlib.sha256(original).hexdigest()
    assert cache_path.read_bytes() == original

    session.content = b'{"lscd":[{"changed":true}]}'
    with pytest.raises(ValueError, match="Immutable NBA cache collision"):
        client._read_or_fetch(cache_path, url, True)
    assert cache_path.read_bytes() == original


def test_cold_fetch_honors_retry_after_jitter_and_success_pacing(
    monkeypatch, tmp_path: Path
) -> None:
    class Response:
        def __init__(self, status_code: int, content: bytes, retry_after: str | None = None):
            self.status_code = status_code
            self.content = content
            self.headers = {} if retry_after is None else {"Retry-After": retry_after}

        def raise_for_status(self) -> None:
            return None

    class Session:
        def __init__(self) -> None:
            self.responses = [
                Response(429, b"", "3"),
                Response(200, b'{"lscd":[]}'),
            ]

        def get(self, *args, **kwargs) -> Response:
            return self.responses.pop(0)

    sleeps: list[float] = []
    monkeypatch.setattr(nba_api_module.time, "sleep", sleeps.append)
    monkeypatch.setattr(nba_api_module.random, "uniform", lambda low, high: high)
    client = NbaApiClient(
        tmp_path / "cache", session=Session(), max_attempts=2,
        backoff_seconds=0.5, success_pause_seconds=0.1, jitter_seconds=0.25,
    )
    cache_path = tmp_path / "cache" / "schedule_2024.json"
    url = NBA_SCHEDULE_URL.format(season=2024)

    assert client._read_or_fetch(cache_path, url, False) == {"lscd": []}
    assert sleeps == [3.25, 0.35]
    assert client._read_or_fetch(cache_path, url, False) == {"lscd": []}
    assert sleeps == [3.25, 0.35]


def test_nontransient_nba_http_error_is_not_retried(monkeypatch, tmp_path: Path) -> None:
    class Response:
        status_code = 404
        headers: dict[str, str] = {}
        content = b""

        def raise_for_status(self) -> None:
            raise nba_api_module.requests.HTTPError("not found", response=self)

    class Session:
        calls = 0

        def get(self, *args, **kwargs) -> Response:
            self.calls += 1
            return Response()

    session = Session()
    sleeps: list[float] = []
    monkeypatch.setattr(nba_api_module.time, "sleep", sleeps.append)
    client = NbaApiClient(tmp_path / "cache", session=session, max_attempts=3)

    with pytest.raises(nba_api_module.requests.HTTPError, match="not found"):
        client._read_or_fetch(
            tmp_path / "cache" / "missing.json", "https://example.invalid/missing", False
        )
    assert session.calls == 1
    assert sleeps == []


def test_literal_phases_and_inclusive_30_second_sensitivity() -> None:
    timing = parse_live_data_play_by_play(_fixture("nba_live_data_pbp.json"))
    q2 = timing.phase_windows[1].start_utc

    assert classify_timestamp(timing, timing.actual_start_utc - timedelta(seconds=1)) == "pregame"
    assert classify_timestamp(timing, timing.actual_start_utc) == "quarter_1"
    assert classify_timestamp(timing, q2 - timedelta(microseconds=1)) == "quarter_1"
    assert classify_timestamp(timing, q2) == "quarter_2"
    assert classify_timestamp(timing, timing.actual_end_utc) == "quarter_4_plus"
    assert classify_timestamp(timing, timing.actual_end_utc + timedelta(seconds=1)) == "post_final"
    assert is_boundary_sensitive(timing, q2 - timedelta(seconds=30)) is True
    assert is_boundary_sensitive(timing, q2 + timedelta(seconds=30)) is True
    assert is_boundary_sensitive(timing, q2 + timedelta(seconds=30, microseconds=1)) is False
    # OT is deliberately not a study-phase boundary.
    overtime_start = timing.periods[4].start_utc
    assert is_boundary_sensitive(timing, overtime_start) is False


def test_live_data_fails_if_any_action_lacks_absolute_time() -> None:
    payload = _fixture("nba_live_data_pbp.json")
    payload["game"]["actions"][1]["timeActual"] = None
    with pytest.raises(ValueError, match="timeActual"):
        parse_live_data_play_by_play(payload)


def test_nonboundary_action_edit_time_may_reorder_but_boundaries_remain_strict() -> None:
    payload = _fixture("nba_live_data_pbp.json")
    # Real official feeds retain ordered action records but can edit an
    # ordinary action with a timeActual earlier than the preceding record.
    payload["game"]["actions"].insert(2, {
        "actionNumber": 4, "orderNumber": 40000, "clock": "PT11M58.00S",
        "timeActual": "2025-03-13T02:13:20.0Z", "period": 1,
        "actionType": "shot", "subType": "2pt",
    })

    timing = parse_live_data_play_by_play(payload)

    assert timing.final_period == 5
    assert timing.action_count == 13


def test_live_data_fails_on_wrong_identity_missing_boundary_and_naive_time() -> None:
    payload = _fixture("nba_live_data_pbp.json")
    with pytest.raises(ValueError, match="Requested NBA game"):
        parse_live_data_play_by_play(payload, expected_game_id="0022400000")

    missing = deepcopy(payload)
    missing["game"]["actions"] = [
        row for row in missing["game"]["actions"]
        if not (row["period"] == 3 and row["subType"] == "start")
    ]
    with pytest.raises(ValueError, match="period 3 requires exactly one"):
        parse_live_data_play_by_play(missing)

    naive = deepcopy(payload)
    naive["game"]["actions"][0]["timeActual"] = "2025-03-13T02:13:12"
    with pytest.raises(ValueError, match="lacks a timezone"):
        parse_live_data_play_by_play(naive)
