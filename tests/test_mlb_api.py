from __future__ import annotations

import json
import sys
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
FIXTURE_DIR = Path(__file__).parent / "fixtures"
sys.path.insert(0, str(MODULE_DIR))

from mlb_api import (  # noqa: E402
    MlbApiClient,
    classify_timestamp,
    completed_schedule_games,
    inning_phase,
    parse_live_feed,
    parse_schedule,
    reconcile_schedule_games,
    winning_mlb_team_id,
)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_schedule_parser_retains_audit_states_and_stable_identity() -> None:
    games = parse_schedule(_fixture("mlb_schedule.json"))

    assert [game.game_pk for game in games] == [1001, 1002, 1003, 1004, 1005]
    assert [game.game_pk for game in completed_schedule_games(games)] == [1001, 1004, 1005]
    opener = games[0]
    assert opener.official_date == date(2025, 7, 4)
    assert opener.scheduled_start_utc == datetime(2025, 7, 4, 17, 5, tzinfo=timezone.utc)
    assert (opener.away_team_id, opener.home_team_id) == (147, 121)
    assert (opener.away_final_score, opener.home_final_score) == (3, 5)
    assert (opener.away_is_winner, opener.home_is_winner) == (False, True)

    postponed = games[1]
    assert postponed.status_detailed == "Postponed"
    assert postponed.is_completed is False
    assert postponed.doubleheader == "S"
    assert postponed.reschedule_date_utc == datetime(
        2025, 7, 5, 17, 5, tzinfo=timezone.utc
    )
    assert postponed.rescheduled_from_date == date(2025, 7, 4)

    suspended = games[2]
    assert suspended.status_detailed == "Suspended"
    assert suspended.resume_date_utc == datetime(2025, 7, 5, 19, 10, tzinfo=timezone.utc)
    assert suspended.resumed_from_date == date(2025, 7, 4)

    assert games[3].doubleheader == "Y"
    assert games[3].game_number == 1
    assert games[4].game_number == 2
    assert games[4].status_detailed == "Completed Early: Rain"


def test_winning_team_requires_consistent_official_scores_and_flags() -> None:
    games = {game.game_pk: game for game in parse_schedule(_fixture("mlb_schedule.json"))}

    assert winning_mlb_team_id(games[1001]) == 121
    assert winning_mlb_team_id(games[1004]) == 111


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"away_final_score": 5, "home_final_score": 5}, "tied final scores"),
        ({"away_final_score": None}, "lacks final scores"),
        ({"away_is_winner": None}, "exactly one"),
        (
            {"away_is_winner": True, "home_is_winner": False},
            "inconsistent score and winner flags",
        ),
        ({"away_is_winner": True, "home_is_winner": True}, "exactly one"),
    ],
)
def test_winning_team_fails_closed(changes: dict, message: str) -> None:
    game = parse_schedule(_fixture("mlb_schedule.json"))[0]
    with pytest.raises(ValueError, match=message):
        winning_mlb_team_id(replace(game, **changes))


def test_winning_team_rejects_nonfinal_game() -> None:
    game = parse_schedule(_fixture("mlb_schedule.json"))[1]
    with pytest.raises(ValueError, match="is not final"):
        winning_mlb_team_id(game)


def test_live_feed_parser_uses_observed_plays_and_builds_phase_windows() -> None:
    timing = parse_live_feed(_fixture("mlb_live_feed.json"))

    assert timing.game_pk == 1001
    assert timing.actual_start_utc == datetime(
        2025, 7, 4, 17, 8, 12, 100000, tzinfo=timezone.utc
    )
    assert timing.actual_end_utc == datetime(
        2025, 7, 4, 20, 8, 44, 500000, tzinfo=timezone.utc
    )
    assert timing.play_count == 8
    assert [(item.inning, item.half, item.phase) for item in timing.inning_halves] == [
        (1, "top", "innings_1_3"),
        (1, "bottom", "innings_1_3"),
        (3, "bottom", "innings_1_3"),
        (4, "top", "innings_4_6"),
        (6, "bottom", "innings_4_6"),
        (7, "top", "innings_7_plus"),
        (10, "bottom", "innings_7_plus"),
    ]
    assert timing.inning_halves[0].first_at_bat_index == 0
    assert timing.inning_halves[0].last_at_bat_index == 1
    windows = [(window.phase, window.start_utc, window.end_utc) for window in timing.phase_windows]
    assert windows == [
        (
            "innings_1_3",
            datetime(2025, 7, 4, 17, 8, 12, 100000, tzinfo=timezone.utc),
            datetime(2025, 7, 4, 18, 13, tzinfo=timezone.utc),
        ),
        (
            "innings_4_6",
            datetime(2025, 7, 4, 18, 13, tzinfo=timezone.utc),
            datetime(2025, 7, 4, 19, 10, tzinfo=timezone.utc),
        ),
        (
            "innings_7_plus",
            datetime(2025, 7, 4, 19, 10, tzinfo=timezone.utc),
            datetime(2025, 7, 4, 20, 8, 44, 500000, tzinfo=timezone.utc),
        ),
    ]


def test_live_feed_parser_rejects_nonfinal_game() -> None:
    payload = _fixture("mlb_live_feed.json")
    payload["gameData"]["status"]["abstractGameState"] = "Live"
    with pytest.raises(ValueError, match="not for a completed game"):
        parse_live_feed(payload)


@pytest.mark.parametrize("inning", [4, 7])
def test_live_feed_parser_rejects_missing_phase_start(inning: int) -> None:
    payload = _fixture("mlb_live_feed.json")
    play = next(
        play
        for play in payload["liveData"]["plays"]["allPlays"]
        if play["about"]["inning"] == inning
    )
    play["about"]["halfInning"] = "bottom"

    with pytest.raises(ValueError, match=f"missing top-of-inning boundaries.*{inning}"):
        parse_live_feed(payload)


def test_live_feed_parser_rejects_truncated_final_feed() -> None:
    payload = _fixture("mlb_live_feed.json")
    payload["liveData"]["plays"]["allPlays"] = payload["liveData"]["plays"]["allPlays"][:4]

    with pytest.raises(ValueError, match="linescore ends in inning 10.*plays end in inning 3"):
        parse_live_feed(payload)


def test_live_feed_parser_rejects_duplicate_or_reordered_indices() -> None:
    payload = _fixture("mlb_live_feed.json")
    payload["liveData"]["plays"]["allPlays"][1]["about"]["atBatIndex"] = 0

    with pytest.raises(ValueError, match="unique, ordered, and contiguous"):
        parse_live_feed(payload)


def test_live_feed_parser_accepts_overlapping_adjacent_plate_appearances() -> None:
    payload = _fixture("mlb_live_feed.json")
    payload["liveData"]["plays"]["allPlays"][1]["about"]["startTime"] = (
        "2025-07-04T17:09:00Z"
    )

    timing = parse_live_feed(payload)

    assert timing.play_count == 8
    assert timing.inning_halves[0].start_utc == datetime(
        2025, 7, 4, 17, 8, 12, 100000, tzinfo=timezone.utc
    )


def test_live_feed_parser_uses_ordered_zero_pitch_events_for_late_intent_walk() -> None:
    payload = _fixture("mlb_live_feed.json")
    play = payload["liveData"]["plays"]["allPlays"][1]
    play["result"] = {"eventType": "intent_walk"}
    play["about"]["startTime"] = "2025-07-04T17:16:00Z"
    play["about"]["endTime"] = "2025-07-04T17:16:30Z"
    play["playEvents"] = [
        {
            "index": index,
            "isPitch": False,
            "type": "no_pitch",
            "startTime": f"2025-07-04T17:10:0{index}.000Z",
            "endTime": f"2025-07-04T17:10:0{index}.001Z",
        }
        for index in range(4)
    ]

    timing = parse_live_feed(payload)

    assert timing.inning_halves[0].end_utc == datetime(
        2025, 7, 4, 17, 10, 3, 1000, tzinfo=timezone.utc
    )
    assert timing.actual_start_utc == datetime(
        2025, 7, 4, 17, 8, 12, 100000, tzinfo=timezone.utc
    )
    assert timing.actual_end_utc == datetime(
        2025, 7, 4, 20, 8, 44, 500000, tzinfo=timezone.utc
    )


def test_live_feed_parser_uses_ordered_zero_pitch_event_for_runner_out() -> None:
    payload = _fixture("mlb_live_feed.json")
    play = payload["liveData"]["plays"]["allPlays"][1]
    play["result"] = {"eventType": "pickoff_1b"}
    play["about"]["startTime"] = "2025-07-04T17:11:00Z"
    play["about"]["endTime"] = "2025-07-04T17:10:30Z"
    play["playEvents"] = [
        {
            "index": 0,
            "isPitch": False,
            "type": "pickoff",
            "startTime": "2025-07-04T17:10:20Z",
            "endTime": "2025-07-04T17:10:30Z",
        }
    ]

    timing = parse_live_feed(payload)

    assert timing.inning_halves[0].end_utc == datetime(
        2025, 7, 4, 17, 10, 30, tzinfo=timezone.utc
    )


@pytest.mark.parametrize(
    ("event_type", "child_type"),
    [
        ("single", "pickoff"),
        ("intent_walk", "action"),
        ("pickoff_1b", "no_pitch"),
    ],
)
def test_live_feed_parser_rejects_zero_pitch_envelope_outside_synthetic_context(
    event_type: str, child_type: str
) -> None:
    payload = _fixture("mlb_live_feed.json")
    play = payload["liveData"]["plays"]["allPlays"][1]
    play["result"] = {"eventType": event_type}
    play["about"]["startTime"] = "2025-07-04T17:11:00Z"
    play["about"]["endTime"] = "2025-07-04T17:10:30Z"
    play["playEvents"] = [
        {
            "index": 0,
            "isPitch": False,
            "type": child_type,
            "startTime": "2025-07-04T17:10:20Z",
            "endTime": "2025-07-04T17:10:30Z",
        }
    ]

    with pytest.raises(ValueError, match="ends before it starts"):
        parse_live_feed(payload)


@pytest.mark.parametrize(
    "event_change",
    [
        {"index": 1},
        {"isPitch": None},
        {"startTime": None},
        {"endTime": None},
        {"endTime": "2025-07-04T17:10:19Z"},
    ],
)
def test_live_feed_parser_rejects_incomplete_or_invalid_zero_pitch_child(
    event_change: dict,
) -> None:
    payload = _fixture("mlb_live_feed.json")
    play = payload["liveData"]["plays"]["allPlays"][1]
    play["result"] = {"eventType": "pickoff_1b"}
    play["about"]["startTime"] = "2025-07-04T17:11:00Z"
    play["about"]["endTime"] = "2025-07-04T17:10:30Z"
    event = {
        "index": 0,
        "isPitch": False,
        "type": "pickoff",
        "startTime": "2025-07-04T17:10:20Z",
        "endTime": "2025-07-04T17:10:30Z",
    }
    event.update(event_change)
    play["playEvents"] = [event]

    with pytest.raises(ValueError, match="ends before it starts"):
        parse_live_feed(payload)


def test_live_feed_parser_rejects_ambiguous_zero_pitch_child_order() -> None:
    payload = _fixture("mlb_live_feed.json")
    play = payload["liveData"]["plays"]["allPlays"][1]
    play["result"] = {"eventType": "intent_walk"}
    play["about"]["startTime"] = "2025-07-04T17:11:00Z"
    play["about"]["endTime"] = "2025-07-04T17:10:30Z"
    play["playEvents"] = [
        {
            "index": 0,
            "isPitch": False,
            "type": "no_pitch",
            "startTime": "2025-07-04T17:10:20Z",
            "endTime": "2025-07-04T17:10:21Z",
        },
        {
            "index": 0,
            "isPitch": False,
            "type": "no_pitch",
            "startTime": "2025-07-04T17:10:19Z",
            "endTime": "2025-07-04T17:10:30Z",
        },
    ]

    with pytest.raises(ValueError, match="ends before it starts"):
        parse_live_feed(payload)


@pytest.mark.parametrize(
    ("about_start", "about_end"),
    [
        ("2025-07-04T17:10:30Z", "2025-07-04T17:10:29Z"),
        ("2025-07-04T17:10:29Z", "2025-07-04T17:10:28Z"),
    ],
)
def test_live_feed_parser_does_not_use_envelope_for_possible_parent_start(
    about_start: str, about_end: str
) -> None:
    payload = _fixture("mlb_live_feed.json")
    play = payload["liveData"]["plays"]["allPlays"][1]
    play["result"] = {"eventType": "pickoff_1b"}
    play["about"]["startTime"] = about_start
    play["about"]["endTime"] = about_end
    play["playEvents"] = [
        {
            "index": 0,
            "isPitch": False,
            "type": "pickoff",
            "startTime": "2025-07-04T17:10:20Z",
            "endTime": "2025-07-04T17:10:30Z",
        }
    ]

    with pytest.raises(ValueError, match="ends before it starts"):
        parse_live_feed(payload)


def test_live_feed_parser_rejects_pitched_event_timestamp_corruption() -> None:
    payload = _fixture("mlb_live_feed.json")
    play = payload["liveData"]["plays"]["allPlays"][1]
    play["about"]["startTime"] = "2025-07-04T17:11:00Z"
    play["about"]["endTime"] = "2025-07-04T17:10:30Z"
    play["playEvents"] = [
        {
            "index": 0,
            "isPitch": True,
            "startTime": "2025-07-04T17:11:00Z",
            "endTime": "2025-07-04T17:11:05Z",
        },
        {
            "index": 1,
            "isPitch": True,
            "startTime": "2025-07-04T17:10:20Z",
            "endTime": "2025-07-04T17:10:30Z",
        },
    ]

    with pytest.raises(ValueError, match="ends before it starts"):
        parse_live_feed(payload)


def test_live_feed_parser_rejects_nonchronological_plate_appearance_starts() -> None:
    payload = _fixture("mlb_live_feed.json")
    payload["liveData"]["plays"]["allPlays"][1]["about"]["startTime"] = (
        "2025-07-04T17:08:00Z"
    )

    with pytest.raises(ValueError, match="start timestamps are not chronological"):
        parse_live_feed(payload)


def test_live_feed_parser_rejects_end_before_start_within_plate_appearance() -> None:
    payload = _fixture("mlb_live_feed.json")
    payload["liveData"]["plays"]["allPlays"][1]["about"]["endTime"] = (
        "2025-07-04T17:10:00Z"
    )

    with pytest.raises(ValueError, match="ends before it starts"):
        parse_live_feed(payload)


def test_boundary_timestamps_follow_half_open_phase_rules() -> None:
    timing = parse_live_feed(_fixture("mlb_live_feed.json"))
    top_four = timing.phase_windows[1].start_utc
    top_seven = timing.phase_windows[2].start_utc

    assert classify_timestamp(timing, timing.actual_start_utc - timedelta(seconds=1)) == "pregame"
    assert classify_timestamp(timing, top_four - timedelta(microseconds=1)) == "innings_1_3"
    assert classify_timestamp(timing, top_four) == "innings_4_6"
    assert classify_timestamp(timing, top_seven) == "innings_7_plus"
    assert classify_timestamp(timing, timing.actual_end_utc) == "innings_7_plus"
    assert classify_timestamp(timing, timing.actual_end_utc + timedelta(seconds=1)) == "post_final"


@pytest.mark.parametrize(
    ("inning", "expected"),
    [(1, "innings_1_3"), (3, "innings_1_3"), (4, "innings_4_6"),
     (6, "innings_4_6"), (7, "innings_7_plus"), (12, "innings_7_plus")],
)
def test_inning_phase(inning: int, expected: str) -> None:
    assert inning_phase(inning) == expected


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict | None, float]] = []

    def get(self, url: str, params: dict | None, timeout: float) -> _FakeResponse:
        self.calls.append((url, params, timeout))
        return self.responses.pop(0)


def _one_game_schedule(game_pk: int, official_date: date) -> dict:
    game = deepcopy(_fixture("mlb_schedule.json")["dates"][0]["games"][0])
    game["gamePk"] = game_pk
    game["officialDate"] = official_date.isoformat()
    game["gameDate"] = f"{official_date.isoformat()}T17:05:00Z"
    return {"dates": [{"date": official_date.isoformat(), "games": [game]}]}


def test_client_reconciles_linked_postponed_and_makeup_records(
    tmp_path: Path,
) -> None:
    destination_start = datetime(2025, 4, 6, 17, 35, tzinfo=timezone.utc)
    original_start = datetime(2025, 4, 5, 20, 10, tzinfo=timezone.utc)
    destination = _one_game_schedule(778443, date(2025, 4, 6))["dates"][0][
        "games"
    ][0]
    destination["gameDate"] = destination_start.isoformat().replace("+00:00", "Z")
    destination["doubleHeader"] = "S"
    destination["rescheduledFromDate"] = "2025-04-05"

    source = deepcopy(destination)
    source["gameDate"] = original_start.isoformat().replace("+00:00", "Z")
    source["doubleHeader"] = "N"
    source.pop("rescheduledFromDate")
    source["rescheduleDate"] = destination["gameDate"]
    source["status"] = {
        "abstractGameState": "Final",
        "detailedState": "Postponed",
        "statusCode": "DR",
    }
    for side in ("away", "home"):
        source["teams"][side].pop("score", None)
        source["teams"][side].pop("isWinner", None)

    payload = {
        "dates": [
            {"date": "2025-04-05", "games": [source]},
            {"date": "2025-04-06", "games": [destination]},
        ]
    }
    parsed_history = parse_schedule(payload)
    assert [game.is_completed for game in parsed_history] == [False, True]
    client = MlbApiClient(
        tmp_path,
        session=_FakeSession([_FakeResponse(payload)]),
        backoff_seconds=0,
    )

    games = client.schedule_games(
        date(2025, 4, 2),
        date(2025, 9, 28),
        include_nonfinal_for_audit=True,
    )

    assert len(games) == 1
    game = games[0]
    assert game.game_pk == 778443
    assert game.status_detailed == "Final"
    assert game.scheduled_start_utc == destination_start
    assert game.doubleheader == "S"
    assert game.reschedule_date_utc == destination_start
    assert game.rescheduled_from_date == date(2025, 4, 5)
    assert (game.away_final_score, game.home_final_score) == (3, 5)
    assert (game.away_is_winner, game.home_is_winner) == (False, True)


def test_reconciles_linked_resumption_using_destination_record() -> None:
    base = parse_schedule(_one_game_schedule(776907, date(2025, 8, 2)))[0]
    original_start = datetime(2025, 8, 2, 23, 15, tzinfo=timezone.utc)
    resumed_start = datetime(2025, 8, 3, 17, 5, tzinfo=timezone.utc)
    source = replace(
        base,
        scheduled_start_utc=original_start,
        series_game_number=3,
        resume_date_utc=resumed_start,
    )
    destination = replace(
        base,
        scheduled_start_utc=resumed_start,
        series_game_number=4,
        resumed_from_date=date(2025, 8, 2),
    )

    assert reconcile_schedule_games((destination, source)) == (
        replace(
            destination,
            resume_date_utc=resumed_start,
            resumed_from_date=date(2025, 8, 2),
        ),
    )


@pytest.mark.parametrize(
    ("destination_changes", "message"),
    [
        ({"home_team_id": 999}, "changes stable identity fields"),
        (
            {"away_final_score": 99},
            "conflicting away_final_score",
        ),
    ],
)
def test_schedule_history_reconciliation_fails_closed_on_conflicts(
    destination_changes: dict, message: str
) -> None:
    base = parse_schedule(_one_game_schedule(778443, date(2025, 4, 6)))[0]
    destination_start = datetime(2025, 4, 6, 17, 35, tzinfo=timezone.utc)
    source = replace(
        base,
        scheduled_start_utc=datetime(2025, 4, 5, 20, 10, tzinfo=timezone.utc),
        reschedule_date_utc=destination_start,
    )
    destination = replace(
        base,
        scheduled_start_utc=destination_start,
        rescheduled_from_date=date(2025, 4, 5),
        **destination_changes,
    )

    with pytest.raises(ValueError, match=message):
        reconcile_schedule_games((source, destination))


def test_schedule_history_reconciliation_rejects_unlinked_or_misdirected_records() -> None:
    base = parse_schedule(_one_game_schedule(778443, date(2025, 4, 6)))[0]
    original = replace(
        base,
        scheduled_start_utc=datetime(2025, 4, 5, 20, 10, tzinfo=timezone.utc),
    )
    destination = replace(
        base,
        scheduled_start_utc=datetime(2025, 4, 6, 17, 35, tzinfo=timezone.utc),
    )
    with pytest.raises(ValueError, match="without one unambiguous"):
        reconcile_schedule_games((original, destination))

    source = replace(
        original,
        reschedule_date_utc=datetime(2025, 4, 6, 18, 0, tzinfo=timezone.utc),
    )
    destination = replace(destination, rescheduled_from_date=date(2025, 4, 5))
    with pytest.raises(ValueError, match="target does not match"):
        reconcile_schedule_games((source, destination))


def test_client_uses_deterministic_schedule_cache_and_filters_nonfinal(tmp_path: Path) -> None:
    session = _FakeSession([_FakeResponse(_fixture("mlb_schedule.json"))])
    client = MlbApiClient(tmp_path, session=session, backoff_seconds=0)

    first = client.schedule_games(date(2025, 7, 4), date(2025, 7, 5))
    second = client.schedule_games(
        date(2025, 7, 4), date(2025, 7, 5), include_nonfinal_for_audit=True
    )

    assert [game.game_pk for game in first] == [1001, 1004, 1005]
    assert len(second) == 5
    assert len(session.calls) == 1
    assert session.calls[0][1]["hydrate"] == "linescore"
    cache_file = tmp_path / "schedule" / "2025-07-04_2025-07-05_linescore.json"
    assert cache_file.exists()
    assert cache_file.read_text(encoding="utf-8").endswith("\n")
    assert cache_file.read_text(encoding="utf-8").startswith('{"dates":')


def test_client_chunks_long_schedule_range_and_deduplicates_boundary_game(
    tmp_path: Path,
) -> None:
    start = date(2025, 4, 2)
    end = date(2026, 6, 21)
    first_boundary = date(2025, 9, 28)
    duplicate = _one_game_schedule(2002, first_boundary)["dates"][0]["games"][0]

    first = _one_game_schedule(2001, start)
    first["dates"].append(
        {"date": first_boundary.isoformat(), "games": [deepcopy(duplicate)]}
    )
    second = _one_game_schedule(2003, date(2026, 1, 1))
    second["dates"].insert(
        0, {"date": first_boundary.isoformat(), "games": [deepcopy(duplicate)]}
    )
    third = _one_game_schedule(2004, end)
    session = _FakeSession(
        [_FakeResponse(first), _FakeResponse(second), _FakeResponse(third)]
    )
    client = MlbApiClient(tmp_path, session=session, backoff_seconds=0)

    games = client.schedule_games(start, end, include_nonfinal_for_audit=True)
    cached_games = client.schedule_games(start, end, include_nonfinal_for_audit=True)

    assert [game.game_pk for game in games] == [2001, 2002, 2003, 2004]
    assert cached_games == games
    assert len(session.calls) == 3
    requested_ranges = [
        (call[1]["startDate"], call[1]["endDate"]) for call in session.calls
    ]
    assert requested_ranges == [
        ("2025-04-02", "2025-09-28"),
        ("2025-09-29", "2026-03-27"),
        ("2026-03-28", "2026-06-21"),
    ]
    assert sorted(path.name for path in (tmp_path / "schedule").iterdir()) == [
        "2025-04-02_2025-09-28_linescore.json",
        "2025-09-29_2026-03-27_linescore.json",
        "2026-03-28_2026-06-21_linescore.json",
    ]


def test_client_retries_transient_http_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session = _FakeSession(
        [_FakeResponse({}, status_code=503), _FakeResponse(_fixture("mlb_live_feed.json"))]
    )
    monkeypatch.setattr("mlb_api.time.sleep", lambda _: None)
    client = MlbApiClient(tmp_path, session=session, max_attempts=2)

    assert client.game_timing(1001).game_pk == 1001
    assert len(session.calls) == 2


def test_client_rejects_live_feed_for_different_game(tmp_path: Path) -> None:
    payload = deepcopy(_fixture("mlb_live_feed.json"))
    session = _FakeSession([_FakeResponse(payload)])
    client = MlbApiClient(tmp_path, session=session)

    with pytest.raises(ValueError, match="Requested game 9999.*returned game 1001"):
        client.game_timing(9999)
