from __future__ import annotations

import copy
import hashlib
import json
import shutil
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd

from analysis.nfl_game_dynamics.artifact_manifest import (
    ArtifactManifestError,
    file_fingerprint,
)
from analysis.nfl_game_dynamics.build_game_timing import (
    TimingBuildError,
    build_from_payloads,
    matched_summary_game_ids,
    verify_timing_run,
)
from analysis.nfl_game_dynamics.build_market_universe import (
    build_market_universe,
    verify_market_universe_run,
)
from analysis.nfl_game_dynamics.build_validated_universe import (
    ValidatedUniverseBuildError,
    build_validated_universe,
    verify_validated_run,
)
from analysis.nfl_game_dynamics.match_games import (
    NFL_TEAM_ALIASES,
    NFL_TEAMS,
    match_market_candidates,
)
from analysis.nfl_game_dynamics.nfl_api import (
    ADMINISTRATIVE_PLAY_TYPES,
    AUDITED_ESPN_GAME_IDS,
    COMPETITIVE_PLAY_TYPES,
    NFL_ANALYSIS_PHASES,
    NFL_PHASE_CONTRACT_PATH,
    NFL_PHASE_CONTRACT_SHA256,
    NFL_TAXONOMY_AUDIT_PATH,
    NFL_TAXONOMY_AUDIT_SHA256,
    POINT_AFTER_TYPES,
    EspnNflClient,
    assign_phase,
    parse_game_timing,
    parse_scoreboard,
    validate_schedule_timing,
)
from analysis.nfl_game_dynamics.validate_moneylines import validate_moneylines


ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


def _payload(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _market(market_id: str, slug: str, question: str, *, tokens: int = 2) -> dict:
    return {
        "market_id": market_id,
        "event_slug": slug,
        "question": question,
        "n_tokens": tokens,
        "n_trades_raw": 100,
        "n_buy_filtered": 60,
        "usd_buy_filtered": 1200.0,
        "first_trade_at": pd.Timestamp("2025-01-01T00:00:00Z"),
        "last_trade_at": pd.Timestamp("2025-01-06T05:00:00Z"),
    }


def _write_frame(frame: pd.DataFrame, path: Path) -> None:
    con = duckdb.connect()
    try:
        con.register("frame", frame)
        quoted = str(path).replace("'", "''")
        con.execute(f"COPY frame TO '{quoted}' (FORMAT PARQUET)")
    finally:
        con.close()


class NflUniverseTests(unittest.TestCase):
    def test_exact_versus_selection_excludes_parlays_special_titles_and_bad_tokens(self) -> None:
        frame = pd.DataFrame([
            _market("moneyline", "nfl-la-lv-2025-01-05", "Rams vs. Raiders"),
            _market("parlay", "nfl-la-lv-2025-01-05", "Rams Parlay - Rams win, Stafford 200+ yards"),
            _market("colon", "nfl-sea-sf-2025-01-05", "Seahawks vs. 49ers: Spread"),
            _market("super-bowl-title", "nfl-kc-phi-2025-02-09", "Super Bowl LIX Winner"),
            _market("one-token", "nfl-buf-ne-2025-01-05", "Bills vs. Patriots", tokens=1),
            _market("season", "nfl-mvp-1", "Will someone win NFL MVP?"),
        ])
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.parquet"
            run = Path(directory) / "stage1"
            _write_frame(frame, source)
            con = duckdb.connect()
            con.execute(f"CREATE VIEW markets AS SELECT * FROM read_parquet('{source}')")
            stats = build_market_universe(con, "markets", source, run)
            selected = con.execute(f"SELECT * FROM read_parquet('{run / 'candidate_markets.parquet'}')").fetchdf()
            reasons = dict(con.execute(
                f"SELECT market_id,exclusion_reason FROM read_parquet('{run / 'candidate_diagnostics.parquet'}')"
            ).fetchall())
            with self.assertRaises(FileExistsError):
                build_market_universe(con, "markets", source, run)
            con.close()
            self.assertEqual(verify_market_universe_run(run), stats)
            self.assertFalse(any(Path(directory).glob(".stage1.staging-*")))
            manifest_path = run / "market_universe_manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["outputs"]["candidate_markets.parquet"]["sha256"] = "0" * 64
            manifest_path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ArtifactManifestError, "Fingerprint mismatch"):
                verify_market_universe_run(run)
        self.assertEqual(stats, {"candidate_markets": 1, "diagnostic_rows": 6, "excluded_rows": 5})
        self.assertEqual(selected["market_id"].tolist(), ["moneyline"])
        self.assertEqual((selected.loc[0, "team_1_slug"], selected.loc[0, "team_2_slug"]), ("la", "lv"))
        self.assertEqual(reasons["parlay"], "question_not_team_versus")
        self.assertEqual(reasons["colon"], "question_contains_colon")
        self.assertEqual(reasons["super-bowl-title"], "question_not_team_versus")
        self.assertEqual(reasons["one-token"], "not_exactly_two_tokens")
        self.assertEqual(reasons["season"], "slug_pattern_mismatch")


class NflTimingTests(unittest.TestCase):
    def test_schedule_date_aliases_ordered_and_reversed_matching(self) -> None:
        game = parse_scoreboard(_payload("nfl_scoreboard.json"))[0]
        self.assertEqual(game.official_date.isoformat(), "2025-01-05")
        self.assertEqual(NFL_TEAM_ALIASES["la"], "lar")
        self.assertEqual(NFL_TEAM_ALIASES["las"], "lv")
        self.assertEqual(NFL_TEAM_ALIASES["was"], "wsh")
        official = match_market_candidates([
            {"market_id": "o", "date": "2025-01-05", "team_1_slug": "lv", "team_2_slug": "la"}
        ], [game])[0]
        reversed_row = match_market_candidates([
            {"market_id": "r", "date": "2025-01-05", "team_1_slug": "la", "team_2_slug": "lv"}
        ], [game])[0]
        self.assertEqual(official.slug_orientation, "official")
        self.assertEqual(reversed_row.slug_orientation, "reversed")
        self.assertEqual(reversed_row.away_team.team_id, NFL_TEAMS["lv"].team_id)
        self.assertEqual(reversed_row.home_team.team_id, NFL_TEAMS["lar"].team_id)

    def test_summary_fetch_spine_contains_only_exact_candidate_matches(self) -> None:
        scoreboard = _payload("nfl_scoreboard.json")
        unrelated = copy.deepcopy(scoreboard["events"][0])
        unrelated["id"] = "9999"
        unrelated["competitions"][0]["id"] = "9999"
        unrelated["competitions"][0]["competitors"][0]["team"] = {
            "id": "2", "abbreviation": "BUF", "displayName": "Buffalo Bills"
        }
        unrelated["competitions"][0]["competitors"][1]["team"] = {
            "id": "17", "abbreviation": "NE", "displayName": "New England Patriots"
        }
        scoreboard["events"].append(unrelated)
        candidate = {
            "market_id": "m1", "date": "2025-01-05",
            "team_1_slug": "la", "team_2_slug": "lv",
        }
        self.assertEqual(matched_summary_game_ids([candidate], [scoreboard]), ("9001",))

    def test_competitive_wallclocks_numeric_sequence_admin_exclusion_and_overtime(self) -> None:
        timing = parse_game_timing(_payload("nfl_summary.json"), "9001")
        self.assertEqual(timing.competitive_play_count, 6)
        self.assertEqual(timing.actual_start_utc.isoformat(), "2025-01-06T01:20:00+00:00")
        self.assertEqual(timing.period_2_start_utc.isoformat(), "2025-01-06T02:00:00+00:00")
        self.assertEqual(timing.period_4_start_utc.isoformat(), "2025-01-06T04:00:00+00:00")
        self.assertEqual(timing.actual_end_utc.isoformat(), "2025-01-06T05:00:00+00:00")
        self.assertEqual(timing.final_period, 5)
        self.assertTrue(timing.went_to_overtime)

    def test_shortened_and_competitive_timestamp_corruption_fail_closed(self) -> None:
        shortened = _payload("nfl_summary.json")
        shortened["drives"]["previous"][0]["plays"] = [
            row for row in shortened["drives"]["previous"][0]["plays"]
            if row["period"]["number"] < 4
        ]
        with self.assertRaisesRegex(ValueError, "complete Q1-Q4"):
            parse_game_timing(shortened)
        corrupt = _payload("nfl_summary.json")
        for row in corrupt["drives"]["previous"][0]["plays"]:
            if row["id"] == "p200":
                row["wallclock"] = "2025-01-06T01:00:00Z"
        with self.assertRaisesRegex(ValueError, "timestamps are not chronological"):
            parse_game_timing(corrupt)
        taxonomy = _payload("nfl_summary.json")
        for row in taxonomy["drives"]["previous"][0]["plays"]:
            if row["id"] == "admin-q1":
                row["type"]["text"] = "New Meaning"
        with self.assertRaisesRegex(ValueError, "changed meaning"):
            parse_game_timing(taxonomy)
        unknown = _payload("nfl_summary.json")
        for row in unknown["drives"]["previous"][0]["plays"]:
            if row["id"] == "p30":
                row["type"] = {"id": "999", "text": "New Play Type"}
        with self.assertRaisesRegex(ValueError, "Unreviewed competitive play taxonomy"):
            parse_game_timing(unknown)
        point_after_mismatch = _payload("nfl_summary.json")
        for row in point_after_mismatch["drives"]["previous"][0]["plays"]:
            if row["id"] == "p30":
                row["pointAfterAttempt"]["id"] = 16
        with self.assertRaisesRegex(ValueError, "Unreviewed point-after taxonomy"):
            parse_game_timing(point_after_mismatch)

    def test_prefix_suffix_ot_status_winner_and_sequence_truncations_fail_closed(self) -> None:
        prefix = _payload("nfl_summary.json")
        prefix["drives"]["previous"][0]["plays"] = [
            row for row in prefix["drives"]["previous"][0]["plays"] if row["id"] != "p10"
        ]
        with self.assertRaisesRegex(ValueError, "opening kickoff"):
            parse_game_timing(prefix)

        suffix = _payload("nfl_summary.json")
        suffix["drives"]["previous"][0]["plays"] = [
            row for row in suffix["drives"]["previous"][0]["plays"] if row["id"] != "admin-game"
        ]
        with self.assertRaisesRegex(ValueError, "terminal End of Game"):
            parse_game_timing(suffix)

        truncated_ot = _payload("nfl_summary.json")
        truncated_ot["drives"]["previous"][0]["plays"] = [
            row for row in truncated_ot["drives"]["previous"][0]["plays"] if row["id"] != "p400"
        ]
        with self.assertRaisesRegex(ValueError, "status implies period 5"):
            parse_game_timing(truncated_ot)

        incomplete = _payload("nfl_summary.json")
        incomplete["header"]["competitions"][0]["status"]["type"]["completed"] = False
        with self.assertRaisesRegex(ValueError, "requires a completed game"):
            parse_game_timing(incomplete)

        bad_winner = _payload("nfl_summary.json")
        bad_winner["header"]["competitions"][0]["competitors"][1]["winner"] = True
        with self.assertRaisesRegex(ValueError, "exactly one true"):
            parse_game_timing(bad_winner)
        nonboolean_winner = _payload("nfl_summary.json")
        nonboolean_winner["header"]["competitions"][0]["competitors"][0]["winner"] = 1
        with self.assertRaisesRegex(ValueError, "two boolean winner flags"):
            parse_game_timing(nonboolean_winner)

        nonintegral = _payload("nfl_summary.json")
        for row in nonintegral["drives"]["previous"][0]["plays"]:
            if row["id"] == "p30":
                row["sequenceNumber"] = 30.5
        with self.assertRaisesRegex(ValueError, "non-integral"):
            parse_game_timing(nonintegral)

    def test_fractional_espn_integral_fields_fail_closed(self) -> None:
        scoreboard_team = _payload("nfl_scoreboard.json")
        scoreboard_team["events"][0]["competitions"][0]["competitors"][0]["team"]["id"] = 26.5
        with self.assertRaisesRegex(ValueError, "non-integral"):
            parse_scoreboard(scoreboard_team)

        scoreboard_score = _payload("nfl_scoreboard.json")
        scoreboard_score["events"][0]["competitions"][0]["competitors"][0]["score"] = 13.5
        with self.assertRaisesRegex(ValueError, "non-integral"):
            parse_scoreboard(scoreboard_score)

        summary_team = _payload("nfl_summary.json")
        summary_team["header"]["competitions"][0]["competitors"][0]["team"]["id"] = 26.5
        with self.assertRaisesRegex(ValueError, "non-integral"):
            parse_game_timing(summary_team)

        summary_period = _payload("nfl_summary.json")
        summary_period["header"]["competitions"][0]["status"]["period"] = 5.5
        with self.assertRaisesRegex(ValueError, "non-integral"):
            parse_game_timing(summary_period)

        play_period = _payload("nfl_summary.json")
        play_period["drives"]["previous"][0]["plays"][2]["period"]["number"] = 1.5
        with self.assertRaisesRegex(ValueError, "non-integral"):
            parse_game_timing(play_period)

    def test_scoreboard_and_summary_identity_result_and_final_period_reconcile(self) -> None:
        game = parse_scoreboard(_payload("nfl_scoreboard.json"))[0]
        summary = _payload("nfl_summary.json")
        validate_schedule_timing(game, parse_game_timing(summary))

        bad_scoreboard_id = _payload("nfl_scoreboard.json")
        bad_scoreboard_id["events"][0]["competitions"][0]["id"] = "other"
        with self.assertRaisesRegex(ValueError, "game IDs disagree"):
            parse_scoreboard(bad_scoreboard_id)

        bad_summary_id = _payload("nfl_summary.json")
        bad_summary_id["header"]["competitions"][0]["id"] = "other"
        with self.assertRaisesRegex(ValueError, "game IDs disagree"):
            parse_game_timing(bad_summary_id)

        mutations = (
            ("team", lambda value: value["header"]["competitions"][0]["competitors"][1]["team"].update(id="2")),
            ("score", lambda value: value["header"]["competitions"][0]["competitors"][1].update(score="19")),
        )
        for label, mutate in mutations:
            with self.subTest(label=label):
                changed = _payload("nfl_summary.json")
                mutate(changed)
                with self.assertRaisesRegex(ValueError, "Scoreboard/summary mismatch"):
                    validate_schedule_timing(game, parse_game_timing(changed))

        regulation_scoreboard = _payload("nfl_scoreboard.json")
        regulation_scoreboard["events"][0]["competitions"][0]["status"]["type"]["detail"] = "Final"
        regulation_game = parse_scoreboard(regulation_scoreboard)[0]
        with self.assertRaisesRegex(ValueError, "Scoreboard/summary mismatch"):
            validate_schedule_timing(regulation_game, parse_game_timing(summary))

    def test_frozen_contract_and_taxonomy_audit_cover_parser_union(self) -> None:
        self.assertEqual(
            NFL_ANALYSIS_PHASES,
            ("pregame", "quarter_1", "quarter_2", "quarter_3", "quarter_4_plus"),
        )
        self.assertEqual(
            hashlib.sha256(NFL_PHASE_CONTRACT_PATH.read_bytes()).hexdigest(),
            NFL_PHASE_CONTRACT_SHA256,
        )
        self.assertEqual(
            hashlib.sha256(NFL_TAXONOMY_AUDIT_PATH.read_bytes()).hexdigest(),
            NFL_TAXONOMY_AUDIT_SHA256,
        )
        taxonomy = json.loads(NFL_TAXONOMY_AUDIT_PATH.read_text())
        self.assertEqual(taxonomy["audited_game_ids"], list(AUDITED_ESPN_GAME_IDS))
        self.assertEqual(taxonomy["competitive_play_types"], COMPETITIVE_PLAY_TYPES)
        self.assertEqual(taxonomy["administrative_play_types"], ADMINISTRATIVE_PLAY_TYPES)
        self.assertEqual(taxonomy["point_after_types"], POINT_AFTER_TYPES)

    def test_raw_cache_refresh_refuses_different_content(self) -> None:
        class Response:
            def __init__(self, payload: dict) -> None:
                self.payload = payload

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return self.payload

        class Session:
            def __init__(self, payload: dict) -> None:
                self.payload = payload

            def get(self, *args, **kwargs) -> Response:
                return Response(self.payload)

        with tempfile.TemporaryDirectory() as directory:
            session = Session({"events": []})
            client = EspnNflClient(Path(directory), session=session)
            client.scoreboard(datetime(2025, 1, 5).date())
            path = client.scoreboard_cache_path(datetime(2025, 1, 5).date())
            original = path.read_bytes()
            session.payload = {"events": [{"id": "changed"}]}
            with self.assertRaisesRegex(ValueError, "Immutable ESPN cache collision"):
                client.scoreboard(datetime(2025, 1, 5).date(), refresh=True)
            self.assertEqual(path.read_bytes(), original)

    def test_client_retry_after_jitter_success_pacing_and_nontransient_failure(self) -> None:
        class Response:
            def __init__(self, status: int, payload: dict, retry_after: str | None = None) -> None:
                self.status_code = status
                self.payload = payload
                self.headers = {} if retry_after is None else {"Retry-After": retry_after}

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    import requests
                    raise requests.HTTPError("request failed", response=self)

            def json(self) -> dict:
                return self.payload

        class Session:
            def __init__(self, responses: list[Response]) -> None:
                self.responses = responses
                self.calls = 0

            def get(self, *args, **kwargs) -> Response:
                self.calls += 1
                return self.responses.pop(0)

        with tempfile.TemporaryDirectory() as directory:
            session = Session([
                Response(429, {}, "3"), Response(200, {"events": []}),
            ])
            client = EspnNflClient(
                Path(directory), session=session, max_attempts=2,
                backoff_seconds=.5, success_pause_seconds=.1, jitter_seconds=.25,
            )
            sleeps: list[float] = []
            with patch(
                "analysis.nfl_game_dynamics.nfl_api.random.uniform",
                side_effect=lambda low, high: high,
            ), patch(
                "analysis.nfl_game_dynamics.nfl_api.time.sleep", side_effect=sleeps.append
            ):
                self.assertEqual(
                    client.scoreboard(datetime(2025, 1, 5).date()), {"events": []}
                )
                self.assertEqual(sleeps, [3.25, .35])
                client.scoreboard(datetime(2025, 1, 5).date())
                self.assertEqual(sleeps, [3.25, .35])

            failed = Session([Response(404, {})])
            client = EspnNflClient(Path(directory) / "other", session=failed, max_attempts=3)
            with self.assertRaisesRegex(Exception, "request failed"):
                client.scoreboard(datetime(2025, 1, 6).date())
            self.assertEqual(failed.calls, 1)

    def test_literal_final_equality_and_inclusive_thirty_second_sensitivity(self) -> None:
        timing = parse_game_timing(_payload("nfl_summary.json"))
        self.assertEqual(assign_phase(timing.period_2_start_utc, timing).phase, "quarter_2")
        final = assign_phase(timing.actual_end_utc, timing)
        self.assertEqual(final.phase, "quarter_4_plus")
        self.assertTrue(final.analysis_eligible)
        self.assertTrue(final.exclude_within_30s)
        self.assertEqual(assign_phase(timing.actual_end_utc + timedelta(seconds=1), timing).phase, "post_final")
        self.assertTrue(assign_phase(timing.actual_start_utc - timedelta(seconds=30), timing).exclude_within_30s)
        self.assertFalse(assign_phase(timing.actual_start_utc - timedelta(seconds=31), timing).exclude_within_30s)


class NflValidationAndArtifactTests(unittest.TestCase):
    def test_token_and_winner_validation(self) -> None:
        game = parse_scoreboard(_payload("nfl_scoreboard.json"))[0]
        candidate = {"market_id": "m1", "date": "2025-01-05", "team_1_slug": "la", "team_2_slug": "lv"}
        match = match_market_candidates([candidate], [game])
        tokens = [
            {"market_id": "m1", "token_id": "away", "outcome": "Raiders", "winning_outcome": "Rams"},
            {"market_id": "m1", "token_id": "home", "outcome": "Rams", "winning_outcome": "Rams"},
        ]
        valid = validate_moneylines([candidate], match, tokens)
        self.assertTrue(valid.audits[0].is_valid)
        self.assertEqual(valid.eligible_markets[0].home_token_id, "home")
        mismatch = copy.deepcopy(tokens)
        for row in mismatch:
            row["winning_outcome"] = "Raiders"
        invalid = validate_moneylines([candidate], match, mismatch)
        self.assertEqual(invalid.audits[0].exclusion_reason, "polymarket_nfl_winner_disagreement")

    def test_atomic_staged_timing_to_validated_round_trip_with_nullable_fields(self) -> None:
        scoreboard = _payload("nfl_scoreboard.json")
        summary = _payload("nfl_summary.json")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = pd.DataFrame([
                _market("m1", "nfl-la-lv-2025-01-05", "Rams vs. Raiders"),
                _market("parlay", "nfl-la-lv-2025-01-05", "Rams Parlay - Rams win, Stafford 200+ yards"),
            ])
            source_path = root / "source_markets.parquet"
            _write_frame(source, source_path)
            con = duckdb.connect()
            con.execute(f"CREATE VIEW markets AS SELECT * FROM read_parquet('{source_path}')")
            stage1 = root / "stage1"
            build_market_universe(con, "markets", source_path, stage1)
            candidates = stage1 / "candidate_markets.parquet"
            candidate_rows = con.execute(f"SELECT * FROM read_parquet('{candidates}')").fetchdf().to_dict("records")
            con.close()

            wrong_rows = copy.deepcopy(candidate_rows)
            wrong_rows[0]["team_1_slug"] = "sea"
            with self.assertRaisesRegex(TimingBuildError, "fingerprinted candidate source"):
                build_from_payloads(
                    wrong_rows,
                    [scoreboard],
                    {"9001": summary},
                    root / "mismatched_candidates",
                    candidate_source=candidates,
                    scoreboard_sources=[FIXTURES / "nfl_scoreboard.json"],
                    summary_sources={"9001": FIXTURES / "nfl_summary.json"},
                )
            self.assertFalse((root / "mismatched_candidates").exists())

            timing_run = root / "timing"
            timing_summary = build_from_payloads(
                candidate_rows,
                [scoreboard],
                {"9001": summary},
                timing_run,
                candidate_source=candidates,
                scoreboard_sources=[FIXTURES / "nfl_scoreboard.json"],
                summary_sources={"9001": FIXTURES / "nfl_summary.json"},
            )
            self.assertEqual(timing_summary["timing_games"], 1)
            self.assertEqual(timing_summary["analysis_phases"], list(NFL_ANALYSIS_PHASES))
            self.assertEqual(verify_timing_run(timing_run), timing_summary)
            self.assertFalse(any(root.glob(".timing.staging-*")))
            timing_hash = hashlib.sha256((timing_run / "summary.json").read_bytes()).hexdigest()
            with self.assertRaises(FileExistsError):
                build_from_payloads(
                    candidate_rows,
                    [scoreboard],
                    {"9001": summary},
                    timing_run,
                    candidate_source=candidates,
                    scoreboard_sources=[FIXTURES / "nfl_scoreboard.json"],
                    summary_sources={"9001": FIXTURES / "nfl_summary.json"},
                )
            self.assertEqual(hashlib.sha256((timing_run / "summary.json").read_bytes()).hexdigest(), timing_hash)

            universe_tokens = root / "universe_tokens.parquet"
            token_map = root / "token_map.parquet"
            _write_frame(pd.DataFrame([
                {"token_id": "away", "market_id": "m1", "winning_outcome": "Rams"},
                {"token_id": "home", "market_id": "m1", "winning_outcome": "Rams"},
            ]), universe_tokens)
            _write_frame(pd.DataFrame([
                {"token_id": "away", "condition_id": "m1", "outcome": "Raiders"},
                {"token_id": "home", "condition_id": "m1", "outcome": "Rams"},
            ]), token_map)
            with self.assertRaisesRegex(ValidatedUniverseBuildError, "overlaps an input"):
                build_validated_universe(
                    candidates, timing_run, universe_tokens, token_map, candidates
                )
            validated_run = root / "validated"
            result = build_validated_universe(candidates, timing_run, universe_tokens, token_map, validated_run)
            self.assertEqual(result["eligible_moneylines"], 1)
            self.assertEqual(result["analysis_phases"], list(NFL_ANALYSIS_PHASES))
            self.assertEqual(verify_validated_run(validated_run), result)
            self.assertFalse(any(root.glob(".validated.staging-*")))
            check = duckdb.connect()
            eligible = check.execute(
                f"SELECT slug_orientation,game_id,final_period,went_to_overtime,week,"
                f"home_won,phase_contract_sha256,period_2_start_utc,period_3_start_utc,"
                f"period_4_start_utc "
                f"FROM read_parquet('{validated_run / 'eligible_moneylines.parquet'}')"
            ).fetchone()
            check.close()
            self.assertEqual(eligible[:4], ("reversed", "9001", 5, True))
            self.assertIsNone(eligible[4])
            self.assertTrue(eligible[5])
            self.assertEqual(eligible[6], NFL_PHASE_CONTRACT_SHA256)
            self.assertTrue(eligible[7] < eligible[8] < eligible[9])
            with self.assertRaises(FileExistsError):
                build_validated_universe(
                    candidates, timing_run, universe_tokens, token_map, validated_run
                )

            mixed_timing = root / "timing_mixed_contract_sha"
            shutil.copytree(timing_run, mixed_timing)
            timing_path = mixed_timing / "game_timing.parquet"
            mixed_path = root / "mixed_timing.parquet"
            con = duckdb.connect()
            con.execute(
                f"COPY (SELECT * FROM read_parquet('{timing_path}') UNION ALL "
                f"SELECT * REPLACE ('wrong'::VARCHAR AS phase_contract_sha256) "
                f"FROM read_parquet('{timing_path}') UNION ALL "
                f"SELECT * REPLACE (NULL::VARCHAR AS phase_contract_sha256) "
                f"FROM read_parquet('{timing_path}')) TO '{mixed_path}' (FORMAT PARQUET)"
            )
            con.close()
            timing_path.unlink()
            mixed_path.rename(timing_path)
            mixed_manifest_path = mixed_timing / "timing_manifest.json"
            mixed_manifest = json.loads(mixed_manifest_path.read_text())
            mixed_manifest["outputs"]["game_timing.parquet"] = file_fingerprint(
                timing_path, relative_to=mixed_timing
            )
            mixed_manifest_path.write_text(json.dumps(mixed_manifest))
            with self.assertRaisesRegex(ArtifactManifestError, "Every NFL timing row"):
                verify_timing_run(mixed_timing)

            mixed_validated = root / "validated_mixed_contract_sha"
            shutil.copytree(validated_run, mixed_validated)
            eligible_path = mixed_validated / "eligible_moneylines.parquet"
            mixed_path = root / "mixed_eligible.parquet"
            con = duckdb.connect()
            con.execute(
                f"COPY (SELECT * FROM read_parquet('{eligible_path}') UNION ALL "
                f"SELECT * REPLACE ('wrong'::VARCHAR AS phase_contract_sha256) "
                f"FROM read_parquet('{eligible_path}') UNION ALL "
                f"SELECT * REPLACE (NULL::VARCHAR AS phase_contract_sha256) "
                f"FROM read_parquet('{eligible_path}')) TO '{mixed_path}' (FORMAT PARQUET)"
            )
            con.close()
            eligible_path.unlink()
            mixed_path.rename(eligible_path)
            mixed_manifest_path = mixed_validated / "validated_manifest.json"
            mixed_manifest = json.loads(mixed_manifest_path.read_text())
            mixed_manifest["outputs"]["eligible_moneylines.parquet"] = file_fingerprint(
                eligible_path, relative_to=mixed_validated
            )
            mixed_manifest_path.write_text(json.dumps(mixed_manifest))
            with self.assertRaisesRegex(ArtifactManifestError, "Every NFL eligible row"):
                verify_validated_run(mixed_validated)

            bad_timing_phases = root / "timing_bad_analysis_phases"
            shutil.copytree(timing_run, bad_timing_phases)
            summary_path = bad_timing_phases / "summary.json"
            bad_summary = json.loads(summary_path.read_text())
            bad_summary["analysis_phases"].append("post_final")
            summary_path.write_text(json.dumps(bad_summary))
            manifest_path = bad_timing_phases / "timing_manifest.json"
            bad_manifest = json.loads(manifest_path.read_text())
            bad_manifest["outputs"]["summary.json"] = file_fingerprint(
                summary_path, relative_to=bad_timing_phases
            )
            manifest_path.write_text(json.dumps(bad_manifest))
            with self.assertRaisesRegex(ArtifactManifestError, "analysis phases"):
                verify_timing_run(bad_timing_phases)

            bad_validated_phases = root / "validated_bad_analysis_phases"
            shutil.copytree(validated_run, bad_validated_phases)
            summary_path = bad_validated_phases / "summary.json"
            bad_summary = json.loads(summary_path.read_text())
            bad_summary["analysis_phases"].append("post_final")
            summary_path.write_text(json.dumps(bad_summary))
            manifest_path = bad_validated_phases / "validated_manifest.json"
            bad_manifest = json.loads(manifest_path.read_text())
            bad_manifest["outputs"]["summary.json"] = file_fingerprint(
                summary_path, relative_to=bad_validated_phases
            )
            manifest_path.write_text(json.dumps(bad_manifest))
            with self.assertRaisesRegex(ArtifactManifestError, "analysis phases"):
                verify_validated_run(bad_validated_phases)

            validated_manifest = json.loads(
                (validated_run / "validated_manifest.json").read_text()
            )
            validated_manifest["outputs"]["summary.json"]["sha256"] = "0" * 64
            (validated_run / "validated_manifest.json").write_text(
                json.dumps(validated_manifest)
            )
            with self.assertRaisesRegex(ArtifactManifestError, "Fingerprint mismatch"):
                verify_validated_run(validated_run)

            changed_candidates = root / "changed_candidates.parquet"
            con = duckdb.connect()
            con.execute(
                f"COPY (SELECT * REPLACE (n_trades_raw + 1 AS n_trades_raw) "
                f"FROM read_parquet('{candidates}')) TO '{changed_candidates}' (FORMAT PARQUET)"
            )
            con.close()
            with self.assertRaisesRegex(ValidatedUniverseBuildError, "candidate SHA"):
                build_validated_universe(
                    changed_candidates, timing_run, universe_tokens, token_map,
                    root / "candidate_sha_rejected",
                )
            self.assertFalse((root / "candidate_sha_rejected").exists())

            bad_timing = root / "timing_bad"
            shutil.copytree(timing_run, bad_timing)
            match_path = bad_timing / "match_audit.parquet"
            con = duckdb.connect()
            bad_copy = root / "bad_match.parquet"
            con.execute(
                f"COPY (SELECT * REPLACE ('wrong'::VARCHAR AS matched_game_id) "
                f"FROM read_parquet('{match_path}')) TO '{bad_copy}' (FORMAT PARQUET)"
            )
            con.close()
            match_path.unlink()
            bad_copy.rename(match_path)
            bad_manifest_path = bad_timing / "timing_manifest.json"
            bad_manifest = json.loads(bad_manifest_path.read_text())
            bad_manifest["outputs"]["match_audit.parquet"] = file_fingerprint(
                match_path, relative_to=bad_timing
            )
            bad_manifest_path.write_text(json.dumps(bad_manifest))
            with self.assertRaisesRegex(ValidatedUniverseBuildError, "Cross-artifact match mismatch"):
                build_validated_universe(candidates, bad_timing, universe_tokens, token_map, root / "rejected")
            self.assertFalse((root / "rejected").exists())

            manifest = json.loads((timing_run / "timing_manifest.json").read_text())
            manifest["outputs"]["summary.json"]["sha256"] = "0" * 64
            (timing_run / "timing_manifest.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ArtifactManifestError, "Fingerprint mismatch"):
                verify_timing_run(timing_run)
            with self.assertRaisesRegex(ArtifactManifestError, "Fingerprint mismatch"):
                build_validated_universe(
                    candidates, timing_run, universe_tokens, token_map,
                    root / "rejected_manifest",
                )
            self.assertFalse((root / "rejected_manifest").exists())

if __name__ == "__main__":
    unittest.main()
