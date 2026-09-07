"""Pure pre-estimation validation for matched MLB moneyline candidates.

This module deliberately does not guess team-name aliases.  Polymarket outcome
labels must appear in the explicit accepted table of observed short nicknames
and full MLB team names after case-folding and collapsing whitespace.
Exclusions remain audit rows; only fully reconciled markets produce an eligible
dimension.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb

from match_games import GameMatchAudit, MLB_TEAMS
from mlb_api import ScheduleGame, winning_mlb_team_id


# Observed Polymarket outcome conventions: 2025 markets use the short club
# nickname and 2026 markets use MLB's full team name.  These are the only
# accepted labels; extending this table requires an explicit reviewed change.
MLB_OUTCOME_LABELS_BY_SLUG: dict[str, tuple[str, str]] = {
    "ari": ("Diamondbacks", "Arizona Diamondbacks"),
    "atl": ("Braves", "Atlanta Braves"),
    "bal": ("Orioles", "Baltimore Orioles"),
    "bos": ("Red Sox", "Boston Red Sox"),
    "chc": ("Cubs", "Chicago Cubs"),
    "cin": ("Reds", "Cincinnati Reds"),
    "cle": ("Guardians", "Cleveland Guardians"),
    "col": ("Rockies", "Colorado Rockies"),
    "cws": ("White Sox", "Chicago White Sox"),
    "det": ("Tigers", "Detroit Tigers"),
    "hou": ("Astros", "Houston Astros"),
    "kc": ("Royals", "Kansas City Royals"),
    "laa": ("Angels", "Los Angeles Angels"),
    "lad": ("Dodgers", "Los Angeles Dodgers"),
    "mia": ("Marlins", "Miami Marlins"),
    "mil": ("Brewers", "Milwaukee Brewers"),
    "min": ("Twins", "Minnesota Twins"),
    "nym": ("Mets", "New York Mets"),
    "nyy": ("Yankees", "New York Yankees"),
    "oak": ("Athletics", "Athletics"),
    "phi": ("Phillies", "Philadelphia Phillies"),
    "pit": ("Pirates", "Pittsburgh Pirates"),
    "sd": ("Padres", "San Diego Padres"),
    "sea": ("Mariners", "Seattle Mariners"),
    "sf": ("Giants", "San Francisco Giants"),
    "stl": ("Cardinals", "St. Louis Cardinals"),
    "tb": ("Rays", "Tampa Bay Rays"),
    "tex": ("Rangers", "Texas Rangers"),
    "tor": ("Blue Jays", "Toronto Blue Jays"),
    "wsh": ("Nationals", "Washington Nationals"),
}


class MoneylineTokenCompositionError(ValueError):
    """Raised before auditing when canonical token inputs cannot be composed."""


@dataclass(frozen=True)
class MoneylineValidationAudit:
    """One validation result for one preliminary market candidate."""

    market_id: str
    matched_game_pk: int | None
    is_eligible: bool
    exclusion_reason: str | None
    unique_token_count: int
    distinct_outcome_count: int
    polymarket_winning_outcome: str | None
    mlb_winning_team_id: int | None


@dataclass(frozen=True)
class EligibleMoneylineMarket:
    """Validated market/game/token mapping consumed by later trade analysis."""

    market_id: str
    game_pk: int
    official_date: date
    away_team_id: int
    away_team_name: str
    home_team_id: int
    home_team_name: str
    away_token_id: str
    home_token_id: str
    winning_team_id: int
    winning_token_id: str
    winning_outcome: str


@dataclass(frozen=True)
class MoneylineValidationResult:
    audits: tuple[MoneylineValidationAudit, ...]
    eligible_markets: tuple[EligibleMoneylineMarket, ...]


def _market_id(row: Mapping[str, Any]) -> str:
    value = row.get("market_id")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Each MLB candidate must have a non-empty string market_id")
    return value.strip()


def _token_id(row: Mapping[str, Any]) -> str | None:
    value = row.get("token_id")
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _normalized_label(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).casefold()
    return normalized or None


def _build_accepted_label_map() -> dict[str, int]:
    if set(MLB_OUTCOME_LABELS_BY_SLUG) != set(MLB_TEAMS):
        raise RuntimeError("MLB accepted outcome-label table must cover all team slugs")
    accepted: dict[str, int] = {}
    for slug, labels in MLB_OUTCOME_LABELS_BY_SLUG.items():
        team_id = MLB_TEAMS[slug].team_id
        for label in labels:
            normalized = _normalized_label(label)
            if normalized is None:
                raise RuntimeError(f"Empty accepted MLB outcome label for {slug}")
            prior = accepted.get(normalized)
            if prior is not None and prior != team_id:
                raise RuntimeError(f"MLB outcome label maps to multiple teams: {label!r}")
            accepted[normalized] = team_id
    return accepted


ACCEPTED_MLB_LABEL_TO_TEAM_ID = _build_accepted_label_map()


def _parquet_relation(path: str | Path, label: str) -> tuple[Path, str]:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} Parquet does not exist: {resolved}")
    escaped = str(resolved).replace("'", "''")
    return resolved, f"read_parquet('{escaped}')"


def _require_columns(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    label: str,
    required: set[str],
) -> None:
    columns = {
        row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }
    missing = sorted(required - columns)
    if missing:
        raise MoneylineTokenCompositionError(
            f"{label} is missing required columns: {missing}"
        )


def _require_nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MoneylineTokenCompositionError(
            f"Relevant canonical token row has null or empty {field}"
        )
    return value.strip()


def load_canonical_token_rows(
    con: duckdb.DuckDBPyConnection,
    universe_tokens_path: str | Path,
    token_map_path: str | Path,
    candidate_market_ids: Iterable[str],
) -> tuple[dict[str, str], ...]:
    """Strictly compose canonical token rows for the requested candidate markets."""

    candidate_ids = tuple(candidate_market_ids)
    if not candidate_ids or any(
        not isinstance(value, str) or not value.strip() for value in candidate_ids
    ):
        raise MoneylineTokenCompositionError(
            "Candidate market IDs for token composition must be non-empty strings"
        )
    candidate_ids = tuple(dict.fromkeys(value.strip() for value in candidate_ids))

    _, universe = _parquet_relation(universe_tokens_path, "universe_tokens")
    _, token_map = _parquet_relation(token_map_path, "token_map")
    _require_columns(
        con,
        universe,
        "universe_tokens",
        {"token_id", "market_id", "winning_outcome"},
    )
    _require_columns(
        con,
        token_map,
        "token_map",
        {"token_id", "condition_id", "outcome"},
    )

    placeholders = ", ".join("?" for _ in candidate_ids)
    relevant = con.execute(
        f"""
        SELECT token_id, market_id, winning_outcome
        FROM {universe}
        WHERE market_id IN ({placeholders})
        ORDER BY market_id, token_id
        """,
        list(candidate_ids),
    ).fetchall()
    if not relevant:
        raise MoneylineTokenCompositionError(
            "No universe_tokens rows exist for the candidate markets; missing candidate "
            f"market IDs: {sorted(candidate_ids)}"
        )

    universe_rows: list[tuple[str, str, str]] = []
    for token_id, market_id, winning_outcome in relevant:
        universe_rows.append(
            (
                _require_nonempty_string(token_id, "universe_tokens.token_id"),
                _require_nonempty_string(market_id, "universe_tokens.market_id"),
                _require_nonempty_string(
                    winning_outcome, "universe_tokens.winning_outcome"
                ),
            )
        )
    universe_token_ids = [row[0] for row in universe_rows]
    observed_market_ids = {row[1] for row in universe_rows}
    missing_market_ids = sorted(set(candidate_ids) - observed_market_ids)
    if missing_market_ids:
        raise MoneylineTokenCompositionError(
            f"universe_tokens is missing candidate market IDs: {missing_market_ids}"
        )
    if len(universe_token_ids) != len(set(universe_token_ids)):
        raise MoneylineTokenCompositionError(
            "Relevant universe_tokens contains duplicate token IDs or rows"
        )

    token_placeholders = ", ".join("?" for _ in universe_token_ids)
    mapped = con.execute(
        f"""
        SELECT token_id, condition_id, outcome
        FROM {token_map}
        WHERE token_id IN ({token_placeholders})
        ORDER BY token_id, condition_id, outcome
        """,
        universe_token_ids,
    ).fetchall()
    token_map_rows: list[tuple[str, str, str]] = []
    for token_id, condition_id, outcome in mapped:
        token_map_rows.append(
            (
                _require_nonempty_string(token_id, "token_map.token_id"),
                _require_nonempty_string(condition_id, "token_map.condition_id"),
                _require_nonempty_string(outcome, "token_map.outcome"),
            )
        )
    mapped_token_ids = [row[0] for row in token_map_rows]
    if len(mapped_token_ids) != len(set(mapped_token_ids)):
        raise MoneylineTokenCompositionError(
            "Relevant token_map contains duplicate token IDs or rows"
        )
    missing = sorted(set(universe_token_ids) - set(mapped_token_ids))
    if missing:
        raise MoneylineTokenCompositionError(
            f"token_map is missing relevant universe token IDs: {missing}"
        )

    token_map_by_id = {row[0]: row for row in token_map_rows}
    composed: list[dict[str, str]] = []
    for token_id, market_id, winning_outcome in universe_rows:
        _, condition_id, outcome = token_map_by_id[token_id]
        if condition_id != market_id:
            raise MoneylineTokenCompositionError(
                f"Token {token_id} condition_id {condition_id!r} does not match "
                f"universe market_id {market_id!r}"
            )
        composed.append(
            {
                "token_id": token_id,
                "market_id": market_id,
                "outcome": outcome,
                "winning_outcome": winning_outcome,
            }
        )
    return tuple(composed)


def _audit(
    market_id: str,
    match: GameMatchAudit | None,
    token_rows: tuple[Mapping[str, Any], ...],
    reason: str,
    *,
    winning_outcome: str | None = None,
    mlb_winner: int | None = None,
) -> MoneylineValidationAudit:
    token_ids = {_token_id(row) for row in token_rows} - {None}
    outcomes = {_normalized_label(row.get("outcome")) for row in token_rows} - {None}
    return MoneylineValidationAudit(
        market_id=market_id,
        matched_game_pk=match.matched_game_pk if match else None,
        is_eligible=False,
        exclusion_reason=reason,
        unique_token_count=len(token_ids),
        distinct_outcome_count=len(outcomes),
        polymarket_winning_outcome=winning_outcome,
        mlb_winning_team_id=mlb_winner,
    )


def _matched_schedule_game(match: GameMatchAudit) -> ScheduleGame | None:
    if len(match.schedule_matches) != 1:
        return None
    game = match.schedule_matches[0]
    if game.game_pk != match.matched_game_pk:
        return None
    return game


def validate_moneylines(
    candidates: Iterable[Mapping[str, Any]],
    match_audits: Iterable[GameMatchAudit],
    token_rows: Iterable[Mapping[str, Any]],
) -> MoneylineValidationResult:
    """Reconcile candidate tokens, labels, and winner against official MLB data."""

    candidates = tuple(candidates)
    candidate_ids = [_market_id(candidate) for candidate in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("Duplicate candidate market IDs in moneyline validation input")
    match_audits = tuple(match_audits)
    token_rows = tuple(token_rows)

    matches_by_market: dict[str, list[GameMatchAudit]] = {}
    for match in match_audits:
        matches_by_market.setdefault(match.market_id, []).append(match)
    tokens_by_market: dict[str, list[Mapping[str, Any]]] = {}
    for row in token_rows:
        market_id = row.get("market_id")
        if isinstance(market_id, str):
            tokens_by_market.setdefault(market_id.strip(), []).append(row)

    audit_rows: list[MoneylineValidationAudit] = []
    eligible: list[EligibleMoneylineMarket] = []
    for candidate in candidates:
        market_id = _market_id(candidate)
        market_tokens = tuple(tokens_by_market.get(market_id, ()))
        matches = matches_by_market.get(market_id, [])
        if not matches:
            audit_rows.append(
                _audit(market_id, None, market_tokens, "missing_match_audit")
            )
            continue
        if len(matches) != 1:
            audit_rows.append(
                _audit(market_id, None, market_tokens, "duplicate_match_audit")
            )
            continue
        match = matches[0]
        if not match.is_matched:
            audit_rows.append(
                _audit(
                    market_id,
                    match,
                    market_tokens,
                    match.exclusion_reason or "unmatched_game",
                )
            )
            continue
        game = _matched_schedule_game(match)
        if game is None:
            audit_rows.append(
                _audit(market_id, match, market_tokens, "invalid_match_audit")
            )
            continue

        token_ids = [_token_id(row) for row in market_tokens]
        unique_tokens = {token_id for token_id in token_ids if token_id is not None}
        if len(unique_tokens) != 2:
            audit_rows.append(
                _audit(market_id, match, market_tokens, "not_exactly_two_unique_tokens")
            )
            continue
        if None in token_ids or len(market_tokens) != 2:
            audit_rows.append(
                _audit(market_id, match, market_tokens, "duplicate_or_invalid_token_rows")
            )
            continue

        normalized_outcomes = [_normalized_label(row.get("outcome")) for row in market_tokens]
        if None in normalized_outcomes or len(set(normalized_outcomes)) != 2:
            audit_rows.append(
                _audit(market_id, match, market_tokens, "not_exactly_two_outcome_labels")
            )
            continue

        normalized_winners = [
            _normalized_label(row.get("winning_outcome")) for row in market_tokens
        ]
        if None in normalized_winners:
            audit_rows.append(
                _audit(market_id, match, market_tokens, "missing_winning_outcome")
            )
            continue
        if len(set(normalized_winners)) != 1:
            audit_rows.append(
                _audit(market_id, match, market_tokens, "contradictory_winning_outcome")
            )
            continue
        normalized_winner = normalized_winners[0]
        if normalized_winner not in normalized_outcomes:
            audit_rows.append(
                _audit(
                    market_id,
                    match,
                    market_tokens,
                    "winning_outcome_not_in_market",
                    winning_outcome=normalized_winner,
                )
            )
            continue

        away_team = match.away_team
        home_team = match.home_team
        if (
            away_team is None
            or home_team is None
            or away_team.team_id != game.away_team_id
            or home_team.team_id != game.home_team_id
        ):
            audit_rows.append(
                _audit(market_id, match, market_tokens, "invalid_match_team_identity")
            )
            continue
        outcome_team_ids = [
            ACCEPTED_MLB_LABEL_TO_TEAM_ID.get(label) for label in normalized_outcomes
        ]
        if None in outcome_team_ids:
            audit_rows.append(
                _audit(market_id, match, market_tokens, "unrecognized_outcome_label")
            )
            continue
        expected_team_ids = {game.away_team_id, game.home_team_id}
        if set(outcome_team_ids) != expected_team_ids:
            audit_rows.append(
                _audit(market_id, match, market_tokens, "outcome_teams_do_not_match_game")
            )
            continue

        try:
            mlb_winner = winning_mlb_team_id(game)
        except ValueError:
            audit_rows.append(
                _audit(market_id, match, market_tokens, "invalid_official_mlb_winner")
            )
            continue

        normalized_winner_team_id = ACCEPTED_MLB_LABEL_TO_TEAM_ID.get(normalized_winner)
        if normalized_winner_team_id is None:
            audit_rows.append(
                _audit(
                    market_id,
                    match,
                    market_tokens,
                    "unrecognized_winning_outcome",
                    winning_outcome=normalized_winner,
                    mlb_winner=mlb_winner,
                )
            )
            continue
        if normalized_winner_team_id not in outcome_team_ids:
            audit_rows.append(
                _audit(
                    market_id,
                    match,
                    market_tokens,
                    "winning_outcome_not_in_market",
                    winning_outcome=normalized_winner,
                    mlb_winner=mlb_winner,
                )
            )
            continue
        row_by_team_id = dict(zip(outcome_team_ids, market_tokens, strict=True))
        away_row = row_by_team_id[game.away_team_id]
        home_row = row_by_team_id[game.home_team_id]
        if normalized_winner_team_id != mlb_winner:
            audit_rows.append(
                _audit(
                    market_id,
                    match,
                    market_tokens,
                    "polymarket_mlb_winner_disagreement",
                    winning_outcome=normalized_winner,
                    mlb_winner=mlb_winner,
                )
            )
            continue

        away_token = _token_id(away_row)
        home_token = _token_id(home_row)
        winning_row = row_by_team_id[normalized_winner_team_id]
        winning_token = _token_id(winning_row)
        assert away_token is not None and home_token is not None and winning_token is not None
        winning_outcome = str(winning_row["outcome"])
        eligible.append(
            EligibleMoneylineMarket(
                market_id=market_id,
                game_pk=game.game_pk,
                official_date=game.official_date,
                away_team_id=game.away_team_id,
                away_team_name=game.away_team_name,
                home_team_id=game.home_team_id,
                home_team_name=game.home_team_name,
                away_token_id=away_token,
                home_token_id=home_token,
                winning_team_id=mlb_winner,
                winning_token_id=winning_token,
                winning_outcome=winning_outcome,
            )
        )
        audit_rows.append(
            MoneylineValidationAudit(
                market_id=market_id,
                matched_game_pk=game.game_pk,
                is_eligible=True,
                exclusion_reason=None,
                unique_token_count=2,
                distinct_outcome_count=2,
                polymarket_winning_outcome=winning_outcome,
                mlb_winning_team_id=mlb_winner,
            )
        )

    assert_unique_eligible_assignments(eligible)
    return MoneylineValidationResult(tuple(audit_rows), tuple(eligible))


def assert_unique_eligible_assignments(
    markets: Iterable[EligibleMoneylineMarket],
) -> None:
    """Fail unless eligible market, game, and token assignments are one-to-one."""

    markets = tuple(markets)
    market_ids = [market.market_id for market in markets]
    if len(market_ids) != len(set(market_ids)):
        raise ValueError("Eligible MLB moneyline market assignments are not unique")
    game_ids = [market.game_pk for market in markets]
    if len(game_ids) != len(set(game_ids)):
        raise ValueError("Eligible MLB moneyline game assignments are not unique")
    token_ids = [
        token_id
        for market in markets
        for token_id in (market.away_token_id, market.home_token_id)
    ]
    if any(market.away_token_id == market.home_token_id for market in markets) or len(
        token_ids
    ) != len(set(token_ids)):
        raise ValueError("Eligible MLB moneyline token assignments are not unique")
