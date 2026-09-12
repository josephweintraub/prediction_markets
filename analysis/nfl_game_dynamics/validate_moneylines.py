"""Strict token/outcome/result validation for matched NFL moneylines."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb

from .match_games import GameMatchAudit, NFL_TEAMS
from .nfl_api import ScheduleGame


NFL_OUTCOME_LABELS_BY_SLUG: dict[str, tuple[str, ...]] = {
    slug: (team.name.split()[-1], team.name) for slug, team in NFL_TEAMS.items()
}
NFL_OUTCOME_LABELS_BY_SLUG["sf"] = ("49ers", "San Francisco 49ers")
NFL_OUTCOME_LABELS_BY_SLUG["lar"] = ("Rams", "Los Angeles Rams", "LAR")


def _normalize(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).casefold()
    return normalized or None


def _label_map() -> dict[str, int]:
    labels: dict[str, int] = {}
    for slug, accepted in NFL_OUTCOME_LABELS_BY_SLUG.items():
        team_id = NFL_TEAMS[slug].team_id
        for label in accepted:
            normalized = _normalize(label)
            if normalized in labels and labels[normalized] != team_id:
                raise RuntimeError(f"NFL label maps to multiple teams: {label}")
            labels[normalized] = team_id
    return labels


ACCEPTED_NFL_LABEL_TO_TEAM_ID = _label_map()


@dataclass(frozen=True)
class MoneylineValidationAudit:
    market_id: str
    matched_game_id: str | None
    is_valid: bool
    exclusion_reason: str | None
    unique_token_count: int
    distinct_outcome_count: int
    polymarket_winning_outcome: str | None
    nfl_winning_team_id: int | None


@dataclass(frozen=True)
class EligibleMoneylineMarket:
    market_id: str
    game_id: str
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


class MoneylineTokenCompositionError(ValueError):
    pass


def _market_id(row: Mapping[str, Any]) -> str:
    value = row.get("market_id")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Each NFL candidate must have a non-empty market_id")
    return value.strip()


def _token_id(row: Mapping[str, Any]) -> str | None:
    value = row.get("token_id")
    return value.strip() if isinstance(value, str) and value.strip() else None


def load_canonical_token_rows(
    con: duckdb.DuckDBPyConnection,
    universe_tokens_path: str | Path,
    token_map_path: str | Path,
    candidate_market_ids: Iterable[str],
) -> tuple[dict[str, str], ...]:
    """Compose token labels and resolution while failing on missing/duplicate rows."""

    ids = tuple(dict.fromkeys(str(value).strip() for value in candidate_market_ids if str(value).strip()))
    if not ids:
        raise MoneylineTokenCompositionError("Candidate market IDs must be nonempty")
    universe = Path(universe_tokens_path).expanduser().resolve()
    token_map = Path(token_map_path).expanduser().resolve()
    for path, label in ((universe, "universe_tokens"), (token_map, "token_map")):
        if not path.is_file():
            raise FileNotFoundError(f"{label} does not exist: {path}")
    uq = str(universe).replace("'", "''")
    tq = str(token_map).replace("'", "''")
    placeholders = ",".join("?" for _ in ids)
    universe_rows = con.execute(
        f"SELECT token_id,market_id,winning_outcome FROM read_parquet('{uq}') "
        f"WHERE market_id IN ({placeholders}) ORDER BY market_id,token_id", list(ids)
    ).fetchall()
    if any(not isinstance(value, str) or not value.strip() for row in universe_rows for value in row):
        raise MoneylineTokenCompositionError("Relevant universe token row has a null/empty field")
    if len({row[0] for row in universe_rows}) != len(universe_rows):
        raise MoneylineTokenCompositionError("Relevant universe token IDs are duplicated")
    observed = {row[1] for row in universe_rows}
    if observed != set(ids):
        raise MoneylineTokenCompositionError(f"Universe token market coverage mismatch: missing={sorted(set(ids)-observed)}")
    token_ids = [row[0] for row in universe_rows]
    token_placeholders = ",".join("?" for _ in token_ids)
    mapped = con.execute(
        f"SELECT token_id,condition_id,outcome FROM read_parquet('{tq}') "
        f"WHERE token_id IN ({token_placeholders}) ORDER BY token_id", token_ids
    ).fetchall()
    if any(not isinstance(value, str) or not value.strip() for row in mapped for value in row):
        raise MoneylineTokenCompositionError("Relevant token-map row has a null/empty field")
    if len(mapped) != len(token_ids) or len({row[0] for row in mapped}) != len(mapped):
        raise MoneylineTokenCompositionError("Token-map coverage is missing or duplicated")
    by_token = {row[0]: row for row in mapped}
    result = []
    for token_id, market_id, winning_outcome in universe_rows:
        if token_id not in by_token:
            raise MoneylineTokenCompositionError(f"Missing token-map row: {token_id}")
        _, condition_id, outcome = by_token[token_id]
        if condition_id != market_id:
            raise MoneylineTokenCompositionError(f"Token {token_id} condition/market mismatch")
        result.append({"token_id": token_id, "market_id": market_id,
                       "outcome": outcome, "winning_outcome": winning_outcome})
    return tuple(result)


def _official_winner(game: ScheduleGame) -> tuple[int | None, str | None]:
    if game.away_final_score is None or game.home_final_score is None:
        return None, "missing_official_score"
    if game.away_final_score == game.home_final_score:
        return None, "official_tie"
    score_winner = game.away_team_id if game.away_final_score > game.home_final_score else game.home_team_id
    flag_winners = []
    if game.away_is_winner is True:
        flag_winners.append(game.away_team_id)
    if game.home_is_winner is True:
        flag_winners.append(game.home_team_id)
    if len(flag_winners) != 1 or flag_winners[0] != score_winner:
        return None, "official_score_winner_inconsistent"
    return score_winner, None


def _audit(market_id: str, match: GameMatchAudit | None, tokens: tuple[Mapping[str, Any], ...], reason: str,
           *, winner: str | None = None, nfl_winner: int | None = None) -> MoneylineValidationAudit:
    return MoneylineValidationAudit(
        market_id=market_id,
        matched_game_id=match.matched_game_id if match else None,
        is_valid=False,
        exclusion_reason=reason,
        unique_token_count=len({_token_id(row) for row in tokens} - {None}),
        distinct_outcome_count=len({_normalize(row.get("outcome")) for row in tokens} - {None}),
        polymarket_winning_outcome=winner,
        nfl_winning_team_id=nfl_winner,
    )


def validate_moneylines(
    candidates: Iterable[Mapping[str, Any]],
    match_audits: Iterable[GameMatchAudit],
    token_rows: Iterable[Mapping[str, Any]],
) -> MoneylineValidationResult:
    candidates = tuple(candidates)
    ids = [_market_id(row) for row in candidates]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate NFL candidate market IDs")
    matches_by_market: dict[str, list[GameMatchAudit]] = {}
    for match in match_audits:
        matches_by_market.setdefault(match.market_id, []).append(match)
    tokens_by_market: dict[str, list[Mapping[str, Any]]] = {}
    for row in token_rows:
        value = row.get("market_id")
        if isinstance(value, str):
            tokens_by_market.setdefault(value.strip(), []).append(row)
    audits: list[MoneylineValidationAudit] = []
    eligible: list[EligibleMoneylineMarket] = []
    for candidate in candidates:
        market_id = _market_id(candidate)
        tokens = tuple(tokens_by_market.get(market_id, ()))
        matches = matches_by_market.get(market_id, [])
        if len(matches) != 1:
            audits.append(_audit(market_id, None, tokens, "missing_or_duplicate_match_audit"))
            continue
        match = matches[0]
        if not match.is_matched:
            audits.append(_audit(market_id, match, tokens, match.exclusion_reason or "unmatched_game"))
            continue
        if len(match.schedule_matches) != 1 or match.schedule_matches[0].game_id != match.matched_game_id:
            audits.append(_audit(market_id, match, tokens, "invalid_match_audit"))
            continue
        game = match.schedule_matches[0]
        official_winner, official_error = _official_winner(game)
        if official_error:
            audits.append(_audit(market_id, match, tokens, official_error))
            continue
        token_ids = [_token_id(row) for row in tokens]
        if len(tokens) != 2 or None in token_ids or len(set(token_ids)) != 2:
            audits.append(_audit(market_id, match, tokens, "not_exactly_two_unique_tokens"))
            continue
        outcomes = [_normalize(row.get("outcome")) for row in tokens]
        if None in outcomes or len(set(outcomes)) != 2:
            audits.append(_audit(market_id, match, tokens, "not_exactly_two_outcome_labels"))
            continue
        outcome_ids = [ACCEPTED_NFL_LABEL_TO_TEAM_ID.get(value) for value in outcomes]
        if None in outcome_ids:
            audits.append(_audit(market_id, match, tokens, "unrecognized_outcome_label"))
            continue
        if set(outcome_ids) != {game.away_team_id, game.home_team_id}:
            audits.append(_audit(market_id, match, tokens, "outcome_teams_do_not_match_game"))
            continue
        winners = [_normalize(row.get("winning_outcome")) for row in tokens]
        if None in winners:
            audits.append(_audit(market_id, match, tokens, "missing_winning_outcome"))
            continue
        if len(set(winners)) != 1:
            audits.append(_audit(market_id, match, tokens, "contradictory_winning_outcome"))
            continue
        winner_label = winners[0]
        winner_id = ACCEPTED_NFL_LABEL_TO_TEAM_ID.get(winner_label)
        if winner_id is None or winner_label not in outcomes:
            audits.append(_audit(market_id, match, tokens, "invalid_winning_outcome", winner=winner_label,
                                 nfl_winner=official_winner))
            continue
        if winner_id != official_winner:
            audits.append(_audit(market_id, match, tokens, "polymarket_nfl_winner_disagreement",
                                 winner=winner_label, nfl_winner=official_winner))
            continue
        away_index = outcome_ids.index(game.away_team_id)
        home_index = outcome_ids.index(game.home_team_id)
        winner_index = outcome_ids.index(winner_id)
        eligible.append(EligibleMoneylineMarket(
            market_id=market_id, game_id=game.game_id, official_date=game.official_date,
            away_team_id=game.away_team_id, away_team_name=game.away_team_name,
            home_team_id=game.home_team_id, home_team_name=game.home_team_name,
            away_token_id=token_ids[away_index], home_token_id=token_ids[home_index],
            winning_team_id=winner_id, winning_token_id=token_ids[winner_index],
            winning_outcome=str(tokens[winner_index]["winning_outcome"]),
        ))
        audits.append(MoneylineValidationAudit(
            market_id=market_id, matched_game_id=game.game_id, is_valid=True,
            exclusion_reason=None, unique_token_count=2, distinct_outcome_count=2,
            polymarket_winning_outcome=str(tokens[winner_index]["winning_outcome"]),
            nfl_winning_team_id=official_winner,
        ))
    return MoneylineValidationResult(tuple(audits), tuple(eligible))


def assert_unique_eligible_assignments(rows: Iterable[EligibleMoneylineMarket]) -> None:
    rows = tuple(rows)
    for field in ("market_id", "game_id"):
        values = [getattr(row, field) for row in rows]
        if len(values) != len(set(values)):
            raise ValueError(f"Eligible NFL {field} assignments are not unique")
    tokens = [token for row in rows for token in (row.away_token_id, row.home_token_id)]
    if len(tokens) != len(set(tokens)):
        raise ValueError("Eligible NFL token assignments are not unique")
