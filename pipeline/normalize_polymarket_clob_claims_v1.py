"""Pure, lossless CLOB-market to binary-resolution candidate normalization.

One literal CLOB market and its caller-supplied source locator are classified.
Only two canonical full-decimal token IDs, two distinct literal outcome labels,
one Boolean winner=True and one Boolean winner=False, and closed=True produce
two reciprocal claim candidates. No prices, Gamma inference, module classifiers,
event-slug fallbacks, clock parsing, files, APIs or global deduplication are used.

The eventual production publisher must verify complete page/source lineage,
global token uniqueness and cross-provider consistency before admitting a spine.
Quarantines retain the raw record and literal winner flags for that reconciliation;
they are not irreversible scientific exclusions. Claim codes, categories, parsed
clocks and a production sample are deliberately outside this single-record core.
"""
from __future__ import annotations

from copy import deepcopy
import re


UINT256 = 2**256 - 1
TOKEN_ID = re.compile(r"[1-9][0-9]{0,77}")
CONDITION_ID = re.compile(r"0x[0-9a-fA-F]{64}")
CLOCK_FIELDS = (
    "created_at", "createdAt", "start_date", "start_date_iso", "startDate",
    "end_date", "end_date_iso", "endDate", "game_start_time", "closed_time",
    "closedTime", "resolved_at", "resolvedAt", "resolution_time", "resolutionTime",
)
SCOPE = {
    "source_universe_admitted": False,
    "global_token_uniqueness_verified": False,
    "cross_provider_consistency_verified": False,
    "complete_page_lineage_verified": False,
    "analysis_admitted": False,
    "final_sample_admitted": False,
}


class Quarantine(ValueError):
    """One deterministic first-failure class; the caller retains this record."""


def require(condition, reason):
    if not condition:
        raise Quarantine(reason)


def winner_flags(market):
    """Keep explicit values/presence even when the market cannot form a pair."""
    if type(market) is not dict or type(market.get("tokens")) is not list:
        return None
    result = []
    for index, token in enumerate(market["tokens"]):
        if type(token) is dict:
            result.append({"token_index": index, "token_is_object": True,
                "token_id": deepcopy(token.get("token_id")),
                "winner_present": "winner" in token, "winner": deepcopy(token.get("winner"))})
        else:
            result.append({"token_index": index, "token_is_object": False, "raw_token": deepcopy(token)})
    return result


def explicit_event_slug(market):
    fields = [market[key] for key in ("event_slug", "eventSlug") if key in market]
    require(all(value is None or type(value) is str for value in fields), "event_slug_schema")
    require(not fields or all(value == fields[0] for value in fields), "event_slug_alias_conflict")
    return fields[0] if fields else None


def normalize_market(market, source_locator):
    """Return two candidate claims or a fully retained, reasoned quarantine.

    Outcome/condition strings and available clock fields remain literal. In
    particular, market_slug is never promoted to event_slug, end_date_iso is not
    renamed to end_date, and missing created_at is not reconstructed. Additional
    fields are preserved in raw_market, not silently treated as new evidence.
    """
    result = {"schema_version": "polymarket_clob_binary_claim_candidates_v1",
        "status": "quarantined", "waterfall_class": None, "reason": None,
        "claim_candidates": [], "input_markets": 1,
        "candidate_markets": 0, "quarantined_markets": 1,
        "provenance": {"source_locator": deepcopy(source_locator), "raw_market": deepcopy(market),
            "literal_token_winner_flags": winner_flags(market),
            "available_clock_fields": ({key: deepcopy(market[key]) for key in CLOCK_FIELDS if key in market}
                if type(market) is dict else {}),
            "literal_event_slug_fields": ({key: deepcopy(market[key]) for key in ("event_slug", "eventSlug") if key in market}
                if type(market) is dict else {}),
            "resolution_source": "literal_clob_token_winner_booleans",
            "prices_used_for_resolution": False, "module_classifier_applied": False,
            "timestamps_parsed": False}, **SCOPE}
    try:
        require(type(source_locator) is str and bool(source_locator.strip()), "source_locator_missing_or_invalid")
        require(type(market) is dict, "market_not_object")
        require("condition_id" in market, "condition_id_missing")
        condition = market["condition_id"]
        require(type(condition) is str and CONDITION_ID.fullmatch(condition) is not None, "condition_id_invalid")
        require("closed" in market, "closed_missing")
        require(type(market["closed"]) is bool, "closed_not_boolean")
        require("tokens" in market, "tokens_missing")
        tokens = market["tokens"]
        require(type(tokens) is list, "tokens_not_array")
        require(len(tokens) == 2, "nonbinary_token_count")
        if "n_outcomes" in market:
            require(type(market["n_outcomes"]) is int, "n_outcomes_not_integer")
            require(market["n_outcomes"] == 2, "n_outcomes_token_count_conflict")
        ids, labels, wins = [], [], []
        for token in tokens:
            require(type(token) is dict, "token_not_object")
            require("token_id" in token, "token_id_missing")
            token_id = token["token_id"]
            require(type(token_id) is str and TOKEN_ID.fullmatch(token_id) is not None
                and int(token_id) <= UINT256, "token_id_noncanonical_or_out_of_range")
            require("outcome" in token, "outcome_missing")
            label = token["outcome"]
            require(type(label) is str and bool(label.strip()), "outcome_not_nonblank_string")
            require("winner" in token, "winner_missing")
            require(type(token["winner"]) is bool, "winner_not_boolean")
            ids.append(token_id); labels.append(label); wins.append(token["winner"])
        require(ids[0] != ids[1], "duplicate_token_id")
        require(labels[0] != labels[1], "duplicate_outcome_label")
        count = sum(wins)
        require(count > 0, "winner_not_identified")
        require(count == 1, "contradictory_multiple_winners")
        require(market["closed"] is True, "contradictory_open_market_with_winner")
        event_slug = explicit_event_slug(market)
        winning_label = labels[wins.index(True)]
        candidates = []
        for index in (0, 1):
            other = 1-index
            candidates.append({"token_id": ids[index], "condition_id": condition, "market_id": condition,
                "outcome": labels[index], "outcome_label": labels[index],
                "winning_outcome": winning_label, "winning_outcome_label": winning_label,
                "Y": int(wins[index]), "complement_token_id": ids[other], "complement_label": labels[other],
                "n_outcomes": 2, "resolved": True, "event_slug": event_slug, "source_locator": source_locator})
        result.update(status="candidate", waterfall_class="resolved_binary_candidate",
            reason="exact_explicit_boolean_winner_pair", claim_candidates=candidates,
            candidate_markets=1, quarantined_markets=0)
    except Quarantine as error:
        reason = str(error)
        result.update(reason=reason, waterfall_class="quarantined_"+reason)
    return result
