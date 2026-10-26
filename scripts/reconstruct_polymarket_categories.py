"""Frozen category reconstruction and small metadata-only validation helpers.

This is not the lost soft/IDF scorer. There is deliberately no real-data CLI;
production metadata adapters must be reviewed and guarded separately.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
from typing import Iterable, Mapping


CATEGORIES = ("Crypto", "Culture", "Economy", "Esports", "Finance", "Geopolitics",
              "Iran", "Mentions", "Politics", "Sports", "Tech", "Weather")
LABELS = (*CATEGORIES, "Unclassified")
FROZEN_TAG_MAP_SHA256 = "06b72d8582f748fb658464dbba36edbbcc5321b43d6cbb3319b9e4689fe69f10"
RULE = {
    "rule_id": "polymarket_category_reconstruction_v1",
    "categories": list(CATEGORIES),
    "fallback": "Unclassified",
    "tag_map_sha256": FROZEN_TAG_MAP_SHA256,
    "existing_labels": "preserve_exact_prim_including_null; only_absent_rows_reconstructed",
    "tag_identity": "exact_case_sensitive_nonempty_strings; unique_labels; no_trimming",
    "votes": "one_per_unique_known_nonexcluded_tag; unique_plurality_wins",
    "mentions_override": "exact_tag_Mentions",
    "iran_override": "literal_case_sensitive_substring_Iran_in_any_tag",
    "both_overrides": "Unclassified:override_conflict",
    "vote_tie": "Unclassified:vote_tie",
    "no_votes": "Unclassified:no_voting_tags",
    "malformed_tags": "Unclassified:malformed_tags",
    "unknown_tags": "retain_evidence; no_vote; no_refit",
    "input_grain": "unique_Gamma_condition; exact_string_key_join",
    "original_scorer_reproduced": False,
    "uses_outcomes_returns_or_trades": False,
}


class ReconstructionError(ValueError):
    """An input cannot meet the frozen metadata contract."""


def rule_contract() -> dict:
    """Return a detached rule contract with a deterministic content hash."""
    encoded = json.dumps(RULE, sort_keys=True, separators=(",", ":")).encode()
    return {"rule": json.loads(encoded), "rule_sha256": hashlib.sha256(encoded).hexdigest()}


def _key(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or not value.strip():
        raise ReconstructionError(f"invalid_{field}")
    return value


def compile_tag_map(rows: Iterable[Mapping]) -> dict[str, str | None]:
    """Compile explicit map records; null values represent excluded tags."""
    result = {}
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != {"tag", "prim_cat", "excluded"}:
            raise ReconstructionError("tag_map_fields_differ")
        tag = _key(row["tag"], "tag")
        if tag in result:
            raise ReconstructionError("duplicate_tag_map_label")
        category, excluded = row["prim_cat"], row["excluded"]
        if type(excluded) is not bool:
            raise ReconstructionError("invalid_tag_exclusion")
        if (excluded and category is not None) or (not excluded and category not in CATEGORIES):
            raise ReconstructionError("invalid_tag_category")
        result[tag] = None if excluded else category
    return result


def load_frozen_tag_map(path: str | Path) -> dict[str, str | None]:
    """Read only the small frozen configuration, checking its exact bytes."""
    path = Path(path)
    if path.stat().st_size > 1_048_576:
        raise ReconstructionError("tag_map_over_1MiB")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != FROZEN_TAG_MAP_SHA256:
        raise ReconstructionError("frozen_tag_map_hash_differs")
    rows = json.loads(raw)
    if not isinstance(rows, list):
        raise ReconstructionError("tag_map_not_list")
    return compile_tag_map(rows)


def candidate_from_tags(tags: object, tag_map: Mapping[str, str | None]) -> dict:
    """Return the candidate and all tag evidence, without reading a saved label."""
    decision = {"category": "Unclassified", "reason": "no_voting_tags", "votes": {},
                "voting_tags": [], "excluded_tags": [], "unknown_tags": [],
                "overrides": []}
    if tags is None:
        return decision
    if not isinstance(tags, list) or any(not isinstance(t, str) or not t.strip() for t in tags):
        return {**decision, "reason": "malformed_tags"}
    unique = sorted(set(tags))
    votes = Counter()
    for tag in unique:
        if tag not in tag_map:
            decision["unknown_tags"].append(tag)
        elif tag_map[tag] is None:
            decision["excluded_tags"].append(tag)
        else:
            category = tag_map[tag]
            if category not in CATEGORIES:
                raise ReconstructionError("invalid_compiled_tag_category")
            votes[category] += 1
            decision["voting_tags"].append(tag)
    decision["votes"] = {c: votes[c] for c in CATEGORIES if votes[c]}
    mentions = "Mentions" in unique
    iran = any("Iran" in tag for tag in unique)
    decision["overrides"] = [c for c, present in (("Iran", iran), ("Mentions", mentions)) if present]
    if mentions and iran:
        return {**decision, "reason": "override_conflict"}
    if mentions or iran:
        category = "Mentions" if mentions else "Iran"
        return {**decision, "category": category, "reason": "override_" + category.lower()}
    if not votes:
        return decision
    leaders = [c for c in CATEGORIES if votes[c] == max(votes.values())]
    if len(leaders) != 1:
        return {**decision, "reason": "vote_tie"}
    return {**decision, "category": leaders[0], "reason": "unique_plurality"}


def _index(rows: Iterable[Mapping], key: str, value: str) -> dict:
    result = {}
    for row in rows:
        if not isinstance(row, Mapping) or key not in row or value not in row:
            raise ReconstructionError(f"missing_{key}_or_{value}")
        market = _key(row[key], key)
        if market in result:
            raise ReconstructionError(f"duplicate_{key}")
        result[market] = row[value]
    return result


def _inputs(metadata_rows: Iterable[Mapping], accepted_rows: Iterable[Mapping]) -> tuple[dict, dict]:
    metadata = _index(metadata_rows, "condition_id", "tags")
    accepted = _index(accepted_rows, "mkt", "prim")
    if any(label is not None and label not in LABELS for label in accepted.values()):
        raise ReconstructionError("saved_label_outside_frozen_categories")
    return metadata, accepted


def reconstruct_rows(metadata_rows: Iterable[Mapping], accepted_rows: Iterable[Mapping],
                     tag_map: Mapping[str, str | None]) -> list[dict]:
    """Bounded fixture helper: preserve saved labels and fill only absent keys."""
    metadata, accepted = _inputs(metadata_rows, accepted_rows)
    result = []
    for market in sorted(metadata.keys() | accepted.keys()):
        candidate = candidate_from_tags(metadata.get(market), tag_map)
        if market not in metadata:
            candidate["reason"] = "metadata_missing"
        saved = market in accepted
        result.append({"mkt": market, "prim": accepted[market] if saved else candidate["category"],
                       "label_source": "accepted_saved_label" if saved else RULE["rule_id"],
                       "classification_reason": "saved_label_preserved" if saved else candidate["reason"],
                       "reconstruction_candidate": candidate})
    return result


def summarize_validation(metadata_rows: Iterable[Mapping], accepted_rows: Iterable[Mapping],
                         tag_map: Mapping[str, str | None]) -> dict:
    """Shadow-score overlap independently; never claim preservation as agreement."""
    metadata, accepted = _inputs(metadata_rows, accepted_rows)
    confusion = {saved: {candidate: 0 for candidate in LABELS} for saved in LABELS}
    strata = {}
    reasons = Counter()
    unknown = Counter()
    assigned = agreement = assigned_agreement = 0
    overlap = sorted(metadata.keys() & accepted.keys())
    for market in overlap:
        candidate = candidate_from_tags(metadata[market], tag_map)
        reference = accepted[market] if accepted[market] is not None else "Unclassified"
        category = candidate["category"]
        confusion[reference][category] += 1
        reasons[candidate["reason"]] += 1
        unknown.update(candidate["unknown_tags"])
        matched = category == reference
        classified = category != "Unclassified"
        agreement += matched
        assigned += classified
        assigned_agreement += classified and matched
        evidence = (len(candidate["voting_tags"]), len(candidate["excluded_tags"]),
                    len(candidate["unknown_tags"]), len(candidate["votes"]),
                    tuple(candidate["overrides"]), candidate["reason"])
        item = strata.setdefault(evidence, {"voting_tag_count": evidence[0],
            "excluded_tag_count": evidence[1], "unknown_tag_count": evidence[2],
            "voting_category_count": evidence[3], "overrides": list(evidence[4]),
            "reason": evidence[5], "markets": 0, "assigned": 0, "agreement": 0})
        item["markets"] += 1
        item["assigned"] += classified
        item["agreement"] += matched
    return {**rule_contract(), "weighting": "equal_Gamma_market", "metadata_markets": len(metadata),
            "saved_label_markets": len(accepted), "overlap_markets": len(overlap),
            "metadata_without_saved_label": len(metadata.keys() - accepted.keys()),
            "saved_label_without_metadata": len(accepted.keys() - metadata.keys()),
            "candidate_assigned": assigned, "candidate_unclassified": len(overlap) - assigned,
            "agreement": agreement, "assigned_agreement": assigned_agreement,
            "confusion": confusion,
            "saved_category_counts": {c: sum(confusion[c].values()) for c in LABELS},
            "candidate_category_counts": {c: sum(confusion[s][c] for s in LABELS) for c in LABELS},
            "reason_counts": dict(sorted(reasons.items())),
            "unknown_tag_market_counts": dict(sorted(unknown.items())),
            "tag_evidence": [strata[key] for key in sorted(strata)]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--describe-rule", action="store_true", required=True)
    parser.parse_args(argv)
    print(json.dumps(rule_contract(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
