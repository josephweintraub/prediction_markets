"""Pure Gamma binary-identity candidates, without outcome or sample admission.

Gamma's explicit version selects clobTokenIds (v1 JSON text) or positionIds
(v2 array), aligned with its JSON-text outcomes. Both ID fields may coexist.
The documented v2 position layout clears the outcome byte for a condition;
native bytes31 conditions and zero-right-padded bytes32 compatibility values
are equivalent only under that explicit version. Literal inputs stay intact.

Primary definitions: docs.polymarket.com/migrate/polymarket-v2/api-integrations
and docs.polymarket.com/resources/onchain-position-data. A single record cannot
prove provider coverage, global uniqueness, deployment semantics or payouts.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import re

from pipeline.normalize_polymarket_clob_claims_v1 import TOKEN_ID, UINT256


SCHEMA = "polymarket_gamma_binary_identity_candidates_v1"
HEX32 = re.compile(r"0x[0-9a-fA-F]{64}")
HEX31 = re.compile(r"0x[0-9a-fA-F]{62}")
RFC3339 = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.([0-9]+))?(Z|[+-][0-9]{2}:[0-9]{2})")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
SCOPE = {name: False for name in (
    "source_universe_admitted", "global_token_uniqueness_verified",
    "cross_provider_consistency_verified", "complete_page_lineage_verified",
    "resolved_outcome_admitted", "analysis_admitted", "final_sample_admitted")}


class Quarantine(ValueError):
    """One ordinary, retained first-failure classification."""


def require(condition, reason):
    if not condition:
        raise Quarantine(reason)


def array_from_json(value, field):
    require(type(value) is str, field + "_not_json_text")
    try:
        decoded = json.loads(value)
    except (ValueError, RecursionError):
        raise Quarantine(field + "_invalid_json") from None
    require(type(decoded) is list, field + "_not_array")
    return decoded


def clock_field(market, field):
    """Parse exact timezone-bearing RFC3339 through microseconds, never guess."""
    result = {"source_field": field, "present": field in market,
              "raw": deepcopy(market.get(field)), "status": "missing",
              "utc": None, "epoch_microseconds": None}
    if field not in market:
        return result
    value = market[field]
    if value is None:
        result["status"] = "null"
        return result
    match = RFC3339.fullmatch(value) if type(value) is str else None
    if match is None:
        result["status"] = "invalid_rfc3339"
        return result
    if match.group(1) is not None and len(match.group(1)) > 6:
        result["status"] = "unsupported_submicrosecond_precision"
        return result
    # RFC3339 -00:00 denotes an unknown local offset, not a proven UTC clock.
    if match.group(2) == "-00:00":
        result["status"] = "unknown_timezone_offset"
        return result
    offset = match.group(2)
    if offset != "Z" and (int(offset[1:3]) > 23 or int(offset[4:6]) > 59):
        result["status"] = "invalid_rfc3339"
        return result
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        utc = parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        result["status"] = "invalid_rfc3339"
        return result
    delta = utc - EPOCH
    result.update(status="parsed_exact_utc",
        utc=utc.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        epoch_microseconds=(delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds)
    return result


def clocks(market):
    result = {"created_at": clock_field(market, "createdAt"),
              "end_date": clock_field(market, "endDate"),
              "interval_status": "unavailable"}
    start, end = (result[key]["epoch_microseconds"] for key in ("created_at", "end_date"))
    if start is not None and end is not None:
        result["interval_status"] = "positive" if end > start else "nonpositive"
    return result


def events(market):
    """Retain all event entries; never pick a slug from conflicting evidence."""
    result = {"present": "events" in market, "raw": deepcopy(market.get("events")),
              "entries": [], "distinct_event_ids": [], "distinct_event_slugs": [],
              "event_slug": None, "status": "missing"}
    if "events" not in market:
        return result
    raw = market["events"]
    if raw is None:
        result["status"] = "null"
        return result
    if type(raw) is not list:
        result["status"] = "events_not_array"
        return result
    if not raw:
        result["status"] = "empty"
        return result
    ids, slugs, issues = set(), set(), False
    for index, entry in enumerate(raw):
        row = {"event_index": index, "raw": deepcopy(entry), "issues": []}
        if type(entry) is not dict:
            row["issues"].append("event_not_object")
        else:
            for field, values in (("id", ids), ("slug", slugs)):
                value = entry.get(field)
                row[field + "_present"] = field in entry
                row[field] = deepcopy(value)
                if type(value) is str and bool(value.strip()):
                    values.add(value)
                else:
                    row["issues"].append("event_" + field + "_missing_or_invalid")
        issues |= bool(row["issues"])
        result["entries"].append(row)
    result["distinct_event_ids"], result["distinct_event_slugs"] = sorted(ids), sorted(slugs)
    if len(ids) > 1 or len(slugs) > 1:
        result["status"] = "ambiguous"
    elif issues:
        result["status"] = "incomplete_or_malformed"
    else:
        result.update(status="unique_explicit_event", event_slug=next(iter(slugs)))
    return result


def condition_value(value, version):
    require(type(value) is str, "condition_id_invalid")
    if version == "v1":
        require(HEX32.fullmatch(value) is not None, "v1_condition_id_not_bytes32")
        return value
    if HEX31.fullmatch(value) is not None:
        return value + "00"
    require(HEX32.fullmatch(value) is not None, "v2_condition_id_invalid_width")
    require(value[-2:] == "00", "v2_condition_id_nonzero_outcome_byte")
    return value


def normalize_market(market, source_locator):
    """Return identity candidates or a lossless ordinary-record quarantine.

    Optional clock/event irregularities are separately flagged, not identity
    exclusions. Unknown/additive fields, including all resolution information,
    remain literal; no Gamma status or price becomes an eventual outcome.
    """
    result = {"schema_version": SCHEMA, "status": "quarantined", "reason": None,
        "waterfall_class": None, "identity": None, "input_markets": 1,
        "candidate_markets": 0, "quarantined_markets": 1,
        "resolution_status": "not_inferred", "economic_eligibility": "not_admitted",
        "clock_evidence": clocks(market) if type(market) is dict else None,
        "event_evidence": events(market) if type(market) is dict else None,
        "provenance": {"source_locator": deepcopy(source_locator),
            "raw_market": deepcopy(market), "prices_used_for_resolution": False,
            "version_inferred": False, "legacy_migration_inferred": False}, **SCOPE}
    try:
        require(type(source_locator) is str and bool(source_locator.strip()), "source_locator_missing_or_invalid")
        require(type(market) is dict, "market_not_object")
        require("version" in market, "version_missing")
        version = market["version"]
        require(type(version) is str and version in ("v1", "v2"), "version_missing_or_unsupported")
        require("conditionId" in market, "condition_id_missing")
        condition = condition_value(market["conditionId"], version)
        require("outcomes" in market, "outcomes_missing")
        labels = array_from_json(market["outcomes"], "outcomes")
        require(len(labels) == 2, "nonbinary_outcome_count")
        require(all(type(label) is str and bool(label.strip()) for label in labels), "outcome_not_nonblank_string")
        require(labels[0] != labels[1], "duplicate_outcome_label")
        field = "clobTokenIds" if version == "v1" else "positionIds"
        require(field in market, field + "_missing")
        if version == "v1":
            ids = array_from_json(market[field], field)
        else:
            ids = market[field]
            require(type(ids) is list, "positionIds_not_array")
        require(len(ids) == len(labels), "id_outcome_count_mismatch")
        require(all(type(value) is str and TOKEN_ID.fullmatch(value) is not None
                    and int(value) <= UINT256 for value in ids), "token_id_noncanonical_or_out_of_range")
        require(ids[0] != ids[1], "duplicate_token_id")
        if "n_outcomes" in market:
            require(type(market["n_outcomes"]) is int, "n_outcomes_not_integer")
            require(market["n_outcomes"] == 2, "n_outcomes_token_count_conflict")
        module, module_status = None, "not_applicable_ctf"
        indices = [0, 1]
        if version == "v2":
            numbers = [int(value) for value in ids]
            require(all((value & ~255) == int(condition, 16) for value in numbers), "position_condition_mismatch")
            indices = [value & 255 for value in numbers]
            require(set(indices) == {0, 1}, "position_outcome_bytes_not_binary_pair")
            module = numbers[0] >> 248
            module_status = {1: "binary_module_observed", 2: "negrisk_module_observed",
                             3: "combinatorial_module_observed"}.get(module, "unknown_module_observed")
        tokens = [{"array_index": index, "token_id": ids[index], "outcome": labels[index],
            "outcome_index": indices[index], "complement_token_id": ids[1-index],
            "complement_label": labels[1-index]} for index in (0, 1)]
        identity = {"version": version,
            "position_system": "ctf_v1" if version == "v1" else "position_manager_v2",
            "selected_id_field": field, "condition_id_raw": market["conditionId"],
            "condition_id_compat32": condition, "n_outcomes": 2, "tokens": tokens,
            "module_id": module, "module_status": module_status,
            "economic_eligibility": "not_admitted", "source_locator": source_locator}
        result.update(status="identity_candidate", reason="exact_explicit_version_binary_identity",
            waterfall_class="binary_identity_candidate", identity=identity,
            candidate_markets=1, quarantined_markets=0)
    except Quarantine as error:
        result.update(reason=str(error), waterfall_class="quarantined_" + str(error))
    return result
