"""Pure, fixture-testable V3 event -> retained archive-convention adapter.

No files, RPC, scans, publication, deduplication or final-sample admission occur.
The caller must independently verify the root admission's saved build receipt,
the source/upgrade evidence, the exact-header artifact and the resolved token
spine. Syntactically valid receipt fields are not proof of those external facts.

Events are already losslessly decoded from the seven V3 OrderFilled data words.
``block_timestamp`` is an optional *assertion*, never a timestamp fallback.
``token_rows`` and ``complement_rows`` are exact-ID lookup candidates, not fuzzy
matches or an inferred legacy condition map. The parent must supply a complete,
globally unique admitted spine; this core checks the requested pair locally.

Event first failure: native schema, era, header, amounts, exchange wallet,
module, token pair, gross archive price. Exclusions/quarantines retain the event.
Both role rows are constructed from fresh scalar values. Counterparty action is
the historical archive convention, NOT a native own-wallet action claim.
Paper eligibility is checked separately per role, as in annotate_month: one
rounded-to-endpoint complement must not discard a valid opposite-role row.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import PurePosixPath
import math
import re

EXCHANGE = "0xe3333700ca9d93003f00f0f71f8515005f6c00aa"
HISTORICAL_EXCHANGES = (
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
)
BUILDS = {
    "old": {"implementation_address": "0x7345c6842b244926125ed4054905cac49620b5dc",
            "runtime_bytes": 22204,
            "runtime_sha256": "dda5fcc0be82f96ae766bf269b5c8f87871cf458fac18423b265ac57123901fb",
            "full_runtime_exact_match_after_declared_immutables": True},
    "new": {"implementation_address": "0x641b40ec414a076b9e79e703fc7bf4ebec248bb7",
            "runtime_bytes": 22256,
            "runtime_sha256": "2dfc1d2e867a7fa851cb195cc7887b614442f4f2de04d3081155a99bd502c6d6",
            "full_runtime_exact_match_after_declared_immutables": True},
}
UPGRADE = {
    "block_number": 93452332,
    "block_hash": "0x1aa733aadf9f051793fc4ab5245fbfd1bc5adba4ba50593afd2ba6b1911844d1",
    "transaction_hash": "0x2b55c1ba874ab18e9f4258c99db25bebd0a34bdcf3834eea1ea77e4e7629d364",
    "transaction_index": 62, "log_index": 752,
    "old_implementation": BUILDS["old"]["implementation_address"],
    "new_implementation": BUILDS["new"]["implementation_address"],
}
SCHEMA = "polymarket_v3_archive_admission_v1"
VALUE_FIELDS = ("proxyWallet", "timestamp", "conditionId", "usdcSize", "price", "side",
                "outcome", "eventSlug", "is_maker", "counterparty", "year_month")
EVENT_FIELDS = {"exchange_address", "transaction_hash", "transaction_index", "log_index",
    "block_number", "block_hash", "order_hash", "maker", "taker", "side", "token_id",
    "maker_amount_filled", "taker_amount_filled", "fee", "builder", "metadata", "removed",
    "block_timestamp"}
SPINE_FIELDS = {"token_id", "complement_token_id", "market_id", "outcome", "winning_outcome",
                "event_slug", "resolved"}
HASH = re.compile(r"0x[0-9a-fA-F]{64}")
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}")
SHA = re.compile(r"[0-9a-f]{64}")
DECIMAL = re.compile(r"[1-9][0-9]*")
UINT256 = 2**256 - 1
SCOPE = {"source_universe_admitted": False, "native_own_actions_admitted": False,
         "analysis_admitted": False, "final_sample_admitted": False}


class AdmissionError(ValueError):
    """The global caller contract is not the explicitly accepted version."""


class RowError(ValueError):
    """One record/pair/header is quarantined rather than guessed."""


def require(value, reason):
    if not value:
        raise RowError(reason)


def exact(left, right):
    """Unlike Python equality, reject bool as int and extra dictionary fields."""
    if type(left) is not type(right):
        return False
    if type(right) is dict:
        return set(left) == set(right) and all(exact(left[key], value) for key, value in right.items())
    return left == right


def validate_admission(value):
    fields = {"schema_version", "status", "chain_id", "exchange_address", "build_receipt", "builds", "upgrade"}
    if (type(value) is not dict or set(value) != fields or value.get("schema_version") != SCHEMA
            or value.get("status") != "root_accepted_exchange_builds_and_era"
            or type(value.get("chain_id")) is not int or value["chain_id"] != 137
            or value.get("exchange_address") != EXCHANGE or not exact(value.get("builds"), BUILDS)
            or not exact(value.get("upgrade"), UPGRADE)):
        raise AdmissionError("explicit_root_build_and_era_admission_required")
    binding = value["build_receipt"]
    if type(binding) is not dict or set(binding) != {"path", "bytes", "sha256"}:
        raise AdmissionError("build_receipt_binding_schema")
    path = binding["path"]
    if (type(path) is not str or not path.startswith("/mnt/data/runs/")
            or PurePosixPath(path).as_posix() != path or any(part in {".", ".."} for part in path.split("/"))
            or type(binding["bytes"]) is not int or not 0 < binding["bytes"] <= 64 * 1024**2
            or type(binding["sha256"]) is not str or SHA.fullmatch(binding["sha256"]) is None):
        raise AdmissionError("build_receipt_binding_schema")


def validate_event(event):
    require(type(event) is dict and set(event) == EVENT_FIELDS, "invalid_native_schema")
    for field in ("block_number", "transaction_index", "log_index"):
        require(type(event[field]) is int and 0 <= event[field] < 2**64, "invalid_native_locator")
    require(event["block_number"] > 0 and event["removed"] is False, "invalid_native_locator")
    for field in ("block_hash", "transaction_hash", "order_hash", "builder", "metadata"):
        require(type(event[field]) is str and HASH.fullmatch(event[field]) is not None, "invalid_native_hash")
    for field in ("exchange_address", "maker", "taker"):
        require(type(event[field]) is str and ADDRESS.fullmatch(event[field]) is not None, "invalid_native_wallet")
    require(event["exchange_address"].lower() == EXCHANGE, "unsupported_emitting_exchange")
    require(type(event["side"]) is int and event["side"] in (0, 1), "invalid_native_side")
    for field in ("token_id", "maker_amount_filled", "taker_amount_filled", "fee"):
        require(type(event[field]) is int and 0 <= event[field] <= UINT256, "invalid_native_uint256")
    require(event["token_id"] > 0, "invalid_native_token")
    require(event["block_timestamp"] is None or type(event["block_timestamp"]) is int
            and event["block_timestamp"] > 0, "invalid_native_clock_assertion")


def source_era(event):
    block, tx, log = (event[field] for field in ("block_number", "transaction_index", "log_index"))
    same_tx = event["transaction_hash"].lower() == UPGRADE["transaction_hash"]
    if block != UPGRADE["block_number"]:
        require(not same_tx, "impossible_upgrade_locator")
        return "old" if block < UPGRADE["block_number"] else "new"
    require(event["block_hash"].lower() == UPGRADE["block_hash"], "upgrade_block_hash_differs")
    utx, ulog = UPGRADE["transaction_index"], UPGRADE["log_index"]
    require(same_tx == (tx == utx) and log != ulog
            and (tx == utx or (tx < utx) == (log < ulog)), "impossible_upgrade_locator")
    return "old" if (tx, log) < (utx, ulog) else "new"


def exact_timestamp(event, headers):
    require(type(headers) is list and len(headers) == 1, "exact_header_not_singleton")
    header = headers[0]
    require(type(header) is dict and set(header) == {"block_number", "block_hash", "timestamp"}
            and type(header["block_number"]) is int and header["block_number"] == event["block_number"]
            and type(header["block_hash"]) is str and HASH.fullmatch(header["block_hash"]) is not None
            and header["block_hash"].lower() == event["block_hash"].lower()
            and type(header["timestamp"]) is int and 0 < header["timestamp"] <= 253402300799,
            "exact_header_key_or_clock_differs")
    require(event["block_timestamp"] is None or event["block_timestamp"] == header["timestamp"],
            "exact_timestamp_assertion_differs")
    return header["timestamp"]


def decimal_token(value):
    return type(value) is str and len(value) <= 78 and DECIMAL.fullmatch(value) is not None and int(value) <= UINT256


def token_pair(token_id, token_rows, complement_rows):
    require(type(token_rows) is list and len(token_rows) == 1, "token_mapping_missing_or_ambiguous")
    require(type(complement_rows) is list and len(complement_rows) == 1, "complement_mapping_missing_or_ambiguous")
    token, complement = token_rows[0], complement_rows[0]
    for row in (token, complement):
        require(type(row) is dict and set(row) == SPINE_FIELDS
                and all(decimal_token(row[field]) for field in ("token_id", "complement_token_id"))
                and all(type(row[field]) is str and bool(row[field]) and row[field].strip() == row[field]
                        for field in ("market_id", "outcome", "winning_outcome"))
                and (row["event_slug"] is None or type(row["event_slug"]) is str), "invalid_spine_schema")
        require(row["resolved"] is True, "unresolved_token_pair")
    require(token["token_id"] == str(token_id), "exact_full_token_id_mismatch")
    require(token["token_id"] != complement["token_id"]
            and token["complement_token_id"] == complement["token_id"]
            and complement["complement_token_id"] == token["token_id"]
            and token["market_id"] == complement["market_id"]
            and token["outcome"] != complement["outcome"]
            and token["winning_outcome"] == complement["winning_outcome"]
            and token["winning_outcome"] in (token["outcome"], complement["outcome"])
            and token["event_slug"] == complement["event_slug"], "binary_complement_or_outcome_conflict")
    require(int(complement["token_id"]) >> 248 == token_id >> 248, "binary_pair_module_conflict")
    return token, complement


def normalize_event(event, admission, token_rows, complement_rows, headers):
    """Classify one typed event. Global admission errors raise; row errors quarantine.

    No multiplicity is removed. Fees/raw amounts stay in ``native``; the published
    price/cash remain gross. The 11 archive fields match lineage.VALUE_FIELDS.
    """
    validate_admission(admission)
    result = {"schema_version": "polymarket_v3_archive_record_v1", "status": "quarantined",
              "reason": None, "native": dict(event) if type(event) is dict else event,
              "rows": [], **SCOPE}
    try:
        validate_event(event)
        era = source_era(event)
        result.update(source_era=era, implementation_address=BUILDS[era]["implementation_address"],
                      native_identity=(EXCHANGE, event["transaction_hash"].lower(), event["log_index"]))
        timestamp = exact_timestamp(event, headers)
        cash, quantity = ((event["maker_amount_filled"], event["taker_amount_filled"]) if event["side"] == 0
                          else (event["taker_amount_filled"], event["maker_amount_filled"]))
        require(cash > 0 and quantity > 0, "nonpositive_gross_cash_or_quantity")
        wallets = {event["maker"].lower(), event["taker"].lower()}
        matches = {"historical": sorted(wallets.intersection(HISTORICAL_EXCHANGES)),
                   "v3_proxy": [EXCHANGE] if EXCHANGE in wallets else []}
        result["exchange_wallet_matches"] = matches
        if matches["historical"] or matches["v3_proxy"]:
            result.update(status="excluded", reason="exchange_wallet_historical" if matches["historical"] else "exchange_wallet_v3_proxy")
            return result
        module = event["token_id"] >> 248
        result["module_id"] = module
        if module == 3:
            result.update(status="excluded", reason="combinatorial_module3")
            return result
        require(module in (1, 2), "unknown_position_module")
        token, complement = token_pair(event["token_id"], token_rows, complement_rows)
        # Preserve the original binary64 scale-then-divide order, without cent rounding.
        usdc, shares = cash / 1_000_000.0, quantity / 1_000_000.0
        price = usdc / shares
        require(math.isfinite(price) and 0 < price <= 1 and math.isfinite(usdc) and usdc > 0, "invalid_recorded_price")
        recorded_y = int(token["outcome"] == token["winning_outcome"])
        month = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m")
        for is_maker, wallet, counterparty, side in (
            (True, event["maker"].lower(), event["taker"].lower(), "BUY" if event["side"] == 0 else "SELL"),
            (False, event["taker"].lower(), event["maker"].lower(), "SELL" if event["side"] == 0 else "BUY"),
        ):
            normalized_p = 1.0 - price if side == "SELL" else price
            eligibility = ("invalid_recorded_price" if price >= 1 else
                           "invalid_normalized_price" if not math.isfinite(normalized_p) or not 0 < normalized_p < 1
                           else "eligible")
            row = {"proxyWallet": wallet, "timestamp": timestamp, "conditionId": token["token_id"],
                "usdcSize": usdc, "price": price, "side": side, "outcome": token["outcome"],
                "eventSlug": token["event_slug"], "is_maker": is_maker, "counterparty": counterparty,
                "year_month": month, "P": normalized_p, "Y": 1-recorded_y if side == "SELL" else recorded_y,
                "claim_id": complement["token_id"] if side == "SELL" else token["token_id"],
                "market_id": token["market_id"], "recorded_Y": recorded_y, "eligibility_reason": eligibility}
            result["rows"].append(row)
        result.update(status="classified", reason="eligible", gross_cash_integer=cash, gross_quantity_integer=quantity)
    except RowError as error:
        result.update(status="quarantined", reason=str(error), rows=[])
    return result


def primary_taker_rows(result):
    """Copy paper-eligible counterparty-role rows, not native taker own actions."""
    return [dict(row) for row in result["rows"] if row["is_maker"] is False
            and row["eligibility_reason"] == "eligible"] if result["status"] == "classified" else []


def value_distinct_key(row):
    """Full published value key, deliberately excluding native identity and derived P/Y."""
    return tuple(row[field] for field in VALUE_FIELDS)


def native_relation(first, second):
    """Diagnostic only: never remove either record or confuse value equality with replay."""
    for event in (first, second):
        validate_event(event)
    key = lambda row: (row["exchange_address"].lower(), row["transaction_hash"].lower(), row["log_index"])
    if key(first) != key(second):
        return "different_native_identity"
    return "equal_native_record" if first == second else "conflicting_native_record"


def coverage_summary(results):
    """Small caller-supplied classified records only; no source-universe certificate."""
    counts, roles, total, archive, primary = Counter(), Counter(), 0, 0, 0
    for result in results:
        require(result.get("schema_version") == "polymarket_v3_archive_record_v1"
                and result.get("status") in {"classified", "excluded", "quarantined"}
                and type(result.get("reason")) is str and all(result.get(key) is False for key in SCOPE), "invalid_classified_result")
        total += 1
        counts[(result["status"], result["reason"])] += 1
        archive += len(result["rows"])
        primary += len(primary_taker_rows(result))
        for row in result["rows"]:
            roles[(row["is_maker"], row["eligibility_reason"])] += 1
    return {"input_records": total, "archive_rows": archive, "primary_taker_rows": primary,
            "counts": [{"status": status, "reason": reason, "records": count}
                       for (status, reason), count in sorted(counts.items())],
            "role_eligibility_counts": [{"is_maker": role, "reason": reason, "rows": count}
                                       for (role, reason), count in sorted(roles.items())], **SCOPE}
