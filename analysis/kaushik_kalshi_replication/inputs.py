"""Pure normalization contracts for the fresh native Kalshi replication.

These helpers perform no I/O and certify neither global trade-id uniqueness nor
source completeness. Production must separately admit schema, source hashes,
one-to-one ticker joins, uniqueness and disjoint exclusion reconciliation.

Combo status, category and dollar face value are upstream *admitted* metadata;
this module never guesses them from a ticker or raw ``notional_value``. Native
``taker_side`` selects the recorded YES/NO dollar price after its field semantics
have been admitted. One native print has weight one, irrespective of size.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
import math
from numbers import Real
import re
from typing import Mapping


SCHEMA_VERSION = "kaushik_kalshi_normalization_v1"
SOURCE_FIRST_DAY = date(2021, 6, 30)
SOURCE_LAST_DAY = date(2026, 10, 1)
TRADE_FIELDS = {
    "trade_id": "string", "ticker": "string", "created_time": "string",
    "trade_date": "date", "taker_side": "string", "size": "double",
    "yes_price": "double", "no_price": "double", "source": "string",
}
OPTIONAL_TRADE_FIELDS = {
    "taker_book_side": "string", "taker_outcome_side": "string",
    "is_block_trade": "bool", "src": "string",
}
SETTLEMENT_ENCODINGS = ("unix_seconds", "ISO8601_UTC")
ISO_UTC_PATTERN = (r"[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])"
                   r"[T ](?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
                   r"(?:\.[0-9]{1,6})?(?:Z|[+-]00:00)")
# A representation check only: never round, replace or complement either price.
PRICE_SUM_TOLERANCE = 2 * math.ulp(1.0)


class InputBlocked(ValueError):
    """An unadmitted structural contract must be resolved before processing."""


def require(condition, message):
    if not condition:
        raise InputBlocked(message)


def definitions(*, settlement_ts_encoding):
    """Return the fixed scientific rules and explicitly admitted clock encoding."""
    require(settlement_ts_encoding in SETTLEMENT_ENCODINGS,
            "settlement_ts encoding must be explicitly admitted")
    return {
        "schema_version": SCHEMA_VERSION,
        "sample_scope": "all_available_dates", "cutoff_utc": None,
        "documented_source_first_day": SOURCE_FIRST_DAY.isoformat(),
        "documented_source_last_day": SOURCE_LAST_DAY.isoformat(),
        "observation": "one native trade print; no maker/wallet expansion",
        "weight": "one per eligible native print",
        "claim": "taker_side selects yes_price/no_price and yes/no payout",
        "price": "recorded dollar binary64; no rounding or 1-price reconstruction",
        "price_sum_tolerance": PRICE_SUM_TOLERANCE,
        "unit_payout": "upstream admitted face_value_dollars exactly 1.0",
        "opening_field": "open_time", "opening_encoding": "ISO8601_UTC",
        "endpoint_field": "settlement_ts",
        "endpoint_encoding": settlement_ts_encoding,
        "duration": "L=(settlement_ts-open_time)/86400; R=(settlement_ts-t)/86400; L>0, R>=0, t>=open_time; D1/D10 only",
        "missing_clocks": "duration-ineligible; baseline remains eligible",
        "combo": "explicit admitted single/combo/unknown; only single eligible",
        "category": "pre-admitted native category; missing becomes Unclassified",
        "cluster": "native event_ticker, otherwise namespaced ticker fallback",
        "outcomes": "payoff=100*(Y-P); roi=100*(Y/P-1)",
        "filters": "finalized yes/no payout, strict 0<P<1, positive size; no bot, lifecycle or extreme-price trim",
    }


def validate_trade_schema(fields: Mapping[str, str]):
    """Validate an inspected footer schema, using documented logical type names.

    The caller converts an Arrow date32[day] to ``date``; no data values are cast
    here. Unexpected fields require fresh schema admission instead of vanishing.
    """
    require(isinstance(fields, Mapping), "trade schema must be a mapping")
    require(set(TRADE_FIELDS) <= set(fields), "required native trade fields absent")
    allowed = {**TRADE_FIELDS, **OPTIONAL_TRADE_FIELDS}
    require(set(fields) <= set(allowed), "native trade schema has unadmitted fields")
    for name, kind in fields.items():
        require(kind == allowed[name], "native trade field type differs: " + name)
    return dict(fields)


def _identifier(value):
    # Native legacy identifiers are opaque and may contain internal ASCII
    # spaces (for example GDP-232022 Q4). Preserve those exact join bytes.
    return (isinstance(value, str) and bool(value) and value == value.strip()
            and not any((character.isspace() and character != " ")
                        or ord(character) < 32 or ord(character) == 127
                        for character in value))


def _evidence(value):
    return isinstance(value, str) and bool(value.strip())


def _number(value):
    return (not isinstance(value, bool) and isinstance(value, Real)
            and math.isfinite(float(value)))


def _day(value):
    if type(value) is date:
        return value
    if isinstance(value, str):
        try:
            parsed = date.fromisoformat(value)
            return parsed if parsed.isoformat() == value else None
        except ValueError:
            pass
    return None


def _seconds(value, encoding):
    if encoding == "unix_seconds":
        if not _number(value) or value < 0:
            return None
        try:
            datetime.fromtimestamp(float(value), timezone.utc)
            return float(value)
        except (ValueError, OverflowError, OSError):
            return None
    if not isinstance(value, str) or re.fullmatch(ISO_UTC_PATTERN,value) is None:
        return None
    try:
        # Python 3.10's fromisoformat rejects some valid 1--6 digit fractions;
        # strptime's %f accepts all admitted widths without truncating them.
        format_ = "%Y-%m-%d"+value[10]+"%H:%M:%S"+(".%f" if "." in value else "")+"%z"
        parsed = datetime.strptime(value.replace("Z", "+00:00"),format_)
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            return None
        seconds = parsed.timestamp()
        return seconds if math.isfinite(seconds) and seconds >= 0 else None
    except (ValueError, OverflowError, OSError):
        return None


def prepare_market(raw, *, combo_state, combo_evidence_rule, category,
                   category_evidence_rule, unit_payoff_admitted,
                   face_value_dollars, payout_evidence_rule,
                   settlement_ts_encoding):
    """Normalize one uniquely joined ticker's admitted metadata.

    ``face_value_dollars`` is already normalized upstream. The raw metadata's
    ``notional_value`` is deliberately never read: legacy cents and newer dollar
    fields cannot be distinguished safely from their value alone. Ambiguous or
    duplicate ticker joins must be excluded by the upstream stage.
    """
    definitions(settlement_ts_encoding=settlement_ts_encoding)
    require(isinstance(raw, Mapping), "market metadata must be a mapping")
    require(combo_state in ("single", "combo", "unknown"), "invalid admitted combo state")
    require(type(unit_payoff_admitted) is bool, "unit_payoff_admitted must be boolean")
    require(category is None or isinstance(category, str), "native category must be text or missing")
    ticker = raw.get("ticker")
    reason = "eligible"
    if not _identifier(ticker):
        reason = "invalid_market_ticker"
    elif combo_state == "unknown" or not _evidence(combo_evidence_rule):
        reason = "unadmitted_combo_classification"
    elif combo_state == "combo":
        reason = "combo_excluded"
    elif raw.get("market_type") != "binary":
        reason = "nonbinary_or_missing_market_type"
    elif raw.get("status") != "finalized":
        reason = "not_finalized"
    elif raw.get("result") not in ("yes", "no"):
        reason = "nonbinary_or_missing_result"
    elif not unit_payoff_admitted or not _evidence(payout_evidence_rule):
        reason = "unadmitted_unit_payout"
    elif not _number(face_value_dollars) or face_value_dollars != 1.0:
        reason = "missing_or_nonunit_face_value"
    elif category not in (None, "") and (category != category.strip() or not _evidence(category_evidence_rule)):
        reason = "unadmitted_native_category"
    event = raw.get("event_ticker")
    event_missing = event is None or event == ""
    if reason == "eligible" and not event_missing and not _identifier(event):
        reason = "invalid_native_event_ticker"
    opening = _seconds(raw.get("open_time"), "ISO8601_UTC")
    endpoint = _seconds(raw.get("settlement_ts"), settlement_ts_encoding)
    return {
        "schema_version": SCHEMA_VERSION, "ticker": ticker,
        "eligibility_reason": reason, "market_type": raw.get("market_type"),
        "result": raw.get("result"),
        "combo_state": combo_state, "combo_evidence_rule": combo_evidence_rule,
        "unit_payoff_admitted": unit_payoff_admitted,
        "face_value_dollars": face_value_dollars,
        "payout_evidence_rule": payout_evidence_rule,
        "category": category if category not in (None, "") else "Unclassified",
        "category_missing": category in (None, ""),
        "category_evidence_rule": category_evidence_rule,
        "event_cluster": ("kalshi:market:" + ticker if event_missing else "kalshi:event:" + event)
                         if _identifier(ticker) and (event_missing or _identifier(event)) else None,
        "cluster_source": "market_fallback" if event_missing else "native_event",
        "opening_seconds": opening, "endpoint_seconds": endpoint,
        "opening_raw": raw.get("open_time"), "endpoint_raw": raw.get("settlement_ts"),
        "endpoint_encoding": settlement_ts_encoding,
    }


def fixed_price_bin(price):
    require(_number(price) and 0 < price < 1, "strict finite claim price required")
    return next((index for index, upper in enumerate((.1, .2, .3, .4, .5, .6, .7, .8, .9), 1)
                 if price < upper), 10)


def normalize_trade(raw, market, *, source_date, source_ordinal):
    """Annotate a native print without dropping it or manufacturing role rows.

    Source-day bounds describe the frozen inventory, not a scientific execution
    cutoff. Global duplicate IDs and join fanout remain independent stage gates.
    Nonfinite derived quantities are retained with an estimation-blocking flag.
    """
    require(isinstance(raw, Mapping) and set(TRADE_FIELDS) <= set(raw),
            "required native trade fields absent")
    day = _day(source_date)
    require(day is not None and SOURCE_FIRST_DAY <= day <= SOURCE_LAST_DAY,
            "source date outside documented admitted inventory")
    require(type(source_ordinal) is int and source_ordinal >= 0, "nonnegative physical source ordinal required")
    require(market is None or (isinstance(market, Mapping) and market.get("schema_version") == SCHEMA_VERSION),
            "prepared market contract required")
    timestamp = _seconds(raw["created_time"], "ISO8601_UTC")
    trade_day = _day(raw["trade_date"])
    side, yes, no, size = raw["taker_side"], raw["yes_price"], raw["no_price"], raw["size"]
    outcome_side, book_side = raw.get("taker_outcome_side"), raw.get("taker_book_side")
    reason = "eligible"
    if not _identifier(raw["trade_id"]) or not _identifier(raw["ticker"]):
        reason = "invalid_trade_identifier"
    elif timestamp is None:
        reason = "invalid_timestamp"
    elif trade_day is None or datetime.fromtimestamp(timestamp, timezone.utc).date() != trade_day:
        reason = "invalid_or_mismatched_trade_date"
    elif trade_day != day:
        reason = "source_day_mismatch"
    elif side not in ("yes", "no"):
        reason = "invalid_taker_side"
    elif outcome_side not in (None, "", "yes", "no"):
        reason = "invalid_taker_outcome_side"
    elif outcome_side not in (None, "") and outcome_side != side:
        reason = "taker_outcome_side_disagreement"
    elif book_side not in (None, "", "bid", "ask"):
        reason = "invalid_taker_book_side"
    elif book_side not in (None, "") and {"bid": "yes", "ask": "no"}[book_side] != side:
        reason = "taker_book_side_disagreement"
    elif not _number(size) or size <= 0:
        reason = "invalid_size"
    elif not _number(yes) or not _number(no) or not 0 < yes < 1 or not 0 < no < 1:
        reason = "invalid_native_price"
    elif abs((yes + no) - 1.0) > PRICE_SUM_TOLERANCE:
        reason = "native_price_complement_disagreement"
    elif market is None:
        reason = "unadmitted_market"
    elif raw["ticker"] != market["ticker"]:
        reason = "trade_market_ticker_disagreement"
    elif market["eligibility_reason"] != "eligible":
        reason = market["eligibility_reason"]
    price = raw[side + "_price"] if side in ("yes", "no") else None
    admitted = reason == "eligible"
    payout = int(side == market["result"]) if admitted else None
    price = float(price) if admitted else None
    payoff = 100 * (payout - price) if admitted else None
    roi = 100 * (payout / price - 1) if admitted else None
    capital = float(size) * price if admitted else None
    finite_outcomes = admitted and math.isfinite(payoff) and math.isfinite(roi)
    band = fixed_price_bin(price) if admitted else None
    opening = market["opening_seconds"] if market is not None else None
    endpoint = market["endpoint_seconds"] if market is not None else None
    clock_reason = "eligible"
    if opening is None:
        clock_reason = "missing_or_invalid_open_time"
    elif endpoint is None:
        clock_reason = "missing_or_invalid_settlement_ts"
    elif endpoint <= opening:
        clock_reason = "nonpositive_lifespan"
    elif timestamp is None or timestamp < opening:
        clock_reason = "trade_before_open_time"
    elif timestamp > endpoint:
        clock_reason = "trade_after_settlement_ts"
    lifespan = (endpoint - opening) / 86400 if opening is not None and endpoint is not None else None
    remaining = (endpoint - timestamp) / 86400 if endpoint is not None and timestamp is not None else None
    duration_reason = ("baseline_ineligible" if not admitted else clock_reason if clock_reason != "eligible"
                       else "not_tail" if band not in (1, 10) else "eligible")
    return {
        "trade_id": raw["trade_id"], "ticker": raw["ticker"],
        "source_date": day.isoformat(), "source_ordinal": source_ordinal,
        "source": raw["source"], "timestamp": timestamp,
        "recorded_src": raw.get("src"),
        "source_src_disagreement": (raw.get("src") not in (None, "")
                                    and raw["source"] not in (None, "")
                                    and raw.get("src") != raw["source"]),
        "trade_month": trade_day.strftime("%Y-%m") if trade_day is not None else None,
        "taker_side": side, "recorded_yes_price": yes, "recorded_no_price": no,
        "size": size, "eligibility_reason": reason, "baseline_eligible": admitted,
        "claim_id": "kalshi:claim:" + raw["ticker"] + ":" + side if admitted else None,
        "P": price, "Y": payout, "weight": 1 if admitted else None,
        "bin": band, "payoff": payoff, "roi": roi, "capital": capital,
        "analytic_outcomes_finite": finite_outcomes,
        "amounts_finite": admitted and math.isfinite(capital),
        "estimation_eligible": admitted and finite_outcomes,
        "category": market["category"] if market is not None else None,
        "event_cluster": market["event_cluster"] if market is not None else None,
        "cluster_source": market["cluster_source"] if market is not None else None,
        "opening_seconds": opening, "endpoint_seconds": endpoint,
        "duration_clock_reason": clock_reason, "duration_reason": duration_reason,
        "duration_eligible": duration_reason == "eligible",
        "L": lifespan, "R": remaining,
        "xL": math.log2(1 + lifespan) if duration_reason == "eligible" else None,
        "xR": math.log2(1 + remaining) if duration_reason == "eligible" else None,
    }
