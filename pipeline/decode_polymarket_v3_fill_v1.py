"""Lossless, pure decoder for the admitted V3 seven-word OrderFilled wire.

Accepts one actual native-collector Parquet row, not a legacy five-word event.
No file reads, APIs, scans, deduplication, timestamp fallback or publication.
The caller must independently verify the explicit root admission and complete
native source artifacts. ``event`` is suitable for normalize_event; provenance
retains the complete input row, provider-only timestamp and seven raw words.

Wire evidence: pinned Sourcify response_001.raw (Exchange.sol:145-156,232-233)
and response_002.raw (Exchange.sol:148-159,235-236), same signature and field
ordering. These source records alone do NOT establish deployed-source matching.
The root-admitted build/upgrade contract is separately required, unchanged.
Actual row schema: collect_polymarket_native_increment.parquet_payload; adaptive
v2 uses that same function. The older pilot's uint8 request IDs are also valid
values of the collector's uint64 field. Collector tags are never silently lost.
"""
from __future__ import annotations

import re

from pipeline import normalize_polymarket_v3_archive_v1 as archive

SIGNATURE = "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
WIRE_FIELDS = (
    ("orderHash", "bytes32", True), ("maker", "address", True), ("taker", "address", True),
    ("side", "uint8", False), ("tokenId", "uint256", False),
    ("makerAmountFilled", "uint256", False), ("takerAmountFilled", "uint256", False),
    ("fee", "uint256", False), ("builder", "bytes32", False), ("metadata", "bytes32", False),
)
SOURCE_EVIDENCE = {
    "old": {"source_package_sha256": "2fa08be23b657b7071abbf89d0e482146a93b521e97142d29320094f174241c0",
            "source_package_bytes": 764915, "declaration_lines": (145, 156), "topic_lines": (232, 233)},
    "new": {"source_package_sha256": "47e25bfebfe8082f8edf183e401c28307c8ef9eadf748c16494d2dbf1fdc5bf9",
            "source_package_bytes": 789270, "declaration_lines": (148, 159), "topic_lines": (235, 236)},
}
RPC_FIELDS = {"address", "blockHash", "blockNumber", "data", "logIndex", "removed", "topics",
              "transactionHash", "transactionIndex", "blockTimestamp"}
COLLECTOR_FIELDS = {"block_number", "log_index", "transaction_index", "exchange_address", "window",
                    "source_request_id", "provider_block_timestamp_present"}
ROW_FIELDS = RPC_FIELDS | COLLECTOR_FIELDS
QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)")
DATA = re.compile(r"0x[0-9a-fA-F]{448}")


class DecodeError(ValueError):
    """Fixed row-quarantine code; the parent must preserve the rejected raw row."""


def require(value, code):
    if not value:
        raise DecodeError(code)


def uint64(value):
    require(type(value) is int and 0 <= value < 2**64, "collector_uint64_schema")
    return value


def rpc_uint64(value):
    require(type(value) is str and len(value) <= 18 and QUANTITY.fullmatch(value) is not None,
            "rpc_quantity_schema")
    return uint64(int(value, 16))


def decode_native_row(row, admission):
    """Return decoded Event + lossless provenance, or reject without guessing.

    This does not verify a saved receipt, a collection window's completeness,
    global native uniqueness, token metadata, or an execution timestamp.
    """
    archive.validate_admission(admission)
    require(type(row) is dict and set(row) == ROW_FIELDS, "native_parquet_row_schema")
    require(row["removed"] is False, "removed_native_log")
    require(type(row["address"]) is str and archive.ADDRESS.fullmatch(row["address"]) is not None
            and row["address"].lower() == archive.EXCHANGE, "unsupported_emitting_exchange")
    require(type(row["exchange_address"]) is str and row["exchange_address"] == row["address"].lower(),
            "collector_exchange_address_differs")
    for field in ("blockHash", "transactionHash"):
        require(type(row[field]) is str and archive.HASH.fullmatch(row[field]) is not None, "native_hash_schema")
    locators = {}
    for raw_name, cached in (("blockNumber", "block_number"), ("logIndex", "log_index"),
                             ("transactionIndex", "transaction_index")):
        value = rpc_uint64(row[raw_name])
        require(uint64(row[cached]) == value, "raw_cached_native_locator_differs")
        locators[cached] = value
    require(locators["block_number"] > 0, "native_block_zero")
    uint64(row["source_request_id"])
    require(type(row["window"]) is str and 0 < len(row["window"]) <= 128
            and all(33 <= ord(char) <= 126 for char in row["window"]), "collector_window_schema")
    present = row["provider_block_timestamp_present"]
    require(type(present) is bool, "provider_timestamp_presence_schema")
    timestamp = row["blockTimestamp"]
    require((not present and timestamp is None) or (present and type(timestamp) is str
            and 0 < len(timestamp) <= 128 and all(32 <= ord(char) <= 126 for char in timestamp)),
            "provider_timestamp_presence_differs")
    topics = row["topics"]
    require(type(topics) is list and len(topics) == 4
            and all(type(topic) is str and archive.HASH.fullmatch(topic) is not None for topic in topics),
            "order_filled_topic_schema")
    require(topics[0].lower() == TOPIC, "unsupported_order_filled_wire_topic")
    require(all(topic[2:26] == "0"*24 for topic in topics[2:]), "noncanonical_indexed_address_padding")
    require(type(row["data"]) is str and DATA.fullmatch(row["data"]) is not None, "order_filled_seven_word_schema")
    words = tuple("0x"+row["data"][2+64*i:2+64*(i+1)] for i in range(7))
    side = int(words[0], 16)
    require(side in (0, 1), "noncanonical_side_enum_or_padding")
    event = {"exchange_address": row["exchange_address"], "transaction_hash": row["transactionHash"].lower(),
        **locators, "block_hash": row["blockHash"].lower(), "order_hash": topics[1].lower(),
        "maker": "0x"+topics[2][-40:].lower(), "taker": "0x"+topics[3][-40:].lower(),
        "side": side, "token_id": int(words[1], 16), "maker_amount_filled": int(words[2], 16),
        "taker_amount_filled": int(words[3], 16), "fee": int(words[4], 16),
        "builder": words[5].lower(), "metadata": words[6].lower(), "removed": False,
        "block_timestamp": None}
    try:
        archive.validate_event(event)
        era = archive.source_era(event)
    except archive.RowError as error:
        raise DecodeError(str(error)) from None
    require(era in SOURCE_EVIDENCE, "unsupported_source_era")
    raw_row = {**row, "topics": list(topics)}
    provenance = {"raw_native_row": raw_row, "raw_data_words": words,
        "source_era": era, "implementation_address": archive.BUILDS[era]["implementation_address"],
        "native_identity": (archive.EXCHANGE, event["transaction_hash"], event["log_index"]),
        "wire_signature": SIGNATURE, "wire_topic": TOPIC,
        "source_path": "src/exchange/Exchange.sol", **SOURCE_EVIDENCE[era],
        "provider_timestamp_used_as_execution_clock": False}
    return {"schema_version": "polymarket_v3_decoded_fill_v1", "event": event,
            "provenance": provenance, "decoding_only": True, **archive.SCOPE}
