"""Bounded, immutable Stage-1 source pilot before sports FIFO estimation.

The raw source is inspected through its footer, then explicitly bounded row
groups. Complete source blocks are retrieved for the pilot so transaction batches
are never constructed from token-filtered or truncated maker legs. This does not
claim population coverage, identify holdings, or estimate calibration.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import runpy
import struct
import subprocess
import sys
from typing import Any, Mapping, Sequence

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from analysis.diagnostics.profit_taking_actions import (
    EXCHANGE_ADDRESSES, RAW_FIELDS, V2_EXCHANGE_ADDRESSES, decode_own_action, deduplicate_raw_fills,
    reconcile_reserved_match_batch,
)
from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint, fingerprint, fresh_run, write_json,
)
from production_guard import require_production_host


def parquet_metadata(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Read footer statistics only, with no full-source content hash or scan."""
    source = pq.ParquetFile(path)
    if tuple(source.schema_arrow.names) != RAW_FIELDS:
        raise ValueError("Raw Stage-1 schema differs from the frozen 12 fields")
    for name in ("maker_amount_filled", "taker_amount_filled", "fee", "block_number", "log_index"):
        if not pa.types.is_integer(source.schema_arrow.field(name).type):
            raise ValueError(f"Raw field {name} must use an integer type")
    groups: list[dict[str, Any]] = []
    constant_addresses: Counter[str] = Counter()
    observed_addresses: set[str] = set()
    for index in range(source.metadata.num_row_groups):
        group = source.metadata.row_group(index)
        block = group.column(source.schema_arrow.get_field_index("block_number")).statistics
        address = group.column(source.schema_arrow.get_field_index("exchange_address")).statistics
        if block is None or not block.has_min_max or address is None or not address.has_min_max:
            raise ValueError("Every raw row group needs block/address footer statistics")
        if block.null_count or address.null_count:
            raise ValueError("Raw source block/address identity has nulls")
        address_min, address_max = address.min.lower(), address.max.lower()
        observed_addresses.update((address_min, address_max))
        if address_min == address_max:
            constant_addresses[address_min] += group.num_rows
        groups.append({"row_group": index, "rows": group.num_rows,
                       "block_min": int(block.min), "block_max": int(block.max),
                       "address_min": address_min, "address_max": address_max,
                       "compressed_bytes":sum(group.column(i).total_compressed_size for i in range(group.num_columns))})
    if not groups:
        raise ValueError("Raw source has no row groups")
    if not observed_addresses <= EXCHANGE_ADDRESSES:
        raise ValueError("Unknown exchange address in source footer")
    with path.open("rb") as handle:
        handle.seek(-8, 2)
        ending = handle.read(8)
        if ending[4:] != b"PAR1":
            raise ValueError("Invalid Parquet footer magic")
        footer_length = struct.unpack("<I", ending[:4])[0]
        handle.seek(-(footer_length + 8), 2)
        footer_digest = hashlib.sha256(handle.read(footer_length + 8)).hexdigest()
    report = {
        "path": str(path.resolve()), "bytes": path.stat().st_size,
        "rows": source.metadata.num_rows, "row_groups": len(groups),
        "schema": str(source.schema_arrow), "footer_sha256": footer_digest,
        "source_content_hash": "not_computed_in_bounded_pilot",
        "block_min": min(g["block_min"] for g in groups),
        "block_max": max(g["block_max"] for g in groups),
        "footer_address_bounds": sorted(observed_addresses),
        "constant_address_rowgroup_rows": dict(sorted(constant_addresses.items())),
        "mixed_address_rowgroup_rows": source.metadata.num_rows - sum(constant_addresses.values()),
        "globally_disjoint_block_order": all(groups[i-1]["block_max"] <= groups[i]["block_min"]
                                               for i in range(1, len(groups))),
    }
    return report, groups


def validate_complements(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    result: dict[str, str] = {}
    markets: dict[str, str] = {}
    for row in rows:
        for name in ("token_id","complement_token_id","market_id"):
            if not isinstance(row.get(name),str) or not row[name] or row[name].strip()!=row[name]:
                raise ValueError("Token spine identities must be nonempty exact strings")
        if any(not row[name].isdecimal() or int(row[name])<=0 for name in ("token_id","complement_token_id")):
            raise ValueError("Token spine outcome identities must be positive decimal strings")
        token, other = str(row["token_id"]), str(row["complement_token_id"])
        if token in result or token == other:
            raise ValueError("Token spine needs unique binary complements")
        result[token] = other
        markets[token] = str(row["market_id"])
    if not result or any(result.get(other) != token for token, other in result.items()):
        raise ValueError("Token spine must be symmetric and complete")
    if any(markets[other] != markets[token] for token, other in result.items()):
        raise ValueError("Complementary tokens must belong to the same market")
    if any(n != 2 for n in Counter(markets.values()).values()):
        raise ValueError("Each accepted binary market needs exactly two tokens")
    return result


def run_support(pilot: Path, receipts: Path, run_dir: Path) -> dict[str, Any]:
    """Persist protocol/mechanism coverage from already-published evidence only."""
    audits=pq.read_table(pilot/"batch_audit.parquet").to_pylist()
    source=json.loads((pilot/"summary.json").read_text())
    native=json.loads((receipts/"summary.json").read_text())
    checked={e["transaction_hash"] for e in native["evidence"] if e["status"]=='verified'}
    def coverage(by: str, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        result=[]
        for key in sorted({r[by] for r in rows}):
            selected=[r for r in rows if r[by]==key and r["status"]=='accepted']
            result.append({by:key,"accepted_batches":len(selected),
                "normal_legs":sum(r["normal_legs"] for r in selected),
                "mint_legs":sum(r["mint_legs"] for r in selected),
                "merge_legs":sum(r["merge_legs"] for r in selected),
                "refund_batches":sum(r["refund_making_micro"]>0 for r in selected),
                "nonzero_fee_batches":sum(r["active_fee_micro"]>0 or r["passive_fee_logs"]>0 for r in selected)})
        return result
    verified=[r for r in audits if r["transaction_hash"] in checked]
    result={"analysis":"bounded_source_and_native_coverage","counts":source["counts"],
        "pilot_by_exchange":coverage("exchange_address",audits),
        "pilot_by_generation":coverage("source_contract_version",audits),
        "native_by_exchange":coverage("exchange_address",verified),
        "native_by_generation":coverage("source_contract_version",verified),
        "native_statuses":native["statuses"],"native_transactions":native["requested_transactions"],
        "native_verified_log_count":sum(e["native_logs"] for e in native["evidence"] if e["status"]=='verified'),
        "discovery_rowgroup_reads":len(source["discovery_row_groups"]),
        "complete_block_probes":len(source["complete_block_row_groups"]),
        "complete_wide_rowgroup_reads":len(source["complete_wide_row_groups"]),
        "compressed_wide_read_bytes":source["compressed_wide_read_bytes"],
        "limits":"MERGE has no native pilot evidence. Coverage is a bounded pilot, not the sports population."}
    with fresh_run(run_dir,[pilot,receipts]) as staging:
        write_json(staging/"summary.json",result)
        write_json(staging/"manifest.json",{"status":"complete","created_utc":datetime.now(timezone.utc).isoformat(),
            "inputs":{"source_summary":fingerprint(pilot/"summary.json"),"batch_audit":fingerprint(pilot/"batch_audit.parquet"),
                      "receipt_summary":fingerprint(receipts/"summary.json")},
            "outputs":{"summary.json":artifact_fingerprint(staging/"summary.json")},
            "producing_script":"analysis/diagnostics/profit_taking_source_audit.py"})
    return result


def choose_discovery_groups(groups: Sequence[Mapping[str, Any]], anchors: Sequence[int],
                            maximum: int) -> list[int]:
    """Pick nearest footer groups per anchor/address, never an unbounded scan."""
    present = [a for a in sorted(EXCHANGE_ADDRESSES)
               if any(g["address_min"] <= a <= g["address_max"] for g in groups)]
    if not anchors or maximum < len(present) or not present:
        raise ValueError("Pilot needs anchors and room for both known exchanges")
    selected: list[int] = []
    for anchor in anchors:
        for address in present:
            candidates = [g for g in groups if g["address_min"] <= address <= g["address_max"]]
            nearest = min(candidates, key=lambda g: (
                abs((g["block_min"] + g["block_max"]) // 2 - anchor), g["row_group"]))
            if nearest["row_group"] not in selected:
                selected.append(nearest["row_group"])
            if len(selected) >= maximum:
                return selected
    return selected


def choose_pilot_blocks(rows: Sequence[Mapping[str, Any]], maximum: int) -> list[int]:
    """Deterministic first/last, fees, repeated orders, then time-stratified blocks."""
    if maximum < 4 or not rows:
        raise ValueError("Pilot needs scoped discovery rows and at least four blocks")
    selected: list[int] = []
    def add(block: int) -> None:
        if block not in selected and len(selected) < maximum:
            selected.append(block)
    for address in sorted(EXCHANGE_ADDRESSES):
        blocks = sorted({int(r["block_number"]) for r in rows if r["exchange_address"].lower() == address})
        if blocks:
            add(blocks[0]); add(blocks[-1])
    for row in sorted(rows, key=lambda r: (r["block_number"], r["log_index"])):
        if row["fee"]:
            add(int(row["block_number"]))
    orders = Counter((r["transaction_hash"], r["order_hash"]) for r in rows)
    for row in sorted(rows, key=lambda r: (r["block_number"], r["log_index"])):
        if orders[(row["transaction_hash"], row["order_hash"])] > 1:
            add(int(row["block_number"]))
    blocks = sorted({int(r["block_number"]) for r in rows})
    for numerator in range(maximum):
        add(blocks[min(len(blocks)-1, numerator * len(blocks) // maximum)])
    return sorted(selected)


def complete_group_indices(groups: Sequence[Mapping[str, Any]], blocks: Sequence[int],
                           maximum: int) -> list[int]:
    selected = [int(g["row_group"]) for g in groups
                if any(g["block_min"] <= block <= g["block_max"] for block in blocks)]
    if len(selected) > maximum:
        raise ValueError(f"Complete-block retrieval would read {len(selected)} row groups, cap {maximum}")
    return selected


def inspect_batches(rows: Sequence[Mapping[str, Any]], complements: Mapping[str, str]
                    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Audit complete transaction/exchange groups with explicit refund accounting.

    Outcomes and wallet flags are never read. Unscoped batches are retained as
    audit statuses, not partially interpreted using an incomplete token map.
    """
    distinct = deduplicate_raw_fills(rows)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in distinct:
        if row["exchange_address"].lower() not in EXCHANGE_ADDRESSES:
            raise ValueError("Unknown exchange address in retrieved pilot records")
        grouped[(row["transaction_hash"], row["exchange_address"].lower())].append(row)
    audits: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for (tx, exchange), records in sorted(grouped.items()):
        pending: list[dict[str, Any]] = []
        for row in sorted(records, key=lambda r: r["log_index"]):
            if row["taker"].lower() != exchange:
                pending.append(row)
                continue
            token = row["taker_asset_id"] if row["maker_asset_id"] == "0" else row["maker_asset_id"]
            audit: dict[str, Any] = {
                "transaction_hash": tx, "exchange_address": exchange,
                "block_number": row["block_number"], "aggregate_log_index": row["log_index"],
                "passive_logs": len(pending), "status": "unscoped_batch",
                "reason": "", "active_side": "BUY" if row["maker_asset_id"] == "0" else "SELL",
                "normal_legs": 0, "mint_legs": 0, "merge_legs": 0,
                "effective_quantity_micro": 0, "effective_cash_micro": 0,
                "original_making_micro": row["maker_amount_filled"],
                "original_taking_micro": row["taker_amount_filled"],
                "refund_making_micro": 0, "active_fee_micro": row["fee"],
                "passive_fee_logs": sum(bool(r["fee"]) for r in pending),
                "source_contract_version": "ctf_exchange_v2_v1" if exchange in V2_EXCHANGE_ADDRESSES else "legacy_reserved_making_v1",
                "fee_rule": "collateral_extra_buy" if exchange in V2_EXCHANGE_ADDRESSES else "received_asset",
            }
            if token in complements:
                try:
                    makers = [decode_own_action(r, fee_rule=audit["fee_rule"]) for r in pending]
                    taker = decode_own_action(row, fee_rule=audit["fee_rule"])
                    batch = reconcile_reserved_match_batch(taker, makers, complements,
                        aggregate_amount_semantics="reserved_making_with_refund")
                    quantity_sum = batch.effective_taker.gross_quantity_micro
                    cash_sum = batch.effective_taker.gross_cash_micro
                    legs = batch.legs
                    kinds = Counter(leg.kind for leg in legs)
                    audit.update(status="accepted", normal_legs=kinds["NORMAL"], mint_legs=kinds["MINT"],
                                 merge_legs=kinds["MERGE"], effective_quantity_micro=quantity_sum,
                                 effective_cash_micro=cash_sum,
                                 refund_making_micro=batch.refund_micro)
                except ValueError as exc:
                    audit.update(status="rejected", reason=str(exc))
            counts[audit["status"]] += 1
            audits.append(audit)
            pending = []
        if pending:
            scoped = [r for r in pending if (r["taker_asset_id"] if r["maker_asset_id"] == "0"
                                           else r["maker_asset_id"]) in complements]
            counts["unassigned_scoped_logs"] += len(scoped)
            counts["unassigned_unscoped_logs"] += len(pending)-len(scoped)
    counts["input_log_rows"] = len(rows)
    counts["distinct_log_rows"] = len(distinct)
    counts["exact_replay_rows"] = len(rows)-len(distinct)
    counts["transaction_exchange_groups"] = len(grouped)
    counts["accepted_refund_batches"] = sum(a["status"] == "accepted" and a["refund_making_micro"] > 0 for a in audits)
    counts["accepted_nonzero_fee_batches"] = sum(a["status"] == "accepted" and (a["active_fee_micro"] > 0 or a["passive_fee_logs"] > 0) for a in audits)
    counts["accepted_normal_batches"] = sum(a["status"] == "accepted" and a["normal_legs"] > 0 for a in audits)
    counts["accepted_mint_batches"] = sum(a["status"] == "accepted" and a["mint_legs"] > 0 for a in audits)
    counts["accepted_merge_batches"] = sum(a["status"] == "accepted" and a["merge_legs"] > 0 for a in audits)
    return audits, dict(sorted(counts.items()))


def run(args: argparse.Namespace) -> dict[str, Any]:
    metadata, groups = parquet_metadata(args.raw_events)
    token_rows = pq.read_table(args.market_tokens,columns=["token_id","market_id","complement_token_id"]).to_pylist()
    complements = validate_complements(token_rows)
    con = duckdb.connect()
    con.execute("SET threads=2")
    clocks = con.execute("SELECT sport,count(*) markets,min(epoch(actual_start_utc)) start_epoch,"
                         "max(epoch(actual_end_utc)) end_epoch FROM read_parquet(?) GROUP BY sport ORDER BY sport",
                         [str(args.market_clocks)]).fetchall()
    anchors: list[int] = []
    for _, _, first, last in clocks:
        for epoch in (first, last):
            value = con.execute("SELECT min(block_number) FROM read_parquet(?) WHERE timestamp>=?",
                                [str(args.block_timestamps), int(epoch)]).fetchone()[0]
            if value is None:
                raise ValueError("Accepted sports clocks extend beyond exact block cache")
            anchors.append(int(value))
    discovery = choose_discovery_groups(groups, anchors, args.max_discovery_groups)
    wide_bytes=sum(groups[i]["compressed_bytes"] for i in discovery)
    if wide_bytes > args.max_wide_bytes:
        raise ValueError("Discovery wide-column reads exceed the compressed byte cap")
    source = pq.ParquetFile(args.raw_events)
    scoped_rows: list[dict[str, Any]] = []
    allowed = pa.array(sorted(complements))
    for index in discovery:
        table = source.read_row_group(index)
        mask = pc.or_(pc.is_in(table["maker_asset_id"], value_set=allowed),
                      pc.is_in(table["taker_asset_id"], value_set=allowed))
        scoped_rows.extend(table.filter(mask).to_pylist())
    blocks = choose_pilot_blocks(scoped_rows, args.max_pilot_blocks)
    complete_indices = complete_group_indices(groups, blocks, args.max_complete_groups)
    all_rows: list[dict[str, Any]] = []
    complete_wide_indices: list[int] = []
    block_array = pa.array(blocks, type=source.schema_arrow.field("block_number").type)
    for index in complete_indices:
        # Cheap projected block-column check avoids reading wide groups whose
        # footer interval contains the block but actual rows do not.
        numbers = source.read_row_group(index, columns=["block_number"])
        if pc.any(pc.is_in(numbers["block_number"], value_set=block_array)).as_py():
            wide_bytes += groups[index]["compressed_bytes"]
            if wide_bytes > args.max_wide_bytes:
                raise ValueError("Complete-block wide-column reads exceed the compressed byte cap")
            complete_wide_indices.append(index)
            table = source.read_row_group(index)
            all_rows.extend(table.filter(pc.is_in(table["block_number"], value_set=block_array)).to_pylist())
    scoped_txs = {(r["transaction_hash"], r["exchange_address"].lower()) for r in all_rows
                  if r["maker_asset_id"] in complements or r["taker_asset_id"] in complements}
    pilot = [r for r in all_rows if (r["transaction_hash"], r["exchange_address"].lower()) in scoped_txs]
    audits, counts = inspect_batches(pilot, complements)
    block_times = con.execute("SELECT block_number,timestamp FROM read_parquet(?) WHERE block_number IN ("
                              + ",".join("?" for _ in blocks) + ") ORDER BY block_number",
                              [str(args.block_timestamps), *blocks]).fetchall()
    if len(block_times) != len(blocks):
        raise ValueError("Selected pilot blocks lack unique exact timestamps")
    con.close()
    result = {
        "analysis": "bounded_raw_source_batch_pilot", "status": "complete_descriptive_audit",
        "source": metadata, "scope_markets": len(token_rows)//2, "scope_tokens": len(token_rows),
        "clocks": [{"sport": s, "markets": n, "start_epoch": first, "end_epoch": last} for s,n,first,last in clocks],
        "discovery_row_groups": discovery, "complete_block_row_groups": complete_indices,
        "complete_wide_row_groups":complete_wide_indices,"compressed_wide_read_bytes":wide_bytes,
        "pilot_blocks": [{"block_number": b, "timestamp": t} for b,t in block_times],
        "counts": counts,
        "limits": "Deterministic bounded pilot, not full-source coverage or causal/holdings evidence.",
        "fee_rules": {"legacy_reserved_making_v1":"Received asset: BUY outcome tokens; SELL collateral.",
                      "ctf_exchange_v2_v1":"BUY fee additional collateral; SELL fee deducted collateral."},
        "active_grain": "One own-order aggregate action, not another action per passive leg.",
        "source_contract": "https://github.com/Polymarket/ctf-exchange/blob/main/src/exchange/mixins/Trading.sol",
    }
    inputs = [args.raw_events,args.market_tokens,args.market_clocks,args.block_timestamps]
    with fresh_run(args.run_dir, inputs) as staging:
        pq.write_table(pa.Table.from_pylist(pilot, schema=source.schema_arrow), staging/"raw_pilot.parquet", compression="zstd")
        if not audits:
            raise ValueError("No scoped/unscoped batch evidence in bounded pilot")
        pq.write_table(pa.Table.from_pylist(audits), staging/"batch_audit.parquet", compression="zstd")
        for filename, expected in (("raw_pilot.parquet",len(pilot)),("batch_audit.parquet",len(audits))):
            if pq.ParquetFile(staging/filename).metadata.num_rows != expected:
                raise ValueError("Pilot output failed row-count reopen gate")
        write_json(staging/"summary.json", result)
        write_json(staging/"manifest.json", {
            "status": "complete", "created_utc": datetime.now(timezone.utc).isoformat(),
            "environment": {"python": sys.version, "platform": platform.platform(),
                            "duckdb": duckdb.__version__, "pyarrow": pa.__version__},
            "code": {"commit": subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),
                     "dirty_status": subprocess.check_output(["git","status","--porcelain"],text=True)},
            "raw_source_footer": metadata,
            "inputs": {p.name:fingerprint(p) for p in inputs[1:]},
            "command_arguments": {k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
            "outputs": {p.name:artifact_fingerprint(p) for p in sorted(staging.iterdir())},
        })
    return result


LEGACY_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
V2_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"


def normalize_receipt_log(log: Mapping[str, Any]) -> dict[str, Any] | None:
    """Verify original native ABI and normalize V2 side/token without guessing."""
    address = str(log["address"]).lower()
    if address not in EXCHANGE_ADDRESSES:
        return None
    topics = log.get("topics", [])
    if not topics or topics[0].lower() not in (LEGACY_TOPIC,V2_TOPIC):
        return None
    v2 = address in V2_EXCHANGE_ADDRESSES
    if topics[0].lower() != (V2_TOPIC if v2 else LEGACY_TOPIC):
        raise ValueError("Native event topic contradicts emitting contract generation")
    if len(topics) != 4 or any(len(t) != 66 for t in topics):
        raise ValueError("OrderFilled native indexed topics are malformed")
    data = log["data"].removeprefix("0x")
    if len(data) != (7 if v2 else 5)*64:
        raise ValueError("OrderFilled native data length contradicts ABI")
    words = [int(data[i:i+64],16) for i in range(0,len(data),64)]
    if v2:
        side,token,making,taking,fee,_,_ = words
        if side not in (0,1) or token <= 0:
            raise ValueError("V2 native side/token is invalid")
        maker_asset,taker_asset = (0,token) if side == 0 else (token,0)
    else:
        maker_asset,taker_asset,making,taking,fee = words
    if log.get("removed",False):
        raise ValueError("Native receipt contains a removed log")
    return dict(zip(RAW_FIELDS,(topics[1].lower(),"0x"+topics[2][-40:].lower(),
        "0x"+topics[3][-40:].lower(),str(maker_asset),str(taker_asset),making,taking,fee,
        int(log["blockNumber"],16),log["transactionHash"].lower(),
        int(log["logIndex"],16),address)))


def choose_receipt_transactions(audits: Sequence[Mapping[str, Any]], maximum: int) -> list[str]:
    if maximum < 1 or maximum > 20:
        raise ValueError("Receipt pilot must use one to twenty transactions")
    accepted = sorted((a for a in audits if a["status"] == "accepted"),
                      key=lambda a:(a["block_number"],a["aggregate_log_index"],a["transaction_hash"]))
    selected: list[str] = []
    def add(row: Mapping[str, Any]) -> None:
        tx = str(row["transaction_hash"])
        if tx not in selected and len(selected)<maximum:
            selected.append(tx)
    for version in ("legacy_reserved_making_v1","ctf_exchange_v2_v1"):
        group = [a for a in accepted if a["source_contract_version"] == version]
        if group:
            add(group[0]); add(group[-1])
        for field in ("refund_making_micro","active_fee_micro","passive_fee_logs","normal_legs","mint_legs","merge_legs"):
            matching = [a for a in group if a[field] > 0]
            if matching:
                add(matching[0])
    for row in accepted:
        add(row)
    return selected


def run_receipts(pilot: Path, run_dir: Path, maximum: int) -> dict[str, Any]:
    """Read-only bounded RPC validation, preserving only sanitized failures."""
    import requests
    rows = pq.read_table(pilot/"raw_pilot.parquet").to_pylist()
    audits = pq.read_table(pilot/"batch_audit.parquet").to_pylist()
    transactions = choose_receipt_transactions(audits,maximum)
    if not transactions:
        raise ValueError("No accepted pilot transactions for native validation")
    endpoint = os.environ.get("POLYGON_RPC_URL")
    if not endpoint:
        endpoint = runpy.run_path("/home/ubuntu/prediction_markets/pipeline/config.py")["RPC_URL"]
    evidence: list[dict[str, Any]] = []
    receipts: dict[str, dict[str, Any]] = {}
    session = requests.Session()
    for index,tx in enumerate(transactions):
        expected = deduplicate_raw_fills([r for r in rows if r["transaction_hash"] == tx])
        exchanges = {r["exchange_address"].lower() for r in expected}
        item: dict[str, Any] = {"transaction_hash":tx,"status":"unavailable",
                               "expected_logs":len(expected),"native_logs":0,"reason":""}
        try:
            response = session.post(endpoint,json={"jsonrpc":"2.0","id":index+1,
                                    "method":"eth_getTransactionReceipt","params":[tx]},timeout=30)
            if response.status_code != 200:
                item["reason"] = f"HTTP_status_{response.status_code}"
            else:
                payload = response.json()
                if "error" in payload:
                    item["reason"] = "RPC_error_code_"+str(payload["error"].get("code","unknown"))
                elif not payload.get("result"):
                    item["reason"] = "null_receipt"
                else:
                    receipt = payload["result"]
                    native = [n for n in (normalize_receipt_log(log) for log in receipt["logs"])
                              if n is not None and n["exchange_address"] in exchanges]
                    native = deduplicate_raw_fills(native)
                    item["native_logs"] = len(native)
                    if receipt.get("status") != "0x1" or receipt.get("transactionHash","").lower() != tx:
                        item.update(status="rejected",reason="receipt_identity_or_execution_status")
                    elif native != expected:
                        item.update(status="rejected",reason="native_normalization_or_complete_log_set_mismatch")
                    else:
                        item.update(status="verified")
                    receipts[tx] = receipt
        except requests.RequestException as exc:
            item["reason"] = "network_exception_"+type(exc).__name__
        except (ValueError,KeyError,TypeError) as exc:
            # Never record arbitrary exception text from HTTP/RPC payloads.
            item.update(status="rejected",reason="validation_exception_"+type(exc).__name__)
        evidence.append(item)
    result = {"analysis":"bounded_native_receipt_validation", "requested_transactions":len(transactions),
              "statuses":dict(Counter(e["status"] for e in evidence)),"evidence":evidence,
              "normalization":"Legacy native asset IDs; V2 native side+tokenId mapped to asset IDs.",
              "limits":"Only selected complete pilot transaction/exchange groups; no population coverage claim."}
    with fresh_run(run_dir,[pilot]) as staging:
        write_json(staging/"summary.json",result)
        write_json(staging/"native_receipts.json",receipts)
        write_json(staging/"manifest.json",{"status":"complete","created_utc":datetime.now(timezone.utc).isoformat(),
            "inputs":{"pilot_manifest":fingerprint(pilot/"manifest.json")},
            "outputs":{p.name:artifact_fingerprint(p) for p in sorted(staging.iterdir())},
            "endpoint":"configured_Polygon_RPC_not_recorded","method":"eth_getTransactionReceipt"})
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("raw-events", "market-tokens", "market-clocks", "block-timestamps"):
        parser.add_argument("--"+name, type=Path)
    parser.add_argument("--run-dir",type=Path,required=True)
    parser.add_argument("--receipt-pilot",type=Path)
    parser.add_argument("--receipt-evidence",type=Path)
    parser.add_argument("--receipt-limit",type=int,default=10)
    parser.add_argument("--max-discovery-groups", type=int, default=18)
    parser.add_argument("--max-pilot-blocks", type=int, default=12)
    parser.add_argument("--max-complete-groups", type=int, default=96)
    parser.add_argument("--max-wide-bytes",type=int,default=1024**3)
    args = parser.parse_args()
    require_production_host()
    if Path("/home/ubuntu/prediction_markets") not in Path(__file__).resolve().parents:
        raise ValueError("Production source pilots run only from the canonical EC2 checkout")
    if any(Path("/mnt/data") not in p.resolve().parents for p in (
            args.raw_events,args.market_tokens,args.market_clocks,args.block_timestamps,args.run_dir,args.receipt_pilot,args.receipt_evidence) if p is not None):
        raise ValueError("Production source inputs and outputs must be under /mnt/data")
    if args.receipt_pilot:
        if args.receipt_evidence:
            print(json.dumps(run_support(args.receipt_pilot,args.receipt_evidence,args.run_dir)["native_statuses"],sort_keys=True))
        else:
            print(json.dumps(run_receipts(args.receipt_pilot,args.run_dir,args.receipt_limit)["statuses"],sort_keys=True))
    else:
        if any(p is None for p in (args.raw_events,args.market_tokens,args.market_clocks,args.block_timestamps)):
            parser.error("A source pilot requires all four input paths")
        print(json.dumps(run(args)["counts"], sort_keys=True))


if __name__ == "__main__":
    main()
