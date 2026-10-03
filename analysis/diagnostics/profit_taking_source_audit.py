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
    EXCHANGE_ADDRESSES, LEGACY_EXCHANGE_ADDRESSES, RAW_FIELDS, V2_EXCHANGE_ADDRESSES, decode_own_action, deduplicate_raw_fills,
    reconcile_reserved_match_batch,
)
from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint, fingerprint, fresh_run, quoted, write_json,
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


def full_source_support(source_dir: Path, market_tokens: Path, maximum: int
                        ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Audit saved complete-scan artifacts, never manufacture partial support."""
    if not 1 <= maximum <= 20:
        raise ValueError("MERGE receipt pilot must use one to twenty transactions")
    manifest=json.loads((source_dir/"manifest.json").read_text())
    summary=json.loads((source_dir/"summary.json").read_text())
    if manifest.get('inputs',{}).get('market_tokens') != fingerprint(market_tokens):
        raise ValueError('Audit token spine differs from the parent source input fingerprint')
    if manifest["status"] != summary["status"] or manifest["counts"] != summary["counts"]:
        raise ValueError("Full source summary and manifest disagree")
    for name in ("summary.json","batch_audit.parquet","batch_links.parquet",
                 "orphan_logs.parquet","source_exclusions.parquet"):
        observed=artifact_fingerprint(source_dir/name)
        if observed != manifest["outputs"][name]:
            raise ValueError("Full source audit input changed: "+name)
    surplus_evidence=verify_source_settlement_surpluses(source_dir/'manifest.json',market_tokens)
    con=duckdb.connect()
    try:
        con.execute("SET threads=2")
        con.execute("SET memory_limit='8GB'")
        con.execute("SET max_temp_directory_size='0GB'")
        for relation,name in (("batches","batch_audit.parquet"),("links","batch_links.parquet"),
                              ("orphans","orphan_logs.parquet"),("exclusions","source_exclusions.parquet")):
            con.execute(f"CREATE VIEW {relation} AS SELECT * FROM read_parquet('{quoted(source_dir/name)}')")
        con.execute(f"CREATE VIEW tokens AS SELECT token_id FROM read_parquet('{quoted(market_tokens)}')")
        def records(sql: str) -> list[dict[str, Any]]:
            result=con.execute(sql)
            names=[column[0] for column in result.description]
            return [dict(zip(names,row)) for row in result.fetchall()]
        statuses=records("""SELECT exchange_address,source_contract_version,status,count(*)::BIGINT batches
            FROM batches GROUP BY ALL ORDER BY exchange_address,status""")
        reasons=records("""SELECT exclusion_reason,count(*)::BIGINT original_logs FROM exclusions
            GROUP BY exclusion_reason ORDER BY exclusion_reason""")
        orphans=records("""SELECT exchange_address,source_contract_version,
            t.token_id IS NOT NULL scoped,count(*)::BIGINT original_logs
            FROM orphans o LEFT JOIN tokens t USING(token_id) GROUP BY ALL ORDER BY exchange_address,scoped""")
        counts=summary["counts"]
        if sum(s["batches"] for s in statuses) != counts["aggregate_batches"]:
            raise ValueError("Full source aggregate counts do not reconcile")
        if sum(s["batches"] for s in statuses if s["status"] not in ('accepted','unscoped_batch')) != counts["rejected_relevant_batches"]:
            raise ValueError("Full source rejected counts do not reconcile")
        if sum(o["original_logs"] for o in orphans if o["scoped"]) != counts["orphan_scoped_logs"]:
            raise ValueError("Full source scoped orphan counts do not reconcile")
        complete=summary["status"]=='complete' and not counts["rejected_relevant_batches"] and not counts["orphan_scoped_logs"]
        result={"analysis":"full_source_reconciliation_and_merge_readiness",
                "source_status":summary["status"],"counts":counts,
                "batch_status_counts":statuses,"excluded_original_log_reasons":reasons,
                "orphan_log_counts":orphans,"accepted_match_support":[],
                "settlement_surplus_gates":surplus_evidence,
                "support_population":"No mechanism profiles from a blocked accepted subset.",
                "source_resource_limits":{"threads":2,"memory_limit":"8GB","spill_limit":"0GB"}}
        if not complete:
            result["status"]="blocked_source_reconciliation"
            return result,[]
        result["support_population"]="All accepted source batches; complete scan has no relevant rejection or scoped orphan."
        observed_surpluses=records('''SELECT active_execution_id,settlement_surplus_cash_micro,
            effective_quantity_micro,effective_cash_micro,original_making_micro,original_taking_micro,
            settlement_surplus_proof_id FROM batches WHERE settlement_surplus_cash_micro>0 ORDER BY active_execution_id''')
        if observed_surpluses!=sorted(surplus_evidence['cases'],key=lambda c:c['active_execution_id']):
            raise ValueError('Full source surplus batch amounts differ from every exact native proof case')
        if con.execute("SELECT count(*) FROM batches WHERE settlement_surplus_cash_micro>0 AND settlement_surplus_proof_status IS DISTINCT FROM 'verified_complete_native_sell_collateral_surplus'").fetchone()[0]:
            raise ValueError('Full source surplus batch lacks its verified native proof status')
        result["accepted_match_support"]=records("""SELECT b.exchange_address,b.source_contract_version,l.kind,
            count(*)::BIGINT legs,sum(l.quantity_micro)::HUGEINT gross_quantity_micro
            FROM links l JOIN batches b ON l.active_execution_id=b.active_execution_id
            WHERE b.status='accepted' GROUP BY ALL ORDER BY b.exchange_address,l.kind""")
        if sum(s["legs"] for s in result["accepted_match_support"]) != counts["accepted_links"]:
            raise ValueError("Full source match-kind population does not reconcile")
        candidates=records("""WITH merge_batches AS (SELECT DISTINCT active_execution_id FROM links WHERE kind='MERGE')
            SELECT b.* FROM batches b JOIN merge_batches m USING(active_execution_id)
            WHERE b.status='accepted'
            QUALIFY row_number() OVER (PARTITION BY exchange_address ORDER BY block_number,log_index,active_execution_id)=1
            ORDER BY exchange_address""")
        if len({r["transaction_hash"] for r in candidates}) > maximum:
            raise ValueError("Receipt cap cannot cover every observed MERGE exchange address")
        result["status"]="pending_native_merge_receipts" if candidates else "complete_no_merge_native_gate_not_applicable"
        result["merge_observed_addresses"]=sorted({r["exchange_address"] for r in candidates})
        result["merge_selected_transactions"]=len({r["transaction_hash"] for r in candidates})
        return result,candidates
    finally:
        con.close()


def retrieve_complete_transaction_groups(raw_events: Path, groups: Sequence[Mapping[str, Any]],
        candidates: Sequence[Mapping[str, Any]], max_complete_groups: int, max_wide_bytes: int
        ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read every raw log in bounded selected tx/exchange groups, not scoped legs."""
    blocks=sorted({int(r["block_number"]) for r in candidates})
    selected={(r["transaction_hash"],r["exchange_address"].lower()) for r in candidates}
    indices=complete_group_indices(groups,blocks,max_complete_groups)
    source=pq.ParquetFile(raw_events)
    block_array=pa.array(blocks,type=source.schema_arrow.field("block_number").type)
    tx_array=pa.array(sorted({tx for tx,_ in selected}))
    rows: list[dict[str, Any]]=[]
    wide_indices: list[int]=[]
    wide_bytes=0
    for index in indices:
        numbers=source.read_row_group(index,columns=["block_number"])
        if not pc.any(pc.is_in(numbers["block_number"],value_set=block_array)).as_py():
            continue
        wide_bytes+=int(groups[index]["compressed_bytes"])
        if wide_bytes > max_wide_bytes:
            raise ValueError("MERGE complete-group wide-column reads exceed the compressed byte cap")
        wide_indices.append(index)
        table=source.read_row_group(index)
        mask=pc.and_(pc.is_in(table["block_number"],value_set=block_array),
                     pc.is_in(table["transaction_hash"],value_set=tx_array))
        rows.extend(r for r in table.filter(mask).to_pylist()
                    if (r["transaction_hash"],r["exchange_address"].lower()) in selected)
    if {(r["transaction_hash"],r["exchange_address"].lower()) for r in rows} != selected:
        raise ValueError("Selected MERGE transaction/exchange group is absent from raw source")
    return rows,{"selected_blocks":blocks,"complete_block_row_groups":indices,
                 "complete_wide_row_groups":wide_indices,"compressed_wide_read_bytes":wide_bytes}


def run_full_source_audit(args: argparse.Namespace) -> dict[str, Any]:
    """Save full-scan coverage; prepare a tiny complete MERGE native pilot only after all gates pass."""
    result,candidates=full_source_support(args.full_source,args.market_tokens,args.receipt_limit)
    inputs=[args.full_source,args.market_tokens]
    pilot: list[dict[str, Any]]=[]
    selected_audits: list[dict[str, Any]]=[]
    if candidates:
        metadata,groups=parquet_metadata(args.raw_events)
        prior=json.loads((args.full_source/"summary.json").read_text())["raw_source"]
        if any(metadata[k] != prior[k] for k in ("bytes","rows","row_groups","footer_sha256","block_min","block_max")):
            raise ValueError("MERGE pilot raw source differs from complete-scan source footer")
        complements=validate_complements(pq.read_table(args.market_tokens,
            columns=["token_id","market_id","complement_token_id"]).to_pylist())
        pilot,budget=retrieve_complete_transaction_groups(args.raw_events,groups,candidates,
                                                        args.max_complete_groups,args.max_wide_bytes)
        audits,pilot_counts=inspect_batches(pilot,complements)
        by_identity={(a["transaction_hash"],a["exchange_address"],a["aggregate_log_index"]):a for a in audits}
        for candidate in candidates:
            key=(candidate["transaction_hash"],candidate["exchange_address"].lower(),candidate["log_index"])
            audit=by_identity.get(key)
            if not audit or audit["status"]!='accepted' or not audit["merge_legs"]:
                raise ValueError("Selected full-source MERGE batch did not reconcile in complete raw group")
            if any(audit[k] != candidate[k] for k in ("effective_quantity_micro","effective_cash_micro",
                                                     "refund_making_micro","source_contract_version")):
                raise ValueError("MERGE raw pilot and full-source effective amounts disagree")
            selected_audits.append(audit)
        # Every selected MERGE address is retained in the chooser input. Raw
        # pilot includes ALL logs, including unrelated sibling batches.
        result.update(merge_pilot_counts=pilot_counts,raw_source=metadata,read_budget=budget)
        inputs.append(args.raw_events)
    with fresh_run(args.run_dir,inputs) as staging:
        if candidates:
            pq.write_table(pa.Table.from_pylist(pilot,schema=pq.ParquetFile(args.raw_events).schema_arrow),
                           staging/"raw_pilot.parquet",compression="zstd")
            pq.write_table(pa.Table.from_pylist(selected_audits),staging/"batch_audit.parquet",compression="zstd")
        write_json(staging/"summary.json",result)
        if verify_source_settlement_surpluses(args.full_source/'manifest.json',args.market_tokens)!=result['settlement_surplus_gates']:
            raise ValueError('Immutable settlement surplus evidence changed during source audit publication')
        write_json(staging/"manifest.json",{
            "status":result["status"],"created_utc":datetime.now(timezone.utc).isoformat(),
            "environment":{"python":sys.version,"platform":platform.platform(),
                           "duckdb":duckdb.__version__,"pyarrow":pa.__version__},
            "code":{"commit":subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),
                    "dirty_status":subprocess.check_output(["git","status","--porcelain"],text=True)},
            "producing_script":"analysis/diagnostics/profit_taking_source_audit.py",
            "inputs":{"source_manifest":fingerprint(args.full_source/"manifest.json"),
                      "market_tokens":fingerprint(args.market_tokens)},
            "command_arguments":{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
            "outputs":{p.name:artifact_fingerprint(p) for p in sorted(staging.iterdir())},
            "native_gate":("Pending: compare exact native normalized log sets for every observed MERGE address; no RPC in this stage."
                           if candidates else result["status"])})
    return result


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
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC1155_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
ERC1155_BATCH_TOPIC = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
GET_COLLATERAL_SELECTOR = "0x5c1548fb"
GET_CTF_SELECTOR = "0x3b521d78"


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


def decode_asset_transfers(receipt: Mapping[str, Any], collateral: str, ctf: str
                          ) -> list[dict[str, Any]]:
    """Decode every collateral/ERC1155 movement in this receipt, without wallet inference."""
    rows: list[dict[str, Any]]=[]
    for log in receipt.get('logs',[]):
        address=str(log['address']).lower()
        topics=log.get('topics',[])
        if not topics or address not in (collateral,ctf):
            continue
        topic=topics[0].lower()
        if topic not in (ERC20_TRANSFER_TOPIC,ERC1155_SINGLE_TOPIC,ERC1155_BATCH_TOPIC):
            continue
        if (log.get('removed',False) or any(len(t)!=66 for t in topics)
                or log.get('transactionHash','').lower()!=receipt.get('transactionHash','').lower()
                or log.get('blockNumber')!=receipt.get('blockNumber')):
            raise ValueError('Malformed native transfer identity')
        data=log['data'].removeprefix('0x')
        if len(data)%64:
            raise ValueError('Malformed native transfer data')
        words=[int(data[i:i+64],16) for i in range(0,len(data),64)]
        if topic==ERC20_TRANSFER_TOPIC:
            if address!=collateral or len(topics)!=3 or len(words)!=1:
                raise ValueError('Transfer ABI contradicts verified collateral identity')
            sender,receiver='0x'+topics[1][-40:].lower(),'0x'+topics[2][-40:].lower()
            assets=[('0',words[0])]; kind='COLLATERAL'
        else:
            if address!=ctf or len(topics)!=4:
                raise ValueError('Transfer ABI contradicts verified CTF identity')
            sender,receiver='0x'+topics[2][-40:].lower(),'0x'+topics[3][-40:].lower()
            kind='OUTCOME'
            if topic==ERC1155_SINGLE_TOPIC:
                if len(words)!=2:
                    raise ValueError('TransferSingle has invalid data length')
                assets=[(str(words[0]),words[1])]
            else:
                if len(words)<4 or words[0]!=64:
                    raise ValueError('TransferBatch has invalid first array offset')
                length=words[2]
                second=3+length
                if words[1]!=32*second or second>=len(words) or words[second]!=length or len(words)!=4+2*length:
                    raise ValueError('TransferBatch arrays do not reconcile')
                assets=[(str(token),amount) for token,amount in zip(words[3:second],words[second+1:])]
        for asset,amount in assets:
            rows.append({'log_index':int(log['logIndex'],16),'token_contract':address,'asset_kind':kind,
                         'asset_id':asset,'from_wallet':sender,'to_wallet':receiver,'amount_raw':amount})
    return sorted(rows,key=lambda r:(r['log_index'],r['asset_id']))


def rejected_receipt_comparison(original: Sequence[Mapping[str, Any]], receipt: Mapping[str, Any],
        collateral: str | None, ctf: str | None, complements: Mapping[str, str]
        ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Compare original rejected records with native logs and actual asset payouts."""
    expected=deduplicate_raw_fills(original)
    groups={(r['transaction_hash'],r['exchange_address'].lower()) for r in expected}
    if len(groups)!=1:
        raise ValueError('Rejected-batch diagnostic is limited to one transaction/exchange group')
    tx,exchange=next(iter(groups))
    native=deduplicate_raw_fills([n for n in (normalize_receipt_log(log) for log in receipt.get('logs',[]))
                                 if n is not None and n['exchange_address']==exchange])
    receipt_ok=(receipt.get('status')=='0x1' and receipt.get('transactionHash','').lower()==tx
                and {r['block_number'] for r in expected}=={int(receipt.get('blockNumber','0x0'),16)})
    native_match=receipt_ok and native==expected
    aggregates=[r for r in expected if r['taker'].lower()==exchange]
    if len(aggregates)!=1:
        raise ValueError('Rejected-batch diagnostic requires exactly one original active aggregate')
    active=aggregates[0]
    active_side='BUY' if active['maker_asset_id']=='0' else 'SELL'
    active_token=active['taker_asset_id'] if active_side=='BUY' else active['maker_asset_id']
    matched_quantity=matched_cash=0
    mechanisms=Counter()
    for passive in expected:
        if passive is active:
            continue
        side='BUY' if passive['maker_asset_id']=='0' else 'SELL'
        token=passive['taker_asset_id'] if side=='BUY' else passive['maker_asset_id']
        quantity=passive['taker_amount_filled'] if side=='BUY' else passive['maker_amount_filled']
        cash=passive['maker_amount_filled'] if side=='BUY' else passive['taker_amount_filled']
        if side!=active_side and token==active_token:
            kind='NORMAL'; active_cash=cash
        elif side==active_side and token==complements.get(active_token):
            kind='MINT' if side=='BUY' else 'MERGE'; active_cash=quantity-cash
        else:
            raise ValueError('Rejected-batch asset relationship remains unexplained')
        mechanisms[kind]+=1
        matched_quantity+=quantity; matched_cash+=active_cash
    transfers=decode_asset_transfers(receipt,collateral,ctf) if collateral and ctf else []
    def flow(wallet: str, asset_kind: str) -> dict[str,int]:
        incoming=sum(r['amount_raw'] for r in transfers if r['asset_kind']==asset_kind and r['to_wallet']==wallet)
        outgoing=sum(r['amount_raw'] for r in transfers if r['asset_kind']==asset_kind and r['from_wallet']==wallet)
        return {'incoming_raw':incoming,'outgoing_raw':outgoing,'net_incoming_raw':incoming-outgoing}
    wallet_roles=defaultdict(set)
    for r in expected:
        wallet_roles[r['maker'].lower()].add('active' if r['taker'].lower()==exchange else 'passive')
    wallets=[{'wallet':wallet,'roles':','.join(sorted(roles)),**flow(wallet,'COLLATERAL')}
             for wallet,roles in sorted(wallet_roles.items())]
    result={'analysis':'bounded_rejected_source_native_and_transfer_audit',
            'status':'verified_native_observations_only' if native_match and collateral and ctf else 'blocked_native_problem_evidence',
            'transaction_hash':tx,'exchange_address':exchange,'block_number':active['block_number'],
            'source_original_logs':len(expected),'native_exchange_orderfilled_logs':len(native),
            'native_original_log_set_exact_match':native_match,'receipt_identity_and_success':receipt_ok,
            'collateral_contract':collateral,'ctf_contract':ctf,'matched_mechanisms':dict(mechanisms),
            'active_side':active_side,'matched_quantity_raw':matched_quantity,'matched_cash_raw':matched_cash,
            'aggregate_original_making_raw':active['maker_amount_filled'],
            'aggregate_original_taking_raw':active['taker_amount_filled'],
            'unexplained_receiving_asset_excess_raw':active['taker_amount_filled']-(matched_quantity if active_side=='BUY' else matched_cash),
            'exchange_collateral_flow':flow(exchange,'COLLATERAL') if collateral and ctf else None,
            'decoded_transfer_rows':len(transfers),
            'limits':'Native receipt proves emitted amounts and movements, not the origin of an opening exchange balance. No source correction or gate relaxation.'}
    return result,native,transfers,wallets


def run_rejected_source_batch(source_dir: Path, market_tokens: Path, run_dir: Path) -> dict[str, Any]:
    """One rejected batch, one native receipt, two immutable-asset getter calls; no raw-source scan."""
    import requests
    manifest=json.loads((source_dir/'manifest.json').read_text())
    if manifest.get('inputs',{}).get('market_tokens') != fingerprint(market_tokens):
        raise ValueError('Problem token spine differs from the parent source input fingerprint')
    if (manifest.get('status')!='blocked_source_reconciliation'
            or manifest.get('counts',{}).get('rejected_relevant_batches')!=1
            or manifest.get('counts',{}).get('orphan_scoped_logs')!=0):
        raise ValueError('Problem diagnostic requires exactly one rejected batch and no scoped orphans')
    exclusions=source_dir/'source_exclusions.parquet'
    if manifest.get('outputs',{}).get(exclusions.name)!=artifact_fingerprint(exclusions):
        raise ValueError('Rejected original records differ from their source-stage manifest')
    table=pq.read_table(exclusions,columns=[*RAW_FIELDS,'exclusion_reason'])
    original=[{name:r[name] for name in RAW_FIELDS} for r in table.to_pylist()]
    groups={(r['transaction_hash'],r['exchange_address'].lower()) for r in original}
    if len(groups)!=1 or len({r['block_number'] for r in original})!=1:
        raise ValueError('Problem diagnostic is limited to one rejected transaction/exchange batch')
    tx,exchange=next(iter(groups)); block=original[0]['block_number']
    complements=validate_complements(pq.read_table(market_tokens,
        columns=['token_id','market_id','complement_token_id']).to_pylist())
    endpoint=os.environ.get('POLYGON_RPC_URL')
    if not endpoint:
        endpoint=runpy.run_path('/home/ubuntu/prediction_markets/pipeline/config.py')['RPC_URL']
    session=requests.Session(); statuses={}; responses={}
    def rpc(name: str, method: str, params: list[Any]) -> Any:
        try:
            response=session.post(endpoint,json={'jsonrpc':'2.0','id':len(statuses)+1,'method':method,'params':params},timeout=30)
            if response.status_code!=200:
                statuses[name]='HTTP_status_'+str(response.status_code); return None
            payload=response.json()
            if 'error' in payload:
                code=payload['error'].get('code')
                statuses[name]='RPC_error_code_'+(str(code) if isinstance(code,int) and not isinstance(code,bool) else 'unknown')
                return None
            value=payload.get('result')
            statuses[name]='available' if value is not None else 'null_result'
            responses[name]=value
            return value
        except requests.RequestException as exc:
            statuses[name]='network_exception_'+type(exc).__name__
        except (ValueError,KeyError,TypeError) as exc:
            statuses[name]='response_exception_'+type(exc).__name__
        return None
    receipt=rpc('receipt','eth_getTransactionReceipt',[tx])
    collateral=ctf=None
    if receipt is not None:
        for name,selector in (('collateral',GET_COLLATERAL_SELECTOR),('ctf',GET_CTF_SELECTOR)):
            value=rpc(name,'eth_call',[{'to':exchange,'data':selector},hex(block)])
            if isinstance(value,str) and len(value)==66 and value.startswith('0x'):
                try:
                    if int(value[2:26],16)==0 and int(value[-40:],16)>0:
                        if name=='collateral': collateral='0x'+value[-40:].lower()
                        else: ctf='0x'+value[-40:].lower()
                    else: statuses[name]='invalid_address_result'
                except ValueError:
                    statuses[name]='invalid_address_result'
            else:
                if value is not None: statuses[name]='invalid_address_result'
    result={'analysis':'bounded_rejected_source_native_and_transfer_audit','status':'blocked_native_problem_evidence'}
    native=[];transfers=[];wallets=[]
    if receipt is not None:
        try:
            result,native,transfers,wallets=rejected_receipt_comparison(original,receipt,collateral,ctf,complements)
        except (ValueError,KeyError,TypeError) as exc:
            result['validation_reason']='validation_exception_'+type(exc).__name__
    result.update(rpc_statuses=statuses,receipt_requests=1,asset_getter_calls=len(statuses)-1,
                  source_exclusion_reasons=sorted(set(table['exclusion_reason'].to_pylist())))
    with fresh_run(run_dir,[source_dir,market_tokens]) as staging:
        pq.write_table(table.select(list(RAW_FIELDS)),staging/'original_rows.parquet',compression='zstd')
        if native:
            pq.write_table(pa.Table.from_pylist(native,schema=table.select(list(RAW_FIELDS)).schema),staging/'normalized_native.parquet',compression='zstd')
        if transfers:
            pq.write_table(pa.Table.from_pylist(transfers),staging/'transfers.parquet',compression='zstd')
        if wallets:
            pq.write_table(pa.Table.from_pylist(wallets),staging/'wallet_collateral_flows.parquet',compression='zstd')
        write_json(staging/'native_receipt.json',receipt or {})
        write_json(staging/'asset_getter_results.json',{k:v for k,v in responses.items() if k!='receipt'})
        write_json(staging/'summary.json',result)
        write_json(staging/'manifest.json',{'status':result['status'],'created_utc':datetime.now(timezone.utc).isoformat(),
            'code':{'commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
                    'dirty_status':subprocess.check_output(['git','status','--porcelain'],text=True)},
            'environment':{'python':sys.version,'platform':platform.platform(),'pyarrow':pa.__version__},
            'inputs':{'source_manifest':fingerprint(source_dir/'manifest.json'),'source_exclusions':fingerprint(exclusions),
                      'market_tokens':fingerprint(market_tokens)},
            'endpoint':'configured_Polygon_RPC_not_recorded','rpc_methods':['eth_getTransactionReceipt','eth_call'],
            'budgets':{'transactions':1,'asset_getters':2,'raw_source_rows_read':0,'native_logs':'entire receipt'},
            'outputs':{p.name:artifact_fingerprint(p) for p in sorted(staging.iterdir())}})
    return result


def load_verified_settlement_surpluses(proof_manifests: Sequence[Path], market_tokens: Path
        ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Reopen complete native evidence; never certify a positive amount alone.

    Only the isolated, fully compared legacy SELL diagnostic is supported. The
    returned original rows let the full source builder compare the entire raw
    transaction/exchange group, rather than matching merely its active log.
    """
    complements=validate_complements(pq.read_table(market_tokens,
        columns=['token_id','market_id','complement_token_id']).to_pylist())
    token_input=fingerprint(market_tokens)
    cases=[]; originals=[]; lineage={}
    for proof_path in proof_manifests:
        proof_path=Path(proof_path)
        if proof_path.name!='manifest.json':
            raise ValueError('Settlement surplus evidence requires a stage manifest.json')
        manifest=json.loads(proof_path.read_text())
        parent_ref=manifest.get('inputs',{}).get('source_manifest',{})
        parent_path=Path(parent_ref.get('path',''))
        if not parent_path.is_file() or fingerprint(parent_path)!=parent_ref:
            raise ValueError('Settlement surplus parent source lineage differs from its proof')
        parent=json.loads(parent_path.read_text())
        if (parent.get('status')!='blocked_source_reconciliation'
                or parent.get('counts',{}).get('rejected_relevant_batches')!=1
                or parent.get('counts',{}).get('orphan_scoped_logs')!=0
                or manifest.get('inputs',{}).get('market_tokens')!=token_input
                or parent.get('inputs',{}).get('market_tokens')!=token_input):
            raise ValueError('Settlement surplus proof has incompatible source/token lineage')
        exclusion_path=parent_path.parent/'source_exclusions.parquet'
        if (manifest.get('inputs',{}).get('source_exclusions')!=fingerprint(exclusion_path)
                or parent.get('outputs',{}).get(exclusion_path.name)!=artifact_fingerprint(exclusion_path)):
            raise ValueError('Settlement surplus original exclusions differ from their parent source')
        needed=('summary.json','original_rows.parquet','normalized_native.parquet',
                'native_receipt.json','asset_getter_results.json','transfers.parquet','wallet_collateral_flows.parquet')
        for filename in needed:
            path=proof_path.parent/filename
            if not path.is_file() or manifest.get('outputs',{}).get(filename)!=artifact_fingerprint(path):
                raise ValueError('Settlement surplus native proof artifact fingerprint differs: '+filename)
        summary=json.loads((proof_path.parent/'summary.json').read_text())
        if (manifest.get('status')!='verified_native_observations_only'
                or summary.get('status')!='verified_native_observations_only'
                or summary.get('rpc_statuses')!={'receipt':'available','collateral':'available','ctf':'available'}):
            raise ValueError('Settlement surplus proof lacks fully available verified native evidence')
        rows=deduplicate_raw_fills(pq.read_table(proof_path.parent/'original_rows.parquet',columns=list(RAW_FIELDS)).to_pylist())
        exclusions=deduplicate_raw_fills(pq.read_table(exclusion_path,columns=list(RAW_FIELDS)).to_pylist())
        if rows!=exclusions:
            raise ValueError('Settlement surplus original rows differ from the complete rejected source records')
        getters=json.loads((proof_path.parent/'asset_getter_results.json').read_text())
        addresses={}
        for name in ('collateral','ctf'):
            value=getters.get(name)
            if (not isinstance(value,str) or len(value)!=66 or not value.startswith('0x')
                    or int(value[2:26],16)!=0 or int(value[-40:],16)<=0):
                raise ValueError('Settlement surplus proof has invalid immutable asset getters')
            addresses[name]='0x'+value[-40:].lower()
        receipt=json.loads((proof_path.parent/'native_receipt.json').read_text())
        checked,native,transfers,wallets=rejected_receipt_comparison(rows,receipt,
            addresses['collateral'],addresses['ctf'],complements)
        if any(summary.get(name)!=value for name,value in checked.items()):
            raise ValueError('Settlement surplus saved summary does not reproduce its native proof')
        if (native!=deduplicate_raw_fills(pq.read_table(proof_path.parent/'normalized_native.parquet').to_pylist())
                or transfers!=pq.read_table(proof_path.parent/'transfers.parquet').to_pylist()
                or wallets!=pq.read_table(proof_path.parent/'wallet_collateral_flows.parquet').to_pylist()):
            raise ValueError('Settlement surplus native observations do not reproduce saved evidence')
        exchange=checked['exchange_address']; surplus=checked['unexplained_receiving_asset_excess_raw']
        if (checked['status']!='verified_native_observations_only'
                or not checked['native_original_log_set_exact_match']
                or exchange not in LEGACY_EXCHANGE_ADDRESSES or checked['active_side']!='SELL'
                or surplus<=0 or checked['matched_quantity_raw']<=0
                or not 0<=checked['matched_cash_raw']<=checked['matched_quantity_raw']):
            raise ValueError('Settlement surplus proof is not a complete legacy SELL collateral-surplus case')
        active=next(r for r in rows if r['taker'].lower()==exchange)
        payout=sum(t['amount_raw'] for t in transfers if t['asset_kind']=='COLLATERAL'
                   and t['from_wallet']==exchange and t['to_wallet']==active['maker'].lower())
        if (payout!=active['taker_amount_filled']-active['fee']
                or checked['exchange_collateral_flow']['net_incoming_raw']!=-surplus):
            raise ValueError('Settlement surplus actual native payout/collateral conservation does not reconcile')
        execution_id=exchange+':'+active['transaction_hash'].lower()+':'+str(active['log_index'])
        if execution_id in lineage:
            raise ValueError('Settlement surplus evidence duplicates one original active execution')
        proof_id=fingerprint(proof_path)['sha256']
        cases.append({'active_execution_id':execution_id,'settlement_surplus_cash_micro':surplus,
                      'effective_quantity_micro':checked['matched_quantity_raw'],
                      'effective_cash_micro':checked['matched_cash_raw'],
                      'original_making_micro':active['maker_amount_filled'],
                      'original_taking_micro':active['taker_amount_filled'],
                      'settlement_surplus_proof_id':proof_id})
        originals.extend(rows)
        lineage[execution_id]={'proof_manifest':fingerprint(proof_path),
            'parent_source_manifest':parent_ref,'raw_events':parent['inputs']['raw_events'],
            'settlement_surplus_cash_micro':surplus,'proof_id':proof_id,
            'status':'verified_complete_native_sell_collateral_surplus'}
    return cases,originals,lineage


def verify_source_settlement_surpluses(source_manifest: Path, market_tokens: Path) -> dict[str,Any]:
    """Revalidate every manifested exception from saved native proof and its lineage."""
    source=json.loads(source_manifest.read_text())
    refs=source.get('inputs',{}).get('surplus_receipt_manifests',[])
    if not isinstance(refs,list):
        raise ValueError('Settlement surplus source proof inputs must be explicit manifest fingerprints')
    paths=[]
    for ref in refs:
        path=Path(ref.get('path',''))
        if not path.is_file() or fingerprint(path)!=ref:
            raise ValueError('Settlement surplus source proof input lineage changed')
        paths.append(path)
    cases,_,lineage=load_verified_settlement_surpluses(paths,market_tokens)
    if source.get('settlement_surplus_proofs',{})!=lineage:
        raise ValueError('Settlement surplus source cases differ from reopened native proof')
    counts=source.get('counts',{})
    if (counts.get('accepted_settlement_surplus_batches',0)!=len(cases)
            or counts.get('accepted_settlement_surplus_cash_micro',0)!=sum(c['settlement_surplus_cash_micro'] for c in cases)):
        raise ValueError('Settlement surplus source counts differ from its exact native proof cases')
    if any(source.get('inputs',{}).get('raw_events')!=p['raw_events'] for p in lineage.values()):
        raise ValueError('Settlement surplus source raw vintage differs from its native proof lineage')
    return {'status':'verified_complete_native_sell_collateral_surplus' if cases else 'not_applicable',
            'cases':cases,'proof_lineage':lineage,
            'policy':'Preserve original settlement; matched execution cash alone defines binary price and trading profit.'}


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


def merge_native_coverage(audits: Sequence[Mapping[str, Any]], evidence: Sequence[Mapping[str, Any]],
                          required_addresses: Sequence[str]) -> dict[str, Any]:
    """Require native-verified MERGE support at every full-source observed address."""
    verified={e["transaction_hash"] for e in evidence if e["status"]=='verified'}
    covered={a["exchange_address"] for a in audits if a["status"]=='accepted'
             and a["merge_legs"]>0 and a["transaction_hash"] in verified}
    required=set(required_addresses)
    complete=bool(evidence) and all(e["status"]=='verified' for e in evidence) and required <= covered
    return {"required_merge_exchange_addresses":sorted(required),
            "verified_merge_exchange_addresses":sorted(covered),
            "merge_native_gate_status":("not_requested" if not required else
                                        "verified" if complete else "blocked_native_merge_evidence")}


def run_receipts(pilot: Path, run_dir: Path, maximum: int) -> dict[str, Any]:
    """Read-only bounded RPC validation, preserving only sanitized failures."""
    import requests
    rows = pq.read_table(pilot/"raw_pilot.parquet").to_pylist()
    audits = pq.read_table(pilot/"batch_audit.parquet").to_pylist()
    pilot_summary=json.loads((pilot/"summary.json").read_text())
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
    result.update(merge_native_coverage(audits,evidence,pilot_summary.get("merge_observed_addresses",[])))
    with fresh_run(run_dir,[pilot]) as staging:
        write_json(staging/"summary.json",result)
        write_json(staging/"native_receipts.json",receipts)
        write_json(staging/"manifest.json",{"status":"complete","created_utc":datetime.now(timezone.utc).isoformat(),
            "code":{"commit":subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),
                    "dirty_status":subprocess.check_output(["git","status","--porcelain"],text=True)},
            "environment":{"python":sys.version,"platform":platform.platform(),
                           "pyarrow":pa.__version__},
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
    parser.add_argument("--full-source",type=Path)
    parser.add_argument("--rejected-source-batch",type=Path)
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
            args.raw_events,args.market_tokens,args.market_clocks,args.block_timestamps,args.run_dir,
            args.receipt_pilot,args.receipt_evidence,args.full_source,args.rejected_source_batch) if p is not None):
        raise ValueError("Production source inputs and outputs must be under /mnt/data")
    if args.rejected_source_batch:
        if args.full_source or args.receipt_pilot or args.receipt_evidence or not args.market_tokens:
            parser.error('Rejected-batch mode requires market-tokens and no other audit mode')
        result=run_rejected_source_batch(args.rejected_source_batch,args.market_tokens,args.run_dir)
        print(json.dumps({key:result.get(key) for key in ('status','native_original_log_set_exact_match',
            'unexplained_receiving_asset_excess_raw','exchange_collateral_flow','rpc_statuses')},sort_keys=True))
        if result['status']!='verified_native_observations_only':
            raise SystemExit(2)
    elif args.full_source:
        if args.receipt_pilot or args.receipt_evidence or not args.raw_events or not args.market_tokens:
            parser.error("A full-source audit requires raw-events and market-tokens, not receipt mode")
        result=run_full_source_audit(args)
        print(json.dumps({"status":result["status"],"counts":result["counts"]},sort_keys=True))
        if result["status"]=='blocked_source_reconciliation':
            raise SystemExit(2)
    elif args.receipt_pilot:
        if args.receipt_evidence:
            print(json.dumps(run_support(args.receipt_pilot,args.receipt_evidence,args.run_dir)["native_statuses"],sort_keys=True))
        else:
            result=run_receipts(args.receipt_pilot,args.run_dir,args.receipt_limit)
            print(json.dumps(result["statuses"],sort_keys=True))
            if result["merge_native_gate_status"]=='blocked_native_merge_evidence':
                raise SystemExit(2)
    else:
        if any(p is None for p in (args.raw_events,args.market_tokens,args.market_clocks,args.block_timestamps)):
            parser.error("A source pilot requires all four input paths")
        print(json.dumps(run(args)["counts"], sort_keys=True))


if __name__ == "__main__":
    main()
