"""Recover complete protocol-aware own-order history for frozen sports tokens.

Two lazy raw-source passes first identify scoped transaction/exchange keys, then
retrieve every OrderFilled log for those keys. Exact log replays alone are
removed. Active aggregates retain one observation and receive effective amounts
only after exact passive-leg reconciliation. Irregular groups remain auditable
and block estimation; no maker-opposite counterparty labels are fabricated.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys
from typing import Any

import duckdb
import pyarrow.parquet as pq

from analysis.diagnostics.profit_taking_actions import EXCHANGE_CONTRACTS, RAW_FIELDS
from analysis.diagnostics.profit_taking_source_audit import parquet_metadata, validate_complements
from analysis.sports_game_dynamics.artifacts import artifact_fingerprint, fingerprint, fresh_run, quoted, write_json
from production_guard import require_production_host


def progress(value: str) -> None:
    print(datetime.now(timezone.utc).isoformat()+" "+value,file=sys.stderr,flush=True)


def count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def prepare_source(con: duckdb.DuckDBPyConnection, raw_events: Path, market_tokens: Path) -> dict[str,int]:
    """Build complete selected transactions, never merely their scoped legs."""
    validate_complements(pq.read_table(market_tokens,columns=["token_id","market_id","complement_token_id"]).to_pylist())
    con.execute(f"CREATE TEMP TABLE tokens AS SELECT token_id::VARCHAR token_id,market_id::VARCHAR market_id,"
                f"complement_token_id::VARCHAR complement_token_id FROM read_parquet('{quoted(market_tokens)}')")
    con.execute(f"CREATE TEMP VIEW raw_source AS SELECT {','.join(RAW_FIELDS)} FROM read_parquet('{quoted(raw_events)}')")
    # Pass one reads assets+identity only; pass two recovers ALL records in each
    # selected transaction/exchange, including unscoped or direct-fill prefixes.
    con.execute("""CREATE TEMP TABLE selected_keys AS SELECT DISTINCT transaction_hash,exchange_address
        FROM raw_source WHERE maker_asset_id IN (SELECT token_id FROM tokens)
                           OR taker_asset_id IN (SELECT token_id FROM tokens)""")
    counts={"selected_transaction_exchange_keys":count(con,"SELECT count(*) FROM selected_keys")}
    if not counts["selected_transaction_exchange_keys"]:
        raise ValueError("No raw transaction keys matched the frozen sports tokens")
    progress(f"selected transaction/exchange keys: {counts['selected_transaction_exchange_keys']}")
    con.execute("""CREATE TEMP TABLE selected_raw AS SELECT r.* FROM raw_source r
        JOIN selected_keys k USING(transaction_hash,exchange_address)""")
    counts["selected_raw_rows"]=count(con,"SELECT count(*) FROM selected_raw")
    con.execute("CREATE TEMP TABLE exact_logs AS SELECT DISTINCT * FROM selected_raw")
    counts["distinct_log_rows"]=count(con,"SELECT count(*) FROM exact_logs")
    counts["exact_replays_removed"]=counts["selected_raw_rows"]-counts["distinct_log_rows"]
    con.execute("DROP TABLE selected_raw")
    con.execute("DROP TABLE selected_keys")
    if count(con,"""SELECT count(*) FROM (SELECT transaction_hash,log_index,exchange_address
                     FROM exact_logs GROUP BY 1,2,3 HAVING count(*)<>1)"""):
        raise ValueError("Conflicting payloads under one original log identity")
    if count(con,"""SELECT count(*) FROM (SELECT block_number,log_index FROM exact_logs
                     GROUP BY 1,2 HAVING count(*)<>1)"""):
        raise ValueError("Conflicting block-global EVM log ordering identities")
    known=",".join("'"+a+"'" for a in sorted(EXCHANGE_CONTRACTS))
    if count(con,f"SELECT count(*) FROM exact_logs WHERE lower(exchange_address) NOT IN ({known})"):
        raise ValueError("Selected source contains an unsupported exchange")
    if count(con,"""SELECT count(*) FROM exact_logs WHERE transaction_hash IS NULL OR trim(transaction_hash)=''
          OR exchange_address IS NULL OR order_hash IS NULL OR trim(order_hash)=''
          OR maker IS NULL OR trim(maker)='' OR taker IS NULL OR trim(taker)=''
          OR log_index IS NULL OR log_index<0 OR block_number IS NULL OR block_number<0"""):
        raise ValueError("Selected source has incomplete original log identity")
    if count(con,"""SELECT count(*) FROM (SELECT transaction_hash,exchange_address FROM exact_logs
                     GROUP BY 1,2 HAVING count(DISTINCT block_number)<>1)"""):
        raise ValueError("One transaction/exchange group spans contradictory blocks")
    progress(f"exact log identities: {counts['distinct_log_rows']}; replays: {counts['exact_replays_removed']}")
    return counts


def reconcile_source(con: duckdb.DuckDBPyConnection) -> dict[str,int]:
    """Exact SQL equivalent of the small verified reserved aggregate wrapper."""
    versions="CASE "+" ".join(f"WHEN lower(exchange_address)='{address}' THEN '{version}'"
                               for address,(version,_) in EXCHANGE_CONTRACTS.items())+" END"
    fees="CASE "+" ".join(f"WHEN lower(exchange_address)='{address}' THEN '{fee}'"
                           for address,(_,fee) in EXCHANGE_CONTRACTS.items())+" END"
    con.execute(f"""CREATE TEMP TABLE events AS SELECT *,
        lower(exchange_address)||':'||lower(transaction_hash)||':'||log_index::VARCHAR execution_id,
        lower(taker)=lower(exchange_address) is_aggregate,
        CASE WHEN maker_asset_id='0' THEN 'BUY' ELSE 'SELL' END side,
        CASE WHEN maker_asset_id='0' THEN taker_asset_id ELSE maker_asset_id END token_id,
        CASE WHEN maker_asset_id='0' THEN taker_amount_filled ELSE maker_amount_filled END quantity_micro,
        CASE WHEN maker_asset_id='0' THEN maker_amount_filled ELSE taker_amount_filled END cash_micro,
        {versions} source_contract_version,{fees} fee_rule,
        coalesce(sum((lower(taker)=lower(exchange_address))::BIGINT) OVER (
            PARTITION BY transaction_hash,exchange_address ORDER BY log_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)::BIGINT batch_number,
        (maker_asset_id IS NULL OR taker_asset_id IS NULL OR
         ((maker_asset_id='0')=(taker_asset_id='0')) OR maker_amount_filled IS NULL
         OR taker_amount_filled IS NULL OR maker_amount_filled<0 OR taker_amount_filled<0
         OR fee IS NULL OR fee<0) invalid_base
        FROM exact_logs""")
    con.execute("DROP TABLE exact_logs")
    con.execute("""CREATE TEMP TABLE links_candidate AS SELECT
        m.execution_id maker_execution_id,a.execution_id active_execution_id,
        m.transaction_hash,m.exchange_address,m.batch_number,
        m.quantity_micro,m.cash_micro passive_cash_micro,
        CASE WHEN m.side<>a.side AND m.token_id=a.token_id THEN 'NORMAL'
             WHEN m.side=a.side AND m.token_id=t.complement_token_id THEN
                  CASE WHEN a.side='BUY' THEN 'MINT' ELSE 'MERGE' END END kind,
        CASE WHEN m.side<>a.side AND m.token_id=a.token_id THEN m.cash_micro
             ELSE m.quantity_micro-m.cash_micro END active_cash_micro,
        (m.invalid_base OR m.quantity_micro<=0 OR m.cash_micro<0 OR m.cash_micro>m.quantity_micro
            OR lower(m.taker)<>lower(a.maker) OR m.log_index>=a.log_index
            OR m.block_number<>a.block_number OR
            (m.fee_rule='received_asset' AND m.fee>m.taker_amount_filled)
            OR (m.side='BUY' AND m.fee_rule='received_asset' AND m.fee>=m.quantity_micro)
            OR (m.side='SELL' AND m.fee>m.cash_micro)) invalid_maker,
        (m.side='BUY' AND m.fee_rule='received_asset' AND m.fee>=m.quantity_micro) nonpositive_net_buy,
        mt.market_id passive_market_id
        FROM events m JOIN events a ON m.transaction_hash=a.transaction_hash
         AND m.exchange_address=a.exchange_address AND m.batch_number=a.batch_number
         AND NOT m.is_aggregate AND a.is_aggregate
        LEFT JOIN tokens t ON a.token_id=t.token_id LEFT JOIN tokens mt ON m.token_id=mt.token_id""")
    con.execute("""CREATE TEMP TABLE batches AS WITH summed AS (
        SELECT a.execution_id active_execution_id,a.transaction_hash,a.exchange_address,a.batch_number,
          a.block_number,a.log_index,a.side,a.token_id,t.market_id,
          a.source_contract_version,a.fee_rule,a.invalid_base,a.fee,
          a.maker_amount_filled original_making_micro,a.taker_amount_filled original_taking_micro,
          count(l.maker_execution_id)::BIGINT passive_logs,
          coalesce(sum(l.quantity_micro),0)::BIGINT effective_quantity_micro,
          coalesce(sum(l.active_cash_micro),0)::BIGINT effective_cash_micro,
          coalesce(sum(l.invalid_maker::INTEGER),0)::BIGINT invalid_makers,
          coalesce(sum(l.nonpositive_net_buy::INTEGER),0)::BIGINT nonpositive_net_acquisitions,
          coalesce(sum((l.kind IS NULL)::INTEGER) FILTER(WHERE l.maker_execution_id IS NOT NULL),0)::BIGINT invalid_matches,
          coalesce(sum((l.passive_market_id IS NOT NULL)::INTEGER),0)::BIGINT scoped_passive_logs
        FROM events a LEFT JOIN tokens t ON a.token_id=t.token_id
        LEFT JOIN links_candidate l ON a.execution_id=l.active_execution_id
        WHERE a.is_aggregate GROUP BY ALL
        ), measured AS (SELECT *,
          CASE WHEN side='BUY' THEN effective_cash_micro ELSE effective_quantity_micro END effective_making_micro,
          CASE WHEN side='BUY' THEN effective_quantity_micro ELSE effective_cash_micro END effective_taking_micro
          FROM summed)
        SELECT *,original_making_micro-effective_making_micro refund_making_micro,
          CASE WHEN market_id IS NULL AND scoped_passive_logs=0 THEN 'unscoped_batch'
               WHEN market_id IS NULL THEN 'scoped_passive_with_unscoped_active'
               WHEN passive_logs=0 THEN 'aggregate_without_maker_legs'
               WHEN nonpositive_net_acquisitions>0 THEN 'nonpositive_net_acquisition'
               WHEN invalid_base OR invalid_makers>0 THEN 'invalid_asset_amount_fee_or_wallet'
               WHEN invalid_matches>0 THEN 'contradictory_match_assets_or_directions'
               WHEN effective_quantity_micro<=0 OR effective_cash_micro<0
                    OR effective_cash_micro>effective_quantity_micro THEN 'invalid_effective_execution_price'
               WHEN original_taking_micro<>effective_taking_micro THEN 'aggregate_received_amount_mismatch'
               WHEN original_making_micro<effective_making_micro THEN 'reserved_making_below_effective_spending'
               WHEN side='BUY' AND fee_rule='received_asset' AND fee>=effective_quantity_micro THEN 'nonpositive_net_acquisition'
               WHEN side='SELL' AND fee>effective_cash_micro THEN 'sale_fee_exceeds_collateral_proceeds'
               ELSE 'accepted' END status FROM measured""")
    con.execute("""CREATE TEMP TABLE orphan_logs AS SELECT e.* FROM events e
       LEFT JOIN batches b ON e.transaction_hash=b.transaction_hash AND e.exchange_address=b.exchange_address
                          AND e.batch_number=b.batch_number
       WHERE NOT e.is_aggregate AND b.active_execution_id IS NULL""")
    counts={"aggregate_batches":count(con,"SELECT count(*) FROM batches"),
            "accepted_batches":count(con,"SELECT count(*) FROM batches WHERE status='accepted'"),
            "unscoped_batches":count(con,"SELECT count(*) FROM batches WHERE status='unscoped_batch'"),
            "rejected_relevant_batches":count(con,"SELECT count(*) FROM batches WHERE status NOT IN ('accepted','unscoped_batch')"),
            "orphan_scoped_logs":count(con,"SELECT count(*) FROM orphan_logs JOIN tokens USING(token_id)"),
            "orphan_unscoped_logs":count(con,"SELECT count(*) FROM orphan_logs ANTI JOIN tokens USING(token_id)"),
            "accepted_refund_batches":count(con,"SELECT count(*) FROM batches WHERE status='accepted' AND refund_making_micro>0")}
    con.execute("""CREATE TEMP TABLE accepted_links AS SELECT l.maker_execution_id,l.active_execution_id,
         l.kind,l.quantity_micro,l.passive_cash_micro,l.active_cash_micro FROM links_candidate l
         JOIN batches b USING(active_execution_id) WHERE b.status='accepted'""")
    con.execute("""CREATE TEMP TABLE own_actions AS
        SELECT e.order_hash,e.maker,e.taker,e.maker_asset_id,e.taker_asset_id,
          CASE WHEN e.is_aggregate THEN b.effective_making_micro ELSE e.maker_amount_filled END::BIGINT maker_amount_filled,
          CASE WHEN e.is_aggregate THEN b.effective_taking_micro ELSE e.taker_amount_filled END::BIGINT taker_amount_filled,
          e.fee,e.block_number,e.transaction_hash,e.log_index,e.exchange_address,e.execution_id,t.market_id,
          CASE WHEN e.is_aggregate THEN 'active_aggregate' ELSE 'passive' END source_role,
          'verified_own_action' source_status,e.source_contract_version,e.fee_rule,e.is_aggregate aggregate_reconciled,
          e.maker_amount_filled original_maker_amount_filled,e.taker_amount_filled original_taker_amount_filled,
          CASE WHEN e.is_aggregate THEN b.refund_making_micro ELSE 0 END::BIGINT refund_making_micro
        FROM events e JOIN tokens t USING(token_id)
        JOIN batches b ON e.transaction_hash=b.transaction_hash AND e.exchange_address=b.exchange_address
                      AND e.batch_number=b.batch_number WHERE b.status='accepted'""")
    counts["accepted_own_actions"]=count(con,"SELECT count(*) FROM own_actions")
    counts["accepted_passive_actions"]=count(con,"SELECT count(*) FROM own_actions WHERE source_role='passive'")
    counts["accepted_active_actions"]=count(con,"SELECT count(*) FROM own_actions WHERE source_role='active_aggregate'")
    counts["accepted_links"]=count(con,"SELECT count(*) FROM accepted_links")
    if counts["accepted_passive_actions"] != counts["accepted_links"] or counts["accepted_active_actions"] != counts["accepted_batches"]:
        raise ValueError("Own-action/link/batch population does not reconcile")
    if count(con,"SELECT count(*) FROM (SELECT execution_id FROM own_actions GROUP BY 1 HAVING count(*)<>1)"):
        raise ValueError("Published own-order grain duplicates an original source log")
    if count(con,"SELECT count(*) FROM tokens ANTI JOIN (SELECT DISTINCT token_id FROM events) USING(token_id)"):
        # Presence is a source coverage diagnostic, not a reason to invent data.
        counts["accepted_tokens_without_raw_history"]=count(con,"SELECT count(*) FROM tokens ANTI JOIN (SELECT DISTINCT token_id FROM events) USING(token_id)")
    else:
        counts["accepted_tokens_without_raw_history"]=0
    progress("batch reconciliation: "+json.dumps(counts,sort_keys=True))
    return counts


def require_native_pilot(pilot: Path, receipts: Path) -> None:
    source=json.loads((pilot/"summary.json").read_text())
    native=json.loads((receipts/"summary.json").read_text())
    if not source.get("counts",{}).get("accepted"):
        raise ValueError("Bounded batch pilot has no accepted evidence")
    if source["counts"].get("rejected",0) or source["counts"].get("unassigned_scoped_logs",0):
        raise ValueError("Bounded batch pilot has unresolved scoped records")
    if native.get("statuses",{}).get("verified",0) != native.get("requested_transactions",0):
        raise ValueError("Native receipt pilot is not completely verified")
    transactions={e["transaction_hash"] for e in native["evidence"] if e["status"]=='verified'}
    audited=pq.read_table(pilot/"batch_audit.parquet").to_pylist()
    versions={a["source_contract_version"] for a in audited if a["transaction_hash"] in transactions and a["status"]=='accepted'}
    if versions != {v for v,_ in EXCHANGE_CONTRACTS.values()}:
        raise ValueError("Native pilot does not verify both contract generations")


def run(args: argparse.Namespace) -> dict[str,Any]:
    if shutil.disk_usage(args.run_dir.parent.parent).free < 25*1024**3:
        raise ValueError("Full source build requires at least 25GiB available")
    metadata,_=parquet_metadata(args.raw_events)
    require_native_pilot(args.source_pilot,args.receipt_pilot)
    inputs=[args.raw_events,args.market_tokens,args.source_pilot,args.receipt_pilot]
    with fresh_run(args.run_dir,inputs) as staging:
        con=duckdb.connect()
        try:
            con.execute(f"SET threads={args.threads}")
            con.execute(f"SET memory_limit='{args.memory_limit}'")
            con.execute(f"SET temp_directory='{quoted(args.temp_directory)}'")
            con.execute("SET max_temp_directory_size='12GB'")
            counts=prepare_source(con,args.raw_events,args.market_tokens)
            counts.update(reconcile_source(con))
            outputs={"own_actions.parquet":("own_actions","market_id,block_number,log_index,exchange_address"),
                     "batch_links.parquet":("accepted_links","active_execution_id,maker_execution_id"),
                     "batch_audit.parquet":("batches","block_number,log_index,exchange_address"),
                     "orphan_logs.parquet":("orphan_logs","block_number,log_index,exchange_address")}
            # Keep every irregular group's original records rather than merely
            # dropping them from a future partial-history ledger.
            con.execute("""CREATE TEMP TABLE source_exclusions AS SELECT e.*,b.status exclusion_reason
                FROM events e JOIN batches b ON e.transaction_hash=b.transaction_hash
                 AND e.exchange_address=b.exchange_address AND e.batch_number=b.batch_number
                WHERE b.status NOT IN ('accepted','unscoped_batch')""")
            outputs["source_exclusions.parquet"]=("source_exclusions","block_number,log_index,exchange_address")
            expected={filename:count(con,f"SELECT count(*) FROM {relation}") for filename,(relation,_) in outputs.items()}
            con.execute("DROP TABLE links_candidate")
            con.execute("DROP TABLE events")
            for filename,(relation,order) in outputs.items():
                progress("publishing "+filename)
                con.execute(f"COPY (SELECT * FROM {relation} ORDER BY {order}) TO '{quoted(staging/filename)}' (FORMAT PARQUET,COMPRESSION ZSTD)")
                if pq.ParquetFile(staging/filename).metadata.num_rows != expected[filename]:
                    raise ValueError("Published source artifact fails row-count reopen")
                con.execute(f"DROP TABLE {relation}")
            status='complete' if not counts["rejected_relevant_batches"] and not counts["orphan_scoped_logs"] else 'blocked_source_reconciliation'
            result={"analysis":"protocol_aware_sports_own_action_source","status":status,"counts":counts,
                    "raw_source":metadata,"grains":{"own_actions":"one original own OrderFilled log",
                    "batch_links":"one passive leg; active aggregate not duplicated"},
                    "limits":"Observed exchange actions, not complete holdings or causal counterfactuals."}
            write_json(staging/"summary.json",result)
            progress("fingerprinting full raw source after scoped extraction")
            write_json(staging/"manifest.json",{"status":status,"created_utc":datetime.now(timezone.utc).isoformat(),
                "code":{"commit":subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip(),
                        "dirty_status":subprocess.check_output(["git","status","--porcelain"],text=True)},
                "environment":{"python":sys.version,"platform":platform.platform(),"duckdb":duckdb.__version__},
                "inputs":{"raw_events":fingerprint(args.raw_events),"market_tokens":fingerprint(args.market_tokens),
                          "source_pilot_manifest":fingerprint(args.source_pilot/"manifest.json"),
                          "receipt_manifest":fingerprint(args.receipt_pilot/"manifest.json")},
                "command_arguments":{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
                "counts":counts,"outputs":{p.name:artifact_fingerprint(p) for p in sorted(staging.iterdir())}})
        finally:
            con.close()
    return result


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ("raw-events","market-tokens","source-pilot","receipt-pilot","run-dir","temp-directory"):
        parser.add_argument("--"+name,type=Path,required=True)
    parser.add_argument("--threads",type=int,default=16)
    parser.add_argument("--memory-limit",default="100GB")
    args=parser.parse_args()
    require_production_host()
    if Path("/home/ubuntu/prediction_markets") not in Path(__file__).resolve().parents:
        raise ValueError("Production source builds run only from the canonical EC2 checkout")
    if any(Path("/mnt/data") not in p.resolve().parents for p in (
            args.raw_events,args.market_tokens,args.source_pilot,args.receipt_pilot,args.run_dir,args.temp_directory)):
        raise ValueError("Production paths must remain under /mnt/data")
    result=run(args)
    print(json.dumps(result["counts"],sort_keys=True))
    if result["status"]!='complete':
        raise SystemExit(2)


if __name__=='__main__':
    main()
