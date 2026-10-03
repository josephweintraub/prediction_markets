"""Stream verified own actions into a bounded, trade-implied FIFO tag ledger.

No focal filters are applied to history. One row is written per original own
OrderFilled action, including a single corrected active aggregate. Opening
balances and nontrade movements are not reconstructed. Exact integer quantities
and rational fill allocations drive classification; displayed monetary sums are
floating descriptive diagnostics, not inputs to profit eligibility.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timezone
from fractions import Fraction
import json
import math
from pathlib import Path
import platform
import re
import shutil
import sys
from typing import Any, Iterator, Mapping

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.diagnostics.profit_taking_actions import (
    RAW_FIELDS, EXCHANGE_CONTRACTS, ExitTag, ObservedFIFO, OwnAction, apply_action, decode_own_action,
)
from analysis.sports_game_dynamics.artifacts import (
    INTEGER_TYPES, artifact_fingerprint, fingerprint, fresh_run, quoted,
    require_columns, write_json,
)


SOURCE_MARKERS = {"source_status": "verified_own_action"}
SOURCE_COLUMNS = RAW_FIELDS + (
    "execution_id", "market_id", "source_role", "source_status",
    "source_contract_version", "fee_rule", "aggregate_reconciled",
    "original_maker_amount_filled", "original_taker_amount_filled", "refund_making_micro",
)
INTEGER_COLUMNS = (
    "maker_amount_filled", "taker_amount_filled", "fee", "block_number", "log_index",
    "original_maker_amount_filled", "original_taker_amount_filled", "refund_making_micro",
)
HISTORY_STATUS = "trade_implied_only_opening_and_nontrade_movements_unknown"
SPILL_CAP_BYTES = 12 * 1024**3
TAG_SCHEMA = pa.schema([
    ("execution_id", pa.string()), ("market_id", pa.string()),
    ("token_id", pa.string()), ("wallet", pa.string()), ("side", pa.string()),
    ("source_role", pa.string()), ("block_number", pa.int64()),
    ("transaction_hash", pa.string()), ("log_index", pa.int64()),
    ("exchange_address", pa.string()), ("gross_quantity_micro", pa.int64()),
    ("gross_cash_micro", pa.int64()), ("fee_micro", pa.int64()),
    ("acquisition_cash_micro", pa.int64()), ("fee_rule", pa.string()),
    ("source_contract_version", pa.string()),
    ("net_acquired_quantity_micro", pa.int64()), ("net_sale_cash_micro", pa.int64()),
    ("pretrade_same_token_quantity_micro", pa.int64()),
    ("pretrade_complement_quantity_micro", pa.int64()),
    ("pretrade_net_favorite_quantity_micro", pa.int64()),
    ("matched_quantity_micro", pa.int64()), ("prior_matched_quantity_micro", pa.int64()),
    ("unmatched_quantity_micro", pa.int64()), ("unmatched_disposal_quantity_micro", pa.int64()),
    ("gross_profitable_disposal_quantity_micro", pa.int64()),
    ("primary_exit_quantity_micro", pa.int64()), ("hedge_profitable_quantity_micro", pa.int64()),
    ("primary_exit_fraction", pa.float64()), ("primary_exit_fraction_numerator", pa.int64()),
    ("primary_exit_fraction_denominator", pa.int64()),
    ("hedge_fraction", pa.float64()), ("hedge_fraction_numerator", pa.int64()),
    ("hedge_fraction_denominator", pa.int64()),
    ("unmatched_disposal_fraction", pa.float64()),
    ("unmatched_disposal_fraction_numerator", pa.int64()),
    ("unmatched_disposal_fraction_denominator", pa.int64()),
    ("favorite_at_exit", pa.bool_()), ("matched_acquisition_cost_usdc", pa.float64()),
    ("matched_exit_value_usdc", pa.float64()), ("matched_profit_usdc", pa.float64()),
    ("qualifying_profit_usdc", pa.float64()),
    ("minimum_matched_holding_blocks", pa.int64()),
    ("maximum_matched_holding_blocks", pa.int64()),
    ("history_status", pa.string()),
])


def _count(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def verify_source_stage(source_manifest: Path, own_actions: Path) -> dict[str, Any]:
    """Do not let an accepted subset bypass a globally blocked source stage."""
    if (source_manifest.name != "manifest.json"
            or source_manifest.resolve().parent != own_actions.resolve().parent
            or own_actions.name != "own_actions.parquet"):
        raise ValueError("Source manifest must belong to the supplied own_actions.parquet stage")
    manifest = json.loads(source_manifest.read_text())
    if manifest.get("status") != "complete":
        raise ValueError("Parent source stage is not complete; partial history cannot enter FIFO")
    counts = manifest.get("counts", {})
    for field in ("accepted_own_actions", "rejected_relevant_batches", "orphan_scoped_logs"):
        if isinstance(counts.get(field), bool) or not isinstance(counts.get(field), int) or counts[field] < 0:
            raise ValueError(f"Parent source stage lacks a valid {field} count")
    if counts["rejected_relevant_batches"] or counts["orphan_scoped_logs"]:
        raise ValueError("Parent source stage retains unreconciled relevant batches or scoped orphans")
    observed = artifact_fingerprint(own_actions)
    if manifest.get("outputs", {}).get("own_actions.parquet") != observed:
        raise ValueError("Own-action output fingerprint does not match its source-stage manifest")
    row_count = pq.ParquetFile(own_actions).metadata.num_rows
    if row_count <= 0 or row_count != counts["accepted_own_actions"]:
        raise ValueError("Own-action output count does not match its complete source stage")
    return {"counts": counts, "own_actions_fingerprint": observed}


def validate_sources(
    con: duckdb.DuckDBPyConnection, own_actions: Path, market_tokens: Path,
) -> dict[str, int]:
    """Validate source grain, exact units, markers, identity and binary maps."""
    con.execute(f"CREATE TEMP VIEW own_actions AS SELECT * FROM read_parquet('{quoted(own_actions)}')")
    observed = require_columns(con, "own_actions", SOURCE_COLUMNS, "Verified own actions")
    if any(observed[name] not in INTEGER_TYPES for name in INTEGER_COLUMNS):
        raise ValueError("Own-action amount and EVM fields must have integer types")
    if observed["aggregate_reconciled"] != "BOOLEAN":
        raise ValueError("Aggregate reconciliation marker must be BOOLEAN")
    marker_failures = " OR ".join(f"{name} IS DISTINCT FROM '{value}'" for name, value in SOURCE_MARKERS.items())
    if _count(con, f"SELECT count(*) FROM own_actions WHERE {marker_failures}"):
        raise ValueError("Unverified own-action source, contract version or fee rule")
    contract_checks = " OR ".join(
        f"(lower(exchange_address)='{address}' AND source_contract_version='{version}' AND fee_rule='{rule}')"
        for address, (version, rule) in EXCHANGE_CONTRACTS.items()
    )
    if _count(con, f"SELECT count(*) FROM own_actions WHERE NOT coalesce(({contract_checks}),false)"):
        raise ValueError("Source version/fee rule does not match its verified emitting exchange")
    if _count(con, """SELECT count(*) FROM own_actions WHERE execution_id IS NULL OR trim(execution_id)=''
        OR market_id IS NULL OR trim(market_id)='' OR aggregate_reconciled IS NULL
        OR source_role NOT IN ('passive','active_aggregate') OR source_role IS NULL
        OR execution_id IS DISTINCT FROM lower(exchange_address)||':'||lower(transaction_hash)||':'||log_index::VARCHAR
        OR (source_role='active_aggregate') IS DISTINCT FROM aggregate_reconciled"""):
        raise ValueError("Own-action identity or role/reconciliation markers are inconsistent")
    if _count(con, "SELECT count(*) FROM (SELECT execution_id FROM own_actions GROUP BY 1 HAVING count(*)<>1)"):
        raise ValueError("Duplicate original own-action execution IDs")
    if _count(con, """SELECT count(*) FROM (SELECT block_number,log_index FROM own_actions
        GROUP BY 1,2 HAVING count(*)<>1)"""):
        raise ValueError("Original block-global EVM order keys are duplicated")
    if _count(con, """SELECT count(*) FROM own_actions WHERE maker_amount_filled IS NULL
        OR taker_amount_filled IS NULL OR fee IS NULL OR block_number IS NULL OR log_index IS NULL
        OR original_maker_amount_filled IS NULL OR original_taker_amount_filled IS NULL
        OR refund_making_micro IS NULL OR maker_amount_filled<0 OR taker_amount_filled<0 OR fee<0
        OR block_number<0 OR log_index<0 OR refund_making_micro<0
        OR original_taker_amount_filled<>taker_amount_filled
        OR original_maker_amount_filled-maker_amount_filled<>refund_making_micro
        OR (source_role='passive' AND refund_making_micro<>0)"""):
        raise ValueError("Original/effective amounts or making-asset refund do not reconcile")
    con.execute(f"CREATE TEMP VIEW market_tokens AS SELECT * FROM read_parquet('{quoted(market_tokens)}')")
    require_columns(con, "market_tokens", ("token_id", "market_id", "complement_token_id"), "Accepted market tokens")
    if _count(con, """SELECT count(*) FROM market_tokens WHERE token_id IS NULL OR market_id IS NULL
        OR complement_token_id IS NULL OR trim(token_id::VARCHAR)='' OR trim(market_id::VARCHAR)=''
        OR token_id::VARCHAR=complement_token_id::VARCHAR"""):
        raise ValueError("Missing or nonbinary accepted token identity")
    if _count(con, "SELECT count(*) FROM (SELECT token_id FROM market_tokens GROUP BY 1 HAVING count(*)<>1)"):
        raise ValueError("Accepted tokens are not unique")
    if _count(con, "SELECT count(*) FROM (SELECT market_id FROM market_tokens GROUP BY 1 HAVING count(*)<>2)"):
        raise ValueError("Every accepted market needs exactly two tokens")
    if _count(con, """SELECT count(*) FROM market_tokens t LEFT JOIN market_tokens c
        ON c.token_id::VARCHAR=t.complement_token_id::VARCHAR
        WHERE c.token_id IS NULL OR c.market_id IS DISTINCT FROM t.market_id
        OR c.complement_token_id::VARCHAR IS DISTINCT FROM t.token_id::VARCHAR"""):
        raise ValueError("Complement mapping must be symmetric within one binary market")
    if _count(con, """SELECT count(*) FROM own_actions a LEFT JOIN market_tokens t
        ON t.token_id::VARCHAR=CASE WHEN a.maker_asset_id::VARCHAR='0'
            THEN a.taker_asset_id::VARCHAR ELSE a.maker_asset_id::VARCHAR END
        WHERE t.token_id IS NULL OR a.market_id IS DISTINCT FROM t.market_id::VARCHAR"""):
        raise ValueError("Own token is unmapped or assigned to the wrong accepted market")
    counts = {
        "input_actions": _count(con, "SELECT count(*) FROM own_actions"),
        "accepted_markets": _count(con, "SELECT count(DISTINCT market_id) FROM market_tokens"),
        "markets_with_actions": _count(con, "SELECT count(DISTINCT market_id) FROM own_actions"),
    }
    if not counts["input_actions"]:
        raise ValueError("No verified own actions to match")
    return counts


def decoded_source_action(row: Mapping[str, Any]) -> OwnAction:
    """Admit an aggregate only from the source builder's explicit verified marker."""
    action = decode_own_action(row, fee_rule=row["fee_rule"])
    expected_role = "active_aggregate" if action.is_taker_aggregate else "passive"
    if row["source_role"] != expected_role:
        raise ValueError("Source role contradicts original wallet/order addresses")
    if action.is_taker_aggregate:
        if action.counterparty != action.exchange_address or row["aggregate_reconciled"] is not True:
            raise ValueError("Active aggregate lacks emitting-exchange reconciliation")
        action = replace(action, aggregate_reconciled=True)
    elif row["aggregate_reconciled"] is not False:
        raise ValueError("Passive maker has an inappropriate aggregate marker")
    if action.gross_price < 0 or action.gross_price > 1:
        raise ValueError("Effective execution price is not binary")
    return action


def tag_row(
    row: Mapping[str, Any], action: OwnAction, tag: ExitTag | None,
    same_stock: int, opposite_stock: int,
) -> dict[str, Any]:
    """Collapse lot allocations to one original action, without fractional row inflation."""
    is_sale = action.side == "SELL"
    allocations = tag.allocations if tag else ()
    direct = tag.qualifying_quantity_micro if tag and is_sale else 0
    primary = tag.exposure_reducing_profitable_quantity_micro if tag and is_sale else 0
    hedge = tag.qualifying_quantity_micro if tag and not is_sale else 0
    unmatched_sale = tag.unmatched_quantity_micro if tag and is_sale else 0
    direct_fraction = Fraction(primary, action.gross_quantity_micro)
    hedge_fraction = tag.qualifying_fill_fraction if tag and not is_sale else Fraction()
    unknown_fraction = Fraction(unmatched_sale, action.gross_quantity_micro)
    # Exact per-lot signs and integer matched quantities already determine tags.
    # Monetary diagnostics sum individually converted rationals to avoid huge
    # least-common-multiple denominators across many heterogeneous acquisitions.
    aggregate = lambda field, qualifying=False: math.fsum(
        float(getattr(lot, field)) / 1_000_000 for lot in allocations if not qualifying or lot.qualifies
    )
    held_blocks = [action.block_number-lot.acquisition_block_number for lot in allocations]
    result = {
        "execution_id": row["execution_id"], "market_id": row["market_id"],
        "token_id": action.token_id, "wallet": action.wallet, "side": action.side,
        "source_role": row["source_role"], "block_number": action.block_number,
        "transaction_hash": action.transaction_hash, "log_index": action.log_index,
        "exchange_address": action.exchange_address,
        "gross_quantity_micro": action.gross_quantity_micro, "gross_cash_micro": action.gross_cash_micro,
        "fee_micro": action.fee_micro,
        "acquisition_cash_micro": None if is_sale else action.acquisition_cash_micro,
        "fee_rule": action.fee_rule, "source_contract_version": row["source_contract_version"],
        "net_acquired_quantity_micro": None if is_sale else action.acquired_quantity_micro,
        "net_sale_cash_micro": action.sale_cash_micro if is_sale else None,
        "pretrade_same_token_quantity_micro": same_stock,
        "pretrade_complement_quantity_micro": opposite_stock,
        "pretrade_net_favorite_quantity_micro": tag.pretrade_net_favorite_quantity_micro if tag else 0,
        "matched_quantity_micro": tag.matched_quantity_micro if tag else 0,
        "prior_matched_quantity_micro": sum(lot.quantity_micro for lot in allocations if lot.prior_transaction),
        "unmatched_quantity_micro": tag.unmatched_quantity_micro if tag else 0,
        "unmatched_disposal_quantity_micro": unmatched_sale,
        "gross_profitable_disposal_quantity_micro": direct,
        "primary_exit_quantity_micro": primary, "hedge_profitable_quantity_micro": hedge,
        "primary_exit_fraction": float(direct_fraction),
        "primary_exit_fraction_numerator": direct_fraction.numerator,
        "primary_exit_fraction_denominator": direct_fraction.denominator,
        "hedge_fraction": float(hedge_fraction),
        "hedge_fraction_numerator": hedge_fraction.numerator,
        "hedge_fraction_denominator": hedge_fraction.denominator,
        "unmatched_disposal_fraction": float(unknown_fraction),
        "unmatched_disposal_fraction_numerator": unknown_fraction.numerator,
        "unmatched_disposal_fraction_denominator": unknown_fraction.denominator,
        "favorite_at_exit": tag.favorite_at_exit if tag else False,
        "matched_acquisition_cost_usdc": aggregate("cost_micro"),
        "matched_exit_value_usdc": aggregate("exit_value_micro"),
        "matched_profit_usdc": aggregate("profit_micro"),
        "qualifying_profit_usdc": aggregate("profit_micro", True),
        "minimum_matched_holding_blocks": min(held_blocks) if held_blocks else None,
        "maximum_matched_holding_blocks": max(held_blocks) if held_blocks else None,
        "history_status": HISTORY_STATUS,
    }
    return result


def iter_rows(cursor: duckdb.DuckDBPyConnection, columns: tuple[str, ...], batch_size: int
              ) -> Iterator[dict[str, Any]]:
    while records := cursor.fetchmany(batch_size):
        for record in records:
            yield dict(zip(columns, record))


def build_ledger(args: argparse.Namespace) -> dict[str, Any]:
    own_actions, market_tokens = Path(args.own_actions), Path(args.market_tokens)
    source_manifest = Path(args.source_manifest)
    inputs = (own_actions, market_tokens, source_manifest)
    if args.threads < 1 or args.batch_size < 1 or not re.fullmatch(r"[1-9][0-9]*(?:MB|GB)", args.memory_limit):
        raise ValueError("Positive threads/batch size and explicit memory-limit MB/GB are required")
    if any(not path.is_file() for path in inputs):
        raise FileNotFoundError("Verified own-action, parent manifest and accepted-token inputs must exist")
    source_evidence = verify_source_stage(source_manifest, own_actions)
    output_parent = Path(args.run_dir).resolve().parent
    existing_parent = output_parent
    while not existing_parent.exists():
        existing_parent = existing_parent.parent
    disk_free = shutil.disk_usage(existing_parent).free
    output_allowance = max(own_actions.stat().st_size * 2, 2 * 1024**3)
    required_free = SPILL_CAP_BYTES + output_allowance + 2 * 1024**3
    if disk_free < required_free:
        raise ValueError(f"Insufficient disk reserve: free={disk_free}, required={required_free}; no stage started")
    with fresh_run(args.run_dir, inputs) as staging:
        con = duckdb.connect()
        try:
            con.execute(f"SET threads={args.threads}")
            con.execute(f"SET memory_limit='{args.memory_limit}'")
            con.execute("SET max_temp_directory_size='12GB'")
            if args.temp_directory:
                con.execute(f"SET temp_directory='{quoted(args.temp_directory)}'")
            counts = validate_sources(con, own_actions, market_tokens)
            tokens = con.execute("SELECT token_id::VARCHAR,market_id::VARCHAR,complement_token_id::VARCHAR FROM market_tokens").fetchall()
            token_to_market = {token: market for token, market, _ in tokens}
            complements = {token: other for token, _, other in tokens}
            columns = SOURCE_COLUMNS
            cursor = con.execute("SELECT "+",".join(columns)+" FROM own_actions ORDER BY market_id,block_number,log_index")
            current_market, book = None, ObservedFIFO()
            buffer: list[dict[str, Any]] = []
            writer = pq.ParquetWriter(staging/"action_tags.parquet", TAG_SCHEMA, compression="zstd")
            processed = 0
            try:
                for row in iter_rows(cursor, columns, args.batch_size):
                    if row["market_id"] != current_market:
                        book.validate_remaining_totals()
                        current_market, book = row["market_id"], ObservedFIFO()
                    action = decoded_source_action(row)
                    if token_to_market.get(action.token_id) != current_market:
                        raise ValueError("Streamed action changed its validated token/market identity")
                    same_stock = book.observed_remaining_micro(action.wallet, action.token_id)
                    opposite_stock = book.observed_remaining_micro(action.wallet, complements[action.token_id])
                    tag = apply_action(book, action, complements)
                    buffer.append(tag_row(row, action, tag, same_stock, opposite_stock))
                    processed += 1
                    if len(buffer) >= args.batch_size:
                        writer.write_table(pa.Table.from_pylist(buffer, schema=TAG_SCHEMA))
                        buffer = []
                        if processed % 1_000_000 == 0:
                            print(f"{datetime.now(timezone.utc).isoformat()} FIFO processed {processed} own actions", file=sys.stderr, flush=True)
                if buffer:
                    writer.write_table(pa.Table.from_pylist(buffer, schema=TAG_SCHEMA))
                book.validate_remaining_totals()
            finally:
                writer.close()
            counts["output_actions"] = processed
            if processed != counts["input_actions"]:
                raise ValueError("Output action count did not reconcile to verified own-source grain")
            con.execute(f"CREATE TEMP VIEW saved_tags AS SELECT * FROM read_parquet('{quoted(staging/'action_tags.parquet')}')")
            if _count(con, "SELECT count(*) FROM saved_tags") != processed or _count(con,
                "SELECT count(*) FROM (SELECT execution_id FROM saved_tags GROUP BY 1 HAVING count(*)<>1)"):
                raise ValueError("Saved tag ledger row count or identity uniqueness changed")
            if _count(con, """SELECT count(*) FROM saved_tags WHERE primary_exit_fraction<0 OR primary_exit_fraction>1
                OR hedge_fraction<0 OR hedge_fraction>1 OR unmatched_disposal_fraction<0 OR unmatched_disposal_fraction>1
                OR (side='BUY' AND (primary_exit_quantity_micro<>0 OR unmatched_disposal_quantity_micro<>0))
                OR (side='SELL' AND hedge_profitable_quantity_micro<>0)
                OR primary_exit_quantity_micro>gross_profitable_disposal_quantity_micro
                OR primary_exit_quantity_micro>pretrade_net_favorite_quantity_micro"""):
                raise ValueError("Saved fractions or exposure caps failed reconciliation")
            summary = con.execute("""SELECT market_id,count(*) actions,
                count(*) FILTER(WHERE side='BUY') buys,count(*) FILTER(WHERE side='SELL') sells,
                sum(unmatched_disposal_quantity_micro)::HUGEINT unmatched_disposal_quantity_micro,
                sum(gross_profitable_disposal_quantity_micro)::HUGEINT gross_profitable_disposal_quantity_micro,
                sum(primary_exit_quantity_micro)::HUGEINT primary_exit_quantity_micro,
                sum(hedge_profitable_quantity_micro)::HUGEINT hedge_profitable_quantity_micro
                FROM saved_tags GROUP BY market_id ORDER BY market_id""")
            summary_columns = tuple(column[0] for column in summary.description)
            summaries = list(iter_rows(summary, summary_columns, args.batch_size))
        finally:
            con.close()
        write_json(staging/"summary.json", {"counts": counts, "markets": summaries})
        manifest = {
            "stage": "profit_taking_trade_implied_fifo_v1", "schema_version": 1,
            "created_at_utc": datetime.now(timezone.utc).isoformat(), "command": sys.argv,
            "command_arguments": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
            "environment": {"python": platform.python_version(), "duckdb": duckdb.__version__, "pyarrow": pa.__version__},
            "code": {"script": fingerprint(Path(__file__)),
                     "primitives": fingerprint(Path(__file__).with_name("profit_taking_actions.py"))},
            "inputs": {name: fingerprint(path) for name, path in zip(("own_actions", "market_tokens", "source_manifest"), inputs)},
            "source_stage_gates": source_evidence,
            "contract": {
                "grain": "One original own OrderFilled action; corrected active aggregate VWAP remains one action.",
                "history": "All verified own BUY and SELL actions, prices and actors; no focal history filters.",
                "fifo": "Same-wallet same-token physical observed-acquisition FIFO, reset by binary market; integer microtokens and exact rational acquisition costs.",
                "favorite": "Direct sale gross effective price>.5; complementary BUY price<.5. No eventual outcome consulted.",
                "prior": "Same-transaction acquisitions update history but cannot qualify as earlier purchases.",
                "direct_primary": "Positive after-fee FIFO acquisition profit, strictly prior and current favorite; quantity capped by positive pretrade trade-implied net favorite exposure.",
                "hedge": "Pre-existing complementary stock offsets oldest favorite lots; current hedge uses the remaining FIFO suffix. Favorite lots remain physically held.",
                "fractions": "Direct primary/gross disposed SELL quantity; hedge qualified net acquired BUY quantity/total net acquired BUY quantity. One original fill unit is preserved.",
                "money": "Per-lot profit signs and eligibility are exact rationals; saved aggregate USDC diagnostics are floating sums after classification.",
                "fees": "Emitting-address dispatch only: legacy BUY fee deducts outcome tokens; V2 BUY fee is extra collateral acquisition cost. Both SELL fees deduct collateral proceeds. Gross execution calibration is unchanged.",
                "limits": HISTORY_STATUS+"; quantity allocation is not certified inventory, unique positions closed across episodes, intent, or causal price attribution.",
            },
            "counts": counts,
            "disk_preflight": {"free_bytes": disk_free, "required_free_bytes": required_free,
                               "spill_cap_bytes": SPILL_CAP_BYTES, "output_allowance_bytes": output_allowance,
                               "policy": "12GiB spill plus max(2x compressed own-actions input,2GiB) output allowance plus2GiB safety; reserve is not a space guarantee."},
            "outputs": {name: artifact_fingerprint(staging/name) for name in ("action_tags.parquet", "summary.json")},
            "completion_status": "complete",
        }
        completed_input = {**manifest["inputs"]["own_actions"], "path": own_actions.name}
        if completed_input != source_evidence["own_actions_fingerprint"]:
            raise ValueError("Immutable own-action input changed during FIFO publication")
        write_json(staging/"manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("own-actions", "market-tokens", "source-manifest", "run-dir"):
        parser.add_argument("--"+name, type=Path, required=True)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="100GB")
    parser.add_argument("--temp-directory", type=Path)
    parser.add_argument("--batch-size", type=int, default=50_000)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    # Production-only CLI guard; synthetic tests call build_ledger directly.
    if not Path("/mnt/data").is_dir():
        raise RuntimeError("Production FIFO ledger runs only on the EC2 /mnt/data host")
    print(json.dumps(build_ledger(args)["counts"], sort_keys=True))
