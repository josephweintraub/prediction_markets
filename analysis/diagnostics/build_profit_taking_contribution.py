"""Publish two-grain descriptive sports calibration contribution summaries.

Run only after complete source recovery and complete all-history FIFO tagging.
This serial stage retains qualified own-action labels for both actual matched
BUY legs and original BUY order-events. It estimates fixed-denominator additive
accounting, not counterfactual causal price effects. No wallet-level output is
published by this stage, and no FIFO lot link becomes a calibration observation.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import re
import shutil
import sys
from typing import Any

import duckdb
import pyarrow.parquet as pq

from analysis.diagnostics.attribute_profit_taking_buys import (
    create_buy_attribution, enrich_buy_attribution,
)
from analysis.diagnostics.build_profit_taking_ledger import verify_native_readiness, verify_source_stage
from analysis.diagnostics.profit_taking_contribution import (
    SPORTS, SAMPLES, WINDOWS, WEIGHTS, SUPPORT_FLOOR, create_sport_contribution_summaries,
)
from analysis.sports_game_dynamics.artifacts import (
    INTEGER_TYPES, artifact_fingerprint, fingerprint, fresh_run, quoted, require_columns, write_json,
)
from production_guard import require_production_host


GRAINS = ("own_order_event", "matched_execution")
SPILL_CAP_BYTES = 12 * 1024**3
INPUT_NAMES = (
    "own_actions", "action_tags", "batch_links", "source_manifest", "ledger_manifest",
    "market_tokens", "market_clocks", "block_timestamps", "wallet_flags",
)
ESTIMATION_COLUMNS = (
    "execution_id", "own_execution_id", "market_id", "token_id", "side", "is_synthetic",
    "price", "residual", "usdc", "exit_fraction", "hedge_fraction", "unknown_history_fraction",
    "gross_quantity_micro", "gross_cash_micro", "sport", "realized_time", "seconds_to_end", "is_nonhuman",
    "crossed_price_exit_quantity_micro", "crossed_bin_exit_quantity_micro", "crossed_price_hedge_quantity_micro",
)


def verify_parent_stages(paths: dict[str, Path], *, require_native: bool=False) -> dict[str, Any]:
    """Require complete parent stages and exact supplied-file lineage."""
    source_evidence = verify_source_stage(paths["source_manifest"], paths["own_actions"])
    source = json.loads(paths["source_manifest"].read_text())
    if (paths["batch_links"].name != "batch_links.parquet"
            or paths["batch_links"].resolve().parent != paths["source_manifest"].resolve().parent
            or source.get("outputs", {}).get("batch_links.parquet") != artifact_fingerprint(paths["batch_links"])):
        raise ValueError("Batch links must match the complete source-stage output")
    if source.get("inputs", {}).get("market_tokens") != fingerprint(paths["market_tokens"]):
        raise ValueError("Accepted token spine differs from the complete source-stage input")
    if (paths["ledger_manifest"].name != "manifest.json"
            or paths["action_tags"].name != "action_tags.parquet"
            or paths["action_tags"].resolve().parent != paths["ledger_manifest"].resolve().parent):
        raise ValueError("Ledger manifest must belong to the supplied action_tags.parquet stage")
    ledger = json.loads(paths["ledger_manifest"].read_text())
    if (ledger.get("completion_status") != "complete"
            or ledger.get("stage") != "profit_taking_trade_implied_fifo_v1"):
        raise ValueError("Parent all-history FIFO stage is not complete")
    for name in ("own_actions", "market_tokens", "source_manifest"):
        if ledger.get("inputs", {}).get(name) != fingerprint(paths[name]):
            raise ValueError(f"Ledger input lineage mismatch: {name}")
    if ledger.get("outputs", {}).get("action_tags.parquet") != artifact_fingerprint(paths["action_tags"]):
        raise ValueError("FIFO tags do not match the complete ledger-stage output")
    tags_count = pq.ParquetFile(paths["action_tags"]).metadata.num_rows
    expected = source_evidence["counts"]["accepted_own_actions"]
    counts = ledger.get("counts", {})
    if (tags_count != expected or tags_count <= 0
            or any(isinstance(counts.get(name), bool) or not isinstance(counts.get(name), int) or counts.get(name) != expected
                   for name in ("input_actions", "output_actions"))):
        raise ValueError("FIFO tag count does not reconcile to complete own-action history")
    native = ledger.get("native_source_gates")
    if native is not None:
        audit_path = ledger.get("inputs", {}).get("source_audit_manifest", {}).get("path")
        receipt_path = ledger.get("inputs", {}).get("merge_receipt_manifest", {}).get("path")
        if not isinstance(audit_path, str):
            raise ValueError("Ledger native proof lacks its saved source-audit manifest path")
        verified_native = verify_native_readiness(paths["source_manifest"], Path(audit_path),
                                                  Path(receipt_path) if receipt_path else None)
        if verified_native != native:
            raise ValueError("Ledger native readiness proof differs from its saved evidence")
    elif require_native:
        raise ValueError("Production contribution CLI requires the ledger's verified native source proof")
    return {"source": source_evidence, "ledger_action_count": tags_count,
            "ledger_tag_fingerprint": artifact_fingerprint(paths["action_tags"]),
            "native_source_gates": native}


def open_inputs(con: duckdb.DuckDBPyConnection, paths: dict[str, Path]) -> None:
    """Inspect metadata schema before expensive attribution or materialization."""
    for name in INPUT_NAMES:
        if name.endswith("manifest"):
            continue
        con.execute(f"CREATE TEMP VIEW {name} AS SELECT * FROM read_parquet('{quoted(paths[name])}')")
    for name, columns in (
        ("market_tokens", ("token_id", "market_id", "complement_token_id", "won")),
        ("market_clocks", ("market_id", "sport", "event_id", "market_date", "actual_start_utc", "actual_end_utc")),
        ("block_timestamps", ("block_number", "timestamp")),
        ("wallet_flags", ("proxyWallet", "is_nonhuman")),
    ):
        observed = require_columns(con, name, columns, "Accepted contribution metadata")
        if name == "block_timestamps" and any(observed[field] not in INTEGER_TYPES for field in columns):
            raise ValueError("Exact block number/timestamp fields must be integer")
        if name == "wallet_flags" and observed["is_nonhuman"] != "BOOLEAN":
            raise ValueError("Nonhuman wallet labels must be BOOLEAN")
        if name == "market_tokens" and observed["won"] != "BOOLEAN":
            raise ValueError("Canonical resolved binary outcomes must be BOOLEAN")
        if name == "market_clocks":
            if observed["market_date"] != "DATE" or any(not observed[field].startswith("TIMESTAMP")
                    for field in ("actual_start_utc", "actual_end_utc")):
                raise ValueError("Accepted market clocks require DATE and UTC timestamp fields")
            if con.execute("""SELECT count(*) FROM market_clocks WHERE market_id IS NULL
                OR sport IS NULL OR event_id IS NULL OR market_date IS NULL
                OR actual_start_utc IS NULL OR actual_end_utc IS NULL OR actual_end_utc<=actual_start_utc
                OR sport NOT IN ("""+",".join(repr(sport) for sport in SPORTS)+")").fetchone()[0]:
                raise ValueError("Accepted clocks must use the frozen sport roster and positive durations")
    for relation, key in (("market_tokens", "token_id"), ("market_clocks", "market_id")):
        if con.execute(f"SELECT count(*) FROM (SELECT {key} FROM {relation} GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]:
            raise ValueError("Accepted metadata keys are not unique")
    if con.execute("""SELECT count(*) FROM market_tokens t LEFT JOIN market_tokens c
        ON c.token_id=t.complement_token_id WHERE t.token_id IS NULL OR t.market_id IS NULL OR t.won IS NULL
        OR c.token_id IS NULL OR c.won IS NULL OR t.token_id=c.token_id
        OR c.market_id IS DISTINCT FROM t.market_id OR c.complement_token_id IS DISTINCT FROM t.token_id
        OR t.won::INTEGER+c.won::INTEGER<>1""").fetchone()[0] or con.execute("""SELECT count(*) FROM
        (SELECT market_id FROM market_tokens GROUP BY 1 HAVING count(*)<>2 OR sum(won::INTEGER)<>1)""").fetchone()[0]:
        raise ValueError("Accepted binary outcomes and token complements do not reconcile")
    if con.execute("""SELECT count(*) FROM wallet_flags WHERE proxyWallet IS NULL
        OR trim(proxyWallet)='' OR is_nonhuman IS NULL""").fetchone()[0] or con.execute("""SELECT count(*) FROM
        (SELECT lower(proxyWallet) FROM wallet_flags GROUP BY 1 HAVING count(DISTINCT is_nonhuman)<>1)""").fetchone()[0]:
        raise ValueError("Accepted wallet flags are missing or conflicting")


def _publish(con: duckdb.DuckDBPyConnection, relation: str, path: Path, order: str) -> int:
    count = int(con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0])
    columns = con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    projection = []
    for name, kind, *_ in columns:
        column = '"'+name.replace('"', '""')+'"'
        # DuckDB otherwise serializes HUGEINT as Parquet DOUBLE, which can
        # silently lose integer micro-units above2^53. Decimal128 retains them.
        projection.append(f"CAST({column} AS DECIMAL(38,0)) AS {column}" if kind == "HUGEINT" else column)
    con.execute(f"COPY (SELECT {','.join(projection)} FROM {relation} ORDER BY {order}) TO '{quoted(path)}' (FORMAT PARQUET,COMPRESSION ZSTD)")
    saved = pq.ParquetFile(path)
    if saved.metadata.num_rows != count:
        raise ValueError("Published contribution row count changed")
    for name, kind, *_ in columns:
        if kind == "HUGEINT" and str(saved.schema_arrow.field(name).type) != "decimal128(38, 0)":
            raise ValueError("Published exact integer aggregate lost its decimal storage type")
    return count


def create_mechanism_summary(con: duckdb.DuckDBPyConnection, actions: str) -> str:
    """Keep own-action disposal evidence that has no focal actual BUY.

    No price or actor filter is applied. MERGE quantities count each actual
    disposed token, not a synthetic complement buyer. Uniform action-level
    fraction allocation to MERGE legs is descriptive, not per-leg certification.
    """
    require_columns(con, "action_tags", (
        "matched_quantity_micro", "prior_matched_quantity_micro", "gross_profitable_disposal_quantity_micro",
        "primary_exit_quantity_micro", "hedge_profitable_quantity_micro", "unmatched_disposal_quantity_micro",
    ), "Complete mechanism ledger diagnostics")
    if con.execute(f"SELECT count(*) FROM {actions} ANTI JOIN market_clocks USING(market_id)").fetchone()[0] \
            or con.execute(f"SELECT count(*) FROM {actions} ANTI JOIN block_timestamps USING(block_number)").fetchone()[0]:
        raise ValueError("All-history action mechanism diagnostics lack accepted clocks or exact timestamps")
    if con.execute(f"""SELECT count(*) FROM (SELECT x.block_number FROM block_timestamps x
        JOIN (SELECT DISTINCT block_number FROM {actions}) a USING(block_number)
        GROUP BY x.block_number HAVING count(*)<>1 OR count(timestamp)<>1 OR min(timestamp)<0)""").fetchone()[0]:
        raise ValueError("All-history exact timestamps are missing, duplicated or invalid")
    windows_sql=",".join(f"('{window}')" for window in WINDOWS)
    sports_sql=",".join(f"('{sport}')" for sport in SPORTS)
    con.execute(f"""CREATE TEMP TABLE mechanism_summary AS WITH merge_sellers AS (
        SELECT maker_execution_id execution_id,quantity_micro FROM batch_links WHERE kind='MERGE'
        UNION ALL SELECT active_execution_id execution_id,quantity_micro FROM batch_links WHERE kind='MERGE'),
        merge_totals AS (SELECT execution_id,sum(quantity_micro)::HUGEINT merge_quantity_micro
            FROM merge_sellers GROUP BY execution_id),
        timed AS (SELECT a.*,m.sport,t.matched_quantity_micro,t.prior_matched_quantity_micro,
            t.gross_profitable_disposal_quantity_micro,
            coalesce(g.merge_quantity_micro,0) merge_quantity_micro,
            ((x.timestamp-epoch(m.actual_start_utc))/(epoch(m.actual_end_utc)-epoch(m.actual_start_utc)))::DOUBLE realized_time,
            (epoch(m.actual_end_utc)-x.timestamp)::DOUBLE seconds_to_end
            FROM {actions} a JOIN action_tags t ON t.execution_id=a.execution_id
            JOIN market_clocks m ON m.market_id=a.market_id
            JOIN block_timestamps x ON x.block_number=a.block_number LEFT JOIN merge_totals g ON g.execution_id=a.execution_id),
        focal AS (SELECT a.*,w.window,r.role FROM timed a CROSS JOIN (VALUES {windows_sql}) w("window")
            CROSS JOIN (VALUES ('all'),('passive'),('active_aggregate')) r(role)
            WHERE (r.role='all' OR r.role=a.source_role) AND CASE w.window
                WHEN 'pregame' THEN realized_time<0 WHEN 'live' THEN realized_time BETWEEN 0 AND 1
                WHEN 'live_first_third' THEN realized_time>=0 AND realized_time<1.0/3
                WHEN 'live_middle_third' THEN realized_time>=1.0/3 AND realized_time<2.0/3
                WHEN 'live_final_third' THEN realized_time>=2.0/3 AND realized_time<=1
                WHEN 'live_80_90' THEN realized_time>=.80 AND realized_time<.90
                WHEN 'live_90_95' THEN realized_time>=.90 AND realized_time<.95
                WHEN 'live_95_99' THEN realized_time>=.95 AND realized_time<.99
                WHEN 'live_99_100' THEN realized_time>=.99 AND realized_time<=1
                ELSE realized_time BETWEEN 0 AND 1 AND seconds_to_end BETWEEN 0 AND 120 END),
        moments AS (SELECT sport,"window",role,count(*)::BIGINT own_actions,
            count(*) FILTER(WHERE side='BUY')::BIGINT own_buys,count(*) FILTER(WHERE side='SELL')::BIGINT own_sells,
            count(*) FILTER(WHERE side='SELL' AND gross_cash_micro/gross_quantity_micro::DOUBLE>.5)::BIGINT favorite_sell_events,
            count(*) FILTER(WHERE gross_profitable_disposal_quantity_micro>0)::BIGINT profitable_disposal_events,
            count(*) FILTER(WHERE primary_exit_quantity_micro>0)::BIGINT primary_exit_events,
            count(*) FILTER(WHERE hedge_profitable_quantity_micro>0)::BIGINT profitable_hedge_events,
            count(*) FILTER(WHERE unmatched_disposal_quantity_micro>0)::BIGINT unmatched_sale_events,
            count(*) FILTER(WHERE merge_quantity_micro>0)::BIGINT merge_disposal_events,
            sum(CASE WHEN side='SELL' THEN gross_quantity_micro ELSE 0 END)::HUGEINT sell_gross_quantity_micro,
            sum(CASE WHEN side='SELL' THEN matched_quantity_micro ELSE 0 END)::HUGEINT matched_sale_quantity_micro,
            sum(CASE WHEN side='SELL' THEN prior_matched_quantity_micro ELSE 0 END)::HUGEINT prior_matched_sale_quantity_micro,
            sum(unmatched_disposal_quantity_micro)::HUGEINT unmatched_sale_quantity_micro,
            sum(CASE WHEN side='SELL' AND gross_cash_micro/gross_quantity_micro::DOUBLE>.5
                THEN unmatched_disposal_quantity_micro ELSE 0 END)::HUGEINT unmatched_favorite_sale_quantity_micro,
            sum(gross_profitable_disposal_quantity_micro)::HUGEINT gross_profitable_disposal_quantity_micro,
            sum(primary_exit_quantity_micro)::HUGEINT primary_exit_quantity_micro,
            sum(hedge_profitable_quantity_micro)::HUGEINT profitable_hedge_net_quantity_micro,
            sum(merge_quantity_micro)::HUGEINT merge_disposal_gross_quantity_micro,
            sum(merge_quantity_micro*primary_exit_fraction)::DOUBLE allocated_primary_merge_quantity_micro,
            sum(merge_quantity_micro*unmatched_disposal_fraction)::DOUBLE allocated_unmatched_merge_quantity_micro
            FROM focal GROUP BY ALL),
        grid AS (SELECT s.sport,w.window,r.role FROM (VALUES {sports_sql}) s(sport)
            CROSS JOIN (VALUES {windows_sql}) w("window")
            CROSS JOIN (VALUES ('all'),('passive'),('active_aggregate')) r(role))
        SELECT g.*,coalesce(m.own_actions,0)::BIGINT own_actions,coalesce(m.own_buys,0)::BIGINT own_buys,
            coalesce(m.own_sells,0)::BIGINT own_sells,coalesce(m.favorite_sell_events,0)::BIGINT favorite_sell_events,
            coalesce(m.profitable_disposal_events,0)::BIGINT profitable_disposal_events,
            coalesce(m.primary_exit_events,0)::BIGINT primary_exit_events,
            coalesce(m.profitable_hedge_events,0)::BIGINT profitable_hedge_events,
            coalesce(m.unmatched_sale_events,0)::BIGINT unmatched_sale_events,
            coalesce(m.merge_disposal_events,0)::BIGINT merge_disposal_events,
            coalesce(m.sell_gross_quantity_micro,0)::HUGEINT sell_gross_quantity_micro,
            coalesce(m.matched_sale_quantity_micro,0)::HUGEINT matched_sale_quantity_micro,
            coalesce(m.prior_matched_sale_quantity_micro,0)::HUGEINT prior_matched_sale_quantity_micro,
            coalesce(m.unmatched_sale_quantity_micro,0)::HUGEINT unmatched_sale_quantity_micro,
            coalesce(m.unmatched_favorite_sale_quantity_micro,0)::HUGEINT unmatched_favorite_sale_quantity_micro,
            coalesce(m.gross_profitable_disposal_quantity_micro,0)::HUGEINT gross_profitable_disposal_quantity_micro,
            coalesce(m.primary_exit_quantity_micro,0)::HUGEINT primary_exit_quantity_micro,
            coalesce(m.profitable_hedge_net_quantity_micro,0)::HUGEINT profitable_hedge_net_quantity_micro,
            coalesce(m.merge_disposal_gross_quantity_micro,0)::HUGEINT merge_disposal_gross_quantity_micro,
            coalesce(m.allocated_primary_merge_quantity_micro,0)::DOUBLE allocated_primary_merge_quantity_micro,
            coalesce(m.allocated_unmatched_merge_quantity_micro,0)::DOUBLE allocated_unmatched_merge_quantity_micro,
            'all_own_actions_no_price_or_actor_filter'::VARCHAR population,
            'trade_implied_only_opening_and_nontrade_movements_unknown'::VARCHAR history_status
        FROM grid g LEFT JOIN moments m USING(sport,"window",role)""")
    return "mechanism_summary"


def build_contribution(args: argparse.Namespace) -> dict[str, Any]:
    """Testable small-fixture API; the CLI separately requires the canonical host."""
    if args.threads < 1 or not re.fullmatch(r"[1-9][0-9]*(?:MB|GB)", args.memory_limit):
        raise ValueError("Positive threads and an explicit MB/GB memory limit are required")
    paths = {name: Path(getattr(args, name)).resolve() for name in INPUT_NAMES}
    if any(not path.is_file() for path in paths.values()):
        raise FileNotFoundError("All verified stages and accepted metadata inputs must exist")
    parent_evidence = verify_parent_stages(paths)
    initial_inputs = {name: fingerprint(path) for name, path in paths.items()}
    if ({**initial_inputs["own_actions"], "path": paths["own_actions"].name} != parent_evidence["source"]["own_actions_fingerprint"]
            or {**initial_inputs["action_tags"], "path": paths["action_tags"].name} != parent_evidence["ledger_tag_fingerprint"]):
        raise ValueError("Verified parent-stage input changed before estimation started")
    existing_parent = Path(args.run_dir).resolve().parent
    while not existing_parent.exists():
        existing_parent = existing_parent.parent
    free_bytes = shutil.disk_usage(existing_parent).free
    required_free = SPILL_CAP_BYTES + 4 * 1024**3
    if free_bytes < required_free:
        raise ValueError(f"Insufficient disk reserve: free={free_bytes}, required={required_free}; no estimation started")
    with fresh_run(args.run_dir, paths.values()) as staging:
        con = duckdb.connect()
        counts: dict[str, Any] = {}
        try:
            con.execute(f"SET threads={args.threads}")
            con.execute(f"SET memory_limit='{args.memory_limit}'")
            con.execute("SET max_temp_directory_size='12GB'")
            con.execute("SET TimeZone='UTC'")
            if args.temp_directory:
                con.execute(f"SET temp_directory='{quoted(args.temp_directory)}'")
            open_inputs(con, paths)
            names = create_buy_attribution(con, "own_actions", "action_tags", "batch_links", prefix="attribution")
            mechanisms=create_mechanism_summary(con,names["actions"])
            counts["mechanism_summary"]=_publish(con,mechanisms,staging/"mechanism_summary.parquet",'sport,"window",role')
            con.execute("DROP TABLE "+mechanisms)
            for output in ("profiles", "tails", "late_delta", "support", "grain_totals", "crossing_diagnostics"):
                # Empty tables are made from the first grain's compact summary,
                # never from the complete source history.
                counts[output] = 0
            for grain, relation in zip(GRAINS, (names["buy_tags"], names["matched_buy_tags"])):
                print(f"{datetime.now(timezone.utc).isoformat()} summarizing {grain}", file=sys.stderr, flush=True)
                enriched = enrich_buy_attribution(con, relation, "market_tokens", "market_clocks",
                    "block_timestamps", "wallet_flags", output="enriched_"+grain)
                con.execute("CREATE TEMP TABLE current_executions AS SELECT "+",".join(ESTIMATION_COLUMNS)+f" FROM {enriched}")
                counts[grain] = int(con.execute("SELECT count(*) FROM current_executions").fetchone()[0])
                summary = create_sport_contribution_summaries(con, "current_executions", prefix="estimate")
                # Rows are small enough to retain both grains while dropping
                # the full, temporary execution table before the second grain.
                for output, key in (("profiles", "profile"), ("tails", "tails"), ("late_delta", "late_delta")):
                    relation_name = summary[key]
                    if grain == GRAINS[0]:
                        con.execute(f"CREATE TEMP TABLE all_{output} AS SELECT '{grain}'::VARCHAR grain,* FROM {relation_name}")
                    else:
                        con.execute(f"INSERT INTO all_{output} SELECT '{grain}',* FROM {relation_name}")
                totals_query = f"""SELECT '{grain}'::VARCHAR grain,sport,count(*)::BIGINT n_executions,
                    count(DISTINCT own_execution_id)::BIGINT n_own_buy_events,
                    sum(gross_quantity_micro)::HUGEINT gross_quantity_micro,
                    sum(gross_cash_micro)::HUGEINT gross_cash_micro,
                    sum(gross_quantity_micro*residual)::DOUBLE quantity_residual_numerator,
                    sum(gross_quantity_micro*residual)/sum(gross_quantity_micro)::DOUBLE quantity_weighted_calibration
                    FROM current_executions GROUP BY sport"""
                crossings_query = f"""SELECT '{grain}'::VARCHAR grain,sport,
                    count(*) FILTER(WHERE crossed_price_exit_quantity_micro>0)::BIGINT exit_crossed_price_executions,
                    sum(crossed_price_exit_quantity_micro)::DOUBLE exit_crossed_price_quantity_micro,
                    count(*) FILTER(WHERE crossed_bin_exit_quantity_micro>0)::BIGINT exit_crossed_bin_executions,
                    sum(crossed_bin_exit_quantity_micro)::DOUBLE exit_crossed_bin_quantity_micro,
                    count(*) FILTER(WHERE crossed_price_hedge_quantity_micro>0)::BIGINT hedge_crossed_price_executions,
                    sum(crossed_price_hedge_quantity_micro)::DOUBLE hedge_crossed_price_quantity_micro
                    FROM current_executions GROUP BY sport"""
                for output, query in (("grain_totals", totals_query), ("crossing_diagnostics", crossings_query)):
                    if grain == GRAINS[0]:
                        con.execute(f"CREATE TEMP TABLE all_{output} AS {query}")
                    else:
                        con.execute(f"INSERT INTO all_{output} {query}")
                con.execute(f"DROP VIEW {summary['weighted']}")
                con.execute(f"DROP VIEW {summary['focal']}")
                for key in ("profile", "tails", "late_delta"):
                    con.execute(f"DROP TABLE {summary[key]}")
                con.execute("DROP TABLE current_executions")
                con.execute(f"DROP VIEW {enriched}")
            if con.execute("""SELECT count(*) FROM
                (SELECT * FROM all_grain_totals WHERE grain='own_order_event') a FULL JOIN
                (SELECT * FROM all_grain_totals WHERE grain='matched_execution') b USING(sport)
                WHERE a.gross_quantity_micro IS DISTINCT FROM b.gross_quantity_micro
                    OR a.gross_cash_micro IS DISTINCT FROM b.gross_cash_micro
                    OR a.n_own_buy_events IS DISTINCT FROM b.n_own_buy_events
                    OR abs(a.quantity_residual_numerator-b.quantity_residual_numerator)>
                        1e-10*greatest(a.gross_quantity_micro,1)""").fetchone()[0]:
                raise ValueError("Cross-grain gross cash, quantity or quantity-weighted calibration failed conservation")
            con.execute("""CREATE TEMP TABLE all_support AS SELECT grain,sport,sample,"window",
                sum(n_executions)::BIGINT n_executions,sum(gross_quantity_micro)::HUGEINT gross_quantity_micro,
                sum(dollars)::DOUBLE dollars,sum(exit_contributing_executions)::BIGINT exit_contributing_executions,
                sum(hedge_contributing_executions)::BIGINT hedge_contributing_executions,
                sum(unknown_history_executions)::BIGINT unknown_history_executions,
                sum(allocated_exit_gross_quantity_micro)::DOUBLE allocated_exit_gross_quantity_micro,
                sum(allocated_hedge_gross_quantity_micro)::DOUBLE allocated_hedge_gross_quantity_micro,
                count(*) FILTER(WHERE NOT suppressed)::BIGINT supported_bins
                FROM all_profiles WHERE weighting='fill' GROUP BY ALL""")
            for output, order in (
                ("profiles", 'grain,sport,sample,"window",weighting,price_bin'),
                ("tails", 'grain,sport,sample,"window",weighting'),
                ("late_delta", "grain,sport,sample,weighting"),
                ("support", 'grain,sport,sample,"window"'),
                ("grain_totals", "grain,sport"), ("crossing_diagnostics", "grain,sport"),
            ):
                counts[output] = _publish(con, "all_"+output, staging/(output+".parquet"), order)
            for output, key in (("own_attribution_diagnostics", "attribution_diagnostics"),
                                ("matched_attribution_diagnostics", "matched_attribution_diagnostics")):
                counts[output] = _publish(con, names[key], staging/(output+".parquet"), "measure")
        finally:
            con.close()
        final_inputs = {name: fingerprint(path) for name, path in paths.items()}
        if final_inputs != initial_inputs:
            raise ValueError("An immutable source, ledger or metadata input changed during estimation")
        native = parent_evidence["native_source_gates"]
        if native is not None and verify_native_readiness(paths["source_manifest"],
                Path(native["source_audit_manifest"]["path"]),
                Path(native["merge_receipt_manifest"]["path"]) if "merge_receipt_manifest" in native else None) != native:
            raise ValueError("Saved native source evidence changed during estimation")
        summary_json = {"analysis": "two_grain_profit_taking_calibration_contributions", "counts": counts,
                        "support_floor": SUPPORT_FLOOR, "uncertainty_status": "not_estimated_descriptive"}
        write_json(staging/"summary.json", summary_json)
        manifest = {
            "stage": "sports_profit_taking_two_grain_contributions_v1", "schema_version": 1,
            "completion_status": "complete", "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "command": sys.argv,
            "command_arguments": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
            "environment": {"python": platform.python_version(), "duckdb": duckdb.__version__},
            "inputs": initial_inputs, "parent_stage_gates": parent_evidence,
            "code": {name: fingerprint(Path(__file__).with_name(name)) for name in (
                "build_profit_taking_contribution.py", "attribute_profit_taking_buys.py", "profit_taking_contribution.py",
                "build_profit_taking_ledger.py", "profit_taking_actions.py")},
            "contract": {
                "grains": list(GRAINS), "matched_execution": "NORMAL one actual BUY, MINT two actual complementary BUYs, MERGE no BUY; unique passive-link plus buyer-role identity. No FIFO lot pseudo-fills.",
                "own_order_event": "One original own OrderFilled BUY log, including one corrected active aggregate at gross cash/quantity VWAP; not an order hash across transactions.",
                "labels": "Identical all-history qualified own-action FIFO/profit labels, uniformly allocated to real legs; focal observation price>.5 direct gate and price<.5 hedge gate. Not independent per-leg profitability certification or causal attribution.",
                "fees": "Ledger emitting-address fee dispatch is preserved. Hedge net/net fraction prorates the complete fill; direct seller gross/gross fraction maps via actual matched gross BUY quantity. Gross cash/quantity defines calibration price.",
                "sports": list(SPORTS), "samples": list(SAMPLES), "windows": list(WINDOWS), "weights": list(WEIGHTS),
                "filters": "filtered: .01<P<.99 and unflagged; interior_all_actors: .01<P<.99; all_trades: 0<P<1. History is never filtered.",
                "time": "Exact block timestamp, T=(timestamp-start)/(end-start); all T<0 pregame. Literal live[0,1], thirds left-closed/right-open except final includes1; terminal[.80,.90),[.90,.95),[.95,.99),[.99,1]. Final120s requires live and0<=seconds_to_end<=120.",
                "denominator": "Each component uses unchanged original sport/sample/window/bin weight denominator. Equal-market normalizes original gross dollars within market/bin, not mechanism subgroups.",
                "late_delta": "Final[.99,1] minus preceding[.95,.99) D10-D1 spread and additive components; suppress unless all four tail bins meet500 observation and positive-weight support.",
                "support": "500 focal actual observations at the respective grain, not lot links or quantity count. Raw suppressed estimates saved, display estimates NULL.",
                "quantity_check": "Full unfiltered gross BUY exposure and quantity-weighted calibration conserve across grains. Quantity-weighting is a reconciliation check, not a fourth reported weighting. Filters/bins/count/dollar weights may change with grain.",
                "uncertainty": "Not estimated; all estimates descriptive. No nominal precision or statistical significance claims.",
                "history_status": "Trade-implied opening and nontrade movements unknown; unmatched favorite counterparty acquisition share is separate from global inventory uncertainty.",
                "mechanism_summary": "All own actions, no actor or boundary-price filter; clock windows only. Report passive/active and combined original-action events, matched/unmatched physical disposal quantities, gross/primary profits, net hedge quantity and MERGE disposal/allocation separately. MERGE counts disposed token legs, never synthetic BUY observations.",
            },
            "counts": counts,
            "disk_preflight": {"free_bytes": free_bytes, "required_free_bytes": required_free,
                               "spill_cap_bytes": SPILL_CAP_BYTES, "policy": "12GiB spill plus2GiB compact-output allowance plus2GiB safety."},
            "outputs": {path.name: artifact_fingerprint(path) for path in sorted(staging.iterdir())},
        }
        write_json(staging/"manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in INPUT_NAMES+ ("run_dir",):
        parser.add_argument("--"+name.replace("_", "-"), type=Path, required=True)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="100GB")
    parser.add_argument("--temp-directory", type=Path)
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    require_production_host()
    if not Path(__file__).resolve().is_relative_to(Path("/home/ubuntu/prediction_markets")):
        raise RuntimeError("Contribution CLI must use the canonical source file")
    verify_parent_stages({name:Path(getattr(args,name)).resolve() for name in INPUT_NAMES},require_native=True)
    print(json.dumps(build_contribution(args)["counts"], sort_keys=True))


if __name__ == "__main__":
    main()
