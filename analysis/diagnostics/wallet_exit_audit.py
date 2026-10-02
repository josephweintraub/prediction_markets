"""Describe terminal maker trading sequences without inferring wallet balances.

The retained maker-side OrderFilled source identifies only its own maker action.
Counterparty direction is deliberately unused: mint/merge matching can put both
orders on the same side. Prior activity includes all valid prices and actors,
but omits taker actions and all nontrade token movements. A prior BUY link is
evidence of an observed sequence, not proof of an open or profitable position.
"""
from __future__ import annotations

import argparse
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from analysis.multisport_game_dynamics.estimate_flb_decay import SPORTS
from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint, fingerprint, fresh_run, quoted, require_columns, write_json,
)


SUPPORT_FLOOR = 500
WINDOWS = (
    ("t80_90", "realized_time>=0.80 AND realized_time<0.90"),
    ("t90_95", "realized_time>=0.90 AND realized_time<0.95"),
    ("t95_99", "realized_time>=0.95 AND realized_time<0.99"),
    ("t99_100", "realized_time>=0.99 AND realized_time<=1.0"),
    ("last_120s", "realized_time>=0 AND seconds_to_end>=0 AND seconds_to_end<=120"),
)
SAMPLES = (
    ("filtered_trades", "price>0.01 AND price<0.99 AND NOT maker_is_flagged_nonhuman"),
    ("interior_all_actors", "price>0.01 AND price<0.99"),
    ("all_trades", "price>0 AND price<1"),
)
GROUPS = (
    ("all_maker_actions", "TRUE"),
    ("maker_winner_sells", "side='SELL' AND won"),
    ("maker_loser_sells", "side='SELL' AND NOT won"),
    ("maker_winner_buys", "side='BUY' AND won"),
    ("maker_loser_buys", "side='BUY' AND NOT won"),
    ("maker_high_price_sells", "side='SELL' AND price>=0.9"),
    ("winner_sells_after_prior_same_token_buy", "side='SELL' AND won AND prior_same_token_buy_count>0"),
    ("high_price_sells_after_lower_price_buy", "side='SELL' AND price>=0.9 AND prior_same_token_min_buy_price<price"),
    ("longshot_buys_after_prior_winner_buy", "side='BUY' AND price<0.1 AND linked_prior_winner_buy"),
)
BUY_GROUPS = (
    ("all_maker_buys", "TRUE"),
    ("prior_winner_buy_link", "linked_prior_winner_buy"),
    ("without_prior_winner_buy_link", "NOT linked_prior_winner_buy"),
)


def sources_from_manifest(path: Path) -> dict[str, Path]:
    """Read the frozen estimator's nine ordered source inputs; reject other layouts."""
    manifest = json.loads(path.read_text())
    names = ("new_phase", "mlb_phase", "nfl_phase", "nba_phase", "new_exact",
             "mlb_exact", "nfl_exact", "nba_exact", "wallet_flags")
    try:
        sources = {name: Path(manifest["inputs"][f"input_{i:02d}"]["path"])
                   for i, name in enumerate(names, 1)}
    except (KeyError, TypeError) as exc:
        raise ValueError("Expected the frozen decay estimator's nine-input manifest") from exc
    for name in names[:4]:
        if sources[name].name != "phase_trades.parquet":
            raise ValueError(f"Unexpected frozen phase source: {name}")
    if sources["new_exact"].name != "exact_buys.parquet":
        raise ValueError("Unexpected added-sport exact source in manifest")
    if set(manifest.get("sports", SPORTS)) != set(SPORTS):
        raise ValueError("Frozen estimator does not contain the nine-sport cohort")
    return sources


def _n(con: duckdb.DuckDBPyConnection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def _progress(message: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat()} {message}", file=sys.stderr, flush=True)


def create_market_clocks(con: duckdb.DuckDBPyConnection, sources: dict[str, Path]) -> None:
    """Use accepted metadata only; do not reuse the estimator's inferred BUY actors."""
    con.execute("SET TimeZone='UTC'")
    retained = ",".join(f"'{sport}'" for sport in SPORTS[3:])
    sql = [f"""SELECT DISTINCT sport,event_slug::VARCHAR event_id,market_id::VARCHAR market_id,
                   market_date::DATE market_date,actual_start_utc,actual_end_utc
                FROM read_parquet('{quoted(sources['new_exact'])}')
                WHERE sport IN ({retained})"""]
    for sport, game in (("mlb", "game_pk"), ("nfl", "game_id"), ("nba", "game_id")):
        sql.append(f"""SELECT DISTINCT '{sport}' sport,{game}::VARCHAR event_id,
                    market_id::VARCHAR market_id,official_date::DATE market_date,
                    actual_start_utc,actual_end_utc
                 FROM read_parquet('{quoted(sources[sport+'_phase'])}')""")
    con.execute("CREATE TEMP TABLE market_clocks AS " + " UNION ALL ".join(sql))
    if _n(con, "SELECT count(*) FROM (SELECT market_id FROM market_clocks GROUP BY 1 HAVING count(*)<>1)"):
        raise ValueError("Accepted market clocks must be unique")
    if _n(con, """SELECT count(*) FROM market_clocks WHERE market_id IS NULL OR event_id IS NULL
                   OR trim(event_id)='' OR actual_start_utc IS NULL OR actual_end_utc IS NULL
                   OR actual_end_utc<=actual_start_utc"""):
        raise ValueError("Invalid accepted market clocks")


def build_maker_actions(
    con: duckdb.DuckDBPyConnection, raw_path: Path, token_map: Path,
    timestamp_path: Path, wallet_flags: Path,
) -> dict[str, int]:
    """Scope once, deduplicate original log identities, and classify own-maker actions."""
    con.execute(f"CREATE TEMP VIEW source AS SELECT * FROM read_parquet('{quoted(raw_path)}')")
    require_columns(con, "source", (
        "maker", "taker", "maker_asset_id", "taker_asset_id", "maker_amount_filled",
        "taker_amount_filled", "fee", "block_number", "transaction_hash", "log_index",
        "exchange_address", "condition_id", "outcome", "winning_outcome",
    ), "Resolved maker-side OrderFilled source")
    con.execute("""CREATE TEMP TABLE scoped_source AS
      SELECT r.maker,r.taker,r.maker_asset_id,r.taker_asset_id,r.maker_amount_filled,
             r.taker_amount_filled,r.fee,r.block_number,r.transaction_hash,r.log_index,
             r.exchange_address,r.condition_id,r.outcome,r.winning_outcome
      FROM source r JOIN market_clocks m ON r.condition_id=m.market_id""")
    counts = {"scoped_source_rows": _n(con, "SELECT count(*) FROM scoped_source")}
    _progress(f"scoped resolved source: {counts['scoped_source_rows']} rows")
    con.execute("CREATE TEMP TABLE source_fills AS SELECT DISTINCT * FROM scoped_source")
    counts["distinct_source_fills"] = _n(con, "SELECT count(*) FROM source_fills")
    counts["exact_replay_rows_removed"] = counts["scoped_source_rows"] - counts["distinct_source_fills"]
    if not counts["distinct_source_fills"]:
        raise ValueError("No resolved maker fills matched accepted market clocks")
    conflicts = _n(con, """SELECT count(*) FROM (SELECT transaction_hash,log_index,exchange_address
                     FROM source_fills GROUP BY 1,2,3 HAVING count(*)<>1)""")
    if conflicts:
        raise ValueError(f"Contradictory original EVM identities: {conflicts}")
    _progress(f"original identities validated: {counts['distinct_source_fills']} distinct fills")
    if _n(con, """SELECT count(*) FROM source_fills WHERE maker IS NULL OR trim(maker)=''
          OR transaction_hash IS NULL OR trim(transaction_hash)='' OR log_index IS NULL OR log_index<0
          OR exchange_address IS NULL OR trim(exchange_address)='' OR block_number IS NULL OR block_number<0"""):
        raise ValueError("Missing maker or original log identity")
    # Stage2 inputs must remain maker-side; exchange-facing aggregate taker fills
    # have a different grain and may include refunds. Do not mix them into this audit.
    if _n(con, """SELECT count(*) FROM source_fills WHERE lower(taker) IN (
        '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e',
        '0xc5d563a36ae78145c45a50134d48a1215220f80a')"""):
        raise ValueError("Exchange-facing taker aggregates present in maker-only source")
    con.execute(f"CREATE TEMP VIEW tokens_source AS SELECT * FROM read_parquet('{quoted(token_map)}')")
    require_columns(con, "tokens_source", ("token_id", "condition_id", "outcome"), "Token map")
    con.execute("""CREATE TEMP TABLE scoped_tokens AS SELECT DISTINCT
      t.token_id::VARCHAR token_id,t.condition_id::VARCHAR market_id,t.outcome::VARCHAR outcome
      FROM tokens_source t JOIN market_clocks m ON t.condition_id=m.market_id""")
    if _n(con, """SELECT count(*) FROM (SELECT token_id FROM scoped_tokens GROUP BY 1 HAVING count(*)<>1)"""):
        raise ValueError("Token map must be unique per token")
    if _n(con, """SELECT count(*) FROM market_clocks m LEFT JOIN
       (SELECT market_id,count(*) n,count(DISTINCT outcome) outcomes FROM scoped_tokens GROUP BY 1) t
       USING(market_id) WHERE coalesce(t.n,0)<>2 OR t.outcomes<>2"""):
        raise ValueError("Each accepted binary market needs two unique mapped tokens/outcomes")
    _progress("canonical two-token maps validated")
    con.execute("""CREATE TEMP TABLE winners AS SELECT DISTINCT condition_id::VARCHAR market_id,
                   winning_outcome::VARCHAR winning_outcome FROM source_fills""")
    if _n(con, """SELECT count(*) FROM (SELECT market_id FROM winners GROUP BY 1 HAVING count(*)<>1)"""):
        raise ValueError("Contradictory winning outcomes in scoped source")
    if _n(con, "SELECT count(*) FROM winners WHERE winning_outcome IS NULL"):
        raise ValueError("Missing winning outcome")
    con.execute("""CREATE TEMP TABLE market_tokens AS
       SELECT t.*,w.winning_outcome,(t.outcome=w.winning_outcome)::BOOLEAN won,
              c.token_id complement_token_id,(c.outcome=w.winning_outcome)::BOOLEAN complement_won
       FROM scoped_tokens t JOIN winners w USING(market_id)
       JOIN scoped_tokens c ON c.market_id=t.market_id AND c.token_id<>t.token_id""")
    if _n(con, """SELECT count(*) FROM (SELECT market_id FROM market_tokens GROUP BY 1
             HAVING count(*)<>2 OR sum(won::INTEGER)<>1)"""):
        raise ValueError("Winning outcome must identify exactly one binary token")
    counts["accepted_markets"] = _n(con, "SELECT count(*) FROM market_clocks")
    counts["markets_without_source_fills"] = _n(con, "SELECT count(*) FROM market_clocks ANTI JOIN winners USING(market_id)")
    # Invalid asset configurations remain in an explicit exclusions artifact.
    con.execute("""CREATE TEMP TABLE source_exclusions AS SELECT *,
       CASE WHEN maker_asset_id IS NULL OR taker_asset_id IS NULL THEN 'missing_asset'
            WHEN (maker_asset_id='0')=(taker_asset_id='0') THEN 'not_one_collateral_asset'
            WHEN maker_amount_filled IS NULL OR taker_amount_filled IS NULL
                 OR maker_amount_filled<=0 OR taker_amount_filled<=0 THEN 'invalid_amount'
            WHEN fee IS NULL OR fee<0 OR fee>taker_amount_filled THEN 'invalid_fee'
            ELSE NULL END exclusion_reason FROM source_fills""")
    counts["excluded_asset_or_amount_rows"] = _n(con, "SELECT count(*) FROM source_exclusions WHERE exclusion_reason IS NOT NULL")
    _progress(f"source asset/amount exclusions: {counts['excluded_asset_or_amount_rows']} rows")
    con.execute("""CREATE TEMP TABLE valid_source AS SELECT *,
       CASE WHEN maker_asset_id='0' THEN taker_asset_id ELSE maker_asset_id END::VARCHAR token_id,
       CASE WHEN maker_asset_id='0' THEN 'BUY' ELSE 'SELL' END side,
       CASE WHEN maker_asset_id='0' THEN maker_amount_filled ELSE taker_amount_filled END/1e6::DOUBLE usdc,
       CASE WHEN maker_asset_id='0' THEN taker_amount_filled ELSE maker_amount_filled END/1e6::DOUBLE quantity
       FROM source_exclusions WHERE exclusion_reason IS NULL""")
    if _n(con, "SELECT count(*) FROM valid_source ANTI JOIN market_tokens USING(token_id)"):
        raise ValueError("Own-maker token missing from canonical scoped token map")
    if _n(con, """SELECT count(*) FROM valid_source s JOIN market_tokens t USING(token_id)
                   WHERE s.condition_id IS DISTINCT FROM t.market_id
                      OR s.outcome IS DISTINCT FROM t.outcome
                      OR s.winning_outcome IS DISTINCT FROM t.winning_outcome"""):
        raise ValueError("Resolved source contradicts canonical token map")
    if _n(con, "SELECT count(*) FROM valid_source WHERE NOT isfinite(usdc/quantity) OR usdc/quantity>1"):
        raise ValueError("Invalid own-maker execution price")
    _progress("own-maker token identities, outcomes and execution prices validated")
    con.execute(f"CREATE TEMP VIEW block_cache AS SELECT * FROM read_parquet('{quoted(timestamp_path)}')")
    require_columns(con, "block_cache", ("block_number", "timestamp"), "Exact block timestamps")
    con.execute("CREATE TEMP TABLE scoped_blocks AS SELECT DISTINCT block_number FROM source_fills")
    con.execute("""CREATE TEMP TABLE exact_blocks AS SELECT b.* FROM block_cache b
                   JOIN scoped_blocks s USING(block_number)""")
    if _n(con, "SELECT count(*) FROM (SELECT block_number FROM exact_blocks GROUP BY 1 HAVING count(*)<>1)"):
        raise ValueError("Duplicate scoped block timestamps")
    counts["missing_exact_blocks"] = _n(con, "SELECT count(*) FROM scoped_blocks ANTI JOIN exact_blocks USING(block_number)")
    if counts["missing_exact_blocks"]:
        raise ValueError(f"Missing exact block timestamps: {counts['missing_exact_blocks']} scoped blocks")
    _progress(f"exact block timestamps validated: {_n(con, 'SELECT count(*) FROM scoped_blocks')} blocks")
    con.execute(f"CREATE TEMP VIEW flags_source AS SELECT * FROM read_parquet('{quoted(wallet_flags)}')")
    require_columns(con, "flags_source", ("proxyWallet", "is_nonhuman"), "Wallet flags")
    con.execute("""CREATE TEMP TABLE flags AS SELECT DISTINCT lower(proxyWallet) wallet,
                   is_nonhuman::BOOLEAN is_nonhuman FROM flags_source""")
    if _n(con, "SELECT count(*) FROM flags WHERE wallet IS NULL OR trim(wallet)='' OR is_nonhuman IS NULL"):
        raise ValueError("Wallet flags need nonempty wallets and nonnull actor flags")
    if _n(con, "SELECT count(*) FROM (SELECT wallet FROM flags GROUP BY 1 HAVING count(*)<>1)"):
        raise ValueError("Conflicting flags for a normalized wallet")
    _progress("wallet flag uniqueness validated")
    con.execute("""CREATE TEMP TABLE maker_actions AS SELECT m.sport,m.event_id,
       m.sport||':'||m.event_id event_cluster,m.market_id,m.market_date,t.token_id,t.outcome,t.won,
       t.complement_token_id,t.complement_won,lower(s.maker)::VARCHAR wallet,s.side,
       s.block_number::BIGINT block_number,b.timestamp::BIGINT trade_timestamp,
       timezone('UTC',to_timestamp(b.timestamp))::DATE trade_day,
       s.transaction_hash::VARCHAR transaction_hash,s.log_index::INTEGER log_index,
       lower(s.exchange_address)::VARCHAR exchange_address,
       s.quantity::DOUBLE quantity,s.usdc::DOUBLE usdc,(s.usdc/s.quantity)::DOUBLE price,
       s.fee/1e6::DOUBLE fee_in_received_asset,
       (CASE WHEN s.side='BUY' THEN s.quantity-s.fee/1e6 ELSE -s.quantity END)::DOUBLE maker_token_delta,
       (CASE WHEN s.side='SELL' THEN s.usdc-s.fee/1e6 ELSE -s.usdc END)::DOUBLE maker_cash_delta,
       coalesce(w.is_nonhuman,false)::BOOLEAN maker_is_flagged_nonhuman,
       m.actual_start_utc,m.actual_end_utc,
       ((b.timestamp-epoch(m.actual_start_utc))/(epoch(m.actual_end_utc)-epoch(m.actual_start_utc)))::DOUBLE realized_time,
       (epoch(m.actual_end_utc)-b.timestamp)::DOUBLE seconds_to_end,
       least(floor(s.usdc/s.quantity*10)::INTEGER+1,10) price_bin,
       (t.won::DOUBLE-s.usdc/s.quantity)::DOUBLE calibration_error
       FROM valid_source s JOIN market_tokens t USING(token_id)
       JOIN market_clocks m ON m.market_id=t.market_id
       JOIN exact_blocks b USING(block_number)
       LEFT JOIN flags w ON w.wallet=lower(s.maker)""")
    counts["all_history_maker_actions"] = _n(con, "SELECT count(*) FROM maker_actions")
    if counts["all_history_maker_actions"] + counts["excluded_asset_or_amount_rows"] != counts["distinct_source_fills"]:
        raise ValueError("Own-maker expansion did not reconcile to source fills")
    if _n(con, "SELECT count(*) FROM maker_actions WHERE trade_timestamp IS NULL OR NOT isfinite(realized_time)"):
        raise ValueError("Invalid exact action timestamp")
    _progress(f"maker actions built and reconciled: {counts['all_history_maker_actions']} rows")
    return counts


def create_prior_links(con: duckdb.DuckDBPyConnection) -> None:
    """Exclude every action in the focal transaction from prior BUY links."""
    con.execute("""CREATE TEMP TABLE wallet_transactions AS
      SELECT wallet,market_id,transaction_hash,min(block_number) block_number,
             min(log_index) first_log,count(DISTINCT block_number) block_count
      FROM maker_actions GROUP BY 1,2,3""")
    if _n(con, "SELECT count(*) FROM wallet_transactions WHERE block_count<>1"):
        raise ValueError("A transaction identity spans multiple blocks")
    con.execute("""CREATE TEMP TABLE transaction_tokens AS
      SELECT x.wallet,x.market_id,x.transaction_hash,x.block_number,x.first_log,t.token_id,
             count(*) FILTER(WHERE a.side='BUY' AND a.maker_token_delta>0)::BIGINT buy_count,
             min(a.price) FILTER(WHERE a.side='BUY' AND a.maker_token_delta>0)::DOUBLE min_buy_price,
             max(a.trade_timestamp) FILTER(WHERE a.side='BUY' AND a.maker_token_delta>0)::BIGINT latest_buy_timestamp
      FROM wallet_transactions x JOIN market_tokens t USING(market_id)
      LEFT JOIN maker_actions a ON a.wallet=x.wallet AND a.market_id=x.market_id
        AND a.transaction_hash=x.transaction_hash AND a.token_id=t.token_id
      GROUP BY 1,2,3,4,5,6""")
    con.execute("""CREATE TEMP TABLE history_by_transaction AS SELECT *,
      coalesce(sum(buy_count) OVER prior,0)::BIGINT prior_buy_count,
      min(min_buy_price) OVER prior::DOUBLE prior_min_buy_price,
      max(latest_buy_timestamp) OVER prior::BIGINT prior_latest_buy_timestamp
      FROM transaction_tokens WINDOW prior AS (
        PARTITION BY wallet,market_id,token_id ORDER BY block_number,first_log
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)""")
    con.execute("""CREATE TEMP TABLE maker_prior_links AS SELECT a.*,
      h.prior_buy_count prior_same_token_buy_count,
      h.prior_min_buy_price prior_same_token_min_buy_price,
      h.prior_latest_buy_timestamp prior_same_token_latest_buy_timestamp,
      (a.trade_timestamp-h.prior_latest_buy_timestamp)::BIGINT seconds_since_latest_same_token_buy,
      c.prior_buy_count prior_complement_buy_count,
      c.prior_min_buy_price prior_complement_min_buy_price,
      c.prior_latest_buy_timestamp prior_complement_latest_buy_timestamp,
      (a.trade_timestamp-c.prior_latest_buy_timestamp)::BIGINT seconds_since_latest_complement_buy,
      (a.complement_won AND c.prior_buy_count>0)::BOOLEAN linked_prior_winner_buy
      FROM maker_actions a JOIN history_by_transaction h
        ON h.wallet=a.wallet AND h.market_id=a.market_id AND h.transaction_hash=a.transaction_hash AND h.token_id=a.token_id
      JOIN history_by_transaction c
        ON c.wallet=a.wallet AND c.market_id=a.market_id AND c.transaction_hash=a.transaction_hash AND c.token_id=a.complement_token_id""")
    if _n(con, "SELECT count(*) FROM maker_prior_links") != _n(con, "SELECT count(*) FROM maker_actions"):
        raise ValueError("Prior links multiplied or dropped own-maker actions")
    if _n(con, """SELECT count(*) FROM maker_prior_links WHERE seconds_since_latest_same_token_buy<0
                   OR seconds_since_latest_complement_buy<0"""):
        raise ValueError("Prior transaction BUY timestamp exceeds focal action timestamp")


def create_summaries(con: duckdb.DuckDBPyConnection) -> None:
    # One materialized terminal relation provides the two weighting diagnostics.
    queries = []
    for sample, rule in SAMPLES:
        for window, window_rule in WINDOWS:
            queries.append(f"SELECT '{sample}' AS sample,'{window}' AS window_id,* FROM maker_prior_links WHERE {rule} AND {window_rule}")
    con.execute("CREATE TEMP TABLE focal AS " + " UNION ALL ".join(queries))
    con.execute("""CREATE TEMP TABLE denominators AS SELECT sample,window_id,sport,
       count(*)::BIGINT n_fills,sum(quantity)::DOUBLE quantity,sum(usdc)::DOUBLE dollars
       FROM focal GROUP BY 1,2,3""")
    sample_sql = " UNION ALL ".join(f"SELECT '{name}' AS sample" for name, _ in SAMPLES)
    window_sql = " UNION ALL ".join(f"SELECT '{name}' window_id" for name, _ in WINDOWS)
    con.execute(f"""CREATE TEMP TABLE focal_sport_grid AS SELECT s.sample,w.window_id,p.sport
       FROM ({sample_sql}) s CROSS JOIN ({window_sql}) w
       CROSS JOIN (SELECT DISTINCT sport FROM market_clocks) p""")
    summaries = []
    for group, rule in GROUPS:
        summaries.append(f"""SELECT g.sample,g.window_id,g.sport,'{group}' action_group,
          coalesce(v.n_fills,0)::BIGINT n_fills,coalesce(v.n_events,0)::BIGINT n_events,
          coalesce(v.n_wallets,0)::BIGINT n_wallets,coalesce(v.quantity,0)::DOUBLE quantity,
          coalesce(v.dollars,0)::DOUBLE dollars,
          v.mean_price,v.equal_event_fill_share,v.mean_seconds_since_same_token_buy,
          v.mean_seconds_since_complement_buy,
          coalesce(v.n_fills,0)/nullif(d.n_fills,0)::DOUBLE fill_share_all_maker_actions,
          coalesce(v.quantity,0)/nullif(d.quantity,0)::DOUBLE quantity_share_all_maker_actions,
          coalesce(v.dollars,0)/nullif(d.dollars,0)::DOUBLE dollar_share_all_maker_actions,
          coalesce(d.n_fills,0)::BIGINT denominator_fills,
          coalesce(v.n_fills,0)<{SUPPORT_FLOOR} suppressed
          FROM focal_sport_grid g LEFT JOIN denominators d USING(sample,window_id,sport)
          LEFT JOIN (SELECT sample,window_id,sport,
            count(*) FILTER(WHERE {rule})::BIGINT n_fills,
            count(DISTINCT event_cluster) FILTER(WHERE {rule})::BIGINT n_events,
            count(DISTINCT wallet) FILTER(WHERE {rule})::BIGINT n_wallets,
            sum(quantity) FILTER(WHERE {rule})::DOUBLE quantity,
            sum(usdc) FILTER(WHERE {rule})::DOUBLE dollars,
            avg(price) FILTER(WHERE {rule})::DOUBLE mean_price,
            avg(seconds_since_latest_same_token_buy) FILTER(WHERE {rule})::DOUBLE mean_seconds_since_same_token_buy,
            avg(seconds_since_latest_complement_buy) FILTER(WHERE {rule})::DOUBLE mean_seconds_since_complement_buy,
            sum(CASE WHEN {rule} THEN 1.0/event_n ELSE 0 END)/count(DISTINCT event_cluster)::DOUBLE equal_event_fill_share
            FROM (SELECT *,count(*) OVER(PARTITION BY sample,window_id,sport,event_cluster) event_n FROM focal)
            GROUP BY 1,2,3) v USING(sample,window_id,sport)""")
    con.execute("CREATE TEMP TABLE terminal_maker_summary AS " + " UNION ALL ".join(summaries))
    buy_queries = [f"SELECT *, '{name}' buy_group FROM focal WHERE side='BUY' AND {rule}"
                   for name, rule in BUY_GROUPS]
    con.execute("CREATE TEMP TABLE focal_buys AS " + " UNION ALL ".join(buy_queries))
    con.execute("""CREATE TEMP TABLE buy_moments AS SELECT sample,window_id,sport,buy_group,price_bin,
       count(*)::BIGINT n_fills,count(DISTINCT event_cluster)::BIGINT n_events,
       count(DISTINCT wallet)::BIGINT n_wallets,sum(quantity)::DOUBLE quantity,
       sum(usdc)::DOUBLE dollars,avg(price)::DOUBLE mean_price,avg(won::DOUBLE)::DOUBLE win_rate,
       avg(calibration_error)::DOUBLE calibration_equal_fill,
       sum(usdc*calibration_error)/nullif(sum(usdc),0)::DOUBLE calibration_dollar,
       sum(calibration_error/event_n)/count(DISTINCT event_cluster)::DOUBLE calibration_equal_event
       FROM (SELECT *,count(*) OVER(
         PARTITION BY sample,window_id,sport,buy_group,price_bin,event_cluster) event_n FROM focal_buys)
       GROUP BY 1,2,3,4,5""")
    group_sql = " UNION ALL ".join(f"SELECT '{name}' buy_group" for name, _ in BUY_GROUPS)
    con.execute(f"""CREATE TEMP TABLE buy_grid AS SELECT f.*,g.buy_group,b.price_bin::INTEGER price_bin
       FROM focal_sport_grid f CROSS JOIN ({group_sql}) g
       CROSS JOIN range(1,11) b(price_bin)""")
    con.execute(f"""CREATE TEMP TABLE linked_buy_profile AS SELECT g.*,
       coalesce(m.n_fills,0)::BIGINT n_fills,coalesce(m.n_events,0)::BIGINT n_events,
       coalesce(m.n_wallets,0)::BIGINT n_wallets,coalesce(m.quantity,0)::DOUBLE quantity,
       coalesce(m.dollars,0)::DOUBLE dollars,coalesce(m.n_fills,0)<{SUPPORT_FLOOR} suppressed,
       CASE WHEN m.n_fills>={SUPPORT_FLOOR} THEN m.mean_price END mean_price,
       CASE WHEN m.n_fills>={SUPPORT_FLOOR} THEN m.win_rate END win_rate,
       CASE WHEN m.n_fills>={SUPPORT_FLOOR} THEN m.calibration_equal_fill END calibration_equal_fill,
       CASE WHEN m.n_fills>={SUPPORT_FLOOR} THEN m.calibration_dollar END calibration_dollar,
       CASE WHEN m.n_fills>={SUPPORT_FLOOR} THEN m.calibration_equal_event END calibration_equal_event
       FROM buy_grid g LEFT JOIN buy_moments m USING(sample,window_id,sport,buy_group,price_bin)""")
    con.execute("""CREATE TEMP TABLE paired_event_tails AS
       WITH bins AS (SELECT sample,window_id,sport,buy_group,event_cluster,price_bin,
                     avg(calibration_error) AS mean_calibration,count(*)::BIGINT n_fills FROM focal_buys
                     WHERE price_bin IN (1,10) GROUP BY 1,2,3,4,5,6),
       paired AS (SELECT sample,window_id,sport,buy_group,event_cluster,
                 max(mean_calibration) FILTER(WHERE price_bin=10)-max(mean_calibration) FILTER(WHERE price_bin=1) spread,
                 sum(n_fills) FILTER(WHERE price_bin=1)::BIGINT d1_n,
                 sum(n_fills) FILTER(WHERE price_bin=10)::BIGINT d10_n
                 FROM bins GROUP BY 1,2,3,4,5 HAVING count(*)=2)
       SELECT sample,window_id,sport,buy_group,count(*)::BIGINT paired_event_count,
              sum(d1_n)::BIGINT paired_d1_n,sum(d10_n)::BIGINT paired_d10_n,
              avg(spread)::DOUBLE spread_equal_paired_event FROM paired GROUP BY 1,2,3,4""")
    con.execute(f"""CREATE TEMP TABLE linked_buy_tails AS SELECT
       a.sample,a.window_id,a.sport,a.buy_group,a.n_fills d1_n,b.n_fills d10_n,
       a.n_events d1_events,b.n_events d10_events,(a.suppressed OR b.suppressed) suppressed,
       CASE WHEN NOT a.suppressed AND NOT b.suppressed THEN a.calibration_equal_fill END d1_error,
       CASE WHEN NOT a.suppressed AND NOT b.suppressed THEN b.calibration_equal_fill END d10_error,
       b.calibration_equal_fill-a.calibration_equal_fill spread_equal_fill,
       b.calibration_dollar-a.calibration_dollar spread_dollar,
       coalesce(p.paired_event_count,0)::BIGINT paired_event_count,
       coalesce(p.paired_d1_n,0)::BIGINT paired_d1_n,coalesce(p.paired_d10_n,0)::BIGINT paired_d10_n,
       (coalesce(p.paired_d1_n,0)<{SUPPORT_FLOOR} OR coalesce(p.paired_d10_n,0)<{SUPPORT_FLOOR}) paired_suppressed,
       CASE WHEN p.paired_d1_n>={SUPPORT_FLOOR} AND p.paired_d10_n>={SUPPORT_FLOOR}
            THEN p.spread_equal_paired_event END spread_equal_paired_event
       FROM linked_buy_profile a JOIN linked_buy_profile b
       USING(sample,window_id,sport,buy_group)
       LEFT JOIN paired_event_tails p USING(sample,window_id,sport,buy_group)
       WHERE a.price_bin=1 AND b.price_bin=10""")
    if _n(con, """SELECT count(*) FROM terminal_maker_summary WHERE n_fills>denominator_fills
                   OR fill_share_all_maker_actions>1+1e-12"""):
        raise ValueError("Terminal shares exceed reconciled maker denominators")
    if _n(con, """SELECT count(*) FROM linked_buy_profile WHERE NOT suppressed
                AND abs(calibration_equal_fill-(win_rate-mean_price))>1e-12"""):
        raise ValueError("Calibration decomposition failed")


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    decay_manifest = Path(args.decay_manifest).resolve()
    sources = sources_from_manifest(decay_manifest)
    raw_path, tokens_path, timestamps_path = map(Path, (args.resolved_trades, args.token_map, args.block_timestamps))
    flags_path = Path(args.wallet_flags) if args.wallet_flags else sources["wallet_flags"]
    inputs = [decay_manifest, raw_path, tokens_path, timestamps_path, flags_path,
              sources["new_exact"], sources["mlb_phase"], sources["nfl_phase"], sources["nba_phase"]]
    if any(not path.is_file() for path in inputs):
        raise FileNotFoundError([str(path) for path in inputs if not path.is_file()])
    with fresh_run(args.run_dir, inputs) as staging:
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone='UTC'")
            con.execute(f"SET threads={args.threads}")
            con.execute(f"SET memory_limit='{args.memory_limit}'")
            con.execute("SET max_temp_directory_size='20GB'")
            if args.temp_directory:
                con.execute(f"SET temp_directory='{quoted(args.temp_directory)}'")
            create_market_clocks(con, sources)
            _progress(f"accepted market clocks built: {_n(con, 'SELECT count(*) FROM market_clocks')} markets")
            counts = build_maker_actions(con, raw_path, tokens_path, timestamps_path, flags_path)
            create_prior_links(con)
            _progress("partial maker-history predecessor links validated")
            create_summaries(con)
            _progress("terminal action and supported calibration summaries built")
            counts["pre_start_actions"] = _n(con, "SELECT count(*) FROM maker_actions WHERE realized_time<0")
            counts["live_actions"] = _n(con, "SELECT count(*) FROM maker_actions WHERE realized_time>=0 AND realized_time<=1")
            counts["post_end_actions"] = _n(con, "SELECT count(*) FROM maker_actions WHERE realized_time>1")
            con.execute("CREATE TEMP TABLE reconciliation(metric VARCHAR,value BIGINT)")
            con.executemany("INSERT INTO reconciliation VALUES (?,?)", list(counts.items()))
            relations = ("market_clocks", "market_tokens", "maker_prior_links",
                         "terminal_maker_summary", "linked_buy_profile", "linked_buy_tails", "reconciliation")
            unique_keys = {
                "market_clocks": "market_id", "market_tokens": "token_id",
                "maker_prior_links": "transaction_hash,log_index,exchange_address",
                "terminal_maker_summary": "sample,window_id,sport,action_group",
                "linked_buy_profile": "sample,window_id,sport,buy_group,price_bin",
                "linked_buy_tails": "sample,window_id,sport,buy_group", "reconciliation": "metric",
            }
            outputs = []
            for relation in relations:
                name = relation + ".parquet"
                con.execute(f"COPY (SELECT * FROM {relation} ORDER BY {unique_keys[relation]}) TO '{quoted(staging/name)}' (FORMAT PARQUET,COMPRESSION ZSTD)")
                outputs.append(name)
            con.execute(f"""COPY (SELECT * FROM source_exclusions WHERE exclusion_reason IS NOT NULL
                ORDER BY transaction_hash,log_index,exchange_address)
                TO '{quoted(staging/'source_exclusions.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)""")
            outputs.append("source_exclusions.parquet")
            # Reopen each publication and verify its grain rather than trusting COPY.
            row_counts = {}
            for relation in relations:
                path = staging/(relation+".parquet")
                read = f"read_parquet('{quoted(path)}')"
                row_counts[relation] = _n(con, f"SELECT count(*) FROM {read}")
                if row_counts[relation] != _n(con, f"SELECT count(*) FROM {relation}"):
                    raise ValueError(f"Publication count changed: {relation}")
                if _n(con, f"SELECT count(*) FROM (SELECT {unique_keys[relation]} FROM {read} GROUP BY ALL HAVING count(*)<>1)"):
                    raise ValueError(f"Publication uniqueness failed: {relation}")
        finally:
            con.close()
        _progress("published parquet staging passed reopen checks; fingerprinting inputs")
        manifest = {
            "schema_version": 1, "stage": "terminal_maker_wallet_sequences_v1",
            "created_at_utc": datetime.now(timezone.utc).isoformat(), "command": sys.argv,
            "environment": {"python": platform.python_version(), "duckdb": duckdb.__version__, "platform": platform.platform()},
            "code": {"script": fingerprint(Path(__file__))},
            "contract": {
                "unit": "one retained maker-side source OrderFilled identity; own maker action only",
                "history": "all valid retained maker actions; price=1 and flagged actors retained for prior links",
                "prior_link": "same maker wallet and binary market; all actions in focal transaction excluded",
                "time": "accepted frozen event clocks and exact UTC block timestamps; focal fills at or before recorded end",
                "weights": "fill, gross token quantity, gross collateral dollars; equal-event descriptive sensitivity",
                "calibration": "eventual outcome minus executed own-maker BUY price, before fees",
                "support_floor": SUPPORT_FLOOR,
                "uncertainty": "descriptive estimates only; no intervals or significance tests",
                "limits": "Prior BUY links do not prove remaining positions, profitable exits, or causation. Taker actions, transfers, splits, merges, redemptions and conversions are absent. Stage2 may have removed repeated order fills. ATP clocks retain the frozen scheduled-start/duration limitation.",
                "source_rule": "https://github.com/Polymarket/ctf-exchange/blob/main/src/exchange/mixins/Trading.sol",
                "raw_taker_recovery": "deferred; exchange-facing aggregates must not be mixed with maker fill grain",
            },
            "windows": dict(WINDOWS), "samples": dict(SAMPLES), "groups": dict(GROUPS),
            "counts": counts, "output_rows": row_counts,
            "inputs": {f"input_{i:02d}": fingerprint(path) for i, path in enumerate(inputs,1)},
            "outputs": {name: artifact_fingerprint(staging/name) for name in outputs},
            "completion_status": "complete",
        }
        write_json(staging/"manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("decay_manifest", "resolved_trades", "token_map", "block_timestamps", "run_dir"):
        parser.add_argument("--"+name.replace("_","-"), required=True)
    parser.add_argument("--wallet-flags")
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--memory-limit", default="150GB")
    parser.add_argument("--temp-directory")
    return parser.parse_args(argv)


if __name__ == "__main__":
    print(json.dumps(run_audit(parse_args()), indent=2, sort_keys=True))
