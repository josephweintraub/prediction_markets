"""Attribute outcome-blind FIFO tags to original actual own-order BUY logs.

Inputs are complete, verified source actions, reconciled batch links, and one
ledger tag per action. NORMAL links carry a direct SELL tag to its true BUY;
MINT contains two BUYs but no sale; MERGE contains no BUY. Active aggregates keep
one original fill unit and use effective execution VWAP, not a per-leg bin.
Metadata is deliberately attached in a separate final step after classification.
This is observed trade-implied accounting, not causal identification or balances.
Status: synthetic-only aggregate-own-log prototype, not a replacement for the
project's matched partial-fill calibration grain. Production grain is unresolved.
"""
from __future__ import annotations

import re

import duckdb

from analysis.diagnostics.profit_taking_actions import EXCHANGE_CONTRACTS
from analysis.diagnostics.profit_taking_contribution import validate_executions
from analysis.sports_game_dynamics.artifacts import require_columns


OWN_COLUMNS = (
    "execution_id", "market_id", "maker", "maker_asset_id", "taker_asset_id",
    "maker_amount_filled", "taker_amount_filled", "fee", "block_number",
    "transaction_hash", "log_index", "exchange_address", "source_role",
    "source_status", "source_contract_version", "fee_rule", "aggregate_reconciled",
)
TAG_COLUMNS = (
    "execution_id", "market_id", "token_id", "wallet", "side", "block_number",
    "transaction_hash", "log_index", "exchange_address", "gross_quantity_micro",
    "gross_cash_micro", "fee_micro", "net_acquired_quantity_micro",
    "primary_exit_quantity_micro", "hedge_profitable_quantity_micro",
    "unmatched_disposal_quantity_micro", "primary_exit_fraction", "hedge_fraction",
    "unmatched_disposal_fraction", "history_status", "fee_rule", "source_contract_version",
)
LINK_COLUMNS = (
    "maker_execution_id", "active_execution_id", "kind", "quantity_micro",
    "passive_cash_micro", "active_cash_micro",
)
HISTORY_STATUS = "trade_implied_only_opening_and_nontrade_movements_unknown"


def _identifier(name: str) -> str:
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*",name):
        raise ValueError("Relation names must be simple SQL identifiers")
    return '"'+name+'"'


def _reject_existing(con: duckdb.DuckDBPyConnection,names: list[str]) -> None:
    for name in names:
        if con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name=?",[name]).fetchone()[0] \
                or con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name=?",[name]).fetchone()[0]:
            raise ValueError(f"Refusing to replace an attribution relation: {name}")


def _require_zero(con: duckdb.DuckDBPyConnection,query: str,message: str) -> None:
    if con.execute(query).fetchone()[0]:
        raise ValueError(message)


def create_buy_attribution(
    con: duckdb.DuckDBPyConnection,
    own_actions: str,
    action_tags: str,
    batch_links: str,
    *,
    prefix: str="profit_taking",
) -> dict[str,str]:
    """Build BUY tags without reading any outcome, clock or actor filters.

    Direct attribution is the SELL's primary qualified gross disposed fraction
    times the link's gross quantity, divided by the original BUY's gross quantity.
    Aggregates with multiple legs use deterministic proportional sale allocation.
    Complement hedge fractions use qualified net received / total net received.

    ``unknown_history_fraction`` narrowly denotes a favorite-priced BUY's share
    linked to favorite SELL quantity without observed FIFO acquisitions. The
    explicit alias is ``unmatched_seller_acquisition_fraction``. An ordinary BUY
    with no prior favorite stock is not made unknown. Opening/nontrade uncertainty
    remains a separate global history status. Unsupported source batches must be
    recorded upstream and must not appear here as falsely verified zero tags.
    """
    source,tags,links=(_identifier(name) for name in (own_actions,action_tags,batch_links))
    _identifier(prefix)
    for relation,columns,label in (
        (own_actions,OWN_COLUMNS,"Verified effective own actions"),
        (action_tags,TAG_COLUMNS,"One-row-per-action FIFO tags"),
        (batch_links,LINK_COLUMNS,"Verified complete batch links"),
    ):
        require_columns(con,relation,columns,label)
    names={key:prefix+"_"+key for key in ("actions","direct_links","buy_tags","attribution_diagnostics")}
    _reject_existing(con,list(names.values()))
    contracts=" OR ".join(
        f"(lower(exchange_address)='{address}' AND source_contract_version='{version}' AND fee_rule='{rule}')"
        for address,(version,rule) in sorted(EXCHANGE_CONTRACTS.items())
    )
    _require_zero(con,f"""SELECT count(*) FROM {source} WHERE execution_id IS NULL OR market_id IS NULL
        OR trim(execution_id)='' OR trim(market_id)='' OR maker IS NULL OR trim(maker)=''
        OR source_status IS DISTINCT FROM 'verified_own_action'
        OR source_contract_version IS NULL OR fee_rule IS NULL OR NOT ({contracts})
        OR source_role NOT IN ('passive','active_aggregate')
        OR source_role IS NULL OR aggregate_reconciled IS NULL
        OR aggregate_reconciled<>(source_role='active_aggregate')
        OR (maker_asset_id='0')=(taker_asset_id='0') OR maker_asset_id IS NULL OR taker_asset_id IS NULL
        OR maker_amount_filled IS NULL OR taker_amount_filled IS NULL
        OR maker_amount_filled<0 OR taker_amount_filled<0 OR fee IS NULL OR fee<0
        OR ((fee_rule='received_asset' OR maker_asset_id<>'0') AND fee>taker_amount_filled)
        OR block_number IS NULL OR block_number<0
        OR transaction_hash IS NULL OR trim(transaction_hash)='' OR log_index IS NULL OR log_index<0
        OR exchange_address IS NULL OR trim(exchange_address)=''
        OR execution_id<>lower(exchange_address)||':'||lower(transaction_hash)||':'||log_index::VARCHAR""",
        "Own actions must carry verified source certificates and original identities")
    for relation in (source,tags):
        _require_zero(con,f"SELECT count(*) FROM (SELECT execution_id FROM {relation} GROUP BY 1 HAVING count(*)<>1)",
                      "Own actions and ledger tags must be unique by original execution ID")
    _require_zero(con,f"SELECT count(*) FROM {source} ANTI JOIN {tags} USING(execution_id)","Missing own-action ledger tags")
    _require_zero(con,f"SELECT count(*) FROM {tags} ANTI JOIN {source} USING(execution_id)","Ledger tags contain unknown actions")
    actions,direct,buys,diagnostics=(_identifier(names[key]) for key in names)
    con.execute(f"""CREATE TEMP VIEW {actions} AS SELECT s.execution_id,s.market_id,lower(s.maker) wallet,
        CASE WHEN maker_asset_id='0' THEN taker_asset_id ELSE maker_asset_id END token_id,
        CASE WHEN maker_asset_id='0' THEN 'BUY' ELSE 'SELL' END side,
        CASE WHEN maker_asset_id='0' THEN taker_amount_filled ELSE maker_amount_filled END::BIGINT gross_quantity_micro,
        CASE WHEN maker_asset_id='0' THEN maker_amount_filled ELSE taker_amount_filled END::BIGINT gross_cash_micro,
        s.fee::BIGINT fee_micro,s.block_number::BIGINT block_number,lower(s.transaction_hash) transaction_hash,
        s.log_index::BIGINT log_index,lower(s.exchange_address) exchange_address,s.source_role,s.fee_rule,s.source_contract_version,
        CASE WHEN maker_asset_id='0' THEN taker_amount_filled-
             CASE WHEN s.fee_rule='received_asset' THEN s.fee ELSE 0 END ELSE NULL END::BIGINT net_acquired_quantity_micro,
        t.primary_exit_quantity_micro,t.hedge_profitable_quantity_micro,t.unmatched_disposal_quantity_micro,
        t.primary_exit_fraction,t.hedge_fraction,t.unmatched_disposal_fraction,t.history_status
        FROM {source} s JOIN {tags} t USING(execution_id)""")
    _require_zero(con,f"SELECT count(*) FROM {actions} WHERE gross_quantity_micro<=0 OR gross_cash_micro>gross_quantity_micro",
                  "Effective binary execution amounts are invalid")
    _require_zero(con,f"""SELECT count(*) FROM {actions} a JOIN {tags} t USING(execution_id) WHERE
        a.market_id IS DISTINCT FROM t.market_id OR a.token_id IS DISTINCT FROM t.token_id
        OR a.wallet IS DISTINCT FROM lower(t.wallet) OR a.side IS DISTINCT FROM t.side
        OR a.block_number IS DISTINCT FROM t.block_number
        OR a.transaction_hash IS DISTINCT FROM lower(t.transaction_hash)
        OR a.log_index IS DISTINCT FROM t.log_index OR a.exchange_address IS DISTINCT FROM lower(t.exchange_address)
        OR a.gross_quantity_micro IS DISTINCT FROM t.gross_quantity_micro
        OR a.gross_cash_micro IS DISTINCT FROM t.gross_cash_micro OR a.fee_micro IS DISTINCT FROM t.fee_micro
        OR a.fee_rule IS DISTINCT FROM t.fee_rule OR a.source_contract_version IS DISTINCT FROM t.source_contract_version
        OR (a.side='BUY' AND t.net_acquired_quantity_micro IS DISTINCT FROM a.net_acquired_quantity_micro)
        OR a.history_status IS DISTINCT FROM '{HISTORY_STATUS}'""",
        "Ledger tags contradict their verified own actions or history status")
    _require_zero(con,f"""SELECT count(*) FROM {actions} WHERE
        primary_exit_quantity_micro IS NULL OR hedge_profitable_quantity_micro IS NULL
        OR unmatched_disposal_quantity_micro IS NULL OR primary_exit_quantity_micro<0
        OR hedge_profitable_quantity_micro<0 OR unmatched_disposal_quantity_micro<0
        OR primary_exit_quantity_micro+unmatched_disposal_quantity_micro>gross_quantity_micro
        OR (side='BUY' AND (primary_exit_quantity_micro<>0 OR unmatched_disposal_quantity_micro<>0
            OR hedge_profitable_quantity_micro>net_acquired_quantity_micro))
        OR (side='SELL' AND hedge_profitable_quantity_micro<>0)
        OR primary_exit_fraction IS NULL OR NOT isfinite(primary_exit_fraction)
        OR hedge_fraction IS NULL OR NOT isfinite(hedge_fraction)
        OR unmatched_disposal_fraction IS NULL OR NOT isfinite(unmatched_disposal_fraction)
        OR abs(primary_exit_fraction-primary_exit_quantity_micro/gross_quantity_micro::DOUBLE)>1e-12
        OR abs(unmatched_disposal_fraction-unmatched_disposal_quantity_micro/gross_quantity_micro::DOUBLE)>1e-12
        OR abs(hedge_fraction-CASE WHEN side='BUY' THEN
            coalesce(hedge_profitable_quantity_micro/nullif(net_acquired_quantity_micro,0)::DOUBLE,0)
            ELSE 0 END)>1e-12
        OR (primary_exit_quantity_micro>0 AND gross_cash_micro/gross_quantity_micro::DOUBLE<=.5)""",
        "Ledger quantities, fraction denominators or sale classifications do not reconcile")
    _require_zero(con,f"""SELECT count(*) FROM {links} WHERE maker_execution_id IS NULL OR active_execution_id IS NULL
        OR kind IS NULL OR kind NOT IN ('NORMAL','MINT','MERGE') OR quantity_micro IS NULL OR quantity_micro<=0
        OR passive_cash_micro IS NULL OR active_cash_micro IS NULL OR passive_cash_micro<0 OR active_cash_micro<0
        OR passive_cash_micro>quantity_micro OR active_cash_micro>quantity_micro""","Invalid reconciled batch links")
    _require_zero(con,f"""SELECT count(*) FROM (SELECT maker_execution_id FROM {links}
        GROUP BY 1 HAVING count(*)<>1)""","A passive own log must map to exactly one active batch")
    for key in ("maker_execution_id","active_execution_id"):
        _require_zero(con,f"SELECT count(*) FROM {links} l ANTI JOIN {actions} a ON a.execution_id=l.{key}",
                      "Batch link references an unknown own action")
    _require_zero(con,f"""SELECT count(*) FROM {links} l JOIN {actions} m ON m.execution_id=l.maker_execution_id
        JOIN {actions} a ON a.execution_id=l.active_execution_id WHERE m.source_role<>'passive'
        OR a.source_role<>'active_aggregate' OR m.market_id<>a.market_id OR m.transaction_hash<>a.transaction_hash
        OR m.exchange_address<>a.exchange_address OR m.block_number<>a.block_number OR m.log_index>=a.log_index
        OR l.quantity_micro<>m.gross_quantity_micro OR l.passive_cash_micro<>m.gross_cash_micro
        OR (l.kind='NORMAL' AND (m.side=a.side OR m.token_id<>a.token_id OR l.passive_cash_micro<>l.active_cash_micro))
        OR (l.kind='MINT' AND (m.side<>'BUY' OR a.side<>'BUY' OR m.token_id=a.token_id
                              OR l.passive_cash_micro+l.active_cash_micro<>l.quantity_micro))
        OR (l.kind='MERGE' AND (m.side<>'SELL' OR a.side<>'SELL' OR m.token_id=a.token_id
                               OR l.passive_cash_micro+l.active_cash_micro<>l.quantity_micro))""",
        "Batch links contradict true own directions, identities or amounts")
    _require_zero(con,f"""SELECT count(*) FROM {actions} a LEFT JOIN
        (SELECT active_execution_id,sum(quantity_micro) quantity,sum(active_cash_micro) cash FROM {links} GROUP BY 1) l
        ON a.execution_id=l.active_execution_id WHERE a.source_role='active_aggregate'
        AND (l.quantity IS DISTINCT FROM a.gross_quantity_micro OR l.cash IS DISTINCT FROM a.gross_cash_micro)""",
        "Active aggregate effective quantities or cash do not reconcile to all legs")
    _require_zero(con,f"""SELECT count(*) FROM {actions} a ANTI JOIN {links} l ON a.execution_id=l.maker_execution_id
        WHERE a.source_role='passive'""","Passive own actions are missing complete batch links")
    con.execute(f"""CREATE TEMP VIEW {direct} AS SELECT
        CASE WHEN m.side='BUY' THEN m.execution_id ELSE a.execution_id END buyer_execution_id,
        CASE WHEN m.side='SELL' THEN m.execution_id ELSE a.execution_id END seller_execution_id,
        l.quantity_micro,
        CASE WHEN m.side='SELL' THEN m.gross_cash_micro/m.gross_quantity_micro::DOUBLE
             ELSE a.gross_cash_micro/a.gross_quantity_micro::DOUBLE END seller_price,
        l.quantity_micro::DOUBLE*CASE WHEN m.side='SELL' THEN m.primary_exit_fraction ELSE a.primary_exit_fraction END
            allocated_primary_exit_quantity_micro,
        l.quantity_micro::DOUBLE*CASE WHEN m.side='SELL' THEN m.unmatched_disposal_fraction ELSE a.unmatched_disposal_fraction END
            allocated_unmatched_disposal_quantity_micro
        FROM {links} l JOIN {actions} m ON m.execution_id=l.maker_execution_id
        JOIN {actions} a ON a.execution_id=l.active_execution_id WHERE l.kind='NORMAL'""")
    con.execute(f"""CREATE TEMP VIEW {buys} AS WITH direct_allocations AS
        (SELECT buyer_execution_id,sum(allocated_primary_exit_quantity_micro) raw_exit_quantity_micro,
            sum(allocated_unmatched_disposal_quantity_micro) unmatched_counterparty_disposal_quantity_micro,
            sum(CASE WHEN seller_price>.5 THEN allocated_unmatched_disposal_quantity_micro ELSE 0 END)
                unmatched_favorite_seller_acquisition_quantity_micro,
            sum(CASE WHEN least(floor(seller_price*10)::INTEGER+1,10)<>
                          least(floor(a.gross_cash_micro/a.gross_quantity_micro::DOUBLE*10)::INTEGER+1,10)
                     THEN allocated_primary_exit_quantity_micro ELSE 0 END) crossed_bin_exit_quantity_micro
         FROM {direct} d JOIN {actions} a ON a.execution_id=d.buyer_execution_id GROUP BY 1),
        own_buys AS (SELECT a.*,a.gross_cash_micro/a.gross_quantity_micro::DOUBLE price,
            coalesce(d.raw_exit_quantity_micro,0)::DOUBLE raw_exit_quantity_micro,
            coalesce(d.unmatched_counterparty_disposal_quantity_micro,0)::DOUBLE unmatched_counterparty_disposal_quantity_micro,
            coalesce(d.unmatched_favorite_seller_acquisition_quantity_micro,0)::DOUBLE unmatched_favorite_seller_acquisition_quantity_micro,
            coalesce(d.crossed_bin_exit_quantity_micro,0)::DOUBLE crossed_bin_exit_quantity_micro
            FROM {actions} a LEFT JOIN direct_allocations d ON d.buyer_execution_id=a.execution_id WHERE a.side='BUY')
        SELECT *,FALSE is_synthetic,
            CASE WHEN price>.5 THEN raw_exit_quantity_micro/gross_quantity_micro ELSE 0 END::DOUBLE exit_fraction,
            CASE WHEN price<.5 THEN hedge_fraction ELSE 0 END::DOUBLE qualified_hedge_fraction,
            CASE WHEN price>.5 THEN unmatched_favorite_seller_acquisition_quantity_micro/gross_quantity_micro
                 ELSE 0 END::DOUBLE unmatched_seller_acquisition_fraction,
            CASE WHEN price<=.5 THEN raw_exit_quantity_micro ELSE 0 END::DOUBLE crossed_price_exit_quantity_micro,
            CASE WHEN price>=.5 THEN hedge_profitable_quantity_micro ELSE 0 END::BIGINT crossed_price_hedge_quantity_micro
        FROM own_buys""")
    _require_zero(con,f"""SELECT count(*) FROM {buys} WHERE exit_fraction<0 OR qualified_hedge_fraction<0
        OR exit_fraction+qualified_hedge_fraction>1+1e-12 OR unmatched_seller_acquisition_fraction<0
        OR exit_fraction+unmatched_seller_acquisition_fraction>1+1e-12""",
        "BUY quantity allocation exceeds its original execution")
    con.execute(f"""CREATE TEMP TABLE {diagnostics} AS
        SELECT 'original_own_buy_logs' measure,count(*)::BIGINT n_records,sum(gross_quantity_micro)::DOUBLE quantity_micro FROM {buys}
        UNION ALL SELECT 'normal_buy_sell_links',count(*),sum(quantity_micro)::DOUBLE FROM {links} WHERE kind='NORMAL'
        UNION ALL SELECT 'mint_links_two_true_buys_no_direct_sale',count(*),sum(quantity_micro)::DOUBLE FROM {links} WHERE kind='MINT'
        UNION ALL SELECT 'merge_links_no_actual_buy',count(*),sum(quantity_micro)::DOUBLE FROM {links} WHERE kind='MERGE'
        UNION ALL SELECT 'qualified_exit_crosses_original_buy_price_half',count(*),sum(crossed_price_exit_quantity_micro)
                  FROM {buys} WHERE crossed_price_exit_quantity_micro>0
        UNION ALL SELECT 'qualified_exit_crosses_original_buy_bin',count(*),sum(crossed_bin_exit_quantity_micro)
                  FROM {buys} WHERE crossed_bin_exit_quantity_micro>0
        UNION ALL SELECT 'qualified_hedge_crosses_original_buy_price_half',count(*),sum(crossed_price_hedge_quantity_micro)::DOUBLE
                  FROM {buys} WHERE crossed_price_hedge_quantity_micro>0""")
    return names


def enrich_buy_attribution(
    con: duckdb.DuckDBPyConnection,
    buy_tags: str,
    market_tokens: str,
    market_clocks: str,
    block_timestamps: str,
    wallet_flags: str,
    *,
    output: str="profit_taking_buyer_executions",
) -> str:
    """Attach resolved outcomes and exact timing only after BUY tags are fixed.

    All original BUYs, including pregame/postgame, zero/one prices and flagged
    wallets, are retained. The caller chooses focal windows and samples later.
    Absence from the nonhuman flag list means unflagged, not verified human;
    ``wallet_flag_present`` preserves this distinction. Missing/conflicting
    token outcomes, exact blocks, or accepted clocks fail closed.
    """
    buys,tokens,clocks,blocks,flags,target=(_identifier(name) for name in (
        buy_tags,market_tokens,market_clocks,block_timestamps,wallet_flags,output))
    _reject_existing(con,[output])
    for relation,columns,label in (
        (buy_tags,("execution_id","market_id","token_id","wallet","side","is_synthetic","price",
            "block_number","gross_quantity_micro","gross_cash_micro","exit_fraction","qualified_hedge_fraction",
            "unmatched_seller_acquisition_fraction","history_status"),"Classified original BUYs"),
        (market_tokens,("token_id","market_id","complement_token_id","won"),"Canonical binary resolutions"),
        (market_clocks,("market_id","sport","event_id","market_date","actual_start_utc","actual_end_utc"),"Accepted sports clocks"),
        (block_timestamps,("block_number","timestamp"),"Exact block timestamps"),
        (wallet_flags,("proxyWallet","is_nonhuman"),"Nonhuman wallet flags"),
    ):
        require_columns(con,relation,columns,label)
    _require_zero(con,f"SELECT count(*) FROM (SELECT execution_id FROM {buys} GROUP BY 1 HAVING count(*)<>1)",
                  "Classified BUYs are not unique original executions")
    _require_zero(con,f"SELECT count(*) FROM {buys} WHERE side IS DISTINCT FROM 'BUY' OR is_synthetic IS DISTINCT FROM FALSE",
                  "Metadata may only enrich original actual BUYs")
    for relation,key in ((tokens,"token_id"),(clocks,"market_id")):
        _require_zero(con,f"SELECT count(*) FROM (SELECT {key} FROM {relation} GROUP BY 1 HAVING count(*)<>1)",
                      "Canonical outcomes and clocks must have unique keys")
    _require_zero(con,f"""SELECT count(*) FROM {tokens} t LEFT JOIN {tokens} c ON c.token_id=t.complement_token_id
        WHERE t.token_id IS NULL OR t.market_id IS NULL OR t.won IS NULL OR c.token_id IS NULL
        OR t.token_id=c.token_id OR c.complement_token_id<>t.token_id OR c.market_id<>t.market_id
        OR try_cast(t.won AS DOUBLE) NOT IN (0,1) OR try_cast(c.won AS DOUBLE) NOT IN (0,1)
        OR try_cast(t.won AS DOUBLE)+try_cast(c.won AS DOUBLE)<>1""","Invalid canonical binary outcome spine")
    _require_zero(con,f"""SELECT count(*) FROM (SELECT market_id FROM {tokens}
        GROUP BY 1 HAVING count(*)<>2 OR sum(won::DOUBLE)<>1)""","Each market needs exactly two complementary outcomes")
    _require_zero(con,f"""SELECT count(*) FROM {clocks} WHERE market_id IS NULL OR sport IS NULL OR event_id IS NULL
        OR market_date IS NULL OR actual_start_utc IS NULL OR actual_end_utc IS NULL OR actual_end_utc<=actual_start_utc""",
        "Invalid accepted clocks")
    _require_zero(con,f"SELECT count(*) FROM {buys} b ANTI JOIN {tokens} t ON b.token_id=t.token_id AND b.market_id=t.market_id",
                  "BUY tokens do not reconcile to canonical resolved markets")
    _require_zero(con,f"SELECT count(*) FROM {buys} ANTI JOIN {clocks} USING(market_id)","BUY markets lack accepted clocks")
    _require_zero(con,f"""SELECT count(*) FROM (SELECT b.block_number FROM {blocks} b
        JOIN (SELECT DISTINCT block_number FROM {buys}) s USING(block_number)
        GROUP BY b.block_number HAVING count(*)<>1 OR count(timestamp)<>1 OR min(timestamp)<0)""",
        "Exact block timestamps are missing or duplicated")
    _require_zero(con,f"SELECT count(*) FROM {buys} ANTI JOIN {blocks} USING(block_number)","BUY blocks lack exact timestamps")
    _require_zero(con,f"""SELECT count(*) FROM {flags} WHERE proxyWallet IS NULL OR trim(proxyWallet)='' OR is_nonhuman IS NULL""",
                  "Wallet flags have missing identities or labels")
    _require_zero(con,f"""SELECT count(*) FROM (SELECT lower(proxyWallet) FROM {flags}
        GROUP BY 1 HAVING count(DISTINCT is_nonhuman)<>1)""","Conflicting normalized wallet flags")
    con.execute(f"""CREATE TEMP VIEW {target} AS SELECT b.* EXCLUDE(hedge_fraction),
        b.qualified_hedge_fraction hedge_fraction,
        b.unmatched_seller_acquisition_fraction unknown_history_fraction,
        b.gross_cash_micro/1e6::DOUBLE usdc,t.won::DOUBLE won,(t.won::DOUBLE-b.price)::DOUBLE residual,
        t.complement_token_id,m.sport,m.event_id,m.sport||':'||m.event_id event_cluster,m.market_date,
        x.timestamp::BIGINT trade_timestamp,to_timestamp(x.timestamp) trade_timestamp_utc,
        timezone('UTC',to_timestamp(x.timestamp))::DATE trade_day,m.actual_start_utc,m.actual_end_utc,
        ((x.timestamp-epoch(m.actual_start_utc))/(epoch(m.actual_end_utc)-epoch(m.actual_start_utc)))::DOUBLE realized_time,
        (epoch(m.actual_end_utc)-x.timestamp)::DOUBLE seconds_to_end,
        coalesce(f.is_nonhuman,false)::BOOLEAN is_nonhuman,(f.wallet IS NOT NULL)::BOOLEAN wallet_flag_present
        FROM {buys} b JOIN {tokens} t ON t.token_id=b.token_id AND t.market_id=b.market_id
        JOIN {clocks} m ON m.market_id=b.market_id JOIN {blocks} x USING(block_number)
        LEFT JOIN (SELECT DISTINCT lower(proxyWallet) wallet,is_nonhuman FROM {flags}) f ON f.wallet=b.wallet""")
    validate_executions(con,output)
    if con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]!=con.execute(f"SELECT count(*) FROM {buys}").fetchone()[0]:
        raise ValueError("Metadata enrichment changed original BUY support")
    return output
