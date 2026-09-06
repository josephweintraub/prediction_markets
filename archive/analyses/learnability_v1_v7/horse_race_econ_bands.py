"""Economics v2: price-band-conditional dollar returns per dimension group.

FLB's transfer is price-conditional: in a biased group longshot buyers lose
and favorite buyers win. Bands: longshot p<=0.30, mid, favorite p>=0.70.
Mature window, same estimation sample as horse_race_v1.
"""
import duckdb
import pandas as pd

TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
OUT = "/mnt/data/learnability/output/horse_race_v1_econ_bands.parquet"

con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")

df = con.execute(f"""
    WITH d AS (
        SELECT condition_id,
               (novelty_vint_decile = 1)                                AS nov_tail,
               anchor_class IN ('data_feed','official_scorer')          AS anchored,
               NTILE(5) OVER (ORDER BY usd_full)                        AS liq_q,
               NTILE(5) OVER (ORDER BY life_d)                          AS life_q
        FROM read_parquet('{DIMS}')
        WHERE n_trades_full >= 1 AND sim_k25_x IS NOT NULL
          AND cluster_k200 IS NOT NULL AND life_d IS NOT NULL
          AND vintage_year IS NOT NULL
    )
    SELECT
        CASE WHEN t.price <= 0.30 THEN 'a_longshot(p<=.30)'
             WHEN t.price >= 0.70 THEN 'c_favorite(p>=.70)'
             ELSE 'b_mid' END                                           AS band,
        d.nov_tail, d.anchored, d.liq_q, d.life_q,
        count(*)                                                        AS n,
        sum(t.usdc)                                                     AS usd,
        sum(t.usdc * (t.won - t.price)) / sum(t.usdc)                   AS dol_ret,
        sum(t.usdc * (t.won - t.price))                                 AS pnl
    FROM read_parquet('{TRADES_INT}', hive_partitioning=1) t
    JOIN d ON t.market_id = d.condition_id
    WHERE t.pos BETWEEN 0.25 AND 0.80
    GROUP BY 1, 2, 3, 4, 5
""").fetchdf()
df.to_parquet(OUT)

def agg(group_expr, name):
    g = (df.assign(grp=group_expr)
           .groupby(["grp", "band"], observed=True)
           .apply(lambda s: pd.Series({
               "usd": s["usd"].sum(),
               "dol_ret": s["pnl"].sum() / s["usd"].sum(),
               "pnl": s["pnl"].sum()}), include_groups=False)
           .reset_index())
    g.insert(0, "dim", name)
    return g

out = pd.concat([
    agg(df["nov_tail"].map({True: "tail", False: "rest"}), "novelty"),
    agg(df["anchored"].map({True: "anchored", False: "judgment"}), "anchor"),
    agg("liq_q" + df["liq_q"].astype(str), "liquidity"),
    agg("life_q" + df["life_q"].astype(str), "lifetime"),
], ignore_index=True)
out.to_parquet("/mnt/data/learnability/output/horse_race_v1_econ_bands_agg.parquet")
print(out.to_string())
