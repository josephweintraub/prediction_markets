"""Data exploration on EC2 — investigate two v5 anomalies:

1. Sports `sports_data` info-type slice: nearly every decile has POSITIVE cal_error
   (win rate > implied prob), but D10-D1 spread is null because both ends shift
   the same direction. Why?

2. Tech primary_category: classic FLB pattern appears only in the 80-100% closing
   window, not in the 25-80% mature window. Why?

Approach (all on EC2):
- A. Sports per-event-template calibration (which sub-sport drives the elevation?)
- B. Sports BUY-only vs SELL-only calibration (selection effect vs mispricing?)
- C. Sports BUY-side at the same deciles split by underdog (price<0.5) vs favorite (price>0.5)
- D. Tech contract overlap between 25-80% and 80-100% windows (composition vs behavior?)
- E. Tech 80-100%-only contracts: when do they enter the dataset?

Output is small summary parquets pulled back to local.
"""
import sys, os, json, time
sys.path.insert(0, "/home/ubuntu")
sys.path.insert(0, "/home/ubuntu/pipeline/analysis")
sys.path.insert(0, "/home/ubuntu/learnability")
from pathlib import Path
import numpy as np
import pandas as pd

from config import OUTPUT_DIR as PIPELINE_OUTPUT_DIR
from data_loader import get_connection

OUT = Path("/mnt/data/learnability/output")
V5_DIMS = OUT / "phase1_v5_contract_dimensions.parquet"
POLYMARKET_START_TIMESTAMP = 1590969600


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    t_total = time.time()
    log("=== Data exploration: Sports anomaly + Tech closing anomaly ===")

    con = get_connection(memory_limit="200GB", threads=16, force_new=True)
    con.execute("SET temp_directory = '/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size = '400GiB'")

    WF_CACHE = Path("/mnt/data/learnability/cache/wallet_flags.parquet")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE wallet_flags AS
        SELECT * FROM read_parquet('{WF_CACHE}')
    """)
    # Same trade view as v5 (up/down excluded, bots out, BUY/SELL both)
    con.execute("DROP VIEW IF EXISTS trades")
    con.execute(f"""
        CREATE VIEW trades AS
        SELECT *, usdcSize / NULLIF(price, 0) AS size
        FROM trades_raw
        WHERE timestamp >= {POLYMARKET_START_TIMESTAMP}
          AND proxyWallet NOT IN (SELECT proxyWallet FROM wallet_flags WHERE is_nonhuman = 1)
          AND eventSlug NOT LIKE '%updown%'
          AND eventSlug NOT LIKE '%up-or-down%'
    """)

    ENRICHED = Path("/home/ubuntu/pipeline/output/market_resolutions_enriched.parquet")
    res_enriched = pd.read_parquet(ENRICHED)
    con.register("_res_enriched_df", res_enriched)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _closed_markets AS
        SELECT conditionId, winning_outcome
        FROM _res_enriched_df
        WHERE winning_outcome IS NOT NULL
    """)
    con.unregister("_res_enriched_df")

    df = pd.read_parquet(V5_DIMS, columns=[
        "token_id", "condition_id", "event_template", "event_slug",
        "dim_primary_category", "dim_info_type_supergroup", "dim_contract_horizon",
        "n_trades", "dollar_volume", "first_ts", "last_ts",
    ])
    log(f"Loaded {len(df):,} contracts × {len(df.columns)} cols")

    # Register full contract dims
    con.register("_cd_df", df)
    con.execute("CREATE OR REPLACE TEMP TABLE _cd AS SELECT * FROM _cd_df")
    con.unregister("_cd_df")

    # ============================================================
    # A. Sports per-event-template calibration (mature window 25-80%)
    # ============================================================
    log("\n=== A. Sports per-event-template calibration (mature 25-80%) ===")
    # Top 15 templates in sports_data by total contract trades
    top_templates = (
        df[df["dim_info_type_supergroup"] == "sports_data"]
        .groupby("event_template")["n_trades"].sum()
        .sort_values(ascending=False).head(15).index.tolist()
    )
    log(f"Top 15 sports templates: {top_templates}")

    # SQL: per-template per-decile win rate + n in 25-80% window
    placeholders = ",".join([f"'{t}'" for t in top_templates])
    sql_a = f"""
    WITH mkt_life AS (
        SELECT conditionId,
               MIN(timestamp) AS mkt_start,
               GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS mkt_duration
        FROM trades
        WHERE side='BUY'
        GROUP BY conditionId
    ),
    td AS (
        SELECT
            cd.event_template AS tmpl,
            LEAST(FLOOR(t.price * 10)::INT, 9) + 1 AS decile,
            t.price,
            CAST(t.outcome = c.winning_outcome AS INT) AS won
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN mkt_life ml       ON t.conditionId = ml.conditionId
        INNER JOIN _cd cd            ON t.conditionId = cd.token_id
        WHERE t.side = 'BUY'
          AND t.price > 0.01 AND t.price < 0.99
          AND (t.timestamp - ml.mkt_start)::FLOAT / ml.mkt_duration BETWEEN 0.25 AND 0.80
          AND cd.event_template IN ({placeholders})
    )
    SELECT tmpl, decile,
           AVG(price) AS mean_price,
           AVG(won::FLOAT) AS win_rate,
           AVG(won::FLOAT) - AVG(price) AS cal_error,
           COUNT(*) AS n
    FROM td
    GROUP BY tmpl, decile
    """
    t0 = time.time()
    res_a = con.execute(sql_a).fetchdf()
    log(f"  done in {time.time()-t0:.1f}s, {len(res_a):,} cells")
    res_a.to_parquet(OUT / "data_explore_sports_by_template.parquet", index=False)

    # ============================================================
    # B. Sports BUY vs SELL calibration (mature window 25-80%)
    # ============================================================
    log("\n=== B. Sports BUY vs SELL calibration ===")
    sql_b = """
    WITH mkt_life AS (
        SELECT conditionId,
               MIN(timestamp) AS mkt_start,
               GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS mkt_duration
        FROM trades
        WHERE side IN ('BUY','SELL')
        GROUP BY conditionId
    ),
    td AS (
        SELECT
            t.side,
            LEAST(FLOOR(t.price * 10)::INT, 9) + 1 AS decile,
            t.price,
            CAST(t.outcome = c.winning_outcome AS INT) AS outcome_match
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN mkt_life ml       ON t.conditionId = ml.conditionId
        INNER JOIN _cd cd            ON t.conditionId = cd.token_id
        WHERE t.price > 0.01 AND t.price < 0.99
          AND (t.timestamp - ml.mkt_start)::FLOAT / ml.mkt_duration BETWEEN 0.25 AND 0.80
          AND cd.dim_info_type_supergroup = 'sports_data'
    )
    SELECT side, decile,
           AVG(price) AS mean_price,
           AVG(outcome_match::FLOAT) AS yes_outcome_rate,
           -- BUY wins when outcome matches; cal_error_buy = outcome_match - price
           CASE WHEN side='BUY' THEN AVG(outcome_match::FLOAT) - AVG(price)
                ELSE AVG(1 - outcome_match::FLOAT) - AVG(1 - price)  -- SELL wins when outcome doesn't match; implied = 1-price
                END AS cal_error,
           COUNT(*) AS n
    FROM td
    GROUP BY side, decile
    """
    t0 = time.time()
    res_b = con.execute(sql_b).fetchdf()
    log(f"  done in {time.time()-t0:.1f}s")
    res_b.to_parquet(OUT / "data_explore_sports_buy_sell.parquet", index=False)
    print(res_b.to_string(index=False))

    # ============================================================
    # C. Same but for Politics (control comparison)
    # ============================================================
    log("\n=== C. Politics BUY vs SELL calibration (control) ===")
    sql_c = """
    WITH mkt_life AS (
        SELECT conditionId,
               MIN(timestamp) AS mkt_start,
               GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS mkt_duration
        FROM trades WHERE side IN ('BUY','SELL') GROUP BY conditionId
    ),
    td AS (
        SELECT
            t.side,
            LEAST(FLOOR(t.price * 10)::INT, 9) + 1 AS decile,
            t.price,
            CAST(t.outcome = c.winning_outcome AS INT) AS outcome_match
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN mkt_life ml       ON t.conditionId = ml.conditionId
        INNER JOIN _cd cd            ON t.conditionId = cd.token_id
        WHERE t.price > 0.01 AND t.price < 0.99
          AND (t.timestamp - ml.mkt_start)::FLOAT / ml.mkt_duration BETWEEN 0.25 AND 0.80
          AND cd.dim_primary_category = 'Politics'
    )
    SELECT side, decile,
           AVG(price) AS mean_price,
           AVG(outcome_match::FLOAT) AS yes_outcome_rate,
           CASE WHEN side='BUY' THEN AVG(outcome_match::FLOAT) - AVG(price)
                ELSE AVG(1 - outcome_match::FLOAT) - AVG(1 - price)
                END AS cal_error,
           COUNT(*) AS n
    FROM td
    GROUP BY side, decile
    """
    t0 = time.time()
    res_c = con.execute(sql_c).fetchdf()
    log(f"  done in {time.time()-t0:.1f}s")
    res_c.to_parquet(OUT / "data_explore_politics_buy_sell.parquet", index=False)
    print(res_c.to_string(index=False))

    # ============================================================
    # D. Tech contract overlap: 25-80% vs 80-100%
    # ============================================================
    log("\n=== D. Tech contract overlap across windows ===")
    sql_d = """
    WITH mkt_life AS (
        SELECT conditionId,
               MIN(timestamp) AS mkt_start,
               GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS mkt_duration
        FROM trades WHERE side='BUY' GROUP BY conditionId
    ),
    contracts_in_window AS (
        SELECT
            t.conditionId,
            (t.timestamp - ml.mkt_start)::FLOAT / ml.mkt_duration AS lifecycle_pos
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN mkt_life ml ON t.conditionId = ml.conditionId
        INNER JOIN _cd cd ON t.conditionId = cd.token_id
        WHERE t.side='BUY' AND t.price > 0.01 AND t.price < 0.99
          AND cd.dim_primary_category = 'Tech'
    ),
    contracts_25_80 AS (
        SELECT DISTINCT conditionId FROM contracts_in_window
        WHERE lifecycle_pos BETWEEN 0.25 AND 0.80
    ),
    contracts_80_100 AS (
        SELECT DISTINCT conditionId FROM contracts_in_window
        WHERE lifecycle_pos BETWEEN 0.80 AND 1.00
    )
    SELECT
        (SELECT COUNT(*) FROM contracts_25_80) AS n_in_25_80,
        (SELECT COUNT(*) FROM contracts_80_100) AS n_in_80_100,
        (SELECT COUNT(DISTINCT conditionId) FROM contracts_25_80
         WHERE conditionId IN (SELECT conditionId FROM contracts_80_100)) AS n_both,
        (SELECT COUNT(*) FROM contracts_25_80
         WHERE conditionId NOT IN (SELECT conditionId FROM contracts_80_100)) AS n_only_25_80,
        (SELECT COUNT(*) FROM contracts_80_100
         WHERE conditionId NOT IN (SELECT conditionId FROM contracts_25_80)) AS n_only_80_100
    """
    t0 = time.time()
    res_d = con.execute(sql_d).fetchdf()
    log(f"  done in {time.time()-t0:.1f}s")
    print(res_d.to_string(index=False))
    res_d.to_parquet(OUT / "data_explore_tech_overlap.parquet", index=False)

    # ============================================================
    # E. Tech 80-100%-only contracts: top by trade count, what are they?
    # ============================================================
    log("\n=== E. Tech 80-100%-only contracts ===")
    sql_e = """
    WITH mkt_life AS (
        SELECT conditionId,
               MIN(timestamp) AS mkt_start,
               GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS mkt_duration
        FROM trades WHERE side='BUY' GROUP BY conditionId
    ),
    by_contract AS (
        SELECT
            t.conditionId,
            COUNT(*) FILTER (WHERE (t.timestamp - ml.mkt_start)::FLOAT / ml.mkt_duration BETWEEN 0.25 AND 0.80) AS n_25_80,
            COUNT(*) FILTER (WHERE (t.timestamp - ml.mkt_start)::FLOAT / ml.mkt_duration BETWEEN 0.80 AND 1.00) AS n_80_100,
            COUNT(*) AS n_total,
            AVG(t.price) FILTER (WHERE (t.timestamp - ml.mkt_start)::FLOAT / ml.mkt_duration BETWEEN 0.80 AND 1.00) AS avg_price_80_100,
            MAX(CAST(t.outcome = c.winning_outcome AS INT)) AS won_outcome
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN mkt_life ml ON t.conditionId = ml.conditionId
        INNER JOIN _cd cd ON t.conditionId = cd.token_id
        WHERE t.side='BUY' AND t.price > 0.01 AND t.price < 0.99
          AND cd.dim_primary_category = 'Tech'
        GROUP BY t.conditionId
    )
    SELECT bc.*, cd.question, cd.event_template, cd.event_slug, cd.dim_contract_horizon
    FROM by_contract bc
    LEFT JOIN _cd cd ON bc.conditionId = cd.token_id
    ORDER BY bc.n_80_100 DESC
    LIMIT 30
    """
    t0 = time.time()
    res_e = con.execute(sql_e).fetchdf()
    log(f"  done in {time.time()-t0:.1f}s")
    print(res_e[["question","event_template","n_25_80","n_80_100","avg_price_80_100","won_outcome","dim_contract_horizon"]].to_string(index=False))
    res_e.to_parquet(OUT / "data_explore_tech_top_contracts.parquet", index=False)

    log(f"\nALL DONE in {(time.time()-t_total)/60:.1f} min")
    log("Outputs:")
    for p in sorted(OUT.glob("data_explore_*.parquet")):
        sz = p.stat().st_size
        s = f"{sz/1e6:.1f} MB" if sz > 1e6 else f"{sz/1e3:.0f} KB"
        log(f"  {p.name} {s}")


if __name__ == "__main__":
    main()
