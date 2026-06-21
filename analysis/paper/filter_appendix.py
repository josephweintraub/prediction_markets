"""Appendix table justifying the (p >= 0.999 AND last 5% of lifetime) filter.

Three things to show (per Kaushik's email):
  1. We're not filtering out too much volume
  2. The excluded trades sit on contracts that look "already ended"
     — i.e. the price at the 70/80/90th percentile of those contracts'
       lifetimes is already very high.
  3. The included trades are on contracts that look like real extreme favorites
     — i.e. their price at the 70/80/90th percentile is more modest.
"""
import duckdb
import pandas as pd
from pathlib import Path

OUT = Path("/home/ubuntu/analysis_final/tables/postevent")
OUT.mkdir(parents=True, exist_ok=True)

TRADES = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
RESOLUTIONS = "/home/ubuntu/pipeline/output/market_resolutions.parquet"

con = duckdb.connect()
con.execute("SET memory_limit='48GB'")
con.execute("SET threads=16")
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET preserve_insertion_order=false")

# Restrict to BUY trades on resolved markets (consistent with FLB analysis)
con.execute(f"""
CREATE TEMP VIEW t_raw AS
SELECT t.timestamp, t.price, t.usdcSize, t.conditionId, t.outcome, t.eventSlug,
       CASE WHEN t.outcome = r.winning_outcome THEN 1 ELSE 0 END AS won
FROM read_parquet('{TRADES}') t
JOIN read_parquet('{RESOLUTIONS}') r USING (conditionId)
WHERE t.side = 'BUY'
""")

# Market lifetime + lifecycle position per trade
con.execute("""
CREATE TEMP TABLE mkt_life AS
SELECT conditionId, MIN(timestamp) AS t_min, MAX(timestamp) AS t_max
FROM t_raw GROUP BY conditionId HAVING MAX(timestamp) > MIN(timestamp)
""")
con.execute("""
CREATE TEMP VIEW t AS
SELECT tr.*, (tr.timestamp - m.t_min) * 1.0 / NULLIF(m.t_max - m.t_min, 0) AS pos
FROM t_raw tr JOIN mkt_life m USING (conditionId)
""")

# Definition of the filter
FILTER = "((price >= 0.999 OR price <= 0.001) AND pos >= 0.95)"

# -----------------------------------------------------------------
# Part 1: Top-line — how much is removed?
# -----------------------------------------------------------------
print("=" * 78)
print("PART 1: Top-line volume / trade removal")
print("=" * 78)

p1 = con.execute(f"""
SELECT
  COUNT(*)                                AS n_all,
  SUM(usdcSize)                           AS vol_all,
  SUM(CASE WHEN {FILTER} THEN 1 ELSE 0 END)                AS n_drop,
  SUM(CASE WHEN {FILTER} THEN usdcSize ELSE 0 END)         AS vol_drop,
  COUNT(DISTINCT conditionId)             AS n_contracts_all,
  COUNT(DISTINCT CASE WHEN {FILTER} THEN conditionId END)  AS n_contracts_touched
FROM t
""").fetchone()
n_all, vol_all, n_drop, vol_drop, n_c_all, n_c_touched = p1
p1_table = pd.DataFrame({
    "metric": ["trades", "volume_usdc", "unique_contracts"],
    "total":  [n_all, vol_all, n_c_all],
    "dropped":[n_drop, vol_drop, n_c_touched],
    "pct":    [100*n_drop/n_all, 100*vol_drop/vol_all, 100*n_c_touched/n_c_all],
})
print(p1_table.to_string(index=False))
p1_table.to_csv(OUT / "appendix_part1_topline.csv", index=False)

# Same breakdown by category
print("\n--- by category ---")
p1c = con.execute(f"""
WITH cat AS (
  SELECT *,
    CASE
      WHEN eventSlug LIKE 'nba-%' OR eventSlug LIKE 'nfl-%'
        OR eventSlug LIKE 'mlb-%' OR eventSlug LIKE 'nhl-%'
        OR eventSlug LIKE 'ncaab-%' OR eventSlug LIKE 'ncaaf-%'
        OR eventSlug LIKE '%-spread-%' OR eventSlug LIKE '%-moneyline%'
        THEN 'sports'
      WHEN eventSlug LIKE '%btc-updown-%' OR eventSlug LIKE '%eth-updown-%'
        OR eventSlug LIKE '%-up-or-down-%'
        THEN 'crypto up/down (intraday)'
      WHEN eventSlug LIKE '%btc%' OR eventSlug LIKE '%bitcoin%'
        OR eventSlug LIKE '%eth%' OR eventSlug LIKE '%ethereum%'
        THEN 'crypto (price levels)'
      WHEN eventSlug LIKE '%election%' OR eventSlug LIKE '%senate%'
        OR eventSlug LIKE '%president%' OR eventSlug LIKE '%primary%'
        OR eventSlug LIKE '%mayor%' THEN 'politics'
      WHEN eventSlug LIKE '%fed-%' OR eventSlug LIKE '%fomc%'
        THEN 'macro/finance'
      ELSE 'other'
    END AS category
  FROM t
  WHERE eventSlug IS NOT NULL AND eventSlug <> ''
)
SELECT category,
  COUNT(*)         AS n_trades,
  SUM(usdcSize)    AS volume,
  SUM(CASE WHEN {FILTER} THEN 1 ELSE 0 END)              AS n_dropped,
  SUM(CASE WHEN {FILTER} THEN usdcSize ELSE 0 END)       AS vol_dropped
FROM cat
GROUP BY category ORDER BY n_trades DESC
""").fetchdf()
p1c["pct_trades_dropped"] = 100 * p1c["n_dropped"] / p1c["n_trades"]
p1c["pct_volume_dropped"] = 100 * p1c["vol_dropped"] / p1c["volume"]
print(p1c.to_string(index=False))
p1c.to_csv(OUT / "appendix_part1_topline_by_category.csv", index=False)

# -----------------------------------------------------------------
# Part 2 & 3: Contract-level price profile
# Group A = contracts with >=1 trade matching filter ("EXCLUDED" contracts)
# Group B = contracts with 0 trades matching filter ("INCLUDED" contracts)
# For each group, compute median (across contracts) of VWAP at lifecycle 70%, 80%, 90%
# -----------------------------------------------------------------
print("\n" + "=" * 78)
print("PART 2: Price profile at 70/80/90% lifetime per contract")
print("=" * 78)

# Per-contract VWAP at each lifecycle decile
con.execute(f"""
CREATE TEMP TABLE per_contract_profile AS
WITH binned AS (
  SELECT conditionId, usdcSize, price,
    CAST(FLOOR(pos * 10) AS INTEGER) AS dec_bin
  FROM t
  WHERE pos IS NOT NULL AND pos BETWEEN 0 AND 0.999999
)
SELECT
  conditionId,
  AVG(CASE WHEN dec_bin = 5 THEN price END) AS p_at_50,
  AVG(CASE WHEN dec_bin = 6 THEN price END) AS p_at_60,
  AVG(CASE WHEN dec_bin = 7 THEN price END) AS p_at_70,
  AVG(CASE WHEN dec_bin = 8 THEN price END) AS p_at_80,
  AVG(CASE WHEN dec_bin = 9 THEN price END) AS p_at_90
FROM binned
GROUP BY conditionId
""")

con.execute(f"""
CREATE TEMP TABLE contract_groups AS
SELECT conditionId,
       SUM(CASE WHEN {FILTER} THEN 1 ELSE 0 END) > 0 AS had_dropped_trade,
       SUM(CASE WHEN {FILTER} THEN usdcSize ELSE 0 END) AS vol_dropped,
       SUM(usdcSize) AS vol_total
FROM t GROUP BY conditionId
""")

# Group A: had dropped trades. Group B: did not.
print("\n--- median, p25, p75 contract-level price at each lifecycle decile ---")
profile = con.execute("""
WITH joined AS (
  SELECT g.had_dropped_trade, p.*
  FROM contract_groups g JOIN per_contract_profile p USING (conditionId)
)
SELECT had_dropped_trade,
  COUNT(*) AS n_contracts,
  MEDIAN(p_at_50) AS med_p50, MEDIAN(p_at_60) AS med_p60,
  MEDIAN(p_at_70) AS med_p70, MEDIAN(p_at_80) AS med_p80,
  MEDIAN(p_at_90) AS med_p90,
  QUANTILE_CONT(p_at_70, 0.25) AS p25_p70,
  QUANTILE_CONT(p_at_70, 0.75) AS p75_p70,
  QUANTILE_CONT(p_at_80, 0.25) AS p25_p80,
  QUANTILE_CONT(p_at_80, 0.75) AS p75_p80
FROM joined
WHERE p_at_50 IS NOT NULL AND p_at_70 IS NOT NULL
GROUP BY had_dropped_trade
""").fetchdf()
print(profile.to_string(index=False))
profile.to_csv(OUT / "appendix_part2_price_profile.csv", index=False)

# Same with how-extreme-the-favorite framing: separate "had dropped trade" further
# by whether the contract resolved YES (winning side traded at p>=0.999 late) or NO
print("\n--- same split but adding winning vs losing side of the dropped trades ---")
profile2 = con.execute(f"""
WITH dropped_side AS (
  SELECT conditionId,
         SUM(CASE WHEN {FILTER} AND won = 1 THEN usdcSize ELSE 0 END) AS vol_dropped_winning,
         SUM(CASE WHEN {FILTER} AND won = 0 THEN usdcSize ELSE 0 END) AS vol_dropped_losing
  FROM t GROUP BY conditionId
), labelled AS (
  SELECT g.conditionId,
    CASE
      WHEN NOT g.had_dropped_trade THEN 'no dropped trades'
      WHEN ds.vol_dropped_winning > ds.vol_dropped_losing THEN 'dropped: mostly winning side'
      ELSE 'dropped: mostly losing side'
    END AS group_lbl
  FROM contract_groups g JOIN dropped_side ds USING (conditionId)
)
SELECT l.group_lbl,
  COUNT(*) AS n_contracts,
  MEDIAN(p.p_at_50) AS med_p50,
  MEDIAN(p.p_at_70) AS med_p70,
  MEDIAN(p.p_at_80) AS med_p80,
  MEDIAN(p.p_at_90) AS med_p90
FROM labelled l JOIN per_contract_profile p USING (conditionId)
WHERE p.p_at_50 IS NOT NULL
GROUP BY l.group_lbl
""").fetchdf()
print(profile2.to_string(index=False))
profile2.to_csv(OUT / "appendix_part2_price_profile_by_side.csv", index=False)

# -----------------------------------------------------------------
# Part 3: Sample contracts on each side
# -----------------------------------------------------------------
print("\n" + "=" * 78)
print("PART 3: Example contracts on each side")
print("=" * 78)

print("\n--- Top 8 contracts with most dropped trades (should look post-event) ---")
ex_drop = con.execute(f"""
SELECT t.conditionId,
       ANY_VALUE(t.eventSlug) AS event_slug,
       SUM(CASE WHEN {FILTER} THEN 1 ELSE 0 END) AS n_dropped,
       SUM(CASE WHEN {FILTER} THEN usdcSize ELSE 0 END) AS vol_dropped,
       AVG(CASE WHEN pos BETWEEN 0.69 AND 0.81 THEN price END) AS price_at_75pct
FROM t GROUP BY t.conditionId
HAVING n_dropped > 0
ORDER BY n_dropped DESC LIMIT 8
""").fetchdf()
print(ex_drop.to_string(index=False))

print("\n--- 8 sampled contracts with NO dropped trades (should NOT look post-event by 75% lifetime) ---")
ex_keep = con.execute(f"""
SELECT t.conditionId,
       ANY_VALUE(t.eventSlug) AS event_slug,
       SUM(t.usdcSize) AS vol_total,
       AVG(CASE WHEN pos BETWEEN 0.69 AND 0.81 THEN price END) AS price_at_75pct
FROM t GROUP BY t.conditionId
HAVING SUM(CASE WHEN {FILTER} THEN 1 ELSE 0 END) = 0
   AND SUM(t.usdcSize) > 50000
USING SAMPLE 8 ROWS
""").fetchdf()
print(ex_keep.to_string(index=False))
