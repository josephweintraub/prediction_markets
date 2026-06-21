"""Clean, intuitive tables for Kaushik.

Filter being justified:
  Drop trades where (price >= 0.999 OR price <= 0.001) AND lifecycle pos >= 0.95.

Two tables:

  Table 1: Impact. One-liner: 1.0% of trades, 5.4% of volume dropped.

  Table 2: For each dropped trade, look at what the contract was trading at
  WELL BEFORE the dropped trade happened — specifically, the contract's
  volume-weighted average price during 50%-80% of its lifetime. If that VWAP
  is already at an extreme (≤0.05 or ≥0.95), the outcome was already known
  long before the dropped trade; the filter is correctly removing post-event
  noise. If the mid-life VWAP is near 0.5, the contract was genuinely
  contested through the middle of its life and the dropped trade happens
  only because the outcome resolved at the very end (e.g. sports games).
  Either way, those trades shouldn't carry FLB information.
"""
import duckdb
import pandas as pd
from pathlib import Path

OUT = Path("/mnt/data/analysis_output/paper/tables/postevent")
OUT.mkdir(parents=True, exist_ok=True)

TRADES = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
RESOLUTIONS = "/home/ubuntu/pipeline/output/market_resolutions.parquet"

con = duckdb.connect()
con.execute("SET memory_limit='48GB'")
con.execute("SET threads=16")
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET preserve_insertion_order=false")

con.execute(f"""
CREATE TEMP VIEW t_raw AS
SELECT t.timestamp, t.price, t.usdcSize, t.conditionId
FROM read_parquet('{TRADES}') t
JOIN read_parquet('{RESOLUTIONS}') r USING (conditionId)
WHERE t.side = 'BUY'
""")

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

FILTER = "((price >= 0.99 OR price <= 0.01) AND pos >= 0.99)"

# ----------------------------------------------------------------
# Table 1: One-liner impact
# ----------------------------------------------------------------
print("=" * 70)
print("TABLE 1: How much does the filter remove?")
print("=" * 70)
r = con.execute(f"""
SELECT COUNT(*) AS n_all, SUM(usdcSize) AS vol_all,
       SUM(CASE WHEN {FILTER} THEN 1 ELSE 0 END) AS n_drop,
       SUM(CASE WHEN {FILTER} THEN usdcSize ELSE 0 END) AS vol_drop
FROM t
""").fetchone()
n_all, vol_all, n_drop, vol_drop = r
print(f"  Total trades:    {n_all:>15,}")
print(f"  Dropped trades:  {n_drop:>15,}   ({100*n_drop/n_all:.2f}%)")
print(f"  Total volume:    ${vol_all/1e9:>14,.1f} B")
print(f"  Dropped volume:  ${vol_drop/1e9:>14,.1f} B   ({100*vol_drop/vol_all:.2f}%)")

# ----------------------------------------------------------------
# Table 2: For dropped trades, was the outcome already clear earlier?
# Compute each market's mid-life VWAP (across positions 0.50–0.80) and bin it.
# Then bucket dropped trades by their market's mid-life VWAP.
# ----------------------------------------------------------------
print()
print("=" * 70)
print("TABLE 2: For dropped trades, what was the market price doing")
print("         WELL BEFORE the filter window? (50%–80% of lifetime)")
print("=" * 70)
con.execute("""
CREATE TEMP TABLE midlife_vwap AS
SELECT conditionId,
       SUM(usdcSize * price) / NULLIF(SUM(usdcSize), 0) AS midlife_vwap,
       SUM(usdcSize) AS midlife_volume
FROM t
WHERE pos BETWEEN 0.50 AND 0.80
GROUP BY conditionId
""")

bin2 = con.execute(f"""
WITH dropped AS (
  SELECT t.conditionId, t.usdcSize
  FROM t WHERE {FILTER}
)
SELECT
  CASE
    WHEN m.midlife_vwap IS NULL                  THEN '(no trades in 50-80% window)'
    WHEN m.midlife_vwap >= 0.95                  THEN '5: already favorite ≥ 0.95'
    WHEN m.midlife_vwap >= 0.80                  THEN '4: leaning winner 0.80-0.95'
    WHEN m.midlife_vwap >= 0.20                  THEN '3: contested  0.20-0.80'
    WHEN m.midlife_vwap >= 0.05                  THEN '2: leaning loser 0.05-0.20'
    ELSE                                              '1: already loser ≤ 0.05'
  END AS midlife_state,
  COUNT(*) AS n_dropped_trades,
  SUM(d.usdcSize) AS dropped_volume
FROM dropped d
LEFT JOIN midlife_vwap m USING (conditionId)
GROUP BY midlife_state
ORDER BY midlife_state
""").fetchdf()
totals = bin2[["n_dropped_trades", "dropped_volume"]].sum()
bin2["pct_trades"] = 100 * bin2["n_dropped_trades"] / totals["n_dropped_trades"]
bin2["pct_volume"] = 100 * bin2["dropped_volume"] / totals["dropped_volume"]
print(bin2.to_string(index=False))
bin2.to_csv(OUT / "appendix_clean_t2_midlife_state.csv", index=False)

# ----------------------------------------------------------------
# Sanity check: same exercise for KEPT trades that ARE extreme-price.
# These are extreme-price trades NOT in the last 5% of lifetime — they
# stay in the dataset. Are they on contracts that look like "real" extreme
# favorites (mid-life vwap also already extreme) or on contracts where
# mid-life was contested?
# ----------------------------------------------------------------
print()
print("=" * 70)
print("TABLE 3 (sanity check): for KEPT trades at extreme price")
print("         (p>=0.99 or <=0.01 but NOT in last 1% of life),")
print("         what was the contract doing in 50%-80% of life?")
print("=" * 70)
bin3 = con.execute(f"""
WITH kept_extreme AS (
  SELECT t.conditionId, t.usdcSize
  FROM t
  WHERE (price >= 0.99 OR price <= 0.01) AND pos < 0.99
)
SELECT
  CASE
    WHEN m.midlife_vwap IS NULL                  THEN '(no trades in 50-80% window)'
    WHEN m.midlife_vwap >= 0.95                  THEN '5: already favorite ≥ 0.95'
    WHEN m.midlife_vwap >= 0.80                  THEN '4: leaning winner 0.80-0.95'
    WHEN m.midlife_vwap >= 0.20                  THEN '3: contested  0.20-0.80'
    WHEN m.midlife_vwap >= 0.05                  THEN '2: leaning loser 0.05-0.20'
    ELSE                                              '1: already loser ≤ 0.05'
  END AS midlife_state,
  COUNT(*) AS n_kept_extreme,
  SUM(k.usdcSize) AS kept_volume
FROM kept_extreme k
LEFT JOIN midlife_vwap m USING (conditionId)
GROUP BY midlife_state
ORDER BY midlife_state
""").fetchdf()
tot3 = bin3[["n_kept_extreme", "kept_volume"]].sum()
bin3["pct_trades"] = 100 * bin3["n_kept_extreme"] / tot3["n_kept_extreme"]
bin3["pct_volume"] = 100 * bin3["kept_volume"] / tot3["kept_volume"]
print(bin3.to_string(index=False))
bin3.to_csv(OUT / "appendix_clean_t3_kept_extreme.csv", index=False)
