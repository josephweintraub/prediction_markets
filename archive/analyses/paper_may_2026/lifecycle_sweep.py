"""Sweep the lifecycle threshold for the filter.

Filter: (price >= 0.999 OR price <= 0.001) AND pos >= THRESHOLD.

Vary THRESHOLD = 0.90, 0.95, 0.97, 0.99, 0.995, 0.999 and compare:
  - % trades dropped
  - % volume dropped
  - % contracts touched
  - FLB top-decile spread (baseline vs filter applied)
"""
import duckdb
import pandas as pd
from pathlib import Path

OUT = Path("/mnt/data/analysis_output/paper/tables/postevent")

TRADES = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
RESOLUTIONS = "/home/ubuntu/pipeline/output/market_resolutions.parquet"

con = duckdb.connect()
con.execute("SET memory_limit='48GB'")
con.execute("SET threads=16")
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET preserve_insertion_order=false")

con.execute(f"""
CREATE TEMP VIEW t_raw AS
SELECT t.timestamp, t.price, t.usdcSize, t.conditionId, t.outcome,
       CASE WHEN t.outcome = r.winning_outcome THEN 1 ELSE 0 END AS won
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
CREATE TEMP TABLE t AS
SELECT tr.*, (tr.timestamp - m.t_min) * 1.0 / NULLIF(m.t_max - m.t_min, 0) AS pos
FROM t_raw tr JOIN mkt_life m USING (conditionId)
""")

# Pre-compute totals once
totals = con.execute("""
SELECT COUNT(*), SUM(usdcSize), COUNT(DISTINCT conditionId)
FROM t
""").fetchone()
n_all, vol_all, c_all = totals

# Baseline top-decile spread (no filter)
baseline = con.execute("""
SELECT COUNT(*), AVG(price), AVG(won), AVG(won) - AVG(price)
FROM t WHERE price >= 0.9 AND price < 1.0
""").fetchone()
print(f"Baseline (no filter): top decile n={baseline[0]:,}  "
      f"mean_price={baseline[1]:.4f}  mean_outcome={baseline[2]:.4f}  "
      f"spread={baseline[3]:+.4f}")
print()

rows = []
for thresh in [0.90, 0.95, 0.97, 0.99, 0.995, 0.999]:
    r1 = con.execute(f"""
    SELECT
      SUM(CASE WHEN (price >= 0.999 OR price <= 0.001) AND pos >= {thresh}
               THEN 1 ELSE 0 END) AS n_drop,
      SUM(CASE WHEN (price >= 0.999 OR price <= 0.001) AND pos >= {thresh}
               THEN usdcSize ELSE 0 END) AS vol_drop,
      COUNT(DISTINCT CASE WHEN (price >= 0.999 OR price <= 0.001) AND pos >= {thresh}
                          THEN conditionId END) AS c_touched
    FROM t
    """).fetchone()
    n_drop, vol_drop, c_touched = r1

    r2 = con.execute(f"""
    SELECT COUNT(*), AVG(price), AVG(won), AVG(won) - AVG(price)
    FROM t
    WHERE price >= 0.9 AND price < 1.0
      AND NOT ((price >= 0.999 OR price <= 0.001) AND pos >= {thresh})
    """).fetchone()
    n_topdec, mp, mo, sp = r2

    rows.append({
        "lifecycle_threshold (last X% of life)": f"{int((1-thresh)*100*10)/10}%",
        "pct_trades_dropped":  100*n_drop/n_all,
        "pct_volume_dropped":  100*vol_drop/vol_all,
        "pct_contracts_touched": 100*c_touched/c_all,
        "top-decile spread after filter": sp,
    })

df = pd.DataFrame(rows)
print(df.to_string(index=False))
df.to_csv(OUT / "appendix_lifecycle_sweep.csv", index=False)
