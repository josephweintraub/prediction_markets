"""Cluster-SE diagnostics — justify the three-way CGM clustering choice.

What this answers:
 1. How big are the clusters?  (a single market often has 10K-1M trades →
    iid SE on within-market correlation is wildly understated)
 2. How much does each cluster dimension inflate the SE vs iid?
    (design effect deff_c = V_c / V_iid, equivalent N_eff = N / deff)
 3. Does any one cluster dimension dominate, or do we need all three?
    (compare 1-way SEs to the 3-way CGM SE)
 4. What's the intraclass correlation (ICC) of the residual within each
    dimension?  (Moulton-style: ICC ≈ (V_c - V_iid) / V_iid / (m̄ - 1)
    where m̄ is average cluster size, weighted)

Uses the same pre-aggregated `agg` table as flb_canonical.py — no need
to re-scan trades.parquet from scratch.

Output:
  tables/cluster_diagnostics.csv          — per-decile, every SE + ICC
  tables/cluster_size_distribution.csv    — cluster-size summaries
"""
import time, sys
sys.path.insert(0, "/home/ubuntu/pipeline/analysis")
import duckdb
import pandas as pd
from pathlib import Path

OUT = Path("/home/ubuntu/analysis_final/tables")
OUT.mkdir(parents=True, exist_ok=True)

TRADES = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
RESOLUTIONS = "/home/ubuntu/pipeline/output/market_resolutions.parquet"
WALLET_FLAGS = "/mnt/data/pipeline_data/wallet_flags.parquet"

con = duckdb.connect()
con.execute("SET memory_limit='180GB'")
con.execute("SET threads=16")
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET preserve_insertion_order=false")

# Rebuild the same pre-aggregated table flb_canonical.py used.
print(">>> building pre-aggregated agg (humans, post-event-filtered) ...")
t0 = time.time()
con.execute(f"""
CREATE TEMP TABLE _mkt_life AS
SELECT t.conditionId, MIN(t.timestamp) AS t_min, MAX(t.timestamp) AS t_max
FROM read_parquet('{TRADES}') t
JOIN read_parquet('{RESOLUTIONS}') r USING (conditionId)
WHERE t.side='BUY'
GROUP BY t.conditionId HAVING MAX(t.timestamp) > MIN(t.timestamp)
""")

con.execute(f"""
CREATE TEMP TABLE agg AS
SELECT
  t.proxyWallet,
  CAST(DATE_TRUNC('day', to_timestamp(t.timestamp)) AS DATE) AS day,
  t.conditionId,
  CAST(FLOOR(t.price * 10) AS INTEGER) AS price_decile,
  SUM((CASE WHEN t.outcome = r.winning_outcome THEN 1.0 ELSE 0.0 END) - t.price) AS sum_resid,
  SUM(((CASE WHEN t.outcome = r.winning_outcome THEN 1.0 ELSE 0.0 END) - t.price) *
      ((CASE WHEN t.outcome = r.winning_outcome THEN 1.0 ELSE 0.0 END) - t.price)) AS sum_resid_sq,
  COUNT(*) AS n_obs
FROM read_parquet('{TRADES}') t
JOIN read_parquet('{RESOLUTIONS}') r USING (conditionId)
JOIN _mkt_life m USING (conditionId)
LEFT JOIN read_parquet('{WALLET_FLAGS}') wf ON t.proxyWallet = wf.proxyWallet
WHERE t.side = 'BUY'
  AND COALESCE(wf.is_nonhuman, FALSE) = FALSE
  AND NOT ((t.price >= 0.99 OR t.price <= 0.01) AND
           (t.timestamp - m.t_min)*1.0/NULLIF(m.t_max-m.t_min,0) >= 0.99)
  AND FLOOR(t.price * 10) BETWEEN 0 AND 9
GROUP BY t.proxyWallet, day, t.conditionId, price_decile
""")
n_agg, total_N = con.execute("SELECT COUNT(*), SUM(n_obs) FROM agg").fetchone()
print(f"    agg rows: {n_agg:,}  underlying trades: {total_N:,}  ({(time.time()-t0)/60:.1f} min)")

# -----------------------------------------------------------------
# Part A: cluster-size distribution (one-pass over agg)
# Three clusterings: trader (proxyWallet), day, market (conditionId).
# -----------------------------------------------------------------
print("\n>>> cluster-size distribution ...")
def size_summary(group_col: str, label: str):
    df = con.execute(f"""
    WITH s AS (
      SELECT {group_col} AS g, SUM(n_obs) AS sz
      FROM agg GROUP BY {group_col}
    )
    SELECT
      '{label}' AS cluster_dim,
      COUNT(*) AS n_clusters,
      MIN(sz) AS min_size,
      QUANTILE_CONT(sz, 0.25) AS p25,
      MEDIAN(sz)               AS median,
      AVG(sz)                  AS mean,
      QUANTILE_CONT(sz, 0.75)  AS p75,
      QUANTILE_CONT(sz, 0.95)  AS p95,
      MAX(sz)                  AS max,
      /* weighted average cluster size = E[m | trade in cluster]
         = Σ m² / Σ m, often called "effective cluster size" in Moulton */
      SUM(sz*sz)*1.0 / NULLIF(SUM(sz),0) AS weighted_avg_size
    FROM s
    """).fetchone()
    return df

sizes = pd.DataFrame([
    size_summary("proxyWallet", "trader"),
    size_summary("day",         "day"),
    size_summary("conditionId", "market"),
], columns=["cluster_dim","n_clusters","min_size","p25","median",
            "mean","p75","p95","max","weighted_avg_size"])
print(sizes.to_string(index=False))
sizes.to_csv(OUT / "cluster_size_distribution.csv", index=False)

# -----------------------------------------------------------------
# Part B: per-decile SE under iid + each cluster + 3-way CGM
# We re-use the closed-form V = Σ_g (sum_g - μ·n_g)² / N²
# -----------------------------------------------------------------
print("\n>>> computing 7 cluster variances per decile ...")

# per-decile mean & var
mean_df = con.execute("""
SELECT price_decile,
       SUM(n_obs) AS n,
       SUM(sum_resid) / SUM(n_obs) AS spread,
       SUM(sum_resid_sq)/SUM(n_obs) -
       (SUM(sum_resid)/SUM(n_obs))*(SUM(sum_resid)/SUM(n_obs)) AS var_resid
FROM agg GROUP BY price_decile ORDER BY price_decile
""").fetchdf()
mean_df["V_iid"] = mean_df["var_resid"] / mean_df["n"]

def cluster_V(group_cols: str, label: str):
    t0 = time.time()
    q = f"""
    WITH g AS (
      SELECT price_decile, SUM(sum_resid) AS s, SUM(n_obs) AS n
      FROM agg GROUP BY price_decile, {group_cols}
    ),
    m AS (
      SELECT price_decile,
             SUM(sum_resid)/SUM(n_obs) AS spread,
             SUM(n_obs) AS N
      FROM agg GROUP BY price_decile
    )
    SELECT g.price_decile,
           SUM((g.s - m.spread * g.n)*(g.s - m.spread * g.n)) / (m.N * m.N) AS V
    FROM g JOIN m USING (price_decile)
    GROUP BY g.price_decile, m.N
    ORDER BY g.price_decile
    """
    out = con.execute(q).fetchdf().set_index("price_decile")["V"]
    print(f"    V_{label} in {(time.time()-t0)/60:.1f} min")
    return out

V = {}
V["T"]   = cluster_V("proxyWallet", "T")
V["D"]   = cluster_V("day",         "D")
V["M"]   = cluster_V("conditionId", "M")
V["TD"]  = cluster_V("proxyWallet, day",                "TD")
V["TM"]  = cluster_V("proxyWallet, conditionId",        "TM")
V["DM"]  = cluster_V("day, conditionId",                "DM")
V["TDM"] = cluster_V("proxyWallet, day, conditionId",   "TDM")

V_3way = (V["T"] + V["D"] + V["M"]
          - V["TD"] - V["TM"] - V["DM"] + V["TDM"]).clip(lower=0)

# Assemble: per-decile design effect for each clustering + 3-way
out = mean_df.set_index("price_decile").copy()
for k in ["T","D","M","TD","TM","DM","TDM"]:
    out[f"V_{k}"]   = V[k]
    out[f"SE_{k}"]  = V[k].pow(0.5)
    out[f"deff_{k}"]= V[k] / out["V_iid"]
out["V_3way"]   = V_3way
out["SE_3way"]  = V_3way.pow(0.5)
out["deff_3way"]= V_3way / out["V_iid"]
out["N_eff_3way"]= (out["n"] / out["deff_3way"]).astype(int)
out["SE_iid"]   = out["V_iid"].pow(0.5)

# ICC approximations: deff ≈ 1 + (m̄ - 1) * ρ  →  ρ ≈ (deff - 1) / (m̄ - 1)
# Using the weighted average cluster size from Part A.
icc = {}
m_T = sizes.set_index("cluster_dim").loc["trader","weighted_avg_size"]
m_D = sizes.set_index("cluster_dim").loc["day",   "weighted_avg_size"]
m_M = sizes.set_index("cluster_dim").loc["market","weighted_avg_size"]
out["ICC_T"] = ((out["deff_T"] - 1) / (m_T - 1)).clip(lower=0)
out["ICC_D"] = ((out["deff_D"] - 1) / (m_D - 1)).clip(lower=0)
out["ICC_M"] = ((out["deff_M"] - 1) / (m_M - 1)).clip(lower=0)

cols_show = ["n","spread","SE_iid","SE_T","SE_D","SE_M","SE_3way",
             "deff_T","deff_D","deff_M","deff_3way","N_eff_3way",
             "ICC_T","ICC_D","ICC_M"]
print()
print(out[cols_show].to_string(float_format=lambda x: f"{x:8.4g}"))
out.to_csv(OUT / "cluster_diagnostics.csv")
print(f"\nwrote {OUT / 'cluster_diagnostics.csv'}")
print(f"wrote {OUT / 'cluster_size_distribution.csv'}")
