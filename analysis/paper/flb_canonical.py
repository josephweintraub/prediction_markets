"""Canonical platform-level FLB analysis (optimized).

Sample: BUY trades on resolved markets, excluding
  - Bot/HFT wallets (cached wallet_flags.parquet, rebuilt if missing)
  - Post-event trades: (p>=0.99 OR p<=0.01) AND lifecycle pos >= 0.99

Per price decile [0.0,0.1)...[0.9,1.0):
  n, mean_price, mean_outcome, spread = mean_outcome - mean_price
  SE under: iid, cluster-on-trader, cluster-on-day, cluster-on-market,
  Cameron-Gelbach-Miller 3-way (trader × day × market)

Optimization: pre-aggregate to (trader, day, market, decile)
  → (sum_resid, n_obs) ONCE. Each cluster variance is then a single
  small GROUP BY on that pre-aggregated table.

Outputs: tables/flb_canonical_deciles.csv, figures/flb_canonical.png
"""
import sys, time, os
sys.path.insert(0, "/home/ubuntu/pipeline/analysis")

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

ANALYSIS = Path("/home/ubuntu/analysis_final")
TABLES = ANALYSIS / "tables"
FIGS = ANALYSIS / "figures"
CACHE = Path("/mnt/data/pipeline_data")
TABLES.mkdir(exist_ok=True); FIGS.mkdir(exist_ok=True)

TRADES = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
RESOLUTIONS = "/home/ubuntu/pipeline/output/market_resolutions.parquet"
WALLET_FLAGS = CACHE / "wallet_flags.parquet"

sns.set_theme(context="paper", style="white", font="DejaVu Serif")
plt.rcParams.update({
    "text.parse_math": False,
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold",
    "axes.labelsize": 11, "axes.linewidth": 0.8,
    "savefig.dpi": 200, "axes.spines.top": False, "axes.spines.right": False,
})

con = duckdb.connect()
con.execute("SET memory_limit='100GB'")
con.execute("SET threads=16")
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET preserve_insertion_order=false")

# -----------------------------------------------------------------
# 1. wallet_flags (cached on disk after first build)
# -----------------------------------------------------------------
if WALLET_FLAGS.exists():
    print(f">>> using cached {WALLET_FLAGS}")
    con.execute(f"""
    CREATE TEMP VIEW wallet_flags AS
    SELECT * FROM read_parquet('{WALLET_FLAGS}')
    """)
else:
    print(">>> building wallet_flags (~8 min, one-time) ...")
    from bot_filter import build_wallet_flags
    con.execute(f"CREATE VIEW trades AS SELECT * FROM read_parquet('{TRADES}')")
    t0 = time.time()
    build_wallet_flags(con, verbose=False)
    con.execute(f"""
    COPY (SELECT * FROM wallet_flags) TO '{WALLET_FLAGS}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print(f"    done in {(time.time()-t0)/60:.1f} min, cached to {WALLET_FLAGS}")

# -----------------------------------------------------------------
# 2. Pre-aggregate flb_base to (trader, day, market, decile)
#    in a single scan: every downstream cluster variance reads this.
# -----------------------------------------------------------------
print(">>> building pre-aggregated table agg = (trader, day, market, decile) → sums ...")
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
  SUM(t.price) AS sum_price,
  SUM(CASE WHEN t.outcome = r.winning_outcome THEN 1.0 ELSE 0.0 END) AS sum_won,
  SUM(((CASE WHEN t.outcome = r.winning_outcome THEN 1.0 ELSE 0.0 END) - t.price) *
      ((CASE WHEN t.outcome = r.winning_outcome THEN 1.0 ELSE 0.0 END) - t.price))
                                                                  AS sum_resid_sq,
  COUNT(*) AS n_obs
FROM read_parquet('{TRADES}') t
JOIN read_parquet('{RESOLUTIONS}') r USING (conditionId)
JOIN _mkt_life m USING (conditionId)
LEFT JOIN wallet_flags wf ON t.proxyWallet = wf.proxyWallet
WHERE t.side = 'BUY'
  AND COALESCE(wf.is_nonhuman, FALSE) = FALSE                       -- non-bot
  AND NOT ((t.price >= 0.99 OR t.price <= 0.01) AND
           (t.timestamp - m.t_min)*1.0/NULLIF(m.t_max-m.t_min,0) >= 0.99)
  AND FLOOR(t.price * 10) BETWEEN 0 AND 9
GROUP BY t.proxyWallet, day, t.conditionId, price_decile
""")
n_agg = con.execute("SELECT COUNT(*) FROM agg").fetchone()[0]
total_N = con.execute("SELECT SUM(n_obs) FROM agg").fetchone()[0]
print(f"    agg rows: {n_agg:,}   total underlying trades: {total_N:,}   build {(time.time()-t0)/60:.1f} min")

# -----------------------------------------------------------------
# 3. Per-decile mean spread (and var for iid SE)
# -----------------------------------------------------------------
print(">>> computing per-decile mean & iid SE ...")
mean_df = con.execute("""
SELECT price_decile,
       SUM(n_obs) AS n,
       SUM(sum_price) / SUM(n_obs) AS mean_price,
       SUM(sum_won)   / SUM(n_obs) AS mean_outcome,
       SUM(sum_resid) / SUM(n_obs) AS spread,
       /* var_pop(resid) = E[resid^2] - E[resid]^2 */
       SUM(sum_resid_sq) / SUM(n_obs) -
       (SUM(sum_resid) / SUM(n_obs)) * (SUM(sum_resid) / SUM(n_obs)) AS var_resid
FROM agg
GROUP BY price_decile ORDER BY price_decile
""").fetchdf()
mean_df["SE_iid"] = (mean_df["var_resid"] / mean_df["n"]) ** 0.5
mu_by_dec = mean_df.set_index("price_decile")["spread"].to_dict()
N_by_dec  = mean_df.set_index("price_decile")["n"].to_dict()

# -----------------------------------------------------------------
# 4. Cluster variances. For each cluster definition,
#       V = (1/N^2) Σ_g (sum_g - μ * n_g)^2
#    where sum_g = Σ resid in cluster g, n_g = count in cluster g.
# -----------------------------------------------------------------
def cv(group_cols: str, label: str) -> pd.Series:
    """Cluster variance per decile."""
    t0 = time.time()
    q = f"""
    WITH g AS (
      SELECT price_decile,
             SUM(sum_resid) AS s,
             SUM(n_obs)     AS n
      FROM agg GROUP BY price_decile, {group_cols}
    )
    SELECT g.price_decile,
           SUM((g.s - mu.spread * g.n) * (g.s - mu.spread * g.n)) / (mu.N * mu.N) AS V
    FROM g
    JOIN (SELECT price_decile, spread, n AS N FROM (SELECT price_decile,
                 SUM(sum_resid) / SUM(n_obs) AS spread, SUM(n_obs) AS n
                 FROM agg GROUP BY price_decile)) mu USING (price_decile)
    GROUP BY g.price_decile, mu.N
    ORDER BY g.price_decile
    """
    out = con.execute(q).fetchdf().set_index("price_decile")["V"]
    print(f"    V_{label} done in {(time.time()-t0)/60:.1f} min")
    return out

# V_TDM is free: agg is ALREADY at (trader, day, market) level, plus decile.
# Compute directly without another GROUP BY.
print(">>> computing 7 cluster variances on pre-aggregated table ...")
V = {}
V["T"]   = cv("proxyWallet",                "T")
V["D"]   = cv("day",                        "D")
V["M"]   = cv("conditionId",                "M")
V["TD"]  = cv("proxyWallet, day",           "TD")
V["TM"]  = cv("proxyWallet, conditionId",   "TM")
V["DM"]  = cv("day, conditionId",           "DM")
V["TDM"] = cv("proxyWallet, day, conditionId", "TDM")

# Cameron-Gelbach-Miller 3-way decomposition
V_3way = V["T"] + V["D"] + V["M"] - V["TD"] - V["TM"] - V["DM"] + V["TDM"]
V_3way = V_3way.clip(lower=0)  # numerical floor

# -----------------------------------------------------------------
# 5. Assemble output table and plot
# -----------------------------------------------------------------
out = mean_df.set_index("price_decile").copy()
out["SE_T"]   = V["T"].pow(0.5)
out["SE_D"]   = V["D"].pow(0.5)
out["SE_M"]   = V["M"].pow(0.5)
out["SE_3way"] = V_3way.pow(0.5)
out["t_iid"]   = out["spread"] / out["SE_iid"]
out["t_3way"]  = out["spread"] / out["SE_3way"]
out["ci_lo"]   = out["spread"] - 1.96 * out["SE_3way"]
out["ci_hi"]   = out["spread"] + 1.96 * out["SE_3way"]
out["bin_label"] = out.index.map(lambda d: f"[{d/10:.1f},{(d+1)/10:.1f})")

cols = ["n","mean_price","mean_outcome","spread",
        "SE_iid","SE_T","SE_D","SE_M","SE_3way","t_iid","t_3way"]
print()
print(out[cols].to_string(float_format=lambda x: f"{x:.4f}"))
out.to_csv(TABLES / "flb_canonical_deciles.csv")
print(f"\nwrote {TABLES / 'flb_canonical_deciles.csv'}")

# Plot
fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))

ax = axes[0]
ax.plot([0, 1], [0, 1], color="grey", linestyle="--", linewidth=0.8,
        label="Perfect calibration", zorder=1)
ax.scatter(out["mean_price"], out["mean_outcome"], s=85, color="#1a3d6d",
           edgecolor="black", linewidth=0.6, zorder=3)
ax.set_xlabel("Mean implied probability")
ax.set_ylabel("Realized win rate")
ax.set_title("(a) Calibration", loc="left", pad=8)
ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.set_aspect("equal")
ax.legend(loc="upper left", fontsize=9)
ax.grid(alpha=0.25, linestyle=":")

ax = axes[1]
colors = ["#a02020" if v < 0 else "#1c6b3a" for v in out["spread"]]
bars = ax.bar(out["bin_label"], out["spread"], color=colors, alpha=0.85,
              edgecolor="black", linewidth=0.5, width=0.78)
ax.errorbar(out["bin_label"], out["spread"],
            yerr=1.96 * out["SE_3way"], fmt="none",
            ecolor="black", linewidth=0.9, capsize=3, zorder=4)
for bar, val in zip(bars, out["spread"]):
    y = val + 0.0010 if val >= 0 else val - 0.0010
    va = "bottom" if val >= 0 else "top"
    ax.text(bar.get_x() + bar.get_width()/2, y, f"{val:+.3f}",
            ha="center", va=va, fontsize=8.5)
ax.axhline(0, color="black", linewidth=0.7)
ax.set_xlabel("Price decile")
ax.set_ylabel("Spread = E[outcome] - E[price]")
ax.set_title("(b) FLB spread by decile  (error bars = ±1.96 × CGM 3-way SE)",
             loc="left", pad=8)
ax.tick_params(axis="x", rotation=45, labelsize=8.5)
ax.grid(axis="y", alpha=0.25, linestyle=":")

fig.suptitle(
    f"Platform-level Favorite-Longshot Bias  ·  "
    f"BUY trades, humans only, post-event-filtered  ·  "
    f"{total_N/1e6:.0f}M trades",
    fontsize=12, y=1.005, fontweight="bold",
)
plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig(FIGS / "flb_canonical.png")
print(f"wrote {FIGS / 'flb_canonical.png'}")
