"""FLB by deciles, before/after bot filter. Compares attrition.

Uses bot_filter.build_wallet_flags from the existing analysis module.
"""
import sys, time
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

sys.path.insert(0, "/home/ubuntu/pipeline/analysis")
from bot_filter import build_wallet_flags

ANALYSIS = Path("/home/ubuntu/analysis_final")
TABLES = ANALYSIS / "tables"
FIGS = ANALYSIS / "figures"
TABLES.mkdir(exist_ok=True); FIGS.mkdir(exist_ok=True)

TRADES = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
RESOLUTIONS = "/home/ubuntu/pipeline/output/market_resolutions.parquet"

con = duckdb.connect()
con.execute("SET memory_limit='100GB'")
con.execute("SET threads=16")
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET preserve_insertion_order=false")

print("Registering trades view ...")
con.execute(f"CREATE VIEW trades AS SELECT * FROM read_parquet('{TRADES}')")

print("Building wallet_flags via bot_filter (this scans the full table) ...")
t0 = time.time()
build_wallet_flags(con, verbose=True)
print(f"bot_filter done in {(time.time()-t0)/60:.1f} min")

# Attrition stats
stats = con.execute("""
SELECT
  COUNT(*) AS total_wallets,
  SUM(CASE WHEN is_nonhuman THEN 1 ELSE 0 END) AS nonhuman_wallets,
  SUM(n_trades) AS total_trades_per_wallet_sum,
  SUM(CASE WHEN is_nonhuman THEN n_trades ELSE 0 END) AS nonhuman_trades
FROM wallet_flags
""").fetchone()
print(f"\nWallet attrition: {stats[1]:,} nonhuman of {stats[0]:,} wallets ({100*stats[1]/stats[0]:.1f}%)")
print(f"Trade attrition:  {stats[3]:,} nonhuman trades of {stats[2]:,} ({100*stats[3]/stats[2]:.1f}%)")

# Compute FLB twice: with and without bot filter
def flb_query(filter_bots: bool):
    bot_clause = "" if not filter_bots else """
        AND t.proxyWallet NOT IN (SELECT proxyWallet FROM wallet_flags WHERE is_nonhuman)
    """
    return con.execute(f"""
    WITH joined AS (
      SELECT t.price,
             CASE WHEN t.outcome = r.winning_outcome THEN 1 ELSE 0 END AS won
      FROM trades t
      JOIN read_parquet('{RESOLUTIONS}') r ON t.conditionId = r.conditionId
      WHERE t.side = 'BUY' AND t.price BETWEEN 0.01 AND 0.99
      {bot_clause}
    )
    SELECT CAST(FLOOR(price * 10) AS INTEGER) AS decile,
           COUNT(*) AS n_trades,
           AVG(price) AS mean_price,
           AVG(won) AS mean_outcome,
           AVG(won) - AVG(price) AS spread
    FROM joined
    WHERE FLOOR(price * 10) BETWEEN 0 AND 9
    GROUP BY decile ORDER BY decile
    """).fetchdf()

print("\nComputing FLB (all wallets) ...")
t0 = time.time()
df_all = flb_query(filter_bots=False)
print(f"  done in {(time.time()-t0)/60:.1f} min")

print("Computing FLB (humans only) ...")
t0 = time.time()
df_h = flb_query(filter_bots=True)
print(f"  done in {(time.time()-t0)/60:.1f} min")

for df, name in ((df_all, "ALL"), (df_h, "HUMAN-only")):
    df["bin"] = df["decile"].map(lambda d: f"[{d/10:.1f},{(d+1)/10:.1f})")
    df["return_per_dollar"] = df["spread"] / df["mean_price"]
    print(f"\n=== {name} ===")
    print(df[["bin","n_trades","mean_price","mean_outcome","spread","return_per_dollar"]].to_string(index=False))

df_all.to_csv(TABLES / "flb_deciles_all.csv", index=False)
df_h.to_csv(TABLES / "flb_deciles_human.csv", index=False)

# Side-by-side plot
fig, axes = plt.subplots(1, 2, figsize=(14, 5.5), sharey=True)
for ax, df, title in (
    (axes[0], df_all, f"All wallets — {df_all['n_trades'].sum()/1e6:.0f}M BUY trades"),
    (axes[1], df_h,   f"Humans only — {df_h['n_trades'].sum()/1e6:.0f}M BUY trades"),
):
    colors = ["#cf3030" if v < 0 else "#1f9e44" for v in df["spread"]]
    bars = ax.bar(df["bin"], df["spread"], color=colors, alpha=0.85,
                  edgecolor="black", linewidth=0.6)
    for bar, val in zip(bars, df["spread"]):
        y = val + 0.001 if val >= 0 else val - 0.001
        va = "bottom" if val >= 0 else "top"
        ax.text(bar.get_x() + bar.get_width()/2, y, f"{val:+.3f}",
                ha="center", va=va, fontsize=8)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Price decile", fontsize=11)
    ax.set_title(title, fontsize=12)
    ax.tick_params(axis="x", rotation=45, labelsize=8)
    ax.grid(axis="y", alpha=0.3)
    ax.set_ylim(-0.04, 0.04)

axes[0].set_ylabel("Spread = realized − implied", fontsize=11)
fig.suptitle("Polymarket FLB — effect of removing bot wallets", fontsize=13, y=1.00)
plt.tight_layout()
plt.savefig(FIGS / "flb_bot_compare.png", dpi=150, bbox_inches="tight")
print(f"\nwrote {FIGS / 'flb_bot_compare.png'}")
