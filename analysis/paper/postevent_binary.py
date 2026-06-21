"""Same as postevent_summary.py but filtered to binary markets (neg_risk = FALSE).

Binary = standalone Yes/No, not part of a multi-outcome NegRisk event.
"""
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

OUT = Path("/mnt/data/analysis_output/paper/tables/postevent")
FIGS = Path("/mnt/data/analysis_output/paper/figures")
OUT.mkdir(parents=True, exist_ok=True)

sns.set_theme(context="paper", style="white", font="DejaVu Serif")
plt.rcParams.update({
    "text.parse_math": False,
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold",
    "axes.labelsize": 11, "axes.linewidth": 0.8,
    "savefig.dpi": 200, "axes.spines.top": False, "axes.spines.right": False,
})

TRADES = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
RESOLUTIONS = "/home/ubuntu/pipeline/output/market_resolutions.parquet"
GAMMA = "/mnt/data/pipeline_data/gamma_markets.parquet"
T2E = "/mnt/data/pipeline_data/token_to_event.parquet"

con = duckdb.connect()
con.execute("SET memory_limit='48GB'")
con.execute("SET threads=16")
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET preserve_insertion_order=false")

print("Building binary_tokens (token_id where parent market has neg_risk=FALSE) ...")
con.execute(f"""
CREATE TEMP TABLE binary_tokens AS
SELECT te.token_id
FROM read_parquet('{T2E}') te
JOIN read_parquet('{GAMMA}') m ON te.condition_id = m.condition_id
WHERE m.neg_risk = FALSE
""")
n_bin = con.execute("SELECT COUNT(*) FROM binary_tokens").fetchone()[0]
print(f"  binary tokens: {n_bin:,}")

print("Building filtered view t (binary markets, BUY, resolved) ...")
con.execute(f"""
CREATE TEMP VIEW t AS
SELECT t.timestamp, t.price, t.usdcSize, t.outcome, t.conditionId, t.eventSlug,
       CASE WHEN t.outcome = r.winning_outcome THEN 1 ELSE 0 END AS won
FROM read_parquet('{TRADES}') t
JOIN read_parquet('{RESOLUTIONS}') r USING (conditionId)
JOIN binary_tokens b ON t.conditionId = b.token_id
WHERE t.side = 'BUY'
""")

# Table 1: price bucket distribution
print(">>> Table 1 (binary) ...")
t1 = con.execute("""
WITH binned AS (
  SELECT
    CASE
      WHEN price >= 0.995 THEN '>= 0.995'
      WHEN price >= 0.99  THEN '[0.99, 0.995)'
      WHEN price >= 0.95  THEN '[0.95, 0.99)'
      WHEN price >= 0.90  THEN '[0.90, 0.95)'
      WHEN price >= 0.50  THEN '[0.50, 0.90)'
      WHEN price >= 0.10  THEN '[0.10, 0.50)'
      WHEN price >= 0.05  THEN '[0.05, 0.10)'
      WHEN price >= 0.01  THEN '[0.01, 0.05)'
      WHEN price >  0.001 THEN '(0.001, 0.01)'
      WHEN price = 0.001  THEN '= 0.001'
      ELSE                     '< 0.001'
    END AS bucket,
    usdcSize
  FROM t
)
SELECT bucket, COUNT(*) AS n_trades, SUM(usdcSize) AS volume
FROM binned GROUP BY bucket ORDER BY bucket
""").fetchdf()
totals = t1[["n_trades", "volume"]].sum()
t1["pct_trades"] = 100 * t1["n_trades"] / totals["n_trades"]
t1["pct_volume"] = 100 * t1["volume"] / totals["volume"]
order = ['>= 0.995','[0.99, 0.995)','[0.95, 0.99)','[0.90, 0.95)','[0.50, 0.90)',
         '[0.10, 0.50)','[0.05, 0.10)','[0.01, 0.05)','(0.001, 0.01)','= 0.001','< 0.001']
t1["bucket"] = pd.Categorical(t1["bucket"], categories=order, ordered=True)
t1 = t1.sort_values("bucket").reset_index(drop=True)
print(t1.to_string(index=False))
t1.to_csv(OUT / "table1_price_buckets_binary.csv", index=False)

# Table 2: lifecycle position
print("\n>>> Table 2 (binary) ...")
con.execute("""
CREATE TEMP TABLE mkt_life AS
SELECT conditionId, MIN(timestamp) AS t_min, MAX(timestamp) AS t_max
FROM t GROUP BY conditionId HAVING MAX(timestamp) > MIN(timestamp)
""")

t2 = con.execute("""
WITH lifecycle AS (
  SELECT
    t.price, t.usdcSize,
    (t.timestamp - m.t_min) * 1.0 / NULLIF(m.t_max - m.t_min, 0) AS pos
  FROM t JOIN mkt_life m USING (conditionId)
)
SELECT
  CASE
    WHEN price >= 0.995 THEN 'p >= 0.995'
    WHEN price <= 0.001 THEN 'p <= 0.001'
    ELSE                     'middle (0.001 < p < 0.995)'
  END AS price_class,
  CASE
    WHEN pos >= 0.99 THEN 'last 1%'
    WHEN pos >= 0.95 THEN 'last 5%'
    WHEN pos >= 0.90 THEN 'last 10%'
    WHEN pos >= 0.80 THEN 'last 20%'
    WHEN pos >= 0.50 THEN '50-80%'
    ELSE                  'first 50%'
  END AS lifecycle_bin,
  COUNT(*) AS n,
  SUM(usdcSize) AS volume
FROM lifecycle GROUP BY price_class, lifecycle_bin
""").fetchdf()
t2.to_csv(OUT / "table2_lifecycle_binary.csv", index=False)

piv_n = t2.pivot_table(index="lifecycle_bin", columns="price_class",
                       values="n", fill_value=0)
order_life = ["first 50%", "50-80%", "last 20%", "last 10%", "last 5%", "last 1%"]
piv_n = piv_n.reindex(order_life)
piv_n_pct = 100 * piv_n / piv_n.sum(axis=0)
print("  % of each price class in each lifecycle bin (column-normalized):")
print(piv_n_pct.round(2).to_string())
piv_n_pct.round(2).to_csv(OUT / "table2_lifecycle_pivot_binary.csv")

# Table 3: by category
print("\n>>> Table 3 (binary) ...")
t3 = con.execute("""
WITH categorized AS (
  SELECT t.price, t.usdcSize,
    (t.timestamp - m.t_min) * 1.0 / NULLIF(m.t_max - m.t_min, 0) AS pos,
    CASE
      WHEN t.eventSlug LIKE 'nba-%' OR t.eventSlug LIKE 'nfl-%'
        OR t.eventSlug LIKE 'mlb-%' OR t.eventSlug LIKE 'nhl-%'
        OR t.eventSlug LIKE 'ncaab-%' OR t.eventSlug LIKE 'ncaaf-%'
        OR t.eventSlug LIKE '%-spread-%' OR t.eventSlug LIKE '%-moneyline%'
        THEN 'sports'
      WHEN t.eventSlug LIKE '%btc-updown-%' OR t.eventSlug LIKE '%eth-updown-%'
        OR t.eventSlug LIKE '%-up-or-down-%'
        THEN 'crypto up/down (intraday)'
      WHEN t.eventSlug LIKE '%btc%' OR t.eventSlug LIKE '%bitcoin%'
        OR t.eventSlug LIKE '%eth%' OR t.eventSlug LIKE '%ethereum%'
        THEN 'crypto (price levels)'
      WHEN t.eventSlug LIKE '%election%' OR t.eventSlug LIKE '%senate%'
        OR t.eventSlug LIKE '%president%' OR t.eventSlug LIKE '%primary%'
        OR t.eventSlug LIKE '%mayor%' OR t.eventSlug LIKE 'will-trump-%'
        THEN 'politics'
      WHEN t.eventSlug LIKE '%fed-%' OR t.eventSlug LIKE '%fomc%'
        THEN 'macro/finance'
      ELSE 'other'
    END AS category
  FROM t JOIN mkt_life m USING (conditionId)
  WHERE t.eventSlug IS NOT NULL AND t.eventSlug <> ''
)
SELECT category,
       COUNT(*) AS total_trades,
       SUM(CASE WHEN price >= 0.995 THEN 1 ELSE 0 END) AS n_extreme,
       SUM(CASE WHEN price >= 0.995 AND pos >= 0.95 THEN 1 ELSE 0 END) AS n_extreme_late,
       SUM(usdcSize) AS volume_total,
       SUM(CASE WHEN price >= 0.995 THEN usdcSize ELSE 0 END) AS volume_extreme,
       SUM(CASE WHEN price >= 0.995 AND pos >= 0.95 THEN usdcSize ELSE 0 END) AS volume_extreme_late
FROM categorized GROUP BY category ORDER BY total_trades DESC
""").fetchdf()
t3["pct_trades_at_extreme"]  = 100 * t3["n_extreme"]    / t3["total_trades"]
t3["pct_extreme_in_last5pct"]= 100 * t3["n_extreme_late"]/ t3["n_extreme"].clip(lower=1)
t3["pct_volume_at_extreme"]  = 100 * t3["volume_extreme"]/ t3["volume_total"]
print(t3.to_string(index=False))
t3.to_csv(OUT / "table3_by_category_binary.csv", index=False)

# Table 4: FLB filter sensitivity
print("\n>>> Table 4 (binary) ...")
con.execute("""
CREATE OR REPLACE TEMP VIEW t_with_pos AS
SELECT t.*, (t.timestamp - m.t_min) * 1.0 / NULLIF(m.t_max - m.t_min, 0) AS pos
FROM t JOIN mkt_life m USING (conditionId)
""")

def flb(filt: str, label: str):
    r = con.execute(f"""
    SELECT COUNT(*), AVG(price), AVG(won), AVG(won) - AVG(price)
    FROM t_with_pos
    WHERE price >= 0.9 AND price < 1.0 {filt}
    """).fetchone()
    return {"filter": label, "n_trades": r[0], "mean_price": r[1],
            "mean_outcome": r[2], "spread": r[3],
            "return_per_dollar": r[3] / r[1] if r[1] else None}

t4 = pd.DataFrame([
    flb("",                                                "no filter (baseline)"),
    flb("AND price < 0.995",                               "drop p >= 0.995"),
    flb("AND price < 0.99",                                "drop p >= 0.99"),
    flb("AND pos < 0.99",                                  "drop last 1% of market lifetime"),
    flb("AND pos < 0.95",                                  "drop last 5% of market lifetime"),
    flb("AND pos < 0.98 AND price < 0.995",                "drop last 2% lifetime AND p >= 0.995"),
])
print(t4.to_string(index=False))
t4.to_csv(OUT / "table4_flb_filter_impact_binary.csv", index=False)

# Plot
print("\n>>> Lifecycle plot (binary) ...")
plot_df = con.execute("""
WITH categorized AS (
  SELECT t.usdcSize,
    (t.timestamp - m.t_min) * 1.0 / NULLIF(m.t_max - m.t_min, 0) AS pos,
    CASE
      WHEN t.eventSlug LIKE 'nba-%' OR t.eventSlug LIKE 'nfl-%'
        OR t.eventSlug LIKE 'mlb-%' OR t.eventSlug LIKE 'nhl-%'
        OR t.eventSlug LIKE 'ncaab-%' OR t.eventSlug LIKE 'ncaaf-%'
        OR t.eventSlug LIKE '%-spread-%' OR t.eventSlug LIKE '%-moneyline%'
        THEN 'sports'
      WHEN t.eventSlug LIKE '%btc-updown-%' OR t.eventSlug LIKE '%eth-updown-%'
        OR t.eventSlug LIKE '%-up-or-down-%'
        THEN 'crypto up/down (intraday)'
      WHEN t.eventSlug LIKE '%election%' OR t.eventSlug LIKE '%senate%'
        OR t.eventSlug LIKE '%president%' OR t.eventSlug LIKE '%primary%'
        OR t.eventSlug LIKE '%mayor%' OR t.eventSlug LIKE 'will-trump-%'
        THEN 'politics'
      WHEN t.eventSlug LIKE '%fed-%' OR t.eventSlug LIKE '%fomc%'
        THEN 'macro/finance'
      ELSE 'other'
    END AS category
  FROM t JOIN mkt_life m USING (conditionId)
  WHERE price >= 0.995
)
SELECT category, CAST(FLOOR(pos * 50) AS INT) AS bin50, COUNT(*) AS n
FROM categorized
WHERE pos IS NOT NULL AND pos BETWEEN 0 AND 1
GROUP BY category, bin50
""").fetchdf()
plot_df["bin_mid"] = (plot_df["bin50"] + 0.5) / 50
piv = plot_df.pivot_table(index="bin_mid", columns="category", values="n", fill_value=0)
piv_pct = 100 * piv / piv.sum(axis=0)

fig, ax = plt.subplots(figsize=(11, 5.5))
for cat in piv_pct.columns:
    ax.plot(piv_pct.index, piv_pct[cat], label=cat, linewidth=1.6, alpha=0.9)
ax.set_xlabel("Position in market lifetime (0 = first trade, 1 = last trade)")
ax.set_ylabel("Share of within-category p>=0.995 trades (%)")
ax.set_title("BINARY markets only: where do p>=0.995 trades occur in each market's lifetime?",
             loc="left", pad=28, fontweight="bold")
ax.text(0.0, 1.045,
        "Each line is normalised within its category. Multi-outcome NegRisk markets excluded.",
        transform=ax.transAxes, fontsize=9, color="#444", style="italic")
ax.legend(fontsize=9, loc="upper left")
ax.grid(alpha=0.25, linestyle=":")
ax.set_xlim(0, 1)
plt.tight_layout(rect=[0, 0, 1, 0.88])
plt.savefig(FIGS / "extreme_lifecycle_binary.png", pad_inches=0.3)
print("wrote", FIGS / "extreme_lifecycle_binary.png")
