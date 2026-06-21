"""FLB by contract type — broad-based vs. narrow (single-name), porting the
Bartlett-O'Hara (2026) Kalshi split to Polymarket.

Definitions (Polymarket slug-based, intended to mirror Appendix A of the paper):

  BROAD-BASED — macro aggregates, indices, asset prices; dispersed information.
    crypto intraday (btc-updown-*, eth-updown-*, *-up-or-down-*),
    crypto price thresholds (will-bitcoin/ethereum-reach-*, *-dip-to-*),
    Fed / FOMC / rate decisions, CPI / inflation, jobs / employment / payrolls,
    S&P 500 / Nasdaq levels, GDP, PCE, recession / debt-ceiling, FX, commodities.

  NARROW (single-name) — specific individual or company action.
    will-trump-say-*, will-trump-tweet-*, *-attend(s)-*, will-musk/elon-*,
    earnings-mention/-call, Tesla deliveries/production, Spotify/Netflix/App
    rankings, CEO departures, IPO timing/event, M&A specific deals,
    SpaceX launches/IPO, OpenAI-specific.
    NOTE: election-winner markets are NOT here — those aggregate voters.

  Politics, sports, and other are tagged but excluded from the headline split.

For each category we compute the per-decile FLB statistics (n, mean_price,
mean_outcome, spread, SE_iid, SE_3way via Cameron-Gelbach-Miller) on the
canonical filtered sample (BUY only, humans, post-event filter:
(p>=0.99 OR p<=0.01) AND lifecycle pos >= 0.99).
"""
import sys, time
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

ANALYSIS = Path("/mnt/data/analysis_output/paper")
TABLES = ANALYSIS / "tables"
FIGS = ANALYSIS / "figures"
TABLES.mkdir(exist_ok=True); FIGS.mkdir(exist_ok=True)

TRADES = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
RESOLUTIONS = "/home/ubuntu/pipeline/output/market_resolutions.parquet"
GAMMA = "/mnt/data/pipeline_data/gamma_markets.parquet"
WALLET_FLAGS = "/mnt/data/pipeline_data/wallet_flags.parquet"

sns.set_theme(context="paper", style="white", font="DejaVu Serif")
plt.rcParams.update({
    "text.parse_math": False, "font.size": 11, "axes.titlesize": 13,
    "axes.titleweight": "bold", "axes.labelsize": 11, "axes.linewidth": 0.8,
    "savefig.dpi": 200, "axes.spines.top": False, "axes.spines.right": False,
})

con = duckdb.connect()
con.execute("SET memory_limit='180GB'")
con.execute("SET threads=16")
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET preserve_insertion_order=false")

# -----------------------------------------------------------------
# 1. Classify markets via gamma_markets slugs (event-level via market_slug).
# Adapt patterns to Polymarket conventions; see header for rationale.
# -----------------------------------------------------------------
print(">>> classifying markets ...")
con.execute(f"""
CREATE TEMP TABLE mkt_class AS
SELECT condition_id,
       market_slug,
       neg_risk,
       CASE
         /* -------- BROAD-BASED -------- */
         /* dropped intraday up/down */
         /* dropped intraday up/down */
         /* dropped intraday up/down */
         WHEN market_slug LIKE '%bitcoin-reach-%'        THEN 'broad'
         WHEN market_slug LIKE '%ethereum-reach-%'       THEN 'broad'
         WHEN market_slug LIKE '%bitcoin-dip-%'          THEN 'broad'
         WHEN market_slug LIKE '%ethereum-dip-%'         THEN 'broad'
         WHEN market_slug LIKE '%bitcoin-hit-%'          THEN 'broad'
         WHEN market_slug LIKE '%ethereum-hit-%'         THEN 'broad'
         WHEN market_slug LIKE '%bitcoin-above-%'        THEN 'broad'
         WHEN market_slug LIKE '%bitcoin-below-%'        THEN 'broad'
         WHEN market_slug LIKE '%bitcoin-multistrike-%'  THEN 'broad'
         WHEN market_slug LIKE '%btc-multistrike-%'      THEN 'broad'
         WHEN market_slug LIKE '%ethereum-above-%'       THEN 'broad'
         WHEN market_slug LIKE '%ethereum-below-%'       THEN 'broad'
         WHEN market_slug LIKE 'sol-%'                   THEN 'broad'
         /* dropped intraday up/down */
         /* dropped intraday up/down */
         WHEN market_slug LIKE '%what-price-will-bitcoin%' THEN 'broad'
         WHEN market_slug LIKE '%what-price-will-ethereum%' THEN 'broad'
         WHEN market_slug LIKE 'will-the-fed-%'          THEN 'broad'
         WHEN market_slug LIKE 'will-no-fed-rate-%'      THEN 'broad'
         WHEN market_slug LIKE 'will-%-fed-rate-%'       THEN 'broad'
         WHEN market_slug LIKE 'fed-decision-%'          THEN 'broad'
         WHEN market_slug LIKE '%fed-rates-%'            THEN 'broad'
         WHEN market_slug LIKE '%fomc-%'                 THEN 'broad'
         WHEN market_slug LIKE '%-rate-cut%'             THEN 'broad'
         WHEN market_slug LIKE '%basis-points%'          THEN 'broad'
         WHEN market_slug LIKE '%cpi-%'                  THEN 'broad'
         WHEN market_slug LIKE '%inflation%'             THEN 'broad'
         WHEN market_slug LIKE '%gdp-%'                  THEN 'broad'
         WHEN market_slug LIKE '%pce-%'                  THEN 'broad'
         WHEN market_slug LIKE '%-jobs-%'                THEN 'broad'
         WHEN market_slug LIKE '%payrolls-%'             THEN 'broad'
         WHEN market_slug LIKE '%unemployment-%'         THEN 'broad'
         WHEN market_slug LIKE '%recession%'             THEN 'broad'
         WHEN market_slug LIKE '%debt-ceiling%'          THEN 'broad'
         WHEN market_slug LIKE 'sp500-%'                 THEN 'broad'
         WHEN market_slug LIKE 'spx-%'                   THEN 'broad'
         WHEN market_slug LIKE 'nasdaq-%'                THEN 'broad'
         WHEN market_slug LIKE 'ndx-%'                   THEN 'broad'
         WHEN market_slug LIKE '%-treasury-%'            THEN 'broad'
         WHEN market_slug LIKE '%oil-price-%'            THEN 'broad'
         WHEN market_slug LIKE '%gold-price-%'           THEN 'broad'
         WHEN market_slug LIKE 'usd-%'                   THEN 'broad'
         WHEN market_slug LIKE 'eur-usd-%'               THEN 'broad'

         /* -------- SINGLE-NAME (NARROW) -------- */
         /* specific named-person action / speech, not election outcomes */
         WHEN market_slug LIKE 'will-trump-say-%'        THEN 'narrow'
         WHEN market_slug LIKE 'will-trump-tweet-%'      THEN 'narrow'
         WHEN market_slug LIKE 'will-trump-mention-%'    THEN 'narrow'
         WHEN market_slug LIKE 'will-trump-post-%'       THEN 'narrow'
         WHEN market_slug LIKE 'trump-of-tweets-%'       THEN 'narrow'
         WHEN market_slug LIKE 'will-elon-musk-tweet-%'  THEN 'narrow'
         WHEN market_slug LIKE 'will-musk-tweet-%'       THEN 'narrow'
         WHEN market_slug LIKE 'will-elon-musk-%-tweet%' THEN 'narrow'
         WHEN market_slug LIKE 'will-elon-musk-say-%'    THEN 'narrow'
         WHEN market_slug LIKE 'will-bezos-%'            THEN 'narrow'
         WHEN market_slug LIKE '%-attend-%'              THEN 'narrow'
         WHEN market_slug LIKE '%-attends-%'             THEN 'narrow'
         WHEN market_slug LIKE 'will-%-attend%'          THEN 'narrow'
         WHEN market_slug LIKE '%-earnings-mention%'     THEN 'narrow'
         WHEN market_slug LIKE '%-earnings-call%'        THEN 'narrow'
         WHEN market_slug LIKE 'tesla-deliveries%'       THEN 'narrow'
         WHEN market_slug LIKE 'tesla-production%'       THEN 'narrow'
         WHEN market_slug LIKE 'tesla-q%-deliveries%'    THEN 'narrow'
         WHEN market_slug LIKE 'spotify-daily-%'         THEN 'narrow'
         WHEN market_slug LIKE 'netflix-daily-%'         THEN 'narrow'
         WHEN market_slug LIKE 'netflix-rankings-%'      THEN 'narrow'
         WHEN market_slug LIKE 'app-store-%'             THEN 'narrow'
         WHEN market_slug LIKE '%-ceo-%resign%'          THEN 'narrow'
         WHEN market_slug LIKE 'will-%-resign%'          THEN 'narrow'
         WHEN market_slug LIKE 'will-%-step-down%'       THEN 'narrow'
         WHEN market_slug LIKE 'will-%-be-fired%'        THEN 'narrow'
         WHEN market_slug LIKE 'will-spacex-launch-%'    THEN 'narrow'
         WHEN market_slug LIKE 'spacex-launch-%'         THEN 'narrow'
         WHEN market_slug LIKE 'will-openai-%'           THEN 'narrow'
         WHEN market_slug LIKE 'will-mamdani-%before-2027%' THEN 'narrow'
         /* "will Mamdani do X" type post-election action markets — these
            are about an individual's action, not election outcome */
         WHEN market_slug LIKE 'will-%-praise-%'         THEN 'narrow'
         WHEN market_slug LIKE 'will-%-criticize-%'      THEN 'narrow'

         /* -------- POLITICS (excluded from main, but tagged) -------- */
         WHEN market_slug LIKE '%-election%'             THEN 'politics'
         WHEN market_slug LIKE '%-primary%'              THEN 'politics'
         WHEN market_slug LIKE '%-senate%'               THEN 'politics'
         WHEN market_slug LIKE '%-presidential-%'        THEN 'politics'
         WHEN market_slug LIKE '%-mayoral-%'             THEN 'politics'
         WHEN market_slug LIKE '%-governor-%'            THEN 'politics'
         WHEN market_slug LIKE '%-house-%'               THEN 'politics'
         WHEN market_slug LIKE '%-congress%'             THEN 'politics'
         WHEN market_slug LIKE 'will-trump-%'            THEN 'politics' /* fallback */
         WHEN market_slug LIKE 'will-biden-%'            THEN 'politics'
         WHEN market_slug LIKE 'will-harris-%'           THEN 'politics'
         WHEN market_slug LIKE 'will-%-win-%'            THEN 'politics'
         WHEN market_slug LIKE 'will-%-concede-%'        THEN 'politics'
         WHEN market_slug LIKE 'will-%-be-elected-%'     THEN 'politics'

         /* -------- SPORTS (excluded) -------- */
         WHEN market_slug LIKE 'nba-%'                   THEN 'sports'
         WHEN market_slug LIKE 'nfl-%'                   THEN 'sports'
         WHEN market_slug LIKE 'mlb-%'                   THEN 'sports'
         WHEN market_slug LIKE 'nhl-%'                   THEN 'sports'
         WHEN market_slug LIKE 'ncaab-%'                 THEN 'sports'
         WHEN market_slug LIKE 'ncaaf-%'                 THEN 'sports'
         WHEN market_slug LIKE 'ufc-%'                   THEN 'sports'
         WHEN market_slug LIKE 'soccer-%'                THEN 'sports'
         WHEN market_slug LIKE 'mls-%'                   THEN 'sports'
         WHEN market_slug LIKE 'epl-%'                   THEN 'sports'
         WHEN market_slug LIKE 'ucl-%'                   THEN 'sports'
         WHEN market_slug LIKE 'uel-%'                   THEN 'sports'
         WHEN market_slug LIKE 'tennis-%'                THEN 'sports'
         WHEN market_slug LIKE 'golf-%'                  THEN 'sports'
         WHEN market_slug LIKE 'cs2-%'                   THEN 'sports'
         WHEN market_slug LIKE 'lol-%'                   THEN 'sports'
         WHEN market_slug LIKE 'cricket-%'               THEN 'sports'
         WHEN market_slug LIKE '%-moneyline%'            THEN 'sports'
         WHEN market_slug LIKE '%-spread-%'              THEN 'sports'

         ELSE 'other'
       END AS category
FROM read_parquet('{GAMMA}')
""")

cls = con.execute("""
SELECT category, COUNT(*) AS n_markets
FROM mkt_class GROUP BY category ORDER BY n_markets DESC
""").fetchdf()
print(cls.to_string(index=False))

# -----------------------------------------------------------------
# 2. Pre-aggregate per (trader, day, market, decile, category), once.
# -----------------------------------------------------------------
print("\n>>> building pre-aggregated agg ...")
t0 = time.time()
con.execute(f"""
CREATE TEMP TABLE _mkt_life AS
SELECT t.conditionId, MIN(t.timestamp) AS t_min, MAX(t.timestamp) AS t_max
FROM read_parquet('{TRADES}') t
JOIN read_parquet('{RESOLUTIONS}') r USING (conditionId)
WHERE t.side='BUY'
GROUP BY t.conditionId HAVING MAX(t.timestamp) > MIN(t.timestamp)
""")

# Map token_id (= trades.conditionId) → market.condition_id via token_to_event,
# then map that to category via mkt_class.
con.execute("""
CREATE TEMP TABLE tok_category AS
SELECT te.token_id, mc.category
FROM read_parquet('/mnt/data/pipeline_data/token_to_event.parquet') te
JOIN mkt_class mc ON te.condition_id = mc.condition_id
""")

con.execute(f"""
CREATE TEMP TABLE agg AS
SELECT
  t.proxyWallet,
  CAST(DATE_TRUNC('day', to_timestamp(t.timestamp)) AS DATE) AS day,
  t.conditionId,
  CAST(FLOOR(t.price * 10) AS INTEGER) AS price_decile,
  COALESCE(tc.category, 'other') AS category,
  SUM((CASE WHEN t.outcome = r.winning_outcome THEN 1.0 ELSE 0.0 END) - t.price) AS sum_resid,
  SUM(t.price)                                                                   AS sum_price,
  SUM(CASE WHEN t.outcome = r.winning_outcome THEN 1.0 ELSE 0.0 END)             AS sum_won,
  SUM(((CASE WHEN t.outcome = r.winning_outcome THEN 1.0 ELSE 0.0 END) - t.price) *
      ((CASE WHEN t.outcome = r.winning_outcome THEN 1.0 ELSE 0.0 END) - t.price)) AS sum_resid_sq,
  COUNT(*) AS n_obs
FROM read_parquet('{TRADES}') t
JOIN read_parquet('{RESOLUTIONS}') r USING (conditionId)
JOIN _mkt_life m USING (conditionId)
LEFT JOIN read_parquet('{WALLET_FLAGS}') wf ON t.proxyWallet = wf.proxyWallet
LEFT JOIN tok_category tc ON t.conditionId = tc.token_id
WHERE t.side = 'BUY'
  AND COALESCE(wf.is_nonhuman, FALSE) = FALSE
  AND NOT ((t.price >= 0.99 OR t.price <= 0.01) AND
           (t.timestamp - m.t_min)*1.0/NULLIF(m.t_max-m.t_min,0) >= 0.99)
  AND FLOOR(t.price * 10) BETWEEN 0 AND 9
GROUP BY t.proxyWallet, day, t.conditionId, price_decile, category
""")
print(f"    agg rows: {con.execute('SELECT COUNT(*) FROM agg').fetchone()[0]:,}  "
      f"trades: {con.execute('SELECT SUM(n_obs) FROM agg').fetchone()[0]:,}  "
      f"({(time.time()-t0)/60:.1f} min)")

# Trade volume per category (post-filter)
print("\n>>> trades per category (post-filter):")
print(con.execute("""
SELECT category, SUM(n_obs) AS n_trades, COUNT(DISTINCT conditionId) AS n_tokens
FROM agg GROUP BY category ORDER BY n_trades DESC
""").fetchdf().to_string(index=False))

# -----------------------------------------------------------------
# 3. Per-category per-decile FLB + 3-way CGM SE
# -----------------------------------------------------------------
def cv(category: str, group_cols: str):
    """Cluster variance per decile for a category."""
    q = f"""
    WITH g AS (
      SELECT price_decile, SUM(sum_resid) AS s, SUM(n_obs) AS n
      FROM agg WHERE category='{category}'
      GROUP BY price_decile, {group_cols}
    ),
    m AS (
      SELECT price_decile, SUM(sum_resid)/SUM(n_obs) AS spread, SUM(n_obs) AS N
      FROM agg WHERE category='{category}' GROUP BY price_decile
    )
    SELECT g.price_decile,
           SUM((g.s - m.spread * g.n)*(g.s - m.spread * g.n)) / (m.N * m.N) AS V
    FROM g JOIN m USING (price_decile)
    GROUP BY g.price_decile, m.N
    ORDER BY g.price_decile
    """
    return con.execute(q).fetchdf().set_index("price_decile")["V"]

results = {}
for cat in ["broad", "narrow", "politics", "sports", "other"]:
    n_cat = con.execute(f"SELECT SUM(n_obs) FROM agg WHERE category='{cat}'").fetchone()[0] or 0
    if n_cat < 10000:
        print(f"\nskipping {cat}: only {n_cat:,} trades")
        continue
    print(f"\n>>> {cat}: {n_cat:,} trades")
    t0 = time.time()
    mean_df = con.execute(f"""
    SELECT price_decile,
           SUM(n_obs) AS n,
           SUM(sum_price)/SUM(n_obs) AS mean_price,
           SUM(sum_won)  /SUM(n_obs) AS mean_outcome,
           SUM(sum_resid)/SUM(n_obs) AS spread,
           SUM(sum_resid_sq)/SUM(n_obs) -
           (SUM(sum_resid)/SUM(n_obs))*(SUM(sum_resid)/SUM(n_obs)) AS var_resid
    FROM agg WHERE category='{cat}'
    GROUP BY price_decile ORDER BY price_decile
    """).fetchdf().set_index("price_decile")
    mean_df["SE_iid"] = (mean_df["var_resid"] / mean_df["n"]) ** 0.5

    Vs = {}
    for lbl, cols in [("T","proxyWallet"), ("D","day"), ("M","conditionId"),
                      ("TD","proxyWallet, day"), ("TM","proxyWallet, conditionId"),
                      ("DM","day, conditionId"),
                      ("TDM","proxyWallet, day, conditionId")]:
        Vs[lbl] = cv(cat, cols)
    V_3way = (Vs["T"]+Vs["D"]+Vs["M"]-Vs["TD"]-Vs["TM"]-Vs["DM"]+Vs["TDM"]).clip(lower=0)
    mean_df["SE_3way"] = V_3way.pow(0.5)
    mean_df["t_3way"]  = mean_df["spread"] / mean_df["SE_3way"]
    mean_df["bin_label"] = mean_df.index.map(lambda d: f"[{d/10:.1f},{(d+1)/10:.1f})")
    mean_df["category"] = cat
    results[cat] = mean_df
    print(mean_df[["n","mean_price","mean_outcome","spread","SE_iid","SE_3way","t_3way"]]
          .to_string(float_format=lambda x: f"{x:8.4g}"))
    print(f"    ({(time.time()-t0)/60:.1f} min)")

combined = pd.concat(results.values()).reset_index()
combined.to_csv(TABLES / "flb_by_category.csv", index=False)
print(f"\nwrote {TABLES / 'flb_by_category.csv'}")

# -----------------------------------------------------------------
# 4. Plot: per-category spread by decile (focus on narrow vs broad)
# -----------------------------------------------------------------
fig, axes = plt.subplots(1, 2, figsize=(13, 5.5), sharey=True)

for ax, cat, title_color in [
    (axes[0], "broad",  "(a) Broad-based"),
    (axes[1], "narrow", "(b) Narrow (single-name)"),
]:
    if cat not in results: continue
    d = results[cat]
    colors = ["#a02020" if v < 0 else "#1c6b3a" for v in d["spread"]]
    bars = ax.bar(d["bin_label"], d["spread"], color=colors, alpha=0.85,
                  edgecolor="black", linewidth=0.5, width=0.78)
    ax.errorbar(d["bin_label"], d["spread"], yerr=1.96 * d["SE_3way"],
                fmt="none", ecolor="black", linewidth=0.9, capsize=3, zorder=4)
    for bar, val in zip(bars, d["spread"]):
        y = val + 0.003 if val >= 0 else val - 0.003
        ax.text(bar.get_x()+bar.get_width()/2, y, f"{val:+.3f}",
                ha="center", va="bottom" if val>=0 else "top", fontsize=8)
    ax.axhline(0, color="black", linewidth=0.7)
    ax.set_xlabel("Price decile")
    n_tot = d['n'].sum()
    ax.set_title(f"{title_color}  · {n_tot/1e6:.1f}M trades",
                 loc="left", pad=8)
    ax.tick_params(axis="x", rotation=45, labelsize=8.5)
    ax.grid(axis="y", alpha=0.25, linestyle=":")

axes[0].set_ylabel("Spread = E[outcome] − E[price]")
fig.suptitle(
    "Polymarket FLB by contract type (Bartlett-O'Hara split, error bars = ±1.96 × CGM 3-way SE)",
    fontsize=12, y=1.005, fontweight="bold",
)
plt.tight_layout(rect=[0, 0, 1, 0.97])
plt.savefig(FIGS / "flb_narrow_broad.png")
print(f"wrote {FIGS / 'flb_narrow_broad.png'}")
