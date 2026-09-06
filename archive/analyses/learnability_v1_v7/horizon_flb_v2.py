"""Horizon-matched FLB on the de-censored clean set, with labels v2.

Reruns the horizon x period calibration design (originally fixed_flb.py,
2026-07-01, lost with EC2 /tmp) as a committed script, on the refreshed
canonical trade set (2,036,128,538 rows, resolutions snapshot 2026-07-03)
with the rebuilt wallet_flags/market_flags and market_labels_v2.

Design (pre-specified; see docs/methods_reference.md for the standard spec):
  A. Cells = time bucket (2022-23 / 2024 / 2025 / 2026 Jan-Apr / 2026 May-Jun,
     by trade date) x horizon class (market lifetime from first to last
     BUY-side trade across the market's tokens: <=1d / 1-7d / 7-30d /
     30-120d / >120d). Mature window (25-80% of token lifetime) primary;
     full window as robustness.
  B. Topic x time bucket (labels-v2 topics), 2025 onward, mature window;
     event_family='Iran' reported as a supplemental row.
  C. Three pre-specified subcategory contrasts (mature window; pooled
     2025-2026 primary, per-bucket secondary):
       1. Sports: field/outright winner vs matchup (team game + head-to-head)
       2. Crypto: memecoin-tagged vs major (Bitcoin/Ethereum), overlap excluded
       3. Politics: elections vs appointments & confirmations
  Measures per cell: signed calibration slope (trade-level OLS of won on
  price; PRIMARY) and the D10-D1 cal-error spread (secondary), each
  count- and dollar-weighted, with CGM 3-way clustered SEs
  (day x wallet x market). Slice floor 5,000 trades; dropped cells logged.
  Filters: BUY side, 0.01<price<0.99, market-level up/down exclusion
  (market_flags.is_updown), behavioral bot exclusion (wallet_flags
  is_nonhuman), resolved markets only (winning_outcome IS NOT NULL).

Outputs (all under /mnt/data/learnability/output/):
  horizon_flb_v2_trades.parquet/   intermediate trade-level cache (rebuildable)
  horizon_flb_v2_cells.parquet     A (both windows)
  horizon_flb_v2_topics.parquet    B
  horizon_flb_v2_contrasts.parquet C
  horizon_flb_v2_deciles.parquet   full decile tables for every reported cell
  horizon_flb_v2_summary.json      run metadata + filter-stage counts
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from flb_per_slice import (  # noqa: E402
    compute_3way_decile_table,
    sig_stars,
)

TRADES_GLOB = "/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet"
MARKET_FLAGS = "/mnt/data/pipeline_output/market_flags.parquet"
WALLET_FLAGS = "/mnt/data/learnability/cache/wallet_flags.parquet"
LABELS = "/mnt/data/learnability/native/market_labels_v2.parquet"
OUT_DIR = Path("/mnt/data/learnability/output")
INTERMEDIATE = OUT_DIR / "horizon_flb_v2_trades.parquet"

MIN_TRADES = 5000
MATURE = (0.25, 0.80)

MEME_TAGS = ["Dogecoin", "Memecoins", "Shiba Inu", "Pepe", "PEPE", "Bonk",
             "Floki", "dogwifhat", "WIF", "SLERF", "Fartcoin", "PENGU"]
MAJOR_TAGS = ["Bitcoin", "Ethereum"]

TB_ORDER = ["1 · 2022-23", "2 · 2024", "3 · 2025", "4 · 2026 Jan-Apr",
            "5 · 2026 May-Jun"]
HZ_ORDER = ["a ≤1d", "b 1-7d", "c 7-30d", "d 30-120d", "e >120d"]
TOPIC_TBS = ["3 · 2025", "4 · 2026 Jan-Apr", "5 · 2026 May-Jun"]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------- slope with CGM 3-way clustered SE ----------

def threeway_cluster_slope(y, x, w, c1, c2, c3):
    """Weighted OLS slope of y on x with a Cameron-Gelbach-Miller 3-way
    clustered SE. Pass w=None for the count-weighted (unweighted) version.
    Mirrors the inclusion-exclusion pattern of flb_per_slice.threeway_cluster_se.
    """
    y = np.asarray(y, float)
    x = np.asarray(x, float)
    n = len(y)
    if n == 0:
        return np.nan, np.nan
    w = np.ones(n) if w is None else np.asarray(w, float)
    W = w.sum()
    if W <= 0:
        return np.nan, np.nan
    xbar = (w * x).sum() / W
    ybar = (w * y).sum() / W
    xt = x - xbar
    D = (w * xt * xt).sum()
    if D <= 0:
        return np.nan, np.nan
    b = (w * xt * y).sum() / D
    a = ybar - b * xbar
    u = w * xt * (y - a - b * x)  # per-trade score

    def cv(codes):
        groups = pd.Series(u).groupby(codes).sum()
        return (groups ** 2).sum()

    c1c, _ = pd.factorize(c1, sort=False)
    c2c, _ = pd.factorize(c2, sort=False)
    c3c, _ = pd.factorize(c3, sort=False)
    c1c = c1c.astype(np.int64)
    c2c = c2c.astype(np.int64)
    c3c = c3c.astype(np.int64)
    n2 = int(c2c.max()) + 1
    n3 = int(c3c.max()) + 1
    c12 = c1c * n2 + c2c
    c13 = c1c * n3 + c3c
    c23 = c2c * n3 + c3c
    c123 = c12 * n3 + c3c
    var = (cv(c1c) + cv(c2c) + cv(c3c)
           - cv(c12) - cv(c13) - cv(c23) + cv(c123)) / (D ** 2)
    return float(b), float(np.sqrt(max(var, 0.0)))


# ---------- intermediate build (the one heavy scan) ----------

def build_intermediate(con) -> None:
    if INTERMEDIATE.exists():
        log(f"intermediate exists, reusing: {INTERMEDIATE}")
        return
    log("building intermediate trade-level cache (single heavy scan) ...")
    con.execute(f"""
        CREATE OR REPLACE VIEW trades_all AS
        SELECT * FROM read_parquet('{TRADES_GLOB}', hive_partitioning=1)
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE nonhuman AS
        SELECT proxyWallet FROM read_parquet('{WALLET_FLAGS}')
        WHERE is_nonhuman
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE flags AS
        SELECT token_id, market_id, winning_outcome
        FROM read_parquet('{MARKET_FLAGS}')
        WHERE winning_outcome IS NOT NULL AND NOT is_updown
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE lab AS
        SELECT condition_id, topic, subcategory, event_family,
               CASE
                 WHEN topic = 'Sports' AND subcategory IN
                      ('Field / Outright Winner (motorsport & golf)',
                       'Field/Outright Winner') THEN 'field_winner'
                 WHEN topic = 'Sports' AND subcategory IN
                      ('Team-Sport Game Markets',
                       'Individual Head-to-Head (combat & racquet)',
                       'Individual Head-to-Head') THEN 'matchup'
               END AS sports_arm,
               CASE
                 WHEN topic = 'Crypto' AND
                      (subcategory = 'Memecoin Price Markets'
                       OR list_has_any(entity_tags, {MEME_TAGS!r}))
                      AND NOT list_has_any(entity_tags, {MAJOR_TAGS!r})
                      THEN 'memecoin'
                 WHEN topic = 'Crypto' AND
                      list_has_any(entity_tags, {MAJOR_TAGS!r})
                      AND NOT (subcategory = 'Memecoin Price Markets'
                               OR list_has_any(entity_tags, {MEME_TAGS!r}))
                      THEN 'major'
               END AS crypto_arm,
               CASE
                 WHEN topic = 'Politics' AND subcategory = 'Elections'
                      THEN 'elections'
                 WHEN topic = 'Politics' AND subcategory IN
                      ('Appointments & Confirmations', 'Appointments')
                      THEN 'appointments'
               END AS politics_arm
        FROM read_parquet('{LABELS}')
    """)
    # bot-excluded BUY tape (mirrors run_phase1's trades/trades_buy views)
    con.execute("""
        CREATE OR REPLACE VIEW trades_buy AS
        SELECT t.proxyWallet, t.timestamp, t.conditionId, t.usdcSize,
               t.price, t.outcome
        FROM trades_all t
        WHERE t.side = 'BUY'
          AND coalesce(t.eventSlug, '') NOT LIKE '%updown%'
          AND coalesce(t.eventSlug, '') NOT LIKE '%up-or-down%'
          AND t.proxyWallet NOT IN (SELECT proxyWallet FROM nonhuman)
    """)
    con.execute(f"""
        COPY (
            WITH tok_life AS (
                SELECT conditionId,
                       MIN(timestamp) AS tok_start,
                       GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS tok_dur
                FROM trades_buy
                GROUP BY conditionId
            ),
            mkt_life AS (
                SELECT f.market_id,
                       (MAX(t.timestamp) - MIN(t.timestamp)) / 86400.0 AS life_d
                FROM trades_buy t
                JOIN flags f ON t.conditionId = f.token_id
                GROUP BY f.market_id
            )
            SELECT
                t.proxyWallet,
                f.market_id,
                DATE_TRUNC('day', to_timestamp(t.timestamp)) AS trade_day,
                t.price,
                CAST(t.outcome = f.winning_outcome AS INT) AS won,
                CASE WHEN t.outcome = f.winning_outcome
                     THEN 1.0 - t.price ELSE -t.price END AS ret,
                t.usdcSize AS usdc,
                LEAST(FLOOR(t.price * 10)::INT, 9) + 1 AS decile,
                (t.timestamp - tl.tok_start)::FLOAT / tl.tok_dur AS pos,
                ml.life_d,
                CASE
                    WHEN ml.life_d <= 1   THEN 'a ≤1d'
                    WHEN ml.life_d <= 7   THEN 'b 1-7d'
                    WHEN ml.life_d <= 30  THEN 'c 7-30d'
                    WHEN ml.life_d <= 120 THEN 'd 30-120d'
                    ELSE 'e >120d'
                END AS hclass,
                CASE
                    WHEN t.timestamp <  1704067200 THEN '1 · 2022-23'
                    WHEN t.timestamp <  1735689600 THEN '2 · 2024'
                    WHEN t.timestamp <  1767225600 THEN '3 · 2025'
                    WHEN t.timestamp <  1777593600 THEN '4 · 2026 Jan-Apr'
                    ELSE '5 · 2026 May-Jun'
                END AS tb,
                CASE
                    WHEN t.timestamp <  1704067200 THEN 1
                    WHEN t.timestamp <  1735689600 THEN 2
                    WHEN t.timestamp <  1767225600 THEN 3
                    WHEN t.timestamp <  1777593600 THEN 4
                    ELSE 5
                END AS tb_code,
                l.topic, l.event_family,
                l.sports_arm, l.crypto_arm, l.politics_arm
            FROM trades_buy t
            JOIN flags f     ON t.conditionId = f.token_id
            JOIN tok_life tl ON t.conditionId = tl.conditionId
            JOIN mkt_life ml ON f.market_id = ml.market_id
            LEFT JOIN lab l  ON f.market_id = l.condition_id
            WHERE t.price > 0.01 AND t.price < 0.99
        ) TO '{INTERMEDIATE}' (FORMAT PARQUET, PARTITION_BY (tb_code))
    """)
    log("intermediate written")


# ---------- per-cell measurement ----------

def cell_stats(sub: pd.DataFrame) -> dict:
    """Engine decile table + spread, plus slope (count & dollar) with CGM SEs."""
    dec, s3 = compute_3way_decile_table(sub, n_bins=10)
    b, b_se = threeway_cluster_slope(sub["won"], sub["price"], None,
                                     sub["trade_day"], sub["proxyWallet"],
                                     sub["market_id"])
    bd, bd_se = threeway_cluster_slope(sub["won"], sub["price"], sub["usdc"],
                                       sub["trade_day"], sub["proxyWallet"],
                                       sub["market_id"])
    return {
        "n": int(len(sub)),
        "n_markets": int(sub["market_id"].nunique()),
        "usd": float(sub["usdc"].sum()),
        "med_life_d": float(sub["life_d"].median()),
        "slope": b, "slope_se": b_se,
        "slope_t": (b - 1.0) / b_se if b_se and b_se > 0 else np.nan,
        "slope_dol": bd, "slope_dol_se": bd_se,
        "slope_dol_t": (bd - 1.0) / bd_se if bd_se and bd_se > 0 else np.nan,
        "spread": s3["spread"], "spread_se": s3["spread_se"],
        "spread_t": s3["spread_t"],
        "spread_dol": s3["spread_dol"], "spread_dol_se": s3["spread_se_dol"],
        "spread_dol_t": s3["spread_t_dol"],
        "_deciles": dec,
    }


def run_group(con, where: str, group_label_sql: str, window, tag: str,
              deciles_acc: list) -> pd.DataFrame:
    """Fetch trades for `where`, group by the label expr, measure each group."""
    lo, hi = window
    q = f"""
        SELECT {group_label_sql} AS grp, proxyWallet, market_id, trade_day,
               price, won, ret, usdc, decile, life_d
        FROM read_parquet('{INTERMEDIATE}/**/*.parquet', hive_partitioning=1)
        WHERE ({where}) AND pos >= {lo} AND pos <= {hi}
    """
    df = con.execute(q).fetchdf()
    rows = []
    for grp, sub in df.groupby("grp", sort=False, observed=True):
        if len(sub) < MIN_TRADES:
            log(f"    {tag} / {grp}: DROPPED (n={len(sub):,} < {MIN_TRADES})")
            continue
        st = cell_stats(sub)
        dec = st.pop("_deciles")
        dec.insert(0, "cell", f"{tag}|{grp}")
        deciles_acc.append(dec)
        st["cell"] = str(grp)
        rows.append(st)
        log(f"    {tag} / {grp}: n={st['n']:,} slope={st['slope']:.3f} "
            f"(t vs 1: {st['slope_t']:+.2f}{sig_stars(st['slope_t'])}) "
            f"spread={st['spread']:+.4f} (t={st['spread_t']:+.2f})")
    out = pd.DataFrame(rows)
    out.insert(0, "analysis", tag)
    return out


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")
    t0 = time.time()

    build_intermediate(con)

    counts = con.execute(f"""
        SELECT tb, count(*) n, count(DISTINCT market_id) n_mkts,
               sum(usdc) usd
        FROM read_parquet('{INTERMEDIATE}/**/*.parquet', hive_partitioning=1)
        GROUP BY tb ORDER BY tb
    """).fetchdf()
    log("intermediate composition:\n" + counts.to_string())

    deciles: list = []
    results = []

    # A. horizon x period cells, mature + full
    for wname, window in [("mature_25_80", MATURE), ("full_0_100", (0.0, 1.0))]:
        log(f"=== A: tb x hclass, window={wname} ===")
        for tb in TB_ORDER:
            r = run_group(con, f"tb = '{tb}'", "hclass", window,
                          f"A_{wname}|{tb}", deciles)
            if len(r):
                r.insert(1, "tb", tb)
                r.insert(2, "window", wname)
                results.append(("cells", r))

    # B. topic x period (2025+), mature; Iran family supplemental
    log("=== B: topic x tb (mature) ===")
    for tb in TOPIC_TBS:
        r = run_group(con, f"tb = '{tb}' AND topic IS NOT NULL", "topic",
                      MATURE, f"B_topic|{tb}", deciles)
        if len(r):
            r.insert(1, "tb", tb)
            results.append(("topics", r))
        r = run_group(con, f"tb = '{tb}' AND event_family = 'Iran'",
                      "'Iran (event family)'", MATURE, f"B_iran|{tb}", deciles)
        if len(r):
            r.insert(1, "tb", tb)
            results.append(("topics", r))

    # C. pre-specified contrasts (mature): pooled 2025+ primary, per-tb secondary
    log("=== C: pre-specified contrasts (mature) ===")
    pooled = "tb IN ('3 · 2025', '4 · 2026 Jan-Apr', '5 · 2026 May-Jun')"
    for cname, col in [("sports_field_vs_matchup", "sports_arm"),
                       ("crypto_meme_vs_major", "crypto_arm"),
                       ("politics_elec_vs_appt", "politics_arm")]:
        r = run_group(con, f"{pooled} AND {col} IS NOT NULL", col, MATURE,
                      f"C_{cname}|pooled", deciles)
        if len(r):
            r.insert(1, "tb", "pooled 2025-26")
            r.insert(2, "contrast", cname)
            results.append(("contrasts", r))
        for tb in TOPIC_TBS:
            r = run_group(con, f"tb = '{tb}' AND {col} IS NOT NULL", col,
                          MATURE, f"C_{cname}|{tb}", deciles)
            if len(r):
                r.insert(1, "tb", tb)
                r.insert(2, "contrast", cname)
                results.append(("contrasts", r))

    for name in ["cells", "topics", "contrasts"]:
        frames = [r for tag, r in results if tag == name]
        if frames:
            df = pd.concat(frames, ignore_index=True)
            df.to_parquet(OUT_DIR / f"horizon_flb_v2_{name}.parquet")
            log(f"wrote horizon_flb_v2_{name}.parquet ({len(df)} rows)")
    if deciles:
        pd.concat(deciles, ignore_index=True).to_parquet(
            OUT_DIR / "horizon_flb_v2_deciles.parquet")

    summary = {
        "run_finished_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "trades_glob": TRADES_GLOB,
        "resolutions_snapshot": "2026-07-03",
        "labels": LABELS,
        "min_trades_floor": MIN_TRADES,
        "windows": {"primary": "mature 25-80%", "robustness": "full 0-100%"},
        "slope_note": "trade-level OLS won~price; t is vs slope=1; CGM 3-way "
                      "clustered (day x wallet x market)",
        "intermediate_counts_by_tb": counts.to_dict(orient="records"),
        "elapsed_min": round((time.time() - t0) / 60, 1),
    }
    (OUT_DIR / "horizon_flb_v2_summary.json").write_text(
        json.dumps(summary, indent=2, default=str))
    log(f"DONE in {summary['elapsed_min']} min")


if __name__ == "__main__":
    main()
