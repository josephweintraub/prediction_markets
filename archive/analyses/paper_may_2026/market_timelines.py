"""Three market lifecycle plots: spot price + volume + annotated events.

Academic styling. Plots extend past resolution to show the post-resolution
"pinned at $0.999" behavior. Price VWAP filters trades < $1 (dust) so a few
arbitrage trades on stale orders don't distort the daily price line; volume
bars count ALL trades so the post-resolution liquidity crater is visible.
"""
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime, timezone, timedelta
from pathlib import Path

ANALYSIS = Path("/mnt/data/analysis_output/paper")
FIGS = ANALYSIS / "figures"
FIGS.mkdir(exist_ok=True)

TRADES = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"

sns.set_theme(context="paper", style="white", font="DejaVu Serif")
plt.rcParams.update({
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold",
    "axes.labelsize": 11, "axes.linewidth": 0.8,
    "xtick.labelsize": 10, "ytick.labelsize": 10,
    "xtick.direction": "in", "ytick.direction": "in",
    "xtick.major.size": 4, "ytick.major.size": 4,
    "legend.frameon": False, "figure.dpi": 150,
    "savefig.dpi": 200, "savefig.bbox": None,
    "axes.spines.top": False, "axes.spines.right": True,
})

PRICE_COLOR = "#1a3d6d"
VOLUME_COLOR = "#c0c0c0"
PRE_GAME_COLOR = "#c47220"
IN_GAME_COLOR = "#3777a9"
RESOLUTION_COLOR = "#196f3d"
DEBATE_COLOR = "#922b21"

con = duckdb.connect()
con.execute("SET memory_limit='32GB'")
con.execute("SET threads=16")
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET preserve_insertion_order=false")


def trades_for(token_id: str, bucket_seconds: int,
               ts_min: int | None = None, ts_max: int | None = None,
               dust_threshold: float = 1.0) -> pd.DataFrame:
    """Return per-bucket (vwap, volume, n_trades).

    `vwap` is computed only from trades with usdcSize >= dust_threshold so that
    a handful of sub-$1 arbitrage trades on stale orders don't drag the bucket
    price. `volume` and `n_trades` use ALL trades so the liquidity profile
    (including post-resolution drop) stays accurate.
    """
    where = [f"conditionId = '{token_id}'"]
    if ts_min is not None: where.append(f"timestamp >= {ts_min}")
    if ts_max is not None: where.append(f"timestamp <= {ts_max}")
    df = con.execute(f"""
    SELECT
      timestamp - (timestamp % {bucket_seconds}) AS bucket_ts,
      SUM(CASE WHEN usdcSize >= {dust_threshold}
               THEN usdcSize * price END)
        / NULLIF(SUM(CASE WHEN usdcSize >= {dust_threshold}
                          THEN usdcSize END), 0) AS vwap,
      SUM(usdcSize) AS volume,
      COUNT(*) AS n_trades
    FROM read_parquet('{TRADES}')
    WHERE {' AND '.join(where)}
    GROUP BY bucket_ts ORDER BY bucket_ts
    """).fetchdf()
    df["dt"] = pd.to_datetime(df["bucket_ts"], unit="s", utc=True)
    return df


def plot_market(df, title, caption, events, out_path,
                ymin=0.0, ymax=1.0, date_format="auto", log_volume=True):
    fig, ax_price = plt.subplots(figsize=(11.5, 5.8))
    ax_vol = ax_price.twinx()

    bar_width = ((df["dt"].iloc[1] - df["dt"].iloc[0]).total_seconds() / 86400) \
                if len(df) > 1 else 0.01
    plot_df = df.copy()
    if log_volume:
        plot_df = plot_df[plot_df["volume"] > 0]
    ax_vol.bar(plot_df["dt"], plot_df["volume"], width=bar_width, color=VOLUME_COLOR,
               alpha=0.55, edgecolor="none", zorder=1)
    ax_vol.set_ylabel("Volume (USDC, per bucket; log scale)" if log_volume
                      else "Volume (USDC, per bucket)",
                      fontsize=10, color="#666")
    ax_vol.tick_params(axis="y", colors="#666", labelsize=9)
    ax_vol.spines["right"].set_color("#aaa")
    ax_vol.spines["top"].set_visible(False)
    if log_volume:
        ax_vol.set_yscale("log")
        lo = max(10, plot_df["volume"].min())
        hi = plot_df["volume"].max() * 2
        ax_vol.set_ylim(lo, hi)
    else:
        ax_vol.yaxis.get_major_formatter().set_scientific(True)
        ax_vol.yaxis.get_major_formatter().set_powerlimits((0, 0))
        if len(df) > 8:
            cap = df["volume"].quantile(0.97) * 1.4
            if cap > 0 and df["volume"].max() > cap * 2:
                ax_vol.set_ylim(0, cap)

    ax_price.plot(df["dt"], df["vwap"], color=PRICE_COLOR, linewidth=1.6, zorder=3)
    ax_price.set_ylabel("Contract price (implied probability)", fontsize=11, color=PRICE_COLOR)
    ax_price.tick_params(axis="y", colors=PRICE_COLOR)
    ax_price.set_ylim(ymin, ymax)
    ax_price.spines["left"].set_color(PRICE_COLOR)

    base_heights = [0.96, 0.86, 0.76, 0.66, 0.56, 0.46]
    for i, (evt_dt, label, color) in enumerate(events):
        ax_price.axvline(evt_dt, color=color, linestyle="-", linewidth=1.3,
                         alpha=0.55, zorder=2)
        y = ymin + (ymax - ymin) * base_heights[i % len(base_heights)]
        ax_price.annotate(label, xy=(evt_dt, y),
                          xytext=(5, 0), textcoords="offset points",
                          va="center", ha="left",
                          fontsize=8.5, color=color, fontweight="bold",
                          bbox=dict(boxstyle="round,pad=0.18", fc="white",
                                    ec=color, alpha=0.95, linewidth=0.7))

    ax_price.set_xlabel("Time (UTC)", fontsize=11)
    ax_price.set_title(title, fontsize=13, loc="left", pad=28, fontweight="bold")
    ax_price.text(0.0, 1.04, caption, transform=ax_price.transAxes,
                  fontsize=10, color="#444", style="italic")
    ax_price.grid(alpha=0.25, linestyle=":")
    ax_price.set_zorder(ax_vol.get_zorder() + 1)
    ax_price.patch.set_visible(False)

    span_days = (df["dt"].iloc[-1] - df["dt"].iloc[0]).total_seconds() / 86400
    if date_format == "hour" or (date_format == "auto" and span_days <= 2):
        ax_price.xaxis.set_major_locator(mdates.HourLocator(interval=4))
        ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%b %d %H:%M"))
    elif date_format == "day" or (date_format == "auto" and span_days <= 60):
        every = max(1, int(span_days // 10))
        ax_price.xaxis.set_major_locator(mdates.DayLocator(interval=every))
        ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    else:
        ax_price.xaxis.set_major_locator(mdates.MonthLocator())
        ax_price.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    fig.autofmt_xdate(rotation=30)

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    plt.savefig(out_path, pad_inches=0.3)
    print("wrote", out_path)
    plt.close(fig)


# ---------------------------------------------------------------------------
# 1. Mamdani — daily buckets, full lifetime (extends ~2 weeks past election)
# ---------------------------------------------------------------------------
print("=== Mamdani ===")
mam_tok = "33945469250963963541781051637999677727672635213493648594066577298999471399137"
mam_end = int(datetime(2025, 11, 22, tzinfo=timezone.utc).timestamp())
mam = trades_for(mam_tok, 86400, ts_max=mam_end)
print(f"  rows: {len(mam)}, vol: \${mam['volume'].sum():,.0f}")

mamdani_events = [
    (datetime(2025, 6, 4, 23, tzinfo=timezone.utc),   "Dem primary debate 1",  DEBATE_COLOR),
    (datetime(2025, 6, 12, 23, tzinfo=timezone.utc),  "Dem primary debate 2",  DEBATE_COLOR),
    (datetime(2025, 6, 24, 4, tzinfo=timezone.utc),   "Primary election",      PRE_GAME_COLOR),
    (datetime(2025, 10, 16, 23, tzinfo=timezone.utc), "General debate 1",      DEBATE_COLOR),
    (datetime(2025, 10, 22, 23, tzinfo=timezone.utc), "General debate 2",      DEBATE_COLOR),
    (datetime(2025, 11, 4, 23, tzinfo=timezone.utc),  "Election night",        RESOLUTION_COLOR),
]
plot_market(
    mam,
    title="Mamdani — 2025 NYC mayoral election (YES outcome)",
    caption=f"Daily VWAP and total volume. \${mam['volume'].sum()/1e6:.0f}M traded, {mam['n_trades'].sum():,} fills.",
    events=mamdani_events, out_path=FIGS / "market_mamdani.png",
)

# ---------------------------------------------------------------------------
# 2. FOMC March 2026 — daily buckets, extends a week past decision
# ---------------------------------------------------------------------------
print("=== FOMC March 2026 ===")
fomc_tok = "102559817034631022221500208641784929295731053857601013029449249654006364919935"
fomc_end = int(datetime(2026, 3, 25, tzinfo=timezone.utc).timestamp())
fomc = trades_for(fomc_tok, 86400, ts_max=fomc_end)
print(f"  rows: {len(fomc)}, vol: \${fomc['volume'].sum():,.0f}")

fomc_events = [
    (datetime(2026, 3, 18, 18, tzinfo=timezone.utc), "FOMC decision (2pm ET Mar 18)", RESOLUTION_COLOR),
]
plot_market(
    fomc, title="FOMC March 2026 — 'No change' outcome (YES)",
    caption=f"Daily VWAP and total volume. \${fomc['volume'].sum()/1e6:.0f}M traded, {fomc['n_trades'].sum():,} fills. Fed held rates.",
    events=fomc_events, out_path=FIGS / "market_fomc_march2026.png",
)

# ---------------------------------------------------------------------------
# 3. NBA Game 7 — 5-min buckets, ~6h pre-tip to ~6h post-buzzer
# ---------------------------------------------------------------------------
print("=== NBA Finals Game 7 ===")
nba_tok = "69141011565843886031610727865262655939831537076627474153116671936676512312401"
window_start = int(datetime(2025, 6, 22, 22, tzinfo=timezone.utc).timestamp())
window_end   = int(datetime(2025, 6, 23,  9, tzinfo=timezone.utc).timestamp())
nba = trades_for(nba_tok, 300, ts_min=window_start, ts_max=window_end)
print(f"  rows: {len(nba)}, vol: \${nba['volume'].sum():,.0f}")

tip = datetime(2025, 6, 23, 0, 0, tzinfo=timezone.utc)
nba_events = [
    (tip + timedelta(minutes=0),    "Tip-off (8:00 PM ET)",    PRE_GAME_COLOR),
    (tip + timedelta(minutes=51),   "Q1 end  (8:51 PM)",       IN_GAME_COLOR),
    (tip + timedelta(minutes=100),  "Halftime end  (9:40 PM)", IN_GAME_COLOR),
    (tip + timedelta(minutes=130),  "Q3 end  (10:10 PM)",      IN_GAME_COLOR),
    (tip + timedelta(minutes=165),  "Final buzzer  (10:45 PM)",RESOLUTION_COLOR),
]
plot_market(
    nba, title="2025 NBA Finals Game 7 — Thunder moneyline (Pacers @ OKC, June 22)",
    caption=f"5-min VWAP and total volume. \${nba['volume'].sum()/1e6:.1f}M traded. OKC won 103–91.",
    events=nba_events, out_path=FIGS / "market_nba_game7.png",
    date_format="hour", log_volume=False,
)
