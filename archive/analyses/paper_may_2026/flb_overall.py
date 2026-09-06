"""Overall FLB by deciles — academic-styled plot. Uses cached CSVs from flb_with_bots."""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

ANALYSIS = Path("/mnt/data/analysis_output/paper")
TABLES = ANALYSIS / "tables"
FIGS = ANALYSIS / "figures"

# Academic styling
sns.set_theme(context="paper", style="white", font="DejaVu Serif")
plt.rcParams.update({
    "font.size": 11, "axes.titlesize": 13, "axes.titleweight": "bold",
    "axes.labelsize": 11, "axes.linewidth": 0.8,
    "xtick.labelsize": 9.5, "ytick.labelsize": 9.5,
    "xtick.direction": "in", "ytick.direction": "in",
    "legend.frameon": False, "figure.dpi": 150,
    "savefig.dpi": 200, "savefig.bbox": "tight",
    "axes.spines.top": False, "axes.spines.right": False,
})

NEG = "#a02020"   # muted red
POS = "#1c6b3a"   # muted green
CAL = "#1a3d6d"   # navy

df_all = pd.read_csv(TABLES / "flb_deciles_all.csv")
df_h   = pd.read_csv(TABLES / "flb_deciles_human.csv")
for df in (df_all, df_h):
    df["bin_label"] = df["decile"].map(lambda d: f"[{d/10:.1f},{(d+1)/10:.1f})")
    df["return_per_dollar"] = df["spread"] / df["mean_price"]

fig, axes = plt.subplots(2, 2, figsize=(13, 9))

def calibration_plot(ax, df, title):
    ax.plot([0, 1], [0, 1], color="grey", linestyle="--", linewidth=0.8,
            label="Perfect calibration", zorder=1)
    ax.scatter(df["mean_price"], df["mean_outcome"], s=80, color=CAL,
               edgecolor="black", linewidth=0.6, zorder=3)
    for _, r in df.iterrows():
        ax.annotate(f"n={r['n_trades']/1e6:.0f}M",
                    (r["mean_price"], r["mean_outcome"]),
                    textcoords="offset points", xytext=(7, -10),
                    fontsize=8, color="#555")
    ax.set_xlabel("Mean implied probability")
    ax.set_ylabel("Realized win rate")
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    ax.set_aspect("equal")
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(alpha=0.25, linestyle=":")
    ax.set_title(title, loc="left", pad=8)


def spread_plot(ax, df, title):
    colors = [NEG if v < 0 else POS for v in df["spread"]]
    bars = ax.bar(df["bin_label"], df["spread"], color=colors, alpha=0.85,
                  edgecolor="black", linewidth=0.5, width=0.78)
    for bar, val in zip(bars, df["spread"]):
        y = val + 0.0008 if val >= 0 else val - 0.0008
        va = "bottom" if val >= 0 else "top"
        ax.text(bar.get_x() + bar.get_width()/2, y, f"{val:+.3f}",
                ha="center", va=va, fontsize=8.5)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Price decile")
    ax.set_ylabel("Spread = realized − implied")
    ax.set_ylim(-0.03, 0.03)
    ax.tick_params(axis="x", rotation=45, labelsize=8.5)
    ax.grid(axis="y", alpha=0.25, linestyle=":")
    ax.set_title(title, loc="left", pad=8)


calibration_plot(axes[0, 0], df_all, "(a) All wallets — calibration")
spread_plot(    axes[0, 1], df_all, "(b) All wallets — FLB spread")
calibration_plot(axes[1, 0], df_h,   "(c) Humans only — calibration")
spread_plot(    axes[1, 1], df_h,   "(d) Humans only — FLB spread")

fig.suptitle(
    f"Polymarket Favorite-Longshot Bias  ·  "
    f"{df_all['n_trades'].sum()/1e6:.0f}M BUY trades total, "
    f"{df_h['n_trades'].sum()/1e6:.0f}M after bot filter (humans only)",
    fontsize=13, y=1.005, fontweight="bold",
)
plt.tight_layout(rect=[0, 0, 1, 0.99])
plt.savefig(FIGS / "flb_overall.png")
print("wrote", FIGS / "flb_overall.png")
