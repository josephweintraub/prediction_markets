"""Plots: 10-panel calibration grid + spread-ranking bar chart."""
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def plot_dimension_calibration(calib_dim, title, ax):
    """One subplot: overlaid calibration curves for all slices of a dimension."""
    slices = sorted(calib_dim["slice"].unique())
    cmap = plt.cm.tab10 if len(slices) <= 10 else plt.cm.tab20
    ax.plot([0, 1], [0, 1], "--", color="gray", lw=1, alpha=0.7)
    for i, slc in enumerate(slices):
        sub = calib_dim[calib_dim["slice"] == slc].sort_values("decile")
        if len(sub) < 3:
            continue
        ax.plot(sub["impl_prob"], sub["win_rate"], "o-",
                color=cmap(i % cmap.N), label=str(slc)[:30], lw=1.5, ms=4)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_xlabel("Implied prob")
    ax.set_ylabel("Win rate")
    ax.set_title(title, fontsize=10)
    ax.legend(loc="upper left", fontsize=7, framealpha=0.85)


def plot_all_dimensions_grid(calib_df, out_path, ncols=3):
    dims = list(calib_df["dim"].drop_duplicates())
    n = len(dims)
    nrows = math.ceil(n / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(5.5 * ncols, 4.5 * nrows))
    axes = np.atleast_1d(axes).flatten()
    for i, d in enumerate(dims):
        plot_dimension_calibration(calib_df[calib_df["dim"] == d], d, axes[i])
    for j in range(len(dims), len(axes)):
        axes[j].axis("off")
    fig.suptitle("FLB Calibration by Learnability Dimension (50-80% lifecycle, 2W SE)",
                 fontsize=14, y=1.005)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    return fig


def plot_spread_ranking(ranking, out_path):
    fig, ax = plt.subplots(figsize=(9, 0.45 * len(ranking) + 2))
    y = np.arange(len(ranking))
    ax.barh(y, ranking["spread_variance"].values, color="#2196f3")
    ax.set_yticks(y)
    ax.set_yticklabels(ranking["dim"].values, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Within-dimension spread variance")
    ax.set_title("Dimensions ranked by within-dim variance of D10–D1 spread (clean slices)",
                 fontsize=11)
    for i, (_, r) in enumerate(ranking.iterrows()):
        ax.text(r["spread_variance"], i,
                f" {r['n_slices']} slices | range {r['min_spread']:+.3f} to {r['max_spread']:+.3f}",
                va="center", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    return fig


def rank_dimensions_by_spread_variance(summary_df, min_trades_per_tail=2000):
    """Filter to clean slices (sufficient trades), compute within-dim variance of spread_2w."""
    df = summary_df.copy()
    if "spread_2w" not in df.columns:
        raise ValueError("expected spread_2w column")
    # Filter: enough trades and finite spread
    clean = df[df["spread_2w"].notna()].copy()
    rows = []
    for d, g in clean.groupby("dim"):
        if len(g) < 2:
            continue
        rows.append({
            "dim": d,
            "n_slices": len(g),
            "spread_variance": float(g["spread_2w"].var()),
            "spread_range": float(g["spread_2w"].max() - g["spread_2w"].min()),
            "min_spread": float(g["spread_2w"].min()),
            "max_spread": float(g["spread_2w"].max()),
            "min_slice": g.loc[g["spread_2w"].idxmin(), "slice"],
            "max_slice": g.loc[g["spread_2w"].idxmax(), "slice"],
        })
    return pd.DataFrame(rows).sort_values("spread_variance", ascending=False).reset_index(drop=True)
