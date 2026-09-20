"""Render the multisport FLB time-regression artifacts as a concise LaTeX report."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,
    fingerprint,
    fresh_run,
    quoted,
    write_json,
)


SPORTS = ("mlb", "nfl", "nba", "nhl", "cbb", "atp", "epl", "cfb", "wnba")
SPORT_LABELS = {
    "mlb": "MLB", "nfl": "NFL", "nba": "NBA", "nhl": "NHL",
    "cbb": "Men's CBB", "atp": "ATP", "epl": "EPL",
    "cfb": "College football", "wnba": "WNBA",
}
MIN_N = 500


def _rows(con: duckdb.DuckDBPyConnection, path: Path) -> list[dict[str, Any]]:
    cursor = con.execute(f"SELECT * FROM read_parquet('{quoted(path)}')")
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _tex(value: Any) -> str:
    if value is None:
        return "--"
    text = str(value)
    replacements = {
        "\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$",
        "#": r"\#", "_": r"\_", "{": r"\{", "}": r"\}",
        "~": r"\textasciitilde{}", "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(character, character) for character in text)


def _num(value: float | None, digits: int = 2, scale: float = 1.0) -> str:
    if value is None or not math.isfinite(float(value)):
        return "--"
    return f"{float(value) * scale:.{digits}f}"


def _count(value: int | None) -> str:
    return "--" if value is None else f"{int(value):,}"


def _p_value(value: float | None) -> str:
    if value is None or not math.isfinite(float(value)):
        return "--"
    return r"$<0.001$" if float(value) < 0.001 else f"{float(value):.3f}"


def _weight_label(value: str) -> str:
    return {
        "equal_fill": "per fill",
        "equal_sport": "equal sports",
        "dollar": "per dollar",
    }[value]


def _coef_cell(row: dict[str, Any] | None) -> str:
    if not row or row["estimate"] is None:
        return "--"
    return _num(row["estimate"], 2, 100.0)


def _se_cell(row: dict[str, Any] | None) -> str:
    if not row or row["standard_error"] is None:
        return ""
    return f"({_num(row['standard_error'], 2, 100.0)})"


def _lookup_coefficients(rows: Iterable[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(row["model_id"], row["term"]): row for row in rows}


def _lookup_models(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["model_id"]: row for row in rows}


def _stargazer_table(
    coefficient_rows: list[dict[str, Any]],
    model_rows: list[dict[str, Any]],
    model_ids: Sequence[str],
    model_labels: Sequence[str],
    terms: Sequence[tuple[str, str]],
    caption: str,
    label: str,
    *,
    landscape: bool = False,
    definition_note: str = "",
) -> str:
    coefficients = _lookup_coefficients(coefficient_rows)
    models = _lookup_models(model_rows)
    model_families = {models[model_id]["family"] for model_id in model_ids}
    interaction_label = (
        r"Sport $\times$ price" if model_families == {"continuous_price"}
        else r"Sport $\times$ D10"
    )
    columns = "l" + "r" * len(model_ids)
    body: list[str] = []
    body.append(" & ".join(["", *(_tex(value) for value in model_labels)]) + r" \\")
    body.append(r"\midrule")
    for term, display in terms:
        selected = [coefficients.get((model_id, term)) for model_id in model_ids]
        body.append(" & ".join([_tex(display), *(_coef_cell(row) for row in selected)]) + r" \\")
        body.append(" & ".join(["", *(_se_cell(row) for row in selected)]) + r" \\")
    body.extend(
        [
            r"\midrule",
            " & ".join(["Observations", *(_count(models[model_id]["n_obs"]) for model_id in model_ids)]) + r" \\",
            " & ".join(["Events", *(_count(models[model_id]["n_events"]) for model_id in model_ids)]) + r" \\",
            " & ".join([r"$R^2$", *(_num(models[model_id]["r_squared"], 3) for model_id in model_ids)]) + r" \\",
            " & ".join(["Sport intercepts", *("Yes" if models[model_id]["adjustment"] != "none" else "No" for model_id in model_ids)]) + r" \\",
            " & ".join([interaction_label, *("Yes" if models[model_id]["adjustment"] in {"sport_composition", "fully_interacted"} else "No" for model_id in model_ids)]) + r" \\",
            " & ".join([r"Sport $\times$ time", *("Yes" if models[model_id]["adjustment"] in {"sport_composition", "fully_interacted"} else "No" for model_id in model_ids)]) + r" \\",
            " & ".join(["Reference sport", *(
                "--" if models[model_id]["adjustment"] == "none" else _tex(
                    SPORT_LABELS[
                        SPORTS[0] if models[model_id]["sport"] == "all"
                        else models[model_id]["sport"].split("+")[0]
                    ]
                ) for model_id in model_ids
            )]) + r" \\",
            " & ".join(["Weighting", *(_tex(_weight_label(models[model_id]["weighting"])) for model_id in model_ids)]) + r" \\",
        ]
    )
    table = rf"""
\begin{{table}}[!htbp]
\centering
\caption{{{_tex(caption)}}}\label{{{label}}}
\small
\begin{{tabular}}{{{columns}}}
\toprule
{chr(10).join(body)}
\bottomrule
\end{{tabular}}
\begin{{minipage}}{{0.98\linewidth}}\footnotesize
Entries are coefficients in percentage points; three-way clustered standard errors are in parentheses. Nuisance sport interactions are included where indicated and omitted from the displayed rows.
{definition_note}
\end{{minipage}}
\end{{table}}
"""
    return rf"\begin{{landscape}}{table}\end{{landscape}}" if landscape else table


def _support_table(support_rows: list[dict[str, Any]]) -> str:
    selected = {
        (row["sport"], row["segment"], row["tail"]): row
        for row in support_rows
        if row["sample"] == "all_pregame_live" and row["time_normalization"] == "realized_duration"
    }
    body = []
    for sport in SPORTS:
        counts = [selected[(sport, segment, tail)]["n_obs"] for segment in ("pregame", "live") for tail in ("D1", "D10")]
        status = "Reported" if min(counts) >= MIN_N else "Withheld"
        body.append(" & ".join([_tex(SPORT_LABELS[sport]), *(_count(value) for value in counts), status]) + r" \\")
    return rf"""
\begin{{table}}[!htbp]
\centering
\caption{{Primary tail support, all pregame and live}}\label{{tab:primary-support}}
\small
\begin{{tabular}}{{lrrrrl}}
\toprule
Sport & Pregame D1 & Pregame D10 & Live D1 & Live D10 & Sport fit \\
\midrule
{chr(10).join(body)}
\bottomrule
\end{{tabular}}
\begin{{minipage}}{{0.94\linewidth}}\footnotesize
D1 is the lowest bought-price bin, $[0,.1)$; D10 is the highest, $[.9,1)$. Pregame and live refer to trades before and after the recorded game start. A sport fit is reported only when every displayed segment-tail cell has at least 500 fills.
\par\smallskip\textit{{Method and interpretation.}} Counts are unweighted numbers of D1 and D10 fills in the primary sample, which includes every retained pregame fill and live fills through $T=1$. A sport-specific fit is reported only if each pregame/live tail cell contains at least 500 fills. A withheld row means at least one required cell is too sparse, not that the effect is zero.
\end{{minipage}}
\end{{table}}
"""


def _sport_slope_table(estimands: list[dict[str, Any]]) -> str:
    lookup = {
        (row["sport"], row["sample"], row["time_normalization"]): row
        for row in estimands
        if row["scope"] == "sport" and row["family"] == "tail_linear"
        and row["estimand"] == "tail_spread_time_slope"
    }
    body = []
    variants = (
        ("all_pregame_live", "realized_duration"),
        ("live_only", "realized_duration"),
        ("bounded_pregame", "realized_duration"),
        ("all_pregame_live", "sport_median_duration"),
    )
    for sport in SPORTS:
        cells = []
        for variant in variants:
            row = lookup[(sport, *variant)]
            cells.append(
                "withheld" if row["suppressed"] else
                f"{_num(row['estimate'], 2, 100)} ({_num(row['standard_error'], 2, 100)})"
            )
        body.append(" & ".join([_tex(SPORT_LABELS[sport]), *cells]) + r" \\")
    return rf"""
\begin{{table}}[!htbp]
\centering
\caption{{Sport-specific D10--D1 time slopes}}\label{{tab:sport-slopes}}
\small
\setlength{{\tabcolsep}}{{4.5pt}}
\begin{{tabular}}{{lrrrr}}
\toprule
Sport & All pregame + live & Live only & Bounded $[-1,1]$ & Sport-median time \\
\midrule
{chr(10).join(body)}
\bottomrule
\end{{tabular}}
\begin{{minipage}}{{0.96\linewidth}}\footnotesize
Coefficients are percentage-point changes in the D10--D1 spread per unit of normalized time; clustered standard errors are in parentheses. All pregame + live imposes no pregame lower bound and includes live fills through $T=1$. Live only uses $T\in[0,1]$. Bounded $[-1,1]$ reproduces the former primary window as a comparability check. Sport-median time scales all retained times by the sport's median game length. Withheld means the 500-fill segment-tail minimum is not met.
\par\smallskip\textit{{Method and interpretation.}} Each cell is a separate per-fill D1/D10 regression, and the displayed coefficient is $\delta$ on $H_iT_i$. It is a spread change per one normalized-duration unit, not a total change over the unbounded pregame sample. Standard errors use three-way clustering.
\end{{minipage}}
\end{{table}}
"""


def _pooled_estimand_table(estimands: list[dict[str, Any]]) -> str:
    wanted = [
        row for row in estimands
        if row["family"] == "tail_linear"
        and row["estimand"] in {"tail_spread_time_slope", "equal_weight_mean_sport_tail_slope"}
        and row["scope"] in {"pooled", "pooled_supported"}
    ]
    wanted = [
        row for row in wanted
        if not (
            row["scope"] == "pooled_supported"
            and len(row["sport"].split("+")) == len(SPORTS)
            and row["adjustment"] != "fully_interacted"
        )
    ]
    order = {"all_pregame_live": 1, "live_only": 2, "bounded_pregame": 3}
    wanted.sort(key=lambda row: (
        order.get(row["sample"], 9), row["time_normalization"], row["scope"],
        row["adjustment"], row["weighting"], row["estimand"]
    ))
    body = []
    for row in wanted:
        if row["adjustment"] == "none":
            specification = "No sport controls"
        elif row["adjustment"] == "sport_intercepts":
            specification = "Sport intercepts"
        elif row["adjustment"] == "fully_interacted":
            specification = "Mean sport slopes"
        else:
            specification = "Sport baselines/trends"
        sample = {
            "all_pregame_live": "All pregame + live",
            "live_only": "Live only",
            "bounded_pregame": "Bounded [-1,1]",
        }[row["sample"]]
        if row["time_normalization"] == "sport_median_duration":
            sample = "Sport-median time"
        sports = "All 9" if row["scope"] == "pooled" else str(len(row["sport"].split("+")))
        body.append(" & ".join((
            sample, specification, _tex(_weight_label(row["weighting"])), sports,
            _num(row["estimate"], 2, 100), _num(row["standard_error"], 2, 100),
            _num(row["t_statistic"], 2), _p_value(row["p_value"]),
            f"[{_num(row['ci95_low'], 2, 100)}, {_num(row['ci95_high'], 2, 100)}]",
            _count(row["n_obs"]),
        )) + r" \\")
    return rf"""
\begin{{landscape}}
\begin{{longtable}}{{llllrrrrlr}}
\caption{{Pooled D10--D1 time-slope variations}}\label{{tab:pooled-variations}}\\
\toprule
Sample & Sport controls & Weighting & Sports & Coef. (pp) & SE & Est./SE & $p$ & 95\% CI & Fills \\
\midrule
\endfirsthead
\toprule
Sample & Sport controls & Weighting & Sports & Coef. (pp) & SE & Est./SE & $p$ & 95\% CI & Fills \\
\midrule
\endhead
{chr(10).join(body)}
\bottomrule
\end{{longtable}}
\begin{{minipage}}{{0.96\linewidth}}\footnotesize
The coefficient is the percentage-point change in the D10-minus-D1 calibration spread per unit of normalized time. All pregame + live has no pregame lower bound; Live only uses $[0,1]$; Bounded $[-1,1]$ retains the former primary window as a comparability check. Sport-median time uses the sport's median game length. No sport controls pools sports without sport terms; Sport intercepts adds sport indicators; Sport baselines/trends also allows sport-specific D10 baselines and general time slopes; Mean sport slopes is the arithmetic mean of separately estimated supported-sport slopes. Per fill gives every trade equal weight; Equal sports gives every included sport equal total weight; Per dollar weights trades by dollars. Est./SE is the estimate divided by its clustered standard error; $p$ and the 95\% interval use a normal reference.
\par\smallskip\textit{{Method and interpretation.}} Each row reports a fitted $H_iT_i$ coefficient or a linear contrast, expressed per unit of the stated time scale. Because the primary pregame interval is unbounded, its slope is not converted into a full-window change. Rows vary the sample, clock, sport adjustment, weighting, or included sport set.
\end{{minipage}}
\end{{landscape}}
"""


def _pooled_bin_table(rows: list[dict[str, Any]]) -> str:
    lookup = {
        (row["weighting"], row["time_bin"]): row
        for row in rows if row["scope"] == "pooled"
    }
    body = []
    for time_bin in range(1, 11):
        cells = []
        for weighting in ("equal_fill", "equal_sport"):
            row = lookup[(weighting, time_bin)]
            cells.extend((
                _num(row["d1_mean_calibration"], 2, 100),
                _num(row["d10_mean_calibration"], 2, 100),
                _num(row["spread_d10_minus_d1"], 2, 100),
                f"[{_num(row['spread_ci95_low'], 2, 100)}, {_num(row['spread_ci95_high'], 2, 100)}]",
                _count(row["d1_n"]), _count(row["d10_n"]),
            ))
        bin_label = f"{{}}[{(time_bin-1)/10:.1f},{time_bin/10:.1f}{']' if time_bin == 10 else ')'}"
        body.append(" & ".join([bin_label, *cells]) + r" \\")
    return rf"""
\begin{{landscape}}
\begin{{table}}[!htbp]
\centering
\caption{{Pooled live D10--D1 spread by normalized-time bin}}\label{{tab:pooled-time-bins}}
\small
\scriptsize
\setlength{{\tabcolsep}}{{3.3pt}}
\begin{{tabular}}{{lrrrrrrrrrrrr}}
\toprule
& \multicolumn{{6}}{{c}}{{Per fill}} & \multicolumn{{6}}{{c}}{{Equal sports}} \\
\cmidrule(lr){{2-7}}\cmidrule(lr){{8-13}}
$T$ bin & \shortstack{{D1 mean\\$Y-P$}} & \shortstack{{D10 mean\\$Y-P$}} & D10--D1 & 95\% CI & D1 $N$ & D10 $N$ & \shortstack{{D1 mean\\$Y-P$}} & \shortstack{{D10 mean\\$Y-P$}} & D10--D1 & 95\% CI & D1 $N$ & D10 $N$ \\
\midrule
{chr(10).join(body)}
\bottomrule
\end{{tabular}}
\begin{{minipage}}{{0.98\linewidth}}\footnotesize
Calibration error is eventual bought-contract outcome minus trade price, $Y-P$, in percentage points. D1 and D10 are the lowest and highest bought-price bins; D10--D1 subtracts the D1 mean from the D10 mean. Per fill gives every trade equal weight; Equal sports reweights trades so each sport has the same total weight within the displayed sample. Rows are raw weighted means, not regression-adjusted estimates; $N$ is the unweighted number of fills.
\par\smallskip\textit{{Method and interpretation.}} This table retains the former ten-bin live summary as a numerical audit alongside the continuous kernel figure. For each fixed live-time bin and weighting scheme, D1 and D10 are weighted means of $Y-P$, the spread is $\bar R_{{D10}}-\bar R_{{D1}}$, and the interval is based on the three-way clustered variance of that difference. The displayed $N$ values are unweighted fill counts. This table begins at $T=0$ because it is a live-only diagnostic; the primary regressions include all pregame trades.
\end{{minipage}}
\end{{table}}
\end{{landscape}}
"""


def _plot_pooled_bins(rows: list[dict[str, Any]], output: Path) -> None:
    fig, axis = plt.subplots(figsize=(7.0, 3.3), constrained_layout=True)
    styles = (("equal_fill", "Per fill", "#1f4e79", "o", -0.012),
              ("equal_sport", "Equal sports", "#b24a33", "s", 0.012))
    for weighting, label, color, marker, offset in styles:
        selected = sorted(
            (row for row in rows if row["scope"] == "pooled" and row["weighting"] == weighting and not row["suppressed"]),
            key=lambda row: row["time_bin"],
        )
        x = np.array([(row["time_low"] + row["time_high"]) / 2 + offset for row in selected])
        y = np.array([row["spread_d10_minus_d1"] * 100 for row in selected])
        low = np.array([row["spread_ci95_low"] * 100 for row in selected])
        high = np.array([row["spread_ci95_high"] * 100 for row in selected])
        axis.errorbar(x, y, yerr=np.vstack((y-low, high-y)), fmt=marker,
                      linestyle="none", color=color, ecolor=color, capsize=2.5,
                      markersize=4.5, elinewidth=0.9, label=label)
    axis.axhline(0, color="black", linewidth=0.8)
    axis.set_xlim(0, 1)
    axis.set_xticks(np.arange(0, 1.01, 0.1))
    axis.set_xlabel("Normalized live time")
    axis.set_ylabel("D10 - D1 calibration spread (pp)")
    axis.grid(axis="y", color="#dddddd", linewidth=0.6)
    axis.spines[["top", "right"]].set_visible(False)
    axis.legend(frameon=False, ncol=2)
    fig.savefig(output, format="pdf", bbox_inches="tight")
    plt.close(fig)


def _plot_sport_bins(rows: list[dict[str, Any]], output: Path) -> None:
    fig, axes = plt.subplots(3, 3, figsize=(7.2, 7.1), sharex=True, sharey=True, constrained_layout=True)
    for axis, sport in zip(axes.flat, SPORTS):
        selected = sorted(
            (row for row in rows if row["scope"] == "sport" and row["sport"] == sport and not row["suppressed"]),
            key=lambda row: row["time_bin"],
        )
        if selected:
            x = np.array([(row["time_low"] + row["time_high"]) / 2 for row in selected])
            y = np.array([row["spread_d10_minus_d1"] * 100 for row in selected])
            low = np.array([row["spread_ci95_low"] * 100 for row in selected])
            high = np.array([row["spread_ci95_high"] * 100 for row in selected])
            axis.errorbar(x, y, yerr=np.vstack((y-low, high-y)), fmt="o",
                          linestyle="none", color="#1f4e79", ecolor="#666666",
                          capsize=2.0, markersize=3.2, elinewidth=0.75)
        axis.axhline(0, color="black", linewidth=0.65)
        axis.set_title(SPORT_LABELS[sport], fontsize=9)
        axis.set_xlim(0, 1)
        axis.set_xticks((0, 0.5, 1.0))
        axis.grid(axis="y", color="#e5e5e5", linewidth=0.5)
        axis.spines[["top", "right"]].set_visible(False)
        axis.tick_params(labelsize=7)
    for axis in axes[-1, :]:
        axis.set_xlabel("Live time", fontsize=8)
    for axis in axes[:, 0]:
        axis.set_ylabel("D10 - D1 (pp)", fontsize=8)
    fig.savefig(output, format="pdf", bbox_inches="tight")
    plt.close(fig)


def _supported_segments(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    ordered = sorted((row for row in rows if not row["suppressed"]), key=lambda row: row["grid_index"])
    segments: list[list[dict[str, Any]]] = []
    for row in ordered:
        if not segments:
            segments.append([row])
            continue
        previous = segments[-1][-1]
        gap = float(row["time_value"]) - float(previous["time_value"])
        if row["grid_index"] != previous["grid_index"] + 1 or gap > 2 * float(row["bandwidth"]):
            segments.append([row])
        else:
            segments[-1].append(row)
    return segments


def _draw_kernel(
    axis: plt.Axes, rows: list[dict[str, Any]], color: str, label: str | None = None
) -> bool:
    drawn = False
    for segment_index, segment in enumerate(_supported_segments(rows)):
        if len(segment) < 2:
            continue
        x = np.array([row["time_value"] for row in segment], dtype=float)
        y = np.array([row["spread_d10_minus_d1"] * 100 for row in segment], dtype=float)
        low = np.array([row["spread_ci95_low"] * 100 for row in segment], dtype=float)
        high = np.array([row["spread_ci95_high"] * 100 for row in segment], dtype=float)
        axis.plot(x, y, color=color, linewidth=1.25,
                  label=label if segment_index == 0 else None)
        axis.fill_between(x, low, high, color=color, alpha=0.14, linewidth=0)
        drawn = True
    return drawn


def _plot_pooled_kernel(rows: list[dict[str, Any]], output: Path) -> None:
    fig, axis = plt.subplots(figsize=(7.0, 3.4), constrained_layout=True)
    styles = (
        ("equal_fill", "Per fill", "#1f4e79"),
        ("equal_sport", "Equal sports", "#b24a33"),
    )
    for weighting, label, color in styles:
        selected = [
            row for row in rows
            if row["scope"] == "pooled" and row["phase"] == "live"
            and row["weighting"] == weighting
        ]
        _draw_kernel(axis, selected, color, label)
    axis.axhline(0, color="black", linewidth=0.8)
    axis.axvline(0, color="#666666", linewidth=0.7)
    axis.set_xlim(0, 1)
    axis.set_xlabel("Normalized live time")
    axis.set_ylabel("D10 - D1 calibration spread (pp)")
    axis.grid(axis="y", color="#dddddd", linewidth=0.6)
    axis.spines[["top", "right"]].set_visible(False)
    axis.legend(frameon=False, ncol=2)
    fig.savefig(output, format="pdf", bbox_inches="tight")
    plt.close(fig)


def _plot_sport_kernel(rows: list[dict[str, Any]], output: Path) -> None:
    fig, axes = plt.subplots(6, 3, figsize=(8.0, 11.0), sharey=True, constrained_layout=True)
    for phase_index, phase in enumerate(("pregame", "live")):
        for sport_index, sport in enumerate(SPORTS):
            axis = axes[phase_index * 3 + sport_index // 3, sport_index % 3]
            selected = [
                row for row in rows
                if row["scope"] == "sport" and row["sport"] == sport
                and row["phase"] == phase and row["weighting"] == "equal_fill"
            ]
            drawn = _draw_kernel(axis, selected, "#1f4e79")
            axis.axhline(0, color="black", linewidth=0.6)
            axis.axvline(0, color="#666666", linewidth=0.6)
            axis.set_title(f"{SPORT_LABELS[sport]}: {phase}", fontsize=8.5)
            if phase == "live":
                axis.set_xlim(0, 1)
                axis.set_xticks((0, 0.5, 1.0))
            else:
                supported = [row for row in selected if not row["suppressed"]]
                if supported:
                    axis.set_xlim(min(row["time_value"] for row in supported), 0)
                if not drawn:
                    axis.text(0.5, 0.5, "No locally supported estimate",
                              transform=axis.transAxes, ha="center", va="center", fontsize=7)
            axis.grid(axis="y", color="#e5e5e5", linewidth=0.5)
            axis.spines[["top", "right"]].set_visible(False)
            axis.tick_params(labelsize=6.5)
    for column in range(3):
        axes[2, column].set_xlabel("Pregame normalized time", fontsize=7.5)
        axes[5, column].set_xlabel("Live normalized time", fontsize=7.5)
    for row in range(6):
        axes[row, 0].set_ylabel("D10 - D1 (pp)", fontsize=7.5)
    fig.savefig(output, format="pdf", bbox_inches="tight")
    plt.close(fig)


def _pregame_time_table(rows: list[dict[str, Any]]) -> str:
    lookup = {(row["sport"], row["tail"]): row for row in rows}
    body = []
    for sport in SPORTS:
        d1, d10 = lookup[(sport, "D1")], lookup[(sport, "D10")]
        body.append(" & ".join((
            _tex(SPORT_LABELS[sport]), _count(d1["n_obs"]), _count(d10["n_obs"]),
            _num(d1["minimum"], 1), _num(d10["minimum"], 1),
            _num(d1["p01"], 1), _num(d10["p01"], 1),
            _num(d1["median"], 1), _num(d10["median"], 1),
        )) + r" \\")
    return rf"""
\begin{{table}}[!htbp]
\centering
\caption{{Retained pregame normalized-time distribution}}\label{{tab:pregame-time}}
\small
\setlength{{\tabcolsep}}{{4pt}}
\begin{{tabular}}{{lrrrrrrrr}}
\toprule
& \multicolumn{{2}}{{c}}{{Fills}} & \multicolumn{{2}}{{c}}{{Minimum $T$}} & \multicolumn{{2}}{{c}}{{1st percentile}} & \multicolumn{{2}}{{c}}{{Median}} \\
\cmidrule(lr){{2-3}}\cmidrule(lr){{4-5}}\cmidrule(lr){{6-7}}\cmidrule(lr){{8-9}}
Sport & D1 & D10 & D1 & D10 & D1 & D10 & D1 & D10 \\
\midrule
{chr(10).join(body)}
\bottomrule
\end{{tabular}}
\begin{{minipage}}{{0.96\linewidth}}\footnotesize
All retained $T<0$ D1 and D10 fills are included. The minimum documents the extreme left tail; the first percentile and median show where ordinary pregame mass lies. These values are descriptive and do not impose a lower cutoff.
\end{{minipage}}
\end{{table}}
"""


def _data_decisions_table(
    duration_rows: list[dict[str, Any]], trade_sample: str
) -> str:
    durations = ", ".join(
        f"{SPORT_LABELS[row['sport']]} {_num(row['median_duration_minutes'], 1)}"
        for row in duration_rows
    )
    if trade_sample == "filtered_trades":
        trade_filter = r"$0.01<P_i<0.99$; flagged outcome-token buyers excluded"
        tail_bins = r"D1: $(.01,.1)$; D10: $[.9,.99)$"
        price_note = (
            r"Under the $0.01<P_i<0.99$ filter, D1 is $0.01<P_i<0.10$ "
            r"and D10 is $0.90\leq P_i<0.99$"
        )
    elif trade_sample == "all_trades":
        trade_filter = r"$0<P_i<1$; flagged outcome-token buyers included"
        tail_bins = r"D1: $(0,.1)$; D10: $[.9,1)$"
        price_note = (
            r"Under the $0<P_i<1$ rule, D1 is $0<P_i<0.10$ "
            r"and D10 is $0.90\leq P_i<1$"
        )
    else:
        raise ValueError(trade_sample)
    rows = (
        ("Observation", r"Resolved moneyline BUY fill; $R_i=Y_i-P_i$"),
        ("Sports", "MLB, NFL, NBA, NHL, men's CBB, ATP, EPL, college football, WNBA"),
        ("Trade filters", trade_filter),
        ("Primary time", r"$T_i=(t_i-s_m)/(e_m-s_m)$; exact block time, realized event duration"),
        ("Primary window", r"All retained $T<0$; start $T=0$; live through $T=1$"),
        ("Variations", r"Live only $[0,1]$; former window $[-1,1]$; sport-median time"),
        ("Kernel plots", r"Epanechnikov kernel; $h=.50$ pregame, $h=.10$ live; phases fit separately"),
        ("Tail bins", tail_bins),
        ("Weighting", r"Per fill; equal sports ($w_i=1/N_s$ within the final fit sample); per dollar"),
        ("Inference", "Cameron--Gelbach--Miller clustering: UTC day, buyer wallet, event"),
        ("Withholding", "500 fills per required regression cell or local kernel tail window"),
        ("Median minutes", durations),
    )
    body = "\n".join(f"{_tex(name)} & {value} \\\\" for name, value in rows)
    return rf"""
\begin{{table}}[!htbp]
\centering
\caption{{Data and estimation decisions}}\label{{tab:data-decisions}}
\small
\begin{{tabular}}{{p{{1.25in}}p{{5.45in}}}}
\toprule
Decision & Definition \\
\midrule
{body}
\bottomrule
\end{{tabular}}
\begin{{minipage}}{{0.98\linewidth}}\footnotesize
\textit{{Method and interpretation.}} The unit of observation is a resolved moneyline BUY fill. For fill $i$, $Y_i\in\{{0,1\}}$ indicates whether the bought contract won, $P_i$ is its purchase price, and calibration error is $R_i=Y_i-P_i$. Exact block time is standardized as $T_i=(t_i-s_m)/(e_m-s_m)$, where $s_m$ and $e_m$ are the recorded start and realized end of the event. {price_note}; these are fixed bins, not sample quantiles. Per-fill estimates use $w_i=1$, equal-sport estimates use $w_i=1/N_s$ within the final analysis sample, and per-dollar estimates use traded dollars as weights. Standard errors use Cameron--Gelbach--Miller three-way clustering by UTC trade day, buyer wallet, and event.
\end{{minipage}}
\end{{table}}
"""


def _namespace_latex(block: str, namespace: str) -> str:
    return (
        block.replace(r"\label{tab:", rf"\label{{tab:{namespace}-")
        .replace(r"\label{fig:", rf"\label{{fig:{namespace}-")
    )


def render_flb_decay(
    estimator_run: str | Path,
    all_estimator_run: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    estimator = Path(estimator_run).expanduser().resolve()
    all_estimator = Path(all_estimator_run).expanduser().resolve()
    input_names = (
        "coefficients.parquet", "model_summary.parquet", "estimands.parquet",
        "support.parquet", "duration_reference.parquet", "time_bin_spreads.parquet",
        "kernel_time_spreads.parquet", "pregame_time_distribution.parquet",
    )
    filtered_inputs = tuple(estimator / name for name in input_names)
    all_inputs = tuple(all_estimator / name for name in input_names)
    inputs = (*filtered_inputs, *all_inputs, estimator / "manifest.json", all_estimator / "manifest.json")
    if any(not path.is_file() for path in inputs):
        raise FileNotFoundError([str(path) for path in inputs if not path.is_file()])
    con = duckdb.connect()
    try:
        coefficients, models, estimands, support, durations, time_bins, kernel_rows, pregame_times = [
            _rows(con, path) for path in filtered_inputs
        ]
        (all_coefficients, all_models, all_estimands, all_support, all_durations,
         all_time_bins, all_kernel_rows, all_pregame_times) = [
            _rows(con, path) for path in all_inputs
        ]
    finally:
        con.close()
    filtered_manifest = json.loads((estimator / "manifest.json").read_text())
    all_manifest = json.loads((all_estimator / "manifest.json").read_text())
    if filtered_manifest.get("trade_sample") != "filtered_trades":
        raise ValueError("First estimator run is not filtered_trades")
    if all_manifest.get("trade_sample") != "all_trades":
        raise ValueError("Second estimator run is not all_trades")
    if ({row["sport"] for row in durations} != set(SPORTS)
            or {row["sport"] for row in all_durations} != set(SPORTS)):
        raise ValueError("Duration artifact does not contain the frozen nine-sport domain")
    target = Path(run_dir).expanduser().resolve()
    with fresh_run(target, inputs) as staging:
        figures = staging / "figures"
        figures.mkdir()
        _plot_pooled_kernel(kernel_rows, figures / "filtered_pooled_live_kernel.pdf")
        _plot_sport_kernel(kernel_rows, figures / "filtered_sport_pregame_live_kernel.pdf")
        _plot_pooled_kernel(all_kernel_rows, figures / "all_trades_pooled_live_kernel.pdf")
        _plot_sport_kernel(all_kernel_rows, figures / "all_trades_sport_pregame_live_kernel.pdf")

        pooled_ids = (
            "pooled_tail_all_pregame_live_realized_duration_none_equal_fill",
            "pooled_tail_all_pregame_live_realized_duration_sport_intercepts_equal_fill",
            "pooled_tail_all_pregame_live_realized_duration_sport_composition_equal_fill",
            "pooled_tail_all_pregame_live_realized_duration_sport_composition_equal_sport",
            "pooled_tail_all_pregame_live_realized_duration_sport_composition_dollar",
        )
        piecewise_ids = (
            "pooled_supported_tail_piecewise_all_pregame_live_realized_duration_sport_composition_equal_fill",
            "pooled_supported_tail_piecewise_all_pregame_live_realized_duration_sport_composition_equal_sport",
        )
        continuous_ids = (
            "pooled_continuous_all_pregame_live_realized_duration_none_equal_fill",
            "pooled_continuous_all_pregame_live_realized_duration_sport_composition_equal_fill",
            "pooled_continuous_all_pregame_live_realized_duration_sport_composition_equal_sport",
        )
        pooled_note = (
            r"The sample includes every retained pregame fill and live fills through $T=1$. "
            r"No controls pools sports without sport terms; Sport intercepts adds sport indicators; "
            r"Sport-specific also allows sport-specific D10-minus-D1 baselines and D1 time slopes. "
            r"Per fill gives every trade equal weight; Equal sports gives every sport equal total "
            r"weight; Per dollar weights trades by dollars. "
            r"\par\smallskip\textit{Method and interpretation.} Weighted least squares is estimated "
            r"on D1 and D10 fills with no pregame lower cutoff. At $T=0$, the intercept is D1 "
            r"calibration, D10 is the D10--D1 spread, Time is the D1 time slope, and D10 $\times$ "
            r"time is the change in that spread per unit of normalized time. No controls fully pools "
            r"sports; Sport intercepts adds sport indicators; Sport-specific adds sport $\times$ D10 "
            r"and sport $\times$ time terms while retaining a common D10 $\times$ time coefficient. "
            r"In Sport intercepts, only the intercept is an MLB reference coefficient; in "
            r"Sport-specific, Equal sports, and Per dollar, the first three displayed coefficients "
            r"are MLB reference values and the tail-spread slope is common across sports. The primary "
            r"coefficient is a change per one normalized-duration unit and is not a total change over "
            r"the unbounded pregame interval."
        )
        piecewise_note = (
            r"Piecewise estimates separate pregame and live changes joined at $T=0$. The sample "
            r"contains the sports meeting the 500-fill minimum in every required pregame/live D1/D10 "
            r"cell. Per fill gives every trade equal weight; Equal sports gives every included sport "
            r"equal total weight. \par\smallskip\textit{Method and interpretation.} Supported sports "
            r"are estimated jointly with $T_i^-=\min(T_i,0)$ and $T_i^+=\max(T_i,0)$. The design "
            r"includes sport-specific intercepts, game-start tail spreads, and D1 pregame and live "
            r"slopes, while the D10 interactions with $T_i^-$ and $T_i^+$ are common pooled spread "
            r"changes. Because both time variables equal zero at game start, the fitted segments join "
            r"continuously at $T=0$. The displayed slope rows are common interactions, not "
            r"reference-sport slopes or simple sport averages. The break is imposed at game start "
            r"rather than estimated."
        )
        continuous_note = (
            r"The continuous-price model replaces the D1/D10 indicator with trade price minus 0.5. "
            r"Price $\times$ time is the change in the calibration-price gradient per unit of $T$. "
            r"No controls pools sports without sport terms; Sport-specific allows sport-specific "
            r"intercepts, price gradients, and general time slopes. Per fill gives every trade equal "
            r"weight; Equal sports gives every sport equal total weight. "
            r"\par\smallskip\textit{Method and interpretation.} This regression uses all eligible "
            r"prices with no pregame lower cutoff and live fills through $T=1$, rather than restricting "
            r"the sample to D1 and D10. With $X_i=P_i-.5$, the fitted model is "
            r"$E[R_i\mid P_i,T_i,s]=\alpha_s+\beta_sX_i+\gamma_sT_i+\delta^pX_iT_i$. "
            r"Thus $\alpha_s$ is calibration at $P=.5,T=0$, $\beta_s$ is the price gradient at game "
            r"start, $\gamma_s$ is the time slope at $P=.5$, and $\delta^p$ is the cross-partial "
            r"$\partial^2E[R]/(\partial P\,\partial T)$. Equivalently, the price gradient is "
            r"$\beta_s+\delta^pT$ and the time slope is $\gamma_s+\delta^p(P-.5)$. The adjusted "
            r"models allow sport-specific intercepts, price gradients, and general time slopes, with "
            r"MLB as the reference and a common price-by-time interaction. This is an all-price "
            r"gradient estimand, not a literal D10--D1 contrast, and it imposes a linear "
            r"calibration-price relation."
        )

        def make_blocks(
            sample_coefficients: list[dict[str, Any]],
            sample_models: list[dict[str, Any]],
            sample_estimands: list[dict[str, Any]],
            sample_support: list[dict[str, Any]],
            sample_durations: list[dict[str, Any]],
            sample_time_bins: list[dict[str, Any]],
            sample_pregame_times: list[dict[str, Any]],
            trade_sample: str,
            namespace: str,
        ) -> dict[str, str]:
            blocks = {
                "data": _data_decisions_table(sample_durations, trade_sample),
                "support": _support_table(sample_support),
                "pregame": _pregame_time_table(sample_pregame_times),
                "pooled": _stargazer_table(
                    sample_coefficients, sample_models, pooled_ids,
                    ("No controls", "Sport intercepts", "Sport-specific", "Equal sports", "Per dollar"),
                    (("Intercept", "D1 at T=0"), ("D10", "D10-D1 at T=0"),
                     ("Time", "D1 time slope"), ("D10 x time", "D10-D1 time slope")),
                    "Pooled pregame-and-live tail regressions, realized-duration time",
                    "tab:pooled-stargazer", definition_note=pooled_note,
                ),
                "sport": _sport_slope_table(sample_estimands),
                "bins": _pooled_bin_table(sample_time_bins),
                "estimands": _pooled_estimand_table(sample_estimands),
                "piecewise": _stargazer_table(
                    sample_coefficients, sample_models, piecewise_ids,
                    ("Per fill", "Equal sports"),
                    (("D10", "Reference-sport D10-D1 at T=0"),
                     ("D10 x pregame time", "Pregame D10-D1 slope"),
                     ("D10 x live time", "Live D10-D1 slope")),
                    "Piecewise pooled tail regressions at game start",
                    "tab:piecewise-stargazer", definition_note=piecewise_note,
                ),
                "continuous": _stargazer_table(
                    sample_coefficients, sample_models, continuous_ids,
                    ("No controls", "Sport-specific", "Equal sports"),
                    (("Intercept", "Calibration at P=.5, T=0"),
                     ("Price centered", "Price gradient at T=0"),
                     ("Time", "Time slope at P=.5"),
                     ("Price x time", "Change in price gradient")),
                    "Pooled continuous-price regressions", "tab:continuous-stargazer",
                    definition_note=continuous_note,
                ),
            }
            return {key: _namespace_latex(value, namespace) for key, value in blocks.items()}

        filtered_blocks = make_blocks(
            coefficients, models, estimands, support, durations, time_bins,
            pregame_times, "filtered_trades", "filtered",
        )
        all_blocks = make_blocks(
            all_coefficients, all_models, all_estimands, all_support, all_durations,
            all_time_bins, all_pregame_times, "all_trades", "all",
        )
        data_decisions_table = filtered_blocks["data"]
        support_table = filtered_blocks["support"]
        pregame_time_table = filtered_blocks["pregame"]
        pooled_stargazer = filtered_blocks["pooled"]
        sport_slope_table = filtered_blocks["sport"]
        pooled_bin_table = filtered_blocks["bins"]
        pooled_estimand_table = filtered_blocks["estimands"]
        piecewise_stargazer = filtered_blocks["piecewise"]
        continuous_stargazer = filtered_blocks["continuous"]
        all_data_decisions_table = all_blocks["data"]
        all_support_table = all_blocks["support"]
        all_pregame_time_table = all_blocks["pregame"]
        all_pooled_stargazer = all_blocks["pooled"]
        all_sport_slope_table = all_blocks["sport"]
        all_pooled_bin_table = all_blocks["bins"]
        all_pooled_estimand_table = all_blocks["estimands"]
        all_piecewise_stargazer = all_blocks["piecewise"]
        all_continuous_stargazer = all_blocks["continuous"]
        tex = rf"""\documentclass[10pt]{{article}}
\usepackage[margin=0.72in]{{geometry}}
\usepackage{{booktabs,longtable,graphicx,pdflscape}}
\usepackage[T1]{{fontenc}}
\usepackage{{lmodern,microtype}}
\usepackage[hidelinks]{{hyperref}}
\setlength{{\LTpre}}{{4pt}}
\setlength{{\LTpost}}{{4pt}}
\renewcommand{{\arraystretch}}{{0.96}}
\title{{Favorite--Longshot Bias Over Normalized Game Time: Filtered and All Trades}}
\author{{}}
\date{{20 September 2026}}
\begin{{document}}
\maketitle
\vspace{{-2em}}
\noindent Nine sport samples of resolved game-winner moneyline markets; common bought-contract calibration. Estimator: \path{{analysis/multisport_game_dynamics/estimate_flb_decay.py}}. Tables and figures read only the saved estimator artifacts.

\paragraph{{Sample construction.}} Both sections are rebuilt from the same frozen exact-fill artifacts, accepted moneyline event cohort, exact block timestamps, realized outcomes, and event start/end records. Filtered trades require $0.01<P_i<0.99$ and exclude flagged outcome-token buyers. All trades require $0<P_i<1$ and include flagged buyers. Every model, time window, support rule, weighting scheme, and uncertainty calculation is otherwise identical. Realized-duration time is retrospective because the denominator uses the realized event end.

\section{{Model specifications}}
\noindent Let $R_i=Y_i-P_i$, $H_i=\mathbf{{1}}\{{P_i\in D10\}}$, and $T_i=(t_i-s_m)/(e_m-s_m)$. The primary tail-spread estimand is
\[
\Delta_s(T)=E[R_i\mid H_i=1,T_i=T,s]-E[R_i\mid H_i=0,T_i=T,s].
\]
The direct tail model, estimated on D1 and D10 only, is
\[
R_i=\alpha_s+\beta_sH_i+\gamma_sT_i+\delta_s(H_iT_i)+\varepsilon_i,
\qquad \Delta_s(T)=\beta_s+\delta_sT.
\]
Thus $\delta_s$ is the D10--D1 spread change per normalized game duration. The primary sample has no finite pregame lower endpoint, so $\delta_s$ is not converted into a whole-window change. The continuous-price model is
\[
R_i=\alpha_s+\beta_s(P_i-.5)+\gamma_sT_i+\delta_s^p((P_i-.5)T_i)+\varepsilon_i.
\]
The game-start diagnostic uses $T_i^- = \min(T_i,0)$ and $T_i^+=\max(T_i,0)$ with separate interactions $H_iT_i^-$ and $H_iT_i^+$. Pooled models with sport-specific baselines allow each sport its own intercept, D10 baseline, and general time slope; the common $H_iT_i$ coefficient is the pooled tail-spread slope.

\subsection*{{Calculation and inference rules}}
For any displayed regression, let $X$ be its design matrix, $W=\mathrm{{diag}}(w_i)$, and $R$ the vector of fill-level calibration errors. Weighted least squares is computed as
\[
\widehat\theta=(X'WX)^{{-1}}X'WR.
\]
Per-fill models set $w_i=1$; equal-sport models set $w_i=1/N_s$ using the sport's count in the final estimation sample; per-dollar models set $w_i$ equal to the fill's traded dollars. Regression covariance uses Cameron--Gelbach--Miller inclusion and exclusion across UTC day ($d$), buyer wallet ($w$), and event ($e$):
\[
\widehat V=B\bigl(M_d+M_w+M_e-M_{{dw}}-M_{{de}}-M_{{we}}+M_{{dwe}}\bigr)B,
\qquad B=(X'WX)^{{-1}}.
\]
No finite-sample covariance correction is applied. Reported standard errors are square roots of diagonal elements of $\widehat V$; $t=\widehat\theta/SE$, two-sided $p=2[1-\Phi(|t|)]$, and 95\% intervals are $\widehat\theta\pm1.96SE$. Displayed $p$-values are nominal and are not multiplicity-adjusted. Coefficients and calibration errors are displayed as percentage points, equal to 100 times their probability-scale values.

Fixed bought-price bins are assigned as $D_i=\min(\lfloor10P_i\rfloor+1,10)$. The alternative sport-median clock is $T_i^{{\mathrm{{med}}}}=(t_i-s_m)/\widetilde D_s$, where $\widetilde D_s$ is the median realized duration among distinct observed events in sport $s$.

For a time bin $b$ and tail $h\in\{{D1,D10\}}$, the descriptive mean is
\[
\bar R_{{hb}}=\frac{{\sum_i w_iR_i\mathbf{{1}}\{{i\in(h,b)\}}}}{{\sum_i w_i\mathbf{{1}}\{{i\in(h,b)\}}}},
\qquad \Delta_b=\bar R_{{D10,b}}-\bar R_{{D1,b}}.
\]
Bin-spread intervals apply the same three-way cluster inclusion-and-exclusion calculation to the joint influence score for $\Delta_b$. The continuous figures replace bin membership with Epanechnikov weights $K(u)=.75(1-u^2)\mathbf{{1}}\{{|u|<1\}}$, using $h=.50$ before start and $h=.10$ live:
\[
\widehat\mu_h(x)=\frac{{\sum_i w_iK((T_i-x)/h)R_i\mathbf{{1}}\{{i\in h\}}}}{{\sum_i w_iK((T_i-x)/h)\mathbf{{1}}\{{i\in h\}}}},
\qquad \widehat\Delta(x)=\widehat\mu_{{D10}}(x)-\widehat\mu_{{D1}}(x).
\]
Pregame and live observations are smoothed separately. Curves and pointwise clustered bands are withheld wherever either local tail window has fewer than 500 fills. A positive spread means calibration is higher in D10 than D1.

\clearpage
\section{{Filtered trades}}
\subsection{{Data decisions}}
{data_decisions_table}
{support_table}
{pregame_time_table}

\subsection{{Regression results}}
{pooled_stargazer}

{sport_slope_table}

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.86\textwidth]{{figures/filtered_pooled_live_kernel.pdf}}
\caption{{Pooled live D10--D1 kernel averages. Shading is a pointwise 95\% three-way clustered interval.}}
\label{{fig:filtered-pooled-live-kernel}}
\begin{{minipage}}{{0.94\linewidth}}\footnotesize
\textit{{Method and interpretation.}} At each live time $x$, the curve subtracts the Epanechnikov-kernel-weighted D1 mean calibration from the corresponding D10 mean using $h=.10$. Per fill uses $w_i=1$; Equal sports uses $w_i=1/N_s$ over the complete all-pregame-plus-live tail sample. Pregame observations never enter a live estimate. Bands are pointwise Cameron--Gelbach--Miller intervals clustered by day, wallet, and event. Curves break wherever either local tail has fewer than 500 fills.
\end{{minipage}}
\end{{figure}}

{pooled_bin_table}

\begin{{figure}}[!htbp]
\centering
\includegraphics[height=0.82\textheight]{{figures/filtered_sport_pregame_live_kernel.pdf}}
\caption{{Sport-specific pregame and live D10--D1 kernel averages. Unsupported portions are omitted.}}
\label{{fig:filtered-sport-time-kernel}}
\begin{{minipage}}{{0.94\linewidth}}\footnotesize
\textit{{Method and interpretation.}} Each panel applies the same kernel calculation separately within a sport and phase using per-fill weights, with $h=.50$ pregame and $h=.10$ live. The wider pregame bandwidth reflects its much lower local density and was chosen from support counts, not from outcome values. The upper nine panels use every retained $T<0$ fill; the lower nine use $0\leq T\leq1$. Pregame grid points are empirical-time quantiles so the unbounded left tail is represented without imposing a cutoff, while the horizontal axis remains raw normalized time. Lines break across unsupported regions or gaps wider than two bandwidths. These are descriptive kernel-weighted means, not fitted values from the linear regressions.
\end{{minipage}}
\end{{figure}}

{pooled_estimand_table}

{piecewise_stargazer}

{continuous_stargazer}

\clearpage
\section{{All trades}}
\subsection{{Data decisions}}
{all_data_decisions_table}
{all_support_table}
{all_pregame_time_table}

\subsection{{Regression results}}
{all_pooled_stargazer}

{all_sport_slope_table}

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.86\textwidth]{{figures/all_trades_pooled_live_kernel.pdf}}
\caption{{Pooled live D10--D1 kernel averages. Shading is a pointwise 95\% three-way clustered interval.}}
\label{{fig:all-pooled-live-kernel}}
\begin{{minipage}}{{0.94\linewidth}}\footnotesize
\textit{{Method and interpretation.}} At each live time $x$, the curve subtracts the Epanechnikov-kernel-weighted D1 mean calibration from the corresponding D10 mean using $h=.10$. Per fill uses $w_i=1$; Equal sports uses $w_i=1/N_s$ over the complete all-pregame-plus-live tail sample. Pregame observations never enter a live estimate. Bands are pointwise Cameron--Gelbach--Miller intervals clustered by day, wallet, and event. Curves break wherever either local tail has fewer than 500 fills.
\end{{minipage}}
\end{{figure}}

{all_pooled_bin_table}

\begin{{figure}}[!htbp]
\centering
\includegraphics[height=0.82\textheight]{{figures/all_trades_sport_pregame_live_kernel.pdf}}
\caption{{Sport-specific pregame and live D10--D1 kernel averages. Unsupported portions are omitted.}}
\label{{fig:all-sport-time-kernel}}
\begin{{minipage}}{{0.94\linewidth}}\footnotesize
\textit{{Method and interpretation.}} Each panel applies the same kernel calculation separately within a sport and phase using per-fill weights, with $h=.50$ pregame and $h=.10$ live. The wider pregame bandwidth reflects its much lower local density and was chosen from support counts, not from outcome values. The upper nine panels use every retained $T<0$ fill; the lower nine use $0\leq T\leq1$. Pregame grid points are empirical-time quantiles so the unbounded left tail is represented without imposing a cutoff, while the horizontal axis remains raw normalized time. Lines break across unsupported regions or gaps wider than two bandwidths. These are descriptive kernel-weighted means, not fitted values from the linear regressions.
\end{{minipage}}
\end{{figure}}

{all_pooled_estimand_table}

{all_piecewise_stargazer}

{all_continuous_stargazer}

\end{{document}}
"""
        source = staging / "flb_time_regressions.tex"
        source.write_text(tex, encoding="utf-8")
        manifest = {
            "schema_version": 3,
            "stage": "flb_time_regression_latex_filtered_all_v1",
            "inputs": {
                **{f"filtered/{path.name}": fingerprint(path) for path in filtered_inputs},
                **{f"all_trades/{path.name}": fingerprint(path) for path in all_inputs},
                "filtered/manifest.json": fingerprint(estimator / "manifest.json"),
                "all_trades/manifest.json": fingerprint(all_estimator / "manifest.json"),
            },
            "outputs": {
                "tex": artifact_fingerprint(source),
                "figures": [
                    "filtered_pooled_live_kernel.pdf",
                    "filtered_sport_pregame_live_kernel.pdf",
                    "all_trades_pooled_live_kernel.pdf",
                    "all_trades_sport_pregame_live_kernel.pdf",
                ],
            },
            "counts": {
                "filtered": {
                    "coefficient_rows": len(coefficients), "model_rows": len(models),
                    "estimand_rows": len(estimands), "support_rows": len(support),
                    "time_bin_rows": len(time_bins), "kernel_rows": len(kernel_rows),
                    "pregame_time_rows": len(pregame_times),
                },
                "all_trades": {
                    "coefficient_rows": len(all_coefficients), "model_rows": len(all_models),
                    "estimand_rows": len(all_estimands), "support_rows": len(all_support),
                    "time_bin_rows": len(all_time_bins), "kernel_rows": len(all_kernel_rows),
                    "pregame_time_rows": len(all_pregame_times),
                },
            },
        }
        write_json(staging / "report_manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--estimator-run", required=True)
    parser.add_argument("--all-estimator-run", required=True)
    parser.add_argument("--run-dir", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    print(json.dumps(render_flb_decay(**vars(parse_args(argv))), sort_keys=True))


if __name__ == "__main__":
    main()
