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
        if row["sample"] == "unified" and row["time_normalization"] == "realized_duration"
    }
    body = []
    for sport in SPORTS:
        counts = [selected[(sport, segment, tail)]["n_obs"] for segment in ("pregame", "live") for tail in ("D1", "D10")]
        status = "Reported" if min(counts) >= MIN_N else "Withheld"
        body.append(" & ".join([_tex(SPORT_LABELS[sport]), *(_count(value) for value in counts), status]) + r" \\")
    return rf"""
\begin{{table}}[!htbp]
\centering
\caption{{Primary tail support, $T\in[-1,1]$}}\label{{tab:primary-support}}
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
\par\smallskip\textit{{Method and interpretation.}} Counts are unweighted numbers of D1 and D10 fills in the primary $T\in[-1,1]$ sample, classified as pregame when $T<0$ and live when $T\geq0$. A sport-specific pregame-and-live tail regression is reported only if each of its four segment-by-tail cells contains at least 500 fills. NBA, men's CBB, ATP, EPL, and college football pass this requirement. A withheld row therefore means that at least one required cell is too sparse for the planned regression, not that the corresponding effect is zero.
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
        ("unified", "realized_duration"),
        ("live_only", "realized_duration"),
        ("wider_pregame", "realized_duration"),
        ("unified", "sport_median_duration"),
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
Sport & Pregame + live & Live only & Wider window & Sport-median time \\
\midrule
{chr(10).join(body)}
\bottomrule
\end{{tabular}}
\begin{{minipage}}{{0.96\linewidth}}\footnotesize
Coefficients are percentage-point changes in the D10--D1 spread per unit of normalized time; clustered standard errors are in parentheses. Pregame + live uses $T\in[-1,1]$; Live only uses $T\in[0,1]$; Wider window uses $T\in[-2,1]$; Sport-median time scales time by the sport's median game length rather than each game's realized length. Withheld means the 500-fill segment-tail minimum is not met.
\par\smallskip\textit{{Method and interpretation.}} Each cell comes from a separate per-fill regression for one sport using only D1 and D10 fills, with the displayed coefficient equal to $\delta$ on $H_iT_i$. Pregame + live uses realized-duration time over $[-1,1]$, Live only uses $[0,1]$, Wider window uses $[-2,1]$, and Sport-median time divides clock time from game start by the median realized duration for that sport. Thus a coefficient is the spread change per one unit of the specified clock, while the fitted full-window changes are $2\delta$, $\delta$, and $3\delta$ for the first three realized-time windows. Standard errors use three-way clustering, and estimates are withheld when a required segment-tail cell has fewer than 500 fills. ATP has a large positive realized-time estimate, men's CBB has a negative estimate, and the other supported estimates are smaller or imprecise. The pooled relationship therefore masks substantial cross-sport heterogeneity and sensitivity to clock construction.
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
    order = {"unified": 1, "live_only": 2, "wider_pregame": 3}
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
            "unified": "Pregame + live",
            "live_only": "Live only",
            "wider_pregame": "Wider window",
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
The coefficient is the percentage-point change in the D10-minus-D1 calibration spread per unit of normalized time. Pregame + live uses $T\in[-1,1]$; Live only excludes pregame trades; Wider window extends the lower bound to $T=-2$; Sport-median time uses the sport's median game length. No sport controls pools sports without sport terms; Sport intercepts adds sport indicators; Sport baselines/trends also allows sport-specific D10 baselines and general time slopes; Mean sport slopes is the arithmetic mean of separately estimated supported-sport slopes. Per fill gives every trade equal weight; Equal sports gives every included sport equal total weight; Per dollar weights trades by dollars. Sports is the number included. Est./SE is the estimate divided by its clustered standard error; $p$ and the 95\% interval use a normal reference.
\par\smallskip\textit{{Method and interpretation.}} Each row reports either a fitted $H_iT_i$ coefficient or a linear contrast of such coefficients, expressed as the D10--D1 spread change per unit of the stated time scale. Rows vary one or more of the sample window, time normalization, sport adjustment, weighting, or sport set. Sport baselines/trends allows sport-specific intercepts, D10 baselines, and general time slopes while constraining the tail-spread slope to be common. Mean sport slopes instead fully interacts the tail slope by sport and reports their arithmetic mean with joint three-way clustered inference. All-nine-sport pooled rows do not require every sport to pass the individual support floor; supported-sport rows include only sports satisfying the relevant 500-fill cells. Live-only estimates are positive under both per-fill and equal-sport weighting, but sport-median-time estimates are negative and imprecise. Since the alternative clock also changes which fills fall inside the window, this comparison captures sensitivity to both normalization and sample composition.
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
\par\smallskip\textit{{Method and interpretation.}} This table reports the calculations underlying Figure~\ref{{fig:pooled-time-bins}}. For each fixed live-time bin and weighting scheme, D1 and D10 are weighted means of $Y-P$, the spread is $\bar R_{{D10}}-\bar R_{{D1}}$, and the interval is based on the three-way clustered variance of that difference. The displayed $N$ values are unweighted fill counts, so they are identical across weighting panels. The final bin, $T\in[.9,1]$, has a per-fill D1 mean of $-2.12$ points and D10 mean of $2.67$ points, yielding a spread of $4.80$ with a 95\% interval of $[3.45,6.14]$. Both tail means contribute arithmetically to the final-bin spread. This table begins at $T=0$ because it is a live-only diagnostic; negative pregame time remains included in the unified regressions.
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


def _data_decisions_table(duration_rows: list[dict[str, Any]]) -> str:
    durations = ", ".join(
        f"{SPORT_LABELS[row['sport']]} {_num(row['median_duration_minutes'], 1)}"
        for row in duration_rows
    )
    rows = (
        ("Observation", r"Resolved moneyline BUY fill; $R_i=Y_i-P_i$"),
        ("Sports", "MLB, NFL, NBA, NHL, men's CBB, ATP, EPL, college football, WNBA"),
        ("Trade filters", r"$0.01<P_i<0.99$; flagged outcome-token buyers excluded"),
        ("Primary time", r"$T_i=(t_i-s_m)/(e_m-s_m)$; exact block time, realized event duration"),
        ("Primary window", r"$T\in[-1,1]$; pregame $T<0$, start $T=0$, end $T=1$"),
        ("Variations", r"Live only $[0,1]$; wider window $[-2,1]$; sport-median time"),
        ("Tail bins", r"D1: $[0,.1)$; D10: $[.9,1)$ under the filtered price support"),
        ("Weighting", r"Per fill; equal sports ($w_i=1/N_s$ within the final fit sample); per dollar"),
        ("Inference", "Cameron--Gelbach--Miller clustering: UTC day, buyer wallet, event"),
        ("Withholding", "500 fills per required tail-by-segment cell for sport-specific tail fits"),
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
\textit{{Method and interpretation.}} The unit of observation is a resolved moneyline BUY fill. For fill $i$, $Y_i\in\{{0,1\}}$ indicates whether the bought contract won, $P_i$ is its purchase price, and calibration error is $R_i=Y_i-P_i$. Exact block time is standardized as $T_i=(t_i-s_m)/(e_m-s_m)$, where $s_m$ and $e_m$ are the recorded start and realized end of the event. Under the $0.01<P_i<0.99$ filter, D1 is $0.01<P_i<0.10$ and D10 is $0.90\leq P_i<0.99$; these are fixed bins, not sample quantiles. Per-fill estimates use $w_i=1$, equal-sport estimates use $w_i=1/N_s$ within the final analysis sample, and per-dollar estimates use traded dollars as weights. Standard errors use Cameron--Gelbach--Miller three-way clustering by UTC trade day, buyer wallet, and event.
\end{{minipage}}
\end{{table}}
"""


def render_flb_decay(estimator_run: str | Path, run_dir: str | Path) -> dict[str, Any]:
    estimator = Path(estimator_run).expanduser().resolve()
    input_names = (
        "coefficients.parquet", "model_summary.parquet", "estimands.parquet",
        "support.parquet", "duration_reference.parquet", "time_bin_spreads.parquet",
    )
    inputs = tuple(estimator / name for name in input_names)
    if any(not path.is_file() for path in inputs):
        raise FileNotFoundError([str(path) for path in inputs if not path.is_file()])
    con = duckdb.connect()
    try:
        coefficients, models, estimands, support, durations, time_bins = [
            _rows(con, path) for path in inputs
        ]
    finally:
        con.close()
    if {row["sport"] for row in durations} != set(SPORTS):
        raise ValueError("Duration artifact does not contain the frozen nine-sport domain")
    target = Path(run_dir).expanduser().resolve()
    with fresh_run(target, inputs) as staging:
        figures = staging / "figures"
        figures.mkdir()
        _plot_pooled_bins(time_bins, figures / "pooled_live_time_bins.pdf")
        _plot_sport_bins(time_bins, figures / "sport_live_time_bins.pdf")

        pooled_ids = (
            "pooled_tail_unified_realized_duration_none_equal_fill",
            "pooled_tail_unified_realized_duration_sport_intercepts_equal_fill",
            "pooled_tail_unified_realized_duration_sport_composition_equal_fill",
            "pooled_tail_unified_realized_duration_sport_composition_equal_sport",
            "pooled_tail_unified_realized_duration_sport_composition_dollar",
        )
        piecewise_ids = (
            "pooled_supported_tail_piecewise_realized_duration_sport_composition_equal_fill",
            "pooled_supported_tail_piecewise_realized_duration_sport_composition_equal_sport",
        )
        continuous_ids = (
            "pooled_continuous_unified_realized_duration_none_equal_fill",
            "pooled_continuous_unified_realized_duration_sport_composition_equal_fill",
            "pooled_continuous_unified_realized_duration_sport_composition_equal_sport",
        )
        data_decisions_table = _data_decisions_table(durations)
        support_table = _support_table(support)
        pooled_stargazer = _stargazer_table(
            coefficients, models, pooled_ids,
            ("No controls", "Sport intercepts", "Sport-specific", "Equal sports", "Per dollar"),
            (("Intercept", "D1 at T=0"), ("D10", "D10-D1 at T=0"),
             ("Time", "D1 time slope"), ("D10 x time", "D10-D1 time slope")),
            "Pooled pregame-and-live tail regressions, realized-duration time",
            "tab:pooled-stargazer",
            definition_note=(
                r"Pregame + live uses $T\in[-1,1]$. No controls pools sports without sport terms; "
                r"Sport intercepts adds sport indicators; Sport-specific also allows sport-specific "
                r"D10-minus-D1 baselines and D1 time slopes. Per fill gives every trade equal weight; "
                r"Equal sports gives every sport equal total weight; Per dollar weights trades by dollars. "
                r"\par\smallskip\textit{Method and interpretation.} Weighted least squares is estimated "
                r"on D1 and D10 fills with $T\in[-1,1]$. At $T=0$, the intercept is D1 calibration, D10 is "
                r"the D10--D1 spread, Time is the D1 time slope, and D10 $\times$ time is the change in that "
                r"spread per unit of normalized time. No controls fully pools sports; Sport intercepts adds "
                r"sport indicators; Sport-specific adds sport $\times$ D10 and sport $\times$ time terms "
                r"while retaining a common D10 $\times$ time coefficient. In Sport intercepts, only the "
                r"intercept is an MLB reference coefficient; in Sport-specific, Equal sports, and Per dollar, "
                r"the first three displayed coefficients are MLB reference values and the tail-spread slope "
                r"is common across sports. The sport-specific per-fill estimate $7.88$ implies a fitted "
                r"spread change of $15.76$ points from $T=-1$ to $T=1$. Its reduction to $3.68$ under "
                r"equal-sport weighting indicates that the per-fill magnitude partly reflects the sports "
                r"contributing the most fills."
            ),
        )
        sport_slope_table = _sport_slope_table(estimands)
        pooled_bin_table = _pooled_bin_table(time_bins)
        pooled_estimand_table = _pooled_estimand_table(estimands)
        piecewise_stargazer = _stargazer_table(
            coefficients, models, piecewise_ids, ("Per fill", "Equal sports"),
            (("D10", "NBA D10-D1 at T=0"),
             ("D10 x pregame time", "Pregame D10-D1 slope"),
             ("D10 x live time", "Live D10-D1 slope")),
            "Piecewise pooled tail regressions at game start",
            "tab:piecewise-stargazer",
            definition_note=(
                r"Piecewise estimates separate pregame and live changes joined at $T=0$. "
                r"The sample contains the five sports meeting the 500-fill minimum in every required "
                r"pregame/live D1/D10 cell. Per fill gives every trade equal weight; Equal sports gives "
                r"every included sport equal total weight. "
                r"\par\smallskip\textit{Method and interpretation.} The five sports passing all four "
                r"pregame/live tail support requirements are estimated jointly with $T_i^-=\min(T_i,0)$ "
                r"and $T_i^+=\max(T_i,0)$. The design includes sport-specific intercepts, game-start tail "
                r"spreads, and D1 pregame and live slopes, while the D10 interactions with $T_i^-$ and "
                r"$T_i^+$ are common pooled spread changes. Because both time variables equal zero at game "
                r"start, the fitted segments join continuously at $T=0$. NBA is the reference sport, so "
                r"$-12.39$ is its fitted game-start spread; the slope rows are common interactions, not NBA "
                r"slopes or simple sport averages. Per-fill estimates imply that the spread falls by $19.57$ "
                r"points per unit as start approaches, then rises by $13.38$ points over one unit of live time. "
                r"For NBA, fitted spreads are $7.18$ at $T=-1$, $-12.39$ at $T=0$, and $0.99$ at $T=1$. "
                r"The break is imposed at game start rather than estimated. These sport-adjusted associations "
                r"are descriptive; the model has no event fixed effects."
            ),
        )
        continuous_stargazer = _stargazer_table(
            coefficients, models, continuous_ids,
            ("No controls", "Sport-specific", "Equal sports"),
            (("Intercept", "Calibration at P=.5, T=0"),
             ("Price centered", "Price gradient at T=0"),
             ("Time", "Time slope at P=.5"),
             ("Price x time", "Change in price gradient")),
            "Pooled continuous-price regressions", "tab:continuous-stargazer",
            definition_note=(
                r"The continuous-price model replaces the D1/D10 indicator with trade price minus 0.5. "
                r"Price $\times$ time is the change in the calibration-price gradient per unit of $T$. "
                r"No controls pools sports without sport terms; Sport-specific allows sport-specific "
                r"intercepts, price gradients, and general time slopes. Per fill gives every trade "
                r"equal weight; Equal sports gives every sport equal total weight. "
                r"\par\smallskip\textit{Method and interpretation.} This regression uses all eligible "
                r"prices over $T\in[-1,1]$, rather than restricting the sample to D1 and D10. With "
                r"$X_i=P_i-.5$, the fitted model is $E[R_i\mid P_i,T_i,s]=\alpha_s+\beta_sX_i+\gamma_sT_i"
                r"+\delta^pX_iT_i$. Thus $\alpha_s$ is calibration at $P=.5,T=0$, $\beta_s$ is the price "
                r"gradient at game start, $\gamma_s$ is the time slope at $P=.5$, and $\delta^p$ is the "
                r"cross-partial $\partial^2E[R]/(\partial P\,\partial T)$. Equivalently, the price gradient "
                r"is $\beta_s+\delta^pT$ and the time slope is $\gamma_s+\delta^p(P-.5)$. The adjusted models "
                r"allow sport-specific intercepts, price gradients, and general time slopes, with MLB as the "
                r"reference and a common price-by-time interaction. The per-fill estimate $\delta^p=12.05$ "
                r"moves the MLB price gradient from $-8.08$ at $T=0$ to $3.97$ at $T=1$; equal-sport weighting "
                r"reduces the interaction to $5.97$ with standard error $3.90$. This is an all-price gradient "
                r"estimand, not a literal D10--D1 contrast, and it imposes a linear calibration-price relation."
            ),
        )
        tex = rf"""\documentclass[10pt]{{article}}
\usepackage[margin=0.72in]{{geometry}}
\usepackage{{booktabs,longtable,graphicx,pdflscape}}
\usepackage[T1]{{fontenc}}
\usepackage{{lmodern,microtype}}
\usepackage[hidelinks]{{hyperref}}
\setlength{{\LTpre}}{{4pt}}
\setlength{{\LTpost}}{{4pt}}
\renewcommand{{\arraystretch}}{{0.96}}
\title{{Favorite--Longshot Bias Over Normalized Game Time}}
\author{{}}
\date{{17 September 2026}}
\begin{{document}}
\maketitle
\vspace{{-2em}}
\noindent Nine sport samples of resolved game-winner moneyline markets; common bought-contract calibration. Estimator: \path{{analysis/multisport_game_dynamics/estimate_flb_decay.py}}. Tables and figures read only the saved estimator artifacts.

\paragraph{{Sample construction.}} The estimator concatenates frozen upstream phase artifacts containing eligible resolved moneyline BUY fills with exact block timestamps. Upstream construction applies $0.01<P_i<0.99$ and excludes outcome-token buyers flagged nonhuman; this stage validates those inputs but does not reconstruct the upstream filters. Realized-duration time is retrospective because the denominator uses the realized event end.

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
Thus $\delta_s$ is the D10--D1 spread change per normalized game duration; over $[-1,1]$ the fitted change is $2\delta_s$. The continuous-price model is
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

Fixed bought-price bins are assigned as $D_i=\min(\lfloor10P_i\rfloor+1,10)$. The alternative sport-median clock is $T_i^{{\mathrm{{med}}}}=(t_i-s_m)/\widetilde D_s$, where $\widetilde D_s$ is the median realized duration among distinct observed events in sport $s$. Restricting this clock to $[-1,1]$ changes both the time denominator and which fills enter the sample.

For a time bin $b$ and tail $h\in\{{D1,D10\}}$, the descriptive mean is
\[
\bar R_{{hb}}=\frac{{\sum_i w_iR_i\mathbf{{1}}\{{i\in(h,b)\}}}}{{\sum_i w_i\mathbf{{1}}\{{i\in(h,b)\}}}},
\qquad \Delta_b=\bar R_{{D10,b}}-\bar R_{{D1,b}}.
\]
Bin-spread intervals apply the same three-way cluster inclusion-and-exclusion calculation to the joint influence score for $\Delta_b$. A positive spread means calibration is higher in D10 than D1; a classic FLB interpretation additionally requires the displayed tail profile to show negative D1 calibration and positive D10 calibration.

\section{{Data decisions}}
{data_decisions_table}
{support_table}

\clearpage
\section{{Regression results}}
{pooled_stargazer}

{sport_slope_table}

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.86\textwidth]{{figures/pooled_live_time_bins.pdf}}
\caption{{Pooled live D10--D1 spreads in fixed normalized-time bins. Per fill weights every trade equally; Equal sports gives every sport equal total weight. Isolated points; 95\% three-way clustered intervals.}}
\label{{fig:pooled-time-bins}}
\begin{{minipage}}{{0.94\linewidth}}\footnotesize
\textit{{Method and interpretation.}} Live fills with $T\in[0,1]$ are assigned to ten fixed-width time bins using $b_i=\min(\lfloor10T_i\rfloor+1,10)$, so the final bin includes $T=1$. Within each bin, the plotted value is the weighted D10 mean of $Y-P$ minus the weighted D1 mean. Per-fill points use $w_i=1$; equal-sport points use $w_i=1/N_s$, where $N_s$ is the sport's observation count across the complete live-tail sample, not within each bin. Whiskers are 95\% Cameron--Gelbach--Miller intervals for the difference of means, clustered by day, wallet, and event. These are raw bin means with no regression adjustment, and bins with fewer than 500 fills in either tail are omitted. The first nine point estimates are negative and the final-bin estimate is positive under both weighting schemes, visually suggesting a terminal change rather than a uniform linear progression.
\end{{minipage}}
\end{{figure}}

{pooled_bin_table}

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.96\textwidth]{{figures/sport_live_time_bins.pdf}}
\caption{{Sport-specific live D10--D1 spreads in fixed normalized-time bins. Withheld bins are omitted.}}
\label{{fig:sport-time-bins}}
\begin{{minipage}}{{0.94\linewidth}}\footnotesize
\textit{{Method and interpretation.}} The calculation is the same raw D10--D1 live-bin contrast as in Figure~\ref{{fig:pooled-time-bins}}, but it is performed separately by sport using per-fill weights. Each point is the difference between sport-specific D10 and D1 mean calibration in one fixed $T$ bin, with a three-way clustered 95\% interval. A sport-bin is omitted when either tail has fewer than 500 fills. These descriptive conditional means are not fitted values from the linear sport regressions in Table~\ref{{tab:sport-slopes}}. Several sports have positive terminal-bin estimates, but earlier paths and precision vary materially. The pooled terminal shift is therefore not attributable to only one sport, although there is no common smooth trajectory across sports.
\end{{minipage}}
\end{{figure}}

{pooled_estimand_table}

{piecewise_stargazer}

{continuous_stargazer}

\end{{document}}
"""
        source = staging / "flb_time_regressions.tex"
        source.write_text(tex, encoding="utf-8")
        manifest = {
            "schema_version": 1,
            "stage": "flb_time_regression_latex_v3",
            "inputs": {path.name: fingerprint(path) for path in inputs},
            "outputs": {
                "tex": artifact_fingerprint(source),
                "figures": ["pooled_live_time_bins.pdf", "sport_live_time_bins.pdf"],
            },
            "counts": {
                "coefficient_rows": len(coefficients), "model_rows": len(models),
                "estimand_rows": len(estimands), "support_rows": len(support),
                "time_bin_rows": len(time_bins),
            },
        }
        write_json(staging / "report_manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--estimator-run", required=True)
    parser.add_argument("--run-dir", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    print(json.dumps(render_flb_decay(**vars(parse_args(argv))), sort_keys=True))


if __name__ == "__main__":
    main()
