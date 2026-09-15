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
            " & ".join(["Weighting", *(_tex(models[model_id]["weighting"].replace("_", " ")) for model_id in model_ids)]) + r" \\",
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
A sport-specific unified tail regression is withheld unless every displayed segment-tail cell has at least 500 fills.
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
Sport & Unified & Live only & Wider pregame & Fixed duration \\
\midrule
{chr(10).join(body)}
\bottomrule
\end{{tabular}}
\begin{{minipage}}{{0.96\linewidth}}\footnotesize
Coefficients are percentage-point changes in the D10--D1 spread per unit of normalized time; clustered standard errors are in parentheses. Unified uses $[-1,1]$, live only uses $[0,1]$, and wider pregame uses $[-2,1]$.
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
            specification = "Unadjusted"
        elif row["adjustment"] == "sport_intercepts":
            specification = "Sport FE"
        elif row["adjustment"] == "fully_interacted":
            specification = "Mean sport slope"
        else:
            specification = "Composition adjusted"
        sample = {"unified": "Unified", "live_only": "Live only", "wider_pregame": "Wider pregame"}[row["sample"]]
        if row["time_normalization"] == "sport_median_duration":
            sample = "Fixed duration"
        sports = "All 9" if row["scope"] == "pooled" else str(len(row["sport"].split("+")))
        body.append(" & ".join((
            sample, specification, _tex(row["weighting"].replace("_", " ")), sports,
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
Sample & Adjustment & Weighting & Sports & Coef. (pp) & SE & $t$ & $p$ & 95\% CI & Fills \\
\midrule
\endfirsthead
\toprule
Sample & Adjustment & Weighting & Sports & Coef. (pp) & SE & $t$ & $p$ & 95\% CI & Fills \\
\midrule
\endhead
{chr(10).join(body)}
\bottomrule
\end{{longtable}}
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
& \multicolumn{{6}}{{c}}{{Equal fill}} & \multicolumn{{6}}{{c}}{{Equal sport}} \\
\cmidrule(lr){{2-7}}\cmidrule(lr){{8-13}}
$T$ bin & D1 cal. & D10 cal. & Spread & 95\% CI & D1 $N$ & D10 $N$ & D1 cal. & D10 cal. & Spread & 95\% CI & D1 $N$ & D10 $N$ \\
\midrule
{chr(10).join(body)}
\bottomrule
\end{{tabular}}
\end{{table}}
\end{{landscape}}
"""


def _plot_pooled_bins(rows: list[dict[str, Any]], output: Path) -> None:
    fig, axis = plt.subplots(figsize=(7.0, 3.3), constrained_layout=True)
    styles = (("equal_fill", "Equal fill", "#1f4e79", "o", -0.012),
              ("equal_sport", "Equal sport", "#b24a33", "s", 0.012))
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
        ("Variations", r"Live only $[0,1]$; wider pregame $[-2,1]$; fixed sport-median duration"),
        ("Tail bins", r"D1: $[0,.1)$; D10: $[.9,1)$ under the filtered price support"),
        ("Weighting", r"Equal fill; equal sport ($w_i=1/N_s$ within the final fit sample); dollar weighting reported separately"),
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
            ("Unadjusted", "Sport FE", "Composition", "Equal sport", "Dollar"),
            (("Intercept", "Intercept"), ("D10", "D10"), ("Time", "Time"),
             ("D10 x time", "D10 x time")),
            "Pooled unified tail regressions, realized-duration time",
            "tab:pooled-stargazer",
        )
        sport_slope_table = _sport_slope_table(estimands)
        pooled_bin_table = _pooled_bin_table(time_bins)
        pooled_estimand_table = _pooled_estimand_table(estimands)
        piecewise_stargazer = _stargazer_table(
            coefficients, models, piecewise_ids, ("Equal fill", "Equal sport"),
            (("D10", "D10 at game start"),
             ("D10 x pregame time", "D10 x pregame time"),
             ("D10 x live time", "D10 x live time")),
            "Piecewise pooled tail regressions at game start",
            "tab:piecewise-stargazer",
        )
        continuous_stargazer = _stargazer_table(
            coefficients, models, continuous_ids,
            ("Unadjusted", "Composition", "Equal sport"),
            (("Intercept", "Intercept"), ("Price centered", "Price - 0.5"),
             ("Time", "Time"), ("Price x time", "Price x time")),
            "Pooled continuous-price regressions", "tab:continuous-stargazer",
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
\date{{15 September 2026}}
\begin{{document}}
\maketitle
\vspace{{-2em}}
\noindent Nine resolved moneyline cohorts; common bought-contract calibration. Estimator: \path{{analysis/multisport_game_dynamics/estimate_flb_decay.py}}. Tables and figures read only the saved estimator artifacts.

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
The game-start diagnostic uses $T_i^- = \min(T_i,0)$ and $T_i^+=\max(T_i,0)$ with separate interactions $H_iT_i^-$ and $H_iT_i^+$. Pooled composition-adjusted models allow sport-specific intercepts, D10 baselines, and general time slopes; the common $H_iT_i$ coefficient is the pooled tail-spread slope.

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
\caption{{Pooled live D10--D1 spreads in fixed normalized-time bins. Isolated points; 95\% three-way clustered intervals.}}
\label{{fig:pooled-time-bins}}
\end{{figure}}

{pooled_bin_table}

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.96\textwidth]{{figures/sport_live_time_bins.pdf}}
\caption{{Sport-specific live D10--D1 spreads in fixed normalized-time bins. Withheld bins are omitted.}}
\label{{fig:sport-time-bins}}
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
            "stage": "flb_time_regression_latex_v1",
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
