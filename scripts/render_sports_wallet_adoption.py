"""Render the audited current nine-sport wallet-adoption comparison as portable LaTeX."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import math
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.estimate_sports_wallet_adoption import (
    CASES, CONTRASTS, CAPS, AdoptionBlocked, atomic_publish, fingerprint, read_json,
    compare_estimands, load_estimates, project_estimand, require, write_json,
)

SPORTS = ("mlb", "nfl", "nba", "nhl", "cbb", "atp", "epl", "cfb", "wnba")
LABELS = {"mlb": "MLB", "nfl": "NFL", "nba": "NBA", "nhl": "NHL", "cbb": "Men's CBB",
          "atp": "ATP", "epl": "EPL", "cfb": "College football", "wnba": "WNBA"}
CONTRAST_LABELS = {"mlb_coverage": "MLB coverage", "historical_flag_gap": "Saved-flag gap",
                   "wallet_identity_repair": "Identity repair", "total_adoption": "Total adoption"}
CASE_LABELS = ("Archived", "Restored, saved flags", "Restored, recomputed", "Adopted", "All trades")
PLOT_HELPER = ROOT/"analysis/multisport_game_dynamics/render_flb_decay.py"
FIGURES = ("filtered_pooled_live_kernel.pdf", "all_trades_pooled_live_kernel.pdf",
           "filtered_sport_pregame_kernel.pdf", "filtered_sport_live_kernel.pdf",
           "all_trades_sport_pregame_kernel.pdf", "all_trades_sport_live_kernel.pdf")


def summary_fingerprint(path: Path) -> dict:
    require(path.stat().st_size <= CAPS["summary_file_bytes"], "Report summary exceeds cap before hashing")
    return fingerprint(path)


def tex(value) -> str:
    replacements = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$",
                    "#": r"\#", "_": r"\_", "{": r"\{", "}": r"\}",
                    "~": r"\textasciitilde{}", "^": r"\textasciicircum{}"}
    return "".join(replacements.get(character, character) for character in str(value))


def number(value, *, count=False) -> str:
    require(value is not None and math.isfinite(float(value)), "Cannot render unavailable value as numeric")
    return f"{int(value):,}" if count else f"{float(value)*100:.2f}"


def evidence_cell(row: dict) -> str:
    if row["suppressed"]:
        require(row["estimate"] is None and row["standard_error"] is None, "Withheld row has numeric value")
        return "withheld"
    return number(row["estimate"])+" ("+number(row["standard_error"])+")"


def table(caption: str, headings: list[str], rows: list[list[str]], note: str,
          *, first_width: str | None = None) -> str:
    columns = (f"p{{{first_width}}}" if first_width else "l")+"r"*(len(headings)-1)
    content = "\n".join(" & ".join(row)+r" \\" for row in rows)
    return rf"""
\begin{{table}}[!htbp]\centering
\caption{{{caption}}}
\small\setlength{{\tabcolsep}}{{4pt}}
\begin{{tabular}}{{{columns}}}\toprule
{' & '.join(headings)} \\
\midrule
{content}
\bottomrule\end{{tabular}}
\begin{{minipage}}{{\linewidth}}\footnotesize {note}\end{{minipage}}
\end{{table}}
"""


def headline_ids() -> list[tuple[str, str]]:
    return [(f"sport_{sport}_tail_linear_all_pregame_live_realized_duration:tail_spread_time_slope", LABELS[sport])
            for sport in SPORTS]+[
        (f"pooled_tail_all_pregame_live_realized_duration_sport_composition_{weight}:tail_spread_time_slope", label)
        for weight, label in (("equal_fill", "Pooled, per fill"), ("equal_sport", "Pooled, equal sports"),
                              ("dollar", "Pooled, per dollar"))]


def validate_comparison(value: dict) -> None:
    require(value.get("schema_version") == "sports_wallet_adoption_comparison_v1" and
            value.get("status") == "comparison_complete" and value.get("data_certified") is False,
            "Incomplete comparison artifact")
    require(set(value.get("cases", {})) == set(CASES), "Incomplete comparison cases")
    require(all(value.get("gates", {}).get(name) is True for name in
                ("all_trade_regime_invariant", "historical_observation_reproduction",
                 "historical_estimate_reproduction",
                 "saved_output_schema_grid_support_rank_interval_checks", "supported_difference_composition")),
            "Comparison gates incomplete")
    for name in CASES:
        case = value["cases"][name]
        require(set(case["observation_counts"]) == set(SPORTS) and
                case["trade_sample"] == ("all_trades" if name == "restored_all" else "filtered_trades"), "Case definition changed")
        ids = [row["estimand_id"] for row in case["estimands"]]
        require(len(ids) == len(set(ids)), "Duplicate report estimands")
        require(all(key in ids for key, _ in headline_ids()), "Report headline grid incomplete")
    changes = {(row["comparison"], row["estimand_id"]): row for row in value["changes"]}
    require(len(changes) == len(value["changes"]), "Duplicate comparison rows")
    for comparison, _, _ in CONTRASTS:
        require(all((comparison, key) in changes for key, _ in headline_ids()), "Report comparison grid incomplete")
    expected_changes = [row for name, before, after in CONTRASTS for row in compare_estimands(
        value["cases"][before]["estimands"], value["cases"][after]["estimands"], name)]
    require(changes == {(row["comparison"], row["estimand_id"]): row for row in expected_changes},
            "Comparison changes/embedded case estimates differ")
    for row in changes.values():
        if row["status"] != "supported":
            require(row["change"] is None, "Withheld comparison has a numeric difference")
        else:
            require(not row["before"]["suppressed"] and not row["after"]["suppressed"] and
                    math.isclose(row["change"], row["after"]["estimate"]-row["before"]["estimate"],
                                 abs_tol=1e-10, rel_tol=1e-8), "Comparison difference changed")


def render_source(value: dict) -> str:
    validate_comparison(value)
    cases = value["cases"]
    counts = [[tex(LABELS[sport]), *(number(cases[name]["observation_counts"][sport], count=True) for name in CASES)] for sport in SPORTS]
    counts.append(["Total", *(number(sum(cases[name]["observation_counts"].values()), count=True) for name in CASES)])
    count_table = table("Eligible inferred BUY fills under each controlled stage",
        ["Sport", "Archived", r"\shortstack{Restored\\saved flags}", r"\shortstack{Restored\\recomputed}", "Adopted", "All trades"], counts,
        "The archived sample retains September coverage and saved shared flags; its current-engine rerun reproduces all archived model, support and uncertainty summaries within recorded floating-reduction tolerances. Restored samples use the October MLB input repair. Recomputed flags use original CLEAN; adopted flags use corrected CLEAN. Filtered samples use $.01<P<.99$ and exclude flagged buyers. All trades use $0<P<1$ and retain flagged buyers; this changes both price support and buyer filtering.")
    builds = value["flags"]["builds"]
    flag_rows = []
    for label, getter in (("Admitted published rows", lambda record: record["rows"]["admitted_rows"]),
                          ("Wallets", lambda record: record["flags"]["wallets"]),
                          ("Flagged wallets", lambda record: record["flags"]["nonhuman_wallets"]),
                          ("Rows assigned to flagged wallets", lambda record: record["flags"]["nonhuman_trades"])):
        flag_rows.append([label, *(number(getter(builds[name]), count=True) for name in ("legacy", "corrected"))])
    flag_table = table("Behavioral classifier support", ["Measure", "Original CLEAN", "Corrected CLEAN"], flag_rows,
        "The unchanged classifier uses all published sides at timestamps at or after 1 June 2020 UTC. These are expanded published rows and heuristic classifications, not distinct native executions or independently verified automation.")
    transition = next(row for row in value["flags"]["comparisons"] if row["comparison"] == "corrected_vs_legacy_recomputed")
    composite = next(row for row in transition["criteria"] if row["criterion"] == "is_nonhuman")
    transition_rows = [["Common wallets newly flagged", number(composite["common_enter"], count=True)],
                       ["Common wallets no longer flagged", number(composite["common_exit"], count=True)],
                       ["New wallet keys", number(transition["right_only_wallets"], count=True)],
                       ["Removed wallet keys", number(transition["left_only_wallets"], count=True)]]
    transition_table = table("Wallet classification changes from identity correction", ["Transition", "Wallets"], transition_rows,
        "Both sides are fresh recomputations with identical rules and admitted source rows. Differences from either historical saved flag artifact remain a separate provenance or vintage gap.")
    lookups = {name: {row["estimand_id"]: row for row in cases[name]["estimands"]} for name in CASES}
    changes = {(row["comparison"], row["estimand_id"]): row for row in value["changes"]}
    selected = headline_ids()
    estimate_rows = [[tex(label), *(evidence_cell(lookups[name][key]) for name in CASES[:4])] for key, label in selected]
    estimate_table = table("Primary D10--D1 time slopes before and after adoption",
        ["Model", "Archived", r"\shortstack{Restored\\saved flags}", r"\shortstack{Restored\\recomputed}", "Adopted"], estimate_rows,
        "Slopes are percentage-point changes in the D10--D1 calibration spread per normalized event duration. Three-way UTC-day, buyer-wallet and event clustered standard errors are in parentheses. Sport fits require 500 fills in each pregame/live tail cell; withheld values are not zero. Pooled composition models retain sport intercepts, tail baselines and general time trends.")
    delta_rows = [[tex(label), *(number(changes[(comparison, key)]["change"]) if
                    changes[(comparison, key)]["status"] == "supported" else "withheld"
                    for comparison, _, _ in CONTRASTS)] for key, label in selected]
    delta_table = table("Changes in the primary slopes, separated by source",
        ["Model", "MLB coverage", "Saved-flag gap", "Identity repair", "Total"], delta_rows,
        "Entries are differences in percentage-point slopes. MLB coverage holds saved flags fixed; the saved-flag gap compares those flags with original-CLEAN recomputation; identity repair compares corrected-CLEAN with original-CLEAN recomputation. Total equals the three components only where all estimates are supported and comparable. Differences are descriptive; cross-run covariance and uncertainty for these differences have not been estimated.")
    status_rows = []
    for comparison, _, _ in CONTRASTS:
        statuses = Counter(row["status"] for row in value["changes"] if row["comparison"] == comparison)
        status_rows.append([tex(CONTRAST_LABELS[comparison]), *(number(statuses[name], count=True) for name in
                           ("supported", "suppression_transition", "both_withheld", "supported_population_changed"))])
    status_table = table("Support and comparability across the complete estimand grid",
        ["Comparison", "Comparable", r"\shortstack{Suppression\\changed}", r"\shortstack{Both\\withheld}", r"\shortstack{Supported sports\\changed}"], status_rows,
        "Counts cover all frozen regression estimands, including sensitivity specifications. No numerical difference is published when either estimate is withheld or the supported-sport population changes.")
    final_rows = []
    for key, label in selected:
        row = lookups["restored_repaired"][key]
        interval = ("withheld" if row["suppressed"] else
                    "["+number(row["ci95_low"])+", "+number(row["ci95_high"])+"]")
        final_rows.append([tex(label), evidence_cell(row), interval,
                           evidence_cell(lookups["restored_all"][key])])
    final_table = table("Adopted filtered and all-trades primary slopes",
        ["Model", "Filtered slope (SE)", r"Filtered 95\% interval", "All-trades slope (SE)"], final_rows,
        "The all-trades observations, support and uncertainty are invariant to reflagging under the same restored input coverage. Intervals retain the existing three-way clustered calculation. The inference still concerns legacy inferred BUY observations; corrected wallet identity does not establish counterparty economic action. Resolved-market censoring and qualified ATP event clocks remain inherited.")
    figures = r"""
\clearpage
\begin{figure}[!htbp]\centering
Filtered\\[3pt]\includegraphics[width=.98\linewidth]{figures/filtered_pooled_live_kernel.pdf}\\[5pt]
All trades\\[3pt]\includegraphics[width=.98\linewidth]{figures/all_trades_pooled_live_kernel.pdf}
\caption{Adopted pooled live D10--D1 calibration spread, with pointwise 95\% intervals.}
\begin{minipage}{\linewidth}\footnotesize Epanechnikov kernel averages use $h=.10$ and live observations only. Per-fill weights are one; equal-sport weights are $1/N_s$ over the complete retained all-pregame-plus-live tail sample. Shading uses the existing joint spread variance clustered by UTC day, buyer wallet and event. Curves break where either local tail has fewer than 500 fills; isolated supported grid points use capped interval whiskers. These are descriptive means, not regression predictions.\end{minipage}
\end{figure}
"""
    for sample, label in (("filtered", "Adopted filtered sample"), ("all_trades", "Restored all-trades sample")):
        for phase in ("pregame", "live"):
            method = (r"Pregame uses $h=.50$ and empirical time-quantile grid points on the raw normalized-time axis, without a lower cutoff."
                      if phase == "pregame" else r"Live uses $h=.10$ on $0\leq T\leq1$; pregame fills never enter a live estimate.")
            figures += rf"""
\clearpage
\begin{{figure}}[!htbp]\centering
\includegraphics[height=.80\textheight,width=\linewidth,keepaspectratio]{{figures/{sample}_sport_{phase}_kernel.pdf}}
\caption{{{label}: nine-sport {phase} D10--D1 calibration spread.}}
\begin{{minipage}}{{\linewidth}}\footnotesize {method} Each sport and phase uses per-fill kernel weights. Shading is a pointwise 95\% interval clustered by UTC day, buyer wallet and event; isolated supported grid points use capped interval whiskers. Curves break at local tail support below 500 or gaps wider than two bandwidths. The pregame/live pages share a vertical scale within each sample. The inferred BUY meaning and qualified ATP event clocks remain unchanged.\end{{minipage}}
\end{{figure}}
"""
    return r"""\documentclass[11pt]{article}
\usepackage[margin=0.75in]{geometry}
\usepackage{booktabs}
\usepackage{graphicx}
\usepackage[T1]{fontenc}
\usepackage{lmodern}
\setlength{\parindent}{0pt}
\setlength{\parskip}{5pt}
\begin{document}
\begin{center}\Large Nine-sport wallet-filter adoption\end{center}
\small
The current nine-sport results are rerun with corrected wallet attribution while separating restored MLB coverage and historical flag provenance.

Calibration is $Y-P$ for the legacy inferred outcome-token BUY. Exact UTC block time gives $T=(t-s)/(e-s)$; all pregame history and live fills through $T=1$ are retained. D1 and D10 are fixed price bins $[0,.1)$ and $[.9,1]$. The primary slope is the coefficient on $H T$, where $H=1$ for D10 and $H=0$ for D1. It is a change per normalized duration, not a total change over unbounded pregame history.

Numbers are rendered by \texttt{scripts/render\_sports\_wallet\_adoption.py} from the saved \texttt{comparison.json} produced by \texttt{scripts/estimate\_sports\_wallet\_adoption.py}. Full model and kernel artifacts accompany this compact comparison.
\normalsize
"""+count_table+flag_table+transition_table+estimate_table+delta_table+status_table+final_table+figures+"\n\\end{document}\n"


def final_kernel_inputs(manifest_path: Path, manifest: dict, comparison: dict) -> tuple[dict, dict]:
    """Reopen all bounded case summaries; only final kernels are plotted, never raw trades."""
    inputs, rows = {}, {}
    for name in CASES:
        expected = manifest["case_manifests"][name]
        require(expected["path"] == f"{name}/manifest.json", "Final case manifest path changed")
        path = manifest_path.parent/expected["path"]
        require({**summary_fingerprint(path), "path": expected["path"]} == expected and
                comparison["cases"][name]["manifest"] == expected, "Final case manifest fingerprint differs")
        run = load_estimates(path, expected_counts=comparison["cases"][name]["observation_counts"],
                             require_resources=True)
        require(run["manifest"]["trade_sample"] == comparison["cases"][name]["trade_sample"], "Final plot sample differs")
        require({row["estimand_id"]: project_estimand(row) for row in run["tables"]["estimands.parquet"]} ==
                {row["estimand_id"]: row for row in comparison["cases"][name]["estimands"]},
                "Final plot/report estimate generation differs")
        inputs[str(path.resolve())] = summary_fingerprint(path)
        for output in run["manifest"]["outputs"]:
            output_path = path.parent/output
            inputs[str(output_path.resolve())] = summary_fingerprint(output_path)
        if name in ("restored_repaired", "restored_all"):
            rows[name] = run["tables"]["kernel_time_spreads.parquet"]
    return inputs, rows


def plot_final_kernels(rows: dict, figures: Path) -> None:
    from analysis.multisport_game_dynamics import render_flb_decay as plots
    figures.mkdir()
    with plots.plt.rc_context({"font.size": 12, "axes.labelsize": 12,
                               "xtick.labelsize": 11, "ytick.labelsize": 11, "legend.fontsize": 11}):
        plot_pooled_kernel(rows["restored_repaired"], figures/FIGURES[0], plots)
        plot_pooled_kernel(rows["restored_all"], figures/FIGURES[1], plots)
    for name, sample in (("restored_repaired", "filtered"), ("restored_all", "all_trades")):
        for phase in ("pregame", "live"):
            plot_sport_phase(rows[name], phase, figures/f"{sample}_sport_{phase}_kernel.pdf", plots)
    for name in FIGURES:
        path = figures/name
        require(0 < path.stat().st_size <= CAPS["summary_file_bytes"] and
                path.read_bytes().startswith(b"%PDF-"), "Vector figure reopen/cap failed")


def draw_kernel(axis, rows: list[dict], color: str, plots, label: str | None = None) -> bool:
    """Keep legacy connected segments, adding point/interval marks for singletons."""
    drawn = plots._draw_kernel(axis, rows, color, label)
    segments = plots._supported_segments(rows)
    for index, segment in enumerate(segments):
        if len(segment) != 1:
            continue
        row = segment[0]
        estimate = row["spread_d10_minus_d1"]*100
        axis.errorbar([row["time_value"]], [estimate],
                      yerr=[[estimate-row["spread_ci95_low"]*100],
                            [row["spread_ci95_high"]*100-estimate]],
                      fmt="o", linestyle="none", color=color, ecolor=color,
                      capsize=2.5, markersize=4., elinewidth=.9,
                      label=label if index == 0 else None)
        drawn = True
    return drawn


def plot_pooled_kernel(rows: list[dict], output: Path, plots) -> None:
    """Legacy pooled layout with the same singleton-safe drawing as sport panels."""
    fig, axis = plots.plt.subplots(figsize=(7., 3.4), constrained_layout=True)
    for weighting, label, color in (("equal_fill", "Per fill", "#1f4e79"),
                                    ("equal_sport", "Equal sports", "#b24a33")):
        selected = [row for row in rows if row["scope"] == "pooled" and row["phase"] == "live" and
                    row["weighting"] == weighting]
        draw_kernel(axis, selected, color, plots, label)
    axis.axhline(0, color="black", linewidth=.8)
    axis.axvline(0, color="#666666", linewidth=.7)
    axis.set_xlim(0, 1)
    axis.set_xlabel("Normalized live time")
    axis.set_ylabel("D10 - D1 calibration spread (pp)")
    axis.grid(axis="y", color="#dddddd", linewidth=.6)
    axis.spines[["top", "right"]].set_visible(False)
    axis.legend(frameon=False, ncol=2)
    fig.savefig(output, format="pdf", bbox_inches="tight")
    plots.plt.close(fig)


def plot_sport_phase(rows: list[dict], phase: str, output: Path, plots) -> None:
    """Readable nine-panel layout; saved means, bands and segmentation are unchanged."""
    fig, axes = plots.plt.subplots(3, 3, figsize=(7., 7.), sharey=True, constrained_layout=True)
    supported = [row for row in rows if row["scope"] == "sport" and not row["suppressed"]]
    if supported:
        bounds = [0., *(row[field]*100 for row in supported for field in ("spread_ci95_low", "spread_ci95_high"))]
        low, high = min(bounds), max(bounds)
        margin = (high-low)*.05 if high > low else .05
        axes[0, 0].set_ylim(low-margin, high+margin)
    for index, sport in enumerate(SPORTS):
        axis = axes[index//3, index%3]
        selected = [row for row in rows if row["scope"] == "sport" and row["sport"] == sport and
                    row["phase"] == phase and row["weighting"] == "equal_fill"]
        drawn = draw_kernel(axis, selected, "#1f4e79", plots)
        axis.axhline(0, color="black", linewidth=.6)
        axis.axvline(0, color="#666666", linewidth=.6)
        axis.set_title(plots.SPORT_LABELS[sport], fontsize=11)
        if phase == "live":
            axis.set_xlim(0, 1)
            axis.set_xticks((0, .5, 1.))
        elif any(not row["suppressed"] for row in selected):
            axis.set_xlim(min(row["time_value"] for row in selected if not row["suppressed"]), 0)
        if not drawn:
            axis.text(.5, .5, "No locally supported\nestimate", transform=axis.transAxes,
                      ha="center", va="center", fontsize=10)
        axis.grid(axis="y", color="#e5e5e5", linewidth=.5)
        axis.spines[["top", "right"]].set_visible(False)
        axis.tick_params(labelsize=10)
        if index//3 == 2:
            axis.set_xlabel(f"{phase.title()} normalized time", fontsize=10)
        if index%3 == 0:
            axis.set_ylabel("D10 - D1 (pp)", fontsize=10)
    fig.savefig(output, format="pdf", bbox_inches="tight")
    plots.plt.close(fig)


def render(manifest_path: Path, target: Path) -> dict:
    require(not target.exists(), "Immutable report target exists")
    manifest = read_json(manifest_path)
    require(manifest.get("status") == "estimates_complete" and manifest.get("data_certified") is False,
            "Incomplete estimator publication")
    comparison_path = manifest_path.parent/"comparison.json"
    expected = manifest["outputs"]["comparison.json"]
    require({**summary_fingerprint(comparison_path), "path": "comparison.json"} == expected, "Comparison fingerprint differs")
    comparison = read_json(comparison_path)
    source = render_source(comparison)
    kernel_inputs, kernels = final_kernel_inputs(manifest_path, manifest, comparison)
    for path in (manifest_path.resolve(), comparison_path.resolve()):
        require(target.resolve() != path and target.resolve() not in path.parents and path not in target.resolve().parents, "Report overlaps inputs")
    inputs = {"manifest": summary_fingerprint(manifest_path), "comparison": summary_fingerprint(comparison_path), "kernel_summaries": kernel_inputs}
    sources = {"renderer": fingerprint(Path(__file__)), "plot_helper": fingerprint(PLOT_HELPER)}
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.staging-", dir=target.parent))
    try:
        plot_final_kernels(kernels, staging/"figures")
        tex_path = staging/"wallet_adoption_comparison.tex"
        tex_path.write_text(source)
        require(tex_path.read_text() == source and tex_path.stat().st_size <= CAPS["summary_file_bytes"], "LaTeX source reopen/cap failed")
        require(inputs["manifest"] == summary_fingerprint(manifest_path) and inputs["comparison"] == summary_fingerprint(comparison_path) and
                all(summary_fingerprint(Path(path)) == expected for path, expected in kernel_inputs.items()), "Report input changed")
        require(sources == {"renderer": fingerprint(Path(__file__)), "plot_helper": fingerprint(PLOT_HELPER)}, "Report source changed")
        result = {"schema_version": "sports_wallet_adoption_report_v1", "status": "source_rendered",
                  "compilation_status": "pending", "visual_qa_status": "pending", "data_certified": False,
                  "inputs": inputs, "source": sources,
                  "outputs": {tex_path.name: {**fingerprint(tex_path), "path": tex_path.name},
                    **{f"figures/{name}": {**fingerprint(staging/"figures"/name), "path": f"figures/{name}"} for name in FIGURES}}}
        write_json(staging/"manifest.json", result)
        atomic_publish(staging, target)
        return result
    except Exception as error:
        write_json(staging/"failure.json", {"status": "report_failed", "error": str(error)})
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    result = render(args.manifest.resolve(), args.run_dir.resolve())
    print(json.dumps({"status": result["status"], "tex": str(args.run_dir.resolve()/"wallet_adoption_comparison.tex")}))


if __name__ == "__main__":
    main()
