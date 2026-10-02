"""Render saved tennis clocks and maker-sequence evidence; never re-estimate."""
from __future__ import annotations

import argparse
import json
import math
import platform
import sys
from pathlib import Path
from typing import Any

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint, fingerprint, fresh_run, quoted, write_json,
)

SPORTS = ("mlb", "nfl", "nba", "nhl", "cbb", "atp", "epl", "cfb", "wnba")
SPORT_LABELS = {"mlb": "MLB", "nfl": "NFL", "nba": "NBA", "nhl": "NHL",
                "cbb": "Men's CBB", "atp": "ATP", "epl": "EPL",
                "cfb": "College football", "wnba": "WNBA"}
SAMPLES = ("filtered_trades", "all_trades")
COHORTS = ("all_atp", "grand_slam", "ao_provider_actual", "ao_same_cohort_scheduled")
COHORT_LABELS = {"all_atp": "All ATP", "grand_slam": "Grand Slams",
                 "ao_provider_actual": "AO: provider clock",
                 "ao_same_cohort_scheduled": "Same AO: legacy clock"}
GROUPS = ("all_maker_buys", "without_prior_winner_buy_link")
MEASURES = ("winner_sell_prevalence", "winner_sell_prior_buy", "longshot_buy_prior_winner_buy")
FLOOR = 500
PDF_METADATA = {"CreationDate": None, "ModDate": None, "Creator": "Saved-artifact report renderer"}


def tex(value: Any) -> str:
    replacements = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$",
                    "#": r"\#", "_": r"\_", "{": r"\{", "}": r"\}",
                    "~": r"\textasciitilde{}", "^": r"\textasciicircum{}"}
    return "".join(replacements.get(c, c) for c in str(value))


def count(value: Any) -> str:
    return "withheld" if value is None else f"{int(value):,}"


def number(value: Any, *, scale: float = 1.0, signed: bool = False) -> str:
    if value is None:
        return "withheld"
    if not math.isfinite(float(value)):
        raise ValueError("Nonfinite saved estimate")
    return format(float(value) * scale, "+.2f" if signed else ".2f")


def indexed(rows: list[dict[str, Any]], keys: tuple[str, ...], required: tuple[str, ...] = ()) -> dict:
    result = {}
    for row in rows:
        if not set(keys + required) <= set(row):
            raise ValueError(f"Missing saved columns: {sorted(set(keys + required) - set(row))}")
        key = tuple(row[k] for k in keys)
        if key in result:
            raise ValueError(f"Duplicate summary grain: {key}")
        result[key] = row
    return result


def load_stage(directory: Path, names: tuple[str, ...], manifest_name: str = "manifest.json") -> tuple[dict, dict, dict]:
    manifest_path = directory / manifest_name
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("status", manifest.get("completion_status")) != "complete":
        raise ValueError(f"Incomplete source publication: {directory}")
    consumed = {manifest_name: fingerprint(manifest_path)}
    data = {}
    con = duckdb.connect()
    try:
        for name in names:
            path = directory / name
            actual = artifact_fingerprint(path)
            if manifest.get("outputs", {}).get(name) != actual:
                raise ValueError(f"Saved output fingerprint mismatch: {path}")
            consumed[name] = fingerprint(path)
            cursor = con.execute(f"SELECT * FROM read_parquet('{quoted(path)}')")
            columns = [c[0] for c in cursor.description]
            data[name[:-8]] = [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        con.close()
    return manifest, data, consumed


def validate_tennis(data: dict) -> None:
    coverage = indexed(data["cohort_coverage"], ("cohort", "sample"),
                       ("accepted_events", "n_fills", "n_pregame", "n_live", "n_post_end"))
    for cohort in COHORTS:
        for sample in SAMPLES:
            row = coverage[(cohort, sample)]
            if row["n_fills"] != row["n_pregame"] + row["n_live"] + row["n_post_end"]:
                raise ValueError("Phase support fails to reconcile")
    slam_counts = grand_slam_coverage(data["event_cohort"])
    if any(sum(slam_counts.values()) != coverage[("grand_slam", sample)]["accepted_events"] for sample in SAMPLES):
        raise ValueError("Grand Slam metadata count disagrees with saved coverage")
    for sample in SAMPLES:
        a, b = [coverage[(c, sample)] for c in COHORTS[2:]]
        if a["accepted_events"] != b["accepted_events"] or a["n_fills"] != b["n_fills"]:
            raise ValueError("AO clock comparison does not preserve source membership")
    curves = indexed(data["kernel_tail_curves"], ("cohort", "sample", "evaluation_time"),
                     ("weighting", "bandwidth", "d1_n", "d10_n", "d1_error", "d10_error",
                      "spread_d10_minus_d1", "suppressed", "uncertainty_status"))
    for cohort in COHORTS:
        for sample in SAMPLES:
            selected = sorted((r for r in curves.values() if r["cohort"] == cohort and r["sample"] == sample),
                              key=lambda r: r["evaluation_time"])
            if len(selected) != 51 or not np.allclose([r["evaluation_time"] for r in selected], np.linspace(0, 1, 51)):
                raise ValueError("Unexpected saved kernel grid")
            for r in selected:
                if r["weighting"] != "equal_fill" or r["bandwidth"] != .10 or r["uncertainty_status"] != "point_estimate_only":
                    raise ValueError("Unsupported kernel estimator contract")
                check_estimate(r, ("d1_error", "d10_error", "spread_d10_minus_d1"),
                               r["d1_n"] < FLOOR or r["d10_n"] < FLOOR)
    support = indexed(data["clock_comparison_support"], ("sample",))
    for sample in SAMPLES:
        if not support[(sample,)]["same_scoped_fill_membership"]:
            raise ValueError("AO phase comparison changed scoped fill membership")
    indexed(data["clock_phase_assignments"], ("sample", "scheduled_phase", "provider_phase"))
    tails = indexed(data["tail_contrasts"], ("cohort", "sample", "weighting", "time_bin"))
    for cohort in COHORTS:
        for sample in SAMPLES:
            fill, dollar = [tails[(cohort, sample, weighting, 10)] for weighting in ("equal_fill", "dollar")]
            if any(fill[k] != dollar[k] for k in ("d1_n", "d10_n")):
                raise ValueError("Dollar and count tennis tails changed fill support")
            check_estimate(dollar, ("d1_error", "d10_error", "spread_d10_minus_d1"),
                           dollar["d1_n"] < FLOOR or dollar["d10_n"] < FLOOR)
    profile = indexed(data["calibration_profile"], ("cohort", "sample", "weighting", "time_bin", "price_bin"))
    for cohort in COHORTS:
        for sample in SAMPLES:
            for bin_id in range(1, 11):
                r = profile[(cohort, sample, "equal_fill", 10, bin_id)]
                check_estimate(r, ("mean_calibration",), r["n_fills"] < FLOOR)
    if len(data["clock_offset_summary"]) != 1:
        raise ValueError("Expected one saved AO offset summary")


def check_estimate(row: dict, fields: tuple[str, ...], expected_suppression: bool) -> None:
    if bool(row["suppressed"]) != expected_suppression:
        raise ValueError("Saved suppression contradicts support")
    for field in fields:
        value = row[field]
        if row["suppressed"]:
            if value is not None:
                raise ValueError("Suppressed estimate must be null")
        elif value is None or not math.isfinite(float(value)):
            raise ValueError("Supported estimate must be finite")


def validate_wallet(wallet: dict, reader: dict) -> None:
    profiles = indexed(wallet["linked_buy_profile"], ("sample", "window_id", "sport", "buy_group", "price_bin"),
                       ("n_fills", "suppressed", "calibration_equal_fill"))
    tails = indexed(wallet["linked_buy_tails"], ("sample", "window_id", "sport", "buy_group"),
                    ("d1_n", "d10_n", "suppressed", "spread_equal_fill", "spread_dollar"))
    shares = indexed(reader["conditional_shares"], ("sample", "window_id", "sport", "measure"),
                     ("numerator_fills", "denominator_fills", "fill_share", "support_status"))
    comparisons = indexed(reader["phase_comparisons"], ("sample", "sport", "measure"))
    for sport in SPORTS:
        for sample in SAMPLES:
            for group in GROUPS:
                for bin_id in range(1, 11):
                    row = profiles[(sample, "t99_100", sport, group, bin_id)]
                    check_estimate(row, ("calibration_equal_fill",), row["n_fills"] < FLOOR)
                row = tails[(sample, "t99_100", sport, group)]
                check_estimate(row, ("spread_equal_fill", "spread_dollar"), row["d1_n"] < FLOOR or row["d10_n"] < FLOOR)
            for measure in MEASURES:
                row = shares[(sample, "t99_100", sport, measure)]
                n, d, ratio = row["numerator_fills"], row["denominator_fills"], row["fill_share"]
                if n < 0 or d < n or (d == 0 and ratio is not None):
                    raise ValueError("Invalid conditional support")
                if d and (ratio is None or not math.isclose(ratio, n / d, abs_tol=1e-12)):
                    raise ValueError("Saved conditional ratio fails count reconciliation")
                if row["support_status"] != "descriptive_counts_no_minimum":
                    raise ValueError("Unexpected descriptive-share contract")
            comparison = comparisons[(sample, sport, "winner_sell_prevalence")]
            if comparison["early_window_id"] != "t80_90" or comparison["late_window_id"] != "t99_100":
                raise ValueError("Unexpected saved early/late windows")
            for prefix in ("early", "late"):
                n, d, ratio = [comparison[f"{prefix}_{suffix}"] for suffix in ("numerator_fills", "denominator_fills", "fill_share")]
                if n < 0 or d < n or (d == 0 and ratio is not None):
                    raise ValueError("Invalid phase-comparison support")
                if d and (ratio is None or not math.isclose(ratio, n / d, abs_tol=1e-12)):
                    raise ValueError("Saved phase ratio fails reconciliation")
            early, late, change = [comparison[k] for k in ("early_fill_share", "late_fill_share", "change_in_fill_share")]
            if early is not None and late is not None and (change is None or not math.isclose(change, late - early, abs_tol=1e-12)):
                raise ValueError("Saved phase change fails reconciliation")


def table(caption: str, columns: str, header: list[str], body: list[list[str]], note: str) -> str:
    return "\n".join([
        r"\begin{table}[!htbp]\centering", r"\begin{threeparttable}",
        rf"\caption{{{caption}}}", r"\small", rf"\begin{{tabular}}{{{columns}}}",
        r"\toprule", " & ".join(header) + r" \\", r"\midrule",
        *[" & ".join(row) + r" \\" for row in body],
        r"\bottomrule", r"\end{tabular}",
        r"\begin{tablenotes}[flushleft]\footnotesize", rf"\item {note}",
        r"\end{tablenotes}\end{threeparttable}\end{table}",
    ])


def grand_slam_coverage(rows: list[dict]) -> dict[str, int]:
    metadata = indexed(rows, ("event_slug",), ("grand_slam_name", "is_grand_slam"))
    result = {"Australian Open": 0, "Roland-Garros": 0}
    for row in metadata.values():
        if not isinstance(row["is_grand_slam"], bool):
            raise ValueError("Missing saved Grand Slam membership flag")
        if row["is_grand_slam"]:
            name = row["grand_slam_name"]
            if name not in result:
                raise ValueError(f"Unexpected saved Grand Slam tournament: {name}")
            result[name] += 1
    return result


def tennis_tables(data: dict, source_manifest: dict) -> str:
    coverage = indexed(data["cohort_coverage"], ("cohort", "sample"))
    body = []
    for cohort in COHORTS:
        filtered, all_rows = [coverage[(cohort, s)] for s in SAMPLES]
        body.append([tex(COHORT_LABELS[cohort]), count(filtered["accepted_events"]),
                     count(filtered["n_live"]), count(all_rows["n_live"]),
                     "Provider recorded" if cohort == "ao_provider_actual" else "Legacy synthetic"])
    c = source_manifest["counts"]
    slam_counts = grand_slam_coverage(data["event_cohort"])
    result = table("Tennis coverage and live-fill support", "lrrrl",
                   ["Scope", "Events", "Filtered live", "All live", "Clock"], body,
                   "Filtered: $0.01<p<0.99$, flagged nonhuman buyers removed. All: $0<p<1$. "
                   f"Grand Slam membership: {count(slam_counts['Australian Open'])} Australian Open and {count(slam_counts['Roland-Garros'])} Roland-Garros matches. "
                   "Legacy clocks use scheduled start plus archived duration. AO source gates: "
                   f"{count(c['accepted_frozen_ao_events'])} matched, {count(c['passed_boundary_gates_events'])} boundary-valid, "
                   f"{count(c['actual_timing_events'])} competitive-chronology-valid. "
                   f"First completed-point records are {number(c['first_point_delta_min_seconds'])}--{number(c['first_point_delta_max_seconds'])} "
                   "seconds after the minute-precision start; they are not first-serve records. "
                   f"{count(len(data['legacy_duration_mismatches']))} corrected Grand Slam identities retain legacy durations assigned from other tournaments; those clocks are not silently corrected. "
                   "No event has a certified second-exact first serve. Resolved-market coverage remains subject to sample-end censoring.")
    r = data["clock_offset_summary"][0]
    body = [[label, *[number(r[f"{stat}_{key}_offset_seconds"], scale=1/60, signed=True)
                       for stat in ("min", "median", "mean", "max")],
             number(r[f"p90_absolute_{key}_offset_seconds"], scale=1/60)]
            for key, label in (("start", "Start"), ("end", "End"))]
    result += table("Same AO events: provider minus legacy clock (minutes)", "lrrrrr",
                    ["Boundary", "Min", "Median", "Mean", "Max", r"P90 $|\Delta|$"], body,
                    "Positive means later under the provider clock. The largest end difference is "
                    f"{number(r['max_end_offset_seconds'], scale=1/60)} minutes. "
                    "Native start is minute-precision; end is the recorded terminal competitive point. Provider latency is unquantified.")
    assignments = indexed(data["clock_phase_assignments"], ("sample", "scheduled_phase", "provider_phase"))
    support = indexed(data["clock_comparison_support"], ("sample",))
    body = []
    for sample, label in zip(SAMPLES, ("Filtered", "All")):
        for phase, phase_label in (("pregame", "Pre"), ("live", "Live"), ("post", "Post")):
            body.append([label, phase_label, *[count(assignments[(sample, phase, p)]["n_fills"])
                                               for p in ("pregame", "live", "post")]])
    result += table("AO phase assignment on identical scoped fills", "llrrr",
                    ["Sample", "Legacy phase", "Provider pre", "Provider live", "Provider post"], body,
                    "Rows are legacy assignments; columns are provider assignments. Scoped fill membership is identical before phase assignment. "
                    f"Changed phase: {count(support[('filtered_trades',)]['n_phase_changed'])} filtered and "
                    f"{count(support[('all_trades',)]['n_phase_changed'])} all fills.")
    return result


def kernel_figure(rows: list[dict], path: Path) -> dict:
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "pdf.fonttype": 42,
                         "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(2, 3, figsize=(10, 6.2), sharex=True, sharey="col")
    styles = (("#222222", "-"), ("#777777", "--"), ("#007F7B", "-"), ("#B65C18", ":"))
    fields = ("d1_error", "d10_error", "spread_d10_minus_d1")
    titles = ("Lowest-price bin (D1)", "Highest-price bin (D10)", "D10 minus D1")
    evidence = []
    for i, sample in enumerate(SAMPLES):
        for j, field in enumerate(fields):
            ax = axes[i, j]
            ax.axhline(0, color="#888888", lw=.7)
            for cohort, (color, line) in zip(COHORTS, styles):
                selected = sorted((r for r in rows if r["sample"] == sample and r["cohort"] == cohort),
                                  key=lambda r: r["evaluation_time"])
                x = [r["evaluation_time"] for r in selected]
                y = [np.nan if r["suppressed"] else 100 * r[field] for r in selected]
                ax.plot(x, y, color=color, linestyle=line, lw=1.7, label=COHORT_LABELS[cohort])
                if j == 0:
                    evidence.extend({k: r[k] for k in ("cohort", "sample", "evaluation_time", "d1_n", "d10_n",
                                                       "d1_error", "d10_error", "spread_d10_minus_d1", "suppressed")}
                                    for r in selected)
            ax.set_xlim(0, 1)
            ax.set_xticks((0, .25, .5, .75, 1))
            if i == 0:
                ax.set_title(titles[j])
            if i == 1:
                ax.set_xlabel("Recorded elapsed fraction T")
            if j == 0:
                ax.set_ylabel(("Filtered" if i == 0 else "All") + "\nCalibration (pp)")
            if i == 0:
                actual = [r for r in rows if r["sample"] == sample and r["cohort"] == "ao_provider_actual"]
                if all(r["suppressed"] for r in actual):
                    ax.text(.03, .96, "AO clock pair withheld", transform=ax.transAxes, va="top", fontsize=8)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=4, frameon=False, fontsize=8)
    fig.tight_layout(rect=(0, .065, 1, 1))
    fig.savefig(path, metadata=PDF_METADATA)
    plt.close(fig)
    return {"kernel_grid": evidence}


def tennis_tail_table(rows: list[dict]) -> str:
    look = indexed(rows, ("cohort", "sample", "weighting", "time_bin"))
    body = []
    for cohort in COHORTS:
        for sample, label in zip(SAMPLES, ("Filtered", "All")):
            r = look[(cohort, sample, "equal_fill", 10)]
            dollar = look[(cohort, sample, "dollar", 10)]
            body.append([tex(COHORT_LABELS[cohort]), label, count(r["d1_n"]), count(r["d10_n"]),
                         number(r["d1_error"], scale=100, signed=True), number(r["d10_error"], scale=100, signed=True),
                         number(r["spread_d10_minus_d1"], scale=100, signed=True),
                         number(dollar["spread_d10_minus_d1"], scale=100, signed=True)])
    return table(r"Tennis final 10\%: discrete tail support and calibration", "llrrrrrr",
                 ["Scope", "Sample", "$n_1$", "$n_{10}$", "D1", "D10", "Spread", r"\$ spread"], body,
                 r"Equal-fill estimates in $0.9\leq T\leq1$. Each tail must contain at least 500 fills; otherwise estimates are withheld. "
                 "D1, D10, and spreads are percentage points; the final column uses gross-collateral-dollar weighting with the same fill support. "
                 "These fixed-bin contrasts are distinct from the kernel endpoint. Descriptive point estimates only.")


def tennis_profile_figure(rows: list[dict], path: Path) -> dict:
    selected = [r for r in rows if r["time_bin"] == 10 and r["weighting"] == "equal_fill"]
    fig, axes = plt.subplots(2, 2, figsize=(10, 6.2), sharex=True, sharey=True)
    evidence = []
    styles = (("#222222", "o", -.06), ("#777777", "^", .06),
              ("#007F7B", "s", -.06), ("#B65C18", "D", .06))
    for i, sample in enumerate(SAMPLES):
        for j, cohorts in enumerate((COHORTS[:2], COHORTS[2:])):
            ax = axes[i, j]
            ax.axhline(0, color="#888888", lw=.7)
            statuses = []
            for cohort, (color, marker, offset) in zip(cohorts, styles[j * 2:j * 2 + 2]):
                points = sorted((r for r in selected if r["cohort"] == cohort and r["sample"] == sample), key=lambda r: r["price_bin"])
                valid = [r for r in points if not r["suppressed"]]
                ax.scatter([r["price_bin"] + offset for r in valid], [100 * r["mean_calibration"] for r in valid],
                           color=color, marker=marker, s=28, label=COHORT_LABELS[cohort], linewidths=.5)
                statuses.append(str(len(valid)))
                evidence.extend({k: r[k] for k in ("cohort", "sample", "price_bin", "n_fills", "n_events", "mean_calibration", "suppressed")}
                                for r in points)
            ax.text(.03, .96, "Supported bins: " + "/".join(statuses) + " of 10", transform=ax.transAxes, va="top", fontsize=8)
            ax.set_xticks(range(1, 11))
            ax.set_xlim(.5, 10.5)
            ax.legend(loc="lower left", frameon=False, fontsize=8)
            if i == 0:
                ax.set_title("ATP and tournament restriction" if j == 0 else "Same AO events, two clocks")
            else:
                ax.set_xlabel("Fixed price bin")
            if j == 0:
                ax.set_ylabel(("Filtered" if i == 0 else "All") + "\nCalibration (pp)")
    fig.tight_layout()
    fig.savefig(path, metadata=PDF_METADATA)
    plt.close(fig)
    return {"tennis_bin_profile": evidence}


def wallet_phase_table(rows: list[dict]) -> str:
    look = indexed(rows, ("sample", "sport", "measure"))
    body = []
    for sport in SPORTS:
        for sample, label in zip(SAMPLES, ("Filtered", "All")):
            r = look[(sample, sport, "winner_sell_prevalence")]
            body.append([tex(SPORT_LABELS[sport]), label, count(r["early_denominator_fills"]),
                         number(r["early_fill_share"], scale=100), count(r["late_denominator_fills"]),
                         number(r["late_fill_share"], scale=100), number(r["change_in_fill_share"], scale=100, signed=True)])
    return table(r"Winner SELL prevalence: 80--90\% versus final 1\%", "llrrrrr",
                 ["Sport", "Sample", "Early actions", r"Early \%", "Late actions", r"Late \%", r"$\Delta$ (pp)"], body,
                 r"Winner SELL / all maker actions, using saved fill-count ratios. Early window is $0.8\leq T<0.9$; late is $0.99\leq T\leq1$. "
                 "Different window widths are not a trading-intensity comparison. Winner status is retrospective, histories are maker-side only, "
                 "and EPL outcomes refer to binary markets. Empty denominators are withheld.")


def wallet_tables(wallet: dict, reader: dict) -> tuple[str, str]:
    shares = indexed(reader["conditional_shares"], ("sample", "window_id", "sport", "measure"))
    body = []
    for sport in SPORTS:
        for sample, label in zip(SAMPLES, ("Filtered", "All")):
            a, b, c = [shares[(sample, "t99_100", sport, m)] for m in MEASURES]
            body.append([tex(SPORT_LABELS[sport]), label, count(a["denominator_fills"]),
                         count(a["numerator_fills"]), number(a["fill_share"], scale=100),
                         number(b["fill_share"], scale=100), count(c["denominator_fills"]),
                         number(c["fill_share"], scale=100)])
    result = table(r"Final 1\%: observed maker sequences", "llrrrrrr",
                   ["Sport", "Sample", "Actions", "W sells", r"W sell \%", r"Prior buy \%", "D1 buys", r"Linked \%"], body,
                   "Fill-count shares. W sell: eventual-winner SELL / all maker actions. Prior buy: winner SELL with an earlier same-token BUY / winner SELL. "
                   "Linked: D1 maker BUY with an earlier complement-winner BUY / D1 maker BUY. Ratios have no minimum-count suppression; empty denominators are withheld. "
                   "History is partial maker-side trading, not balances or profitable exits; taker actions and nontrade token movements are absent. "
                   "Eventual winner is retrospective. EPL refers to matched binary outcome markets, not an unconditional team-win claim. ATP wallet clocks retain legacy timing.")
    tails = indexed(wallet["linked_buy_tails"], ("sample", "window_id", "sport", "buy_group"))
    body = []
    for sport in SPORTS:
        for sample, label in zip(SAMPLES, ("Filtered", "All")):
            a, b = [tails[(sample, "t99_100", sport, group)] for group in GROUPS]
            body.append([tex(SPORT_LABELS[sport]), label, count(a["d1_n"]), count(a["d10_n"]),
                         number(a["spread_equal_fill"], scale=100, signed=True), count(b["d1_n"]), count(b["d10_n"]),
                         number(b["spread_equal_fill"], scale=100, signed=True)])
    tail_table = table(r"Final 1\% maker BUYs: before and after excluding prior-winner links", "llrrrrrr",
                    ["Sport", "Sample", "$n_1$ before", "$n_{10}$ before", "Spread", "$n_1$ after", "$n_{10}$ after", "Spread"], body,
                    "Spread is equal-fill D10 minus D1 calibration, in percentage points. After removes BUYs linked to an earlier BUY of the eventual winning complement "
                    "by the same maker in the same binary market; the focal transaction is excluded from prior history. At least 500 fills in each tail are required. "
                    "A prior winning-complement link retrospectively selects focal losing-side BUYs; removal is not causal evidence of exits or a balance adjustment.")
    dollar_body = []
    for sport in SPORTS:
        for sample, label in zip(SAMPLES, ("Filtered", "All")):
            a, b = [tails[(sample, "t99_100", sport, group)] for group in GROUPS]
            dollar_body.append([tex(SPORT_LABELS[sport]), label,
                                count(a["d1_n"]) + " / " + count(a["d10_n"]),
                                count(b["d1_n"]) + " / " + count(b["d10_n"]),
                                number(a["spread_dollar"], scale=100, signed=True), number(b["spread_dollar"], scale=100, signed=True)])
    tail_table += table(r"Final 1\% maker BUYs: dollar-weighted before/after spread", "llrrrr",
                        ["Sport", "Sample", "$n_1/n_{10}$ before", "$n_1/n_{10}$ after", "Before (pp)", "After (pp)"], dollar_body,
                        "Saved gross-collateral-dollar-weighted D10 minus D1 calibration. Same fills, prior-link exclusion, and 500-fill-per-tail support gate as the count-weighted table. "
                        "Weighting can change the sign; a spread alone does not establish the full favorite--longshot pattern. Descriptive point estimates only.")
    return result, tail_table


def wallet_figure(rows: list[dict], path: Path) -> dict:
    selected = [r for r in rows if r["window_id"] == "t99_100" and r["sample"] in SAMPLES and r["buy_group"] in GROUPS]
    fig, axes = plt.subplots(3, 3, figsize=(10, 8), sharex=True, sharey=True)
    styles = (("#222222", "o", -.15), ("#007F7B", "s", -.05),
              ("#777777", "^", .05), ("#B65C18", "D", .15))
    series = [(s, g) for s in SAMPLES for g in GROUPS]
    labels = ("Filtered: before", "Filtered: after", "All: before", "All: after")
    evidence = []
    for ax, sport in zip(axes.flat, SPORTS):
        ax.axhline(0, color="#888888", lw=.7)
        statuses = []
        for (sample, group), label, (color, marker, offset) in zip(series, labels, styles):
            points = sorted((r for r in selected if r["sport"] == sport and r["sample"] == sample and r["buy_group"] == group),
                            key=lambda r: r["price_bin"])
            valid = [r for r in points if not r["suppressed"]]
            ax.scatter([r["price_bin"] + offset for r in valid], [100 * r["calibration_equal_fill"] for r in valid],
                       color=color, marker=marker, s=18, label=label, linewidths=.5)
            statuses.append(str(len(valid)))
            evidence.extend({k: r[k] for k in ("sport", "sample", "buy_group", "price_bin", "n_fills", "n_events",
                                               "calibration_equal_fill", "suppressed")} for r in points)
        ax.set_title(SPORT_LABELS[sport])
        ax.text(.03, .97, "Supported bins: " + "/".join(statuses), transform=ax.transAxes, va="top", fontsize=7)
        ax.set_xticks(range(1, 11))
        ax.set_xlim(.5, 10.5)
    for ax in axes[:, 0]:
        ax.set_ylabel("Calibration (pp)")
    for ax in axes[-1]:
        ax.set_xlabel("Fixed price bin")
    handles, legend_labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, legend_labels, loc="lower center", ncol=4, frameon=False, fontsize=8)
    fig.tight_layout(rect=(0, .055, 1, 1))
    fig.savefig(path, metadata=PDF_METADATA)
    plt.close(fig)
    return {"wallet_bin_profile": evidence}


def report_tex(tennis: dict, source_manifest: dict, wallet: dict, reader: dict) -> str:
    ao_filtered = [r for r in tennis["kernel_tail_curves"]
                   if r["cohort"] == "ao_provider_actual" and r["sample"] == "filtered_trades"]
    withheld_note = ("The filtered AO clock pair is entirely withheld. "
                     if all(r["suppressed"] for r in ao_filtered) else "")
    activity_table, exclusion_table = wallet_tables(wallet, reader)
    return r"""\documentclass[10pt]{article}
\usepackage[letterpaper,margin=0.7in]{geometry}
\usepackage[T1]{fontenc}
\usepackage{booktabs,threeparttable,graphicx,amsmath}
\usepackage{microtype}
\setlength{\parindent}{0pt}
\setlength{\parskip}{4pt}
\renewcommand{\arraystretch}{1.12}
\begin{document}
\begin{center}{\Large Tennis timing and terminal maker sequences}\end{center}
Saved evidence from accepted resolved sports markets; tennis timing comparisons and maker-side sequence diagnostics.

Calibration is $Y-P$, eventual contract outcome minus executed price, before fees. Tennis retains the frozen exposure-normalized fills and inferred direction; clock comparisons hold those inputs fixed. Wallet results use verified own-maker BUY/SELL actions. All effects below are descriptive point estimates; no intervals were computed.
Fixed price bins have width 0.1 (D1 lowest, D10 highest). Filtered fills exclude flagged nonhuman actors and require $0.01<p<0.99$; all fills require $0<p<1$.
Classic favorite--longshot bias has D1 $<0$ and D10 $>0$; the signed spread alone does not establish that pattern.
Normalized time is $T=(\text{trade UTC}-\text{start UTC})/(\text{end UTC}-\text{start UTC})$; live fills satisfy $0\leq T\leq1$.
{\footnotesize Sources: \texttt{collect\_ao\_actual\_timing} produces \texttt{actual\_timing}; \texttt{tennis\_timing\_cohort} produces the coverage, clock, calibration-profile and kernel artifacts. \texttt{wallet\_exit\_audit} produces maker profiles; \texttt{summarize\_wallet\_exit\_audit} produces conditional shares and early/late comparisons.}
\section*{Tennis scope and clock comparison}
""" + tennis_tables(tennis, source_manifest) + r"""
\clearpage
\section*{Complete tennis final-10\% price-bin profiles}
\begin{figure}[!htbp]\centering
\includegraphics[width=\linewidth]{tennis_price_bins.pdf}
\caption{Equal-fill calibration across all ten fixed price bins in $0.9\leq T\leq1$. Isolated marks are saved point estimates, not connected bins; cells with fewer than 500 fills are omitted. Panel counts give supported bins in legend order; omitted bins are withheld, not zero. The two AO series retain the same events and scoped fills before phase assignment. Native provider start is minute-precision and provider latency is unquantified.}
\end{figure}
""" + tennis_tail_table(tennis["tail_contrasts"]) + r"""
\clearpage
\section*{Complete tennis tail trajectories}
\begin{figure}[!htbp]\centering
\includegraphics[width=\linewidth]{tennis_kernel.pdf}
\caption{Equal-fill kernel calibration in the two price tails and their spread. Epanechnikov bandwidth $h=0.10$; 51 saved grid points using only live fills. Curves omit grid points with fewer than 500 positive-weight fills in either tail. """ + withheld_note + r"""The AO pair uses identical events and scoped fills before phase assignment. Start is minute-precision and terminal point time is provider recorded, with unquantified latency; neither clock is a certified physical first-serve-to-end interval.}
\end{figure}
""" + r"""
\clearpage
\section*{Terminal maker activity and observed prior BUYs}
""" + wallet_phase_table(reader["phase_comparisons"]) + activity_table + r"""
\clearpage
\section*{Prior-winner link exclusion and tail support}
""" + exclusion_table + r"""
\clearpage
\section*{Complete final-1\% maker BUY calibration profiles}
\begin{figure}[!htbp]\centering
\includegraphics[width=\linewidth]{wallet_price_bins.pdf}
\caption{Equal-fill calibration before and after removing prior-winner BUY links, in the final 1\% of recorded elapsed time. Isolated marks are fixed price-bin estimates; suppressed bins ($n<500$) have no mark. Panel counts give supported bins out of ten in legend order. Shared axes preserve cross-sport comparison; no uncertainty intervals were computed. The before/after tail support and spread are in the preceding table.}
\end{figure}
\end{document}
"""


def render(args: argparse.Namespace) -> dict:
    tennis_dir, source_dir, wallet_dir, reader_dir = [Path(getattr(args, key)).resolve()
                                                   for key in ("tennis_dir", "source_dir", "wallet_dir", "reader_dir")]
    tm, tennis, ti = load_stage(tennis_dir, ("cohort_coverage.parquet", "clock_offset_summary.parquet",
                                           "clock_phase_assignments.parquet", "clock_comparison_support.parquet",
                                           "kernel_tail_curves.parquet", "tail_contrasts.parquet", "calibration_profile.parquet",
                                           "legacy_duration_mismatches.parquet", "event_cohort.parquet"))
    tennis["event_cohort"] = [{key: row[key] for key in ("event_slug", "grand_slam_name", "is_grand_slam")}
                              for row in tennis["event_cohort"]]
    sm, source, si = load_stage(source_dir, ("actual_timing.parquet",), "actual_timing_manifest.json")
    wm, wallet, wi = load_stage(wallet_dir, ("linked_buy_profile.parquet", "linked_buy_tails.parquet"))
    rm, reader, ri = load_stage(reader_dir, ("conditional_shares.parquet", "phase_comparisons.parquet"))
    validate_tennis(tennis)
    validate_wallet(wallet, reader)
    if tm.get("schema_version") != 4 or tm.get("stage") != "atp_timing_cohort_audit_v4":
        raise ValueError("Report requires corrected identity and saved dollar-weighted tennis publication")
    if len(tennis["legacy_duration_mismatches"]) != tm["counts"]["legacy_archive_duration_mismatches"]:
        raise ValueError("Legacy duration audit count disagrees")
    if len(source["actual_timing"]) != sm["counts"]["actual_timing_events"]:
        raise ValueError("AO source count does not reconcile")
    if not all(r["competitive_chronology_valid"] for r in source["actual_timing"]):
        raise ValueError("AO evidence is not chronology qualified")
    if tm["counts"]["provider_actual_events"] != len(source["actual_timing"]):
        raise ValueError("Tennis analysis/source AO coverage disagrees")
    coverage = indexed(tennis["cohort_coverage"], ("cohort", "sample"))
    for cohort, key in (("all_atp", "accepted_atp_events"), ("grand_slam", "grand_slam_events"),
                        ("ao_provider_actual", "provider_actual_events"), ("ao_same_cohort_scheduled", "provider_actual_events")):
        if any(coverage[(cohort, s)]["accepted_events"] != tm["counts"][key] for s in SAMPLES):
            raise ValueError("Coverage table/source manifest count disagreement")
    script = Path(__file__).resolve()
    tests = script.parents[2] / "tests" / "test_render_tennis_wallet_investigation.py"
    with fresh_run(args.run_dir, (tennis_dir, source_dir, wallet_dir, reader_dir)) as staging:
        evidence = kernel_figure(tennis["kernel_tail_curves"], staging / "tennis_kernel.pdf")
        evidence.update(tennis_profile_figure(tennis["calibration_profile"], staging / "tennis_price_bins.pdf"))
        evidence.update(wallet_figure(wallet["linked_buy_profile"], staging / "wallet_price_bins.pdf"))
        evidence["table_sources"] = {"tennis": {k: v for k, v in tennis.items() if k != "legacy_duration_mismatches"},
                                     "wallet_tails": wallet["linked_buy_tails"], "wallet_reader": reader}
        (staging / "tennis_wallet_investigation.tex").write_text(report_tex(tennis, sm, wallet, reader), encoding="utf-8")
        write_json(staging / "displayed_evidence.json", evidence)
        outputs = ("tennis_wallet_investigation.tex", "tennis_kernel.pdf", "tennis_price_bins.pdf", "wallet_price_bins.pdf", "displayed_evidence.json")
        manifest = {
            "status": "complete", "schema_version": 3, "stage": "tennis_wallet_report_v3", "command": sys.argv,
            "inputs": {"tennis": ti, "ao_source": si, "wallet": wi, "wallet_reader": ri},
            "outputs": {name: artifact_fingerprint(staging / name) for name in outputs},
            "code": {"script": fingerprint(script), "tests": fingerprint(tests)},
            "environment": {"python": platform.python_version(), "duckdb": duckdb.__version__,
                            "matplotlib": matplotlib.__version__, "numpy": np.__version__, "platform": platform.platform()},
            "contract": {"computation": "format saved estimates only; no estimation or raw scans",
                         "uncertainty": "descriptive point estimates only",
                         "suppression": "saved support floor 500; suppressed estimates omitted, not plotted at zero",
                         "wallet_plots": "isolated fixed-bin marks; no point-connecting paths",
                         "publication": "immutable atomic stage; portable primary LaTeX source"},
            "counts": {"atp_events": tm["counts"]["accepted_atp_events"], "grand_slam_events": tm["counts"]["grand_slam_events"],
                       "grand_slam_tournaments": grand_slam_coverage(tennis["event_cohort"]),
                       "ao_events": len(source["actual_timing"]), "wallet_sports": len(SPORTS)},
        }
        write_json(staging / "manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("tennis_dir", "source_dir", "wallet_dir", "reader_dir", "run_dir"):
        parser.add_argument("--" + name.replace("_", "-"), required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    print(json.dumps(render(parse_args()), indent=2, sort_keys=True))
