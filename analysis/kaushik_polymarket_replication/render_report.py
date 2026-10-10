"""Render accepted saved estimates; this module never estimates or scans trades.

The report-only entry point is intentionally usable on a Mac. Production input
admission and estimation live elsewhere. JSON controls and grids are fail-closed;
figures use saved CR0 intervals and discrete isolated marks only.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping

SPORTS = ("mlb", "nfl", "nba", "nhl", "cbb", "atp", "epl", "cfb", "wnba")
SCOPES = ("pooled", *SPORTS)
OUTCOMES = ("payoff_cents", "roi_percent")
PHASES = ("pregame", "in_play")
CATEGORIES = ("Crypto", "Culture", "Economy", "Esports", "Finance", "Geopolitics",
              "Iran", "Mentions", "Politics", "Sports", "Tech", "Weather", "Unclassified")
CONVENTIONS = ("taker_direction", "all_buy", "maker_buy", "taker_buy")
CONVENTION_LABELS = {"taker_direction": "Taker direction", "all_buy": "All BUY",
                     "maker_buy": "Maker BUY", "taker_buy": "Taker BUY"}
WINDOWS = (
    ("pregame", "pre_lt24h", "<-24h"), ("pregame", "pre_24to6h", "-24 to -6h"),
    ("pregame", "pre_6to1h", "-6 to -1h"), ("pregame", "pre_60to15m", "-60 to -15m"),
    ("pregame", "pre_15to0m", "-15 to 0m"), ("since_start", "live_0to15m", "0-15m"),
    ("since_start", "live_15to30m", "15-30m"), ("since_start", "live_30to60m", "30-60m"),
    ("since_start", "live_1to2h", "1-2h"), ("since_start", "live_2hplus", "2h+"),
    ("final_hour", "final_60to30m", "60-30m"), ("final_hour", "final_30to15m", "30-15m"),
    ("final_hour", "final_15to5m", "15-5m"), ("final_hour", "final_5to0m", "5-0m"),
)
MAX_JSON_BYTES = 16_000_000


class ReportBlocked(ValueError):
    """Saved inputs do not establish a complete accepted report population."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReportBlocked(message)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def _read_json(path: Path):
    require(path.is_file() and not path.is_symlink(), "regular non-symlink JSON required")
    before = path.stat()
    require(0 < before.st_size <= MAX_JSON_BYTES, "JSON size cap exceeded before read")
    raw = path.read_bytes()
    require(path.stat() == before, "JSON changed during read")
    value = json.loads(raw, object_pairs_hook=_unique_object,
                       parse_constant=lambda value: (_ for _ in ()).throw(ReportBlocked(value)))
    require(type(value) is dict, "JSON object required")
    return value, hashlib.sha256(raw).hexdigest()


def load_accepted_estimates(folder: str | Path):
    folder = Path(folder).resolve()
    manifest, manifest_hash = _read_json(folder / "manifest.json")
    acceptance, acceptance_hash = _read_json(folder / "acceptance.json")
    require(manifest.get("schema_version") == "kaushik_replication_estimate_stage_v1" and
            manifest.get("status") == "estimates_complete", "complete estimate stage required")
    require(acceptance.get("schema_version") == "kaushik_replication_estimate_acceptance_v1" and
            acceptance.get("status") == "estimates_reopened_accepted" and
            acceptance.get("all_outputs_reopened") is True, "accepted estimate receipt required")
    require(acceptance.get("manifest_sha256") == manifest_hash and
            acceptance.get("source_head") == manifest.get("source", {}).get("head"),
            "estimate acceptance manifest/source binding differs")
    gates = manifest.get("reconciliation", {})
    require(all(gates.get(key) is True for key in (
        "all_inputs_reopened", "all_outputs_reopened", "common_duration_population",
        "buy_role_partition", "expected_grids_serialized")), "estimate reconciliation incomplete")
    data, digest = _read_json(folder / "estimates.json")
    require(digest == manifest.get("estimates_json", {}).get("sha256") ==
            acceptance.get("estimates_sha256"), "accepted estimates fingerprint differs")
    validate_estimates(data)
    return data, {"estimate_dir": str(folder), "manifest_sha256": manifest_hash,
                  "acceptance_sha256": acceptance_hash, "estimates_sha256": digest,
                  "source_head": acceptance["source_head"]}


def _index(rows, fields, expected=None):
    require(isinstance(rows, list), "saved row list required")
    result = {}
    for row in rows:
        require(isinstance(row, dict) and all(field in row for field in fields), "incomplete saved row")
        key = tuple(row[field] for field in fields)
        require(key not in result, "duplicate report grid cell: " + str(key))
        result[key] = row
    if expected is not None:
        require(set(result) == set(expected), "incomplete or unexpected report grid: " + str(fields))
    return result


def _integer(value, name):
    require(type(value) is int and 0 <= value <= 2**63-1, "bounded nonnegative integer required: " + name)


def _validate_estimate(row, *, sports=False):
    require(type(row.get("suppressed")) is bool, "suppression status missing")
    reasons = row.get("suppression_reasons")
    require(isinstance(reasons, list) and all(isinstance(item, str) for item in reasons),
            "suppression reasons missing")
    if "n_observations" in row:
        _integer(row["n_observations"], "N")
        _integer(row["n_clusters"], "G")
    if sports:
        cells = row.get("cell_support")
        require(isinstance(cells, list) and bool(cells), "sports cell support absent")
        for cell in cells:
            _integer(cell.get("n_observations"), "cell N")
            _integer(cell.get("n_clusters"), "cell G")
        paper = all(cell["n_clusters"] >= 30 for cell in cells)
        project = all(cell["n_observations"] >= 500 for cell in cells)
        require(row.get("paper_support") is paper and row.get("project_support") is project,
                "sports paper/project support flags disagree with counts")
        require((paper and project) or row["suppressed"], "unsupported sports estimate not withheld")
    if row["suppressed"]:
        require(bool(reasons) and row.get("CR0") is None and row.get("cluster_count_adjusted") is None,
                "withheld estimate must be null with a reason")
        return
    require(not reasons, "reported estimate has suppression reasons")
    for name in ("CR0", "cluster_count_adjusted"):
        interval = row.get(name)
        if interval is None and name == "cluster_count_adjusted":
            require(bool(row.get("supplementary_suppression_reasons")), "missing adjusted interval unexplained")
            continue
        require(isinstance(interval, dict), "reported interval missing: " + name)
        for field in ("estimate", "standard_error", "ci95_low", "ci95_high"):
            require(type(interval.get(field)) in (int, float) and math.isfinite(interval[field]),
                    "nonfinite reported interval: " + field)
        e, low, high = (interval[key] for key in ("estimate", "ci95_low", "ci95_high"))
        require(interval["standard_error"] >= 0 and low <= e <= high and
                math.isfinite(e-low) and math.isfinite(high-e), "invalid or overflowing saved interval")


def _validate_model(model):
    _integer(model.get("n_observations"), "model sample N")
    _integer(model.get("n_clusters"), "model sample G")
    metadata = _metadata(model)
    if model.get("joint") is None:
        require(model.get("suppressed") is True and
                "empty_estimation_sample" in model.get("suppression_reasons", []) and
                model["n_observations"] == model["n_clusters"] == 0,
                "missing model support is not an explicit empty sample")
    else:
        require(0 < model["n_clusters"] <= model["n_observations"], "invalid nonempty model sample support")
        _integer(metadata.get("n"), "model N")
        _integer(metadata.get("cluster_count"), "model G")
        require(metadata["n"] == model["n_observations"], "model sample and moment N differ")


def _exclusion_rows(sample):
    monthly = sample.get("archive_exclusions")
    require(isinstance(monthly, dict), "monthly exclusion dictionary required")
    for month, rows in sorted(monthly.items()):
        require(isinstance(month, str) and isinstance(rows, list), "invalid monthly exclusion rows")
        for row in rows:
            require(isinstance(row, dict) and isinstance(row.get("reason"), str) and row["reason"] and
                    all(key in row for key in ("side", "is_maker", "rows", "precut_rows")),
                    "incomplete monthly exclusion row")
            require(row["is_maker"] is None or type(row["is_maker"]) is bool, "invalid exclusion role")
            _integer(row["rows"], "monthly source N")
            _integer(row["precut_rows"], "monthly pre-cutoff N")
            require(row["precut_rows"] <= row["rows"], "pre-cutoff exclusion count exceeds source")
            yield row


def validate_estimates(data: Mapping[str, Any]) -> None:
    require(data.get("schema_version") == "kaushik_replication_estimates_v1" and
            data.get("status") == "estimates_complete", "complete estimates summary required")
    require(data.get("definitions", {}).get("cutoff_utc") == "2026-03-25T00:00:00Z",
            "cutoff definition differs")
    uncertainty = data["definitions"].get("uncertainty", {})
    require(uncertainty.get("primary") == "event_cluster_CR0" and
            uncertainty.get("full_N_minus_k_CR1_used") is False, "variance convention differs")
    sample = data.get("sample", {})
    for key in ("rows", "conditions", "normalized_claims", "clusters", "tail_rows",
                "duration_tail_rows", "duration_tail_clusters", "future_ending_rows",
                "unique_event_clusters", "market_fallback_clusters"):
        _integer(sample.get(key), key)
    list(_exclusion_rows(sample))
    for key in ("source", "baseline_all_roles", "excluded", "primary_taker"):
        _integer(sample.get("source_row_counts", {}).get(key), "source " + key)
    totals = sample["source_row_counts"]
    require(totals["source"] == totals["baseline_all_roles"]+totals["excluded"] and
            totals["primary_taker"] == sample["rows"], "source waterfall does not reconcile")
    categories = _index(data.get("categories"), ("category",), [(name,) for name in CATEGORIES])
    for row in categories.values():
        _integer(row.get("n_observations"), "category N")
        require(row.get("share") is None or (type(row["share"]) in (int, float) and
                math.isfinite(row["share"]) and 0 <= row["share"] <= 1), "invalid category share")
    require(sum(row["n_observations"] for row in categories.values()) == sample["rows"],
            "category rows do not exhaust primary sample")
    expected_bins = [(target, number) for target in OUTCOMES for number in range(1, 11)]
    profiles = _index(data.get("table1", {}).get("profile_rows"), ("outcome", "bin"), expected_bins)
    gaps = _index(data["table1"].get("gap_rows"), ("outcome",), [(target,) for target in OUTCOMES])
    for target in OUTCOMES:
        require(sum(profiles[(target, number)]["n_observations"] for number in range(1, 11)) == sample["rows"],
                "Table 1 bin population differs")
    for row in (*profiles.values(), *gaps.values()):
        _validate_estimate(row)
    for name, models, count in (("table2", data.get("table2"), 5),
                                ("L_gt1", data.get("table3", {}).get("L_gt1"), 3),
                                ("R_gt1", data.get("table3", {}).get("R_gt1"), 3)):
        indexed = _index(models, ("model_id",))
        require(len(indexed) == count and sorted(model["spec"]["column"] for model in models) == list(range(1, count+1)),
                "incomplete duration model grid: " + name)
        for model in models:
            _validate_model(model)
            expected = [(target, clock) for target in OUTCOMES for clock in model["spec"]["report_clocks"]]
            for row in _index(model.get("slopes"), ("outcome", "clock"), expected).values():
                _validate_estimate(row)
    a1 = data.get("appendix_a1", {})
    _validate_model(a1)
    for row in _index(a1.get("slopes"), ("outcome", "clock"), [(target, clock) for target in OUTCOMES for clock in ("xL", "xR")]).values():
        _validate_estimate(row)
    a2 = _index(data.get("appendix_a2"), ("convention",), [(name,) for name in CONVENTIONS])
    require(a2[("all_buy",)]["counts"]["rows"] == a2[("maker_buy",)]["counts"]["rows"] + a2[("taker_buy",)]["counts"]["rows"],
            "BUY role partition differs")
    for convention in a2.values():
        convention_profiles = _index(convention.get("profile_rows"), ("outcome", "bin"), expected_bins)
        for target in OUTCOMES:
            require(sum(convention_profiles[(target, number)]["n_observations"] for number in range(1, 11)) == convention["counts"]["rows"],
                    "BUY price-bin population differs")
        for row in convention_profiles.values():
            _validate_estimate(row)
        for row in _index(convention.get("gap_rows"), ("outcome",), [(target,) for target in OUTCOMES]).values():
            _validate_estimate(row)
    sports = data.get("sports", {})
    require(isinstance(sports.get("coverage_qualification"), str) and sports["coverage_qualification"],
            "provider coverage qualification absent")
    _index(sports.get("coverage"), ("sport",), [(sport,) for sport in SPORTS])
    phase_counts = _index(sports.get("phase_counts"), ("scope", "phase"),
                          [(scope, phase) for scope in SCOPES for phase in PHASES])
    for row in phase_counts.values():
        _integer(row.get("n_observations"), "phase all-band N")
        _integer(row.get("n_games"), "phase all-band G")
    scope_counts = _index(sports.get("scope_counts"), ("scope",), [(scope,) for scope in SCOPES])
    for scope in SCOPES:
        row = scope_counts[(scope,)]
        _integer(row.get("n_observations"), "scope all-band N")
        _integer(row.get("n_games"), "scope all-band G")
        require(sum(phase_counts[(scope, phase)]["n_observations"] for phase in PHASES) == row["n_observations"],
                "phase/scope record populations differ")
    for key, fields, grid in (
        ("profile_rows", ("scope", "outcome", "phase", "bin"),
         [(scope, target, phase, number) for scope in SCOPES for target in OUTCOMES for phase in PHASES for number in range(1, 11)]),
        ("phase_rows", ("scope", "outcome", "phase"),
         [(scope, target, phase) for scope in SCOPES for target in OUTCOMES for phase in (*PHASES, "in_play_minus_pregame")]),
        ("window_rows", ("scope", "outcome", "window"),
         [(scope, target, window) for scope in SCOPES for target in OUTCOMES for _, window, _ in WINDOWS])):
        for row in _index(sports.get(key), fields, grid).values():
            _validate_estimate(row, sports=True)


def tex(value: Any) -> str:
    replacements = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$",
                    "#": r"\#", "_": r"\_", "{": r"\{", "}": r"\}",
                    "~": r"\textasciitilde{}", "^": r"\textasciicircum{}",
                    "−": "-", "–": "--", "—": "---", "’": "'", "≤": r"$\leq$",
                    "≥": r"$\geq$", "×": r"$\times$", "∆": r"$\Delta$"}
    return "".join(replacements.get(char, char) for char in str(value))


def number(value, digits=2, *, signed=False):
    if value is None:
        return "withheld"
    require(type(value) in (int, float) and math.isfinite(value), "nonfinite display value")
    if value != 0 and (abs(value) >= 1_000_000 or abs(value) < 10**(-digits-2)):
        mantissa, exponent = f"{value:+.{digits}e}".split("e") if signed else f"{value:.{digits}e}".split("e")
        return rf"${mantissa}\times 10^{{{int(exponent)}}}$"
    return f"{value:+.{digits}f}" if signed else f"{value:.{digits}f}"


def count(value):
    _integer(value, "display count")
    return f"{value:,}"


def cell(row):
    if row["suppressed"]:
        return "withheld"
    interval = row["CR0"]
    flags = (row.get("influence") or {}).get("flags", [])
    marker = r"$^{\dagger}$" if any(flag != "zero_cluster_score_variance" for flag in flags) else ""
    return number(interval["estimate"], signed=True) + marker


def se(row):
    return "" if row["suppressed"] else "(" + number(row["CR0"]["standard_error"]) + ")"


def status(row):
    if not row["suppressed"]:
        return "reported"
    pieces = []
    if row.get("paper_support") is False:
        pieces.append("<30 games")
    if row.get("project_support") is False:
        pieces.append("<500 records")
    if not pieces:
        pieces = ["empty cell"] if "empty_cell" in row["suppression_reasons"] else [reason.replace("_", " ") for reason in row["suppression_reasons"]]
    return "withheld: " + "; ".join(pieces)


def withholding_note(labeled_rows):
    """Show saved reasons only for cells actually withheld in this table."""
    labeled_rows = list(labeled_rows)
    grouped = defaultdict(list)
    for label, row in labeled_rows:
        if row["suppressed"]:
            reasons = "; ".join(reason.replace("_", " ") for reason in row["suppression_reasons"])
            grouped[reasons].append(label)
    if not grouped:
        return ""
    if len(grouped) == 1 and sum(len(labels) for labels in grouped.values()) == len(labeled_rows):
        return " All displayed estimates withheld: " + tex(next(iter(grouped))) + "."
    return " Withheld: " + ". ".join(tex(", ".join(labels) + ": " + reasons)
                                       for reasons, labels in sorted(grouped.items())) + "."


def outcome_label(target):
    return "Payoff" if target == "payoff_cents" else "Return"


def clock_label(clock):
    return "Original" if clock == "xL" else "Remaining"


def outcome_withholding_note(rows):
    """Compact paired-outcome note; the table retains per-cell counts/status."""
    reasons = defaultdict(set)
    for row in rows:
        if row["suppressed"]:
            reasons[outcome_label(row["outcome"])].update(row["suppression_reasons"])
    return "" if not reasons else " Saved withholding reasons: " + ". ".join(
        tex(label + ": " + "; ".join(reason.replace("_", " ") for reason in sorted(saved)))
        for label, saved in sorted(reasons.items())) + "."


def table(title, headers, rows, note="", *, widths=None, multipage=False):
    columns = widths or ("l" + "r"*(len(headers)-1))
    columns = columns.replace("p{", r">{\raggedright\arraybackslash}p{")
    body = [" & ".join(headers) + r" \\", r"\midrule"]
    body += [" & ".join(row) + r" \\" for row in rows]
    if multipage:
        return "\n".join((r"{\small", rf"\begin{{longtable}}{{{columns}}}", rf"\caption*{{{title}}}\\",
                           r"\toprule", *body[:2], r"\endfirsthead", r"\toprule", *body[:2],
                           r"\endhead", r"\bottomrule\endfoot", *body[2:],
                           r"\end{longtable}}", rf"{{\footnotesize {note}\par}}"))
    return "\n".join((r"\begin{table}[!htbp]\centering\small", rf"\caption*{{{title}}}",
                       rf"\begin{{tabular}}{{{columns}}}\toprule", *body,
                       r"\bottomrule\end{tabular}",
                       rf"\begin{{minipage}}{{\linewidth}}\footnotesize {note}\end{{minipage}}",
                       r"\end{table}"))


VARIANCE_NOTE = (r"Parentheses contain event/game-clustered CR0 standard errors, the unscaled clustered sandwich; intervals are pointwise normal 95\%. "
                 r"Joint scores retain covariance across tails and phases. The saved supplementary covariance multiplies CR0 by $G/(G-1)$, "
                 r"not a full $N-k$ CR1 correction; the paper's finite-sample scaling is unspecified. "
                 r"$\dagger$: fewer than 30 effective influence clusters or a cluster variance share above 25\%.")
SPORTS_NOTE = (r"Available audited provider-covered full-game winner cohort, including EPL draws; no Kalshi match or added date bound. "
               r"At least 30 games and 500 archive records in every required cell; the 500-record guard is additional to the paper. "
               r"Pregame is $t<s$; in-play is $s\leq t\leq e$, where s/e are accepted game start/end clocks. "
               r"Archive execution clocks are approximate. Eight sports use accepted first/last play clocks; ATP uses scheduled start plus "
               r"match duration, so delays can misclassify phases. Post-endpoint records are excluded.")


def _metadata(model):
    return (model.get("joint") or {}).get("metadata", {})


def _model_count(model, key):
    """Original estimation sample support, not the number of generated scores."""
    _validate_model(model)
    return count(model[{"n": "n_observations", "cluster_count": "n_clusters"}[key]])


def _model_table(data, target):
    models = sorted(data["table2"], key=lambda row: row["spec"]["column"])
    rows = []
    for clock, label in (("xL", "Original duration"), ("xR", "Time remaining")):
        selected = [next((row for row in model["slopes"] if row["outcome"] == target and row["clock"] == clock), None) for model in models]
        rows += [[label, *(cell(row) if row else "--" for row in selected)],
                 ["", *(se(row) if row else "" for row in selected)]]
    rows += [["Records", *(_model_count(model, "n") for model in models)],
             ["Event/market clusters", *(_model_count(model, "cluster_count") for model in models)]]
    for group, label in (("D1", r"$R^2$: D1"), ("D10", r"$R^2$: D10"), ("stacked", r"$R^2$: stacked")):
        values = []
        for model in models:
            meta = _metadata(model)
            grouped = meta.get("grouped_r_squared") or {}
            if group == "stacked":
                value = (grouped.get("stacked_weighted_tss_r_squared_by_target") or meta.get("r_squared_including_fixed_effects_by_target") or {}).get(target)
            else:
                value = grouped.get("group_summaries", {}).get(group, {}).get("r_squared_including_fixed_effects_by_target", {}).get(target)
            values.append(number(value, 4))
        rows.append([label, *values])
    for effect, label in (("cat_code", "Category FE"), ("price_code", "Exact price FE"), ("month_code", "UTC month FE")):
        rows.append([label, *("Yes" if effect in model["spec"]["effects"] else "No" for model in models)])
    outcome = "payoff, cents" if target == "payoff_cents" else "return, pp"
    return table("Table 2" + ("A" if target == "payoff_cents" else "B") + ". Duration gradients (" + outcome + ")",
                 ["Clock / controls", "(1) Original", "(2) Category", "(3) Remaining", "(4) Category", "(5) Both full"], rows,
                 r"Tail-specific OLS; entries are D10 minus D1 clock slopes per unit of $\log_2(1+\mathrm{days})$. "
                 r"Opening is native \texttt{created\_at}; endpoint is \texttt{end\_date}, a maturity proxy. "
                 r"All columns share eligible tails, positive lifespan, nonnegative remaining time and trade at/after opening. "
                 r"Fixed effects (FE) vary by tail; exact normalized binary64 price levels are unrounded. No claim FE. "
                 r"Separate-tail and stacked $R^2$ include controls; stacked TSS uses the overall outcome mean. " + VARIANCE_NOTE +
                 withholding_note((f"({model['spec']['column']}) {clock_label(row['clock'])}", row)
                                  for model in models for row in model["slopes"] if row["outcome"] == target))


def _support_profile(rows, title):
    lookup = _index(rows, ("phase", "bin", "outcome"))
    body = []
    for phase in PHASES:
        for number_ in range(1, 11):
            payoff, roi = [lookup[(phase, number_, target)] for target in OUTCOMES]
            body.append(["Pre" if phase == "pregame" else "In", "D"+str(number_),
                         count(payoff["n_observations"]), count(payoff["n_clusters"]),
                         cell(payoff), cell(roi), tex(status(payoff) if payoff["suppressed"] else status(roi))])
    return table(title, ["Phase", "Bin", "N", "G", "Payoff", "Return", "Status"], body,
                 r"N is archive records; G is contributing games. Payoff is cents; return is percent. "
                 r"Both paper and additional project support flags are saved; withheld cells are not zero." +
                 outcome_withholding_note(rows),
                 widths="llrrrrp{6.0cm}")


def _support_windows(rows, title):
    lookup = _index(rows, ("window", "outcome"))
    body = []
    for _, window, label in WINDOWS:
        payoff, roi = [lookup[(window, target)] for target in OUTCOMES]
        support = payoff["cell_support"]
        body.append([tex(label), count(support[0]["n_observations"]), count(support[1]["n_observations"]),
                     count(payoff["n_clusters"]), cell(payoff), cell(roi),
                     tex(status(payoff) if payoff["suppressed"] else status(roi))])
    return table(title, ["Window", "D1 N", "D10 N", r"$G_{\min}$", "Payoff gap", "Return gap", "Status"], body,
                 r"Final-hour and since-start windows overlap. $G_{\min}$ is the smaller tail game count. "
                 r"Payoff gaps are cents; return gaps are pp. Unsupported estimates remain withheld." +
                 outcome_withholding_note(rows),
                 widths="lrrrrrp{6.0cm}")


def _support_buy(conventions):
    rows = []
    for convention in CONVENTIONS[1:]:
        lookup = _index(conventions[(convention,)]["profile_rows"], ("outcome", "bin"))
        for bin_ in range(1, 11):
            payoff, roi = [lookup[(target, bin_)] for target in OUTCOMES]
            rows.append([CONVENTION_LABELS[convention], "D"+str(bin_), count(payoff["n_observations"]),
                         count(payoff["n_clusters"]), cell(payoff), cell(roi),
                         tex(status(payoff) if payoff["suppressed"] else status(roi))])
    return table("Appendix A2. Recorded BUY bin estimates and support",
                 ["Convention", "Bin", "N", "G", "Payoff", "Return", "Status"], rows,
                 r"N is archive BUY records; G is contributing event/market clusters. Payoff is cents; individual return is percent. "
                 r"Role shares can vary across bins. Withheld estimates retain support and are omitted from the figure." +
                 outcome_withholding_note(row for name in CONVENTIONS[1:] for row in conventions[(name,)]["profile_rows"]),
                 widths="llrrrrp{6.0cm}", multipage=True)


def figure(filename, caption, note):
    require(filename in figure_names(), "unexpected figure reference")
    return "\n".join((r"\begin{figure}[!htbp]\centering",
                       rf"\includegraphics[width=\linewidth]{{figures/{filename}}}",
                       rf"\caption*{{{caption}}}", rf"\begin{{minipage}}{{\linewidth}}\footnotesize {note}\end{{minipage}}",
                       r"\end{figure}"))


def figure_names():
    return tuple([f"figure1_{scope}.pdf" for scope in SCOPES] +
                 [f"figure2_{scope}.pdf" for scope in SCOPES] + ["appendix_a2_buy_profiles.pdf"])


def render_source(data):
    validate_estimates(data)
    sample = data["sample"]
    parts = [r"\documentclass[11pt]{article}", r"\usepackage[margin=0.65in]{geometry}",
             r"\usepackage{booktabs,graphicx,caption,longtable,array}", r"\usepackage[T1]{fontenc}",
             r"\setlength{\parindent}{0pt}\setlength{\parskip}{5pt}\setlength{\tabcolsep}{4pt}",
             r"\makeatletter\setlength{\@fptop}{0pt}\makeatother",
             r"\captionsetup{font=small}\begin{document}",
             r"{\Large Favorite--longshot bias: Polymarket replication}\par",
             r"September 28 specifications; existing native categories and available provider-covered sports cohort.\par",
             r"Archive-recorded taker BUY retains its claim; taker SELL uses its verified binary complement. "
             r"Roles/actions are archive conventions, not independently certified native own-order actions or unique fills. "
             r"Equal record weights; $0<P<1$; no bot filter, price trim or up/down exclusion; executions before March 25, 2026, 00:00 UTC. "
             r"P is normalized claim price in dollars and Y is its eventual binary payout (0 or 1). "
             r"Payoff $100(Y-P)$ is cents per \$1 claim; individual return $100(Y/P-1)$ is averaged in percent, before fees and without annualization. "
             r"D1--D10 are fixed ten-cent bins; tail gaps are signed D10 minus D1; full profiles remain primary.\par",
             r"FE means fixed effects; pp means percentage points. Raw columns are unadjusted, with the same logarithmic clocks.\par",
             r"Computed by \texttt{run\_estimates.py}; saved \texttt{estimates.json}. "
             r"The renderer reads accepted saved outputs and performs no estimation.\par"]
    if data.get("fixture_only") is True:
        parts.insert(3, r"\usepackage{fancyhdr}")
        header = (r"\fancyhf{}\fancyhead[L]{\footnotesize SYNTHETIC FIXTURE --- NOT RESEARCH FINDINGS}"
                  r"\fancyhead[R]{\thepage}\renewcommand{\headrulewidth}{0pt}")
        parts.insert(8, r"\setlength{\headheight}{14pt}\pagestyle{fancy}" + header + r"\fancypagestyle{plain}{" + header + "}")
        parts.insert(9, r"\textbf{SYNTHETIC FIXTURE --- NOT RESEARCH FINDINGS}\par")
    coverage = [("First execution UTC", tex(sample["first_execution_utc"])),
                ("Last execution UTC", tex(sample["last_execution_utc"])),
                *[(label, count(sample[key])) for key, label in (
                    ("rows", "Baseline records"), ("conditions", "Conditions"), ("normalized_claims", "Normalized claims"),
                    ("clusters", "Event/market clusters"), ("unique_event_clusters", "Unique native-event clusters"),
                    ("market_fallback_clusters", "Market fallback clusters"), ("tail_rows", "Baseline D1 + D10 records"),
                    ("duration_tail_rows", "Duration-eligible tail records"), ("duration_tail_clusters", "Duration tail clusters"),
                    ("future_ending_rows", "Records in future-ending claims"))]]
    parts.append(table("Sample. Coverage and inclusion", ["Quantity", "Polymarket"], coverage,
                       r"Recorded source timestamps are approximate, not exact native block times. Eventual payouts/endpoints may follow the cutoff. "
                       r"The resolved-source build can omit unresolved long-horizon claims; this is not an exchange census or a recency trend."))
    totals = sample["source_row_counts"]
    waterfall = [[tex(key.replace("_", " ")), count(value)] for key, value in sorted(totals.items())]
    excluded = defaultdict(int)
    for row in _exclusion_rows(sample):
        excluded[(row["reason"], "Maker" if row["is_maker"] is True else "Taker" if row["is_maker"] is False else "Role absent",
                  str(row["side"]) if row["side"] is not None else "Action absent")] += row["precut_rows"]
    parts.append(table("Sample. Source accounting", ["Population", "Records"], waterfall,
                       r"Source totals span the admitted source files; the next table restricts counts to records before the cutoff. "
                       r"Eligible maker records enter Appendix A2 only. The primary uses eligible taker records."))
    parts.append(table("Sample. Pre-cutoff eligibility waterfall", ["First eligibility reason", "Role", "Action", "N"],
                       [[tex(reason.replace("_", " ")), tex(role), tex(side), count(n)] for (reason, role, side), n in sorted(excluded.items())],
                       r"Reasons are disjoint first-failure assignments, including the eligible category; archive multiplicity is preserved.",
                       widths="p{9cm}llr", multipage=len(excluded)>25))
    parts.append(r"\clearpage")
    parts.append(table("Categories. Native mutually exclusive partition", ["Category", "N", r"Share (\%)"],
                       [[tex(row["category"]), count(row["n_observations"]), number(100*row["share"]) if row["share"] is not None else "withheld"] for row in data["categories"]],
                       r"Existing 12 native categories plus explicit Unclassified; user-selected adaptation of the paper's five investigator categories."))
    profile = _index(data["table1"]["profile_rows"], ("outcome", "bin"))
    baseline_gaps = _index(data["table1"]["gap_rows"], ("outcome",))
    rows = []
    for bin_ in range(1, 11):
        payoff, roi = [profile[(target, bin_)] for target in OUTCOMES]
        range_ = r"$0<P<.10$" if bin_ == 1 else rf"${(bin_-1)/10:.2f}\leq P<{bin_/10:.2f}$"
        rows.append(["D"+str(bin_), range_, number(100*payoff["mean_price"]) if payoff.get("mean_price") is not None else "withheld",
                     cell(payoff), cell(roi), count(payoff["n_observations"]), count(payoff["n_clusters"])])
        rows.append(["", "", "", se(payoff), se(roi), "", ""])
    rows += [[r"D10$-$D1", "--", "--", cell(baseline_gaps[(OUTCOMES[0],)]), cell(baseline_gaps[(OUTCOMES[1],)]),
              count(sample["tail_rows"]), count(baseline_gaps[(OUTCOMES[0],)]["n_clusters"])],
             ["", "", "", se(baseline_gaps[(OUTCOMES[0],)]), se(baseline_gaps[(OUTCOMES[1],)]), "", ""]]
    parts.append(table("Table 1. Payoffs and individual returns by fixed price bin", ["Bin", "Price", "Mean price", "Payoff", "Return", "N", "G"], rows,
                       r"Prices/payoffs are cents; returns are percent; the return gap is pp. G is contributing event/market support; for the gap it is the smaller tail count. " + VARIANCE_NOTE +
                       withholding_note([(f"D{row['bin']} {outcome_label(row['outcome'])}", row) for row in profile.values()] +
                                        [(f"D10-D1 {outcome_label(row['outcome'])}", row) for row in baseline_gaps.values()])))
    parts += [r"\clearpage", _model_table(data, OUTCOMES[0]), _model_table(data, OUTCOMES[1]), r"\clearpage"]
    for panel, heading in (("L_gt1", "A. Original lifespan >1 day; final-day records retained"),
                           ("R_gt1", "B. Remaining time >1 day; final-day records excluded")):
        models = sorted(data["table3"][panel], key=lambda row: row["spec"]["column"])
        rows = []
        for target, label in zip(OUTCOMES, ("Payoff gap slope (cents)", "Return gap slope (pp)")):
            selected = [next(row for row in model["slopes"] if row["outcome"] == target) for model in models]
            rows += [[label, *(cell(row) for row in selected)], ["", *(se(row) for row in selected)]]
        rows += [["N", *(_model_count(model, "n") for model in models)],
                 ["G", *(_model_count(model, "cluster_count") for model in models)]]
        parts.append(table("Table 3. " + heading, ["Outcome / support", "(1) Raw", "(2) Category FE", "(3) Both + full FE"], rows,
                           r"All columns share their panel's observations. Column (3) holds the other clock fixed and includes tail-specific native category, exact-price and UTC-month FE. "
                           r"Slopes are per doubling of $1+\mathrm{days}$, not cutoff contrasts; no claim FE or causal duration interpretation. " + VARIANCE_NOTE +
                           withholding_note((f"({model['spec']['column']}) {outcome_label(row['outcome'])}", row)
                                            for model in models for row in model["slopes"])))
    parts.append(r"\clearpage")
    sports = data["sports"]
    exclusions = {row["sport"]: row for row in sports["exclusions"]}
    scope_counts = _index(sports["scope_counts"], ("scope",))
    rows = []
    for row in sorted(sports["coverage"], key=lambda row: SPORTS.index(row["sport"])):
        source = exclusions.get(row["sport"])
        require(source is not None or scope_counts[(row["sport"],)]["n_observations"] == 0,
                "nonempty sports sample lacks source exclusion counts")
        source = source or {"joined_rows": 0, "after_end_rows": 0, "admitted_rows": 0}
        dates = tex(str(row["first_game_utc"])[:10] + " to " + str(row["last_game_utc"])[:10]) if row["first_game_utc"] is not None else "--"
        rows.append([row["sport"].upper(), count(row["games"]), count(row["markets"]), count(source["joined_rows"]),
                     count(source["after_end_rows"]), count(source["admitted_rows"]), dates])
    parts.append(table("Sports. Provider proof and archive coverage", ["Sport", "Provider games", "Markets", "Joined N", "Post-end N", "Admitted N", "Game starts UTC"], rows,
                       tex(sports["coverage_qualification"]) + " Provider-map games can lack admitted archive records; Table 5 reports observed games. " + SPORTS_NOTE, widths="lrrrrrp{3.8cm}"))
    phases = _index(sports["phase_rows"], ("scope", "outcome", "phase"))
    phase_counts = _index(sports["phase_counts"], ("scope", "phase"))
    rows = [["All price-band records", *(count(phase_counts[("pooled", phase)]["n_observations"]) for phase in PHASES), "--"],
            ["Games with records", *(count(phase_counts[("pooled", phase)]["n_games"]) for phase in PHASES), "--"]]
    for index, label in ((0, "D1 records"), (1, "D10 records")):
        rows.append([label, *(count(phases[("pooled", OUTCOMES[0], phase)]["cell_support"][index]["n_observations"]) for phase in PHASES), "--"])
    rows.append(["Minimum contributing games", *(count(phases[("pooled", OUTCOMES[0], phase)]["n_clusters"])
                for phase in (*PHASES, "in_play_minus_pregame"))])
    for target, label in zip(OUTCOMES, ("Payoff gap (cents)", "Return gap (pp)")):
        selected = [phases[("pooled", target, phase)] for phase in (*PHASES, "in_play_minus_pregame")]
        rows += [[label, *(cell(row) for row in selected)], ["", *(se(row) for row in selected)]]
    parts.append(table("Table 4. Sports: pregame versus in-play FLB", ["Outcome / support", "Pregame", "In-play", "In minus pre"], rows,
                       SPORTS_NOTE + " " + VARIANCE_NOTE +
                       withholding_note((f"{row['phase'].replace('_', ' ')} {outcome_label(row['outcome'])}", row) for row in phases.values() if row["scope"] == "pooled")))
    parts += [r"\clearpage", figure("figure1_pooled.pdf", "Figure 1. Sports: complete pregame and in-play price profiles", SPORTS_NOTE + " " + VARIANCE_NOTE),
              _support_profile([row for row in sports["profile_rows"] if row["scope"] == "pooled"], "Figure 1. Supporting bin estimates and support"), r"\clearpage"]
    for target, title in zip(OUTCOMES, ("Payoff gap (cents)", "Return gap (pp)")):
        rows = []
        for sport in SPORTS:
            selected = [phases[(sport, target, phase)] for phase in (*PHASES, "in_play_minus_pregame")]
            rows.append([sport.upper(), count(scope_counts[(sport,)]["n_games"]), *(cell(row) for row in selected), count(selected[2]["n_clusters"])])
            if any(not row["suppressed"] for row in selected):
                rows.append(["", "", *(se(row) for row in selected), ""])
        parts.append(table("Table 5. Sports phase FLB by sport: " + title, ["Sport", "Observed games", "Pregame", "In-play", "In minus pre", r"$G_{\min}$"], rows,
                           r"Observed games have admitted all-band archive records in either phase. Phase and contrast support are assessed independently. "
                           r"$G_{\min}$ is the least populated of four phase/tail game cells. " + SPORTS_NOTE + " " + VARIANCE_NOTE +
                           withholding_note((f"{row['scope'].upper()} {row['phase'].replace('_', ' ')}", row) for row in phases.values() if row["scope"] != "pooled" and row["outcome"] == target)))
    parts += [r"\clearpage", figure("figure2_pooled.pdf", "Figure 2. Sports: actual-time-window tail gaps", SPORTS_NOTE + " " + VARIANCE_NOTE),
              _support_windows([row for row in sports["window_rows"] if row["scope"] == "pooled"], "Figure 2. Supporting window estimates and support"), r"\clearpage"]
    a1 = data["appendix_a1"]
    a1_rows = _index(a1["slopes"], ("outcome", "clock"))
    rows = []
    for target, label in zip(OUTCOMES, ("Payoff (cents)", "Return (pp)")):
        selected = [a1_rows[(target, clock)] for clock in ("xL", "xR")]
        rows += [[label, *(cell(row) for row in selected)], ["", *(se(row) for row in selected)]]
    claims = a1["claim_support"]
    rows += [["N / event clusters", _model_count(a1, "n"), _model_count(a1, "cluster_count")],
             ["Claims / both-tail claims", count(claims["claims"]), count(claims["both_tail_claims"])],
             ["Records in both-tail claims", count(claims["both_tail_claim_observations"]), "--"]]
    parts.append(table("Appendix A1. Claim fixed effects: price-path diagnostic", ["Outcome / support", r"Original $\times$ D10", r"Remaining $\times$ D10"], rows,
                       r"Claim FE absorb standalone original duration. $H=1$ for D10 and $0$ for D1; $x_L=\log_2(1+L)$ and $x_R=\log_2(1+R)$, "
                       r"where L is opening-to-endpoint lifespan and R is trade-to-endpoint remaining days. Regressors are $H,Hx_L,x_R,Hx_R$; no category, price or month effects. "
                       r"Within claim, $\widetilde{Y-P}=-\widetilde P$: payoff coefficients diagnose price paths, not independent calibration or causal FLB. "
                       r"The D10 intercept is not average FLB. " + VARIANCE_NOTE +
                       withholding_note((f"{outcome_label(row['outcome'])} {clock_label(row['clock'])}", row) for row in a1_rows.values())))
    a2 = _index(data["appendix_a2"], ("convention",))
    rows = []
    for convention in CONVENTIONS:
        saved = a2[(convention,)]
        estimates = _index(saved["gap_rows"], ("outcome",))
        selected = [estimates[(target,)] for target in OUTCOMES]
        rows += [[CONVENTION_LABELS[convention], count(saved["counts"]["rows"]), count(saved["counts"]["clusters"]), *(cell(row) for row in selected)],
                 ["", "", "", *(se(row) for row in selected)]]
    parts.append(table("Appendix A2. Archive observation-convention comparisons", ["Convention", "All-band N", "Clusters", "Payoff gap", "Return gap"], rows,
                       r"Same archive, cutoff and binary-token eligibility; no duration filter or controls. BUY rows retain the recorded claim, "
                       r"and all BUY partitions into maker and taker BUY. Equal counts do not imply identical prices or observations. "
                       r"Complemented SELL exposure is not realized seller investment return; role differences reflect selection/composition. " + VARIANCE_NOTE +
                       withholding_note((f"{CONVENTION_LABELS[name]} {outcome_label(row['outcome'])}", row) for (name,), saved in a2.items() for row in saved["gap_rows"])))
    parts.append(figure("appendix_a2_buy_profiles.pdf", "Appendix A2. Recorded BUY price profiles", r"All BUY, maker BUY and taker BUY; equal archive-record weights. Isolated means and saved event-clustered CR0 95\% intervals."))
    parts += [r"\clearpage", _support_buy(a2)]
    for scope in SPORTS:
        label = scope.upper()
        parts += [r"\clearpage", figure(f"figure1_{scope}.pdf", f"Figure 1 ({label}). Pregame and in-play price profiles", SPORTS_NOTE),
                  _support_profile([row for row in sports["profile_rows"] if row["scope"] == scope], f"Figure 1 ({label}). Bin estimates and support"),
                  r"\clearpage", figure(f"figure2_{scope}.pdf", f"Figure 2 ({label}). Actual-time-window tail gaps", SPORTS_NOTE),
                  _support_windows([row for row in sports["window_rows"] if row["scope"] == scope], f"Figure 2 ({label}). Window estimates and support")]
    parts.append(r"\end{document}")
    return "\n\n".join(parts) + "\n"


def _pyplot():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt


def draw_discrete(axis, rows, positions, *, label, color, marker, offset=0.0):
    """Saved supported intervals only; never connect bins or insert withheld zeroes."""
    selected = [(position+offset, row["CR0"]) for position, row in zip(positions, rows) if not row["suppressed"]]
    if not selected:
        return None
    x = [position for position, _ in selected]
    y = [interval["estimate"] for _, interval in selected]
    low = [interval["estimate"]-interval["ci95_low"] for _, interval in selected]
    high = [interval["ci95_high"]-interval["estimate"] for _, interval in selected]
    return axis.errorbar(x, y, yerr=[low, high], fmt=marker, linestyle="none", color=color,
                         capsize=3, markersize=4, elinewidth=.85, label=label)


def _limits(rows, outcome):
    values = [0.0]
    for row in rows:
        if row["outcome"] == outcome and not row["suppressed"]:
            values += [row["CR0"]["ci95_low"], row["CR0"]["ci95_high"]]
    low, high = min(values), max(values)
    spread = high-low
    require(math.isfinite(spread), "figure range overflow")
    padding = .07*spread if spread else 1.0
    require(math.isfinite(low-padding) and math.isfinite(high+padding), "figure padded range overflow")
    return low-padding, high+padding


def _save_figure(fig, path):
    fig.savefig(path, format="pdf", bbox_inches="tight",
                metadata={"Creator": "Polymarket replication renderer", "CreationDate": None, "ModDate": None})


def plot_profile(data, scope, path):
    plt = _pyplot()
    rows = data["sports"]["profile_rows"]
    lookup = _index([row for row in rows if row["scope"] == scope], ("outcome", "phase", "bin"))
    fig, axes = plt.subplots(2, 1, figsize=(8.0, 3.6), sharex=True)
    try:
        for axis, outcome, ylabel in zip(axes, OUTCOMES, ("Mean payoff (cents)", "Mean return (%)")):
            for phase, label, color, marker, offset in (("pregame", "Pregame", "#0072B2", "o", -.10), ("in_play", "In-play", "#D55E00", "s", .10)):
                draw_discrete(axis, [lookup[(outcome, phase, number)] for number in range(1, 11)], range(1, 11),
                              label=label, color=color, marker=marker, offset=offset)
            axis.axhline(0, color="0.45", linewidth=.7, zorder=0)
            axis.set_ylim(*_limits(rows, outcome))
            axis.set_ylabel(ylabel)
            axis.spines[["top", "right"]].set_visible(False)
            axis.grid(axis="y", alpha=.2)
        if axes[0].get_legend_handles_labels()[0]:
            axes[0].legend(frameon=False, loc="best", ncol=2)
        for axis in axes:
            if not axis.containers:
                axis.text(.5, .5, "No supported cells", ha="center", transform=axis.transAxes, color="0.4")
        axes[-1].set_xticks(range(1, 11), [f"D{number}" for number in range(1, 11)])
        axes[-1].set_xlabel("Equivalent claim-price bin")
        fig.tight_layout()
        _save_figure(fig, path)
    finally:
        plt.close(fig)


def plot_windows(data, scope, path):
    plt = _pyplot()
    rows = data["sports"]["window_rows"]
    lookup = _index([row for row in rows if row["scope"] == scope], ("outcome", "window"))
    fig, axes = plt.subplots(2, 3, figsize=(7.0, 3.4), sharey="row")
    try:
        for index, panel in enumerate(("pregame", "since_start", "final_hour")):
            windows = [(key, label) for family, key, label in WINDOWS if family == panel]
            for row_index, outcome in enumerate(OUTCOMES):
                axis = axes[row_index, index]
                draw_discrete(axis, [lookup[(outcome, key)] for key, _ in windows], range(len(windows)),
                              label=scope.upper(), color="#0072B2", marker="o")
                axis.axhline(0, color="0.45", linewidth=.7, zorder=0)
                axis.set_ylim(*_limits(rows, outcome))
                labels = [label.replace(" to ", "\n") if panel == "pregame" else label.replace("-", "-\n") for _, label in windows]
                axis.set_xticks(range(len(windows)), labels, fontsize=9)
                axis.set_xlim(-.5, len(windows)-.5)
                axis.spines[["top", "right"]].set_visible(False)
                axis.grid(axis="y", alpha=.2)
                if not axis.containers:
                    axis.text(.5, .5, "No supported cells", ha="center", fontsize=9, transform=axis.transAxes, color="0.4")
                if row_index == 0:
                    axis.set_title({"pregame": "Before start", "since_start": "Since start", "final_hour": "Final recorded hour"}[panel], fontsize=10)
        axes[0, 0].set_ylabel("Payoff gap (cents)")
        axes[1, 0].set_ylabel("Return gap (pp)")
        fig.tight_layout()
        _save_figure(fig, path)
    finally:
        plt.close(fig)


def plot_buy_profiles(data, path):
    plt = _pyplot()
    conventions = _index(data["appendix_a2"], ("convention",))
    all_rows = [row for item in data["appendix_a2"] if item["convention"] != "taker_direction" for row in item["profile_rows"]]
    fig, axes = plt.subplots(2, 1, figsize=(8.0, 5.0), sharex=True)
    try:
        for axis, outcome, label in zip(axes, OUTCOMES, ("Mean payoff (cents)", "Mean individual return (%)")):
            for convention, color, marker, offset in (("all_buy", "#0072B2", "o", -.16), ("maker_buy", "#D55E00", "s", 0), ("taker_buy", "#009E73", "^", .16)):
                lookup = _index(conventions[(convention,)]["profile_rows"], ("outcome", "bin"))
                draw_discrete(axis, [lookup[(outcome, number)] for number in range(1, 11)], range(1, 11),
                              label=CONVENTION_LABELS[convention], color=color, marker=marker, offset=offset)
            axis.axhline(0, color="0.45", linewidth=.7, zorder=0)
            axis.set_ylim(*_limits(all_rows, outcome))
            axis.set_ylabel(label)
            axis.spines[["top", "right"]].set_visible(False)
        if axes[0].get_legend_handles_labels()[0]:
            axes[0].legend(frameon=False, ncol=3, loc="best")
        for axis in axes:
            if not axis.containers:
                axis.text(.5, .5, "No supported cells", ha="center", transform=axis.transAxes, color="0.4")
        axes[-1].set_xticks(range(1, 11), [f"D{number}" for number in range(1, 11)])
        axes[-1].set_xlabel("Recorded BUY-price bin")
        fig.tight_layout()
        _save_figure(fig, path)
    finally:
        plt.close(fig)


def render(estimate_dir, run_dir):
    data, binding = load_accepted_estimates(estimate_dir)
    destination = Path(run_dir).resolve()
    require(not destination.exists(), "report target exists; immutable publication required")
    require(destination.parent.is_dir(), "report parent must exist")
    require(destination != Path(estimate_dir).resolve() and Path(estimate_dir).resolve() not in destination.parents,
            "report target must not modify estimate stage")
    staging = Path(tempfile.mkdtemp(prefix="."+destination.name+".staging-", dir=destination.parent))
    try:
        (staging / "figures").mkdir()
        (staging / "source.tex").write_text(render_source(data), encoding="utf-8")
        for scope in SCOPES:
            plot_profile(data, scope, staging / "figures" / f"figure1_{scope}.pdf")
            plot_windows(data, scope, staging / "figures" / f"figure2_{scope}.pdf")
        plot_buy_profiles(data, staging / "figures" / "appendix_a2_buy_profiles.pdf")
        _, fresh = load_accepted_estimates(estimate_dir)
        require(fresh == binding, "accepted estimate inputs changed during rendering")
        outputs = {str(path.relative_to(staging)): {"bytes": path.stat().st_size,
                   "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                   for path in sorted(staging.rglob("*")) if path.is_file()}
        manifest = {"schema_version": "kaushik_replication_report_v1", "status": "source_and_figures_complete",
                    "inputs": binding, "outputs": outputs, "primary_deliverable": "source.tex",
                    "renderer_source": {"path": "analysis/kaushik_polymarket_replication/render_report.py",
                                        "sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
                    "compilation_status": "pending", "visual_qa_status": "pending", "renderer_estimates": False,
                    "fixture_only": data.get("fixture_only") is True,
                    "discrete_figures": "isolated points; saved CR0 intervals; suppressed estimates omitted"}
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False)+"\n", encoding="utf-8")
        require(not destination.exists(), "report target appeared before publication")
        staging.rename(destination)
        return manifest
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--estimate-dir", required=True)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args()
    print(json.dumps(render(args.estimate_dir, args.run_dir), sort_keys=True))


if __name__ == "__main__":
    main()
