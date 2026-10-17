"""Independent, read-only reconciliation of saved Kalshi coefficient scores.

This module deliberately imports neither estimator nor renderer. Saved projected
event influences identify covariance, not the original design or coefficient
fit. Counts, estimands and R-squared are checked against their saved contracts;
raw membership, coefficient fitting and original-Y TSS are not re-estimated.
The caller may publish the returned receipt only after this function succeeds.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
import re

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


TARGETS = ("payoff_cents", "roi_percent")
CELLS = tuple("D" + str(i) for i in range(1, 11))
Z95 = 1.959963984540054
ATOL = 1e-9
RTOL = 1e-9
VARIANCE = {
    "primary": "event_cluster_CR0",
    "supplementary": "event_cluster_CR0_times_G_over_G_minus_1",
    "full_N_minus_k_CR1_used": False,
    "paper_finite_sample_scaling": "unknown; reconstructed covariance declared explicitly",
    "interval": "pointwise normal 95%",
}


class AuditBlocked(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise AuditBlocked(message)


@dataclass(frozen=True)
class AuditLimits:
    json_bytes: int = 16_000_000
    artifact_bytes: int = 3_000_000_000
    total_score_bytes: int = 12_000_000_000
    score_rows: int = 16_000_000
    batch_rows: int = 8192
    batch_bytes: int = 64_000_000

    def validate(self):
        require(all(type(x) is int and x > 0 for x in asdict(self).values()),
                "positive integer audit ceilings required")
        require(self.json_bytes <= self.artifact_bytes <= self.total_score_bytes,
                "audit byte ceilings disagree")
        return self


def _count(value, label, maximum=None):
    require(type(value) is int and value >= 0 and (maximum is None or value <= maximum),
            "invalid saved count: " + label)
    return value


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def _close(actual, expected, label):
    if expected is None:
        require(actual is None, label + " must remain null")
    else:
        require(_finite(actual) and math.isclose(actual, expected, rel_tol=RTOL, abs_tol=ATOL),
                label + " differs from independent saved-score reconstruction")


def _hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1_048_576), b""):
            digest.update(block)
    return digest.hexdigest()


def _regular(path, ceiling):
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "regular non-symlink artifact required")
    require(path.stat().st_size <= ceiling, "artifact exceeds audit byte ceiling")
    return path


def _no_duplicate_keys(items):
    result = {}
    for key, value in items:
        require(key not in result, "duplicate JSON object key")
        result[key] = value
    return result


def _json(path, expected_hash, ceiling):
    path = _regular(path, ceiling)
    require(isinstance(expected_hash, str) and re.fullmatch(r"[0-9a-f]{64}", expected_hash),
            "explicit SHA256 binding required")
    raw = path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == expected_hash, "JSON SHA256 binding differs")
    try:
        value = json.loads(raw, object_pairs_hook=_no_duplicate_keys,
                           parse_constant=lambda value: (_ for _ in ()).throw(AuditBlocked("nonfinite JSON constant")))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise AuditBlocked("invalid saved JSON") from error
    require(isinstance(value, dict), "saved JSON object required")
    return value


def _artifact(root, info, limits):
    require(isinstance(info, dict), "saved score inventory record required")
    relative = info.get("path")
    require(isinstance(relative, str) and relative and "\\" not in relative
            and not Path(relative).is_absolute()
            and all(part not in ("", ".", "..") for part in relative.split("/")),
            "normalized relative score artifact path required")
    path = root / relative
    require(root in path.resolve().parents and not any(part.is_symlink() for part in [path, *path.parents] if part != root.parent),
            "score artifact escapes accepted stage or traverses a symlink")
    _regular(path, limits.artifact_bytes)
    require(path.stat().st_size == info.get("bytes") and _hash(path) == info.get("sha256"),
            "score body size/SHA256 differs")
    parquet = pq.ParquetFile(path)
    require(parquet.metadata.num_rows == info.get("rows")
            and parquet.metadata.num_row_groups == info.get("row_groups")
            and str(parquet.schema_arrow) == info.get("schema"), "score footer identity differs")
    require(parquet.metadata.num_rows <= limits.score_rows, "score row ceiling exceeded")
    return path, parquet


def _interval(estimate, variance):
    require(math.isfinite(estimate) and math.isfinite(variance) and variance >= 0,
            "nonfinite independently reconstructed interval")
    se = math.sqrt(variance)
    statistic = estimate / se if se > 0 else None
    return {"estimate": estimate, "standard_error": se,
            "ci95_low": estimate - Z95 * se, "ci95_high": estimate + Z95 * se,
            "normal_statistic": statistic,
            "normal_p_value": math.erfc(abs(statistic) / math.sqrt(2)) if statistic is not None else None}


class _ScalarScores:
    """Scale-normalized scalar second/fourth moments, bounded across batches."""
    def __init__(self, width):
        self.scale = np.zeros(width)
        self.second = np.zeros(width)
        self.fourth = np.zeros(width)

    def add(self, values):
        new = np.maximum(self.scale, np.max(np.abs(values), axis=0))
        ratio = np.divide(self.scale, new, out=np.zeros_like(new), where=new > 0)
        normalized = np.divide(values, new, out=np.zeros_like(values), where=new > 0)
        squares = normalized ** 2
        self.second = self.second * ratio ** 2 + squares.sum(axis=0)
        self.fourth = self.fourth * ratio ** 4 + (squares ** 2).sum(axis=0)
        self.scale = new

    def interval_variance(self, index):
        return float(self.scale[index] ** 2 * self.second[index])

    def influence(self, index):
        total = float(self.second[index])
        effective = total ** 2 / float(self.fourth[index]) if total > 0 else None
        maximum = 1.0 / total if total > 0 else None
        flags = []
        if effective is not None and effective < 30:
            flags.append("effective_clusters_below_30")
        if maximum is not None and maximum > .25:
            flags.append("cluster_variance_share_above_25_percent")
        if total == 0:
            flags.append("zero_cluster_score_variance")
        return {"effective_clusters": effective, "maximum_cluster_variance_share": maximum, "flags": flags}


def _check_estimate(row, values, vector, scalar, index, g, coefficient_rows, label):
    require(isinstance(row, dict) and type(row.get("suppressed")) is bool,
            label + " explicit suppression status required")
    used = np.flatnonzero(vector)
    reasons = sorted({reason for i in used for reason in coefficient_rows[i]["suppression_reasons"]})
    suppressed = any(coefficient_rows[i]["suppressed"] for i in used)
    require(row["suppressed"] == suppressed and row.get("suppression_reasons") == reasons,
            label + " suppression mask/reasons differ")
    if suppressed:
        require(row.get("CR0") is None and row.get("cluster_count_adjusted") is None
                and row.get("influence") is None, label + " suppressed estimate must remain null")
        return
    require(g > 1, label + " supported estimate has fewer than two clusters")
    estimate = math.fsum(values[i] * vector[i] for i in used)
    variance = scalar.interval_variance(index)
    for convention, multiplier in (("CR0", 1), ("cluster_count_adjusted", g / (g - 1))):
        expected = _interval(estimate, variance * multiplier)
        actual = row.get(convention)
        require(isinstance(actual, dict) and set(actual) == set(expected), label + " interval fields differ")
        for name, value in expected.items():
            _close(actual[name], value, label + ":" + convention + ":" + name)
    require(row.get("supplementary_suppression_reasons") == [], label + " unexpected supplementary suppression")
    actual = row.get("influence")
    expected = scalar.influence(index)
    require(isinstance(actual, dict) and set(actual) == set(expected) and actual["flags"] == expected["flags"],
            label + " influence flags differ")
    for name in ("effective_clusters", "maximum_cluster_variance_share"):
        _close(actual[name], expected[name], label + ":" + name)


def _audit_joint(root, joint, info, contrasts, limits, label):
    require(isinstance(joint, dict) and joint.get("variance_convention") == VARIANCE,
            label + " variance convention differs")
    names = joint.get("names")
    require(isinstance(names, list) and names and len(names) <= 40 and len(set(names)) == len(names)
            and all(isinstance(name, str) and name for name in names), label + " coefficient names invalid")
    width = len(names)
    rows = joint.get("estimates")
    require(isinstance(rows, list) and len(rows) == width and [row.get("name") for row in rows] == names,
            label + " coefficient order differs")
    require(joint.get("contrasts") == [], label + " unadmitted joint contrast inventory")
    for row in rows:
        require(type(row.get("suppressed")) is bool and isinstance(row.get("suppression_reasons"), list)
                and all(isinstance(reason, str) and reason for reason in row["suppression_reasons"])
                and row["suppressed"] == bool(row["suppression_reasons"]), label + " coefficient suppression invalid")
        require(row["suppressed"] or (isinstance(row.get("CR0"),dict)
                and _finite(row["CR0"].get("estimate"))), label + " supported coefficient point invalid")
    metadata = joint.get("metadata", {})
    require(metadata.get("score_artifact") == info and info.get("coefficient_order") == names
            and info.get("score_definition") == "projected joint coefficient influence; CR0=score.T@score",
            label + " score/coefficient inventory binding differs")
    path, parquet = _artifact(root, info, limits)
    schema = parquet.schema_arrow
    require(schema.names == ["cluster_key", "projected_score"]
            and pa.types.is_string(schema.field("cluster_key").type)
            and pa.types.is_fixed_size_list(schema.field("projected_score").type)
            and schema.field("projected_score").type.list_size == width
            and pa.types.is_float64(schema.field("projected_score").type.value_type), label + " score schema differs")
    g = _count(metadata.get("cluster_count"), label + ":cluster_count")
    require(info["rows"] == g, label + " saved score/cluster count differs")
    vectors = [np.eye(width)[i] for i in range(width)]
    for _, coefficients, _ in contrasts:
        require(set(coefficients) <= set(names), label + " contrast names absent")
        vectors.append(np.array([coefficients.get(name, 0.) for name in names]))
    vectors = np.array(vectors)
    covariance = np.zeros((width, width))
    scalar = _ScalarScores(len(vectors))
    # Kalshi accepted input schemas require string event_cluster. Both the
    # means constructor and ClusterScoreMoments.ordered sort (type name,str),
    # and _Writer.joint preserves cluster_levels unchanged. For these native
    # strings this is lexical order, including numerical-looking suffixes.
    previous, count = None, 0
    for batch in parquet.iter_batches(batch_size=limits.batch_rows):
        require(batch.nbytes <= limits.batch_bytes, label + " score batch memory ceiling exceeded")
        keys = batch.column(0).to_pylist()
        require(all(isinstance(key, str) and key for key in keys), label + " missing score cluster key")
        require(all(left < right for left, right in zip(keys, keys[1:]))
                and (previous is None or not keys or previous < keys[0]),
                label + " score cluster keys not strictly ordered/unique")
        previous = keys[-1] if keys else previous
        require(batch.column(1).null_count == 0 and batch.column(1).values.null_count == 0,
                label + " null saved projected score")
        matrix = batch.column(1).values.to_numpy(zero_copy_only=False).reshape(-1, width)
        require(np.all(np.isfinite(matrix)), label + " nonfinite saved projected score")
        covariance += matrix.T @ matrix
        if len(matrix):
            scalar.add(matrix @ vectors.T)
        count += len(matrix)
    require(count == g and np.all(np.isfinite(covariance)), label + " nonfinite covariance or score row loss")
    values = [float(row["CR0"]["estimate"]) if not row["suppressed"] and isinstance(row.get("CR0"), dict) else None for row in rows]
    require(all(value is None or math.isfinite(value) for value in values), label + " coefficient point is nonfinite")
    for convention, multiplier in (("covariance_CR0", 1), ("covariance_cluster_count_adjusted", g / (g - 1) if g > 1 else 1)):
        actual = joint.get(convention)
        require(isinstance(actual, list) and len(actual) == width
                and all(isinstance(row, list) and len(row) == width for row in actual), label + " covariance shape differs")
        for i in range(width):
            for j in range(width):
                expected = None if rows[i]["suppressed"] or rows[j]["suppressed"] else float(covariance[i,j] * multiplier)
                _close(actual[i][j], expected, label + ":" + convention)
    for i, row in enumerate(rows):
        _check_estimate(row, values, vectors[i], scalar, i, g, rows, label + ":" + names[i])
    for i, (row, _, name) in enumerate(contrasts, width):
        require(row.get("name") == name, label + " reported contrast name differs")
        _check_estimate(row, values, vectors[i], scalar, i, g, rows, label + ":" + name)
    require(_hash(path) == info["sha256"], label + " score body drift during audit")
    return {"score_rows": count, "coefficient_count": width, "reported_contrasts": len(contrasts)}


def _support(row, cells, ns, gs):
    expected = [{"cell": cell, "n_observations": ns[cell], "n_clusters": gs[cell]} for cell in cells]
    require(row.get("cell_support") == expected and row.get("n_observations") == sum(ns[cell] for cell in cells)
            and row.get("n_clusters") == min(gs[cell] for cell in cells)
            and row.get("contributing_cluster_minimum") == min(gs[cell] for cell in cells)
            and row.get("paper_support") is None and row.get("project_support") is None,
            "baseline saved support record differs")


def _baseline(summary):
    table = summary["table1"]
    joint = table["joint"]
    expected_names = [target + "|" + cell for target in ("price", "win_rate", *TARGETS) for cell in CELLS]
    require(joint.get("names") == expected_names, "baseline estimand/coefficient order differs")
    meta = joint["metadata"]
    require(meta.get("cell_names") == list(CELLS) and meta.get("target_names") == ["price", "win_rate", *TARGETS]
            and meta.get("minimum_observations") == 1 and meta.get("minimum_clusters") == 2,
            "baseline support/target contract differs")
    ns, gs = meta.get("n_by_cell"), meta.get("cluster_count_by_cell")
    require(isinstance(ns, dict) and set(ns) == set(CELLS) and isinstance(gs, dict) and set(gs) == set(CELLS),
            "complete baseline support grid required")
    g = _count(meta.get("cluster_count"), "baseline clusters")
    for cell in CELLS:
        _count(ns[cell], cell + ":N")
        _count(gs[cell], cell + ":G", min(ns[cell], g))
        _close(meta.get("weight_sum_by_cell", {}).get(cell), ns[cell], cell + ":equal-print weights")
        reasons = ([] if ns[cell] >= 1 else ["observation_support_below_floor"])
        if gs[cell] < 2:
            reasons.append("cluster_support_below_floor")
        if ns[cell] == 0:
            reasons.append("empty_cell")
        for row in [joint["estimates"][expected_names.index(target + "|" + cell)] for target in ("price", "win_rate", *TARGETS)]:
            require(row.get("suppression_reasons") == sorted(reasons), "baseline support suppression differs")
    require(meta.get("n") == sum(ns.values()), "baseline N support sum differs")
    profiles = table.get("profile_rows")
    require(isinstance(profiles, list) and len(profiles) == 20, "complete baseline profile required")
    seen, contrasts = set(), []
    coefficients = {row["name"]: row for row in joint["estimates"]}
    for row in profiles:
        key = (row.get("outcome"), row.get("bin"))
        require(key[0] in TARGETS and type(key[1]) is int and 1 <= key[1] <= 10 and key not in seen
                and row.get("scope") == "all_markets" and row.get("phase") is None, "baseline profile membership differs")
        seen.add(key)
        cell = "D" + str(key[1])
        _support(row, [cell], ns, gs)
        price, win = row.get("mean_price"), row.get("win_rate")
        if ns[cell] == 0:
            require(price is None and win is None, "empty baseline auxiliary must remain null")
        else:
            require(_finite(price) and _finite(win) and 0 < price < 1 and 0 <= win <= 1,
                    "invalid saved baseline price/win rate")
            require((key[1]-1)/10-ATOL <= price <= key[1]/10+ATOL, "mean price outside saved fixed bin")
            for target, value in (("price", price), ("win_rate", win)):
                coefficient = coefficients[target + "|" + cell]
                if not coefficient["suppressed"]:
                    _close(coefficient["CR0"]["estimate"], value, "baseline auxiliary coefficient")
            if key[0] == "payoff_cents" and not row["suppressed"]:
                _close(row["CR0"]["estimate"], 100*(win-price), "baseline payoff identity")
        contrasts.append((row, {key[0] + "|" + cell: 1}, cell))
    gaps = table.get("gap_rows")
    require(isinstance(gaps, list) and [row.get("outcome") for row in gaps] == list(TARGETS), "baseline tail-gap inventory differs")
    for row in gaps:
        _support(row, ["D1", "D10"], ns, gs)
        target = row["outcome"]
        contrasts.append((row, {target+"|D10": 1, target+"|D1": -1}, "D10_minus_D1"))
    return contrasts, ns


def _r_squared(meta, claim_fe):
    overall = meta.get("r_squared_including_fixed_effects_by_target")
    if overall is not None:
        require(isinstance(overall, dict) and set(overall) == set(TARGETS)
                and all(value is None or (_finite(value) and value <= 1 + ATOL) for value in overall.values()),
                "saved overall R-squared fields invalid")
    grouped = meta.get("grouped_r_squared")
    if grouped is None:
        return
    require(not claim_fe and set(grouped.get("group_summaries", {})) == {"D1", "D10"},
            "tail R-squared summaries differ")
    for group in grouped["group_summaries"].values():
        for target in TARGETS:
            tss = group.get("weighted_tss_by_target", {}).get(target)
            sse = group.get("weighted_sse_by_target", {}).get(target)
            require(_finite(tss) and _finite(sse) and tss >= 0 and sse >= 0, "invalid saved R-squared sums")
            _close(group.get("r_squared_including_fixed_effects_by_target", {}).get(target),
                   1-sse/tss if tss > 0 else None, "tail R-squared ratio")
    require(isinstance(overall, dict), "stacked R-squared requires overall saved fit summary")
    for target in TARGETS:
        _close(grouped.get("stacked_weighted_tss_r_squared_by_target", {}).get(target), overall[target],
               "stacked/overall saved R-squared consistency")


def _model(model, spec, model_id, claim_fe=False):
    require(isinstance(model, dict) and model.get("model_id") == model_id, "model inventory differs")
    saved = model.get("spec", {})
    require(all(saved.get(key) == value for key, value in spec.items()), "model clocks/effects/estimand differ: " + model_id)
    n = _count(model.get("n_observations"), model_id + ":N")
    g = _count(model.get("n_clusters"), model_id + ":G", n)
    slopes = model.get("slopes")
    require(isinstance(slopes, list) and [(row.get("outcome"),row.get("clock")) for row in slopes]
            == [(target,clock) for target in TARGETS for clock in spec["report_clocks"]], "reported slope grid differs")
    if n == 0:
        require(g == 0 and model.get("joint") is None and model.get("suppressed") is True
                and model.get("suppression_reasons") == ["empty_estimation_sample"], "empty model is not explicitly withheld")
        for row in slopes:
            require(row.get("suppressed") is True and row.get("suppression_reasons") == ["empty_estimation_sample"]
                    and row.get("CR0") is None and row.get("cluster_count_adjusted") is None and row.get("influence") is None,
                    "empty model slope must remain null")
        return []
    joint = model.get("joint")
    require(isinstance(joint, dict), "nonempty model lacks saved joint scores")
    terms = (["H", "H:xL", "xR", "H:xR"] if claim_fe else
             [tail+":"+term for tail in ("D1","D10") for term in
              (["level", *spec["clocks"]] if not spec["effects"] else spec["clocks"])])
    require(joint.get("names") == [target+"|"+term for target in TARGETS for term in terms], "model coefficient estimands differ")
    meta = joint.get("metadata", {})
    require(meta.get("n") == n and meta.get("target_names") == list(TARGETS) and meta.get("term_names") == terms
            and meta.get("structural_column_count") == len(terms), "model saved counts/design dimensions differ")
    _close(meta.get("weight_sum"), n, model_id + ":equal-print weights")
    score_g = _count(meta.get("cluster_count"), model_id + ":scoreG", g)
    coefficients = joint.get("estimates", [])
    if any(not row.get("suppressed", True) for row in coefficients):
        require(score_g == g and meta.get("residualized_design_rank") == len(terms), "supported model saved rank/G differs")
        require(model.get("suppressed") is False and model.get("suppression_reasons") == [], "supported model has fit suppression")
        absorption = meta.get("absorption")
        require((not spec["effects"] and absorption is None) or (spec["effects"] and isinstance(absorption, dict)
                and absorption.get("converged") is True), "supported model lacks converged saved absorption")
    require(type(model.get("suppressed")) is bool and isinstance(model.get("suppression_reasons"), list)
            and model["suppressed"] == bool(model["suppression_reasons"]), "invalid model fit suppression status")
    _r_squared(meta, claim_fe)
    return [(row, ({row["outcome"]+"|H:"+row["clock"]: 1} if claim_fe else
                   {row["outcome"]+"|D10:"+row["clock"]: 1, row["outcome"]+"|D1:"+row["clock"]: -1}),
             "claim_FE_interaction" if claim_fe else "D10_minus_D1_clock_slope") for row in slopes]


def _contract(summary):
    require(summary.get("schema_version") == "kaushik_kalshi_estimates_v1"
            and summary.get("status") == "estimates_complete" and summary.get("sample_scope") == "all_available_dates",
            "completed full-scope native Kalshi estimates required")
    definition = summary.get("definitions", {})
    require(definition.get("observation") == "one admitted native trade print"
            and definition.get("weight") == "one per native print"
            and definition.get("duration_endpoint") == "settlement_ts" and definition.get("combos") == "excluded"
            and definition.get("price") == "exact normalized binary64 P; no rounding"
            and definition.get("duration") == "open_time/settlement_ts; log2(1+days); common eligible tail sample"
            and definition.get("outcomes") == {"payoff_cents":"100*(Y-P)","roi_percent":"100*(Y/P-1), before averaging"}
            and definition.get("uncertainty") == VARIANCE, "saved scientific definitions differ")
    require(summary.get("appendix_a2", {}).get("status") == "unavailable_from_public_trade_prints"
            and summary.get("sports", {}).get("status") == "pending_not_estimated", "unadmitted expanded result scope")
    baseline_contrasts, ns = _baseline(summary)
    models = [("table1", summary["table1"]["joint"], baseline_contrasts)]
    sample = summary.get("sample", {})
    populations = sample.get("cell_populations", {})
    baseline_n = sum(ns.values())
    duration_n = _count(populations.get("duration", {}).get("observations"), "duration observations")
    duration_g = _count(populations.get("duration", {}).get("clusters"), "duration clusters", duration_n)
    require(sample.get("baseline_rows") == baseline_n and sample.get("duration_tail_rows") == duration_n
            and populations.get("baseline", {}).get("observations") == baseline_n
            and populations.get("baseline", {}).get("clusters") == summary["table1"]["joint"]["metadata"]["cluster_count"]
            and duration_n <= ns["D1"]+ns["D10"]
            and populations.get("claim", {}).get("observations") == duration_n
            and populations.get("claim", {}).get("clusters") == duration_g, "saved population counts disagree")
    table2 = summary.get("table2")
    require(isinstance(table2, list) and len(table2) == 5, "five Table2 models required")
    specs = [(["xL"],[]),(["xL"],["cat_code"]),(["xR"],[]),(["xR"],["cat_code"]),
             (["xL","xR"],["cat_code","price_code","month_code"])]
    for i, (model, (clocks,effects)) in enumerate(zip(table2,specs),1):
        spec = {"column":i,"clocks":clocks,"effects":effects,"report_clocks":clocks}
        contrasts = _model(model,spec,"table2_c"+str(i))
        require(model["n_observations"] == duration_n and model["n_clusters"] == duration_g, "Table2 common sample differs")
        if model["joint"] is not None:
            models.append((model["model_id"],model["joint"],contrasts))
    table3 = summary.get("table3", {})
    require(set(table3) == {"L_gt1","R_gt1"}, "Table3 panel grid differs")
    for panel, clock in (("L_gt1","xL"),("R_gt1","xR")):
        panel_n = _count(sample.get("duration_panel_counts", {}).get(panel), panel+":N",duration_n)
        require(isinstance(table3[panel],list) and len(table3[panel]) == 3, "three Table3 models required")
        panel_g = None
        for i, model in enumerate(table3[panel],1):
            spec = {"column":i,"clocks":[clock] if i<3 else ["xL","xR"],
                    "effects":[] if i==1 else ["cat_code"] if i==2 else ["cat_code","price_code","month_code"],
                    "report_clocks":[clock]}
            contrasts = _model(model,spec,"table3_"+panel+"_c"+str(i))
            require(model["n_observations"] == panel_n and model["n_clusters"] <= duration_g
                    and (panel_g is None or model["n_clusters"] == panel_g), "Table3 common panel population differs")
            panel_g = model["n_clusters"]
            if model["joint"] is not None:
                models.append((model["model_id"],model["joint"],contrasts))
    a1 = summary.get("appendix_a1", {})
    contrasts = _model(a1,{"column":1,"clocks":["xL","xR"],"effects":["claim_fe_code"],
                            "report_clocks":["xL","xR"]},"appendix_a1",True)
    require(a1["n_observations"] == duration_n and a1["n_clusters"] == duration_g, "A1/common duration population differs")
    support = a1.get("claim_support", {})
    claims = _count(support.get("claims"),"claims",duration_n)
    both = _count(support.get("both_tail_claims"),"both-tail claims",claims)
    both_n = _count(support.get("both_tail_claim_observations"),"both-tail observations",duration_n)
    require(support.get("observations") == duration_n and both_n >= 2*both
            and (both > 0 or both_n == 0), "A1 saved both-tail support differs")
    if "claim_support" in sample:
        require(isinstance(sample["claim_support"],dict) and all(support.get(key) == value for key,value in sample["claim_support"].items()),
                "A1 sample/support binding differs")
    if a1["joint"] is not None:
        models.append(("appendix_a1",a1["joint"],contrasts))
    return models


def audit_saved_estimates(*, estimates_path, estimate_manifest_path,
                          estimate_acceptance_path, estimates_sha256,
                          estimate_manifest_sha256, acceptance_sha256=None,
                          limits=AuditLimits()):
    """Reopen bounded saved artifacts and return a receipt; never write outputs."""
    limits.validate()
    estimates_path, manifest_path, acceptance_path = map(Path,
        (estimates_path,estimate_manifest_path,estimate_acceptance_path))
    root = estimates_path.resolve().parent
    require(manifest_path.resolve().parent == root and acceptance_path.resolve().parent == root,
            "estimate JSON files must share the accepted stage")
    acceptance_path = _regular(acceptance_path,limits.json_bytes)
    acceptance_sha256 = acceptance_sha256 or _hash(acceptance_path)
    summary = _json(estimates_path,estimates_sha256,limits.json_bytes)
    manifest = _json(manifest_path,estimate_manifest_sha256,limits.json_bytes)
    acceptance = _json(acceptance_path,acceptance_sha256,limits.json_bytes)
    require(manifest.get("schema_version") == "kaushik_kalshi_estimate_stage_v1"
            and manifest.get("status") == "estimates_complete" and manifest.get("estimates_sha256") == estimates_sha256,
            "completed estimate manifest binding differs")
    require(acceptance.get("schema_version") == "kaushik_kalshi_estimate_acceptance_v1"
            and acceptance.get("status") == "estimates_reopened_accepted"
            and acceptance.get("all_outputs_reopened") is True
            and acceptance.get("manifest_sha256") == estimate_manifest_sha256
            and acceptance.get("estimates_sha256") == estimates_sha256
            and acceptance.get("source_head") == manifest.get("source", {}).get("head"),
            "estimate acceptance binding differs")
    require(re.fullmatch(r"[0-9a-f]{40}", acceptance.get("source_head") or ""), "saved source commit identity invalid")
    models = _contract(summary)
    inventory = summary.get("score_artifacts")
    require(isinstance(inventory,dict) and set(inventory) == {name for name,_,_ in models}
            and manifest.get("outputs") == inventory, "manifest/summary score inventory differs")
    require(all(isinstance(record,dict) for record in inventory.values()), "score inventory records must be objects")
    require(len({record.get("path") for record in inventory.values()}) == len(inventory), "score artifact roles share a path")
    require(all(type(info.get("bytes")) is int and 0 <= info["bytes"] <= limits.artifact_bytes for info in inventory.values())
            and sum(info["bytes"] for info in inventory.values()) <= limits.total_score_bytes, "total score byte ceiling exceeded")
    audited = {name:_audit_joint(root,joint,inventory[name],contrasts,limits,name) for name,joint,contrasts in models}
    require(_hash(estimates_path) == estimates_sha256 and _hash(manifest_path) == estimate_manifest_sha256
            and _hash(acceptance_path) == acceptance_sha256, "saved JSON drift during independent audit")
    checks = {name:True for name in (
        "json_hashes_reopened","estimate_acceptance_bound","score_inventory_bound","score_bodies_reopened",
        "score_keys_unique_and_ordered","full_joint_covariance_reconstructed","coefficient_intervals_reconstructed",
        "reported_contrasts_reconstructed","influence_diagnostics_reconstructed","saved_support_and_estimands_reconciled",
        "saved_r_squared_consistency","suppression_masks_reconciled")}
    return {"schema_version":"kaushik_kalshi_score_audit_v1","status":"saved_scores_reconciled",
        "estimates_sha256":estimates_sha256,"estimate_manifest_sha256":estimate_manifest_sha256,
        "estimate_acceptance_sha256":acceptance_sha256,"source_head":acceptance["source_head"],
        "auditor_source_sha256":_hash(Path(__file__)),"checks":checks,"models":audited,
        "tolerances":{"absolute":ATOL,"relative":RTOL},"resource_limits":asdict(limits),
        "scope":"independent saved projected-score covariance/contrasts/intervals/influence; saved support and estimand consistency",
        "not_reestimated":["raw trade membership, joins and source completeness", "coefficients from original design moments",
                            "original-Y stacked/A1 R-squared or absorption/rank from source design"],
        "r_squared_scope":"tail SSE/TSS ratios and stacked/overall saved-summary consistency only; original-Y totals are not saved"}
