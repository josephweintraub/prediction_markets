#!/usr/bin/env python3
"""Guarded, sequential audit of accepted saved scores; never reads raw trades."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from analysis.kaushik_polymarket_replication import render_report as grids

SOURCE_FILES = ("scripts/audit_kaushik_replication_saved_scores.py",
                "tests/test_kaushik_replication_independent_qa.py",
                "analysis/kaushik_polymarket_replication/render_report.py",
                "analysis/kaushik_polymarket_replication/build_inputs.py",
                "production_guard.py")
MAX_JSON = 16_000_000
MAX_SCORE = 1_000_000_000
MAX_TOTAL_SCORE = 8_000_000_000
MAX_READ = 25_000_000_000  # Three score passes plus bounded JSON/source reads.
MAX_OUTPUT = 1_000_000
BATCH_ROWS = 2048
RTOL, ATOL = 2e-8, 1e-10
NORMAL_95 = 1.959963984540054


def require(condition, message):
    if not condition:
        raise ValueError("saved-score audit blocked: " + message)


def identity(path):
    require(path.is_file() and not path.is_symlink(), "regular non-symlink input required: " + str(path))
    s = path.stat()
    return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)


class Reads:
    def __init__(self):
        self.bytes = 0

    def charge(self, count):
        require(self.bytes + count <= MAX_READ, "read ceiling before next pass")
        self.bytes += count

    def digest(self, path, maximum):
        before = identity(path)
        require(before[2] <= maximum, "input byte ceiling: " + path.name)
        self.charge(before[2])
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            remaining = before[2]
            while remaining:
                block = stream.read(min(1_048_576, remaining))
                require(bool(block), "input shortened while hashing")
                digest.update(block)
                remaining -= len(block)
            require(not stream.read(1), "input grew while hashing")
        require(identity(path) == before, "input changed while hashing: " + path.name)
        return digest.hexdigest(), before

    def json(self, path, expected):
        require(re.fullmatch(r"[0-9a-f]{64}", expected or ""), "expected SHA256 required")
        before = identity(path)
        require(0 < before[2] <= MAX_JSON, "JSON byte ceiling before read")
        self.charge(before[2])
        with path.open("rb") as stream:
            raw = stream.read(MAX_JSON + 1)
        require(len(raw) == before[2] and identity(path) == before, "JSON changed while reading")
        require(hashlib.sha256(raw).hexdigest() == expected, "JSON hash binding: " + path.name)
        def unique(pairs):
            result = {}
            for key, value in pairs:
                require(key not in result, "duplicate JSON key")
                result[key] = value
            return result
        def finite_float(value):
            parsed = float(value)
            require(math.isfinite(parsed), "nonfinite JSON number")
            return parsed
        data = json.loads(raw, object_pairs_hook=unique,
            parse_float=finite_float,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite JSON: " + value)))
        require(type(data) is dict, "JSON object required")
        return data


def close(actual, expected, label):
    if expected is None:
        require(actual is None, "expected null: " + label)
    else:
        require(type(actual) in (int, float) and math.isfinite(actual) and
                math.isclose(actual, expected, rel_tol=RTOL, abs_tol=ATOL), "numeric mismatch: " + label)


def vector(names, terms):
    require(set(terms) <= set(names), "unknown contrast term")
    return np.array([terms.get(name, 0.) for name in names], dtype=float)


def mean_checks(joint, profiles, gaps, *, phases=False, windows=False):
    names = joint["names"]
    checks = []
    for row in profiles:
        cell = f"D{row['bin']}" + (":" + row["phase"] if phases else "")
        checks.append((row, vector(names, {f"{row['outcome']}|{cell}": 1}), [cell]))
    for row in gaps:
        label = row.get("window") if windows else row.get("phase") if phases else None
        cells = (["D1:pregame", "D10:pregame", "D1:in_play", "D10:in_play"]
                 if label == "in_play_minus_pregame" else
                 ["D1" + (":" + label if label else ""), "D10" + (":" + label if label else "")])
        terms = {f"{row['outcome']}|{cells[0]}": -1, f"{row['outcome']}|{cells[1]}": 1}
        if len(cells) == 4:
            terms = {f"{row['outcome']}|{cells[0]}": 1, f"{row['outcome']}|{cells[1]}": -1,
                     f"{row['outcome']}|{cells[2]}": -1, f"{row['outcome']}|{cells[3]}": 1}
        checks.append((row, vector(names, terms), cells))
    return checks


def joints(data):
    table = data["table1"]
    yield "table1", table["joint"], mean_checks(table["joint"], table["profile_rows"], table["gap_rows"])
    models = [*data["table2"], *data["table3"]["L_gt1"], *data["table3"]["R_gt1"], data["appendix_a1"]]
    for model in models:
        joint = model["joint"]
        if joint is None:
            continue
        checks = []
        for row in model["slopes"]:
            key = f"{row['outcome']}|"
            terms = ({key + "H:" + row["clock"]: 1} if model["model_id"] == "appendix_a1" else
                     {key + "D10:" + row["clock"]: 1, key + "D1:" + row["clock"]: -1})
            checks.append((row, vector(joint["names"], terms), None))
        yield model["model_id"], joint, checks
    for table in data["appendix_a2"]:
        yield "a2_" + table["convention"], table["joint"], mean_checks(table["joint"], table["profile_rows"], table["gap_rows"])
    sports = data["sports"]
    for scope in grids.SCOPES:
        for suffix in ("phases", "profiles", "windows"):
            key = "sport_" + scope + "_" + suffix
            joint = sports["joint_artifacts"][key]
            profiles = [r for r in sports["profile_rows"] if r["scope"] == scope] if suffix == "profiles" else []
            gaps = [r for r in sports["phase_rows"] if r["scope"] == scope] if suffix == "phases" else (
                [r for r in sports["window_rows"] if r["scope"] == scope] if suffix == "windows" else [])
            yield key, joint, mean_checks(joint, profiles, gaps, phases=suffix != "windows", windows=suffix == "windows")


def support(joint, checks):
    m = joint["metadata"]
    if "cell_names" not in m:
        return
    cells, ns, gs = m["cell_names"], m["n_by_cell"], m["cluster_count_by_cell"]
    require(set(ns) == set(gs) == set(cells) and sum(ns.values()) == m["n"], "joint cell population")
    require(all(type(ns[c]) is int and type(gs[c]) is int and 0 <= gs[c] <= min(ns[c], m["cluster_count"]) for c in cells), "cell support bounds")
    require(m["weight_sum_by_cell"] == ns, "unweighted mean cell totals")
    for index, row in enumerate(joint["estimates"]):
        cell = cells[index % len(cells)]
        reasons = []
        if ns[cell] < m["minimum_observations"]:
            reasons.append("observation_support_below_floor")
        if gs[cell] < max(2, m["minimum_clusters"]):
            reasons.append("cluster_support_below_floor")
        if ns[cell] == 0:
            reasons.append("empty_cell")
        if reasons:
            require(row["suppressed"] and row["suppression_reasons"] == sorted(reasons), "mean coefficient support withholding")
        else:
            require(not row["suppressed"] or row["suppression_reasons"] == ["nonfinite_estimate_or_cluster_variance"], "unexpected mean coefficient withholding")
    for row, _, used in checks:
        expected = [{"cell": c, "n_observations": ns[c], "n_clusters": gs[c]} for c in used]
        require(row["cell_support"] == expected and row["n_observations"] == sum(ns[c] for c in used), "contrast support N/cells")
        require(row["n_clusters"] == row["contributing_cluster_minimum"] == min(gs[c] for c in used), "contrast G minimum, not union G")
        if row.get("paper_support") is not None:
            require(row["paper_support"] == all(gs[c] >= 30 for c in used) and
                    row["project_support"] == all(ns[c] >= 500 for c in used), "separate sports support flags")


def interval(row, point, variance, label):
    se = math.sqrt(max(0., variance))
    statistic = point / se if se > 0 else None
    expected = {"estimate": point, "standard_error": se,
        "ci95_low": point - NORMAL_95 * se, "ci95_high": point + NORMAL_95 * se,
        "normal_statistic": statistic,
        "normal_p_value": math.erfc(abs(statistic) / math.sqrt(2)) if statistic is not None else None}
    require(isinstance(row, dict), "interval absent: " + label)
    for key, value in expected.items():
        close(row.get(key), value, label + ":" + key)


def check_row(row, c, coefficients, supported, variance, effective, share, g):
    used = c != 0
    if not np.all(supported[used]):
        reasons = sorted({reason for item in np.flatnonzero(used) for reason in coefficients[item]["suppression_reasons"]})
        require(row["suppressed"] is True and sorted(row["suppression_reasons"]) == reasons and
                row.get("CR0") is None and row.get("cluster_count_adjusted") is None and row.get("influence") is None,
                "unsupported contrast must retain reasons/nulls")
        return
    points = [coefficients[i].get("CR0", {}).get("estimate") if coefficients[i].get("CR0") else None for i in np.flatnonzero(used)]
    if not math.isfinite(variance) or any(v is None for v in points):
        require(row["suppressed"] is True and row["suppression_reasons"] == ["nonfinite_estimate_or_cluster_variance"] and
                row.get("CR0") is None and row.get("cluster_count_adjusted") is None and
                row.get("influence") is None, "nonfinite score contrast withholding")
        return
    point = math.fsum(float(c[i]) * coefficients[i]["CR0"]["estimate"] for i in np.flatnonzero(used))
    require(not row["suppressed"] and not row["suppression_reasons"] and g > 1, "reported contrast status/union G")
    interval(row["CR0"], point, variance, "CR0")
    adjusted = variance * (g / (g - 1))
    if math.isfinite(adjusted):
        interval(row["cluster_count_adjusted"], point, adjusted, "G/(G-1), not CR1")
        require(row.get("supplementary_suppression_reasons") == [], "finite adjusted interval withholding reason")
    else:
        require(row["cluster_count_adjusted"] is None and row["supplementary_suppression_reasons"] == ["nonfinite_adjusted_cluster_variance"], "adjusted overflow withholding")
    influence = row["influence"]
    close(influence["effective_clusters"], effective, "effective clusters")
    close(influence["maximum_cluster_variance_share"], share, "maximum variance share")
    flags = []
    if influence["effective_clusters"] is not None and influence["effective_clusters"] < 30:
        flags.append("effective_clusters_below_30")
    if influence["maximum_cluster_variance_share"] is not None and influence["maximum_cluster_variance_share"] > .25:
        flags.append("cluster_variance_share_above_25_percent")
    if effective is None:
        flags.append("zero_cluster_score_variance")
    require(influence["flags"] == flags, "influence flags")


def score_file(folder, key, joint, checks, artifacts, outputs, reads):
    names, coefficients, m = joint["names"], joint["estimates"], joint["metadata"]
    d = len(names)
    require(0 < d <= 64 and len(set(names)) == d, "coefficient dimension/order")
    terms = m.get("cell_names", m.get("term_names"))
    if "cell_names" in m:
        require((m["minimum_observations"], m["minimum_clusters"]) ==
                ((500, 30) if key.startswith("sport_") else (1, 2)), "fixed mean support floors")
    require(names == [target + "|" + term for target in m["target_names"] for term in terms], "target/term coefficient order")
    require(len(coefficients) == d and [r["name"] for r in coefficients] == names, "coefficient estimate order")
    artifact = artifacts[key]
    require(artifact == m["score_artifact"] and artifact["coefficient_order"] == names, "score metadata/order")
    filename = artifact["path"]
    require(re.fullmatch(r"[A-Za-z0-9_]+_scores\.parquet", filename or ""), "safe score filename")
    path = folder / filename
    info = outputs[filename]
    require(all(artifact[k] == info[k] for k in ("bytes", "rows", "row_groups", "schema", "sha256")), "manifest score binding")
    digest, before = reads.digest(path, MAX_SCORE)
    require(digest == info["sha256"] and before[2] == info["bytes"], "score fingerprint/bytes")
    reads.charge(before[2])
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    require(schema.names == ["cluster_key", "projected_score"] and schema.field(0).type == pa.string() and
            pa.types.is_fixed_size_list(schema.field(1).type) and schema.field(1).type.list_size == d and
            schema.field(1).type.value_type == pa.float64() and str(schema) == info["schema"], "score schema")
    require(pf.metadata.num_rows == info["rows"] == m["cluster_count"] and pf.metadata.num_row_groups == info["row_groups"], "score row/union-cluster count")
    support(joint, checks)
    all_checks = [(r, np.eye(d)[i], None) for i, r in enumerate(coefficients)] + checks
    require(len(all_checks) <= 128 and not joint.get("contrasts"), "bounded, explicitly mapped contrasts")
    c = np.column_stack([v for _, v, _ in all_checks])
    covariance = np.zeros((d, d))
    variances = np.zeros(len(all_checks))
    scale = np.zeros(len(all_checks)); sum2 = scale.copy(); sum4 = scale.copy()
    count, previous = 0, None
    baseline_cluster_sources = {"event": 0, "market": 0}
    for batch in pf.iter_batches(batch_size=BATCH_ROWS):
        ids, lists = batch.column(0), batch.column(1)
        require(ids.null_count == lists.null_count == lists.values.null_count == 0, "null score/cluster")
        for cluster in ids.to_pylist():
            require(cluster and (previous is None or cluster > previous), "score cluster keys must be unique and ordered")
            if key == "table1":
                prefix = cluster.split(":", 1)[0]
                require(prefix in baseline_cluster_sources, "baseline native/fallback score identifier")
                baseline_cluster_sources[prefix] += 1
            previous = cluster
        scores = lists.values.slice(lists.offset * d, len(batch) * d).to_numpy(zero_copy_only=False).reshape(len(batch), d)
        require(np.all(np.isfinite(scores)), "nonfinite projected scores")
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            covariance += scores.T @ scores
            projected = scores @ c
            variances += np.sum(projected ** 2, axis=0)
            updated = np.maximum(scale, np.max(np.abs(projected), axis=0))
            ratio = np.divide(scale, updated, out=np.zeros_like(scale), where=updated > 0)
            normalized = np.divide(projected, updated, out=np.zeros_like(projected), where=updated[None, :] > 0)
            sum2 = sum2 * ratio ** 2 + np.sum(normalized ** 2, axis=0)
            sum4 = sum4 * ratio ** 4 + np.sum(normalized ** 4, axis=0)
            scale = updated
        count += len(batch)
    require(count == info["rows"] and identity(path) == before, "score count/stat drift during stream")
    require(reads.digest(path, MAX_SCORE)[0] == digest, "score changed after stream")
    supported = np.array([not r["suppressed"] or r["suppression_reasons"] == ["nonfinite_estimate_or_cluster_variance"] for r in coefficients])
    factor = count / (count - 1) if count > 1 else 1.
    for field, multiplier in (("covariance_CR0", 1.), ("covariance_cluster_count_adjusted", factor)):
        saved = joint[field]
        require(len(saved) == d and all(len(row) == d for row in saved), "covariance dimensions")
        for i in range(d):
            for j in range(d):
                value = float(covariance[i, j] * multiplier)
                close(saved[i][j], value if supported[i] and supported[j] and math.isfinite(value) else None, field)
    for index, (row, weights, _) in enumerate(all_checks):
        effective = float(sum2[index] ** 2 / sum4[index]) if sum2[index] > 0 else None
        share = float(1 / sum2[index]) if sum2[index] > 0 else None
        check_row(row, weights, coefficients, supported, float(variances[index]), effective, share, count)
    require(joint["variance_convention"]["primary"] == "event_cluster_CR0" and
            joint["variance_convention"]["supplementary"] == "event_cluster_CR0_times_G_over_G_minus_1" and
            joint["variance_convention"]["full_N_minus_k_CR1_used"] is False, "declared CR0/scaling")
    result = {"artifact": filename, "sha256": digest, "union_clusters": count,
              "coefficient_dimension": d, "checked_estimates_and_contrasts": len(all_checks)}
    if key == "table1":
        result["cluster_identifier_partition"] = baseline_cluster_sources
    return result


def count_checks(data, manifest):
    base, sample = manifest["preflight"]["base_manifest"], data["sample"]
    require(sample["source_row_counts"] == base["rows"] and sample["archive_exclusions"] == base["exclusions"], "accepted source/count exclusions")
    require(sample["clusters"] == sample["unique_event_clusters"] + sample["market_fallback_clusters"], "native/fallback observed cluster partition")
    primary_support = [row for row in base["support"] if row["is_maker"] is False]
    require(data["table1"]["joint"]["metadata"]["n"] == sample["rows"] and
            data["table1"]["joint"]["metadata"]["cluster_count"] == sample["clusters"], "baseline joint population")
    for row in data["categories"]:
        require(row["n_observations"] == sum(r["rows"] for r in primary_support if r["category"] == row["category"]), "accepted category support")
    for row in data["table1"]["profile_rows"]:
        require(row["n_observations"] == sum(r["rows"] for r in primary_support if r["bin"] == row["bin"]), "accepted bin support")
    duration = sample["duration_reconciliation"]
    require(duration["source"] == duration["group_cache"] and duration["source"]["duration_rows"] == sample["duration_tail_rows"], "common duration populations")
    accepted_duration = sum(r["duration_tail_rows"] for r in base["support"] if r["is_maker"] is False)
    require(accepted_duration == sample["duration_tail_rows"], "accepted duration support")
    require([m["model_id"] for m in data["table2"]] == [f"table2_c{i}" for i in range(1, 6)], "five-model IDs")
    for m in data["table2"]:
        require(m["n_observations"] == accepted_duration, "same-sample five models")
    for panel, field in (("L_gt1", "lifespan_gt1_rows"), ("R_gt1", "remaining_gt1_rows")):
        require([m["model_id"] for m in data["table3"][panel]] == [f"table3_{panel}_c{i}" for i in range(1, 4)], "duration panel IDs")
        require(all(m["n_observations"] == duration["source"][field] for m in data["table3"][panel]), "duration restricted panel population")
    a1 = data["appendix_a1"]
    require(a1["n_observations"] == a1["claim_support"]["observations"] == accepted_duration, "A1 common population")
    ns = {r["convention"]:r["counts"]["rows"] for r in data["appendix_a2"]}
    require(ns["taker_direction"] == sample["rows"] and ns["all_buy"] == ns["maker_buy"] + ns["taker_buy"], "BUY role partition")
    for convention, maker in (("all_buy", None), ("maker_buy", True), ("taker_buy", False)):
        require(ns[convention] == sum(r["rows"] for r in base["support"] if r["side"] == "BUY" and
                (maker is None or r["is_maker"] is maker)), "accepted BUY convention count")
    for table in data["appendix_a2"]:
        m = table["joint"]["metadata"]
        require(m["n"] == table["counts"]["rows"] and m["cluster_count"] == table["counts"]["clusters"] and
                m["n_by_cell"]["D1"] + m["n_by_cell"]["D10"] == table["counts"]["tail_rows"], "convention joint count reconciliation")
    sports = data["sports"]
    require(sum(r["n_observations"] for r in sports["scope_counts"] if r["scope"] != "pooled") ==
            next(r["n_observations"] for r in sports["scope_counts"] if r["scope"] == "pooled"), "sports pooled/sport record partition")
    phase_counts = {(r["scope"], r["phase"]): r for r in sports["phase_counts"]}
    for row in sports["scope_counts"]:
        m = sports["joint_artifacts"]["sport_" + row["scope"] + "_profiles"]["metadata"]
        require(m["n"] == row["n_observations"] and m["cluster_count"] == row["n_games"], "sports profile/scope joint population")
        for phase in grids.PHASES:
            require(sum(m["n_by_cell"][f"D{i}:{phase}"] for i in range(1, 11)) ==
                    phase_counts[(row["scope"], phase)]["n_observations"], "sports profile/phase population")
        tail = sports["joint_artifacts"]["sport_" + row["scope"] + "_phases"]["metadata"]
        require(all(tail[field][cell] == m[field][cell] for field in ("n_by_cell", "cluster_count_by_cell")
                    for cell in tail["cell_names"]), "sports tail/profile cell support")
    windows = {(r["panel"], r["window"]): r for r in sports["window_counts"]}
    require(len(windows) == len(sports["window_counts"]) and set(windows) == {(panel, key) for panel, key, _ in grids.WINDOWS}, "complete sports window support grid")
    pooled_windows = sports["joint_artifacts"]["sport_pooled_windows"]["metadata"]
    for (panel, window), row in windows.items():
        require(sum(pooled_windows["n_by_cell"][f"{tail}:{window}"] for tail in ("D1", "D10")) <=
                row["n_observations"], "window tail/all-band support bounds")
    phases = {r["phase"]:r["n_observations"] for r in sports["phase_counts"] if r["scope"] == "pooled"}
    require(sum(r["n_observations"] for r in sports["window_counts"] if r["panel"] == "pregame") == phases["pregame"] and
            sum(r["n_observations"] for r in sports["window_counts"] if r["panel"] == "since_start") == phases["in_play"], "sports window partitions")
    cache_info = sports["observation_cache"]
    require(cache_info.get("sport_clock_columns") == {
        "elapsed_seconds": "sport_elapsed_seconds", "remaining_seconds": "sport_remaining_seconds"},
        "corrected sports seconds clock columns required")
    cache = cache_info["reconciliation"]
    require(type(cache.get("sport_clock_mismatch_rows")) is int and
            cache["sport_clock_mismatch_rows"] == 0,
            "saved sports seconds clock equation proof required")
    saved_footer = manifest["outputs"].get("sports_observations.parquet", {})
    schema = cache_info.get("schema")
    require(isinstance(schema, str) and schema == saved_footer.get("schema"),
            "sports cache/saved footer schema differs")
    fields = [line.split(": ", 1) for line in schema.splitlines()]
    require(all(len(field) == 2 for field in fields) and
            len({field[0].casefold() for field in fields}) == len(fields),
            "sports cache seconds schema fields must be distinct")
    types = dict(fields)
    require(types.get("sport_elapsed_seconds") == types.get("sport_remaining_seconds") == "double" and
            not ({"u", "r"} & {name.casefold() for name in types}),
            "sports cache requires distinct seconds fields, not legacy u/r/R")
    require(cache["joined_rows"] == cache["admitted_rows"] + cache["after_end_rows"] and
            cache["admitted_rows"] == cache["pregame_rows"] + cache["in_play_rows"] == sum(phases.values()), "saved sports cache partitions")
    for field in ("joined_rows", "admitted_rows", "after_end_rows"):
        require(sum(row[field] for row in sports["exclusions"]) == cache[field], "saved sports exclusion partition")


def audit_scores(folder, manifest_sha256, acceptance_sha256):
    """Fixture-accessible bounded worker; real execution uses guarded main()."""
    folder = Path(folder)
    require(folder.is_dir() and not folder.is_symlink(), "accepted directory required")
    folder = folder.resolve()
    reads = Reads()
    manifest = reads.json(folder / "manifest.json", manifest_sha256)
    acceptance = reads.json(folder / "acceptance.json", acceptance_sha256)
    require(manifest.get("schema_version") == "kaushik_replication_estimate_stage_v1" and manifest.get("status") == "estimates_complete", "complete producer manifest")
    require(re.fullmatch(r"[0-9a-f]{40}", manifest["source"]["head"] or "") and
            not (folder / "failure.json").exists(), "producer source HEAD/failure status")
    require(acceptance.get("schema_version") == "kaushik_replication_estimate_acceptance_v1" and
            acceptance.get("status") == "estimates_reopened_accepted" and acceptance.get("all_outputs_reopened") is True and
            acceptance.get("manifest_sha256") == manifest_sha256 and acceptance.get("source_head") == manifest["source"]["head"], "producer acceptance/source")
    require(all(manifest["reconciliation"].get(k) is True for k in
        ("all_inputs_reopened", "all_outputs_reopened", "common_duration_population", "buy_role_partition", "expected_grids_serialized")), "producer reconciliation gates")
    require(acceptance["estimates_sha256"] == manifest["estimates_json"]["sha256"], "estimates acceptance binding")
    data = reads.json(folder / "estimates.json", acceptance["estimates_sha256"])
    require(identity(folder / "estimates.json")[2] == manifest["estimates_json"]["bytes"], "estimates bytes")
    grids.validate_estimates(data)  # Reuse the frozen complete-grid/suppression validator.
    count_checks(data, manifest)
    records = list(joints(data))
    artifacts = data["score_artifacts"]
    require(len(records) <= 47 and len({r[0] for r in records}) == len(records) and set(artifacts) == {r[0] for r in records}, "complete score-reference grid")
    filenames = [artifacts[key]["path"] for key, _, _ in records]
    require(len(set(filenames)) == len(filenames) and
            set(filenames) == {p for p in manifest["outputs"] if p.endswith("_scores.parquet")}, "manifest score inventory")
    require(all(0 <= artifacts[k]["bytes"] <= MAX_SCORE for k in artifacts) and
            sum(artifacts[k]["bytes"] for k in artifacts) <= MAX_TOTAL_SCORE, "declared score input ceiling")
    results = [score_file(folder, key, joint, checks, artifacts, manifest["outputs"], reads) for key, joint, checks in records]
    require(results[0]["cluster_identifier_partition"] == {"event": data["sample"]["unique_event_clusters"],
                "market": data["sample"]["market_fallback_clusters"]}, "saved baseline cluster identifier partition")
    require(reads.json(folder / "manifest.json", manifest_sha256) == manifest and
            reads.json(folder / "acceptance.json", acceptance_sha256) == acceptance and
            reads.json(folder / "estimates.json", acceptance["estimates_sha256"]) == data, "accepted JSON drift after score stream")
    return {"schema_version": "kaushik_replication_saved_score_audit_v1", "status": "saved_scores_reconciled",
        "input": {"directory": str(folder), "manifest_sha256": manifest_sha256, "acceptance_sha256": acceptance_sha256,
                  "estimates_sha256": acceptance["estimates_sha256"], "producer_source_head": acceptance["source_head"]},
        "score_artifacts": results, "checks": {"CR0_score_outer_products": True, "named_contrasts_SE_CI": True,
            "supplementary_G_over_G_minus_1_not_CR1": True, "influence_diagnostics": True,
            "saved_support_and_complete_grids": True, "common_duration_and_BUY_partition": True,
            "saved_sports_seconds_clock_contract": True},
        "bounds": {"read_bytes": reads.bytes, "read_ceiling_bytes": MAX_READ, "batch_rows": BATCH_ROWS,
                   "per_score_file_bytes": MAX_SCORE, "total_score_file_bytes": MAX_TOTAL_SCORE,
                   "relative_tolerance": RTOL, "absolute_tolerance": ATOL},
        "limitations": "No raw trade or cache bodies read; no independent raw membership or coefficient estimation certified. "
            "Support N/cell G reconcile saved joint/input metadata, not raw participation. Union G is independently score-row count. "
            "Contrast point estimates are reconstructed from saved coefficient estimates; covariance and influence diagnostics from saved scores. "
            "Sports clock equations and distinct seconds fields reconcile saved metadata/footer descriptions, not an independent timing reconstruction."}


def source_snapshot(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or ""), "audit expected committed HEAD required")
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=REPO, text=True).strip()
    require(git("rev-parse", "HEAD") == expected_head, "audit committed HEAD differs")
    git("ls-files", "--error-unmatch", *SOURCE_FILES)
    require(not git("status", "--porcelain", "--", *SOURCE_FILES), "audit source/dependencies have uncommitted changes")
    files = {}
    for name in SOURCE_FILES:
        path = REPO / name
        require(identity(path)[2] <= 1_000_000, "audit source byte ceiling")
        files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"head": expected_head, "files": files}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--estimate-dir", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--acceptance-sha256", required=True)
    parser.add_argument("--expected-head", required=True, help="audit's own committed source, not producer HEAD")
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    require_production_host()  # Before any artifact/source read; no local real-data mode.
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production Python differs")
    source = source_snapshot(args.expected_head)
    target = args.run_dir.resolve()
    folder = args.estimate_dir.resolve()
    require(not args.run_dir.is_symlink() and not args.run_dir.parent.is_symlink() and
            not target.exists() and target.parent.is_dir() and not target.parent.is_symlink() and
            target != folder and folder not in target.parents and target not in folder.parents, "new independent output directory required")
    result = audit_scores(args.estimate_dir, args.manifest_sha256, args.acceptance_sha256)
    require(source_snapshot(args.expected_head) == source, "audit source changed during score audit")
    result.update({"audit_source": source, "command": [sys.executable, *sys.argv]})
    raw = (json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    require(len(raw) <= MAX_OUTPUT, "audit output byte ceiling")
    stage = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    with (stage / "audit.json").open("xb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    from analysis.kaushik_polymarket_replication.build_inputs import atomic_publish
    atomic_publish(stage, target)
    require((target / "audit.json").read_bytes() == raw, "published audit differs")
    print(json.dumps({"status": result["status"], "run_dir": str(target), "score_files": len(result["score_artifacts"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
