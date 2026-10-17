"""Bounded estimation of independently accepted native Kalshi sufficient cells.

Production admission is separate from the fixture-accessible numerical functions.
Repeated partial cells are valid: their centered moments describe their own rows,
and their scores are added by native event. Nothing merges centered moments by
simple addition or invents maker, wallet, sports, or raw-observation outputs.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import sys
import tempfile
from typing import Mapping

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import production_guard
from analysis.kaushik_polymarket_replication import build_inputs as io
from analysis.kaushik_polymarket_replication import estimators as engine
from analysis.kaushik_polymarket_replication import run_estimates as grouped


TARGETS = ("payoff_cents", "roi_percent")
FREE_FLOOR = 20_000_000_000
RUN_ROOT = Path("/mnt/data/runs")
REQUIRED_ACCEPTANCE_CHECKS = (
    "schema_validated", "body_hashes_validated", "source_commit_validated",
    "global_trade_ids_unique", "source_rows_reconciled", "no_join_fanout",
    "cell_moments_validated", "duration_membership_validated",
    "claim_invariants_validated", "codes_validated",
)
SOURCE_FILES = (
    "analysis/kaushik_kalshi_replication/estimate.py",
    "analysis/kaushik_polymarket_replication/estimators.py",
    "analysis/kaushik_polymarket_replication/run_estimates.py",
    "analysis/kaushik_polymarket_replication/build_inputs.py",
    "analysis/kaushik_polymarket_replication/exchange_filter_contract.py",
    "production_guard.py", "tests/test_kaushik_kalshi_estimate.py",
)
BASELINE_FIELDS = {
    "event_cluster": "string", "bin": "integer", "n_rows": "integer",
    "sum_price": "double", "sum_outcome": "double",
    "sum_payoff": "double", "sum_roi": "double",
}
DURATION_KEYS = {
    "event_cluster": "string", "tail": "integer", "cat_code": "integer",
    "price_code": "integer", "month_code": "integer",
    "lifespan_gt1": "bool", "remaining_gt1": "bool", "n_rows": "integer",
}
CLAIM_KEYS = {"event_cluster": "string", "claim_fe_code": "integer", "n_rows": "integer"}


@dataclass(frozen=True)
class ResourceLimits:
    """Explicit stage bounds; numerical tolerances are never configurable here."""

    output_bytes: int = 12_000_000_000
    artifact_bytes: int = 3_000_000_000
    transient_bytes: int = 4_000_000_000
    spill_bytes: int = 4_000_000_000
    read_bytes: int = 8_000_000_000_000
    json_bytes: int = 16_000_000
    minimum_free_bytes: int = FREE_FLOOR

    def validate(self):
        for name, value in asdict(self).items():
            io.require(type(value) is int and value > 0, "positive integer resource bound required: " + name)
        io.require(self.minimum_free_bytes >= FREE_FLOOR, "20GB free-space floor cannot be relaxed")
        io.require(self.artifact_bytes <= self.transient_bytes <= self.output_bytes,
                   "artifact/transient/output resource bounds disagree")
        io.require(self.json_bytes <= self.artifact_bytes, "JSON bound exceeds artifact bound")
        io.require(self.read_bytes <= grouped.CAPS["maximum_read_bytes"], "read ceiling exceeds existing numerical driver")
        return self

    @property
    def reservation_bytes(self):
        return self.output_bytes + self.transient_bytes + self.spill_bytes + self.minimum_free_bytes


def _moments_fields(width):
    return {**{f"a{i}": "double" for i in range(width)},
            **{f"c{i}_{j}": "double" for i in range(width) for j in range(i, width)}}


def _fields(kind):
    return (BASELINE_FIELDS if kind == "baseline" else
            {**DURATION_KEYS, **_moments_fields(4)} if kind == "duration" else
            {**CLAIM_KEYS, **_moments_fields(6)})


def _check_schema(schema, kind):
    expected = _fields(kind)
    io.require(set(schema.names) == set(expected), kind + " cell schema fields differ")
    for name, expected_type in expected.items():
        actual = schema.field(name).type
        valid = (pa.types.is_string(actual) or pa.types.is_large_string(actual)) if expected_type == "string" else (
            pa.types.is_integer(actual) if expected_type == "integer" else
            pa.types.is_boolean(actual) if expected_type == "bool" else pa.types.is_float64(actual))
        io.require(valid, kind + " cell field type differs: " + name)


def _validate_relation(con, relation, kind, ledger, size):
    """Structural cell checks supplement, never replace independent admission."""
    ledger.charge(size, "validate_cells:" + kind)
    fields = _fields(kind)
    numeric = [name for name, field_type in fields.items() if field_type == "double"]
    bad = ["event_cluster IS NULL", "event_cluster=''", "n_rows IS NULL", "n_rows<=0", "n_rows>9007199254740992"]
    bad += [f"{io.ident(name)} IS NULL OR NOT isfinite({io.ident(name)})" for name in numeric]
    if kind == "baseline":
        bad += ["bin IS NULL OR bin NOT BETWEEN 1 AND 10", "sum_price<=0 OR sum_price>=n_rows",
                "sum_outcome<0 OR sum_outcome>n_rows"]
    else:
        codes = ("cat_code", "price_code", "month_code") if kind == "duration" else ("claim_fe_code",)
        bad += [f"{name} IS NULL OR {name}<0" for name in codes]
        width = 4 if kind == "duration" else 6
        bad += [f"c{i}_{i} < -1e-10*greatest(1.0,abs(a{i}))" for i in range(width)]
        if kind == "duration":
            bad += ["tail IS NULL OR tail NOT IN (0,1)",
                    "lifespan_gt1 IS NULL OR remaining_gt1 IS NULL",
                    "a0<=0 OR a1<0 OR a1>a0+1e-10*greatest(1.0,abs(a0))",
                    "remaining_gt1 AND NOT lifespan_gt1"]
        else:
            bad += ["a0<0 OR a0>n_rows OR a0<>floor(a0)", "a1<0 OR a2<0 OR a3<0"]
    found = con.execute(f"SELECT count(*) FILTER(WHERE {' OR '.join('('+item+')' for item in bad)}),"
                        f"count(*),coalesce(sum(n_rows),0),count(DISTINCT event_cluster) FROM {io.ident(relation)}").fetchone()
    io.require(found[0] == 0, kind + " cells contain invalid values or membership flags")
    io.require(found[2] <= 2**53, kind + " observation count exceeds exact frequency-weight range")
    if kind != "baseline":
        for name in codes:
            ledger.charge(size, "validate_code_family:" + name)
            n, low, high = con.execute(f"SELECT count(DISTINCT {name}),min({name}),max({name}) FROM {io.ident(relation)}").fetchone()
            io.require(n == 0 or (low == 0 and high == n - 1), "cell codes are not globally dense: " + name)
            if kind == "duration":
                ledger.charge(size, "validate_tail_interaction:" + name)
                io.require(con.execute(f"SELECT count(*) FROM (SELECT {name} FROM {io.ident(relation)} GROUP BY 1 HAVING count(DISTINCT tail)<>1)").fetchone()[0] == 0,
                           "fixed-effect code is shared across tails: " + name)
    if kind == "claim":
        ledger.charge(size, "validate_claim_event_mapping")
        io.require(con.execute(f"SELECT count(*) FROM (SELECT claim_fe_code FROM {io.ident(relation)} GROUP BY 1 HAVING count(DISTINCT event_cluster)<>1)").fetchone()[0] == 0,
                   "claim fixed-effect code spans multiple event clusters")
    return {"groups": int(found[1]), "observations": int(found[2]), "clusters": int(found[3])}


class _Writer:
    def __init__(self, directory, limits, ledger):
        self.directory, self.limits, self.ledger = Path(directory), limits.validate(), ledger
        self.artifacts = {}

    def reserve(self, allowance=0):
        used, spill = 0, 0
        for path in self.directory.rglob("*"):
            if path.is_file():
                if path.relative_to(self.directory).parts[0] == "spill":
                    stat = path.stat()
                    spill += min(stat.st_size, stat.st_blocks * 512)
                else:
                    used += path.stat().st_size
        io.require(used + allowance <= self.limits.output_bytes, "estimate total-output ceiling exceeded")
        needed = (self.limits.output_bytes - used + self.limits.transient_bytes
                  + max(0, self.limits.spill_bytes - spill) + self.limits.minimum_free_bytes)
        io.require(shutil.disk_usage(self.directory).free >= needed, "estimate disk reservation/free-space floor failed")

    def json(self, name, value):
        raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
        io.require(len(raw) <= self.limits.json_bytes, "estimate JSON ceiling exceeded")
        self.reserve(len(raw))
        path = self.directory / name
        with path.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        self.reserve()
        self.ledger.charge(len(raw), "output_json_reopen:" + name)
        io.require(path.read_bytes() == raw, "estimate JSON failed reopen")

    def joint(self, result, key):
        scores = result.coefficient_cluster_influence
        io.require(scores.nbytes <= grouped.CAPS["numpy_memory_bytes"], "joint-score allocation exceeds NumPy bound")
        path = self.directory / (key + "_scores.parquet")
        io.require(not path.exists(), "immutable joint score already exists")
        values = pa.FixedSizeListArray.from_arrays(pa.array(scores.reshape(-1)), len(result.names))
        table = pa.table({"cluster_key": pa.array([str(item) for item in result.metadata["cluster_levels"]], type=pa.string()),
                          "projected_score": values})
        self.reserve(self.limits.artifact_bytes)
        with io.copy_ceiling(self.limits.transient_bytes):
            pq.write_table(table, path, compression="zstd")
        io.require(path.stat().st_size <= self.limits.artifact_bytes, "saved score exceeds artifact ceiling")
        self.reserve()
        self.ledger.charge(path.stat().st_size * 2, "score_hash_and_reopen:" + key)
        info = io._output_info(path)
        io.require(pq.read_table(path).equals(table), "saved joint score failed reopen")
        info.update(path=path.name, coefficient_order=list(result.names),
                    score_definition="projected joint coefficient influence; CR0=score.T@score")
        self.artifacts[key] = info
        output = result.to_dict()
        output["metadata"].pop("cluster_levels", None)
        output["metadata"].pop("cluster_counts", None)
        output["metadata"]["score_artifact"] = info
        return output


def _regression(con, relation, width, spec, model_id, writer, ledger, scan_bytes, *, claim_fe=False):
    """Existing lossless grouped FWL/OLS procedure, with explicit stage writes."""
    effects, clocks = spec["effects"], spec["clocks"]
    terms = (("H", "H:xL", "xR", "H:xR") if claim_fe else
             tuple(f"{tail}:{name}" for tail in ("D1", "D10")
                   for name in (("level", *clocks) if not effects else clocks)))
    p, q = len(terms), len(TARGETS)
    ledger.charge(scan_bytes * (len(effects) + 1), "model_codes_population:" + model_id)
    query, levels = grouped.remap_effect_codes(con, relation, effects, model_id)
    levels_by_tail = None
    if effects and not claim_fe:
        ledger.charge(scan_bytes, "model_effects_by_tail:" + model_id)
        sql = ",".join(f"count(DISTINCT {io.ident(effect)}) {io.ident(effect)}" for effect in effects)
        levels_by_tail = {"D10" if row["tail"] else "D1": {effect:row[effect] for effect in effects}
                          for row in grouped.rows(con,f"SELECT tail,{sql} FROM {io.ident(relation)} GROUP BY 1 ORDER BY 1")}
    group_count, n, g = con.execute(f"SELECT count(*),coalesce(sum(n_rows),0),count(DISTINCT event_cluster) FROM {io.ident(relation)}").fetchone()
    if not n:
        return {"model_id": model_id, "spec": spec, "n_observations": 0, "n_clusters": 0,
                "suppressed": True, "suppression_reasons": ["empty_estimation_sample"], "joint": None,
                "slopes": [{"outcome": target, "clock": clock, "suppressed": True,
                            "suppression_reasons": ["empty_estimation_sample"], "CR0": None,
                            "cluster_count_adjusted": None, "influence": None}
                           for target in TARGETS for clock in spec["report_clocks"]]}
    persistent = 8 * (sum(levels.values()) * (p + q) + max(levels.values(), default=0) * (p + q + 1) + g * p * q * 6)
    workspace = grouped.CAPS["batch_rows"] * (width ** 2 + p ** 2 + p * q + 30) * 8
    io.require(persistent + workspace <= grouped.CAPS["numpy_memory_bytes"], "model arrays exceed 32GB NumPy budget")

    def replay(projection_only=False):
        ledger.charge(scan_bytes, "projection_or_moments:" + model_id)
        yield from grouped.regression_batches(con, query, spec, width, claim_fe=claim_fe,
                                               projection_only=projection_only)

    absorber, capture = None, None
    if effects:
        captured, capture = grouped.capture_projection_batches(lambda: replay(True),
            expected_groups=group_count, expected_observations=n, column_count=p, target_count=q,
            code_names=effects, persistent_bytes=persistent, batch_workspace_bytes=workspace)
        try:
            absorber = engine.absorb_categorical_effects(lambda: iter(captured), term_names=terms,
                target_names=TARGETS, level_counts=levels,
                tolerance=grouped.CAPS["projection_tolerance"],
                max_iterations=2 if len(effects) == 1 else grouped.CAPS["maximum_projection_iterations"],
                memory_limit_bytes=grouped.CAPS["numpy_memory_bytes"] // 2)
        finally:
            captured.clear()
            capture["released_before_full_moment_replay"] = True
    moments = engine.RegressionMoments(terms, TARGETS)
    within_y, within_x = np.zeros((q, q)), np.zeros(p)
    for batch in replay():
        x, y, w = absorber.transform(batch) if absorber else (batch["x"], batch["y"], batch["weights"])
        moments.add(x, y, w, batch["observation_counts"])
        for name, key in (("xtx", "within_xx"), ("xty", "within_xy"), ("yty", "within_yy")):
            getattr(moments, name)[:] += batch[key].sum(axis=0)
        within_y += batch["within_yy"].sum(axis=0)
        within_x += np.diagonal(batch["within_xx"], axis1=1, axis2=2).sum(axis=0)
    if absorber:
        absorber.original_yty += within_y
        absorber.original_x_squared_norms += within_x
    fit = engine.fit_ols_moments(moments, absorption=absorber)
    scores = engine.ClusterScoreMoments(p * q, memory_limit_bytes=grouped.CAPS["numpy_memory_bytes"] // 4)
    fit_stats = {}
    if fit.beta is not None:
        for batch in replay():
            x, y, w = absorber.transform(batch) if absorber else (batch["x"], batch["y"], batch["weights"])
            correction = batch["within_xy"] - batch["within_xx"] @ fit.beta
            scores.add(batch["clusters"], fit.score_batch(x, y, w)
                       + correction.transpose(0, 2, 1).reshape(len(x), -1), batch["observation_counts"])
            residual = y - x @ fit.beta
            residual_within = (batch["within_yy"] - fit.beta.T @ batch["within_xy"]
                - batch["within_xy"].transpose(0, 2, 1) @ fit.beta + fit.beta.T @ batch["within_xx"] @ fit.beta)
            if not claim_fe:
                for tail, label in ((0, "D1"), (1, "D10")):
                    selected = batch["tail"] == tail
                    total = fit_stats.setdefault(label, {"weight_sum": 0., "y_sum": np.zeros(q),
                        "y_squared_sum": np.zeros(q), "residual_squared_sum": np.zeros(q)})
                    total["weight_sum"] += float(w[selected].sum())
                    total["y_sum"] += (batch["y"][selected] * w[selected, None]).sum(axis=0)
                    total["y_squared_sum"] += (batch["y"][selected] ** 2 * w[selected, None]).sum(axis=0) + np.diagonal(batch["within_yy"][selected], axis1=1, axis2=2).sum(axis=0)
                    total["residual_squared_sum"] += (residual[selected] ** 2 * w[selected, None]).sum(axis=0) + np.diagonal(residual_within[selected], axis1=1, axis2=2).sum(axis=0)
    result = engine.finalize_clustered_regression(fit, scores, fit_statistics_by_group=fit_stats or None)
    joint = writer.joint(result, model_id)
    ranks = None
    if not claim_fe:
        block = p // 2
        ranks = {label: engine.continuous_design_diagnostics(moments.xtx[index*block:(index+1)*block, index*block:(index+1)*block],
            original_squared_norms=absorber.original_x_squared_norms[index*block:(index+1)*block] if absorber else None)[2]
            for index, label in enumerate(("D1", "D10"))}
    joint["metadata"].update(group_count=int(group_count), admitted_allocation_bytes=int(persistent),
        batch_workspace_bytes=workspace, projection_cache=capture, effect_cardinality=levels,
        effect_cardinality_by_tail=levels_by_tail, residualized_continuous_design_by_tail=ranks,
        combined_absorbed_fixed_effect_rank="unknown; not inferred from cardinalities",
        moment_restoration="n*mean_i*mean_j plus centered c_i_j; partial cells retained separately",
        event_score_restoration="n*Xmean_residual*residual + withinXY-withinXX*beta",
        exact_price_effect="recorded normalized binary64 P; no rounding; separate tails")
    slopes = []
    for target in TARGETS:
        for clock in spec["report_clocks"]:
            coefficients = ({f"{target}|H:{clock}": 1} if claim_fe else
                            {f"{target}|D10:{clock}": 1, f"{target}|D1:{clock}": -1})
            vector = engine.contrast_vector(result.names, coefficients)
            slopes.append({"outcome": target, "clock": clock, **result.contrast(vector,
                name="claim_FE_interaction" if claim_fe else "D10_minus_D1_clock_slope")})
    return {"model_id": model_id, "spec": spec, "n_observations": int(n), "n_clusters": int(g),
            "suppressed": bool(fit.suppressed_reasons), "suppression_reasons": list(fit.suppressed_reasons),
            "joint": joint, "slopes": slopes}


def estimate_relations(con, *, baseline_relation, duration_relation, claim_relation,
                       output_dir, sample_metadata, resource_limits,
                       input_bytes=None, ledger=None):
    """Estimate admitted-shaped relations; local use is for synthetic fixtures only.

    The caller owns the new output directory. No publication or input-admission
    claim is made by this numerical function. Input schema/body validation is the
    guarded entrypoint's responsibility; value/code gates also run here.
    """
    limits = resource_limits.validate()
    output_dir = Path(output_dir)
    io.require(output_dir.is_dir() and not any(output_dir.iterdir()), "new empty estimator directory required")
    io.require(isinstance(sample_metadata, Mapping), "explicit sample metadata required")
    sizes = dict(input_bytes or {"baseline": 0, "duration": 0, "claim": 0})
    io.require(set(sizes) == {"baseline", "duration", "claim"} and all(type(value) is int and value >= 0 for value in sizes.values()), "explicit cell read sizes invalid")
    ledger = ledger or grouped.ReadLedger(ceiling=limits.read_bytes)
    writer = _Writer(output_dir, limits, ledger)
    writer.reserve()
    relations = {"baseline": baseline_relation, "duration": duration_relation, "claim": claim_relation}
    populations = {kind: _validate_relation(con, relation, kind, ledger, sizes[kind]) for kind, relation in relations.items()}
    io.require(populations["duration"]["observations"] == populations["claim"]["observations"], "A1/common duration observation counts differ")
    io.require(populations["duration"]["observations"] <= populations["baseline"]["observations"], "duration rows exceed baseline")
    ledger.charge(sizes["duration"] + sizes["claim"], "duration_claim_event_population")
    mismatches = con.execute(f"""WITH d AS (SELECT event_cluster,sum(n_rows) n FROM {io.ident(duration_relation)} GROUP BY 1),
        c AS (SELECT event_cluster,sum(n_rows) n FROM {io.ident(claim_relation)} GROUP BY 1)
        SELECT count(*) FROM d FULL OUTER JOIN c USING(event_cluster) WHERE d.n IS DISTINCT FROM c.n""").fetchone()[0]
    io.require(mismatches == 0, "A1/common duration event populations differ")
    for key, kind in (("baseline_rows", "baseline"), ("duration_tail_rows", "duration")):
        if key in sample_metadata:
            io.require(sample_metadata[key] == populations[kind]["observations"], "sample metadata count differs: " + key)
    baseline_g = populations["baseline"]["clusters"]
    io.require(baseline_g * 10 * 6 * 8 * 6 <= grouped.CAPS["numpy_memory_bytes"], "baseline joint arrays exceed 32GB NumPy budget")
    ledger.charge(sizes["baseline"], "baseline_joint_means")
    query = f"SELECT * FROM {io.ident(baseline_relation)} ORDER BY event_cluster,bin"
    def mean_records():
        for batch in grouped.array_batches(con, query):
            for index in range(len(batch["n_rows"])):
                n = int(batch["n_rows"][index])
                yield {"cluster": batch["event_cluster"][index], "cell": "D" + str(int(batch["bin"][index])),
                    "count": n, "weight_sum": float(n),
                    "weighted_sums": [float(batch[name][index]) for name in ("sum_price", "sum_outcome", "sum_payoff", "sum_roi")]}
    result = engine.joint_clustered_means_from_totals(mean_records(), cells=tuple(f"D{i}" for i in range(1, 11)),
        target_names=("price", "win_rate", *TARGETS), memory_limit_bytes=grouped.CAPS["numpy_memory_bytes"] // 4)
    table1 = {"profile_rows": grouped.profile_rows(result, scope="all_markets"), "gap_rows": []}
    for target in TARGETS:
        vector = engine.contrast_vector(result.names, {f"{target}|D10": 1, f"{target}|D1": -1})
        table1["gap_rows"].append({"outcome": target, **grouped.support_record(result, ["D1", "D10"], sports=False),
                                   **result.contrast(vector, name="D10_minus_D1")})
    table1["joint"] = writer.joint(result, "table1")
    del result
    table2 = [_regression(con, duration_relation, 4, {**spec, "report_clocks": spec["clocks"]},
                         f"table2_c{spec['column']}", writer, ledger, sizes["duration"])
              for spec in grouped.model_specs()]
    table3, panel_counts = {}, {}
    for panel, flag, clock in (("L_gt1", "lifespan_gt1", "xL"), ("R_gt1", "remaining_gt1", "xR")):
        relation = "kalshi_" + panel
        con.execute(f"CREATE TEMP VIEW {io.ident(relation)} AS SELECT * FROM {io.ident(duration_relation)} WHERE {flag}")
        ledger.charge(sizes["duration"], "panel_count:" + panel)
        panel_counts[panel] = int(con.execute(f"SELECT coalesce(sum(n_rows),0) FROM {io.ident(relation)}").fetchone()[0])
        table3[panel] = []
        for column, effects, clocks in ((1, [], [clock]), (2, ["cat_code"], [clock]),
                                        (3, ["cat_code", "price_code", "month_code"], ["xL", "xR"])):
            spec = {"column": column, "label": panel, "clocks": clocks, "effects": effects, "report_clocks": [clock]}
            table3[panel].append(_regression(con, relation, 4, spec, f"table3_{panel}_c{column}", writer, ledger, sizes["duration"]))
    a1_spec = {"column": 1, "label": "Claim FE diagnostic", "clocks": ["xL", "xR"],
               "report_clocks": ["xL", "xR"], "effects": ["claim_fe_code"]}
    a1 = _regression(con, claim_relation, 6, a1_spec, "appendix_a1", writer, ledger, sizes["claim"], claim_fe=True)
    ledger.charge(sizes["claim"], "claim_tail_support")
    support = con.execute(f"""SELECT count(*),coalesce(sum(n),0),count(*) FILTER(WHERE h>0 AND h<n),
        coalesce(sum(n) FILTER(WHERE h>0 AND h<n),0) FROM
        (SELECT claim_fe_code,sum(n_rows) n,sum(a0) h FROM {io.ident(claim_relation)} GROUP BY 1)""").fetchone()
    a1["claim_support"] = dict(zip(("claims", "observations", "both_tail_claims", "both_tail_claim_observations"), map(int,support)))
    io.require(a1["claim_support"]["observations"] == populations["duration"]["observations"], "claim support/common duration counts differ")
    if "claim_support" in sample_metadata:
        supplied_support = sample_metadata["claim_support"]
        io.require(isinstance(supplied_support, Mapping) and all(a1["claim_support"].get(name) == value for name,value in supplied_support.items()), "supplied claim support differs from cells")
    io.require(all(model["n_observations"] == populations["duration"]["observations"] for model in table2), "Table2 common sample differs")
    io.require(all(model["n_observations"] == panel_counts[panel] for panel, models in table3.items() for model in models), "Table3 common panel sample differs")
    output = {"schema_version": "kaushik_kalshi_estimates_v1", "status": "estimates_complete",
        "sample_scope": "all_available_dates", "sample": {**dict(sample_metadata), "cell_populations": populations, "duration_panel_counts": panel_counts},
        "definitions": {"observation": "one admitted native trade print", "weight": "one per native print",
            "duration_endpoint": "settlement_ts", "combos": "excluded", "price": "exact normalized binary64 P; no rounding",
            "duration": "open_time/settlement_ts; log2(1+days); common eligible tail sample",
            "outcomes": {"payoff_cents": "100*(Y-P)", "roi_percent": "100*(Y/P-1), before averaging"},
            "uncertainty": engine.VARIANCE_NOTE, "a1": "claim-FE price-path diagnostic; payoff within claim reduces mechanically to -P"},
        "table1": table1, "table2": table2, "table3": table3, "appendix_a1": a1,
        "appendix_a2": {"status": "unavailable_from_public_trade_prints", "reason": "no admitted maker/wallet role observations"},
        "sports": {"status": "pending_not_estimated", "reason": "separate admitted sports identity/result/start/end mapping required"},
        "score_artifacts": writer.artifacts, "resources": asdict(limits),
        "read_ledger": json.loads(json.dumps(ledger.to_dict()))}
    writer.json("estimates.json", output)
    return output


def _source_snapshot(expected_head):
    io.require(re.fullmatch(r"[0-9a-f]{40}", expected_head or ""), "explicit committed estimator source HEAD required")
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=io.REPO, text=True).strip()
    io.require(actual == expected_head, "estimator source HEAD differs")
    files = {}
    for relative in SOURCE_FILES:
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", relative], cwd=io.REPO, capture_output=True)
        clean = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", relative], cwd=io.REPO)
        io.require(tracked.returncode == clean.returncode == 0, "estimator source uncommitted or changed: " + relative)
        files[relative] = io.sha256(io.REPO / relative)
    return {"head": actual, "files": files}


def _artifact_path(root, value):
    root = Path(root).resolve()
    io.require(isinstance(value, str) and bool(value) and "\\" not in value
               and not Path(value).is_absolute() and all(part not in ("", ".", "..") for part in value.split("/")),
               "compact artifact path must be normalized and relative")
    resolved = (root / value).resolve()
    io.require(root in resolved.parents, "compact artifact path escapes accepted stage")
    return resolved


def _admit_inputs(*, manifest_path, manifest_sha256, acceptance_path, acceptance_sha256,
                  paths, expected_input_source_head, ledger):
    manifest, manifest_id = io.read_json(manifest_path, manifest_sha256)
    acceptance, acceptance_id = io.read_json(acceptance_path, acceptance_sha256)
    ledger.charge(manifest_id["stat"]["bytes"] + acceptance_id["stat"]["bytes"], "input_manifests")
    io.require(manifest.get("schema_version") == "kaushik_kalshi_inputs_v1" and manifest.get("status") == "accepted", "accepted Kalshi compact manifest required")
    io.require(not (Path(manifest_id["path"]).parent / "failure.json").exists(), "compact input stage has a failure marker")
    io.require(acceptance.get("schema_version") == "kaushik_kalshi_inputs_acceptance_v1" and acceptance.get("status") == "accepted"
               and acceptance.get("manifest_sha256") == manifest_id["sha256"], "independent Kalshi input acceptance binding required")
    checks = acceptance.get("checks", {})
    io.require(all(checks.get(name) is True for name in REQUIRED_ACCEPTANCE_CHECKS), "independent compact input checks incomplete")
    source = manifest.get("source", {})
    io.require(re.fullmatch(r"[0-9a-f]{40}", expected_input_source_head or "") and source.get("head") == expected_input_source_head,
               "compact input source commit differs")
    io.require(isinstance(source.get("files"), dict) and bool(source["files"]) and all(re.fullmatch(r"[0-9a-f]{64}", value or "") for value in source["files"].values()), "compact input source fingerprints absent")
    contract = manifest.get("scientific_contract", {})
    io.require(all(contract.get(name) == value for name, value in {
        "sample_scope": "all_available_dates", "combos": "excluded", "duration_endpoint": "settlement_ts", "equal_print_weight": True}.items()), "Kalshi scientific input contract differs")
    io.require(contract.get("equal_print_weight") is True and contract.get("cutoff_utc") is None
               and contract.get("cutoff_seconds") is None, "full-date scientific contract contains a cutoff or invalid weight")
    io.require(contract.get("source_last_day") == "2026-10-01", "full source inventory endpoint differs")
    io.require(isinstance(manifest.get("counts"), dict) and isinstance(manifest.get("sample_metadata"), dict)
               and bool(manifest.get("input_inventory")), "compact counts, sample metadata and source inventory required")
    for name in ("baseline_rows", "duration_tail_rows"):
        value = manifest["counts"].get(name)
        io.require(type(value) is int and value >= 0 and manifest["sample_metadata"].get(name) == value,
                   "compact count/sample binding differs: " + name)
    records = manifest.get("artifacts", {})
    io.require(isinstance(records, dict) and bool(records), "explicit compact artifact inventory required")
    admitted = {}
    root = Path(manifest_id["path"]).parent
    resolved_records = []
    for item in records.values():
        io.require(isinstance(item, dict), "compact artifact record must be an object")
        resolved_records.append((_artifact_path(root, item.get("path")), item))
    for kind, supplied in paths.items():
        supplied = Path(supplied).resolve()
        matching = [item for path, item in resolved_records if path == supplied]
        io.require(len(matching) == 1, "explicit compact path lacks unique manifest binding: " + kind)
        expected = matching[0]
        identity = io.stat_identity(supplied)
        ledger.charge(identity["bytes"], "input_hash:" + kind)
        io.require(identity["bytes"] == expected.get("bytes") and io.sha256(supplied) == expected.get("sha256"), "compact body size/hash differs: " + kind)
        parquet = pq.ParquetFile(supplied)
        _check_schema(parquet.schema_arrow, kind)
        io.require(parquet.metadata.num_rows == expected.get("rows") and str(parquet.schema_arrow) == expected.get("schema"), "compact footer schema/count differs: " + kind)
        io.require(identity == io.stat_identity(supplied), "compact input drift during admission")
        admitted[kind] = {"path": str(supplied), "stat": identity, "sha256": expected["sha256"], "rows": expected["rows"]}
    io.require(len({item["path"] for item in admitted.values()}) == 3, "compact artifact roles share a path")
    return manifest, {"manifest": manifest_id, "acceptance": acceptance_id}, admitted


def estimate_cells(*, manifest_path, manifest_sha256, acceptance_path, acceptance_sha256,
                   baseline_path, duration_path, claim_path, output_dir,
                   expected_source_head, expected_input_source_head,
                   resource_limits=ResourceLimits()):
    """Guarded immutable production wrapper; all artifact paths are explicit."""
    production_guard.require_production_host()
    io.require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "canonical production Python required")
    limits = resource_limits.validate()
    source = _source_snapshot(expected_source_head)
    target = Path(output_dir).resolve()
    io.require(target.parent == RUN_ROOT and not target.exists(), "new top-level production output directory required")
    io.require(shutil.disk_usage(target.parent).free >= limits.reservation_bytes, "compact estimator capacity insufficient")
    io.require((os.cpu_count() or 0) >= grouped.CAPS["threads"], "four estimator threads unavailable")
    available = re.search(r"^MemAvailable:\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.MULTILINE)
    io.require(available and int(available.group(1)) * 1024 >= grouped.CAPS["minimum_available_ram_bytes"], "240GB available RAM required")
    io.require(all(value == resource.RLIM_INFINITY or value >= limits.transient_bytes for value in resource.getrlimit(resource.RLIMIT_FSIZE)), "inherited file limit below admitted transient bound")
    ledger = grouped.ReadLedger(ceiling=limits.read_bytes)
    manifest, binding, admitted = _admit_inputs(manifest_path=manifest_path, manifest_sha256=manifest_sha256,
        acceptance_path=acceptance_path, acceptance_sha256=acceptance_sha256,
        paths={"baseline": baseline_path, "duration": duration_path, "claim": claim_path},
        expected_input_source_head=expected_input_source_head, ledger=ledger)
    planned_reads = (8 * admitted["baseline"]["stat"]["bytes"] + 100 * admitted["duration"]["stat"]["bytes"]
                     + 16 * admitted["claim"]["stat"]["bytes"] + 6 * limits.output_bytes + 8 * limits.json_bytes)
    io.require(ledger.charged_bytes + planned_reads <= limits.read_bytes, "compact worst-case read plan exceeds ceiling")
    for path in [manifest_path, acceptance_path, *[item["path"] for item in admitted.values()]]:
        resolved = Path(path).resolve()
        io.require(target != resolved and target not in resolved.parents and resolved not in target.parents, "estimate output overlaps an input")
    stage = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    con, published = None, False
    try:
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'")
        con.execute("SET memory_limit='192GB'")
        con.execute("SET threads=4")
        con.execute("SET preserve_insertion_order=true")
        con.execute("SET enable_object_cache=false")
        con.execute("SET temp_directory=" + io.literal(stage / "spill"))
        con.execute("SET max_temp_directory_size=" + io.literal(str(limits.spill_bytes) + "B"))
        con.execute("SET disabled_optimizers='join_order,build_side_probe_side'")
        policy = grouped.join_planning_policy(con)
        for kind, item in admitted.items():
            con.execute(f"CREATE TEMP VIEW {io.ident('kalshi_'+kind)} AS SELECT * FROM read_parquet({io.literal(item['path'])})")
        output = estimate_relations(con, baseline_relation="kalshi_baseline", duration_relation="kalshi_duration",
            claim_relation="kalshi_claim", output_dir=stage, sample_metadata=manifest["sample_metadata"],
            resource_limits=limits, input_bytes={kind: item["stat"]["bytes"] for kind, item in admitted.items()}, ledger=ledger)
        io.require(_source_snapshot(expected_source_head) == source, "estimator source drift before publication")
        for kind, item in admitted.items():
            ledger.charge(item["stat"]["bytes"], "input_hash_after:" + kind)
            io.require(io.stat_identity(item["path"]) == item["stat"] and io.sha256(item["path"]) == item["sha256"], "compact input drift before publication")
        for item in binding.values():
            io.require(io.read_json(item["path"], item["sha256"])[1] == item, "compact acceptance/manifest drift")
        io.require(grouped.join_planning_policy(con) == policy, "join planning policy drift")
        writer = _Writer(stage, limits, ledger)
        estimate_identity = io.sha256(stage / "estimates.json")
        result = {"schema_version": "kaushik_kalshi_estimate_stage_v1", "status": "estimates_complete", "source": source,
            "input_binding": binding, "input_artifacts": admitted, "scientific_contract": manifest["scientific_contract"],
            "resources": asdict(limits), "numerical_caps": {name: grouped.CAPS[name] for name in (
                "memory_limit", "threads", "numpy_memory_bytes", "total_memory_bytes", "minimum_available_ram_bytes",
                "batch_rows", "maximum_projection_iterations", "projection_tolerance")},
            "planned_read_bytes": planned_reads, "join_planning_policy": policy,
            "outputs": output["score_artifacts"], "estimates_sha256": estimate_identity,
            "read_ledger": json.loads(json.dumps(ledger.to_dict())), "reconciliation": {"inputs_reopened": True, "common_duration_population": True,
                "joint_scores_saved": True, "sports_pending": True, "maker_roles_unavailable": True},
            "environment": {"python": sys.version, "duckdb": duckdb.__version__, "numpy": np.__version__}}
        writer.json("manifest.json", result)
        con.close()
        con = None
        io.atomic_publish(stage, target)
        published = True
        io.require(io.read_json(target / "manifest.json")[0] == result and io.sha256(target / "estimates.json") == estimate_identity, "published estimator failed reopen")
        for item in output["score_artifacts"].values():
            io.require(io._output_info(target / item["path"]) == {key: item[key] for key in ("bytes", "sha256", "rows", "schema", "row_groups")}, "published score failed reopen")
        receipt = {"schema_version": "kaushik_kalshi_estimate_acceptance_v1", "status": "estimates_reopened_accepted",
            "manifest_sha256": io.sha256(target / "manifest.json"), "estimates_sha256": estimate_identity,
            "source_head": expected_source_head, "all_outputs_reopened": True,
            "independent_saved_score_audit": "pending"}
        _Writer(target, limits, ledger).json("acceptance.json", receipt)
        return result
    except BaseException as error:
        failed = target if published else stage
        if failed.exists() and not (failed / "failure.json").exists():
            io.write_json(failed / "failure.json", {"status": "estimates_incomplete", "error_type": type(error).__name__, "reason": str(error)})
        raise
    finally:
        if con is not None:
            con.close()
