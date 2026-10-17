"""Tiny deterministic cells and independent dense oracles; no real data access."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_kalshi_replication import estimate as k


def fixture(con):
    rng = np.random.default_rng(8914)
    raw = []
    prices = (.035, .065, .925, .965)
    for event in range(18):
        for claim_index in range(3):
            claim = event * 3 + claim_index
            lifespan = (.8, 1., 1.000001, 1.8, 2.9, 5.)[(event + claim_index) % 6]
            outcome = float((event + claim_index) % 2)
            for visit in range(16):
                price = prices[(visit + claim_index + event) % 4]
                remaining = lifespan * (.04 + .91 * rng.random())
                if visit == 0 and lifespan >= 1:
                    remaining = 1.
                raw.append({"event_cluster": f"kalshi:event:{event}", "claim": claim,
                    "tail": int(price >= .9), "category": event % 3,
                    "P": price, "Y": outcome, "bin": 1 if price < .1 else 10,
                    "month": (visit // 4 + claim_index) % 2, "L": lifespan, "R": remaining,
                    "xL": float(np.log2(1 + lifespan)), "xR": float(np.log2(1 + remaining)),
                    "payoff": 100 * (outcome - price), "roi": 100 * (outcome / price - 1),
                    "partial": visit % 2})
    # Baseline-only rows are intentionally absent from both duration/A1 cells.
    baseline = raw + [{**raw[i], "P": .4, "bin": 5, "payoff": 100*(raw[i]["Y"]-.4),
                       "roi": 100*(raw[i]["Y"]/.4-1)} for i in range(0, len(raw), 58)]
    con.register("native_print_fixture", pa.Table.from_pylist(raw))
    con.register("baseline_print_fixture", pa.Table.from_pylist(baseline))
    con.execute("""CREATE TEMP TABLE fixture_coded AS SELECT *,
        dense_rank() OVER(ORDER BY tail,category)-1 cat_code,
        dense_rank() OVER(ORDER BY tail,P)-1 price_code,
        dense_rank() OVER(ORDER BY tail,month)-1 month_code,
        dense_rank() OVER(ORDER BY claim)-1 claim_fe_code,
        L>1 lifespan_gt1,R>1 remaining_gt1 FROM native_print_fixture""")
    con.execute("""CREATE TEMP TABLE baseline_cells AS SELECT event_cluster,bin,count(*)::BIGINT n_rows,
        sum(P)::DOUBLE sum_price,sum(Y)::DOUBLE sum_outcome,sum(payoff)::DOUBLE sum_payoff,
        sum(roi)::DOUBLE sum_roi FROM baseline_print_fixture GROUP BY 1,2""")
    for name, keys, features in (
        ("duration_cells", "event_cluster,tail,cat_code,price_code,month_code,lifespan_gt1,remaining_gt1",
         ("xL", "xR", "payoff", "roi")),
        ("claim_cells", "event_cluster,claim_fe_code", ("tail::DOUBLE", "tail*xL", "xR", "tail*xR", "payoff", "roi"))):
        derived = ",".join(f"{feature} f{i}" for i, feature in enumerate(features))
        expressions = [f"sum(f{i})::DOUBLE a{i}" for i in range(len(features))]
        expressions += [f"(count(*)*covar_pop(f{i},f{j}))::DOUBLE c{i}_{j}"
                        for i in range(len(features)) for j in range(i, len(features))]
        # Partial is part of GROUP BY but omitted from the saved key. This
        # deliberately creates repeated accepted cells without merging moments.
        con.execute(f"CREATE TEMP TABLE {name} AS WITH features AS (SELECT *,{derived} FROM fixture_coded) "
                    f"SELECT {keys},count(*)::BIGINT n_rows,{','.join(expressions)} FROM features GROUP BY {keys},partial")
    return raw, baseline


def dense_oracle(rows, spec, *, claim_fe=False):
    h = np.array([row["tail"] for row in rows])
    xl, xr = (np.array([row[name] for row in rows]) for name in ("xL", "xR"))
    if claim_fe:
        x = np.column_stack((h, h*xl, xr, h*xr))
    else:
        controls = np.column_stack([xl if clock == "xL" else xr for clock in spec["clocks"]])
        base = controls if spec["effects"] else np.column_stack((np.ones(len(rows)), controls))
        x = np.column_stack((base*(1-h[:, None]), base*h[:, None]))
    y = np.array([[row["payoff"], row["roi"]] for row in rows])
    residual_x, residual_y = x.copy(), y.copy()
    if spec["effects"]:
        families = []
        for family in spec["effects"]:
            key = {"cat_code": "category", "price_code": "P", "month_code": "month", "claim_fe_code": "claim"}[family]
            values = [(row["tail"], row[key]) for row in rows] if not claim_fe else [row[key] for row in rows]
            levels = sorted(set(values))
            families.append(np.array([[int(value == level) for level in levels] for value in values], dtype=float))
        dummies = np.column_stack(families)
        residual_x -= dummies @ np.linalg.lstsq(dummies, x, rcond=None)[0]
        residual_y -= dummies @ np.linalg.lstsq(dummies, y, rcond=None)[0]
    beta = np.linalg.lstsq(residual_x, residual_y, rcond=None)[0]
    bread = np.linalg.inv(residual_x.T @ residual_x)
    residual = residual_y - residual_x @ beta
    score = (residual[:, :, None] * residual_x[:, None, :]).reshape(len(rows), -1)
    events = sorted({row["event_cluster"] for row in rows})
    ids = np.array([row["event_cluster"] for row in rows])
    grouped_score = np.array([score[ids == event].sum(axis=0) for event in events])
    influence = grouped_score @ np.kron(np.eye(2), bread)
    return beta.T.reshape(-1), influence.T @ influence


class CellEstimatorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.con = duckdb.connect()
        self.raw, self.baseline = fixture(self.con)
        self.disk = patch.object(k.shutil, "disk_usage", return_value=type("Disk", (), {"free": 10**15})())
        self.disk.start()

    def tearDown(self):
        self.disk.stop()
        self.con.close()
        self.temporary.cleanup()

    def estimate(self, name="run", **kwargs):
        output = self.root / name
        output.mkdir()
        return k.estimate_relations(self.con, baseline_relation="baseline_cells",
            duration_relation="duration_cells", claim_relation="claim_cells", output_dir=output,
            sample_metadata={"baseline_rows": len(self.baseline), "duration_tail_rows": len(self.raw),
                             "claim_support": {"claims": 54}},
            resource_limits=kwargs.pop("resource_limits", k.ResourceLimits()), **kwargs)

    def test_models_and_joint_scores_match_independent_dense_print_oracles(self):
        result = self.estimate()
        self.assertEqual(len(result["table2"]), 5)
        models = [(model, self.raw, False) for model in result["table2"]]
        for panel, selected in (("L_gt1", [row for row in self.raw if row["L"]>1]),
                                ("R_gt1", [row for row in self.raw if row["R"]>1])):
            self.assertEqual(len(result["table3"][panel]), 3)
            models += [(model, selected, False) for model in result["table3"][panel]]
            self.assertEqual({model["n_observations"] for model in result["table3"][panel]}, {len(selected)})
        models.append((result["appendix_a1"], self.raw, True))
        for model, selected, a1 in models:
            with self.subTest(model=model["model_id"]):
                self.assertFalse(model["suppressed"], model["suppression_reasons"])
                values, covariance = dense_oracle(selected, model["spec"], claim_fe=a1)
                saved = model["joint"]
                actual = [row["CR0"]["estimate"] for row in saved["estimates"]]
                np.testing.assert_allclose(actual, values, atol=2e-7, rtol=2e-8)
                np.testing.assert_allclose(saved["covariance_CR0"], covariance, atol=2e-6, rtol=2e-7)
                scores = pq.read_table(self.root / "run" / saved["metadata"]["score_artifact"]["path"])
                matrix = np.array(scores["projected_score"].to_pylist())
                np.testing.assert_allclose(matrix.T@matrix, saved["covariance_CR0"], atol=1e-10)
                self.assertEqual(model["n_observations"], len(selected))
        self.assertEqual({model["n_observations"] for model in result["table2"]}, {len(self.raw)})
        self.assertEqual(result["sports"]["status"], "pending_not_estimated")
        self.assertEqual(result["appendix_a2"]["status"], "unavailable_from_public_trade_prints")
        self.assertEqual(len(result["score_artifacts"]), 13)
        self.assertEqual(json.loads((self.root/"run"/"estimates.json").read_text()), result)

    def test_baseline_equal_print_means_and_cross_tail_covariance(self):
        result = self.estimate()
        joint = result["table1"]["joint"]
        targets = {"price":"P", "win_rate":"Y", "payoff_cents":"payoff", "roi_percent":"roi"}
        names = joint["names"]
        scores = []
        for event in sorted({row["event_cluster"] for row in self.baseline}):
            event_scores = []
            for target, field in targets.items():
                for band in range(1,11):
                    selected = [row for row in self.baseline if row["bin"] == band]
                    mean = np.mean([row[field] for row in selected]) if selected else 0
                    if selected:
                        index = names.index(f"{target}|D{band}")
                        self.assertAlmostEqual(joint["estimates"][index]["CR0"]["estimate"], mean)
                    event_scores.append(sum(row[field]-mean for row in selected if row["event_cluster"]==event)/len(selected) if selected else 0)
            scores.append(event_scores)
        covariance = np.array(scores).T@np.array(scores)
        left, right = names.index("payoff_cents|D1"), names.index("payoff_cents|D10")
        self.assertNotEqual(covariance[left,right], 0)
        self.assertAlmostEqual(joint["covariance_CR0"][left][right], covariance[left,right])
        gap = result["table1"]["gap_rows"][0]
        expected = covariance[left,left]+covariance[right,right]-2*covariance[left,right]
        self.assertAlmostEqual(gap["CR0"]["standard_error"]**2, expected)

    def test_threshold_flags_and_repeated_partials_preserve_source_membership(self):
        result = self.estimate()
        self.assertEqual(result["sample"]["duration_panel_counts"]["L_gt1"], sum(row["L"]>1 for row in self.raw))
        self.assertEqual(result["sample"]["duration_panel_counts"]["R_gt1"], sum(row["R"]>1 for row in self.raw))
        groups, distinct = self.con.execute("SELECT count(*),count(DISTINCT (event_cluster,claim_fe_code)) FROM claim_cells").fetchone()
        self.assertGreater(groups, distinct)
        self.con.execute("UPDATE duration_cells SET remaining_gt1=true,lifespan_gt1=false WHERE rowid=0")
        with self.assertRaisesRegex(ValueError,"membership flags"):
            self.estimate("invalid_flags")

    def test_invalid_codes_population_and_resource_limits_fail_closed(self):
        self.con.execute("UPDATE duration_cells SET cat_code=cat_code+1")
        with self.assertRaisesRegex(ValueError,"globally dense"):
            self.estimate()
        with self.assertRaisesRegex(ValueError,"cannot be relaxed"):
            k.ResourceLimits(minimum_free_bytes=1).validate()
        self.con.execute("UPDATE duration_cells SET cat_code=cat_code-1")
        with self.assertRaisesRegex(ValueError,"read-pass budget"):
            self.estimate("reads", input_bytes={"baseline":10,"duration":10,"claim":10},
                          resource_limits=k.ResourceLimits(read_bytes=1))

    def test_rank_deficiency_is_withheld_with_saved_empty_scores(self):
        self.con.execute("UPDATE duration_cells SET a0=4*n_rows,c0_0=0,c0_1=0,c0_2=0,c0_3=0,lifespan_gt1=true")
        result = self.estimate()
        model = result["table2"][0]
        self.assertTrue(model["suppressed"])
        self.assertIn("rank_deficient_residualized_design",model["suppression_reasons"])
        self.assertTrue(all(row["suppressed"] and row["CR0"] is None for row in model["joint"]["estimates"]))
        path = self.root/"run"/model["joint"]["metadata"]["score_artifact"]["path"]
        self.assertEqual(pq.ParquetFile(path).metadata.num_rows,0)

    def test_claim_cell_event_population_drift_is_blocked(self):
        self.con.execute("UPDATE claim_cells SET event_cluster='wrong-event' WHERE claim_fe_code=0")
        with self.assertRaisesRegex(ValueError,"event populations differ"):
            self.estimate()

    def test_disk_artifact_json_and_existing_output_fail_closed(self):
        with patch.object(k.shutil,"disk_usage",return_value=type("Disk",(),{"free":k.FREE_FLOOR})()):
            with self.assertRaisesRegex(ValueError,"disk reservation"):
                self.estimate("disk")
        with self.assertRaisesRegex(ValueError,"artifact ceiling"):
            self.estimate("artifact", resource_limits=k.ResourceLimits(artifact_bytes=100,json_bytes=100))
        with self.assertRaisesRegex(ValueError,"JSON ceiling"):
            self.estimate("json",resource_limits=k.ResourceLimits(json_bytes=100))
        directory = self.root/"exists"
        directory.mkdir()
        (directory/"existing").write_text("preserved")
        with self.assertRaisesRegex(ValueError,"new empty"):
            k.estimate_relations(self.con,baseline_relation="baseline_cells",duration_relation="duration_cells",claim_relation="claim_cells",
                output_dir=directory,sample_metadata={},resource_limits=k.ResourceLimits())
        self.assertEqual((directory/"existing").read_text(),"preserved")

    def test_guard_precedes_any_production_io(self):
        with patch.object(k.production_guard,"require_production_host",side_effect=RuntimeError("guard called")), \
                patch.object(k,"_source_snapshot",side_effect=AssertionError("source read")):
            with self.assertRaisesRegex(RuntimeError,"guard called"):
                k.estimate_cells(manifest_path="missing",manifest_sha256="x",acceptance_path="missing",acceptance_sha256="x",
                    baseline_path="missing",duration_path="missing",claim_path="missing",output_dir="missing",
                    expected_source_head="x",expected_input_source_head="x")

    def admitted_inputs(self):
        artifacts, paths = {}, {}
        for kind, relation in (("baseline","baseline_cells"),("duration","duration_cells"),("claim","claim_cells")):
            path = self.root/(kind+".parquet")
            table = self.con.execute("SELECT * FROM "+relation).fetch_arrow_table()
            pq.write_table(table,path)
            artifacts[kind] = {"path":path.name,**k.io._output_info(path)}
            paths[kind] = path
        manifest = {"schema_version":"kaushik_kalshi_inputs_v1","status":"accepted",
            "scientific_contract":{"sample_scope":"all_available_dates","combos":"excluded","duration_endpoint":"settlement_ts","equal_print_weight":True,
                                   "source_last_day":"2026-10-01"},
            "counts":{"baseline_rows":len(self.baseline),"duration_tail_rows":len(self.raw)},
            "sample_metadata":{"baseline_rows":len(self.baseline),"duration_tail_rows":len(self.raw)},
            "source":{"head":"a"*40,"files":{"builder.py":"b"*64}},
            "input_inventory":[{"path":"synthetic-only"}],"artifacts":artifacts}
        manifest_path = self.root/"inputs.json"
        k.io.write_json(manifest_path,manifest)
        acceptance = {"schema_version":"kaushik_kalshi_inputs_acceptance_v1","status":"accepted",
            "manifest_sha256":k.io.sha256(manifest_path),"checks":dict.fromkeys(k.REQUIRED_ACCEPTANCE_CHECKS,True)}
        acceptance_path = self.root/"inputs_acceptance.json"
        k.io.write_json(acceptance_path,acceptance)
        return manifest_path,acceptance_path,paths

    def test_manifest_body_schema_source_and_independent_acceptance_bindings(self):
        manifest,acceptance,paths = self.admitted_inputs()
        kwargs = {"manifest_path":manifest,"manifest_sha256":k.io.sha256(manifest),
            "acceptance_path":acceptance,"acceptance_sha256":k.io.sha256(acceptance),"paths":paths,
            "expected_input_source_head":"a"*40,"ledger":k.grouped.ReadLedger()}
        admitted,_,records = k._admit_inputs(**kwargs)
        self.assertEqual(set(records),{"baseline","duration","claim"})
        self.assertEqual(admitted["scientific_contract"]["duration_endpoint"],"settlement_ts")
        with self.assertRaisesRegex(ValueError,"schema fields differ"):
            k._check_schema(pa.schema([pa.field("event_cluster",pa.string())]),"baseline")
        with self.assertRaisesRegex(ValueError,"source commit differs"):
            k._admit_inputs(**{**kwargs,"expected_input_source_head":"f"*40})
        invalid = deepcopy(admitted)
        invalid["scientific_contract"]["equal_print_weight"] = False
        wrong = self.root/"wrong.json"
        k.io.write_json(wrong,invalid)
        with self.assertRaisesRegex(ValueError,"acceptance binding"):
            k._admit_inputs(**{**kwargs,"manifest_path":wrong,"manifest_sha256":k.io.sha256(wrong)})
        with paths["duration"].open("ab") as stream:
            stream.write(b"drift")
        with self.assertRaisesRegex(ValueError,"body size/hash differs"):
            k._admit_inputs(**kwargs)

    def test_estimator_source_and_missing_independent_checks_are_blocked(self):
        with patch.object(k.subprocess,"check_output",return_value="b"*40):
            with self.assertRaisesRegex(ValueError,"source HEAD differs"):
                k._source_snapshot("a"*40)
        manifest,acceptance,paths = self.admitted_inputs()
        receipt = json.loads(acceptance.read_text())
        receipt["checks"]["cell_moments_validated"] = False
        rejected = self.root/"rejected_acceptance.json"
        k.io.write_json(rejected,receipt)
        with self.assertRaisesRegex(ValueError,"input checks incomplete"):
            k._admit_inputs(manifest_path=manifest,manifest_sha256=k.io.sha256(manifest),
                acceptance_path=rejected,acceptance_sha256=k.io.sha256(rejected),paths=paths,
                expected_input_source_head="a"*40,ledger=k.grouped.ReadLedger())

    def test_compact_artifact_paths_must_stay_inside_the_accepted_stage(self):
        for value in ("../outside.parquet", "/tmp/outside.parquet", "sub/../baseline.parquet", "sub//baseline.parquet"):
            with self.subTest(path=value), self.assertRaisesRegex(ValueError,"normalized and relative"):
                k._artifact_path(self.root,value)
        outside = self.root.parent/"outside.parquet"
        symlink = self.root/"escape.parquet"
        symlink.symlink_to(outside)
        with self.assertRaisesRegex(ValueError,"escapes accepted stage"):
            k._artifact_path(self.root,symlink.name)
        self.assertEqual(k._artifact_path(self.root,"inside.parquet"),self.root.resolve()/"inside.parquet")

    def test_production_wrapper_publishes_frozen_manifest_ledger_on_tiny_fixture(self):
        manifest,acceptance,paths = self.admitted_inputs()
        run_parent = (self.root/"mock-production-runs").resolve()
        run_parent.mkdir()
        meminfo = self.root/"mock-meminfo"
        meminfo.write_text("MemAvailable: 300000000 kB\n")
        def fixture_path(value):
            path = Path(value)
            return meminfo if path == Path("/proc/meminfo") else path
        source = {"head":"c"*40,"files":{"synthetic_fixture_only":"d"*64}}
        with patch.object(k.production_guard,"require_production_host"), \
                patch.object(k,"_source_snapshot",return_value=source), \
                patch.object(k,"RUN_ROOT",run_parent), \
                patch.object(k,"Path",side_effect=fixture_path), \
                patch.object(k.sys,"executable","/home/ubuntu/venv/bin/python"), \
                patch.object(k.os,"cpu_count",return_value=4):
            result = k.estimate_cells(manifest_path=manifest,manifest_sha256=k.io.sha256(manifest),
                acceptance_path=acceptance,acceptance_sha256=k.io.sha256(acceptance),
                baseline_path=paths["baseline"],duration_path=paths["duration"],claim_path=paths["claim"],
                output_dir=run_parent/"tiny-estimates",expected_source_head="c"*40,
                expected_input_source_head="a"*40)
        published = run_parent/"tiny-estimates"
        self.assertEqual(json.loads((published/"manifest.json").read_text()),result)
        self.assertNotIn("output_json_reopen:manifest.json",result["read_ledger"]["scans"])
        self.assertEqual(json.loads((published/"acceptance.json").read_text())["status"],"estimates_reopened_accepted")
        self.assertFalse((published/"failure.json").exists())


if __name__ == "__main__":
    unittest.main()
