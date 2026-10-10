"""Bounded synthetic stage tests; no production reads or guard bypasses."""
from __future__ import annotations

import json
import errno
import os
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_polymarket_replication import run_estimates as driver
from scripts import estimate_kaushik_polymarket_replication as cli


def fixture(con, folder):
    records, claims, maps = [], [], []
    start = 1738368000  # 2025-02-01 UTC; far before the frozen execution cutoff.
    for market in range(27):
        opening = start - 3600
        lifespan = 1 + market % 8
        endpoint = opening + lifespan * 86400
        sport = driver.SPORTS[market % 9]
        maps.append({"market_id": f"m{market}", "sport": sport, "game_key": f"{sport}:g{market}",
            "actual_start_seconds": float(start), "actual_end_seconds": float(start + 7200),
            "timing_quality": "tiny accepted fixture clocks", "provenance": "synthetic only"})
        for outcome in (0, 1):
            claim = 2 * market + outcome
            claims.append({"token_id": f"t{claim}", "market_id": f"m{market}", "claim_code": claim,
                           "Y": float(outcome), "outcome_label": f"label{outcome}"})
            for index in range(20):
                p = (.03, .06, .91, .96)[(index + market + outcome) % 4]
                timestamp = start - 900 + (index % 12) * 600
                remaining = (endpoint - timestamp) / 86400
                maker = index % 7 == 0
                records.append({"claim_code": claim, "claim_id": f"t{claim}", "market_id": f"m{market}",
                    "event_cluster": f"event:g{market}", "cluster_source": "native_event" if market % 2 else "market_fallback",
                    "category": ("Sports", "Politics", "Crypto")[market % 3], "timestamp": timestamp,
                    "P": p, "Y": outcome, "payoff": 100 * (outcome - p), "roi": 100 * (outcome / p - 1),
                    "is_maker": maker, "side": "BUY" if maker or index % 3 else "SELL", "bin": 1 if p < .1 else 10,
                    "duration_eligible": True, "L": float(lifespan), "R": remaining,
                    "xL": np.log2(1 + lifespan), "xR": np.log2(1 + remaining), "trade_month": "2025-02",
                    "opening_seconds": float(opening), "endpoint_seconds": float(endpoint)})
    con.register("fixture_records", pa.Table.from_pylist(records))
    con.execute("CREATE TEMP TABLE analysis_base AS SELECT * FROM fixture_records")
    con.register("fixture_claims", pa.Table.from_pylist(claims))
    con.execute("CREATE TEMP TABLE claims AS SELECT * FROM fixture_claims")
    map_path = Path(folder) / "fixture_sports_map.parquet"
    pq.write_table(pa.Table.from_pylist(maps), map_path)
    binding = {"schema_version": "kaushik_replication_sports_binding_v1", "sports": list(driver.SPORTS),
        "provider_cohort_admitted": True, "retained_identity_result_timing_proof": True,
        "coverage_qualification": "tiny synthetic provider-covered cohort only",
        "inputs": [{"role": "sports_market_map", "path": str(map_path), "sha256": driver.inputs.sha256(map_path)}]}
    n = sum(not row["is_maker"] for row in records)
    manifest = {"rows": {"primary_taker": n, "baseline_all_roles": len(records), "source": len(records), "excluded": 0},
                "exclusions": {}, "metadata_health": {"synthetic": True},
                "support": [{"is_maker": False, "duration_tail_rows": n}]}
    return records, manifest, binding


class EndToEndFixtureTests(unittest.TestCase):
    def test_full_grid_common_samples_buy_partition_and_reopened_scores(self):
        with tempfile.TemporaryDirectory() as temporary:
            con = duckdb.connect()
            try:
                records, manifest, binding = fixture(con, temporary)
                stage = Path(temporary) / "out"
                result = driver.estimate_all(con, stage, manifest, binding)
                self.assertEqual(result["schema_version"], "kaushik_replication_estimates_v1")
                self.assertEqual(result["status"], "estimates_complete")
                json.dumps(result, allow_nan=False)
                self.assertEqual(len(result["table1"]["profile_rows"]), 20)
                self.assertEqual(len(result["table1"]["gap_rows"]), 2)
                self.assertEqual(len(result["table2"]), 5)
                self.assertEqual(len(result["table3"]["L_gt1"]), 3)
                self.assertEqual(len(result["table3"]["R_gt1"]), 3)
                common = {item["joint"]["metadata"]["n"] for item in result["table2"]}
                self.assertEqual(common, {manifest["rows"]["primary_taker"]})
                price_levels = result["table2"][4]["joint"]["metadata"]["effect_cardinality_by_tail"]
                self.assertEqual(price_levels["D1"]["price_code"],2)
                self.assertEqual(price_levels["D10"]["price_code"],2)
                for model in result["table2"]:
                    meta = model["joint"]["metadata"]
                    self.assertEqual(sum(item["rank"] for item in meta["residualized_continuous_design_by_tail"].values()),
                                     meta["residualized_design_rank"])
                    self.assertIn("unknown",meta["combined_absorbed_fixed_effect_rank"])
                    if model["spec"]["effects"]:
                        captured = meta["projection_cache"]
                        self.assertEqual(captured["groups"],meta["group_count"])
                        self.assertEqual(captured["observations"],meta["n"])
                        self.assertTrue(captured["released_before_full_moment_replay"])
                        self.assertLessEqual(captured["admitted_total_bytes"],driver.CAPS["numpy_memory_bytes"])
                reconciliation = result["sample"]["duration_reconciliation"]
                self.assertEqual(reconciliation["source"], reconciliation["group_cache"])
                self.assertTrue(reconciliation["matches_accepted_input_support"])
                self.assertEqual(result["appendix_a1"]["n_observations"], result["sample"]["duration_tail_rows"])
                for panel, key in (("L_gt1", "lifespan_gt1_rows"), ("R_gt1", "remaining_gt1_rows")):
                    self.assertTrue(all(item["n_observations"] == reconciliation["source"][key] for item in result["table3"][panel]))
                a2 = {item["convention"]: item for item in result["appendix_a2"]}
                self.assertEqual(a2["all_buy"]["counts"]["rows"], a2["maker_buy"]["counts"]["rows"] + a2["taker_buy"]["counts"]["rows"])
                self.assertEqual(a2["taker_direction"]["counts"]["rows"], manifest["rows"]["primary_taker"])
                self.assertEqual(len(result["sports"]["phase_rows"]), 60)
                self.assertEqual(len(result["sports"]["profile_rows"]), 400)
                self.assertEqual(len(result["sports"]["window_rows"]), 280)
                self.assertTrue(all(row["suppressed"] for row in result["sports"]["profile_rows"]))
                self.assertTrue(all(row["CR0"] is None for row in result["sports"]["profile_rows"]))
                self.assertEqual(len(result["sports"]["coverage"]), 9)
                self.assertEqual(result["appendix_a1"]["claim_support"]["claims"], 54)
                self.assertEqual(result["appendix_a1"]["claim_support"]["both_tail_claims"], 54)
                native = len({row["event_cluster"] for row in records if not row["is_maker"] and row["cluster_source"] == "native_event"})
                self.assertEqual(result["sample"]["unique_event_clusters"], native)
                self.assertEqual(result["sample"]["unique_event_clusters"] + result["sample"]["market_fallback_clusters"], result["sample"]["clusters"])
                self.assertEqual(sum(row["n_observations"] for row in result["categories"]), result["sample"]["rows"])
                self.assertTrue((stage / "sports_observations.parquet").is_file())
                for artifact in result["score_artifacts"].values():
                    path = stage / artifact["path"]
                    self.assertEqual(driver.inputs.sha256(path), artifact["sha256"])
                    self.assertEqual(pq.ParquetFile(path).metadata.num_rows, artifact["rows"])
                self.assertTrue(all("cluster_levels" not in model["joint"]["metadata"] for model in result["table2"]))
            finally:
                con.close()

    def test_sports_cache_preserves_records_clocks_and_excludes_after_endpoint(self):
        with tempfile.TemporaryDirectory() as temporary:
            con = duckdb.connect()
            try:
                _, _, binding = fixture(con, temporary)
                driver.prepare_sports_map(con, binding)
                con.execute("INSERT INTO analysis_base SELECT * REPLACE(1738368000+7201 AS timestamp) FROM analysis_base WHERE NOT is_maker LIMIT 1")
                expected = con.execute("SELECT count(*) FROM analysis_base a JOIN sports_market_map m USING(market_id) WHERE NOT a.is_maker AND a.timestamp<=m.actual_end_seconds").fetchone()[0]
                stage = Path(temporary) / "cache"
                stage.mkdir()
                ledger = driver.ReadLedger(ceiling=1_000_000)
                info = driver.create_sports_view(con, stage, ledger, base_bytes=10)
                self.assertEqual(info["reconciliation"]["admitted_rows"], expected)
                self.assertEqual(info["reconciliation"]["after_end_rows"], 1)
                self.assertEqual(info["raw_source_count"], expected + 1)
                self.assertEqual(ledger.scans["sports_observation_cache"]["charged_bytes"], 10)
                self.assertEqual(con.execute("SELECT count(*) FROM sports_base WHERE r<0").fetchone()[0], 0)
            finally:
                con.close()


class LeanProjectionFixtureTests(unittest.TestCase):
    def test_projection_capture_admission_ownership_and_source_count_gates(self):
        source = {"x":np.array([[1.,2.],[3.,4.],[5.,6.]]), "y":np.array([[7.],[8.],[9.]]),
                  "weights":np.array([2.,3.,4.]), "observation_counts":np.array([2,3,4]),
                  "codes":{"category":np.array([2,0,1])}}
        kwargs = {"expected_groups":3, "expected_observations":9, "column_count":2,
                  "target_count":1, "code_names":("category",), "persistent_bytes":128,
                  "batch_workspace_bytes":256}
        calls = []
        def replay():
            calls.append(True)
            yield source
        with patch.dict(driver.CAPS,{"batch_rows":3, "numpy_memory_bytes":100}):
            with self.assertRaisesRegex(ValueError,"before capture"):
                driver.capture_projection_batches(replay,**kwargs)
        self.assertEqual(calls,[])
        with patch.dict(driver.CAPS,{"batch_rows":3, "numpy_memory_bytes":100_000}):
            captured, stats = driver.capture_projection_batches(replay,**kwargs)
            self.assertEqual(stats["unique_owned_buffer_bytes"],3*8*(2+1+2+1))
            self.assertEqual(stats["groups"],3)
            self.assertEqual(stats["observations"],9)
            self.assertEqual(set(captured[0]),{"x","y","weights","observation_counts","codes"})
            for name in ("x","y","weights","observation_counts"):
                value = captured[0][name]
                np.testing.assert_array_equal(value,source[name])
                self.assertIsNone(value.base)
                self.assertTrue(value.flags.owndata and value.flags.c_contiguous)
                self.assertFalse(value.flags.writeable)
            np.testing.assert_array_equal(captured[0]["codes"]["category"],source["codes"]["category"])
            source["x"][0,0] = -100
            self.assertEqual(captured[0]["x"][0,0],1)
            with self.assertRaises(ValueError):
                captured[0]["codes"]["category"][0] = 1
            with self.assertRaises(TypeError):
                captured[0]["x"] = source["x"]
            with self.assertRaises(TypeError):
                captured[0]["codes"]["category"] = source["codes"]["category"]
            with self.assertRaisesRegex(ValueError,"source group/observation counts differ"):
                driver.capture_projection_batches(replay,**{**kwargs,"expected_observations":10})
            with self.assertRaisesRegex(ValueError,"exceeds source group/observation counts"):
                driver.capture_projection_batches(replay,**{**kwargs,"expected_observations":8})
            invalid = {**source,"x":np.full((3,2),np.nan)}
            with self.assertRaisesRegex(ValueError,"nonfinite"):
                driver.capture_projection_batches(lambda:iter([invalid]),**kwargs)

    def assert_replay_equivalent(self, con, query, spec, width, levels, *, claim_fe=False):
        def full():
            return driver.regression_batches(con, query, spec, width, claim_fe=claim_fe)
        def lean():
            return driver.regression_batches(con, query, spec, width, claim_fe=claim_fe, projection_only=True)
        left, right = list(full()), list(lean())
        self.assertEqual(len(left), len(right))
        for complete, projected in zip(left, right):
            self.assertEqual(set(projected), {"x", "y", "weights", "observation_counts", "codes"})
            for name in ("x", "y", "weights", "observation_counts"):
                np.testing.assert_array_equal(complete[name], projected[name])
            for name in levels:
                np.testing.assert_array_equal(complete["codes"][name], projected["codes"][name])
            self.assertIn("within_xx", complete)
            self.assertIn("within_xy", complete)
            self.assertIn("within_yy", complete)
            self.assertIn("clusters", complete)
        if not levels:
            return
        kwargs = {"term_names":tuple(f"t{i}" for i in range(left[0]["x"].shape[1])),
                  "target_names":driver.TARGETS, "level_counts":levels,
                  "tolerance":driver.CAPS["projection_tolerance"],
                  "max_iterations":2 if len(levels) == 1 else driver.CAPS["maximum_projection_iterations"]}
        full_progress, lean_progress = [], []
        original = driver.engine.absorb_categorical_effects(full, progress=full_progress.append, **kwargs)
        projected = driver.engine.absorb_categorical_effects(lean, progress=lean_progress.append, **kwargs)
        self.assertEqual(original.diagnostics(), projected.diagnostics())
        self.assertEqual(full_progress, lean_progress)
        for a, b in zip(original.effects, projected.effects):
            np.testing.assert_array_equal(a, b)
        for name in ("original_y_sum", "original_yty", "original_x_squared_norms"):
            np.testing.assert_array_equal(getattr(original, name), getattr(projected, name))

    def test_same_query_projection_preserves_mean_fields_all_five_specs_and_a1(self):
        with tempfile.TemporaryDirectory() as temporary:
            con = duckdb.connect()
            try:
                _, _, _ = fixture(con, temporary)
                stage = Path(temporary) / "cache"
                stage.mkdir()
                cache = driver.make_duration_cache(con, stage, driver.ReadLedger(), 0)
                for spec in driver.model_specs():
                    query, levels = driver.remap_effect_codes(con, "duration_groups", spec["effects"], "lean_test_"+str(spec["column"]))
                    with self.subTest(column=spec["column"]):
                        with patch.object(driver, "array_batches", wraps=driver.array_batches) as batches:
                            self.assert_replay_equivalent(con, query, spec, 4, levels)
                        for call in batches.call_args_list:
                            self.assertEqual(call.args[1], query)
                            selected = call.kwargs["columns"]
                            if selected is not None:
                                self.assertFalse(any(name.startswith("c") and name[1:2].isdigit() for name in selected))
                                self.assertNotIn("event_cluster", selected)
                con.execute("CREATE TEMP VIEW claim_features AS SELECT *,CASE WHEN bin=10 THEN 1 ELSE 0 END H FROM analysis_base WHERE NOT is_maker")
                claim_cache = driver.group_cache(con, "claim_features", stage/"lean_claim_groups.parquet",
                    ("event_cluster", "claim_code"), ("H", "H*xL", "xR", "H*xR", "payoff", "roi"))
                spec = {"clocks":["xL", "xR"], "effects":["claim_code"]}
                query, levels = driver.remap_effect_codes(con, "lean_claim_groups", spec["effects"], "lean_test_claim")
                self.assert_replay_equivalent(con, query, spec, claim_cache["feature_count"], levels, claim_fe=True)
            finally:
                con.close()

    def test_bounded_same_query_full_vs_lean_projection_benchmark(self):
        # Two complete repeats of a balanced three-FE projection. This is a
        # deterministic 131,072-group synthetic cache, not a real-data sample.
        with tempfile.TemporaryDirectory() as temporary:
            index = np.arange(131_072, dtype=np.int64)
            tail = index % 2
            n = 2 + index % 3
            features = np.column_stack((.2+(index%53)/53, .4+(index%71)/71,
                                        50*np.sin(index/17), 100*np.cos(index/29)))
            columns = {"event_cluster":[f"fixture:{i//64}" for i in index], "tail":tail,
                       "n_rows":n, "cat_code_active":(index//2)%8+8*tail,
                       "price_code_active":(index//16)%64+64*tail,
                       "month_code_active":(index//1024)%4+4*tail}
            columns.update({f"a{i}":features[:,i]*n for i in range(4)})
            centered = np.column_stack((.01+(index%11)/100, .02+(index%13)/100,
                                        .3+(index%17)/10, .5+(index%19)/10))
            columns.update({f"c{i}_{j}":centered[:,i]*centered[:,j]*(n-1)
                            for i in range(4) for j in range(i,4)})
            path = Path(temporary)/"synthetic_groups.parquet"
            pq.write_table(pa.table(columns), path, compression="zstd")
            con = duckdb.connect()
            try:
                con.execute("SET threads=4")
                con.execute("SET preserve_insertion_order=true")
                query = "SELECT * FROM read_parquet("+driver.inputs.literal(path)+")"
                spec = driver.model_specs()[4]
                levels = {"cat_code":16, "price_code":128, "month_code":8}
                kwargs = {"term_names":("D1:xL", "D1:xR", "D10:xL", "D10:xR"),
                          "target_names":driver.TARGETS, "level_counts":levels,
                          "tolerance":driver.CAPS["projection_tolerance"],
                          "max_iterations":driver.CAPS["maximum_projection_iterations"]}
                # Warm identical SQL and both conversions; then alternate modes
                # so disk-cache warming is not awarded solely to the lean path.
                for lean in (False, True):
                    list(driver.regression_batches(con, query, spec, 4, projection_only=lean))
                def projection():
                    return driver.regression_batches(con, query, spec, 4, projection_only=True)
                capture_begin = time.perf_counter()
                captured, capture_stats = driver.capture_projection_batches(projection,
                    expected_groups=len(index), expected_observations=int(n.sum()),
                    column_count=4, target_count=2, code_names=spec["effects"],
                    persistent_bytes=8*(sum(levels.values())*6+max(levels.values())*7),
                    batch_workspace_bytes=driver.CAPS["batch_rows"]*8*(4**2+4**2+4*2+30))
                capture_seconds = time.perf_counter()-capture_begin
                elapsed, fitted = {"full":[], "lean":[], "frozen":[]}, {"full":[], "lean":[], "frozen":[]}
                for mode in ("full", "lean", "frozen", "frozen", "lean", "full"):
                    begin = time.perf_counter()
                    result = driver.engine.absorb_categorical_effects(
                        (lambda:iter(captured)) if mode == "frozen" else
                        (lambda: driver.regression_batches(con, query, spec, 4, projection_only=mode == "lean")), **kwargs)
                    elapsed[mode].append(time.perf_counter()-begin)
                    fitted[mode].append(result)
                for result in fitted["full"]+fitted["lean"]+fitted["frozen"]:
                    self.assertEqual(result.diagnostics(), fitted["full"][0].diagnostics())
                    for a, b in zip(result.effects, fitted["full"][0].effects):
                        np.testing.assert_array_equal(a, b)
                evidence = {"benchmark":"same_query_lean_projection", "groups":len(index),
                    "parquet_bytes":path.stat().st_size, "full_seconds":elapsed["full"], "lean_seconds":elapsed["lean"],
                    "frozen_seconds":elapsed["frozen"], "capture_seconds":capture_seconds,
                    "full_median_seconds":float(np.median(elapsed["full"])),
                    "lean_median_seconds":float(np.median(elapsed["lean"])),
                    "frozen_median_seconds":float(np.median(elapsed["frozen"])), "capture_stats":capture_stats,
                    "diagnostics_exactly_equal":True, "effect_arrays_exactly_equal":True,
                    "projection_iterations":fitted["full"][0].iterations}
                captured.clear()
                print(json.dumps(evidence, sort_keys=True), flush=True)
            finally:
                con.close()


class AdmissionFixtureTests(unittest.TestCase):
    def test_memory_budget_arithmetic_configuration_and_admission_boundary(self):
        self.assertEqual(driver.CAPS["memory_limit"], "192GB")
        self.assertEqual(driver.CAPS["total_memory_bytes"], 192_000_000_000 + driver.CAPS["numpy_memory_bytes"])
        self.assertEqual(driver.CAPS["minimum_available_ram_bytes"] - driver.CAPS["total_memory_bytes"], 16_000_000_000)
        unchanged = {"numpy_memory_bytes":32_000_000_000, "threads":4, "spill_bytes":16_000_000_000,
                     "maximum_transient_file_bytes":16_000_000_000, "maximum_cache_bytes":3_000_000_000,
                     "maximum_output_bytes":8_000_000_000, "maximum_read_bytes":8_000_000_000_000,
                     "minimum_free_bytes":20_000_000_000}
        for key, value in unchanged.items():
            self.assertEqual(driver.CAPS[key], value)
        class RecordedConfiguration:
            def __init__(self):
                self.commands = []
            def execute(self, command):
                self.commands.append(command)
        con = RecordedConfiguration()
        driver.configure(con, "/tmp/synthetic-estimator-spill")
        self.assertIn("SET memory_limit='192GB'", con.commands)
        self.assertIn("SET max_temp_directory_size='16000000000B'", con.commands)
        roles = ("six_candidates", "six_timing", "six_proof", "six_tokens", "nfl_moneylines", "nba_moneylines", "mlb_phase")
        inventory = [{"role":role, "path":"/tmp/synthetic-" + role, "stat":{"bytes":0}} for role in roles]
        with patch("production_guard.require_production_host"), \
                patch.object(driver.sys, "executable", "/home/ubuntu/venv/bin/python"), \
                patch.object(driver, "source_snapshot", return_value={}), \
                patch.object(driver, "load_base", return_value=({}, {}, [], [])), \
                patch.object(driver.inputs, "read_json", return_value=({}, {})), \
                patch.object(driver, "sports_input_inventory", return_value=inventory), \
                patch.object(driver.inputs, "validate_destination"), \
                patch.object(driver.shutil, "disk_usage", return_value=type("Capacity", (), {"free":60_000_000_000})()), \
                patch.object(driver.resource, "getrlimit", return_value=(driver.resource.RLIM_INFINITY, driver.resource.RLIM_INFINITY)), \
                patch.object(driver.os, "cpu_count", return_value=4), \
                patch.object(Path, "read_text") as meminfo:
            meminfo.return_value = "MemAvailable: 234375000 kB\n"  # Exactly 240 decimal GB.
            admitted = driver.preflight("/tmp/synthetic-base", "", "", "/tmp/synthetic-binding", "", "/tmp/synthetic-run", "", sports_metadata_only=True)
            self.assertEqual(admitted["required_free_bytes"], 60_000_000_000)
            self.assertEqual(admitted["caps"]["minimum_available_ram_bytes"], 240_000_000_000)
            self.assertEqual(admitted["observed_available_ram_bytes"], 240_000_000_000)
            self.assertEqual(admitted["memory_budget_policy"], {
                "duckdb_memory_limit":"192GB", "duckdb_memory_bytes":192_000_000_000,
                "numpy_memory_bytes":32_000_000_000, "combined_memory_bytes":224_000_000_000,
                "minimum_available_ram_bytes":240_000_000_000, "minimum_headroom_bytes":16_000_000_000})
            meminfo.return_value = "MemAvailable: 234374999 kB\n"
            with self.assertRaisesRegex(ValueError, "available RAM insufficient"):
                driver.preflight("/tmp/synthetic-base", "", "", "/tmp/synthetic-binding", "", "/tmp/synthetic-run", "", sports_metadata_only=True)

    def test_process_write_limit_allows_bounded_spill_not_accepted_cache_overshoot(self):
        # The actual RLIMIT is process-wide: a synthetic COPY's spill write
        # reproduces the production failure independently of Parquet size.
        with tempfile.TemporaryDirectory() as temporary:
            stage = Path(temporary) / "stage"
            (stage / "spill").mkdir(parents=True)
            output = stage / "cache.parquet"
            previous = driver.resource.getrlimit(driver.resource.RLIMIT_FSIZE)
            def spill_write(path, count):
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    written = 0
                    while written < count:
                        written += os.write(descriptor,b"x"*(count-written))
                finally:
                    os.close(descriptor)
            with driver.inputs.copy_ceiling(4096):
                with self.assertRaises(OSError) as failure:
                    spill_write(Path(temporary) / "old-process-limit.tmp",8192)
                self.assertEqual(failure.exception.errno,errno.EFBIG)
            self.assertEqual(driver.resource.getrlimit(driver.resource.RLIMIT_FSIZE),previous)
            class SyntheticCopy:
                def execute(self, query):
                    self.observed_limit = driver.resource.getrlimit(driver.resource.RLIMIT_FSIZE)[0]
                    spill_write(stage / "spill" / "synthetic-duckdb.tmp",8192)
                    pq.write_table(pa.table({"fixture":[1]}),output)
                    return self
                def fetchone(self):
                    return (1,)
            con = SyntheticCopy()
            with patch.dict(driver.CAPS,{"maximum_transient_file_bytes":16384,"spill_bytes":16384,
                                         "maximum_output_bytes":65536,"minimum_free_bytes":0}):
                info = driver.copy_parquet(con,"synthetic",output,ceiling=4096)
                self.assertEqual(con.observed_limit,16384)
                self.assertLess(info["bytes"],4096)
                self.assertEqual((stage / "spill" / "synthetic-duckdb.tmp").stat().st_size,8192)
                with driver.inputs.copy_ceiling(driver.CAPS["maximum_transient_file_bytes"]):
                    with self.assertRaises(OSError) as failure:
                        spill_write(Path(temporary) / "hard-transient-limit.tmp",16385)
                    self.assertEqual(failure.exception.errno,errno.EFBIG)
            self.assertEqual(driver.resource.getrlimit(driver.resource.RLIMIT_FSIZE),previous)

    def test_post_copy_accepted_cache_overshoot_blocks_hash_and_preserves_stage(self):
        with tempfile.TemporaryDirectory() as temporary:
            stage = Path(temporary) / "stage"
            stage.mkdir()
            output = stage / "cache.parquet"
            con = duckdb.connect()
            try:
                with patch.dict(driver.CAPS,{"maximum_transient_file_bytes":16384,"spill_bytes":16384,
                                             "maximum_output_bytes":65536,"minimum_free_bytes":0}), \
                        patch.object(driver,"artifact_info") as inspect:
                    with self.assertRaisesRegex(ValueError,"accepted per-file ceiling"):
                        driver.copy_parquet(con,"SELECT 1 fixture",output,ceiling=100)
                    inspect.assert_not_called()
                self.assertTrue(stage.is_dir())
                self.assertTrue(output.is_file())
                self.assertGreater(output.stat().st_size,100)
                self.assertFalse((stage / "acceptance.json").exists())
            finally:
                con.close()

    def test_transient_disk_reservation_accounts_existing_bytes_and_retains_floor(self):
        with tempfile.TemporaryDirectory() as temporary:
            stage = Path(temporary)
            caps = {"maximum_output_bytes":8_000,"maximum_transient_file_bytes":16_000,
                    "spill_bytes":16_000,"minimum_free_bytes":20_000}
            with patch.dict(driver.CAPS,caps), patch.object(driver.shutil,"disk_usage") as usage:
                usage.return_value = type("Capacity",(),{"free":60_000})()
                driver.reserve_output(stage,3_000)
                usage.return_value.free = 59_999
                with self.assertRaisesRegex(ValueError,"free-space"):
                    driver.reserve_output(stage,3_000)
                (stage / "accepted.fixture").write_bytes(b"x"*2000)
                (stage / "spill").mkdir()
                (stage / "spill" / "occupied.tmp").write_bytes(b"x"*8192)
                item = (stage / "spill" / "occupied.tmp").stat()
                occupied = min(item.st_size,item.st_blocks*512)
                usage.return_value.free = 60_000 - 2000 - occupied
                driver.reserve_output(stage,3_000)
                usage.return_value.free -= 1
                with self.assertRaisesRegex(ValueError,"free-space"):
                    driver.reserve_output(stage,3_000)
                usage.return_value.free = 100_000
                with self.assertRaisesRegex(ValueError,"output capacity"):
                    driver.reserve_output(stage,6_001)

    def test_accepted_input_support_mismatch_fails_before_models(self):
        with tempfile.TemporaryDirectory() as temporary:
            con = duckdb.connect()
            try:
                _, manifest, _ = fixture(con, temporary)
                manifest["support"][0]["duration_tail_rows"] -= 1
                with self.assertRaisesRegex(ValueError, "independently accepted input support"):
                    driver.describe_sample(con, manifest, driver.ReadLedger(), 0)
            finally:
                con.close()

    def test_sports_cache_rejects_duplicate_archive_locators_before_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            con = duckdb.connect()
            try:
                _, _, binding = fixture(con, temporary)
                driver.prepare_sports_map(con, binding)
                con.execute("ALTER TABLE analysis_base ADD COLUMN source_month VARCHAR DEFAULT '2025-02'")
                con.execute("ALTER TABLE analysis_base ADD COLUMN source_ordinal BIGINT DEFAULT 1")
                stage = Path(temporary) / "cache"
                stage.mkdir()
                with self.assertRaisesRegex(ValueError, "locator missing or not unique"):
                    driver.create_sports_view(con, stage, driver.ReadLedger(), 10)
                self.assertFalse((stage / "sports_observations.parquet").exists())
            finally:
                con.close()

    def test_metadata_stage_reads_only_claims_and_requires_bound_review_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            base = folder / "base"
            base.mkdir()
            con = duckdb.connect()
            try:
                fixture(con, base)
                pq.write_table(con.execute("SELECT * FROM claims").fetch_arrow_table(), base / "claims.parquet")
            finally:
                con.close()
            start = datetime(2025, 2, 1, tzinfo=timezone.utc)
            end = datetime(2025, 2, 1, 2, tzinfo=timezone.utc)
            clock = pa.timestamp("us", tz="UTC")
            candidates = pa.table({"market_id": ["m0"], "sport": ["epl"], "event_slug": ["fixture-epl"]})
            timing = pa.Table.from_pylist([{"sport": "epl", "event_slug": "fixture-epl", "game_id": "1",
                "actual_start_utc": start, "actual_end_utc": end, "timing_quality": "fixture"}], schema=pa.schema([
                ("sport",pa.string()),("event_slug",pa.string()),("game_id",pa.string()),
                ("actual_start_utc",clock),("actual_end_utc",clock),("timing_quality",pa.string())]))
            proof = pa.table({"sport":["epl"],"event_slug":["fixture-epl"],"eligible":[True],
                              "match_exclusion_reason":pa.array([None],type=pa.string()),
                              "timing_exclusion_reason":pa.array([None],type=pa.string())})
            tokens = pa.table({"market_id":["m0","m0"],"token_id":["t0","t1"],
                               "outcome":["label0","label1"],"won":[False,True]})
            legacy = pa.schema([("market_id",pa.string()),("game_id",pa.string()),
                ("winning_token_id",pa.string()),("actual_start_utc",clock),("actual_end_utc",clock)])
            mlb = pa.schema([("market_id",pa.string()),("game_pk",pa.int64()),
                ("winning_token_id",pa.string()),("actual_start_utc",clock),("actual_end_utc",clock)])
            role_tables = {"six_candidates":candidates,"six_timing":timing,"six_proof":proof,"six_tokens":tokens,
                           "nfl_moneylines":pa.Table.from_pylist([],schema=legacy),
                           "nba_moneylines":pa.Table.from_pylist([],schema=legacy),"mlb_phase":pa.Table.from_pylist([],schema=mlb)}
            bound = []
            for role, table in role_tables.items():
                path = folder / (role + ".parquet")
                pq.write_table(table,path)
                bound.append({"role":role,"path":str(path),"sha256":driver.inputs.sha256(path)})
            binding = {"schema_version":"kaushik_replication_sports_binding_v1","sports":list(driver.SPORTS),
                "provider_cohort_admitted":True,"retained_identity_result_timing_proof":True,
                "coverage_qualification":"synthetic only","inputs":bound}
            json_ids = {}
            for key, value in (("manifest",{"fixture":True}),("acceptance",{"fixture":True}),("sports",binding)):
                path = folder / (key + ".json")
                driver.inputs.write_json(path,value)
                json_ids[key] = driver.inputs.read_json(path)[1]
            claims = {**driver.parquet_footer(base / "claims.parquet"),"relative_path":"claims.parquet",
                      "expected_sha256":driver.inputs.sha256(base / "claims.parquet")}
            source = {"head":"0"*40,"files":{}}
            fresh = {"schema_version":"kaushik_replication_estimate_preflight_v1","status":"preflight_complete",
                "target":str(folder / "metadata"),"source":source,"base_dir":str(base),
                "base_binding":{"manifest":json_ids["manifest"],"acceptance":json_ids["acceptance"]},
                "base_manifest":{},"base_files":[claims],"monthly_paths":[],"sports_binding":binding,
                "sports_binding_identity":json_ids["sports"],"sports_files":driver.sports_input_inventory(binding),"caps":driver.CAPS,
                "required_free_bytes":0,"write_limit_policy":{},"memory_budget_policy":{},"mode":"sports_metadata_only","sports_metadata_review":None,"observed_free_bytes":2}
            reviewed = {**fresh,"observed_free_bytes":1}
            driver.inputs.write_json(folder / "reviewed_preflight.json",reviewed)
            reviewed_id = driver.inputs.read_json(folder / "reviewed_preflight.json")[1]
            with patch("production_guard.require_production_host"), patch.object(driver.inputs,"validate_destination"), \
                    patch.object(driver,"source_snapshot",return_value=source):
                manifest = driver.run_sports_metadata_stage(reviewed,fresh,["synthetic fixture"],reviewed_identity=reviewed_id)
            self.assertFalse(manifest["trade_bodies_read"])
            self.assertEqual(manifest["coverage"][0]["markets"],1)
            self.assertEqual(set(manifest["outputs"]),{"sports_market_map.parquet","sports_metadata_exclusions.parquet"})
            target = Path(fresh["target"])
            binding["reviewed_metadata_stage"] = {"reviewed":True,"manifest_path":str(target / "manifest.json"),
                "manifest_sha256":driver.inputs.sha256(target / "manifest.json"),"acceptance_path":str(target / "acceptance.json"),
                "acceptance_sha256":driver.inputs.sha256(target / "acceptance.json")}
            review, files = driver.reviewed_sports_metadata(binding,fresh["base_binding"])
            self.assertEqual(review["source"],source)
            self.assertEqual(len(files),2)
            with self.assertRaisesRegex(ValueError,"input/cohort drift"):
                driver.reviewed_sports_metadata(binding,{"different":True})
            binding["reviewed_metadata_stage"]["acceptance_sha256"] = "0"*64
            with self.assertRaisesRegex(ValueError,"hash binding differs"):
                driver.reviewed_sports_metadata(binding,fresh["base_binding"])

    def test_read_pass_budget_and_output_are_bounded_before_work(self):
        ledger = driver.ReadLedger(ceiling=10)
        ledger.charge(6, "first")
        with self.assertRaisesRegex(ValueError, "before second"):
            ledger.charge(5, "second")
        self.assertEqual(ledger.charged_bytes, 6)
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(driver.shutil, "disk_usage", return_value=type("Capacity", (), {"free": 1})()):
                with self.assertRaisesRegex(ValueError, "free-space"):
                    driver.reserve_output(Path(temporary), 1)

    def test_entrypoint_stops_at_production_guard(self):
        args = ["--base-dir", "/tmp/none", "--base-manifest-sha256", "0" * 64,
                "--base-acceptance-sha256", "0" * 64, "--sports-binding", "/tmp/none.json",
                "--sports-binding-sha256", "0" * 64, "--expected-head", "0" * 40, "--run-dir", "/tmp/new"]
        with patch("production_guard.require_production_host", side_effect=RuntimeError("fixture host blocked")):
            with self.assertRaisesRegex(RuntimeError, "fixture host blocked"):
                cli.main(args)


if __name__ == "__main__":
    unittest.main()
