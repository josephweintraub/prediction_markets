"""Small saved-artifact, comparison, and fixed-resource controls; no production reads."""
from __future__ import annotations

from copy import deepcopy
from datetime import date
import inspect
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import numpy as np
from analysis.sports_game_dynamics.artifacts import write_parquet
from scripts import estimate_sports_wallet_adoption as adoption


def row(schema, **values):
    return {**{name: None for name, _ in schema}, **values}


def interval(estimate):
    return dict(estimate=estimate, standard_error=.001, t_statistic=1., p_value=.3,
                ci95_low=estimate-1.96*.001, ci95_high=estimate+1.96*.001)


def synthetic_run(offset=0., trade_sample="filtered_trades"):
    manifest = {"schema_version": 3, "stage": "multisport_flb_time_regressions_v3",
                "sports": list(adoption.engine.SPORTS), "support_floor": 500,
                "trade_sample": trade_sample, "observation_counts": {sport: 4500 for sport in adoption.engine.SPORTS}}
    tables = {name: [] for name in adoption.OUTPUT_SCHEMAS}
    support_schema = adoption.OUTPUT_SCHEMAS["support.parquet"]
    for sample, norm in (("all_pregame_live", "realized_duration"), ("live_only", "realized_duration"),
                         ("bounded_pregame", "realized_duration"), ("all_pregame_live", "sport_median_duration")):
        for sport in adoption.engine.SPORTS:
            for segment in (("live",) if sample == "live_only" else ("pregame", "live")):
                for tail in ("D1", "D10"):
                    tables["support.parquet"].append(row(support_schema, sample=sample, time_normalization=norm,
                        sport=sport, segment=segment, tail=tail, n_obs=1000, n_events=20))
    for model_id, spec in adoption.model_grid().items():
        model = {**spec, "sport": spec.get("sport", "+".join(adoption.engine.SPORTS))}
        active = adoption.engine.SPORTS if model["sport"] == "all" else model["sport"].split("+")
        n_obs = (4500 if model["family"] == "continuous_price" else 2000 if model["sample"] == "live_only" else 4000)*len(active)
        _, low, high = adoption.engine._sample_clause(model["sample"], "realized_time")
        features = adoption.features_for(model)
        model = row(adoption.engine.MODEL_SCHEMA, **model, model_id=model_id, window_low=low, window_high=high,
                    n_obs=n_obs, n_events=20, n_days=10, n_wallets=30, n_event_clusters=20,
                    n_parameters=len(features), rank=len(features), r_squared=.2, condition_number=10.,
                    suppressed=False, status="reported")
        tables["model_summary.parquet"].append(model)
        coefficients = {}
        for order, (term, _) in enumerate(features, 1):
            value = order*.001+offset
            coefficients[term] = value
            tables["coefficients.parquet"].append(row(adoption.engine.COEFFICIENT_SCHEMA,
                **{field: model[field] for field in ("family", "scope", "sport", "sample", "time_normalization", "weighting", "adjustment")},
                model_id=model_id, term_order=order, term=term, **interval(value)))
        names = (("pregame_tail_spread_change", "live_tail_spread_change") if model["family"] == "tail_piecewise" else
                 ("price_gradient_time_slope",) if model["family"] == "continuous_price" else
                 ("equal_weight_mean_sport_tail_slope",) if model["adjustment"] == "fully_interacted" else
                 ("tail_spread_time_slope",))
        for name in names:
            if name == "equal_weight_mean_sport_tail_slope":
                contrast = adoption.engine._mean_sport_slope_contrast(features, active)
                estimate = float(contrast @ np.array([coefficients[term] for term, _ in features]))
            else:
                term = {"tail_spread_time_slope": "D10 x time", "price_gradient_time_slope": "Price x time",
                        "pregame_tail_spread_change": "D10 x pregame time", "live_tail_spread_change": "D10 x live time"}[name]
                estimate = coefficients[term]
            tables["estimands.parquet"].append(row(adoption.engine.ESTIMAND_SCHEMA,
                **{field: model[field] for field in ("family", "scope", "sport", "sample", "time_normalization", "weighting", "adjustment")},
                estimand_id=f"{model_id}:{name}", source_model_id=model_id, estimand=name,
                n_obs=n_obs, n_events=20, suppressed=False, status="reported", **interval(estimate)))
    for sport in adoption.engine.SPORTS:
        tables["duration_reference.parquet"].append(row(adoption.engine.DURATION_SCHEMA,
            sport=sport, event_count=20, median_duration_seconds=3600., median_duration_minutes=60.))
        for tail in ("D1", "D10"):
            tables["pregame_time_distribution.parquet"].append(row(adoption.engine.PREGAME_TIME_SCHEMA,
                sport=sport, tail=tail, n_obs=1000, n_events=20, minimum=-1., p01=-.9,
                p05=-.8, p25=-.6, median=-.4, p75=-.2))
    specifications = (("pooled", "all", "equal_fill"), ("pooled", "all", "equal_sport"),
                      *(("sport", sport, "equal_fill") for sport in adoption.engine.SPORTS))
    for scope, sport, weighting in specifications:
        for index in range(1, 11):
            n = 900 if scope == "pooled" else 100
            suppressed = n < 500
            values = {} if suppressed else dict(d1_mean_calibration=.01, d10_mean_calibration=.03,
                spread_d10_minus_d1=.02, spread_standard_error=.001,
                spread_ci95_low=.02-1.96*.001, spread_ci95_high=.02+1.96*.001)
            tables["time_bin_spreads.parquet"].append(row(adoption.engine.TIME_BIN_SCHEMA,
                scope=scope, sport=sport, weighting=weighting, time_bin=index, time_low=(index-1)/10,
                time_high=index/10, d1_n=n, d10_n=n, d1_events=20, d10_events=20,
                suppressed=suppressed, status="withheld_tail_n_lt_500" if suppressed else "reported", **values))
        for phase in ("pregame", "live"):
            for index in range(1 if phase == "pregame" else 101):
                tables["kernel_time_spreads.parquet"].append(row(adoption.engine.KERNEL_SCHEMA,
                    scope=scope, sport=sport, weighting=weighting, phase=phase, kernel="epanechnikov",
                    bandwidth=adoption.engine.KERNEL_BANDWIDTHS[phase], grid_index=index,
                    time_value=0. if phase == "pregame" else index/100,
                    d1_n=0, d10_n=0, d1_events=0, d10_events=0, suppressed=True,
                    status="withheld_tail_n_lt_500"))
    return {"manifest": manifest, "tables": tables,
            "manifest_fingerprint": {"path": "/synthetic/manifest.json", "bytes": 10, "sha256": "0"*64}}


def synthetic_comparison():
    runs = {name: synthetic_run(index*.001, "all_trades" if name == "restored_all" else "filtered_trades")
            for index, name in enumerate(adoption.CASES)}
    flags = {"builds": {name: {"rows": {"admitted_rows": 100}, "flags":
        {"wallets": 10, "nonhuman_wallets": 2, "nonhuman_trades": 20}} for name in ("legacy", "corrected")},
        "comparisons": [{"comparison": "corrected_vs_legacy_recomputed", "left_only_wallets": 1,
            "right_only_wallets": 1, "criteria": [{"criterion": "is_nonhuman", "common_enter": 1, "common_exit": 1}]}]}
    samples = {"scenarios": {}, "historical_estimate_reproduction": True,
               "gates": {"all_trade_regime_invariant": True, "historical_observation_reproduction": True}}
    return adoption.build_comparison(runs, flags, samples)


def save_run(folder: Path, value: dict):
    folder.mkdir(parents=True)
    outputs = {}
    for name, schema in adoption.OUTPUT_SCHEMAS.items():
        rows = [tuple(item[field] for field, _ in schema) for item in value["tables"][name]]
        write_parquet(folder/name, schema, rows, adoption.UNIQUE_KEYS[name])
        outputs[name] = {**adoption.fingerprint(folder/name), "path": name}
    value["manifest"]["outputs"] = outputs
    value["manifest"]["execution_resources"] = {"memory_limit": "96GB", "threads": 8,
        "max_temp_directory_size": "4000000000B", "timezone": "UTC", "temp_directory": str(folder/"duckdb_tmp"),
        "disabled_optimizers": ["common_subplan"]}
    adoption.write_json(folder/"manifest.json", value["manifest"])
    return adoption.fingerprint(folder/"manifest.json")


def sample_controls(folder: Path):
    """Tiny metadata fixture; no raw trade or production admission bypass."""
    head = "a"*40
    base = {}
    for name in adoption.INPUT_NAMES:
        path = folder/(name+".parquet")
        path.write_bytes(name.encode())
        base[name] = adoption.fingerprint(path)
    regime_flags = {}
    for name in ("historical", "legacy_recomputed_flags", "wallet_flags"):
        path = folder/(name+".parquet")
        path.write_bytes(name.encode())
        regime_flags[name] = adoption.fingerprint(path)
    source = {"head": head, "files": {"analysis/multisport_game_dynamics/estimate_flb_decay.py":
              {"path": "/fixture/estimator.py", "bytes": 10, "sha256": "b"*64}}}
    scenarios = {}
    for name in ("old_H", "restored_H", "restored_F0", "restored_F1"):
        scenarios[name] = {"counts": [{"sport": sport, "raw": 4500, "outside_cohort": 0,
            "post_end": 0, "invalid_price": 0, "all": 4500, "extreme": 0, "flagged_interior": 0,
            "filtered": 4500, "missing_flag_all": 0, "null_flag_all": 0, "pregame_all": 1000,
            "end_equal_all": 10} for sport in adoption.engine.SPORTS]}
    cases = {}
    for name, regime in zip(adoption.CASES,
            ("historical", "historical", "legacy_recomputed_flags", "wallet_flags", "wallet_flags")):
        cases[name] = {"engine_inputs": {**base, "wallet_flags": regime_flags[regime]},
            "trade_sample": "all_trades" if name == "restored_all" else "filtered_trades",
            "observation_counts": {sport: 4500 for sport in adoption.engine.SPORTS}}
    flags = {"schema_version": "polymarket_wallet_flags_rebuild_v1", "status": "wallet_flags_rebuild_complete",
        "data_certified": False, "downstream_adoption": "pending", "source": {"head": head},
        "reconciliation": {"exact_source_wallet_keys_and_counts": True},
        "contract": {"timestamp_lower_inclusive": 1590969600, "sides": "all published sides",
            "timezone": "UTC", "classifier_sha256": adoption.CLASSIFIER_SHA256},
        "binding": {"repair_manifest": {"sha256": adoption.REPAIR_SHA256}, "repair_qa": {"sha256": adoption.REPAIR_QA_SHA256}},
        "outputs": [regime_flags[name] for name in ("legacy_recomputed_flags", "wallet_flags")],
        "inputs": {"historical_flags": {"pipeline_data": {"stat": {"bytes": regime_flags["historical"]["bytes"]},
            "expected_content_sha256": regime_flags["historical"]["sha256"]}}}}
    flag_path = folder/"flags.json"
    adoption.write_json(flag_path, flags)
    samples = {"schema_version": 1, "status": "sports_wallet_samples_complete", "data_certified": False,
        "source": {**source, "expected_head": head}, "flag_manifest": adoption.fingerprint(flag_path),
        "cases": cases, "scenarios": scenarios,
        "gates": {"all_trade_regime_invariant": True, "historical_observation_reproduction": True},
        "reconciliation": {name: True for name in ("historical_H_labels_reproduced", "all_nonflag_payloads_bit_exact",
            "native_ids_unique", "saved_flags_rejoined", "normalizer_oracle_equal", "filtered_subset_of_all",
            "disjoint_exclusions", "all_support_invariant", "input_and_source_freeze")}}
    sample_path, historic_path = folder/"samples.json", folder/"historic.json"
    adoption.write_json(sample_path, samples)
    adoption.write_json(historic_path, {"observation_counts": cases["historic_filtered"]["observation_counts"],
        "inputs": {"input_09": regime_flags["historical"]}})
    frozen_historic = adoption.fingerprint(historic_path)
    samples["inputs"] = {"sep20_filtered": {"path": frozen_historic["path"], "sha256": frozen_historic["sha256"],
        "stat_before": {"bytes": frozen_historic["bytes"]}}}
    adoption.write_json(sample_path, samples)
    return sample_path, flag_path, historic_path, samples, flags, source


class AdoptionEstimateTests(unittest.TestCase):
    def test_complete_model_table_grid_and_support(self):
        value = synthetic_run()
        adoption.validate_tables(value["tables"], value["manifest"])
        self.assertEqual(len(value["tables"]["model_summary.parquet"]), 82)
        self.assertEqual(len(value["tables"]["estimands.parquet"]), 93)

    def test_preflight_binds_target_source_generation_and_estimator(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            sample_path, flag_path, historic_path, samples, flags, source = sample_controls(folder)
            target = folder/"estimates"
            with patch.object(adoption, "committed_source", return_value=source), \
                 patch.object(adoption, "free_bytes", return_value=100_000_000_000):
                reviewed = adoption.preflight(sample_path, flag_path, historic_path, target, source["head"])
                self.assertEqual(reviewed["target"], str(target.resolve()))
                with self.assertRaisesRegex(adoption.AdoptionBlocked, "Reviewed preflight differs: target"):
                    adoption.body(sample_path, flag_path, historic_path, folder/"other", source["head"], reviewed)
                historic = adoption.read_json(historic_path)
                historic["changed_estimates"] = "same counts and flag source, different archive"
                adoption.write_json(historic_path, historic)
                with self.assertRaisesRegex(adoption.AdoptionBlocked, "frozen sample evidence binding"):
                    adoption.preflight(sample_path, flag_path, historic_path, target, source["head"])
                del historic["changed_estimates"]
                adoption.write_json(historic_path, historic)
                samples["source"]["head"] = "c"*40
                adoption.write_json(sample_path, samples)
                with self.assertRaisesRegex(adoption.AdoptionBlocked, "generation"):
                    adoption.preflight(sample_path, flag_path, historic_path, target, source["head"])
                samples["source"]["head"] = source["head"]
                samples["source"]["files"] = {}
                adoption.write_json(sample_path, samples)
                with self.assertRaisesRegex(adoption.AdoptionBlocked, "scientific estimator binding"):
                    adoption.preflight(sample_path, flag_path, historic_path, target, source["head"])
            flags["reconciliation"]["exact_source_wallet_keys_and_counts"] = False
            with self.assertRaisesRegex(adoption.AdoptionBlocked, "wallet reconciliation"):
                adoption.sample_contract(samples, flags)

    def test_missing_model_and_live_bin_counts_fail_closed(self):
        value = synthetic_run()
        value["tables"]["model_summary.parquet"].pop()
        with self.assertRaisesRegex(adoption.AdoptionBlocked, "model grid"):
            adoption.validate_tables(value["tables"], value["manifest"])
        value = synthetic_run()
        next(row for row in value["tables"]["time_bin_spreads.parquet"] if row["scope"] == "sport")["d1_n"] += 1
        with self.assertRaisesRegex(adoption.AdoptionBlocked, "support reconciliation"):
            adoption.validate_tables(value["tables"], value["manifest"])

    def test_interval_and_estimand_coefficient_reconciliation(self):
        value = synthetic_run()
        value["tables"]["estimands.parquet"][0]["ci95_low"] += .1
        with self.assertRaisesRegex(adoption.AdoptionBlocked, "Interval"):
            adoption.validate_tables(value["tables"], value["manifest"])
        value = synthetic_run()
        value["tables"]["estimands.parquet"][0].update(interval(.9))
        with self.assertRaisesRegex(adoption.AdoptionBlocked, "contrast differs"):
            adoption.validate_tables(value["tables"], value["manifest"])

    def test_saved_output_hash_schema_and_counts_reopened(self):
        value = synthetic_run()
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            outputs = {}
            for name, schema in adoption.OUTPUT_SCHEMAS.items():
                rows = [tuple(item[field] for field, _ in schema) for item in value["tables"][name]]
                write_parquet(folder/name, schema, rows, adoption.UNIQUE_KEYS[name])
                outputs[name] = {**adoption.fingerprint(folder/name), "path": name}
            value["manifest"]["outputs"] = outputs
            adoption.write_json(folder/"manifest.json", value["manifest"])
            loaded = adoption.load_estimates(folder/"manifest.json", expected_counts=value["manifest"]["observation_counts"])
            self.assertEqual(len(loaded["tables"]["estimands.parquet"]), 93)
            value["manifest"]["outputs"]["estimands.parquet"]["sha256"] = "0"*64
            adoption.write_json(folder/"manifest.json", value["manifest"])
            with self.assertRaisesRegex(adoption.AdoptionBlocked, "hash differs"):
                adoption.load_estimates(folder/"manifest.json")

    def test_difference_composition_preserves_uncertainty(self):
        value = synthetic_comparison()
        self.assertTrue(all(item["supported"] for item in value["decomposition"]))
        item = value["decomposition"][0]
        self.assertAlmostEqual(item["total_adoption"], item["mlb_coverage"]+item["historical_flag_gap"]+item["wallet_identity_repair"])
        self.assertTrue(all(item["change_uncertainty"] == "not estimated; cross-run covariance unavailable" for item in value["changes"]))

    def test_suppression_or_population_change_withholds_difference(self):
        rows = synthetic_run()["tables"]["estimands.parquet"][:1]
        changed = deepcopy(rows)
        changed[0].update(suppressed=True, estimate=None, standard_error=None, ci95_low=None, ci95_high=None)
        result = adoption.compare_estimands(rows, changed, "repair")[0]
        self.assertEqual(result["status"], "suppression_transition")
        self.assertIsNone(result["change"])
        changed = deepcopy(rows)
        changed[0]["sport"] = "different-supported-sports"
        result = adoption.compare_estimands(rows, changed, "repair")[0]
        self.assertEqual(result["status"], "supported_population_changed")
        self.assertIsNone(result["change"])

    def test_historical_reproduction_tolerates_only_small_numeric_reductions(self):
        archived = synthetic_run()
        reproduced = deepcopy(archived)
        reproduced["tables"]["coefficients.parquet"][0]["estimate"] += 1e-12
        result = adoption.verify_historical_reproduction(archived, reproduced)
        self.assertTrue(result["verified"])
        self.assertEqual(set(result["tables"]), set(adoption.OUTPUT_SCHEMAS))
        reproduced["tables"]["coefficients.parquet"][0]["standard_error"] += .01
        with self.assertRaisesRegex(adoption.AdoptionBlocked, "Historical numeric reproduction"):
            adoption.verify_historical_reproduction(archived, reproduced)
        reproduced = deepcopy(archived)
        reproduced["tables"]["model_summary.parquet"][0]["rank"] -= 1
        with self.assertRaisesRegex(adoption.AdoptionBlocked, "definition/count/status"):
            adoption.verify_historical_reproduction(archived, reproduced)

    def test_atomic_publish_never_replaces_existing_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)/"existing"
            target.mkdir()
            (target/"keep").write_text("original")
            staging = Path(directory)/"staging"
            staging.mkdir()
            with self.assertRaises(OSError):
                adoption.atomic_publish(staging, target)
            self.assertEqual((target/"keep").read_text(), "original")

    def test_engine_fixed_resources_compatible_api_and_optimizer_preservation(self):
        class StopBeforeScience(Exception):
            pass
        class Connection:
            def __init__(self, available):
                self.available, self.statements, self.result = available, [], None
            def execute(self, sql, parameters=None):
                self.statements.append((sql, parameters))
                if "current_setting" in sql:
                    self.result = ("filter_pushdown",)
                elif "duckdb_optimizers" in sql:
                    self.result = (1,) if self.available else None
                return self
            def fetchone(self):
                return self.result
            def close(self):
                pass
        signature = inspect.signature(adoption.engine.estimate_flb_decay)
        self.assertNotIn("duckdb_config", signature.parameters)
        for available in (True, False):
            with self.subTest(common_subplan_available=available), tempfile.TemporaryDirectory() as directory:
                folder = Path(directory)
                paths = {}
                for name in adoption.INPUT_NAMES:
                    path = folder/(name+".parquet")
                    path.write_bytes(b"fixture")
                    paths[name] = path
                connection = Connection(available)
                with patch.object(adoption.engine.duckdb, "connect", return_value=connection) as connect, \
                     patch.object(adoption.engine, "_create_exact_observations", side_effect=StopBeforeScience):
                    with self.assertRaises(StopBeforeScience):
                        adoption.engine.estimate_flb_decay(**paths, run_dir=folder/"run")
                config = connect.call_args.kwargs["config"]
                self.assertEqual({key: config[key] for key in ("memory_limit", "threads", "max_temp_directory_size")},
                                 {"memory_limit": "96GB", "threads": 8, "max_temp_directory_size": "4000000000B"})
                self.assertTrue(config["temp_directory"].endswith("/duckdb_tmp"))
                settings = [parameters for sql, parameters in connection.statements if "SET disabled_optimizers" in sql]
                self.assertEqual(settings, [["filter_pushdown,common_subplan"]] if available else [])
                self.assertIn(("SET TimeZone='UTC'", None), connection.statements)

    def test_joint_tail_covariance_retained(self):
        con = duckdb.connect()
        try:
            con.execute("CREATE TABLE weighted_observations(sport VARCHAR,event_cluster VARCHAR,trade_day DATE,proxyWallet VARCHAR,price_decile INTEGER,calibration_error DOUBLE,realized_time DOUBLE)")
            rows = []
            for index in range(500):
                residual = -.04 if index % 2 == 0 else .04
                rows.extend([("mlb", f"event-{index%10}", date(2025,1,index%5+1), f"w{index%20}", tail,
                              residual+(0.02 if tail == 10 else 0.), .55) for tail in (1,10)])
            con.executemany("INSERT INTO weighted_observations VALUES (?,?,?,?,?,?,?)", rows)
            output = adoption.engine._time_bin_rows(con)
            target = next(item for item in output if item[0:4] == ("sport", "mlb", "equal_fill", 6))
            self.assertAlmostEqual(target[12], .02)
            self.assertLess(abs(target[13]), 1e-9)
            # Both tail means vary by shared wallet/event; their equal residuals cancel in the joint spread score.
            self.assertGreater(sum((row[5]-.0)**2 for row in rows if row[4] == 1), 0.)
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
