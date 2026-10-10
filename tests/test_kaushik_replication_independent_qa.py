"""Independent tiny numerical oracles; no production data or remote execution."""
from __future__ import annotations

import json
import contextlib
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import io
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_polymarket_replication import build_inputs
from analysis.kaushik_polymarket_replication import run_estimates as driver
from analysis.kaushik_polymarket_replication import render_report as report

from analysis.kaushik_polymarket_replication.estimators import (
    ClusterScoreMoments, RegressionMoments, a1_claim_design,
    absorb_categorical_effects, contrast_vector, finalize_clustered_regression,
    fit_ols_moments, fixed_probability_bins,
    grouped_r_squared_from_sufficient_statistics, joint_clustered_means_from_totals,
    joint_tail_phase_contrasts, paper_time_controls, payoff_and_roi,
    tail_varying_design,
)


def replay(x, y, codes=None, size=43):
    def source():
        for begin in range(0, len(x), size):
            yield {"x": x[begin:begin + size], "y": y[begin:begin + size],
                   "codes": {name: value[begin:begin + size]
                             for name, value in (codes or {}).items()}}
    return source


def streamed_fit(x, y, names, targets, events, codes=None, max_iterations=1000):
    source = replay(x, y, codes)
    absorber = (absorb_categorical_effects(source, term_names=names,
                target_names=targets,
                level_counts={name: int(code.max()) + 1 for name, code in codes.items()},
                max_iterations=max_iterations) if codes else None)
    moments = RegressionMoments(names, targets)
    for batch in source():
        values = absorber.transform(batch) if absorber else (batch["x"], batch["y"], None)
        moments.add(*values)
    fitted = fit_ols_moments(moments, absorption=absorber)
    scores = ClusterScoreMoments(len(names) * len(targets))
    if fitted.beta is not None:
        begin = 0
        for batch in source():
            values = absorber.transform(batch) if absorber else (batch["x"], batch["y"], None)
            n = len(batch["x"])
            scores.add(events[begin:begin + n], fitted.score_batch(*values))
            begin += n
    return fitted, scores, absorber


class IndependentNumericalQA(unittest.TestCase):
    def test_exact_binary64_tail_fe_matches_full_dense_ols_and_sandwich(self):
        rng = np.random.default_rng(88183)
        n = 640
        prices = np.array([.04, np.nextafter(.04, 1), .065, .085,
                           .92, np.nextafter(.92, 1), .95, .975])
        price = np.tile(prices, n // len(prices))
        h = (price >= .9).astype(int)
        # Adjacent representable prices must remain different categorical levels.
        levels, exact_code = np.unique(price, return_inverse=True)
        self.assertEqual(len(levels), 8)
        self.assertEqual(len(np.unique(np.round(price, 2))), 6)
        category, month = rng.integers(0, 3, n), rng.integers(0, 4, n)
        codes = {"category_tail": category + 3 * h, "exact_price_tail": exact_code,
                 "month_tail": month + 4 * h}
        controls = np.column_stack((rng.uniform(.4, 4, n), rng.uniform(.1, .4, n)))
        x, names = tail_varying_design(controls, ("xL", "xR"), h, intercept=False)
        # Both outcomes are constructed at the row level, before any mean.
        outcome = rng.integers(0, 2, n)
        y = np.column_stack(payoff_and_roi(price, outcome))
        targets = ("payoff_cents", "roi_percent")
        events = np.repeat(np.arange(40), 16)
        fit, scores, absorber = streamed_fit(x, y, names, targets, events, codes)
        self.assertTrue(absorber.converged)
        self.assertEqual(fit.rank, 4)
        result = finalize_clustered_regression(fit, scores)
        z = np.column_stack([np.eye(int(code.max()) + 1)[code] for code in codes.values()])
        dense = np.column_stack((x, z))
        beta = np.linalg.lstsq(dense, y, rcond=None)[0]
        residue = y - dense @ beta
        bread = np.linalg.pinv(dense.T @ dense, rcond=1e-13)
        row_score = (residue[:, :, None] * dense[:, None, :]).reshape(n, -1)
        grouped_score = np.array([row_score[events == event].sum(axis=0) for event in range(40)])
        dense_influence = grouped_score @ np.kron(np.eye(2), bread)
        selected = np.concatenate((np.arange(4), dense.shape[1] + np.arange(4)))
        expected_influence = dense_influence[:, selected]
        np.testing.assert_allclose(fit.beta, beta[:4], atol=1e-7)
        np.testing.assert_allclose(result.covariance,
            expected_influence.T @ expected_influence, rtol=1e-8, atol=1e-7)
        c = contrast_vector(result.names, {"payoff_cents|D10:xL": 1,
                                          "payoff_cents|D1:xL": -1})
        expected_scalar = expected_influence @ c
        report = result.contrast(c)
        self.assertAlmostEqual(report["CR0"]["standard_error"] ** 2,
                               float(expected_scalar @ expected_scalar), places=7)
        self.assertAlmostEqual(report["cluster_count_adjusted"]["standard_error"] ** 2 /
                               report["CR0"]["standard_error"] ** 2, 40 / 39)
        self.assertGreater(abs(result.covariance[0, 2]), 1e-6)
        original_tss = ((y - y.mean(axis=0)) ** 2).sum(axis=0)
        np.testing.assert_allclose(fit.r_squared, 1 - (residue ** 2).sum(axis=0) / original_tss)
        self.assertFalse(result.to_dict()["variance_convention"]["full_N_minus_k_CR1_used"])
        json.dumps(result.to_dict(), allow_nan=False)

    def test_a1_payoff_equals_negative_price_after_claim_absorption(self):
        rng = np.random.default_rng(90018)
        claim = np.repeat(np.arange(42), 12)
        h = rng.integers(0, 2, len(claim))
        h[claim < 3] = 0  # Keep claims that appear in only one tail.
        h[(claim >= 3) & (claim < 6)] = 1
        p = np.where(h == 1, rng.uniform(.91, .985, len(claim)),
                     rng.uniform(.015, .095, len(claim)))
        outcome = (claim % 2).astype(float)
        xl = np.log2(2 + claim % 9)
        xr = rng.uniform(0, 1, len(claim))
        x, names = a1_claim_design(h, xl, xr)
        payoff, roi = payoff_and_roi(p, outcome)
        targets = ("payoff_cents", "negative_price_cents", "roi_percent")
        y = np.column_stack((payoff, -100 * p, roi))
        fit, scores, absorber = streamed_fit(x, y, names, targets, claim // 2,
                                           {"claim": claim})
        self.assertTrue(absorber.converged)
        self.assertEqual(fit.n, len(claim))
        np.testing.assert_allclose(fit.beta[:, 0], fit.beta[:, 1], atol=1e-10)
        result = finalize_clustered_regression(fit, scores)
        np.testing.assert_allclose(result.covariance[:4, :4], result.covariance[4:8, 4:8],
                                   atol=1e-10)
        transformed = absorber.transform({"x": x, "y": y, "codes": {"claim": claim}})[1]
        np.testing.assert_allclose(transformed[:, 0], transformed[:, 1], atol=1e-12)
        # Standalone original lifespan is exactly absorbed and must withhold a fit.
        fit_bad, scores_bad, _ = streamed_fit(np.column_stack((x, xl)), y,
            (*names, "standalone_xL"), targets, claim // 2, {"claim": claim})
        self.assertIn("rank_deficient_residualized_design", fit_bad.suppressed_reasons)
        self.assertTrue(all(row["suppressed"] for row in
                            finalize_clustered_regression(fit_bad, scores_bad).to_dict()["estimates"]))

    def test_joint_phase_covariance_and_effective_clusters_from_explicit_rows(self):
        cells = ("D1:pre", "D10:pre", "D1:in", "D10:in")
        rng = np.random.default_rng(11187)
        values = rng.normal(size=(37, 4)) + np.arange(37)[:, None] * .3
        records = [{"cluster": event, "cell": cell, "count": 15 + event % 5,
                    "weight_sum": 15 + event % 5,
                    "weighted_sums": [(15 + event % 5) * values[event, j]]}
                   for event in range(37) for j, cell in enumerate(cells)]
        result = joint_clustered_means_from_totals(records, cells=cells,
                 target_names=("payoff_cents",), min_observations=500, min_clusters=30)
        count = np.array([15 + event % 5 for event in range(37)])
        means = np.average(values, axis=0, weights=count)
        influence = count[:, None] * (values - means) / count.sum()
        c = joint_tail_phase_contrasts(result.names, target="payoff_cents",
            phases=("pre", "in"))["tail_spread_change:in_minus_pre"]
        report = result.contrast(c)
        scalar = influence @ np.array([1, -1, -1, 1])
        squares = scalar ** 2
        self.assertFalse(report["suppressed"])
        self.assertAlmostEqual(report["CR0"]["estimate"], float(means @ [1, -1, -1, 1]))
        self.assertAlmostEqual(report["CR0"]["standard_error"] ** 2, float(squares.sum()))
        self.assertAlmostEqual(report["influence"]["effective_clusters"],
                               float(squares.sum() ** 2 / (squares ** 2).sum()))
        self.assertAlmostEqual(report["influence"]["maximum_cluster_variance_share"],
                               float(squares.max() / squares.sum()))
        self.assertNotAlmostEqual(float(np.diag(result.covariance).sum()), float(squares.sum()))

    def test_sports_independent_500_rows_30_games_and_supported_inplay_level(self):
        cells = ("D1:pre", "D10:pre", "D1:in", "D10:in")
        records = []
        for j, cell in enumerate(cells):
            games = 29 if j == 0 else 30
            for game in range(games):
                n = 20 if j != 1 else (16 if game < 29 else 35)  # D10 pre has 499 rows.
                records.append({"cluster": game, "cell": cell, "count": n,
                    "weight_sum": n, "weighted_sums": [n * (game * (j + 1) + j)]})
        result = joint_clustered_means_from_totals(records, cells=cells,
                 target_names=("payoff_cents",), min_observations=500, min_clusters=30)
        contrasts = joint_tail_phase_contrasts(result.names, target="payoff_cents",
                                               phases=("pre", "in"))
        self.assertEqual(result.metadata["cluster_count"], 30)
        self.assertEqual(result.metadata["cluster_count_by_cell"]["D1:pre"], 29)
        self.assertEqual(result.metadata["n_by_cell"]["D10:pre"], 499)
        self.assertIn("cluster_support_below_floor", result.reasons[0])
        self.assertIn("observation_support_below_floor", result.reasons[1])
        self.assertTrue(result.contrast(contrasts["D10_minus_D1:pre"])["suppressed"])
        self.assertFalse(result.contrast(contrasts["D10_minus_D1:in"])["suppressed"])
        self.assertTrue(result.contrast(contrasts["tail_spread_change:in_minus_pre"])["suppressed"])
        json.dumps(result.to_dict(contrasts), allow_nan=False)

    def test_regression_support_proof_is_required_and_union_games_are_insufficient(self):
        rng = np.random.default_rng(18156)
        h = np.r_[np.zeros(600, dtype=int), np.ones(600, dtype=int)]
        x, names = tail_varying_design(rng.normal(size=len(h)), ("xL",), h)
        events = np.r_[np.arange(600) % 30, np.arange(600) % 29]
        y = x @ [3, 1, 4, 2] + rng.normal(size=len(h))
        fit, scores, _ = streamed_fit(x, y, names, ("payoff_cents",), events)
        with self.assertRaises(ValueError):
            finalize_clustered_regression(fit, scores, min_observations=500, min_clusters=30)
        support = {name: {"n_observations": 600,
                         "cluster_count": 29 if name.startswith("D10:") else 30}
                   for name in names}
        result = finalize_clustered_regression(fit, scores, min_observations=500,
                  min_clusters=30, support_by_term=support)
        self.assertFalse(result.contrast([0, 1, 0, 0])["suppressed"])
        self.assertTrue(result.contrast([0, -1, 0, 1])["suppressed"])
        self.assertIn("term_cluster_support_below_floor",
                      result.contrast([0, -1, 0, 1])["suppression_reasons"])

    def test_tail_and_stacked_r_squared_center_correctly_without_mixing_units(self):
        y = np.array([[1, 100], [3, 160], [15, 800], [21, 1100]], dtype=float)
        residue = np.array([[.2, 20], [.3, 30], [.4, 40], [.5, 50]])
        groups = {}
        for name, positions in (("D1", slice(0, 2)), ("D10", slice(2, 4))):
            groups[name] = {"weight_sum": 2, "y_sum": y[positions].sum(axis=0),
                "y_squared_sum": (y[positions] ** 2).sum(axis=0),
                "residual_squared_sum": (residue[positions] ** 2).sum(axis=0)}
        result = grouped_r_squared_from_sufficient_statistics(groups,
                    target_names=("payoff_cents", "roi_percent"))
        expected = 1 - (residue ** 2).sum(axis=0) / ((y - y.mean(axis=0)) ** 2).sum(axis=0)
        for j, target in enumerate(("payoff_cents", "roi_percent")):
            self.assertAlmostEqual(result["stacked_weighted_tss_r_squared_by_target"][target], expected[j])
            for name, positions in (("D1", slice(0, 2)), ("D10", slice(2, 4))):
                group_y = y[positions, j]
                expected_tail = 1 - (residue[positions, j] ** 2).sum() / ((group_y - group_y.mean()) ** 2).sum()
                self.assertAlmostEqual(result["group_summaries"][name]
                    ["r_squared_including_fixed_effects_by_target"][target], expected_tail)
        self.assertNotAlmostEqual(expected[0], expected[1])

    def test_log_clocks_and_exact_bin_edges(self):
        clocks = paper_time_controls([3, 7, 15], [0, 3, 7])
        self.assertEqual(set(clocks), {"original_log2_days", "remaining_log2_days"})
        np.testing.assert_allclose(clocks["original_log2_days"], [2, 3, 4])
        np.testing.assert_allclose(clocks["remaining_log2_days"], [0, 2, 3])
        for invalid in (([1], [2]), ([2], [-1]), ([np.nan], [0])):
            with self.assertRaises(ValueError):
                paper_time_controls(*invalid)
        edges = np.array([.1, .2, .3, .4, .5, .6, .7, .8, .9])
        np.testing.assert_array_equal(fixed_probability_bins(edges), np.arange(1, 10))
        np.testing.assert_array_equal(fixed_probability_bins(np.nextafter(edges, 0)), np.arange(9))
        np.testing.assert_array_equal(fixed_probability_bins(np.nextafter(edges, 1)), np.arange(1, 10))

    def test_nonconvergence_withholds_and_zero_influence_diagnostics_are_undefined(self):
        rng = np.random.default_rng(42219)
        n = 240
        x, y = rng.normal(size=(n, 2)), rng.normal(size=(n, 1))
        codes = {"category": rng.integers(0, 3, n), "month": rng.integers(0, 5, n)}
        fit, scores, absorber = streamed_fit(x, y, ("xL", "xR"), ("payoff_cents",),
                    np.arange(n) % 30, codes, max_iterations=1)
        self.assertFalse(absorber.converged)
        self.assertIn("categorical_projection_not_converged", fit.suppressed_reasons)
        output = finalize_clustered_regression(fit, scores).to_dict()
        self.assertTrue(all(row["suppressed"] for row in output["estimates"]))
        records = [{"cluster": event, "cell": "D1", "count": 1,
                    "weight_sum": 1, "weighted_sums": [3]} for event in range(30)]
        level = joint_clustered_means_from_totals(records, cells=("D1",), target_names=("payoff_cents",))
        diagnostic = level.contrast([1])["influence"]
        self.assertIsNone(diagnostic["effective_clusters"])
        self.assertIsNone(diagnostic["maximum_cluster_variance_share"])
        self.assertIn("zero_cluster_score_variance", diagnostic["flags"])
        json.dumps(level.to_dict(), allow_nan=False)

    def test_influence_is_scale_invariant_for_large_finite_individual_returns(self):
        values = np.arange(1, 5, dtype=float)
        def model(scale):
            records = [{"cluster": event, "cell": "D1", "count": 1,
                        "weight_sum": 1, "weighted_sums": [scale * value]}
                       for event, value in enumerate(values)]
            return joint_clustered_means_from_totals(records, cells=("D1",),
                         target_names=("roi_percent",))
        ordinary, large = model(1), model(1e100)
        ordinary_diagnostic = ordinary.contrast([1])["influence"]
        large_diagnostic = large.contrast([1])["influence"]
        self.assertAlmostEqual(large_diagnostic["effective_clusters"],
                               ordinary_diagnostic["effective_clusters"])
        self.assertAlmostEqual(large_diagnostic["maximum_cluster_variance_share"],
                               ordinary_diagnostic["maximum_cluster_variance_share"])
        json.dumps(large.to_dict(), allow_nan=False)

    def test_unrepresentable_variance_is_withheld_and_serializable(self):
        records = [{"cluster": event, "cell": "D1", "count": 1,
                    "weight_sum": 1, "weighted_sums": [float(value) * 1e200]}
                   for event, value in enumerate(range(1, 5))]
        model = joint_clustered_means_from_totals(records, cells=("D1",),
                        target_names=("roi_percent",))
        contrast = model.contrast([1])
        self.assertTrue(contrast["suppressed"])
        self.assertIsNone(contrast["CR0"])
        self.assertTrue(contrast["suppression_reasons"])
        json.dumps(model.to_dict(), allow_nan=False)

    def test_finite_supplementary_variance_avoids_intermediate_overflow(self):
        scale = 1.5e154
        records = [{"cluster": event, "cell": "D1", "count": 1,
                    "weight_sum": 1, "weighted_sums": [float(value) * scale]}
                   for event, value in enumerate(range(1, 5))]
        model = joint_clustered_means_from_totals(records, cells=("D1",),
                        target_names=("roi_percent",))
        result = model.contrast([1])
        variance = .3125 * scale * scale
        self.assertAlmostEqual(result["cluster_count_adjusted"]["standard_error"] /
                               np.sqrt(variance * (4 / 3)), 1)
        json.dumps(model.to_dict(), allow_nan=False)

    def test_frequency_counts_reproduce_expanded_records_exactly(self):
        rng = np.random.default_rng(90167)
        n = 90
        count = 1 + np.arange(n) % 7
        x, y = rng.normal(size=(n, 2)), rng.normal(size=(n, 2))
        codes = {"category": np.arange(n) % 5, "month": np.arange(n) % 3}
        events = np.arange(n) % 30
        names, targets = ("xL", "xR"), ("payoff_cents", "roi_percent")
        def grouped_source():
            yield {"x": x, "y": y, "codes": codes, "weights": count,
                   "observation_counts": count}
        absorber = absorb_categorical_effects(grouped_source, term_names=names,
                target_names=targets, level_counts={"category": 5, "month": 3})
        batch = next(grouped_source())
        residual_x, residual_y, weights = absorber.transform(batch)
        moments = RegressionMoments(names, targets)
        moments.add(residual_x, residual_y, weights, observation_counts=count)
        fit = fit_ols_moments(moments, absorption=absorber)
        scores = ClusterScoreMoments(4)
        scores.add(events, fit.score_batch(residual_x, residual_y, weights),
                   observation_counts=count)
        grouped = finalize_clustered_regression(fit, scores)
        positions = np.repeat(np.arange(n), count)
        expanded_fit, expanded_scores, _ = streamed_fit(x[positions], y[positions], names,
            targets, events[positions], {name: code[positions] for name, code in codes.items()})
        expanded = finalize_clustered_regression(expanded_fit, expanded_scores)
        self.assertEqual(fit.n, len(positions))
        self.assertEqual(absorber.n, len(positions))
        np.testing.assert_allclose(fit.beta, expanded_fit.beta, atol=1e-11)
        np.testing.assert_allclose(fit.r_squared, expanded_fit.r_squared, atol=1e-11)
        np.testing.assert_allclose(grouped.covariance, expanded.covariance, atol=1e-11)
        self.assertEqual(grouped.metadata["cluster_counts"], expanded.metadata["cluster_counts"])

    def test_completely_empty_joint_mean_grid_is_explicitly_withheld(self):
        cells = ("D1:pregame", "D10:pregame", "D1:in_play", "D10:in_play")
        model = joint_clustered_means_from_totals([], cells=cells,
                target_names=("payoff_cents", "roi_percent"), min_clusters=30, min_observations=500)
        result = model.to_dict()
        self.assertEqual(len(result["estimates"]), 8)
        self.assertTrue(all(row["suppressed"] and row["CR0"] is None for row in result["estimates"]))
        self.assertEqual(result["metadata"]["cluster_count"], 0)
        self.assertEqual(model.coefficient_cluster_influence.shape, (0, 8))
        json.dumps(result, allow_nan=False)


class IndependentInputQA(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="kaushik-independent-qa-")
        self.addCleanup(self.temporary.cleanup)
        self.folder = Path(self.temporary.name)
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)
        # Winner labels are shared across a condition's two tokens, never token
        # payouts. Arbitrary native labels and shared events are deliberate.
        self.metadata = {
            "token_map": [{"token_id": token, "condition_id": market, "outcome": label}
                for market, pair in (("0xa", (("100", "Yes"), ("101", "No"))),
                                     ("0xb", (("200", "Arsenal"), ("201", "Chelsea"))),
                                     ("0xc", (("300", "0"), ("301", "1"))))
                for token, label in pair],
            "spine": [{"token_id": token, "market_id": market, "winning_outcome": winner}
                for market, winner, pair in (("0xa", "Yes", ("100", "101")),
                                             ("0xb", "Chelsea", ("200", "201")),
                                             ("0xc", "0", ("300", "301")))
                for token in pair],
            "native": [{"condition_id": market, "n_outcomes": 2,
                        "created_at": "2026-03-10T00:00:00.500Z" if market == "0xa" else None,
                        "end_date": "2026-03-11T00:00:00.500Z" if market == "0xa" else None,
                        "event_slug": "shared-event" if market != "0xc" else ""}
                       for market in ("0xa", "0xb", "0xc")],
            "categories": [{"mkt": "0xa", "prim": "Sports"},
                           {"mkt": "0xb", "prim": "Sports"}],
        }
        self.paths = {name: str(self.folder / (name + ".parquet")) for name in self.metadata}
        self.refresh()

    def refresh(self):
        for name, rows in self.metadata.items():
            pq.write_table(pa.Table.from_pylist(rows), self.paths[name])
        return build_inputs.prepare_metadata(self.con, self.paths)

    def test_shared_condition_winner_labels_arbitrary_labels_and_cluster_namespaces(self):
        claims = self.con.execute("SELECT token_id,Y,event_cluster,category FROM claims ORDER BY token_id").fetchall()
        self.assertEqual(claims, [("100", 1, "event:shared-event", "Sports"),
                                 ("101", 0, "event:shared-event", "Sports"),
                                 ("200", 0, "event:shared-event", "Sports"),
                                 ("201", 1, "event:shared-event", "Sports"),
                                 ("300", 1, "market:0xc", "Unclassified"),
                                 ("301", 0, "market:0xc", "Unclassified")])
        # The winner must match one of the native labels, with exact case.
        self.metadata["spine"][0]["winning_outcome"] = "YES"
        self.metadata["spine"][1]["winning_outcome"] = "YES"
        self.refresh()
        self.assertEqual(self.con.execute("SELECT count(*) FROM claims WHERE market_id='0xa'").fetchone()[0], 0)
        # A disagreement across tokens excludes the entire condition, including
        # the otherwise individually consistent token; no partial pair survives.
        self.metadata["spine"][2]["winning_outcome"] = "Arsenal"
        self.refresh()
        self.assertEqual(self.con.execute("SELECT count(*) FROM claims WHERE market_id='0xb'").fetchone()[0], 0)

    def test_complement_binary64_cutoff_and_fractional_duration_edges(self):
        start = 1773100800  # 2026-03-10T00:00:00Z, before fractional opening.
        end = start + 86400
        schema = pa.schema([("proxyWallet", pa.string()), ("timestamp", pa.int64()),
            ("conditionId", pa.string()), ("usdcSize", pa.float64()), ("price", pa.float64()),
            ("side", pa.string()), ("outcome", pa.string()), ("eventSlug", pa.string()),
            ("is_maker", pa.bool_()), ("counterparty", pa.string()), ("year_month", pa.string())])
        rows = []
        for stamp, side, price in ((start, "BUY", .1), (start + 1, "BUY", .1),
                                  (end, "SELL", .9), (end + 1, "SELL", .1),
                                  (build_inputs.CUTOFF_SECONDS - 1, "BUY", .5),
                                  (build_inputs.CUTOFF_SECONDS, "BUY", .5)):
            rows.append({"proxyWallet": "ignored", "timestamp": stamp, "conditionId": "100",
                "usdcSize": 2., "price": price, "side": side, "outcome": "Yes",
                "eventSlug": "wrong-source-slug", "is_maker": False, "counterparty": "ignored",
                "year_month": "2026-03"})
        rows.insert(2, dict(rows[1]))  # Preserve physical multiplicity.
        path = self.folder / "trades.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        build_inputs.annotate_month(self.con, str(path), "2026-03")
        annotated = self.con.execute("SELECT P,Y,eligibility_reason,duration_clock_valid FROM annotated_month ORDER BY source_ordinal").fetchall()
        self.assertEqual([item[2] for item in annotated], ["eligible"] * 6 + ["at_or_after_cutoff"])
        self.assertEqual([item[3] for item in annotated], [False, True, True, True, False, False, False])
        self.assertEqual(annotated[3][:2], (1 - .9, 0))
        self.assertEqual(annotated[4][:2], (1 - .1, 0))
        self.assertEqual(sum(row["rows"] for row in build_inputs.month_counts(self.con, 7, "2026-03")), 7)
        stage = self.folder / "stage"
        (stage / "year_month=2026-03").mkdir(parents=True)
        self.con.execute(f"COPY claims TO {build_inputs.literal(stage / 'claims.parquet')} (FORMAT PARQUET)")
        self.con.execute(f"COPY (SELECT source_ordinal,year_month source_month,claim_code,recorded_token_code,timestamp,price recorded_price,P,Y,usdcSize,side,is_maker,duration_clock_valid duration_eligible FROM annotated_month WHERE eligibility_reason='eligible') TO {build_inputs.literal(stage / 'year_month=2026-03' / 'base.parquet')} (FORMAT PARQUET)")
        build_inputs.create_analysis_view(self.con, stage)
        derived = self.con.execute("SELECT source_ordinal,P,bin,L,R,xL,xR,payoff,roi,claim_id,quantity,event_cluster FROM analysis_base ORDER BY source_ordinal").fetchall()
        self.assertEqual(len(derived), 6)
        self.assertEqual([row[2] for row in derived], [2, 2, 2, 1, 10, 6])
        self.assertEqual(derived[1][3], 1.)
        self.assertAlmostEqual(derived[1][4], 86399.5 / 86400)
        self.assertEqual(derived[1][5], 1.)
        self.assertAlmostEqual(derived[1][6], np.log2(1 + 86399.5 / 86400))
        self.assertEqual(derived[3][9], "101")
        self.assertEqual(derived[3][10], 2 / .9)
        self.assertTrue(all(row[-1] == "event:shared-event" for row in derived))


class IndependentDriverQA(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="kaushik-driver-qa-")
        self.addCleanup(self.temporary.cleanup)
        self.folder = Path(self.temporary.name)
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)
        # Only the fixture's free-disk reservation is bypassed. Production entry
        # points and production host guards are never called by these oracles.
        reservation = patch.object(driver, "reserve_output")
        reservation.start()
        self.addCleanup(reservation.stop)

    def dense_oracle(self, x, y, codes, events):
        z = (np.column_stack([np.eye(int(code.max()) + 1)[code]
                              for code in codes.values()]) if codes else np.empty((len(x), 0)))
        dense = np.column_stack((x, z))
        beta = np.linalg.lstsq(dense, y, rcond=None)[0]
        residual = y - dense @ beta
        bread = np.linalg.pinv(dense.T @ dense, rcond=1e-13)
        row_scores = (residual[:, :, None] * dense[:, None, :]).reshape(len(x), -1)
        clustered = np.array([row_scores[events == event].sum(axis=0) for event in np.unique(events)])
        influence = clustered @ np.kron(np.eye(y.shape[1]), bread)
        selected = np.concatenate([target * dense.shape[1] + np.arange(x.shape[1])
                                   for target in range(y.shape[1])])
        influence = influence[:, selected]
        return beta[:x.shape[1]], residual, influence.T @ influence

    def assert_driver_matches_dense(self, result, beta, residual, covariance, y, tail=None):
        joint = result["joint"]
        observed = np.array([row["CR0"]["estimate"] for row in joint["estimates"]])
        np.testing.assert_allclose(observed, beta.T.reshape(-1), atol=1e-7)
        np.testing.assert_allclose(np.asarray(joint["covariance_CR0"]), covariance,
                                   atol=1e-7, rtol=1e-7)
        self.assertEqual(joint["metadata"]["n"], len(y))
        expected_r2 = 1 - (residual ** 2).sum(axis=0) / ((y - y.mean(axis=0)) ** 2).sum(axis=0)
        for index, target in enumerate(driver.TARGETS):
            self.assertAlmostEqual(joint["metadata"]["r_squared_including_fixed_effects_by_target"][target],
                                   expected_r2[index], places=9)
            if tail is not None:
                grouped = joint["metadata"]["grouped_r_squared"]
                self.assertAlmostEqual(grouped["stacked_weighted_tss_r_squared_by_target"][target],
                                       expected_r2[index], places=9)
                for indicator, label in ((0, "D1"), (1, "D10")):
                    selected = tail == indicator
                    tss = ((y[selected, index] - y[selected, index].mean()) ** 2).sum()
                    tail_r2 = 1 - (residual[selected, index] ** 2).sum() / tss
                    self.assertAlmostEqual(grouped["group_summaries"][label]
                        ["r_squared_including_fixed_effects_by_target"][target], tail_r2, places=9)
        score_artifact = joint["metadata"]["score_artifact"]
        saved = pq.read_table(self.folder / score_artifact["path"])
        influence = np.array(saved["projected_score"].to_pylist())
        np.testing.assert_allclose(influence.T @ influence, covariance, atol=1e-7, rtol=1e-7)
        json.dumps(result, allow_nan=False)

    def test_varying_within_cells_restore_all_five_models_and_joint_scores(self):
        rng = np.random.default_rng(861903)
        events = np.repeat(np.arange(36), 12 * 7)
        cell = np.tile(np.repeat(np.arange(12), 7), 36)
        h = cell % 2
        raw_price_code = (cell // 2) % 4 + h * 4
        price_levels = np.array([.025, np.nextafter(.025, 1), .06, .08,
                                 .91, np.nextafter(.91, 1), .95, .98])
        price = price_levels[raw_price_code]
        cat_code = (cell // 4) + h * 3
        month_code = ((events + cell // 2) % 3) + h * 3
        lifespan = 2 + events % 6 + rng.uniform(0, 1.4, len(events))
        remaining = .1 + rng.uniform(0, 1.7, len(events))
        xl, xr = np.log2(1 + lifespan), np.log2(1 + remaining)
        outcome = rng.integers(0, 2, len(events))
        y = np.column_stack(payoff_and_roi(price, outcome))
        table = pa.table({"event_cluster": np.array([f"event:{event}" for event in events]),
            "tail": h, "cat_code": cat_code, "price_code": raw_price_code, "month_code": month_code,
            "xL": xl, "xR": xr, "payoff": y[:, 0], "roi": y[:, 1]})
        self.con.register("fixture", table)
        cache = driver.group_cache(self.con, "fixture", self.folder / "varying_groups.parquet",
            ("event_cluster", "tail", "cat_code", "price_code", "month_code"),
            ("xL", "xR", "payoff", "roi"))
        self.assertEqual(cache["rows"], len(events) // 7)
        self.assertIn("covar_pop", cache["within_moment_definition"])
        names = self.con.execute("DESCRIBE varying_groups").fetchall()
        self.assertEqual(len(names), 5+1+4+4*5//2)
        self.assertFalse(any(name[0] == "a0_0" for name in names))
        centered_within = self.con.execute("SELECT sum(c0_0),sum(c1_1),sum(c2_2) FROM varying_groups").fetchone()
        self.assertTrue(all(value > 0 for value in centered_within))
        raw_features = np.column_stack((xl, xr, y))
        expected_centered = np.zeros((4, 4))
        for begin in range(0, len(events), 7):
            values = raw_features[begin:begin+7]
            centered = values-values.mean(axis=0)
            expected_centered += centered.T@centered
        saved_centered = self.con.execute("SELECT "+",".join(
            f"sum(c{i}_{j})" for i in range(4) for j in range(i, 4))+" FROM varying_groups").fetchone()
        np.testing.assert_allclose(saved_centered, [expected_centered[i,j] for i in range(4) for j in range(i,4)],
                                   rtol=2e-14, atol=1e-8)
        artifacts, sample_counts = {}, []
        for spec in driver.model_specs():
            spec = {**spec, "report_clocks": spec["clocks"]}
            result = driver.regression_model(self.con, cache, "varying_groups", spec,
                     f"qa_duration_{spec['column']}", self.folder, artifacts, driver.ReadLedger())
            chosen = np.column_stack([xl if clock == "xL" else xr for clock in spec["clocks"]])
            base = chosen if spec["effects"] else np.column_stack((np.ones(len(events)), chosen))
            x = np.column_stack((base * (1 - h[:, None]), base * h[:, None]))
            all_codes = {"cat_code": cat_code, "price_code": raw_price_code, "month_code": month_code}
            codes = {effect: all_codes[effect] for effect in spec["effects"]}
            beta, residual, covariance = self.dense_oracle(x, y, codes, events)
            self.assert_driver_matches_dense(result, beta, residual, covariance, y, h)
            metadata = result["joint"]["metadata"]
            self.assertIn("unknown", metadata["combined_absorbed_fixed_effect_rank"])
            self.assertIn("covar_pop", metadata["grouped_within_moment_definition"])
            for indicator, label in ((0, "D1"), (1, "D10")):
                selected = h == indicator
                raw_design = base[selected]
                if codes:
                    dummies = np.column_stack([np.eye(int(code.max())+1)[code[selected]] for code in codes.values()])
                    dense_rank = np.linalg.matrix_rank(np.column_stack((raw_design,dummies)))-np.linalg.matrix_rank(dummies)
                else:
                    dense_rank = np.linalg.matrix_rank(raw_design)
                diagnostic = metadata["residualized_continuous_design_by_tail"][label]
                self.assertEqual(diagnostic["rank"], dense_rank)
                self.assertEqual(diagnostic["continuous_columns"], base.shape[1])
                self.assertEqual(diagnostic["relative_rank_tolerance"], 1e-10)
                self.assertIsNotNone(diagnostic["condition_number"])
            self.assertEqual(sum(item["rank"] for item in metadata["residualized_continuous_design_by_tail"].values()),
                             metadata["residualized_design_rank"])
            sample_counts.append(result["joint"]["metadata"]["n"])
        self.assertEqual(sample_counts, [len(events)] * 5)
        self.assertEqual(len(artifacts), 5)

    def test_grouped_a1_with_varying_tails_within_claim_matches_full_dummy_fit(self):
        rng = np.random.default_rng(811582)
        claim = np.repeat(np.arange(42), 14)
        events = claim // 2
        h = rng.integers(0, 2, len(claim))
        h[claim < 4] = 0
        h[(claim >= 4) & (claim < 8)] = 1
        price = np.where(h, rng.uniform(.91, .99, len(claim)), rng.uniform(.01, .095, len(claim)))
        xl = np.log2(2 + claim % 8)
        xr = rng.uniform(0, 1.5, len(claim))
        x = np.column_stack((h, h * xl, xr, h * xr))
        y = np.column_stack(payoff_and_roi(price, claim % 2))
        self.con.register("claim_fixture", pa.table({"event_cluster": [f"event:{event}" for event in events],
            "claim_code": claim, "H": h, "HxL": h * xl, "xR": xr, "HxR": h * xr,
            "payoff": y[:, 0], "roi": y[:, 1]}))
        cache = driver.group_cache(self.con, "claim_fixture", self.folder / "a1_varying_groups.parquet",
            ("event_cluster", "claim_code"), ("H", "HxL", "xR", "HxR", "payoff", "roi"))
        self.assertEqual(cache["rows"], 42)
        spec = {"clocks": ["xL", "xR"], "effects": ["claim_code"], "report_clocks": ["xL", "xR"]}
        result = driver.regression_model(self.con, cache, "a1_varying_groups", spec,
                 "qa_a1", self.folder, {}, driver.ReadLedger(), claim_fe=True)
        beta, residual, covariance = self.dense_oracle(x, y, {"claim": claim}, events)
        self.assert_driver_matches_dense(result, beta, residual, covariance, y)
        self.assertEqual(result["joint"]["metadata"]["effect_cardinality"], {"claim_code": 42})
        self.assertIsNone(result["joint"]["metadata"]["residualized_continuous_design_by_tail"])
        self.assertEqual(result["joint"]["metadata"]["residualized_design_rank"], 4)
        self.assertIn("unknown", result["joint"]["metadata"]["combined_absorbed_fixed_effect_rank"])

    def test_grouped_constant_within_fe_clock_is_not_identified_by_cancellation_dust(self):
        # Non-binary decimal constants leave positive SUM(x*x)-n*mean(x)^2
        # roundoff, but are mathematically entirely absorbed by category FE.
        records = []
        for event in range(12):
            for tail in (0, 1):
                for category, clock in enumerate((.3, .7)):
                    price = ((.025, .06) if not tail else (.91, .96))[category]
                    for index in range(3):
                        payout = (event+index) % 2
                        records.append({"event_cluster": f"event:{event}", "tail": tail,
                            "cat_code": category+2*tail, "price_code": category+2*tail,
                            "month_code": tail, "xL": clock, "xR": .2+.1*index,
                            "payoff": 100*(payout-price), "roi": 100*(payout/price-1)})
        self.con.register("constant_clock_fixture", pa.Table.from_pylist(records))
        cache = driver.group_cache(self.con, "constant_clock_fixture", self.folder/"constant_clock_groups.parquet",
            ("event_cluster", "tail", "cat_code", "price_code", "month_code"),
            ("xL", "xR", "payoff", "roi"))
        x = np.array([record["xL"] for record in records])
        tail = np.array([record["tail"] for record in records])
        codes = np.array([record["cat_code"] for record in records])
        continuous = np.column_stack((x*(1-tail), x*tail))
        dummy = np.eye(4)[codes]
        self.assertEqual(np.linalg.matrix_rank(np.column_stack((continuous, dummy))), 4)
        residualized = continuous-dummy@np.linalg.lstsq(dummy, continuous, rcond=None)[0]
        self.assertLess(np.linalg.norm(residualized), 1e-13)
        for raw_spec in (driver.model_specs()[1], driver.model_specs()[4]):
            spec = {**raw_spec, "report_clocks": raw_spec["clocks"]}
            with self.subTest(column=spec["column"]), contextlib.redirect_stdout(io.StringIO()):
                result = driver.regression_model(self.con, cache, "constant_clock_groups", spec,
                    f"qa_constant_clock_{spec['column']}", self.folder, {}, driver.ReadLedger())
                self.assertIn("rank_deficient_residualized_design", result["suppression_reasons"])
                self.assertTrue(all(row["suppressed"] and row["CR0"] is None for row in result["slopes"]))
                ranks = result["joint"]["metadata"]["residualized_continuous_design_by_tail"]
                expected_rank = 0 if spec["column"] == 2 else 1
                self.assertEqual({tail: item["rank"] for tail,item in ranks.items()},
                                 {"D1": expected_rank, "D10": expected_rank})
                self.assertEqual(result["joint"]["metadata"]["residualized_design_rank"], 2*expected_rank)
                self.assertTrue(all(item["condition_number"] is None for item in ranks.values()))
    def test_duration_dictionary_preserves_exact_prices_common_cohort_and_panel_cutoffs(self):
        price_levels = np.array([.025, np.nextafter(.025, 1), .06, .08,
                                 .91, np.nextafter(.91, 1), .95, .98])
        price = np.r_[np.repeat(price_levels, 8), .5, .025, .95]
        lifespan = np.r_[np.tile([1., 2., 3., 4.], 16), 2., 2., 2.]
        remaining = np.r_[np.tile([0., 1., np.nextafter(1., 2.), .5], 16), 1., 1., 1.]
        maker = np.zeros(len(price), dtype=bool)
        maker[-2] = True
        duration_eligible = np.ones(len(price), dtype=bool)
        duration_eligible[-1] = False
        binned = fixed_probability_bins(price) + 1
        y = np.column_stack(payoff_and_roi(price, np.arange(len(price)) % 2))
        self.con.register("analysis_base", pa.table({
            "event_cluster": [f"event:{i % 8}" for i in range(len(price))],
            "P": price, "bin": binned, "is_maker": maker,
            "duration_eligible": duration_eligible, "category": ["Sports"] * len(price),
            "trade_month": ["2026-03"] * len(price), "L": lifespan, "R": remaining,
            "xL": np.log2(1 + lifespan), "xR": np.log2(1 + remaining),
            "payoff": y[:, 0], "roi": y[:, 1]}))
        cache = driver.make_duration_cache(self.con, self.folder, driver.ReadLedger(), 1000)
        exact_prices = pq.read_table(self.folder / "price_dictionary.parquet")["P"].to_numpy()
        np.testing.assert_array_equal(exact_prices, price_levels)
        self.assertEqual(len(exact_prices), 8)
        self.assertEqual(self.con.execute("SELECT sum(n_rows) FROM duration_groups").fetchone()[0], 64)
        self.assertEqual(self.con.execute("SELECT sum(n_rows) FROM duration_groups WHERE lifespan_gt1").fetchone()[0], 48)
        self.assertEqual(self.con.execute("SELECT sum(n_rows) FROM duration_groups WHERE remaining_gt1").fetchone()[0], 16)
        self.assertEqual(self.con.execute("SELECT sum(n_rows) FROM duration_groups WHERE lifespan_gt1 AND NOT remaining_gt1").fetchone()[0], 32)
        self.assertEqual(cache["feature_expressions"], ["xL", "xR", "payoff", "roi"])

    def normalized_sports_binding(self, records):
        path = self.folder / "normalized_sports.parquet"
        pq.write_table(pa.Table.from_pylist(records), path)
        return {"inputs": [{"role": "sports_market_map", "path": str(path)}],
                "coverage_qualification": "synthetic fixture only"}

    def test_sports_null_clock_cannot_hide_behind_another_same_game_market(self):
        records = [{"market_id": "0xa", "sport": "mlb", "game_key": "mlb:1",
                    "actual_start_seconds": 100000., "actual_end_seconds": 110800.,
                    "timing_quality": "fixture", "provenance": "fixture"},
                   {"market_id": "0xb", "sport": "mlb", "game_key": "mlb:1",
                    "actual_start_seconds": None, "actual_end_seconds": 110800.,
                    "timing_quality": "fixture", "provenance": "fixture"}]
        with self.assertRaises(build_inputs.InputBlocked):
            driver.prepare_sports_map(self.con, self.normalized_sports_binding(records))

    def test_native_event_and_market_fallback_sample_counts_match_input_literals(self):
        self.con.register("analysis_base", pa.table({
            "market_id": ["0xa", "0xb", "0xc"], "claim_id": ["1", "2", "3"],
            "event_cluster": ["event:a", "event:b", "market:0xc"],
            "cluster_source": ["native_event", "native_event", "market_fallback"],
            "timestamp": [1773100800] * 3, "bin": [1, 10, 5],
            "duration_eligible": [True, True, False], "endpoint_seconds": [1773200000.] * 3,
            "is_maker": [False] * 3, "category": ["Sports", "Sports", "Unclassified"]}))
        sample, categories = driver.describe_sample(self.con,
             {"exclusions": {}, "rows": {"primary_taker": 3}}, driver.ReadLedger(), 0)
        self.assertEqual(sample["unique_event_clusters"], 2)
        self.assertEqual(sample["market_fallback_clusters"], 1)
        self.assertEqual(sample["clusters"], 3)
        self.assertEqual(sum(row["n_observations"] for row in categories), 3)

    def test_sports_exact_phase_and_all_fourteen_window_edges(self):
        binding = self.normalized_sports_binding([{
            "market_id": "0xa", "sport": "epl", "game_key": "epl:123",
            "actual_start_seconds": 100000., "actual_end_seconds": 110800.,
            "timing_quality": "fixture", "provenance": "fixture"}])
        driver.prepare_sports_map(self.con, binding)
        elapsed = [-86401, -86400, -21601, -21600, -3601, -3600, -901, -900, -1,
                   0, 899, 900, 1799, 1800, 3599, 3600, 7199, 7200, 9000, 9900,
                   10500, 10800, 10801, 0]
        self.con.register("analysis_base", pa.table({"row_id": np.arange(len(elapsed)),
            "market_id": ["0xa"] * len(elapsed),
            "timestamp": np.array(elapsed) + 100000, "is_maker": [False] * 23 + [True],
            "event_cluster": ["native-claim-cluster"] * len(elapsed)}))
        driver.create_sports_view(self.con)
        admitted = self.con.execute("SELECT row_id,phase,event_cluster FROM sports_base ORDER BY row_id").fetchall()
        self.assertEqual([row[0] for row in admitted], list(range(22)))
        self.assertEqual([row[1] for row in admitted], ["pregame"] * 9 + ["in_play"] * 13)
        self.assertTrue(all(row[2] == "epl:123" for row in admitted))
        expected = {
            0: {"pre_lt24h"}, 1: {"pre_24to6h"}, 2: {"pre_24to6h"},
            3: {"pre_6to1h"}, 4: {"pre_6to1h"}, 5: {"pre_60to15m"}, 6: {"pre_60to15m"},
            7: {"pre_15to0m"}, 8: {"pre_15to0m"}, 9: {"live_0to15m"}, 10: {"live_0to15m"},
            11: {"live_15to30m"}, 12: {"live_15to30m"}, 13: {"live_30to60m"}, 14: {"live_30to60m"},
            15: {"live_1to2h"}, 16: {"live_1to2h"}, 17: {"live_2hplus", "final_60to30m"},
            18: {"live_2hplus", "final_30to15m"}, 19: {"live_2hplus", "final_15to5m"},
            20: {"live_2hplus", "final_5to0m"}, 21: {"live_2hplus", "final_5to0m"}}
        observed = {}
        for row_id, window in self.con.execute("SELECT row_id,clock_window FROM sports_windows").fetchall():
            observed.setdefault(row_id, set()).add(window)
        self.assertEqual(observed, expected)
        self.assertEqual(len(driver.WINDOWS), 14)

    def test_sports_cache_retains_after_end_audit_rows_and_detaches_raw_base(self):
        start = 1772323200
        binding = self.normalized_sports_binding([{
            "market_id": "0xa", "sport": "mlb", "game_key": "mlb:123",
            "actual_start_seconds": float(start), "actual_end_seconds": float(start + 7200),
            "timing_quality": "fixture", "provenance": "fixture"}])
        driver.prepare_sports_map(self.con, binding)
        self.con.register("analysis_base", pa.table({"market_id": ["0xa"] * 5,
            "source_month": ["2026-03"] * 5, "source_ordinal": np.arange(5),
            "timestamp": np.array([-1, 0, 7200, 7201, 0]) + start,
            "is_maker": [False] * 4 + [True], "event_cluster": ["native-event"] * 5,
            "P": [.05] * 5, "Y": [1] * 5, "bin": [1] * 5,
            "payoff": [95.] * 5, "roi": [1900.] * 5}))
        ledger = driver.ReadLedger()
        cache = driver.create_sports_view(self.con, self.folder, ledger, base_bytes=1000)
        self.assertEqual(cache["rows"], 4)
        self.assertEqual(cache["raw_source_count"], 4)
        self.assertTrue(cache["source_locator_retained"])
        self.assertTrue(cache["source_locator_unique"])
        self.assertEqual(cache["reconciliation"], {"joined_rows": 4, "admitted_rows": 3,
            "after_end_rows": 1, "pregame_rows": 1, "in_play_rows": 2})
        self.assertEqual(ledger.scans["sports_raw_join_count_and_locator_proof"]["charged_bytes"], 1000)
        self.assertEqual(ledger.scans["sports_observation_cache"]["charged_bytes"], 1000)
        self.assertEqual(ledger.scans["sports_cache_reconciliation"]["charged_bytes"], cache["bytes"])
        self.assertEqual(ledger.scans["sports_cache_phase_counts"]["charged_bytes"], cache["bytes"])
        self.con.unregister("analysis_base")
        self.assertEqual(self.con.execute("SELECT source_ordinal FROM sports_joined ORDER BY source_ordinal").fetchall(),
                         [(0,), (1,), (2,), (3,)])
        self.assertEqual(self.con.execute("SELECT source_ordinal,event_cluster FROM sports_base ORDER BY source_ordinal").fetchall(),
                         [(0, "mlb:123"), (1, "mlb:123"), (2, "mlb:123")])

    def test_sports_driver_guards_are_per_cell_and_complete_profile_grid_is_retained(self):
        records = []
        cells = ("D1:pregame", "D10:pregame", "D1:in_play", "D10:in_play")
        for index, cell in enumerate(cells):
            bin_number = 1 if cell.startswith("D1:") else 10
            phase = cell.split(":")[1]
            games = 29 if index == 0 else 30
            for game in range(games):
                n = 20 if index != 1 else (16 if game < 29 else 35)
                price = .05 if bin_number == 1 else .95
                for _ in range(n):
                    records.append({"event_cluster": f"game:{game}", "bin": bin_number,
                        "phase": phase, "P": price, "Y": game % 2,
                        "payoff": 100 * (game % 2 - price), "roi": 100 * (game % 2 / price - 1)})
        self.con.register("sports_fixture", pa.Table.from_pylist(records))
        result = driver.group_mean_result(self.con, "sports_fixture", cells,
                 "'D'||bin||':'||phase", sports=True)
        gaps = driver.gap_rows(result, scope="pooled", phases=["pregame", "in_play"], sports=True)
        self.assertEqual(len(gaps), 6)
        for row in gaps:
            if row["phase"] == "in_play":
                self.assertTrue(row["paper_support"])
                self.assertTrue(row["project_support"])
                self.assertFalse(row["suppressed"])
            else:
                self.assertFalse(row["paper_support"])
                self.assertFalse(row["project_support"])
                self.assertTrue(row["suppressed"])
                self.assertIsNone(row["CR0"])
        profile_cells = tuple(f"D{number}:{phase}" for phase in ("pregame", "in_play")
                              for number in range(1, 11))
        profiles = driver.profile_rows(driver.group_mean_result(self.con, "sports_fixture", profile_cells,
                      "'D'||bin||':'||phase", sports=True), scope="pooled",
                      phases=["pregame", "in_play"], sports=True)
        self.assertEqual(len(profiles), 40)
        self.assertTrue(all(row["suppressed"] and row["CR0"] is None
                            for row in profiles if row["bin"] in range(2, 10)))
        json.dumps({"profiles": profiles, "gaps": gaps}, allow_nan=False)

    def test_sports_preflight_requires_retained_result_and_token_proof_roles(self):
        roles = ("six_candidates", "six_timing", "nfl_moneylines", "nba_moneylines", "mlb_phase")
        inventory = []
        for role in roles:
            path = self.folder / (role + ".parquet")
            pq.write_table(pa.table({"fixture_only": [1]}), path)
            inventory.append({"role": role, "path": str(path), "sha256": build_inputs.sha256(path)})
        binding = {"schema_version": "kaushik_replication_sports_binding_v1",
            "sports": list(driver.SPORTS), "provider_cohort_admitted": True,
            "coverage_qualification": "synthetic fixture only", "retained_identity_result_timing_proof": True,
            "inputs": inventory}
        with self.assertRaises(build_inputs.InputBlocked):
            driver.sports_input_inventory(binding)

    def test_full_estimate_grid_common_samples_and_buy_role_partition(self):
        records, maps = [], []
        start_base = 1772323200  # 2026-03-01 UTC; all executions precede cutoff.
        for game in range(30):
            start = start_base + game * 3600
            maps.append({"market_id": f"market:{game}", "sport": "mlb", "game_key": f"mlb:{game}",
                "actual_start_seconds": float(start), "actual_end_seconds": float(start + 7200),
                "timing_quality": "fixture", "provenance": "synthetic fixture only"})
            lifespan = 5 + game % 6
            endpoint = start + 86400
            for phase in ("pregame", "in_play"):
                for number in range(1, 11):
                    price = (number - .5) / 10
                    for repeat in range(17):
                        elapsed = -3600 if phase == "pregame" else repeat * 400
                        timestamp = start + elapsed
                        payout = (repeat + number) % 2
                        remaining = (endpoint - timestamp) / 86400
                        records.append({"market_id": f"market:{game}", "claim_id": f"claim:{game}:{payout}",
                            "claim_code": game * 2 + payout, "event_cluster": f"event:{game}",
                            "cluster_source": "native_event", "timestamp": timestamp,
                            "endpoint_seconds": float(endpoint), "bin": number, "P": price, "Y": payout,
                            "is_maker": False, "side": "BUY" if repeat % 3 else "SELL",
                            "duration_eligible": True, "category": "Sports", "trade_month": "2026-03",
                            "L": float(lifespan), "R": remaining, "xL": np.log2(1 + lifespan),
                            "xR": np.log2(1 + remaining), "payoff": 100 * (payout - price),
                            "roi": 100 * (payout / price - 1)})
        # Same-source maker rows affect A2 BUY counts, while every primary table
        # keeps its original taker population. Their recorded BUY claim is retained.
        for row in records[:25]:
            records.append({**row, "is_maker": True, "side": "BUY"})
        self.con.register("analysis_base", pa.Table.from_pylist(records))
        output = driver.estimate_all(self.con, self.folder,
            {"exclusions": {}, "rows": {"primary_taker": 10200, "baseline_all_roles": 10225}},
            self.normalized_sports_binding(maps))
        self.assertEqual(output["sample"]["rows"], 10200)
        self.assertEqual(output["sample"]["unique_event_clusters"], 30)
        self.assertEqual(output["sample"]["market_fallback_clusters"], 0)
        self.assertEqual([item["joint"]["metadata"]["n"] for item in output["table2"]], [2040] * 5)
        self.assertEqual([item["joint"]["metadata"]["n"] for item in output["table3"]["L_gt1"]], [2040] * 3)
        self.assertEqual([item["joint"]["metadata"]["n"] for item in output["table3"]["R_gt1"]], [1020] * 3)
        self.assertEqual(output["appendix_a1"]["claim_support"]["claims"], 60)
        self.assertEqual(output["appendix_a1"]["claim_support"]["observations"], 2040)
        self.assertEqual(output["appendix_a1"]["claim_support"]["both_tail_claims"], 60)
        a2 = {item["convention"]: item for item in output["appendix_a2"]}
        self.assertEqual(a2["taker_direction"]["counts"]["rows"], 10200)
        self.assertEqual(a2["maker_buy"]["counts"]["rows"], 25)
        self.assertEqual(a2["all_buy"]["counts"]["rows"],
                         a2["maker_buy"]["counts"]["rows"] + a2["taker_buy"]["counts"]["rows"])
        sports = output["sports"]
        self.assertEqual(len(sports["phase_rows"]), 60)
        self.assertEqual(len(sports["profile_rows"]), 400)
        self.assertEqual(len(sports["window_rows"]), 280)
        expected_scopes = {"pooled", *driver.SPORTS}
        for key in ("phase_rows", "profile_rows", "window_rows"):
            self.assertEqual({item["scope"] for item in sports[key]}, expected_scopes)
            self.assertTrue(all(item["suppressed"] and item["CR0"] is None for item in sports[key]
                                if item["scope"] not in ("pooled", "mlb")))
        self.assertTrue(all(not item["suppressed"] and item["paper_support"] and item["project_support"]
                            for item in sports["profile_rows"] if item["scope"] in ("pooled", "mlb")))
        json.dumps(output, allow_nan=False)

    def test_original_provider_proof_retains_shared_games_and_audits_unadmitted_pairs(self):
        begin = datetime(2026, 3, 1, 12, tzinfo=timezone.utc)
        end = datetime(2026, 3, 1, 14, tzinfo=timezone.utc)
        clock = pa.timestamp("us", tz="UTC")
        candidate_schema = pa.schema([("market_id", pa.string()), ("sport", pa.string()), ("event_slug", pa.string())])
        timing_schema = pa.schema([("sport", pa.string()), ("event_slug", pa.string()), ("game_id", pa.string()),
            ("actual_start_utc", clock), ("actual_end_utc", clock), ("timing_quality", pa.string())])
        proof_schema = pa.schema([("sport", pa.string()), ("event_slug", pa.string()), ("eligible", pa.bool_()),
            ("match_exclusion_reason", pa.string()), ("timing_exclusion_reason", pa.string())])
        token_schema = pa.schema([("market_id", pa.string()), ("token_id", pa.string()), ("outcome", pa.string()), ("won", pa.bool_())])
        legacy_schema = pa.schema([("market_id", pa.string()), ("game_id", pa.string()),
            ("actual_start_utc", clock), ("actual_end_utc", clock), ("winning_token_id", pa.string())])
        mlb_schema = pa.schema([("market_id", pa.string()), ("game_pk", pa.int64()),
            ("actual_start_utc", clock), ("actual_end_utc", clock), ("winning_token_id", pa.string())])
        candidates = [{"market_id": market, "sport": "epl", "event_slug": "native-epl-game"}
                      for market in ("draw", "home", "absent")]
        token_records, claims = [], []
        for market in ("draw", "home", "absent"):
            winner = "No" if market == "home" else "Yes"
            for label in ("Yes", "No"):
                payout = label == winner
                token = market + ":" + label
                token_records.append({"market_id": market, "token_id": token, "outcome": label, "won": payout})
                if market != "absent":
                    claims.append({"market_id": market, "token_id": token, "Y": float(payout), "outcome_label": label})
        self.con.register("claims", pa.Table.from_pylist(claims))
        roles = {
            "six_candidates": (candidates, candidate_schema),
            "six_timing": ([{"sport": "epl", "event_slug": "native-epl-game", "game_id": "game123",
                            "actual_start_utc": begin, "actual_end_utc": end, "timing_quality": "fixture"}], timing_schema),
            "six_proof": ([{"sport": "epl", "event_slug": "native-epl-game", "eligible": True,
                           "match_exclusion_reason": None, "timing_exclusion_reason": None}], proof_schema),
            "six_tokens": (token_records, token_schema), "nfl_moneylines": ([], legacy_schema),
            "nba_moneylines": ([], legacy_schema), "mlb_phase": ([], mlb_schema),
        }
        inventory = []
        for role, (records, schema) in roles.items():
            path = self.folder / ("original_" + role + ".parquet")
            pq.write_table(pa.Table.from_pylist(records, schema=schema), path)
            inventory.append({"role": role, "path": str(path)})
        coverage = driver.prepare_sports_map(self.con, {"inputs": inventory}, self.folder)
        self.assertEqual(coverage[0]["games"], 1)
        self.assertEqual(coverage[0]["markets"], 2)
        admitted = self.con.execute("SELECT market_id,game_key FROM sports_market_map ORDER BY market_id").fetchall()
        self.assertEqual(admitted, [("draw", "epl:game123"), ("home", "epl:game123")])
        excluded = pq.read_table(self.folder / "sports_metadata_exclusions.parquet").to_pylist()
        self.assertEqual(excluded, [{"market_id": "absent", "sport": "epl", "reason": "no_admitted_native_binary_claim_pair"}])
        # A contradictory provider payout never changes a native payout or the
        # admitted pair. The affected condition is saved as an explicit exclusion.
        for record in token_records:
            if record["market_id"] == "home":
                record["won"] = not record["won"]
        pq.write_table(pa.Table.from_pylist(token_records, schema=token_schema),
                       self.folder / "original_six_tokens.parquet")
        with duckdb.connect() as contradictory:
            contradictory.register("claims", pa.Table.from_pylist(claims))
            driver.prepare_sports_map(contradictory, {"inputs": inventory})
            self.assertEqual(contradictory.execute("SELECT market_id FROM sports_market_map").fetchall(), [("draw",)])
            self.assertEqual(contradictory.execute("SELECT reason FROM sports_metadata_exclusions WHERE market_id='home'").fetchone()[0],
                             "provider_native_resolution_disagreement")


class IndependentResourceQA(unittest.TestCase):
    def test_sparse_spill_reserves_unallocated_bytes_and_full_free_floor(self):
        with tempfile.TemporaryDirectory() as temporary:
            stage = Path(temporary)
            (stage / "spill").mkdir()
            sparse = stage / "spill" / "sparse.fixture"
            with sparse.open("xb") as stream:
                stream.write(b"x")
            # Some fixture filesystems allocate seek-created holes eagerly.
            # Explicit sparse stat metadata makes the guard oracle portable.
            original_stat = Path.stat
            def sparse_stat(path, *args, **kwargs):
                stat = original_stat(path, *args, **kwargs)
                if path == sparse:
                    return SimpleNamespace(st_size=8_000_000, st_blocks=8, st_mode=stat.st_mode)
                return stat
            allocated = 4096
            caps = {"maximum_output_bytes": 8_000,
                    "maximum_transient_file_bytes": 16_000,
                    "spill_bytes": 16_000_000, "minimum_free_bytes": 20_000}
            required = 8_000 + 16_000 + 16_000_000 - allocated + 20_000
            capacity = SimpleNamespace(free=required)
            with patch.dict(driver.CAPS, caps), \
                    patch.object(Path, "stat", sparse_stat), \
                    patch.object(driver.shutil, "disk_usage", return_value=capacity):
                driver.reserve_output(stage, 3_000)
                capacity.free -= 1
                with self.assertRaisesRegex(ValueError, "free-space"):
                    driver.reserve_output(stage, 3_000)
                # Logical sparse length cannot be credited as occupied spill.
                capacity.free = 8_000 + 16_000 + 16_000_000 - 8_000_000 + 20_000
                with self.assertRaisesRegex(ValueError, "free-space"):
                    driver.reserve_output(stage, 3_000)

    def test_cumulative_post_copy_overshoot_blocks_hash_and_downstream_use(self):
        with tempfile.TemporaryDirectory() as temporary:
            stage = Path(temporary)
            output = stage / "cache.parquet"
            con = duckdb.connect()
            class SyntheticCopy:
                def execute(self, query):
                    con.execute(query)
                    (stage / "other-accepted.fixture").write_bytes(b"x" * 4096)
                    return self
                def fetchone(self):
                    return (1,)
            try:
                caps = {"maximum_output_bytes": 4096,
                        "maximum_transient_file_bytes": 16384,
                        "spill_bytes": 16384, "minimum_free_bytes": 0}
                with patch.dict(driver.CAPS, caps), \
                        patch.object(driver, "artifact_info") as inspect:
                    with self.assertRaisesRegex(ValueError, "output capacity"):
                        driver.copy_parquet(SyntheticCopy(), "SELECT 1 fixture", output, ceiling=3000)
                    inspect.assert_not_called()
                self.assertLessEqual(output.stat().st_size, 3000)
                self.assertGreater(sum(p.stat().st_size for p in stage.iterdir()), 4096)
                self.assertFalse((stage / "acceptance.json").exists())
            finally:
                con.close()

    def test_score_actual_size_is_checked_before_hash_or_reopen(self):
        with tempfile.TemporaryDirectory() as temporary:
            stage = Path(temporary)
            output = stage / "qa_scores.parquet"
            result = SimpleNamespace(metadata={"cluster_levels": ["g0", "g1"]},
                names=("payoff_cents|intercept",),
                coefficient_cluster_influence=np.array([[1.0], [-1.0]]))
            original_stat = Path.stat
            def oversize_stat(path, *args, **kwargs):
                stat = original_stat(path, *args, **kwargs)
                if path == output:
                    return SimpleNamespace(st_size=1_000_000_001, st_blocks=stat.st_blocks)
                return stat
            def tiny_writer(table, path, **kwargs):
                Path(path).write_bytes(b"x")
            artifacts = {}
            with patch.dict(driver.CAPS, {"maximum_transient_file_bytes": 16384,
                    "spill_bytes": 16384, "minimum_free_bytes": 0}), \
                    patch.object(driver.pq, "write_table", side_effect=tiny_writer), \
                    patch.object(Path, "stat", oversize_stat), \
                    patch.object(driver, "artifact_info") as inspect, \
                    patch.object(driver.pq, "read_table") as reopen:
                with self.assertRaisesRegex(ValueError, "score artifact exceeds accepted per-file ceiling"):
                    driver.compact_joint(result, stage, "qa", artifacts, driver.ReadLedger())
                inspect.assert_not_called()
                reopen.assert_not_called()
            self.assertEqual(artifacts, {})
            self.assertFalse((stage / "acceptance.json").exists())


class IndependentReportQA(unittest.TestCase):
    """Saved-source and in-memory artist checks; no PDF authoring/compilation."""

    @classmethod
    def setUpClass(cls):
        from tests.test_kaushik_replication_driver import fixture
        cls.temporary = tempfile.TemporaryDirectory()
        cls.con = duckdb.connect()
        records, manifest, binding = fixture(cls.con, cls.temporary.name)
        tally = Counter((row["is_maker"], row["side"]) for row in records)
        manifest["exclusions"] = {
            "2025-02": [{"reason": "eligible", "side": side, "is_maker": maker,
                         "rows": n, "precut_rows": n}
                        for (maker, side), n in tally.items()],
            "2025-01": [{"reason": "invalid_size", "side": "SELL", "is_maker": False,
                         "rows": 11, "precut_rows": 7}],
        }
        manifest["rows"]["source"] += 11
        manifest["rows"]["excluded"] += 11
        with patch.object(driver, "reserve_output"), contextlib.redirect_stdout(io.StringIO()):
            cls.saved = driver.estimate_all(cls.con, Path(cls.temporary.name)/"out", manifest, binding)
        cls.source = report.render_source(cls.saved)

    @classmethod
    def tearDownClass(cls):
        cls.con.close()
        cls.temporary.cleanup()

    def test_actual_driver_saved_counts_exclusions_bins_and_signed_se_map_to_source(self):
        sample = self.saved["sample"]
        for key, label in (("rows", "Baseline records"), ("conditions", "Conditions"),
                           ("normalized_claims", "Normalized claims"), ("clusters", "Event/market clusters"),
                           ("tail_rows", "Baseline D1 + D10 records"),
                           ("duration_tail_rows", "Duration-eligible tail records")):
            self.assertIn(label+" & "+report.count(sample[key]), self.source)
        self.assertIn("invalid size & Taker & SELL & 7", self.source)
        self.assertNotIn("invalid size & Taker & SELL & 11", self.source)
        for category in self.saved["categories"]:
            self.assertIn(report.tex(category["category"])+" & "+report.count(category["n_observations"]), self.source)
        profile = {(row["outcome"], row["bin"]): row for row in self.saved["table1"]["profile_rows"]}
        for bin_ in range(1, 11):
            payoff, roi = [profile[(target, bin_)] for target in report.OUTCOMES]
            range_ = r"$0<P<.10$" if bin_ == 1 else rf"${(bin_-1)/10:.2f}\leq P<{bin_/10:.2f}$"
            expected = " & ".join(("D"+str(bin_), range_, report.number(100*payoff["mean_price"]) if payoff["mean_price"] is not None else "withheld",
                report.cell(payoff), report.cell(roi), report.count(payoff["n_observations"]), report.count(payoff["n_clusters"])))
            self.assertIn(expected, self.source)
        for row in self.saved["table1"]["gap_rows"]:
            self.assertIn(report.cell(row), self.source)
            self.assertIn(report.se(row), self.source)
        self.assertEqual(self.source.count(r"\includegraphics"), 21)
        self.assertNotIn("https://", self.source)

    def test_actual_grouped_r_squared_keys_preserve_target_and_tail_units(self):
        for target in report.OUTCOMES:
            source = report._model_table(self.saved, target)
            for group in ("D1", "D10", "stacked"):
                values = []
                for model in self.saved["table2"]:
                    grouped = model["joint"]["metadata"]["grouped_r_squared"]
                    value = (grouped["stacked_weighted_tss_r_squared_by_target"][target] if group == "stacked"
                             else grouped["group_summaries"][group]["r_squared_including_fixed_effects_by_target"][target])
                    values.append(report.number(value, 4))
                self.assertIn(r"$R^2$: "+group+" & "+" & ".join(values), source)
            for model in self.saved["table2"]:
                for row in model["slopes"]:
                    if row["outcome"] == target:
                        self.assertIn(report.cell(row), source)
                        self.assertIn(report.se(row), source)
        self.assertIn(r"\log_2(1+\mathrm{days})", self.source)
        self.assertIn("overall outcome mean", self.source)

    def test_primary_cr0_estimate_se_and_signed_units_are_not_rescaled_or_adjusted(self):
        from tests.test_kaushik_replication_report import synthetic_estimates
        saved = synthetic_estimates()
        for row in saved["table2"][0]["slopes"]:
            estimate = -12.345 if row["outcome"] == "payoff_cents" else -1234.567
            row["CR0"].update(estimate=estimate, standard_error=.45675,
                              ci95_low=estimate-1, ci95_high=estimate+1)
            row["cluster_count_adjusted"].update(estimate=estimate, standard_error=99.99,
                                                ci95_low=estimate-200, ci95_high=estimate+200)
        for target, expected in (("payoff_cents", "-12.35"), ("roi_percent", "-1234.57")):
            source = report._model_table(saved, target)
            self.assertTrue("Original duration & "+expected+" &" in source,
                            "saved signed slope must retain its cents/pp units")
            self.assertTrue(" & (0.46) &" in source, "display must use primary CR0 SE")
            self.assertNotIn("(99.99)", source)

    def test_both_sports_guards_and_numerical_reasons_remain_distinct(self):
        from tests.test_kaushik_replication_report import synthetic_estimates, suppress
        value = synthetic_estimates()
        for row in value["sports"]["profile_rows"]:
            if row["scope"] == "mlb" and row["phase"] == "pregame" and row["bin"] in (1, 2):
                if row["bin"] == 1:
                    row.update(n_observations=499, paper_support=True, project_support=False)
                    row["cell_support"][0]["n_observations"] = 499
                else:
                    row.update(n_clusters=29, paper_support=False, project_support=True)
                    row["cell_support"][0]["n_clusters"] = 29
                suppress(row)
        source = report.render_source(value)
        self.assertIn("Pre & D1 & 499 & 40 & withheld & withheld & withheld: <500 records", source)
        self.assertIn("Pre & D2 & 500 & 29 & withheld & withheld & withheld: <30 games", source)
        self.assertNotIn("Pre & D1 & 499 & 40 & +0.00", source)

    def test_withheld_baseline_and_duration_values_show_saved_numerical_reason(self):
        from tests.test_kaushik_replication_report import suppress
        saved = deepcopy(self.saved)
        suppress(saved["table1"]["gap_rows"][0], "contrast_variance_unrepresentable")
        suppress(saved["table2"][0]["slopes"][0], "unidentified_clock")
        source = report.render_source(saved)
        self.assertTrue("contrast variance unrepresentable" in source, "baseline numerical reason is absent")
        self.assertTrue("unidentified clock" in source, "duration numerical reason is absent")

    def test_paired_outcome_support_tables_preserve_both_numerical_reasons(self):
        from tests.test_kaushik_replication_report import synthetic_estimates, suppress
        saved = synthetic_estimates()
        profiles = [row for row in saved["sports"]["profile_rows"] if row["scope"] == "mlb"]
        windows = [row for row in saved["sports"]["window_rows"] if row["scope"] == "mlb"]
        buys = {(row["convention"],): row for row in saved["appendix_a2"]}
        for rows, predicate in ((profiles, lambda row: row["phase"] == "pregame" and row["bin"] == 1),
                                (windows, lambda row: row["window"] == "pre_lt24h"),
                                (buys[("all_buy",)]["profile_rows"], lambda row: row["bin"] == 1)):
            for row in rows:
                if predicate(row):
                    suppress(row, "payoff_variance_unrepresentable" if row["outcome"] == "payoff_cents"
                             else "roi_variance_unrepresentable")
        rendered = (report._support_profile(profiles, "Profile support"),
                    report._support_windows(windows, "Window support"), report._support_buy(buys))
        for index, source in enumerate(rendered):
            for reason in ("payoff variance unrepresentable", "roi variance unrepresentable"):
                self.assertTrue(reason in source, f"support table {index} hides {reason}")

    def test_support_game_minima_are_not_union_counts(self):
        saved = deepcopy(self.saved)
        windows = [row for row in saved["sports"]["window_rows"] if row["scope"] == "pooled"]
        payoff = next(row for row in windows if row["outcome"] == "payoff_cents" and row["window"] == "pre_lt24h")
        payoff["cell_support"] = [{"cell": "D1", "n_observations": 11, "n_clusters": 3},
                                  {"cell": "D10", "n_observations": 13, "n_clusters": 5}]
        payoff.update(n_observations=24, n_clusters=3, contributing_cluster_minimum=3)
        source = report._support_windows(windows, "Saved windows")
        self.assertIn("<-24h & 11 & 13 & 3 & withheld", source)
        self.assertIn("smaller tail game count", source)
        self.assertIn("least populated of four phase/tail game cells", self.source)

    def test_report_defines_probability_payoff_return_and_a1_regressors(self):
        for phrase in ("percentage points", "payout"):
            self.assertTrue(phrase in self.source, "definition absent: "+phrase)
        self.assertTrue("claim price" in self.source or "normalized dollar price" in self.source,
                        "the probability/price P needs a definition")
        self.assertTrue("no categorical effects" in self.source or "Raw columns are unadjusted" in self.source,
                        "raw needs the no-FE meaning, not a linear-day meaning")
        self.assertTrue("H=1" in self.source or "H = 1" in self.source)
        self.assertTrue("x_L=" in self.source or "x_L =" in self.source)
        self.assertTrue("x_R=" in self.source or "x_R =" in self.source)
        self.assertTrue("t<s" in self.source or "t < s" in self.source)
        self.assertIn("scheduled start", self.source)
        self.assertIn("approximate", self.source)
        self.assertIn("including EPL draws", self.source)
        self.assertIn("no Kalshi match", self.source)
        self.assertIn("no added date bound", self.source.replace("or added date bound", "no added date bound"))

    def test_table5_observed_games_are_labeled_apart_from_provider_games_and_cell_minima(self):
        saved = deepcopy(self.saved)
        for row in saved["sports"]["coverage"]:
            row["games"] += 999
        source = report.render_source(saved)
        start = source.index("Table 5.")
        end = source.index(r"\end{table}", start)
        section = source[start:end]
        self.assertTrue("Provider games" in source or "provider-covered eligible games" in source)
        self.assertTrue("Observed games" in section and "all-band archive records" in section,
                        "observed games need a population label")
        observed = {row["scope"]: row["n_games"] for row in saved["sports"]["scope_counts"]}
        for sport in report.SPORTS:
            self.assertTrue(sport.upper()+" & "+report.count(observed[sport])+" &" in section,
                            "Table 5 must use observed, not provider-eligible, games")
        self.assertIn(r"$G_{\min}$", section)

    def test_profile_and_window_artists_use_saved_asymmetric_intervals_without_paths(self):
        from tests.test_kaushik_replication_report import synthetic_estimates, suppress
        saved = synthetic_estimates()
        for row in saved["sports"]["profile_rows"]:
            if row["scope"] == "mlb" and row["phase"] == "pregame" and row["bin"] == 2:
                suppress(row)
            if row["scope"] == "mlb" and row["outcome"] == "payoff_cents" and row["phase"] == "pregame" and row["bin"] == 1:
                row["CR0"].update(estimate=-12.3, ci95_low=-15.8, ci95_high=-11.0)
        observed = []
        def inspect_figure(fig, path):
            axes = []
            for axis in fig.axes:
                series = []
                for container in axis.containers:
                    line, caps, bars = container.lines
                    series.append((list(line.get_xdata()), list(line.get_ydata()), line.get_linestyle(),
                                   len(caps), bars[0].get_segments()))
                axes.append(series)
            observed.append(axes)
        with patch.object(report, "_save_figure", side_effect=inspect_figure), patch.dict(os.environ, {"MPLCONFIGDIR": self.temporary.name}):
            report.plot_profile(saved, "mlb", Path(self.temporary.name)/"unused_profile.pdf")
            report.plot_windows(saved, "mlb", Path(self.temporary.name)/"unused_windows.pdf")
        first = observed[0][0][0]
        np.testing.assert_allclose(first[0], np.array([1, *range(3, 11)])-.10)
        self.assertEqual(first[1][0], -12.3)
        np.testing.assert_allclose(first[4][0], [[.9, -15.8], [.9, -11.0]])
        for axes in observed:
            for series in axes:
                for line in series:
                    self.assertEqual(line[2], "None")
                    self.assertEqual(line[3], 2)
        self.assertFalse((Path(self.temporary.name)/"unused_profile.pdf").exists())
        self.assertFalse((Path(self.temporary.name)/"unused_windows.pdf").exists())


if __name__ == "__main__":
    unittest.main()
