"""Tiny synthetic numerical tests; no production inputs, network or real scans."""
from __future__ import annotations

import json
import unittest

import numpy as np

from analysis.kaushik_polymarket_replication.estimators import (
    ClusterScoreMoments, RegressionMoments, a1_claim_design,
    absorb_categorical_effects, contrast_vector, finalize_clustered_regression,
    fit_ols_moments, fixed_probability_bins, joint_clustered_means_from_totals,
    grouped_r_squared_from_sufficient_statistics, joint_tail_phase_contrasts,
    paper_time_controls, payoff_and_roi,
    r_squared_from_sufficient_statistics, tail_varying_design,
)


def batches(x, y, codes, weights=None, size=37):
    """A replayable bounded fixture source, with the same contract as the driver."""
    def replay():
        for begin in range(0, len(x), size):
            end = begin + size
            yield {"x": x[begin:end], "y": y[begin:end],
                   "codes": {name: values[begin:end] for name, values in codes.items()},
                   "weights": weights[begin:end] if weights is not None else None}
    return replay


def fit_batches(x, y, names, targets, clusters, *, absorber=None,
                codes=None, weights=None):
    moments = RegressionMoments(names, targets)
    source = batches(x, y, codes or {}, weights)
    for batch in source():
        values = (absorber.transform(batch) if absorber else
                  (batch["x"], batch["y"], batch["weights"]))
        moments.add(*values)
    fit = fit_ols_moments(moments, absorption=absorber)
    scores = ClusterScoreMoments(len(names) * len(targets))
    if fit.beta is not None:
        offset = 0
        for batch in source():
            values = (absorber.transform(batch) if absorber else
                      (batch["x"], batch["y"], batch["weights"]))
            n = len(batch["x"])
            scores.add(clusters[offset:offset + n], fit.score_batch(*values))
            offset += n
    return fit, finalize_clustered_regression(fit, scores), moments


class PrimitiveTests(unittest.TestCase):
    def test_payoff_roi_and_endpoints_without_trimming(self):
        price = np.array([0.0, .05, .95, 1.0])
        payoff, roi = payoff_and_roi(price, [1, 1, 0, 1])
        np.testing.assert_allclose(payoff, [100, 95, -95, 0])
        self.assertTrue(np.isnan(roi[0]))
        np.testing.assert_allclose(roi[1:], [1900, -100, 0])
        with self.assertRaises(ValueError):
            payoff_and_roi([1.01], [1])
        with self.assertRaises(ValueError):
            payoff_and_roi([.5], [2])

    def test_fixed_width_bins_and_binary64_boundaries(self):
        edges = np.array([0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1])
        np.testing.assert_array_equal(fixed_probability_bins(edges), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9])
        np.testing.assert_array_equal(fixed_probability_bins(np.nextafter(edges[1:-1], 0)), np.arange(9))
        with self.assertRaises(ValueError):
            fixed_probability_bins([np.nan])

    def test_time_and_fully_tail_varying_design(self):
        controls = paper_time_controls([3, 7], [1, 0])
        np.testing.assert_array_equal(controls["original_log2_days"], [2, 3])
        np.testing.assert_array_equal(controls["remaining_log2_days"], [1, 0])
        x, names = tail_varying_design([[2, 1], [3, 0]], ["xL", "xR"], [0, 1])
        self.assertEqual(names, ("D1:level", "D1:xL", "D1:xR", "D10:level", "D10:xL", "D10:xR"))
        np.testing.assert_array_equal(x, [[1, 2, 1, 0, 0, 0], [0, 0, 0, 1, 3, 0]])
        with self.assertRaises(ValueError):
            paper_time_controls([1], [2])

    def test_a1_omits_absorbed_standalone_lifespan(self):
        x, names = a1_claim_design([0, 1], [2, 2], [1, .5])
        self.assertEqual(names, ("H", "H:xL", "xR", "H:xR"))
        np.testing.assert_array_equal(x, [[0, 0, 1, 0], [1, 2, .5, .5]])


class JointMeanTests(unittest.TestCase):
    def fixture(self):
        cells = ("D1:early", "D10:early", "D1:late", "D10:late")
        # Common event shocks create deliberately nonzero cross-tail covariance.
        values = np.array([[-5, 5, -2, 8], [-2, 9, 1, 14], [-1, 12, 4, 15], [2, 13, 6, 19]], dtype=float)
        records = []
        for event, row in enumerate(values):
            for cell, value in zip(cells, row):
                records.append({"cluster": f"event-{event}", "cell": cell, "count": 20,
                                "weight_sum": 20.0, "weighted_sums": [20 * value, 40 * value + 20]})
        return cells, values, records

    def test_joint_covariance_spreads_and_phase_changes(self):
        cells, values, records = self.fixture()
        fitted = joint_clustered_means_from_totals(records, cells=cells,
                                                 target_names=["payoff_cents", "roi_percent"])
        mean = values.mean(axis=0)
        scalar = (values - mean) / 4
        all_scores = np.column_stack((scalar, 2 * scalar))
        np.testing.assert_allclose(fitted.covariance, all_scores.T @ all_scores)
        vectors = joint_tail_phase_contrasts(fitted.names, target="payoff_cents", phases=["early", "late"])
        contrast = vectors["tail_spread_change:late_minus_early"]
        reported = fitted.contrast(contrast)
        expected_scalar = scalar @ np.array([1, -1, -1, 1])
        self.assertAlmostEqual(reported["CR0"]["estimate"], float(mean @ [1, -1, -1, 1]))
        self.assertAlmostEqual(reported["CR0"]["standard_error"] ** 2, float(expected_scalar @ expected_scalar))
        self.assertAlmostEqual(reported["cluster_count_adjusted"]["standard_error"] ** 2,
                               float(expected_scalar @ expected_scalar) * 4 / 3)
        independent_wrong = float(np.diag(fitted.covariance)[:4].sum())
        self.assertNotAlmostEqual(independent_wrong, reported["CR0"]["standard_error"] ** 2)
        self.assertIn("effective_clusters_below_30", reported["influence"]["flags"])
        self.assertIn("cluster_variance_share_above_25_percent", reported["influence"]["flags"])
        output = fitted.to_dict(vectors)
        json.dumps(output, allow_nan=False)
        self.assertFalse(output["variance_convention"]["full_N_minus_k_CR1_used"])
        self.assertEqual(output["metadata"]["n_by_cell"]["D1:early"], 80)

    def test_sports_support_and_absent_cells_are_null(self):
        cells, _, records = self.fixture()
        fitted = joint_clustered_means_from_totals(records, cells=cells + ("missing",),
                                                 target_names=["a", "b"], min_clusters=30)
        output = fitted.to_dict()
        self.assertTrue(all(row["suppressed"] for row in output["estimates"]))
        self.assertIsNone(output["covariance_CR0"][0][0])
        self.assertIsNone(output["estimates"][0]["CR0"])
        self.assertIn("empty_cell", output["estimates"][4]["suppression_reasons"])
        json.dumps(output, allow_nan=False)

    def test_aggregation_is_additive_and_invalid_records_fail(self):
        cells, _, records = self.fixture()
        split = []
        for record in records:
            for _ in range(2):
                split.append({**record, "count": 10, "weight_sum": 10,
                              "weighted_sums": np.asarray(record["weighted_sums"]) / 2})
        first = joint_clustered_means_from_totals(records, cells=cells, target_names=["a", "b"])
        second = joint_clustered_means_from_totals(split, cells=cells, target_names=["a", "b"])
        np.testing.assert_array_equal(first.values, second.values)
        np.testing.assert_array_equal(first.covariance, second.covariance)
        with self.assertRaises(MemoryError):
            joint_clustered_means_from_totals(records, cells=cells, target_names=["a", "b"], memory_limit_bytes=1)
        with self.assertRaises(ValueError):
            joint_clustered_means_from_totals([{**records[0], "cluster": None}], cells=cells, target_names=["a", "b"])


class StreamingRegressionTests(unittest.TestCase):
    def fixture(self):
        rng = np.random.default_rng(718)
        n = 480
        codes = {"category_tail": rng.integers(0, 6, n),
                 "exact_price": rng.integers(0, 11, n),
                 "utc_month_tail": rng.integers(0, 8, n)}
        x = rng.normal(size=(n, 4))
        weights = 1 + rng.random(n)
        z = np.column_stack([np.eye(int(code.max()) + 1)[code] for code in codes.values()])
        y = x @ np.array([[2, 4], [-1, 2], [3, -2], [.5, 1]]) + z @ rng.normal(size=(z.shape[1], 2))
        y += rng.normal(size=(n, 2))
        return x, y, codes, z, weights, np.arange(n) % 40

    def test_absorption_matches_independent_dense_fe_and_joint_cluster_covariance(self):
        x, y, codes, z, weights, clusters = self.fixture()
        names = ("D1:xL", "D1:xR", "D10:xL", "D10:xR")
        targets = ("payoff_cents", "roi_percent")
        absorber = absorb_categorical_effects(batches(x, y, codes, weights), term_names=names,
            target_names=targets, level_counts={name: int(code.max()) + 1 for name, code in codes.items()})
        self.assertTrue(absorber.converged)
        fit, result, _ = fit_batches(x, y, names, targets, clusters, absorber=absorber, codes=codes, weights=weights)
        root_w = np.sqrt(weights)[:, None]
        zx = np.linalg.lstsq(z * root_w, x * root_w, rcond=None)[0]
        zy = np.linalg.lstsq(z * root_w, y * root_w, rcond=None)[0]
        x_res, y_res = x - z @ zx, y - z @ zy
        expected_beta = np.linalg.lstsq(x_res * root_w, y_res * root_w, rcond=None)[0]
        np.testing.assert_allclose(fit.beta, expected_beta, atol=1e-9)
        residue = y_res - x_res @ expected_beta
        bread = np.linalg.inv(x_res.T @ (weights[:, None] * x_res))
        row_scores = (residue[:, :, None] * x_res[:, None, :] * weights[:, None, None]).reshape(len(x), -1)
        scores = np.array([row_scores[clusters == event].sum(axis=0) for event in range(40)])
        expected_influence = scores @ np.kron(np.eye(2), bread)
        np.testing.assert_allclose(result.covariance, expected_influence.T @ expected_influence, atol=1e-10)
        tss = ((y - np.average(y, weights=weights, axis=0)) ** 2 * weights[:, None]).sum(axis=0)
        sse = (residue ** 2 * weights[:, None]).sum(axis=0)
        np.testing.assert_allclose(fit.r_squared, 1 - sse / tss)
        vector = contrast_vector(result.names, {"payoff_cents|D10:xL": 1, "payoff_cents|D1:xL": -1})
        contrast = result.contrast(vector)
        self.assertAlmostEqual(contrast["CR0"]["estimate"], expected_beta[2, 0] - expected_beta[0, 0])
        self.assertAlmostEqual(contrast["cluster_count_adjusted"]["standard_error"] ** 2 /
                               contrast["CR0"]["standard_error"] ** 2, 40 / 39)
        output = result.to_dict({"tail_slope": vector})
        self.assertIsNone(output["metadata"]["grouped_r_squared"])
        self.assertEqual(output["metadata"]["cluster_count"], 40)
        json.dumps(output, allow_nan=False)

    def test_claim_fe_a1_equals_independent_within_claim_fit(self):
        rng = np.random.default_rng(115)
        claim = np.repeat(np.arange(60), 8)
        h, remaining = rng.integers(0, 2, len(claim)), rng.uniform(0, 1, len(claim))
        lifespan = np.log2(1 + 2 + claim % 7)
        x, names = a1_claim_design(h, lifespan, remaining)
        y = x @ np.array([1, -2, 3, 4]) + claim * .2 + rng.normal(size=len(claim))
        codes = {"claim": claim}
        absorber = absorb_categorical_effects(batches(x, y, codes), term_names=names,
            target_names=["payoff_cents"], level_counts={"claim": 60})
        self.assertTrue(absorber.converged)
        fit, result, _ = fit_batches(x, y, names, ["payoff_cents"], claim // 2, absorber=absorber, codes=codes)
        xr, yr = x.copy(), y.copy()
        for value in np.unique(claim):
            selected = claim == value
            xr[selected] -= x[selected].mean(axis=0)
            yr[selected] -= y[selected].mean()
        expected = np.linalg.lstsq(xr, yr, rcond=None)[0]
        np.testing.assert_allclose(fit.beta[:, 0], expected, atol=1e-9)
        self.assertEqual(result.metadata["cluster_count"], 30)
        # Standalone claim-constant xL must not become an identified dust column.
        constant_x = np.column_stack((x, lifespan))
        constant_absorber = absorb_categorical_effects(batches(constant_x, y, codes),
            term_names=[*names, "xL"], target_names=["payoff_cents"], level_counts={"claim": 60})
        constant_fit, constant_result, _ = fit_batches(constant_x, y, [*names, "xL"],
            ["payoff_cents"], claim // 2, absorber=constant_absorber, codes=codes)
        self.assertIn("rank_deficient_residualized_design", constant_fit.suppressed_reasons)
        self.assertTrue(constant_result.to_dict()["estimates"][0]["suppressed"])

    def test_nonconvergence_rank_and_score_population_gates(self):
        x, y, codes, _, weights, clusters = self.fixture()
        names, targets = ["a", "b", "c", "d"], ["payoff_cents", "roi_percent"]
        absorber = absorb_categorical_effects(batches(x, y, codes, weights), term_names=names,
            target_names=targets, level_counts={name: int(code.max()) + 1 for name, code in codes.items()},
            max_iterations=1)
        self.assertFalse(absorber.converged)
        fit, result, _ = fit_batches(x, y, names, targets, clusters, absorber=absorber, codes=codes, weights=weights)
        self.assertIn("categorical_projection_not_converged", fit.suppressed_reasons)
        json.dumps(result.to_dict(), allow_nan=False)
        rank_x = np.column_stack((x[:, 0], x[:, 0]))
        rank_fit, rank_result, _ = fit_batches(rank_x, y[:, 0], ["a", "duplicate"], ["a"], clusters)
        self.assertIsNone(rank_fit.beta)
        self.assertIn("rank_deficient_residualized_design", rank_fit.suppressed_reasons)
        self.assertIsNone(rank_result.to_dict()["covariance_CR0"][0][1])
        valid_fit, _, _ = fit_batches(x, y, names, targets, clusters)
        scores = ClusterScoreMoments(8)
        scores.add(clusters[:-1], valid_fit.score_batch(x[:-1], y[:-1]))
        output = finalize_clustered_regression(valid_fit, scores).to_dict()
        self.assertIn("score_and_regression_population_disagree", output["estimates"][0]["suppression_reasons"])

    def test_global_aggregate_and_grouped_score_entrypoints(self):
        x, y, _, _, weights, clusters = self.fixture()
        names, targets = ["a", "b", "c", "d"], ["a", "b"]
        moment = RegressionMoments(names, targets)
        moment.add(x, y, weights)
        aggregate = RegressionMoments(names, targets)
        aggregate.add_sufficient_statistics(n=moment.n, weight_sum=moment.weight_sum,
            xtx=moment.xtx, xty=moment.xty, yty=moment.yty, y_sum=moment.y_sum)
        first, second = fit_ols_moments(moment), fit_ols_moments(aggregate)
        np.testing.assert_array_equal(first.beta, second.beta)
        scores = ClusterScoreMoments(8)
        row_scores = first.score_batch(x, y, weights)
        for event in np.unique(clusters):
            selected = clusters == event
            scores.add_cluster_score(int(event), row_scores[selected].sum(axis=0), n=int(selected.sum()))
        self.assertFalse(finalize_clustered_regression(first, scores).to_dict()["estimates"][0]["suppressed"])
        with self.assertRaises(MemoryError):
            ClusterScoreMoments(8, memory_limit_bytes=1).add(clusters, row_scores)
        with self.assertRaises(MemoryError):
            absorb_categorical_effects(batches(x, y, {"event": clusters}), term_names=names,
                target_names=targets, level_counts={"event": 40}, memory_limit_bytes=1)

    def test_compact_code_contract_rejects_missing_or_unused_levels(self):
        x, y, codes, _, _, _ = self.fixture()
        names, targets = ["a", "b", "c", "d"], ["a", "b"]
        with self.assertRaises(ValueError):
            absorb_categorical_effects(batches(x, y, codes), term_names=names,
                target_names=targets, level_counts={"category_tail": 7})
        with self.assertRaises(ValueError):
            absorb_categorical_effects(batches(x, y, {"bad": np.ones(len(x), dtype=float)}),
                term_names=names, target_names=targets, level_counts={"bad": 2})

    def test_separate_tail_support_and_score_balance_gates(self):
        x, y, _, _, _, clusters = self.fixture()
        names, targets = ["D1:xL", "D1:xR", "D10:xL", "D10:xR"], ["a", "b"]
        fit, _, _ = fit_batches(x, y, names, targets, clusters)
        scores = ClusterScoreMoments(8)
        scores.add(clusters, fit.score_batch(x, y))
        support = {name: {"n_observations": 240, "cluster_count": 40 if name.startswith("D1:") else 20}
                   for name in names}
        result = finalize_clustered_regression(fit, scores, min_clusters=30,
            min_observations=50, support_by_term=support)
        output = result.to_dict()
        self.assertFalse(output["estimates"][0]["suppressed"])
        self.assertTrue(output["estimates"][2]["suppressed"])
        vector = contrast_vector(result.names, {"a|D10:xL": 1, "a|D1:xL": -1})
        self.assertTrue(result.contrast(vector)["suppressed"])
        self.assertIn("term_cluster_support_below_floor", result.contrast(vector)["suppression_reasons"])
        # A same-count source mismatch still fails the moment/score identity.
        scores.add_cluster_score(np.int64(0), np.ones(8), n=1)
        scores.counts[0] -= 1
        invalid = finalize_clustered_regression(fit, scores).to_dict()
        self.assertIn("cluster_score_normal_equations_failed", invalid["estimates"][0]["suppression_reasons"])
        json.dumps(invalid, allow_nan=False)

    def test_tail_r_squared_uses_original_centered_tss(self):
        y = np.array([[1, 3], [2, 4], [6, 8]], dtype=float)
        residual = np.array([[.1, .2], [.2, .3], [.4, .5]])
        w = np.array([1, 2, 1])
        summary = r_squared_from_sufficient_statistics(weight_sum=w.sum(),
            y_sum=(y * w[:, None]).sum(axis=0), y_squared_sum=(y ** 2 * w[:, None]).sum(axis=0),
            residual_squared_sum=(residual ** 2 * w[:, None]).sum(axis=0), target_names=["a", "b"])
        tss = ((y - np.average(y, weights=w, axis=0)) ** 2 * w[:, None]).sum(axis=0)
        sse = (residual ** 2 * w[:, None]).sum(axis=0)
        self.assertAlmostEqual(summary["r_squared_including_fixed_effects_by_target"]["a"], 1 - sse[0] / tss[0])
        groups = {}
        for group, mask in (("D1", np.array([True, True, False])), ("D10", np.array([False, False, True]))):
            groups[group] = {"weight_sum": w[mask].sum(), "y_sum": (y[mask] * w[mask, None]).sum(axis=0),
                "y_squared_sum": (y[mask] ** 2 * w[mask, None]).sum(axis=0),
                "residual_squared_sum": (residual[mask] ** 2 * w[mask, None]).sum(axis=0)}
        grouped = grouped_r_squared_from_sufficient_statistics(groups, target_names=["a", "b"])
        expected_d1_tss = ((y[:2] - np.average(y[:2], weights=w[:2], axis=0)) ** 2 * w[:2, None]).sum(axis=0)
        self.assertAlmostEqual(grouped["stacked_weighted_tss_r_squared_by_target"]["a"],
                               1 - sse[0] / tss[0])
        self.assertIsNone(grouped["group_summaries"]["D10"]["r_squared_including_fixed_effects_by_target"]["a"])
        json.dumps(summary, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
