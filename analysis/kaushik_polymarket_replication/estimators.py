"""Bounded-memory numerical core; this module never reads production data.

The caller admits inputs, constructs frozen controls and dense categorical codes,
and supplies replayable batches from lazy DuckDB views.  No category taxonomy or
price rounding is chosen here.  Exact binary64 price levels can be one of the
categorical code families.  Arrays passed directly to the convenience wrappers
are for admitted, bounded samples and tiny tests, never the full trade universe.

Event-cluster CR0 is primary.  The supplementary covariance multiplies CR0 by
G/(G-1); it is *not* the full CR1 correction involving N-k.  Paper-specific
finite-sample scaling is unknown.  All intervals are pointwise normal 95%.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations
from math import erfc, sqrt
from typing import Any, Callable, Iterable, Mapping, Sequence

import numpy as np


NORMAL_95 = 1.959963984540054
VARIANCE_NOTE = {
    "primary": "event_cluster_CR0",
    "supplementary": "event_cluster_CR0_times_G_over_G_minus_1",
    "full_N_minus_k_CR1_used": False,
    "paper_finite_sample_scaling": "unknown; reconstructed covariance declared explicitly",
    "interval": "pointwise normal 95%",
}


def payoff_and_roi(price: Any, outcome: Any) -> tuple[np.ndarray, np.ndarray]:
    """Return payoff in cents and individual ROI in percent, without price trimming.

    ROI is undefined at a zero price and is returned as NaN there.  The caller
    must preserve and audit that exclusion rather than silently replacing it.
    """
    p, y = np.broadcast_arrays(np.asarray(price, dtype=float), np.asarray(outcome, dtype=float))
    if not np.all(np.isfinite(p)) or np.any((p < 0) | (p > 1)):
        raise ValueError("prices must be finite probabilities in [0, 1]")
    if not np.all((y == 0) | (y == 1)):
        raise ValueError("outcomes must be binary")
    roi = np.full(p.shape, np.nan, dtype=float)
    np.divide(y, p, out=roi, where=p > 0)
    return 100.0 * (y - p), 100.0 * (roi - 1.0)


def fixed_probability_bins(price: Any) -> np.ndarray:
    """Zero-based [0,.1), ... [.9,1] fixed-width bins; endpoints are retained."""
    p = np.asarray(price, dtype=float)
    if not np.all(np.isfinite(p)) or np.any((p < 0) | (p > 1)):
        raise ValueError("prices must be finite probabilities in [0, 1]")
    edges = np.asarray([0.0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0])
    return np.minimum(np.searchsorted(edges, p, side="right") - 1, 9)


def paper_time_controls(lifespan_days: Any, remaining_days: Any) -> dict[str, np.ndarray]:
    """Both frozen duration clocks use log2(1+days); raw means no effects."""
    lifespan, remaining = np.broadcast_arrays(
        np.asarray(lifespan_days, dtype=float), np.asarray(remaining_days, dtype=float))
    if not np.all(np.isfinite(lifespan)) or not np.all(np.isfinite(remaining)):
        raise ValueError("time controls require finite days")
    if np.any(lifespan < 0) or np.any(remaining < 0) or np.any(remaining > lifespan):
        raise ValueError("time controls require 0 <= remaining <= lifespan")
    return {"original_log2_days": np.log2(1 + lifespan),
            "remaining_log2_days": np.log2(1 + remaining)}


def tail_varying_design(controls: Any, names: Sequence[str], favorite: Any,
                        *, intercept: bool = True) -> tuple[np.ndarray, tuple[str, ...]]:
    """Separate D1/D10 levels and slopes on a previously admitted tail sample.

    Call with ``intercept=False`` when tail-specific categorical effects absorb
    both tail constants.  The caller also interacts categorical codes with tail.
    """
    x = np.asarray(controls, dtype=float)
    if x.ndim == 1:
        x = x[:, None]
    h = np.asarray(favorite)
    if x.ndim != 2 or x.shape[1] != len(names) or h.shape != (len(x),):
        raise ValueError("control and tail dimensions disagree")
    if not np.all((h == 0) | (h == 1)) or not np.all(np.isfinite(x)):
        raise ValueError("finite controls and a binary favorite indicator are required")
    base = np.column_stack((np.ones(len(x)), x)) if intercept else x
    base_names = ("level", *names) if intercept else tuple(names)
    return np.column_stack((base * (1 - h[:, None]), base * h[:, None])), tuple(
        f"{tail}:{name}" for tail in ("D1", "D10") for name in base_names)


def a1_claim_design(favorite: Any, lifespan_log: Any,
                    remaining_log: Any) -> tuple[np.ndarray, tuple[str, ...]]:
    """A1 structural columns after claim FE: H, H*xL, xR, H*xR.

    Standalone xL is constant within claim and is absorbed.  The caller must
    absorb claim effects only; price and month effects are absent in A1.
    """
    h, xl, xr = np.broadcast_arrays(np.asarray(favorite, dtype=float),
                                     np.asarray(lifespan_log, dtype=float),
                                     np.asarray(remaining_log, dtype=float))
    if h.ndim != 1 or not np.all((h == 0) | (h == 1)):
        raise ValueError("favorite must be a one-dimensional binary indicator")
    if not np.all(np.isfinite(xl)) or not np.all(np.isfinite(xr)):
        raise ValueError("A1 controls must be finite")
    return np.column_stack((h, h * xl, xr, h * xr)), ("H", "H:xL", "xR", "H:xR")


def _xyw(x: Any, y: Any, weights: Any = None) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if x.ndim != 2:
        raise ValueError("X must be two-dimensional")
    if y.ndim == 1:
        y = y[:, None]
    if y.ndim != 2 or y.shape[0] != x.shape[0]:
        raise ValueError("X and Y row counts disagree")
    w = np.ones(len(x)) if weights is None else np.asarray(weights, dtype=float)
    if w.shape != (len(x),) or not np.all(np.isfinite(w)) or np.any(w <= 0):
        raise ValueError("weights must be finite and strictly positive")
    if not np.all(np.isfinite(x)) or not np.all(np.isfinite(y)):
        raise ValueError("X and Y must be finite after input admission")
    return x, y, w


def _observation_counts(values: Any, n: int) -> np.ndarray:
    if values is None:
        return np.ones(n, dtype=np.int64)
    counts = np.asarray(values)
    if counts.shape != (n,) or not np.issubdtype(counts.dtype, np.integer) or np.any(counts <= 0):
        raise ValueError("observation counts must be positive integers")
    return counts.astype(np.int64, copy=False)


@dataclass
class RegressionMoments:
    """Global sufficient statistics; storage is O(p² + q²), independent of N."""
    term_names: Sequence[str]
    target_names: Sequence[str] = ("payoff_cents",)
    n: int = field(init=False, default=0)
    weight_sum: float = field(init=False, default=0.0)
    xtx: np.ndarray = field(init=False)
    xty: np.ndarray = field(init=False)
    yty: np.ndarray = field(init=False)
    y_sum: np.ndarray = field(init=False)

    def __post_init__(self) -> None:
        self.term_names = tuple(self.term_names)
        self.target_names = tuple(self.target_names)
        if not self.term_names or not self.target_names:
            raise ValueError("nonempty term and target names required")
        if len(set(self.term_names)) != len(self.term_names) or len(set(self.target_names)) != len(self.target_names):
            raise ValueError("term and target names must be unique")
        p, q = len(self.term_names), len(self.target_names)
        self.xtx, self.xty = np.zeros((p, p)), np.zeros((p, q))
        self.yty, self.y_sum = np.zeros((q, q)), np.zeros(q)

    def add(self, x: Any, y: Any, weights: Any = None, observation_counts: Any = None) -> None:
        x, y, w = _xyw(x, y, weights)
        if x.shape[1] != len(self.term_names) or y.shape[1] != len(self.target_names):
            raise ValueError("moment dimensions disagree with declared names")
        self.n += int(_observation_counts(observation_counts, len(x)).sum())
        self.weight_sum += float(w.sum())
        self.xtx += x.T @ (x * w[:, None])
        self.xty += x.T @ (y * w[:, None])
        self.yty += y.T @ (y * w[:, None])
        self.y_sum += (y * w[:, None]).sum(axis=0)

    def add_sufficient_statistics(self, *, n: int, weight_sum: float, xtx: Any,
                                  xty: Any, yty: Any, y_sum: Any) -> None:
        """Accept bounded global aggregates supplied by a lazy SQL scan."""
        arrays = [np.asarray(value, dtype=float) for value in (xtx, xty, yty, y_sum)]
        expected = (self.xtx.shape, self.xty.shape, self.yty.shape, self.y_sum.shape)
        if any(value.shape != shape or not np.all(np.isfinite(value))
               for value, shape in zip(arrays, expected)):
            raise ValueError("invalid sufficient-statistic dimensions or values")
        if not isinstance(n, (int, np.integer)) or n < 0 or not np.isfinite(weight_sum) or weight_sum < 0:
            raise ValueError("invalid sufficient-statistic counts")
        self.n += int(n)
        self.weight_sum += float(weight_sum)
        self.xtx += arrays[0]
        self.xty += arrays[1]
        self.yty += arrays[2]
        self.y_sum += arrays[3]


@dataclass
class CategoricalAbsorption:
    """Group effects for replayable bounded batches, with a convergence gate."""
    fe_names: tuple[str, ...]
    level_counts: tuple[int, ...]
    effects: tuple[np.ndarray, ...]
    column_count: int
    target_count: int
    converged: bool
    iterations: int
    maximum_group_mean: float
    tolerance: float
    n: int
    weight_sum: float
    original_y_sum: np.ndarray
    original_yty: np.ndarray
    original_x_squared_norms: np.ndarray

    def transform(self, batch: Mapping[str, Any]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        x, y, w = _xyw(batch["x"], batch["y"], batch.get("weights"))
        if x.shape[1] != self.column_count or y.shape[1] != self.target_count:
            raise ValueError("absorption batch dimensions changed")
        values = np.column_stack((x, y))
        for name, levels, effect in zip(self.fe_names, self.level_counts, self.effects):
            code = _codes(batch["codes"][name], len(x), levels)
            values -= effect[code]
        return values[:, :self.column_count], values[:, self.column_count:], w

    def diagnostics(self) -> dict[str, Any]:
        return {"method": "alternating weighted categorical projections",
                "converged": self.converged, "iterations": self.iterations,
                "maximum_scaled_group_mean": self.maximum_group_mean,
                "tolerance": self.tolerance,
                "level_counts": dict(zip(self.fe_names, self.level_counts)),
                "effect_storage_bytes": sum(item.nbytes for item in self.effects),
                "full_absorbed_design_rank": "not inferred from level counts"}


def _codes(values: Any, n: int, level_count: int) -> np.ndarray:
    code = np.asarray(values)
    if code.shape != (n,) or not np.issubdtype(code.dtype, np.integer):
        raise ValueError("categorical codes must be one-dimensional integers")
    if np.any((code < 0) | (code >= level_count)):
        raise ValueError("categorical code outside declared level count")
    return code


def _cluster_identity(value: Any) -> str | int | float:
    """Normalize scalar batch identities to deterministic JSON-compatible keys."""
    if isinstance(value, np.generic):
        value = value.item()
    if (not isinstance(value, (str, int, float)) or isinstance(value, bool)
            or (isinstance(value, str) and not value)
            or (isinstance(value, float) and not np.isfinite(value))):
        raise ValueError("nonmissing scalar string/integer cluster identity required")
    return value


def absorb_categorical_effects(batch_factory: Callable[[], Iterable[Mapping[str, Any]]],
                               *, term_names: Sequence[str], target_names: Sequence[str],
                               level_counts: Mapping[str, int], tolerance: float = 1e-10,
                               max_iterations: int = 100,
                               memory_limit_bytes: int | None = None,
                               progress: Callable[[Mapping[str, Any]], None] | None = None) -> CategoricalAbsorption:
    """FWL projection without expanding categorical dummies or retaining rows.

    Each replay yields mappings containing X, Y, optional weights, and ``codes``
    for every declared effect.  Codes are frozen dense integer codes; the caller
    is responsible for their semantic definition.  Storage scales with the sum
    of category levels, including exact recorded price levels, times p+q.  The
    optional allocation cap covers persistent effects and the largest group
    workspace; the driver must additionally cap batch/source memory.

    Both X and Y are residualized.  The final rank of residualized X is tested
    separately by ``fit_ols_moments``.  Failure to converge must suppress fits.
    """
    p, q = len(term_names), len(target_names)
    names, levels = tuple(level_counts), tuple(level_counts.values())
    if not names or p == 0 or q == 0 or any(not isinstance(v, (int, np.integer)) or v <= 0 for v in levels):
        raise ValueError("positive effect levels and nonempty X/Y dimensions required")
    if not np.isfinite(tolerance) or tolerance <= 0 or max_iterations < 1:
        raise ValueError("positive tolerance and iteration limit required")
    width = p + q
    allocation = 8 * (sum(levels) * width + max(levels) * (width + 1))
    if memory_limit_bytes is not None and allocation > memory_limit_bytes:
        raise MemoryError(f"categorical projection arrays require {allocation} bytes")
    effects = tuple(np.zeros((value, width)) for value in levels)
    n, weight_sum = 0, 0.0
    original_y_sum, original_yty = np.zeros(q), np.zeros((q, q))
    raw_squares = np.zeros(width)
    for batch in batch_factory():
        x, y, w = _xyw(batch["x"], batch["y"], batch.get("weights"))
        if x.shape[1] != p or y.shape[1] != q:
            raise ValueError("initial absorption dimensions disagree")
        for name, count in zip(names, levels):
            _codes(batch["codes"][name], len(x), count)
        n += int(_observation_counts(batch.get("observation_counts"), len(x)).sum())
        weight_sum += float(w.sum())
        original_y_sum += (y * w[:, None]).sum(axis=0)
        original_yty += y.T @ (y * w[:, None])
        raw_squares += (np.column_stack((x, y)) ** 2 * w[:, None]).sum(axis=0)
    if n == 0:
        raise ValueError("empty absorption input")
    scale = np.maximum(1.0, np.sqrt(raw_squares / weight_sum))
    result = CategoricalAbsorption(names, levels, effects, p, q, False, 0, float("inf"),
                                  tolerance, n, weight_sum, original_y_sum, original_yty,
                                  raw_squares[:p])
    for iteration in range(1, max_iterations + 1):
        maximum_update = 0.0
        for family, (name, count) in enumerate(zip(names, levels)):
            sums, counts = np.zeros((count, width)), np.zeros(count)
            replay_n, replay_weight_sum = 0, 0.0
            for batch in batch_factory():
                x, y, w = _xyw(batch["x"], batch["y"], batch.get("weights"))
                if x.shape[1] != p or y.shape[1] != q:
                    raise ValueError("absorption replay dimensions changed")
                values = np.column_stack((x, y))
                selected_code = None
                for other, (other_name, other_count, effect) in enumerate(zip(names, levels, effects)):
                    code = _codes(batch["codes"][other_name], len(x), other_count)
                    if other == family:
                        selected_code = code
                    else:
                        values -= effect[code]
                np.add.at(sums, selected_code, values * w[:, None])
                np.add.at(counts, selected_code, w)
                replay_n += int(_observation_counts(batch.get("observation_counts"), len(x)).sum())
                replay_weight_sum += float(w.sum())
            if replay_n != n or not np.isclose(replay_weight_sum, weight_sum, rtol=1e-12, atol=1e-10):
                raise ValueError("absorption source count/weight changed between replays")
            if np.any(counts == 0):
                raise ValueError(f"unused dense categorical code in {name}; freeze compact codes")
            sums /= counts[:, None]
            maximum_update = max(maximum_update, float(np.max(np.abs(sums - effects[family]) / scale)))
            effects[family][:] = sums
        result.iterations = iteration
        if progress:
            progress({"iteration": iteration, "maximum_scaled_update": maximum_update})
        # Update size is an inexpensive preliminary check.  A final explicit
        # orthogonality scan prevents false convergence from cancelling updates.
        if maximum_update <= tolerance or iteration == max_iterations:
            maximum_mean = 0.0
            for name, count in zip(names, levels):
                sums, counts = np.zeros((count, width)), np.zeros(count)
                for batch in batch_factory():
                    x, y, w = result.transform(batch)
                    code = _codes(batch["codes"][name], len(x), count)
                    np.add.at(sums, code, np.column_stack((x, y)) * w[:, None])
                    np.add.at(counts, code, w)
                maximum_mean = max(maximum_mean, float(np.max(np.abs(sums / counts[:, None]) / scale)))
            result.maximum_group_mean = maximum_mean
            if maximum_mean <= tolerance:
                result.converged = True
                break
    return result


@dataclass
class OLSFit:
    term_names: tuple[str, ...]
    target_names: tuple[str, ...]
    n: int
    weight_sum: float
    rank: int
    condition_number: float | None
    beta: np.ndarray | None
    bread: np.ndarray | None
    sse: np.ndarray | None
    tss: np.ndarray | None
    r_squared: np.ndarray | None
    suppressed_reasons: tuple[str, ...]
    absorption: dict[str, Any] | None

    @property
    def coefficient_names(self) -> tuple[str, ...]:
        return tuple(f"{target}|{term}" for target in self.target_names for term in self.term_names)

    def score_batch(self, x: Any, y: Any, weights: Any = None) -> np.ndarray:
        if self.beta is None:
            raise ValueError("cannot score a suppressed regression")
        x, y, w = _xyw(x, y, weights)
        if x.shape[1] != len(self.term_names) or y.shape[1] != len(self.target_names):
            raise ValueError("score dimensions disagree with fitted model")
        residual = y - x @ self.beta
        return (residual[:, :, None] * x[:, None, :] * w[:, None, None]).reshape(len(x), -1)


def continuous_design_diagnostics(xtx: Any, *, original_squared_norms: Any = None,
                                  relative_rank_tolerance: float = 1e-10):
    """Identical normalized-Gram/dust convention for stacked and tail ranks.

    Return column norms, normalized Gram and a JSON-compatible diagnostic. This
    certifies only the continuous residualized design, never combined FE rank.
    """
    gram = np.asarray(xtx, dtype=float)
    if gram.ndim != 2 or gram.shape[0] != gram.shape[1]:
        raise ValueError("square continuous-design Gram required")
    gram = (gram + gram.T) / 2
    norms = np.sqrt(np.maximum(np.diag(gram), 0))
    if original_squared_norms is not None:
        original = np.asarray(original_squared_norms, dtype=float)
        if original.shape != norms.shape:
            raise ValueError("original continuous-design norms disagree")
        norms[np.diag(gram) <= original * relative_rank_tolerance ** 2] = 0
    normalized = np.divide(gram, np.outer(norms, norms), out=np.zeros_like(gram),
                           where=np.outer(norms, norms) > 0)
    singular_values = np.linalg.svd(normalized, compute_uv=False)
    rank = int(np.count_nonzero(singular_values > relative_rank_tolerance))
    condition = (float(singular_values[0] / singular_values[-1])
                 if rank == len(norms) and rank > 0 else None)
    return norms, normalized, {"rank": rank, "continuous_columns": len(norms),
        "condition_number": condition, "relative_rank_tolerance": relative_rank_tolerance,
        "convention": "normalized residualized Gram SVD; absorbed dust<=original norm squared*tolerance squared"}


def fit_ols_moments(moments: RegressionMoments, *,
                    absorption: CategoricalAbsorption | None = None,
                    relative_rank_tolerance: float = 1e-10) -> OLSFit:
    """Fit the continuous residualized design and fail closed on deficient rank.

    R² uses original weighted Y TSS after absorption, so it includes fixed
    effects.  Separate tail R² and a stacked overall-mean weighted-TSS R²
    additionally require original-Y sufficient statistics for each tail.
    """
    p = len(moments.term_names)
    reasons: list[str] = []
    gram = (moments.xtx + moments.xtx.T) / 2
    norms, normalized, diagnostics = continuous_design_diagnostics(gram,
        original_squared_norms=absorption.original_x_squared_norms if absorption is not None else None,
        relative_rank_tolerance=relative_rank_tolerance)
    rank, condition = diagnostics["rank"], diagnostics["condition_number"]
    if moments.n <= p or moments.weight_sum <= 0:
        reasons.append("insufficient_observations_for_design")
    if rank != p:
        reasons.append("rank_deficient_residualized_design")
    absorption_info = absorption.diagnostics() if absorption else None
    if absorption and not absorption.converged:
        reasons.append("categorical_projection_not_converged")
    if absorption and (moments.n != absorption.n or not np.isclose(moments.weight_sum, absorption.weight_sum)):
        reasons.append("absorption_and_regression_population_disagree")
    fit = OLSFit(tuple(moments.term_names), tuple(moments.target_names), moments.n,
                 moments.weight_sum, rank, condition, None, None, None, None, None,
                 tuple(reasons), absorption_info)
    if reasons:
        return fit
    bread = np.linalg.inv(normalized) / np.outer(norms, norms)
    beta = bread @ moments.xty
    sse_matrix = moments.yty - beta.T @ moments.xty - moments.xty.T @ beta + beta.T @ gram @ beta
    sse = np.diag(sse_matrix).copy()
    original_yty = absorption.original_yty if absorption else moments.yty
    original_y_sum = absorption.original_y_sum if absorption else moments.y_sum
    tss = np.diag(original_yty) - original_y_sum ** 2 / moments.weight_sum
    roundoff = 1e-10 * np.maximum(1.0, np.diag(original_yty))
    if np.any(sse < -roundoff) or np.any(tss < -roundoff):
        fit.suppressed_reasons = ("negative_residual_or_total_sum_of_squares",)
        return fit
    sse, tss = np.maximum(sse, 0), np.maximum(tss, 0)
    r_squared = np.divide(sse, tss, out=np.full_like(sse, np.nan), where=tss > 0)
    fit.beta, fit.bread, fit.sse, fit.tss, fit.r_squared = beta, bread, sse, tss, 1 - r_squared
    return fit


def r_squared_from_sufficient_statistics(*, weight_sum: float, y_sum: Any,
                                         y_squared_sum: Any,
                                         residual_squared_sum: Any,
                                         target_names: Sequence[str]) -> dict[str, Any]:
    """Original-Y TSS summary for a whole model or an audited individual tail.

    Inputs are streamed group totals.  Residuals must include the fitted
    categorical effects.  Targets may have different units; their TSS are never
    added together.  To compute stacked overall-mean TSS separately per target,
    use ``grouped_r_squared_from_sufficient_statistics``.
    """
    ys, yss, rss = (np.asarray(value, dtype=float) for value in
                    (y_sum, y_squared_sum, residual_squared_sum))
    shape = (len(target_names),)
    if (weight_sum <= 0 or not np.isfinite(weight_sum) or any(
            value.shape != shape or not np.all(np.isfinite(value)) for value in (ys, yss, rss))):
        raise ValueError("finite sufficient statistics and positive weight sum required")
    tss = yss - ys ** 2 / weight_sum
    tolerance = 1e-10 * np.maximum(1.0, np.abs(yss))
    if np.any(tss < -tolerance) or np.any(rss < -tolerance):
        raise ValueError("negative total/residual sum of squares")
    tss, rss = np.maximum(tss, 0), np.maximum(rss, 0)
    return {"r_squared_including_fixed_effects_by_target": {
                name: float(1 - residual / total) if total > 0 else None
                for name, residual, total in zip(target_names, rss, tss)},
            "weighted_tss_by_target": dict(zip(target_names, tss.tolist())),
            "weighted_sse_by_target": dict(zip(target_names, rss.tolist()))}


def grouped_r_squared_from_sufficient_statistics(
        group_totals: Mapping[str, Mapping[str, Any]], *,
        target_names: Sequence[str]) -> dict[str, Any]:
    """Tail R² and stacked original-Y TSS around the overall sample mean."""
    if not group_totals:
        raise ValueError("nonempty group sufficient statistics required")
    summaries = {name: r_squared_from_sufficient_statistics(**totals, target_names=target_names)
                 for name, totals in group_totals.items()}
    result = {}
    for target in target_names:
        index = list(target_names).index(target)
        ys = sum(float(value["y_sum"][index]) for value in group_totals.values())
        yss = sum(float(value["y_squared_sum"][index]) for value in group_totals.values())
        weight_sum = sum(float(value["weight_sum"]) for value in group_totals.values())
        tss = max(0.0, yss - ys ** 2 / weight_sum)
        sse = sum(value["weighted_sse_by_target"][target] for value in summaries.values())
        result[target] = float(1 - sse / tss) if tss > 0 else None
    return {"group_summaries": summaries,
            "stacked_weighted_tss_r_squared_by_target": result,
            "stacked_r_squared_definition": "1 - sum(group SSE)/sum squared deviations from overall sample mean, separately per outcome"}


@dataclass
class ClusterScoreMoments:
    """Second-pass cluster scores, O(Gpq); no per-cluster X'X matrices."""
    dimension: int
    memory_limit_bytes: int | None = None
    scores: dict[Any, np.ndarray] = field(default_factory=dict)
    counts: dict[Any, int] = field(default_factory=dict)

    def add(self, clusters: Any, row_scores: Any, observation_counts: Any = None) -> None:
        scores = np.asarray(row_scores, dtype=float)
        ids = np.asarray(clusters)
        if scores.ndim != 2 or scores.shape[1] != self.dimension or ids.shape != (len(scores),):
            raise ValueError("cluster score dimensions disagree")
        if not np.all(np.isfinite(scores)):
            raise ValueError("cluster scores must be finite")
        if ids.dtype.kind in "fc" and not np.all(np.isfinite(ids)):
            raise ValueError("missing cluster identity")
        if ids.dtype.kind == "O" and any(item is None for item in ids):
            raise ValueError("missing cluster identity")
        unique, inverse = np.unique(ids, return_inverse=True)
        sums = np.zeros((len(unique), self.dimension))
        np.add.at(sums, inverse, scores)
        ns = np.zeros(len(unique), dtype=np.int64)
        np.add.at(ns, inverse, _observation_counts(observation_counts, len(scores)))
        for cluster, score, n in zip(unique.tolist(), sums, ns):
            self.add_cluster_score(cluster, score, n=int(n))

    def add_cluster_score(self, cluster: Any, score: Any, *, n: int) -> None:
        cluster = _cluster_identity(cluster)
        value = np.asarray(score, dtype=float)
        if value.shape != (self.dimension,) or not np.all(np.isfinite(value)) or n <= 0:
            raise ValueError("invalid grouped cluster score")
        if cluster not in self.scores:
            # The numeric lower bound is explicit; dictionary overhead and the
            # incoming batch belong in the driver's overall memory preflight.
            required = (len(self.scores) + 1) * self.dimension * 8
            if self.memory_limit_bytes is not None and required > self.memory_limit_bytes:
                raise MemoryError(f"numeric cluster scores require at least {required} bytes")
            self.scores[cluster], self.counts[cluster] = value.copy(), int(n)
        else:
            self.scores[cluster] += value
            self.counts[cluster] += int(n)

    def ordered(self) -> tuple[list[Any], np.ndarray]:
        keys = sorted(self.scores, key=lambda value: (type(value).__name__, str(value)))
        return keys, np.asarray([self.scores[key] for key in keys]).reshape(len(keys), self.dimension)


def _influence(scalar_cluster_scores: np.ndarray) -> dict[str, Any]:
    scale = float(np.max(np.abs(scalar_cluster_scores))) if len(scalar_cluster_scores) else 0.0
    squares = (scalar_cluster_scores / scale) ** 2 if scale > 0 else np.zeros_like(scalar_cluster_scores)
    total = float(squares.sum())
    effective = total ** 2 / float((squares ** 2).sum()) if total > 0 else None
    maximum = float(squares.max() / total) if total > 0 else None
    flags = []
    if effective is not None and effective < 30:
        flags.append("effective_clusters_below_30")
    if maximum is not None and maximum > .25:
        flags.append("cluster_variance_share_above_25_percent")
    if total == 0:
        flags.append("zero_cluster_score_variance")
    return {"effective_clusters": effective, "maximum_cluster_variance_share": maximum,
            "flags": flags}


def _interval(value: float, variance: float) -> dict[str, Any]:
    if variance < -1e-12 or not np.isfinite(variance):
        raise ValueError("invalid contrast variance")
    se = sqrt(max(0.0, variance))
    statistic = value / se if se > 0 else None
    return {"estimate": float(value), "standard_error": se,
            "ci95_low": float(value - NORMAL_95 * se),
            "ci95_high": float(value + NORMAL_95 * se),
            "normal_statistic": statistic,
            "normal_p_value": erfc(abs(statistic) / sqrt(2)) if statistic is not None else None}


@dataclass
class JointClusteredEstimate:
    names: tuple[str, ...]
    values: np.ndarray
    coefficient_cluster_influence: np.ndarray
    supported: np.ndarray
    reasons: tuple[tuple[str, ...], ...]
    metadata: dict[str, Any]

    @property
    def covariance(self) -> np.ndarray:
        with np.errstate(over="ignore", invalid="ignore"):
            return self.coefficient_cluster_influence.T @ self.coefficient_cluster_influence

    def contrast(self, weights: Any, *, name: str = "contrast") -> dict[str, Any]:
        c = np.asarray(weights, dtype=float)
        if c.shape != self.values.shape or not np.all(np.isfinite(c)) or not np.any(c):
            raise ValueError("contrast must be a finite nonzero vector with the fitted dimension")
        used = c != 0
        reasons = sorted({reason for item in np.flatnonzero(used) for reason in self.reasons[item]})
        if not np.all(self.supported[used]):
            return {"name": name, "suppressed": True, "suppression_reasons": reasons,
                    "CR0": None, "cluster_count_adjusted": None,
                    "influence": None}
        influence = self.coefficient_cluster_influence @ c
        with np.errstate(over="ignore", invalid="ignore"):
            variance = float(influence @ influence)
        g = len(influence)
        estimate = float(self.values[used] @ c[used])
        if not np.isfinite(variance) or not np.isfinite(estimate):
            return {"name": name, "suppressed": True,
                    "suppression_reasons": ["nonfinite_estimate_or_cluster_variance"],
                    "CR0": None, "cluster_count_adjusted": None, "influence": None}
        adjusted_variance = variance * (g / (g - 1))
        return {"name": name, "suppressed": False, "suppression_reasons": [],
                "CR0": _interval(estimate, variance),
                "cluster_count_adjusted": (_interval(estimate, adjusted_variance)
                                           if np.isfinite(adjusted_variance) else None),
                "supplementary_suppression_reasons": ([] if np.isfinite(adjusted_variance)
                                                     else ["nonfinite_adjusted_cluster_variance"]),
                "influence": _influence(influence)}

    def to_dict(self, contrasts: Mapping[str, Any] | None = None) -> dict[str, Any]:
        covariance = self.covariance
        mask = self.supported[:, None] & self.supported[None, :]
        def matrix(factor: float) -> list[list[float | None]]:
            return [[float(covariance[i, j] * factor) if mask[i, j] and np.isfinite(covariance[i, j] * factor) else None
                     for j in range(len(self.names))] for i in range(len(self.names))]
        g = len(self.coefficient_cluster_influence)
        return {"names": list(self.names), "metadata": self.metadata,
                "variance_convention": VARIANCE_NOTE.copy(),
                "estimates": [self.contrast(np.eye(len(self.names))[index], name=name)
                              for index, name in enumerate(self.names)],
                "covariance_CR0": matrix(1.0),
                "covariance_cluster_count_adjusted": matrix(g / (g - 1)) if g > 1 else matrix(1.0),
                "contrasts": [self.contrast(vector, name=name) for name, vector in (contrasts or {}).items()]}


def finalize_clustered_regression(fit: OLSFit, scores: ClusterScoreMoments, *,
                                  min_clusters: int = 2,
                                  min_observations: int = 0,
                                  support_by_term: Mapping[str, Mapping[str, int]] | None = None,
                                  fit_statistics_by_group: Mapping[str, Mapping[str, Any]] | None = None,
                                  normal_equation_tolerance: float = 1e-7) -> JointClusteredEstimate:
    """Retain cross-tail and cross-target covariance before any contrast.

    Audited original-design support can be supplied per term with integer
    ``n_observations`` and ``cluster_count`` fields.  This is necessary to certify
    a separate 30-game floor for each sports tail: union-model G alone cannot
    establish it.  Support is never guessed from residualized cluster scores.
    """
    if (min_observations > 0 or min_clusters > 2) and support_by_term is None:
        raise ValueError("per-term support proof required for nondefault support floors")
    names = fit.coefficient_names
    if scores.dimension != len(names):
        raise ValueError("cluster score dimension disagrees with regression")
    cluster_ids, cluster_scores = scores.ordered()
    g, p = len(cluster_ids), len(fit.term_names)
    reasons = list(fit.suppressed_reasons)
    if g < max(2, min_clusters):
        reasons.append("cluster_support_below_floor")
    if fit.beta is not None and sum(scores.counts.values()) != fit.n:
        reasons.append("score_and_regression_population_disagree")
    normal_equation_error = (float(np.max(np.abs(cluster_scores.sum(axis=0)) /
                             np.maximum(1.0, np.linalg.norm(cluster_scores, axis=0))))
                             if len(cluster_scores) else None)
    if normal_equation_error is not None and normal_equation_error > normal_equation_tolerance:
        reasons.append("cluster_score_normal_equations_failed")
    term_reasons: list[tuple[str, ...]] = []
    audited_support = {}
    if support_by_term is not None and set(support_by_term) != set(fit.term_names):
        raise ValueError("audited support must supply every structural term exactly once")
    for term in fit.term_names:
        local_reasons = list(reasons)
        if support_by_term is not None:
            support = support_by_term[term]
            n_support, g_support = support["n_observations"], support["cluster_count"]
            if (not isinstance(n_support, (int, np.integer)) or not isinstance(g_support, (int, np.integer))
                    or n_support < 0 or g_support < 0 or n_support > fit.n or g_support > g):
                raise ValueError("invalid audited original-design term support")
            audited_support[term] = {"n_observations": int(n_support), "cluster_count": int(g_support)}
            if n_support < min_observations:
                local_reasons.append("term_observation_support_below_floor")
            if g_support < max(2, min_clusters):
                local_reasons.append("term_cluster_support_below_floor")
        term_reasons.append(tuple(local_reasons))
    if fit.beta is not None and not reasons:
        bread = np.kron(np.eye(len(fit.target_names)), fit.bread)
        influences = cluster_scores @ bread
        values = fit.beta.T.reshape(-1)
    else:
        influences = np.zeros((g, len(names)))
        values = np.full(len(names), np.nan)
    r_squared = ({name: float(value) if np.isfinite(value) else None
                  for name, value in zip(fit.target_names, fit.r_squared)}
                 if fit.r_squared is not None else None)
    metadata = {"n": fit.n, "weight_sum": fit.weight_sum, "cluster_count": g,
                "cluster_levels": cluster_ids, "cluster_counts": [scores.counts[item] for item in cluster_ids],
                "term_names": list(fit.term_names), "target_names": list(fit.target_names),
                "residualized_design_rank": fit.rank, "structural_column_count": p,
                "condition_number_scaled_gram": fit.condition_number,
                "r_squared_including_fixed_effects_by_target": r_squared,
                "grouped_r_squared": (grouped_r_squared_from_sufficient_statistics(
                    fit_statistics_by_group, target_names=fit.target_names)
                    if fit_statistics_by_group is not None and fit.beta is not None else None),
                "absorption": fit.absorption, "minimum_clusters": min_clusters,
                "minimum_observations_per_term": min_observations,
                "audited_original_design_support_by_term": audited_support or None,
                "cluster_score_normal_equation_error": normal_equation_error,
                "cluster_score_normal_equation_tolerance": normal_equation_tolerance,
                "cluster_score_storage_bytes": cluster_scores.nbytes,
                "sign_convention": "positive payoff/ROI means realized outcome exceeds entry price"}
    all_reasons = tuple(item for _ in fit.target_names for item in term_reasons)
    return JointClusteredEstimate(names, values, influences,
                                  np.asarray([not item for item in all_reasons]),
                                  all_reasons, metadata)


def joint_clustered_means_from_totals(records: Iterable[Mapping[str, Any]], *,
                                      cells: Sequence[str], target_names: Sequence[str],
                                      min_observations: int = 1,
                                      min_clusters: int = 2,
                                      memory_limit_bytes: int | None = None) -> JointClusteredEstimate:
    """Joint means from streamed cluster×cell aggregate records.

    Each record supplies cluster, cell, count, weight_sum, and weighted_sums
    (one value per target).  Multiple records per cluster×cell are summed.  The
    scores retain covariance across all cells, phases, tails and targets.  For
    sports, the caller freezes ``min_clusters=30`` for the game support gate.
    """
    cells, targets = tuple(cells), tuple(target_names)
    if not cells or not targets or len(set(cells)) != len(cells) or len(set(targets)) != len(targets):
        raise ValueError("unique nonempty cells and targets required")
    cell_index, c, q = {name: i for i, name in enumerate(cells)}, len(cells), len(targets)
    totals: dict[Any, np.ndarray] = {}
    counts: dict[Any, np.ndarray] = {}
    for record in records:
        cluster, cell = _cluster_identity(record["cluster"]), record["cell"]
        if cell not in cell_index:
            raise ValueError("unknown aggregate cell")
        ns, w = record["count"], float(record["weight_sum"])
        sums = np.asarray(record["weighted_sums"], dtype=float)
        if not isinstance(ns, (int, np.integer)) or ns <= 0 or w <= 0 or not np.isfinite(w):
            raise ValueError("aggregate records require positive counts and weights")
        if sums.shape != (q,) or not np.all(np.isfinite(sums)):
            raise ValueError("invalid weighted outcome sums")
        if cluster not in totals:
            allocation = (len(totals) + 1) * c * (q + 2) * 8
            if memory_limit_bytes is not None and allocation > memory_limit_bytes:
                raise MemoryError(f"numeric mean cluster arrays require at least {allocation} bytes")
            totals[cluster], counts[cluster] = np.zeros((c, q + 1)), np.zeros(c, dtype=np.int64)
        index = cell_index[cell]
        totals[cluster][index, 0] += w
        totals[cluster][index, 1:] += sums
        counts[cluster][index] += int(ns)
    ids = sorted(totals, key=lambda value: (type(value).__name__, str(value)))
    arr = np.asarray([totals[key] for key in ids]).reshape(len(ids), c, q + 1)
    count_array = np.asarray([counts[key] for key in ids], dtype=np.int64).reshape(len(ids), c)
    weights = arr[:, :, 0]
    total_weight = weights.sum(axis=0)
    n = count_array.sum(axis=0)
    gs = (weights > 0).sum(axis=0)
    means = np.divide(arr[:, :, 1:].sum(axis=0), total_weight[:, None],
                      out=np.full((c, q), np.nan), where=total_weight[:, None] > 0)
    centered = arr[:, :, 1:] - weights[:, :, None] * np.nan_to_num(means)[None, :, :]
    influence = np.divide(centered, total_weight[None, :, None],
                          out=np.zeros_like(centered), where=total_weight[None, :, None] > 0)
    reasons_by_cell = []
    for index in range(c):
        reasons = []
        if n[index] < min_observations:
            reasons.append("observation_support_below_floor")
        if gs[index] < max(2, min_clusters):
            reasons.append("cluster_support_below_floor")
        if total_weight[index] <= 0:
            reasons.append("empty_cell")
        reasons_by_cell.append(tuple(reasons))
    names = tuple(f"{target}|{cell}" for target in targets for cell in cells)
    reasons = tuple(reasons_by_cell[index] for _ in targets for index in range(c))
    metadata = {"n": int(n.sum()), "n_by_cell": dict(zip(cells, n.tolist())),
                "weight_sum_by_cell": dict(zip(cells, total_weight.tolist())),
                "cluster_count": len(ids), "cluster_levels": ids,
                "cluster_count_by_cell": dict(zip(cells, gs.tolist())),
                "cell_names": list(cells), "target_names": list(targets),
                "minimum_observations": min_observations, "minimum_clusters": min_clusters,
                "cluster_score_storage_bytes": influence.nbytes,
                "units": {name: name for name in targets},
                "sign_convention": "favorite-minus-longshot contrasts are D10 minus D1"}
    return JointClusteredEstimate(names, means.T.reshape(-1),
                                  influence.transpose(0, 2, 1).reshape(len(ids), c * q),
                                  np.asarray([not item for item in reasons]), reasons, metadata)


def contrast_vector(names: Sequence[str], coefficients: Mapping[str, float]) -> np.ndarray:
    """Construct a named contrast and reject misspelled/absent terms."""
    index = {name: i for i, name in enumerate(names)}
    if len(index) != len(names) or any(name not in index for name in coefficients):
        raise ValueError("contrast names must identify unique fitted terms")
    result = np.zeros(len(names))
    for name, weight in coefficients.items():
        result[index[name]] = weight
    if not np.all(np.isfinite(result)) or not np.any(result):
        raise ValueError("finite nonzero contrast required")
    return result


def joint_tail_phase_contrasts(names: Sequence[str], *, target: str,
                               phases: Sequence[str]) -> dict[str, np.ndarray]:
    """Tail spreads and every later-minus-earlier phase spread, jointly fitted.

    Cell names follow ``target|D1:phase`` / ``target|D10:phase``.  These are
    differences of joint estimates, never sums of independently fitted SEs.
    """
    result = {}
    for phase in phases:
        result[f"D10_minus_D1:{phase}"] = contrast_vector(
            names, {f"{target}|D10:{phase}": 1, f"{target}|D1:{phase}": -1})
    for first, last in combinations(phases, 2):
        result[f"tail_spread_change:{last}_minus_{first}"] = (
            result[f"D10_minus_D1:{last}"] - result[f"D10_minus_D1:{first}"])
    return result
