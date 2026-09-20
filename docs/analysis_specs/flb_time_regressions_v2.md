# Multisport FLB time regressions v2

**Status:** production contract, 20 September 2026

## Research contract

The observation remains an eligible resolved moneyline BUY fill in the frozen
nine-sport cohort. Calibration is the eventual outcome of the bought contract minus
its purchase price, `R = Y - P`. Exact block timestamps define

\[
T_i = \frac{t_i-s_m}{e_m-s_m},
\]

where `T < 0` is pregame, `T = 0` is game start, and `T = 1` is the realized event
end. The primary sample now includes every retained pregame trade, with no lower
bound on `T`, plus live trades through `T = 1`.

The former `[-1,1]` window is retained as a labeled comparability check. Live-only
`[0,1]` and sport-median-duration time remain variations.

## Regression models

For the direct tail model, `H = 1` for fixed bought-price bin D10 and `H = 0` for D1:

\[
R_i=\alpha_s+\beta_sH_i+\gamma_sT_i+\delta_s(H_iT_i)+\varepsilon_i.
\]

The estimand is `delta_s`, the change in the D10-minus-D1 calibration spread per one
normalized-duration unit. Because the primary pregame interval is unbounded, no
whole-window multiple of `delta_s` is defined. The piecewise model uses
`min(T,0)` and `max(T,0)` to estimate separate pregame and live tail-spread slopes.
The continuous-price model is unchanged except that its primary sample also has no
pregame lower cutoff.

Pooled adjustment and weighting definitions remain those in v1: no controls, sport
intercepts, sport-specific baselines and general trends, equal-sport weights, the
literal mean supported-sport slope, and dollar weights.

## Continuous kernel profiles

The continuous plots estimate tail-specific Nadaraya-Watson means and subtract them:

\[
\widehat\mu_h(x)=
\frac{\sum_i w_iK((T_i-x)/b)R_i\mathbf 1\{i\in h\}}
     {\sum_i w_iK((T_i-x)/b)\mathbf 1\{i\in h\}},
\qquad
\widehat\Delta(x)=\widehat\mu_{D10}(x)-\widehat\mu_{D1}(x),
\]

with Epanechnikov kernel `K(u)=0.75(1-u^2)` for `|u|<1`. Pregame and live are
estimated separately so no observation smooths across game start. The bandwidth is
`0.50` before start and `0.10` live. The wider pregame bandwidth was selected from
local support counts before inspecting outcome profiles.

Live curves use 101 equally spaced points on `[0,1]`. Pregame curves use 101 empirical
quantiles of the complete negative-time tail plus `T=0`; the horizontal coordinate is
still raw normalized time. This represents the unbounded history without imposing a
display cutoff. Lines break across suppressed grid points or gaps exceeding two
bandwidths.

Pooled curves report per-fill and equal-sport weights. Sport curves use per-fill
weights. Equal-sport kernel weights use `1/N_s`, where `N_s` is the sport's count in
the complete all-pregame-plus-live tail sample.

## Support and inference

Regression support requires at least 500 fills in every required tail-by-phase cell.
A kernel grid point is reported only when at least 500 D1 and 500 D10 fills lie within
its compact-support window. Unsupported points are stored with null estimates and are
not plotted.

Regression and kernel-spread uncertainty use Cameron-Gelbach-Miller three-way
clustering by UTC trade day, buyer wallet, and event. Kernel intervals are pointwise,
not simultaneous. The former ten-bin live table remains only as a numerical audit of
the continuous live plot.

## Reproducibility

- Estimator: `analysis/multisport_game_dynamics/estimate_flb_decay.py`
- Renderer: `analysis/multisport_game_dynamics/render_flb_decay.py`
- Focused tests: `tests/test_multisport_flb_decay.py`
- Production estimates:
  `/mnt/data/runs/2026-09-20_flb_time_regressions_all_pregame_v1/01_estimates_v2`
- Local report bundle: `output/flb_time_regressions_all_pregame_v1/report_v2`

