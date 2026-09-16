# Multisport FLB time regressions v1

**Status:** production contract, 15 September 2026

## Research contract

The observation is an eligible BUY fill in the frozen nine-sport resolved moneyline
cohort. Calibration is the eventual outcome of the bought contract minus its purchase
price, `R = Y - P`. MLB, NFL, NBA, NHL, men's college basketball, ATP, EPL, college
football, and WNBA use the same bought-contract definition.

Exact block timestamps define normalized time:

\[
T_i = \frac{t_i-s_m}{e_m-s_m},
\]

where `s_m` and `e_m` are the accepted event start and end. Pregame trades have
`T < 0`, game start is `T = 0`, and event end is `T = 1`.

The primary window is `T in [-1,1]`. Required variations are live-only `[0,1]`, wider
pregame `[-2,1]`, and normalization by the sport-specific median realized duration.

## Models

For the direct tail model, `H = 1` for fixed bought-price bin D10 and `H = 0` for D1:

\[
R_i=\alpha_s+\beta_sH_i+\gamma_sT_i+\delta_s(H_iT_i)+\varepsilon_i.
\]

The primary estimand is `delta_s`, the change in the D10-minus-D1 calibration spread
per normalized game duration. For `T in [-1,1]`, the fitted whole-window change is
`2 delta_s`. A live-only coefficient is the fitted start-to-end change.

The continuous-price model is secondary:

\[
R_i=\alpha_s+\beta_s(P_i-.5)+\gamma_sT_i+
\delta_s^p((P_i-.5)T_i)+\varepsilon_i.
\]

Its interaction changes the calibration gradient across price; it is not a literal
D10-minus-D1 contrast.

The game-start diagnostic replaces `T` with `T- = min(T,0)` and `T+ = max(T,0)` and
includes separate tail interactions for the pregame and live segments.

## Pooled specifications and weighting

- Unadjusted pooled: common intercept, D10 level, time slope, and D10-by-time slope.
- Sport fixed effects: unadjusted model plus sport intercepts.
- Composition adjusted: sport-specific intercepts, D10 baselines, and general time
  slopes, with a common D10-by-time coefficient.
- Equal sport: composition-adjusted regression with `w_i = 1/N_s`, using final eligible
  estimation-sample counts.
- Mean sport slope: fully interacted model followed by an equal-weight linear contrast
  of the supported sport-specific D10-by-time coefficients.
- Dollar weighted: separate robustness result using fill dollars as weights.

## Support and inference

Sport-specific unified and piecewise tail models require at least 500 fills in each
required D1/D10-by-segment cell. Unsupported estimates are serialized as withheld.
Balanced-support pooled and pooled piecewise estimates retain only sports that pass the
same rule. Live fixed-time-bin profiles suppress a bin when either tail contains fewer
than 500 fills.

All coefficient and bin-spread intervals use Cameron-Gelbach-Miller three-way clustering
by UTC trade day, buyer wallet, and event. P-values use the asymptotic standard-normal
reference for the clustered t-statistic.

## Reproducibility

- Estimator: `analysis/multisport_game_dynamics/estimate_flb_decay.py`
- Renderer: `analysis/multisport_game_dynamics/render_flb_decay.py`
- Focused tests: `tests/test_multisport_flb_decay.py`
- Production estimates: `/mnt/data/runs/2026-09-15_flb_time_regressions_v1/01_estimates_v4`
- Reader-facing report: local immutable bundle `output/flb_time_regressions_v4/report_v4`
