# Multisport FLB time regressions: all pregame and kernel profiles

**Status:** complete production run  
**Estimate run:** `/mnt/data/runs/2026-09-20_flb_time_regressions_all_pregame_v1/01_estimates_v2`  
**Local data bundle:** `output/flb_time_regressions_all_pregame_v1/data`  
**Local report bundle:** `output/flb_time_regressions_all_pregame_v1/report_v2`

## Contract and inputs

This run implements `docs/analysis_specs/flb_time_regressions_v2.md`. It uses the
same four frozen phase artifacts and bought-contract calibration definition as the
15 September run. All 9,992,788 eligible fills reconcile to the previous production
cohort.

The primary regression sample includes all retained `T < 0` fills and live fills
through `T = 1`. The former `[-1,1]` sample is retained as a comparability check.
Continuous figures use phase-specific Epanechnikov kernel averages with bandwidth
`0.50` pregame and `0.10` live.

## Outputs

- `coefficients.parquet`: 910 displayed and nuisance coefficients.
- `model_summary.parquet`: 82 model records.
- `estimands.parquet`: 93 direct and derived estimands.
- `support.parquet`: 126 tail-by-phase support records.
- `duration_reference.parquet`: nine sport duration records.
- `time_bin_spreads.parquet`: 110 retained live-bin audit records.
- `kernel_time_spreads.parquet`: 2,206 pooled and sport kernel grid records.
- `pregame_time_distribution.parquet`: 18 sport-tail time-distribution records.
- `manifest.json`: immutable input/output provenance and frozen definitions.

## Reconciliation and QA

- Focused estimator tests: `7 passed` locally and on EC2.
- Relevant multisport suite: `37 passed` locally; one unrelated dependency
  deprecation warning.
- Every reported model is full rank. The largest design condition number is about
  977,392 in an all-price pooled model; it is recorded rather than treated as a silent
  failure.
- Kernel keys are unique. No reported row has a missing estimate or interval, every
  reported interval contains its estimate, and every suppressed row has null results.
- The kernel support gate reports all 101 pooled live points and 92 of 102 pooled
  pregame points. Sport pregame curves are locally supported for ATP, men's CBB,
  college football, EPL, and NBA; the other four are explicitly withheld.
- The final PDF compiles in two passes to eight letter-size pages. All pages were
  rendered to PNG and checked for clipping, overflow, broken labels, and plotted
  suppressed values.

## Result map

Including all pregame history materially changes the linear slope. The all-nine-sport,
sport-baseline/trend-adjusted estimate is -0.13 percentage points per normalized
duration under per-fill weighting and -0.12 under equal-sport weighting. The supported
seven-sport counterparts are -0.19 and -0.17. The former bounded `[-1,1]` estimates
remain +7.88 and +3.68, while the live-only estimates remain +12.83 and +8.22.

The all-pregame piecewise supported-sport estimates are -0.26 percentage points per
normalized duration before start and +3.08 live under per-fill weighting; equal-sport
estimates are -0.17 and -0.18. The full-history pregame slope therefore summarizes a
much longer and compositionally different horizon than the former one-duration window.

The pooled live kernel curve remains nonlinear: the spread is negative through most of
live time and turns positive near the event end. Sport pregame curves are displayed only
where both local tails pass the 500-fill rule. The retained pregame history is very long
in normalized units, reaching minima near -862 in NBA, which is why the report includes
the full sport-tail time-distribution table and keeps the old bounded regression visible.
