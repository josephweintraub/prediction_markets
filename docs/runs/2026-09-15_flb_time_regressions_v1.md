# Multisport FLB time regressions v1

**Status:** complete production run  
**Estimate run:** `/mnt/data/runs/2026-09-15_flb_time_regressions_v1/01_estimates_v4`  
**Local report bundle:** `output/flb_time_regressions_v4/report`

## Frozen inputs

- New six-sport phase fills:
  `/mnt/data/runs/2026-09-14_multisport_game_dynamics_v1/03_trades/phase_trades.parquet`
- MLB phase fills:
  `/mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/05_phase_dataset_v3/phase_trades.parquet`
- NFL phase fills:
  `/mnt/data/runs/2026-09-12_nfl_game_dynamics_v3/06_phase/phase_trades.parquet`
- NBA phase fills:
  `/mnt/data/runs/2026-09-12_nba_game_dynamics_v3/06_phase/phase_trades.parquet`

The production manifest fingerprints every input and output. All 9,992,788 eligible
phase fills reconcile to the preceding multisport estimator.

## Outputs

- `coefficients.parquet`: 854 displayed and nuisance coefficients.
- `model_summary.parquet`: 82 model-level records.
- `estimands.parquet`: 118 direct and derived estimands.
- `support.parquet`: 126 tail-by-segment support records.
- `duration_reference.parquet`: one median-duration record per sport.
- `time_bin_spreads.parquet`: 110 live-time-bin tail-spread records.
- `manifest.json`: input/output provenance and frozen definitions.

## Reconciliation and QA

- Focused regression tests: `4 passed` locally and on EC2.
- Relevant multisport suite: `34 passed`; one unrelated dependency deprecation warning.
- Every non-withheld model is full rank; every withheld sport-specific estimate is tied
  to an explicit sub-500 segment-tail support record.
- Report compilation: 8 letter-size pages, two passes, zero final LaTeX warnings.
- Every PDF page was rendered to PNG and visually checked for clipping, overflow,
  broken labels, blank pages, and omitted suppressed points.

## Result map

The realized-duration, composition-adjusted pooled tail slope is +7.88 percentage points
per normalized duration under equal-fill weighting and +3.68 under equal-sport weighting.
The live-only counterparts are +12.83 and +8.22. Fixed sport-median-duration estimates
are -3.20 and -3.78, respectively. In the balanced five-sport pool, the literal mean
sport slope is +4.01 for realized-duration time and -2.85 for fixed-duration time.

The live-bin profile is nonlinear. The equal-fill D10-minus-D1 spread is negative in the
first nine live-time bins and +4.80 percentage points in the final tenth; equal-sport
weighting gives the same qualitative shape. The supported five-sport piecewise model
estimates a negative pregame change (-19.57 equal fill; -13.43 equal sport) and a
positive live change (+13.38; +7.80). The simple linear slope does not provide a robust
monotone-decline summary.

Unified sport-specific tail fits are reported for NBA, men's CBB, ATP, EPL, and college
football. MLB, NFL, NHL, and WNBA are withheld because at least one pregame D1/D10 cell
within `T in [-1,1]` contains fewer than 500 fills. All nine live-only sport fits pass the
tail support floor.
