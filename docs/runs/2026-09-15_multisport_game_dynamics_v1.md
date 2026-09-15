# Multisport game dynamics v1

**Status:** completed production run on 2026-09-15. Results are descriptive
bought-contract calibration estimates for moneyline markets.

## Scope and final artifacts

- Added cohorts: NHL, men's college basketball, ATP, WTA, EPL, college football,
  WNBA, and UFC. ATP and WTA remain separate report cohorts.
- Combined report: those eight cohorts plus the frozen MLB, NFL, and NBA runs.
- Run root: `/mnt/data/runs/2026-09-14_multisport_game_dynamics_v1`.
- Accepted stages: `01_candidates`, `02_timing_v3`, `03_trades`, `04_estimates`,
  and `05_report_v2`.
- Local report source and compiled preview:
  `output/major_sports_game_dynamics/major_sports_game_dynamics.tex` and
  `output/major_sports_game_dynamics/major_sports_game_dynamics.pdf`.

The report contains the two separate closing-calibration tables, a complete phase
D10-minus-D1 spread table, all fixed D1--D10 phase tables, and isolated-point figures.
It contains no boundary grace period and does not label closing calibration as CLV.

## Reconciliation

| Gate | Result |
|---|---:|
| Strict candidates | 18,921 events |
| Provider/result matches | 12,444 events |
| Timing-eligible events | 9,714 events |
| Exact scoped BUY fills | 17,073,480 |
| Missing exact block timestamps | 0 |
| Filtered phase BUY fills, new cohorts | 6,360,243 |
| Combined phase BUY fills | 11,212,997 |
| Combined closing observations | 31,397 |
| Phase fixed-bin rows | 460 |
| Closing fixed-bin rows | 220 |
| D1/D10 spread rows | 68 |

Timing-eligible event counts were NHL 1,363; men's CBB 4,432; CFB 488; WNBA 388;
EPL 237; ATP 1,355; WTA 1,163; and UFC 288. The final timing pass requests full
Division-I/FBS college slates, ignores provider serialization order while retaining
literal wallclock values, and permits early-finish UFC bouts without inventing later
rounds.

Tennis elapsed thirds use the ESPN scoreboard competition time plus a uniquely matched
completed duration, not inferred set boundaries. The six-hour timing audit placed
2,708,036 ATP and 1,755,809 WTA fills inside the constructed live window, versus
280,368 and 205,691 in the following six hours. Median all-trades close age was 32
seconds for ATP and 74.5 seconds for WTA.

## Main D10-minus-D1 phase estimates

Estimates are percentage points. Intervals are the estimator's nominal clustered 95%
intervals. Only compact results that materially organize the full tables are recorded
here.

| Cohort and phase | Spread | 95% interval |
|---|---:|---:|
| NFL pregame | 14.84 | [11.71, 17.96] |
| NBA pregame | 7.46 | [0.21, 14.71] |
| Men's CBB pregame | 6.26 | [2.80, 9.72] |
| WNBA pregame | 11.77 | [4.74, 18.80] |
| UFC pregame | 17.54 | [16.23, 18.84] |
| Men's CBB first half | 4.76 | [2.18, 7.34] |
| Men's CBB second half and overtime | -0.02 | [-2.87, 2.83] |
| CFB quarter 4 and overtime | 4.84 | [1.54, 8.15] |
| WNBA quarter 2 | 8.06 | [1.89, 14.23] |
| UFC round 1 | 7.49 | [0.85, 14.13] |
| UFC round 3 and later | 8.86 | [5.45, 12.26] |
| NBA quarter 4 and overtime | -5.47 | [-10.55, -0.40] |
| ATP first elapsed third | -20.11 | [-38.69, -1.52] |
| ATP middle elapsed third | -13.72 | [-24.45, -2.99] |
| WTA middle elapsed third | -10.13 | [-20.21, -0.04] |
| WTA final elapsed third | 3.46 | [0.24, 6.68] |

Pregame MLB and NHL tail spreads were suppressed for insufficient D10 support. Closing
D1/D10 spreads were also suppressed for every cohort except men's CBB; its all-trades
closing spread was 3.64 pp [1.27, 6.01] and filtered spread was 5.12 pp [2.87, 7.37].
The complete fixed-bin profiles remain the primary evidence.

## Verification

- Focused multisport suite: 49 tests passed on EC2.
- Full relevant multisport and existing sports-game-dynamics suite: 106 tests passed.
- Estimator cardinality and normalization gates passed for all 11 report cohorts.
- LaTeX compiled in two passes to a 30-page PDF.
- All 30 pages were rendered and visually inspected; no clipped tables, blank pages, or
  connected decile points were present.
