# Multisport game dynamics v2

**Status:** completed production revision on 2026-09-15. Results are descriptive
bought-contract calibration estimates for moneyline markets.

## Scope and final artifacts

- Report cohorts: MLB, NFL, NBA, NHL, men's college basketball, ATP, EPL, college
  football, and WNBA.
- WTA and UFC remain in the immutable upstream candidate, timing, and trade stages but
  are excluded from every released Stage 04 observation and summary artifact.
- Every fixed-bin estimate with fewer than 500 trades is withheld. A D10-minus-D1
  contrast is withheld when either tail has fewer than 500 trades.
- Run root: `/mnt/data/runs/2026-09-14_multisport_game_dynamics_v1`.
- Accepted revised stages: `04_estimates_v2` and `05_report_v3`; upstream stages
  `01_candidates`, `02_timing_v3`, and `03_trades` are unchanged.
- Local report source and compiled preview:
  `output/major_sports_game_dynamics_v2/major_sports_game_dynamics.tex` and
  `output/major_sports_game_dynamics_v2/major_sports_game_dynamics.pdf`.

The report retains support and an explicit status for withheld estimates and omits
suppressed estimates from isolated-point figures.

## Reconciliation

| Gate | Result |
|---|---:|
| Strict upstream candidates | 18,921 events |
| Upstream timing-eligible events | 9,714 events |
| Upstream exact scoped BUY fills | 17,073,480 |
| Missing exact block timestamps | 0 |
| Combined retained phase BUY fills | 9,992,788 |
| Combined retained closing observations | 28,525 |
| Phase fixed-bin rows | 380 |
| Closing fixed-bin rows | 180 |
| D1/D10 spread rows | 56 |
| Withheld phase-bin rows | 10 |
| Withheld closing-bin rows | 167 |
| Withheld spread rows | 21 |

Every released Parquet has exactly the nine report cohorts. WTA and UFC have zero rows in
`normalized_phase_trades.parquet`, `normalized_closing_lines.parquet`,
`phase_calibration.parquet`, `closing_calibration.parquet`, and `flb_spreads.parquet`.

## NHL pregame support check

| Bin | Trades | Status |
|---|---:|---|
| D1 | 90 | Withheld (<500) |
| D2 | 292 | Withheld (<500) |
| D3 | 15,637 | Reported |
| D4 | 120,043 | Reported |
| D5 | 293,462 | Reported |
| D6 | 314,114 | Reported |
| D7 | 117,267 | Reported |
| D8 | 12,414 | Reported |
| D9 | 96 | Withheld (<500) |
| D10 | 27 | Withheld (<500) |

The NHL pregame D10-minus-D1 spread is withheld because D1 has 90 trades and D10 has
27. No tail estimate is plotted or reported as evidence of reverse FLB.

## Verification

- Synthetic boundary coverage verifies that 499 trades are withheld and 500 are
  reported for phase and closing fixed bins.
- Renderer coverage verifies that suppressed rows never reach plotting calls, all point
  series use `linestyle="none"`, and stale WTA/UFC inputs fail closed.
- Full relevant sports regression suite: 378 tests and 15 subtests passed.
- The LaTeX report compiled without substantive warnings to a 26-page PDF.
- All 26 pages were rendered and visually inspected; tables fit the print area and no
  connected decile points, clipped content, or blank pages were present.
