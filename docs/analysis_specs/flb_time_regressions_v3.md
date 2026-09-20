# Multisport FLB time regressions v3

**Status:** production contract, 20 September 2026

## Scope

This contract retains every model, time definition, kernel estimator, support rule,
weighting scheme, and uncertainty calculation in
`docs/analysis_specs/flb_time_regressions_v2.md`. It adds a parallel trade-sample
comparison built from the same frozen exact-fill artifacts.

## Trade samples

Both samples contain resolved game-winner moneyline BUY fills with exact block
timestamps. They use the same accepted event cohort, winning token, recorded event
start and end, and post-end exclusion.

- `filtered_trades`: `0.01 < price < 0.99` and the outcome-token buyer is not flagged
  nonhuman.
- `all_trades`: `0 < price < 1`; flagged outcome-token buyers are retained.

The comparison therefore changes both buyer filtering and extreme-price support, as
specified by the repository's established sample names. It is not a bots-only
sensitivity. A bots-only sensitivity would keep the strict filtered price support and
requires a separate label.

## Exact-fill normalization

The estimator reads the frozen pre-filter exact-fill artifacts for the six newer
sports, MLB, NFL, and NBA. The frozen phase artifacts supply only the accepted market,
outcome, and start/end metadata for MLB/NFL/NBA. Timing metadata must be unique by
market before joining, every estimated fill must satisfy `timestamp <= event end`, and
the bought-contract outcome is always token normalized.

The MLB filtered sample is rebuilt by joining the exact outcome-token buyer to the
same frozen wallet-flag artifact used by the six newer sports. This corrects the prior
legacy normalization, which applied `analysis_eligible` to MLB but did not actually
exclude flagged buyers. NFL/NBA and the six newer sports must reconcile exactly to
their previously frozen filtered counts; the corrected MLB count is recorded in the
run manifest.

## Report contract

The LaTeX report presents the common model and calculation definitions once, followed
by complete parallel sections for `filtered_trades` and `all_trades`. Each section
contains the same data-decision table, support table, pregame-time table, pooled and
sport regressions, continuous kernel figures, live-bin numerical audit, pooled
variations, piecewise model, and continuous-price model. Table labels and figure files
are namespaced by trade sample.

## Required gates

- Exact-fill identities are unique and timing/outcome metadata are unique by market.
- `filtered_trades` is a subset of `all_trades` under the common event/time cohort.
- Every output manifest records the explicit trade-sample rule and all input
  fingerprints.
- Both estimator runs pass the same rank, support, suppression, and interval checks.
- The combined LaTeX source compiles in two passes; every page is rendered and visually
  checked before release.

## Reproducibility

- Estimator: `analysis/multisport_game_dynamics/estimate_flb_decay.py`
- Renderer: `analysis/multisport_game_dynamics/render_flb_decay.py`
- Focused tests: `tests/test_multisport_flb_decay.py`
- Production root:
  `/mnt/data/runs/2026-09-20_flb_time_regressions_filtered_all_v1`
- Local report bundle: `output/flb_time_regressions_filtered_all_v1/03_report`
