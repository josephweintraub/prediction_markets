# MLB pre-filter exact extract and sample repair

**Status:** implementation and fixture review; production outputs not yet published.
**Authorization:** user approved the proposed MLB extract rebuild on 9 October 2026.

## Scope

Recover MLB candidate fills excluded by the historical extract's upstream price
and inferred-buyer bot filters. Preserve the existing accepted market cohort,
recorded outcomes, official event boundaries, exact block timestamp source and
legacy inferred-BUY normalization. Create a new immutable pre-filter extract and
derive filtered and all-trades samples separately. Do not overwrite historical
artifacts or rerun other sports or scientific estimators in this stage.

The all-trades sample retains flagged inferred buyers and requires `0 < P < 1`.
The filtered sample requires `0.01 < P < 0.99` and excludes buyers flagged by the
frozen shared pipeline wallet-flags artifact. Both exclude post-end trades and
retain all pregame history. The old learnability-cache flags are not substituted
for the shared flags used by the latest report contract.

## Inputs and reconciliation

- Resolved fills: `/mnt/data/pipeline_data/resolved_trades.parquet`.
- Exact cache: `/mnt/data/pipeline_data/block_timestamps.parquet`, validated against
  `configs/data_vintages/mlb_exact_timestamps_2026-07-04.json`.
- Candidate markets, old exact extract and phase-derived accepted metadata:
  `/mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/`.
- Sample flags: `/mnt/data/pipeline_data/wallet_flags.parquet`.
- New production destination: `/mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1`.

Require immutable fill identity and payload reconciliation, zero missing exact
timestamps, unique join keys, explicit exclusion accounting, a filtered subset of
all trades, and unchanged payloads for every historical exact row. Replaying the
old extract through the current cohort and flag definitions checks historical
sample counts; those counts are not targets for the repaired samples.

## Infrastructure and release

Canonical starting revision: `33fdf9f0772793cddd7227d130a214883d6fea65`.
The root owns all EC2 lifecycle and storage operations. The data volume UUID was
verified before mounting, with 25,854,373,888 bytes available. Use bounded memory,
spill and output budgets; disable the previously diagnosed DuckDB common-subplan
optimizer without changing the installed runtime.

Before release, commit scoped source changes on EC2, run fixtures and independent
artifact QA, transfer compact summaries, compile and visually check a concise
LaTeX count report, then stop and verify the instance after all work and transfers.
Broader native-action completeness and wallet-label correctness remain separate
unresolved questions; this repair does not certify them.
