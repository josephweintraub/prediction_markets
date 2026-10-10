# September 28 FLB writeup: Polymarket-only replication

Status: in progress; no accepted estimates or final findings yet.

## Contract and authority

- Replicate the full supplied `flb_summary.pdf` using repaired Polymarket CLEAN,
  retaining the archived taker-direction convention and separately reporting BUY
  observations. These roles are not independently certified native actions.
- User selected the existing native categories and exact normalized binary64
  claim-price levels, not a reconstructed broad taxonomy or one-cent bins.
- Executions before 2026-03-25 UTC; no bot or up/down filter. Sports use the
  available audited nine-sport provider cohort, not a whole-history census or
  the unavailable cross-venue matched sample.
- Scientific specification: `docs/analysis_specs/kaushik_polymarket_replication_v1.md`.
  Kaushik's upstream estimation scripts remain unavailable, so reconstructed
  details are identified rather than asserted to match his implementation.

## Infrastructure and source

- Root owns EC2 lifecycle: `i-0f5b31a268af53938`, `us-east-1`.
- Mounted data UUID: `d0cd087b-94c4-428c-bae9-ae28929059f6`.
- Canonical branch: `codex/kaushik-polymarket-replication`.
- Initial input producer commit: `0165a719f9b6ec4f587653ebd110c2be77da78b9`.
- Root runs production stages serially; agents implement or audit locally and
  may not manage the instance or run production stages.
- Shutdown is required and has not yet been verified.

## Preserved input-build failure

The first build reconciled and wrote all 44 monthly outputs, then failed during
the final grouped support-count aggregation at the 4 GB DuckDB spill cap. It did
not publish an accepted input directory. No sample definition changed.

- Target: `/mnt/data/runs/2026-10-10_kaushik_polymarket_inputs_v1`.
- Preserved failed stage:
  `/mnt/data/runs/.2026-10-10_kaushik_polymarket_inputs_v1.staging-94an1044`.
- Execution receipt:
  `/mnt/data/runs/2026-10-10_kaushik_polymarket_controls_v1/input_execution/receipt.json`.
- Execution: 2026-10-10 20:35:27 to 20:48:59 UTC; exit 1.
- Wall time: 13:31.69; peak RSS: 33,383,480 KiB; no swaps.
- Failed-stage outputs occupy approximately 11.2 GB; free disk after failure:
  86,981,029,888 bytes. Available memory exceeds 264 GB.
- Small execution evidence is retained in the local report run's
  `failed_input_v1/` directory. The failed output is not an accepted sample.

A fresh versioned retry will preserve the failed evidence and bind separately
reviewed source, inputs and resource caps. Acceptance, estimation, report QA,
source publication and verified shutdown remain pending.
