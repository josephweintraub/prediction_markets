# September 28 FLB writeup: Polymarket-only replication

Status: inputs accepted; sports metadata and estimation pending.

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

## Accepted fresh input retry

- Input run: `/mnt/data/runs/2026-10-10_kaushik_polymarket_inputs_v2`.
- Committed source: `41f44b25dcf945d06c5ade3aaf5b766da6a8b41f`.
- New preflight bound 64 GB DuckDB memory, 16 GB spill, 32 GB total output,
  a 20 GB free floor and a 68 GB output/spill/free-space reservation. Source
  definitions, per-file limits and row membership were unchanged.
- Execution: 2026-10-10 21:03:33 to 21:22:34 UTC; exit 0. All saved outputs,
  schemas, hashes, row counts and support reopened before acceptance.
- Source rows: 2,036,128,538; at/after-cutoff exclusions: 986,448,998.
- Pre-cutoff rows: 1,049,679,540; exclusions: 5,474,900 unadmitted-token-pair
  records and two invalid-price records.
- All-role eligible records: 1,044,204,638; primary taker records: 522,102,319.
- Primary tails: 158,580,409; duration-eligible tails: 140,293,451.
- Analytic and descriptive amount anomalies: zero. The 89 saved Parquet outputs
  total 11,195,816,047 bytes.
- Count/support arithmetic was independently checked from saved metadata.
  That check does not independently establish native-action correctness,
  whole-history collection completeness or exact trade/event timestamps.

The failed first run remains preserved. Sports metadata, estimates, report QA,
source publication and verified shutdown remain pending.
