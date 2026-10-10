# September 28 FLB writeup: Polymarket-only replication

Status: inputs and sports metadata accepted; estimation resource retry pending.

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

The failed first input run remains preserved.

## Sports metadata and preserved first estimation failure

- Sports metadata v1 reopened and accepted under source
  `a39b22d17c59ec689b6f9b1606fe3660586e6c96` without reading monthly trade bodies.
- The nine provider cohorts contain 13,909 games and 14,383 markets. These are
  metadata coverage counts, not observed trade-sample support.
- The first estimation stage stopped during the sports observation COPY before
  numerical estimation. Its process-wide 3 GB COPY file limit also capped the
  DuckDB spill file despite a separately declared 16 GB total spill allowance.
- Preserved stage:
  `/mnt/data/runs/.2026-10-10_kaushik_polymarket_estimates_v1.staging-i44p6ze1`.
- Execution exited 1; no accepted estimates or research report was published.
  Small failure evidence is saved locally in `failed_estimate_v1/`.
- The resource-only retry retains all sample definitions and accepted output
  limits. Independent review accepted the compatible transient write guard.
  The full focused suite passed 89 tests locally; the independent resource and
  numerical subset passed 66 tests. Production artifact size remains unverified.
  The transient process per-file bound is 16 GB; the accepted cache/map/score
  limits remain 3 GB/500 MB/1 GB. The final 8 GB output cap includes saved JSON
  and receipts. Initial disk admission reserves 60 GB, including a 20 GB floor.
  A new source commit requires fresh sports-metadata admission; existing source
  bindings will not be weakened or overwritten.

Fresh metadata admission, accepted estimates, report QA, source publication and
verified shutdown remain pending.
