# September 28 FLB writeup: Polymarket-only replication

Status: inputs and sports metadata accepted; validated projection-cache retry pending.

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

## Preserved second estimation failure and memory-only retry

- Sports metadata v2 and estimation preflight v2 were independently admitted
  under source `0be85ec728f8c733a27d6699c671cdff281d51e2`. Both sports metadata
  output fingerprints and their coverage/exclusion census exactly match v1.
- Estimation v2 reached the declared 16 GB total DuckDB spill cap in the sports
  cache COPY. The process-file guard conflict was resolved; the new failure is
  separately recorded and no numerical estimates were published.
- Preserved stage:
  `/mnt/data/runs/.2026-10-10_kaushik_polymarket_estimates_v2.staging-rt4oaij_`.
  Local failure evidence is in `failed_estimate_v2/`.
- Execution exited 1 after 2:42.44; peak RSS 63,447,124 KiB; no swaps. Free disk
  after failure was 75,779,145,728 bytes. Observed available RAM after failure
  was 264,425,848,832 bytes, with 267,392,999,424 bytes total and no swap.
- A bounded memory-only retry was admitted: 192 GB DuckDB plus 32 GB
  NumPy, 224 GB total budget, with at least 240 GB available RAM required.
  The 16 GB spill/transient bounds, 3 GB accepted cache, 8 GB total publication,
  60 GB disk reservation and 8 TB read ceiling remain unchanged. No sample,
  estimator, weighting or uncertainty definition changes.

## Preserved incomplete third estimate and validated projection caching

- Sports metadata v3 reopened and accepted under source
  `5f1fb327cca7974aad165f08d9beb4b48200aed9`. Its output fingerprints, provider
  coverage and exclusion census exactly match the preceding admitted versions.
- Estimation v3 built the sports and lossless duration caches, then completed
  the first four duration specifications on 140,293,451 records and 201,288
  event/market clusters. These are incomplete-run diagnostics, not released results.
- The full model retained 3,180,741 exact normalized price levels. Its repeated
  fixed-effect projections unnecessarily reconstructed covariance/cluster fields
  and replayed the same grouped query at every update. Root interrupted only the
  verified estimator, preserving its stage and execution evidence before syncing
  any new source. No accepted estimates were published.
- Preserved stage:
  `/mnt/data/runs/.2026-10-10_kaushik_polymarket_estimates_v3.staging-ls5xs33j`.
  Execution: 2026-10-10 21:56:11 to 22:52:52 UTC; exit 130, `KeyboardInterrupt`.
  Local receipts and logs are in `interrupted_estimate_v3/`.
- The reviewed optimization freezes only lossless grouped X/Y means, integer
  observation counts, frequency weights and effect codes from the unchanged
  first query stream. It admits retained buffers plus construction, Python and
  existing absorber/workspace reservations against the unchanged 32 GB NumPy
  ceiling; owned arrays and mappings are read-only. Exact groups/N reconcile.
  The captured buffers are released before the unchanged full-moment, event-score
  and R-squared replays. No trade-body arrays or covariance/string fields are retained.
- The production-sized full-model admission estimate is about 2.17 GB under
  the existing 32 GB ceiling. This is a resource estimate, not observed production
  allocation or a timing guarantee. SQL, sample definitions, exact price levels,
  tolerance, iteration limit, rank and uncertainty gates remain unchanged.
- Independent local QA passed 106 tests in 49.636 seconds. Dense five-model/A1
  oracles and uncached-full/uncached-lean/frozen-lean comparisons preserve
  coefficients, event scores, covariance, R-squared and projection diagnostics.
  Cap-boundary, malformed/overflow counts, mutation, source-count and
  release-before-full-replay checks pass. No new global join-order guarantee or
  independent raw/native-action certification is claimed.
- A separately guarded saved-score auditor is ready to reconcile every accepted
  joint covariance, named contrast, uncertainty interval, influence diagnostic
  and saved count/support grid after production. It reads saved scores and
  summary metadata, not raw trades, and records those limitations explicitly.

Fresh source-bound sports metadata, estimation v4, saved-score QA, final report,
publication and verified shutdown remain pending.
