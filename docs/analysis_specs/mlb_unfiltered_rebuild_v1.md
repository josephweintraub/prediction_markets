# MLB unfiltered exact-input rebuild v1

Status: implementation contract, 9 October 2026. This stage repairs analytic input
coverage; it computes no calibration, regressions, kernels, closes, or other new
scientific estimates. Historical builders, inputs, and completed runs remain unchanged.

## Frozen scope and semantics

Rebuild the 3,790-market candidate extract from the same resolved-fill vintage,
preserving the original source projection, case normalization, exact-payload replay
deduplication, immutable fill identities, and exact timestamp cache. Import the
unchanged original MLB helpers (builder SHA256 `6802cfe15e21b6f4375fc8655f88b33a7d9de0e21d09a716d688f78dea125f44`)
from the canonical checkout. The full pre-filter extract retains all 6,912,624 distinct
candidate fills: no upstream binary-price-range or bot exclusion is permitted.

The observation remains the legacy inferred outcome-token BUY per resolved fill.
It is not certified counterparty own economic action. This task does not repair
wallets, native collection/canon, flags, fees, or NORMAL/MINT/MERGE semantics.
Resolution censoring and uncertainty about historical flag labels remain.

Analytic samples use only the existing frozen phase-derived metadata market set:
market ID, MLB game ID, official date, winning outcome, actual start, and actual end.
Do not recover additional games or replace timing/provider records. Market metadata
must be unique and nonnull, with finite, positive and ordered event timestamps.
Tokens and outcomes must map unambiguously within each accepted market.

- `all_trades`: same accepted cohort, exact timestamp at or before recorded event
  end, and `0 < price < 1`; flagged inferred buyers are retained.
- `filtered_trades`: same cohort/time, `0.01 < price < 0.99`, and
  `NOT coalesce(current_shared_flags.is_nonhuman,false)`.

All pregame history is retained, with no lower time cutoff. Event-end equality is
included. UTC trade day and realized time `(t-start)/(end-start)` follow the existing
v3 loader. Fixed-duration time is deliberately not exported: the unchanged loader
later recomputes the sport median duration among unique events represented in each
sample. No pooled constant or newly frozen denominator is substituted.

“Unfiltered” means the v3 all-trades definition within the frozen accepted cohort,
not every exchange action or price. Positive native filled amounts, positive finite
cash/price, nonnull payload, and positive exact block timestamps are validity gates.
Any invalid source row blocks the run rather than being silently omitted. Finite
prices at or above one remain in the pre-filter artifact and are counted separately
when deriving valid-price samples. Nullable `is_nonhuman` follows the current
COALESCE-false rule; missing and null flags are counted separately in `all_trades`.
Wallet keys must be nonblank and unique after lowercase normalization.

## Inputs, frozen bindings, and reproduction

Required named inputs are `raw`, `candidates`, `timestamp_provenance`, `cache`,
`phase`, `wallet_flags`, and `old_exact`. Use the Sep7 candidate and exact artifacts,
existing `05_phase_dataset_v3/phase_trades.parquet`, exact-cache declaration
`configs/data_vintages/mlb_exact_timestamps_2026-07-04.json`, and the Sep20 shared
`/mnt/data/pipeline_data/wallet_flags.parquet`. No legacy learnability flags are needed
or presumed unchanged.

Production binds raw/cache/old-exact/shared-flag SHA256 values in the new script.
Candidate, phase, and provenance files are frozen by stat and complete hash before
body queries. Every input's stat and hash must agree again before publication.
Source HEAD and producer/helper/spec hashes are similarly bound before and after.

The old exact artifact must contain unique nonnull native fill identities and be an
exact, null-aware, full-14-column payload subset of the new pre-filter extract.
Reapplying the current v3 cohort/time/flag predicates to that old artifact must
reproduce 2,502,803 old all-trades rows and 2,412,918 old filtered rows; its exact
population must be 2,555,139 rows. These are legacy-reproduction gates, not target
counts for the corrected samples. New all/filtered samples may add eligible rows.

## Row laws and outputs

The immutable fill identity is `(transaction_hash, log_index, exchange_address)`.
One normalized pre-filter row must correspond to every distinct candidate fill.
Equal economics under distinct identities remain separate. Every output is reopened
and checked for full multiset payload equality, identity uniqueness and exact cache
timestamps. Sample identity/payload subset laws must hold.

Sequential, nonoverlapping accounting is:

1. `prefilter_exact_rows = outside_accepted_market_rows + post_end_rows +
   invalid_sample_price_rows + accepted_valid_price_rows`.
2. `new_accepted_all_rows = accepted_valid_price_rows`.
3. `new_accepted_all_rows = filtered_extreme_price_exclusions +
   filtered_flagged_interior_exclusions + new_accepted_filtered_rows`.
4. `restored_{all,filtered}_rows = new_accepted_{all,filtered}_rows -
   old_accepted_{all,filtered}_rows`, with each old sample a full-payload subset.
5. Raw candidate rows equal distinct fills plus exact-payload replay rows.

Publish `exact_trades.parquet` with the original ordered 14-column schema.
Publish `all_trades.parquet` and `filtered_trades.parquet` with those columns plus
`game_pk BIGINT`, `official_date DATE`, `actual_start_utc TIMESTAMPTZ`,
`actual_end_utc TIMESTAMPTZ`, `buyer_is_flagged_nonhuman BOOLEAN`, `won BOOLEAN`,
`calibration_error DOUBLE`, `trade_day DATE`, and `realized_time DOUBLE`.
The exact artifact is the compatible replacement for the estimator's `--mlb-exact`
input. The enriched samples are saved audit/analysis inputs; they are not arguments
to the current estimator. All other estimator input paths and algorithms stay fixed.

`summary.json` contains completion/noncertification status, explicit definitions,
source/command/environment, input identities, output hashes/schemas/counts,
disjoint exclusions, restored rows, represented market/event support, resource
preflight and reconciliation gates. `manifest.json` also binds summary bytes/hash.
Atomic no-replace directory publication exposes both together; existing directories
are never replaced. Failure staging and `failure.json` remain recoverable evidence.

## Production admission and execution

Entrypoint: `scripts/rebuild_mlb_unfiltered_samples.py`. Its production CLI always
calls `production_guard.require_production_host()`. Local tests use tiny synthetic
files, explicit test hash/count dictionaries, and the source-only helper snapshot;
there are no production hash/count override CLI flags.
The production CLI also requires the producer, fixture tests, specification and
frozen helpers to be tracked and unmodified relative to that committed HEAD.

First run `--preflight-only` with all seven paths, `--expected-head`, and a new
`--run-dir`. This reads footer/stat/resource metadata, not trade queries, and clearly
records that complete input hashes have not yet been checked. Root reviews that
saved `summary.json`. Body execution requires its path and SHA256 through
`--reviewed-preflight` and `--reviewed-preflight-sha256`, plus another fresh run dir.

Fixed production budgets: DuckDB 96 GB, eight threads, maximum spill 4,000,000,000 B,
combined final output 4,000,000,000 B, minimum remaining free disk 12,000,000,000 B,
eight million scoped raw candidate rows, and a conservative 1 TiB read-footprint cap.
Initial free space must cover free-floor plus spill and output allowances. Record
and disable `common_subplan` when available; use UTC. Memory and read estimates are
planning gates, not hard RSS limits or measured physical I/O. After each output,
check actual artifact bytes and free-space floor before publication. The root owns
the durable runner, source commit and all infrastructure actions.

Focused fixtures: `tests/test_rebuild_mlb_unfiltered_samples.py`. Independent saved
artifact QA and any later numerical/report reruns are separately authorized stages.

### Immutable stage destinations and command templates

The approved run parent is `/mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1`.
Use `00_preflight_v1` for metadata admission, `01_samples_v1` for the body,
`02_independent_qa_v1` for the separate saved-artifact checker, and `03_report_v1`
for local-first compact reporting. These are fresh child directories, not repeated
publications to the parent. A root-owned durable tmux runner records the timed
command, regular-file stdout/stderr, and final exit status in `runner_v1`, outside
the atomic body destination. Do not reuse a completed destination after interruption.

Run from `/home/ubuntu/prediction_markets`. Root must replace `REVIEWED_COMMIT_SHA`
with the reviewed committed 40-hex HEAD and, for the body, replace
`REVIEWED_PREFLIGHT_SHA256` with the complete saved preflight-summary SHA256.
Neither placeholder is a production override or an accepted literal.

```sh
/home/ubuntu/venv/bin/python -u scripts/rebuild_mlb_unfiltered_samples.py \
  --raw /mnt/data/pipeline_data/resolved_trades.parquet \
  --candidates /mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/01_market_universe/candidate_markets.parquet \
  --timestamp-provenance /home/ubuntu/prediction_markets/configs/data_vintages/mlb_exact_timestamps_2026-07-04.json \
  --cache /mnt/data/pipeline_data/block_timestamps.parquet \
  --phase /mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/05_phase_dataset_v3/phase_trades.parquet \
  --wallet-flags /mnt/data/pipeline_data/wallet_flags.parquet \
  --old-exact /mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/02_exact_trades/exact_trades.parquet \
  --expected-head REVIEWED_COMMIT_SHA \
  --preflight-only \
  --run-dir /mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1/00_preflight_v1

/home/ubuntu/venv/bin/python -u scripts/rebuild_mlb_unfiltered_samples.py \
  --raw /mnt/data/pipeline_data/resolved_trades.parquet \
  --candidates /mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/01_market_universe/candidate_markets.parquet \
  --timestamp-provenance /home/ubuntu/prediction_markets/configs/data_vintages/mlb_exact_timestamps_2026-07-04.json \
  --cache /mnt/data/pipeline_data/block_timestamps.parquet \
  --phase /mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/05_phase_dataset_v3/phase_trades.parquet \
  --wallet-flags /mnt/data/pipeline_data/wallet_flags.parquet \
  --old-exact /mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/02_exact_trades/exact_trades.parquet \
  --expected-head REVIEWED_COMMIT_SHA \
  --reviewed-preflight /mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1/00_preflight_v1/summary.json \
  --reviewed-preflight-sha256 REVIEWED_PREFLIGHT_SHA256 \
  --run-dir /mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1/01_samples_v1

/home/ubuntu/venv/bin/python -u scripts/audit_mlb_unfiltered_samples.py \
  --manifest /mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1/01_samples_v1/manifest.json \
  --expected-head REVIEWED_COMMIT_SHA \
  --run-dir /mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1/02_independent_qa_v1
```

The checker stage is serialized after completed body publication and separate root
approval. It reopens the saved exact/sample artifacts and small frozen inputs; it
does not query the resolved-fill source. Actual report generation requires that
completed checker receipt and the full build manifest, with no estimator rerun.
