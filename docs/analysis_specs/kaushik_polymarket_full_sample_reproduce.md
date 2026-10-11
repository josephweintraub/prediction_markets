# Reproducing the Kaushik Polymarket full available-date sample

Status: staged reproduction guide, 11 October 2026. Full-date inputs v2 have
reopened and been accepted, the cutoff-subset audit has passed, and new sports
metadata has reopened and been accepted. Root has dispatched full estimation
after a fresh reviewed preflight; estimation v1 failed during sports-cache COPY
before fitting and is not accepted. Root verified EC2 stopped after confirming
no process or transfer still needed it. A physical-only retry/source version,
saved-score auditing, report QA and shared publication remain pending. This guide
records stage admission, reconciliation and failure evidence, not estimated
findings. Root must append later accepted bindings and execution receipts as
those stages finish.

The [frozen full-sample specification](kaushik_polymarket_full_sample_v1.md)
inherits the [cutoff scientific specification](kaushik_polymarket_replication_v1.md)
except for execution-date scope. The [cutoff reproduction guide](kaushik_polymarket_replication_reproduce.md)
continues to describe that separate accepted run; do not overwrite or relabel its
inputs, corrected estimates v5 or report v2. The
[methods reference](../methods_reference.md) remains authoritative for data and
censoring qualifications.

## Scientific and source boundary

The full-date contract explicitly sets `sample_scope: all_available_dates` and
null cutoff fields. It retains every available execution date in the admitted
source, subject to the original validity predicates; it introduces no replacement
upper bound or lower bound. Former post-cutoff records must pass every non-date
eligibility rule. Full-date `scope_rows` includes invalid source records before
eligibility exclusions. Cutoff-relative future-ending diagnostics are null/not
applicable and omitted from the full-date report, never shown as zero.

Retain equal archive-record weights, taker BUY plus verified complemented taker
SELL, unrounded normalized binary64 prices, eventual binary payouts, the native
category taxonomy, exact-price/UTC-month controls, duration proxies/inequalities,
all original models, joint event/game CR0 uncertainty and the original support
guards. No bot, up/down, lifecycle or extreme-price filter is added. The full
writeup includes every original table, pooled/sport-specific figure and archive
BUY convention. The renderer performs no estimation.

Inputs use the same repaired CLEAN generation and 44 monthly partitions,
November 2022 through June 2026, under
`/mnt/data/runs/2026-10-09_polymarket_wallet_attribution_repair_v1/clean/`, with the
same token map, token/outcome spine, native metadata and category map. EC2 archive
`conditionId` is a token identifier. Do not replace sources, refresh resolutions,
redirect shared paths or change join keys for this reproduction. The retained
resolved-source vintage, approximate execution clocks and omission of unresolved
long-horizon claims remain qualifications; full-date eligibility is not an
exchange census or evidence of a recency trend.

The retained nine-sport provider inventory is recorded in
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_v1.json`.
It has no new API collection or provider games. Sports means all available
execution dates within that audited provider-covered winner cohort, including
EPL draws. Its sport-specific historical ranges remain limited. Eight sports use
accepted play clocks; ATP retains scheduled-start/archive-duration qualification.
Fresh sports metadata must bind the full-date base; old sports observation bodies
or cutoff-base metadata receipts are not interchangeable with this new admission.

## Environment and verified stage bindings

Production uses Linux, the canonical `/home/ubuntu/prediction_markets` checkout,
mounted `/mnt/data` and `/home/ubuntu/venv/bin/python`. Entrypoints enforce the
production-host guard. Local work is limited to source review, fixture tests,
bounded summaries and report-only rendering. Follow [EC2_SETUP.md](../EC2_SETUP.md);
root alone manages instance/storage lifecycle and production execution.

Recorded input/subset/sports producer and failed estimation v1 HEAD:
`2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a`, branch
`codex/kaushik-polymarket-full-sample`.
Canonical controls directory:
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1`.
Local small evidence copies:
`output/kaushik_polymarket_full_sample_2026-10-10_v1/`.

The accepted full-date base is
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2`.
Its `inputs_complete` manifest is accompanied by an
`inputs_reopened_accepted` receipt with the full-date scope, matching source/rows
and a true schema/hash/count/support reopening gate. The saved input body receipt
records exit zero; this acceptance does not establish completed estimates.

Verified small-artifact SHA256 bindings:

| Artifact | SHA256 |
| --- | --- |
| Full input contract v2 | `0ab3c86e22d3a880d321ab9df7e4913d6a51d51dc9666994b51fb0822ef2d0b3` |
| Full input reviewed preflight v2 | `88fec8c03588ac2e8444bcca49cdf749cbbc5ea426300fa97ca7e51590f5d9f6` |
| Full input manifest v2 | `72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c` |
| Full input acceptance v2 | `e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519` |
| Cutoff input manifest v2 | `a005f59a4b2da3f6eb34d97c912c5ff08aff842d3dcbac109e11c92bf43e1cc9` |
| Cutoff input acceptance v2 | `426ac4ba9b68b77e8b101f129a82009bc5f92a8f9f1179b3c09cd5c70589841a` |
| Subset reviewed preflight v1 | `f0cb9264b11200e540c9d6764b0bb8faad42dc3dd30c723a2128297f6adddfe2` |
| Accepted subset audit v1 | `19bcee0a823aaf156945699f300475afdda3ffb6210f212f25e6d1242af844e1` |
| Subset body execution receipt v1 | `ca3dbac4c1997eac2d7485f18af22eeed318574681d9482cba0b5f3ce8d61e41` |
| Retained sports binding v1, before new metadata review | `0c207591b610f5231091fc2d6d33606e98a6ce3ca28d543e2763eae5023a5cfe` |
| Sports reviewed preflight v1 | `5895c201f8880dc858c97a0cc9e967ad71ce42e101f932e982c36c9b153c43e2` |
| Accepted sports metadata manifest v1 | `2f02ab263bfcf615ee5592331b85d9215e8ad0545c8618735f90928a13459279` |
| Sports metadata acceptance v1 | `56ad997cc92f9e6910d661a7801a15f4c8ef2128b9c1dab9e7fb3cb986569f41` |
| Sports body execution receipt v1 | `d331f8b5f75461e04252293d33b9d37e4c439b7e6d2748987f329e69cba158bd` |
| Reviewed sports binding v1, with accepted full-date metadata | `ca61615860ad60fda267df2652f1e87a9fbde66718257d21ec31117a24c3fc58` |
| Estimate reviewed preflight v1 | `f96d6d50a8ae3306e6060a65b14b20875b6f5ddae062dc788ae010b87332341c` |
| Failed estimate body execution receipt v1 | `12c000c024af95ef6fcae644efc75f9852d7c311dd516dabcc079e5b5ad96460` |
| Estimate failure record v1 | `2d9710b24f98f531b2c63e69dee25f432dd2483d5f72abc7ec6966a38c9f16de` |
| Estimate stderr v1 | `7f75bffe51c28aebea0d06f71df823db24200301bcae47c7f88592d3fc32036a` |
| Failed-build recovery receipt v1 | `40b45e436bbb712ffa2dacd44fe7ce6be8c011b6e19aa9de993949813a24cf13` |

Per-file source and data inventories belong in the bound manifests, not the
reader-facing manuscript. The local accepted input copies contain small manifest
and acceptance summaries; computational reproduction still requires the admitted
EC2 Parquet bodies.

## Resource-only retry and verified failure recovery

The first full-date input build used source
`4098527caddadec5c16ca3979c3fcf768fc2adfd` and failed at final exact support
aggregation under 64GB DuckDB memory and the unchanged 16GB spill cap. It was
never an accepted input stage. The new committed producer increases only the
input memory cap from 64GB to 192GB and requires at least 240GB available RAM.
Threads, spill, read, per-file, total-output and free-space limits remain
unchanged, as do all scientific predicates and saved analytical fields. The
fresh v2 preflight and root review admit that change explicitly.

The subset auditor permits unchanged 64GB or exactly 64GB-to-192GB input memory;
all other caps, non-date contracts, definitions, source inventories and schemas
must agree. Its reviewed preflight records the sole cap delta as
`caps.memory_limit`, classified `approved_resource_only`, and preserves both
contracts. It revalidates that exception before its exact body comparison.

Separately, the user authorized moving only the two preserved failed input
builds to the same instance's system disk after per-file verification. The
recovery receipt reports `approved_recovery_verified_and_originals_removed`:

| Removed original failed-stage location | Verified retained recovery location |
| --- | --- |
| `/mnt/data/runs/.2026-10-10_kaushik_polymarket_inputs_v1.staging-94an1044` | `/home/ubuntu/research_recovery/2026-10-11_failed_input_builds_v1/cutoff_failed_inputs_v1` |
| `/mnt/data/runs/.2026-10-10_kaushik_polymarket_full_sample_inputs_v1.staging-_8i9fcvd` | `/home/ubuntu/research_recovery/2026-10-11_failed_input_builds_v1/full_sample_failed_inputs_v1` |

Only those failed-stage originals were removed after destination size/hash
verification. Accepted cutoff inputs/estimates and the repaired source were not
moved. Recovery preserves failure evidence; it neither accepts a failed stage
nor replaces the fresh successful v2 input build. The recovery receipt is retained
locally as `failed_build_recovery_receipt_v1.json` and on EC2 at
`/home/ubuntu/research_recovery/2026-10-11_failed_input_builds_v1/receipt.json`.
No recovery operation needs repeating to reproduce the accepted scientific run.

## Executed input and accepted subset commands

These commands record existing immutable operations, not new destinations for a
repeat run. A reproduction needs absent versioned targets and fresh separately
reviewed preflights. Preserve failures and execution receipts. Each retained
execution control ran in `/home/ubuntu/prediction_markets` and recorded command,
start/end/status, stdout/stderr and resource-log bindings.

Input preflight v2, from `input_preflight_execution_v2.py`:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/build_kaushik_polymarket_replication.py \
  --contract /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/input_contract_v2.json \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_input_preflight_v2
```

Accepted input body v2, from `input_body_execution_v2.py`:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/build_kaushik_polymarket_replication.py \
  --contract /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/input_contract_v2.json \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_input_preflight_v2/manifest.json \
  --reviewed-preflight-sha256 88fec8c03588ac2e8444bcca49cdf749cbbc5ea426300fa97ca7e51590f5d9f6
```

Subset preflight v1, from `subset_preflight_execution_v1.py`:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/audit_kaushik_full_sample_subset.py \
  --previous-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_inputs_v2 \
  --previous-manifest-sha256 a005f59a4b2da3f6eb34d97c912c5ff08aff842d3dcbac109e11c92bf43e1cc9 \
  --previous-acceptance-sha256 426ac4ba9b68b77e8b101f129a82009bc5f92a8f9f1179b3c09cd5c70589841a \
  --full-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --full-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --full-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_subset_audit_v1 \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_subset_preflight_v1
```

Accepted subset body v1, from `subset_body_execution_v1.py`; the saved
`subset_v1_execution_receipt.json` records completion with exit zero:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/audit_kaushik_full_sample_subset.py \
  --previous-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_inputs_v2 \
  --previous-manifest-sha256 a005f59a4b2da3f6eb34d97c912c5ff08aff842d3dcbac109e11c92bf43e1cc9 \
  --previous-acceptance-sha256 426ac4ba9b68b77e8b101f129a82009bc5f92a8f9f1179b3c09cd5c70589841a \
  --full-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --full-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --full-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_subset_audit_v1 \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_subset_preflight_v1/manifest.json \
  --reviewed-preflight-sha256 f0cb9264b11200e540c9d6764b0bb8faad42dc3dd30c723a2128297f6adddfe2
```

### Accepted cutoff-subset gate

The saved `subset_audit_v1.json` reports `accepted_cutoff_subset_equal` for the
bound cutoff and full-date inputs v2. The claims map and all 44 monthly
before-cutoff subsets pass bidirectional `EXCEPT ALL` exactly. The 12 compared
fields are `source_ordinal`, `source_month`, `claim_code`, `recorded_token_code`,
`timestamp`, `recorded_price`, `P`, `Y`, `usdcSize`, `side`, `is_maker` and
`duration_eligible`. Locator uniqueness, accepted support counts, unchanged input
bindings, same canonical sources/non-date contract, retained-plus-added role
counts and the BUY partition also pass.

The primary taker population reconciles as 522,102,319 retained plus 478,868,297
newly admitted records, totaling 1,000,970,616. Across all saved roles,
1,044,204,638 retained plus 957,736,594 newly admitted records total
2,001,941,232. The auditor charged 275,681,582,243 read bytes, below the separately
reviewed 368,814,643,780-byte plan. Its completed receipt and audit bind producer
HEAD `2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a` and the reviewed preflight above.

This establishes exact preservation of the admitted before-cutoff records and
their saved analytical fields. The auditor reads accepted saved-input bodies and
bound metadata, not raw archive trades or native APIs. Added rows are admitted
builder outputs, not every old date-excluded record; raw eligibility and exclusion
assignment are not independently reconstructed. This gate does not refresh
outcomes or timing, expand provider coverage, or establish native actions or
whole-history collection completeness.

## Executed and accepted sports metadata

These commands record the completed metadata-only stage. They do not authorize
reuse or overwrite of its immutable targets. A new reproduction requires fresh
targets and separately reviewed preflights. Production bodies remain serial when
they share inputs or caches.

Sports preflight v1, from `sports_preflight_execution_v1.py`:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_v1.json \
  --sports-binding-sha256 0c207591b610f5231091fc2d6d33606e98a6ce3ca28d543e2763eae5023a5cfe \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v1 \
  --sports-metadata-only \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_preflight_v1
```

Accepted sports metadata body v1, from `sports_body_execution_v1.py`; the saved
`sports_v1_execution_receipt.json` records completion with exit zero:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_v1.json \
  --sports-binding-sha256 0c207591b610f5231091fc2d6d33606e98a6ce3ca28d543e2763eae5023a5cfe \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v1 \
  --sports-metadata-only \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_preflight_v1/manifest.json \
  --reviewed-preflight-sha256 5895c201f8880dc858c97a0cc9e967ad71ce42e101f932e982c36c9b153c43e2
```

The new immutable
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v1`
has a `sports_metadata_complete` manifest and
`sports_metadata_reopened_accepted` receipt with all outputs reopened. It binds
accepted full-date inputs v2 and retains exactly 13,909 provider games and 14,383
markets across nine sports. `trade_bodies_read` is false and
`native_pair_and_provider_result_proof` is true. This metadata-only admission
does not establish observed trade populations or completed sports estimates.

The fresh `sports_binding_reviewed_v1.json` binds this accepted metadata manifest
and receipt, the same retained provider inventory and its unchanged qualification.
No provider games, resolution refresh or API collection were added. Preserve the
sport-specific historical coverage limits, EPL draw claims and ATP timing caveat.

## Fresh estimate preflight and failed estimation v1

The new `preflight_complete` estimate admission binds accepted inputs v2, the
reviewed full-date sports metadata and producer HEAD above. Its resource plan
reserves 60,000,000,000 disk bytes: 8GB accepted output, 16GB transient file, 16GB
spill and 20GB free floor. It observed 64,685,264,896 free disk bytes and
264,257,695,744 available RAM bytes. The memory policy budgets 192GB DuckDB plus
32GB NumPy, totaling 224GB, and requires at least 240GB available RAM. Planned
maximum reads are 6,951,547,448,572 bytes under the retained 8TB read cap. These
are preflight capacity observations and limits, not actual completed-body usage.
The input retry's admission did not substitute for this fresh estimate review.

Estimate preflight v1, from `estimate_preflight_execution_v1.py`:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_reviewed_v1.json \
  --sports-binding-sha256 ca61615860ad60fda267df2652f1e87a9fbde66718257d21ec31117a24c3fc58 \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v1 \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimate_preflight_v1
```

Executed estimate body v1, from `estimate_body_execution_v1.py`; the completed
`estimate_v1_execution_receipt.json` records exit 1, not successful estimation:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_reviewed_v1.json \
  --sports-binding-sha256 ca61615860ad60fda267df2652f1e87a9fbde66718257d21ec31117a24c3fc58 \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v1 \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimate_preflight_v1/manifest.json \
  --reviewed-preflight-sha256 f96d6d50a8ae3306e6060a65b14b20875b6f5ddae062dc788ae010b87332341c
```

### Preserved failure and pending physical-only retry

The body started at `2026-10-11T14:57:10.006609+00:00` and ended at
`2026-10-11T15:02:33.134285+00:00`. Its `estimates_incomplete` failure record and
stderr identify `OutOfMemoryException` during `create_sports_view`'s Parquet
`COPY` of `sports_observations`, before model fitting. The unchanged 16GB spill
cap was reached (14.9 GiB/14.9 GiB in the diagnostic) under 192GB DuckDB memory.
The failure ledger charged 58,701,085,017 read bytes; it did not complete the
estimated-output or sports-cache gates.

The failed stage is preserved at
`/mnt/data/runs/.2026-10-10_kaushik_polymarket_full_sample_estimates_v1.staging-0nxa_ukt`;
root reports a 0-byte sports cache. Local evidence is
`estimate_v1_execution_receipt.json`, `estimate_v1_failure.json` and
`estimate_v1_stderr.log`, bound above. Inputs v2, the accepted cutoff-subset audit
and accepted sports metadata remain unchanged. No estimates, saved-score audit
or report have been accepted.

Root verified no processes or transfers remained, stopped EC2 and verified the
stopped state. No stop timestamp is asserted here. Local implementation is
removing only the unnecessary global raw sports-cache `ORDER BY`, with
equivalence fixtures. This physical-only hypothesis is not yet production-proven
and does not authorize any scientific predicate, model, weighting or resource-cap
change. Reviewed committed retry source, fixture results, a new retry version,
immutable targets, controls and a fresh preflight remain pending. Preserve the
failed v1 stage; do not reuse its cache or treat its receipt as acceptance.

Require reopened `estimates_complete` outputs and an
`estimates_reopened_accepted` receipt, full-date scope provenance, common duration
populations, BUY partition, complete support grids and numerical/convergence
gates. Require distinct double `sport_elapsed_seconds` and
`sport_remaining_seconds`, their saved clock-column mapping and zero post-cache
clock-equation mismatches; legacy `u/r/R` caches remain inadmissible. Estimator
source files must agree with the accepted sports metadata's source binding.
Record actual manifest, acceptance and estimates JSON digests when available.

## Serial later-stage templates: pending

The following are templates, not execution or acceptance records. Replace every
`PENDING_*` token with a separately reviewed actual source/hash binding before
execution. Verify that each future stage target is absent; bump versions if
needed. Keep bodies serial when they share inputs or caches. Estimate acceptance
and every later audit/report/publication gate remain pending.

Pending independent saved-score audit, after estimate acceptance:

```sh
/home/ubuntu/venv/bin/python -B scripts/audit_kaushik_replication_saved_scores.py \
  --estimate-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v1 \
  --manifest-sha256 PENDING_ESTIMATE_MANIFEST_SHA256 \
  --acceptance-sha256 PENDING_ESTIMATE_ACCEPTANCE_SHA256 \
  --expected-head PENDING_AUDITOR_SOURCE_HEAD \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_saved_score_audit_v1
```

Require `saved_scores_reconciled`. This auditor reconstructs covariance,
contrasts/SE/CI and influence from saved scores and reconciles saved support and
clock metadata. It does not independently refit coefficients or reconstruct raw
window membership. The two audits answer different questions; neither certifies
native actions, exact archive clocks or complete historical collection.

## Pending report, package and completion

After accepted estimates and audits, copy only small accepted `estimates.json`,
`manifest.json` and `acceptance.json` to the new local run without changing bytes.
The following report-only paths are planned, not existing accepted artifacts:

```sh
cd /Users/josephweintraub/prediction_markets
python3 -B -m analysis.kaushik_polymarket_replication.render_report \
  --estimate-dir /Users/josephweintraub/prediction_markets/output/kaushik_polymarket_full_sample_2026-10-10_v1/accepted_estimates_v1 \
  --run-dir /Users/josephweintraub/prediction_markets/output/kaushik_polymarket_full_sample_2026-10-10_v1/report_v1
```

Require new portable `source.tex`, all 21 relative vector figure assets and a
report manifest. Rendered scope must say no execution-date cutoff, use all-date
eligibility accounting, retain provider-map versus observed-game distinctions
and actual sport-date ranges, and preserve resolved-source/clock limitations.
Do not describe new execution eligibility as new provider games or resolutions.

Use the project's data-first reporting workflow. Compile twice in a new clean
`build/` directory, then render page images for QA:

```sh
pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build source.tex
pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build source.tex
pdftoppm -r 85 -png build/source.pdf build/page
```

These preview commands are pending and run from the finished full-date report
folder. Inspect every generated page, not an assumed old page count. Reconcile
every displayed number/support/status/influence mark and plotted CR0 interval to
saved outputs. Require legible native tables, isolated discrete points with
capped whiskers/zero references, omitted suppressed marks, complete pooled and
nine-sport panels, and no substantive compilation/layout failures. Source and
vector assets are primary; the verified PDF is a preview. Open both in Codex.

Root must record final source/control/estimate/audit/report/QA digests before
publishing a new finished bundle. Include this guide, both scientific contracts,
committed source/tests or retrievable revisions, reviewed controls, accepted
small summaries, both audits, portable report assets and verified preview.
Exclude raw/cache/score bodies, failure-recovery bodies, spill, credentials and
scratch page images. Do not append this run into the completed cutoff bundle.
Publication destination, finished-bundle transfer verification and any later
retry's final shutdown receipt remain pending. The failed v1 run has already been
verified stopped by root as recorded above. After any future restart, root must
again confirm no agent/process/transfer still needs the instance, stop it and
verify stopped. The local run index and actual receipts track current execution
state; this guide's pending templates do not imply completion.
