# Kaushik Polymarket replication: full available-date rerun

**Status:** recovery, input v2, exact cutoff-subset audit and sports metadata
accepted; estimation v1 failed before fitting, no estimate or report accepted.
Root verified EC2 stopped after failure; physical-only retry/source version pending,
11 October 2026. The user
confirmed removing the March 25 execution cutoff and rerunning the full writeup
over all available source dates, keeping all other scientific definitions
unchanged. This record contains no full-sample estimated findings or predicted sample sizes.

## Authority and boundaries

- Active full-sample contract:
  [kaushik_polymarket_full_sample_v1.md](../analysis_specs/kaushik_polymarket_full_sample_v1.md).
- Inherited scientific definitions:
  [kaushik_polymarket_replication_v1.md](../analysis_specs/kaushik_polymarket_replication_v1.md).
- Explicit input-contract mode: `sample_scope: all_available_dates`;
  `paper_cutoff` remains the legacy/default behavior. Full-date cutoff fields
  are null; no replacement execution cutoff or new lower bound is introduced.
  Valid timestamps and source-month gates remain. Full-date `scope_rows` counts
  every source row, including invalid records; cutoff-relative future-ending
  diagnostics are null/not applicable, never zero.
- Same repaired CLEAN generation and 44-month November 2022--June 2026 source
  inventory. Same token/outcome, native metadata and category inputs. No new
  resolution refresh, native BUY source, API collection or provider games.
- Retain all observation, eligibility, bin, model, control, weighting,
  uncertainty and support definitions. The larger available-date population is
  the sole scientific change.
- Sports retain the audited available provider-covered cohort and its per-sport
  historical ranges. Full-date execution eligibility is not complete historical
  game coverage. Approximate archive clocks, ATP clock qualification and
  resolved-source censoring remain.
- Accepted cutoff inputs v2, corrected estimates v5 and report v2 remain
  immutable and separately identified. Do not overwrite or relabel them.

## Existing reference bindings

Accepted cutoff inputs:
`/mnt/data/runs/2026-10-10_kaushik_polymarket_inputs_v2`.

Repaired archive source:
`/mnt/data/runs/2026-10-09_polymarket_wallet_attribution_repair_v1/clean/`.

Retained audited provider inventory:
`/mnt/data/runs/2026-10-10_kaushik_polymarket_controls_v1/sports_binding_reviewed_v5.json`.

The [original reproduction guide](../analysis_specs/kaushik_polymarket_replication_reproduce.md)
records the accepted cutoff source and digests. These paths identify reference
artifacts, not reusable full-sample acceptance receipts. Fresh controls must
reopen and bind the source inventory and accepted cutoff baseline.

## Planned stages and required evidence

Root plans branch `codex/kaushik-polymarket-full-sample` from
`c3a769fcfe6f76d9abc2cc467134691822c66057`. The new committed producer revision,
controls and exact commands must be bound before execution; no new commit or run
is asserted here. Existing capacity limits remain gates until fresh reviewed
evidence admits any necessary resource change.

The planned immutable production prefix is
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample`, with these stage
suffixes: `_controls_v1`, `_inputs_v1`, `_input_preflight_v1`,
`_sports_metadata_v1`, `_sports_preflight_v1`, `_estimates_v1`,
`_estimate_preflight_v1`, `_saved_score_audit_v1` and `_subset_audit_v1`.
Every target must be absent and pass no-overlap admission. Local small summaries,
controls and report-only artifacts are planned under
`output/kaushik_polymarket_full_sample_2026-10-10_v1/`.

| Stage | Required completion evidence | Current status |
| --- | --- | --- |
| Source and fixture QA | Committed explicit full-date mode; focused and relevant broader tests; unchanged cutoff behavior and scientific definitions | Passed, H12; 140 fixtures; physical-only retry revision/QA pending |
| Input preflight | Reopened same-source inventory and field semantics; fresh read/RAM/cache/spill/output/disk admission | v2 root reviewed |
| Full-date inputs | Immutable outputs and reopened acceptance; source/exclusion/support reconciliation; no fanout or silent repairs | v2 accepted |
| Cutoff-subset reconciliation | Exact source-locator membership and analytic-field agreement with inputs v2; full population partitions into retained earlier and newly eligible later records | v1 accepted; independent metadata review passed |
| Sports metadata | New-base binding to unchanged provider inputs; native pair/result/timing proof; retained date ranges and exclusions | v1 accepted |
| Estimator preflight | Fresh committed-source/base/provider bindings and separately reviewed resource admission | v1 root reviewed; fresh retry preflight pending |
| Full estimation | Full writeup grids; common duration samples; BUY-role partition; numerical/rank/convergence gates; distinct sports seconds and zero clock-equation mismatches | v1 failed during sports-cache COPY before fitting; retry pending |
| Independent saved-score audit | `saved_scores_reconciled`; reopened covariance, contrasts, intervals, influence, support and clock metadata | Planned |
| Data-first report QA | Accepted-summary-only rendering; portable source/vector assets; displayed-value reconciliation, successful compilation and every-page review | Planned |
| Publication and shutdown | Root verifies finished-bundle transfer and no active dependencies, stops exact instance and verifies stopped | Publication pending; failed-run shutdown verified stopped by root |

The subset comparison must use source month plus original physical row ordinal,
not estimated coefficients or coincidentally matching aggregate counts. Compare
recorded/normalized claim identities, timestamp, recorded/normalized prices,
payout, gross size, role/action and duration eligibility. Reopen metadata lineage,
categories, event/market keys and duration clocks; dictionary codes are not
substitutes for identity. The guarded
`scripts/audit_kaushik_full_sample_subset.py` compares accepted base bodies
serially by month using a bidirectional `EXCEPT ALL` oracle; it reads no raw
archive trade bodies. Save counts and reconciliation evidence for the disjoint
newly admitted population and its exclusions. Record all comparisons and
reopened-output evidence in manifests.

The score audit independently verifies saved-score computations and saved
support identities. It does not independently reconstruct raw timing/window
membership, refit raw-trade coefficients, prove native own-order actions or
establish whole-history completeness. Keep those limits explicit beside affected
evidence in the completed delivery.

## Operations and completion record

Entries below are chronological. Earlier pending/running statuses and original
failed-input locations describe their then-current state; later entries preserve
the transitions and establish current status.

Production runs on the guarded canonical EC2 checkout with mounted `/mnt/data`
and `/home/ubuntu/venv/bin/python`, following [EC2_SETUP.md](../EC2_SETUP.md).
Root alone owns source commits, synchronization, instance/storage lifecycle,
production execution and publication. Agents may implement or audit their
assigned local scope; they do not manage the instance or run unassigned production
work. Serialize stages that share inputs/caches.

Root must append actual source and control bindings, immutable target paths,
commands, execution times/statuses, test results, resource observations,
reconciliation results, accepted artifact paths and release/shutdown receipts
as work completes. Preserve failures and receipts instead of overwriting them.
No execution, source publication, completed report, external transfer or stopped
instance state is asserted by this planned record.

### Root infrastructure admission before producer execution

Root restarted the exact project instance after confirming it was stopped. The
data volume was initially unmounted; root verified ext4 UUID
`d0cd087b-94c4-428c-bae9-ae28929059f6` on `/dev/nvme1n1` and mounted it at
`/mnt/data`. No volume was altered or detached. The canonical checkout remained
clean at the previous release commit. Only network-dispatch and unattended-upgrade
system Python daemons were present; no analysis, notebook or transfer was active.

Metadata-only capacity observation after mounting: the data filesystem had
73,015,885,824 available bytes; the system filesystem had 92,192,768,000 available
bytes. This clears the existing 68 GB input reservation but does not prove that
the later 60 GB estimator reservation will fit after the enlarged input stage.
Root requested approval to preserve the explicitly identified failed input v1
stage on the system disk rather than deleting it. No such move is asserted here.
Fresh stage preflights and body execution remain pending; no full-sample findings
or stage acceptance are claimed by these infrastructure checks.

### Executed input v1 and preserved capacity failure

Producer branch `codex/kaushik-polymarket-full-sample` was committed at
`4098527caddadec5c16ca3979c3fcf768fc2adfd`. The five-module suite passed
124 tests locally (29.618 seconds) and on EC2 (38.062 seconds). Accepted cutoff
LaTeX regenerated byte-for-byte unchanged. The separate subset auditor's
13 fixtures also passed locally (0.687 seconds); no production subset audit
has run.

Input preflight v1 admitted the same 44 files, 2,036,128,538 source records and
29,838,148,561 source bytes, with no execution-date cutoff. Its manifest SHA256
was `f1639ef137f98f071ab42568e07b5031b17cd7a60877668ce36b314ae71c7e3a`.
It recorded 73,015,869,440 free bytes, clearing the unchanged 68 GB reservation.
All non-date predicates and other resource bounds were retained.

The fixed body command started at `2026-10-11T06:24:51.668939+00:00` and exited
with status 1 at `2026-10-11T06:41:33.973347+00:00`. All 44 monthly output pairs
completed, but the final exact support query exhausted the 16 GB DuckDB spill
allowance under the 64 GB memory limit. Maximum observed resident size was
65,424,864 KiB; no swap occurred. Monthly outputs are failed-stage evidence,
not accepted inputs or reportable full-sample findings.

The complete failed stage remains at
`/mnt/data/runs/.2026-10-10_kaushik_polymarket_full_sample_inputs_v1.staging-_8i9fcvd`
(19,524,587,428 directory bytes). The original earlier failed input stage remains
at `/mnt/data/runs/.2026-10-10_kaushik_polymarket_inputs_v1.staging-94an1044`
(11,197,865,200 directory bytes). No files have been deleted or moved. Small
failure, execution, resource and progress records were copied to the local
task output directory.

After process closure the data filesystem had 53,490,765,824 free bytes and the
system filesystem 92,192,149,504 free bytes. Root requested permission to move
only these two failed builds to the system filesystem, verifying every file
and recording recovery locations. This would preserve all failures, accepted
outputs and raw sources without expanding the volume. Approval remains pending.

A same-host resource-only retry is under fixture review: DuckDB memory
64 GB to 192 GB and a 240 GB available-RAM admission floor; four threads,
16 GB spill, exact DISTINCT, scientific predicates and all other bounds remain
unchanged. Fresh committed source, contract, preflight and immutable v2 targets
are required. A saved-input subset checker must explicitly record this sole
resource-contract difference while rejecting all other non-date differences.
No retry, sports-metadata stage, estimation, report or new external publication
is claimed by this record.

### Shutdown while storage authorization is pending

Root confirmed no analysis, notebook, transfer or producer process remained,
then stopped exact instance `i-0f5b31a268af53938`. AWS reported `stopped`, verified
at `2026-10-11 08:10:47 UTC`. Both failed directories and all accepted runs remain
in place. Agents' remaining source and fixture preparation is local-only.
Canonical EC2 source remains clean at the input-v1 producer commit; uncommitted
local resource-retry edits are not a production revision. No source push or
finished-bundle publication for this full-sample task has occurred. Resume requires
the requested storage authorization, root restart/mount verification, committed
reviewed retry source and a fresh v2 contract/preflight/immutable destination.

### User-approved recovery and reviewed memory-only retry

On 11 October 2026 the user explicitly approved moving both failed input builds
to unused space on the same instance's system filesystem, verifying every file
and retaining the failures for recovery. Root restarted the exact instance,
verified and mounted the unchanged data-volume UUID, confirmed no research or
transfer process was active, and inventoried only the approved two directories.

The reviewed recovery preflight binds 180 regular files, 30,722,452,628 bytes,
the source identities, exact two destinations and recovery helper fingerprint.
It requires both copies to pass full inventory, size, mode, mtime and SHA256
checks before any original file is removed. Target root:
`/home/ubuntu/research_recovery/2026-10-11_failed_input_builds_v1`.
Preflight SHA256:
`ae1c3b97733482954eaee17a9e9340e2f7b06149f50af07798af1c798ace8c00`.
Its observed system-disk free space was 92,193,148,928 bytes against a
50,722,452,628-byte copy-plus-floor reservation. Copy/verification is underway;
completion and original removal are not yet claimed here.

The resource-only source revision has passed 140 fixtures in 32.394 seconds,
with all 17 checked source/dependency fingerprints unchanged during testing.
An independent read-only review found no scientific or non-memory cap change.
Only builder memory 64 to 192 GB and available-RAM admission 70 to 240 GB change.
The new guarded subset checker binds both complete input contracts and records
exactly the 64 to 192 GB resource exception; it rejects every other additional
cap or scientific difference. Its own 64 GB memory and all other audit limits
remain unchanged. Retry source is synchronized but not yet committed; fresh
`_inputs_v2` and `_input_preflight_v2` targets remain pending.

### Verified recovery completed

Both approved destination copies passed every file-level check. Only then did
the root-owned helper remove the originals' exact verified files and directories.
The final receipt reports `approved_recovery_verified_and_originals_removed`:
`/home/ubuntu/research_recovery/2026-10-11_failed_input_builds_v1/receipt.json`.
The two recoverable directories are `cutoff_failed_inputs_v1` and
`full_sample_failed_inputs_v1` under that same recovery root. All 180 files,
30,722,452,628 bytes, remain recoverable there. No accepted output or raw source
was moved, modified or removed. The data filesystem now has 84,213,981,184 free
bytes; system filesystem has 61,469,675,520 free bytes. The old failed-stage
paths no longer exist; provenance uses the receipt and new recovery locations.

### Accepted higher-memory input v2

Root committed the reviewed resource-only retry and subset auditor at
`2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a` on the same full-sample branch.
Fresh input preflight v2 SHA256 is
`88fec8c03588ac2e8444bcca49cdf749cbbc5ea426300fa97ca7e51590f5d9f6`.
The fixed body completed with exit 0 and empty stderr, from
`2026-10-11T13:55:30.287531+00:00` to
`2026-10-11T14:26:23.580860+00:00`.

Accepted target:
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2`.
Manifest SHA256:
`72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c`.
Reopened acceptance SHA256:
`e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519`.
The 89 saved outputs occupy 19,525,404,268 bytes. Source 2,036,128,538 records
partition into 2,001,941,232 eligible all-role records and 34,187,306 exclusions;
the primary taker population is 1,000,970,616. These are archive records, not
certified unique native executions. Numerical amount and outcome anomalies are
zero. The post-body data free space was 64,687,759,360 bytes.

Independent metadata review verifies all 44 source months, 48 unchanged source
bindings, 520 support cells, native categories, BUY-role partitions and disjoint
exclusion totals. Exclusions comprise 34,187,304 unadmitted binary pairs and two
invalid prices. Of the former date-excluded population, 28,712,404 still fails
the retained non-date eligibility rules. All scope fields select full dates,
cutoffs are null and the sole input resource exception is recorded explicitly.
Metadata agreement is not a retained-record equality proof: the new claims
Parquet has different physical bytes, so semantic claim-map comparison remains
required, together with exact source-locator and analytic-field matching.

### Running independent cutoff-subset gate

Fresh subset preflight v1 SHA256:
`f0cb9264b11200e540c9d6764b0bb8faad42dc3dd30c723a2128297f6adddfe2`.
Root reviewed both accepted-stage bindings, unchanged non-date contracts and
the sole approved input-memory exception. It admits 368,814,643,780 conservative
read bytes below the 1 TB ceiling, with a 36,001,000,000-byte disk reservation.
Observed disk free space was 64,686,923,776 bytes, available RAM 264,330,690,560
bytes and 32 cores. The auditor itself remains limited to 64 GB, four threads
and 16 GB spill. Root started its fixed serial body; no successful subset audit,
sports stage, estimate or report is asserted by this entry.

### Accepted exact subset and sports gates; admitted estimation

The subset body exited zero with empty stderr, completing at
`2026-10-11T14:52:29.693020+00:00`. Accepted audit SHA256 is
`19bcee0a823aaf156945699f300475afdda3ffb6210f212f25e6d1242af844e1`.
All 44 months' 88 bidirectional analytic-field differences are zero; the complete
claims map also agrees semantically. Retained all-role/primary counts are exactly
1,044,204,638/522,102,319, added counts 957,736,594/478,868,297, and full counts
2,001,941,232/1,000,970,616. Five role partitions and reopened input fingerprints
reconcile. The auditor charged 275,681,582,243 bytes below its admitted plan.
Independent saved-JSON review recomputed every monthly/global identity and read
ledger. This certifies accepted saved-record membership, multiplicity and fields;
it does not independently certify raw collection completeness, native actions,
exact timestamps or added-record eligibility reconstruction.

The separate metadata-only sports stage exited zero at
`2026-10-11T14:54:34.400873+00:00`, read no trade bodies and reopened its native
pair/result/timing proof. It preserves 13,909 games and 14,383 markets in the same
nine-sport provider-covered cohort. Sports metadata manifest/acceptance SHA256:
`2f02ab263bfcf615ee5592331b85d9215e8ad0545c8618735f90928a13459279` /
`56ad997cc92f9e6910d661a7801a15f4c8ef2128b9c1dab9e7fb3cb986569f41`.
New reviewed sports-binding SHA256:
`ca61615860ad60fda267df2652f1e87a9fbde66718257d21ec31117a24c3fc58`.
The provider game-date ranges and ATP timing qualification remain unchanged.

Fresh estimator preflight SHA256:
`f96d6d50a8ae3306e6060a65b14b20875b6f5ddae062dc788ae010b87332341c`.
Root verified identical estimator/sports producer files, full-base/acceptance
bindings and the unchanged models/resources. Admission observed 64,685,264,896
free data bytes against the 60 GB reservation and 264,257,695,744 available RAM
bytes against the 240 GB floor. Its conservative read plan is 6,951,547,448,572
bytes below 8 TB. DuckDB 192 GB and NumPy 32 GB remain separate within 224 GB;
four threads, 16 GB spill, 3 GB cache, 8 GB accepted outputs and all other bounds
are unchanged. Root started the fixed serial estimator body at H12. No accepted
estimate, saved-score audit, report or full-sample publication is claimed yet.
See the [full-sample reproduction guide](../analysis_specs/kaushik_polymarket_full_sample_reproduce.md)
for exact executed commands and artifact bindings.

### Estimation v1 spill failure and verified shutdown

The fixed estimation v1 body ran at producer
`2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a`, starting at
`2026-10-11T14:57:10.006609+00:00` and ending with exit 1 at
`2026-10-11T15:02:33.134285+00:00`. Its failure record reports
`estimates_incomplete` and `OutOfMemoryException`. The traceback identifies
`create_sports_view` materializing `sports_observations` through Parquet `COPY`,
before model fitting. DuckDB reached the unchanged 16GB temporary-spill cap
(reported as 14.9 GiB/14.9 GiB used) despite the admitted 192GB memory limit.
The failure read ledger charged 58,701,085,017 bytes below the 8TB read ceiling;
this is a spill-cap failure, not completed estimation.

The failed stage is preserved at
`/mnt/data/runs/.2026-10-10_kaushik_polymarket_full_sample_estimates_v1.staging-0nxa_ukt`.
Root reports its sports cache is 0 bytes. There are no accepted estimates, fitted
results, saved-score audit, report or full-sample publication. Accepted inputs v2,
the exact cutoff-subset gate and sports metadata remain accepted and unchanged.
Local small failure evidence is retained under
`output/kaushik_polymarket_full_sample_2026-10-10_v1/`:

| Evidence | SHA256 |
| --- | --- |
| `estimate_v1_execution_receipt.json` | `12c000c024af95ef6fcae644efc75f9852d7c311dd516dabcc079e5b5ad96460` |
| `estimate_v1_failure.json` | `2d9710b24f98f531b2c63e69dee25f432dd2483d5f72abc7ec6966a38c9f16de` |
| `estimate_v1_stderr.log` | `7f75bffe51c28aebea0d06f71df823db24200301bcae47c7f88592d3fc32036a` |

Root confirmed no process or transfer still needed the instance, stopped it and
verified EC2 is stopped. This entry records root's verified stopped state without
inventing a stop timestamp. The failed stage and accepted earlier stages remain
preserved; no storage recovery or deletion is asserted for this estimation failure.

Local implementation work is limited to removing the unnecessary global raw
sports-cache `ORDER BY`, with equivalence fixtures. This is a physical-only
resource hypothesis, not yet production-proven; it must preserve eligibility,
analytic fields, multiplicity, models, weights and every scientific definition.
The reviewed committed retry source, fixture results, retry version, immutable
destination, controls and fresh capacity preflight are pending. The failed v1
stage must not be reused or relabeled as accepted. Root owns any later restart,
retry execution and publication.

### Reviewed physical-only cache retry

The raw sports-observation COPY now omits its global sort only. All 17 projected
fields, joins, predicates, exact binary64 prices, source locators, clocks,
reconciliation gates and resource limits remain unchanged. Downstream keyed
summaries and numerical caches keep their own sorting. Three focused fixtures
passed, including exact bidirectional multiset equality and the no-sort query
plan. Thirty pooled/sport grid comparisons retain support, means and scores;
the maximum covariance rounding difference is `1.8189894035458565e-11`, within
the existing `1e-10` absolute fixture tolerance. This is not a production-capacity
guarantee or a requirement of bitwise-identical aggregated floating sums.

All 143 local fixtures passed in 33.201 seconds. Independent review admitted
the physical change after three new and four existing timing/support oracles
passed. A first EC2 test run passed 142 of 143 tests; its one rejection-branch
fixture implicitly assumed a non-production host. The fixture now explicitly
simulates Darwin and checks the specific runtime-block message, without
changing the production guard. All 143 EC2 fixtures then passed in 35.958
seconds. Both immutable test execution records remain under the control stage.

Root restarted the same instance and mounted the verified data UUID. AWS reports
`r5.8xlarge`, which differs from the setup note; no instance-type change was
performed. Measured available RAM was 264,997,736,448 bytes and data free space
64,684,126,208 bytes before source/test synchronization. Fresh production
preflights must remeasure admission instead of relying on these earlier values.

The retry requires fresh source-bound `_sports_metadata_v2`,
`_sports_preflight_v2`, `_estimate_preflight_v2` and `_estimates_v2` stages.
Accepted inputs v2 and the exact subset audit are reused by their recorded
hashes. Neither the failed estimate cache nor the old cutoff estimates may be
reused as full-date results. The new source HEAD and actual accepted stage
digests will be saved in the fresh controls. Estimation remains unaccepted.
