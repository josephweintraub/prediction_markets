# Kaushik Polymarket replication: full available-date rerun

**Status:** input v2, exact cutoff-subset audit and fresh sports metadata v3
accepted; full estimation v3 and independent saved-score audit v1 are accepted
under H14. The v3 body completed under the unchanged resource caps and physical
join policy; this is run-specific capacity evidence, not a general guarantee. Estimates v1/v2
failed before fitting and remain preserved. Root previously verified shutdown at
2026-10-11 15:58:09 UTC, then restarted for tested v3 execution; the current run's
shutdown remains pending. Independent numerical release QA and report v1
numerical/visual QA are accepted. Final source release/push, finished-bundle
assembly/QA and publication remain pending,
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
| Source and fixture QA | Committed explicit full-date mode; focused and relevant broader tests; unchanged cutoff behavior and scientific definitions | H14 committed; 145 local and 145 production-runtime tests plus independent QA passed; source remained frozen through completed body/audit; final release head pending |
| Input preflight | Reopened same-source inventory and field semantics; fresh read/RAM/cache/spill/output/disk admission | v2 root reviewed |
| Full-date inputs | Immutable outputs and reopened acceptance; source/exclusion/support reconciliation; no fanout or silent repairs | v2 accepted |
| Cutoff-subset reconciliation | Exact source-locator membership and analytic-field agreement with inputs v2; full population partitions into retained earlier and newly eligible later records | v1 accepted; independent metadata review passed |
| Sports metadata | New-base binding to unchanged provider inputs; native pair/result/timing proof; retained date ranges and exclusions | v3 accepted; v1/v2 preserved; provider maps byte-identical |
| Estimator preflight | Fresh committed-source/base/provider bindings and separately reviewed resource admission | v3 root-admitted; actual physical policy bound; unchanged scientific definitions/caps |
| Full estimation | Full writeup grids; common duration samples; BUY-role partition; numerical/rank/convergence gates; distinct sports seconds and zero clock-equation mismatches | v3 reopened accepted; root-reviewed; v1/v2 failures preserved |
| Independent saved-score audit | `saved_scores_reconciled`; reopened covariance, contrasts, intervals, influence, support and clock metadata | v1 accepted; 47 score files/all seven checks passed |
| Independent numerical release QA | Source-bound accepted summaries, models, saved-score algebra and reference comparisons | v3 accepted; 39 gates passed; no failed/pending numerical checks |
| Data-first report QA | Accepted-summary-only rendering; portable source/vector assets; displayed-value reconciliation, successful compilation and every-page review | v1 accepted; 31 pages/21 vector figures/36 native tables; two compile passes and all-page review passed |
| Publication and shutdown | Root verifies finished-bundle transfer and no active dependencies, stops exact instance and verifies stopped | Historical v2 shutdown verified; current release/report work remains active; publication/final shutdown pending |

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

### Accepted sports metadata v2 and dispatched estimation v2

The physical-only retry producer is frozen at
`94cd21798e9961f6cfdb173ec034df75228ed19b` on the same full-sample branch.
Only the unnecessary global raw sports-observation cache sort was removed;
keyed numerical sorts, scientific definitions, source populations and all caps
remain unchanged. The 143-test local result is 33.201 seconds; the saved second
EC2 fixture receipt records exit zero and 143 tests in 35.958 seconds. The first
EC2 fixture failure remains preserved; its correction changed only the test's
host assumption, not the production guard. The fresh metadata's success is not
yet proof that the expanded sports cache or complete estimation fits production
capacity.

Root's direct sports and estimate preflight invocations are recorded in
`preflight_execution_records_v2.json`, with canonical working directory, exact
argument vectors, completion exit zero and manifest digests. They were direct
root invocations, not timed body wrappers; no preflight timestamp is invented.
The [full-sample reproduction guide](../analysis_specs/kaushik_polymarket_full_sample_reproduce.md)
records those exact commands and both body commands.

The sports v2 body completed with exit zero and empty stderr, from
`2026-10-11T15:31:01.001956+00:00` to
`2026-10-11T15:31:06.371088+00:00`. Accepted target:
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v2`.
Its outputs reopen and have exactly the same byte counts, hashes, schemas and
coverage as v1: 13,909 games and 14,383 markets across nine sports. It read no
trade bodies and retains native pair/provider-result proof. Provider inventory,
historical date ranges, EPL draw claims and ATP timing qualification are unchanged;
no provider games, resolution refresh or API collection were added.

| New retained evidence/binding | SHA256 |
| --- | --- |
| `cache_fix_ec2_fixture_v2_receipt.json` | `977ac44058e5fd88d10d7370f228375a1700b4b713e35c38f6bcf065543b16f7` |
| `preflight_execution_records_v2.json` | `548ff3dfce3c3ff1e619b7c8f75a81736dd326946342076ef4435c6520377cb2` |
| Sports preflight manifest v2 | `03ca6ddb6af72ea6b069ae5489347314a52f2885288c00b0960902dc0f848975` |
| Sports metadata manifest v2 | `97a774fcad3c80d74aa1970a594c91186a6da3e19ffb974146f50389468d1a3e` |
| Sports metadata acceptance v2 | `b282e6cd15a7b40ca24cd845a7c00d97e44b3a9dbc26945f2464b2a24c5d07dd` |
| `sports_v2_execution_receipt.json` | `783085681c79f5017b1c443ff5c6dbaa39c95a8781ee081dcd1a03e112326a79` |
| Reviewed sports binding v2 | `ad60852b83c922cb11711d4b22d23bf5cd153aaeb26f4ea43e0481f8aa8f8abe` |
| Estimate preflight manifest v2 | `ab266c8e31abc83baaeaf1591a4ba9ec44ce144f0b1ce095e4351040e0e56307` |
| Estimate root review v2 | `339e09735f944cbf92f896eed47f7469d9221ca732c8a3d005cd37cc32fd756f` |

AWS reports the same `r5.8xlarge`; no instance-type change was made. The fresh
estimate preflight measured 264,739,303,424 available RAM bytes and 64,682,438,656
free data bytes against the unchanged 60,000,000,000-byte reservation. Planned
maximum reads remain 6,951,547,448,572 bytes. The 192GB DuckDB/32GB NumPy budget,
240GB RAM admission, four threads, 16GB spill and every other cap match v1.
The root review reports `admitted_same_sample_same_caps_physical_cache_fix` and
matches the frozen producer to fresh accepted sports metadata v2. Accepted inputs
v2 and the exact subset audit remain bound and unchanged.

Root dispatched `estimate_body_execution_v2.py` against the new immutable
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v2` target.
At dispatch, estimation v2 was running, not accepted; no successful body receipt,
estimate manifest, saved-score audit or report was asserted. All v1 stages and failures
remain preserved and must not be relabeled as v2 results. Later audit/report
templates now refer to estimates v2. These status edits are local only and are
not synchronized into the canonical checkout while the production body is active.

### Estimation v2 spill failure; bounded diagnostic and shutdown pending

The fixed estimation v2 receipt records exit 1, from
`2026-10-11T15:33:59.002652+00:00` to
`2026-10-11T15:40:51.389462+00:00` (6 minutes 52.387 seconds from the receipt).
The failure is again `estimates_incomplete` / `OutOfMemoryException` in
`create_sports_view`'s Parquet `COPY` of `sports_observations`, before fitting.
DuckDB again reached the unchanged 16GB temporary-spill cap (14.9 GiB/14.9 GiB
used) under 192GB memory. Peak resident size was 189,177,812 KiB, with no swap.
The failure read ledger charged 58,701,085,017 bytes below the 8TB ceiling.

The final failed stage is preserved at
`/mnt/data/runs/.2026-10-10_kaushik_polymarket_full_sample_estimates_v2.staging-xjev5f0z`.
Root reports the raw sports cache is absent from that final failed stage. No
model fitting or accepted estimates, saved-score audit or report resulted.
Accepted inputs v2, the exact subset audit and accepted sports metadata v2 remain
unchanged. The fixture-equivalent sort removal was not a successful production
capacity fix; its passing fixtures and byte-identical metadata do not establish
a completed cache or estimation run.

| Preserved v2 failure evidence | SHA256 |
| --- | --- |
| `estimate_v2_execution_receipt.json` | `0b028c03cb8c3ba76be8e846819637295ed09f1bfe3f0037e50fde7bf8377ddb` |
| `estimate_v2_failure.json` | `a3e78d249f27b87d9aeebd60b71ad30656fc89b904f2845d490cdb5a251de748` |
| `estimate_v2_stderr.log` | `47afebbc6e0a807873d25748086f3adf9ae8f288d6381ea421f34a00735ebdfb` |
| `estimate_v2_resources.log` | `bc70e5852c03af2c9112c8ffd008e9d9c48390bb3a4dd707c101c91ea7996cea` |

At that prior failure-update moment, root reported the same instance remained on
only for a bounded metadata-only `EXPLAIN` diagnostic. Diagnostic results,
further source changes and a new retry were not yet asserted. Process/transfer
closure and post-v2-failure shutdown verification were still pending; the later
entry below records the completed diagnostics and verified stopped state.
Preserve both failed stages, their exact commands and all earlier accepted
bindings. Further estimation, saved-score auditing, report QA and publication
remain pending; no scientific definition or resource cap has changed here.

### Production join-plan evidence and verified post-v2 shutdown

Root completed two saved read-only production diagnostics at clean producer
`94cd21798e9961f6cfdb173ec034df75228ed19b` under DuckDB 1.5.0. The original
sports SELECT/COPY plan and the follow-up baseline plans for raw sports COPY,
Table 1 grouped means and the duration-tail projection put the much larger
trade branch on the right/build side of both claims joins. The planner's
underestimated trade cardinalities are estimates, not observed sample counts.
The saved input counts and plan orientation identify a physical build-side
problem; they do not independently prove the complete cause of either failure.

The follow-up temporarily set
`disabled_optimizers='join_order,build_side_probe_side'`. Both exact optimizer
controls were supported in this runtime. Each controlled plan instead puts the
smaller claims input on the right/build side of its two claims joins, retaining
the trade branch on the probe side. The original empty optimizer setting was
restored and the connection closed. Both diagnostics were `EXPLAIN` only: no
`ANALYZE`, body query or COPY was executed, no remote file was written, and the
COPY target remained absent before and after. This confirms plan orientation,
not production throughput, completed cache capacity or a successful estimate.

| Retained diagnostic/shutdown evidence | SHA256 |
| --- | --- |
| `production_sports_plan_diagnostic_v1.json` | `9f795eb5dad7d5075da58f0a5c5fb200025fa9432f5e1e5c636810c55c412ecf` |
| `production_join_control_plan_diagnostic_v1.json` | `882e91f248f8d45ac341cd60e78696e6368f889c478de719edab2c56a62eb007` |
| `estimate_v2_shutdown_receipt.json` | `90f133385939386b5a33ee7d5620d31a722ea11125e467dccdf2a498f8dee26a` |

After the diagnostics, root verified zero active research/transfer processes,
the clean canonical source at that same HEAD and both failed stages preserved.
The shutdown waiter exited zero, and AWS describe independently reported
`stopped` for exact instance `i-0f5b31a268af53938`, actual `r5.8xlarge`, verified at
`2026-10-11 15:58:09 UTC`. The receipt records that state; it is not inferred
solely from waiter completion.

Local implementation is adding this physical join policy configure-wide, with
saved policy/runtime provenance. Scientific predicates, projections, population,
models, weights and resource caps must remain unchanged. The next source-bound
metadata, sports preflight, estimate preflight and estimate stages will use fresh
v3 targets. Exact reviewed source, commands, controls and digests remain pending.
Accepted inputs v2, the exact subset audit and sports metadata v1/v2 remain intact;
neither failed estimate stage is accepted or reusable as results. No restart is
scheduled until local tests and independent review pass. All further lifecycle,
production admission and execution remain root-owned; these document edits are
local only.

### Locally frozen configure-wide policy; production admission pending

The local implementation candidate and driver fixtures are frozen at these file
SHA256 bindings, not yet a new committed production HEAD:

| Local candidate file | SHA256 |
| --- | --- |
| `analysis/kaushik_polymarket_replication/run_estimates.py` | `003fd2068681afd94a831cc19dd972ace888cabfe051131874c5917e9cac320c` |
| `tests/test_kaushik_replication_driver.py` | `1e2703b752f9df2b71c6e89aed76d86f97312ff13b448e7e9d24c95f337c786e` |

The explicit physical join policy is outside `CAPS`. Fresh preflight records
actual scalar optimizer-setting readback and runtime provenance; runtime checks
repeat before body work and before publication. Both sports metadata and estimate
stages bind that evidence and fail closed if the policy/readback drifts. The
policy changes physical join planning only; core numerical estimators,
`build_inputs.py`, frozen scientific specifications and all resource caps remain
unchanged.

The earlier implementation-agent result was seven focused tests in 1.802 seconds under
DuckDB 1.5 and Python 3.10.13, not the older DuckDB 1.4.4 runtime. The same
fixture covariance discrepancy, `1.8189894035458565e-11`, remains within the
existing `1e-10` absolute allowance; no tolerance was widened. This is bounded
fixture equivalence evidence, not a bitwise-output or production-capacity proof.

The first root 145-test broader suite had three failing subtests in 32.150
seconds, solely from whitespace padding in JSON operator labels. Root authorized
fixture-only `.strip()` normalization across DuckDB 1.4.4/1.5.0; no producer,
guard, scientific definition or cap changed. This supersedes the earlier fixture
candidate SHA256
`351d7b99168069ef1abb055de8dc98498cb585358a62bc6b943024277f994f0c`
with the final binding above. Root's broader suite then passed all 145 tests in
30.514 seconds under Python 3.13.1 and DuckDB 1.4.4.

Independent QA passed seven tests in 1.002 seconds plus five dense oracles in
1.507 seconds, including 14 configured scalar-setting readbacks. Reversing only
the authorized join-policy additions in memory recovers exact H13 producer file
SHA256 `8d8b24c68becb5f2f5a7de2d5c6e5a734143c95e42e6082e803ed6ce1160f687`,
establishing the retained code/cap/scientific boundary. These local gates passed;
they do not establish production-capacity success.

Root initiated restart for reviewed production fixtures and fresh v3 admission;
root now reports the same `r5.8xlarge` running, the verified UUID mounted at
`/mnt/data`, clean canonical H13, zero active research/transfer processes and a
passed production-host guard. Preliminary free data space is 64,681,312,256 bytes
and available RAM 264,999,567,360 bytes; fresh stage preflights must still bind
their own observations. The AWS status-ok wait completed with exit 0. Root synced
only the reviewed producer/test and fixed fixture wrapper, inspected the exact
canonical diff (29 producer lines and 90 test lines), and started the serial
six-module production-runtime fixture suite under
`controls/join_policy_ec2_fixture_execution_v1`, without real-data scans. The suite
passed all 145 tests in 37.458 seconds, with receipt start
`2026-10-11T16:10:39.729823+00:00`, end
`2026-10-11T16:11:18.487637+00:00` and exit 0; root confirmed session 96378 closed
and canonical producer/test hashes match the frozen bindings above. Local small
evidence was size-checked before reading:

| Production-runtime fixture evidence | SHA256 |
| --- | --- |
| `join_policy_ec2_fixture_v1_receipt.json` | `b24cea6d9b88794d58e86e9d6b9214b214bd6938a0547ef780ff22fbfffdedf8` |
| `join_policy_ec2_fixture_v1_stderr.log` | `d0eab82ee84e897d7ecc01e58974e57b45e4e5f82fd01cc05bf51e13232d260e` |
| `join_policy_ec2_fixture_v1_resources.log` | `bb1fca1aa8b7150fc93c078e44d38a87747ff740f48e742965cdad6363933d80` |

This is fixture admission evidence, not production-capacity success. At that
fixture-admission moment, committed H14, exact
reviewed v3 source/control/metadata/preflight bindings and all v3 execution remain
pending. The local documents are frozen-ready for integration, not a production
acceptance record. Accepted stages and both failed estimates remain unchanged
and preserved; root alone owns subsequent lifecycle and execution.

### Committed H14, accepted sports metadata v3 and running estimation v3

Root committed exactly the five reviewed files as H14
`7c7d2badcddcb28ac9d24189ce35096bbfc78201` on the existing full-sample branch.
Producer/test file hashes remain `003fd2068681afd94a831cc19dd972ace888cabfe051131874c5917e9cac320c`
and `1e2703b752f9df2b71c6e89aed76d86f97312ff13b448e7e9d24c95f337c786e`.
Canonical source remained frozen through the estimate body and audits; these
status edits are local only. Final source release is pending. Accepted inputs v2
and subset audit v1 are unchanged.

The fresh sports metadata stage is
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v3`.
Its fixed body receipt records `2026-10-11T16:16:17.638530+00:00` through
`2026-10-11T16:16:23.178347+00:00`, exit 0. Reopening accepted all outputs;
native binary-pair/provider-result proof is true and `trade_bodies_read` is false.
Small manifest comparison verifies output bytes/hashes/schema, per-sport coverage
and provider inventory identical to v1/v2: 13,909 provider games, 14,383 markets,
nine sports. No provider-game or resolution expansion is implied.

Fresh sports and estimate preflights, metadata acceptance, reviewed provider
binding and root capacity review are bound as follows:

| v3 evidence | SHA256 |
| --- | --- |
| Sports preflight manifest | `7292ef0db184b8f05d10246c676ea46f49e8cd32845e8b04c4fbf9ebb7bbb862` |
| Sports metadata manifest | `3b51578df6958d991151e41befe5f2f1a1ef68a12c3978065403504ee432ee6c` |
| Sports metadata acceptance | `f0bd0918a18c850a89e841f55fb8b1dd450813ad3b2afdb0a65c8dbc38e2353a` |
| Sports body execution receipt | `0f5c167c2f372e799be1ba17eccd0438b7b7e9632add4b95516b27c4c9161af0` |
| Reviewed sports binding | `e52cefcd23d2451faccc1c2d1cc7861dc85d5cd7161913bfe6d86cdcd4ca881d` |
| Estimate preflight manifest | `ed1c444987a194f29bcb1d363a88510d21179c74b0d4d208506e033e1fd03603` |
| Root estimate preflight review | `eb43fcee8450d95e2cab40cf050335ccc6ac42c91090b1b4907eb6435b34fb39` |
| Direct preflight execution records | `6aaaddea709585a948941a41efb5288fad9510a808809af6f281efc528677573` |
| Fixed sports body wrapper | `db6cd4348653f049ccdc19288a8f4d6c4f7b64469f72d542f845025961e508e3` |
| Fixed estimate body wrapper | `a09b8226011c8274ccdde7f7702e7d46b9d557f7c4e0e938d9a04040ffa4cbec` |

The exact four executed/dispatched commands are recorded in the
[full-sample reproduction guide](../analysis_specs/kaushik_polymarket_full_sample_reproduce.md#committed-v3-admission-and-dispatched-commands).
Preflight records identify actual root direct CLI invocations, each exit 0;
their root-observation timestamps are not timed remote body-receipt timestamps.
Sports metadata used the unchanged retained binding, then estimates used the
reviewed v3 binding. All targets retain the full-sample prefix and fresh v3 suffixes.

Estimate preflight observed available RAM 264,716,660,736 bytes and free data
space 64,679,702,528 bytes, against the unchanged 60,000,000,000-byte reservation
and 6,951,547,448,572-byte read plan. Sports preflight separately observed
264,754,626,560 bytes RAM and 64,681,246,720 bytes free. The physical policy
`disabled_optimizers=join_order,build_side_probe_side` is outside `CAPS`; actual
scalar readback/runtime bindings are validated in preflight and runtime, before
body work and publication, with fail-closed drift gates. Resource caps remain
192GB DuckDB, 32GB NumPy, 224GB combined, 240GB available-RAM floor, four threads,
16GB spill, 3GB cache, 8GB outputs and the prior read/projection limits. Scientific
predicates, models, weights and provider/source vintage remain unchanged.

Root dispatched the fixed `estimate_body_execution_v3.py` wrapper, execution
directory `_controls_v1/estimate_body_execution_v3`, session 83963, targeting
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v3`.
At that dispatch update, the body was running; no completion receipt, accepted estimate, successful
capacity outcome or numerical finding is asserted. Saved-score auditing,
accepted-summary report rendering, numerical/layout QA, publication and final
shutdown remain pending. Preserve both failed estimate stages and all prior
accepted stages; root alone owns execution, synchronization and lifecycle.

### Accepted estimation v3 and independent saved-score audit v1

The unchanged fixed v3 command completed from
`2026-10-11T16:19:14.106945+00:00` to `2026-10-11T17:55:34.970494+00:00`, exit 0.
The manifest reports `estimates_complete`; reopening reports
`estimates_reopened_accepted`, full-date scope and all outputs reopened. Root's
`accepted_estimate_root_review_v3.json` binds H14, the exact command, input/sports
bindings, caps, output fingerprints and reconciliation gates. Canonical source
remained unchanged throughout estimation and the following audit.

| Accepted stage evidence | SHA256 |
| --- | --- |
| Estimate manifest v3 | `3d4611756e0e9df9cb93d418861cd0041dd7adff087f080ace05b833a4950103` |
| Estimate acceptance v3 | `9d723fe5dfc0517033737c8be5df44c9cbba49df6b343453d0fa3773d6eb90eb` |
| Estimates JSON v3 | `6c3543cb4f4065dfd80d4961c85b86e5371ce3e5ba436f8e8dff01e8597f049a` |
| Estimate body receipt v3 | `14f58f61135f1f59276ae91b76e47a89955fd13e45b26ad702102cbf03b5b60e` |
| Root accepted-estimate review v3 | `f4918b1d2e76fb0d8de7a75f16d7b76367af711b1888aad1fbe49ffe4c83a169` |
| Saved-score audit v1 | `ca0d2b9198cec65fe5022da326a8849cf8b2173f819dce59029aad14a273bf86` |
| Saved-score audit body receipt v1 | `c53cbebb88bbb39b5269257cf1b12ab8250f65271c256bc496137cc23b2a7cce` |

Reopened support is 1,000,970,616 primary records and 2,001,941,232 baseline
all-role records. The common duration sample reconciles at N=252,319,669 and
G=497,313. These are cohort/support provenance, not a release findings table.
Observed primary executions range from `2022-11-21T19:50:09+00:00` through
`2026-06-21T21:41:20+00:00`; no new execution cutoff was introduced. The same
source-resolution vintage, provider cohort and timing qualifications remain.

Accepted caps remain 192GB DuckDB, 32GB NumPy, 224GB combined memory,
240GB available-RAM floor, four threads, 16GB spill, 3GB cache, 8GB outputs,
8TB read ceiling and unchanged projection bounds. Charged estimate reads were
598,451,354,311 bytes. Actual scalar physical-policy readbacks before body and
publication matched `join_order,build_side_probe_side`; no scientific predicate,
model, weight or cap changed in v3. The estimate manifest records Python 3.12.3
(GCC 13.3.0), DuckDB 1.5.0 and NumPy 2.4.3. Root additionally confirmed PyArrow
23.0.1 on Linux 7.0.0-1014-aws, x86_64, glibc 2.39; distinguish this production
environment from the local source-only fixture capsule.

The exact saved-score command is recorded in the reproduction guide. Under the
same H14, its receipt records `2026-10-11T18:01:50.458272+00:00` through
`2026-10-11T18:02:19.144175+00:00`, exit 0. The immutable audit target is
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_saved_score_audit_v1`.
Status is `saved_scores_reconciled`: all 47 saved-score files and seven checks
passed—CR0 outer products; common duration/BUY partition; influence diagnostics;
named contrasts/SE/CI; saved sports-seconds contract; saved support/complete grids;
and supplementary `G/(G-1)` distinguished from CR1. Charged audit reads were
1,855,979,124 bytes under the 25,000,000,000-byte ceiling, with unchanged
relative/absolute tolerances `2e-8`/`1e-10` and 2,048-row batches.

The audit reconstructs covariance/influence from saved scores and contrasts from
saved coefficients; it does not independently refit coefficients, reconstruct
raw membership/timing or certify native actions or whole-history completeness.
At this accepted-stage update, independent numerical release QA and report/visual
QA were still pending; the next entry records their subsequent acceptance.
Final documentation/source release and push, finished-bundle assembly/QA,
Dropbox publication and final verified shutdown remain pending. No release
transfer or current-run stopped state is inferred from estimation/audit acceptance.

### Accepted independent numerical release and report v1 QA

`release_numerical_qa_v3.json` is sealed with status
`accepted_summary_and_saved_score_reconciled`: all 39 gates passed, with no failed
or pending numerical checks and 18 JSON bindings reopened. All 12 model
rank/convergence gates, 47 saved-score artifacts and 2,118 coefficient/contrast
checks passed. The review reconciles 878 keyed result rows, unchanged results for
the three unchanged sports populations, and 22 Table 1/A2 reference rows across
308 scalars. These checks do not certify a raw coefficient refit or complete
historical collection. All 28 duration/A1 headline effective cluster counts
remain below 30; concentration warnings and nominal-interval qualifications
remain, separately from support admission. The combined absorbed fixed-effect
rank is not independently certified.

`report_v1_qa.json` is sealed with status `report_qa_passed`, recorded at
`2026-10-11T18:11:38.535Z`. Report v1 contains 31 pages, 21 vector PDF figures
and 36 native tables (35 tabular and one longtable). Both `pdflatex` passes
exited zero; every page was visually inspected and passed. QA reconciled 1,998
PDF-text numerical table cells, 660 plotted CR0 marks and omission of 80 withheld
plot cells. All 28 duration/A1 influence marks and nominal qualifications remain.
No substantive compilation/layout failure remains; minor underfull-box and
isolated-cache notices are recorded in the QA receipt. The report manifest's
initial compile/visual fields remain immutable; the external QA receipt records
completed acceptance. Portable TeX and relative vector figures are primary;
the verified PDF is a preview.

| Release QA/report artifact | SHA256 |
| --- | --- |
| Independent numerical release QA v3 | `5623dd9bd969a38ddd42d359a754375f01c4648f57bbb718f0a722180896ff28` |
| Report v1 QA | `6b282bd1ee61d22be8e7e7bda7bf87035ea0402bdf70c1bc668b44740f19c8cb` |
| Report v1 source | `429cb940cde5a3b2a6e4d13f509b87bcf137112822de74eff78d7160f5dc5acb` |
| Report v1 immutable manifest | `71fd5d7cd54e402bc34ea0fd27b25cdd4c26c2f939becaa3f10b8d100c90c47a` |
| Report v1 compiled preview | `26aac815332a6ce4a6e753f8df797dedf077fbc27569e8dfd8d8bd4b47e1240b` |

The no-cutoff dates, eligibility waterfall, unchanged provider-cohort/source
vintage and resolution/clock qualifications were retained. Report QA reconciles
presentation to accepted saved outputs, not raw eligibility or timing. Final
release source head/push, finite bundle assembly and independent capsule QA,
Dropbox transfer verification and current-run shutdown are not yet asserted.
