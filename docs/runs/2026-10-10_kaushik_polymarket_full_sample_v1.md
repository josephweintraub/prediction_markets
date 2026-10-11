# Kaushik Polymarket replication: full available-date rerun

**Status:** approved failed-build recovery underway; input v2 pending,
no full-sample result accepted,
11 October 2026. The user
confirmed removing the March 25 execution cutoff and rerunning the full writeup
over all available source dates, keeping all other scientific definitions
unchanged. This record contains no full-sample findings or predicted sample sizes.

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
| Source and fixture QA | Committed explicit full-date mode; focused and relevant broader tests; unchanged cutoff behavior and scientific definitions | Planned |
| Input preflight | Reopened same-source inventory and field semantics; fresh read/RAM/cache/spill/output/disk admission | Planned |
| Full-date inputs | Immutable outputs and reopened acceptance; source/exclusion/support reconciliation; no fanout or silent repairs | Planned |
| Cutoff-subset reconciliation | Exact source-locator membership and analytic-field agreement with inputs v2; full population partitions into retained earlier and newly eligible later records | Planned |
| Sports metadata | New-base binding to unchanged provider inputs; native pair/result/timing proof; retained date ranges and exclusions | Planned |
| Estimator preflight | Fresh committed-source/base/provider bindings and separately reviewed resource admission | Planned |
| Full estimation | Full writeup grids; common duration samples; BUY-role partition; numerical/rank/convergence gates; distinct sports seconds and zero clock-equation mismatches | Planned |
| Independent saved-score audit | `saved_scores_reconciled`; reopened covariance, contrasts, intervals, influence, support and clock metadata | Planned |
| Data-first report QA | Accepted-summary-only rendering; portable source/vector assets; displayed-value reconciliation, successful compilation and every-page review | Planned |
| Publication and shutdown | Root verifies finished-bundle transfer and no active dependencies, stops exact instance and verifies stopped | Planned |

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
