# Kaushik Polymarket replication: full available-date rerun

**Status:** planned, not yet executed or accepted, 10 October 2026. The user
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
