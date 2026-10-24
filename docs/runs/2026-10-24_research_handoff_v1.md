# Research handoff, 24 October 2026

Scope: organize the existing code and completed research for collaborator handoff
and further final work. This task changes navigation, packaging and declared runtime
compatibility only. It does not estimate a new result or revise a completed cohort,
estimator, table, figure or scientific manifest.

## Source and selection

The canonical starting revision is
`78f53a9c820ba3197a9d824cb7ec72bef5e5d967` on
`codex/kaushik-kalshi-replication`. Reviewed changes use the new branch
`codex/research-handoff`. The Mac repository has no historical baseline/upstream;
only explicitly reviewed files are synchronized, never a bulk Mac commit.

The [handoff index](../handoff/README.md) and
[study catalog](../handoff/study_catalog.json) separate current exchange-excluded
Polymarket, native Kalshi and corrected-wallet nine-sport results from frozen
mechanism diagnostics, deliberately retained comparisons and historical studies.
All existing source import paths remain stable. Earlier agent guidance and status
are preserved in the documentation archive; live guidance points to `AGENTS.md`.

The run-owned handling directory is
`/mnt/data/runs/2026-10-24_research_handoff_v1`; local small handling records are under
`output/research_handoff_2026-10-24_v1/`. The generated selection enumerates exact
files; its hashes, source revision, environment, test results and exclusions belong
in machine-facing records. Old capsule bodies/manifests are kept unchanged.

## Boundaries

- Share current tracked source, fixtures, frozen specifications and run records;
  accepted portable reports, vector assets, compact summaries and acceptance proofs.
- Include bounded public event/clock/exclusion metadata, not raw trade tapes,
  per-wallet histories, FIFO records, provider caches or fitted score bodies.
- Do not export Git history, private environment/key files or data symlinks.
  Current canonical RPC configuration is environment/private-file based; the stale
  Mac fallback is not part of the exported source.
- Prior release provenance may contain intentionally private absolute paths or
  execution-time pending statements. Do not rewrite hash-bound history; the new
  catalog and completion receipts describe current handoff status separately.
- A summary-only handoff is not a full private-data reproduction capsule. Exact
  historical producer/auditor revisions and omitted data remain dependencies.

## Validation and publication protocol

1. Check the canonical clean source and exact file selection; screen credentials
   without displaying secret values. Validate compact Parquet schemas/row counts
   independently of their operator-assigned packaging role.
2. Run focused packaging fixtures, syntax/reference checks and the broader suite in
   an isolated source-only snapshot with no live-data symlinks. Preserve failures;
   runtime mismatch or a historical failing test is not silently waived.
3. Build the handoff with explicit paths/hashes and no overwrite. Reopen its exact
   inventory; separately inspect nested source archives, live Markdown references,
   report assets and study coverage. No new numerical validation is implied by
   byte-level checks.
4. Publish a new immutable private Dropbox release under
   `Polymarket Data and Code/joseph_code/2026-10-24_v1`; preserve earlier folders.
   Verify content readback, then write the external publication receipt last.
5. Confirm workers, processes and transfers have released their dependencies;
   stop the shared EC2 instance and verify the stopped state.

At this source-record stage, final package QA, transfer and shutdown are not yet
asserted. Their actual outcomes are recorded in the completed release's external
receipts and local task status, rather than claimed in advance here.
