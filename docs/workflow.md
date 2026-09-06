# Research workflow

This workflow keeps exploratory work useful without allowing it to become an accidental
paper result. It applies to pipeline changes, analysis changes, robustness checks, and
report production.

## 1. Choose the work state

Every task starts in one of four states:

| State | Purpose | May support a paper claim? |
|---|---|---|
| Exploration | Learn, debug, and compare specifications | No |
| Candidate | A promising result with a named script and saved artifact | Not yet |
| Confirmatory | Pre-specified analysis on a held-out or frozen sample | Yes, after validation |
| Archived | Superseded, invalidated, or no longer active | No |

An exploratory result becomes a candidate only after its ad-hoc work is promoted into a
script and configuration. A candidate becomes confirmatory only through a dated decision
record made before inspecting the confirmatory output.

## 2. Branch and commit discipline

- Keep `main` deployable and documented.
- Use a short-lived branch for refactors, data-pipeline changes, and new analyses.
- Name branches by purpose, for example `codex/repository-cleanup` or
  `analysis/long-binary-liquidity`.
- Make small commits that describe why the project changed.
- Update `CHANGELOG.md` in the same session as a notable change.
- Do not combine repository restructuring with new empirical analysis.

## 3. Declare the analysis before running it

A run specification must state:

- Research question and work state
- Input data vintage
- Universe and exclusions
- Unit of observation and weighting
- Lifecycle window
- Slice construction and minimum cell sizes
- Primary and secondary estimands
- Inference and multiple-testing rules
- Expected output tables and validation checks

The specification may be a small committed configuration or a dated section in
`docs/decisions.md`. Any post-result change must create a new specification and run.

## 4. Use immutable run directories

Generated work belongs outside Git under:

```text
/mnt/data/runs/<YYYY-MM-DD>_<analysis>_<run-id>/
  manifest.json
  config/
  intermediates/
  tables/
  figures/
  report/
  logs/
```

Never overwrite a completed run. A corrected or extended analysis receives a new run ID.
The convenience pointer `/mnt/data/runs/latest/<analysis>` may point to a completed run,
but reports must cite the immutable run ID.

Each manifest must contain:

- Analysis name, work state, and run ID
- Git commit and repository dirty status
- Exact command and configuration files
- Python and dependency versions
- Input paths, sizes, modification times, and content fingerprints where practical
- Data-vintage identifiers and expected row counts
- Filters, windows, thresholds, and random seeds
- Output paths, row counts, and validation results
- Start/end timestamps and final status

Input mismatch, missing metadata, an empty scheme join, or a dirty repository in a
confirmatory run must fail loudly.

## 5. Separate computation from presentation

- Computation scripts write machine-readable Parquet or JSON artifacts.
- Renderers read those artifacts and produce figures, tables, and HTML.
- Reports never contain hand-entered headline values.
- Every report names its producing script, configuration, run ID, and source artifacts.
- Publication figures and tables are copied from a validated immutable run, not regenerated
  ad hoc in the paper directory.

## 6. Validate proportionately

Before a full-data run:

1. Compile all Python files.
2. Run unit tests over synthetic data.
3. Run a tiny integration fixture through the full workflow.
4. Verify schema, join coverage, filters, and expected cardinalities.
5. Confirm the data vintage and Git commit.

After the run:

1. Check row, market, wallet, and cluster counts.
2. Check dropped slices and thin tails.
3. Reconcile summary values with their underlying decile tables.
4. Compare count-, dollar-, and equal-market-weighted estimates.
5. Record failures and deviations in the manifest and decision log.

## 7. Archive instead of accumulating

When a direction is no longer active:

- Move its code or documents into a dated archive with `git mv`.
- Add a short status file explaining what it did and why it was retired.
- Remove stale claims from current documents in the same commit.
- Keep source history, but do not keep generated outputs in Git.
- Do not delete EBS artifacts until they appear in a reviewed retention inventory.

## 8. Publication gate

A result is publication-ready only if a clean environment can reproduce it from the
documented inputs and command, all headline cells pass their sample-size rules, statistical
tests match the current methods specification, and the report identifies an immutable run.
