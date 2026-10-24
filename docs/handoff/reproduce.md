# Reproduction routes

## 1. Inspect and verify the delivered files

At the **release root**, using Python 3.12 or newer:

```sh
python code/scripts/build_research_handoff.py verify --bundle .
```

This checks the explicit inventory and file bytes. It does not prove statistical
validity, source completeness, or access to omitted private data. The separately
saved QA record states which fixtures and references were actually checked.

Reports can be read immediately. Portable LaTeX source and companion figures are
primary; use the existing PDF previews where supplied. Compile from the report's
own directory so relative figure paths resolve, but direct build outputs to a
separate temporary directory or work on a copy. Do not add compiler debris to the
sealed bundle. A missing private provenance path
inside an old manifest is not an instruction to fabricate that input.

## 2. Run small code fixtures

Work from `code/`, not from an arbitrary installed-package location. The repository
is a source checkout with runnable scripts, not a promised pip-importable library.

Python **3.12+** is required by existing modules that use PEP 701 f-string syntax.
Git is required for provenance-related code and the synthetic Git fixture tests.
The production interpreter used by the latest studies is Python 3.12. Existing
dependency declarations are in `pyproject.toml`; the handoff also records installed
production versions. Those versions describe the observed environment, not proof
that every historical study works with any allowed dependency version.

```sh
python -m pip install -e '.[analysis,dev]'
python -m unittest discover -s tests -p 'test_*research_handoff.py'
python -m pytest tests/test_production_guard.py tests/test_kaushik_replication_estimators.py
```

These are synthetic/fixture checks, not commands to scan private data. Installation
may require network access. Use the selected study's recorded runtime when replaying
its exact numerical fixtures. The completed-release QA record, rather than these
example commands, states the tests actually executed during packaging.

## 3. Rebuild a selected study

1. Read its frozen contract and run record from the [catalog](study_catalog.json).
2. Obtain private inputs and admission/acceptance proofs listed in that record.
3. Check out the exact committed producer revision, not merely the newest handoff
   HEAD. A release may use different producer, auditor and renderer revisions.
4. Run only on the admitted canonical production host, with mounted `/mnt/data`.
   Do not remove or bypass `production_guard` to use real data elsewhere.
5. Use new output directories, reopen the saved evidence, and repeat the independent
   acceptance gates before rendering or publishing.

Detailed routes:

- [Polymarket cutoff reproduction](../analysis_specs/kaushik_polymarket_replication_reproduce.md)
- [Polymarket full-date reproduction](../analysis_specs/kaushik_polymarket_full_sample_reproduce.md)
  plus the [exchange-exclusion contract](../analysis_specs/kaushik_exchange_wallet_exclusion_v1.md)
  and [executed exchange-excluded run](../runs/2026-10-14_kaushik_exchange_filtered_rerun_v1.md)
- [Native Kalshi reproduction](../analysis_specs/kaushik_kalshi_reproduce_v1.md)
- [Corrected-wallet adoption contract](../analysis_specs/sports_wallet_adoption_v1.md)
- [Tennis](../analysis_specs/tennis_timing_wallet_exits_v1.md),
  [profit taking](../analysis_specs/sports_profit_taking_v1.md), and
  [terminal diagnostics](../analysis_specs/terminal_pattern_diagnostics_v1.md)

Required placeholder paths/hashes in those guides must come from the recorded run.
They are intentionally not executable defaults. Some consumers reopen large score,
provider or membership bodies even when final tables need only compact summaries;
the summary-only handoff cannot promise a complete fresh render or production
rerun without restoring those bodies.

## Build the next handoff

The recorded selection lists every included source/artifact file explicitly. The
planner hashes those exact files; the builder creates a new directory and writes
its completion marker last. Neither searches for additional data nor overwrites a
finished package.

```sh
python scripts/plan_research_handoff.py --source-root /ABS/SCREENED_SOURCE \
  --artifact-root /ABS/ARTIFACTS --selection /ABS/selection.json --output /ABS/new_plan.json
python scripts/build_research_handoff.py build --plan /ABS/new_plan.json --output /ABS/NEW_RELEASE
python scripts/build_research_handoff.py verify --bundle /ABS/NEW_RELEASE
```

Credential-content and nested-archive checks are separate from file-inventory QA.
Publish bodies to a new private Dropbox release, verify downloaded contents, then
publish the external completion receipt last. Keep that receipt outside the sealed
bundle so it does not create a circular self-hash or rewrite an accepted inventory.
