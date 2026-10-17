# Reproducing the fresh Kalshi replication

Workflow reference, not a completion or acceptance statement. The scientific
contract is [Kalshi v1](kaushik_kalshi_replication_v1.md), alongside the
[methods reference](../methods_reference.md). Exact executed commands, committed
source revisions, environment, immutable stage paths, SHA-256 bindings, failures
and final release inventory belong in the
[run record](../runs/2026-10-16_kaushik_kalshi_replication_v1.md).

Every `REQUIRED_FROM_RUN_RECORD:...` below is a required replacement, not an
inferred path/hash or a runnable default. Keep metadata-builder, trade-builder,
estimator, map-producer and auditor revisions distinct: they may differ. Execute
only the recorded committed sources; do not replace a historical producer HEAD
with the current checkout HEAD. No new driver or CLI is defined by this guide.

## Contract and prerequisites

One observation is one canonical native public `trade_id`, equally weighted,
not one contract unit, wallet fill or inferred maker/taker pair. Use all admitted
archive dates, exclude combinations and unknown combination identities, and
retain disjoint exclusions. Native dollar prices select the recorded taker YES
or NO claim; do not divide prices by 100 or replace its price with a calculated
complement. Binary payout admission is explicit and independent of raw
missing/zero notional fields. Measures are `100*(Y-P)` cents and individual
`100*(Y/P-1)` percent before averaging, with fixed ten-cent bins and signed
D10-minus-D1 contrasts. This run has no wallet/bot/BUY-role filter and no
Appendix A2 maker/taker comparison. It does not inherit the generic Polymarket
filters where the approved Kalshi contract explicitly differs.
Finalized-market eligibility is resolution-censored at the pinned metadata
vintage, especially near the archive endpoint; this is not exchange completeness.

Opening is native `open_time`; duration ends at native `settlement_ts`, not
expiration, first/last trade or game end. Missing clocks affect duration, not an
otherwise eligible baseline. Core uncertainty is joint native-event-cluster
CR0, with namespaced market fallback, pointwise normal intervals and separately
reported `G/(G-1)` adjustment. Sports use provider-overlap full-game winners in
nine admitted series, not a full Kalshi game census. Sports clusters are
sport-namespaced provider games; retain the 500-print/30-game cell guards.
Pregame is `t<start`, live is `t>=start`, and admission retains `t<=end` with no
grace. ATP explicitly uses scheduled start plus archived duration. Other sports
retain their reviewed provider convention and literal source-quality label;
these labels are not guarantees of independently observed actual boundaries.

All real source/cell/cache/score work runs on canonical Linux
`/home/ubuntu/prediction_markets`, mounted `/mnt/data`, with
`/home/ubuntu/venv/bin/python`. The production runtime inspected for this run is
DuckDB **1.5.0**, also available in local `/opt/anaconda3/bin/python`; record the
complete actual environment in the run record rather than assuming another
DuckDB version is compatible. Local execution is limited to fixtures and
genuinely small saved-summary/report work. Never bypass `production_guard`.

Obtain the hash-bound admission plan and its original trade/metadata manifests,
native events/series/collections, and the configured read-only Dropbox remote.
The plan pins all 80 metadata shards and all 1,919 daily trade partitions of
this source vintage. The staged source helper verifies remote size and local
size/SHA before consumption, processes one shard at a time, and releases only
its exact run-owned temporary copy after callback success and durable receipt.
Failures and original datasets remain intact; no whole-archive copy or implicit
resume is part of this workflow.

## 1. Metadata, capacity probe and candidate trade inputs

From the canonical repository, using a new top-level `/mnt/data/runs/` directory
for each output:

```sh
/home/ubuntu/venv/bin/python scripts/build_kaushik_kalshi_inputs.py metadata \
  --expected-head 'REQUIRED_FROM_RUN_RECORD:METADATA_HEAD' \
  --admission-root 'REQUIRED_FROM_RUN_RECORD:ADMISSION_ROOT' \
  --admission-plan 'REQUIRED_FROM_RUN_RECORD:ADMISSION_PLAN' \
  --output-dir 'REQUIRED_FROM_RUN_RECORD:NEW_METADATA_DIR'

/home/ubuntu/venv/bin/python scripts/audit_kaushik_kalshi_inputs.py probe \
  --metadata-dir 'REQUIRED_FROM_RUN_RECORD:METADATA_DIR' \
  --metadata-manifest-sha256 'REQUIRED_FROM_RUN_RECORD:METADATA_CANDIDATE_SHA256' \
  --expected-builder-head 'REQUIRED_FROM_RUN_RECORD:METADATA_HEAD' \
  --expected-audit-head 'REQUIRED_FROM_RUN_RECORD:PROBE_HEAD' \
  --output-dir 'REQUIRED_FROM_RUN_RECORD:NEW_PROBE_DIR'

/home/ubuntu/venv/bin/python scripts/build_kaushik_kalshi_inputs.py trades \
  --expected-head 'REQUIRED_FROM_RUN_RECORD:TRADE_BUILDER_HEAD' \
  --admission-root 'REQUIRED_FROM_RUN_RECORD:ADMISSION_ROOT' \
  --admission-plan 'REQUIRED_FROM_RUN_RECORD:ADMISSION_PLAN' \
  --metadata-dir 'REQUIRED_FROM_RUN_RECORD:METADATA_DIR' \
  --metadata-manifest-sha256 'REQUIRED_FROM_RUN_RECORD:METADATA_CANDIDATE_SHA256' \
  --output-dir 'REQUIRED_FROM_RUN_RECORD:NEW_INPUT_DIR'
```

Metadata structural completion and `candidate_manifest.json` do not accept trade
inputs. The probe measures actual same-shaped saved bytes on eight pinned days;
any upstream-print-count extrapolation is provisional and composition-sensitive,
not a capacity guarantee or input acceptance. The trade build must finish all
daily receipts, global native-ID proof, lossless cells and diagnostic inventories
before its candidate is audited.

## 2. Independent input replay and separate publication

```sh
/home/ubuntu/venv/bin/python scripts/audit_kaushik_kalshi_inputs.py replay \
  --input-dir 'REQUIRED_FROM_RUN_RECORD:INPUT_DIR' \
  --candidate-sha256 'REQUIRED_FROM_RUN_RECORD:INPUT_CANDIDATE_SHA256' \
  --expected-builder-head 'REQUIRED_FROM_RUN_RECORD:TRADE_BUILDER_HEAD' \
  --expected-audit-head 'REQUIRED_FROM_RUN_RECORD:INPUT_AUDIT_HEAD' \
  --output-dir 'REQUIRED_FROM_RUN_RECORD:NEW_INPUT_AUDIT_DIR'

/home/ubuntu/venv/bin/python scripts/audit_kaushik_kalshi_inputs.py publish \
  --input-dir 'REQUIRED_FROM_RUN_RECORD:INPUT_DIR' \
  --candidate-sha256 'REQUIRED_FROM_RUN_RECORD:INPUT_CANDIDATE_SHA256' \
  --audit-dir 'REQUIRED_FROM_RUN_RECORD:INPUT_AUDIT_DIR' \
  --audit-sha256 'REQUIRED_FROM_RUN_RECORD:INPUT_AUDIT_CANDIDATE_SHA256' \
  --expected-audit-head 'REQUIRED_FROM_RUN_RECORD:INPUT_AUDIT_HEAD'
```

`replay` reopens all saved bodies and source/count proofs, then independently
replays 2021-06-30, 2025-03-12, 2025-03-13, 2025-04-01, 2026-04-01, 2026-06-01,
2026-09-01 and 2026-10-01, with at most 2 GB aggregate pinned source bytes.
It compares physical-row choices/exclusions and direct grouped moments.
All-history UUID uniqueness remains the reopened committed-builder exact proof,
not a second full-tape scan. Full saved sports-cache multiplicities and
winner-event/bin totals are checked; individual raw cache price/time identity
is replayed only on those eight days. `saved` accepts the same flags as `replay`
but withholds raw-dependent gates and cannot be promoted. `publish` reopens the
successful proof inventory, preserves the candidate, creates new `manifest.json`
and writes `acceptance.json` last. A failed or preexisting release is not replaced.

## 3. Native sports map, independent audit and map acceptance

The reviewed map config binds the native metadata manifest, native
events/milestones/entities, retained provider binding/index, MLB outcome body,
reviewed alias receipt and saved provider-cache root. No API lookup, nearest
date, ticker-suffix participant inference or outcome-based identity matching is
authorized by this step.

```sh
/home/ubuntu/venv/bin/python -m analysis.kaushik_kalshi_replication.sports_binding \
  --config 'REQUIRED_FROM_RUN_RECORD:MAP_CONFIG' \
  --config-sha256 'REQUIRED_FROM_RUN_RECORD:MAP_CONFIG_SHA256' \
  --expected-source-head 'REQUIRED_FROM_RUN_RECORD:MAP_PRODUCER_HEAD' \
  --output-dir 'REQUIRED_FROM_RUN_RECORD:NEW_MAP_DIR'

/home/ubuntu/venv/bin/python scripts/audit_kaushik_kalshi_sports.py map-audit \
  --manifest 'REQUIRED_FROM_RUN_RECORD:MAP_MANIFEST' \
  --manifest-sha256 'REQUIRED_FROM_RUN_RECORD:MAP_MANIFEST_SHA256' \
  --expected-producer-head 'REQUIRED_FROM_RUN_RECORD:MAP_PRODUCER_HEAD' \
  --expected-audit-head 'REQUIRED_FROM_RUN_RECORD:SPORTS_AUDIT_HEAD' \
  --output-dir 'REQUIRED_FROM_RUN_RECORD:NEW_MAP_AUDIT_DIR'

/home/ubuntu/venv/bin/python scripts/audit_kaushik_kalshi_sports.py map-publish \
  --audit-dir 'REQUIRED_FROM_RUN_RECORD:MAP_AUDIT_DIR' \
  --audit-sha256 'REQUIRED_FROM_RUN_RECORD:MAP_AUDIT_CANDIDATE_SHA256' \
  --expected-audit-head 'REQUIRED_FROM_RUN_RECORD:SPORTS_AUDIT_HEAD'
```

The producer leaves acceptance pending. Independent QA reopens named-YES native
field proofs, UUID/literal-or-reviewed-alias participant identities, exact
provider memberships, retained clocks and literal provider results. It checks
all indexed completed snapshots, including contradictory or ambiguous ones,
and retained incomplete evidence. Six-column exclusions preserve canonical
provider-failure JSON/SHA pairs where relevant; unrelated exclusions have both
fields null. This is complete disjoint inventory/proof reconciliation, not a
certificate that every theoretically possible match was found. Map publication
creates only a new `acceptance.json`; the pending map manifest remains unchanged.

## 4. Core estimates, saved-score QA and reader metadata

These are public Python APIs, not additional command-line entrypoints. Invoke
them through the committed orchestration/invocation recorded for the run.
Supply every path and SHA below from the run record; never construct acceptance
objects or use fixture relation APIs on production bodies.

```python
from analysis.kaushik_kalshi_replication.estimate import estimate_cells
estimate_cells(*, manifest_path, manifest_sha256, acceptance_path, acceptance_sha256,
               baseline_path, duration_path, claim_path, output_dir,
               expected_source_head, expected_input_source_head,
               resource_limits=ResourceLimits())

from analysis.kaushik_kalshi_replication.score_audit import audit_saved_estimates
audit_saved_estimates(*, estimates_path, estimate_manifest_path,
                      estimate_acceptance_path, estimates_sha256,
                      estimate_manifest_sha256, acceptance_sha256=None,
                      limits=AuditLimits())

from analysis.kaushik_kalshi_replication.summary import build_reader_metadata
build_reader_metadata(con, *, manifest_path, manifest_sha256, acceptance_path,
                      acceptance_sha256, expected_input_source_head)
```

These blocks show signatures, not executable Python calls. Pass an explicit
`acceptance_sha256` even where optional. The three core cell paths must match
the accepted input manifest's baseline/duration/claim artifacts. Core estimation
saves `estimates.json`, score bodies, manifest and a reopening acceptance; that
acceptance still leaves independent saved-score QA pending. The audit returns
a receipt without writing files. Its scope is saved projected-score covariance,
reported contrasts/intervals/influence, support/suppression and saved-summary
consistency; it does not independently refit coefficients, original-design rank,
absorption or original-Y R-squared. Save its successful return immutably with
the recorded invocation and bindings before report release. This read-only audit
API has no internal host guard: its production caller must enforce
`production_guard.require_production_host()` before real saved-body work.

The guarded reader returns `(reader_metadata, provenance)` without file writes.
The caller owns a bounded DuckDB connection/spill policy and immutable adapter
serialization. Native metadata paths come only from the accepted trade
manifest's binding. Preserve admitted-baseline and duration-tail execution
extrema separately from raw-archive extrema. Year/source coverage uses saved UTC
source partitions, not execution extrema; retain exact yearly counts, tail-only
duration exclusions and source-version coverage. Keep all reopened body bindings
from `provenance` with the saved adapter.

## 5. Fresh sports estimates and independent score QA

The guarded public API is:

```python
from analysis.kaushik_kalshi_replication.sports import estimate_saved
estimate_saved(*, input_manifest_path, input_manifest_sha256,
               input_acceptance_path, input_acceptance_sha256,
               map_manifest_path, map_manifest_sha256,
               map_acceptance_path, map_acceptance_sha256,
               core_manifest_path, core_manifest_sha256,
               core_acceptance_path, core_acceptance_sha256,
               output_dir, expected_source_head, limits=SportsLimits())
```

Use the run-record bindings for the accepted native inputs, independently
accepted map and accepted core stage. The fresh sports reopening acceptance is
not the independent sports-score audit. Run the latter separately:

```sh
/home/ubuntu/venv/bin/python scripts/audit_kaushik_kalshi_sports.py scores \
  --sports-dir 'REQUIRED_FROM_RUN_RECORD:SPORTS_DIR' \
  --sports-manifest-sha256 'REQUIRED_FROM_RUN_RECORD:SPORTS_MANIFEST_SHA256' \
  --sports-sha256 'REQUIRED_FROM_RUN_RECORD:SPORTS_ESTIMATES_SHA256' \
  --input-manifest 'REQUIRED_FROM_RUN_RECORD:INPUT_MANIFEST' \
  --input-acceptance 'REQUIRED_FROM_RUN_RECORD:INPUT_ACCEPTANCE' \
  --map-manifest 'REQUIRED_FROM_RUN_RECORD:MAP_MANIFEST' \
  --map-acceptance 'REQUIRED_FROM_RUN_RECORD:MAP_ACCEPTANCE' \
  --expected-producer-head 'REQUIRED_FROM_RUN_RECORD:SPORTS_ESTIMATOR_HEAD' \
  --expected-audit-head 'REQUIRED_FROM_RUN_RECORD:SPORTS_AUDIT_HEAD' \
  --output-dir 'REQUIRED_FROM_RUN_RECORD:NEW_SPORTS_SCORE_AUDIT_DIR'
```

QA replays every accepted saved sports-cache body against the accepted map,
reconstructs game cells/direct means and game-centered scores, then checks all
30 joints, CR0 covariance, contrasts, intervals, influence and full support grids.
Derived contrast arithmetic retains individual binary64 coefficient/score dots;
it does not substitute a fused multi-contrast projection near zero variance.
This saved-cache proof is not a second whole-history native-tape replay.

## 6. Render only accepted saved evidence

The report-only CLI binds saved JSON and canonical renderer-source identities;
it does not estimate or scan raw tape, cells or cached prints. Actual authoring requires
root authorization and a new absolute output directory:

```sh
/opt/anaconda3/bin/python scripts/render_kaushik_kalshi_report.py \
  --config 'REQUIRED_FROM_RUN_RECORD:REPORT_CONFIG' \
  --config-sha256 'REQUIRED_FROM_RUN_RECORD:REPORT_CONFIG_SHA256' \
  --output-dir 'REQUIRED_FROM_RUN_RECORD:NEW_REPORT_DIR'
```

Use the recorded Python/runtime if rendering elsewhere; this command illustrates
the local saved-summary rendering boundary, not permission for local data work.
The reviewed JSON config has exactly four top-level keys: `schema_version`
(`kaushik_kalshi_report_config_v1`), `status` (`reviewed`), `canonical_sources`
and `files`. `canonical_sources` has exactly `head` and `files`: the head is the
recorded 40-character canonical renderer commit; its file mapping contains
exactly these eight repository-relative paths, each bound by `{bytes, sha256}`:

```text
scripts/render_kaushik_kalshi_report.py
analysis/kaushik_kalshi_replication/__init__.py
analysis/kaushik_kalshi_replication/report.py
analysis/kaushik_kalshi_replication/inputs.py
analysis/kaushik_polymarket_replication/__init__.py
analysis/kaushik_polymarket_replication/render_report.py
analysis/kaushik_polymarket_replication/exchange_filter_contract.py
analysis/kaushik_polymarket_replication/build_inputs.py
```

Canonical `analysis` is a namespace package: `analysis/__init__.py` must be
absent, imported `analysis.__file__` must be `None`, and `analysis.__path__`
must contain only the absolute `REPO/analysis` directory. Regular package
markers, extra namespace roots or misdirected origins are refused. The two
listed child-package initializers remain real bound files; do not invent
top-level package markers to make imports pass. A local rendering checkout
must satisfy the same namespace-origin contract.

Top-level `files` contains exactly the following ten roles, each bound by
`{path, bytes, sha256}` with a distinct absolute regular JSON path, positive
integer byte count and SHA-256. Every final value is
`REQUIRED_FROM_RUN_RECORD`; do not replace recorded identities with unverified
current/local files.

```text
core_estimates       core_manifest       core_acceptance       core_score_audit
reader_metadata      reader_provenance
sports_estimates     sports_manifest     sports_acceptance     sports_score_audit
```

The reader metadata/provenance pair is separately hash-bound by this config;
the provenance retains schema `kaushik_kalshi_reader_metadata_provenance_v1`.
Its accepted-input manifest/acceptance hashes must equal both the core and
sports accepted-input bindings. Core, sports, reopening acceptances and their
independent score-audit hashes must agree throughout. The CLI has no draft or
non-sports-scope escape. Local source bytes must match the supplied canonical
identities and imported origins; that check does not establish local Git
equivalence or reconstruct unshared canonical history.

For validation without authoring, the existing API is
`prepare_render(*, config_path, config_sha256)` in
`scripts.render_kaushik_kalshi_report`; it returns validated source/specs without
file writes. `render(*, config_path, config_sha256, output_dir)` is the authoring
API used by the CLI. Config is bounded to 1 MB, individual saved JSON to 16 MB,
all ten JSON bodies to 128 MB and total authored outputs to 100 MB. Inputs and
source files are rechecked; publication cannot replace an existing output.

The fixed primary output is `kalshi_flb_replication.tex`, with exactly 21 vector
PDF assets: `figures/baseline_profiles.pdf` plus
`figures/sports_<scope>_{profiles,windows}.pdf` for pooled and the nine sports.
`render_manifest.json` records status `rendered_pending_compile_and_page_qa`,
not a compiled or page-reviewed release. These are output contracts, not a claim
that actual artifacts have been written. Compilation and every-page visual QA
remain a separate root-authorized stage; exact commands and final bundle
bindings are `REQUIRED_FROM_RUN_RECORD`. Open final source/preview in Codex.

## Resources, retained inputs and local verification

Default admission limits below are hard ceilings/floors, not permission to
increase them. GB values are decimal bytes where encoded numerically.

| Stage | Memory / threads | Output / spill / free-space admission |
| --- | --- | --- |
| Metadata/trade build; core estimation | DuckDB 192 GB, 4 threads; 240 GB available RAM (core NumPy up to 32 GB) | 12 GB output + 4 GB transient + 4 GB spill + 20 GB floor = 40 GB initial free |
| Input probe/replay audit | DuckDB 192 GB, 4 threads; 240 GB available RAM | 8 GB audit output + 2 GB sampled source + 4 GB spill + 20 GB floor = 34 GB initial free |
| Sports map producer | DuckDB 2 GB, 1 thread | 1 GB output, no spill; 22 GB initial free |
| Sports estimator | DuckDB 16 GB, 2 threads; 32 GB available RAM; 8 GB NumPy | 4 GB output + 1 GB transient + 4 GB spill + 20 GB floor = 29 GB initial free |
| Sports audit | DuckDB 16 GB, 2 threads; 32 GB available RAM; 8 GB NumPy | 1 GB output + 1 GB transient + 4 GB spill + 20 GB floor = 26 GB initial free; 12 GB cache and 128 GB planned source-read ceilings |

Sports audits use fixed `disabled_optimizers='join_order,build_side_probe_side'`
to preserve the explicit candidate-left/map-right join build policy, matching
the producer without changing SQL, eligibility or statistical definitions.

Core saved-score QA separately bounds JSON to 16 MB, each score artifact to
3 GB, total scores to 12 GB and batches to 64 MB. Read and honor each recorded
stage's actual manifest/limits; a passing capacity probe does not relax them.
Serialize stages sharing source/cache/memory. Root alone manages EC2/storage;
after success, failure or interruption it must confirm no process/transfer/agent
still needs the instance, finish required data handling, stop it and verify the
stopped state. Follow [EC2_SETUP.md](../EC2_SETUP.md), not lifecycle commands
invented in this guide.

A report/share bundle is not a rebuildable raw-data archive. Unless its explicit
inventory includes them, reproduction also needs original pinned native trade
and metadata shards, all admission/reference bodies, retained provider metadata
and scoreboard cache, alias/config receipts, full accepted input cell/daily
cache inventories, and saved score bodies/audits. Source access and credentials
are separate; never include credentials or private keys in a bundle.

Safe local fixture checks, using no real data or network:

```sh
/opt/anaconda3/bin/python -m unittest tests.test_kaushik_kalshi_input_audit tests.test_kaushik_kalshi_sports_audit
/opt/anaconda3/bin/python -m unittest discover -s tests -p 'test_kaushik_kalshi*.py'
```

CLI syntax can be checked locally with `--help` on the four script entrypoints
above and the `sports_binding` module. Record exact canonical-runtime test
results for the committed production revision separately; local fixture success
does not establish source completeness, full-run capacity or empirical findings.
