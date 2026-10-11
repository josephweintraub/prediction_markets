# Reproducing the Kaushik Polymarket replication

Status: bundle guide, 10 October 2026. Corrected v5 estimation and independent
saved-score auditing, release reconciliation and final report review have passed.
This document contains no findings and grants no production or
instance-lifecycle authorization.

The [frozen research contract](kaushik_polymarket_replication_v1.md) defines the
analysis; [methods_reference.md](../methods_reference.md) defines the data canon.
The replication deliberately overrides the project's default filters, weights
and clustering. Do not substitute a different calibration engine or source.

## Execution and inputs

Production builds, estimation and saved-score auditing require Linux, the canonical
`/home/ubuntu/prediction_markets` checkout, mounted `/mnt/data` and
`/home/ubuntu/venv/bin/python`. The entrypoints enforce
`production_guard.require_production_host()`; do not bypass it to run real data on
a Mac. Follow [EC2_SETUP.md](../EC2_SETUP.md) for operational procedures; root alone
owns instance lifecycle. Local work is limited to source review, fixture tests,
bounded summaries and report-only rendering.

The accepted input stage is
`/mnt/data/runs/2026-10-10_kaushik_polymarket_inputs_v2`. It uses repaired CLEAN
monthly archives under
`/mnt/data/runs/2026-10-09_polymarket_wallet_attribution_repair_v1/clean/`, not the
legacy shared CLEAN path. The input manifest binds every monthly file, the repair
manifest and these native metadata inputs:

- `/mnt/data/pipeline_data/token_map.parquet`;
- `/mnt/data/pipeline_output/market_flags.parquet` (token/outcome spine);
- `/mnt/data/learnability/native/native_market_meta.parquet`;
- `/mnt/data/learnability/native/market_native_categories.parquet`.

EC2 archive `conditionId` is a token identifier, not a market condition key. Binary
complements, outcomes, event mapping and categories must pass the input gates.
Repaired identity preserves archive economic fields and multiplicity; it does not
certify native own-order action, unique executions or whole-history collection.
Approximate execution clocks and resolved-build censoring remain qualifications.

Provider artifacts are enumerated and fingerprinted in
`/mnt/data/runs/2026-10-10_kaushik_polymarket_controls_v1/sports_binding_reviewed_v5.json`.
Only the audited available nine-sport provider-covered winner cohort enters,
including EPL draw claims; no Kalshi match or additional date bound is imposed.
Eight sports use accepted live-play boundaries; ATP uses scheduled start plus
archive duration. No new API collection is required. Do not reuse legacy sports
observation bodies.

## Frozen estimands and models

The primary unit is an archive-recorded taker-role row: recorded BUY retains its
claim; recorded SELL uses its verified binary complement, including `P=1-p` and
`Y=1-y`. All observations receive weight one. Execution must precede
`2026-03-25T00:00:00Z`, with finite `0<P<1`, positive size and eventual binary
payout. No bot, up/down, lifecycle or extreme-price filter enters the baseline.
Payoff is `100*(Y-P)` cents per $1 claim. Individual return is
`100*(Y/P-1)` percent, computed before averaging; return gaps are percentage
points. Fees and annualization are absent. D1--D10 are fixed ten-cent bins, not
sample quantiles; tail gaps are signed D10 minus D1.

Duration uses native `created_at` opening fallback and `end_date` maturity proxy:
`L=(end-opening)/86400`, `R=(end-trade)/86400`, `xL=log2(1+L)`,
`xR=log2(1+R)`. All five Table 2 models share the duration-eligible tail sample:
original raw; original plus category FE; remaining raw; remaining plus category
FE; both clocks plus category, exact price and UTC month FE. Effects vary by tail.
Categories are the existing 12 native labels plus `Unclassified`; exact-price FE
use unrounded normalized binary64 `P`, not one-cent bins. Table 3 has separate
`L>1` and `R>1` three-model panels. Original lifespan is constant within claim,
not game progress; larger remaining time means farther from the endpoint proxy.
A1 claim FE are a price-path diagnostic: within-claim payoff is mechanically
minus price. A2 compares archive taker direction with all, maker and taker BUY
under the same baseline rules; it does not recover native own-order actions.

Primary uncertainty is the joint event/market-cluster raw sandwich CR0, or game
clusters for sports. The supplementary `G/(G-1)` adjustment is not full CR1.
Retain cross-tail/phase covariance, separate-tail and labeled stacked R-squared,
influence concentration flags, original sample N/G and numerical withholding
reasons. Sports require 30 games and an additional project 500-record minimum in
each required cell; unsupported estimates are null, never zero.

Corrected v5 sports caches store `sport_elapsed_seconds` and
`sport_remaining_seconds` as distinct doubles, not legacy `u/r/R` fields. Saved
`sport_clock_columns` must map elapsed/remaining seconds to those exact names,
and integer `sport_clock_mismatch_rows` must equal zero. This protects against
the native uppercase `R`-days / lowercase `r`-seconds collision that invalidated
v4 final-hour panels. Neither v4 estimates nor its preserved `report_v1` may enter
the finished bundle.

## Entry points and current bindings

- `scripts/build_kaushik_polymarket_replication.py`: separately reviewed
  input preflight, then immutable input body/reopened acceptance.
- `scripts/estimate_kaushik_polymarket_replication.py`: separately reviewed
  estimator preflight, accepted provider metadata (`--sports-metadata-only`),
  then estimation and reopened acceptance. `run_estimates.py` implements the
  frozen models; `estimators.py` implements the statistical calculations.
- `scripts/audit_kaushik_replication_saved_scores.py`: independent bounded
  saved-score audit, not another coefficient fit or raw-trade membership audit.
- `analysis/kaushik_polymarket_replication/render_report.py`: accepted-summary-only
  rendering; it performs no estimation or trade/cache/score-body reads.

Recorded producer source H9 is `df33a370ebb88ee15389b5770c1869d6b7abd677`.
The accepted input build retains its own earlier committed source
`41f44b25dcf945d06c5ade3aaf5b766da6a8b41f`; the v5 preflight explicitly admits
that immutable input binding. Source snapshots and all per-file fingerprints
belong in the supplied manifests, not the results manuscript.

Verified SHA256 bindings:

| Artifact | SHA256 |
| --- | --- |
| Input contract v2 | `79d63387bcfe29322a018d80844594d43a10285c955b64ba0d908c0dd1a8a101` |
| Input reviewed preflight v2 | `a585c2631b5d4afc3cd624f6ecd23fdab4a9eaaf8bc0f9dd71f5119d8427b053` |
| Input manifest v2 | `a005f59a4b2da3f6eb34d97c912c5ff08aff842d3dcbac109e11c92bf43e1cc9` |
| Input acceptance v2 | `426ac4ba9b68b77e8b101f129a82009bc5f92a8f9f1179b3c09cd5c70589841a` |
| Sports metadata v5 manifest | `1bdecb150f8eb2a1c3cb6b3efc9347bcd3a28aa77de47bb259fdf0b095922678` |
| Sports metadata v5 acceptance | `af1895dee8e726e898bb28291b9727a133519373ad3652e085039667c864bd61` |
| Reviewed sports binding v5 | `68620226d998590f637be7e545f76cd197c3c53893c4e7de28931e30a73e5735` |
| Estimator reviewed preflight v5 | `cd55c0cd3686ec2ee08da8349723f68ddb41bf808a57c6b7eeb753de9af70c23` |
| Retained execution control v5 | `9fd97af4492022d5073874e12864570ebdc39321a404c8e9e626bfc5d340868e` |
| Estimate v5 manifest | `600866e9f5a60bee4f9441917d789dfe271318acd3c819f725eaa6ce06fd6f50` |
| Estimate v5 acceptance | `59d4a0457c0f991f115fc4e6dbc2ef8387e4b89944b691bf7334f3dc1a7bc6b5` |
| Estimate v5 JSON | `fcd46b268400f429ad39348273c7c54d906674a5e6914f93704b77567cdbf269` |
| Independent saved-score audit | `539dcef4ea6016f79c5aba52323366fd225982cb720e4e437873b3728aabdd81` |

The local control copies are under
`output/kaushik_polymarket_replication_2026-10-10_v1/`; their filenames are
`execute_estimates_v5.py`, `estimate_preflight_v5.json`,
`sports_binding_reviewed_v5.json`, and `sports_metadata_v5/{manifest,acceptance}.json`.
The canonical controls directory is
`/mnt/data/runs/2026-10-10_kaushik_polymarket_controls_v1/`.
The v5 reviewed preflight binds four threads, DuckDB `192GB`, 32,000,000,000
NumPy bytes, 16,000,000,000 spill bytes and 8,000,000,000 published-output bytes,
plus separate read/cache/RAM/disk ceilings. Do not silently raise limits or change
source under a reviewed preflight.

Recorded v5 body command, from the reviewed execution control:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_inputs_v2 \
  --base-manifest-sha256 a005f59a4b2da3f6eb34d97c912c5ff08aff842d3dcbac109e11c92bf43e1cc9 \
  --base-acceptance-sha256 426ac4ba9b68b77e8b101f129a82009bc5f92a8f9f1179b3c09cd5c70589841a \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_controls_v1/sports_binding_reviewed_v5.json \
  --sports-binding-sha256 68620226d998590f637be7e545f76cd197c3c53893c4e7de28931e30a73e5735 \
  --expected-head df33a370ebb88ee15389b5770c1869d6b7abd677 \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_estimates_v5 \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_estimate_preflight_v5/manifest.json \
  --reviewed-preflight-sha256 cd55c0cd3686ec2ee08da8349723f68ddb41bf808a57c6b7eeb753de9af70c23
```

This records provenance, not permission to start another producer. Existing stage
directories are immutable and cannot be overwritten. A new reproduction requires
new targets and fresh separately reviewed preflights; retain the prior stages,
failures and execution receipts. The accepted sports metadata stage is
`/mnt/data/runs/2026-10-10_kaushik_polymarket_sports_metadata_v5`.

## Audit, report and package gates

The saved-score auditor reopens accepted JSON and bounded score Parquets,
reconstructs CR0 covariance, named-contrast SE/CI and influence diagnostics, and
reconciles saved support, complete grids, duration samples, BUY partitions and
corrected sports-clock metadata/footer descriptions. It does **not** independently
reconstruct raw timing/window membership or fit coefficients from raw trades.
Require `saved_scores_reconciled` and inspect those limits before release.

The following audit command records the completed immutable run. A reproduction
requires a new run directory. `--expected-head` is the auditor's own committed
source, not automatically the producer HEAD; both happen to be H9 here.

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python scripts/audit_kaushik_replication_saved_scores.py \
  --estimate-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_estimates_v5 \
  --manifest-sha256 600866e9f5a60bee4f9441917d789dfe271318acd3c819f725eaa6ce06fd6f50 \
  --acceptance-sha256 59d4a0457c0f991f115fc4e6dbc2ef8387e4b89944b691bf7334f3dc1a7bc6b5 \
  --expected-head df33a370ebb88ee15389b5770c1869d6b7abd677 \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_saved_score_audit_v5
```

Report-only rendering runs from the local checkout after copying the three
accepted small JSON files without changing their bytes:

```sh
cd /Users/josephweintraub/prediction_markets
python3 -m analysis.kaushik_polymarket_replication.render_report \
  --estimate-dir /Users/josephweintraub/prediction_markets/output/kaushik_polymarket_replication_2026-10-10_v1/accepted_estimates_v5 \
  --run-dir /Users/josephweintraub/prediction_markets/output/kaushik_polymarket_replication_2026-10-10_v1/report_v2
```

Rendering requires `estimates.json`, `manifest.json` and `acceptance.json`, complete
reopened statuses/gates, exact hash/source bindings, complete expected grids and
the corrected sports-clock proof. Independent audit and numerical/layout review
are additional release gates, not implied by a renderer success. It publishes
new `source.tex`, 21 vector PDFs in relative `figures/` paths, and a report manifest;
it refuses existing targets and rechecks input bindings before atomic publication.
Source is portable with those assets; no network or remote fonts are needed.

For the companion preview, use a clean `build/` directory inside the accepted
report folder and compile the multi-file project twice:

```sh
pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build source.tex
pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build source.tex
pdftoppm -r 85 -png build/source.pdf build/page
```

Create that new build directory first; never reuse an invalid draft build. The
current native Codex compiler supports standalone single-file documents, so the
external vector-asset project needs this companion compilation. Reconcile every
displayed value/support/withholding/influence mark to accepted saved outputs,
review substantive compile warnings, and inspect **every page** for clipping,
legibility, captions, blank pages and panel coverage. Fixed bins/windows are
isolated points with capped saved CR0 intervals and zero references; suppressed
values are not plotted. Primary deliverable is `.tex` plus vector assets; PDF is
a verified preview, not a replacement for source.

Final AUDIT: `saved_score_audit_v5/audit.json` in the shared bundle, sourced from
`/mnt/data/runs/2026-10-10_kaushik_polymarket_saved_score_audit_v5/audit.json`;
verified SHA256 appears above. Final REPORT is `report_v2/` in the shared bundle:
`source.tex` (SHA256 `246700771196f9091beca6db26bcf68bf0ed50a05038336ff6b276b5ed128599`),
the 21 relative `figures/` vector assets, `manifest.json`, and the verified
`build/source.pdf` preview (SHA256 `4a6b00f6e817016c293dff62e3721a2f458b4b38b9a6e8063ffee3e58f9c7bb2`).
`report_v2_qa.json` binds every page and displayed value/support/flag check;
SHA256 `677e51398c53e92b7b30fb26676fecdb73da9b40c69b4a413428477cd45adfe0`.
All 31 pages were inspected, with no clipping or overfull boxes; two cosmetic
underfull provider-note lines remain. Root accepted the non-final-hour v4-to-v5
comparison and saved-score gates before release. Invalid `report_v1` remains excluded.

The finished shared bundle should contain this guide, the frozen specification,
required committed source/tests or their retrievable revision, reviewed controls,
accepted small estimate JSON/manifest/receipt, independent `audit.json`, portable
report source/vector assets, report manifest and verified PDF preview. Exclude
raw trades, analytical/cache bodies, score Parquet bodies, credentials/private
keys, spill files, failed stages, invalid v4 reports and scratch page images.
Manifests retain external input paths and fingerprints without copying those
large bodies into the shared bundle. Full computational reproduction therefore
requires access to the admitted EC2 data, not merely the presentation bundle.
