# Exchange-filtered full-date Kaushik Polymarket rerun

Status: production inputs, complete estimates, both independent release audits
and report QA accepted. Finished-package publication and root-owned shutdown
are separate final handling steps, not claimed complete in this commit.
Date: 14 October 2026, America/New_York. Execution timestamps use UTC.

## Authorization and scope

The user requested a rerun after being offered a versioned read-only filtered
view over accepted original Parquets and wallet masks. This approves the scoped
logical-input architecture and a full-date refit. It does not authorize changing
the original data, science, numerical estimators, filters beyond the frozen
exchange-wallet rule, resource reservations, or deleting other datasets.

The research contract is
[`kaushik_exchange_wallet_exclusion_v1.md`](../analysis_specs/kaushik_exchange_wallet_exclusion_v1.md),
with unchanged model grids in the existing replication and full-sample specs.
Both original accepted studies remain immutable. The cutoff population has zero
eligible removals and is not refitted.

## Accepted dependencies

- Original full-date input generation:
  `/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2`.
  Manifest SHA256:
  `72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c`.
  Acceptance SHA256:
  `e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519`.
- Accepted count and locator-mask audit:
  `/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_v4`.
  Audit SHA256:
  `3001812c6534bcf68cbe2597f487a86bd1be1546ce72cd31898f7f11c31ee1fb`.
  Acceptance SHA256:
  `e9a793a8bd5a90087c90aa51cb4e1a88ab905da99736bdc3550f13954eeec183`.
- Original full-date estimates for descriptive comparison:
  `/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v3`.
- Provider inputs remain the original full-date sports binding; a fresh
  metadata stage and reviewed binding will reference the logical input.

## Accepted reconciliation

| Primary taker population | Original | Eligible exchange removed | Retained |
| --- | ---: | ---: | ---: |
| Baseline | 1,000,970,616 | 110,520,928 | 890,449,688 |
| Fixed D1/D10 tails | 289,367,763 | 29,752,572 | 259,615,191 |
| Common duration tails | 252,319,669 | 25,510,437 | 226,809,232 |
| Lifespan above one day | 172,191,421 | 12,909,064 | 159,282,357 |
| Remaining duration above one day | 93,963,589 | 5,273,924 | 88,689,665 |

Original all-role eligible records are 2,001,941,232; retained all-role records
are 1,780,899,376. Source records remain 2,036,128,538. Original exclusions are
34,187,306; the effective waterfall adds only 221,041,856 previously eligible
records. Already-ineligible wallet matches are not counted twice.

## Work allocation and lifecycle

One agent owns the logical-input producer and estimate-driver integration.
A second owns report and saved-score consumer admission. A third independently
reviews source and small fixtures. The root freezes contracts, integrates and
reviews changes, manages canonical commits, production admission, artifacts,
and lifecycle. Agents may not manage the instance or storage.

The instance was verified stopped before implementation and started only after
reviewed source and focused tests were ready. Every production stage is
serial and immutable, uses committed source, fixed capacity checks, detached
execution with an exclusive saved log, and exact input/output reopening.
After all required jobs and transfers finish, the root must confirm no process
or agent needs the instance, stop it, and verify the stopped state. Completion,
failure and interruption all require this lifecycle check.

## Release gates

1. Fixture equivalence of logical versus physically filtered records; strict
   mask keys, policy, immutable lineage and backward-compatible legacy checks.
2. Fresh admitted logical inputs with effective support and exact waterfall.
3. Fresh provider metadata binding and full original estimate grid.
4. Independent saved-score, covariance, contrast, support and numerical QA.
5. Portable data-first LaTeX report, unchanged full table/figure grid, concise
   descriptive original-versus-filtered comparison, and every-page visual QA.
6. Reviewed canonical source push, verified finished Dropbox package, and
   recorded stopped-state receipt.

The original pre-execution gates above were all met. Accepted execution and
result evidence are recorded below.

## Local implementation and independent review

The implementation author completed the logical producer and driver adaptation;
the consumer author completed v2 report/saved-score admission and the optional
descriptive accepted-run comparison. Independent review found no remaining
source-level blocker. The original input builder and numerical estimator are
byte-identical. A lazy-versus-physical fixture reproduces the full model grid,
covariance, support and withholding; malformed keys, flags and lineage block.

Recorded local validation: relevant integration 200 tests in 38.251 seconds;
consumer modules 86 tests in 25.987 seconds; independent release fixtures 10
tests in 0.703 seconds. Root subsequently ran both new input/independent modules,
16 tests in 3.748 seconds. These suites overlap and must not be added together.
The release-audit peer review independently checked actual tiny driver summaries:
all twelve models and five Table1/A2 profile families reconcile. None of these
fixture checks reads real data or replaces fresh production acceptance.

The new guarded `scripts/audit_kaushik_exchange_filtered_release.py` independently
derives fixed bins from price and calculates raw record-weighted C/Q means,
signed tail gaps and duration counts in one grouped pass. It binds accepted
filtered inputs, estimates and saved-score QA, verifies numerical diagnostics
and withholding, then reopens content before immutable publication. It does not
refit coefficients or independently reconstruct sports timing/window membership.

## Canonical integration and start

Root requested instance start at `2026-10-15T00:59:31Z` after the stable
implementation passed local tests. The actual instance type is `r5.8xlarge`.
The canonical branch was clean at
`f201a4806e4adb4cf826a1173560b9a057b100b2` before the scoped transfer. `rg` is not
installed on this host; bounded standard-tool fallbacks are used there.

The data volume was initially unmounted. Root verified data UUID
`d0cd087b-94c4-428c-bae9-ae28929059f6` on `/dev/nvme1n1`, separate from the root
disk, then mounted only that exact UUID at `/mnt/data`. Observed data free space
was 62,008,090,624 bytes and available RAM was 264,970,674,176 bytes before
integration. No research, transfer or notebook process was active. These
observations do not substitute for each stage's fresh resource preflight.

All fifteen scoped source/doc files matched the local reviewed bytes after
transfer; canonical whitespace checks passed. The complete twelve-module
fixture suite ran with `/home/ubuntu/venv/bin/python -B -m unittest` and passed
219 tests in 58.697 seconds, exit zero. The exclusive log is
`/mnt/data/runs/2026-10-14_kaushik_exchange_filtered_controls_v1/runtime_fixture_v1.log`.
This is synthetic-fixture validation on the production runtime, not execution
of the real-data pipeline. Root-owned lifecycle and all production gates remain
required.

## Accepted production execution

The scientific source was committed and pushed on
`codex/kaushik-exchange-contract-exclusion` at
`b1c9816cf69ae6d2609ed5a8effc4865c67ff9ee` before real-data execution. The
original `build_inputs.py` and numerical `estimators.py` remain byte-identical.
Subsequent documentation-only release commits do not replace this producer
watermark. Every long stage used the saved detached execution envelope and an
explicitly reviewed request; no source, science, tolerance or resource ceiling
changed while a body was running.

All following directories are under `/mnt/data/runs/`:

| Stage | Accepted directory | Manifest/audit SHA256 | Acceptance SHA256 |
| --- | --- | --- | --- |
| Logical inputs | `2026-10-14_kaushik_exchange_filtered_inputs_v1` | `61e99d04169b34e1872abb88e78ab30033a0f2cc2559f9ce061bafe7e53f4465` | `353ec7bb234f3b9347b7d34fe70bfe016ceeb32bafb05371e99b4611a5cedc46` |
| Sports metadata | `2026-10-14_kaushik_exchange_filtered_sports_metadata_v1` | `40a43954cc15f40afcdf82692fd91e6d58c1ef98753e29d9a13ca9934d61c315` | `6a63a6f4150086ccec45ded0ec80d4706b6fd33361d69510887264ca9cbd882b` |
| Estimates | `2026-10-14_kaushik_exchange_filtered_estimates_v1` | `cd6397a1f906e89ea070ade24b4ff8491014bd58c71f552c3de658848cb685d7` | `32012aaf016cfb61fa3ebccfa05c904cabe4d41491b8782e60cfe9230021abb6` |
| Saved-score audit | `2026-10-14_kaushik_exchange_filtered_saved_score_audit_v1` | `38cb5786ca0f99a83bb31e3570a4d0acbaa3a80aab8659eacbc9128f310af149` | Audit itself is the receipt |
| Raw release audit | `2026-10-14_kaushik_exchange_filtered_release_audit_v1` | `0ed3dd7519af0c3ee9bb431583a9650145343142708c5ca4ff83c350578c410c` | Audit itself is the receipt |

The estimate-summary SHA256 is
`a90870bbfd1091cbbf241a2580cc94807380592d58a49b3f0b7df5eb1c900caa`.
All five estimate reconciliation gates and output reopening passed. All twelve
regression models completed without numerical withholding. All 47 score files
and the complete profile/window/support grids are saved, including suppressed
cells with null estimates rather than fabricated zeros.

Execution evidence:

- Logical input body: exit zero, 1,119.761 seconds, completed
  `2026-10-15T01:40:43.652775Z`; all five input gates passed. The layer has no
  copied base/claim outputs and references 133 immutable source files/masks.
- Sports metadata body: exit zero, 4.838 seconds. The original 13,909 games,
  14,383 markets, nine sports, provider hashes and coverage are unchanged.
- Estimate body: `2026-10-15T01:54:33.453337Z` to
  `2026-10-15T03:30:17.410960Z`, exit zero, 5,743.957 seconds. Actual charged
  reads were 593,532,391,811 bytes, below the reviewed 6,962,865,559,932-byte
  plan and unchanged 8 TB ceiling. Accepted Parquet outputs total
  1,728,634,683 bytes; the score subset is 588,915,988 bytes.
- Saved-score audit: `2026-10-15T05:16:10.286030Z` to
  `2026-10-15T05:16:37.942941Z`, exit zero, 27.657 seconds; all seven checks
  passed. Its 1,780,613,792 charged read bytes are below 25 GB. It independently
  reconciles CR0 covariance, contrasts/SEs/intervals, influence, support and
  the supplementary covariance factor from all 47 saved score files.
- Raw release body: completed `2026-10-15T06:26:38.153918Z`, exit zero,
  270.781 seconds, all seven checks passed. Charged reads were 79,795,673,442
  bytes against the reviewed 179,559,241,430-byte plan and 1 TB ceiling. It
  independently calculates Table 1/A2 bin means and gaps and duration-panel
  counts; verifies all twelve numerical models and full-grid withholding;
  binds saved-score acceptance; and reopens physical inputs unchanged.

The final raw audit is not another coefficient refit or an independent raw
provider-timing reconstruction. Neither audit certifies native-action or
unique-fill completeness. All execution requests reopened unchanged and every
stage retains its original versioned evidence.

## Report and interpretation checks

The local report and accepted summaries are under
`output/kaushik_exchange_filtered_2026-10-14_v1/`. The primary artifact is
`report_v1/source.tex`, generated only from accepted new and original JSON
summaries with the committed renderer exported at the scientific source HEAD.
It includes 21 vector figures and 38 native tables, including the original full
grid and two descriptive original-versus-excluded comparisons. The portable
source uses relative figures and contains no reader-facing fingerprint appendix
or generic closing interpretation page.

The report compiled twice in an isolated temporary directory with pdfTeX
1.40.27 / TeX Live 2025. All 32 pages were rendered at 150 dpi and independently
visually checked, pages 1-16 and 17-32 by separate agents. Root reviewed selected
pages and checked page-4 footer separation. No clipping, overlap, overfull box,
missing asset, undefined reference or remaining rerun warning was found. Two
underfull paragraph messages on page 7 are cosmetic; the text is contained and
readable. All figure PDFs were checked for vector content and every source/asset
hash reconciled. Final compilation, page checks, numerical-audit bindings and
the complete 24-file report inventory are in `report_v1_qa.json`.

The committed comparison reports the payoff D10-minus-D1 gap moving from
0.508864 to 0.615428 cents and the average individual-return gap from 24.243773
to 29.796090 percentage points. The change is driven by a more negative D1,
not stronger D10 returns. No standard error or test of the old-new difference
was computed. All four full-model duration-gap slope intervals include zero;
the influence qualifications remain. A separate read-only reviewer rebuilt
these contrasts and intervals from saved joint covariance, with maximum
discrepancy 7.11e-15.

The science is unchanged apart from the four-address either-source-wallet
exclusion. In particular: equal primary taker-record weights, complemented
SELL positions, signed C/Q outcomes, fixed probability bins, native categories,
exact recorded normalized price effects and UTC month effects; no added bot,
price-cent, calendar-cutoff, lifecycle or matched-Kalshi filter. Duration uses
`created_at`/`end_date` proxies and `log2(1+days)` clocks, not normalized game
time. Source executions end 21 June 2026 with the original July resolution
vintage and its censoring qualification. All eligible exchange removals are
in April-June 2026; the March 25 cutoff study is unchanged and not refitted.

## Publication and lifecycle handoff

The scientific source capsule contains 35 exact tracked files exported from
the scientific commit, with no invented namespace markers. Its source manifest
SHA256 is `6f2233566835efd65232b06895dedbc4f7d62d06be039ca2c2e591604b9027fe`.
The finished bundle requires separate root admission, a fresh isolated
219-test candidate run, summary/import/credential/inventory checks and atomic
non-replacing publication before Dropbox copying. Raw trades, provider/cache/
score/mask bodies, credentials and scratch files are not sharing payloads.
The finished package contains portable report source/assets, accepted summary
evidence, frozen code/fixtures and reproduction documentation; repeating real
data estimation still requires the private canonical inputs.

The serial production operator released its instance dependency after a
`2026-10-15T06:27:59Z` check found no production or transfer processes. Root
still requires the instance for reviewed documentation integration and the
verified Dropbox transfer. At this documentation commit these final handling
steps and shutdown remain pending. The local task/release receipts record
their actual completion; do not interpret this as a stopped-state receipt.
