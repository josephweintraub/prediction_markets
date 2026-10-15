# Exchange-filtered full-date Kaushik Polymarket rerun

Status: implementation, independent review and production-runtime fixture QA
passed; canonical source commit and fresh production admission pending.
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

## Expected reconciliation

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

The instance was verified stopped before implementation. It will remain stopped
until reviewed source and focused tests are ready. Every production stage is
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

No new coefficient result has yet been computed or accepted.

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
