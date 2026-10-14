# Exchange-wallet audit, 14 October 2026

Status: fresh v2 source/fixture admission pending. Neither an accepted
sample-wide count result nor revised coefficient estimates are claimed here.

The [scientific specification](../analysis_specs/kaushik_exchange_wallet_exclusion_v1.md)
defines the frozen four-address either-wallet rule against accepted full-date
inputs and the existing cutoff subset. No bot filter, source refresh, clock,
price, outcome, role convention, weighting or model change is introduced.

## Preserved initial execution

Root verified the stopped `i-0f5b31a268af53938`, started it at approximately
2026-10-14 10:25:41 UTC and mounted the confirmed data UUID
`d0cd087b-94c4-428c-bae9-ae28929059f6`. The measured instance type was
`r5.8xlarge`; no instance/storage resize or deletion was performed.

Initial canonical source HEAD:
`ab8ba1080f89b3a445a36b990c3fc3433e625f90`.
Both independent local QA (165 tests, 29.613 seconds) and production-runtime
fixture QA (165 tests, 40.169 seconds) passed before metadata admission.

V1 reviewed metadata preflight:
`/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_preflight_v1/manifest.json`,
SHA256 `2e025166367df185009e947fcd3dc439f425f37b782353245b5756f01475f8e5`.
It bound all 44 months, seven tested source dependencies, accepted reference
manifest/receipt, original repair/native metadata generation and runtime
optimizer readback. Independent review passed. The plan conservatively reserved
771,189,784,360 read bytes under a 1 TB ceiling and 44 GB storage against
62,735,515,648 free bytes, with 264,757,972,992 available RAM bytes.

Root deliberately interrupted only owned audit PID 6361 with SIGINT at
approximately 2026-10-14 10:58:08 UTC during the redundant full-row comparison
for an empty mask. Exit was 1 with `RuntimeError: Query interrupted`. Forty
months had completed, but these are partial evidence, not a population result.
The stage remains unaccepted. Its handler-preserved directory is
`/mnt/data/runs/.2026-10-14_kaushik_exchange_wallet_audit_v1.staging-z8ch2qg1`;
failure JSON SHA256 is
`8a5660e8b493a793bc0f74a2dad62347011244770367a158baa671d38bcf74b3`.
No original input write occurred; fresh v2 must reopen content rather than rely
on the interrupted stage's missing final checks.

## V2 admission boundary

The only computation change is exact identity for an empty source-validated
wallet mask. The direct original 12-field projection, null/false match flags,
unchanged reference hashes and no-fanout count gates prove the same multiset.
The saved month record explicitly identifies this proof and records that
literal `EXCEPT ALL` queries were not executed. Every nonempty mask retains the
original literal bidirectional comparison, even if all matches were already
ineligible. Source/metadata/reference validity and before/after content
reopening, exclusions, unsupported-address blocking, count grids, policy and
resource/read ceilings remain unchanged.

Use immutable new paths:
`/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_preflight_v2` and
`/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_v2`.
Fresh committed source, fixture results, preflight review, body/acceptance
bindings and shutdown evidence must be appended after those gates finish.

The report-only renderer validates accepted audit/receipt bindings and
recomputes all monthly/global role counts before writing native LaTeX tables.
It does not fit coefficients or read trading-data bodies. Prior cutoff and
full-date reports remain immutable.
