# Exchange-wallet audit, 14 October 2026

Status: v3 failed before final reopening/publication; v4 retry pending. Neither an accepted
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

## Preserved v2 capacity failure and v3 memory-only retry

V2 source HEAD `dcfd39f6d0d6affceeb6a9ddab0bf347b3acfabd` passed independent
182-test QA (38.228 seconds) and production-runtime QA (41.547 seconds).
Fresh v2 preflight SHA256:
`54b57a63afef9f7231ca69f70d0f7e651d79fba2526e0b4ce9995a768d122f0d`.
Its reference, raw/native metadata, binding, policy, caps, optimizer and read
plan exactly matched v1; independent review passed. Source/target/resources
were the only changed top-level keys.

V2 exited 1 at the original nonempty-mask comparison in April 2026 after the
unchanged 16 GB spill ceiling was exhausted. This is a capacity failure, not
accepted April count evidence. Forty-one prior months had completed. Keep
`/mnt/data/runs/.2026-10-14_kaushik_exchange_wallet_audit_v2.staging-jt3rv5o9`
unaccepted; its failure JSON SHA256 is
`a756d0b5bc1492ec9a118543341ecb41d51dc32cb6e83bb6bf0692b0634183bd`.
No retry changed the source files or old completed analysis outputs.

V3 changes only DuckDB memory from 64 to 192 GB and its available-RAM admission
floor from 70 to 240 GB, plus the corresponding error text. Reversing those
changes exactly reproduces the prior complete script hash. The same measured
instance has 267,392,999,424 total RAM bytes and 264,772,878,336 available bytes
after v2 exit, with no overlapping research job. All other caps, science and
proof paths remain unchanged. Independent 183-test QA passed in 31.313 seconds.
Use fresh committed source and immutable
`/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_preflight_v3` and
`/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_v3` paths. Fresh runtime
fixture QA, metadata/resource admission and final body acceptance are required.

## V3 connection failure and v4 exact comparison

V3 source HEAD `f1289efced03c554af1072c9ac8a5114522f3971` passed 183
production-runtime fixture tests in 43.359 seconds. Its reviewed preflight
SHA256 was `2ca918919cb291a9a075aa048d53661b735f19e435ab122a4f802d7113ec7398`.
All 44 month computations finished, including the three nonempty masks and
their literal zero-difference comparisons. The stdout connection then failed
with `BrokenPipeError` before final source-content reopening and publication.
No count or coefficient result is accepted from that stage.

Preserve `/mnt/data/runs/.2026-10-14_kaushik_exchange_wallet_audit_v3.staging-0mkbz5be`.
Its 274,029-byte failure JSON SHA256 is
`d6a602e0e2a76ca0d56fe70060233e1867e475b1368f4656f8d0631d6d99f755`;
the recorded read charge is 404,778,884,775 bytes. Root found PID 18698 absent
before attempting interruption, so no signal was delivered. Only system Python
daemons remained. An apparently running client session was not evidence that
the remote audit was still executing.

V4 retains the exact scientific and resource contract. The nonempty validation
now guards both exact typed schemas and source-locator uniqueness/range, then
executes one full outer locator join comparing all twelve payload fields. The
two resulting multiset difference counts are mathematically identical to
`EXCEPT ALL` under those guards. An independent seven-test oracle includes a
bare-join duplicate counterexample. The nonempty proof charges 5 base passes,
4 mask passes and 4 claims passes, below the prior 6/4/4 reservation. The empty
identity path, other gates and content reopening are unchanged.

Use new immutable v4 preflight/run directories and a new, non-overwritten
remote log with detached execution. Redirect all standard streams so a client
connection cannot invalidate a completed month. Root still supervises the
owned process and requires final acceptance before delivery or shutdown.
