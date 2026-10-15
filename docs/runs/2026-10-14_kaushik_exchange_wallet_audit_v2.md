# Exchange-wallet audit, 14 October 2026

Status: v4 count/mask audit accepted. Full-date regression refitting is pending
the requested storage/architecture choice; no new coefficient estimates are claimed.

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

V4 frozen source HEAD: `8e8c1ffb735035df5db391df53aa923b34901c95`.
Independent local integration passed 195 tests in 40.322 seconds; the same
suite passed 195 tests in 42.556 seconds on the production runtime. The
50-test focused exchange/report/oracle suite passed in 2.477 seconds.

The fresh 444,035-byte v4 preflight SHA256 is
`6c12ff00433a2f1fe51450bf6a64d60cb053963e9ba777d5508fcffb0e243c98`.
Root and independent metadata review passed. Reference, raw/native sources,
binding, policy, caps, optimizer settings and read plan exactly match v3.
Available disk was 62,363,308,032 bytes against a 44 GB reservation; available
RAM was 264,612,917,248 bytes against 240 GB. Eight tested dependencies are
bound. The unchanged plan reserves 771,189,784,360 bytes under 1 TB.

The assigned agent launched exactly one detached process, PID 25584, with a
new exclusively opened `/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_v4.log`.
`start_new_session=True`, `close_fds=True`, null stdin and log-backed stdout/
stderr isolate execution from the client connection. The wrapper exited 0.
Production source remains frozen until body acceptance or preserved failure.

## V4 accepted count and mask result

Accepted at `2026-10-14T13:01:29.056385+00:00` under
`/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_v4`:

- `audit.json`: 288,657 bytes, SHA256
  `3001812c6534bcf68cbe2597f487a86bd1be1546ce72cd31898f7f11c31ee1fb`.
- `acceptance.json`: 2,359 bytes, SHA256
  `e9a793a8bd5a90087c90aa51cb4e1a88ab905da99736bdc3550f13954eeec183`.

Scientific admission and all-mask reopening are true. All five exactness,
partition, role, input-reopening and unsupported-address gates pass. The 44
masks occupy 353,690,980 bytes. Conservative charged reads are 497,652,847,595
bytes, below the reviewed 771,189,784,360-byte plan and 1 TB ceiling.
The month proof grid is 41 empty identities and three executed unique-locator
full-payload joins. All differences are `[0,0]`; literal `EXCEPT ALL` queries
are not claimed. The completed 5,044-byte log SHA256 is
`4dfb67031b4080df7a5e044a439b560c770f15a3c9562c6019cac2aa5cfa9345`.
PID 25584 is absent and the assigned agent released its EC2 dependency. No OS
wait exit code was collected; completion is evidenced by the reopened accepted
receipt and final logged completion status, not an invented exit code.

| Primary-taker population | Original full | Removed | Retained | Cutoff unchanged |
| --- | ---: | ---: | ---: | ---: |
| All prices | 1,000,970,616 | 110,520,928 | 890,449,688 | 522,102,319 |
| D1 and D10 | 289,367,763 | 29,752,572 | 259,615,191 | 158,580,409 |
| Common duration tails | 252,319,669 | 25,510,437 | 226,809,232 | 140,293,451 |
| Original lifespan >1 day | 172,191,421 | 12,909,064 | 159,282,357 | 116,949,679 |
| Remaining maturity >1 day | 93,963,589 | 5,273,924 | 88,689,665 | 70,436,234 |

Full all-role baseline: 2,001,941,232 = 1,780,899,376 retained + 221,041,856
removed. Raw matches total 230,985,982; 9,944,126 already fail original
eligibility. Those records reconcile to original exclusions, not missing valid
observations. Every cutoff role/metric removal count is zero. The exact prior
cutoff-subset receipt binds this unchanged full input generation and the same
twelve fields, supporting unchanged cutoff estimates conditional on fixed
source/model/provider choices. Full-date coefficients require refitting.

## Count-only report and remaining estimation boundary

The renderer's added notes identify approximate archive dates and an unchanged
cutoff sample without claiming a coefficient refit. Nine focused report tests
passed in 0.031 seconds. Renderer SHA256:
`96c074ee3344dab825b3d5f8226032c5cdb439ba1dd16d1f4827c8137905e5dc`.
Report test SHA256:
`fe3d57a165e21dd10036f7ba4b6c7a256a63b10ba7ea2f096f60d04feb6a161c`.

Portable source was generated only from the accepted JSONs:
`output/kaushik_exchange_wallet_exclusion_2026-10-14_v1/report_v1/exchange_wallet_comparison.tex`.
Native Codex compilation succeeded. Independent local compilation produced one
letter-size page with no substantive warnings; root inspected that entire page
and verified no clipping, overflow or unintended blank page. The source SHA256
is `84283dfae2a8fb387fab49ed166511a1be0f7663294357a549ef620d7e6a7ce8`;
the preview SHA256 is
`5e5b6f151a73bbd7b46301f33a81b8df3fa604816a9c04eda38b0153af74dc4e`.
Native source opening was queued in the calling Codex task; no external viewer
was launched. Independent numerical report QA and shutdown evidence follow.

Independent final numerical QA passed: 1,320 monthly partition cells and 150
global role cells were recomputed from all 44 saved month groups, with raw,
noneligible and address-field reconciliations. Every one of the 13 displayed
numeric rows and the source/role/time/non-fit semantics agrees. The independent
focused report suite passed nine tests in 0.057 seconds. Final compilation and
visual/numerical QA are saved separately in `report_v1/qa_receipt.json`;
the initial build manifest remains unchanged. The old exact cutoff-subset
receipt's full input bindings match this audit, confirming the unchanged-cutoff
claim conditional on unchanged model/provider choices.

Root's post-audit process check found only system Python/SSH daemons, no
research or transfer job. Data-disk free space was 62,008,516,608 bytes. Finish
only scoped source/document publication, then stop the exact shared instance
and verify the state. The definitive local shutdown receipt is retained in
`output/kaushik_exchange_wallet_exclusion_2026-10-14_v1/task_state.json`.

The old input stage is 19,525,404,268 bytes. A fresh physical copy does not leave
enough disk for the unchanged 60 GB estimator/metadata reservation. The
requested versioned read-only filtering layer has not been implemented without
the user's storage/architecture choice. No trade-source deletion, new input
base, sports metadata or coefficient estimation was performed by this task.

## Publication and verified shutdown

Canonical release commit `f201a4806e4adb4cf826a1173560b9a057b100b2` was pushed
to `origin/codex/kaushik-exchange-contract-exclusion`; `git ls-remote` confirmed
the exact remote commit and the canonical checkout was clean. The updated
report fixture suite passed nine tests in 0.052 seconds on the production
runtime. No trading bodies, generated report files or credentials were committed.

Six finished files were copied into the new Dropbox folder
`Polymarket Data and Code/Kaushik Exchange Wallet Count Audit 2026-10-14 v1`:
the LaTeX source, PDF preview, initial report manifest, final QA receipt and
accepted audit/acceptance JSONs. `rclone check --one-way --download` completed
with six matching files and zero differences at 2026-10-14 13:22:03 UTC.
The existing Dropbox folders and all original datasets/results were preserved.

Root confirmed every agent released its infrastructure dependency and no
research job or transfer remained. Root stopped `i-0f5b31a268af53938` and
verified `stopped` in `us-east-1` at **2026-10-14 13:26:11 UTC**. The definitive
receipt is saved in the local task-state JSON. These post-shutdown lines are
a local run-record supplement pending the next normal canonical synchronization;
the instance was not restarted solely to amend documentation.

Remaining boundary: permission for the versioned read-only filtered-input
architecture. No affected full-date coefficient was fitted or relabeled.
