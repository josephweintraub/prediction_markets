# Polymarket Oct 1 finalization

Status: in progress, 27 October 2026. No refreshed data or results are admitted.

## Authorized contract

- Continue the full refresh under `polymarket_final_sample_refresh_v1.md`.
  Execution cutoff remains `2026-10-02T00:00:00Z`, exclusive, with no new lower cutoff.
- Preserve the accepted archive-record representation, filters, retention rules,
  weighting, uncertainty and model definitions. Raw native logs are not economic fills.
- Complete historical V3 coverage/semantics, exact timestamps, fresh market
  metadata/outcomes/categories, corrected-wallet/native-retention bridge, nine-sport
  clocks, paper inputs, estimates and independent acceptance before promotion.
- Publish the accepted generation to
  `dropbox:Polymarket Data and Code/Polymarket - Oct 1/`, using immutable releases,
  verified copied content and a completion receipt last. Keep prior releases recoverable.
  Do not upload scratch, credentials, incomplete data or unsupported certification claims.

## Starting evidence and infrastructure

The completed append v2 has 582 accepted chunks and remains unchanged. Its terminal
checkpoint is `output/polymarket_adaptive_resume_2026-10-26_v2/terminal_checkpoint_2026-10-27T033212Z.json`.
Raw collection completion does not admit a final sample.

Root started the existing `r5.8xlarge` instance, obtained fresh DNS and verified
canonical clean revision `c39ddce3d9a010fe65faac652359fd401561e877`.
Root mounted only UUID `d0cd087b-94c4-428c-bae9-ae28929059f6`, after verifying
serial `vol0acbba4565a64f792` and the existing 750 GiB device.
Available bytes after mounting were 326,624,571,392. These are starting observations,
not a capacity promise for unadmitted later stages.

Operational run: `/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1`.
Local small controls: `output/polymarket_oct1_finalization_2026-10-27_v1`.

## Delegated work and sequencing

Independent initial audits covered source/timestamp admission, metadata/sports/export
admission and first-party V3 protocol evidence. They found that old collectors and
paper loaders cannot safely be redirected to this generation unchanged.

The next separately versioned implementation units are a bounded exact-header pilot,
a bounded V3 sparse-history diagnostic and a bounded public-market-pagination pilot.
These are implementation/diagnostic units, not final-universe admission. Production
execution is serialized after review, focused fixtures, clean canonical commits and
fresh resource checks. No production worker is authorized by this record alone.

All root/agent infrastructure holds must drain before root stops EC2 and verifies
stopped, including on failure or interruption. The old append monitor remains paused.

## Completed exact-header pilot

The reviewed collector and fixtures were committed and pushed at
`04f958661612e5b250f1ec0eff7b44e9ffb357a2`. Canonical-runtime verification passed
152 focused tests, exit zero; receipt and log are in the operational run.

The pilot requested the first 999 append blocks (88,978,538–88,979,536) and
the separately header-proven cutoff block 94,796,039. The latter request supplied
its expected hash. The input is `header_pilot_keys_v1.json`, SHA256
`af83e45810ab3f3e156c54420fca5b1017ac402067c941fff5ef9be4a865a150`.
This deterministic literal selection is not a final-universe required-key inventory.

`header_pilot_v1/RESULT.json` reports 1,000 accepted keys, zero missing keys,
zero fallback rows, 103 HTTP batch attempts, 1,004 method items and 11,922,825
response bytes. Elapsed collector time was 8.829 seconds, with 863,756 retained
bytes before its 55,238-byte result. The final result SHA256 is
`6afa31e23be7a9abd61c91bf90ab39ade7eef55ffe8da635a69d2c036828ada4`.
The launch/exit receipts bind process identity and exit zero. No timestamp cache
was replaced, and no sample or whole-history coverage was admitted.

The pilot's per-ten-key output overhead must not be extrapolated into a full
history run under the 8 GB header envelope. A separate required-key inventory and
efficient, globally accounted campaign need review before full execution.

## Completed V3 diagnostics and metadata acquisition boundary

The sparse-history diagnostic completed at revision
`835b94e8111372952be7fe03d104d24aa34c87be`: its exact literal address/topic
range receipts support conservative raw-event collection from block 87,479,447.
The saved result SHA256 is
`faebc665f82de3a66ae73bdb1ea96a089ee39fca8be8a20cdbb27046eb87e10b`.
This is not first-deployment or event/token/economic-equivalence admission.

The observation-only runtime/getter diagnostic was committed and pushed at
`ce9edd599d73039678ca58b0379e9994f4d431d6`. Canonical verification passed
111 focused fixtures after preserving an earlier test-invocation import error.
Its 48-request execution exited zero, retained the two exact implementation
runtimes, and recorded historical configuration/dependency getters. The result
SHA256 is `761eb05565361d5303e14a4b0fceb21dbc5b18b6181879ce3aea3f53cf1f156b`.
Source equivalence, implementation histories of dependencies, position-family
mapping and payout semantics remain separate gates.

The original public-metadata pilot failed unaccepted on schema irregularities.
Its first raw response is preserved at
`/mnt/data/runs/2026-10-27_polymarket_market_metadata_pilot_v1/responses/a01.body`:
54 of 1,000 objects had blank condition IDs, and 110 token IDs were blank.
These are observed identity issues, not proof of missing native trades. No
completion marker or normalized source was admitted. A separate raw-retaining
v2 probe preserves irregular rows and distinguishes pagination evidence from
returned-scope verification; it must not rewrite the failed v1 run.

## Next admitted implementation boundaries

The projected header-key inventory binds an explicit immutable file roster,
rehashes new native payloads, reads only required block/hash column extents,
and accounts for every source row before producing sorted required keys.
Its completion remains literal-roster-only while historical V3 is outstanding.
The raw metadata v2 probe fixes five public streams and at most two pages each;
neither terminal cursors nor pilot success certify the final native universe.
Canonical combined fixture verification passed 140 tests before execution.

Historical V3 collection must use a separately reviewed parent acceptance wrapper,
not the old child's completion marker alone. Global admission must carry forward
all inherited, failed and diagnostic charges; full history additionally requires
an independently accepted positive-row probe. No full historical collection or
full-header/metadata/sports stage is admitted by this record alone.

## Reviewed historical wrapper and metadata continuation

Independent wrapper review reproduced an unsafe false-idle claim after a child
birth-capture failure, before any production launch. The repaired wrapper retains
the spawned handle/process receipt, withholds terminal success or false-idle
claims when ownership/drainage is unknown, and signals only a proven owned group.
Real-constructor fixtures cover missing births, identity reuse, partial pipe
setup and lingering descendants. Independent recheck accepted the repaired
wrapper (`f3cdafdf19e5bdfaf892c37b86c1f600365643751db88e46ba24381ed3f30248`).

The initial raw-retaining metadata v2 invocation preserved two accepted CLOB pages
and failed unaccepted on an additive Gamma `$schema` field. Its raw response
hashes were independently reopened; all ten returned Gamma rows matched the
literal requested version/open scope. A source-versioned continuation permits
only the exact optional schema URI, leaves every other gate/cap unchanged and
requires a fresh directory. The earlier failed run is never resumed or relabeled.

The root historical-probe accounting receipt is
`v3_historical_global_admission_probe_v1.json`, SHA256
`9423177e947daa643d25bb820b3adf150e210988d15fa43ffc774a007f484488`.
Its prior charges are 39,228,112,651 retained bytes, 498,999 RPC calls,
571,259,492,106 response bytes and 97,902.126 seconds. These include explicit
conservative diagnostic/control reservations, not merely observed usage.
The v2 terminal aggregate already includes its inherited/failed raw roots;
those roots are not re-added. New profile reservations remain inside the
unchanged global ceilings. Full historical admission must additionally account
for the probe's charged use and independently reopen its positive-row acceptance.

## Historical collection and downstream preparation

The separately versioned metadata continuation completed its ten-page probe at
clean revision `2ae681f0df162fa96c2988860a43ba312b208b8f`. It retained 2,080 raw
rows across five streams, with 10 HTTP attempts and 4,080,344 charged response
bytes. All five streams remain nonterminal at the probe's two-page limit. Its
manifest SHA256 is `20f91af26c4690f77b0a1fed83954439f135caa82a891222a8eb79e8cbdc52d1`;
this accepts the bounded raw probe, not full market coverage or payout mappings.

Root independently reopened the historical positive probe: 384 distinct native
records across ten 100-block windows. The accepted parent completion SHA256 is
`7ba3cedafff5569474da56148ea406b60b4c5d1143634767f2a5406d0ce04c5b`.
Its retained bytes, RPC/response charges and full runtime reservation were added
before admitting the full historical raw child. The separate full admission is
`v3_historical_global_admission_full_v1.json`, SHA256
`c82018677c88db8a32d494d3a43b368da5f4aae408746353395e074967bb780e`.

Root launched `v3_historical_full_v1` at 14:48:47 UTC. Its parent PID/birth ticks
are 20318/541599, and its child PID/birth ticks are 20332/541618, on boot
`b9008fdc-0b94-4daf-b498-13403e96c475`. At 15:45:36 UTC all 14,991 requested
windows had successful receipts and the child had exited with a saved drain
receipt. The parent was still independently validating outputs; no parent
acceptance was yet present. Source and canonical HEAD remain frozen at the
revision above until the parent and all owned consumers exit.

The parent subsequently accepted 398,285 raw records across all 14,991 windows
and exited zero at 15:46:35 UTC. Parent completion SHA256 is
`ebed5d73293f09c6979b5e522c4da66b4ac0765f24789fa9840031561ad1e56d`;
the result SHA256 is `ee20282b39fb4ae3c1edcbeb2b66e5bb63096ffd8319e1063c5072586c0106fa`.
Root independently reopened the committed native validator against the saved
payloads, range proofs, request ledgers and controls. It reconciled the same
398,285 records and 14,991 windows, confirmed the caller's exit zero and both
owned births absent, and saved `v3_historical_full_root_acceptance_v1.json`
(SHA256 `a26a2c5fd6d9e9bb716f24ed8e52556be8ccd82d4e4fec9b73f96ce85463d750`).
The raw stage charges 59,967 RPC attempts, 960,401,873 response bytes,
305,702,649 retained bytes including terminal reserve, and its full 7,200-second
runtime reservation. The root recheck separately reserves 600 diagnostic seconds,
uses no API calls and writes only the small acceptance receipt. Raw acceptance
does not admit economic decoding, exact interior clocks or the final sample.

Independent local review accepted a compact 1,000-key exact-header scale pilot,
a two-address raw source lookup and a terminal-pagination metadata collector.
None has yet been executed in production. The metadata review repaired three
reproduced faults before launch: a late cumulative-deadline check, retained
prefix accounting after interrupted reads, and rejection of prior collection
roots with unreconciled request charges. Those changes do not normalize raw
metadata, verify deployed source equivalence or admit an analysis sample.

The sealed source-roster preflight is bound by SHA256
`acb848a8866959dd6fcaf5a8c5d62800c14b1b29cbed250be8fa2994bd71561e`.
It found 116,364 declared native payload files in the accepted append/probe
sources, plus one legacy file. It did not read trade bodies or adopt the active
historical run. A reviewed literal-roster builder must add the independently
accepted historical full result, and the subsequent inventory must rehash the
native payloads and project their actual block keys before full timestamp
collection can be admitted. The historical probe is diagnostic overlap and
must not be counted again as an authoritative source.

The first canonical downstream fixture invocation passed the executed checks
but ended with a test-runner import error from a misspelled metadata test-module
name. Its failed receipt/log are retained as `downstream_collectors_runtime_fixture_v1.*`.
The corrected invocation uses the verified existing module name and a fresh v2
receipt; no API collection is authorized by a failed fixture invocation.
The corrected canonical run passed 241 tests in 127.943 seconds, exit zero.
The saved v2 fixture log SHA256 is
`e02d6b72f8149a76d5c77ad00bd282c50771b1bf40953f00ad57b1bcaadd4b79`.
The header inventory retains two dedicated footer passes and one native-payload
hash pass; its repaired staged-completion gate rechecks source identities,
roster/source bindings, outputs and deadline after marker staging.

## Compact clock and source-evidence pilots

The reviewed downstream units are committed and pushed at clean revision
`922265ed7e5253da72af6132b5fa3c0e95d5d434`, in separate logical source commits.
The fixed two-address source lookup completed with two HTTP 200 records,
1,554,185 charged response bytes and no compiler execution. The raw result
SHA256 is `5d2a7eeecca8f1c8d665884ba24eb43402cc0bf1ddf0a634e4aaafe59bdc3d76`.
Reported compiler/source matches remain untrusted until their independent
source/runtime and build checks pass; no economic source is admitted by lookup.

The compact timestamp pilot completed the same literal 1,000 keys as the
original pilot, with every projected time/hash matching, zero missing keys and
zero fallback rows. Its 72,750-byte Parquet has SHA256
`5eb9aff29f59ab374697281e471aad22149f974aa629a2e8cecf16e5f928e668`.
The cumulative usage is 116 HTTP attempts, 2,008 method items and 23,846,667
response bytes, including the original pilot. Its 11.016-second cumulative
elapsed figure includes 8.829 seconds inherited from that original pilot.
This is a scale diagnostic, not full-history feasibility or timestamp coverage.
The original pilot has 408 retained files; adding its separate input gives 409.
All 422 prior header/control ledger entries and the ledger itself total
1,320,569 bytes before this pilot's separately bounded outputs.

The next literal-roster builder is independently reviewed, including full
historical parent acceptance and zero-row intervals. Its 2 GB cumulative JSON
ceiling includes output reopens and all prior preflight reads, and its one
footer pass remains separate from the subsequent inventory's two passes.
Native payload bytes are not read by this builder. Final coverage requires the
later native-payload rehash and projected key inventory; no archive-discovery
or filename glob can substitute for the bound source lineage.
Canonical verification of the roster builder passed 106 focused/related
synthetic tests, exit zero. The fixture log SHA256 is
`063138d6dfe78eeb24a4b8fcb5875a271352c25e5f0cb9ac99a71b45eb304673`.

## Literal source roster and key inventory

The roster builder completed at clean pushed revision
`c14d79c86d572c21f0af300156a6a04a89387f26`. Its literal 131,356-file roster
is `/mnt/data/runs/2026-10-27_polymarket_exact_header_roster_v1/roster.json`,
65,678,058 bytes, SHA256
`1e0e9663a4507f2ba1d5d478c7774d1a212823f315ef17a123fc8a3ea84c02cb`.
It includes the authoritative historical full repair and excludes the overlapping
diagnostic historical probe. The 2,015,058,879 declared source records are raw
records, not paper observations or unique economic fills. The builder's
completion SHA256 is
`5d40b55434192b4aaede87fbe4a803894562b337f422d7be43d909431f004b51`.

Root launched the committed key inventory at 16:31:36 UTC on 2026-10-27,
PID 30058, Linux birth ticks 1158554, boot
`b9008fdc-0b94-4daf-b498-13403e96c475`. Its launch receipt is
`/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1/header_key_inventory_v1.LAUNCH.json`;
outputs are under
`/mnt/data/runs/2026-10-27_polymarket_exact_header_key_inventory_v1`.
The separate admission SHA256 is
`eb9a47ac94c02ffbf3d85befd2faeb65e0a201fb511200d7ee2a9045491057b9`.
Keep canonical source frozen until this process and its owned children exit.
The run has fixed 2,400-second runtime, 64 GB native-payload rehash, 2 GB
projected compressed reads, 2 GB footer reads, 1 GB output, zero spill and
20 GB free-space floor. It must reconcile every projected source row and
native hash before sealing the required block keys. No fallback, full-header
campaign, analysis sample or Dropbox publication is admitted by launch.

The next full-header collector passed 49 focused and 193 combined local
fixtures, plus independent review, but is not yet canonical or admitted.
Its actual-key-count and complete cumulative-ledger gates remain outstanding.
The independent source compiler verifier is still under repair after review
reproduced a per-child startup timeout gap. No compiler has been downloaded or
executed, and no deployed-source equivalence is claimed.

The inventory subsequently exited zero at 16:58:59 UTC. Its completion SHA256 is
`ad6b125cfa56c4c7feb989b383f18317a6aeb333941fa22855d3931945c93eef`,
and its 2,689-byte manifest SHA256 is
`ba00b4e762ff614c45c87bfd55d75465394b7418ddc1d856b920adabe1eae108`.
It reconciled all 2,015,058,879 declared raw records, 131,356 source files and
32,901,442 sorted distinct required blocks, with zero invalid/null block keys,
invalid/null native hashes or conflicting block hashes. Required-key Parquet is
293,488,549 bytes, SHA256
`bbf4502da63275225605d665790e8083542c3d9a7cfaee77cb42847501c41be9`.
The source audit is 14,612,374 bytes, SHA256
`43cb1377699c6e1d483e2cda1a940582b492b3d764f1af7bec65cad75f305012`.
The inventory rehashed 37,381,873,746 native payload bytes, read 1,367,693,740
dedicated footer bytes and 479,002,442 compressed projected bytes. Its manifest
records 1,610.516 seconds before terminal checks; the whole CLI finished within
its fixed 2,400-second limit. These are raw-lineage checks, not unique economic
fill or final paper observation counts.

The compiler verifier's startup deadline repair passed 39 local fixtures and
independent replays of the 601/600-second and 11/10-second overruns. The full
header collector's final 49-fixture version and this verifier together passed
88 local synthetic tests. Both still need canonical fixture/commit integration
before production. Compiler download and deployed-build admission remain
outstanding.

Root's independent key-output recheck is saved as
`header_key_inventory_root_acceptance_v1.json`, SHA256
`5dd1a0c34927529f3cc213c656ac9b6d4d2f66f199d3547de31771815bba77bc`.
It rehashed the published outputs, checked every source-audit record and source
identity, and independently read the sorted key sequence. It found 5,884,493
keys with native hash assertions, first required block 35,896,869 and last
94,796,039. It did not repeat the 37 GB native payload hash pass. The check used
zero API calls and 20.934 seconds under a separate 600-second diagnostic
reservation.

Canonical verification of the next full-header/source-build units and related
helpers passed 325 synthetic tests in 138.902 seconds, exit zero. The log SHA256
is `970a0ee166d7810ca9b3621f4a43edea74d7a79695eab7f2400ff029f643fbad`.
Actual prior-ledger integration subsequently found one legitimate CLI stdout
copy of the accepted scale manifest. A narrow fixed-path/size/SHA plus decoded
manifest-equality allowance is being reviewed; generic protocol-artifact
admission remains forbidden.

A separate pure V3 archive projection core passed 28 focused and 86 related
fixtures and independent review. It requires explicit accepted build/era,
singleton exact-header and reciprocal full-token-ID inputs. It retains both
archive roles, checks eligibility per role, keeps eligible module-2 binary
claims, and distinguishes excluded module-3 claims and either-wallet exchange
hits. A reproduced binary64 rounding case was repaired before acceptance so
an invalid complemented role cannot erase a valid primary-role observation.
This core does not scan, publish, deduplicate or admit a final sample.

## Next collection-stage separation

Root may run the admitted full-header and full raw-metadata campaigns together
only at one clean frozen source revision. They have disjoint immutable inputs,
fresh output directories, no shared mutable caches, different API authorities,
separate unchanged 8 GB storage partitions, and separate 8 GiB process limits.
The header stage reads the accepted literal block keys; the metadata stage
walks public market pages and never changes those keys or source payloads.
Both retain their cumulative request/runtime accounting and 20 GB free-space
floor. This independence permits concurrent acquisition, not concurrent
downstream estimation or source mutation. Keep canonical source frozen until
both campaigns and their owned processes exit. Neither may be used downstream
without its own terminal proof and root reconciliation.

## Resumed source integration

After a usage interruption, root rechecked AWS at 18:00 UTC: the same instance
was running, the verified data UUID remained mounted, and no production,
notebook or transfer consumer was active. The completed inventory and its
acceptance were unchanged; no full-header or full-metadata output directory
existed. Only the eight expected reviewed source/doc paths were modified or
untracked in canonical Git. Pending local compiler-acquisition and decoder work
was not copied or admitted.

The narrow scale-stdout allowance passed all 54 focused tests and independent
review; the related local suite passed 198 tests. It accepts only the literal
1,723-byte stdout artifact with SHA256
`1fa7aadb16d893677a10ed00948806d1bef87c6698d63fc826b80ed620e37e3b`,
requires decoded equality to the accepted scale manifest and complete inclusion
of that producer root, and charges it as ordinary control bytes.

Final canonical verification passed 358 synthetic tests in 139.996 seconds,
exit zero, including the new pure archive core. The v2 fixture log is 1,408
bytes, SHA256
`66b6f64585b74710563451e2055454768ccb9a5f3bbb9450399427d15d349db8`.
The full-header root admission will debit 7,200 prior seconds conservatively,
including roster/inventory reservations, pilots, independent checks, fixtures
and bounded admission preparation. The raw-metadata root admission will debit
49 prior HTTP attempts, 117,440,512 response bytes and 4,200 seconds, including
the separate source lookup and bounded root preparation. These are inherited
debits inside unchanged cumulative ceilings, not permission to increase them.

The first full-header root preflight stopped before writing an admission or
launching because the roster builder had also echoed its manifest to stdout.
Its saved failure receipt SHA256 is
`82ee9758a1dea351ea180e81853e0a9a78b1d2c86dace772a9790eb73866bf63`.
The second narrow allowance binds only that literal 3,084-byte artifact, SHA256
`4bd368bc90ac33203989b43174425a504da55730314fea833ff8d87d8f6a8aaf`,
decoded equality to the ledger-bound producer manifest, whole-root inclusion
and the ordinary marker/source/count gates. All other new operational controls
passed the original strict validator. Independent verification passed 58
focused tests; the related suite passed 202 tests. No generic exception or cap
change was introduced.

The raw-metadata admission at revision
`1ce194e6de336083bdafe5f804ff3eedac2e415d` was saved but never launched. Preserve
`full_metadata_admission_v1.json` (20,300 bytes, SHA256
`ade12ee405e4241f0ea3dec7f9840543871a14128aa9d5aded835a5ffc837caa`)
as prior control evidence; the reviewed source continuation needs a fresh v2
admission and must charge this old file rather than overwrite it.

The separate lossless V3 decoder passed 13 focused and 99 related fixtures,
then independent byte-level randomized/adversarial checks and 101 related
fixtures. Its provider timestamp remains opaque provenance. Canonical focused
integration of the final ledger delta, decoder and unchanged related units
passed 185 tests in 31.808 seconds, exit zero; its v3 log SHA256 is
`0ae68d5bf677ed9189d6a45b29f625c62619996bcd5000689562585143e391e7`.
No data decoding or final sample is admitted by these pure-core fixtures.

## Live acquisition checkpoint (local operational addendum)

The reviewed source continuation is clean, committed and pushed at
`1bbeac146d22b2ca0ec290180f6dc7bf89318b6c`; its source-commit receipt SHA256 is
`e493f7c6d86a0c233451b635ad9782c582a115111b715ccf043aed2d54099c20`.
Do not synchronize this local operational addendum into canonical source while
either campaign runs. Keep that HEAD and all source bytes frozen until both
workers and their owned children exit.

Root admitted and launched exactly one of each independent campaign:

- Exact headers: `/mnt/data/runs/2026-10-27_polymarket_exact_header_campaign_v3`,
  at 18:13:41 UTC, PID 37847, birth ticks 1771023. Admission is
  `full_header_admission_v1.json`, 114,157 bytes, SHA256
  `60d13a5afb93968371a49c0d6b567d9d2d5a4a9cb80331377c74d8d067236519`.
  The complete prior ledger has 466 files: 374,921,850 baseline and 544,613
  control bytes. Actual K is 32,901,442; the conservative new runtime projection
  is 143,905 seconds and the accepted-output size projection is 2,991,974,882
  bytes, both within the unchanged ceilings. Launch is not completed coverage.
- Raw metadata: `/mnt/data/runs/2026-10-27_polymarket_market_metadata_full_v3`,
  at 18:13:43 UTC, PID 37980, birth ticks 1771282. Admission is
  `full_metadata_admission_v2.json`, 20,604 bytes, SHA256
  `c310cc43f4ee68bb7713808b34fdc245f55efaccf707e98f1679a872628bff56`.
  The prior ledger has 83 files: 4,765,476 raw and 578,085 control bytes.
  All five terminal pagination walks remain required before raw acquisition
  acceptance; no normalized market/outcome universe is admitted by launch.

Both launches bind boot `b9008fdc-0b94-4daf-b498-13403e96c475`. Root independently
observed matching live birth identities and the metadata START receipt.
Launch/exit/log controls use prefixes `full_header_v1` and `full_metadata_v1`
under the operational run. Workers have independent process sessions so an SSH
interruption alone is not evidence of worker failure. No retries, duplicate
campaigns, source changes, higher caps, promotion, estimation or Dropbox upload
are authorized by a monitoring check. The existing heartbeat must monitor both
explicit births and durable controls, preserve small checkpoints, and leave
EC2 running while either bounded stage still needs it. After both drain, root
alone verifies all process/agent/transfer holds, stops EC2 and verifies stopped
when no separately admitted next stage remains. Raw collection completion is
not the final-data or Dropbox-release completion.

The header worker then exited one at 18:14:52 UTC, before sealing its first
chunk. `FAILED.json` is 558 bytes, SHA256
`c4b06c4ce038f6a4a581c51e6cad789f0051eda191b4629ec1adb44f310911f0`;
the root exit receipt SHA256 is
`3a2b7bc09f4a5a02fd3ee6c18deff71b7ac7f4d9171ba3faf5244570f26cddb3`.
It reports `rpc_reply_shape`, zero accepted keys, and cumulative 219 HTTP
attempts, 12,209 method items, 100,653,201 response bytes and 7,271.485 seconds
including inherited usage. The failed request covers key offset 10,100,
100 keys, first RPC ID 12,110, request SHA256
`8bccba52eb4055dde5c65cc47bf69b679abba6ee7d28b3c42c672f0467f8a21d`.
Its HTTP response was 200 with a complete hashed body, but that body was not
retained. The exact provider error therefore remains unresolved; do not infer
rate limiting, missing blocks or missing trades from this generic code.

The metadata worker remains healthy. Its active source lock still applies,
even though the header process exited. A proposed single read-only provider
observation was blocked by automatic safety review before any diagnostic RPC
or remote artifact was created. Root requested new explicit human approval;
do not bypass the rejection, restart collection, or treat a later diagnostic
as an admitted timestamp source. Keep every failed-attempt charge for a future
reviewed continuation. Local compiler-acquisition code passed independent QA
but remains outside canonical source while metadata runs.

Root subsequently confirmed the failed header process, its process group and
session were absent, every saved request had a FINISH record, and no owned
chunk/header output was sealed. The scoped drain receipt is
`full_header_drain_v1.json`, SHA256
`fefe1c46202384e490ca0fe13004f858555f15bfb46ad52545cfdf953a93f893`.
It reserves 120 new failed-run seconds and 60 seconds for chunk zero, compared
with observed 71.668 caller seconds and 10.404 journal seconds. New durable
usage reconciles to 103 HTTP attempts, 10,201 method items and 76,806,534
response bytes. This proves only that this failed worker drained, not that the
host is idle: metadata remains active. At 18:20 UTC its CLOB stream had 715
successful page calls and no failure marker. No timestamp diagnostic was sent.

A local-only one-call diagnostic draft is retained at
`/private/tmp/polymarket-header-observation.hmhvL8/observe_once.py`, SHA256
`6f44ef39d781d122df6199d8eaa5795a4dc6b048ebce7f482225533e93293c41`.
It still requires root's full artifact/resource review and explicit human
approval after the safety rejection. Do not execute it from a routine monitor
or infer approval from its prepared code or fixture checks.

Root's local review found that the prepared diagnostic draft rounds the saved
elapsed counter, so its exact comparison would fail before RPC. A corrected,
separately reviewed admission must also retain the conservative scoped-drain
runtime charge and review deadline propagation and endpoint-echo handling.
The draft remains unapproved and unexecuted; this record does not grant a retry.

The local-only pure CLOB claim normalizer passed independent scoped QA:
`pipeline/normalize_polymarket_clob_claims_v1.py`, SHA256
`87f0aebf133aa950c687b623c6b623ed54686f9c14edcaf96fcebae21858b8bc`,
and its fixture file SHA256
`ff8033ef372fd7b367c00ae941a0ab066a981d4f078fe71c627a74b612686a8a`.
The author reports 25 focused and 127 related passing synthetic tests;
independent QA passed the 25 focused fixtures and 19 additional adversarial
cases with no blocker. It
retains exact full token identities, explicit Boolean winner evidence and
quarantined contradictions without price, clock or slug inference. Complete
page lineage, global uniqueness and cross-provider consistency remain separate
production-publisher gates. No metadata universe or sample is admitted, no
production scan ran, and canonical source remains untouched while metadata runs.

The human subsequently explicitly approved exactly one read-only, 100-block
diagnostic request to the same provider, saving its response without restarting
collection. This supersedes only the pending human-approval gate. The old draft
remains unexecuted; its corrected version, exact evidence/resource preflight and
independent review are still required. A routine monitor must not dispatch the
diagnostic. No campaign continuation, timestamp adoption or source change is
authorized by this one-request approval.

## Single diagnostic completed; metadata terminal handoff

Root executed exactly one approved provider request after final independent
review and 22 native Linux fixtures. The final one-off source is retained in
`/mnt/data/runs/2026-10-27_polymarket_header_error_observation_review_v4`,
SHA256 `b8570efa53decad170f129af500a499c3810b29a91018d8058751c287a9aa286`.
Its fixture SHA256 is
`65a95fb8520d1ab0a7d46e07dd261ea2192b79204760d59f3907f6b45fd93c67`;
the accepted Linux log SHA256 is
`91b040d6eab0126381fccc3ff0cad5b22f503061a14a2e67b888a1218cd575e3`.
The actual source/fixtures/log are also mirrored beneath local
`output/polymarket_oct1_finalization_2026-10-27_v1/header_diagnostic_observation_review_v4`.

Earlier drafts and failed fixture logs remain unchanged. One real script
invocation failed at a disk-space helper reference before output creation or
any HTTP dispatch. Root independently proved its process/group drained and
zero requests; its failure/drain receipt SHA256 is
`3a4a206c7a1c8cba9f6868d1cbee45f4cf92aed89d67300c19a5a6597a799fdd`.
The corrected version imports its own disk-space helper and adds a real-module
dependency-contract fixture. These were diagnostic implementation issues, not
observations about missing blocks, trades or the scientific sample.

The single consumed HTTP request reproduces the original failed 100-key
payload exactly: request SHA256
`8bccba52eb4055dde5c65cc47bf69b679abba6ee7d28b3c42c672f0467f8a21d`.
It returned 100 result envelopes, zero errors/missing/duplicate IDs, and all
100 header objects passed independent number/hash/clock-shape checks. The raw
response is 782,156 bytes, SHA256
`98a80b96f982eb2b2448310dd2476a0234d5e392be6238be0a70d5697d460677`,
at `/mnt/data/runs/2026-10-27_polymarket_header_error_observation_v1/response.raw`.
The result SHA256 is
`d0abd0d56bfd9735bd35277a233c9b1f8e8fe71254c8ace0e03f98d039ebb1f1`.
Root acceptance SHA256 is
`f3cf41e30d6bf6bbeba35c8e276d4de42fc9a286376250a0e256a2b7298e15af`.
The original provider error was not reproduced and its cause remains unknown.
This is neither missing-trade evidence nor accepted whole-history clocks.
No timestamp was adopted, no header Parquet/COMPLETE was created, and neither
collection was restarted. The one-request approval is consumed; never repeat it
from a monitor. Cumulative header charges are 220 HTTP attempts, 12,309 method
items and 101,435,357 response bytes. Root's authoritative 7,680-second runtime
reservation also carries the failed zero-HTTP invocation and root controls;
the diagnostic program's narrower 7,440-second profile is not the whole ledger.

Meanwhile, raw metadata acquisition exited one at 19:02:26 UTC with
`response_byte_limit` on its 4,810th call, CLOB page index 4,809. It retained
4,809 accepted but nonterminal pages and 4,809,000 raw market records. These
are metadata records, not trade/fill counts. CLOB never reached its terminal
cursor and no Gamma walk completed. The last accepted cursor is saved in
FAILURE; the final partial response is preserved unchanged. FAILURE SHA256 is
`e8c215008efe90a593fa848003581458a6d2909bf7befe13e7bcf1736fea70ad`;
EXIT SHA256 is
`9607851c2e1d6b7444c4f2423ba2c70854318f6209876a891dec9bbc5f8d77b3`.

Root reopened its final FINISH receipt and independently rehashed the retained
250,591-byte gzip (SHA256
`3a212e3d93ce4e175dca7e62a6fef8a0aca89275652301c3f2050ab3b64e46d3`)
and 4,194,304-byte incomplete response prefix (SHA256
`14087e2bd309477b1b9e7e9f9156e9abc4019e0c4e836a32c7f531430ca9b53f`).
The failed stage charges 4,859 HTTP attempts including 49 inherited attempts,
11,970,589,082 response bytes, 1,223,425,859 retained raw bytes and 14,293,757
control bytes including the explicit external reservation. Its saved runtime
is 7,122.942 seconds including inherited use; the terminal handoff carries
7,800 seconds conservatively after root checks. No final universe, resolved
claim spine, analysis sample or Dropbox release is admitted.

Root verified 603 saved PROCESS/launch birth records across the bound campaign
and control roots, every current owned birth/group/session, all agent holds,
and relevant host compute/notebook/transfer consumers. There were no unknown or
live owned births and no remaining relevant consumers. The drain receipt is
232,044 bytes, SHA256
`188c8153f91eada372fe52b678d985976a1216bae4978e5d8c2342e13c13b795`.
Canonical source stayed clean and unchanged at
`1bbeac146d22b2ca0ec290180f6dc7bf89318b6c`. Root then stopped EC2 and independently
verified `stopped` at 19:16:46 UTC on 27 October 2026. Data and the volume were
not deleted or changed. The same heartbeat monitor is now paused.

A new reviewed, versioned metadata continuation must preserve every prior and
failed charge and validate full coverage. Smaller requests are already
authorized within unchanged resource ceilings; this handoff does not admit a
new stage or higher caps. Full exact clocks, independent source-build proof,
metadata/spine/category/sports gates, estimates and the verified Dropbox release
remain outstanding. Do not restart anything from this terminal monitor alone.

## Authorized continuation after terminal review

The user asked root to keep working toward the complete release without routine
approval requests. The existing sample, resource ceilings, source-validation and
publication gates remain unchanged. Smaller requests are already authorized;
ordinary reviewed implementation and fresh, bounded stage launches do not need
another human decision. New spending limits, access grants or research choices
would still require explicit authority.

Root delegated three disjoint local implementation/review units before further
production: a compact-metadata continuation, exact-header recovery and an
independent compiler-acquisition admission. Old source versions and failed runs
remain immutable. The compact route must use the documented, non-sampling
`/simplified-markets` endpoint, beginning its own cursor walk at `MA==`; the
4,809 accepted full-market pages are supplemental evidence, not a certified
prefix of that different walk. Gamma's documented keyset limit remains 100.
The compiler's authenticated toolchain download need not depend on successful
market pagination, but its metadata storage and request charges must still
reconcile to the shared unchanged ceilings. Exact-header recovery must preserve
the failed run and consumed single diagnostic, retain bounded failure evidence
and require complete validated results before admitting any clocks.

The instance remains stopped during local implementation and fixtures. No new
production stage, timestamp cache, normalized universe, sample or release is
admitted by this continuation record alone.

Root restarted EC2 and at 19:47 UTC verified the same volume UUID/serial,
750 GiB device, 324,027,625,472 available bytes, clean prior source and no
relevant compute/transfer consumers. The new boot is
`1ad746ce-103e-4e8f-a2f5-ba4df810e899`; the restart receipt SHA256 is
`5cba8e854ffcecaa80db8d777669105e4c216234e134a03fb8dec285a5e43ff3`.

The independently reviewed compiler-acquisition v2 passed 53 Linux fixtures
and was committed and pushed at
`07b881577a77baa3fa25b4f13efccd3c50c406ea`. Its separate root admission binds
4,906 prior files, the failed metadata terminal evidence and unchanged resource
ceilings. Admission SHA256 is
`4ed49d6645ca840c1e3adccfd168c1b91b9c4ff5723843dee13f27bb129059d1`.
Root launched one bounded acquisition at 19:50:06 UTC; it exited zero at
19:50:08 UTC. The 16,865,544-byte nonexecutable binary matches the fixed official
SHA256 `d40adc6f9fdbb22a97d32a02fa05688bf2ee7886affc48c9851b0afd4a726b39`.
Root independently reopened completion, result, binary, source and owned-process
evidence. Acceptance SHA256 is
`dd43c90be3cde023db8621e67ff92e51173e2c1b7a21ab804d592f4bf0c30146`.
The first root recheck used full stat equality, which includes access time, and
stopped before acceptance; its corrected check excludes only access time while
preserving identity/change fields and exact content authentication. No download
was repeated. The acquisition used one HTTP request and 16,865,544 charged
response bytes, with the full 300-second stage reservation retained. Cumulative
metadata charges are now 4,860 HTTP attempts, 11,987,454,626 response bytes and
8,700 reserved seconds before subsequent root checks. The producer PID/group
drained. The compiler has not been executed and source-build proof remains open.

Canonical Linux verification of the accepted pure claim normalizer and unchanged
V3 decoder/archive cores passed 66 tests, exit zero, in 2.218 seconds. Log SHA256
is `11bcd31338ca447c86f1abdefccaea1fdca4650ce55b62a335a1c8c33f1f2474`.
The normalizer was committed and pushed at
`d83da557050d49fa555e976351bdd935fd8e8d0f`; no production metadata scan or global
claim spine was created. Root is integrating the reviewed header/metadata
continuations before the next source freeze, not rerunning the old failed roots.

## Recovery integration and compact pilot

The reviewed compact-metadata v4 source passed independent local QA and 132
canonical Linux fixtures, including compiler-profile integration and hard
request/receipt deadlines. Its source/test SHA256 values are
`ea075e230166ec2538f3982678eb435eb9a70e1e3f240770dddbff201615e94c` and
`7817da0839ea8d4c543f909753b2ac5dafecad4fc25a419e7770baeead7c3e6d`.
It was committed and pushed at `754d8730a7356730c3ac66ba231d588c0d1781c6`.
Independent review corrected the actual compiler source-map shape and recorded
versus actual request headers before any v4 request.

Root admitted the three-observation pilot against 4,924 prior files and the
unchanged cumulative caps. The admission SHA256 is
`c32ab846ae9e27565d1f67334cc17900847f027418daefdb15d43b81dd5b2c73`;
its external 1,009,507-byte prior inventory SHA256 is
`f832c98287903b25dcbef25007acf70677310b5d9f9f676f073b662cccd7a69b`.
PID 4071/birth 115069 ran from 20:04:39 to 20:05:00 UTC and exited zero.
All three responses were complete: 495,662 and 494,350 bytes for the fresh
prefix, and 506,299 bytes for the separately labeled old-cursor diagnostic.
Each contained 1,000 raw metadata records. The diagnostic is not fresh-walk
coverage. Completion SHA256 is
`e0d9b8c45c76ee15b56767eba049b9b20926b17559e1ad76433a3b4fe2abd9bc`;
root replay/drain acceptance SHA256 is
`cee6c87aa7545910fe911a388df0e94ae03563c1356bacdcefffcaaebc8b7285`.
Three new HTTP attempts charged 1,496,311 response bytes. Cumulative use is
4,863 attempts and 11,988,950,937 bytes. Preserve the full 300-second pilot
reservation rather than refunding its shorter observed runtime. Raw pagination
and the final universe remain incomplete.

The reboot changed Linux device numbers only. Root fully rehashed all nine
timestamp input artifacts (359,346,005 bytes) and confirmed identical paths,
hashes, inodes, sizes, modes and modification/change times. The verified data
UUID/serial stayed unchanged while device 66309 became 66305; the separately
verified root/private-authority device changed from 66305 to 66306. Certificate
`header_reboot_equivalence_v4_v1.json` is 7,275 bytes, SHA256
`dad6bc2bc9b2e63c5008bab091b8cb9100cbd39b3fff1e00d0b29060cc284bd7`.
The narrow v4 adapter preserves old OWNER/drain/source evidence and uses only
fresh identities for requests and final gates. It passed independent QA and 96
canonical Linux fixtures; log SHA256 is
`099883ba12bacce34cd7c7523e5323b6bdc478aa112722ba33df023e25b97142`.
Source/test SHA256 values are
`e8d297d0726dcc60d070fc3e986f9f961ad3d862bd0ad4e5d59e310d6394d24d` and
`c438f1df6dc925f4f66462945edafb158320244d6eeb5fad1309f1dad163702d`;
the source commit is `9f8a8ae7304d7ce0eee56bd51a01f7b3f108af33`.
No device rule, cap, filter or cohort is relaxed generally.

Root may now prepare fresh full-header v4 and full compact-metadata v4
admissions at one clean, pushed source revision. Their disjoint-input and
resource separation from the earlier acquisition contract still applies.
Keep source frozen until both producers drain; a successful compact pilot
does not admit a full universe. Independent source compilation is deferred
until metadata's full stage is terminal, so its new files and charges cannot
mutate the raw-metadata stage's prior ledger.

## Full-stage integration and detached supervision

Canonical commit `d36a8bf3af2e041d6b413b7d758abcf4f61f3b35` adds only an
exact-once allowance for the original failed-header CLI echo. The literal
558-byte `full_header_v1.log` was initially assumed to match the rostered
`FAILED.json` byte for byte. The zero-request admission correctly refused that
assumption. Its actual SHA256 is
`2e4f9ecd4b3c66969d5b60473116c0ce322f98efe640c20b8da86a54cc9fdaa9`;
the failure file's separate SHA256 remains
`c4b06c4ce038f6a4a581c51e6cad789f0051eda191b4629ec1adb44f310911f0`.
It remains charged as control; arbitrary renamed protocol artifacts and duplicate
standalone entries are not admitted. All 103 related Linux fixtures passed;
log SHA256 is `a2c83d948692a97e7bd97dd31c0a50ec262b449d218ba43fca0e1b72129a25e0`.

The pure Gamma identity helper was adopted at
`8ffc64d18f7fbb052cd761c23d2bf4133c3288d1` after 49 canonical fixtures.
Its source SHA256 is
`d97402b7ceffe8a301ea546c050359a445914df07ac20801b8fcc61ed496c5be`.
Explicit API version chooses the identity field. Missing versions and malformed
identities are quarantined; Gamma status or price never supplies an inferred
winner. This helper has not processed the full raw metadata or admitted a spine.

Full acquisitions require a reviewed detached caller for exactly header v4 and
metadata v4 full. It records exclusive START/LAUNCH/EXIT receipts, caller and
producer Linux birth/boot/session identities, and at most a 1 MiB log prefix
inside each stage's existing 2 MiB external-control allowance. Its deadline
includes preflight and cleanup, deducts authenticated inherited runtime, and
never retries or resumes. Root must independently reopen producer evidence and
owned-process drainage; receipt presence is not stage acceptance or host idleness.

The fresh admissions retain all prior failures, diagnostic charges and full pilot
reservations. The superseded first preparation reserves 10,800 prior seconds and 147,197 new
projected seconds within the unchanged 72-hour cap. Metadata preparation carries
4,863 HTTP attempts, 11,988,950,937 response bytes and 12,000 prior seconds within
the unchanged 24-hour cap. No new provider observation is authorized by an old,
consumed diagnostic approval. The later human instruction authorizes reviewed
routine continuations within the existing sample and resource ceilings.

The legacy cleaned archive cannot simply be relabeled with exact clocks: its
11-field value grain has no block identifier and can collapse observations from
different blocks. The next bridge must reconstruct that convention from complete
raw lineage, exact headers and admitted metadata, then reconcile UTC partition
migrations and full-value DISTINCT groups. A sparse corrected-parent shortcut is
not admitted without proof. Final data, results and Dropbox publication remain
unadmitted.

The fixed detached caller was committed and pushed at
`12054c03f3a6ae0187a4f6bc298a99c0a3667a4d`. All 105 canonical Linux caller/header/
metadata fixtures passed, including real `/proc` birth and session drainage;
the log SHA256 is
`03cd61730cf427b1ec88b833a807d128507e7535b29c0852bc1591d24ea886e2`.
Source/test SHA256 values are
`4bad170c635b000af3f580a63c5ac11277ed2f83e7a39b7fd81056550e44e403` and
`3e148073d1183a947d9c80263c8b0efa0987d9b05b8d886bfba5fb13646874c8`.
Full launch still requires the separately sealed admissions and root dispatch
receipts in the operational run. Its root builder makes zero provider requests,
revalidates all prior producer chains, and records current RAM/free space for the
two disjoint acquisitions. No collection is inferred from source adoption alone.

### Zero-request preflight findings and fresh admission generation

The first wrapper invocation conservatively detected its own temporary Python
parent as another compute consumer and stopped before creating admissions. A
direct invocation then sealed only the header admission draft and refused its
incorrect historical stdout assumption, before either producer output directory
or any provider request existed. Preserve
`full_header_v4_admission_v1.json` (132,288 bytes, SHA256
`730739e0d67e75fadecdecf6230bc4fc7a74b181f88236d09ca99f93063e3633`),
both v1 root programs and `full_acquisition_preflight_failure_v1.json` unchanged.

Root reopened both original error files and verified that the sole decoded
difference is `usage.new_retained_bytes.control`: 164,433 in FAILED versus
164,991 in stdout. The original `Budget.usage()` retains `self.used` by
reference; writing the 558-byte FAILED file increments that dict before main
serializes the same returned result to stdout. The narrowed recovery pins both
whole artifacts separately and permits only that exact source-proved +558
transition, with every other field unchanged. No trade data, clock or RPC charge
is corrected by this operational adapter.

The first Linux fixture attempt for that adapter failed at import because its
long fixture patch chain exceeded Python 3.12's static nesting limit. Its v3 log
and receipt remain retained. The test-only ExitStack correction passed all 106
canonical fixtures in the fresh v4 receipt; log SHA256 is
`8156f2444ca7d4fd6e2db704962b97bc96252d6725c3a32a11ea111347c949ee`.
Source/test SHA256 values are
`ce2cdc82503e788023fbb7bc0a40a6a75f5094e83798439741e00cb1a1a40e1d` and
`72ee1e20f864c9bce37a1acfc17a4651a2c299c524191ebcbdef408ae5393a93`;
the clean pushed source commit is `d03b4db77339dc55527c04af67499d9653252de3`.
No production collection ran during these preflights or fixture corrections.

Fresh v2 root programs and admissions must retain these earlier drafts, failures
and control charges. Header inherited runtime is conservatively rounded to
12,000 seconds, and metadata to 13,200 seconds, within the same original 72-hour
and 24-hour caps. Request/response ceilings, providers, sample and filters remain
unchanged. No draft status field is an accepted full stage without the separate
successful root admission-check receipt and durable caller dispatch.
