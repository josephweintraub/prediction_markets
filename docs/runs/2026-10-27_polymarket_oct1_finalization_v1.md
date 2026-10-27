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
