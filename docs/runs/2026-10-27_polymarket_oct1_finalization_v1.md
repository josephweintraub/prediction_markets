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
