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
