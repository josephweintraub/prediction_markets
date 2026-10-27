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
