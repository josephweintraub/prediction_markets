# Exact-header fresh admission v8

Status: reviewed-source implementation and synthetic fixtures only until the root
admits an actual cold-boot run. This stage collects raw block-header clocks, not
a scientific trade sample or proof of historical market coverage.

## Fixed scope and unchanged limits

The only newly authorized retention change is the caller's saved stdout prefix:
512 KiB (524,288 bytes), with the complete stream's byte count and SHA-256 still
recorded. The shared external-control pool remains 2 MiB and 256 references. Its
nine future files reserve 786,432 bytes: the 512 KiB log, 192 KiB admission,
16 KiB check and six 8 KiB caller/root receipts. Materialized historical controls
are never refunded or rewritten to make room.

All other v7 collection contracts remain unchanged: the exact 32,901,442-key
inventory and native expected hashes, 400 method items/second, batches at most
100, one active HTTP request, no catch-up bursts, original 72-hour cumulative
runtime and all request, byte, storage, address-space, free-space, attempt and
spill ceilings. Retries retain their request and response reservations.

The fixed new run is
`/mnt/data/runs/2026-10-29_polymarket_exact_header_campaign_v8`.
Controls use new v8 names inside the existing October27 operations directory.
No old producer, caller, admission, journal, payload, receipt or source is edited.

## Cold-boot certificate and historical carry

The prior has exactly `base`, `stopped_v6`, `rpc` and `external_controls`.
`base` is the literal old6 admission prior. The fixed stopped6 handoff preserves
all 413 files, 102 complete seals and the unsealed chunk102/a01 bytes; the next
unsealed attempt is chunk102/a02. Only independently verified seals add reused
coverage. Open durable requests and short torn journal tails retain conservative
debits, never inferred successful responses or adopted headers.

The new certificate `header_reboot_equivalence_v8_v1.json` is written before the
new ledger and rostered there, not inserted into the historical base. It binds
the old3/4/6 owner/certificate/drain chain, actual old6 stopped handoff, expired
unlaunched7 admission and immutable terminal checkpoint. The checkpoint is
historical evidence, not a current host-idle assertion.

All nine inputs, the original1000-request normalized-key fingerprint and private
authority are rehashed. Only the filesystem device number may change. Inode,
size, timestamps, mode and all other identity fields and content remain exact.
The private authority uses the pilot's full nine-field identity, not the generic
six-field file capture. The mounted data UUID, serial, 750 GiB size, root UUID,
filesystem and fresh boot are checked. The live context keeps the actual current
original-request identity through the frozen historical validators. Certified
historical6 projections apply only at the historical OWNER6 equality gate.

The old7 ledger's exact58 controls plus the ledger, expired admission/check and
failed preconstructor receipt form the pinned62-reference, 981,353-byte retained
union. New controls and source paths use finite explicit allowlists. Every old
and new external reference is included in the same pool.

Failed Linux QA1 and its complete log remain mandatory retained controls. The
consumer and preparer reopen both exact files; all43 QA1 source entries are
checked against their immutable committed epoch8542073491092e1acf35f83bca4639596002d494,
not against corrected live source. A new QA2 receipt/log uses distinct v2 names
and must attest the new committed source. The old failed receipt cannot satisfy
the current pass gate. The root runs the new fixture suite in a fresh child with
a separate300-second supervisor timeout, so the supervisor's alarm is not
inherited by tests that inspect their own timer state. Prior assertions and
historical sources remain unchanged. Both QA runs count in the same2 MiB pool.

The preparer carries stopped6 runtime, all time since its canonical UTC drain,
the existing104777-second floor and one fresh1800-second preparation reserve.
The consumer independently captures its entry UTC and requires declared prior
runtime at least the maximum of104777 and the ceiling of base runtime plus
stopped6 charge plus elapsed time since that drain. It does not add another1800.
Its own preflight duration is charged separately against the same72-hour ceiling.

## Admission, collection and publication

Fresh root census, ownership, source-commit, resources and fixture evidence are
required; a historical drain cannot establish current idleness. The root alone
manages instance lifecycle and launches the fixed caller once. The preparer
writes an immutable v8 certificate, ledger, admission and check. All proof
readers fail closed on missing, extra, changed, symlinked or unrecognized files.

The v8 consumer retains the v7 transport, durable request journals, pacing,
attempt conservation, native-hash, integer-clock, adjacent-parent and seal
semantics. Required new runtime depends on remaining unsealed keys, not all keys.
Historical and new payloads retain final full content/identity and exact namespace
checks. Callbacks run before the final primitive reopen. Completion is staged,
rechecked and atomically published without replacement; late changes withhold
completion and preserve investigation evidence. OWNER, manifest and root
completion schemas are explicitly v8, and OWNER records producer entry UTC.

Focused tests use synthetic files, simulated clocks/transports and OS/Git fixture
boundaries. The cold integration exercises the real v3/4/5/6 prior validators,
real cold certificate and actual-current device-hop request identity. No API,
production input scan or instance operation belongs in local fixture execution.
