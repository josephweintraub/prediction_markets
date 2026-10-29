# Exact-header stopped-v6 continuation v7

Status: local implementation candidate, 29 October 2026. This specification
does not assert that v6 has stopped, export a handoff, launch production, or
admit timestamps or a scientific sample.

## Authorized change

The human approved 400 method items per second with the existing provider,
initial batch100 and active HTTP1. The v7 RPC `call` method is defined in v7,
so its pacing and forecast resolve the new400 constant in that module.
Changing a constant next to an inherited v5 method would not work: inherited
methods retain their defining module's150 constant. No old helper global is
modified. Start-to-start pacing is shared across RPC instances and chain
checks. Mark the actual dispatch after source checks and durable START; idle
time does not accumulate burst credit. Numeric429 remains a whole-batch retry
with2/4-second delays. Other retry, refusal, size-splitting and transport rules
remain unchanged. A durable START is conservatively charged before transport.

The inventory remains32,901,442 exact keys with all native expected-hash
assertions preserved through the same authenticated required-key file.
Reuse only complete sealed chunks by reference. Every unsealed chunk is
refetched as a whole under its remaining original attempt allowance. Do not
copy old headers, infer new coverage, reinitialize counters or restart the
inventory. The old v6 HEAD is045deabcdb9b8a295680d1b6e3411b85fe80eeea.

## Stable admission interface

The collector is `scripts/collect_polymarket_exact_header_campaign_v7.py`.
Its CLI matches v6. The schema is
`polymarket_exact_header_campaign_admission_v7`, status
`root_exact_header_campaign_admitted`. Top-level keys are exactly the v6
admission keys. The new run is
`/mnt/data/runs/2026-10-29_polymarket_exact_header_campaign_v7`.

The `prior` object has exactly four fields:

- `base`: the literal original v6 admission's prior, not an edited projection;
- `stopped_v6`: a strict absolute path/bytes/SHA reference to
  `full_header_v6_stopped_handoff_v1.json` in the Oct27 operational directory;
- `rpc`: cumulative HTTP attempts, method items, response bytes and elapsed
  seconds, never smaller than durable or previously published usage;
- `external_controls`: one strict reference to the shared external closure.

The reboot adapter passes only `base` to the frozen v6 validator. The same
boot/volume/current input identities and private-authority content bindings
must still pass the unchanged old certificate checks. A reboot is not silently
authorized by v7. `source_snapshot`, `previous_source`, `validate_inputs`,
`validate_reboot`, `validate_prior`, and `forecast_seconds` are the preparation
entrypoints. `validate_prior` returns the standard Budget prior structure.

## Stopped-prefix handoff

The handoff has schema `polymarket_header_v6_stopped_handoff_v1`, status
`root_bound_stopped_v6_header_prefix`, and exactly:

`schema_version,status,run_dir,source_head,refs,files,runtime_charge_seconds,`
`chunk_runtime_seconds,raw_clocks_only,final_sample_admitted`.

`refs` has exactly owner/admission/launch/exit/drain strict references.
The first three are pinned to the original v6 bytes. EXIT is
`full_header_v6.EXIT.json`; drain is
`full_header_v6_root_drain_v7_handoff_v1.json` in the same operational directory.
Old source blobs must equal the committed old producer identity. The OWNER,
caller and child births must actually be dead, never merely absent from a
stale JSON claim. EXIT must prove drained ownership, observed stdout EOF,
unchanged source/admission, and the accepted terminal case. A controlled
caller interruption is accepted only as `caller_failed_closed` with
`caller_interrupted`, producer exit-15 and all drain checks. No FAILED file is
required for a producer killed directly by SIGTERM.

The root drain has exactly schema_version/status/run_dir/source_head,
owner/launch/caller_exit references, header_owned_processes_drained,
remaining_owned_group_members, no_owned_workers_or_transfers, host_idle_claimed,
final_sample_admitted, checked_utc and external_control_closure. Schema is
`polymarket_header_v6_root_drain_v7_handoff_v1`, status
`root_verified_stopped_header_v6`. Drained flags are true, remaining members
empty, host-idle and final-sample claims false. checked_utc is canonical aware
UTC. The closure explicitly preserves old ledger entries, the ledger itself,
and actual old future controls. No operational-directory sweep substitutes
for this explicit evidence.

`files` is the exact entire v6 physical file roster, each entry containing
path/bytes/SHA/category/chunk. Symlinks, extra files, missing files, unknown
paths, duplicates and incorrect categories block. Unsealed headers remain
failed bytes. Every sealed chunk is fully reopened against its original keys,
native expected hashes, request/projection journal and adjacent chunk clocks.
Only those seals contribute coverage. Existing attempts and per-chunk runtime
remain charged. A new v7 run does not grant an additional third attempt.
Handoff chunk times are fresh v6 charges. A v6 seal's active time already
includes the historical chunk floor: compare it against historical plus fresh
time, adding that historical floor exactly once.

Complete journal records retain the unchanged strict parser semantics.
An open durable START charges its full16MiB allowance and full HTTP/method
reservation. An incomplete last record shorter than384bytes remains retained
and charges an additional conservative1 HTTP/100methods/16MiB, never coverage;
a sealed chunk cannot have such a tail. Larger/unknown malformed records block.
This explicit extra debit may overstate usage; it cannot refund a request.

## Conserved resources and publication gates

Runtime is base prior plus observed v6 wall time and newly charged preparation
exactly once. Concurrent local development is already inside v6 wall time,
not billed again as independent elapsed work. The preparer anchors only fresh
post-drain preparation to drain.checked_utc. All historical conservative
reservations remain. No steady-state forecast is replaced with the maximum
retried/timed-out call: the old measured pilot/observation bounds remain, with
the new paced400 rate and3600-second forecast reserve.
Apply all three feasibility lower bounds to the verified remaining unsealed
key count, not the full inventory. Exact sealed widths are revalidated before
subtracting them. Final coverage and the final ordered seal reopen still use
all32,901,442 keys, including carried seals.

All original ceilings remain:72hours;1.3M HTTP attempts;130M methods;
2TB charged responses;16MiB per response;8GB global header artifacts with
unchanged baseline/accepted/control/failed partitions;8GiB address space;
20GB free floor; zero spill; and all original per-chunk limits.

The external ledger has exactly schema_version/files/future_reserved_bytes/
future_reserved_files/ceiling_bytes. Schema is
`polymarket_header_v7_external_control_ledger_v1`. The SAME2,097,152-byte,
256-reference pool includes all historical closure files, actual old future
controls, new handoff/proofs/source copies, the new ledger's own bytes, and
at least1,245,184 future bytes with9 future references. This partition is
960KiB log,192KiB admission,16KiB check, three8KiB caller controls and three8KiB
root controls. The ledger does not hash itself. No stage-specific fresh pool
is added. Failure to fit blocks publication.
The prospective v7 log prefix is64KiB smaller than v6 so mandatory declared
controls fit the unchanged shared pool. Full stdout byte counts and hashes
are unchanged; no already retained bytes or historical charges are refunded.

Before dispatch and final sealing, reopen source, admission, reboot evidence,
all prior payload/control files, private-authority identity, and exact file
sets. Keep fresh output exclusive and publish completion only after full
reconciliation and deadline checks. The root alone may stop v6, export actual
evidence, synchronize reviewed source, obtain Linux QA, commit/push, prepare
admission and launch v7. Independent review of the first fresh seal and new
monitor bindings remains a separate root gate. Literal raw clocks are not
final-sample, semantic-bridge, estimates or release admission.
