# Exact-header same-volume reboot recovery v6

Status: local reviewed-implementation candidate, not production admission.
Preserve every v3/v4/v5 source, control, failed request and unsealed output.
The final refresh cutoff and scientific representation are unchanged.

## Sole source-versioned change

Header v5 already implements bounded numeric429 retries, two/four-second
backoff, initial batches100, shared150-method-item/second start-to-start pacing,
and splitting only on proven oversized/batch-refused responses. V6 reuses the
same RPC, transport, Budget, limits and validation classes. Its orchestration
differs only in version/source bindings and reboot/prior-proof adapters.
There is no new provider observation, sampling, partial adoption or cap override.

The old v4 certificate pins boot1ad746ce and cannot authorize a later boot.
The fresh SHA-bound certificate binds the actual current boot and same ext4
volume UUIDd0cd087b-94c4-428c-bae9-ae28929059f6,
serialvol0acbba4565a64f792,750GiB and fixed root-volume UUID.
It retains the exact previous8377-byte certificate
SHA89efc5894cb2e60671242c1abe348178e09b9c7826170441e85a71e9a614da7d,
original v3 OWNER/drain, and the fixed v4 OWNER/drain.
The v6 environment reader obtains the mounted SOURCE from findmnt, verifies
it is the actual block device by rdev, and queries that exact source for serial
and size. It does not assume any fixed NVMe enumeration or manage mounts.

For each of exactly nine named inputs it records old-v3, old-v4 and current
whole path/bytes/SHA/stat bindings. Every current payload is fully rehashed.
All non-device identity fields must agree; device changes require the actual
verified mount mapping. Original1000-key request bytes, normalized keys and
full nested identity are reparsed and reconciled. Private authority content
stays private: preserve its exact reference/hash and full identity, never
copy its content into a receipt.

Only prior validation receives the explicitly certified historical-v4 input
projection. The frozen v5 validator retains its old producer/blob/drain,
journal/counter, stdout-alias, scope and classification gates. Returned prior
file bindings are freshly rehashed. New OWNER, request checks, collection and
final full reopens use actual current identities. No old helper global is
modified, and no generic stat equality exception is introduced.
The final seal rehashes current bindings after historical validator callbacks,
then rechecks source identities and the deadline. Interrupted or late-mutated
admission/certificate/source evidence cannot publish COMPLETE.

## Fixed preparation and publication

The production-only, zero-HTTP entrypoint is
scripts/prepare_polymarket_exact_header_campaign_v6.py. It accepts only a
SHA-bound literal header_v6_preparation_request_v1.json in the existing
Oct27operational directory and an expected clean committed HEAD.
The request has exactly:

- schema_version=polymarket_header_v6_preparation_request_v1
- status=root_ready_for_bounded_header_admission_preparation
- source_head,host_proof,linux_qa,source_commit
- extra_prior_controls,extra_external_controls
- root_complete_header_control_roster=true

Refs are exact path/bytes/SHA triples. Request/proofs are immutable, current
HEAD/blob-bound and rechecked before the final publication. Host proof uses
polymarket_header_v6_fresh_host_preflight_v1 /
root_verified_header_recovery_host and exactly the fields implemented by
host_gate: checkedUTC<=300seconds, current environment/boot, clean HEAD,
same instance, current resources,603-or-more saved receipt checks, zero
live/unknown births, no research/notebook/transfer consumers, agents released,
explicit monitor-ancestry checks and no running production stage. Root owns
the full census and every lifecycle action.

Linux QA binds the exact six-module QA_COMMAND, exact frozen loader count,
zero skips, exit0, full source union and bounded log. The source-commit proof
binds the same source map and clean pushed HEAD. The source union includes the
collector, caller and preparer dependencies and this specification.
The current exact loader count is209; local Linux-only skips are not accepted
as canonical production QA.
Use full_header_v6_linux_fixtures_v3.json and its v3.log for the final HEAD.
The v1/v2 receipts and logs remain immutable after the preproduction metadata
compatibility fixes, including its exact nested source-lookup root and pinned
historical process-receipt birth proof. Bind and charge all retained v1/v2
files once in the same external-control closure; do not overwrite them or
substitute their prior HEAD/source maps for the v3 proof. Root publishes the
current-HEAD header QA v3 only after the bounded metadata/compiler gates pass.

The new run is
/mnt/data/runs/2026-10-28_polymarket_exact_header_campaign_v6.
The operational directory receives exclusive certificate, external ledger,
full_header_v6_admission_v1.json and
full_header_v6_root_admission_check_v1.json.
The check is staged, fsynced, fully reopened and committed atomically last.
After historical callbacks, the preparer fully reopens every accepted prior
file's content and stat binding, including inherited OWNER/journal evidence,
as well as the physical file sets and direct/nested controls.
It is not a producer launch or accepted timestamps.

Carry the exact v4 admission, five owned v4 files, five required operational
proofs, charge attestation, old post-admission check, CALLER and dispatcher
COMPLETE. Extra header controls must be explicitly rostered; no OP-directory
sweep or invented absent files. Both original START files remain untouched,
outside the consumer standalone roster, hashed in the root external closure.
Their existing combined3247bytes are not zero-cost omissions.

The external closure has one unchanged2MiB/256-reference pool, not one per
stage/file. It includes actual old STARTs, fresh preparation/proof sources and
receipts, ledger bytes and a fixed1769472-byte future reservation:1MiB log,
up to256KiB admission, and seven64KiB JSON control allocations. Already
consumer-rostered producer files keep their original ordinary categories;
the external pool is not substituted for their accounting. No current
admission hashes itself, and no future check is inserted into its own input.
Root must reopen the full external closure before dispatch and terminal
acceptance. Failure to fit blocks publication rather than increasing limits.

## Unchanged cumulative gates

Required keys32901442; full coverage and all5884493 native hash assertions
remain required. Re-fetch the full unsealed first chunk. Keep318HTTP,
22010methods,174515481response bytes and at least14400runtime seconds.
The fixed conservative fresh-preparation anchor is2026-10-28T17:45:44Z,
not an asserted exact human-start time. Charge all wall time from that anchor,
plus1800seconds: this operator's hard1200 and600for remaining root controls/
dispatch. Never refund idle time or inherited conservative reservations.

Runtime remains259200seconds; the paced forecast is at least222943.34seconds
for the unchanged inventory, including3600forecast-reserve seconds. Include
measured pilot/observation bounds and actual preflight time. Fail before HTTP
if remaining runtime cannot fit. Other unchanged maxima are1.3MHTTP,
130Mmethods,2TBresponses,16MiB per reply,8GB header artifacts,8GiB address
space,20GB free floor and zero spill.

Chunk0already used a01/a02. Its next a03 is its last allowed attempt;
all existing per-chunk charges and three-attempt ceilings remain. No small
production pilot may consume it. Root independently reopens the first
100k-key seal before quiet background handoff. Sparse unoccupied blocks are
not missing trades. Successful completion admits literal required-key RPC
clocks only, never the final sample, semantic bridge, estimates or release.
