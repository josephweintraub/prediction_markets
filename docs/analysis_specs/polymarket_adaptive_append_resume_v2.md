# Polymarket adaptive append resume v2

Status: implementation and canonical fixtures passed; bounded execution pending, 26 October 2026. The user
approved smaller requests and resuming preserved work after the response-size
failure. This contract changes collection mechanics only. The
[final-sample contract](polymarket_final_sample_refresh_v1.md) and
[capacity envelope](polymarket_refresh_capacity_v2.md) retain their scientific
definitions, exclusive `2026-10-02T00:00:00Z` cutoff and downstream gates.

## Immutable sources and scope

Preserve `collect_polymarket_native_increment.py` and
`run_polymarket_native_collection_campaign.py` byte-for-byte. Implement v2
entrypoints and fixtures separately, sharing reviewed validation helpers where
safe. Never run changed code with `--resume` on the old campaign directory.

The old campaign is
`/mnt/data/runs/2026-10-26_polymarket_native_append_campaign_v1`, produced at
`19202a820bef569f449a34fc731107f16470e155`. Bind its PLAN file SHA256
`c4fba53915536077f3619ac3fae352edbd509722d6d223f54b4c2f4973917526`
and internal plan SHA256
`364a81171cb31f098e1876832ad9a703c6ead963862fc56cd2084a44a1ac6682`.
Reopen its source, run, contract, COMPLETE, manifest, SUCCESS, EXIT, INVENTORY,
payload and durable-request bindings before reuse. Reject live or unknown
recovered processes instead of duplicating their work.

V2 retains the original 582-chunk grid, blocks 88,979,538–94,796,039. Import
exactly 19 accepted old chunks (`c000001`–`c000017`, `c000019`, `c000020`)
by read-only reference, not copying or rewriting payloads. Recollect the whole
failed `c000018` into fresh output; its 75 partial windows remain audit-only.
There are 563 pending chunks, covering 5,626,502 blocks. The separately
accepted first 1,000-block probe remains a distinct source outside this grid.

New campaign output:
`/mnt/data/runs/2026-10-26_polymarket_native_append_campaign_v2`.

## Smaller requests only when necessary

Begin with the original 50-block base windows. Only a proved
`eth_getLogs` response-size failure may split an interval. Write an immutable,
request-bound SPLIT receipt, then partition `[a,b]` into `[a,m]` and `[m+1,b]`,
where `m=floor((a+b)/2)`. Recurse no smaller than one block; an oversized
singleton fails closed. Transport faults, malformed data, conflicting native
identities, header drift or exhausted resources do not authorize splitting,
dropping records or relaxing validation.

The unsuccessful parent request and its bytes remain charged. Only successful
terminal leaves enter the collection manifest. Prove exact address/topic
predicates, explicit successful-zero intervals and no-gap/no-overlap leaf
coverage of each base window and original chunk. Resume must reconstruct and
validate the saved split tree before new requests. Ambiguous, contradictory,
unbound or duplicate coverage blocks completion.

## Unchanged finite ceilings

Campaign ceilings remain 128,000,000,000 retained bytes, 900,000 RPC attempts,
3,000,000,000,000 response bytes and 172,800 cumulative seconds. Each child
retains the 2 GB reservation including controls, 1,200 attempts, 4 GiB response
bodies, 3 million rows, 8 GiB address space and 3,600 seconds. Every individual
response still has a 16 MiB ceiling; the extra detection byte is charged but
never treated as a successful response. Keep the 20 GB free-space floor and
all in-flight reservations. A finite ceiling can stop an incomplete run; it
does not prove sufficient capacity for unknown dense periods.

Charge the unique physical union of the six original prior directories and
the old campaign, including failed/partial/control files, once. Hierarchical
inventories of parent controls and bounded child attempts avoid constructing
an unbounded flat file list. Recompute journal usage and reconcile against
old terminal totals: 45,663 charged calls and 76,324,722,328 response bytes.
Those totals already include conservative diagnostic reservations and accepted
children. Do not add either group again. Reinventory current retained bytes:
the old 1,828,839,120-byte result field preceded terminal RESULT/FAILED writes.

For runtime, include frozen original-prior charges and the old parent's
1,307.7926040820003 session seconds, not the sum of parallel child runtimes.
Sealed stopped downtime is not execution time. Preserve restored-state,
expensive validation/hash and terminal-publication deadline gates. There is no
automatic cap increase, file deletion or unreviewed retry of other failures.

## Admission, initial exercise and monitoring

### Explicit identity admission after reboot

The approved restart changed Linux's transient `st_dev` identifier from
66,304 to 66,309. The mounted filesystem UUID, NVMe serial corresponding to
the same EBS volume, ext4 type and 805,306,368,000-byte capacity agree with
the preserved expansion witness. The strict old plan reader correctly refuses
the changed identity; do not bypass it or weaken the historical helpers.

Use a separate, reviewed reboot-admission helper to produce a fresh inventory,
fresh native-header-validated source plan and immutable equivalence witness
under `/mnt/data/runs/2026-10-26_polymarket_reboot_admission_v1`.
Pin the old expansion receipt SHA256
`568ba4ef26fa24ba2e0559c8a78f40a40fea55520181ee761592fbb1dac11559`,
old inventory SHA256
`869dbef6c7cc72c1a44caba81725bef5339a6db23f4bb1f678b375f54045d48e`,
old source-plan SHA256
`8dc0aa67cae304072ccaa3be3660c4e06e13e75db68e91403ddcde336fe383ce`
and the old campaign/result identities above. The witness must prove the live
UUID, EBS serial, capacity, boot identity and fixed device-number mapping.

Require exact equality of every original input and repair-manifest field
except the specifically listed `entry.device` and `target.device` leaves
inside identity records. In particular preserve all six regular files,
missing deduped input, paths, inodes, ownership, modes, sizes, nanosecond
modification/change times, topology, footer hashes, schema, counts and extrema.
Bind the old inventory/source-plan files themselves by digest and witnessed
device-only physical-identity changes. Reject arbitrary device stripping or
any other changed field. Require identical old/fresh cutoff, append bounds,
raw-tail header, cutoff headers and known exchange roster after fresh native
revalidation. This is not a whole-file body hash or a history-completeness claim.

New collectors use the fresh plan through the unchanged strict reader.
Imported old chunks retain their exact original plan binding and producer
HEAD. The campaign explicitly validates and freezes the bridge linking those
bindings, then reopens it at resume and before completion. Producer ceilings:
100 RPC attempts including failures, 256 MiB response bodies, 1,200 seconds,
8,000,000 output bytes, 8 GiB RAM, zero spill, existing footer-read budgets and the 20 GB
free floor. Conservatively charge those request/response/time ceilings once
plus actual retained admission bytes inside the original campaign envelope.
Do not reset the old once-only baseline or charge its six roots twice.
Any failed admission attempt remains preserved and must also be charged before
a separately approved retry; no automatic retry is authorized.

### Execution gate

Require committed clean source, focused fixtures, independent adversarial QA
and canonical-runtime testing. Test nested splits, singleton refusal,
interrupted-tree replay, malformed payload refusal, cumulative charges,
noncontiguous reuse, changed old evidence, hierarchical inventories and
expired all-accepted publication. Re-run original collector/campaign/overlap
regressions and verify their source hashes stayed unchanged.

Independent review passed 191 focused fixtures, including 16 reboot-admission
fixtures and adversarial mutation/deadline checks. Canonical Python testing
passed 255 tests and 187 subtests in 18.87 seconds. The original collector and
campaign source hashes remain unchanged. Runtime and launch receipts must
still prove production admission; fixture success does not imply collection.

Launch only the first fresh pending chunk (`c000018`) until it is accepted.
Imported old chunks do not satisfy this initial gate. Only then allow up to
four concurrent independent children. Root observes the first new accepted
chunk and its actual request/split/leaf evidence before handing off to quiet
background monitoring. Failure stops launches and drains bounded children.

Re-enable the existing current-chat heartbeat with exact new run, source and
parent birth bindings. Healthy progress stays quiet. On completion or failure,
root independently checks all saved process births and host consumers, saves
small receipts, stops EC2, verifies stopped and pauses monitoring. Subagents
never manage infrastructure. No raw completion admits exact interior clocks,
V3 economic semantics, metadata, sports cohorts or a final paper sample.
