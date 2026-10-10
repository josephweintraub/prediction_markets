# Legacy wallet-attribution repair

**Status:** Stage6 writer safeguard committed and tested on EC2; approved archive
verified and its exact local targets removed. The immutable historical repair
completed successfully and passed saved-evidence QA on 10 October 2026.
Current nine-sport downstream adoption completed separately in the
[10 October run](2026-10-10_sports_wallet_adoption_v1.md). Older studies remain
unadopted; the historical repair receipts below are unchanged.
**Authorization:** user requested the wallet-attribution fix on 9 October 2026,
then requested investigation of disk cleanup instead of immediate expansion.
The user subsequently approved archiving exactly the three named datasets to
Dropbox, verifying every copied file, then removing only verified local copies.

## Scope and preserved definitions

Correct the legacy published counterparty wallet assignment without changing
prices, cash, outcomes, timestamps, cohort, row multiplicities or inferred sides.
The published non-maker representation should reverse the maker wallet pair.
This identity correction does not establish the counterparty's own economic
action, native collection completeness, genuine replay removal or holdings.

Historical ROOT/CLEAN, previously published wallet flags, saved encoded
bases/codes and scientific results remain unchanged by this repair. The repair
publishes separately gated immutable copies;
publication does not adopt those copies in existing readers or analyses.
Never run the old in-place cleaner or destructive Stage6 writer to perform this
repair. The new writer safeguard prevents the diagnosed projection error but
does not change that writer's existing output-deletion behavior.

## Writer safeguard

Canonical baseline: `24cbbb535c1e025e45ca2d4562d108772f504cd5`.
Reviewed original `pipeline/transform/build_trades.py`: 29,089 bytes, SHA256
`b3ad89521c9cf22874a7cbc8f869bb3b167ec6db68fb92958bc190b46faf6d28`.
The original source is retained in the local task's `source_inputs_v1/`.

`_guard_stage6_wallet_projection` disables only `common_subplan`, when available,
and preserves the connection's prior disabled-optimizer settings. Stage6 calls it
before output mutation or COPY. A direct Stage6 invocation must also pass the
production-host guard before opening its connection. Other stages and global
connection settings are unchanged.

The exact Stage6 expansion COPY source retains SHA256
`be2b10e0f338ae9b9adec236d64e56ae4f2a4dbcf401bfaa67628bfed8402d0f`.
Historical runtime probes and their frozen source/blob contracts were not edited.
Patched writer: 29,835 bytes, SHA256
`b30ba9501fc0e84d56c5873f15b8f85930372814b66bd0dc0e95ece223d350c5`.
New `tests/test_stage6_wallet_projection_guard.py`: 9,682 bytes, SHA256
`a3690364ee1e8e75def554a667f8507f4bc7c268622b6d93c48c7fae7f3f5d85`.

Tests parse the source and execute only the extracted guard and COPY on tiny
fixtures. They do not import the side-effecting builder or execute Stage6.
Author review: 61 tests passed on DuckDB 1.5.0; DuckDB 1.4.4 completed 61 tests
with one expected version-specific skip. Root's 1.5.0 integration repeated all
61 tests successfully in 5.164 seconds. Independent review passed 18 focused
tests on 1.5.0 and 18 on 1.4.4 with one expected skip. An independent seven-input
oracle passed 12 cases on 1.5.0 and eight on 1.4.4, including fallback timestamps,
self-wallet rows, duplicate multiplicity, invalid-price rules and prior settings.
Canonical integration passed all 61 tests in 8.061 seconds. Writer commit:
`058f5117e528b293ada80032040b4bfcdf86f721` on
`codex/wallet-attribution-repair`.

## Historical repair design and capacity

The completed frozen 44-month census proves exact copied-pair construction for
both ROOT and CLEAN. A transformation swapping `proxyWallet`/`counterparty` only
for `is_maker=false` is bijective: all other nine fields, roles and multiplicities
are preserved, and it commutes with full-row DISTINCT. Preserve self-wallet and
reciprocal collision classes. Bind the transformation to the original frozen
input identities: applying it twice restores the defect.

Physical immutable Parquet copies preserve reader compatibility better than a
read-time view. A view requires explicit adoption by every raw reader. Neither
option repairs already encoded bases, wallet codes, old flags or estimates.
Corrected inputs need a new version and independent membership/payload checks.
Historical repair commit: `177eb026ccb2c0c8f5a37203926740aceda45368`.
The canonical combined fixture suite passed 102 tests in 10.345 seconds.
Independent review passed 21 focused tests on each of DuckDB 1.4.4 and 1.5.0,
plus all-field/binary64, real write-ceiling, no-Parquet-scan comparison and late
input-layout-drift oracles. All 616 relation/leaf pairs are admitted by saved
metadata; maximum paired payload is 13,050,658,294 bytes. COPY has a temporary
enforced 2x-original-byte ceiling; each leaf is scalar-admitted before bounded
DuckDB materialization, exact comparisons and table disposal. Disk spill is zero.
Production preflight, body and saved-evidence QA passed. Coherent downstream
adoption must bind every consumed input to its documented generation. The
current nine-sport run refreshes flags while preserving native exact buyer IDs;
it does not adopt older wallet codes or encoded bases.
Never point existing loaders at repaired data while silently retaining old flags.

The saved-evidence checker is independently reviewed and released at
`b9952ada1f04c521aa238f34192103f3094e12fd`. The final canonical focused suite
passed 133 tests in 22.810 seconds. Independent metadata oracles reconcile
88 files and 616 relation/time leaves, and 18 rehashed corruption cases fail
closed. This checker does not independently re-scan or rehash Parquet data;
it validates strict saved producer evidence, source identities, count laws,
command/stdout/timing bindings and unchanged certification/adoption limits.
Canonical source remained frozen at that commit throughout production execution
and its saved-evidence QA.

Root verified the mounted data UUID
`d0cd087b-94c4-428c-bae9-ae28929059f6`. Existing ROOT/CLEAN contain 44 files each,
with apparent sizes 41,335,439,747 and 23,800,759,225 bytes. Before cleanup, free disk
was 24,635,293,696 bytes. Simultaneously retaining originals and new copies requires
more capacity plus spill/free-space headroom. AWS `DescribeVolumes` is not
authorized for the current IAM user; no volume modification was attempted.

## Read-only cleanup inventory

The initial inventory inspected filesystem metadata only. Temporary directories
offer under 3 MB, not meaningful
repair capacity. Current/raw tables, caches and completed research are protected.

Possible archival targets, not established disposable data:

| Path | Allocated bytes |
| --- | ---: |
| `/mnt/data/pipeline_root_output/trades_no_event_slug.parquet` | 39,481,602,048 |
| `/mnt/data/pipeline_root_output/trades_snap20260624.parquet` | 38,759,682,048 |
| `/mnt/data/telonex/quotes_ticks` | 70,073,913,344 |

Directory names and dates alone do not prove redundancy or recoverability.
Historical copies retain provenance value; quotes would need restoration for
future use. The approved archive requires source/backup verification, exact target
resolution and a recoverability manifest before any local removal. Dropbox's
read-only quota check returned 1,889,577,118,009 bytes free.

The admitted inventory comprises 168 files, 101 directories and
148,314,013,690 apparent file bytes. Quotes have an active daily-rebuild and
acquisition/merge dependency; the archive includes restoration instructions.
Destination: `dropbox:Polymarket Data and Code/Archives/2026-10-09_disk_cleanup_v1`.
Archive completed in 36:29.52 with GNU-time exit 0, an empty error log and a
VERIFIED 168-file receipt. Both data and restoration controls passed content-hash
checks. An independent review reconciled exact inventory, byte totals, admission
binding and restoration paths. The stale local SSH transport was closed only
after the remote producer had finished; transport exit 255 is not the producer's
recorded exit 0. Subsequent connections use explicit keepalive settings.

After confirming no active process held the approved targets, root ran the separate
removal phase. It independently rechecked current remote content/controls and
local source identities before removing only manifest-listed nodes. Removal exited
0 with an empty error log. All three approved roots are absent; current ROOT/CLEAN
remain present. Available space after cleanup was 172,949,794,816 bytes. The datasets remain
recoverable in Dropbox; restore quote ticks before workflows that consume them.
No volume expansion or other deletion occurred.

The metadata-only repair preflight completed with exit 0 in 3.99 seconds, binding
88 inputs and 65,136,198,972 input bytes to the original frozen census and QA.
It requires 135,733,381,752 free bytes, including capped outputs and reserve,
against 172,949,794,816 observed free bytes. Planned charged reads are
6,738,973,385,852 bytes. Its technical key `leaf_count_per_relation` is misnamed:
616 is the total ROOT+CLEAN relation/time checks, 308 per relation. Both the
producer and QA use that total consistently; no scientific sample count depends
on the label. Independent admission review passed against all original metadata,
source hashes, count laws and capacity bounds. Root confirmed no competing scan,
notebook or transfer held ROOT/CLEAN before starting the serial repair body.
Root owned the execution; a delegated read-only agent monitored saved progress,
process resources and disk reserve. Canonical source remained unchanged while
the run was active. Large delivery/scheduling gaps in monitoring tools were not
treated as proof of a stall; subsequent fresh snapshots showed continued work.

Archive helper commit: `7ff63964559d77b3b817a09faac7064bb338c97f`.
The canonical 20-test focused suite passed in 0.135 seconds; root's local
archive/production-guard suite passed 26 tests in 0.481 seconds. Two optional
local-only guard-test modules were absent in the canonical checkout; their first
combined invocation failed imports, and the scoped canonical suite was rerun.
Independent review passed 20 focused tests plus six independent safety/hash
checks. No library or environment installation was performed.

## Published repair and saved-evidence QA

The body and its root-owned SSH session both exited 0. All 88 files covering
November 2022 through June 2026 passed exact expected-wallet and unchanged-field
multiset checks before atomic publication. ROOT retains 2,114,623,452 rows and
CLEAN retains 2,036,128,538 rows. These are expanded published-table row counts,
not counts of distinct native executions; do not sum them as independent fills.
All 616 relation/time leaves reconcile, with no recorded full11 or other9
differences, unchanged schemas/roles/multiplicities and original-input reopen.

Corrected tables are separate immutable copies under
`/mnt/data/runs/2026-10-09_polymarket_wallet_attribution_repair_v1/{root,clean}`.
Original current ROOT/CLEAN, old flags, saved code maps/bases and scientific
results were not replaced or rerun. Identity correction is not certification of
native economic actions, holdings, data completeness or causal profit taking.

The published manifest is 1,582,547 bytes, SHA256
`f142313238e9be25541f97dac4ba24de7ad6b49484812e81e7c6640eefd50daa`.
Its exact summary projection is 1,582,636 bytes, SHA256
`255db24f0fca0723c8ec2c90d4ae84254baa399961b6c29c4ad6930d49fbf277`.
The body took 6:39:41 by GNU time, with peak RSS 31,285,293,056 bytes and no swap.
Declared Parquet output is 71,186,314,005 bytes. Charged reads total
4,835,032,557,207 bytes: this conservative scan-accounting footprint is not
measured physical disk I/O. Available space after repair is 101,758,840,832 bytes.
The error log contains only 88 file-completion progress records.

The released metadata-only checker exited 0 in 4.02 seconds, with an empty error
log. Its immutable receipt is
`/mnt/data/runs/2026-10-09_wallet_repair_saved_manifest_qa_v1/receipt.json`,
22,305 bytes, SHA256
`06d0fd376c7c6aede78016e50bc34cd63220cf8a15d481598303679d7de2cbb3`.
It validates saved producer evidence and frozen input/source/execution bindings;
it does not independently decode or content-hash repaired Parquet. Both the
producer and QA retain `data_certified=false` and `downstream_adoption=pending`.
Local retained summary/receipt hashes match their remote counterparts.
Independent local review approved those saved bindings, all 2,464 comparison
charge records, exact summary projection and successful exit/stdout receipts;
it did not reread monthly leaf manifests or trade data locally.

## Completion and lifecycle receipts

Local task journal:
`output/wallet_attribution_repair_2026-10-09_v1/index.json`.
It records canonical release, approved scope and the actual final lifecycle
state; do not infer shutdown from completion of an individual agent.
The body driver receipts are
`/mnt/data/runs/2026-10-09_wallet_repair_driver_v1/repair.receipt.json`,
`repair.time.txt` and `repair.stderr.log`. The new immutable saved-evidence QA
destination is `/mnt/data/runs/2026-10-09_wallet_repair_saved_manifest_qa_v1`.
The final stopped-state receipt belongs in the local task journal; inspect it
before calling lifecycle work complete.
Root alone owns data handling and storage/lifecycle operations. Finish all required
transfers, confirm no active dependency, stop the instance and verify stopped
before delivering completion. Never infer instance shutdown from job completion.
