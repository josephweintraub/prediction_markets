# Polymarket September refresh: source readiness, 26 October 2026

Status: source-readiness work only. No updated final sample, new estimates or
replacement Dropbox release is admitted by this record. The current June
source and all completed releases remain unchanged.

## Requested change and approved scope

The user requests extending Polymarket through the end of September, retaining
exact timestamps, with all real-data work on EC2. A completed, independently
validated generation should supersede current paper references; old immutable
sources/results remain recoverable rather than being rewritten behind their
manifests.

The user confirmed both scope questions on 26 October: match the existing
Kalshi archive through **1 October 2026 UTC**, and refresh the **entire writeup,
including sports**. The common upper-exclusive execution bound is
`t < 2026-10-02T00:00:00Z`. Preserve all earlier eligible Polymarket history.
The inventory command itself remains cutoff-neutral and does not admit a sample.

The [accepted Kalshi record](2026-10-16_kaushik_kalshi_replication_v1.md)
separately reports its admitted duration-tail executions ending on 25 July,
because later settlement clocks are absent. Common calendar cutoff alone does
not make the duration cohorts equally covered.

## Source-generation boundary

The current [exchange-excluded Polymarket run](2026-10-14_kaushik_exchange_filtered_rerun_v1.md)
uses repaired CLEAN archive records through June and explicitly approximate
archive execution clocks. CLEAN has no block/log identity. Recover exact time
from identity-bearing native source and block headers, not a price/wallet/size
value join. Exact clocks may change old month, duration and boundary membership;
those differences must be separated from new-date and fresh-resolution effects.

Retain the approved Kaushik exposure/weight/model conventions and source-wallet
exchange exclusion unless separately revised. Do not silently change native
log multiplicity, value-row retention or economic direction. New emitting
exchange versions, binary token/outcome mappings, categories, duration clocks
and sports identities need fresh source admission. Refreshing historical
outcomes does not remove resolution censoring.

The original `pipeline/refresh.py` is not an approved unattended refresh path.
Review found mismatched legacy/V2 collector wiring, partial-RPC-success paths,
timestamp fallback/coverage-count shortcuts, lossy deduplication and incomplete
metadata-pagination admission. Do not invoke it against shared paths unchanged.
The new generation requires immutable chunk/range completion, native-identity
reconciliation, exactly covered unique block headers, zero approximation,
row-conserving metadata joins, corrected wallet expansion and disjoint sample
accounting before estimation. The statistical estimator itself need not change.

The official [contract directory](https://docs.polymarket.com/resources/contracts)
and [V2 source](https://github.com/Polymarket/ctf-exchange-v2) were checked on
26 October. They identify legacy CTF successors and a separate V3 generation;
current documentation is not proof of a historical activation date. Verify
activity and the emitting ABI before admitting the requested new period.

## Bounded readiness implementation

`scripts/inventory_polymarket_refresh.py` reads only explicitly selected
Parquet footers, schemas, producer-declared counts/statistics, file identity,
resources and optional three-call private RPC readiness. It never reads trade
bodies, selects a cutoff, fetches logs or certifies completeness. Missing footer
statistics remain unknown. Input identity is reopened after reads; metadata
budgets and exclusive output paths are enforced. Credentials are not exported.

Canonical starting revision: `539d3551af899700f7021a73cc9e300d2d0896ea`.
Root started the stopped instance, verified a clean checkout and mounted only
data-volume UUID `d0cd087b-94c4-428c-bae9-ae28929059f6` at `/mnt/data`.
The actual type remains `r5.8xlarge`. A bounded connectivity probe verified
Polygon chain 137 and a live exact block header through the existing private
configuration; no trade-log collection occurred.

The data disk has approximately 50 GiB free. The old input builder alone
reserves 68 decimal GB for output, spill and its free-space floor, before any
new collection or full-copy intermediates. Do not start that default workflow
or delete existing datasets to fit it. A measured bounded-storage design or
separately authorized storage/archival plan is required.

Execution/test receipts belong under the new run root
`/mnt/data/runs/2026-10-26_polymarket_september_refresh_preflight_v1`, with
small local copies under `output/polymarket_september_refresh_2026-10-26_v1`.
The canonical-runtime scoped suite passed **41 tests and 28 subtests** in
2.11 seconds, exit zero. `runtime_fixture_receipt_v2.json` binds the exact
command, source hashes and `runtime_fixtures_v2.log`. An earlier attempted
combined command named two tests absent from the canonical checkout and ran
no tests; its failed receipt/log remain preserved. The first standalone run
also exposed a host-dependent CLI-refusal fixture: on the actual production
host the guard correctly passed and the test read only default footers before
failing its expected-refusal assertion. The fixture now explicitly forces
guard refusal and proves no source/inventory access on every host. No production
guard or data-admission rule was weakened.

Actual saved footer evidence and shutdown must be recorded after execution;
they are not asserted in advance here. Root must confirm no worker
or process still needs EC2, stop it and verify stopped even while scope choices
remain unadmitted. No handoff/current-release pointer is changed by preflight.

## Verified endpoint and approved storage handling

The committed inventory at `29cae389eb5a9c09319a363434f9c4868aff7358`
inspected six source footers, 59,427,658 metadata bytes and 21,233 row groups,
with no trade-body reads. The expected absent `deduped_events.parquet` makes
its status incomplete; that status is not evidence of a missing final-trade
count. The other input identities reopened unchanged. The native raw footer
declares 1,541,318,092 rows and maximum block 88,978,537; the exact cache declares
27,117,476 rows. Footer declarations are not independent body completeness.

The source-bound planner at `fd3dd32a315f0f13cc126c83eaaadc9fbc4fc076`
used 38 RPC calls and verified adjacent headers: block **94,796,039** has UTC
second **1790899199**, and its child **94,796,040** has second **1790899200**.
Thus the inclusive collection end is 94,796,039. The provisional missing
interval has 5,817,502 blocks, not a certified missing-trade count. All five
known exchange addresses have code at the cutoff, including V3. Presence of
code does not admit trading activity, historical implementation or ABI.

After an explicit user approval and a correction of the initial parent-folder
estimate, root archived exactly these two targets:

| Original tree | Files | Directories | File bytes |
|---|---:|---:|---:|
| `/mnt/data/pipeline_root_output/trades.parquet` | 44 | 45 | 41,335,439,747 |
| `/mnt/data/pipeline_output/trades_clean.parquet` | 44 | 45 | 23,800,759,225 |

`scripts/archive_final_sample_storage.py` is a separate fixed profile of the
previously tested archive safety kernel. Its 26 non-restoration function bodies
are unchanged. The combined canonical fixture suite passed **105 tests and
73 subtests**, exit zero, in 2.64 seconds before execution; its receipt/log are
`runtime_fixture_receipt_v4.json` and `runtime_fixtures_v4.log`.

Archive producer `042c6e744be92312a40b874711605dc6b89ecf0a` uploaded to
`dropbox:Polymarket Data and Code/Archives/2026-10-26_final_sample_storage_archive_v1`.
Every one of 88 paths, byte sizes and nonempty Dropbox content hashes matched,
as did both restoration controls and the unchanged source inventory. The
verified manifest SHA256 is
`9088776d52ea4b8f54d3914f8fb72ad177a20baef93409d9fc70c305fd971046`.

Root's privileged consumer check at `2026-10-26T16:01:10.539113Z` found no open
target handles, no uninspected process permissions and no surviving archive
producer. The separate removal phase rechecked current remote controls and
hashes and removed only the manifest-listed 88 files/90 directories. It
completed at `2026-10-26T16:01:50.967169Z`, exit zero, with all other bound
native/metadata/repair inputs unchanged. Free bytes rose from 52,617,736,192 to
117,754,662,912. `archive_removal_receipt_v1.json` records the actual result.

Historical readers of those shared original paths now require restoration;
current accepted Kaushik bases and repaired CLEAN remain intact. A restored
copy recovers bytes, not old filesystem inodes, so strict identity-based stages
need a new restoration/re-admission receipt. Do not disguise that boundary
with a symlink. No new final sample or empirical estimates are implied.

## Category reconstruction authorization and diagnostic boundary

The saved category labels and frozen tag map are available, but bounded source
and history searches did not recover the original v1 scoring program, soft tag
probabilities or IDF weights. The user explicitly approved reconstruction and
validation on 26 October. Keep accepted labels unchanged. Freeze the simplest
transparent candidate for new/unmatched markets before inspecting parity;
report disagreement against the saved labels and do not claim exact recovery
of the lost scorer. Categories do not determine baseline trade eligibility.

The first increment-capacity pilot failed closed because the RPC provider
adds a `blockTimestamp` field to log objects. Preserve that failed run. A
bounded one-call replay established only the additional field's string type;
it did not validate its encoding, units or clock accuracy. The revised pilot
retains the optional bounded printable-ASCII value and its presence flag as
uninterpreted provenance, never as an exact timestamp or native identity key.
All other unexpected fields remain blocked. A subsequent pilot must use a
fresh run directory and independently validated header clocks.

The raw exchange-coverage diagnostic projects only emitting address and block
number, binds the saved source/positive activity window, and reconciles counts
to the immutable footer inventory. Its completion is not source admission;
positive count agreement is not native-identity agreement. Missing known
observations must block final admission until repaired and reconciled.
