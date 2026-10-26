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
