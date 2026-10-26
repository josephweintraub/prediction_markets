# Polymarket refresh capacity contract v2

Status: **proposed hard-cap envelope; full campaign not admitted**, 26 October
2026. This document replaces neither the historical [v1 assessment](polymarket_refresh_capacity_v1.md)
nor its measurements. The [final-sample contract](polymarket_final_sample_refresh_v1.md)
still governs sample membership, the exclusive `2026-10-02T00:00:00Z` cutoff,
exact execution timestamps and the full nine-sport refresh.

## Verified storage boundary

Root reports that the existing data volume and its ext4 filesystem were grown
to 750 GiB, with **371,217,793,024 available bytes** after growth. The six bound
source-file identities reopened unchanged. These are root-supplied execution
facts, not new measurements performed by this planning review. Bind the actual
growth/verification receipt when admitting a production stage.

Use this measured available space, not the earlier counterfactual of adding
250 GiB to the previous free-space figure. Filesystem metadata and reserved
space mean the counterfactual is not the writable capacity. All GB amounts
below are decimal: 1 GB = 1,000,000,000 bytes. Already occupied sources, accepted
results and existing failed runs are already deducted from the measured free
space; do not subtract them a second time.

## Hard-cap envelope, not predicted sizes

These are proposed aggregate ceilings for **new bytes beyond the verified
starting inventory**. They do not demonstrate that the required data or
calculations will finish within those ceilings. Every output, partial file,
manifest, log, cache, retry artifact and retained failed attempt must be charged
to exactly one category. Any physical copy counts, including copies used for
publication. No uncharged full-history intermediate or second full raw copy is
permitted.

| Category | Aggregate ceiling and contents | GB |
|---|---|---:|
| Native raw collection | All append and historical-backfill shards, range ledgers, raw staging, simultaneously retained replacement/partial shards, failed attempts and collector logs | 128 |
| Exact headers | New header responses, exact-cache additions, header indices and coverage proofs | 8 |
| Market metadata | API response caches, token/position-family mapping, authoritative outcome evidence, market/category outputs and retrieval ledgers | 8 |
| Sports | All nine-sport provider caches, candidate/game maps, clocks, outcome/timing proofs and cohort audit artifacts | 8 |
| Native bridge | At most 64 GB retained decoded/bridge/lineage/monthly outputs and scratch artifacts, plus at most 16 GB spill | 80 |
| Paper inputs and estimation | At most 32 GB retained compact inputs, plus the 60 GB analysis reservation detailed below | 92 |
| **Total reservation including the untouched floor** | | **324** |

At the reported measurement, the reservation leaves
**47,217,793,024 bytes (47.218 GB)** beyond its separately included 20 GB
untouched floor. This margin is not an automatic increase to any stage cap and
does not authorize unrelated work, more copies, cleanup or another resize.
Recheck actual available space before each stage and account for other live
consumers. If the bound inventory or space changes, recompute admission before
writing, rather than relying on this original margin.

The 60 GB paper analysis reservation comprises 8 GB accepted outputs,
16 GB transient files, 16 GB spill and the 20 GB untouched floor. Charge
estimator caches, deterministic figures, LaTeX source/build products, result
manifests and handoff staging together to that **8 GB aggregate output cap**.
The existing 3 GB estimator cache cap is a sub-cap, not another reservation.
Existing estimators only enforce their own output directory; this wider
combined accounting still needs enforcement before reporting/publication.
Do not assume the entire 8 GB can be spent by the estimator and then add reports
or a second handoff copy without a check.

For the compact input build, 32 GB output + 16 GB spill + 20 GB floor = 68 GB.
Its spill is reusable only after the stage terminates. The retained-input plus
estimator peak is 32 + 60 = 92 GB, rather than 68 + 60. The table conservatively
also reserves the native bridge's separate 16 GB spill; no simultaneous-spill
reuse is required to justify the 324 GB envelope. Retained bridge outputs are
not assumed deleted before fitting. Final report/handoff duplication is not
assumed free.

## Stage admission and enforcement

Serialize collection, metadata updates, native bridge construction, compact
input construction and estimation where they share inputs, caches or the
resource envelope. Do not run another memory-heavy analysis beside these
stages. The existing input/estimation gates require four threads and at least
240 GB available RAM; retain their 192 GB DuckDB limit, estimator's separate
32 GB numerical-workspace budget and admitted read/iteration ceilings. Extra
disk does not waive RAM, numerical convergence or source-identity gates.

Before each affected stage, its reviewed implementation must:

1. Bind committed source, immutable inputs, measured free space, stage caps and
   all output/staging directories. Reopen unchanged source identities after
   execution. Preserve completed earlier generations and failed attempts.
2. Count all currently retained bytes against that stage's aggregate cap,
   including previous subruns and incomplete files. Reserve bounded writes
   before creating them, with explicit spill and transient-file limits.
3. Abort or refuse the next write before crossing the cap or the 20 GB
   available-space floor. A failed allocation, exhausted retry, malformed
   response or incomplete range must never produce a completion certificate.
4. Save the failure and completed-chunk inventory without automatic cap growth,
   artifact deletion, overwrite, timestamp fallback or relaxed cohort gates.
   Any cap revision needs a new reviewed version and fresh resource admission.

**Enforcement is not yet complete.** This document alone installs no quotas.
The existing pilot has small fixed request/response/output limits, but is not a
full collector. There is no admitted campaign-wide 128 GB collector accounting,
nor admitted 8 GB header/metadata/sports adapters or 80 GB native bridge. The
existing input and estimator enforce some local resource caps but remain
source-bound to old repair/input/sports generations; do not simply change
constants or execute them against the refresh. A new exact-clock input adapter
and source-specific admission are still required. Each stage must demonstrate
its actual safeguards with fixtures and bounded execution before campaign
approval. A strict cap can safely stop an incomplete stage; it is not evidence
that the stage will complete.

## Immediate bounded raw-staging subrun

After root accepts the filesystem/source checks and independently reviewed
code enforces the following ceilings, the envelope permits **only a bounded
raw diagnostic/staging subrun**, not full-range collection, economic decoding,
new labels, an analysis input generation or final-data admission:

| Subrun resource | Proposed ceiling |
|---|---:|
| Explicit requested block coverage | 1,000 distinct blocks in one frozen interval |
| RPC attempts, including failed calls and retries | 100 |
| Response body per call | 16 MiB |
| Total response bodies across all attempts | 256 MiB |
| Accepted raw log rows | 200,000 |
| All new retained and temporary output bytes | 128,000,000 |
| Process address space | 8 GiB, with reviewed effective enforcement |
| Available RAM before launch | At least 8 GiB |
| Runtime | 1,200 seconds |
| DuckDB/external spill | 0 |

MiB here means 1,048,576 bytes and GiB means 1,073,741,824 bytes. The process
address-space limit includes Python/PyArrow mappings and is not an assertion
about resident memory per row. Together with the explicit row/response limits,
it must be effectively enforced; a memory failure leaves an incomplete stage.
These are proposed ceilings, not observed usage or a promise that all 1,000
blocks will complete. Raw HTTP bodies can be much larger than compressed
Parquet; the old pilot's approximately 12 MB response
for one 100-block window rules out promising only a few MiB of downloads.
Do not silently truncate a larger response or drop excess rows to meet a cap.
The subrun's bytes also count against the 128 GB raw-campaign ceiling, including
failed attempts. Repeated small subruns do not bypass campaign accounting or
authorize an unattended full-history sweep. Pending safeguards must block
execution, not merely appear in its manifest.

Use a fresh immutable run directory, one serialized writer, exact literal
address/topic predicates and a frozen interval inside the verified endpoint.
Bind the previous endpoint and native-probe evidence, validate returned native
locators, address/topic, block bounds and consistent payloads, and retain the
successful zero-log/range completion evidence. Preserve native identities and
raw payload without claiming that ABI or settlement semantics are admitted.
Keep the provider's optional `blockTimestamp` solely as uninterpreted
provenance; authoritative execution time still requires verified headers.
No completion marker may be written unless all requested predicates/ranges,
source checks, schemas and saved-output checks pass. A completed diagnostic
subrun certifies only its literal bounded request and raw staging, not the
history, paper population or completeness of all exchange protocols.

## Scientific and source gates remain unchanged

Known historical V3 activity is absent from the existing raw archive. An
append-only extension cannot repair it. Admit historical V3 deployment,
implementation/upgrade periods, event ABI and amount semantics before economic
decoding or final collection coverage claims. Resolve V3 position-family and
binary-payout mappings without treating a matching event signature as proof of
legacy token compatibility. Unsupported active protocols remain blockers.

Across old and new retained observations require exact header-key coverage,
native-lineage and retention reconciliation, corrected wallet attribution,
complete metadata pagination, reciprocal complements and authoritative
outcomes. The approved historical category comparison is not fresh-market
coverage; ambiguous categories must not remove baseline trades. Refresh all
nine sports with their provider evidence, preserving the stated tennis clock
qualification. Reconcile exact-clock, refreshed-outcome and recovered-source
membership changes separately before unchanged models are fitted.

Only independent data/result acceptance permits promotion of a new current
paper generation and Dropbox handoff. Root alone manages EC2/storage and must
stop and verify the instance stopped after confirming no agent, process or
transfer still needs it. No additional deletion, expansion or instance change
is authorized by this capacity contract.
