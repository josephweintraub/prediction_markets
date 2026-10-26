# Polymarket final-paper sample refresh v1

Status: frozen execution contract, 26 October 2026. The user approved an
execution cutoff of **2026-10-02T00:00:00Z, exclusive**, and a full refresh of
the sports cohorts and provider clocks. This is a plan and admission contract,
not a completed refresh, collection-completeness certificate or results claim.

## Purpose and governing definitions

Extend the current Polymarket replication through all of 1 October 2026 UTC,
replace approximate execution clocks with exact block timestamps throughout
the admitted history, and regenerate the paper evidence with refreshed sports
coverage. The cutoff is the approved alignment date, not an inferred meaning
of “end of September.” Do not impose a new lower execution-date cutoff.

The [methods reference](../methods_reference.md) governs shared data practices.
Retain the [full-date replication](kaushik_polymarket_full_sample_v1.md) and
[exchange-wallet exclusion](kaushik_exchange_wallet_exclusion_v1.md) scientific
conventions except for the explicitly approved source, timestamp and provider
updates here. The [Kalshi specification](kaushik_kalshi_replication_v1.md) is a
comparison reference, not authorization to change its accepted data or results.
The [historical certification contract](polymarket_data_certification_v1.json)
remains read-only and historical; this contract does not rewrite its scope.

Primary observations remain archive-recorded taker-role records, not newly
asserted native unique economic fills. Retain the approved BUY/SELL binary
normalization, eventual payout, eligibility filters, exchange-wallet exclusion,
exact recorded normalized price levels, fixed probability bins, equal-record
weights, regression specifications, fixed effects, clustering, uncertainty and
suppression rules. Do not add a bot filter, trim prices or remove up/down
markets merely because another study used those choices. Keep payoff
`100*(Y-P)` distinct from individual return `100*(Y/P-1)`.

## Immutable generation and recovery

Use new, versioned run and stage directories under `/mnt/data/runs/`. Bind
committed source, commands, environment, exact input inventories, schema/grain,
hashes, resource limits, counts, exclusions and output identities in manifests.
Preserve the accepted June source generation, repaired-wallet artifacts,
exchange masks, old input/estimate acceptances and published handoff unchanged.
Never replace content underneath their existing paths or manifest identities.

“Override older data” means promoting a validated new current-generation
reference after independent acceptance. It does not authorize deleting the
recoverable old generation, mutating old reports or relabeling old results.
New outputs must reopen successfully before atomic publication; failed or
incomplete stages remain explicitly unaccepted and resumable where safe.

## Resource gate before ingestion or rebuilding

The 26 October readiness inventory reports approximately **92.16 GB raw events**,
**69.50 GB resolved native records**, and only **50 GiB free**. These are existing
source sizes and disk availability, not projected final-generation sizes.
The old input-builder reservation alone is 32 GB output + 16 GB spill + 20 GB
free floor = **68 GB**, exceeding current free space before collection,
metadata, trade reconstruction, sports caches or estimation are considered.

Do not start an unbudgeted full-copy refresh. First review a stage-by-stage
capacity plan covering incremental logs, timestamps, metadata, replacement
monthly outputs, temporary files, estimator caches and a retained safety floor.
Prefer immutable incremental chunks plus explicitly inventoried historical
sources and bounded monthly materialization over redundant full-history copies.
Logical reuse must preserve input identity and may not conceal changed data.
The actual approved plan must establish peak simultaneous storage, RAM, spill,
API request limits and restart behavior from fresh measurements. No automatic
dataset deletion, volume expansion or instance resizing is authorized here.
If capacity cannot be admitted, stop before the affected production stage.

## Stage 1: freeze the endpoint and protocol map

Determine the first Polygon block whose authoritative header timestamp is
at least the exclusive UTC cutoff, using bounded header lookup/binary search.
Persist that block and its predecessor, numbers, hashes and UTC seconds;
verify the bracket and use the predecessor as the inclusive collection end.
Never derive this endpoint from a linear block-time approximation.

Inventory every emitting exchange relevant to the admitted interval. Verify
deployment/activity dates, supported topics and ABI/amount semantics against
first-party deployment/source evidence and on-chain observations. The existing
four-address legacy/V2 wallet rule is historical. V3, including the previously
diagnostic `0xe3333700ca9d93003f00f0f71f8515005f6c00aa`, needs explicit activity
and ABI admission for the cutoff interval, not an assumption of presence or
absence. Version the emitting-contract and exchange-wallet policies separately.
Unsupported active protocols or unexplained eligible matches block admission;
do not silently omit them or filter every exchange-emitted constituent fill.

## Stage 2: collect the increment with native lineage

Collect missing block intervals on EC2 into immutable, source-bound chunks,
with a reviewed overlap at the historical endpoint. Persist a per-contract
interval ledger, request predicates, attempts, successful terminal status,
response counts and hashes, including successful zero-log intervals. Require
gap-free requested-range coverage, no unexpected overlap and successful worker
exits. Exhausted retries, malformed/removed logs, ABI decode failures or absent
completion records must fail closed, not produce a partial “success.”

Retain native identity `(emitting_exchange, transaction_hash, log_index)`, block
number/hash and decoded payload. Separate exact replayed identities,
conflicting payloads at one identity, aggregate logs and different identities
with equal published values. Reconcile the endpoint overlap independently.
Preserving these fields does not itself establish whole-history collection
completeness, native own-wallet direction or unique economic executions.

## Stage 3: exact timestamps for the entire admitted history

Use authoritative Polygon block headers for all required historical and new
trade blocks; reuse only a verified immutable cache and fetch missing keys.
Validate returned block number/hash and request identity, positive integer UTC
seconds, unique consistent block mappings and a nondecreasing block clock.
Require **zero missing required block keys/rows**, no join fanout, row
conservation and **zero fallback-derived output rows**. Total cache-row count
is not a coverage test. Verify published timestamps equal their block-cache
values and retain native block identity in source or a bound lineage sidecar.

“Exact” means the chain's integer-second block timestamp, shared by all fills
in that block. It is not a nanosecond fill-arrival or matching-engine timestamp.
Do not retain a linear approximation through `COALESCE`, an inner join that
silently drops uncached trades, or declarations unsupported by reopened data.

## Stage 4: refresh metadata and construct the approved representation

Refresh token/market binary pairs, authoritative final outcomes, native
creation/end metadata, event mappings and category tags. Record the retrieval
vintage, complete pagination/terminal proof and every failed lookup. Require
unique noncontradictory join keys, reciprocal binary complements, outcome
agreement and zero metadata fanout. Preserve unmapped, unresolved, contradictory
and otherwise irregular records in an explicit audit waterfall.

Use the existing frozen category taxonomy on fresh metadata, retain its
Unclassified treatment and report classification coverage. Do not silently
change taxonomy or substitute a new LLM classifier. Resolution availability is
as of the declared refresh vintage, not necessarily the execution cutoff;
retain the resolved-market censoring qualification and quantify the affected
population. Do not guess payouts from a near-one trading price.

Build corrected maker/counterparty identities with the known DuckDB runtime
safeguard and independently reopen the wallet-pair reconciliation. Do not
apply the historical swap repair again to newly corrected rows. Retain the
approved archive action convention and its qualification, not a new native
own-action interpretation. Preserve the existing replay/value-DISTINCT
convention through an explicit bridge: record native replays, repeated order
hashes, equal published values and retention/exclusion counts separately.
Any proposed change to that scientific membership rule requires explicit
approval; never silently switch the paper to native-unique observations.

## Stage 5: refresh the full sports cohort and provider clocks

Refresh the current nine-sport cohorts, not only the old provider-covered
games. Rebuild candidate-to-game mappings, binary claims, results and accepted
sport-specific clocks for games represented in the execution-date sample.
Save authoritative provider responses, retrieval indices and reproducible
identity/result/timing proofs. Reconcile candidates, duplicates, unmatched
games, incomplete/ambiguous clocks and all other exclusions by sport.

Retain the existing phase/window definitions, binning, model weights and
support rules, including the 500-record and applicable game-count guards.
Refresh does not justify treating scheduled or duration-inferred tennis times
as verified actual boundaries. Report each sport's admitted game-date range,
clock quality and provider coverage. The execution cutoff alone does not impose
an unapproved game-start or game-end cutoff. Do not reuse old encoded bases,
wallet artifacts or provider acceptance receipts as if they covered new data.

## Stage 6: independent reconciliation and promotion

Before fitting, require complete source-to-sample first-failure accounting,
schema/grain/identity checks, exact clocks, metadata conservation and cohort
support. Reconcile the historical overlap by retained native identity and
execution payload. Separately bridge changes attributable to exact timestamps,
refreshed outcomes/metadata and any newly recovered observations; include
changed UTC month, cutoff, duration and sports-window membership counts.
Old approximate-clock rows need not be byte-identical to exact-clock rows.

Regenerate the unchanged table/figure/model contract from admitted inputs.
Independently audit coefficients or their retained sufficient evidence,
covariances, contrasts, support, suppression and displayed values; retain all
failed numerical gates. Publish portable LaTeX source and saved deterministic
figures only after data and result acceptance. Promote the final generation and
update the organized Dropbox handoff with recoverable prior-version navigation.

All production runs and collections occur on EC2, following the
[EC2 guide](../EC2_SETUP.md). Only the root controls lifecycle/storage; serialize
shared stages and stop and verify the instance stopped after confirming no
agent, process, notebook or transfer still needs it.
