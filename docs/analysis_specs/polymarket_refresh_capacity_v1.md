# Polymarket refresh capacity assessment v1

Status: **full refresh not admitted**, 26 October 2026. Read-only assessment of
root-supplied pilot summaries and committed entrypoint reservations; no new
EC2 inspection, data scan, API request or infrastructure change. The
[refresh contract](polymarket_final_sample_refresh_v1.md) still governs the
exact-clock sample ending before `2026-10-02T00:00:00Z`, including sports.

## Decision

The reported **117,751,996,416 free bytes (117.752 decimal GB)** do not support
responsible admission of the complete workflow under the existing conservative
reservations. No fit has been demonstrated without further deletion or a
reviewed lower-storage implementation. The user-approved two-tree archive is
already complete; this assessment does not authorize deleting more artifacts.

Adding **150 GiB** would add **161.061 decimal GB**, producing approximately
**278.813 decimal GB free**, holding other usage constant. This plausibly removes
the immediate known reservation bottleneck. It is a reasonable expansion to
request, **not a demonstrated sufficient size or permission to collect the full
sample**. Historical V3 recovery and several other components remain unbounded.

## Observed pilot and limited extrapolations

The completed v2 pilot sampled three fixed, disjoint 100-block windows at
25%, 50% and 75% of the provisional 5,817,502-block append range. It completed
all three windows with 15 RPC calls. The source raw audit separately found no
V3 rows despite 44 known June observations, so append-only collection cannot
repair the history.

| Window | Native log rows | Compressed pilot Parquet bytes |
|---|---:|---:|
| 25% | 11,000 | 815,321 |
| 50% | 11,214 | 826,329 |
| 75% | 11,598 | 893,687 |

Root supplied the following append-only scenarios. They depend on sampled
activity, protocol composition, raw schema, compression and shard overhead.
Three fixed windows do not bound unsampled density. In particular, doubling
the densest window is **not an upper bound**. None includes historical V3
backfill, headers, refreshed metadata or downstream outputs.

| Illustrative append scenario | Decimal GB |
|---|---:|
| Mean sampled compressed bytes per block | 49.164 |
| Densest sampled compressed bytes per block | 51.990 |
| Twice densest sampled rate | 103.981 |

## Existing guarded reservations

These are code-enforced capacities, not forecasts of actual new-sample sizes.
The old builders are source-bound to the historical generation; source and
exact-clock adapters still need review before use on the refresh.

| Stage | Reservation beyond already occupied bytes | Decimal GB |
|---|---|---:|
| Physical analysis-input build | Output 32 + spill 16 + untouched floor 20 | 68 |
| Estimation after input build | Accepted output 8 + transient file 16 + spill 16 + untouched floor 20 | 60 |
| Retained new input plus estimator peak | Retained input cap 32 + estimator reservation 60 | 92 |

Source: `analysis/kaushik_polymarket_replication/build_inputs.py:50` and `:258`;
`analysis/kaushik_polymarket_replication/run_estimates.py:40`, `:291` and `:353`.
The estimator's 3 GB cache-file cap is within its accepted-output accounting,
not an extra 3 GB added to the 60 GB. Serializing stages does not remove retained
input bytes; equally, adding 68 and 60 would double-count reusable stage
reservations. Existing files are already deducted from the free-space snapshot
and must not be subtracted again.

With only the mean append scenario, the input-build gate leaves **0.588 GB**
for everything else. The densest-window scenario already exceeds that gate.
For the retained-input/estimator peak:

| Append scenario + 92 GB | Budget, GB | Margin at current free space, GB | Margin after +150 GiB, GB |
|---|---:|---:|---:|
| Mean sampled rate | 141.164 | -23.412 | 137.649 |
| Densest sampled rate | 143.990 | -26.238 | 134.823 |
| Twice densest sampled rate | 195.981 | -78.229 | 82.833 |

Margins exclude the unknown components below. The negative values show failure
to cover the retained-capacity planning envelope, not proof that actual writes
will reach those caps. Positive expansion margins are not admission certificates.

## Components still requiring bounded stage budgets

- Historical V3 logs and any other recovered historical source intervals.
- Exact-header cache additions, coverage proofs and native-lineage sidecars.
- Fresh token/position mapping, market/outcome/category metadata and retrieval
  caches, including V3 representation and settlement admission.
- Refreshed nine-sport provider responses, timing proofs and cohort artifacts.
- Native decoding, replay/value-retention bridges, corrected wallet expansion
  and any physical monthly outputs beyond the analysis-input layer above.
- Collector staging, external sort/deduplication, failed-attempt artifacts,
  transient files and any simultaneously retained replacement shards.
- Expanded estimator/cache cardinality, deterministic figures, LaTeX build and
  verified handoff staging; filesystem overhead and other live consumers.

No sizes are asserted for these components. Some may be reusable or mutually
exclusive, but only a source-bound liveness plan can establish that. Existing
entrypoints also require at least 240 GB available RAM for their separate
memory budgets and four threads; extra disk does not establish those gates.

## Smallest safe next step

Pause full-range collection and request explicit authority for additional
storage, with **150 GiB as a plausible immediate expansion rather than a
promised final requirement**. Preserve accepted sources/results and both pilot
runs. Before admitting full collection, freeze hard per-stage output/spill
caps and retained-file lifetimes covering all components above, then compare
peak simultaneous reservations with freshly measured free space. Reject or
pause a stage before exceeding its cap, with no automatic cap increase,
artifact deletion or weakening of the 20 GB safety floor.

A smaller footprint might be feasible through reviewed incremental reuse and
bounded monthly materialization, but that implementation and its reconciliation
are not yet demonstrated. Do not claim an existing-space fit by assuming it,
or by treating the twice-densest scenario as a capacity ceiling.
