# EBS storage inventory

**Captured:** 2026-09-06

**Volume:** `/mnt/data`, 492 GB capacity, 411 GB used, 56 GB available (89% used)

This is a retention-planning snapshot, not authorization to delete anything. A candidate
must be tied to a reproducible replacement or external backup before removal.

## Largest areas

| Path | Approximate size | Initial classification |
|---|---:|---|
| `/mnt/data/pipeline_data` | 154 GB | Pipeline insurance and intermediates; review carefully |
| `/mnt/data/pipeline_root_output` | 112 GB | Raw/current/snapshot trade outputs; contains likely duplication |
| `/mnt/data/telonex` | 68 GB | Active external quote data |
| `/mnt/data/pipeline_output` | 45 GB | Canonical cleaned trades plus snapshot/staging |
| `/mnt/data/embedding_difficulty` | 12 GB | Active analysis intermediates with mixed vintages |
| `/mnt/data/analysis_output` | 8.9 GB | Mixed legacy/external outputs |
| `/mnt/data/learnability` | 8.4 GB | Mostly historical analysis artifacts plus active flags/metadata |
| `/mnt/data/kalshi` | 4.1 GB | Supporting cross-platform work |

## Largest individual or grouped candidates for review

| Path | Approximate size | Note |
|---|---:|---|
| `pipeline_data/raw_events.parquet` | 86 GB | Core refresh insurance; retain unless independently backed up |
| `pipeline_data/resolved_trades.parquet` | 65 GB | Rebuildable from upstream stages, but expensive |
| `pipeline_root_output/trades.parquet` | 39 GB | Raw canonical comparison input |
| `pipeline_root_output/trades_snap20260624.parquet` | 37 GB | Historical snapshot candidate |
| `pipeline_root_output/trades_no_event_slug.parquet` | 37 GB | Intermediate candidate |
| `pipeline_output/trades_clean.parquet` | 23 GB | Current canonical analysis input; retain |
| `pipeline_output/trades_clean_snap20260624.parquet` | 22 GB | Historical snapshot candidate |
| `telonex/quotes_ticks` | 66 GB | Active acquisition output; document completeness before action |
| `embedding_difficulty/schemes` | 1.5 GB | Generated and likely reproducible |
| Five embedding matrices | about 1.3 GB each | Pre-refresh positional artifacts; preserve until replacement verified |

## Recommended retention process

1. Record checksum, schema, row count, producing code commit, and downstream consumers.
2. Mark each object `canonical`, `required intermediate`, `rebuildable cache`, `historical
   snapshot`, or `external source`.
3. Confirm backup location for canonical and external-source objects.
4. Reproduce one candidate cache before declaring it rebuildable.
5. Review a concrete deletion list separately.
6. Prefer moving reviewed historical material to cheaper storage before permanent deletion.

## Provisional retention review

No files were moved or deleted during this review. The following is the proposed policy,
ordered by how much independent information an object preserves.

| Class | Objects | Proposed action |
|---|---|---|
| Canonical analysis input | `pipeline_output/trades_clean.parquet` | Retain on EBS and add an off-instance backup before publication |
| Expensive source/refresh insurance | `pipeline_data/raw_events.parquet`, `pipeline_root_output/trades.parquet` | Retain until checksummed backup and a tested rebuild path both exist |
| External-source acquisition | `telonex/quotes_ticks`, Kalshi source artifacts | Retain; first document acquisition completeness and replacement cost |
| Active compact inputs | current flags, metadata, universe, code maps, FLB bases, and scheme maps | Retain on EBS; bind them to committed vintage declarations and run manifests |
| Historical snapshots | `pipeline_root_output/trades_snap20260624.parquet`, `pipeline_output/trades_clean_snap20260624.parquet` | First cold-storage candidates; together approximately 59 GB |
| Rebuildable but expensive intermediate | `pipeline_data/resolved_trades.parquet` | Keep for now; prove one full rebuild before considering cold storage or removal |
| Likely superseded intermediate | `pipeline_root_output/trades_no_event_slug.parquet` | Verify no downstream consumer, then prefer cold storage; approximately 37 GB |
| Stale positional analysis cache | five pre-refresh embedding matrices and dependent nearest-neighbor/PCA artifacts | Preserve as historical reproduction inputs until refreshed embeddings reproduce expected coverage |
| Historical analysis outputs | older `analysis_output`, `learnability`, and mixed-vintage `embedding_difficulty` outputs | Inventory by producing commit/run before moving; do not infer disposability from age |

The safest first capacity action, after backup verification, is moving the two June 24
trade snapshots to cold storage. The next review should resolve downstream consumers of
`trades_no_event_slug.parquet`; deletion is not proposed here. Immutable new results now
go under `/mnt/data/runs`, which makes future retention decisions attributable to a run
instead of a mutable filename.
