# Polymarket on-chain pipeline

This pipeline reconstructs Polymarket `OrderFilled` events from Polygon and produces the
canonical trade dataset used by the calibration analyses. It is an infrequent data-refresh
workflow, not the ordinary analysis entry point.

## Canonical outputs

| Path | Purpose |
|---|---|
| `/mnt/data/pipeline_output/trades_clean.parquet` | Deduplicated canonical analysis trades |
| `/mnt/data/pipeline_root_output/trades.parquet` | Raw transformed trades retained for comparison |
| `/mnt/data/pipeline_output/market_flags.parquet` | Token-to-market spine, outcomes, and market-level up/down flag |
| `/mnt/data/pipeline_data/` | Expensive refresh intermediates and source caches |

The current cleaned build has 2,036,128,538 rows through 2026-06-23. It contains resolved
markets only; see `../docs/methods_reference.md` for the resolution-censoring caveat.

## Active implementation

```text
refresh.py
  extraction/extract_orderfilled_v2.py   Polygon OrderFilled extraction
  extraction/dedup.py                    event deduplication
  extraction/fetch_block_timestamps.py   block timestamps
  transform/build_trades.py              token mapping, resolution, and trade transform
  goldsky/                                alternate event-source utilities
```

`_legacy/` contains superseded pipeline stages retained temporarily for provenance. Do not
use them for a refresh. The pre-cleanup version is preserved by Git tag
`pre-cleanup-2026-09-06`.

## Authentication

Extraction requires a Polygon archive-node URL. Supply it through `POLYGON_RPC_URL` or a
mode-600 file at `~/.polygon_rpc_url`. Never put the endpoint or key in Git.

## Running a refresh

Run from `/home/ubuntu/prediction_markets/pipeline` on EC2 after mounting `/mnt/data`:

```bash
/home/ubuntu/venv/bin/python refresh.py --help
```

`refresh.py` has explicit skip flags for resuming stages. Before running it:

1. Record the current input and output row counts and checksums.
2. Confirm at least 100 GB of temporary free space or calculate the actual requirement.
3. Preserve the current canonical output until the replacement validates.
4. Rebuild `market_flags.parquet` and wallet flags when their upstream inputs change.
5. Validate coverage, resolution agreement, schema, timestamps, and duplicate rates.
6. Assign a new data-vintage identifier and update project documentation.

The attached EBS volume was 89% full on 2026-09-06, so a refresh must not begin until its
temporary-space requirement has been reviewed.

## Data conventions

- One fill expands to maker and taker rows.
- In the canonical EC2 trades, `conditionId` is the per-outcome token ID, not the hex
  market condition ID.
- Do not exclude up/down markets using the trade `eventSlug`; it is empty on many newer
  rows. Join `market_flags.parquet` on token ID and use its market-level flag.
- Never load the full trade dataset into pandas; use DuckDB over Parquet.
