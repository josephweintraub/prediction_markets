# Polymarket Pipeline (EC2)

End-to-end extraction and analysis of Polymarket OrderFilled events from
Polygon mainnet. Outputs a 1.01-billion-row `trades.parquet` used in the
favorite-longshot-bias paper.

## Layout

```
pipeline/
├── config.py                          # paths, RPC, DuckDB settings
├── extraction/                        # raw chain → events  (slow, run-once)
│   ├── extract_orderfilled.py        #   Polygon RPC → raw_events.parquet
│   ├── dedup.py                      #   dedup paired emit events
│   └── fetch_block_timestamps.py     #   block_number → unix_ts (RPC)
├── transform/                         # events → trades  (~5 min)
│   └── build_trades.py               #   token-map → resolve → maker/taker rows
├── goldsky/                           # alternative event source
│   └── pull_orderfilled.py           #   Goldsky GraphQL → JSON.gz chunks
├── analysis/                          # paper-relevant analysis
│   ├── results.ipynb                 #   primary paper notebook (39 cells)
│   ├── exploration.ipynb             #   exploratory (kept for context)
│   ├── data_loader.py / config.py
│   ├── bot_filter.py
│   ├── favorite_longshot.py          #   FLB compute + plots
│   ├── trader_flb.py                 #   trader-typology FLB
│   ├── trader_characteristics.py
│   ├── pnl_analysis.py
│   ├── market_accuracy.py
│   ├── closing_prices.py
│   ├── fetch_manifold.py
│   └── output/                        #   figures, intermediate parquets
├── output/                            # final products (used by analysis)
│   ├── trades.parquet                 #   1.01B rows, partitioned by year_month
│   ├── trades_v1_no_maker_flag.parquet  # backup before maker/taker patch
│   ├── market_resolutions.parquet
│   └── market_resolutions_enriched.parquet
├── data → /mnt/data/pipeline_data     # symlink to large data volume
└── _legacy/                           # superseded scripts kept for reference
```

## Data flow

```
Polygon RPC ──┐
              ▼
       extract_orderfilled.py ──► raw_events.parquet           (~519M rows, 47GB)
              │
              ▼
            dedup.py ──────────► deduped_events.parquet         (~519M, 31GB)
              │
              ▼
       build_trades.py ────────► resolved_trades.parquet        (~505M, 32GB)
              │                       │
              │                       ▼ (maker + taker expansion)
              ▼                  trades.parquet                 (1.01B, 15GB)
       market_resolutions.parquet
```

Goldsky alternative path: `goldsky/pull_orderfilled.py` pulls the same
OrderFilledEvent stream from Goldsky's hosted subgraph; output is comparable
to `raw_events.parquet` and includes maker/taker plus real block timestamps
(our on-chain pipeline approximates timestamps).

## trades.parquet schema

| column        | type    | notes                                        |
|---------------|---------|----------------------------------------------|
| proxyWallet   | VARCHAR | trader address                               |
| timestamp     | BIGINT  | unix epoch seconds (approximated from block) |
| conditionId   | VARCHAR | per-outcome token id                         |
| usdcSize      | DOUBLE  | dollar size                                  |
| price         | DOUBLE  | 0..1                                         |
| side          | VARCHAR | BUY or SELL                                  |
| outcome       | VARCHAR | "Yes", "No", etc.                            |
| eventSlug     | VARCHAR | parent event slug                            |
| is_maker      | BOOLEAN | true for limit-order rows, false for market  |
| counterparty  | VARCHAR | the other side's wallet                      |
| year_month    | VARCHAR | partition key                                |

Each on-chain fill produces TWO rows: one maker (`is_maker=true`) and one
taker (`is_maker=false`). 505M fills → 1.01B trade rows.

## Reproduction

All scripts assume `cwd=/home/ubuntu/pipeline` and use `/home/ubuntu/venv/bin/python`.

```bash
# 1. Extract OrderFilledEvent logs from Polygon mainnet (slow: hours)
python extraction/extract_orderfilled.py
python extraction/dedup.py
python extraction/fetch_block_timestamps.py    # optional, for real timestamps

# 2. Transform to trades.parquet (~5 min)
python transform/build_trades.py --stages 3 4 6

# Alternative: pull from Goldsky (faster, real timestamps, with maker/taker)
python goldsky/pull_orderfilled.py
```

## Analysis

```bash
jupyter notebook analysis/results.ipynb
```

The notebook reads `output/trades.parquet` and produces the paper figures
under `analysis/output/figures/`.

## Hardware

- Instance: `m5.4xlarge` or larger (16 cores, 64GB RAM minimum for DuckDB)
- Storage: 100GB root + 500GB attached volume mounted at `/mnt/data`
- Memory limit set in `config.py` (`DUCKDB_MEMORY_LIMIT`)
