# Learnability dimensions — Phase 1 (EC2 / 1.4B-trade dataset)

Catalog of dimensions used to slice Polymarket contracts for FLB calibration analysis. The hypothesis: contracts whose outcomes are more "learnable" (richer data feeds, recurring contract families, high volume, narrower subject matter, etc.) should show measurably weaker favorite-longshot bias than contracts on the opposite end of each axis.

## Data protocol (mirrors `pipeline/analysis/results.ipynb`)

- **Trade dataset**: `/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet` (1.4B trades, 1.2M unique wallets, 1.5M unique outcomes)
- **Bot filter**: behavioral via `pipeline/analysis/bot_filter.build_wallet_flags(con)` — composite of inter-trade-interval, trades-per-active-day, hour-of-day HHI, and fixed-trade-size criteria
- **Excluded markets**: up/down (`%updown%` or `%up-or-down%` in eventSlug)
- **Trade-level returns**: BUY-side only, `ret = (1 - price) if won else -price`
- **Price filter**: `0.01 < price < 0.99`
- **Lifecycle window**: trades in 50-80% of contract lifetime (snap closer to resolution)
- **Bins**: deciles (n_bins=10), spread = D10 - D1
- **Primary SE**: 2-way clustered (day × `proxyWallet`) on trade-level data
- **Robustness SE**: Fama-MacBeth on contract-level monthly returns
- **Min trades per slice**: 5,000

## Join key

Augmented per-contract parquet uses both `token_id` (77-digit decimal, per-outcome) and `condition_id` (0x... hex, per-market). The trades parquet's `conditionId` column is **actually the per-outcome token_id**, so the join is:
```sql
trades.conditionId = augmented.token_id
```
Coverage: all 1,117,358 classified tokens map to ~1.4B trade rows (99.5% of trades).

## Phase 1 dimensions (10 total)

Each dimension is computed either from the augmented per-contract columns alone or from a single trades_buy aggregation.

| # | Dimension column | Source | Hypothesized direction |
|---|---|---|---|
| 1 | `dim_resolution_type` | `event_resolution_type` as-is | data_driven_numeric → weaker FLB |
| 2 | `dim_info_type_supergroup` | substring regex over `event_info_type` (market_data / sports_data / weather_data / awards / politics_governance / culture_media / other) | market_data → weakest; politics/awards → strongest |
| 3 | `dim_primary_category` | first entry of `categories` list | Crypto/Sports → weaker; Politics/Mentions → stronger |
| 4 | `dim_subject_specificity` | `len(event_subjects)` binned (1 / 2 / 3+) | fewer subjects → weaker bias |
| 5 | `dim_event_family_size` | per-`event_template` token count, tiered (Singleton / Small / Medium / Large) | larger family → weaker bias |
| 6 | `dim_outcomes_per_event` | distinct tokens per `event_slug`, tiered (Binary / Few 2-5 / Many 6+) | binary → weaker bias |
| 7 | `dim_market_specificity` | `len(market_subjects) − len(event_subjects)` | narrower market → stronger FLB on longshot legs |
| 8 | `dim_dollar_volume_tier` | per-token sum of `usdcSize`, quartiles | higher volume → weaker bias |
| 9 | `dim_contract_horizon` | per-token (max_ts − min_ts), tiered (<1h / 1h-1d / 1d-1w / 1wk-1mo / >1mo) | medium horizon → weakest; ultra-short and ultra-long worse |
| 10 | `dim_recurrence_class` | event_template family size × time span (One-off / Episodic / Recurring / Daily) | Daily → weakest; One-off → strongest bias |

## Files

- `dimensions.py` — per-contract dimension extractors (dims 1-7). No trades scan.
- `dimensions_from_trades.py` — trade-aggregated dimension extractors (dims 8-10). One trades_buy scan.
- `flb_per_slice.py` — per-(dim, slice) FLB calibration with 2W primary + FM robustness; the SQL joins on `trades.conditionId = _contract_dims.token_id`.
- `plots.py` — calibration grid + dimension ranking bar chart.
- `run_phase1.py` — orchestrator: bot filter → views → load classifications → tag → SQL register → batch FLB → save artifacts + plots.
- `learnability_dimensions.ipynb` — notebook form of `run_phase1.py` for interactive use.

## Outputs (saved to `/mnt/data/learnability/output/`)

- `phase1_contract_dimensions.parquet` — per-token-id dimension assignments (1.12M rows)
- `phase1_flb_per_slice.parquet` — long-format per-(dim, slice, decile) calibration with 2W SE
- `phase1_spread_summary.parquet` / `.csv` — per-(dim, slice) spread + 2W SE + FM SE
- `phase1_dim_ranking.csv` — dimensions ranked by within-dim spread variance
- `phase1_dim_slice_counts.csv` — contract counts per slice (used for slice viability decisions)
- `phase1_calibration_grid.png` — 10-panel calibration curves
- `phase1_spread_ranking.png` — bar chart
- `phase1_bot_report.json` — attrition stats
- `phase1_findings.md` — written summary of findings

## Reproducing

From EC2 (instance must be started + EBS mounted at `/mnt/data`):
```bash
cd /home/ubuntu/learnability && /home/ubuntu/venv/bin/python -u run_phase1.py
```
Or open `learnability_dimensions.ipynb` and run cells in order.

## Phase 2 (deferred)

- No-bot-filter comparison pass (test whether bots learn over time too)
- Cross-cut by trader profitability tercile (informed / middle / uninformed — same definition as `results.ipynb` §3.2)
- Temporal evolution: per-template monthly FLB, volume-cohort-at-time-T, trader-experience curves, first-occurrence within family
- Subject_familiarity_score (top-100 entity table), base_rate_class, complexity_index
- Cross-platform pooling once Kalshi trade-level data lands
