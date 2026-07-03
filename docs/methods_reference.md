# Methods & data-practices reference

The durable "how we do it": the practices applied consistently across this project's analyses.
This doc deliberately contains **no result claims** — findings anchor future analysis, and the
point of the current pivot is to measure without a prior. Historical writeups (with their
superseded claims annotated) live in `docs/archive/`.

## The project, stated neutrally

We study **price calibration on prediction markets** (Polymarket trade-level data; a parallel
Kalshi contract classification exists for comparison work). The focal object is the
**favorite-longshot bias (FLB)**: a miscalibration pattern in which low-priced contracts win
less often than their price implies and high-priced contracts win more often. The current
question is **how calibration varies with market characteristics** derived from native
Polymarket metadata — recurrence/series structure, resolution mechanics, anchorability,
feedback speed ("learnability" dimensions). Direction is measured, not presumed: dimensions
are built and run without encoding an expected sign.

## Data canon

- **Canonical trade set:** `/mnt/data/pipeline_output/trades_clean.parquet` (EC2) —
  **2,018,709,888 rows through 2026-06-23**. Cleaning removed only full-row (all-11-column)
  exact duplicates (~4%, ingestion replays); multi-counterparty partial fills are real and
  retained. Raw set kept for diffing at `/mnt/data/pipeline_root_output/trades.parquet`.
- **Resolution-censoring caveat (read before any recency analysis):** `trades_clean` contains
  only markets that had **resolved by build time** (Stage-4 INNER JOIN against Gamma closed
  markets). Recent months are therefore censored toward fast-resolving markets — at the
  June 2026 build, ~7.5% of events / ~12.2% of dollars were absent, concentrated in
  long-horizon judgment markets. End-of-sample or across-time comparisons must be
  horizon-matched or carry this caveat explicitly. Refresh path: re-pull Gamma resolutions,
  rerun pipeline stages 4/6 from stored raw events (no Polygon re-fetch needed).
- **Native market metadata:** `/mnt/data/learnability/native/` —
  `native_market_meta.parquet` (Gamma re-pull 2026-06-21; 1.44M markets; **closed markets
  only** — mirrors the trade set's censoring, same caveat applies) and
  `market_native_categories.parquet` + `final_tag_map_v1.json` (native Gamma tags → category
  map v1, curated 2026-07-01: 264 category tags → 12 primary categories, holdout-validated
  against the LLM labels; the JSON map is committed in `analysis/learnability/native/`).
- **Join-key gotcha:** in the trades parquet, `conditionId` holds the **77-digit per-outcome
  token id**, not the 0x-hex per-market condition id. The augmented classification parquet
  carries both (`token_id`, `condition_id`); join trades on `token_id`. The **local** sample
  parquet is keyed differently from the EC2 set — never run joins locally.
- **Pipeline trust:** the trade set was externally cross-validated (2026-06-22) against an
  independently collected dataset — near-total token coverage and resolution agreement.
  Re-validate after any pipeline change, not before each analysis.

## Standard trade filters (defaults for every calibration run)

- **BUY-side trades only**; per-trade return `ret = (1 − price)` if the outcome resolved YES
  for the bought side, else `−price`.
- **Price filter** `0.01 < price < 0.99`.
- **Exclude up/down markets** (`eventSlug LIKE '%updown%' OR '%up-or-down%'`) — mechanical
  short-horizon crypto series that would otherwise dominate counts.
- **Exclude bot wallets** via the behavioral composite (inter-trade interval,
  trades-per-active-day, hour-of-day concentration/HHI, fixed-trade-size share) —
  `scripts/build_wallet_flags_clean.py`.
- **Slice floor:** minimum 5,000 trades per slice; slices below the floor are dropped, and
  when a run caps or drops slices it must say so in its output.
- **Lifecycle windows:** trades at 25–80% of contract lifetime ("mature"), 80–100%
  ("closing"), and 0–100% ("full"); report at least mature and closing separately.

## Calibration measurement

- **10 price-decile bins** per slice; realized win rate vs. price per decile.
- **Primary summary: signed calibration slope** — keep it directional. The **D10−D1
  calibration-error spread** is a secondary summary only: it is known to manufacture
  apparent "reversals" when tail deciles are thin or composition shifts, so never headline
  a sign flip from D10−D1 alone; check the slope and the full decile curve first.
- **Weighting:** report both count-weighted and dollar-weighted versions.
- **Standard errors:** Cameron–Gelbach–Miller **3-way clustered** (day × wallet × market).
  Across many slices, use Bonferroni-adjusted significance stars.
- **Grain:** trade-level calibration is the default; contract-level (equal-weighted VWAP)
  is the robustness variant.
- **Engine:** `analysis/learnability/flb_per_slice.py`; drivers `run_phase1.py` (v6 dims)
  and `run_v7.py` (native dims). Outputs land in `/mnt/data/learnability/output/`.

## Compute practices

- All real computation on EC2 (workflow, paths, and costs in `CLAUDE.md`). DuckDB lazy views
  over parquet; never load the full trade set into pandas.
- Memory-heavy intermediate builds run in subprocesses that write parquet and exit; the
  parent re-registers the file as a lazy VIEW (`analysis/subprocess_runner.py`). These are
  views — clear with `DROP VIEW IF EXISTS`, and delete the parquet to force a rebuild.

## Retired claims — do not reassert

Recorded only so they are not rediscovered and re-anchored on:

- **"High-volume crypto/sports markets show reversed FLB."** A D10−D1 artifact; the signed
  calibration slopes there are ≈ 1 (well-calibrated). Retired 2026-07.
- **"FLB disappeared in the newest data" / per-category collapse headlines.** Resolution-
  censoring composition artifacts (see caveat above). Retired 2026-07.
- **"~17% duplicate / ~20% wash trading in the trade set."** Partial-fill counting artifact;
  the real replay rate was ~4%, removed in `trades_clean`. Retired 2026-06.
