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
  **2,036,128,538 rows through 2026-06-23** (resolutions snapshot 2026-07-03;
  first de-censoring refresh executed 2026-07-04, Stage-4 coverage 97.4%). Cleaning removed only full-row (all-11-column)
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
- **Consolidated market dimensions (2026-07-04):**
  `/mnt/data/learnability/output/market_dimensions_v1.parquet` (built by
  `analysis/learnability/market_dimensions_v1.py`) — one row per market joining
  labels v2, embedding novelty/cluster assignments, trade-derived lifetime and
  filtered dollar volume, the resolution-domain anchor ladder, series ordinals,
  and vintage. Preferred starting point for any cross-dimension analysis.
- **Canonical token spine (2026-07-03):** `/mnt/data/pipeline_output/market_flags.parquet`
  (built by `scripts/build_market_flags.py`) — one row per token: `token_id`, `market_id`
  (0x hex), `winning_outcome`, market-level `is_updown` flag, `question`. Covers **100% of
  the distinct tokens in trades_clean** (hard-checked at build). Use this for resolution
  joins and the up/down exclusion. `pipeline/output/market_resolutions_enriched.parquet`
  (Mar 2026) is **retired as a spine** — it covers only ~49% of extended-set trade rows.
- **Join-key gotcha:** in the trades parquet, `conditionId` holds the **77-digit per-outcome
  token id**, not the 0x-hex per-market condition id. The augmented classification parquet
  carries both (`token_id`, `condition_id`); join trades on `token_id`. The **local** sample
  parquet is keyed differently from the EC2 set — never run joins locally.
- **eventSlug gotcha (extended set):** trades' `eventSlug` is the **empty string** for newer
  markets (June-2026 refresh gap), so no filter may rely on trades' `eventSlug` alone.
- **Pipeline trust:** the trade set was externally cross-validated (2026-06-22) against an
  independently collected dataset — near-total token coverage and resolution agreement.
  Re-validate after any pipeline change, not before each analysis.

## Standard trade filters (defaults for every calibration run)

- **BUY-side trades only**; per-trade return `ret = (1 − price)` if the outcome resolved YES
  for the bought side, else `−price`.
- **Price filter** `0.01 < price < 0.99`.
- **Exclude up/down markets** — mechanical short-horizon crypto series that would otherwise
  dominate counts. **Excluded at MARKET level** via `market_flags.parquet` (`is_updown`,
  derived from Gamma event_slug/series_slug/tags/question patterns), joined on token id.
  The historical trade-level filter (`eventSlug LIKE '%updown%' OR '%up-or-down%'`) no
  longer works on the extended set (empty eventSlug) and must not be used alone.
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

## Reporting reproducibility

- A findings report (HTML) is a **rendering of computed artifacts, not the computation**:
  a finalized report's headline numbers must come from a **committed script** (repo path)
  whose summary output (parquet/JSON) is saved — large artifacts under `/mnt/data/`, small
  ones alongside the report. The report states, near the top, which script and artifact(s)
  produced its numbers.
- Ad-hoc DuckDB queries are fine while exploring; **promote them into a script before the
  report is finalized**. If a report's numbers can't be regenerated by rerunning a named
  script, the report isn't done.

## Retired claims — do not reassert

Recorded only so they are not rediscovered and re-anchored on:

- **"High-volume crypto/sports markets show reversed FLB."** A D10−D1 artifact; the signed
  calibration slopes there are ≈ 1 (well-calibrated). Retired 2026-07.
- **"FLB disappeared in the newest data" / per-category collapse headlines.** Resolution-
  censoring composition artifacts (see caveat above). Retired 2026-07.
- **"Within-horizon, long-horizon FLB genuinely declined into early 2026."** The >120d
  version of this read was itself still censoring-contaminated: on the de-censored set
  (resolutions 2026-07-03) the >120d slope sits at its 2025 magnitude in both 2026
  periods (dollar-weighted Bonferroni-robust in May–Jun). The 30–120d attenuation
  remains open and not significant either way; 2026 long-horizon cells stay partially
  resolution-selected until further refreshes. Retired 2026-07
  (`analysis/learnability/horizon_flb_v2.py` artifacts).
- **"~17% duplicate / ~20% wash trading in the trade set."** Partial-fill counting artifact;
  the real replay rate was ~4%, removed in `trades_clean`. Retired 2026-06.
