# MLB game-dynamics pilot

## Research question

This pilot asks whether market predictability or structure is related to the
shape and magnitude of calibration bias (including favorite–longshot bias).
MLB game-winner markets are the first controlled subset because an official
game clock lets us distinguish trading before play from trading during early,
middle, and late innings. The subset is a tractable test bed, not a claim that
baseball is representative of all Polymarket markets.

The deliberately minimal v1 scope is:

- Polymarket MLB game-winner moneylines only; no props and no confidence
  prefilter. Price deciles belong in estimation, not candidate selection.
- Exact candidate slug shape `mlb-<team-1>-<team-2>-YYYY-MM-DD`, a nonempty
  question with no colon, and one candidate per event slug. The legacy
  candidate columns named `away` and `home` preserve the two observed slug
  positions; they do not assert official orientation.
- Exact official-date and exact two-team matching using stable MLB team IDs.
  Observed slug order is tried first as official away/home order; reversed order
  is tried only when no official-order game exists. The match audit records
  `slug_orientation`, while canonical away/home IDs and names come only from
  the MLB schedule. No fuzzy names, reschedule-origin matching, start-time
  proximity, or guessing among multiple games within the selected orientation.
- Official MLB schedule and live-feed data only. There is no ESPN fallback in
  v1.
- Ingestion replays removed by immutable EVM event identity. Buyer-level trade
  calibration uses BUY fills with `0.01 < price < 0.99` and excludes buyers
  carrying the existing `is_nonhuman` flag. The primary market close instead
  uses all valid exact prestart fills with bot participants included.
- A conservative **standard-timing core**: rescheduled, resumed, suspended,
  doubleheader-coded, shortened, or boundary-incomplete games remain in the
  audit but do not enter the core phase dataset. Competition stage is not a
  filter: `game_type` is retained, and a normal-timing postseason or spring
  game can remain eligible for later stratification.

## Measurement language

Calibration and the market close are different objects.

- For a trade in the purchased outcome, let `p` be its trade price and let
  `y = 1` if that outcome eventually won and `0` otherwise. The trade-level
  calibration error is `y - p`.
- The primary v1 **pregame closing line** is the last exact valid fill strictly
  before observed first play, ordered by block number, log index, and
  transaction hash. It requires the expected moneyline token, positive finite
  amounts, and `0 < p < 1`, but it includes bot participants. Its price is
  normalized to home-win probability (`p` for a home token, `1 - p` for an
  away token).
- The fixed closing sensitivity retains the existing trader-view rules:
  `0.01 < p < 0.99` and exclusion when the outcome-token buyer has
  `is_nonhuman = true`. It is buyer-centered, not an either-participant or
  human-to-human filter. `A - C`, the primary-versus-filtered close difference,
  is filter sensitivity and is not CLV.
- The close is an observed market price, not an eventual outcome or a latent
  “closing probability.” No quote midpoint is available in this v1 trade-only
  pipeline. To avoid ambiguity, no variable named CLV is estimated here. If a
  later analysis defines a trade-to-close difference, its side orientation and
  sign must be specified separately and it must remain distinct from `y - p`.

## Time and game-boundary trust model

The prior general trade transform cannot establish exact event time: its Stage
6 path permits linear block-time interpolation. Consequently,
`trades_clean` is not accepted as retrospective proof of exact timestamps for
boundary-sensitive MLB analysis.

The MLB pipeline instead rebuilds a narrow extract from raw
`resolved_trades.parquet` and joins it to the exact Polygon block-timestamp
cache. The declaration in
`configs/data_vintages/mlb_exact_timestamps_2026-07-04.json` is the trust root.
It pins the cache format, method, coverage metadata, byte size, and SHA-256;
requires zero missing blocks and zero fallback rows; and is explicitly scoped
to a new raw-source MLB extract.

The declaration is necessary but not sufficient. The extract retains
`block_number` and `timestamp`, and the pipeline fails on a null, a missing
cache block, a changed timestamp, a duplicate cache block, a cache hash
mismatch, or unequal extract/cache-subset block coverage. The phase builder
repeats the row-level proof before assigning a phase.

Official game boundaries come from completed MLB live-feed plate appearances,
not the scheduled start:

- game start: observed start of the first plate appearance;
- middle boundaries: observed starts of the top of the fourth and seventh;
- game end: observed end of the final plate appearance.

Starts are inclusive and ends are exclusive, except the final-play timestamp
remains in `innings_7_plus`. Trades after the final play are retained as
`post_final` audit rows and are not analysis-eligible. Raw MLB responses are
kept in a separate read-through cache so the published analysis run remains
immutable.

The primary phase view uses these literal boundaries. Its fixed sensitivity
drops, without reassigning, every trade within 30 seconds inclusive of first
play, the starts of innings 4 and 7, or final play.

## Seven-stage build

Run these commands from the repository root. Choose a new run ID and do not
reuse an output directory. The paths below are concrete templates for the
current data layout; verify them against the active data-vintage declarations
before a production run.

```sh
MLB_RUN_ROOT=/mnt/data/research_runs/mlb_game_dynamics/2026-09-07_v1
MLB_API_CACHE=/mnt/data/research_cache/mlb_stats_api
MLB_TIMESTAMP_DECL=configs/data_vintages/mlb_exact_timestamps_2026-07-04.json

mkdir -p "$MLB_RUN_ROOT/01_universe"
```

### 1. Select preliminary market candidates

```sh
python analysis/mlb_game_dynamics/build_market_universe.py \
  --markets /mnt/data/embedding_difficulty/universe_markets.parquet \
  --output "$MLB_RUN_ROOT/01_universe/candidates.parquet" \
  --diagnostics "$MLB_RUN_ROOT/01_universe/candidate_diagnostics.parquet"
```

Inspect the diagnostic reasons before continuing. The candidate file is only a
syntactic market universe; it is not yet proof of an MLB game or a valid
two-team moneyline. Its `away` and `home` fields retain observed slug order for
compatibility; Stage 2 determines official orientation. Duplicate event
candidates fail rather than being ranked.

### 2. Match official games and build timing audits

```sh
python analysis/mlb_game_dynamics/build_game_timing.py \
  --candidates "$MLB_RUN_ROOT/01_universe/candidates.parquet" \
  --cache-dir "$MLB_API_CACHE" \
  --output-dir "$MLB_RUN_ROOT/02_timing"
```

Use `--refresh` only for a deliberate API-cache refresh. This stage emits one
candidate match audit even when matching or live-feed parsing fails; it never
chooses one game when the selected official or reversed orientation contains
multiple same-date records. `away_slug` and `home_slug` preserve observed slug
positions, `slug_orientation` records their relationship to the official
schedule, and `away_team_*`/`home_team_*` are canonical schedule fields. A
moved game's `rescheduled_from_date` is retained for audit but is not a match
key.

### 3. Validate moneylines and freeze timing eligibility

```sh
python analysis/mlb_game_dynamics/build_validated_universe.py \
  --candidates "$MLB_RUN_ROOT/01_universe/candidates.parquet" \
  --timing-run-dir "$MLB_RUN_ROOT/02_timing" \
  --universe-tokens /mnt/data/embedding_difficulty/universe_tokens.parquet \
  --token-map /mnt/data/pipeline_data/token_map.parquet \
  --output-run-dir "$MLB_RUN_ROOT/03_validated"
```

This stage composes canonical token rows, requires exactly two unique tokens
and two recognized team outcomes, and checks the Polymarket resolution against
the official score and winner flags. Moneyline validity and timing suitability
are separate columns. Only their intersection enters
`eligible_moneylines.parquet`; every candidate remains in the audit.

### 4. Build the exact-timestamp filtered trader extract

```sh
python analysis/mlb_game_dynamics/build_exact_trades.py \
  --raw /mnt/data/pipeline_data/resolved_trades.parquet \
  --candidates "$MLB_RUN_ROOT/01_universe/candidates.parquet" \
  --timestamp-provenance "$MLB_TIMESTAMP_DECL" \
  --wallet-flags /mnt/data/learnability/cache/wallet_flags.parquet \
  --run-dir "$MLB_RUN_ROOT/04_exact_trades"
```

Candidate filtering happens before the expensive trade expansion. Every
candidate must have raw source coverage. Event-identity deduplication,
timestamp coverage, price attrition, bot attrition, output rows, and dollars
must reconcile before atomic publication.

This builder supplies the buyer-filtered trade-calibration view. Because that
view has already removed extreme-price and flagged-buyer fills, it is not the
source for the primary closing line. Stage 6 constructs both close definitions
directly from the exact deduplicated raw-fill projection.

### 5. Assign phases and retain the filtered close audit

```sh
python analysis/mlb_game_dynamics/build_phase_dataset.py \
  --trades "$MLB_RUN_ROOT/04_exact_trades/exact_trades.parquet" \
  --eligible-moneylines "$MLB_RUN_ROOT/03_validated/eligible_moneylines.parquet" \
  --timestamp-provenance "$MLB_TIMESTAMP_DECL" \
  --run-dir "$MLB_RUN_ROOT/05_phases"
```

The five exhaustive phases are `pregame`, `innings_1_3`, `innings_4_6`,
`innings_7_plus`, and `post_final`. The reconciliation report must show that
eligible rows and dollars partition exactly across all five.

The existing `closing_lines.parquet` is definition C, the buyer-filtered
sensitivity, not the all-valid-fill primary definition A. Estimator inputs must
keep those labels explicit and must not silently substitute C for A. The
estimator uses Stage 6's dual-close artifact; this Stage 5 close remains an
independent reconciliation target.

### 6. Build the primary and sensitivity closing lines

```sh
python analysis/mlb_game_dynamics/build_dual_closes.py \
  --raw /mnt/data/pipeline_data/resolved_trades.parquet \
  --eligible-moneylines "$MLB_RUN_ROOT/03_validated/eligible_moneylines.parquet" \
  --timestamp-provenance "$MLB_TIMESTAMP_DECL" \
  --wallet-flags /mnt/data/learnability/cache/wallet_flags.parquet \
  --run-dir "$MLB_RUN_ROOT/07_dual_closes_v1"
```

This implemented builder returns `game_closes.parquet`, exactly one row per
eligible market/game, and `reconciliation.json`. The row carries shared game,
team, token, winner, and start-time dimensions; source-fill counts; and
parallel `primary_*` and `sensitivity_*` availability, missing-reason, price,
home-probability, wallet, bot-label, timestamp, block, and immutable-event
identity fields. Primary uses `0 < price < 1` with bots included. Sensitivity
uses `0.01 < price < 0.99` and excludes only flagged outcome-token buyers.

The reconciliation records input fingerprints, raw/distinct/replay counts,
exact-cache coverage, primary and sensitivity coverage, missing reasons,
same-versus-different close identities, counterparty-bot diagnostics, and
hard Boolean partition/subset gates. Publication uses a fresh staging sibling
and atomic rename.

### 7. Estimate the fixed-bin calibration profiles

```sh
python analysis/mlb_game_dynamics/estimate_calibration.py \
  --phase-trades "$MLB_RUN_ROOT/05_phases/phase_trades.parquet" \
  --dual-closes "$MLB_RUN_ROOT/07_dual_closes_v1/game_closes.parquet" \
  --run-dir "$MLB_RUN_ROOT/08_calibration_v1"
```

The estimator consumes the authoritative `primary_*` and `sensitivity_*`
dual-close fields directly. It validates one-to-one game coverage, close-event
identity, prestart timing, exact phase membership, and the phase/close game
relationship before publishing a fresh immutable run. Phase games must be a
dimension-consistent subset of the dual-close universe; close-only games remain
in closing estimates. The builder has already
validated the underlying token-to-home-probability normalization.

## Published run layout

```text
<run-id>/
├── 01_universe/
│   ├── candidates.parquet
│   └── candidate_diagnostics.parquet
├── 02_timing/
│   ├── schedule_audit.parquet
│   ├── match_audit.parquet
│   ├── game_timing.parquet
│   └── summary.json
├── 03_validated/
│   ├── candidate_validation_audit.parquet
│   ├── eligible_moneylines.parquet
│   └── summary.json
├── 04_exact_trades/
│   ├── exact_trades.parquet
│   └── build_audit.json
├── 05_phases/
│   ├── phase_trades.parquet
│   ├── closing_lines.parquet
│   ├── closing_audit.parquet
│   ├── boundary_audit.parquet
│   └── reconciliation.json
├── 07_dual_closes_v1/
│   ├── game_closes.parquet
│   └── reconciliation.json
└── 08_calibration_v1/
    ├── closing_calibration.parquet
    ├── closing_paired_sensitivity.parquet
    ├── trade_phase_calibration.parquet
    └── estimator_summary.json
```

Stages 2–7 publish through fresh sibling staging directories and atomic rename;
an existing destination is an error. Stage 1 writes two explicit files, so the
operator must also place them only in a fresh run root. Never “repair” an old
run in place. A rerun receives a new ID, and the raw MLB API cache remains
outside this layout.

## Gates before estimation

Do not run or inspect an estimator until all of the following are reviewed:

- Candidate and diagnostic counts reconcile; there is one candidate per event.
- There is one match-audit row per candidate and one eligible market per game;
  ambiguous, nonfinal, All-Star, and unmatched cases remain explainable.
- Observed slug teams match the schedule exactly in the recorded orientation;
  `slug_orientation` and canonical schedule away/home identity reconcile across
  candidate, match, validated, and timing artifacts.
- Match, schedule, timing, date, score, and winner fields agree across artifacts.
- Canonical token composition has full candidate coverage and every eligible
  market has exactly two unique recognized team outcomes.
- The standard-timing exclusions and missing-boundary counts are accepted
  before seeing estimates; `game_type` remains available for stratification.
- The timestamp declaration and its SHA-256 pass, every raw candidate block is
  covered, and every extract row equals its cached timestamp.
- Replay deduplication and price/bot attrition reconcile without collapsing
  distinct EVM events.
- Phase rows and dollars reconcile; missing or stale pregame closes and trade
  mass near each timing boundary are explicitly audited.
- The primary all-valid-fill close covers the eligible game dimension and the
  buyer-filtered sensitivity reproduces the published filtered close exactly.
- Literal phase membership and the inclusive 30-second boundary exclusion
  reconcile independently to their source rows.
- Dual-close source, replay, per-game, availability, identity, and subset
  reconciliation gates all pass.
- Each estimator artifact has its fixed row count; phase outputs reconcile to
  the filtered phase sample while closing outputs retain the full dual-close
  universe.

The production v3 matching, timing, validation, phase, and close-recency audits
are complete. The dual-close builder and estimator are implemented and have
passed independent cross-review. Their immutable `07_dual_closes_v1` and
`08_calibration_v1` production runs are complete and independently approved.

## Next research step

The implemented first estimator is a descriptive calibration profile using
fixed bins `[0, 0.1)`, `[0.1, 0.2)`,
through `[0.8, 0.9)`, and `[0.9, 1]`. It reports mean `y - p` separately for
pregame and each live third under the literal-boundary primary and the inclusive
30-second exclusion sensitivity. The phase profile is equal-trade/count-weighted
only; dollars are descriptive exposure. Closing calibration uses one
equal-weight observation per game for both the all-valid-fill primary and the
buyer-filtered sensitivity. A bin whose effective `n` is below 50 remains in
the audit but has its estimate suppressed and is labeled exploratory.

No complexity proxy, price-path variance, heterogeneity regression, or broader
structural model belongs in this first estimator.

### Implemented estimator artifact contract

The estimator emits four files:

- `closing_calibration.parquet` has exactly **22 rows**: for each of the
  `primary` and `sensitivity` close definitions, one overall row plus ten
  fixed-bin rows. It reports game count, suppression
  status, mean home probability, home win rate, mean calibration, official-date
  clustered standard error and 95% interval, and Brier score.
  Its columns are `close_definition`, `profile_scope`, `price_decile`,
  `price_bin`, `game_count`, `suppressed`, `status`, `mean_probability`,
  `win_rate`, `mean_calibration`, `calibration_se`, `calibration_ci95_low`,
  `calibration_ci95_high`, and `brier_score`.
- `closing_paired_sensitivity.parquet` has exactly **11 rows**: one overall row
  plus ten bins defined by the primary closing probability. It reports common
  games, same/different close-event and timestamp counts, paired primary and
  sensitivity probabilities, calibration and Brier summaries, and their
  paired differences. The price comparison is defined as primary A minus
  sensitivity C; it is filter attribution, not CLV.
  Its columns are `profile_scope`, `primary_price_decile`,
  `primary_price_bin`, `common_games`, `suppressed`, `status`,
  `same_close_event_games`, `different_close_event_games`,
  `same_close_timestamp_games`, `different_close_timestamp_games`,
  `primary_mean_probability`, `sensitivity_mean_probability`,
  `mean_probability_difference`, `mean_absolute_probability_difference`,
  `primary_mean_calibration`, `sensitivity_mean_calibration`,
  `mean_calibration_difference`, `primary_brier_score`,
  `sensitivity_brier_score`, and `mean_brier_difference`.
- `trade_phase_calibration.parquet` has exactly **80 rows**: literal and
  `exclude_within_30s` samples x four ordered phases x ten fixed bins. The sole
  estimator is equal-trade/count weighted. Trade count, game count, and dollars
  are retained as audit denominators; dollars are not weights. Reported fields
  are mean price, win rate, mean calibration, CGM day x wallet x game clustered
  standard error, and 95% interval.
  Its columns are `boundary_sample`, `phase`, `phase_order`, `price_decile`,
  `price_bin`, `trade_count`, `game_count`, `dollars`, `suppressed`, `status`,
  `mean_price`, `win_rate`, `mean_calibration`, `calibration_se`,
  `calibration_ci95_low`, and `calibration_ci95_high`.
- `estimator_summary.json` fingerprints both inputs and records the calibration,
  bin, close, boundary, weighting, suppression, and uncertainty definitions;
  phase and close coverage; per-sample phase counts and dollars; the fixed
  output row counts; output names; and `exploratory_descriptive` status.

Every fixed-bin grid row is retained. When the relevant count is below 50,
counts remain visible while estimate and uncertainty fields are null and the
row status is `suppressed_n_lt_50`. Code implementation and independent cross-review are
complete; the immutable production artifacts have also passed independent count, schema,
lineage, and result audits. Interpretation remains exploratory rather than confirmatory.

## Team workflow

- The main orchestrator owns the broad research path, cross-stage decisions,
  integration, and production-run authorization. It is the only actor allowed
  to start, stop, mount, unmount, reboot, or otherwise manage EC2 resources.
- Worker agents receive bounded tasks and isolated file ownership. They never
  manage instance lifecycle, never commit or push canonical changes, and do not
  alter files outside their assignment.
- Each worker reports files changed, design choices, uncertainties, and direct
  test results. Another agent independently reviews interfaces, failure modes,
  and scientific assumptions before integration.
- The orchestrator resolves review findings, integrates only reviewed changes,
  and records material research decisions here or in `RESEARCH_LOG.md`.
- Exploration happens in a new isolated branch/worktree or run directory. The
  canonical tree and published artifacts remain reproducible and immutable.
