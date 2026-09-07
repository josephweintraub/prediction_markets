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
- BUY fills with `0.01 < price < 0.99`, ingestion replays removed by immutable
  EVM event identity, and known nonhuman buyer wallets excluded.
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
- The v1 **pregame closing line** is the last eligible fill strictly before the
  observed first play, ordered by block number, log index, and transaction
  hash. The phase builder records both that fill's own-outcome price and a
  side-normalized home-win probability (`p` for a home token, `1 - p` for an
  away token).
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

## Five-stage build

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

### 4. Build the exact-timestamp BUY-fill extract

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

### 5. Assign phases and construct the close

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
└── 05_phases/
    ├── phase_trades.parquet
    ├── closing_lines.parquet
    ├── closing_audit.parquet
    ├── boundary_audit.parquet
    └── reconciliation.json
```

Stages 2–5 publish through fresh sibling staging directories and atomic rename;
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

Production coverage figures are provisional until the complete real-data run
finishes and these artifacts are reviewed.

## Next research step

The current pipeline stops before estimation. The simplest next step is a
descriptive price-decile calibration profile—mean `y - p`, with clearly stated
trade and dollar weighting—for pregame and each live third, accompanied by
market/game counts and uncertainty appropriate to the existing FLB framework.
Closing-line age and the boundary audits should be reported before interpreting
phase differences.

Only after that baseline is stable should the analysis add complexity proxies.
A natural candidate is within-game variation in the side-normalized implied
probability, measured separately by phase. Because realized price variance is
endogenous to news and trading activity, it should initially be described as
path variability rather than inherent market complexity, with its definition
frozen before estimates are viewed.

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
