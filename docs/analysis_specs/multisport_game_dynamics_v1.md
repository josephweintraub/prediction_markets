# Multisport moneyline game dynamics v1

**Status:** amended production contract. The upstream collection covers NHL, men's college
basketball (CBB), ATP, WTA, English Premier League (EPL), college football (CFB), WNBA,
and UFC. The released combined estimator retains NHL, CBB, ATP, EPL, CFB, and WNBA;
WTA and UFC remain in the upstream audit artifacts but are excluded for insufficient
support.

## Research contract

The observation is a BUY fill in a resolved, provider-matched event market.  The phase
estimand is the equal-fill mean of bought-contract calibration,
`eventual outcome of the bought token - purchase price`.  The closing estimand is the same
quantity for one final pregame BUY fill per market.  Dollars are descriptive and never an
estimator weight.  The analysis reports complete fixed-bin profiles first and D1/D10 tails
second; it does not treat the D10-D1 spread alone as proof of favorite-longshot bias.

The cohort inherits the resolved-market censoring of the canonical Polymarket inputs.  It
does not support end-of-sample or across-time claims without a horizon-matched analysis.

## Cohorts and source status

`analysis/multisport_game_dynamics/contracts.py` is the source of truth for sport keys,
provider paths, phase order, and folding.  ESPN resources are third-party undocumented,
not official league feeds.  Raw responses used by Stage 02 must be retained and
fingerprinted in its immutable `source_cache`.

| Cohort | ESPN scoreboard/summary path below `/apis/site/v2/sports/` | Live phase unit | Folded final phase |
|---|---|---|---|
| NHL | `hockey/nhl` | period | Period 3 and overtime/shootout |
| CBB | `basketball/mens-college-basketball` | half | Second half and overtime |
| ATP | `tennis/atp` | elapsed third | Final elapsed third |
| WTA | `tennis/wta` | elapsed third | Final elapsed third |
| EPL | `soccer/eng.1` | half | Second half and stoppage time |
| CFB | `football/college-football` | quarter | Quarter 4 and overtime |
| WNBA | `basketball/wnba` | quarter | Quarter 4 and overtime |
| UFC | `mma/ufc` | round | Round 3 and later rounds |

For NHL, CBB, EPL, CFB, and WNBA, scoreboard data supply identity, completion, and result,
while summary play-by-play supplies literal wall clocks.  UFC identity/status comes from
the site scoreboard and round timing from hydrated ESPN Core UFC play resources.  ATP/WTA
identity/status and start time come from the ESPN scoreboard; duration comes from the
Tennis Abstract Jeff Sackmann archive mirror, explicitly classified
`third_party_public_archive`.  Tennis retirements, walkovers, defaults, abandonments,
missing duration, or non-unique duration matches are excluded.

## Strict candidate and provider matching

Stage 01 reads closed native market metadata and the canonical token spine.  A candidate
must satisfy all of the following:

- sport key is one of the eight keys above and `event_slug` ends in an ISO date;
- native `sports_market_type = 'moneyline'`;
- exactly two distinct token IDs and outcome labels, one common winning-outcome label,
  and exactly one winning token;
- exactly one market per event for every cohort except EPL;
- EPL has exactly three negative-risk binary propositions representing the two teams and
  draw; exactly one proposition resolves YES;
- a two-participant matchup and the resolved result can be parsed without guessing.

Every event reaching event-level classification within the selected native-moneyline and
valid-token universe remains reconcilable in candidate diagnostics.  Stage 02
normalizes only case, accents, punctuation, and whitespace; it permits exact or contiguous
whole-token containment and accepts only a unique highest-scoring two-name mapping.  It
does not use edit distance, phonetics, input order, market volume, or a confidence score.

Non-tennis events match on cohort, market date, two participants, provider state `post`,
and the provider result. Tennis permits at most a seven-calendar-day difference between
the market listing date and the ESPN competition timestamp because a tournament market
can be listed several days before the match. EPL accepts either one unique winner or a provider draw with no winner;
all other cohorts require one unique provider winner matching Polymarket resolution.
Zero matches, multiple completed matches, duplicate candidate-to-provider competition
mappings, nonfinal status, or result disagreement are exclusions, not imputations.

## Literal phase windows

Let `t` be the exact UTC block timestamp, `s_j` the accepted start of provider period
`j`, and `e` the accepted final event time. Pregame is `t < s_1`. Every nonfinal
live phase is `[s_j, s_{j+1})`; the folded final live phase is `[s_k, e]`.  Rows with
`t > e` remain in the exact-fill audit and do not enter phase estimation.  There is no
grace period, buffer, or boundary reassignment in v1.

| Cohort | Ordered analysis phases and exact windows |
|---|---|
| NHL | Pregame `t<s1`; Period 1 `[s1,s2)`; Period 2 `[s2,s3)`; Period 3 and overtime/shootout `[s3,e]` |
| CBB | Pregame `t<s1`; First half `[s1,s2)`; Second half and overtime `[s2,e]` |
| ATP | Pregame `t<s1`; First elapsed third `[s1,s2)`; Middle elapsed third `[s2,s3)`; Final elapsed third `[s3,e]` |
| WTA | Pregame `t<s1`; First elapsed third `[s1,s2)`; Middle elapsed third `[s2,s3)`; Final elapsed third `[s3,e]` |
| EPL | Pregame `t<s1`; First half `[s1,s2)`; Second half and stoppage time `[s2,e]` |
| CFB | Pregame `t<s1`; Quarter 1 `[s1,s2)`; Quarter 2 `[s2,s3)`; Quarter 3 `[s3,s4)`; Quarter 4 and overtime `[s4,e]` |
| WNBA | Pregame `t<s1`; Quarter 1 `[s1,s2)`; Quarter 2 `[s2,s3)`; Quarter 3 `[s3,s4)`; Quarter 4 and overtime `[s4,e]` |
| UFC | Pregame `t<s1`; Round 1 `[s1,s2)`; Round 2 `[s2,s3)`; Round 3 and later rounds `[s3,e]` |

For standard ESPN play feeds, `s_j` is the first literal, timezone-aware event timestamp
in period `j`; periods must be contiguous and timestamps monotone.  The accepted end is
the final literal competitive or terminal timestamp after administrative timeout-like
rows are excluded.  Every required start and the end must be strictly ordered.

ATP/WTA windows are elapsed thirds, not inferred set boundaries. Let `s_1` be the
ESPN scoreboard competition time and `d` the unique completed-match archive duration in seconds:
`s_2=s_1+d/3`, `s_3=s_1+2d/3`, and `e=s_1+d`.  The archive match must agree on tour,
both players, winner, and a tournament date no more than 21 days before the market date;
an ambiguous match is excluded. Tennis phase estimates remain separately labeled and
must pass the exact-fill pre/live/post timing-distribution audit before release.

## Trades, closes, calibration, and uncertainty

Stage 03 scopes raw resolved fills to timing-eligible markets, removes exact replay rows,
and requires every immutable EVM identity to be unique.  BUY expansion must reconcile one
row per source fill.  Timestamps come only from
`/mnt/data/pipeline_data/block_timestamps.parquet`: every scoped block must have exactly
one cached timestamp, with zero missing blocks and no interpolation or fallback.

Phase profiles retain `0.01 < price < 0.99` BUY fills and exclude a fill only when its
outcome-token buyer has `is_nonhuman = true`.  Closing tables are separate:

- **All trades:** last valid `0 < price < 1` BUY fill strictly before `s_1`, including
  flagged buyers.
- **Filtered trades:** last `0.01 < price < 0.99` BUY fill strictly before `s_1` whose
  outcome-token buyer is not flagged.

The comparison is filter sensitivity, not closing-line value.  A close is selected per
market, so an eligible EPL event can contribute three proposition-market closes.

Bins are fixed bought-price intervals D1-D10:
`[0,.1), [.1,.2), ..., [.8,.9), [.9,1]`.  Each profile reports support, event count,
dollars, mean price, win rate, mean calibration, standard error, and nominal 95% interval.
Cells with fewer than 500 observations retain support but suppress estimates.  Tail rows
report D1 error, D10 error, and `D10 - D1`; the entire tail estimate is suppressed if
either tail has fewer than 500 observations.

Phase standard errors use Cameron-Gelbach-Miller clustering by UTC trade day, buyer
wallet, and event.  Closing tables use one-way market-date clustering.  D10-D1 uncertainty
is computed jointly from cluster scores and includes D1/D10 covariance.

The six retained new cohorts define 24 phases, hence 240 phase-bin rows, 120 closing-bin
rows, and 36 tail rows before suppression. Stage 04 also normalizes the frozen MLB/NFL/NBA
artifacts, producing a nine-cohort grid of 380 phase-bin rows, 180 closing-bin rows,
and 56 tail rows. Missing grid cells must be serialized with zero support and suppressed
estimates rather than omitted. WTA and UFC must be absent from every Stage 04 normalized
observation and summary artifact.

## Immutable production stages

Run from `/home/ubuntu/prediction_markets` with the project interpreter. Each target stage
directory must be new; each stage publishes through a staging directory and atomic rename.
The MLB/NFL/NBA inputs below are the frozen approved runs and must not be substituted or
mixed with another vintage.

```sh
PY=/home/ubuntu/venv/bin/python
RUN_ROOT=/mnt/data/runs/2026-09-14_multisport_game_dynamics_v1
MLB_ROOT=/mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1
NFL_ROOT=/mnt/data/runs/2026-09-12_nfl_game_dynamics_v3
NBA_ROOT=/mnt/data/runs/2026-09-12_nba_game_dynamics_v3
```

### 01_candidates

```sh
"$PY" -m analysis.multisport_game_dynamics.build_candidates \
  --market-meta /mnt/data/learnability/native/native_market_meta.parquet \
  --token-map /mnt/data/pipeline_data/token_map.parquet \
  --universe-tokens /mnt/data/embedding_difficulty/universe_tokens.parquet \
  --run-dir "$RUN_ROOT/01_candidates"
```

Review `candidate_manifest.json` and `candidate_diagnostics.parquet` before Stage 02.

### 02_timing_v3

```sh
"$PY" -m analysis.multisport_game_dynamics.build_timing \
  --candidate-run "$RUN_ROOT/01_candidates" \
  --run-dir "$RUN_ROOT/02_timing_v3"
```

Review `match_audit.parquet`, `event_timing.parquet`, `phase_boundaries.parquet`, the
provider inventory, every exclusion count, and per-sport timing quality before Stage 03.

### 03_trades

```sh
"$PY" -m analysis.multisport_game_dynamics.build_trade_dataset \
  --raw-trades /mnt/data/pipeline_data/resolved_trades.parquet \
  --timestamp-cache /mnt/data/pipeline_data/block_timestamps.parquet \
  --wallet-flags /mnt/data/pipeline_data/wallet_flags.parquet \
  --candidate-run "$RUN_ROOT/01_candidates" \
  --timing-run "$RUN_ROOT/02_timing_v3" \
  --run-dir "$RUN_ROOT/03_trades"
```

Require `missing_exact_blocks = 0`, no duplicate block mapping or EVM identity, and exact
source-fill/BUY/phase/close reconciliation in `trade_manifest.json`.

### 04_estimates_v2

```sh
"$PY" -m analysis.multisport_game_dynamics.estimate_combined \
  --new-trade-run "$RUN_ROOT/03_trades" \
  --mlb-phase "$MLB_ROOT/05_phase_dataset_v3/phase_trades.parquet" \
  --mlb-closes "$MLB_ROOT/07_dual_closes_v1/game_closes.parquet" \
  --nfl-phase "$NFL_ROOT/06_phase/phase_trades.parquet" \
  --nfl-closes "$NFL_ROOT/07_closes/game_closes.parquet" \
  --nfl-exact "$NFL_ROOT/05_exact/exact_trades.parquet" \
  --nfl-eligible "$NFL_ROOT/03_handoff/eligible_moneylines.parquet" \
  --nba-phase "$NBA_ROOT/06_phase/phase_trades.parquet" \
  --nba-closes "$NBA_ROOT/07_closes/game_closes.parquet" \
  --nba-exact "$NBA_ROOT/05_exact/exact_trades.parquet" \
  --nba-eligible "$NBA_ROOT/03_handoff/eligible_moneylines.parquet" \
  --run-dir "$RUN_ROOT/04_estimates_v2"
```

Require the complete row grids above, support/suppression reconciliation, and estimator
manifest fingerprints before rendering.

### 05_report_v3

```sh
"$PY" -m analysis.multisport_game_dynamics.render_latex \
  --estimator-run "$RUN_ROOT/04_estimates_v2" \
  --timing-run "$RUN_ROOT/02_timing_v3" \
  --run-dir "$RUN_ROOT/05_report_v3"
```

The portable `.tex` source and deterministic PDF figures are the report artifacts.
Compilation is visual QA; the renderer performs no estimation.  Do not publish unless
the report manifest reopens every input and all fixed-bin tables show support and
suppression status.

## Production stop rule

Only the root operator may start, mount, unmount, or stop EC2 resources.  Subagents never
manage instance or storage lifecycle.  After success, failure, or interruption, the root
must finish required artifact handling, confirm that no agent, process, notebook, or
transfer still needs the instance, stop it, and verify that it reaches the stopped state.
