# NFL and NBA moneyline game dynamics: 2026-09-12

**State:** completed and independently audited exploratory production runs; descriptive,
not confirmatory

**Canonical code state:** `934b8383eb0fecf594df19a1550352d3f6a18392` on
`codex/repository-cleanup` before this documentation-only commit

**Immutable production roots:**

- NFL: `/mnt/data/runs/2026-09-12_nfl_game_dynamics_v3`
- NBA: `/mnt/data/runs/2026-09-12_nba_game_dynamics_v3`

These v3 roots are the only approved NFL/NBA report inputs. The immutable NFL
`2026-09-11_nfl_game_dynamics_v1` and `2026-09-11_nfl_game_dynamics_v2` roots and NBA
`2026-09-12_nba_game_dynamics_v1` and `2026-09-12_nba_game_dynamics_v2` roots are
superseded, quarantined audit attempts. They remain for provenance but are not
publication outputs and must not be mixed with v3 artifacts.

## Frozen contracts and providers

| Sport | Timing contract | Provider and immutable cache |
| --- | --- | --- |
| NFL | `nfl_phase_contract_v1.json`, SHA-256 `62826a4e7e647db78dfcd7862e16d73396c7942b9cdbeb7dc50b9c8b38f21cfd` | ESPN site API scoreboard and summary endpoints, third-party and undocumented. Taxonomy audit v2 SHA-256 `2d2f4b3a10d73090da64f54898503d75a095df7262e74d7f598ce725a78ddfee`; reviewed cache `/mnt/data/research_cache/2026-09-11_nfl_espn_v1` contains 150 scoreboards and 659 summaries and has inventory SHA-256 `f432485a8868b85046f106f711d7e2a89044715f86409a833d833c480271a807`. |
| NBA | `nba_phase_contract_v2.json`, SHA-256 `e6f93a693e9b004cd3fbf20115bfe08919f6994bd7784992e0a68e32ad924471` | Official `data.nba.com` historical schedules and NBA LiveData play-by-play. Reviewed cache `/mnt/data/research_cache/2026-09-12_nba_official_v1` contains two schedules and 1,365 play-by-play resources and has inventory SHA-256 `7adac545cb08071305d05385eb07514e25d6712a0a170ecea36f2d3dc490be31`. |

NFL timing uses the opening kickoff, first complete competitive play in each later
quarter, and final competitive play followed by exact normal terminal evidence. ESPN's
undocumented schemas and lack of reliable completed-game reschedule history remain
limitations. NBA v2 uses the audited opening-tip action, official period starts, and the
final-period `period/end` action, with overtime folded into Quarter 4+.

The NBA schedule-source vintage represented completed regulation and overtime games as
bare `Final`; it did not state the final period. The adapter therefore derives the
observed final period only from complete, contiguous play-by-play period and game-end
evidence and reconciles the final score and winner to the schedule. That conclusion is
bound to the cached completed cohort dated 2024-10-22 through 2025-10-14. Matching uses
the official NBA game date only: there is no UTC-date fallback. This caused no false
unmatched final in the audited cohort, but a future source refresh requires a new
forward-coverage audit.

## Stage reconciliation

| Stage | NFL v3 | NBA v3 |
| --- | ---: | ---: |
| 01 strict candidates | 661 candidates from 11,528 diagnostics | 2,796 candidates from 53,034 diagnostics |
| 02 exact final matches | 659; 2 no-schedule exclusions | 1,365; 300 no-schedule, 1,126 nonfinal, and 5 special-event exclusions |
| 02 timing | 587 passes; 72 strict parse/timing exclusions | 1,363 passes; one opening-signature failure and one schedule/PBP score mismatch |
| 03 moneyline valid / jointly eligible | 659 / 587 | 1,365 / 1,363 |
| 04 exact timestamp coverage | 951,037 distinct blocks; 0 missing; 0 fallback | 1,419,098 distinct blocks; 0 missing; 0 fallback |
| 05 exact BUY fills | 1,734,592; 0 replay duplicates | 2,544,785; 0 replay duplicates |
| 06 filtered phase rows | 819,976, including 1,620 post-final audit rows | 1,532,488, including 893 post-final audit rows |
| 06 exclusions | 873,683 flagged-buyer and 40,933 price exclusions | 936,896 flagged-buyer and 75,401 price exclusions |
| 07 primary / sensitivity closes | 587 / 587 | 1,363 / 1,361 |
| 08 fixed-grid outputs | 22 closing, 11 paired, 100 phase rows | 22 closing, 11 paired, 100 phase rows |
| 09 D1/D10 outputs | 12 rows: 10 reported, 2 suppressed | 12 rows: 10 reported, 2 suppressed |

The Stage-04 declarations prove complete row-level coverage of the sport-scoped raw
fills by `/mnt/data/pipeline_data/block_timestamps.parquet`, SHA-256
`fec60fed5c1a36664f164457447b120cddae057aebb6ca43592e48d68ae28002`.
Every published trade and close timestamp comes from that exact cache; neither sport
used an interpolated or fallback timestamp.

Stage 06 keeps BUY fills with `0.01 < price < 0.99` and excludes a flagged
outcome-token buyer only. Dollars are descriptive; calibration estimates give each fill
equal weight. The literal phase sample is primary and the sensitivity removes, without
reassignment, fills within 30 seconds inclusive of any phase boundary.

| Sport / phase | Literal rows / dollars | Removed by inclusive ±30s sensitivity |
| --- | ---: | ---: |
| NFL pregame | 426,120 / $123,426,432.27 | 828 / $867,539.12 |
| NFL Quarter 1 | 79,602 / $16,445,307.17 | 1,642 / $760,704.69 |
| NFL Quarter 2 | 109,741 / $21,771,055.22 | 1,553 / $525,741.45 |
| NFL Quarter 3 | 77,502 / $13,102,641.38 | 1,468 / $306,425.37 |
| NFL Quarter 4+ | 125,391 / $22,787,232.20 | 2,019 / $346,089.03 |
| NFL post-final audit | 1,620 / $468,186.39 | 804 / $176,733.92 |
| NBA pregame | 842,268 / $63,798,312.01 | 1,016 / $261,919.53 |
| NBA Quarter 1 | 139,514 / $19,292,533.20 | 3,441 / $596,385.54 |
| NBA Quarter 2 | 189,897 / $23,708,551.92 | 3,186 / $557,676.29 |
| NBA Quarter 3 | 168,750 / $25,848,730.87 | 3,429 / $772,374.11 |
| NBA Quarter 4+ | 191,166 / $36,699,893.75 | 3,303 / $515,548.80 |
| NBA post-final audit | 893 / $193,544.31 | 597 / $115,165.99 |

Post-final rows are audit-only and never enter estimation.

## Closing calibration

The primary close is the last exact pregame BUY fill with `0 < price < 1`, flagged
buyers included. The sensitivity close uses `0.01 < price < 0.99` and excludes only a
flagged outcome-token buyer. They are equal-game-weighted. A-minus-C is filter
attribution, not CLV; calibration remains eventual home outcome minus home probability.

| Sport / close | Games | Mean calibration | 95% CI | Brier score |
| --- | ---: | ---: | ---: | ---: |
| NFL primary | 587 | -1.157 pp | [-4.639, 2.324] pp | 0.2154 |
| NFL sensitivity | 587 | -1.083 pp | [-4.594, 2.428] pp | 0.2155 |
| NBA primary | 1,363 | -1.058 pp | [-3.335, 1.220] pp | 0.2029 |
| NBA sensitivity | 1,361 | -1.072 pp | [-3.353, 1.210] pp | 0.2029 |

The exact primary and sensitivity close identities agree for 367 NFL games and 1,052
NBA games. Closing FLB tails are not estimable under the frozen support rule: NFL
primary D1/D10 counts are 2/11 and sensitivity counts are 2/9; NBA primary counts are
15/50 and sensitivity counts are 14/53. Both closing rows for each sport are explicitly
suppressed because at least one tail has fewer than 50 games.

## Phase-tail findings

The spread is D10 mean calibration minus D1 mean calibration. Values and joint clustered
95% intervals are percentage points; full ten-bin profiles remain primary.

| Sport / phase | Literal spread [95% CI] | Exclude ±30s spread [95% CI] | Point pattern |
| --- | ---: | ---: | --- |
| NFL pregame | +12.320 [5.569, 19.070] | +12.319 [5.568, 19.070] | classic signs |
| NFL Quarter 1 | +10.850 [7.727, 13.973] | +11.026 [8.049, 14.002] | classic signs |
| NFL Quarter 2 | -4.442 [-19.546, 10.663] | -4.397 [-19.414, 10.619] | both tails positive |
| NFL Quarter 3 | -0.351 [-8.091, 7.389] | -0.399 [-8.177, 7.379] | both tails positive |
| NFL Quarter 4+ | -2.780 [-10.662, 5.101] | -2.848 [-10.851, 5.155] | both tails positive |
| NBA pregame | +7.376 [0.946, 13.805] | +7.371 [0.937, 13.805] | classic signs |
| NBA Quarter 1 | +4.938 [-0.695, 10.571] | +5.234 [-0.279, 10.747] | both tails negative |
| NBA Quarter 2 | -1.842 [-8.182, 4.499] | -1.786 [-8.130, 4.559] | reverse signs |
| NBA Quarter 3 | -1.031 [-5.837, 3.775] | -0.993 [-5.788, 3.803] | both tails negative |
| NBA Quarter 4+ | -5.198 [-10.094, -0.303] | -5.330 [-10.278, -0.382] | reverse signs |

NFL pregame and Quarter 1 have the classic point signs under both boundary samples and
positive joint intervals; later quarters do not show robust classic FLB. NBA pregame has
the classic point pattern and a positive joint interval, but live phases do not show a
consistent classic pattern; Quarter 4+ instead has a negative spread and reverse signs.
These are exploratory descriptive patterns, not causal or confirmatory findings. The
workflow emits no slopes, p-values, or multiplicity-adjusted claims.

## Reports and use boundary

- NFL report:
  `/mnt/data/runs/2026-09-12_nfl_game_dynamics_v3/10_report/sports_flb_report.html`,
  50,338 bytes, SHA-256
  `f5649ca97aa41e78df9805be2400e11e3b539c48b4790897aa7ba1f0a31833ea`.
- NBA report:
  `/mnt/data/runs/2026-09-12_nba_game_dynamics_v3/10_report/sports_flb_report.html`,
  50,433 bytes, SHA-256
  `d52a8b74921509d751682c05edb2c802de9ac77a4e3adbf1e164890b848608bc`.

Both reports are deterministic, standalone, and offline, with five phase panels, both
boundary samples, complete D1-D10 grids, both closing definitions, all suppression rows,
and embedded source limitations. Their `report_manifest.json` files fingerprint every
input and the HTML output. The results should not be generalized beyond binary
moneylines, the audited provider vintages, the resolved-market trade universe, or the
strict timing and support gates documented above.
