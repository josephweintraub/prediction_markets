# MLB game-dynamics research log

This is a chronological decision, audit, and concise results trail. Entries are ordered
by the sequence of the work; the implementation review occurred in the same
September 2026 work session. Production-v3 pre-estimation sample coverage is
complete and audited. The immutable dual-close and calibration production runs are also
complete and independently approved.

## 1. Pilot question and simplest scope

The project selected MLB as a tractable subset for studying whether market
predictability or structure is associated with stronger calibration bias or
favorite–longshot bias. Baseball supplies observable game starts and inning
boundaries, permitting a pregame analysis and separate early-, middle-, and
late-game comparisons.

The first pass was deliberately frozen at game-winner moneylines. It does not
start with “high-confidence” markets because price deciles will already expose
confidence. Props, other sports, quote data, rich complexity models, and causal
claims were deferred.

## 2. Measurement terminology corrected

Calibration was separated from the market close:

- calibration error is eventual purchased-outcome indicator minus trade price,
  `y - p`;
- the closing line is the final observed eligible pregame trade price, with a
  home-side probability also recorded for comparability.

The close itself is not `close - trade price`, and it is not the eventual
outcome. The v1 pipeline therefore does not emit a quantity named CLV. A later
trade-to-close measure, if wanted, must receive a separate frozen definition.

## 3. Timestamp reality discovered and the design changed

Audit of the general Polymarket pipeline found that its Stage 6 timestamp path
permits a linear block-time fallback. The historical `trades_clean` artifact
therefore cannot be treated as proven exact merely because it contains a
timestamp column.

An exact Polygon block-timestamp cache was available with declaration metadata
reporting full cache/source block coverage, zero missing blocks, and zero
fallback rows. A dedicated declaration now pins that cache by SHA-256. The
declaration is only a trust root for a **new** MLB extract from raw
`resolved_trades.parquet`; it is explicitly not retrospective proof of
`trades_clean`.

Review strengthened this from metadata-only assurance to a data-level gate.
The exact extractor and phase builder require `block_number` and `timestamp`,
verify the cache hash and unique non-null cache keys, prove coverage of the
extract's exact block set, and compare every row timestamp with the cache.
Synthetic audits include same-count/different-block-set and altered-timestamp
failures.

The declaration's cache-wide counts are evidence about that cache, not the MLB
sample. MLB candidate, game, trade, and phase coverage remains provisional
until the real run.

## 4. Preliminary market-universe rule frozen

The simplest inspectable rule selects an exact
`mlb-<team-1>-<team-2>-YYYY-MM-DD` event slug and a present question containing
no colon. The legacy candidate fields named `away` and `home` retain those two
observed slug positions but do not establish official orientation. All `mlb-`
rows receive a diagnostic classification. More than one candidate for an event
is a hard error; the code never silently ranks or deduplicates them.

These rows are preliminary candidates only. No claim of a valid game or
two-outcome moneyline is made at this stage.

## 5. Official game identity and timing audited

The official MLB Stats API was selected over an ESPN fallback for v1. Candidate
matching uses exact official date plus the exact two stable team IDs. Observed
slug order is tested first as official away/home order, then reversed only when
no official-order game exists. The match audit records `official` or `reversed`;
canonical away/home identity comes only from the MLB schedule. Shifted dates
and reschedule-origin dates do not match, the requested game ID must equal the
live feed's returned ID, and actual boundaries do not shift to scheduled start
times.

Game start is the first completed plate appearance's observed start; phase
transitions are the observed starts of the top of the fourth and seventh; game
end is the final plate appearance's observed end. The parser fails on incomplete,
nonchronological, noncontiguous, or boundary-incomplete feeds rather than
inventing times.

The candidate scan exposed All-Star-style `al`/`nl` slugs. Those are not
franchise teams and receive the explicit `non_team_all_star` audit reason.
Same-date two-team doubleheaders can yield multiple exact schedule matches;
they remain ambiguous and are never guessed by start time or game number.
Nonfinal games remain visible in the schedule/match audit.

## 6. Moneyline token and winner audit completed

Canonical token inspection showed a real naming change: observed 2025 markets
use short club labels (for example, `Red Sox`), while observed 2026 markets use
full MLB team names (for example, `Boston Red Sox`). A reviewed explicit
short/full mapping covers all 30 club slugs. There is no fuzzy name matching.

One known malformed candidate had only one token row. It is retained in the
validation audit with `not_exactly_two_unique_tokens` and cannot enter the
eligible dimension. The validator also requires two distinct recognized team
outcomes, a single consistent Polymarket winning outcome, and agreement between
that resolution and MLB's score/winner flags.

Independent review added hard failures for malformed canonical token-table
grain and for partial candidate coverage, including the case where some
candidate IDs are present but another is missing. Eligible market, game, and
token assignments must be globally one-to-one.

## 7. Exact BUY-fill extraction audited

The extractor now filters the raw resolved source to candidate IDs before
expensive expansion. Replays are deduplicated only by immutable EVM event
identity `(transaction_hash, log_index, exchange_address)`; distinct fills with
identical economics remain distinct, and contradictory payloads for one
identity fail.

Maker/taker orientation was checked against the canonical BUY formulas. The
purchased token, buyer wallet, counterparty, price, and USDC size switch sides
according to whether the outcome token is the maker or taker asset. Price and
known-bot attrition must reconcile to the final output.

An initial review rejected incomplete candidate-source coverage, declaration-
only timestamp assurance, and nonfresh publication. The revised build now
requires every candidate to have source rows, performs cache and row-level
timestamp proof, and publishes only to a fresh run directory through a sibling
staging directory and atomic rename. Failure cleanup and deterministic outputs
were tested synthetically.

## 8. Standard-timing universe frozen before estimation

Moneyline validity and timing suitability were kept as separate audit gates.
Every candidate receives one validation row. A candidate is excluded from the
standard-timing core for upstream match/timing failure, missing timing, explicit
rescheduling or resumption, suspended status, a doubleheader value other than
ordinary `N`/null, a final inning below nine, or missing/misordered core
boundaries. Excluded rows are preserved with explicit reasons.

This is a timing rule, not a regular-season rule. Review caught an initially
missing `game_type` field; it is now retained in both the candidate audit and
eligible dimension. Normal-timing postseason and spring games are not filtered
solely for their competition stage, leaving `game_type` available for later
stratification.

Cross-artifact date, key, team, status, score, and winner mismatches are hard
errors. The validated output is atomically published only after one-row-per-
candidate and eligible-count reconciliation.

## 9. Phase and closing datasets specified

Exact fills are partitioned into `pregame`, `innings_1_3`, `innings_4_6`,
`innings_7_plus`, and `post_final`. Starts are inclusive; intermediate ends are
exclusive; the recorded final-play timestamp remains live. `post_final` is
retained for accounting but marked analysis-ineligible.

The pregame close is the last pregame fill in immutable chain-event order. A
closing audit preserves games with no pregame fill and reports close age/stale
flags. A boundary audit reports trade count and dollars within 5, 10, and 30
seconds of each official boundary. All input rows and dollars must reconcile to
ineligible-market rows plus the five eligible phases.

## 10. Pre-estimation stopping point at that stage

At that stage, the code reached an audited, pre-estimation phase dataset. Synthetic tests
cover the happy path, strict schema/grain/coverage failures, timestamp attacks,
matching ambiguity, label conventions, irregular timing, reconciliation,
atomic cleanup, and deterministic publication.

No calibration estimate, price-decile profile, phase comparison, or variance
result had been viewed. This preserved a clean opportunity to freeze the first
estimator and its reporting table before looking at results. The proposed next
step was the simplest descriptive decile calibration profile by pregame and live
third. Implied-probability path variance remained a later exploratory proxy; it
was not yet labeled inherent complexity.

## 11. Production audit v1 preserved and quarantined

The first production pass was preserved as an immutable audit run. Its observed
counts are:

| Artifact or gate | Production audit v1 observation |
| --- | ---: |
| MLB-prefixed diagnostic rows | 25,176 |
| Preliminary candidates | 3,790 |
| Raw candidate fills | 6,912,624 |
| Filtered exact-timestamp BUY rows | 2,555,139 |
| Filtered exact-timestamp BUY dollars | $444,040,476.38 |
| Exact-source blocks missing from timestamp cache | 0 |
| Output timestamp mismatches | 0 |
| MLB schedule records returned | 3,017 |
| Exact final game matches | 2,567 |
| Timing parses passed | 1,637 |
| Timing parser failures | 930 |

The timing request extended through 2026-06-21, but the returned schedule audit
ended on 2026-04-03. The 1,637 passes and 930 parser failures reconcile to all
2,567 exact matches, but they do not make the truncated schedule complete.
Therefore the production-v1 timing artifacts—and anything derived from
them—are **audit-only** and must not feed estimation.

## 12. Timing root causes and fixes

The incomplete schedule was caused by making one overly broad official-MLB
schedule request. The response silently stopped before the requested end date.
The client now divides inclusive schedule ranges into chunks of at most 180
days, caches each chunk separately, combines games by `game_pk`, and fails if
two chunks return conflicting records for the same game.

A separate 40-game parser investigation sampled 24 failed and 16 passed feeds.
It established that official live-feed intervals for adjacent plate appearances
can overlap: the next plate appearance may start before the prior one reports
its end. That overlap is not evidence that plate-appearance starts themselves
are out of order. The parser now permits adjacent-interval overlap while still
requiring each plate appearance to end no earlier than it starts, start times to
be nondecreasing, at-bat indices and innings to be ordered, and the final inning
to reconcile with the linescore.

Both fixes have focused synthetic regression coverage. A new production timing
run is still required; the fixes do not retroactively validate audit v1.

## 13. Canonical-versus-exact trade lineage discrepancy

The preliminary candidate comparison found the following after applying the
same BUY, price, and wallet filters:

| Trade view | Rows | Dollars |
| --- | ---: | ---: |
| Historical canonical clean table | 2,054,853 | $411.876 million |
| Exact raw-source output | 2,555,139 | $444,040,476.38 |
| Exact output projected to canonical `DISTINCT` grain | 2,487,986 | $440.502 million |

Reducing the exact output to canonical grain removes 67,153 rows and $3.538
million, so projection/deduplication explains only part of the raw gap. The
remaining discrepancy between like-grain exact and canonical views is 433,133
rows and $28.626 million.

A fill-level example exposed the relevant failure mode in the historical
artifact: when the outcome token is the maker asset, the maker supplies tokens
and the taker pays USDC, so the BUY wallet is the taker. The historical
canonical rows assigned both BUY and SELL to the maker in that example; because
the maker was bot-flagged, the BUY was then excluded under the wrong wallet.

Code review found that both checked-in transforms are correct: the general
Stage 6 transform and the dedicated exact extractor assign the BUY wallet by
`outcome_token_side` and agree on token, price, size, counterparty, and maker
status. Existing regression coverage exercises both maker-side and taker-side
outcome-token cases. The clean-table builder only applies full-row `DISTINCT`
to a separately generated trade tree and does not record enough build lineage
to prove which transform revision produced the historical input.

The historical canonical artifact's lineage is therefore unresolved. It must
not be used to adjudicate the MLB exact extract or overwrite the exact results.
No broader canonical-dataset rewrite, deletion, or replacement is authorized by
this audit. Any such project-wide action requires a separately scoped decision,
provenance plan, rebuild, and validation.

### Production maker-wallet counterfactual

A subsequent read-only production audit projected the same candidate raw fills
to canonical grain under two wallet-assignment rules: the checked-in correct
maker/taker rule and a counterfactual rule that always assigns the maker as the
BUY wallet. It then applied the same bot exclusion and compared both projections
with the historical canonical table.

| Scenario | Rows | Dollars |
| --- | ---: | ---: |
| Historical canonical, before bot exclusion | 6,397,592 | $889,385,053.88 |
| Correct projection, before bot exclusion | 6,397,592 | $889,385,053.89 |
| Correct projection, after bot exclusion | 2,487,986 | $440,501,844.59 |
| Always-maker counterfactual, before bot exclusion | 6,388,165 | $889,233,009.93 |
| Always-maker counterfactual, after bot exclusion | 2,053,071 | $411,836,149.75 |
| Historical canonical, after bot exclusion | 2,054,853 | $411,875,657.08 |
| Raw fills present only under correct versus always-maker assignment | 509,390 | $37,741,793.24 |
| Raw fills present only under always-maker versus correct assignment | 65,358 | $8,612,315.85 |

Before bot exclusion, the historical canonical and correct projections have the
same row count and differ by only $0.01 in their aggregate dollar sums. After
bot exclusion, however, the always-maker counterfactual reproduces the
historical canonical result to within 1,782 rows and $39,507.33. Relative to
the historical canonical after-bot totals, those residuals are about 0.087% of
rows and 0.010% of dollars.

This close reproduction strongly supports maker-wallet misassignment before bot
filtering as the dominant mechanism behind the canonical-versus-exact
discrepancy. It does not prove which historical transform revision built the
canonical artifact, establish its complete build provenance, or explain the
remaining 1,782-row/$39,507.33 residual. The checked-in transforms remain the
approved implementation, historical lineage remains unresolved, and the
no-rewrite boundary above remains in force.

## 14. Next authorized step after audit v1

Preserve audit v1 unchanged, run the corrected schedule/timing pipeline into a
new immutable run directory, and review its full requested-date coverage and
parser attrition before building a new validated universe or phase dataset.
Calibration and variance estimates remain unseen and unauthorized until those
pre-estimation gates pass.

## 15. Production v2 orientation audit and matcher correction

The complete v2 schedule artifact contained 4,053 unique games over the full
candidate date range. Under the initial ordered-slug matcher, the 3,790
candidates produced 3,612 exact final matches, 144 `no_schedule_match` rows, 33
`multiple_schedule_matches` rows, and one `non_team_all_star` row. Of the 3,612
matches, 3,596 timing parses passed and 16 failed explicitly.

An exhaustive read-only comparison showed that slug position was not a stable
away/home convention. Among the 144 no-match rows, 107 had the exact two teams
on the exact candidate date in reversed order. Of those, 106 identified one
completed ordinary-`N` game; the remaining BOS-STL candidate on 2025-04-06
identified both games of a split doubleheader. The reversal was systematic:
102 same-day cases occurred from 2025-04-02 through 2025-04-10, and five more
were the 2025 TB/MIN series. Two additional early reversed candidates referred
to games moved from their slug dates.

The other no-match rows were appropriate under the standard-timing v1 rule: 36
mapped only through MLB `rescheduled_from_date` metadata, and one STL-TB slug
named 2025-08-23, when no official game existed. All 33 original multiple-match
rows were genuine completed same-date doubleheaders and were retained without
ranking.

The matcher was therefore narrowed to exact candidate date plus exact two-team
identity. It first tests observed slug order as official away/home order and
uses reversed order only if the official-order set is empty. It preserves both
observed slug positions, records `slug_orientation`, and sources canonical
away/home identity only from the official schedule. It still never uses start
time, market price, confidence, activity, or reschedule origin to choose a
game. More than one record within the selected orientation remains an explicit
ambiguity.

Replaying the corrected pure matcher against the copied v2 artifacts produced
3,718 exact final matches, 34 multiple matches, 37 no-schedule matches, and one
All-Star exclusion. This recovers all 106 uniquely identified reversed games.
The ordered-first rule also preserves both previously matched CIN/SF spring
candidates on 2026-03-13: their opposite observed orientations identify the two
completed games with opposite home clubs without ranking. Across all
candidates, 3,645 orientations were `official`, 107 were `reversed`, and 38
were undefined because no same-date game orientation could be established.

The copied v2 artifacts predate this schema and remain audit-only. A fresh
immutable timing run is required before validation or estimation. No
calibration, phase, or variance estimates were viewed while making this change.

## 16. Production v3 timing and validated-universe audit

The fresh v3 run applied the orientation-aware matcher and retained all 3,790
candidates. It produced 3,718 exact final matches—3,612 in official slug order
and 106 in reversed order—and 72 explicit match exclusions: 34 multiple
matches, 37 no-schedule matches, and one non-team All-Star market. Timing was
written for 3,713 matches. The remaining five failed for concrete malformed
live-feed timing: four incomplete final plate appearances (MIN-CIN, TOR-CWS,
TB-BOS, and TOR-PHI) and one plate appearance ending before it started
(ARI-KC). No boundary was imputed for those games.

Relative to v2, v3 added 117 timing rows and lost none. All 3,596 common timing
rows were identical across every retained field. The added coverage therefore
did not perturb previously accepted game boundaries.

The validated-universe audit reconciled as follows:

| Gate | Passing candidates |
| --- | ---: |
| Moneyline valid | 3,715 |
| Standard-timing eligible | 3,699 |
| Jointly eligible moneylines | 3,697 |

Moneyline exclusions were the 72 upstream match exclusions, one
`not_exactly_two_unique_tokens`, and two
`polymarket_mlb_winner_disagreement` rows. Timing-core exclusions were the 72
upstream match exclusions, five timing parse failures, three rescheduled games,
five resumed games, and six shortened games. These counts reconcile to the
3,790-candidate input.

The 3,697 eligible moneylines comprise 2,328 games from 2025 and 1,369 from
2026. By MLB `game_type`, they are 3,412 `R`, 238 `S`, 18 `D`, 11 `F`, 11 `L`,
and seven `W`; competition stage remains visible rather than filtered. Their
slug orientations are 3,592 `official` and 105 `reversed`.

Three moneyline exceptions received candidate-level review. OAK-SD on 2025-04-08
has only one canonical token and remains the single token-cardinality
exclusion. HOU-LAD on 2025-07-04 resolved to the Dodgers although Houston won
the official game 18-1; this is a genuine Polymarket/MLB winner disagreement.
MIL-CHC on 2025-08-18 is instead a schedule-date collision: exact-date matching
links the market to game 777459, which was moved from June 18 and won by
Milwaukee, while the game originally scheduled for August 18 moved to August
19 and was won by Chicago. The row is already excluded both for winner
disagreement and as an irregular rescheduled game, so it is not evidence of an
oracle error.

BAL-BOS has an analogous collision: the game moved from May 22 was played on
May 23, while the game originally scheduled for May 23 moved to May 24. Both
were won by Boston, so winner agreement does not expose the mis-link; the
exact-date match is still excluded from the core as rescheduled. These cases
confirm that `rescheduled_from_date` must not become an automatic remapping key
in v1. Resolving schedule chains would require a separately specified matcher,
not a silent repair.

Game 824295 is marked `Completed Early` by MLB, but it completed nine innings,
has ordinary `N` doubleheader metadata, and has no reschedule or resume marker.
It remains eligible under the frozen standard-timing rule; status wording alone
is not a post hoc exclusion.

The phase build is approved using only the 3,697-row
`eligible_moneylines.parquet` dimension. Excluded candidate rows must remain in
the validation audit and must not enter phase estimation. No calibration,
phase, or variance estimates had been viewed at this approval point.

## 17. Production phase-v3 audit and pending estimation choices

The phase-v3 source split reconciled to 2,509,553 trade rows in eligible
markets and 45,586 rows in excluded markets. The eligible-market rows were
classified as follows:

| Phase | Rows | Dollars |
| --- | ---: | ---: |
| Pregame | 997,929 | $306,415,433.94 |
| Innings 1–3 | 483,364 | $45,689,401.48 |
| Innings 4–6 | 478,794 | $38,070,146.08 |
| Innings 7+ | 542,716 | $45,802,777.64 |
| Post-final, audit only | 6,750 | $937,059.36 |

The five phase row counts sum exactly to the 2,509,553 eligible-market source
rows. The close audit found a last prestart trade for 3,690 of the 3,697
eligible moneylines; the remaining seven are legitimate no-close cases. Among
the 3,690 observed closes, 792 (21.46%) were more than five minutes stale, 214
(5.80%) were more than 30 minutes stale, and 53 (1.44%) were more than two
hours stale. The 30-second boundary audit identified 33,900 rows representing
$6,558,319.13, or 1.351% of eligible-market rows.

The phase-v3 artifacts are technically approved: their schemas, source split,
phase reconciliation, close coverage, and boundary audit passed review. No
estimator had been run. At this audit point, the remaining pre-estimation gates
were the literal-boundary and closing-line choices. They were subsequently
resolved and frozen as recorded below.

## 18. User-approved first-estimator decisions

The close-recency audit reconstructed three definitions from exact raw fills.
Definition A—the last valid exact-timestamp prestart fill with `0 < price < 1`
and no wallet filter—covered all 3,697 eligible games. It had 443 closes older
than five minutes (11.98%). Definition C—the existing
`0.01 < price < 0.99` buyer-filtered view—covered 3,690 games and reproduced
the published filtered closes exactly. Of its 792 closes older than five
minutes, 436 were already stale under A and 356 became stale after buyer-bot
filtering; the seven games with every C-eligible pregame fill removed were also
already stale under A. The intervening strict price band alone changed only one
close and did not change the five-minute stale count.

The user approved the following before any calibration estimate was run:

- The primary closing line is A: the last exact valid fill strictly before
  official first play, with bot participants included.
- The closing sensitivity is C: retain the existing strict price band and
  exclude fills whose outcome-token buyer carries the existing `is_nonhuman`
  flag. This remains a buyer-centered sensitivity, not an either-participant or
  human-to-human market series. `A - C` measures filter sensitivity and is not
  CLV.
- Primary phase membership uses the literal official MLB boundaries. The fixed
  sensitivity excludes, without reassignment, trades within 30 seconds
  inclusive of first play, the starts of innings 4 and 7, or final play.
- Closing-line calibration gives every game equal weight. Trade notional is an
  audit field, not a closing-line weight.
- Calibration remains realized purchased-outcome indicator minus price,
  `y - p`.
- Calibration uses fixed-width probability bins `[0, 0.1)`, `[0.1, 0.2)`,
  through `[0.8, 0.9)`, and `[0.9, 1]`. A bin with effective `n < 50` remains
  visible for accounting but has its estimate suppressed and is labeled
  exploratory.
- The first estimator contains no complexity proxy, price-variance analysis,
  or regression. Those require a later, separately approved specification.

The initial 260-row, three-weighting table proposal was not implemented. It was
superseded by the simpler approved equal-trade phase estimator documented
below.

## 19. Dual-close builder and first estimator implemented

The dedicated dual-close builder is now implemented. It writes exactly one
`game_closes.parquet` row per eligible market/game plus `reconciliation.json`.
The Parquet uses authoritative parallel `primary_*` and `sensitivity_*` fields
for close availability, missing reasons, normalized home probability, price,
time and age, participant bot labels, and immutable event identity. The
reconciliation records input fingerprints, source/dedup counts, exact-cache
coverage, both close-coverage partitions, missing reasons, same/different
close identities, flagged-counterparty diagnostics, and hard subset and
partition gates.

The descriptive estimator is also implemented with four immutable outputs:

- `closing_calibration.parquet`: 22 rows, comprising two close definitions x
  one overall plus ten fixed-bin profiles;
- `closing_paired_sensitivity.parquet`: 11 rows, comprising one paired overall
  plus ten primary-price-bin profiles;
- `trade_phase_calibration.parquet`: 80 rows, comprising two boundary samples
  x four phases x ten fixed bins; and
- `estimator_summary.json`: input fingerprints, definitions, coverage,
  per-sample trade counts and dollars, fixed output counts, output names, and
  exploratory status.

The phase estimator gives each eligible BUY fill equal weight. Dollar volume
and game count are descriptive audit fields, not alternative phase weights.
Closing calibration gives each game equal weight. Cells below the frozen
effective `n = 50` threshold remain present with counts but have estimate and
uncertainty fields suppressed. The paired price difference is primary A minus
sensitivity C and is filter attribution, not CLV.

Independent implementation cross-review passed. At that point production execution still
required separate authorization; the authorized run and its audit are recorded next.

## 20. Production dual-close and calibration audit completed

The authorized run published immutable stages `07_dual_closes_v1` and
`08_calibration_v1`. Independent read-only review approved both stages.

### Dual-close coverage and lineage

`07_dual_closes_v1/game_closes.parquet` contains one row for each of 3,697 eligible
market/game pairs. Its source reconciles to 6,794,341 raw candidate rows, 6,794,341
distinct fills, zero duplicate ingestion replays, and 2,148,528 distinct source blocks.
Primary A has 3,697 closes. Sensitivity C has 3,690; the seven missing C closes all have
the reason `all_strict_price_pregame_fills_have_flagged_bot_buyer`. Among the 3,690
paired games, 1,990 definitions select the same EVM event and 1,700 select different
events.

The C projection reproduces the historical `closing_lines.parquet` exactly. After mapping
the historical timestamp, buyer wallet, token, side, outcome, price, normalized home
probability, age, USDC, block, EVM identity, and stale flags to their `sensitivity_*`
counterparts, both 3,690-row spines match, both `EXCEPT ALL` directions are empty, and
every comparable numeric difference is zero.

The label “bot excluded” is shorthand only. C excludes a fill when its outcome-token
buyer is flagged `is_nonhuman`; it does not filter on the seller/counterparty. Exactly
1,206 C closes have a flagged counterparty, so C is neither bot-free nor a
human-to-human series.

The nested provenance field `analysis_extract_verified = false` belongs to the generic
cache-declaration validator, whose scope is `cache_declaration_only`; it does not claim
that the production builder failed verification. The builder separately passed its
data-level exact-cache coverage gate across all source blocks, used zero fallback rows,
and recorded `all_close_timestamps_from_exact_cache = true`. Internal block/time,
prestart, age, close-order, token, winner, and home-normalization checks had zero
failures.

### Estimator coverage and boundary sensitivity

`08_calibration_v1` consumes 2,509,553 phase-input rows across 3,696 games and
$436,914,818.50. Of these, 2,502,803 are in the four analysis phases and 6,750 are
post-final audit rows. The dual-close spine has one additional close-only game, MLB game
831532: it remains in the primary closing profile but has no filtered phase row and no C
close. The estimator records this explicitly rather than dropping the game or requiring
false equality between the phase and close spines.

The inclusive 30-second sensitivity removes the following analysis-phase observations:

| Phase | Rows removed | Dollars removed |
| --- | ---: | ---: |
| Pregame | 4,135 | $2,781,720.45 |
| Innings 1–3 | 7,888 | $1,524,793.98 |
| Innings 4–6 | 5,628 | $615,957.76 |
| Innings 7+ | 11,620 | $1,180,092.60 |
| **Total** | **29,271** | **$6,102,564.79** |

This estimator count excludes post-final rows; the earlier 33,900-row boundary audit
covered the broader eligible-market source and therefore is not contradictory.

The fixed outputs reconcile exactly: `closing_calibration.parquet` has 22 rows,
`closing_paired_sensitivity.parquet` has 11, and
`trade_phase_calibration.parquet` has 80.

### Exploratory results

The equal-game primary closing profile has mean home probability 0.531444, home win rate
0.542602, mean calibration `y - p` of +0.011158 (95% interval -0.004154 to +0.026471),
and Brier score 0.244415 across 3,697 games. C has mean home probability 0.532275, home
win rate 0.542005, mean calibration +0.009731 (95% interval -0.005515 to +0.024976), and
Brier score 0.243560 across 3,690 games. On the 3,690 common games, A minus C is
-0.000568 in probability, +0.000568 in calibration, and +0.000746 in Brier score. These
are filter-sensitivity comparisons, not CLV.

The descriptive equal-trade phase-wide mean `y - p` values are +0.000055 pregame,
+0.000787 in innings 1–3, -0.000882 in innings 4–6, and -0.003048 in innings 7+.
After the 30-second exclusion they are +0.000047, +0.000933, -0.001028, and -0.003037,
respectively. The sign pattern is mixed and stable to the boundary sensitivity; no broad
monotone pattern appears in these descriptive aggregates. No phase-wide hypothesis test
is reported.

At the bin level, only the first two pregame bins and the `[0.8, 0.9)` innings 1–3 bin
have nominal 95% intervals excluding zero under both boundary definitions. The
`[0.4, 0.5)` closing bin is the only reported closing bin with an interval above zero
under both A and C. These are isolated exploratory cells, not a smooth cross-bin pattern.
The ten fixed-width bins use the frozen `n < 50` suppression rule, and this first output
contains no multiplicity-adjusted inference. No broader complexity proxy, regression, or
causal interpretation has been authorized.

## 21. Fixed-bin FLB tail summary completed

The immutable production stage `09_flb_tail_v1` derives a deliberately minimal
tail summary from the complete Stage 8 profiles. It defines D1 as `[0, 0.1)`, D10 as
`[0.9, 1]`, classic FLB point signs as `D1 < 0` and `D10 > 0`, and the spread as
`D10 mean(y - p) - D1 mean(y - p)`. The complete ten-bin profile remains primary.
No slope, regression, p-value, or multiplicity adjustment was added.

Closing rows use official-home probability and equal game weights. Phase rows use the
bought-token probability and the existing buyer-filtered eligible BUY sample. `A - C`
is still filter attribution, not CLV. The spread standard error is estimated jointly
from D1 and D10 cluster scores under the same uncertainty scheme as the source profile,
so the tail covariance is retained.

The output has exactly ten rows: six reported and four suppressed. Supported phase rows
are:

| Boundary sample | Phase | D1 n; y - p [95% CI] | D10 n; y - p [95% CI] | D10 - D1 [95% CI] | Pattern |
| --- | --- | --- | --- | --- | --- |
| Literal | Innings 1–3 | 8,793; +0.001908 [-0.027616, +0.031433] | 10,971; +0.012973 [-0.016128, +0.042074] | +0.011065 [-0.041781, +0.063911] | both positive |
| Literal | Innings 4–6 | 36,864; -0.002318 [-0.017178, +0.012542] | 48,760; +0.001861 [-0.015652, +0.019374] | +0.004179 [-0.026329, +0.034687] | classic FLB |
| Literal | Innings 7+ | 71,537; +0.002395 [-0.010141, +0.014930] | 95,161; -0.003795 [-0.018522, +0.010932] | -0.006189 [-0.032146, +0.019767] | reverse FLB |
| Exclude within 30s | Innings 1–3 | 8,676; +0.002258 [-0.027617, +0.032134] | 10,812; +0.012490 [-0.017001, +0.041980] | +0.010231 [-0.043265, +0.063728] | both positive |
| Exclude within 30s | Innings 4–6 | 36,434; -0.002388 [-0.017347, +0.012571] | 48,125; +0.001711 [-0.015875, +0.019296] | +0.004099 [-0.026484, +0.034681] | classic FLB |
| Exclude within 30s | Innings 7+ | 69,042; +0.003117 [-0.009734, +0.015967] | 92,022; -0.004934 [-0.020185, +0.010316] | -0.008051 [-0.034818, +0.018716] | reverse FLB |

The four suppressed rows retain support but withhold every estimate and interval:
primary closing has D1/D10 counts 11/5, sensitivity closing 5/2, literal pregame
245/20, and 30-second-exclusion pregame 238/20. This is the frozen fail-closed rule
when either tail has `n < 50`.

Innings 4–6 shows classic point signs under both boundary definitions, but both joint
intervals include zero. Early live trading is both-positive and late live trading has
the reverse sign pattern under both definitions. The full profiles and tail summary
therefore provide no robust classic FLB result.

## 22. Standalone FLB report approved

The deterministic offline renderer validated and fingerprinted the immutable Stage 7–9
artifacts, recomputed no headline estimate, and published a self-contained report with
no external resources. The approved artifact is
`10_flb_report_v3/mlb_flb_report.html`, exactly 104,878 bytes with SHA-256
`1407f6d7f8229c625c7ee5b8a2f639652d1d5e0f3e61fb7883c126ed1e6ffaf4`.
Its manifest records a matching deterministic second render, complete schemas and fixed
grains, source-fingerprint reconciliation, fail-closed suppression, semantic tables and
labelled SVGs, and atomic fresh publication.

The manifest records responsive targets of 320, 375, 768, and 1,440 pixels. Independent
runtime QA found no page overflow at 1,024, 736, and 360 pixels; content and print review
approved chart whiskers and zero lines, suppression rendering, table and mobile overflow
handling, filter/orientation/CLV caveats, numerical fidelity, and the absence of external
dependencies or overclaim.
The retained `10_flb_report_v1` and `10_flb_report_v2` directories are superseded QA
renders retained as immutable QA iterations, not approved publications; they were not
deleted or treated as research results.
