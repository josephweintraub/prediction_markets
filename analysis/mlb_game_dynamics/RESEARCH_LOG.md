# MLB game-dynamics research log

This is a decision and audit trail, not a results document. Entries are ordered
by the sequence of the work; the implementation review occurred in the same
September 2026 work session. Final production-sample coverage remains
provisional until a corrected complete real-data run is published and audited.

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

## 10. Current stopping point

The code now reaches an audited, pre-estimation phase dataset. Synthetic tests
cover the happy path, strict schema/grain/coverage failures, timestamp attacks,
matching ambiguity, label conventions, irregular timing, reconciliation,
atomic cleanup, and deterministic publication.

No calibration estimate, price-decile profile, phase comparison, or variance
result has been viewed. This preserves a clean opportunity to freeze the first
estimator and its reporting table before looking at results. The proposed next
step is the simplest descriptive decile calibration profile by pregame and live
third. Implied-probability path variance remains a later exploratory proxy; it
should not yet be labeled inherent complexity.

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
estimator has been run. The remaining pre-estimation gates are research choices
and are pending user decision.

The simplest proposed specification is a recommendation only, not a frozen or
approved analysis choice: use literal MLB timing boundaries and every available
last-prestart close in the primary estimate; then report a sensitivity that
excludes trades within plus or minus 30 seconds of a phase boundary and
stratifies closes by age (`<=5m`, `5–30m`, and `>30m`). Estimation must wait
until the user accepts or revises that primary/sensitivity treatment.
