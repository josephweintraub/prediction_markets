# Kaushik September 28 FLB writeup: Polymarket replication v1

**Status:** frozen execution contract, 10 October 2026. The scientific definitions
and user-selected controls below are frozen. The versioned input build has
reopened and passed its schema, hash, count and support gates. Corrected full v5
estimation, independent saved-score QA and final report review passed. Publication
and verified shutdown are tracked in the run record. This is not certification of
native actions, exact timestamps or whole-history collection completeness.
This document authorizes no estimates from unadmitted inputs. It is a Polymarket-only replication of the specifications in the 12-page September 28,
2026 writeup, with explicit category, sports-cohort and display adaptations. It
does not promise identical paper observations or numerical reproduction.

## Scope and evidence

Source: `/Users/josephweintraub/Downloads/flb_summary.pdf`, titled
*Favorite–longshot bias: main specifications*, September 28, 2026. Page references
below refer to that supplied PDF. Its reported counts and estimates are reference
values, not acceptance targets for a different source vintage. No Kalshi inputs,
estimates or matching restriction enter this run.

Authoritative project guidance is [methods_reference.md](../methods_reference.md).
The [wallet repair record](../runs/2026-10-09_wallet_attribution_repair_v1.md)
and [current sports adoption record](../runs/2026-10-10_sports_wallet_adoption_v1.md)
establish repaired identity and adoption boundaries. The latter's accepted native
BUY results are optional context and its audited provider clocks are possible
sports inputs; its model grid, three-way uncertainty, filters and existing accepted game set are not the
paper specifications. [Sports timing rules](multisport_game_dynamics_v1.md) and
[current BUY conventions](flb_time_regressions_v3.md) document these differences.

The question is whether the paper's record-weighted price profile, duration
associations and sports phase/time profiles appear in the admitted Polymarket
data. Direction is measured, not presumed. Mean payoff, mean individual return,
their D10-minus-D1 gaps and duration slopes remain separate estimands.

## Common observation contract

Primary observation: one **archive-recorded taker-role row**, preserving its
published multiplicity. A BUY row retains its recorded token claim. A SELL row
maps to the verified complementary token in the same binary condition. Archive
role/action fields define this convention; they do not independently prove the
counterparty's own order, unique native execution, wallet holdings or seller
investment return. No wallet identity is needed for primary event clustering.

For recorded price `p`, binary payout `y` and positive quantity `q`:

| Recorded action | Equivalent claim price `P` | Payout `Y` | Claim identifier | Descriptive claim capital |
| --- | --- | --- | --- | --- |
| BUY | `p` | `y` | recorded token | `q*p` |
| SELL | `1-p` | `1-y` | verified complementary token | `q*(1-p)` |

If the archive reports cash `v`, use its documented execution meaning and
`q=v/p` only after that field contract is admitted. Require finite valid recorded
prices and positive finite size. Save descriptive capital and numerical anomalies
without adding an unapproved dollar threshold. Prices and cash are gross
execution quantities; fees and annualization are absent.

Baseline eligibility, shared by pages 1–3 and Appendix A2:

- Execution timestamp `t < 2026-03-25T00:00:00Z`; retain all admitted earlier
  history, evaluated at the archive's recorded source precision. Legacy CLEAN
  timestamps use the published approximate source; do not describe them as
  exact native block timestamps. Payout, settlement and endpoint may occur after
  the cutoff.
- Finite normalized price `0 < P < 1`, positive size, valid UTC timestamp,
  admitted metadata and observed eventual payout exactly 0 or 1.
- A unique pair of complementary native tokens, one condition, and agreeing
  trade/token outcome labels. Preserve contradictory or ambiguous records in
  exclusion artifacts; do not repair labels by guessing.
- All topics and both winning/losing claims are eligible. Retain up/down markets
  and flagged wallets. There is **no bot filter, extreme-price trim, lifecycle
  filter, sports-only filter or Kalshi-match requirement** in the baseline.
- Missing endpoints and trades after a recorded endpoint remain eligible here.
  Their duration/sports exclusions occur only in the relevant secondary sample.

This is an explicit override of the project's default BUY-only, `.01–.99`, bot,
up/down, lifecycle and slice-floor settings. Do not import the default 5,000-row
slice floor, 50-tail floor, equal-market/dollar weights or three-way clustering.

Every eligible observation has weight one. Define payoff `C=100*(Y-P)` cents per
$1 claim and individual return `Q=100*(Y/P-1)` percent **before averaging**.
Return gaps are percentage points. A gap is signed `mean(D10)-mean(D1)`, not a
difference in absolute calibration errors. Claim capital is descriptive only.

Price intervals are fixed bins: D1 `(0,.10)`, D2 `[.10,.20)`, ..., D9 `[.80,.90)`,
D10 `[.90,1)`. Preserve the normalized binary64 price used in these formulas;
never round it to assign an observation or compute a payoff/return. Save mean
price, win rate, payoff, individual return, row count and cluster count by bin.

Cluster on a uniquely mapped native event; otherwise use the condition/market
identifier. Namespace event and market fallback keys. Never use the extended
trade table's often-empty `eventSlug` as the only mapping. Save unique-event,
market-fallback and ambiguous-event-map counts. Primary clusters span all claims,
bins, tails and phases belonging to the same event.

## User-selected controls and reconstructed uncertainty

Categories use the **existing native primary taxonomy**, currently documented as
12 categories, plus explicit `Unclassified` for absent coverage. This is a
mutually exclusive exhaustive partition, not the paper's investigator-built
Crypto/Sports/Politics/Other/Unclassified classification (page 2). Bind the exact
retained taxonomy/map and its label order before execution; do not construct a
five-group crosswalk, change topic precedence or infer labels from results.

Claim-price fixed effects use **exact normalized binary64 `P` levels**, separately
by tail. No one-cent bins or rounding are authorized. Record the source price
construction, exact level encoding and per-tail cardinality/rank. The PDF does
not define its claim-price-effect granularity, so this is a selected replication
specification rather than a verified identical paper control. Trade-month effects
use the UTC execution calendar month. All category, price and month effects in
duration models vary by tail.

Use a one-way event/market cluster sandwich with joint scores for every requested
contrast. The raw sandwich **CR0 is primary**; also save the supplementary
cluster-count-only covariance adjustment `G/(G-1)` for `G>1`. Label that adjustment
literally, not as full CR1. The paper's finite-sample scaling is unknown. Do not
invent full `N-k` degrees of freedom for absorbed exact-price/category/month or
claim effects whose combined rank is not certified. Save group counts, absorbed
continuous-design rank, rank/convergence diagnostics and cluster scores. A
conventional full CR1 variant requires a later explicitly admitted absorbed-rank
calculation. Use pointwise normal 95% intervals
(`estimate ± 1.959963984540054*SE`) and label them; do not invent multiplicity
adjustment or significance stars.

For a scalar reported contrast, let `s_g` be its projected residual score for
cluster `g`. Save effective influence clusters
`(sum(s_g^2))^2/sum(s_g^4)` and maximum variance share
`max(s_g^2)/sum(s_g^2)`. Mark concentrated influence when effective clusters are
below 30 or the maximum share exceeds .25, matching the paper's verbal criterion
with this explicit reconstructed formula. Store undefined diagnostics separately
when their denominator is zero. Compute diagnostics on the full joint contrast,
not the less populated tail alone.

Never drop observations differently across a duration table's columns because a
nuisance level is sparse. Use a deterministic reference/rank convention, retain
sample membership, and withhold a coefficient when it is unidentified or its
variance cannot be computed. Sports retain both the paper's 30-game rule and the
additional project 500-observation guard below. Concentrated influence is a
warning, not an invented 30-event suppression rule for the baseline or duration tables.

## Table-by-table and figure contract

| PDF page / output | Required Polymarket artifact and display |
| --- | --- |
| 1, Sample | First/last execution UTC; all-band rows; distinct conditions; distinct normalized claims; event/market clusters; baseline D1+D10 rows; duration-eligible D1+D10 rows/clusters. Also reconcile all pre-cutoff source taker rows to admitted rows and disjoint exclusions. Counts are archive observations, not unique fills/investors. |
| 2, Categories | Existing native-category labels, observation count/share and total; exhaustively reconcile to baseline. Label the taxonomy adaptation beside this table. |
| 3, Table 1 | D1–D10 mean price, payoff, individual return and count, followed by D10−D1 payoff/return, joint clustered SE and combined tail count. Retain bin/cluster support in saved output even where the paper display omits it. |
| 4, Table 2A | Five duration specifications below, payoff slopes; D10-minus-D1 original/remaining slopes, SEs, influence flags, common rows/clusters, control indicators and R². |
| 5, Table 2B | Identical sample/specifications to 2A; individual-return slopes in percentage points. |
| 6, Table 3 | Separate `L>1 day` and `R>1 day` samples; three models per panel for payoff and return; slopes, SEs, influence/support and sample counts. |
| 7, Table 4 | Pooled sports pregame/in-play tail gaps and in-play-minus-pregame contrast for both outcomes; all-bin rows, games with rows, D1/D10 rows and contributing-game minima. |
| 8, Figure 1 | Pooled sports complete D1–D10 payoff/return profiles for pregame and in-play, 40 outcome/phase/bin cells. Pointwise game-clustered intervals and support/suppression tables. |
| 9, Table 5 | Nine sports separately: games, four-cell minimum game support, pre/in/contrast gaps for payoff and return, clustered SEs and independent support decisions. |
| 10, Figure 2 | Pooled sports pregame, time-since-start and final-hour window tail gaps, 14 windows × two outcomes; game-clustered intervals and full support table. |
| 11, Appendix A1 | Claim-FE diagnostic below for payoff/return; original×D10 and remaining×D10 coefficients/SEs, claims/events, both-tail claims and their observation counts. |
| 12, Appendix A2 | Four archive conventions: taker direction, all recorded BUY, maker BUY, taker BUY; all-band counts and tail gaps/SEs. The BUY figure contains all three BUY conventions' complete price profiles, with role/bin support. |

Provide the sports Figure 1 and Figure 2 grids for **each of the nine sports** as
well as pooled, even though the paper plots only pooled profiles. Unsupported
cells remain visible in supporting tables, with null estimates and an explicit
reason. All expected rows are serialized; no supported-only grid truncation.

### Duration sample and models: pages 4–6

Root's metadata-only schema inspection verified that
`/mnt/data/learnability/native/native_market_meta.parquet` has no start-date field.
For normalized claim `c`, freeze opening `o_c = created_at` (the paper's creation
fallback) and endpoint `e_c = end_date`. Record this field-level adaptation and
the retained metadata vintage, timestamp units/timezone and precision. End date
is a proxy for Polymarket maturity, not cash settlement or realized game end.
Missing or invalid `created_at`/`end_date` excludes only the duration sample;
do not invent an opening from the first trade or substitute an unavailable start.

`L_c=(e_c-o_c)/86400` and `R_ic=(e_c-t_i)/86400` use seconds at each recorded source
timestamp's precision, not rounded dates or asserted exact block times. The
common duration sample contains baseline D1/D10
observations with finite `L_c>0`, finite `R_ic>=0` and `t_i>=o_c`.
Set `xL=log2(1+L)` and `xR=log2(1+R)`. Invalid clocks are excluded, never set to zero.

For each outcome, fit separate tail-specific intercepts, clock coefficients and
nuisance effects (or an exactly equivalent fully interacted stacked model).
Report `beta_D10-beta_D1` for each included clock with covariance across tails.

| Table 2 column | Clocks | Tail-specific controls |
| --- | --- | --- |
| (1) Original raw | `xL` | intercept |
| (2) Original category | `xL` | intercept + native category FE |
| (3) Remaining raw | `xR` | intercept |
| (4) Remaining category | `xR` | intercept + native category FE |
| (5) Both full | `xL`, `xR` | intercept + native category + exact price + UTC month FE |

Taker status is constant and absorbed; no wallet or claim FE enters Table 2. The
same rows enter all five columns and both outcomes. Save both tails' R² and a
labeled stacked R² including all controls: `1-(SSE_D1+SSE_D10)/SST_stacked`, where
`SST_stacked` is squared deviation from the overall estimation-sample outcome
mean. The paper's one displayed R² for separate-tail OLS is not fully defined.

Table 3 Panel A restricts the common sample to `L>1` and retains final-day trades.
Panel B restricts it to `R>1` and excludes final-day trades. Each panel fits:
(1) indicated clock only; (2) indicated clock plus category FE; (3) both clocks
plus category/exact-price/month FE. Report only the indicated clock's tail-slope
difference in each panel. All columns within a panel share rows. These are
continuous slopes per doubling of `1+days`, not above/below-cutoff contrasts.
For a change `a→b` days, multiply by `log2((1+b)/(1+a))`. In full models each
clock conditions on the other; original lifespan at fixed remaining time also
changes claim age. Describe conditional associations, never causal duration effects.

### Sports cohort, phases and clock windows: pages 7–10

Sports are MLB, NFL, NBA, NHL, men's CBB, ATP, EPL, CFB and WNBA, including EPL
draw claims; exclude spreads, totals and player props. WTA/UFC are outside this
nine-sport replication. The cohort comprises eligible Polymarket full-game winner
events in the **available audited provider-bound artifacts**, with validated
identity, binary claim resolution and recorded start/end clocks. This is a
provider-covered cohort adaptation, not all historically eligible games on the
exchange. **Do not require a Kalshi match or inherit the paper's 9,039-game
cohort, April 15, 2025 start bound or March 23, 2026 end bound**. All admitted
earlier trade history remains eligible under the common execution cutoff. Record
the observed game-date range instead of silently fixing it to the paper range.

Root will bind the available provider artifacts after metadata review. Audit
their cached coverage, inherited date restrictions, candidate exclusions and
admitted games per sport/date, rather than claiming universal historical coverage.
No new API collection is required for this cohort adaptation. All eligible events
within the admitted provider-covered cohort enter; no extra date bound is added.
Eligible games need not contribute both phases or both tails. Cluster all claims
of a game together, including the three
EPL propositions. Pooled means weight games and sports by observed record activity.

Let `s` be accepted start and `e` accepted play endpoint. Require `e>s`. Pregame
is all `t<s`; in-play is `s<=t<=e`; exclude `t>e` from sports only. For eight
sports, inherit the audited first/last accepted ESPN play clocks. ATP uses its
recorded scheduled start plus completed-match archive duration; retain the
explicit delay/misclassification qualification. Do not substitute native market
end dates for play endpoints. Store provider quality, identity/result agreement,
fallbacks and excluded timing records. State the archive's approximate execution
timestamp source/precision beside sports evidence because it can affect phase
and clock-window boundaries even when provider clocks are audited.

Table 4 and Table 5 phase levels require at least 30 contributing games and 500
trade observations in **each** tail of that phase. The phase contrast requires
both thresholds in each of the four phase×tail cells. Its variance retains
cross-tail and cross-phase covariance. An unsupported contrast does not suppress
an otherwise supported in-play level. Figure 1 bin means require at least 30
games and 500 trade observations in that phase/bin independently.

The 30-game threshold reproduces the paper's support rule; **the additional
500-trade-observation minimum is a project noise guard, not a paper specification**.
Save separate `paper_support` and `project_support` flags, counts and final
withholding reasons for every cell/contrast. Report an estimate only when both
guards and numerical gates pass. A paper-supported cell failing the additional
guard stays in the table with its support and a null estimate, never zero.

Use the following literal calendar windows; no sport-duration normalization or
continuous smoothing enters the paper outputs. With `u=t-s` and `r=e-t`:

| Panel | Seconds / inclusion | Display labels, in order |
| --- | --- | --- |
| Pregame | `u<-86400`; `[-86400,-21600)`; `[-21600,-3600)`; `[-3600,-900)`; `[-900,0)` | `<−24h`, `−24 to −6h`, `−6 to −1h`, `−60 to −15m`, `−15 to 0m` |
| Since start | `[0,900)`; `[900,1800)`; `[1800,3600)`; `[3600,7200)`; `[7200,∞)`, each intersected with `t<=e` | `0–15m`, `15–30m`, `30–60m`, `1–2h`, `2h+` |
| Final hour | `1800<r<=3600`; `900<r<=1800`; `300<r<=900`; `0<=r<=300`, each intersected with `t>=s` | `60–30m`, `30–15m`, `15–5m`, `5–0m` |

The final-hour and since-start panels intentionally overlap; do not sum their
counts as disjoint observations. The PDF's clock labels do not resolve all exact
edge assignments; the inequalities above freeze this implementation. Every
Figure 2 tail gap requires 30 contributing games and 500 trade observations in
each tail, with both support flags saved separately. Save window
membership and reconcile pregame/since-start partitions including exact start/end.
Changing game/price composition prevents interpreting pooled profiles as an
individual claim's time path.

### Claim-FE diagnostic: page 11

On the full common duration D1/D10 sample, set `H=1{P>=.90}` and fit, for payoff
and individual return separately:

`Z_ic = alpha_c + gamma*H_ic + deltaL*(H_ic*xL_c)
        + betaR*xR_ic + deltaR*(H_ic*xR_ic) + error_ic`.

Use the normalized token claim ID, including complemented SELL claim IDs.
Standalone original duration is absorbed. Include no claim-price, month or
category effects. Keep all eligible claims, not only claims observed in both
tails; report both-tail claim/observation support separately. Verify constant
resolved `Y` within claim. For payoff, within-claim `Y-P` reduces mechanically
to `-P`; these coefficients describe price paths and do not establish independent
calibration, causal FLB or an average FLB level. The D10 intercept is not a mean
tail gap. Individual return remains nonlinear in price.

### Observation conventions and BUY comparison: page 12

Appendix A2 uses the same admitted archive, cutoff, size, outcome and token-pair
rules in all rows. Taker direction retains taker BUY and complements taker SELL.
All BUY retains the recorded BUY claim and partitions exactly into maker BUY and
taker BUY. No duration filter or regression controls apply. Maker/taker labels
remain archive labels. Equal total counts across conventions, if observed, do
not establish identical prices, observations or native executions.

The required **BUY-side comparison is Appendix A2's same-source baseline**:
all recorded BUY, maker BUY and taker BUY versus archive taker direction. It does
not require a new whole-universe native BUY source, native action recovery or a
new native/archive matched-grain study. Existing native exact BUY results for the
current nine sports may appear as optional context only, labeled apart from
Appendix A2 with their own cohort, source grain, flag generation, price filters,
timestamp quality and uncertainty. They are not a controlled direction contrast.

## Required production admission and reproducibility

Verified from retained documentation: the immutable wallet repair preserves
economic fields, roles and multiplicities; current nine-sport adoption refreshes
flags coherently and preserves native BUY IDs. Neither certifies archive action
truth, native completeness, replay removal or complete historical sports coverage.
Root's metadata-only footer/schema inspection verified the absence of a native
start-date field and availability of `created_at`/`end_date`; legacy CLEAN trade
times retain their published approximate-source precision. Row-level source,
mapping, outcome and clock accuracy have not been established by those footers.

Before any production trade scan, admit the source/field/capacity metadata
contract. During the admitted bounded sample build and before estimation, bind
and independently review the following factual gates. Bind the available audited
provider-covered sports sample and disclose its coverage rather than silently
expanding its historical scope:

1. Exact ROOT/CLEAN/archive source choice, vintage, field/schema semantics,
   multiplicity and role/action completeness. Use repaired inputs for any
   identity-dependent consumer; do not silently redirect shared paths or mix
   generations. A source must not be chosen merely because it matches paper counts.
2. Token spine and binary-complement map uniqueness/coverage, condition IDs,
   resolution label agreement and event/market fallback construction. Join EC2
   trades on token ID, never the misnamed `conditionId` as a market condition.
3. Native category-map labels/precedence and complete assignment, designated
   duration fields, exact price-level construction/cardinality, timestamp
   precision, baseline→duration reconciliation and strict cutoff inequalities.
4. Available nine-sport provider-artifact identities, inherited cached coverage
   and exclusions, timing/result proof and admitted games; same-archive Appendix
   A2 BUY/role partition. Do not require new API collection or native action
   recovery, and do not infer full historical coverage from accepted games.
5. Capacity/read/spill limits and serial-stage plan; declared CR0 and optional
   cluster-count adjustment, R² definitions, absorbed-FE convergence/design-rank
   handling and score diagnostics. Numerical degeneracy must withhold affected
   estimates with a saved reason.

The resolved-source build censoring remains inherited: resolution eligibility at
build time can omit long-horizon claims. Do not infer exchange-wide prevalence,
end-of-sample improvement or a calendar trend from this cutoff/vintage. Record
resolution refresh date and future-ending claims beside the relevant coverage
table. Counts need not equal the supplied PDF's counts.

Production uses `/home/ubuntu/venv/bin/python` in the canonical EC2 checkout with
the production-host guard and mounted `/mnt/data`. Publish new immutable stage
directories under `/mnt/data/runs/`; use lazy Parquet views and bounded serial
stages. Preserve prior runs, controls, failures and audited exclusions. Manifests
bind committed source, inputs, schema/grain, commands, environment, counts,
fingerprints, gates and every reopened output. Root alone owns lifecycle.

Before production, test synthetic complement/outcome, bin/cutoff/window edges,
duration gates, tail/phase covariance, control rank and support suppression.
Independently reconcile saved estimates to their admitted observations and score
summaries before reporting. Final findings must come from committed estimators
and saved Parquet/JSON artifacts; the renderer performs no estimation.

The portable `.tex` source is the primary report, with native tables and saved
vector figures. Retain the paper's ordering and minimal outcome/control notes.
Use isolated points and capped interval whiskers, a zero reference, consistent
axes across comparable panels and equally spaced clock-window labels. **Do not
copy the paper's connecting lines** for discrete bins/windows. Withheld values
are not zero and are not plotted. Put the taxonomy/cohort/source qualifications
beside their evidence, end with the final evidence table/figure, and keep hashes
in manifests. Open finished source and useful compiled preview in Codex after
numerical and page-by-page visual QA; drafts stay out of Dropbox.
