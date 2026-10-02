# Tennis timing and terminal wallet-sequence investigation

## Status

Complete. Final analysis is `03_tennis_cohorts_v3`; final reader report is
`04_report_v3/tennis_wallet_investigation.tex`, with three portable vector figures
and a six-page compiled QA preview.
This is a descriptive investigation, not a causal estimate or a holdings ledger.
The completed September runs are unchanged.

## Questions and measurement

1. Compare ATP, verified Grand Slam membership, and the same Australian Open
   matches under scheduled and provider-recorded actual clocks.
2. Measure eventual-winner sales and earlier-purchase links in late trading, then
   recompute verified maker-BUY calibration excluding purchases linked
   to an earlier purchase of the winning complement.

The scoped contract is
[`../analysis_specs/tennis_timing_wallet_exits_v1.md`](../analysis_specs/tennis_timing_wallet_exits_v1.md).
Calibration is `Y-P`, before fees, and the spread is D10 minus D1 in percentage
points. Classic FLB is D1 < 0 and D10 > 0; its D10-minus-D1 spread is positive.
The late upward turn reverses the earlier trajectory, not this terminology.
Time is exact block UTC relative to the
accepted start, divided by the accepted start-to-end duration. Literal windows
are `[.80,.90)`, `[.90,.95)`, `[.95,.99)`, `[.99,1]`, and the last 120 seconds.
No grace period is used.

The tennis comparison freezes the inherited exposure-normalized observations.
It does not establish the economic action of an inferred counterparty. The
wallet study separately identifies each maker's own BUY or SELL from its order
asset fields. Complementary BUY orders can mint, and complementary SELL orders
can merge, so the counterparty need not take the opposite economic action.

## Code, inputs and reproduction

Producing scripts:

- `analysis/diagnostics/collect_ao_actual_timing.py`
- `analysis/diagnostics/tennis_timing_cohort.py`
- `analysis/diagnostics/wallet_exit_audit.py`
- `analysis/diagnostics/summarize_wallet_exit_audit.py`
- `analysis/diagnostics/render_tennis_wallet_investigation.py`

Canonical code head: `28d5efba0907bcdd98e2dc240784ac19ae7fe823`, including renderer
commit `6a772df`. Earlier source/sequence gates and corrections are committed in
`8889588`, `f422b38`, `b6814d3`, `37b532e`, `88f48ec`, `b78fe2a`, `d13b0f9`,
`752ec14`, and `79ad9ea`. The final documentation is a separate logical commit.

Canonical root:
`/mnt/data/runs/2026-10-02_tennis_wallet_investigation_v1`.
Compact local copy:
`output/tennis_wallet_investigation_v1`.
Use `/home/ubuntu/venv/bin/python`; each stage manifest records exact commands,
inputs, environment, code and output fingerprints. Replay the recorded command
with a fresh output directory rather than overwriting a completed stage.

The wallet stage uses the frozen nine-sport event clocks from the filtered
September estimator manifest, the canonical resolved maker records, token map,
exact block timestamps and wallet flags. The tennis stage uses the frozen ATP
timing/match audit, cached ESPN scoreboards and ATP match archive, the legacy
exact-fill selection, and the newly gated AO evidence.

Published stages:

| Stage | Role |
| --- | --- |
| `01_wallet_sequences` | Verified own-maker actions, prior links, terminal groups, full price profiles and tail contrasts |
| `01b_wallet_reader_summary` | Conditional descriptive ratios and 80–90% versus final-1% comparisons from saved summaries only |
| `02_ao_source_v2` | Official AO native fields with full competitive chronology gates |
| `03_tennis_cohorts_v3` | Final Grand Slam identity, clock comparison, count/dollar fixed bins, kernels and support |
| `04_report_v3` | LaTeX source, deterministic figures and displayed-evidence manifest |

The initial `02_ao_source` boundary-only stage and `03_tennis_cohorts` preliminary
metadata stage remain for audit only. The `v2` analysis preserves final membership
and clocks; `v3` adds dollar-weighted fixed-bin estimates without changing them.
The 2.87 GB `maker_prior_links.parquet` stays on EC2; local review uses compact
saved summaries rather than a raw-history copy or local trade scan.

## Tennis timing gates and coverage

Official AO sources are
[`year/2026/period/MD/day/{day}/results`](https://prod-scores-api.ausopen.com/year/2026/period/MD/day/1/results)
and [`match-centre/{id}`](https://prod-scores-api.ausopen.com/match-centre/MS701).
The start combines the native `actual_start_time` and official competition date
in `Australia/Melbourne`. The endpoint is the native final completed competitive
point timestamp, with winner and complete score reconciled. Synthetic serve rows
with null timestamps do not establish first serve.

Of 55 frozen AO matches, 48 pass initial boundary gates and 33 pass complete
competitive chronology. Exclusions are five terminal timestamps that are not
last, two first-point conflicts, and 15 internal timestamp reversals. All accepted
competitive point identities are unique and their logical timestamps are
nondecreasing. First completed points are 17 to 94 seconds after recorded starts.
No trade price or calibration result enters this selection gate.

The start has minute precision. Provider timestamp latency is unquantified, so
these are qualified provider-recorded clocks, not second-exact physical first
serve. The strict physical-first-serve cohort remains empty. No paired actual
start and end was established for Roland-Garros; an endpoint alone is not used
to infer a start by subtracting archive duration.

Grand Slam identity requires canonical tournament, year, provider full-name
pair, winner, G-level archive match and provider-local match date within the
documented tournament-week interval. Market listing dates do not establish match
dates. This produces 120 Slams: 55 AO and 65 Roland-Garros, within 1,355 ATP
matches.

Two legacy archive matches used earlier tournaments involving the same players:

- `atp-agut-nakashi-2026-05-24`: Roland-Garros 128 minutes, legacy Rome 82.
- `atp-mochizu-tsitsip-2026-01-18`: AO 177 minutes, legacy United Cup 82.

New metadata corrects these identities and preserves both durations. The
scheduled comparison deliberately retains the old clock to isolate the actual
clock change. This is not a full rewrite of all ATP synthetic durations.

## Tennis descriptive findings

The live Epanechnikov endpoint at `T=1` uses bandwidth `.10`, so it is a weighted
final-tenth estimate, not a literal final-one-percent statistic. Each shown
calibration bin or kernel tail requires 500 contributing observations; estimates
without sufficient support are withheld, not displayed at zero. These new
diagnostics do not estimate confidence intervals.

- Grand Slam count-weighted endpoint spread remains positive: filtered +10.97 pp,
  all trades +10.04 pp. Thus the late upward turn is not confined to non-Slam ATP
  matches. With negative D1 and positive D10 errors, this endpoint is classic FLB.
- On the same 33 AO matches with all trades, the endpoint spread is +6.15 pp
  under provider timing versus +5.70 pp under the legacy scheduled clock, a
  +0.45 pp change. Actual endpoint tail counts are 1,101 and 937; legacy counts
  are 938 and 792.
- Filtered AO endpoint counts are only 222 and 335, versus 189 and 296 under
  legacy timing. Its kernel results are withheld. Early all-trade AO tails also
  fail support, so this comparison does not resolve the large early ATP swing.
- Dollar-weighted discrete final-tenth spreads are +11.98 pp for filtered Grand
  Slams and +10.51 pp for all-trade Grand Slams. For the paired AO all-trade
  cohort they are +5.64 pp with the provider clock versus +4.89 pp with the
  preserved legacy clock. These are discrete final-tenth estimates, not the
  count-weighted kernel endpoint above.
- Among 62,540 identical scoped all-trade observations in the paired AO cohort,
  1,019 change pregame/live/post-end phase under provider timing. Actual live
  observations increase from 42,012 to 42,741; post-end decrease from 2,932 to
  2,367. The same-membership comparison separates clock reassignment from
  selection of different matches.

## Wallet sequence findings and limits

The source contains 25,025,735 retained maker actions across 14,382 accepted
markets, including 9,862,282 pregame, 14,285,237 live and 878,216 post-end actions.
Post-end observations are excluded from focal terminal estimates but retained
as audit records. No source fill lacks an exact timestamp or accepted market;
there are no exact replay duplicates or asset/amount exclusions in this scope.

Prior history retains all valid maker actions, including flagged actors and
price-one records. Focal filtered actions use `.01<P<.99` and exclude flagged
makers; the all-actor comparison uses `0<P<1`. Prior links exclude the entire
focal transaction and require the same maker wallet and binary market.

In the filtered sample, eventual-winner SELLs increase as a share of all maker
actions from `[.80,.90)` to `[.99,1]` in all nine sports. Examples are ATP
13.14% to 18.99%, MLB 11.68% to 15.71%, and NFL 17.30% to 24.10%.
Among final-one-percent ATP winner sales, 1,931 of 2,227 (86.71%) have an earlier
same-token maker BUY. Among final-one-percent ATP D1 maker BUYs, 872 of 1,580
(55.19%) have an earlier maker BUY of the eventual-winning complement.

Removing maker BUYs linked to earlier winning-complement purchases gives supported filtered
final-one-percent spread comparisons:

| Sport | Full maker-BUY spread, pp | Without linked purchases, pp | D1 counts before/after | D10 counts before/after |
| --- | ---: | ---: | ---: | ---: |
| ATP | 10.072 | 9.691 | 1,580 / 708 | 2,262 / 2,260 |
| MLB | 8.018 | 6.099 | 4,130 / 2,067 | 5,122 / 5,121 |
| NHL | 6.087 | 5.554 | 1,151 / 817 | 1,115 / 1,112 |

Other filtered before/after comparisons fail the 500-per-tail floor. They cannot
be read as zero effect. These table entries are count-weighted, not dollar-weighted.
MLB's dollar-weighted final-one-percent spread is -0.656 to -7.566 pp under the
same exclusion, so the positive count-weighted sign is not a statement about
dollar exposure. The broader all-price/all-actor maker sample has more
supported comparisons, with attenuation in several sports; NBA changes from
+0.457 to -1.373 pp. Those sample-specific contrasts do not establish causation.

A verified SELL is disposal at that instant. An earlier BUY link alone does not
establish that the earlier position remained open or that a later sale was
profitable. Taker actions, transfers, splits, merges, redemptions, conversions and
opening balances are absent; Stage 2 may also have collapsed repeated order fills.
Consequently this is partial observed trading history, not reconstructed
inventory. Eventual winners are identified retrospectively, and an EPL
complement is a binary proposition rather than necessarily the named opponent.
The prior-winning-complement condition also retrospectively selects losing-side
focal BUYs, so dropping those records can change calibration mechanically.
The supported filtered positive count-weighted spreads persist after the linked
group is removed. Neither attenuation nor prior links identify a causal exit
mechanism or a complete explanation.

## Publication and QA

Wallet attempt 02 exhausted the default 20 GB temporary spill cap while sorting
the publication copy, after research gates and estimates had completed. The
final attempt frees redundant intermediate tables before publication and keeps
the same 150 GB memory and 20 GB spill caps. No sample or research gate changes.
Failed temporary stages were cleaned by the atomic publisher; attempt logs are
preserved outside the completed stage.

Every completed stage reopens saved outputs, checks row counts/key uniqueness,
and records fingerprints. Source cache and compact transfers were independently
verified. The dollar addition reuses identical input fingerprints and preserves
all 6,274 pre-existing output rows up to floating-reduction roundoff (largest
absolute difference 5.6e-9 in gross-dollar aggregates). There are 2,400 final
profile rows, 240 tail contrasts and 408 unchanged-definition kernel rows;
dollar and fill estimates share population counts and suppression gates.
Independent read-only QA reconciled 4,050 wallet profile rows, 405 wallet tail
contrasts, all 540 conditional shares, all 108 baseline comparisons, and the
paired AO phase matrices. It verified the relevant script and transferred
artifact fingerprints, and highlighted retrospective outcome selection and
weighting as interpretation constraints.

Final validation:

- Canonical complete repository suite: **466 tests and 15 subtests passed**,
  165.23 seconds, after the final code commits.
- Local available suite: **129 passed**, 49.43 seconds, with one upstream
  `fontTools.py23` deprecation warning.
- Tennis focused and AO/provider-adjacent suite: **60 passed** canonically and
  locally; renderer focused suite: **15 passed**; wallet focused suite:
  **14 passed** canonically and locally.
- Native Codex compilation was attempted. Its single-file compiler cannot load
  the report's separate vector figure files; the source itself is preserved and
  queued in the native editor. Existing local `pdflatex` successfully compiled
  the complete portable source/figure bundle in a clean temporary directory,
  with no substantive LaTeX warnings. All six rendered pages were inspected for
  overflow, clipping, suppression, labels, and table/figure legibility.
- Primary source and compiled preview are queued in native Codex file panels.
  No external viewer was opened.

Finished shared release destination:
`dropbox:Polymarket Data and Code/learnability_paper_v1/Tennis_Timing_Wallet_Exits_2026-10-02`.
The source archive contains this investigation's producing code, tests, contract,
methods warning and run record; scripts use the existing repository's common
modules and canonical private inputs. The report bundle is portable. Raw maker
history, wallet flags, credentials and private keys are not shared in the release.

All agents have confirmed their production and transfer processes are finished.
The root alone must finish publication, check active processes, stop EC2, and
verify the stopped state through AWS. Final shutdown confirmation belongs to the
task handoff; this record does not claim a future lifecycle outcome in advance.
