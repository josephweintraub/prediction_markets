# Fresh Kalshi FLB replication v1

Status: user approved a fresh raw-data replication on 16 October 2026.
This is an execution contract, not an accepted result. Input admission and
resource gates below must pass before estimation. Prior saved Kalshi sports
estimates are reference material only and may not supply missing fresh results.

## Scope and observation

Replicate the baseline, duration models and claim-fixed-effect diagnostic from
the [Polymarket specification](kaushik_polymarket_replication_v1.md), adapting
only documented native field semantics. Use all available dates in the pinned
Kalshi trade archive, documented as 30 June 2021 through 1 October 2026. Preserve
its actual observed UTC range and source vintage; do not impose the paper's
March 2026 cutoff or require cross-venue matches. Exclude combination contracts,
as previously requested. This is not an exchange-wide completeness certificate.

An observation is one unique native public `trade_id`, not one contract unit,
account fill, maker/taker pair or inferred wallet record. Preserve print size
for eligibility and descriptive totals, but weight each admitted print equally.
Audit duplicate identities, conflicting payloads and partition placement before
acceptance; do not silently deduplicate incompatible rows.

Native event and market identifiers are opaque strings. Preserve internal
ASCII spaces exactly in joins, claim IDs and clusters; reject empty keys,
leading/trailing whitespace, other whitespace and control characters. Do not
strip or rewrite the source identity. Trade IDs separately require canonical,
losslessly round-tripping UUIDs.

For the taker's YES direction use the recorded YES price and YES payout; for NO
use the recorded NO price and NO payout. Prices in this archive are already
dollars despite the legacy column names. Do not divide them by 100. Use the
named side's recorded binary64 price rather than calculating its complement.
When both legacy and newer direction fields are available, require agreement.
This directional claim exposure does not identify position opening or profit
realization by an account.

Baseline admission requires a valid native trade ID/ticker, positive finite size,
valid UTC execution timestamp in its documented UTC daily partition, valid
direction, finite complementary dollar prices and selected `0<P<1`, admitted
single-contract identity, and a finalized, unambiguous binary YES/NO payout.
Unit-dollar face value must be separately admitted from source-version evidence;
never guess raw notional-value units or silently fill missing numeric metadata.
Scalar/unresolved, unknown-combination, contradictory and unmatched records are
retained in disjoint exclusion accounting rather than assigned guessed outcomes.

The inspected native timestamp representation is UTC ISO text with four-digit
year, zero-padded month/day, valid 00--23 hour and 00--59 minute/second, optional
fraction of at most six decimal places, and `Z` or zero UTC offset. Freeze the
same format/range/precision gate in the pure oracle, trade SQL and metadata SQL;
do not allow database timestamp rollover or silently round higher precision.
Preserve and inspect every invalid timestamp count before estimation. The
additional observed `src` text field is provenance-only: retain it and audit
disagreement with `source`, without inventing a filter or a field meaning.

The admitted venue rule is that simple YES/NO determinations pay the winning
side $1 per contract, documented in
[Kalshi's settlement definition](https://docs.kalshi.com/getting_started/market_settlement).
This is an explicit normalized binary-payout convention, not an imputed native
numeric field. Inspected `bulk_v2` metadata carries dollar notional 1; inspected
`forward` metadata carries notional 0, while `event_v1` numeric fields are
documented NULL. Preserve those raw missing/zero quality indicators. They are
not evidence of zero payout. Native scalar results remain excluded and a
positive contradictory notional or settlement value requires separate unit
admission, never an automatic conversion inferred from magnitude.

Keep all topics, prices strictly inside (0,1), available dates and block prints.
There is no bot, wallet, up/down, lifecycle, `.01-.99` or trade-size threshold.
Public Kalshi prints do not expose Polymarket wallet roles; do not invent an
exchange-address filter or the Polymarket Appendix A2 BUY-role comparison.

## Measures, groups and uncertainty

For admitted price `P` and resolved payout `Y`, compute individual payoff
`C=100*(Y-P)` cents and return `Q=100*(Y/P-1)` percent before averaging. Use fixed
bins D1=(0,.1), D2=[.1,.2), ..., D10=[.9,1), with no price rounding. Report the
full profile and the signed D10-minus-D1 contrasts. Returns are gross exposure
returns, not fee-adjusted account profits.

Use verified native `event_ticker` clusters, with a namespaced market-ticker
fallback only when the event key is absent. Keep YES/NO claims and all prices
for an event in the same cluster. Categories must come from a pinned, explicitly
admitted native mapping; do not infer precedence or labels from outcomes. Exact
price effects use the actual selected binary64 price, interacted with tail;
month effects use the UTC execution calendar month.

For this source, exact membership of `event_ticker` in the pinned native
`/events` reference establishes a non-multivariate event: the
[official endpoint contract](https://docs.kalshi.com/api-reference/events/get-events)
explicitly excludes multivariate events. Known project compound families
`KXMVE*`, `KXOSCARWINNERS` and `KXCITIESWEATHER` remain excluded even if a native
reference matches. Absent event membership is unknown, not automatically single.
Use that event row's native category verbatim, with `Unclassified` for missing
category; audit series categories separately without inventing precedence or a
crosswalk to the Polymarket taxonomy.

Retain the existing joint event-cluster sandwich CR0, pointwise normal 95%
intervals, supplementary `G/(G-1)` adjustment, continuous-design rank and
absorption-convergence gates, and joint-contrast influence diagnostics. Do not
substitute independent tail standard errors. Undefined or unidentified estimates
are withheld with an explicit reason, never presented as zero.

## Duration and model contract

Opening is native `open_time`; endpoint is native `settlement_ts`, matching the
paper's Kalshi convention. Decode its actual source representation explicitly.
Do not replace settlement with close, expected expiration, expiration, a game
endpoint or first/last trade. Missing or malformed clocks exclude only duration
models, not an otherwise admitted baseline print.

Define `L=(settlement-opening)/86400`, `R=(settlement-execution)/86400`,
`xL=log2(1+L)`, `xR=log2(1+R)`. The common duration sample is baseline D1/D10
with finite L>0, finite R>=0 and execution>=opening. All five Table 2 columns
and both outcomes share that sample. Fit separate tail coefficients/effects:

1. xL plus intercepts;
2. xL plus tail-specific category effects;
3. xR plus intercepts;
4. xR plus tail-specific category effects;
5. xL and xR plus tail-specific category, exact-price and month effects.

Report D10-minus-D1 clock slopes, not the FLB level or a causal duration effect.
Table 3 separately restricts L>1 and R>1, using indicated-clock raw/category
models and both-clock full controls. Within each panel all columns share rows.
Appendix A1 retains the claim-FE specification in the Polymarket contract, using
native ticker-plus-YES/NO claim IDs. Save both-tail support and the mechanical
within-claim payoff qualification. No standalone original duration survives
claim effects.

## Input, capacity and reproducibility gates

Pin the exact Dropbox manifests, source paths, byte sizes and SHA-256 values.
The trade manifest has `file,bytes,sha256`; metadata has
`path,bytes,rows,sha256`, whose documented S3 paths require a verified bijective
mapping to Dropbox basenames. Check the actual schema rather than trusting the
README: the inspected metadata footer does not carry its claimed series key or
native multivariate-leg fields. Admit combination/category evidence separately.
Measure historical trade-weighted metadata coverage and preserve every loss.

All bodies run under `production_guard` on canonical EC2 with mounted data.
Stage and verify one source shard at a time. Only run-owned temporary source
copies may be released after successful processing and a durable receipt;
preserve failures and all original datasets. Retain a 20 GB free-space floor and
explicit input-cache/output/transient/spill ceilings. Do not copy the complete
57.6 GB trade archive plus 18.5 GB metadata onto a 60.2 GB-free disk.

Build lossless event/bin totals and duration/claim cell moments. Partial cells
may remain separate; never merge centered covariance by simply summing it.
Restore within-cell cross-products and event scores using the tested numerical
core. Validate IDs, schema, grain, uniqueness, field units, join cardinality,
outcomes, clocks, exact-price codes, exclusions and print-count reconciliation
before accepting the inputs. Independently audit saved coefficients/scores,
covariance, contrasts, support and every reported number before release.

Save raw-archive, admitted-baseline and duration-tail execution extrema
separately, including explicit null bounds for empty cohorts. Preserve tail-only
duration exclusion counts; all-band clock failures cannot substitute for that
reconciliation. Before the full trade build, a separate eight-day, at-most-2 GB
native-source probe measures actual cell/cache bytes. Its upstream-count-based
storage extrapolation is provisional and composition-sensitive, never an input
acceptance or a completeness claim. Independent release QA reopens all saved
bodies and separately replays those pinned days; its all-tape UUID evidence is
the reopened committed builder proof, not a second full-history raw-ID scan.

## Sports and report boundary

The intended sports counterpart covers the same nine full-game winner sports
and literal pregame/live/window inequalities in the Polymarket contract, with
the same 30-game and 500-print guards. Fresh Kalshi game IDs, outcomes and actual
play clocks require independent binding to the audited provider sources. Native
milestones are candidate links only; settlement/expiration are not play clocks.
Do not import saved sports estimates or silently substitute scheduled endpoints.
If this binding cannot be established, report the exact missing scope and ask
before replacing it with a different sports estimand.

Produce a separate portable LaTeX report from accepted saved artifacts, native
tables and deterministic vector plots. Keep fixed bins/windows unconnected and
withheld cells absent from plotted marks. Explain material source/cohort
adaptations beside their evidence. Keep hashes in manifests, not the report;
end with the final evidence table/figure. Compile and inspect all pages, open in
Codex, publish only the finished bundle, then stop and verify the instance.
