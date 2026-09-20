# ATP normalized-time swing audit v2

## Status

Complete. This is an exploratory diagnostic of the unusually large ATP
D10-minus-D1 calibration movement. It does not change the published estimator.

## Research contract

- Observation: exact resolved ATP full-match moneyline BUY fill.
- Calibration: eventual bought-contract outcome minus purchase price, `Y - P`.
- Tails: fixed bought-price D1 and D10 bins.
- Spread: mean D10 calibration minus mean D1 calibration.
- Production time: trade time relative to the ESPN scheduled start, divided by
  the independently matched completed-match duration. The end is scheduled start
  plus duration, not an observed match-completion timestamp.
- Samples: filtered trades; the same interior price support with all buyers; and
  all available `0 < P < 1` trades.
- Diagnostics: fixed time bins, equal-event estimates, fixed clock shifts,
  post-synthetic-end trading, event and wallet concentration, calendar splits,
  all price deciles, and match-level spread attribution.

## Reproduction

Script:
`analysis/multisport_game_dynamics/audit_atp_swing.py`

Canonical output:
`/mnt/data/runs/2026-09-20_atp_swing_audit_v2`

Compact local copy:
`output/diagnostics/2026-09-20_atp_swing_audit_v2`

The run contains the source manifest and ten deterministic Parquet summaries.

## Reconciliation and QA

- The exact-fill and timing inputs reconcile to the same 1,355 ATP events, with
  identical start and end values for every event.
- The accepted sample contains 1,355 full-match moneyline questions. No accepted
  question or group title contains set, game, round, or ordinal terms.
- Output hashes, manifest row counts, event uniqueness, tail-spread identities,
  market-scope gates, and contribution ranks passed.
- The focused provider-timing and FLB suites passed: 28 tests.

## Findings

The ATP live kernel moves from a trough of -27.97 percentage points at normalized
time `.34` to +9.54 points at the recorded end, a 37.51-point range. In the more
stable ten-bin summary, the range is 42.27 points. ATP's filtered live linear
slope is +39.53 points per normalized match duration, about 3.2 times the next
reported sport-specific slope, not an order-of-magnitude difference.

The large early negative estimates are strongly amplified by fill weighting and
changing match composition. In the first tenth, the per-fill spread is -22.19
points but the equal-event spread is -5.53. In the trough fourth tenth, the
corresponding estimates are -32.77 and -10.04. Only 113 to 116 matches supply
the first-tenth tails, compared with 838 to 856 in the final tenth. The top ten
matches account for 45 to 47 percent of first-tenth tail fills and approximately
35 percent in the fourth tenth.

Match attribution identifies the immediate source. In the first tenth,
`cerundo-svajda`, `djokovi-sinner`, `sinner-cerund`, and `darderi-zverev`
contribute -27.83 points before offsetting matches, while the aggregate is
-22.19. In the fourth tenth, six high-volume reversal matches contribute -33.84
points before offsets, close to the -32.77 aggregate. These are matches in which
contracts traded through both extreme tails and the then-low-priced player
eventually won, creating roughly +90-point D1 errors and -90-point D10 errors.

The end of the curve is broader and less concentration-sensitive. In the final
tenth, the per-fill spread is +9.50 points and the equal-event spread is +7.71;
the top ten matches supply only 14 to 19 percent of tail fills. The mechanism is
terminal outcome polarization: in the final elapsed third, D1 contracts average
5.41 cents and win 3.80 percent, while D10 contracts average 94.23 cents and win
95.01 percent. Both tail errors therefore reverse sign as the outcome becomes
nearly known.

The result survives the broad all-trades sample. Its live slope is +35.51 points,
and its first, fourth, and final tenth spreads are -18.82, -24.76, and +8.65
points. Buyer and boundary-price filters amplify the pattern but do not create
it.

The ATP clock is imperfect but a large systematic delay is not supported by the
post-end diagnostic. Of 1,355 events, 276 have at least one filtered central-price
fill after the synthetic end, but only 22 have one more than 30 minutes later,
11 more than 60 minutes later, and four more than two hours later. Among events
with any central post-end fill, the median last such fill is 125 seconds after
the synthetic end and the 90th percentile is 548 seconds. Removing the 22 events
with central trading beyond 30 minutes reduces the filtered slope from 39.53 to
34.88 points, so clock anomalies contribute but do not explain the result.

Fixed clock shifts are stress tests, not corrected timestamps. Moving every start
and end 30 minutes later reduces the filtered slope to 32.44 points; a 60-minute
shift reduces it to 20.25. However, the final-bin support collapses under these
large shifts because most markets have already polarized or stopped trading.
This, together with the post-end evidence, argues against interpreting a uniform
30- to 60-minute delay as the true correction.

The defensible interpretation is therefore compositional. ATP supplies a small,
selected, fill-weighted early cohort in which several heavily traded comeback or
upset matches dominate both tails, while the late cohort is much larger and
records broad terminal outcome polarization. The scheduled-start clock adds
measurement uncertainty and should be replaced with observed first-serve and
match-completion timestamps before making a structural tennis claim, but it is
not the main explanation supported by the present data.
