# Changelog

## 2026-05-14
- `flb_canonical.py`: platform-level FLB on humans + post-event-filtered
  sample (132M trades). Decile spreads with Cameron-Gelbach-Miller 3-way
  clustered SE (trader × day × market). iid t-stats are ~50-200,
  clustered t-stats are 0-5. Four deciles are 5% significant under
  3-way: [0.0,0.1) t=-2.07, [0.1,0.2) t=-2.17, [0.8,0.9) t=+2.70,
  [0.9,1.0) t=+4.57.
- `flb_canonical.py`: optimization — pre-aggregate to
  (trader, day, market, decile) once, then run 7 cluster variances on
  that smaller table. wallet_flags also cached to parquet for re-use.
- canonical post-event filter: `(p>=0.99 OR p<=0.01) AND last 1% of
  lifetime`. Symmetric ±1pp price tails + last 1% of lifetime.
  Earlier in the day used 0.999 winner-side; relaxed to 0.99 for
  symmetry with the loser side.
- `clean_appendix.py`: sync thresholds across Table 1/2/3 + print
  labels.
- back-fill log entries for prior session work.

## 2026-05-13
- `lifecycle_sweep.py`: sweep filter window over 0.1%-10% of lifetime.
  FLB top-decile spread is stable (+0.0070 to +0.0074) across the range,
  so the exact threshold doesn't matter; settling on 5%.

## 2026-05-12
- `clean_appendix.py`: replace earlier verbose appendix tables with two
  intuitive ones — total impact + mid-life price of each dropped trade's
  contract. Answers Kaushik's "is the filter dropping the right trades?"
  directly.
- `filter_appendix.py`: add post-event filter `(p>=0.999 OR p<=0.001)
  AND last 5% of lifetime`. Drops 0.99% of trades, 5.4% of volume.
- `postevent_binary.py`: re-run post-event analysis restricted to
  binary (neg_risk=FALSE) markets. Confirms the "first 50% extreme"
  pattern is not just a NegRisk artifact — driven by binaries with
  trivially decided outcomes (BTC $200K thresholds, CFB longshots).
- `postevent_summary.py`: tally trades by price bucket and lifecycle
  bin, plus by-category split. p>=0.995 trades cluster at last-5%
  lifetime, varying by category (sports 53%, politics 17%).

## 2026-05-11
- `market_timelines.py`: use log scale for volume axis on Mamdani &
  FOMC plots. Daily volumes span 4 OOM ($100/day early -> $8M/day
  near resolution) — linear scale hid the debate-day spikes.
- `market_timelines.py`: filter VWAP to trades >=$1 (dust trades on
  stale post-resolution orders were dragging the daily VWAP).
- `market_timelines.py`: re-anchor NBA Game 7 quarter markers using
  observed volume lulls in the trade data, not fixed estimates.

## 2026-05-10
- `flb_with_bots.py`: bot filter pass on full trades.parquet (21% of
  wallets flagged, 81% of trades). Top-decile spread doubles from
  +0.007 to +0.013 after bot exclusion — MMs were diluting the signal.
- `market_timelines.py`: academic styling (serif font, muted palette,
  truncated at resolution event) for the three market plots.
- `flb_overall.py`: overall FLB decile table + calibration plot.
  Confirms classic pattern — longshot decile [0.1, 0.2) loses 7.1c/$
  on a BUY basis.
