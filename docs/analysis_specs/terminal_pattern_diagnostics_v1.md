# Alternative explanations for terminal calibration

Status: exploratory diagnostics requested 2026-10-03. Preserve the completed
profit-taking estimator, its report and every completed stage. No new FIFO,
raw-chain extraction, clock collection or causal model is in scope.

## Question and frozen reference

Investigate whether terminal calibration changes reflect within-bin prices,
outcome/contract composition, changing market membership, influential games,
sample filters or recorded-end timing. The pattern is not presumed universal.

Reuse the accepted resolved moneyline cohort for MLB, NFL, NBA, NHL, CBB, CFB,
ATP, EPL and WNBA, both genuine matched-BUY and own-order-event grains, and the
same three samples and weights as the completed profit-taking study. The
primary contrast remains final `[.99,1]` minus preceding `[.95,.99)` normalized
live time. No 30-second exclusion or pregame-history truncation is introduced.
Bins remain fixed-width; D1 is `0<P<.1`, D10 is `.9<=P<1` before sample filters.

Production outputs belong in a fresh run:
`/mnt/data/runs/2026-10-03_terminal_pattern_diagnostics_v1/01_diagnostics`.
Use exact block timestamps and inherited accepted clocks. ATP's original clocks
remain qualified scheduled-start/archive-duration proxies; do not silently
replace them or call any inferred timestamp public outcome-knowledge time.

## Diagnostics

1. **Price/outcome identity.** Save weighted mean outcome, mean price,
   calibration and support for each tail and primary window. Exactly reconcile
   `C=mean(Y)-mean(P)` and
   `delta(S)=delta(mean(Y)_D10-mean(Y)_D1)
   -delta(mean(P)_D10-mean(P)_D1)`. These are accounting terms, not causal effects.
   Add 1-cent bands within each tail with counts/dollar shares, mean prices and
   outcome rates. Shares may be shown when sparse, but conditional estimates
   with fewer than 500 observations must remain NULL.
   Retain original market-by-tail-by-window equal-market weights inside bands;
   do not renormalize each market inside a band. Band weight/numerator sums must
   reconstruct their parent tail, with count/dollar shares independent of the
   weighting and a separate original-weight share.
2. **Separate filter changes.** Use the existing all-trades,
   interior-all-actors and filtered estimates. Decompose the filtered-minus-all
   contrast sequentially into the boundary-price-filter change, then the
   flagged-wallet-filter change. Each component requires its two contributing
   estimates to pass support, even if the third sample fails. The complete
   filtered-minus-all decomposition requires all three samples to pass; save
   separate component suppression flags and NULL unsupported estimates.
   This ordering is explicit and does not identify a causal bot effect.
3. **Market/game composition.** Save market-by-window-by-tail sufficient
   statistics. Show market/event counts and top-1/top-10 event weight shares.
   Remove the highest-gross-dollar-volume event across the four primary cells,
   per grain/sport/sample, including every proposition market of that event;
   use the same selected event for all weighting variants, recompute remaining
   denominators and apply the original support rule. Also restrict to markets
   with positive dollar support in both tails and both windows. Recompute the
   original weights and equal-market estimate on that common membership;
   show retained counts, markets/events and original support shares. Balanced
   results are selected-support diagnostics, not replacement primary results.
   EPL propositions belonging to one game are not independent events.
4. **Recorded-end diagnostics.** Preserve primary boundaries. Add disjoint
   literal seconds-relative-to-end windows `[-120,-60)`, `[-60,0]`, `(0,60]`,
   `(60,120]`, an exact-end share, and counts/cash of central-price BUYs
   (`.1<=P<=.9`) before/after recorded end. Keep these separate from primary
   tails and show prices/outcomes/support. Post-end observations must not leak
   into primary estimates. These checks can flag timing concerns, not establish
   corrected clocks, stale quotes, information arrival or trader motives.

## Implementation and gates

Reuse the completed own-action source, batch links, FIFO tags, token/clock
spines, block timestamps and actor flags through existing attribution helpers.
Do not modify the completed builder or bypass its native proof requirements.
Validate complete parent manifests, unchanged input fingerprints, identity and
timestamp coverage, gross cash/quantity and quantity-residual conservation, and
reproduce saved baseline tail estimates and support before publication.

Use lazy DuckDB views and serial per-grain aggregation on EC2. Retain only
needed fields. Save compact Parquet summaries and JSON/manifest with contracts,
commands, environment, code/input/output fingerprints and reconciliation gates.
Wallet-level inputs stay on EC2. No new inference is promised: results are
descriptive unless uncertainty is actually computed and independently checked.

Test pure price shifts, pure outcome-composition shifts, all sample boundaries,
T=.95/.99/1, exact and post-end seconds, a dominant event with multiple markets,
missing balanced cells, support/NULL suppression, unchanged original weights
and both decomposition identities. Run focused fixtures first, then relevant
existing attribution/contribution tests and independent compact-output QA.

Only the root may start/mount/stop the instance. Verify no active workload or
transfer remains and safely stop/verify stopped after success or failure.
Do not attempt the previously blocked Dropbox upload without explicit approval.
