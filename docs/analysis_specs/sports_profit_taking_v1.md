# Sports profit-taking and calibration contributions

Status: implementation and source reconciliation, 2026-10-02. Do not overwrite
the completed tennis investigation or change the canonical trade pipeline.

## Question and estimand

Identify profitable reductions in favorite exposure from wallets' earlier trading
history and quantify their observed contribution to late-game calibration. The
user approved favorite status at disposal, including tokens acquired when they
were underdogs, and oldest-purchase-first (FIFO) matching.

For a binary market, selling the favorite and buying its complement reduce the
same directional exposure. They are not the same recorded execution. Recover
both wallets' own orders, distinguish direct SELLs from complementary BUYs, and
never create a synthetic complementary BUY in addition to an actual execution.

The identified estimand is an additive accounting decomposition of observed
calibration. It is not the market price or calibration that would have occurred
without unwinding. Wallet sequences alone do not identify that counterfactual.

## Cohort, history, and time

Use the accepted resolved moneyline markets and clocks for MLB, NFL, NBA, NHL,
CBB, CFB, ATP, EPL, and WNBA from the completed multisport investigations.
WTA and UFC remain excluded. Binary token identity must be unique and verified.

Retain all available own-wallet BUY and SELL history for matching, including
flagged actors, boundary prices, pregame and postgame executions. Apply focal
calibration filters only after history construction. Preserve incomplete or
ambiguous records with explicit status; do not manufacture a balance from them.

Use exact block timestamps. Normalized time is `(timestamp-start)/(end-start)`.
The literal live interval is `[0,1]`. Terminal windows are `[.80,.90)`,
`[.90,.95)`, `[.95,.99)`, `[.99,1]` and the final 120 seconds, inclusive.
Pregame has no `T=-1` cutoff. Provider-clock qualifications remain inherited.

## Recovering actual own-wallet actions

Start from raw Stage-1 OrderFilled logs, before the legacy exchange-facing-log
removal or order-hash ranking. Remove only exact log replays keyed by emitting
exchange, transaction hash, and log index. Conflicting payloads at one identity
are a hard failure.

The legacy counterparty-opposite-action inference cannot establish own-wallet
history. Orders can match as BUY/SELL of one token (normal), BUY/BUY of
complementary tokens (mint), or SELL/SELL of complementary tokens (merge).

For a verified legacy exchange batch, recover passive own actions and the active
wallet's constituent executions from the closing exchange-facing aggregate.
Reconcile exact received amounts and actual spent amounts. Aggregate gross
making amounts may include a refund and must not be treated as executed cash.
Preserve aggregate records as reconciliation evidence, not additional fills.
Validate exchange/version, log order, token identity, action compatibility,
quantities, cash, refunds and fee units before extending recovery to production.
Standalone or irreconcilable groups are separate unknown-coverage strata.

Legacy BUY fees are charged in received outcome tokens. V2 BUY fees are additional
collateral spending with no deduction from received tokens. Both versions deduct
SELL fees from collateral proceeds. Dispatch by verified emitting exchange and
retain explicit version/fee markers; do not use one fee rule across all history.
Preserve gross and net quantities and cash separately. If an active
batch fee must be allocated to constituent legs, record the deterministic
allocation convention; allocated per-leg fees are not directly observed.

## Quantity matching and mechanism labels

Maintain a physical, trade-implied FIFO lot book for each wallet and token.
Integer microtoken quantities and exact cost allocations prevent partial-lot
reuse or rounding-created profits. A SELL consumes available same-token lots
once. Unmatched quantity remains explicit. Same-transaction acquisitions update
history but cannot qualify as strictly earlier acquisitions in the primary test.

Favorite status uses current execution price above 0.5, not eventual outcome.
Direct profitable disposal compares sale proceeds with the cost of consumed
earlier lots. Separately record gross profitable disposal and its portion that
reduces positive pretrade net favorite exposure, defined as favorite stock minus
complement stock. The latter is the primary direct-unwinding quantity.

A complementary BUY below 0.5 can hedge earlier favorite holdings. Its qualifying
quantity is capped by both the new purchase and positive pretrade net favorite
exposure. Pre-existing complementary stock offsets oldest favorite lots; match
the current hedge to the remaining FIFO suffix. Add the new complement tokens
to the physical book without virtually removing the favorite. A profitable
complete-set lock additionally requires combined acquisition cost below its
verified unit payout. The primary profit test uses fee-adjusted acquisition cost
and proceeds; preserve original gross execution amounts for reconciliation.
The hedge fraction divides qualified net received tokens by total net received
tokens, allocating the entire BUY fill proportionally. The direct-sale fraction
uses gross disposed tokens; its mapped buyer fraction uses gross matched BUY
quantity. Fees affect profitability through actual acquisition cost and sale
proceeds, not by changing the calibration price definition.

These are trade-implied books, not certified balances. Opening holdings, token
transfers, splits, merges, redemptions and conversions can alter holdings outside
the recovered order history. Do not claim unique positions closed across
episodes or causal effects from these labels. Preserve unknown-history support.

## Calibration decomposition

The observation is an original actual BUY execution, not a matched lot link.
The wallet-history unit is an own-order log, including one corrected active
aggregate. Its passive matched legs are genuine individual executions, distinct
from the FIFO acquisition-lot links. The root has asked the user to confirm
retaining the existing matched-trade calibration unit versus using one active
own-order VWAP observation. The latter is a tested prototype, not an approved
change to the primary result. Freeze the final grain before production estimation.
Let `r_i=Y_i-P_i`, `w_i` its original weight, `e_i` the fraction paired with a
profitable exposure-reducing direct favorite sale, and `h_i` its profitable
complementary-hedge fraction. Require `0<=e_i,h_i` and `e_i+h_i<=1`.
The primary direct component additionally requires the focal BUY's own-order
price above 0.5, while the hedge component requires its price below 0.5. Keep
qualified sale quantities crossing an active counterparty's aggregate price
class in the reconciliation diagnostics rather than silently relabeling them.

For probability bin `b`, use the unchanged original denominator `W_b=sum(w_i)`:

```
C_b = sum(w_i r_i) / W_b
A_exit,b = sum(w_i e_i r_i) / W_b
A_hedge,b = sum(w_i h_i r_i) / W_b
A_rest,b = sum(w_i (1-e_i-h_i) r_i) / W_b
C_b = A_exit,b + A_hedge,b + A_rest,b
```

The D10-minus-D1 spread decomposes into the corresponding component contrasts.
Do not renormalize a subgroup and call the resulting difference a contribution
or causal effect. Conditional subgroup calibration may be shown separately.
Normal matched sales can label the actual same-token buyer's execution. Merge
sales have no corresponding actual BUY and must be reported separately. Active
and passive sales must remain distinguishable; a passive sale is not by itself
evidence of aggressive downward price pressure.

Report original fill, gross-dollar and equal-market weights. Equal-market
weights normalize gross dollars within the original market-by-bin population,
not each mechanism subgroup. Show the complete fixed-bin profile, tail support,
mechanism contribution counts, quantities, and unknown-history coverage.
Withhold bins and tail contrasts with fewer than 500 original focal BUYs.
Reuse three-way day/wallet/market clustered inference with joint tail covariance
where supported; label unestimated uncertainty as descriptive.

## Production and deliverables

Serialize raw extraction, recovery, FIFO processing and estimation. Run bounded
source/version/amount pilots before full extraction. Use new immutable run and
stage directories, exact reconciliation gates, and saved audit tables/manifests.
Large raw logs and wallet-level lot links stay on EC2. Publish compact audited
summaries, committed scripts/tests, and a data-first LaTeX report only after
source recovery and contribution gates pass. Reader-facing reports omit wallet
identifiers and fingerprint appendices.

Only the root controls the shared instance and data volume. Stop and verify the
instance after all agents, production processes and transfers finish, including
when a source gate blocks completion.
