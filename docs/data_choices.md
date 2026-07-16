# Data and screening choices: Polymarket trade-level work

Prepared 2026-07-15 for co-authors (Toby's request; Kaushik is writing the
informed/uninformed section separately). This is the complete record of the
choices, with exact parameter values and the rationale for each. The terse
internal spec is `docs/methods_reference.md`; every screen described here is
implemented in committed code in the replication package
(`Dropbox/Polymarket Data and Code/learnability_paper_v1/code/`).

## 0. Sample construction (context for everything below)

- **Source.** The complete on-chain trade tape, reconstructed from Polymarket
  exchange `OrderFilled` events on Polygon (both exchange deployments; the
  April 2026 contract redeployment is handled by a v2 extractor). Not the
  public API feeds. **2,036,128,538 fills through 2026-06-23** after
  deduplication. Cross-validated 2026-06-22 against an independently collected
  dataset (near-total token coverage and resolution agreement).
- **Deduplication.** We remove only full-row exact duplicates, identical on
  all 11 columns (ingestion replays), about 4% of raw rows. Rows matching on
  (wallet, market, timestamp, side, price) but differing in counterparty or
  size are **retained**: they are one order filled against multiple resting
  orders, i.e., legitimate partial fills. An early internal audit reported
  ~17% duplicates and ~20% wash trading; both were artifacts of keying on too
  few columns. Literal self-trades (wallet == counterparty) are ~0.
- **Resolution join and censoring.** Trades are joined to resolved markets
  (Gamma metadata), so the dataset contains only markets resolved by the
  resolutions snapshot (currently 2026-07-03; 97.4% of mapped fills). The
  ~2.6% remainder is markets still open, concentrated in long-horizon
  markets, so **any across-time comparison must be horizon-matched or carry
  this caveat**; one of our own earlier claims ("long-horizon bias declined
  into 2026") did not survive a resolutions refresh. Refreshes are cheap and
  run periodically.
- Each fill appears twice in the tape (buyer row and seller row), each
  carrying that party's wallet and a maker/taker flag.

## 1. Informed vs. uninformed

Kaushik's section. Two tape facts relevant to it: (a) mature-window BUY
positions as a class earn +2.4% dollar-weighted against resolution, an
informed-flow drift that any return benchmark should account for; (b) the
maker/taker flag is per-fill and per-party, so aggressor-side definitions of
informed flow are implementable directly.

## 2. Bots vs. market makers vs. takers

**Philosophy: we exclude automation, not liquidity provision, and we exclude
it at the wallet level, not the trade level.** Market making per se is not a
data problem for calibration work; machine-gun activity patterns are, because
they dominate row counts (79% of raw rows) and are concentrated in mechanical
series.

**Behavioral composite (wallet level).** A wallet is flagged `is_nonhuman`
from four criteria computed over its full history on the deduplicated tape
(`analysis/bot_filter.py`, recomputed 2026-07-04 on the current clean set):

| Criterion | Definition | Definite | Likely |
|---|---|---|---|
| A. Inter-trade interval | median seconds between the wallet's trades | < 1 s | 1-10 s |
| B. Intensity | trades per active day | > 500 | > 200 |
| C. Round-the-clock | hour-of-day HHI < 0.06 and > 500 trades | -- | -- |
| E. Fixed sizing | CV of trade size < 0.05 and > 50 trades | -- | -- |

Composite rule: flagged if **A-definite**, or **A-likely plus any one** of
{B-definite, B-likely, C, E}, or **B-definite and C**, or **any two** of
{B-likely, C, E}. A fifth criterion (cross-market simultaneity) was
considered and dropped because block timestamps lack sub-second precision.
Result: **333,676 wallets flagged (20.3% of wallets, 79.4% of rows)**. All
calibration estimates exclude flagged wallets' trades.

**Makers vs. takers.** We do **not** exclude by role. The tape lets us split
any estimate by the row party's maker/taker flag; pooled calibration is
indistinguishable across roles (maker-initiated buys +0.003, taker-initiated
+0.008 slope deviation, both n.s.), so the headline results are not an
aggressor-side artifact. Role splits remain available for any analysis that
wants them.

**Mechanical series exclusion (market level).** The 5-minute/hourly crypto
"up or down" series are excluded as markets, not as trades: a market is
`is_updown` if its native event slug or series slug matches `%updown%` or
`%up-or-down%`, it carries the `Up or Down` tag, or its question contains
"up or down" (`scripts/build_market_flags.py`). Two implementation notes that
cost us real time: the trades' own `eventSlug` column is empty for newer
markets, so trade-level slug filters silently stop working; and the older
resolutions spine covered only ~49% of extended-sample rows. Both are fixed
by a full-coverage token->market spine (`market_flags.parquet`, checked at
build time to cover 100% of tokens in the tape).

## 3. What prices we look at

- **Trade prices only.** The on-chain tape has no quotes; there is no
  order-book snapshot in any of our numbers. Where a per-market price series
  is needed (variance ratios), we use daily last-trade prices on interior
  prices 0.05-0.95 with gaps of at most 3 days; where a single per-market
  price is needed (base-rate comparisons), the dollar-weighted mean Yes-price
  in the mature window.
- **BUY-side convention.** Every fill enters once, as the buyer's position at
  price p. This is provably innocuous for market-level calibration: the
  seller's position is the exact mirror (complement outcome at 1-p), and
  mirroring preserves regression slopes, so buyer-side and
  seller-as-complement estimates are identical by construction in every
  market-level cell (verified numerically). The convention only matters for
  **wallet-level** analyses, where buy-only conditioning half-observes a
  wallet; those use the two-sided tape (buys as-is, sells as complement
  purchases at 1-p; 66.5M mature positions).
- **Price bounds.** Trades at p <= 0.01 or p >= 0.99 are dropped everywhere
  (settlement-cleanup and dust regions). Robustness variant restricts to
  0.10 <= p <= 0.90; note that excluding extremes mechanically *strengthens*
  measured slopes in thin markets (bounded bands attenuate), so the interior
  variant is not a conservative choice, just a different one.
- **Lifecycle windows.** Trade position in a contract's life = (t - first
  trade) / (last trade - first trade), computed on BUY-side trades of the
  token. Primary window: **mature, 25-80%**. The closing window (80-100%) is
  a distinct public-flow regime and is reported separately, never pooled.
  Full-window (0-100%) is a robustness variant.
- **Binning and weighting.** Fixed-width price deciles (not quantile bins).
  Every estimate is reported count-weighted and dollar-weighted (by fill
  USDC size); trade-level grain is primary, per-contract VWAP the robustness
  grain. No within-contract dollar reweighting beyond fill size (avoids
  single-whale domination of a contract).
- **The statistic.** Calibration slope deviation: trade-level OLS of outcome
  on price, minus one (0 = calibrated, positive = classic favorite-longshot
  direction). Decile-spread summaries (D10 minus D1) are secondary only; they
  manufactured a sign-reversal artifact for us once (thin tail deciles) and
  that claim is formally retired. Inference: Cameron-Gelbach-Miller three-way
  clustered SEs (calendar day x wallet x market) everywhere.

## 4. Trimming outliers

What we trim:

- Full-row duplicate fills (~4%, see section 0).
- Price bounds 0.01 < p < 0.99 (section 3).
- **Slice floor:** any reported slice needs >= 5,000 trades; dropped slices
  are disclosed, never silently omitted.
- Slices whose clustered SE is degenerate (effectively a single cluster) are
  flagged and excluded from tables.

What we deliberately do **not** trim, with the evidence:

- **No dollar-size winsorization.** Instead of trimming large fills we report
  count- and dollar-weighted estimates side by side; divergence between them
  is information (big money is usually *more* miscalibrated in the biased
  cells, not less), and winsorization would destroy it.
- **No volume/"junk market" floor.** We tested this properly rather than
  assuming it. Defining relative volume r = market volume / trailing-90-day
  median volume of markets born around it: price informativeness (slope of
  outcome on price) is ~1.0 in every r bin including the lowest, and prices
  beat their own template-family precedent base rates in every bin (t = 55 in
  the lowest). A skill-crossing estimator therefore finds no junk threshold;
  a two-regime threshold regression picks the top 2% of markets, i.e., it
  recovers the liquidity gradient, not a junk boundary. A 25% floor excludes
  35% of markets but 0.06% of dollars and changes no estimate. Thin markets
  are *biased but informative*: excluding them would remove the phenomenon
  under study while leaving the estimates untouched. (Write-up section XII.)
- **No outcome/market-type trimming** beyond the up/down exclusion; sports,
  esports, and negRisk multi-outcome markets all stay in, with topic and
  mechanic labels available to slice them.

## Where things live

- Internal spec + retired-claims register: `docs/methods_reference.md`.
- All filter code and this document: Dropbox
  `Polymarket Data and Code/learnability_paper_v1/code/` (paths in the
  package resolve against its own `data/` directory).
- Canonical data: same package, `data/` (trade tape, market/wallet flags,
  labels, per-market dimension layer, all result artifacts).
- Current write-up: `learnability_paper_v1.html` in the same folder.
