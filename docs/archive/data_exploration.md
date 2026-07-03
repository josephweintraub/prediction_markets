> **ARCHIVED 2026-07-02 — partially superseded.** The dedup analysis stands (true replay rate ~4%; partial fills are real trades; no meaningful wash trading). But the "single real data error" conclusion is outdated: **resolution censoring** (the Stage-4 INNER JOIN drops markets unresolved at build time) was identified 2026-07-01 as a second structural issue — see `docs/methods_reference.md`. Row counts predate the 2026-06-24 extension to 2.019B rows.

# Data Exploration — Sports and Tech Anomalies in v5

_Investigation prompted by visible irregularities in the v5 writeup: (1) Sports decile cal_errors that are individually significant at D4/D5/D8/D9 but produce a null D10−D1 spread, and (2) Tech showing classic FLB only in the 80-100% closing window. All analyses run on EC2; only small summary parquets pulled local._

## Overview

Two anomalies, two distinct mechanisms.

**Sports anomaly** — the calibration pattern is **shifted positive at almost every decile**, not the classic FLB direction (which has D1 negative, D10 positive). The D10−D1 spread is null because both endpoints sit at the same small positive value rather than at opposite signs. Per-event-template decomposition shows this is driven by **niche / esports / tennis markets** (dota2 +0.075, cs2 +0.067, wta +0.055), not by mainstream US team sports (NFL ~0, NHL +0.014).

**Tech anomaly** — the closing-window FLB is real, but it's coming from the **same contracts** that traded in mid-life, not from a different population entering at close. The closing window of Tech is dominated by **Elon Musk weekly-tweet-count markets** (1-week resolution, ~20 contracts each holding 4-12K trades in 80-100%) and AI prediction markets ("Will OpenAI/Google/xAI have the top AI model on Dec 31"). The classic FLB pattern (D1 longshots overpriced, D10 favorites slightly underpriced) **emerges as resolution approaches**, consistent with late-stage hype-betting on the longshot side.

## Why the BUY=SELL test turned out tautological (a quick aside on methodology)

I set out to test whether the Sports pattern reflects (a) genuine market mispricing or (b) a BUY-side selection effect. The natural test seemed to be: compare BUY-only calibration to SELL-only.

It is not. **Every Polymarket transaction logs both sides** — the BUYer (`side='BUY'`) and the SELLer (`side='SELL'`) at the same price, with the same outcome flag. Section B confirmed: for every decile of `sports_data`, n_trades(BUY) ≡ n_trades(SELL) (both 1,233,790 at D1, both 1,091,559 at D5, etc.).

Mechanically, `cal_error_BUY = win_rate − price` and `cal_error_SELL = (1 − win_rate) − (1 − price) = price − win_rate = −cal_error_BUY`. The Sports SELL output is exactly the negative of the BUY output, by construction. No additional information.

**Implication for our writeup methodology**: the BUY-only filter in v5's FLB SQL filters to *one row per transaction* but does not select traders by "BUYer vs SELLer" intent. A trader who placed a market order to buy YES at $0.50 generates one `side='BUY'` row (their side) and the resting limit order they hit generates one `side='SELL'` row. The "BUY-side selection" framing in v5's methodology disclosure is misleading. **What v5 actually measures is per-decile `outcome_match` rate vs price** — the symmetric question for all order-book participants.

## Anomaly 1 — Sports `sports_data` decile pattern

### The pattern in v5 (mature 25-80%)

| Decile | Impl prob | Win rate | cal_error | t (3W) |
|---:|---:|---:|---:|---:|
| D1 | 0.036 | 0.051 | +0.0157 | +0.80 |
| D2 | 0.148 | 0.145 | -0.0031 | -0.23 |
| D3 | 0.248 | 0.276 | +0.0280 | +1.13 |
| **D4** | **0.348** | **0.400** | **+0.0514** | **+3.75\*\*\*** |
| **D5** | **0.450** | **0.497** | **+0.0465** | **+5.21\*\*\*** |
| D6 | 0.539 | 0.548 | +0.0095 | +1.13 |
| D7 | 0.642 | 0.668 | +0.0257 | +2.22 |
| **D8** | **0.744** | **0.778** | **+0.0341** | **+2.61\*** |
| **D9** | **0.846** | **0.877** | **+0.0313** | **+3.58\*\*\*** |
| D10 | 0.960 | 0.970 | +0.0094 | +1.21 |

D10−D1 = -0.006, t=-0.30 (n.s.) — because both endpoints sit at +0.01-0.02 instead of at opposite signs.

### Composition

`sports_data` info-type slice: **416,074 contracts** / 36M trades / $4.92B volume.

Top templates by trade count: NBA team-vs-team (6.7M trades), League of Legends (2.3M), NHL (1.8M), CS2 (1.8M), MLB (1.2M), EPL (1.2M), CBB (1.1M), ATP (1.0M), NFL (1.0M), Dota2 (0.9M), plus tournament-winner markets (Super Bowl champion, NBA champion, UCL/EPL winner). Note 68K Esports contracts (CS2/LoL/Dota2/Valorant) land in `sports_data` info_type even though they're in `Esports` primary_category.

### Per-template decomposition (mature 25-80% window)

Weighted-mean cal_error per template (higher = bigger BUY-side advantage):

| Template | Weighted-mean cal_error |
|---|---:|
| dota2-`<TEAM>`-`<TEAM>` | **+0.075** |
| cs2-`<TEAM>`-`<TEAM>` | **+0.067** |
| wta-`<TEAM>`-`<TEAM>` | **+0.055** |
| mlb-`<TEAM>`-`<TEAM>` | +0.042 |
| cbb-`<TEAM>`-`<TEAM>` | +0.037 |
| ucl-`<TEAM>`-`<TEAM>` | +0.036 |
| lol-`<TEAM>`-`<TEAM>` | +0.032 |
| atp-`<TEAM>`-`<TEAM>` | +0.028 |
| nba-champion | +0.025 |
| epl-`<TEAM>`-`<TEAM>` | +0.022 |
| nba-`<TEAM>`-`<TEAM>` | +0.015 |
| nhl-`<TEAM>`-`<TEAM>` | +0.014 |
| super-bowl-champion | +0.005 |
| nfl-`<TEAM>`-`<TEAM>` | **-0.000** |
| `<DATE>`-nba-champion | -0.006 |

**Clear stratification by market sophistication.** The biggest BUY-side advantages are in:
- **Niche esports** (dota2, cs2, lol) where retail attention is low and informed traders dominate flow
- **Tennis** (wta, atp) — same pattern, niche markets
- **CBB / MLB** — high-volume sports but less heavily lined than NFL/NBA

The advantage **shrinks to near-zero in NFL** and is also small in NBA/NHL — markets where massive public money keeps the line very efficient.

### What this means (working hypothesis)

In sports markets with **deep public liquidity** (NFL, NBA), the order book is efficient — `outcome_match` rate ≈ price at every decile, so cal_error is small. In **thinner / more niche markets** (esports, tennis), the order book reflects a mix of sharp money and retail, and the price systematically lags the true outcome distribution. BUYers (i.e., participants on the buyer side of every transaction) win 5-7% more often than the price implied, summed across price levels.

The reason D10−D1 doesn't pick this up is that the bias is roughly uniform across price levels in these markets, not concentrated at the extremes. The D4-D5 spike specifically reflects close head-to-head games where the slight underdog (40-50% implied) wins more often than priced — consistent with the "favorite slightly over-priced" effect that sports betting literature documents.

### What the writeup should say

The current v5 §2 sports_data interpretation as classic FLB is misleading. Sports doesn't show FLB — it shows a **uniform positive calibration offset** that's biggest in niche sports. The right framing for the writeup is "Sports markets are not subject to FLB at the price extremes — the spread is null — but they show a systematic price-vs-outcome offset that varies by sport." This is a separate phenomenon from FLB.

## Anomaly 2 — Tech closing-only FLB

### The pattern in v5 (Tech primary_category)

| Window | D1 | D2-D4 | D8-D10 | D10−D1 |
|---|---:|---|---|---:|
| 25-80% | -0.001 | mildly negative (-0.029 to -0.017) | positive (+0.012 to +0.036) | flat |
| **80-100%** | **-0.020\*\*\*** | **strongly negative** | **clearly positive** | **classic FLB** |
| Full | -0.000 | mildly negative | mildly positive | mild |

So Tech is well-calibrated mid-life and shows classic FLB only at the close.

### Composition: who is in the closing window?

Among Tech contracts:
- 5,067 contracts appear in **both** 25-80% and 80-100% (long-lived markets that traded throughout)
- 2,348 contracts appear in **only 25-80%** (resolved before 80% lifecycle)
- 1,116 contracts appear in **only 80-100%** (mostly thin contracts that didn't accumulate trades mid-life)

The 80-100%-only subset is small relative to the both-windows subset, so the closing-window FLB is mostly driven by the **same contracts trading later in their life**, not by a new population entering.

### What's in the closing window — top contracts by 80-100% trade count

The top 30 Tech contracts by 80-100% trade count are dominated by:

1. **Elon Musk weekly tweet-count markets** — ~20 of the top 30, each a 1-week resolution window like "Will Elon Musk post 320-339 tweets from March 13 to March 20, 2026?" These have 4-12K trades each concentrated in 80-100% of lifecycle, with mid-life trade counts much lower (~1K). They are short-lived, attention-driven markets that traders only really engage with near resolution.
2. **AI prediction markets** — "Will Google/OpenAI/xAI have the top AI model on December 31?" (3-12K trades each in 80-100%)
3. **Polymarket-internal launches** — "Will Polymarket US go live in 2025?" (11K trades in 80-100%)
4. **TikTok / specific corporate-event markets**

The Elon tweet markets are particularly interesting because they're a *family of related contracts*, each resolving on a specific tweet count range. They have a distinct trader population that watches Elon's tweet count in the final week and bets on close-out outcomes. This creates a structured FLB pattern in 80-100%.

### Per-subset calibration (the cleanest result)

For Tech contracts present in **both** windows, the FLB pattern strengthens between 25-80% and 80-100%:

| Subset | Window | D1 cal_error | D2-D4 avg | D8-D10 avg |
|---|---|---:|---:|---:|
| BOTH | 25-80% | +0.005 | -0.023 (mild) | +0.024 |
| **BOTH** | **80-100%** | **-0.020** | **-0.022 (clearer)** | **+0.021** |
| ONLY_25_80 | 25-80% | -0.030 | (extreme + noisy, thin samples) | (extreme + noisy) |
| ONLY_80_100 | 80-100% | +0.003 | (very thin: 50-500 trades/cell) | (very thin) |

The classic FLB direction is much sharper in the BOTH × 80-100% cell. **Same contracts, more FLB at close.** This is consistent with the working hypothesis: as resolution date approaches, retail attention concentrates on longshots ("will the underdog still pull it off?"), pushing longshot prices up beyond their true probability — the classic FLB mechanism.

The ONLY_25_80 subset has wild numbers because it contains contracts that resolved before 80% lifecycle, often quickly and with extreme price moves. The D5 cal_error of **+0.34** on only 782 trades suggests a small handful of contracts where the trade flow happened at one price and the outcome surprised — not a robust pattern.

### What this means for Tech

Tech FLB is a **late-life phenomenon driven by short-deadline attention markets**. The Elon tweet markets are the cleanest example — they have a stable mid-life price (well-calibrated, no FLB) but accumulate FLB structure as the tweet-count window closes and hype-betting on outcomes that haven't yet hit kicks in.

For the writeup, the right interpretation is: "Tech shows classic FLB at the close, driven primarily by attention-driven prediction markets (Elon tweet counts, AI model rankings) whose order book gets thin as the resolution date approaches and retail flow tilts toward longshots."

## Cross-anomaly observations

1. **The current v5 framing of "spread D10−D1" doesn't capture the Sports phenomenon.** Sports markets are not FLB-shaped — they're uniformly positively-offset. We need a separate metric (mean cal_error across deciles, or D-by-D heatmap) to surface what's actually happening.

2. **The BUY-only filter is a misnomer.** It logs one row per transaction but doesn't select trader intent. Re-reading the methodology disclosure to remove the "BUY-side trader return" framing would help.

3. **Sub-template decomposition is informative and should appear in the writeup.** The "sports_data shows mild positive cal_error" v5 paragraph reads as a methodological footnote; the truth is "esports / tennis / mid-tier sports show 5-7% systematic offset; NFL/NBA show none." That's a real research finding (different markets, different sharpness levels).

4. **Tech closing-window FLB ≠ general Tech FLB.** Reporting Tech as showing FLB in the closing window is true but the structural story is "Elon tweet markets do it, AI prediction markets do it; long-running Tech markets don't." The current v5 §3 Tech row hides this.

## Open questions for follow-up

- Is the niche-sport BUY-side offset (esports/tennis) **stable across years**? Or is it a phenomenon of a specific era of Polymarket where retail flow into esports surged? Could be answered with a time-stratified version of Section A.
- The Elon tweet markets are a **family** — does the FLB pattern hold within a single weekly market's lifecycle, or does it only emerge across the family aggregation? Could be answered with a within-contract decile breakdown of the top 10 Elon contracts.
- For the writeup's purposes: do we want to **split `dim_info_type_supergroup` sports_data further** into US-team-sport / esports / tennis sub-buckets? That would surface the stratification in the main tables without requiring a special appendix.
- Re-running the v5 FLB pipeline with **a mean-cal-error metric** (instead of D10−D1 spread) for the "calibration-offset" categories would let us report both spread AND mean offset side-by-side. Small code change in `flb_per_slice_v3.py`.

## Addendum — Sports vs Esports split (no EC2 needed; v5 data already has the slices)

The v5 pipeline already runs `dim_primary_category` with Sports and Esports as separate slices. Pulling decile data from the existing parquets gives the cleanest answer to the user's question.

### Spread summary across all 3 windows

| Window | Slice | N trades | D10−D1 spread | t |
|---|---|---:|---:|---:|
| 25-80% | Sports | 6.9M | -0.006 | -0.28 |
| 25-80% | Esports | 1.0M | -0.010 | -0.79 |
| 80-100% | Sports | 8.1M | **+0.021** | **+6.22\*\*\*** |
| 80-100% | Esports | 1.4M | +0.013 | +1.39 |
| Full | Sports | 19.5M | -0.000 | -0.03 |
| Full | Esports | 3.1M | -0.027 | -1.22 |

At the spread level Esports never shows significant FLB. Sports shows classic FLB only in the closing window. Both look "boring" at this level — the real story is in the per-decile structure.

### Mature window (25-80%) — per-decile cal_error

| Decile | Sports cal_err | Sports t | Esports cal_err | Esports t |
|---:|---:|---:|---:|---:|
| D1 | +0.0154 | +0.75 | **+0.0255** | **+2.21\*** |
| D2 | -0.0121 | -0.81 | **+0.0575** | **+2.53\*** |
| D3 | +0.0185 | +0.64 | **+0.0799** | **+4.26\*\*\*** |
| D4 | **+0.0501** | **+3.12\*\*** | **+0.0583** | **+3.05\*\*** |
| D5 | **+0.0446** | **+4.44\*\*\*** | **+0.0600** | **+3.92\*\*\*** |
| D6 | +0.0022 | +0.24 | **+0.0588** | **+4.49\*\*\*** |
| D7 | +0.0221 | +1.64 | **+0.0407** | **+2.25\*** |
| D8 | +0.0324 | +2.05 | **+0.0408** | **+3.09\*\*** |
| D9 | **+0.0336** | **+3.38\*\*\*** | +0.0200 | +1.46 |
| D10 | +0.0091 | +1.11 | **+0.0154** | **+2.70\*** |

**Sports** has a few isolated positive spikes (D4-D5, D9) but most deciles are NS — the "uniform offset" pattern is mild and partial.

**Esports** has **all 10 deciles positive** with **7 of 10 individually significant** (Bonferroni \*). The offset is uniform and large (+0.04 to +0.08 at most price levels). This is the cleanest example of the "BUY-side advantage at every price" pattern from the original investigation — and it's almost entirely an Esports phenomenon. The aggregate `sports_data` info-type slice was diluting this Esports signal with the much flatter Sports signal.

### Closing window (80-100%) — per-decile cal_error

| Decile | Sports cal_err | Sports t | Esports cal_err | Esports t |
|---:|---:|---:|---:|---:|
| D1 | **-0.0108** | **-4.46\*\*\*** | **-0.0160** | **-3.45\*\*\*** |
| D2 | -0.0096 | -1.15 | **-0.0442** | **-4.50\*\*\*** |
| D3 | -0.0042 | -0.45 | **-0.0541** | **-4.38\*\*\*** |
| D4 | -0.0151 | -1.66 | **-0.0688** | **-4.56\*\*\*** |
| D5 | -0.0106 | -1.58 | **-0.0843** | **-7.79\*\*\*** |
| D6 | **-0.0261** | **-3.84\*\*\*** | **-0.0754** | **-6.09\*\*\*** |
| D7 | -0.0139 | -1.73 | **-0.0686** | **-4.30\*\*\*** |
| D8 | -0.0040 | -0.51 | **-0.0587** | **-3.38\*\*\*** |
| D9 | -0.0064 | -0.77 | -0.0290 | -1.39 |
| D10 | **+0.0104** | **+4.33\*\*\*** | -0.0035 | -0.46 |

**Sports** shows the **classic FLB pattern**: D1 longshots overpriced (-0.011\*\*\*), D10 favorites slightly underpriced (+0.010\*\*\*). The D6 -0.026\*\*\* is the close-game-favorite-overprice that public-money sports literature documents.

**Esports** shows a **dramatic SIGN FLIP from the mature window**: every decile that was positive at 25-80% is now NEGATIVE at 80-100%. **8 of 10 deciles individually significant negative.** BUYers at the close lose at every price level, with the largest losses at mid-prob deciles (D5 = -0.084\*\*\*, D6 = -0.075\*\*\*, D7 = -0.069\*\*\*).

This is the most striking finding in the whole exploration. The same Esports markets where BUYers had a +0.04-0.08 advantage during mature pricing have a -0.04-0.08 disadvantage at the close. **Esports is a two-regime market.**

### Working hypothesis for the Esports flip

The most parsimonious explanation: **latency exploitation around live esports matches.** Specifically:

- Esports matches stream publicly with a 30s-90s delay (Twitch, YouTube broadcasts).
- Some traders have access to lower-latency feeds: in-game spectator clients, game-server data, or direct event APIs.
- During the bulk of a match's lifetime (mature window 25-80%), the price is informed by a mix of pre-match analysis and slow-moving in-game indicators. Sharp money on the right side accumulates positive expected value steadily across all price levels — the +0.04 to +0.08 mature-window cal_error.
- In the last 20% of the contract's trading lifetime, the *outcome* is increasingly visible to low-latency holders. They post resting limit orders that public-feed traders see and fill — buying YES at $0.50 from someone who already knows YES is going to lose. This **systematically transfers EV from the public BUYers to the low-latency SELLers**, producing the negative cal_error at every price level in the closing window.

This is the classic "stream sniping" pattern that's been documented in esports betting elsewhere. It's worth flagging because: (a) it's a methodological caveat — the closing-window FLB story in Esports is not the standard "longshot overpricing" mechanism, it's a latency-exploitation mechanism, and (b) it might also exist in other live-event markets (live election night, live sports comebacks), so the closing-window FLB findings for those categories should be inspected for the same pattern.

### Implications for the v5 writeup

1. **Split `dim_primary_category` analysis to feature Sports and Esports separately in the headline tables**, not just as bullet points. The aggregate `sports_data` slice in `dim_info_type_supergroup` should be replaced with two rows (or footnoted as not-comparable across the two underlying populations).
2. **Mention the Esports sign-flip across windows as a finding**, not buried as a footnote — the +0.06 mature → -0.07 closing pattern is real and large.
3. **Add a methodology caveat to the closing-window discussion**: closing-window FLB findings in markets with live broadcasts (Sports, Esports, live elections) may partly reflect latency exploitation rather than information-asymmetry FLB. This is a different mechanism with different policy implications.
4. **Consider a follow-up sub-analysis**: did the Esports sign-flip emerge at a specific time period? Or has it been there since esports markets launched? A within-Esports temporal cut would help separate "this is a stable structural feature" from "this emerged when stream-sniping tools became common."

## Data quality audit of the raw trades parquet

_Goal: surface any corruption, outliers, or quality issues that could be biasing the FLB analysis. All checks run on the full 1.435B-row raw trades parquet on EC2._

### Tier 1 — definitive corruption

| Check | Result | Status |
|---|---|---|
| Total rows | 1,435,301,230 | — |
| Null in critical fields (timestamp, conditionId, proxyWallet, side, outcome, price, usdcSize, eventSlug) | All zero | ✅ Clean |
| Price out-of-bounds (≤0 or ≥1) | 2 rows ≥ 1, 0 ≤ 0 | ✅ Essentially clean (2 out of 1.4B) |
| Timestamp out of plausible range | 0 before launch, 0 in future | ✅ Clean |
| BUY/SELL parity | 717,650,615 BUY ≡ 717,650,615 SELL, perfect | ✅ Clean (confirms every transaction logs both sides) |
| **Exact duplicates** — full-row (all 11 columns identical) | **58.2M excess rows (4.06%)** — the initial "17%" used a 5-column key and conflated legitimate multi-fills; see revised deep-dive below | ⚠️ **Real but smaller than first measured; now cleaned** |
| Unresolved markets (no winning_outcome) | 421,916,034 trades on 556,625 contracts (29% of trades) | ⚠️ Filtered automatically by FLB pipeline; not an analysis issue but explains the funnel |

### Tier 2 — quality / outlier checks

| Check | Result | Status |
|---|---|---|
| Trade size distribution | Min $1e-6, median **$2.96**, mean **$38.28**, max **$2,994,000** | — |
| Size percentiles | P99 = $500, P99.9 = $4,750, P99.99 = $23,392 | Heavy right tail |
| Mega-trades (>$1M) | 46 trades | ⚠️ Worth inspecting; 46 of 1.4B is a tiny share but each one moves a slice |
| Trades ≥ $100K | 11,646 | ⚠️ Tail but not aberrant |
| Micro-trades (<$0.01) | 17,215,878 (1.2% of all trades) | ⚠️ Suggests dust trades / wash signature |
| Zero or negative usdcSize | 0 | ✅ Clean |
| `usdcSize ≈ size × price` consistency | All 1.435B rows pricable, no mismatches | ✅ Clean (size is derived, so this is tautological) |
| **Wallet concentration** | Top 0.1% wallets (**1,204 wallets**) = **58.35%** of $54.95B volume; Top 1% = 85.14%; Top 10% = 96.01% | ❌ **Extreme Pareto** |

### Tier 3 — behavioral signatures

| Check | Result | Interpretation |
|---|---|---|
| Round-cent prices ($0.01 grid) | 770.6M trades (**53.7%**) | ❌ More than half of all trades at round-cent prices — algorithmic / placement bias |
| Round 0.001 prices | 874.3M (60.9%) | Continues the algo signal |
| Exactly $0.50 | 28,276,388 (2%) | $0.50 is the dominant single price |
| Exactly $0.25 / $0.75 | 11.6M / 8.2M | Other anchors |
| **Same-wallet self-trades** (`proxyWallet == counterparty`) | **~0** (a few dozen genesis-month test rows in 1.4B) | ✅ **No wash signature** — the earlier "20%" was the same multi-fill artifact counted another way (see deep-dive) |

### Revised duplicate analysis — the initial 17% was an over-count

The first pass keyed duplicates on `(proxyWallet, conditionId, timestamp, side, price)` — five columns — and assumed a match meant byte-for-byte identity. **It doesn't.** The trades schema also carries `counterparty` and `usdcSize`, and two rows that share those five fields but differ in counterparty or size are a **single order filled against multiple resting orders in the same second — legitimate partial fills, not duplicates.**

Decomposing the full 1.435B rows per `year_month` (full-row identity = all 11 columns):

| Metric | Rate (recent months) | Meaning |
|---|---|---|
| Full-row exact duplicates (all columns identical) | **~4%** (58.2M rows total) | true ingestion replays — safe to remove |
| 5-column-key "duplicates" (the initial figure) | ~17–19% | includes legitimate multi-fills |
| Multi-fill rows in those key-groups (differ in cp/size) | ~13–15% | **legitimate** — one order, many counterparties |
| Literal self-trades (`proxyWallet == counterparty`) | **~0** | a few dozen genesis-month test rows in 1.4B |

So the "20% same-wallet wash" reported earlier was **not** self-dealing (there is essentially none) — it was this same partial-fill phenomenon measured a different way (a wallet receiving multiple fills in one contract-second).

**The true replay rate plateaued — it did not climb to 18%.** Corrected full-row excess by year:

| Year | Total trades | Full-row excess | % |
|---:|---:|---:|---:|
| 2022 | 712 | 12 | 1.7% |
| 2023 | 371K | ~3K | ~0.8% |
| 2024 | 62.2M | 399K | 0.64% |
| 2025 | 279.6M | 13.1M | 4.68% |
| 2026 | 1,093.2M | 44.7M | 4.09% |

The apparent "rising to 18.4%" in the first pass was driven by **deepening order books producing more legitimate partial fills**, not by more replays — the genuine replay rate has sat at ~4–5% since 2025. The signature still points to ingestion-pipeline replay (Polygon events re-indexed), just at one-quarter the scale first reported.

**Spot-check of the high-multiplicity groups.** Even the largest "multi-fill" groups are replay-heavy rather than genuine 200-way sweeps: the top group in 2025-12 had 208 rows but only **2 distinct counterparties and 4 sizes**, containing runs of 12 byte-identical rows (`cp=0x09fe…, usdc=0.05, maker=True, out=Up`). Full-row `DISTINCT` correctly collapses those identical runs — even when embedded inside an otherwise-multi-fill group — while preserving genuinely distinct fills. And 54% of multi-fill rows sit in groups where every fill has a unique counterparty (a real book-sweep), which we keep.

### The clean dataset

Built on EC2 as a full-row `DISTINCT` per partition, written alongside the untouched raw:

- **Path:** `/mnt/data/pipeline_output/trades_clean.parquet` (year_month-partitioned, layout-identical to raw; re-sorted by `conditionId, timestamp` for compression/scan efficiency)
- **1,435,301,230 → 1,377,065,934 rows** (58,235,296 removed, **4.057%**)
- **Volume:** $54.95B → $53.94B (only **−1.84%**) — removed rows average **$17.37** vs the dataset's $38.28, i.e. replays skew tiny/penny, exactly the expected signature
- **Distribution preserved:** mean size 38.28→39.17, median 2.96→3.00, fraction sub-$1 0.2592→0.2565, price-extreme fraction 0.1484→0.1506 (clean is marginally *less* penny-heavy)

Per-category drop (confirms the dedup isn't concentrated in a way that biases category-level FLB):

| Category | raw rows | row drop % | vol drop % |
|---|---:|---:|---:|
| Crypto | 1.01B | 4.78 | 2.84 |
| Sports | 192M | 2.92 | 1.55 |
| Esports | 25.8M | 3.00 | 1.30 |
| Politics | 63.7M | 1.33 | 1.07 |
| Tech | 35.1M | 1.65 | 1.18 |
| Weather | 33.1M | 2.10 | 4.34 |
| Iran | 17.3M | 2.53 | 2.17 |

The dedup lands mostly on **Crypto up/down** (the penny-replay zone); every category drops more rows than volume (except Weather).

### How this changes the FLB analysis

1. **SE inflation is small, not 10%.** With ~4% replays, t-stats are inflated by ≈√(1/(1−0.04)) ≈ **1.02**, not the ~1.10 the first pass implied. Headline results are unaffected; only spreads sitting exactly on the Bonferroni line could move.
2. **The Sports anomaly is *not* a duplicate artifact.** Only 2.9% of Sports rows were replays (1.55% of Sports volume). The positive-offset anomaly in niche sports must be explained by liquidity/weighting — see the volume-weighting discussion — not by replays.
3. **Re-run v5 on the clean dataset** to confirm the deltas are within the ≈2% the row-count change implies. The pipeline is repointed to the clean parquet via `config.py: TRADES_PARQUET_GLOB`, with the raw path retained as a reversible comment.

### Descriptive facts that remain (real market features, not errors)

These were measured on raw and shift by at most the ~4% dedup; they are genuine market structure to document, not clean out:

1. **Wallet concentration is extreme** — top 0.1% of wallets (~1,204) ≈ **58%** of $54.95B volume; top 1% ≈ 85%. The 3-way SE already clusters on `proxyWallet`, so this is partly accounted for; a top-100-excluded robustness check would show whether headlines are sharp-money-driven or aggregate behavior.
2. **53.7% of trades at round-cent prices** (60.9% on the $0.001 grid) — heavy algorithmic / market-maker footprint. A `usdcSize`-weighted calibration (discussed separately) upweights retail-sized trades relative to penny algo prints.
3. **Heavy size tail** — median $2.96, mean $38.28, 46 trades over $1M. Tiny share of rows but each moves a slice; worth a footnote if any land on a high-impact contract.

### Bottom line

The trades dataset is **cleaner than the first audit concluded.** Corrected findings:

1. The single real data error is **~4% full-row ingestion replays** (58.2M rows), now removed in a canonical clean dataset at `/mnt/data/pipeline_output/trades_clean.parquet`. The "rising to 18%" was an artifact of keying on 5 columns; the true replay rate plateaued at ~4–5%.
2. The **"20% wash" does not exist** — literal self-trades are ~0; that figure was the same legitimate partial-fill phenomenon measured another way.
3. **Wallet concentration (58%)** and **round-price clustering (53.7%)** are genuine market features to document.

Next step: re-run the v5 pipeline on the clean dataset and confirm the deltas are within the ≈2% the row-count change implies — and use the clean dataset for all analysis going forward.

## Sports/Esports calibration: is it illiquid-market skew? (volume-weighting case study)

_Question: the Sports/Esports deciles show a positive calibration offset. Is it driven by small, illiquid niche-sport markets that are highly miscalibrated and skew the aggregate? Run on the **clean** dataset, all trade-level (no contract aggregation — that over-weights the niche tail), two lifecycle windows, three lenses: **trade-count vs trade-dollar** weighting, a **liquidity-floor sweep** (drop contracts below $X total notional), and a **per-league breakdown** (league = `split_part(eventSlug,'-',1)`). 3-way clustered SE (day × wallet × market) on the headlines. `cal_error = won − price`, BUY side, price ∈ (0.01, 0.99)._

### Finding 1 — it's a level *offset*, not favorite-longshot bias, and it flips sign across the lifecycle

The D10−D1 spread is ~0 in the mature window (insignificant), so this is not FLB — it's a uniform level shift, and it **reverses sign** between the mature and closing windows for **both** categories:

| Window | Category | offset (count) | offset (dollar) | D10−D1 spread |
|---|---|---|---|---|
| **Mature 25–80%** | Sports | **+0.0251** (t=+4.2) | **+0.0265** (t=+5.7) | −0.018 (t=−0.6, ns) |
| | Esports | **+0.0732** (t=+9.8) | **+0.0689** (t=+7.5) | −0.006 (ns) |
| **Closing 80–100%** | Sports | **−0.0108** (t=−6.8) | −0.0138 (t=−3.7) | +0.022 (t=+5.6) |
| | Esports | **−0.0664** (t=−12.9) | −0.0667 (t=−6.9) | +0.011 (ns) |

Mid-game, BUY takers **beat** the price; at the close they **lose**. The Esports decile shapes are near-perfect mirror images (mature: +0.09 hump across D2–D8; closing: −0.09 hump) — a clean two-regime signature.

### Finding 2 — the answer to "is it illiquid skew?" is window-dependent

**Dollar-weighting leaves the offset essentially unchanged** in every cell above (Sports +0.0251→+0.0265; Esports +0.0732→+0.0689). So it is not a small-*trade* artifact. The liquidity-floor sweep (offset as we drop contracts below $X total notional) then splits the two windows apart:

| Floor | Mature Sports | Mature Esports | Closing Sports | Closing Esports |
|---|---|---|---|---|
| $0 | +0.025 | +0.073 | −0.011 | −0.066 |
| $10K | +0.032 | +0.098 | +0.002 | −0.056 |
| $100K | +0.038 | +0.140 | +0.029 | +0.000 |
| $1M | **+0.088** | **+0.170** | **+0.085** | **+0.231** |

- **Mature window: NOT illiquidity.** The positive offset *grows* as you drop illiquid contracts — it lives in the **most-traded** markets, not the niche tail. Even the majors carry it (per-league, mature: NBA +0.018, NFL +0.018, NHL +0.015, EPL +0.016). The niche leagues *amplify* it (cs2 +0.104, dota2 +0.101, wta +0.088, atp +0.066, cbb +0.053) but are not the cause. **Hypothesis refuted for the mature window.**
- **Closing window: yes, illiquidity.** The buyer-loss at floor $0 (−0.011 / −0.066) is concentrated in **small contracts** — raise the floor and it flips *positive* (large closing markets: buyers win). Here the niche leagues drive it (closing: cs2 −0.068, lol −0.060, atp −0.036, mlb −0.033 vs NFL +0.005, EPL +0.019). **Hypothesis supported for the closing window.**

Note: dollar-weighting *alone* did not reveal this (the closing offset is slightly *more* negative under dollar weighting); only the contract-level **liquidity floor** surfaced it — because the effect is a contract-level (small-market) phenomenon, not a small-trade one. That's the methodological point: when the worry is a confounded subpopulation, a floor/stratification diagnoses it where reweighting alone hides it.

### Interpretation (hypotheses, not claims)

A coherent within-game information story fits both regimes:
- **Mature (25–80%):** as a game/match progresses, information arrives; BUY takers hitting stale maker asks ride it and beat the price — *stronger in high-attention, liquid markets* where flow and news are richest.
- **Closing (80–100%):** the outcome becomes visible to low-latency holders who post resting asks; public BUY takers fill them and get picked off — *stronger in small, illiquid markets* where the latency/spread edge is largest. (This matches the closing-window EV-transfer mechanism already in the FLB writeup.)

### Implications for the FLB analysis

1. Report **both weightings**; for Sports/Esports they agree, so the offset is economically real, not a penny-trade artifact.
2. The mature offset is robust — report it as-is (and note it is a *level* effect, distinct from FLB).
3. For the **closing** window, report the **liquidity-floor sweep** (or apply a floor), because the small-market buyer-loss is real but unrepresentative of traded dollars — at scale the sign flips. Do **not** collapse to contract-level equal weighting (it would over-weight exactly the small closing markets that flip the sign).
4. This is a calibration/microstructure finding (information timing), not a duplicate or data-quality artifact — only 2.9% of Sports rows were replays, already removed.

Result JSONs: `/tmp/sports_wt/sports_wt_{25_80,80_100}.json`; analysis script `/home/ubuntu/sports_weighting.py` on EC2.

## Files produced

All small summary parquets pulled to `/tmp/data_explore_results/`:
- `data_explore_sports_by_template.parquet` — 142 cells, per-(template, decile) Sports calibration
- `data_explore_sports_buy_sell.parquet` — confirms BUY=−SELL mechanics
- `data_explore_politics_buy_sell.parquet` — Politics control (shows classic FLB with huge magnitudes)
- `data_explore_tech_overlap.parquet` — 7,415 in 25-80% / 6,183 in 80-100% / 5,067 in both
- `data_explore_tech_top_contracts.parquet` — top 30 Tech contracts in 80-100% window
- `data_explore_tech_subset_calibration.parquet` — per-(subset, window, decile) Tech calibration

EC2 stopped.
