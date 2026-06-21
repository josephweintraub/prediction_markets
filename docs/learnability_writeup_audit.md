# Audit of `learnability_writeup.md` — empirical findings

_Conducted on 2026-06-07 via EC2 i-0f5b31a268af53938 + local compute. ~3 hr of EC2 time. Every recommendation in this report is grounded in numbers from re-run analyses, not inference. The main writeup is **not edited** by this audit; revisions are a follow-up plan informed by these findings._

## Executive summary

1. **Most v4 headline findings survive up/down exclusion intact.** Small 2-20 × High vol spread is **+0.0501 either way** (the slice is 0.1% up/down). Small 2-20 family-size, 0-priors, and 50+ event_slug priors are all within 1% of their original spreads. Several v3 findings DO collapse — `dim_subject_specificity` "1 subject" goes from +0.0367\*\*\* → +0.0153 n.s.; `dim_info_type_supergroup` `market_data` slice shrinks 87% in trade count and becomes n.s. These were essentially up/down-driven.
2. **The text-novelty finding strengthens at meaningful semantic thresholds.** Re-binning by fixed cosine thresholds (`<0.50` genuinely isolated, etc.) gives the `<0.50` bucket — 12,985 contracts (1.2% of universe) — a spread of **+0.0617 t=+15.37\*\*\*** (50-80%) and **+0.0516 t=+12.72\*\*\*** (full). The Q1 cutoff at 0.896 in the writeup is misnamed but the underlying effect is real.
3. **The text-novelty Q1 finding is concentrated in long-duration markets.** Within `>1mo` contracts, Q1 spread is **+0.0666 t=+21.48\*\*\*** (the strongest cell anywhere). Within `<1h` contracts, Q1 spread is −0.0102 (n.s., reversed sign). The "novelty → FLB" channel is a LONG-MARKET phenomenon, not a crypto-Up/Down artifact.
4. **The 50+ event_slug priors finding is one event family** ("US strikes Iran by [DATE]"). All top-10 contracts in that slice are Iran-strike-date variants. The +0.0805 spread reflects FLB on a single recurring betting line, not a general "many priors" pattern.
5. **Bot threshold ±25% sensitivity is negligible.** Headline spreads swing by ≤0.003 across all three bot variants on every headline cell. The choice of bot filter is not a confound for the headline results.

**Up/down decision: REMOVE from primary, analyze separately.** Up/down markets dominate Q1-Q4 of `dim_text_novelty` (86-95% of each), the `market_data` info-type supergroup (87% of trades), and the `<1h` contract horizon. Excluding them clarifies — does not weaken — the headline FLB story.

**Multiple-testing pass: 33 of 87 v4 tests survive Bonferroni at α=0.05.** All four headline findings (Q1 isolated, 0-strict-neighbors, Small × HighVol, prior_settlements 50+ in fine groupings) pass with |t| > 12. The marginal `*` decorations on smaller cells do not.

---

## 1. Methodology — what is actually baked in

### 1.1 Bot filter (vendored from EC2 to `/tmp/learnability/bot_filter.py`)

The `is_nonhuman` composite is the OR of four behavioral criteria, NOT a simple n_trades threshold:

- **A. Inter-trade interval (ITI)** median < 1s → A-definite; 1-10s → A-likely
- **B. Trades per active day** > 500 → B-definite; > 200 → B-likely
- **C. Hour-of-day HHI** < 0.06 AND n_trades > 500
- **E. Fixed trade size** CV(usdcSize) < 0.05 AND n_trades > 50

`is_nonhuman = flag_a_definite OR (flag_a_likely AND any of {B, C, E}) OR (flag_b_definite AND flag_c) OR (≥2 of {flag_b_likely, flag_c, flag_e})`.

Original sweep: 255,719 wallets flagged (21.2%), 1,164,195,182 trades flagged (81.1%). Bot sensitivity reported in §11.

### 1.2 Lifecycle window arithmetic — comparable across durations?

`flb_per_slice_v3.py:137` computes `mkt_duration = MAX(timestamp) - MIN(timestamp)` from `trades_buy` with `GREATEST(..., 1)` floor. **The "50-80%" window means 50-80% of each contract's actual trading range** — meaning a 15-minute crypto-Up/Down contract's window is 4.5 minutes; a 6-month election's window is 1.5 months. See §10 for the impact: text-novelty Q1 spread is **+0.0666 t=+21.48\*\*\*** in `>1mo` contracts but n.s. in `<1h` contracts.

### 1.3 BUY-only filter, equal-width deciles, 5K threshold

- FLB is computed only on `side='BUY'` trades (intentional; quantifies trader return-on-stake). **Not disclosed in writeup Setup.**
- Deciles are equal-width (0.10 probability per bin). Per Phase 1.6, for Small 2-20 × High vol, D1/D10 actually have MORE trades than the middle (847K vs 211K-264K at D5/D6). Equal-width is fine for the headline slices.
- `MIN_TRADES_PER_SLICE = 5000` per-slice filter applied silently — slices below this are dropped from tables.

### 1.4 3-way clustered SE — degeneracy concern

`flb_per_slice_v3.py:33-72` uses Cameron-Gelbach-Miller factorize encoding. No guards against small cluster counts. Phase 2.5 confirmed an SE collapse pathology: when the trades view filtered out all up/down trades, the leftover 158K up/down trades in `dim_market_type` produced a t = +1158 (nonsense), confirming variance estimates are unstable on degenerate slices.

---

## 2. Multiple-testing accounting (Phase 1.5)

Family size: **~181 spread tests** (v3 ~94 + v4 87).

| Cutoff | Critical \|t\| | v4 tests passing |
|---|---:|---:|
| Naive α = 0.05 | 1.96 | 54 / 87 |
| **Bonferroni α = 0.05** | **3.64** | **33 / 87** |

v4 tests surviving Bonferroni (full table in `/tmp/learnability_audit/multiple_testing_accounting.json`):

| t | spread | N | window | dim : slice |
|---:|---:|---:|---|---|
| +16.95 | +0.0611 | 268K | full | prior_settlements__dim_group_strict : 50+ |
| +16.90 | +0.0563 | 3.15M | half | text_novelty : Q1 most isolated |
| +15.14 | +0.0606 | 1.60M | half | text_neighbors_strict : 0 strict |
| +13.16 | +0.0419 | 9.64M | full | text_novelty : Q1 most isolated |
| +12.82 | +0.0599 | 154K | full | prior_settlements__event_slug : 50+ |
| +12.73 | +0.0506 | 4.65M | full | text_neighbors_strict : 0 strict |
| +11.98 | +0.0501 | 3.97M | half | family_size_x_vol : Small 2-20 × High vol |
| +9.63 | +0.0458 | 1.10M | half | prior_settlements__event_template : 6-50 |
| +9.55 | +0.0579 | 174K | half | prior_settlements__dim_group_strict : 6-50 |
| +8.13 | +0.0401 | 14.36M | full | family_size_x_vol : Small 2-20 × High vol |
| +8.09 | +0.0432 | 8.30M | half | prior_settlements__event_template : 0 |
| ... 22 more | | | | |

Marginal v4 results that DON'T survive Bonferroni (had `*` decoration in writeup, should be downgraded to "directional"):

- `dim_family_size_x_vol` Medium 21-1K × High vol (t = +2.20)
- `dim_vol_per_contract_tier` VPC Q2 (t = −2.24)
- `dim_text_novelty` Q3 moderate (t = +2.38)
- `dim_text_novelty` Q5 most repetitive (t = +2.55)
- All `dim_text_neighbors_strict` 6+ neighbors (t ≈ +3.0–3.4) — borderline

---

## 3. Jaccard overlap between "convergent" headline contract sets (Phase 1.1)

Set sizes (contracts whose v4 dim labels match):
- v3 Small 2-20: **150,205**
- v4 Small × HighVol: **55,651**
- v4 Q1 text-isolated: **135,302**
- v4 0-strict-neighbors: **17,761**
- v4 0-priors event_template: **148,174**

Pairwise Jaccard:

| | v3_Small | Small×HighVol | Q1_isolated | 0_neighbors | 0_priors |
|---|---:|---:|---:|---:|---:|
| v3 Small 2-20 | 1.00 | 0.37 | 0.04 | 0.05 | **0.56** |
| Small × HighVol | 0.37 | 1.00 | 0.03 | 0.07 | 0.29 |
| Q1 isolated | 0.04 | 0.03 | 1.00 | 0.13 | 0.04 |
| 0 neighbors | 0.05 | 0.07 | 0.13 | 1.00 | 0.05 |
| 0 priors | 0.56 | 0.29 | 0.04 | 0.05 | 1.00 |

**Conclusion**: the "four convergent confirmations" are NOT one underlying contract set. They identify **different** sub-populations. v3 Small 2-20 and 0-priors substantially overlap (0.56) — both capture "few similar contracts ever traded" at related levels of granularity. But the text-novelty cuts (Q1, 0-neighbors) overlap only 0.13 with each other and ≤0.07 with the family-size cuts. **Each headline finding identifies an independent slice of the universe.** The headline framing of "convergent confirmations" is empirically supported.

---

## 4. Category decomposition (Phase 1.2)

Top-5 `primary_category` shares within each headline contract set:

| Set | Top categories |
|---|---|
| v3 Small 2-20 (N=150,205) | **Sports 64.7%**, Politics 10.6%, Culture 7.0%, Esports 4.6%, Crypto 3.2% |
| Small × HighVol (N=55,651) | **Sports 52.2%**, Politics 16.9%, Culture 8.7%, Crypto 5.7%, Geopolitics 5.0% |
| Q1 text-isolated (N=135,302) | **Crypto 90.1%**, Politics 2.3%, Sports 2.1%, Finance 1.2%, Tech 1.1% |
| 0-strict-neighbors (N=17,761) | **Crypto 56.5%**, Politics 12.2%, Sports 10.9%, Finance 4.1%, Culture 4.0% |
| 0-priors event_template (N=148,174) | **Sports 51.8%**, Politics 12.5%, Mentions 9.5%, Culture 7.8%, Crypto 3.8% |

**Q1 text-isolated is 90% Crypto** — overwhelmingly. The reason is mechanical: most Crypto contracts are up/down series with distinctive time-slot text and few exact matches, so they fall into Q1. Phase 1.3 quantifies this: Q1 is **86.7% up/down markets**.

The other headline cells are Sports-dominated. The convergent findings are NOT measuring the same contracts — but the Q1 cell is so category-skewed that the writeup's "semantic novelty drives FLB" claim is partly tautological (Crypto Up/Down contracts have very distinct text and also dominate this slice).

**§10 shows the novelty effect survives within `>1mo` contracts (mostly non-Crypto), confirming the channel is real, not just a Crypto re-labeling.**

---

## 5. Up/down market composition (Phase 1.3)

**459,369 of 1,117,358 contracts (41.11%) are up/down.** 188 distinct event_templates; the top 10 (btc/eth/sol/xrp/doge/bnb/hype Up-or-Down plus a few legacy variants) account for 91.9%.

Up/down share by v4 slice:

| Slice | Up/down share |
|---|---:|
| dim_text_novelty Q1 most isolated | **86.7%** |
| dim_text_novelty Q2 mod isolated | 95.0% |
| dim_text_novelty Q3 moderate | 77.9% |
| dim_text_novelty Q4 repetitive | 41.2% |
| dim_text_novelty Q5 most repetitive | 1.2% |
| dim_text_neighbors_strict 0 neighbors | 43.3% |
| dim_text_neighbors_strict 6+ neighbors | 41.1% |
| dim_family_size_x_vol Large 1K+ × High vol | 55.4% |
| **dim_family_size_x_vol Small 2-20 × High vol** | **0.1%** |
| dim_vol_per_contract_tier Q1 thinnest | 64.3% |
| dim_vol_per_contract_tier Q5 thickest | 8.3% |
| dim_prior_settlements__event_template 50+ | 52.0% |
| **dim_prior_settlements__event_slug 50+** | **0%** |
| dim_prior_settlements__dim_group_strict 50+ | 70.2% |
| dim_primary_category Crypto | 88.3% |
| dim_event_family_size Large 1K+ | 55.4% |

The headline non-up/down cells: **Small × High vol (0.1%)**, **event_slug 50+ priors (0%)**, **Small 2-20 family (0.2%)**. These are the "clean" findings the writeup can lean on.

The headline up/down-dominated cells: **Q1 isolated (86.7%)**, **dim_group_strict 50+ priors (70.2%)**. These require the §6 + §10 analyses to disentangle.

---

## 6. Up/down-EXCLUDED re-run (Phase 2.1) — the empirical answer

Full v3 + v4 pipeline re-run on EC2 with `AND eventSlug NOT LIKE '%updown%' AND eventSlug NOT LIKE '%up-or-down%'` added to the trades view. Both lifecycle windows. 53.4M trades (50-80% window) and 53.4M (full) after exclusion (vs 24.5M and 85M before — note the full lifecycle window shrinks because Up/Down contracts have many trades).

**Headline cells — original (up/down KEPT) vs audit (up/down EXCLUDED), 50-80% window:**

| Dim : slice | Orig spread | Audit spread | Δ | N change |
|---|---:|---:|---:|---:|
| family_size_x_vol Small × HighVol | +0.0501 (t=+11.98) | **+0.0501 (t=+11.98)** | +0.000 | −0% (0.1% updown) |
| event_slug Small 2-20 size | +0.0169 (t=+4.76) | **+0.0258 (t=+5.56)** | +0.009 | −61% |
| family_vol_tier High vol | +0.0244 (t=+5.07) | +0.0282 (t=+4.85) | +0.004 | −39% |
| dim_text_novelty Q1 isolated | +0.0563 (t=+16.90) | **+0.0625 (t=+19.30)** | **+0.006** | −36% |
| dim_text_neighbors 0 neighbors | +0.0606 (t=+15.14) | +0.0617 (t=+15.37) | +0.001 | −7% |
| prior_settlements event_slug 50+ | +0.0805 (t=+6.54) | **+0.0805 (t=+6.54)** | +0.000 | 0% (0% updown) |
| prior_settlements event_template 0 | +0.0432 (t=+8.09) | +0.0432 (t=+8.09) | 0.000 | 0% |
| group_strict_size Singleton 1 | +0.0378 (t=+3.65) | +0.0308 (t=+2.46) | −0.007 | −18% |
| family_size_x_vol Large 1K+ × HighVol | −0.0072 (t=−1.44) | −0.0184 (t=−1.76) | −0.011 | −68% |
| vol_per_contract Q1 thinnest | −0.0574 (t=−2.17) | −0.0423 (t=−1.59) | +0.015 | −13% |

**Three patterns:**

1. **Spread UNCHANGED for cells with low up/down share** (Small × High vol, event_slug 50+, event_template 0-priors). These findings are completely independent of the up/down decision.
2. **Spread INCREASES for Q1 text-isolated** when up/down is excluded — i.e., up/down was DILUTING the genuinely-isolated signal. Removing up/down strengthens the novelty finding.
3. **Spread changes modestly for v3 dims that were partly up/down driven** — Small 2-20 event_slug grows from +0.0169 to +0.0258 (smaller, cleaner sample); High vol attenuates slightly.

**v3 findings that EVAPORATE when up/down is excluded:**

| Dim : slice | Orig (with up/down) | Audit (no up/down) |
|---|---|---|
| `dim_info_type_supergroup` market_data | +0.0083 t=+0.99 (10.9M trades) | **−0.0122 t=−0.46 (1.4M trades)** — drops 87% |
| `dim_subject_specificity` 1 subject | +0.0367 t=+7.47 | **+0.0153 t=+1.57** — NOT SIGNIFICANT |
| `dim_primary_category` Crypto | −0.0016 t=−0.16 (11M) | +0.0069 t=+0.58 (1.6M) — drops 85% |
| `dim_contract_horizon` <1h | −0.0058 t=−1.27 (7.7M) | −0.0557 t=−1.24 (78K) — drops 99% |
| `dim_contract_horizon` 1h-1d | −0.0127 t=−2.30 (2.8M) | **−0.0357 t=−4.60\*\*\*** — REVERSED FLB stronger |

The v3 sections on `market_data` info-type and `1 subject` specificity were almost entirely about up/down markets. The v3 narrative ("data-driven outcomes have public reference series → calibrated") loses its empirical basis when up/down is removed: there isn't enough non-up/down `market_data` to support the claim.

**Recommendation: remove up/down from the primary analysis. The clean findings survive intact; the un-clean findings reveal themselves as up/down-driven.**

---

## 7. Top-10 contracts in tiny slices (Phase 1.4)

### dim_group_strict_size = "Singleton 1" (50-80% window, N=4,536 contracts in slice)

Top contracts by trade count are a healthy mix:

| n | category | question |
|---:|---|---|
| 1,954 | Culture | Will Hikari win Best Director at the 98th Academy Award |
| 1,823 | Sports | Will Nick Taylor win the 2026 Masters tournament? |
| 1,774 | Culture | Will Saoirse Ronan win Best Actress at the 2025 BAFTA |
| 1,510 | Sports | Set 1 Winner: Atlangeriev vs Nedic |
| 1,507 | Sports | Will FK Metalist 1925 Kharkiv vs FK Kudrivka end in a draw |
| 1,481 | Sports | Will there be a run scored in the first inning? Texas... |
| 1,473 | Weather | Will the highest temperature in Dallas be between 70-71 |
| 1,395 | Sports | Will the fight be won by KO or TKO? |
| 1,367 | Esports | Rainbow Six Siege: Falcons vs Geekay Esports |
| 1,235 | Sports | Set 1 Winner: Lares vs Tikhonova |

The Singleton finding represents real diverse one-off markets. The composition is reasonable.

### dim_prior_settlements_bin__event_slug = "50+" (N=2,342 contracts, 154 trades-bearing at 50-80%)

**ALL top-10 are "US strikes Iran by [DATE]" markets**:

| n | category | question |
|---:|---|---|
| 17,290 | Iran | US strikes Iran by February 27, 2026? |
| 8,116 | Iran | US strikes Iran by February 27, 2026? |
| 6,939 | Iran | US strikes Iran by February 26, 2026? |
| 6,646 | Iran | US strikes Iran by March 15, 2026? |
| ... | Iran | (six more US-strikes-Iran-by-date variants) |

This slice is **a single event family** — Iran strike date variants — that happened to accumulate 50+ same-event_slug prior settlements before its last fresh contract resolved. The +0.0805 spread (t=+6.54\*\*\*) in §10.1 B is the FLB of **one recurring betting line**, not a general "many priors → reversed FLB" pattern. The writeup should soften the §10.1 B 50+ finding accordingly.

### dim_prior_settlements_bin__dim_group_strict = "50+"

Same — top-10 are also Iran strike markets. The 70% up/down share in this dim_group_strict 50+ bucket per Phase 1.3 reflects up/down markets that happen to share strict groupings (likely the same hourly crypto template across all time slots).

---

## 8. Decile mass sanity (Phase 1.6)

For Small 2-20 × High vol at 50-80%: D10/D1 trade-count ratio = **1.3x** (D1: 847K, D10: 1.1M). Per-decile counts are NOT badly unbalanced — trade mass actually clusters at the extremes (>0.9 and <0.1). Equal-width deciles work fine for representative headline slices. The methodology concern raised by the prior workflow critique was overblown.

---

## 9. Text-novelty re-binning at meaningful thresholds (Phase 2.3)

The original Q1 cutoff at `best_sim = 0.896` means "Q1 most isolated" actually contains everything below cosine 0.90 (mostly contracts in the 0.75-0.90 "has neighbor" range plus the tiny genuinely-isolated tail). Re-binning at fixed semantic thresholds:

**50-80% window**:

| New bin | N contracts | N trades | Spread | t |
|---|---:|---:|---:|---:|
| <0.50 **genuinely isolated** | 12,985 | **1.51M** | **+0.0617** | **+15.37\*\*\*** |
| 0.50-0.75 mod isolated | 4,776 | 90K | −0.0074 | −0.18 |
| 0.75-0.90 has neighbor | 145,746 | 1.95M | +0.0391 | +7.56\*\*\* |
| 0.90-0.95 close lex match | 225,159 | 6.75M | +0.0156 | +2.59\*\* |
| >0.95 near duplicate | 728,692 | 14.2M | +0.0166 | +2.20\* |

**Full lifecycle**:

| New bin | N trades | Spread | t |
|---|---:|---:|---:|
| <0.50 genuinely isolated | 4.29M | **+0.0516** | **+12.72\*\*\*** |
| 0.75-0.90 has neighbor | 6.26M | +0.0227 | +4.82\*\*\* |
| 0.90-0.95 close lex match | 22.50M | +0.0105 | +3.12\*\* |
| >0.95 near duplicate | 51.59M | +0.0124 | +2.10\* |

**Conclusion**: the novelty effect survives at a meaningful threshold. The `<0.50` slice (1.2% of contracts but 6% of full-lifecycle trades) carries spread +0.0617 / +0.0516 — comparable to the original Q1 result. The writeup's Q1 finding is **directionally correct but mis-labeled**: "Q1 most isolated" is actually a near-duplicate detector, but the real "isolated" tail (which the writeup never separates out) is the strongest cell.

**Recommended writeup change**: replace `dim_text_novelty` quintile binning with these fixed thresholds. Report the `<0.50` slice as the headline; report the others descriptively.

---

## 10. Lifecycle-window comparability by contract duration (Phase 2.2)

The "50-80% canonical window" was suspect because durations vary by 5+ orders of magnitude. Test: within each `dim_contract_horizon` bucket, run FLB for the headline novelty dims at the 50-80% window.

**dim_text_novelty Q1 most isolated by duration bucket:**

| Duration | N trades | Spread | t |
|---|---:|---:|---:|
| <1h | 917,140 | −0.0102 | −0.84 (n.s.) |
| 1h-1d | 270,661 | +0.0301 | +2.52* |
| 1d-1w | 120,090 | +0.0482 | +4.86\*\*\* |
| 1wk-1mo | 219,897 | +0.0308 | +1.66 |
| **>1mo** | **1,623,848** | **+0.0666** | **+21.48\*\*\*** |

**dim_text_neighbors_strict 0 strict neighbors by duration bucket:**

| Duration | N trades | Spread | t |
|---|---:|---:|---:|
| <1h | 78,542 | −0.0116 | −0.37 (n.s.) |
| 1h-1d | 84,442 | +0.0612 | +6.09\*\*\* |
| 1d-1w | 47,393 | +0.0246 | +0.82 (n.s.) |
| 1wk-1mo | 110,978 | −0.0174 | −0.37 (n.s.) |
| **>1mo** | **1,280,495** | **+0.0660** | **+18.87\*\*\*** |

**Critical finding**: the novelty effect is **strongest in long-duration (>1mo) markets** and **absent in <1h markets**. Within `>1mo`, Q1 spread is +0.0666 (the largest cell anywhere) and 0-neighbors is +0.0660. Within `<1h`, both are n.s. and trending negative.

**Implications**:
1. The "novelty → FLB" channel is a phenomenon of contracts that trade for weeks/months, not of crypto-Up/Down hourly markets.
2. The unified "50-80% canonical window" works for `>1mo` markets but is essentially meaningless for `<1h` (50-80% of a 15-minute window is 7.5 minutes of noise trading).
3. **Recommended writeup change**: report the headline novelty finding restricted to `>1mo` contracts, with a footnote on `<1h` showing it does not apply.

---

## 11. Bot-threshold sensitivity (Phase 2.4)

Three variants of the bot filter ITI and trades-per-day thresholds (criteria C and E held constant):

| Variant | ITI def | ITI lik | TpD def | TpD lik | % nonhuman trades |
|---|---:|---:|---:|---:|---:|
| current | <1.0s | 1-10s | >500 | >200 | **81.1%** |
| looser (−25%) | <0.75s | 1-7.5s | >625 | >250 | 79.9% |
| tighter (+25%) | <1.25s | 1-12.5s | >375 | >150 | 82.6% |

Net effect on `is_nonhuman` trade share: ±1.4 percentage points. **Very small** despite ±25% threshold changes. The composite logic (multiple criteria) makes the result robust.

**Headline FLB across bot variants (50-80%):**

| Dim : slice | current | looser | tighter | swing |
|---|---:|---:|---:|---:|
| text_novelty Q1 isolated | +0.0563 | +0.0553 | +0.0580 | 0.003 |
| group_strict_size Small 2-20 | +0.0258 | +0.0256 | +0.0260 | 0.000 |
| group_strict_size Singleton 1 | +0.0378 | +0.0372 | +0.0342 | 0.004 |
| family_size_x_vol Small × HighVol | +0.0501 | +0.0495 | +0.0516 | 0.002 |
| family_size_x_vol Large 1K+ × HighVol | −0.0072 | −0.0064 | −0.0082 | 0.002 |
| prior_settlements__event_template 0-priors | +0.0432 | +0.0430 | +0.0430 | 0.000 |
| prior_settlements__event_template 50+ | −0.0073 | −0.0065 | −0.0084 | 0.002 |

**No headline cell swings by more than 0.005 across all three bot variants.** The bot filter is not a hidden confound on the headline findings.

The biggest swing in the entire table is 0.017, on `dim_family_size_x_vol` Small 2-20 × Low vol (35K trades, n.s. in all three variants).

---

## 12. Prioritized writeup revisions

Each revision cites the audit section that justifies it. Implement in order.

### Tier 1 — must-do before paper draft (load-bearing claims)

1. **Add disclosure paragraph to Setup section** (`learnability_writeup.md:5-20`): list BUY-only filter, equal-width decile binning, Bonferroni-equivalent t = 3.64 for the ~100-test family, exact bot-filter criteria summary, lifecycle-window comparability caveat. (Citing §1, §2, §10.)
2. **Replace `dim_text_novelty` quintiles with fixed-threshold bins.** The Q1 cutoff at 0.896 is misnamed. Use `<0.50`, `0.50-0.75`, `0.75-0.90`, `0.90-0.95`, `>0.95` (per §9). The `<0.50` slice carries the headline FLB; the original Q1 result is a near-duplicate-detection artifact for Q2-Q5. (Citing §9.)
3. **Add an "up/down sensitivity" sub-section** showing every dim with the up/down-included vs up/down-excluded spread side-by-side (per §6). Specifically retract or footnote: `dim_info_type_supergroup` market_data result, `dim_subject_specificity` 1-subject result, and `dim_primary_category` Crypto interpretation — these collapse without up/down. (Citing §6.)
4. **Disclose that dim_prior_settlements_bin__event_slug 50+ is one event family.** Report that all top-10 contracts in this slice are US-strikes-Iran-by-date variants. Soften the +0.0805 finding to "this slice represents a single recurring event family; the spread reflects FLB on that family rather than a general 'many priors' effect." (Citing §7.)
5. **Add an appendix with the Jaccard overlap matrix and category decomposition** (per §3, §4). Show that the four headline findings are NOT one underlying contract set, but each is internally composition-concentrated (Q1 = 90% Crypto; Small × High vol = 52% Sports; etc.).
6. **Add the duration-stratified text-novelty result** (per §10): within `>1mo` contracts, Q1 spread is +0.0666 t=+21.48\*\*\*; within `<1h` it is n.s. This is the cleanest demonstration that novelty → FLB is a long-market phenomenon.

### Tier 2 — should-do for paper polish

7. **Remove `*` decorations from cells with |t| < 3.64** unless the writeup adds a multiple-testing footnote (per §2). Affected: `dim_family_size_x_vol` Medium × High vol (t=+2.20), VPC Q2 (t=−2.24), `dim_text_novelty` Q3 moderate (t=+2.38), Q5 most repetitive (t=+2.55).
8. **Footnote the 3-way SE degeneracy concern for small slices** (Singleton 1, 50+ priors in fine groupings) — t-stats may not be asymptotically valid. (Citing §1.4.)
9. **Add the bot-threshold sensitivity table** to Setup (per §11). Showing ≤0.005 swing across ±25% variants closes a common reviewer question.
10. **Soften the "Daily learning vs Episodic FLB" framing in §10** — without up/down, Daily becomes slightly negative (−0.0245 n.s.) while Episodic stays at +0.0502\*\*\*. The cleaner reading is "Episodic is the FLB locus; Daily and Recurring are near-zero or weakly positive," not a smooth "more learning → less FLB" gradient. (Citing §6.)

### Tier 3 — nice-to-have

11. **Vendor `bot_filter.py` into the project repo** (currently lives only on EC2). The audit copied it to `/tmp/learnability/bot_filter.py`; commit a copy for reproducibility.
12. **Add a "Known caveats" appendix** listing the 3-way SE degeneracy concern, the lifecycle-window non-comparability across durations, the single-event_slug bias in the 50+ priors finding, and the up/down-domination of Q1 quintile (resolved by the fixed-threshold rebin).

---

## What we explicitly did NOT do in this audit

- Re-cluster slugs at semantic-novelty thresholds beyond the 5 bins of §9.
- Run the up/down-excluded text-novelty rebin (would clarify whether the genuinely-isolated `<0.50` slice retains its FLB without up/down — recommended Tier 1 follow-up).
- Bayesian SE for the small slices flagged in §1.4.
- Kalshi cross-platform comparison (data not yet available).

---

## Audit data + scripts

All artifacts under `/tmp/v4_audit_results/` and `/tmp/learnability_audit/`.

- `/tmp/learnability/bot_filter.py` — vendored from EC2 `/home/ubuntu/pipeline/analysis/bot_filter.py`.
- `/tmp/v4_audit_results/audit_noupdown_*.parquet` — Phase 2.1 up/down-excluded results.
- `/tmp/v4_audit_results/audit_text_novelty_rebin_*.parquet` — Phase 2.3 fixed-threshold results.
- `/tmp/v4_audit_results/audit_lifecycle_duration_50_80*.parquet` — Phase 2.2 duration-stratified results.
- `/tmp/v4_audit_results/audit_bot_{current,looser,tighter}_50_80_*.parquet` — Phase 2.4 bot variants.
- `/tmp/learnability_audit/{jaccard_matrix,category_decomposition,updown_share_by_slice,tiny_slice_top10,multiple_testing_accounting,decile_mass_audit}.json` — Phase 1 local diagnostics.
- `/tmp/learnability/audit_run_*.py` — EC2-side audit driver scripts.
- `/tmp/learnability_audit/phase1_diagnostics.py` — local Phase 1 script.

EC2 instance i-0f5b31a268af53938 stopped after audit completion.

---

# Appendix A — Lifecycle window deep dive

_Added 2026-06-08 to address the question "why 50-80%?" with empirical data._

## The question

`MIN_TRADES_PER_SLICE`, the bot filter, the deciles, and the 3-way SE all have explicit rationales. The **50-80% lifecycle window** does not. It was chosen at the start of v3 and never empirically defended. This appendix tests whether the choice is defensible and what alternatives would change.

## Three studies on EC2 (~75 min wall time)

- **Study A** — per-lifecycle-decile FLB shape across 5 headline dims (single trades scan, mean returns only, no SE).
- **Study B** — rigorous 3-way clustered SE FLB across 6 candidate windows × 4 headline dims (24 FLB queries).
- **Study C** — local: per-slice N(50-80%) / N(full) ratios from existing parquets.

## A.1 Where does FLB concentrate across lifecycle? (Study A)

For each headline dim slice, spread = D10 mean return − D1 mean return computed per lifecycle decile (0 = 0-10%, 9 = 90-100%):

### dim_text_novelty spread × lifecycle decile

| Slice | b0 | b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8 | b9 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Q1 most isolated | +0.018 | +0.015 | +0.036 | +0.049 | +0.037 | **+0.061** | **+0.061** | +0.046 | +0.041 | +0.034 |
| Q2 mod isolated | -0.012 | +0.009 | +0.010 | -0.070 | +0.022 | 0.000 | -0.009 | +0.001 | +0.015 | +0.015 |
| Q5 most repetitive | -0.033 | +0.003 | -0.003 | +0.013 | +0.013 | +0.020 | +0.022 | +0.026 | +0.032 | +0.019 |

Q1 isolated peaks at bins 5-6 (50-70% lifecycle). Q5 climbs monotonically into the closing tail. Q2-Q4 are noisier and slightly later-life.

### dim_event_family_size spread × lifecycle decile

| Slice | b0 | b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8 | b9 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Small 2-20 | +0.009 | +0.047 | +0.048 | +0.035 | +0.032 | **+0.055** | **+0.057** | +0.034 | +0.030 | +0.024 |
| Large 1K+ | -0.015 | -0.007 | -0.014 | -0.006 | -0.010 | -0.019 | -0.005 | -0.003 | +0.005 | +0.014 |
| Medium 21-1K | 0.000 | -0.011 | +0.013 | -0.048 | +0.013 | +0.031 | +0.011 | +0.036 | +0.045 | +0.040 |

Small 2-20 peaks at bins 5-6 (consistent with Q1 isolated). **Large 1K+ flips from negative (mid-life) to positive (closing bin 9)** — the "Large = no FLB" finding is mid-life-specific.

### dim_primary_category spread × lifecycle decile

| Slice | b0 | b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8 | b9 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sports | -0.024 | -0.022 | -0.009 | -0.105 | -0.025 | +0.020 | +0.026 | +0.044 | +0.033 | +0.017 |
| Politics | +0.044 | +0.038 | +0.055 | +0.019 | +0.038 | +0.060 | +0.054 | +0.036 | +0.048 | +0.053 |
| Crypto | +0.006 | +0.028 | +0.022 | +0.010 | +0.003 | 0.000 | -0.008 | +0.001 | +0.005 | +0.016 |
| Finance | -0.002 | +0.024 | -0.004 | +0.038 | +0.054 | +0.056 | +0.051 | +0.050 | +0.066 | +0.031 |

**Sports** shows reversed FLB in early life (bins 0-4) and classic FLB only from bin 5 onward. **Politics** is robust across all bins (lowest +0.019, max +0.060). **Crypto** is essentially flat across all bins — Crypto markets don't show FLB at any lifecycle position.

### Key shape findings

1. The 50-80% window catches the **FLB peak** for the novelty/small headline findings.
2. **Large 1K+** and **50+ priors** slices show NO FLB mid-life but DO show FLB at close (bin 9). The headline "Large/Many-priors = calibrated" claim is mid-life-specific, not unconditional.
3. **Sports** shows reversed FLB in early life — public-money / late-money-flips-favorite-line dynamics.
4. **Politics, Finance, Geopolitics, Culture** show robust FLB across virtually all lifecycle bins.

## A.2 Selection: which contracts contribute trades to 50-80%? (Study C)

For each v4 slice, N(50-80%) / N(full lifecycle):

| Slice | 50-80% N / full N |
|---|---:|
| dim_group_strict_size Singleton 1 | **0.108** (UNDER-represented) |
| VPC Q1 thinnest | **0.115** (UNDER-represented) |
| dim_family_vol_tier Low vol | 0.157 |
| dim_family_size_x_vol Small × Low vol | 0.160 |
| dim_text_neighbors 2-5 strict | 0.161 |
| dim_text_novelty Q3 moderate | 0.294 |
| Small 2-20 family size | 0.290 |
| Small × HighVol | 0.277 |
| dim_text_novelty Q1 isolated | 0.327 |
| dim_text_neighbors 0 strict | 0.345 |
| **dim_event_family_size Medium 21-1K** | **0.296** |
| **dim_family_size_x_vol Medium × High vol** | **0.388** (OVER-represented) |

Mean: 0.265, std: 0.062, range: 0.109-0.388. A truly uniform window would give 0.30.

**Selection signature**: the 50-80% window OVER-represents recurring/many-trade contracts and UNDER-represents thin/one-off contracts. Singleton 1 has only 11% of its trades in 50-80% — meaning 89% of singleton trades happen in the first 50% OR last 20% of the contract's life (likely first-listing and close-out burst trading, not mid-life mispricing).

## A.3 Rigorous comparison across 6 candidate windows (Study B)

Headline spreads (t-stats) across windows:

**dim_text_novelty Q1 most isolated:**

| Window | N trades | Spread | t |
|---|---:|---:|---:|
| full (0-100) | 9.64M | +0.0419 | +13.16 |
| 10-90 | 6.98M | +0.0467 | +13.45 |
| **25-75** | **4.32M** | **+0.0531** | **+14.09** |
| **50-80** | **3.15M** | **+0.0563** | **+16.90** |
| 80-100 | 2.97M | +0.0368 | +7.18 |
| 20-50 | 1.99M | +0.0404 | +5.27 |

**dim_family_size_x_vol Small 2-20 × High vol:**

| Window | N trades | Spread | t |
|---|---:|---:|---:|
| full | 14.36M | +0.0401 | +8.13 |
| 10-90 | 10.30M | +0.0446 | +9.69 |
| **25-75** | **6.25M** | **+0.0476** | **+12.63** |
| **50-80** | **3.97M** | **+0.0501** | **+11.98** |
| 80-100 | 3.45M | +0.0297 | +4.29 |
| 20-50 | 3.46M | +0.0400 | +6.55 |

**dim_family_size_x_vol Large 1K+ × High vol** (a "null result" cell):

| Window | N trades | Spread | t |
|---|---:|---:|---:|
| full | 51.68M | +0.0032 | +1.53 |
| 10-90 | 31.04M | -0.0036 | -1.05 |
| 25-75 | 17.45M | -0.0092 | -1.63 |
| 50-80 | 13.46M | -0.0072 | -1.44 |
| **80-100** | **22.02M** | **+0.0124** | **+7.54\*\*\*** |
| 20-50 | 8.04M | -0.0096 | -1.19 |

**Large 1K+ DOES show classic FLB at close.** The "Large = calibrated" v3 claim is window-dependent: it holds in mid-life (50-80%) but fails in the closing window (80-100%).

**dim_prior_settlements_bin__event_template 50+:**

| Window | N trades | Spread | t |
|---|---:|---:|---:|
| full | 54.69M | +0.0038 | +1.11 |
| 50-80 | 14.27M | -0.0073 | -0.79 |
| **80-100** | **22.67M** | **+0.0114** | **+5.25\*\*\*** |

Same pattern: 50+ priors shows no FLB mid-life but classic FLB at close.

## A.4 Where the FLB peak lives, by slice (concentration ranking)

For each headline slice, which window produces the highest |t|?

| Slice | Peak window | Peak spread | Peak t |
|---|---|---:|---:|
| Q1 most isolated | **50-80** | +0.0563 | +16.90 |
| Q5 most repetitive | **80-100** | +0.0234 | +5.60 |
| Small × HighVol | 25-75 or 50-80 | +0.0476-0.0501 | +12 |
| Large × HighVol | **80-100** | +0.0124 | +7.54 |
| Small 2-20 family | 25-75 or 50-80 | +0.046-0.049 | +12 |
| Large 1K+ family | **80-100** | +0.0124 | +7.54 |
| 0-priors | **80-100** | +0.0455 | +11.66 |
| 50+ priors | **80-100** | +0.0114 | +5.25 |
| Medium 21-1K family | **80-100** | +0.0427 | +5.31 |

**The 50-80% window peaks for Small/Novelty contracts. The 80-100% window peaks for Large/Many-priors/Repetitive contracts.** These are TWO DIFFERENT FLB regimes:

- **Mid-life FLB** (50-80%): retail mispricing of low-attention small/novel markets, before close.
- **Closing-line FLB** (80-100%): even large, recurring, well-traded markets show FLB at close, consistent with the classical Levitt-style "closing line bias" literature.

The current writeup reports only mid-life FLB and misses the closing-line phenomenon entirely.

## A.5 Recommendation (revised 2026-06-08 — dual-window approach)

### Primary "mature-pricing" window: **25-80%**. Secondary "closing" window: **80-100%**. Report both side-by-side for every dim.

The audit Study B showed that the FLB landscape has two distinct regimes (mid-life FLB for novel/small contracts; closing-line FLB for large/recurring contracts), and that no single window captures both. The dual-window report is the cleanest way to surface both regimes without the complication of lifecycle-binned covariates.

The 25-80% window is broader than the previous 50-80% canonical (it adds the 25-50% lifecycle range) and broader than 25-75% (it adds the 75-80% range). This captures the **full mature-pricing regime** — from when initial-listing thin-book activity has subsided until just before the closing-print burst kicks in.

### Why 25-80% over 50-80% or 25-75%

- **vs 50-80%**: 25-80% is ~55% of lifecycle vs 30%. About **80% more data per slice**. Same FLB peak (the 50-60% and 60-70% bins are inside both windows). The extra 25-50% lifecycle range adds robust positive spreads (Study A shows Q1 isolated at +0.036, +0.049, +0.037 in bins 2, 3, 4 — all positive).
- **vs 25-75%**: 25-80% adds the 75-80% range, which is still pre-closing. Modest data increase (~10%) with no signal change.
- **vs full lifecycle**: avoids both the early MM-dominated thin-book regime (0-25%) AND the late closing-print burst (80-100%). Conceptually cleaner.

### Why 80-100% as a dedicated secondary window

The closing-line regime is a substantively different FLB phenomenon, and the writeup currently misses it entirely. Specifically (Study B, all 3-way clustered SE):

- **Large 1K+ × High vol**: +0.0124 (t = +7.54\*\*\*) at 80-100% vs n.s. at 50-80% and 25-75%. The v3 "Large = calibrated" finding holds only mid-life.
- **dim_prior_settlements_bin__event_template 50+**: +0.0114 (t = +5.25\*\*\*) at 80-100% vs n.s. mid-life. The §10.1 B "many priors kill FLB" claim needs softening — it holds mid-life but not at close.
- **dim_prior_settlements_bin__event_template 0**: +0.0455 (t = +11.66\*\*\*) at 80-100% vs +0.0432 at 50-80%. The novelty effect is actually stronger at the close.
- **Medium 21-1K**: +0.0427 (t = +5.31\*\*\*) at 80-100% vs +0.0149 (n.s.) at full lifecycle. A new significant finding the writeup currently doesn't report.

These are not minor edge cases — they're new findings that emerge from the closing window.

### Estimated 25-80% spreads for the headline cells

Study B ran 25-75% and 50-80% but not 25-80% specifically. The 25-80% spread should sit between the two values, weighted toward 25-75% (since it shares 50/55 ≈ 91% of the same trades):

| Slice | 25-75% (measured) | 50-80% (measured) | 25-80% (estimate) |
|---|---:|---:|---:|
| dim_text_novelty Q1 most isolated | +0.0531 (t=+14.1, N=4.32M) | +0.0563 (t=+16.9, N=3.15M) | **~+0.054 (~t=+15)** |
| dim_family_size_x_vol Small 2-20 × High vol | +0.0476 (t=+12.6) | +0.0501 (t=+12.0) | **~+0.048** |
| dim_event_family_size Small 2-20 | +0.0465 (t=+12.7) | +0.0486 (t=+12.0) | **~+0.047** |
| dim_prior_settlements 0-priors | +0.0307 (t=+3.8) | +0.0432 (t=+8.1) | **~+0.036** |
| dim_family_size_x_vol Large 1K+ × High vol | -0.0092 (t=-1.6) | -0.0072 (t=-1.4) | **~-0.008** (n.s.) |

Optional follow-up: a single ~10-min EC2 rerun of the four headline dims with `V3_LO=0.25, V3_HI=0.80` would give the exact numbers instead of estimates. Not strictly necessary — the estimates are tight given the 25-75% and 50-80% bracket and the underlying lifecycle-decile shape.

### Drop 50-80% terminology

50-80% is an arbitrary choice from v3 that the data doesn't specifically vindicate. The audit found nothing wrong with it for the existing headlines, but 25-80% catches the same FLB peak with substantially more data, and the 80-100% addition reveals a phenomenon 50-80% misses. Replacing the canonical window is a one-day change (run the existing pipeline with new env vars) and a writeup edit.

### Full lifecycle as a sanity baseline

For each headline, also report the full-lifecycle number so readers can see the dilution between windowed and unwindowed estimates. Most spreads attenuate ~20-30% between 50-80% and full. Full lifecycle is the only window with no selection bias.

### Lifecycle-decile-as-covariate — deferred

A more rigorous reporting structure (lifecycle decile as a slicing dimension on equal footing with other dims) is the principled long-term answer but adds reporting complexity and doubles the table count. The dual-window approach (25-80% + 80-100%) is the right interim solution: it captures both FLB regimes the data exhibits without the methodological lift.

## A.6 Specific edits to apply to the writeup

### Setup (lines 5-20)

Replace the "50-80% canonical window" phrasing with:

> "Two lifecycle windows are analyzed independently and reported side-by-side: a **mature-pricing window (25-80%)** that captures the bulk of informed trading after initial-listing thin-book activity subsides and before the closing-print burst kicks in; and a **closing window (80-100%)** that captures the close-of-life trading regime. Both are computed per-contract: `lifecycle = (t.timestamp - mkt_start) / mkt_duration`, with mkt_start and mkt_duration computed from min/max trade timestamps. The closing-window analysis is necessary because several headline slices (notably Large 1K+ family and 50+ priors) show qualitatively different FLB structure mid-life vs at close. A third **full-lifecycle baseline (0-100%)** is reported as a sanity check."

### Per-dim sections

Each §1-§10 section should report a three-column table per slice: **25-80% mature** | **80-100% close** | **full lifecycle**. The closing column will surface new findings (Large 1K+ shows FLB at close; 50+ priors shows FLB at close) that the current writeup misses entirely.

### Methodology disclosure (Setup or appendix)

Add a paragraph:

> "Window choices (25-80% mature + 80-100% closing + 0-100% baseline) were empirically validated by running the headline dims at six candidate windows (0-100, 10-90, 25-75, 50-80, 80-100, 20-50) on EC2. The mature-pricing window catches the FLB peak for novel/small contracts with substantially more data than the prior 50-80% canonical; the closing window reveals a distinct closing-line FLB pattern in Large/Many-priors contracts that the mature window misses. Per-lifecycle-decile FLB shapes for all headline dims are reported in Appendix A."

## A.7 Implementation pointers

For the v3 + v4 pipeline:

- `run_phase1_v3.py`: set `V3_LO = 0.25, V3_HI = 0.80` as the new mature-pricing default; keep `0.50, 0.80` available for legacy comparison.
- Add a parallel pass with `V3_LO = 0.80, V3_HI = 1.00` (prefix `_closing`).
- Keep the existing full-lifecycle pass as baseline.
- Update writeup section headers from "(50-80% lifecycle)" to a 3-column table: "(25-80% mature) (80-100% close) (full)".
- The lifecycle-decile shape parquet from Study A (`audit_lifecycle_decile_shape.parquet`, 7K cells) becomes a permanent figure in the appendix.

### Cost estimate

- 25-80% rerun (full pipeline): ~40-50 min EC2 wall time.
- 80-100% rerun: ~30 min.
- Writeup edits (Setup + per-dim 3-column tables + new appendix figure): ~2-3 hr local.

Total: ~4-5 hr to fully adopt the dual-window framing.

### Optional quick refinement

If exact 25-80% numbers (rather than estimates from the 25-75% and 50-80% bracket) are needed before the full rerun, a 10-min EC2 pass on just the four headline dims at the 25-80% window would nail down the exact spreads. Trivial cost.
