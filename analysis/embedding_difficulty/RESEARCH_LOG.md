# Embedding-based intrinsic difficulty — research log

Running log of everything tried in this workstream, in order, including dead ends.
Newest entries at the bottom. Findings are recorded neutrally (measured, not presumed).

**Question.** Can we measure how *intrinsically difficult to price* a market is, at finer
and more honest granularity than curated categories, using text embeddings of the market's
question/rules? Difficulty proxy: favorite-longshot bias (FLB) per slice — measured with the
project-standard filters and the signed calibration slope (see `docs/methods_reference.md`).

**Design stance.** This workstream is deliberately designed from first principles — it does
not reuse the prior learnability dimension definitions. It shares only: the canonical trade
set, the standard trade filters, the calibration measurement spec, and the EC2 workflow.

---

## 2026-07-03 — Session 1: design + infrastructure

### Planned approaches (in intended order)

| # | Approach | Idea | Status |
|---|---|---|---|
| A | Full-question embedding → PCA | Embed `question` (+ rules text) for every market; inspect principal components; test whether FLB varies systematically along components | planned |
| B | Precedent density / novelty-at-birth | For market m created at t_m, similarity to markets created before t_m (kNN distance / neighbor count). "Many close precedents" = learnable; "novel" = difficult. Time-honest by construction | planned |
| C | Multi-granularity clustering | k-means at several k (coarse→fine); FLB per cluster; how much FLB dispersion exists across clusters at each granularity, vs. what categories capture | planned |
| D | Action/subject decomposition | Parse question into subject vs. action (LLM or structural); embed separately; does difficulty live in the action, the subject, or the pair | later phase |
| E | Supervised probe | Predict slice-level miscalibration from embedding features out-of-sample; how much difficulty signal does text carry at all | candidate |

### Pre-registered measurement decisions (before seeing any results)

- FLB per slice = project-standard: BUY-side, 0.01<price<0.99, up/down + bot exclusion,
  ≥5,000 trades per slice, signed calibration slope primary (D10−D1 secondary only),
  count- and dollar-weighted, mature (25–80% lifetime) and closing (80–100%) windows.
- Novelty (approach B) is computed **only from markets created strictly before** the focal
  market's `createdAt` — no lookahead. PCA bases used in any time-sliced analysis are fit
  on pre-t markets only.
- Embedding text v1 = `question` alone; v2 = `question + " || " + description` (rules).
  Both are built so we can test whether rules text adds signal, rather than assuming it.
- No direction is presumed: we test whether FLB *varies* along each measure; sign and
  shape are outcomes, not priors.

### Known confounds to name (not silently absorb)

- Novelty correlates with time (early platform era = everything novel) → include
  market-vintage controls / within-era comparisons.
- Novelty correlates with category composition (recurring sports/crypto series are
  precedent-dense) → report novelty gradients within category as well as pooled.
- Liquidity/attention (volume, trader count) correlate with recurrence → report alongside,
  as named confounders, not as difficulty itself.
- Resolution censoring (see methods_reference): trade set contains only markets resolved by
  build time; any late-sample statement carries that caveat.

### Log

- [x] EC2 started; lit-review agent launched (FLB determinants, embedding novelty measures,
      PCA-on-embeddings pitfalls, forecast-difficulty literature).
- [x] Schema recon. Trades: `conditionId`=77-digit token id; standard-filter plumbing
      confirmed (wallet_flags.is_nonhuman, updown by eventSlug, start ts 1590969600).
      Canonical engine `flb_per_slice.py` == local v3 (same md5): deciles + D10−D1 with
      CGM 3-way SEs; **no signed-slope implementation exists in the repo** → implemented
      here (OLS of ret=won−price on price; slope>0 ⇔ classic FLB direction).
- [x] **Data-plumbing finding (matters beyond this workstream):**
      `market_resolutions_enriched.parquet` (947K tokens, Mar 2026) covers only ~49% of
      trade rows in the June-2026 extended trades_clean (1.04B rows / $33B unmatched; none
      of it updown). Fresh June-24 artifacts give full coverage:
      `pipeline/output/market_resolutions.parquet` (2,373,197 tokens = exactly the distinct
      tokens in trades_clean) + `/mnt/data/pipeline_data/token_map.parquet` (token→market,
      question) + `gamma_markets.parquet` (1.54M markets, question/description/tags).
      This workstream uses the fresh spine. Any other analysis on the extended trade set
      still joining through the enriched file silently halves its sample.
- [x] Universe v1 (stale spine) built, then **rebuilt as v2 on the fresh spine**
      (`build_universe.py`). v1 numbers (for the record): 520,058 markets, 100% text.
- [x] Lit review returned. Design adoptions:
      encoder = BAAI/bge-small-en-v1.5 (contrastive → cosine meaningful; skip whitening),
      novelty = mean cosine similarity to k nearest predecessors, k ∈ {1,5,25}, plus
      threshold-count and percentile variants; strict backward discipline (pre-t corpus
      only, KPSS-style); report novelty pooled AND excluding same-series neighbors;
      confound regressions (question length, template/series, category, vintage, volume);
      hubness diagnostics; PCA centered with top-PC suspicion (Mu et al. 2018).
      Cited gaps we can own: recurrence-vs-FLB untested; embedding-novelty as
      forecastability measure unused; reference-class availability never operationalized.
      Positioning: reconcile with Reichenbach & Walther (no aggregate FLB claim) — ours is
      a heterogeneity design; Ottaviani-Sørensen (2010) info-to-noise as theory anchor.
- [x] **Second data-plumbing finding (severe): the standard up/down trade filter is
      broken on the June-2026 extended trades_clean.** The `eventSlug` column is the
      EMPTY STRING for newer markets (event-slug-map gap in the refresh), so
      `eventSlug NOT LIKE '%updown%'` catches only 33K rows — while Gamma metadata shows
      **415,991 up/down markets carrying 1.34B raw rows (66% of the entire clean set)**
      and 135M standard-filtered BUY trades (more than the 115M from all real markets).
      Any standard-filter run on the extended set is currently updown-poisoned.
      Fix here: up/down flagged at MARKET level from Gamma event_slug/series_slug/question
      patterns (`build_universe.py`), inherited by the FLB base via universe_tokens.
- [x] Universe v3 (final): **850,015 non-updown markets**, 100% question+description,
      99.0% created_at (fallback = first trade, flagged), 521,240 with ≥1 filtered trade.
      Artifacts: `/mnt/data/embedding_difficulty/universe_markets.parquet`, `universe_tokens.parquet`,
      `build_universe_coverage.json`.
- [x] `build_flb_base.py` (fresh spine + market-level updown exclusion): 145.3M filtered
      BUY rows → mature (25–80%) 37.7M, closing (80–100%) 66.3M. Integer-coded compact
      tables; every slicing scheme joins these instead of re-scanning 2B rows.
- [x] Known caveat logged: wallet_flags built 2026-06-11 — bot flags may under-cover
      wallets first active after the June data extension; new-era bot share unaudited here.
- [ ] Embed universe (bge-small-en-v1.5, q and qd variants) — q running (~590 texts/s);
      analysis chain armed to fire on completion (run_chain.sh → PCA, clustering,
      novelty, diagnostics, FLB mature all schemes + closing key schemes).
- [x] Engine validated end-to-end on baseline schemes (mature window, 37.7M trades,
      5.7 min for 3 schemes). First numbers (neutral record):
      pooled ALL slope −0.0004 (t≈0.0), dollar-weighted −0.0330 (t=−1.3) — the aggregate
      sample is ≈ calibrated, consistent with recent Polymarket literature; heterogeneity
      is the live question. in_series slope +0.0001 (t≈0) vs standalone −0.0010 (t=−0.1);
      dollar-weighted in_series −0.0390 (t=−2.0), standalone −0.0063 (t=−0.1).
- [x] scheme_category v1 used native `category` field — 99.99% UNKNOWN (field sparse).
      Replaced with curated tags→category map (`market_native_categories.parquet`,
      12 primary categories) inside run_chain.sh.
- [x] **Approach D built** (`make_actsubj_slices.py`): action/subject precedent counts
      from Stage-2 labels (raw labels reused as data, not as approach). Coverage caveat:
      379K/850K markets, 59% of filtered trades, labels stop at the pre-June universe.
      Distribution: action_prec ≥1000 for 65% of labeled markets; action_prec=0 for only
      6K; 2×2 cells: bothKnown 343K, actNew_subjKnown 15K, actKnown_subjNew 13K,
      bothNew 7.3K. Precedent is the norm; novelty is the thin tail.
- [x] PCA (approach A) structure: EVR = 12.4/5.9/5.1/3.8/3.3% for PC1–5 — no single
      dominant axis. PC1–PC2 correlate with question length (+0.38 each) and PC2 with
      log trades (+0.34) / series membership (−0.34): the known artifact/attention mix.
      PCs will be interpreted only jointly with the confound table.
- [x] k-means (approach C) at k=12 recovers clean semantic families with no labels
      (sports outcomes, crypto price levels, Trump/Musk social, esports, weather,
      player props) — embedding space is meaningful at coarse granularity.
- [x] **First approach-D gradient (mature, count-weighted, CGM 3-way t):**
      action_prec=0 slope +0.0700 (t=+2.7**), 10–99 +0.0378 (t=+2.5*), 1–9 +0.0274
      (t=+1.1), 100–999 −0.0069 (t=−0.3), ≥1000 −0.0035 (t=−0.4). FLB-direction
      miscalibration in never-seen action types, ≈calibrated once the action type is
      heavily precedented. 2×2: actKnown_subjNew +0.0481 (t=+2.3*) vs bothKnown +0.0039
      (t=+0.3) [actNew cells pending]. Dollar-weighted ≥1000 slope −0.0560 (t=−3.1):
      big-money trades in precedented markets lean the OTHER way. Confound caution:
      act_0 markets skew early-vintage → added `scheme_act_prec_vint` (within-birth-year
      quintiles) as the vintage-controlled check.
- [x] Novelty τ calibration: τ=0.8716 (99.5th pct of random pairs; median random-pair
      cosine 0.53 — bge sims are shifted, so percentile-based τ, not absolute).
- [x] Novelty computed: 138 min for 850,015 markets (chronological exact GEMM kNN).
      **Hubness caveat:** k-occurrence skew 91 (max: one market is a top-25 neighbor of
      9,256 others) — dense template families; deciles are rank-based which softens but
      does not remove it. Robustness TODO: mutual-proximity/local-scaling rescale.
- [x] Vintage-controlled action precedent (mature): q1 +0.0273 (t=+1.7) → q5 −0.0117
      (t=−0.6), monotone. Raw act_prec gradient partly vintage; within-year residual
      gradient remains, marginal significance.
- [x] k=12 × terms (mature): tennis/ITF −0.113 (t=−4.4***), esports maps −0.101
      (t=−4.6***), weather −0.033*** (dollar −0.091, t=−8.1) vs crypto price / social /
      mainstream sports ≈ 0. Pooled calibration masks opposite-signed family-level
      miscalibration (negative slope = longshots UNDERpriced there).
- [x] PC1 quintiles (mature): monotone −0.127 (t=−5.7***) → +0.010; PC4 similar milder.
      PC2/PC3 flat. Subject precedent flat (contrast with action precedent).
- [x] **Decision:** qd (question+rules) embedding variant killed at 34% — 4.3h ETA on CPU
      for a robustness artifact not used in session 1; deferred to a future session
      (consider GPU spot or API embeddings). emb_q.npy (question-only) is the session-1
      basis.
- [x] **Approach B results (mature, count-weighted; d01 = MOST novel):**
      nov_k25 d01 +0.0433 (t=+3.0**); nov_k25x d01 +0.0362 (t=+2.1*), d02 +0.0324 (t=+1.9);
      nov_k25x_vint (within-year) d01 **+0.0656 (t=+3.9***)** — all other deciles ≈ 0.
      nov_cnt (τ-neighbor counts) flat. Read: novelty is a TAIL effect — the ~10% most
      novel markets of each era carry classic-FLB miscalibration; having *no close analog*
      matters, the number of analogs does not. Consistent with the action-precedent
      gradient and with a reference-class-availability interpretation.
- [x] Closing window (80–100% lifetime): pooled ALL +0.0137 (t=+1.0) — calibrated.
      **PC1 gradient disappears at the close** (all quintiles ≈ 0 vs monotone −0.127→+0.010
      mature): position-in-question-space miscalibration is corrected as resolution nears.
      **Novelty tail persists at ~half strength:** novx_vint_d01 +0.0359 (t=+2.1*) count-
      weighted, +0.0509 (t=+2.1) dollar-weighted (mature was +0.0656, t=+3.9***).
- [x] Report rendered (`render_report.py`) → `/mnt/data/embedding_difficulty/report/`.

### Session 1 summary (what we learned)

1. **The pooled sample is calibrated** (slope ≈ 0 both windows) — mis-calibration on this
   platform is a heterogeneity phenomenon, not an aggregate one.
2. **Intrinsic-difficulty signal exists in question text.** Along the main embedding axis
   (PC1) miscalibration runs monotonically from −0.127 (t=−5.7***) to +0.010 (mature);
   fine clusters show significant slopes of BOTH signs (tennis −0.113***, esports maps
   −0.101***, weather −0.033*** vs crypto/social/sports-outcome ≈ 0). Curated categories
   are far coarser than the structure that exists.
3. **Precedent/experience matters in the way the learnability hypothesis suggests, but as
   a tail effect:** the ~10% most-novel markets of each era (no close analog among
   predecessors) show classic FLB (+0.066, t=3.9*** mature; ~half that closing); novel
   ACTION types show FLB (+0.070, t=2.7**) fading with precedent count, while subject
   familiarity is flat. It is the absence of any analog — not the count of analogs —
   that predicts miscalibration.
4. **Direction nuance:** the difficulty/novelty tail errs in the classic FLB direction
   (longshots overpriced); several precedent-dense recurring families (tennis, esports,
   weather) err the OPPOSITE way, strongest dollar-weighted. Pooled zero = offsetting
   structured biases.
5. Confounds handled: vintage (within-year deciles), same-series/template leakage
   (exclusion variant), length/attention (confound table in report). Open: hubness
   rescale, encoder robustness, qd text variant, lexical (TF-IDF) baseline,
   contract-level robustness, two-way FE difficulty measure, within-series designs.

## 2026-07-03 — Session 2: plumbing fixes shared, liquidity axis, report v2

- [x] **Shared plumbing fix committed:** `scripts/build_market_flags.py` →
      `/mnt/data/pipeline_output/market_flags.parquet` (token_id, market_id,
      winning_outcome, market-level `is_updown` from Gamma event_slug/series_slug/tags/
      question — no reliance on trades' eventSlug). Hard coverage check: 2,373,197 tokens,
      0 unmatched vs trades_clean; 828,816 up/down tokens flagged.
      `run_phase1.py` (and therefore `run_v7.py`, which delegates to it) patched to use it;
      smoke-tested (view construction + join). `docs/methods_reference.md` amended
      (canonical spine; up/down exclusion now market-level; eventSlug gotcha).
- [x] Liquidity axis design: proxy = market-level standard-filtered BUY dollars (volume,
      not book depth — native `liquidity` unreliable on closed markets; named honestly).
      Schemes: absolute tiers, within-birth-month quintiles (era-relative), pooled floors
      ($1k/$10k/$100k), novelty-within-year deciles rebuilt on ≥$10k subset, rolling-median
      rule (volume ≥ 25% of trailing-90-day median among viable markets; markets with no
      trailing window kept).
- [x] Volume distribution facts (junk-market intuition confirmed): 61% of trade-viable
      markets <$1k volume but ~4% of trades; $10k floor keeps 77K/521K markets and 80% of
      trades; rolling-median rule excludes ~34% of markets, stable across years.
- [x] **Liquidity FLB results (mature, count-weighted):**
      absolute tiers: <$1k +0.0984 (t=+32.1***), $1–10k +0.0304 (t=+7.6***),
      $10–100k +0.0083 (ns), $100k–1M −0.0035 (ns), ≥$1M −0.0436 (ns; dollar-weighted
      mid tiers ≈ −0.02…−0.035, t≈−2.9). Era-relative (within birth month): q1–q4 all
      +0.06…+0.09 (t 8–22***), top quintile −0.0216 (t=−1.8) — only the top-volume
      quintile of each era is calibrated.
- [x] **Floor sensitivity:** pooled slope −0.0004 (none) → −0.0063 ($1k) → −0.0151 ($10k)
      → −0.0236 ($100k), all ns — aggregate calibration robust to floors. Rolling-median
      25% rule: excludes 34% of markets but only 0.7% of trades; pooled −0.0012 (ns).
      Implication: floors are safe for inclusion policy; they change market counts, not
      trade-weighted conclusions.
- [x] **Novelty × liquidity:** novelty-within-year deciles rebuilt on ≥$10k markets:
      d01 +0.0629 (t=+3.1**), all other deciles ≈ 0 — the novelty tail SURVIVES a
      liquidity floor; it is not thin-market noise.
- [ ] Report v2 render → pull, commit, stop instance.

## 2026-07-03 — Session 3: multi-field embeddings (question / rules / context)

- Design (pre-registered before results): three native text fields per market —
  question, RULES (= market `description`, resolution criteria), CONTEXT (= event-level
  `event_description` from the native pull; the closest native field to "market context";
  NOT Polymarket's newer AI context cards, which are in no API pull we hold).
  Each field embedded separately (bge-small); combined variants = per-market weight-
  renormalized sums of normalized field embeddings with FIXED weights: comb_eq (⅓,⅓,⅓)
  and comb_qc (0.45 q, 0.10 rules, 0.45 context — prior: rules least informative).
  Weights deliberately NOT tuned on FLB outcomes.
- Recon: rules 100% coverage, 539,308 unique texts; context 97.8% coverage, only
  **116,821 unique texts** (event-grain) → unique-dedup before encoding; 38% of markets
  have context ≡ rules (single-market events). Example split is as intended (rules =
  mechanical resolution clause; context = event information).
- Engineering: `compute_novelty.py` inner loop ported numpy→torch (GEMM/mask/topk all
  multithreaded; session-1 run spent ~2h in single-threaded masking). Port gated by a
  correctness check: torch rerun on emb_q must reproduce session-1 sim_k25_x (corr ≥
  0.999) before the four new variants run. `embed_fields.py` dedups unique texts and
  writes validity masks (empty fields → zero vector, excluded as focal & candidate).
- Execution notes: context embed 11.6 min (117K uniques — event-grain dedup 7×);
  rules embed ~75 min (539K uniques, 600-char truncation); torch novelty port
  **bit-identical to numpy (corr 1.000000, max diff 0)** at 29.8 vs 138 min (4.6×).
- **Bug caught & fixed:** chain's FLB stage requested schemes `nv_*_vint` but the slice
  maker wrote `scheme_nv_*` — run_schemes matched 0 schemes and the first v3 render
  silently skipped section 4b's panels. Names aligned; FLB + render rerun (ed_fixup.log).
  Lesson: run_schemes should FAIL on an empty scheme filter, not print "0 schemes" and
  exit 0 — fixed expectation noted for a future engine pass.
- **Cross-variant novelty agreement (Pearson, 509K viable markets):**
  q↔context **0.36**, q↔rules 0.58, rules↔context 0.49, q↔comb_eq 0.77, q↔comb_qc 0.81,
  comb_eq↔comb_qc 0.97. The three fields measure substantially different precedent
  structure — context is NOT interchangeable with the question (supports treating it as
  its own signal); the two pre-registered combined weightings are nearly equivalent.
- [x] **Per-variant novelty-tail FLB (mature, within-vintage d01 = most novel):**
      q (session 1): +0.0656 (t=+3.9***) | rules +0.0446 (t=+1.6), f10k +0.0549 (t=+1.7,
      dollar t=+2.0) | context +0.0355 (t=+1.1), f10k +0.0318 (t=+0.9) |
      comb_eq +0.0371 (t=+1.4) | comb_qc +0.0392 (t=+1.5); f10k variants similar.
      Read: every field's most-novel tail leans the same (classic-FLB) direction, but
      the QUESTION text carries the sharpest difficulty signal; combined weightings do
      not improve on question-only. Context novelty measures something real and distinct
      (q↔context corr 0.36) — but its distinct component does not sharpen the FLB tail.
      (User prior half-confirmed: question dominant — yes; context rivaling question as a
      difficulty signal — not in these data; rules weakest prior — actually the strongest
      of the three new variants.)
- [x] Report v3 rendered with section 4b (field coverage, agreement matrix, port check,
      per-variant d01 table + panels).

### Artifacts (this session)

- `/mnt/data/embedding_difficulty/`: universe_markets/tokens, flb_base_{mature,closing},
  code_maps, emb_q.npy + emb_ids, pca_{evr,scores,correlates}, novelty.parquet +
  neighbors_top25.npy + diagnostics, cluster_terms_k{12,50,200,1000}, schemes/ (20),
  output/ (flb_{deciles,summary,dropped} per scheme×window), report/.
- Local mirrors of the small summaries: `analysis/embedding_difficulty/output_session1/`.
