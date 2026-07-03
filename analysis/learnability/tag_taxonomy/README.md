# Tag taxonomy — native Polymarket tags → market categories

Canonical home of the tag→category taxonomy used for per-market labeling
(replaces the LLM contract classification for category slicing). Built
2026-06-28, baked 2026-07-01, repo-ified 2026-07-03.

## Files

| File | What |
|---|---|
| `final_tag_map_v1.json` | **Authoritative baked map** (1,406 tags → `prim_cat` + `excluded`). Copy of `/mnt/data/learnability/native/final_tag_map_v1.json`. Includes both round-2 fixes (Tweet Markets/Elon Tweets demoted; no Geopolitics precedence). |
| `curated_tags.json` | Curation provenance: the 300 top-tag hand-curation with per-tag notes from the multi-agent workflow (curate → subcategories → adversarial-validate → synthesize). **Caution:** a few `fixed:true` entries carry reviewer *instructions* in the `primary` field, not category names — the authoritative resolution of every tag is `final_tag_map_v1.json`, never this file. |
| `subcategories.json` | The designed two-level taxonomy: ~35 FLB-motivated subcategories across 11 primaries, each with definition + FLB rationale + member tags. Input to the labels-v2 subcategory vote. |
| `overrides_v2.json` | Explicit v2 semantic changes vs v1 (Iran topic/event-family split, Mentions precedence retained) + documented resolutions of every instruction-valued curated entry + open questions for the hand-lock. |

## How per-market assignment works (v1 mechanics)

IDF-weighted **soft vote** over a market's non-excluded tags: each tag carries
`P(category | tag)` learned on the ~619K markets with both native tags and an
LLM label; rarer tags weigh more (`ln(N/df)`); the top-scoring category wins.
Tag-precedence overrides applied after the vote: `Mentions` tag → Mentions.
Tags outside the curated 300 ride an auto majority-vote prior (unaudited tail).

## v2 semantics (user-decided 2026-07-03)

- **Iran** is NOT a topic: Iran-cluster tags vote **Geopolitics**; the cluster
  instead defines `event_family = 'Iran'` in a separate column.
- **Mentions** stays a category (a coherent market type: "will X say ___"),
  and `mechanic = 'mentions'` is also set.
- Ambiguous markets get labeled best-effort but carry `vote_margin` and an
  `abstain` flag (margin below threshold) so exclusion is a filter, not a
  rebuild.

## Validation history

- Held-out vs LLM labels (70/30): **98.7% count / 94.2% dollar-weighted**
  agreement. The gap between those two numbers = disagreement concentrates in
  high-volume markets → hence the top-market hand-lock.
- Market-level adversarial hand-checks: round 1 **79/6000 (1.3%)** errors
  (dominated by Mentions-tag markets losing the vote) → precedence fix →
  round 2 **19/6000 (0.3%)**; residual is genuine ambiguity.
- Soft vote beat hard one-primary-per-tag assignment (97.1/89.2).

## Known open questions (route to the hand-lock / stress test)

- **Fed / Fed Rates → Politics** in the baked map; the curation notes argued
  Economy (monetary policy ≠ nomination politics). High-volume; hand-lock decides.
- `Business` tag → Finance but spans Economy/Tech/Politics in samples.
- Esports events that Polymarket tags `Sports`; IPO markets (Tech vs Finance);
  crime (Culture vs Other) — the irreducible ~0.1–0.3%.

## v2 validation (2026-07-03)

- **Head (hand-lock)**: 703 top-volume markets (top-400 trade dollars + top-200
  open + top-200 closed by Gamma volume) — blind classification (29 agents, no
  anchoring) vs the tag vote: **94.2% topic agreement (95.2% dollar-weighted)**;
  41 disagreements + 31 ambiguous adjudicated by 3-judge panels citing border
  rules R1–R9 (70/72 unanimous) → `locked_overrides.json`, applied at bake time.
- **Tail (adversarial audit)**: 500-market sample (250 dollar-weighted + 250
  uniform, non-locked): **456 correct / 27 debatable / 15 subcat wrong / 2 topic
  wrong (0.4%)**. Systematic finds, all fixed in the same day: `hype` tag
  misrouted 44,100 Hyperliquid price markets into Listings/FDV (moved to Coin
  Price Targets); Mentions-precedence markets inherited the raw vote topic
  subcategory (subvote now uses the final topic); sports non-game markets
  (free agency/drafts/awards) now route to "Props, Drafts & Fantasy" via
  question patterns when sportsMarketType is absent.
- Known conventions (deliberate, not bugs): up/down direction markets carry the
  asset-class price subcategory + `mechanic=updown`; Elon tweet counts follow
  the Tech "Elon Musk & Social Platform Behavior" bucket; tennis tournament
  progression sits in the tennis H2H bucket.
