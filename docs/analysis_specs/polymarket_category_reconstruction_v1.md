# Polymarket category reconstruction v1

Status: candidate frozen on 26 October 2026, before real-data validation.
The user approved reconstruction and validation of the existing category scheme.
This specification admits a rule and synthetic fixtures, not refreshed labels or
a production sample. The [methods reference](../methods_reference.md) and
[final-sample refresh contract](polymarket_final_sample_refresh_v1.md) still apply.

## Purpose and evidence

Preserve every saved accepted category and assign categories only to new or
previously unmatched Gamma markets. Categories do not select or exclude trades.
The category order remains Crypto, Culture, Economy, Esports, Finance,
Geopolitics, Iran, Mentions, Politics, Sports, Tech, Weather, then Unclassified.
No outcomes, returns, prices, volume, or trade records enter this rule.

The original category scorer is lost within the bounded recovery scope. The
retained taxonomy README describes IDF-weighted soft category probabilities and
Mentions precedence. Its `overrides_v2.json` also documents a removed v1 override:
any tag containing `Iran` forced Iran. Their relative precedence is not recorded.
The retained map has 1,406 hard tag assignments/exclusions, not the original
probabilities or IDF state. Its exact SHA256 is
`06b72d8582f748fb658464dbba36edbbcc5321b43d6cbb3319b9e4689fe69f10`.
The first committed map (`42bec5401d815c628dc026f85733a4477b4f4e40`) is
byte-identical; consolidation did not remove richer state.

Evidence paths, relative to the accepted source bundle or canonical checkout:

- `archive/analyses/learnability_v1_v7/tag_taxonomy/README.md`, v1 mechanics.
- `archive/analyses/learnability_v1_v7/tag_taxonomy/overrides_v2.json`,
  `topic_changes._rule` and `precedence_retained.Mentions`.
- `archive/analyses/learnability_v1_v7/tag_taxonomy/final_tag_map_v1.json`.

The implementation is `scripts/reconstruct_polymarket_categories.py`. Its
`--describe-rule` output binds the exact candidate rule and map hash. This is a
transparent reconstruction, not reproduction of the lost soft/IDF scorer and
not adoption of the later v2 classifier, abstention threshold, or hand locks.

## Frozen rule

1. Join saved labels on the exact Gamma condition key. Preserve their raw `prim`
   value, including null and Unclassified, regardless of fresh tags. Absence of
   a saved row is the only condition permitting a reconstructed replacement.
2. For the independent candidate, accept a list of nonempty string tag labels;
   null/empty means no evidence and malformed values mean Unclassified. Use
   exact case-sensitive labels, without trimming or case folding. Duplicate
   labels count once, and input order has no effect.
3. The exact tag `Mentions` supplies Mentions override evidence. A tag containing
   the literal case-sensitive substring `Iran` supplies Iran override evidence,
   including an otherwise unmapped tag, following the documented any-tag rule.
   If both occur, return Unclassified with `override_conflict`; do not invent
   their relative precedence. If only one occurs, apply it before voting.
4. Each unique known, non-excluded tag contributes one vote to its retained
   `prim_cat`. Excluded and unknown tags contribute no vote and remain in the
   evidence record. A map-assigned Iran category is retained as Iran; it is not
   remapped to Geopolitics. Nonliteral Iran-cluster tags vote normally.
5. Without an override, select a unique category with the largest vote count
   (a plurality, not a majority requirement). Tied leaders return Unclassified
   with `vote_tie`; no votes return Unclassified with `no_voting_tags`.

Map schema violations, duplicate map labels, duplicate condition keys, and saved
category values outside the frozen labels/null are errors, even when duplicate
rows agree. These conditions cannot silently change votes or multiply joins.
Unknown fresh tags alone are not schema errors and are never learned/refitted.

## Input and output contract

Inputs to the pure, bounded metadata helpers are iterables of records:

| Input | Required fields | Grain |
|---|---|---|
| Fresh metadata | `condition_id`: nonempty string; `tags`: string list or null | Exactly one row per Gamma condition |
| Accepted labels | `mkt`: nonempty string; `prim`: frozen category, Unclassified, or null | Exactly one row per Gamma condition |
| Frozen map | `tag`: nonempty string; `prim_cat`: one of 12 categories or null; `excluded`: boolean | Exactly one row per exact tag label |

Map exclusion rows must have null `prim_cat`; non-excluded rows must have a
known category. The file loader checks exact map bytes against the frozen hash.
The fixture compiler accepts explicit small map records to test edge cases.
Additional metadata/label columns are ignored; the scorer consumes only the
fields named above. This helper does not infer identity or migration links.

Output has one row per union of accepted-label and metadata keys, sorted by
key. It retains `mkt`, `prim`, `label_source`, and `classification_reason`, plus
the independent candidate category, reason, votes, known voting tags, excluded
tags, unknown tags, and override evidence. Accepted conditions absent from
fresh metadata are retained with `metadata_missing` candidate evidence.
Null saved `prim` remains null; its effective comparison category is explicitly
Unclassified, matching the accepted input-builder convention.

The script has no data-processing CLI. `--describe-rule` only prints the rule
contract and hash. These in-memory helpers are for small metadata fixtures;
a separately reviewed, production-guarded, bounded EC2 adapter is required for
real metadata validation/materialization. Do not read production metadata or
trade bodies from the Mac or add a CLI guard bypass.

## Proposed metadata-only validation, not yet executed

Freeze source/map/rule hashes and explicitly inventory fresh metadata and the
saved `/mnt/data/learnability/native/market_native_categories.parquet` labels.
Declare the immutable output directory, read/row/memory caps and metadata vintage
before EC2 work. Use only condition keys, tags and saved labels, without trade,
outcome, price, return or embedding inputs. Reconcile all input keys, duplicates,
missing metadata, missing saved labels, malformed tags and category values.

Compute the candidate independently of the saved label on the entire admitted
historical metadata overlap, rather than measuring the trivial preservation
path. Report equal-market counts, the full 13-by-13 saved/candidate confusion
matrix, saved/candidate category counts, assigned/Unclassified coverage,
agreement overall and among assigned candidates, and disagreement examples.
Break these out by candidate reason and by tag evidence: numbers of known
voting/excluded/unknown tags, number of voting categories, and override status.
Retain unknown-tag identities/counts and separate the no-vote, tie, malformed,
missing-metadata and conflicting-override populations. The implemented summary
helper provides these counts for bounded metadata fixtures.

Hard gates are zero accepted-label changes (including nulls), one output row
per union key, no join fanout, no unaccounted rows, exact map/rule/source bindings,
and deterministic reopened outputs. Agreement with the old soft scorer is a
diagnostic, not expected identity and not a validation of missing original
weights. Do not manufacture a numeric agreement threshold after inspection or
tune this candidate against the same comparison. Any revised rule receives a
new version before another real-data comparison. Review disagreement and
coverage evidence before admitting reconstructed labels for new conditions.
