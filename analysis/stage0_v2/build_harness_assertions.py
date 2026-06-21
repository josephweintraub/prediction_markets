"""Extract representative real slugs from the existing dataset and bake them into
a regression-test harness JSON.

Two kinds of assertions:
  * COLLAPSE: a list of raw slugs that must all produce the same template
  * DISTINGUISH: a list of raw slugs that must all produce DIFFERENT templates

The slugs are pulled from the EXISTING per-contract parquet (which has both raw
slugs and the current templates) so the assertions encode "what already works."
A candidate normalizer must keep every assertion green.

We also bake in a small set of EXPECTED-IMPROVEMENT assertions: groupings the
current normalizer gets wrong that Phase A is intended to fix. Each has a
`current_status` field of "fails" so the harness can report progress on them
without treating them as regressions.
"""
import json
from pathlib import Path
import re
import pandas as pd

SRC = "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract.parquet"
OUT = Path(__file__).parent / "harness_assertions.json"

df = pd.read_parquet(SRC, columns=["event_template","market_template","event_slug","market_slug","question"])
print(f"loaded {len(df):,} rows")

# Drop nulls
df = df[df["market_slug"].notna() & df["market_template"].notna()]
print(f"  with non-null slug/template: {len(df):,}")

def sample_slugs_under_template(market_template, n=8, seed=1):
    """Get up to n raw market_slugs that currently map to this market_template."""
    sub = df[df["market_template"] == market_template]["market_slug"].drop_duplicates()
    return sub.sample(n=min(n, len(sub)), random_state=seed).tolist()

def sample_slug_per_template(market_templates, seed=1):
    """For each template in the list, pull one representative raw slug."""
    out = []
    for t in market_templates:
        slugs = df[df["market_template"] == t]["market_slug"]
        if len(slugs):
            out.append(slugs.sample(n=1, random_state=seed).iloc[0])
    return out

assertions = {"collapse": [], "distinguish": [], "expected_improvements": []}

# ===========================================================================
# COLLAPSE invariants: each list of slugs must produce a single template
# ===========================================================================

# --- crypto updown families (highest-volume / highest-stakes invariant) ---
for asset_prefix in ["btc-updown-<NUM>", "eth-updown-<NUM>", "sol-updown-<NUM>",
                     "xrp-updown-<NUM>", "doge-updown-<NUM>", "bnb-updown-<NUM>",
                     "hype-updown-<NUM>"]:
    slugs = sample_slugs_under_template(asset_prefix, n=12)
    if len(slugs) >= 2:
        assertions["collapse"].append({
            "name": f"crypto_updown_{asset_prefix.split('-')[0]}",
            "description": f"All {asset_prefix.split('-')[0]} up-down candle markets collapse to one template",
            "slugs": slugs,
        })

# --- bitcoin/ethereum/solana up-or-down (date + time) ---
for tmpl in ["bitcoin-up-or-down-<DATE>-<TIME>", "ethereum-up-or-down-<DATE>-<TIME>",
             "solana-up-or-down-<DATE>-<TIME>", "xrp-up-or-down-<DATE>-<TIME>"]:
    slugs = sample_slugs_under_template(tmpl, n=10)
    if len(slugs) >= 2:
        assertions["collapse"].append({
            "name": f"crypto_up_or_down_{tmpl.split('-')[0]}",
            "description": f"All {tmpl.split('-')[0]} up-or-down markets collapse",
            "slugs": slugs,
        })

# --- bitcoin/ethereum strike templates ---
for tmpl in ["bitcoin-above-<NUM>-on-<DATE>", "bitcoin-above-<NUM>-on-<DATE>-<TIME>",
             "ethereum-above-<NUM>-on-<DATE>", "solana-above-<NUM>-on-<DATE>",
             "xrp-above-<NUM>-on-<DATE>"]:
    slugs = sample_slugs_under_template(tmpl, n=10)
    if len(slugs) >= 2:
        assertions["collapse"].append({
            "name": f"crypto_strike_{tmpl.replace('<','').replace('>','').replace('-','_')}",
            "description": f"Strike-on-date markets collapse: {tmpl}",
            "slugs": slugs,
        })

# --- NBA / NFL / NHL / MLB canonical bet templates ---
for tmpl in [
    "nba-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline",
    "nba-<TEAM>-<TEAM>-<DATE>-<NUM>-total-<NUM>",
    "nba-<TEAM>-<TEAM>-<DATE>-<NUM>-spread-home-<NUM>",
    "nba-<TEAM>-<TEAM>-<DATE>-<NUM>-spread-away-<NUM>",
    "nfl-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline",
    "nhl-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline",
    "mlb-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline",
    "cbb-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline",
    "cfb-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline",
]:
    slugs = sample_slugs_under_template(tmpl, n=10)
    if len(slugs) >= 2:
        league, bet = tmpl.split("-<TEAM>-<TEAM>-<DATE>-<NUM>-")[0], tmpl.split("-")[-1]
        assertions["collapse"].append({
            "name": f"sports_{league}_{tmpl.split('-')[-2] if 'spread' not in tmpl and 'total' not in tmpl else '-'.join(tmpl.split('-')[-3:])}",
            "description": f"Same bet type across many matchups/dates collapses: {tmpl}",
            "slugs": slugs,
        })

# --- ESPORTS: cs2 / lol / val team-pair templates with game1 ---
for tmpl in [
    "cs2-<TEAM>-<TEAM>-<DATE>-<NUM>-game1",
    "lol-<TEAM>-<TEAM>-<DATE>-<NUM>-game1",
    "val-<TEAM>-<TEAM>-<DATE>-<NUM>-game1",
]:
    slugs = sample_slugs_under_template(tmpl, n=8)
    if len(slugs) >= 2:
        assertions["collapse"].append({
            "name": f"esports_{tmpl.split('-')[0]}_game1",
            "description": f"Esports game1 across many matches: {tmpl}",
            "slugs": slugs,
        })

# --- Fed-decision parametric: same action, varying bps and date ---
for tmpl in ["fed-decreases-interest-rates-by-<NUM>-bps-after-<DATE>-meeting",
             "fed-increases-interest-rates-by-<NUM>-bps-after-<DATE>-meeting"]:
    slugs = sample_slugs_under_template(tmpl, n=8)
    if len(slugs) >= 2:
        assertions["collapse"].append({
            "name": f"fed_{tmpl.split('-')[1]}",
            "description": f"Fed rate decisions across bps/months: {tmpl}",
            "slugs": slugs,
        })

# --- weather: same city / threshold across dates ---
weather_tmpls = (df[df["market_template"].str.startswith("highest-temperature-in-", na=False)]
                 ["market_template"].value_counts().head(8).index.tolist())
for tmpl in weather_tmpls:
    slugs = sample_slugs_under_template(tmpl, n=6)
    if len(slugs) >= 2:
        # extract city for naming
        city = tmpl.replace("highest-temperature-in-", "").split("-on-")[0]
        assertions["collapse"].append({
            "name": f"weather_{city}",
            "description": f"Weather threshold for {city} across dates collapses",
            "slugs": slugs,
        })

# --- Trump-say-X for SAME word across weeks (date should collapse but word stay) ---
trump_say_tmpls = df[df["market_template"].str.contains("trump-say|will-trump-say", regex=True, na=False)]["market_template"].value_counts().head(5).index.tolist()
for tmpl in trump_say_tmpls:
    slugs = sample_slugs_under_template(tmpl, n=6)
    if len(slugs) >= 2:
        # take last meaningful word as identifier
        last = re.sub(r'-?<[^>]+>-?', '', tmpl).strip('-').split('-')[-1]
        assertions["collapse"].append({
            "name": f"trump_say_{last}",
            "description": f"Same Trump-say word across dates collapses: {tmpl}",
            "slugs": slugs,
        })

# --- Stock-above-strike-on-date ---
for tmpl in (df[df["market_template"].str.match(r"(meta|tsla|aapl|nvda|googl|msft)-above-<NUM>-on-<DATE>", na=False)]
             ["market_template"].value_counts().head(5).index.tolist()):
    slugs = sample_slugs_under_template(tmpl, n=6)
    if len(slugs) >= 2:
        assertions["collapse"].append({
            "name": f"stock_strike_{tmpl.split('-')[0]}",
            "description": f"Stock strike-on-date collapses: {tmpl}",
            "slugs": slugs,
        })

# ===========================================================================
# DISTINGUISH invariants: each list of slugs must produce N distinct templates
# ===========================================================================

# --- presidential election candidates ---
pres_cands = (df[df["event_template"].str.contains("presidential-election-winner", na=False)]
              ["market_template"].drop_duplicates())
# Pick 10 distinct candidate templates
pres_sample = pres_cands.sample(n=min(10, len(pres_cands)), random_state=1).tolist()
slugs = sample_slug_per_template(pres_sample)
if len(slugs) >= 3:
    assertions["distinguish"].append({
        "name": "presidential_candidates_distinct",
        "description": "Distinct presidential election candidates produce distinct templates",
        "slugs": slugs,
        "min_distinct": len(slugs),  # all must be different
    })

# --- senate confirmations ---
senate_cands = (df[df["market_template"].str.contains("confirm|confirmed-as", regex=True, na=False) &
                   df["categories"].apply(lambda c: isinstance(c, (list,)) and "Politics" in c) if False else False] if False else
                df[df["market_template"].str.contains("confirm.*as", regex=True, na=False)]["market_template"].drop_duplicates())
senate_sample = senate_cands.sample(n=min(8, len(senate_cands)), random_state=1).tolist()
slugs = sample_slug_per_template(senate_sample)
if len(slugs) >= 3:
    assertions["distinguish"].append({
        "name": "senate_confirmations_distinct",
        "description": "Distinct nominees for confirmation stay distinct",
        "slugs": slugs,
        "min_distinct": len(slugs),
    })

# --- Mentions: different words for the SAME speaker ---
trump_say_tmpls_d = df[df["market_template"].str.contains("trump-say|will-trump-say", regex=True, na=False)]["market_template"].drop_duplicates()
ts_sample = trump_say_tmpls_d.sample(n=min(8, len(trump_say_tmpls_d)), random_state=2).tolist()
slugs = sample_slug_per_template(ts_sample)
if len(slugs) >= 3:
    assertions["distinguish"].append({
        "name": "trump_mentions_words_distinct",
        "description": "Different mentioned-words for Trump produce distinct templates",
        "slugs": slugs,
        "min_distinct": len(slugs),
    })

# --- Different speakers ---
speakers = []
for kw in ["trump-say", "starmer-say", "powell-say", "elon-musk", "mrbeast"]:
    tmpls = df[df["market_template"].str.contains(kw, na=False)]["market_template"].drop_duplicates()
    if len(tmpls):
        speakers.append(tmpls.iloc[0])
slugs = sample_slug_per_template(speakers)
if len(slugs) >= 3:
    assertions["distinguish"].append({
        "name": "different_speakers_distinct",
        "description": "Different speakers in Mentions produce distinct templates",
        "slugs": slugs,
        "min_distinct": len(slugs),
    })

# --- Different cryptos ---
crypto_tmpls = ["btc-updown-<NUM>", "eth-updown-<NUM>", "sol-updown-<NUM>", "xrp-updown-<NUM>", "doge-updown-<NUM>"]
slugs = sample_slug_per_template(crypto_tmpls)
if len(slugs) >= 3:
    assertions["distinguish"].append({
        "name": "different_cryptos_distinct",
        "description": "Different crypto assets produce distinct templates",
        "slugs": slugs,
        "min_distinct": len(slugs),
    })

# --- NBA bet types must remain distinct ---
nba_bet_tmpls = [
    "nba-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline",
    "nba-<TEAM>-<TEAM>-<DATE>-<NUM>-total-<NUM>",
    "nba-<TEAM>-<TEAM>-<DATE>-<NUM>-spread-home-<NUM>",
    "nba-<TEAM>-<TEAM>-<DATE>-<NUM>-spread-away-<NUM>",
]
slugs = sample_slug_per_template(nba_bet_tmpls)
if len(slugs) >= 3:
    assertions["distinguish"].append({
        "name": "nba_bet_types_distinct",
        "description": "NBA moneyline, spread, and total stay distinct",
        "slugs": slugs,
        "min_distinct": len(slugs),
    })

# --- Different leagues distinct ---
league_tmpls = ["nba-<TEAM>-<TEAM>", "nhl-<TEAM>-<TEAM>", "mlb-<TEAM>-<TEAM>", "nfl-<TEAM>-<TEAM>",
                "cbb-<TEAM>-<TEAM>", "cfb-<TEAM>-<TEAM>", "epl-<TEAM>-<TEAM>"]
slugs = sample_slug_per_template(league_tmpls)
if len(slugs) >= 3:
    assertions["distinguish"].append({
        "name": "different_leagues_distinct",
        "description": "Different sports leagues produce distinct templates",
        "slugs": slugs,
        "min_distinct": len(slugs),
    })

# --- Different weather cities distinct ---
weather_tmpls_d = df[df["market_template"].str.startswith("highest-temperature-in-", na=False)]["market_template"].value_counts().head(6).index.tolist()
slugs = sample_slug_per_template(weather_tmpls_d)
if len(slugs) >= 3:
    assertions["distinguish"].append({
        "name": "different_weather_cities_distinct",
        "description": "Different weather cities stay distinct",
        "slugs": slugs,
        "min_distinct": len(slugs),
    })

# --- Tournament-winner candidates (e.g., IEM Rio) ---
iem_tmpls = df[df["event_template"].str.contains("iem-rio", na=False)]["market_template"].drop_duplicates()
iem_sample = iem_tmpls.sample(n=min(8, len(iem_tmpls)), random_state=1).tolist()
slugs = sample_slug_per_template(iem_sample)
if len(slugs) >= 3:
    assertions["distinguish"].append({
        "name": "tournament_winner_candidates_distinct",
        "description": "Each tournament-winner candidate stays distinct",
        "slugs": slugs,
        "min_distinct": len(slugs),
    })

# --- Oscar best-picture nominees ---
oscar_tmpls = df[df["event_template"].str.contains("academy-awards|oscars", regex=True, na=False) &
                 df["market_template"].str.contains("best-picture|win-best-picture|nominated-for-best-picture", regex=True, na=False)]["market_template"].drop_duplicates()
oscar_sample = oscar_tmpls.sample(n=min(6, len(oscar_tmpls)), random_state=1).tolist()
slugs = sample_slug_per_template(oscar_sample)
if len(slugs) >= 3:
    assertions["distinguish"].append({
        "name": "oscar_best_picture_nominees_distinct",
        "description": "Oscar best-picture nominees stay distinct",
        "slugs": slugs,
        "min_distinct": len(slugs),
    })

# ===========================================================================
# EXPECTED IMPROVEMENTS: things the current normalizer gets wrong that
# Phase A is intended to fix. Each test reports current vs. desired behavior.
# ===========================================================================

# --- NBA 1H bets should be SEPARATE from full-game ---
# Pull current ML and 1H ML raw slugs (currently both map to same template)
ml_template = "nba-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline"
ml_slugs = df[df["market_template"] == ml_template]["market_slug"]
# Heuristic: 1H slugs have "-1h-" in them
ml_1h = ml_slugs[ml_slugs.str.contains("-1h-", na=False)].head(3).tolist()
ml_full = ml_slugs[~ml_slugs.str.contains("-1h-", na=False)].head(3).tolist()
if ml_1h and ml_full:
    assertions["expected_improvements"].append({
        "name": "nba_1h_separated_from_full_game",
        "description": "1H NBA moneyline must produce a DIFFERENT template from full-game moneyline (currently collapsed)",
        "kind": "distinguish_groups",
        "group_a": ml_1h,
        "group_b": ml_full,
        "current_status": "fails",
    })

# --- updown vs up-or-down typography: should collapse ---
btc_updown = sample_slugs_under_template("btc-updown-<NUM>", n=3)
btc_uord = sample_slugs_under_template("btc-up-or-down-<NUM>", n=3)
if btc_updown and btc_uord:
    assertions["expected_improvements"].append({
        "name": "btc_updown_typography_unified",
        "description": "btc-updown and btc-up-or-down typography variants should map to same template",
        "kind": "collapse",
        "slugs": btc_updown + btc_uord,
        "current_status": "fails",
    })

# --- valorant- prefix should alias to val- ---
val_short = sample_slugs_under_template("val-<TEAM>-<TEAM>", n=3) if (df["market_template"] == "val-<TEAM>-<TEAM>").any() else []
val_long_tmpls = df[df["market_template"].str.startswith("valorant-", na=False)]["market_template"].drop_duplicates()
val_long = sample_slug_per_template(val_long_tmpls.head(3).tolist()) if len(val_long_tmpls) else []
if val_short and val_long:
    assertions["expected_improvements"].append({
        "name": "valorant_alias_to_val",
        "description": "valorant- and val- prefix variants must map to same namespace",
        "kind": "collapse_namespace",
        "val_slugs": val_short,
        "valorant_slugs": val_long,
        "current_status": "fails",
    })

# --- dota- should team-collapse ---
dota_tmpls = df[df["market_template"].str.startswith("dota-", na=False)]["market_template"].drop_duplicates()
dota_sample = dota_tmpls.sample(n=min(6, len(dota_tmpls)), random_state=1).tolist() if len(dota_tmpls) else []
dota_slugs = sample_slug_per_template(dota_sample)
if len(dota_slugs) >= 3:
    assertions["expected_improvements"].append({
        "name": "dota_team_pair_collapse",
        "description": "dota- prefix should get team-pair <TEAM>-<TEAM> collapse like cs2/lol/val",
        "kind": "collapse_after",
        "slugs": dota_slugs,
        "current_status": "fails",
    })

# --- International soccer leagues should team-collapse (la liga, serie A, etc) ---
for prefix in ["lal-", "sea-", "mex-", "tur-", "por-", "j1100-", "den-", "chi1-", "kor-", "col-"]:
    tmpls = df[df["market_template"].str.startswith(prefix, na=False)]["market_template"].drop_duplicates()
    if len(tmpls) >= 3:
        s = sample_slug_per_template(tmpls.sample(n=min(5, len(tmpls)), random_state=1).tolist())
        if len(s) >= 3:
            assertions["expected_improvements"].append({
                "name": f"intl_soccer_{prefix.rstrip('-')}_team_collapse",
                "description": f"International league '{prefix.rstrip('-')}' should team-collapse to <TEAM>-<TEAM>",
                "kind": "collapse_after",
                "slugs": s,
                "current_status": "fails",
            })

# --- numeric token in person name: lee-jun-seok ---
lee_slugs = df[df["market_slug"].str.contains("lee-jun-seok", na=False)]["market_slug"].drop_duplicates().head(3).tolist()
if lee_slugs:
    assertions["expected_improvements"].append({
        "name": "lee_jun_seok_not_corrupted",
        "description": "Korean candidate name 'lee-jun-seok' must not be corrupted (jun should not match <DATE>)",
        "kind": "no_substitution",
        "slugs": lee_slugs,
        "forbidden_substring": "<DATE>",
        "current_status": "fails",
    })

# --- Terminal date should collapse (cs2-<TEAM>-<TEAM>-2026-04-03) ---
bare_event = sample_slugs_under_template("cs2-<TEAM>-<TEAM>", n=4) if (df["market_template"] == "cs2-<TEAM>-<TEAM>").any() else []
if len(bare_event) >= 2:
    assertions["expected_improvements"].append({
        "name": "cs2_terminal_date_collapses",
        "description": "Terminal -YYYY-MM-DD should collapse to <DATE> even without trailing tokens",
        "kind": "collapse",
        "slugs": bare_event,
        "current_status": "fails",
    })

# --- winter-olympics-winter-olympics doubled prefix ---
wow = df[df["market_template"].str.contains("winter-olympics-winter-olympics", na=False)]["market_slug"].drop_duplicates().head(5).tolist()
if wow:
    assertions["expected_improvements"].append({
        "name": "winter_olympics_doubled_prefix",
        "description": "Doubled 'winter-olympics-winter-olympics' should be deduplicated to single prefix",
        "kind": "no_substitution",
        "slugs": wow,
        "forbidden_substring": "winter-olympics-winter-olympics",
        "current_status": "fails",
    })

# ===========================================================================
# WRITE
# ===========================================================================

OUT.write_text(json.dumps(assertions, indent=2))
print(f"\nwrote {OUT}")
print(f"  collapse assertions:       {len(assertions['collapse'])}")
print(f"  distinguish assertions:    {len(assertions['distinguish'])}")
print(f"  expected_improvements:     {len(assertions['expected_improvements'])}")

# Print summary
for cat in ["collapse", "distinguish", "expected_improvements"]:
    print(f"\n  {cat.upper()}:")
    for a in assertions[cat]:
        n_slugs = len(a.get("slugs", a.get("group_a", []) + a.get("group_b", []) + a.get("val_slugs", []) + a.get("valorant_slugs", [])))
        print(f"    [{a['name']}] {n_slugs} slugs")
