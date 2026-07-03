"""Bake market_labels_v2.parquet — the full per-market label table.

Inputs:
  /mnt/data/learnability/native/native_market_meta_v2.parquet   (open+closed, 2026-07-03)
  repo tag_taxonomy/final_tag_map_v1.json                        (authoritative tag->cat)
  repo tag_taxonomy/subcategories.json                           (~35 designed subcats)
  repo tag_taxonomy/curated_tags.json                            (structural-tag types)
  /mnt/data/learnability/output/phase1_v6fix_contract_dimensions.parquet (LLM labels,
      only to derive the auto-prior for uncurated tail tags — same as v1 bake)

v2 semantics (tag_taxonomy/overrides_v2.json):
  * Iran-cluster tags vote Geopolitics; event_family='Iran' is a separate column.
  * Mentions precedence retained (category) + mechanic='mentions'.
  * vote_margin + abstain instead of silent forced calls.
  * label_source provenance; hand-lock overlays applied later from locked_overrides.json.

Output: /mnt/data/learnability/native/market_labels_v2.parquet (one row per condition_id)
"""
import duckdb, json, os, time
import pandas as pd

T0 = time.time()
def log(m): print(f"[{time.time()-T0:7.1f}s] {m}", flush=True)

TAX = "/home/ubuntu/prediction_markets/analysis/learnability/tag_taxonomy"
NM2 = "/mnt/data/learnability/native/native_market_meta_v2.parquet"
V6  = "/mnt/data/learnability/output/phase1_v6fix_contract_dimensions.parquet"
OUT = "/mnt/data/learnability/native/market_labels_v2.parquet"
LOCK = f"{TAX}/locked_overrides.json"   # applied if present
ABSTAIN_RATIO = 1.25

con = duckdb.connect()
con.execute("PRAGMA threads=16"); con.execute("PRAGMA memory_limit='100GB'")
con.execute("SET temp_directory='/mnt/data/tmp'"); con.execute("SET preserve_insertion_order=false")

# ---- 1) effective tag->topic map: curated/baked map, Iran-cluster -> Geopolitics ----
fm = pd.DataFrame(json.load(open(f"{TAX}/final_tag_map_v1.json")))
iran_tags = set(fm.loc[fm.prim_cat == "Iran", "tag"])
fm.loc[fm.prim_cat == "Iran", "prim_cat"] = "Geopolitics"
log(f"map: {len(fm)} tags, {int(fm.excluded.sum())} excluded, {len(iran_tags)} Iran-cluster->Geopolitics")
con.register("fmdf", fm[["tag", "prim_cat", "excluded"]])
con.execute("CREATE TEMP TABLE fm AS SELECT * FROM fmdf")

# structural tags (mechanic hints) from curation provenance
cur = json.load(open(f"{TAX}/curated_tags.json"))
structural = {c["tag"] for c in cur if c.get("type") == "structural"}

# ---- 2) auto-prior for uncurated tail tags (majority LLM category per tag) ----
log("auto-prior (sigtop) from LLM overlap...")
con.execute(f"""CREATE TEMP TABLE ov AS
  SELECT m.condition_id, unnest(m.tags) tag, v.cat llm FROM read_parquet('{NM2}') m
  JOIN (SELECT condition_id, ANY_VALUE(dim_primary_category) cat FROM read_parquet('{V6}')
        WHERE dim_primary_category IS NOT NULL AND dim_primary_category<>'Uncategorized'
        GROUP BY condition_id) v USING(condition_id)
  WHERE m.tags IS NOT NULL""")
con.execute("""CREATE TEMP TABLE sigtop AS SELECT tag, cat FROM (
  SELECT tag, llm cat, ROW_NUMBER() OVER (PARTITION BY tag ORDER BY COUNT(*) DESC) rn
  FROM ov GROUP BY tag, llm) WHERE rn=1""")
# Iran-cluster consistency for auto-mapped tags too
con.execute("UPDATE sigtop SET cat='Geopolitics' WHERE cat='Iran'")

con.execute(f"""CREATE TEMP TABLE alltags AS
  SELECT tag, COUNT(DISTINCT condition_id) df FROM
  (SELECT condition_id, unnest(tags) tag FROM read_parquet('{NM2}') WHERE tags IS NOT NULL)
  GROUP BY tag""")
N = con.execute("SELECT COUNT(DISTINCT condition_id) FROM ov").fetchone()[0]
con.execute("""CREATE TEMP TABLE eff AS SELECT a.tag, a.df,
  CASE WHEN fm.tag IS NOT NULL THEN (CASE WHEN fm.excluded THEN NULL ELSE fm.prim_cat END)
       ELSE s.cat END prim,
  (fm.tag IS NOT NULL AND NOT COALESCE(fm.excluded, false)) AS curated
  FROM alltags a LEFT JOIN fm ON a.tag=fm.tag LEFT JOIN sigtop s ON a.tag=s.tag""")

# ---- 3) topic vote with margin ----
log("topic vote...")
con.execute(f"CREATE TEMP TABLE mt AS SELECT condition_id mkt, unnest(tags) tag FROM read_parquet('{NM2}') WHERE tags IS NOT NULL")
con.execute(f"""CREATE TEMP TABLE vote AS
  SELECT mt.mkt, e.prim, SUM(ln({N}*1.0/GREATEST(e.df,1))) score,
         SUM(CASE WHEN e.curated THEN 1 ELSE 0 END) curated_voters
  FROM mt JOIN eff e ON mt.tag=e.tag WHERE e.prim IS NOT NULL GROUP BY mt.mkt, e.prim""")
con.execute("""CREATE TEMP TABLE ranked AS SELECT *, ROW_NUMBER() OVER (PARTITION BY mkt ORDER BY score DESC) rn FROM vote""")
con.execute("""CREATE TEMP TABLE mcat AS
  SELECT a.mkt, a.prim topic_vote, a.score s1, a.curated_voters,
         b.prim topic_2nd, b.score s2
  FROM (SELECT * FROM ranked WHERE rn=1) a
  LEFT JOIN (SELECT * FROM ranked WHERE rn=2) b USING(mkt)""")

# ---- 4) subcategory map + vote within winning topic ----
sub = json.load(open(f"{TAX}/subcategories.json"))
srows = []
for blk in sub:
    for sc in blk.get("subcategories", []):
        for t in sc.get("tags", []):
            srows.append({"prim": blk["primary"], "tag": t, "subcat": sc["name"]})
con.register("subdf", pd.DataFrame(srows))
con.execute("CREATE TEMP TABLE submap AS SELECT * FROM subdf")
log(f"subcat map: {len(srows)} (tag,primary)->subcat rows")
con.execute("""CREATE TEMP TABLE subvote AS
  SELECT mt.mkt, sm.subcat, SUM(ln(1000000.0/GREATEST(e.df,1))) score
  FROM mt JOIN mcat mc ON mt.mkt=mc.mkt
  JOIN submap sm ON mt.tag=sm.tag AND sm.prim=mc.topic_vote
  JOIN eff e ON mt.tag=e.tag
  GROUP BY mt.mkt, sm.subcat""")
con.execute("""CREATE TEMP TABLE msub AS SELECT mkt, subcat FROM
  (SELECT *, ROW_NUMBER() OVER (PARTITION BY mkt ORDER BY score DESC) rn FROM subvote) WHERE rn=1""")

# ---- 5) final assembly ----
log("assembling...")
iran_sql = ",".join("'" + t.replace("'", "''") + "'" for t in iran_tags)
struct_sql = ",".join("'" + t.replace("'", "''") + "'" for t in structural)
con.execute(f"""CREATE TEMP TABLE labels AS SELECT
  m.condition_id, m.question, m.market_slug,
  -- topic: Mentions precedence > vote (Iran precedence removed for topic)
  CASE WHEN list_contains(m.tags,'Mentions') THEN 'Mentions' ELSE mc.topic_vote END AS topic,
  CASE WHEN list_contains(m.tags,'Mentions') THEN 'precedence:mentions'
       WHEN mc.mkt IS NULL THEN 'no_tags' ELSE 'vote' END AS label_source,
  mc.topic_2nd, mc.s1 AS topic_score, mc.s2 AS topic_2nd_score,
  CASE WHEN mc.s2 IS NULL OR mc.s2<=0 THEN NULL ELSE mc.s1/mc.s2 END AS vote_margin,
  (mc.mkt IS NOT NULL AND NOT list_contains(m.tags,'Mentions') AND
   ((mc.s2 IS NOT NULL AND mc.s1/GREATEST(mc.s2,0.0001) < {ABSTAIN_RATIO})
    OR COALESCE(mc.curated_voters,0)=0)) AS abstain,
  ms.subcat AS subcategory,
  -- event family (separate axis)
  CASE WHEN len(list_filter(m.tags, x -> x LIKE '%Iran%' OR x IN ({iran_sql})))>0
       THEN 'Iran' END AS event_family,
  -- mechanic by precedence
  CASE WHEN list_contains(m.tags,'Mentions') THEN 'mentions'
       WHEN list_contains(m.tags,'Up or Down') OR COALESCE(m.event_slug,'') ILIKE '%updown%'
            OR COALESCE(m.event_slug,'') ILIKE '%up-or-down%'
            OR COALESCE(m.question,'') ILIKE '%up or down%' THEN 'updown'
       WHEN list_contains(m.tags,'Hit Price') OR list_contains(m.tags,'Multi Strikes') THEN 'price_target'
       WHEN m.sports_market_type IS NOT NULL AND m.sports_market_type<>'' THEN 'sports_'||m.sports_market_type
       WHEN COALESCE(m.neg_risk,false) THEN 'negrisk_multi'
       ELSE 'vanilla' END AS mechanic,
  list_filter(m.tags, x -> x NOT IN ({struct_sql})) AS entity_tags,
  m.tags AS all_tags,
  -- native structural detail
  m.closed, m.active, m.event_slug, m.series_slug, m.recurrence, m.resolution_source,
  m.uma_status, m.automatically_resolved, m.neg_risk, m.neg_risk_market_id,
  m.sports_market_type, m.game_start_time, m.tick_size, m.resolved_by,
  m.n_outcomes, m.volume_num, m.liquidity, m.comment_count,
  m.created_at, m.end_date, m.closed_time,
  length(COALESCE(m.description,'')) AS rules_len
FROM read_parquet('{NM2}') m
LEFT JOIN mcat mc ON m.condition_id=mc.mkt
LEFT JOIN msub ms ON m.condition_id=ms.mkt""")

# ---- 6) hand-lock overlay (if present) ----
if os.path.exists(LOCK):
    lk = pd.DataFrame(json.load(open(LOCK)))
    con.register("lockdf", lk)
    con.execute("""UPDATE labels SET topic=l.topic,
        subcategory=COALESCE(l.subcategory, labels.subcategory),
        label_source='hand-lock', abstain=false
      FROM lockdf l WHERE labels.condition_id=l.condition_id""")
    log(f"hand-lock overlay applied: {len(lk)} markets")
else:
    log("no locked_overrides.json yet — vote-only bake")

con.execute(f"COPY labels TO '{OUT}' (FORMAT PARQUET, COMPRESSION ZSTD)")
r = con.execute(f"""SELECT COUNT(*), COUNT(topic), COUNT(subcategory),
    COUNT(*) FILTER (abstain), COUNT(*) FILTER (event_family='Iran'),
    COUNT(*) FILTER (closed=false) FROM read_parquet('{OUT}')""").fetchone()
log(f"WROTE {OUT}: {r[0]:,} markets | topic {r[1]:,} | subcat {r[2]:,} | abstain {r[3]:,} | Iran fam {r[4]:,} | open {r[5]:,}")
print(con.execute(f"SELECT topic, COUNT(*) n FROM read_parquet('{OUT}') GROUP BY 1 ORDER BY 2 DESC").fetchdf().to_string(index=False), flush=True)
print(con.execute(f"SELECT mechanic, COUNT(*) n FROM read_parquet('{OUT}') GROUP BY 1 ORDER BY 2 DESC").fetchdf().to_string(index=False), flush=True)
