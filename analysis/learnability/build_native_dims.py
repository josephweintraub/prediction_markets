"""Build the v7 contract-dimensions table: kept LLM-free v6 dims + new native dims."""
import pandas as pd, numpy as np, duckdb, re
V5="/mnt/data/learnability/output/phase1_v5_contract_dimensions.parquet"
NAT="/mnt/data/learnability/native/native_market_meta.parquet"
OUT="/mnt/data/learnability/output/phase1_v7_contract_dimensions.parquet"
KEEP=["dim_dollar_volume_tier","dim_contract_horizon","dim_outcomes_per_event",
      "dim_event_slug_size","dim_prior_settlements_bin__event_slug",
      "dim_text_novelty","dim_text_neighbors_strict"]
d5=pd.read_parquet(V5)
print("kept LLM-free present:", [c for c in KEEP if c in d5.columns])
print("kept MISSING:", [c for c in KEEP if c not in d5.columns])
base=d5[["token_id","condition_id"]+[c for c in KEEP if c in d5.columns]].copy()
nat=pd.read_parquet(NAT)
FEED=re.compile(r"chain\.link|binance|pyth|coingecko|coinmarketcap", re.I)
def anchor(s):
    if not isinstance(s,str) or s.strip()=="": return "3_unsourced"
    return "1_priced_feed" if FEED.search(s) else "2_sourced"
nat["dim_anchor"]=nat["resolution_source"].apply(anchor)
def recur(r):
    if r in ("5m","hourly"): return "intraday"
    if r=="daily": return "daily"
    if r in ("weekly","monthly","annual"): return "multiday"
    return "one_off"
nat["dim_recurrence"]=nat["recurrence"].apply(recur)
ca=pd.to_datetime(nat["created_at"],errors="coerce",utc=True)
cl=pd.to_datetime(nat["closed_time"],errors="coerce",utc=True)
lag=(cl-ca).dt.total_seconds()/86400
nat["dim_feedback_lag"]=pd.cut(lag,[-1e9,1,7,30,1e9],labels=["<1d","1-7d","1-4wk",">1mo"]).astype("object")
# prior settlements within native series (window method, O(n log n))
con=duckdb.connect()
tmp=nat[["condition_id","series_slug"]].copy(); tmp["ca"]=ca.values; tmp["cl"]=cl.values
con.register("s", tmp[tmp["series_slug"].notna() & tmp["ca"].notna()])
ps=con.execute("""
WITH ev AS (
  SELECT series_slug, ca AS t, 0 AS ord, condition_id FROM s
  UNION ALL SELECT series_slug, cl AS t, -1 AS ord, NULL AS condition_id FROM s WHERE cl IS NOT NULL
)
SELECT condition_id, n_before FROM (
  SELECT condition_id, ord,
    SUM(CASE WHEN ord=-1 THEN 1 ELSE 0 END) OVER (PARTITION BY series_slug ORDER BY t, ord ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_before
  FROM ev) WHERE ord=0
""").fetchdf()
nat=nat.merge(ps,on="condition_id",how="left")
def psb(row):
    if pd.isna(row["series_slug"]): return None
    n=0 if pd.isna(row["n_before"]) else row["n_before"]
    return "0" if n==0 else "1-10" if n<=10 else "11-100" if n<=100 else "100+"
nat["dim_prior_settlements"]=nat.apply(psb,axis=1)
NEW=["dim_anchor","dim_recurrence","dim_feedback_lag","dim_prior_settlements"]
out=base.merge(nat[["condition_id"]+NEW].drop_duplicates("condition_id"),on="condition_id",how="left")
out.to_parquet(OUT,index=False)
print("WROTE",OUT,out.shape)
for c in NEW:
    print("\n==",c,"==\n",out[c].value_counts(dropna=False).head(8).to_string())
