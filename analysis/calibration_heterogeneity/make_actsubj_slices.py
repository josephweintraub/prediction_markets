"""Approach D: action / subject precedent counts (categorical, no embeddings).

Uses the existing Stage-2 LLM extractions (market_action_generic,
market_subjects_generic) as raw labels — one action string and a list of
subject strings per market. For each labeled market at its birth time:
  action_prec   # prior labeled markets with the SAME action
  subject_prec  max over its subjects of # prior labeled markets sharing
                that subject ("how well-known is its best-known subject")
Strictly-earlier births only (ties excluded via rank method='min').

CAVEAT (logged): Stage-2 labels cover only the pre-June-2026 universe
(379K/850K markets, 59% of filtered trades) — precedent counts are computed
within the labeled subset, and coverage is confounded with vintage.
Exploratory; treat gradients as suggestive.

Schemes written (schemes/):
  scheme_act_prec.parquet      bins 0 / 1-9 / 10-99 / 100-999 / 1000+
  scheme_subj_prec.parquet     same bins
  scheme_actsubj_2x2.parquet   action_seen (>=10) x subject_seen (>=10)
  scheme_act_prec_vint.parquet action_prec quintiles WITHIN birth year
                               (vintage-controlled variant)
"""
from __future__ import annotations
import os

import duckdb
import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
S = "/mnt/data/learnability/stage2_per_contract_augmented.parquet"
os.makedirs(f"{BASE}/schemes", exist_ok=True)

con = duckdb.connect()
lab = con.execute(f"""
    SELECT condition_id AS market_id,
           ANY_VALUE(market_action_generic) AS action,
           ANY_VALUE(market_subjects_generic) AS subjects
    FROM read_parquet('{S}')
    GROUP BY condition_id
""").fetchdf()
uni = pd.read_parquet(f"{BASE}/universe_markets.parquet",
                      columns=["market_id", "created_at", "first_trade_at"])
birth = pd.to_datetime(uni["created_at"])
ft = pd.to_datetime(uni["first_trade_at"])
if getattr(ft.dt, "tz", None) is not None:
    ft = ft.dt.tz_localize(None)
uni["birth"] = birth.fillna(ft)

df = uni.merge(lab, on="market_id", how="inner")
df = df[df["action"].notna()].reset_index(drop=True)
print(f"{len(df):,} labeled universe markets", flush=True)

# action precedent count: strictly-earlier same-action markets
df["action_prec"] = (df.groupby("action")["birth"]
                       .rank(method="min").astype(int) - 1)

# subject precedent count: explode, rank within subject, max back per market
ex = df[["market_id", "birth", "subjects"]].explode("subjects")
ex = ex[ex["subjects"].notna() & (ex["subjects"] != "")]
ex["sp"] = ex.groupby("subjects")["birth"].rank(method="min").astype(int) - 1
sp = ex.groupby("market_id")["sp"].max().rename("subject_prec")
df = df.merge(sp, on="market_id", how="left")
df["subject_prec"] = df["subject_prec"].fillna(0).astype(int)


def bins(v: pd.Series, name: str) -> pd.Series:
    return pd.Series(np.select(
        [v == 0, v < 10, v < 100, v < 1000],
        [f"{name}_0", f"{name}_1_9", f"{name}_10_99", f"{name}_100_999"],
        default=f"{name}_1000p"), index=v.index)


pd.DataFrame({"market_id": df["market_id"],
              "slice": bins(df["action_prec"], "act")}) \
    .to_parquet(f"{BASE}/schemes/scheme_act_prec.parquet", index=False)
pd.DataFrame({"market_id": df["market_id"],
              "slice": bins(df["subject_prec"], "subj")}) \
    .to_parquet(f"{BASE}/schemes/scheme_subj_prec.parquet", index=False)

a = np.where(df["action_prec"] >= 10, "actKnown", "actNew")
s = np.where(df["subject_prec"] >= 10, "subjKnown", "subjNew")
pd.DataFrame({"market_id": df["market_id"],
              "slice": pd.Series(a) + "_" + pd.Series(s)}) \
    .to_parquet(f"{BASE}/schemes/scheme_actsubj_2x2.parquet", index=False)

# vintage-controlled: action_prec quintiles within birth year
df["year"] = df["birth"].dt.year
q = df.groupby("year")["action_prec"].transform(
    lambda v: pd.qcut(v.rank(method="first"), 5, labels=False))
pd.DataFrame({"market_id": df["market_id"],
              "slice": [f"actvint_q{int(x)+1}" for x in q]}) \
    .to_parquet(f"{BASE}/schemes/scheme_act_prec_vint.parquet", index=False)

for f in ("act_prec", "subj_prec", "actsubj_2x2", "act_prec_vint"):
    t = pd.read_parquet(f"{BASE}/schemes/scheme_{f}.parquet")
    print(f"scheme_{f}:\n{t['slice'].value_counts().to_string()}", flush=True)
