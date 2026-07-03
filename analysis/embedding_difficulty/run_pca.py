"""Approach A: PCA structure of the market-embedding space.

Descriptive (full-sample) PCA — the time-honest measure is approach B; this
answers "does FLB vary systematically along the main axes of question space,
and what do those axes encode?" Top PCs of sentence-embedding matrices are
known to carry frequency/length artifacts (Mu et al. 2018), so each PC is
correlated against observables (question length, volume, vintage, category)
before any interpretation.

Outputs:
  pca_evr.json                 explained variance ratios (50 comps)
  pca_scores.parquet           market_id + pc1..pc20
  pca_correlates.parquet       corr(PC, observable) table
  schemes/scheme_pca_pc{1..4}_quintile.parquet
"""
from __future__ import annotations
import json
import os

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA

BASE = "/mnt/data/embedding_difficulty"
N_COMP = 50
N_SAVE = 20
N_SCHEME = 4

os.makedirs(f"{BASE}/schemes", exist_ok=True)
uni = pd.read_parquet(f"{BASE}/universe_markets.parquet") \
        .sort_values("market_id").reset_index(drop=True)
emb = np.load(f"{BASE}/emb_q.npy")
assert len(uni) == len(emb)

pca = PCA(n_components=N_COMP, svd_solver="randomized", random_state=0)
scores = pca.fit_transform(emb.astype(np.float64))
with open(f"{BASE}/pca_evr.json", "w") as f:
    json.dump({"evr": pca.explained_variance_ratio_.tolist()}, f, indent=2)
print("EVR top10:", np.round(pca.explained_variance_ratio_[:10], 4), flush=True)

sc = pd.DataFrame({"market_id": uni["market_id"]})
for i in range(N_SAVE):
    sc[f"pc{i+1}"] = scores[:, i].astype(np.float32)
sc.to_parquet(f"{BASE}/pca_scores.parquet", index=False)

# --- correlates ---
obs = pd.DataFrame(index=uni.index)
obs["q_len"] = uni["question"].str.len()
obs["d_len"] = uni["description"].str.len()
obs["log_trades"] = np.log1p(uni["n_buy_filtered"].fillna(0))
obs["log_usd"] = np.log1p(uni["usd_buy_filtered"].fillna(0))
obs["vintage"] = pd.to_datetime(uni["created_at"]).dt.year.fillna(
    pd.to_datetime(uni["first_trade_at"]).dt.year).astype(float)
obs["has_series"] = uni["series_slug"].notna().astype(float)
top_cats = uni["category"].value_counts().head(8).index
for c in top_cats:
    obs[f"cat_{c}"] = (uni["category"] == c).astype(float)

rows = []
for i in range(N_SAVE):
    for col in obs.columns:
        v = obs[col]
        m = v.notna()
        r = np.corrcoef(scores[m.to_numpy(), i], v[m])[0, 1]
        rows.append({"pc": i + 1, "observable": col, "corr": float(r)})
pd.DataFrame(rows).to_parquet(f"{BASE}/pca_correlates.parquet", index=False)

# --- quintile schemes for the FLB engine ---
for i in range(N_SCHEME):
    q = pd.qcut(scores[:, i], 5, labels=False, duplicates="drop")
    out = pd.DataFrame({"market_id": uni["market_id"],
                        "slice": [f"pc{i+1}_q{int(x)+1}" for x in q]})
    out.to_parquet(f"{BASE}/schemes/scheme_pca_pc{i+1}_quintile.parquet",
                   index=False)
print("pca schemes written", flush=True)
