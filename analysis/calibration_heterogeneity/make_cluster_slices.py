"""Approach C: multi-granularity k-means over market embeddings.

k in {12, 50, 200, 1000} — from category-coarse to template-fine. Embeddings
are L2-normalized so k-means on them approximates spherical clustering.
Also writes two baseline schemes: native category (comparison at coarse
granularity) and ALL (single pooled slice).

Outputs:
  schemes/scheme_cluster_k{k}.parquet      market_id, slice="k{k}_c{idx}"
  cluster_terms_k{k}.parquet               c-TF-IDF top terms + examples
  schemes/scheme_category.parquet, schemes/scheme_all.parquet
"""
from __future__ import annotations
import os

import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sklearn.feature_extraction.text import TfidfVectorizer

BASE = "/mnt/data/embedding_difficulty"
KS = (12, 50, 200, 1000)

os.makedirs(f"{BASE}/schemes", exist_ok=True)
uni = pd.read_parquet(f"{BASE}/universe_markets.parquet") \
        .sort_values("market_id").reset_index(drop=True)
emb = np.load(f"{BASE}/emb_q.npy")
assert len(uni) == len(emb)

for k in KS:
    km = MiniBatchKMeans(n_clusters=k, random_state=0, batch_size=8192,
                         n_init=5, max_no_improvement=50)
    lab = km.fit_predict(emb)
    pd.DataFrame({
        "market_id": uni["market_id"],
        "slice": [f"k{k}_c{int(l):04d}" for l in lab],
    }).to_parquet(f"{BASE}/schemes/scheme_cluster_k{k}.parquet", index=False)

    # interpretation aids: c-TF-IDF top terms + 3 questions nearest centroid
    docs = (uni["question"].fillna("").str.lower()
            .str.replace(r"[^a-z0-9 ]", " ", regex=True))
    agg = docs.groupby(lab).apply(" ".join)
    vec = TfidfVectorizer(max_features=40000, stop_words="english",
                          token_pattern=r"[a-z][a-z0-9]+")
    X = vec.fit_transform(agg)
    vocab = np.array(vec.get_feature_names_out())
    rows = []
    for c in range(k):
        r = X[list(agg.index).index(c)] if c in agg.index else None
        terms = ""
        if r is not None and r.nnz:
            idx = np.asarray(r.todense()).ravel().argsort()[::-1][:12]
            terms = ", ".join(vocab[idx])
        members = np.where(lab == c)[0]
        ex = []
        if len(members):
            d = emb[members] @ km.cluster_centers_[c]
            ex = uni["question"].iloc[members[np.argsort(-d)[:3]]].tolist()
        rows.append({"cluster": f"k{k}_c{c:04d}", "n_markets": int(len(members)),
                     "n_buy_filtered": int(uni["n_buy_filtered"].iloc[members].fillna(0).sum()) if len(members) else 0,
                     "top_terms": terms,
                     "ex1": ex[0] if len(ex) > 0 else "",
                     "ex2": ex[1] if len(ex) > 1 else "",
                     "ex3": ex[2] if len(ex) > 2 else ""})
    pd.DataFrame(rows).to_parquet(f"{BASE}/cluster_terms_k{k}.parquet", index=False)
    print(f"k={k} done", flush=True)

print("cluster schemes written", flush=True)
