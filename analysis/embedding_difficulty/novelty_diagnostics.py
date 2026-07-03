"""Diagnostics for the novelty / precedent-density measure.

Produces (all small, for the report):
  novelty_confounds.parquet   corr + OLS of novelty on question length, log
                              volume, vintage, series membership, category
                              (the known artifact channels)
  novelty_hubness.json        k-occurrence skewness of the top-25 neighbor
                              graph (hubness diagnostic, Radovanovic 2010)
  novelty_examples.parquet    most-novel and most-precedented trade-viable
                              markets with their nearest predecessor text
  novelty_dist.parquet        per-birth-year distribution of sim_k25_x
"""
from __future__ import annotations
import json

import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"

uni = pd.read_parquet(f"{BASE}/universe_markets.parquet") \
        .sort_values("market_id").reset_index(drop=True)
nov = pd.read_parquet(f"{BASE}/novelty.parquet")
df = uni.merge(nov, on="market_id", validate="1:1")
nb = np.load(f"{BASE}/neighbors_top25.npy")

# ---- confounds ----
X = pd.DataFrame(index=df.index)
X["q_len"] = df["question"].str.len().astype(float)
X["d_len"] = df["description"].str.len().astype(float)
X["log_trades"] = np.log1p(df["n_buy_filtered"].fillna(0))
X["log_n_prior"] = np.log1p(df["n_prior"])
X["vintage_year"] = pd.to_datetime(df["birth_at"]).dt.year.astype(float)
X["in_series"] = df["series_slug"].notna().astype(float)

rows = []
for target in ("sim_k25", "sim_k25_x", "top1_sim"):
    y = df[target]
    for col in X.columns:
        m = y.notna() & X[col].notna()
        rows.append({"target": target, "var": col,
                     "corr": float(np.corrcoef(y[m], X[col][m])[0, 1])})
    # multivariate OLS (standardized betas) for the confound table
    m = y.notna() & X.notna().all(axis=1)
    Z = (X[m] - X[m].mean()) / X[m].std()
    Z.insert(0, "const", 1.0)
    yy = (y[m] - y[m].mean()) / y[m].std()
    beta, *_ = np.linalg.lstsq(Z.to_numpy(), yy.to_numpy(), rcond=None)
    r2 = 1 - ((yy - Z.to_numpy() @ beta) ** 2).sum() / (yy ** 2).sum()
    for name, b in zip(Z.columns, beta):
        if name != "const":
            rows.append({"target": target, "var": f"beta_{name}", "corr": float(b)})
    rows.append({"target": target, "var": "ols_r2", "corr": float(r2)})
pd.DataFrame(rows).to_parquet(f"{BASE}/novelty_confounds.parquet", index=False)

# ---- hubness ----
flat = nb[nb >= 0].ravel()
occ = np.bincount(flat, minlength=len(df))
sk = float(pd.Series(occ).skew())
with open(f"{BASE}/novelty_hubness.json", "w") as f:
    json.dump({"k_occurrence_skewness": sk,
               "max_occurrence": int(occ.max()),
               "p999_occurrence": float(np.quantile(occ, 0.999))}, f, indent=2)
print("hubness skew:", sk, flush=True)

# ---- qualitative examples (trade-viable only) ----
viable = df[df["n_buy_filtered"].fillna(0) >= 1000].copy()
ex_rows = []
for label, sub in (("most_novel", viable.nsmallest(15, "sim_k25_x")),
                   ("most_precedented", viable.nlargest(15, "sim_k25_x"))):
    for _, r in sub.iterrows():
        t1 = int(r["top1_row"])
        ex_rows.append({
            "group": label, "question": r["question"][:160],
            "sim_k25_x": float(r["sim_k25_x"]), "top1_sim": float(r["top1_sim"]),
            "birth": str(r["birth_at"])[:10], "category": r["category"],
            "n_trades": int(r["n_buy_filtered"]),
            "nearest_predecessor": uni["question"].iloc[t1][:160] if t1 >= 0 else "",
        })
pd.DataFrame(ex_rows).to_parquet(f"{BASE}/novelty_examples.parquet", index=False)

# ---- distribution by vintage ----
g = df.assign(year=pd.to_datetime(df["birth_at"]).dt.year) \
      .groupby("year")["sim_k25_x"]
pd.DataFrame({"mean": g.mean(), "p10": g.quantile(0.1), "p50": g.median(),
              "p90": g.quantile(0.9), "n": g.size()}).reset_index() \
    .to_parquet(f"{BASE}/novelty_dist.parquet", index=False)
print("diagnostics done", flush=True)
