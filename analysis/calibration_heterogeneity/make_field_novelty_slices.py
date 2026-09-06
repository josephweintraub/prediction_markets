"""Per-variant novelty slices + cross-variant comparison artifacts.

For each embedding variant (q = session-1 baseline, rules, context, comb_eq,
comb_qc): within-birth-year deciles of sim_k25_x over trade-viable markets
(d01 = most novel), plus the same restricted to >= $10k volume.

Also writes field_compare.parquet: pairwise Pearson/Spearman correlations of
sim_k25_x across variants (viable markets with all variants defined), plus
coverage counts — do the fields even agree on what is novel?
"""
from __future__ import annotations
import os

import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
VARIANTS = {"q": "novelty.parquet",
            "rules": "novelty_rules.parquet",
            "context": "novelty_context.parquet",
            "comb_eq": "novelty_comb_eq.parquet",
            "comb_qc": "novelty_comb_qc.parquet"}
os.makedirs(f"{BASE}/schemes", exist_ok=True)

uni = pd.read_parquet(f"{BASE}/universe_markets.parquet",
                      columns=["market_id", "n_buy_filtered", "usd_buy_filtered"])
merged = uni.copy()
for v, f in VARIANTS.items():
    p = f"{BASE}/{f}"
    if not os.path.exists(p):
        print(f"skip {v} (missing {f})", flush=True)
        continue
    nv = pd.read_parquet(p, columns=["market_id", "sim_k25_x", "birth_at"])
    nv = nv.rename(columns={"sim_k25_x": f"nov_{v}"})
    if "birth_at" not in merged.columns:
        merged = merged.merge(nv, on="market_id", how="left")
    else:
        merged = merged.merge(nv.drop(columns=["birth_at"]), on="market_id",
                              how="left")

viable = merged[merged["n_buy_filtered"].fillna(0) > 0].copy()
viable["year"] = pd.to_datetime(viable["birth_at"]).dt.year
print(f"{len(viable):,} viable markets", flush=True)


def vint_deciles(df: pd.DataFrame, col: str, tag: str) -> None:
    m = df[col].notna() & df["year"].notna()
    q = df.loc[m].groupby("year")[col].transform(
        lambda s: pd.qcut(s.rank(method="first"), 10, labels=False))
    out = pd.DataFrame({"market_id": df.loc[m, "market_id"],
                        "slice": [f"{tag}_d{int(x)+1:02d}" for x in q]})
    out.to_parquet(f"{BASE}/schemes/scheme_{tag}.parquet", index=False)
    print(f"scheme_{tag}: {int(m.sum()):,} markets", flush=True)


for v in ("rules", "context", "comb_eq", "comb_qc"):
    col = f"nov_{v}"
    if col not in viable.columns:
        continue
    vint_deciles(viable, col, f"nv_{v}")
    vint_deciles(viable[viable["usd_buy_filtered"].fillna(0) >= 1e4],
                 col, f"nv_{v}_f10k")

# cross-variant agreement
cols = [c for c in viable.columns if c.startswith("nov_")]
comp = viable[cols].dropna()
rows = []
for i, a in enumerate(cols):
    for b in cols[i + 1:]:
        rows.append({"a": a, "b": b,
                     "pearson": float(comp[a].corr(comp[b])),
                     "spearman": float(comp[a].corr(comp[b], method="spearman")),
                     "n": int(len(comp))})
for c in cols:
    rows.append({"a": c, "b": "coverage", "pearson": np.nan,
                 "spearman": np.nan,
                 "n": int(viable[c].notna().sum())})
pd.DataFrame(rows).to_parquet(f"{BASE}/field_compare.parquet", index=False)
print(pd.DataFrame(rows).to_string(index=False), flush=True)
