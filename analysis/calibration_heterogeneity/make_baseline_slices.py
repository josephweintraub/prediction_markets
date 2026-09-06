"""Baseline slicing schemes (no embeddings needed).

  scheme_all.parquet                single pooled slice (reference calibration)
  scheme_category.parquet           curated native tags -> 12 primary categories
                                    (market_native_categories.parquet); UNKNOWN
                                    where unmapped
  scheme_series_membership.parquet  in_series vs standalone (native series_slug)
"""
from __future__ import annotations
import os

import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
CATS = "/mnt/data/learnability/native/market_native_categories.parquet"
os.makedirs(f"{BASE}/schemes", exist_ok=True)

uni = pd.read_parquet(f"{BASE}/universe_markets.parquet",
                      columns=["market_id", "series_slug"])

pd.DataFrame({"market_id": uni["market_id"], "slice": "ALL"}) \
    .to_parquet(f"{BASE}/schemes/scheme_all.parquet", index=False)

cat = pd.read_parquet(CATS)
m = uni.merge(cat, left_on="market_id", right_on="mkt", how="left")
pd.DataFrame({"market_id": m["market_id"],
              "slice": m["prim"].fillna("UNKNOWN")}) \
    .to_parquet(f"{BASE}/schemes/scheme_category.parquet", index=False)

ser = np.where(uni["series_slug"].notna(), "in_series", "standalone")
pd.DataFrame({"market_id": uni["market_id"], "slice": ser}) \
    .to_parquet(f"{BASE}/schemes/scheme_series_membership.parquet", index=False)
print("baseline schemes written", flush=True)
