#!/bin/bash
# Embedding-difficulty analysis chain — fires once emb_q.npy exists.
set -e
PY=/home/ubuntu/venv/bin/python
D=/home/ubuntu/prediction_markets/analysis/embedding_difficulty
B=/mnt/data/embedding_difficulty
cd $D
echo "=== waiting for emb_q.npy ==="
until [ -f $B/emb_q.npy ]; do sleep 30; done
sleep 15
echo "=== rebuild scheme_category from curated map ==="
$PY - <<'EOF'
import pandas as pd
BASE = "/mnt/data/embedding_difficulty"
uni = pd.read_parquet(f"{BASE}/universe_markets.parquet", columns=["market_id"])
cat = pd.read_parquet("/mnt/data/learnability/native/market_native_categories.parquet")
m = uni.merge(cat, left_on="market_id", right_on="mkt", how="left")
m["slice"] = m["prim"].fillna("UNKNOWN")
m[["market_id","slice"]].to_parquet(f"{BASE}/schemes/scheme_category.parquet", index=False)
print(m["slice"].value_counts().to_string())
EOF
echo "=== PCA ==="
$PY run_pca.py
echo "=== clustering ==="
$PY make_cluster_slices.py
echo "=== novelty ==="
$PY compute_novelty.py
echo "=== novelty diagnostics ==="
$PY novelty_diagnostics.py
echo "=== novelty slices ==="
$PY make_novelty_slices.py
echo "=== FLB mature (all schemes) ==="
$PY run_schemes.py --window mature
echo "=== FLB closing (key schemes) ==="
$PY run_schemes.py --window closing --schemes all category series_membership nov_k25 nov_k25x nov_k25x_vint nov_cnt cluster_k12 cluster_k50 pca_pc1_quintile pca_pc2_quintile pca_pc3_quintile pca_pc4_quintile
echo "=== CHAIN DONE ==="
