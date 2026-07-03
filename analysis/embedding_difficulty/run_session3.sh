#!/bin/bash
# Session 3: multi-field embeddings (rules / context / combined weightings).
set -e
PY=/home/ubuntu/venv/bin/python
D=/home/ubuntu/prediction_markets/analysis/embedding_difficulty
B=/mnt/data/embedding_difficulty
cd $D
echo "=== field recon json ==="
$PY - <<'EOF'
import duckdb, os, json
con = duckdb.connect(); con.execute(f"SET threads TO {os.cpu_count()}")
U = "/mnt/data/embedding_difficulty/universe_markets.parquet"
N = "/mnt/data/learnability/native/native_market_meta.parquet"
r = con.execute(f"""
WITH u AS (SELECT market_id, description FROM read_parquet('{U}')),
     n AS (SELECT condition_id, event_description FROM read_parquet('{N}'))
SELECT COUNT(*) AS markets,
  COUNT(*) FILTER (WHERE LENGTH(TRIM(u.description)) > 0) AS with_rules,
  COUNT(*) FILTER (WHERE LENGTH(TRIM(n.event_description)) > 0) AS with_context,
  COUNT(DISTINCT NULLIF(TRIM(u.description), '')) AS unique_rules,
  COUNT(DISTINCT NULLIF(TRIM(n.event_description), '')) AS unique_context,
  COUNT(*) FILTER (WHERE TRIM(u.description) = TRIM(n.event_description)) AS context_identical_to_rules
FROM u LEFT JOIN n ON u.market_id = n.condition_id
""").fetchdf().iloc[0].to_dict()
json.dump({k: int(v) for k, v in r.items()},
          open("/mnt/data/embedding_difficulty/field_recon.json", "w"), indent=2)
print(r)
EOF
echo "=== embed context ==="
$PY embed_fields.py --field context --chars 800
echo "=== embed rules ==="
$PY embed_fields.py --field rules --chars 600
echo "=== combined variants ==="
$PY make_field_variants.py
echo "=== novelty engine port check (q) ==="
$PY compute_novelty.py --emb $B/emb_q.npy --out $B/novelty_q_torch.parquet \
  --neighbors-out $B/neighbors_q_torch.npy --meta-out $B/novelty_q_torch_meta.json
$PY - <<'EOF'
import pandas as pd, json, sys
a = pd.read_parquet("/mnt/data/embedding_difficulty/novelty.parquet")[["market_id","sim_k25_x"]]
b = pd.read_parquet("/mnt/data/embedding_difficulty/novelty_q_torch.parquet")[["market_id","sim_k25_x"]]
m = a.merge(b, on="market_id", suffixes=("_np","_th")).dropna()
corr = float(m["sim_k25_x_np"].corr(m["sim_k25_x_th"]))
mad = float((m["sim_k25_x_np"]-m["sim_k25_x_th"]).abs().max())
json.dump({"corr": corr, "max_abs_diff": mad, "n": len(m)},
          open("/mnt/data/embedding_difficulty/novelty_port_check.json","w"), indent=2)
print(f"port check: corr={corr:.6f} max_abs_diff={mad:.2e}")
if corr < 0.999: sys.exit("PORT CHECK FAILED")
EOF
for V in context rules comb_eq comb_qc; do
  echo "=== novelty $V ==="
  MASK=""
  if [ -f $B/emb_${V}_mask.npy ]; then MASK="--mask $B/emb_${V}_mask.npy"; fi
  $PY compute_novelty.py --emb $B/emb_${V}.npy $MASK \
    --out $B/novelty_${V}.parquet --neighbors-out $B/neighbors_${V}.npy \
    --meta-out $B/novelty_${V}_meta.json
done
echo "=== field novelty slices + compare ==="
$PY make_field_novelty_slices.py
echo "=== FLB mature (field variants) ==="
$PY run_schemes.py --window mature --schemes nv_rules nv_rules_f10k nv_context nv_context_f10k nv_comb_eq nv_comb_eq_f10k nv_comb_qc nv_comb_qc_f10k
echo "=== render report v3 ==="
$PY render_report.py
echo "=== SESSION3 DONE ==="
