#!/bin/bash
# Session 7b: re-baseline trade-side artifacts on refreshed canonical data
# (universe frozen to the same June-24 spine -> same 850,015 markets).
set -e
PY=/home/ubuntu/venv/bin/python
cd /home/ubuntu/prediction_markets/analysis/embedding_difficulty
echo "=== rebuild universe (same spine, current trades, new bot list) ==="
$PY build_universe.py
$PY - <<'EOF'
import json
cov = json.load(open("/mnt/data/embedding_difficulty/build_universe_coverage.json"))
n = cov["markets_written"]
print(f"markets_written = {n:,}")
assert n >= 850015, f"UNIVERSE SHRANK: {n} < 850015 — STOP AND REASSESS"
print(f"universe grew by {n-850015:,} newly resolved markets (refreshed spine);"
      " embedding-positional artifacts remain pinned to the original 850,015 —"
      " top up embeddings before any embedding rerun")
EOF
echo "=== rebuild base tables ==="
$PY build_flb_base.py
echo "=== rebuild viability-dependent schemes ==="
$PY make_baseline_slices.py
$PY make_novelty_slices.py
$PY make_liquidity_slices.py
$PY make_horizon_slices.py
$PY make_actsubj_slices.py
$PY make_liq_horizon_slices.py
$PY make_timeliq_slices.py
echo "=== FLB full window (liquidity/maturity set) ==="
$PY run_schemes.py --window full --schemes all horizon liqrate liqrate_vint hor_x_liqrate horizon_binary horizon_final2 liq1d liq7d liq30d hor_x_liq1d
echo "=== render brief (full, single vintage) ==="
$PY render_liq_maturity_brief.py
echo "=== SESSION7B DONE ==="
