#!/bin/bash
# Follow-up sequencer: fires after compute_novelty.py and the 12-scheme
# mature FLB run both complete; finishes all remaining session steps.
set -e
PY=/home/ubuntu/venv/bin/python
D=/home/ubuntu/prediction_markets/analysis/embedding_difficulty
B=/mnt/data/embedding_difficulty
cd $D
echo "=== waiting for novelty.parquet ==="
until [ -f $B/novelty.parquet ]; do sleep 60; done
sleep 10
echo "=== novelty diagnostics ==="
$PY novelty_diagnostics.py
echo "=== novelty slices ==="
$PY make_novelty_slices.py
echo "=== FLB mature (novelty schemes) ==="
$PY run_schemes.py --window mature --schemes nov_k25 nov_k25x nov_k25x_vint nov_cnt
echo "=== waiting for 12-scheme mature run ==="
until grep -q "all schemes done" /mnt/data/ed_flb_mature.log; do sleep 60; done
echo "=== FLB closing (key schemes) ==="
$PY run_schemes.py --window closing --schemes all category series_membership nov_k25 nov_k25x nov_k25x_vint nov_cnt cluster_k12 cluster_k50 pca_pc1_quintile pca_pc2_quintile pca_pc3_quintile pca_pc4_quintile actsubj_2x2
echo "=== render report ==="
$PY render_report.py
echo "=== FOLLOWUP DONE ==="
