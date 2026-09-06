"""Build combined-field embedding variants from per-field embeddings.

Combined embedding = L2-normalized weighted sum of the (already normalized)
per-field embeddings, with weights RENORMALIZED per market over the fields
that are actually available (question is always available; rules/context can
be empty → excluded via their masks). Weights are PRE-REGISTERED, not tuned:
  comb_eq : question 1/3, rules 1/3, context 1/3
  comb_qc : question 0.45, rules 0.10, context 0.45  (prior: rules least
            informative)

Inputs : emb_q.npy, emb_rules.npy + emb_rules_mask.npy,
         emb_context.npy + emb_context_mask.npy  (row-aligned)
Outputs: emb_comb_eq.npy, emb_comb_qc.npy (+ *_mask.npy = all-True)
"""
from __future__ import annotations
import numpy as np

BASE = "/mnt/data/embedding_difficulty"
WEIGHTS = {"comb_eq": (1 / 3, 1 / 3, 1 / 3), "comb_qc": (0.45, 0.10, 0.45)}

eq = np.load(f"{BASE}/emb_q.npy")
er = np.load(f"{BASE}/emb_rules.npy")
ec = np.load(f"{BASE}/emb_context.npy")
mr = np.load(f"{BASE}/emb_rules_mask.npy")
mc = np.load(f"{BASE}/emb_context_mask.npy")
n = len(eq)
assert len(er) == n and len(ec) == n

for name, (wq, wr, wc) in WEIGHTS.items():
    w_q = np.full(n, wq, dtype=np.float32)
    w_r = np.where(mr, wr, 0.0).astype(np.float32)
    w_c = np.where(mc, wc, 0.0).astype(np.float32)
    tot = w_q + w_r + w_c
    comb = (eq * (w_q / tot)[:, None] + er * (w_r / tot)[:, None]
            + ec * (w_c / tot)[:, None])
    norm = np.linalg.norm(comb, axis=1, keepdims=True)
    comb = (comb / np.maximum(norm, 1e-9)).astype(np.float32)
    np.save(f"{BASE}/emb_{name}.npy", comb)
    np.save(f"{BASE}/emb_{name}_mask.npy", np.ones(n, dtype=bool))
    print(f"{name}: saved {comb.shape}", flush=True)
