"""Mutual-proximity (MP) rescaled novelty — the hubness robustness check.

Why: in high-dimensional embedding spaces some points ("hubs") appear in
everyone's nearest-neighbor lists (k-occurrence skewness 91.5 in the session-1
diagnostics). A market whose nearest predecessors are hubs looks "precedented"
even if nothing is genuinely close to it. Mutual proximity (Schnitzer et al.
2012, JMLR) rescales each similarity by BOTH points' similarity distributions:
    MP(s_ij) = P(S_i < s_ij) * P(S_j < s_ij)   [Gaussian approximation]
so a neighbor counts only if each point is unusually similar FROM THE OTHER'S
perspective. Hubs (high mean similarity to everything) get discounted.

Recomputes the strict-predecessor novelty score under MP for the TRADED focal
universe (candidates = all markets), same-series/same-event predecessors
excluded (the _x variant). Output: market_id, mp_k25_x, n_prior.
"""
from __future__ import annotations

import json
import os
import time

import numpy as np
import pandas as pd
import torch

BASE = "/mnt/data/embedding_difficulty"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
OUT = f"{BASE}/novelty_mp_q.parquet"
META = f"{BASE}/novelty_mp_q_meta.json"
KMAX = 25
NEG = -2.0
BLOCK = 4096
PRIOR_CHUNK = 250_000
N_REF = 2048


def group_codes(series):
    codes, _ = pd.factorize(series, sort=False)
    codes = codes.astype(np.int64)
    miss = codes == -1
    codes[miss] = -(np.arange(miss.sum(), dtype=np.int64) + 1)
    return codes


def naive_us(s):
    s = pd.to_datetime(s)
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_localize(None)
    return s.astype("datetime64[us]")


def main():
    torch.set_num_threads(max(os.cpu_count() - 8, 8))
    t0 = time.time()
    uni = pd.read_parquet(f"{BASE}/universe_markets.parquet") \
            .sort_values("market_id").reset_index(drop=True)
    emb = np.load(f"{BASE}/emb_q.npy")
    n = len(uni)
    assert len(emb) == n

    birth = naive_us(uni["created_at"])
    birth = birth.fillna(naive_us(uni["first_trade_at"]))
    birth_ts = birth.astype("int64").to_numpy()
    ser = group_codes(uni["series_slug"])
    evt = group_codes(uni["event_slug"])

    order = np.argsort(birth_ts, kind="stable")
    emb_o = np.ascontiguousarray(emb[order]).astype(np.float32)
    ts_o = birth_ts[order]
    ser_o = torch.from_numpy(ser[order])
    evt_o = torch.from_numpy(evt[order])

    # focal = traded markets (same universe the calibration runs use)
    traded = set(pd.read_parquet(DIMS, columns=["condition_id", "n_trades_full"])
                 .query("n_trades_full >= 1")["condition_id"])
    focal_mask_orig = uni["market_id"].isin(traded).to_numpy()
    focal_o = focal_mask_orig[order]
    focal_rows = np.where(focal_o)[0]
    print(f"universe {n:,}; focal (traded) {len(focal_rows):,}", flush=True)

    E = torch.from_numpy(emb_o)

    # per-point similarity distribution vs a fixed random reference set
    rng = np.random.default_rng(20260704)
    ref_idx = rng.choice(n, N_REF, replace=False)
    R = E[ref_idx]
    mu = torch.empty(n)
    sd = torch.empty(n)
    with torch.no_grad():
        for bs in range(0, n, 100_000):
            be = min(bs + 100_000, n)
            G = E[bs:be] @ R.T
            mu[bs:be] = G.mean(1)
            sd[bs:be] = G.std(1).clamp(min=1e-3)
    print(f"mu/sd reference done ({time.time()-t0:.0f}s); "
          f"mu range [{mu.min():.3f},{mu.max():.3f}]", flush=True)

    top_mp = torch.full((len(focal_rows), KMAX), NEG)
    n_prior = np.zeros(len(focal_rows), dtype=np.int64)

    with torch.no_grad():
        for fs in range(0, len(focal_rows), BLOCK):
            fe = min(fs + BLOCK, len(focal_rows))
            rows = focal_rows[fs:fe]
            B = len(rows)
            bound_np = np.searchsorted(ts_o, ts_o[rows], side="left")
            n_prior[fs:fe] = bound_np
            pmax = int(bound_np.max())
            if pmax == 0:
                continue
            bound = torch.from_numpy(bound_np).unsqueeze(1)
            q = E[rows]
            zq_mu = mu[rows].unsqueeze(1)
            zq_sd = sd[rows].unsqueeze(1)
            for cs in range(0, pmax, PRIOR_CHUNK):
                ce = min(cs + PRIOR_CHUNK, pmax)
                S = q @ E[cs:ce].T
                MP = (torch.special.ndtr((S - zq_mu) / zq_sd)
                      * torch.special.ndtr((S - mu[cs:ce].unsqueeze(0))
                                           / sd[cs:ce].unsqueeze(0)))
                col = torch.arange(cs, ce).unsqueeze(0)
                same = ((ser_o[cs:ce].unsqueeze(0) == ser_o[rows].unsqueeze(1))
                        | (evt_o[cs:ce].unsqueeze(0) == evt_o[rows].unsqueeze(1)))
                ok = (col < bound) & ~same
                MP = torch.where(ok, MP, torch.tensor(NEG))
                k = min(KMAX, MP.shape[1])
                cand, _ = torch.topk(MP, k, dim=1)
                if k < KMAX:
                    cand = torch.cat([cand, torch.full((B, KMAX - k), NEG)], 1)
                merged = torch.cat([top_mp[fs:fe], cand], 1)
                top_mp[fs:fe], _ = torch.topk(merged, KMAX, dim=1)
            if (fs // BLOCK) % 10 == 0:
                rate = fe / max(time.time() - t0, 1e-9)
                print(f"  {fe:,}/{len(focal_rows):,} focal "
                      f"({rate:,.0f}/s, {(time.time()-t0)/60:.1f} min)",
                      flush=True)

    tm = top_mp.numpy().astype(np.float64)
    cnt = (tm > NEG + 1).sum(1)
    s = np.where(tm > NEG + 1, tm, 0.0)
    with np.errstate(invalid="ignore"):
        mp_k25 = s.sum(1) / np.maximum(cnt, 1)
    mp_k25[cnt == 0] = np.nan

    out = pd.DataFrame({
        "market_id": uni["market_id"].to_numpy()[order][focal_rows],
        "mp_k25_x": mp_k25,
        "n_prior": n_prior,
    })
    out.to_parquet(OUT, index=False)
    json.dump({"n_focal": int(len(focal_rows)), "n_ref": N_REF,
               "elapsed_min": round((time.time() - t0) / 60, 1)},
              open(META, "w"))
    print(f"wrote {OUT}: {len(out):,} rows, "
          f"{(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
