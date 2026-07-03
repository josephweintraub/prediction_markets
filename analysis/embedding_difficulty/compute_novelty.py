"""Precedent density / novelty-at-birth from market-text embeddings.

For each market m with birth time t_m, similarity is computed ONLY against
markets born strictly before t_m (no lookahead; equal timestamps excluded —
same-batch listings are not precedents). Birth = native created_at, fallback
first_trade_at (flagged).

Measures per market (Tetlock 2011 fixed-k, Hoberg-Phillips threshold count):
  sim_k1, sim_k5, sim_k25      mean cosine similarity to top-k predecessors
  cnt_tau                      # predecessors with sim >= tau
  *_x variants                 excluding same-series / same-event predecessors
  share_top25_same_group       share of top-25 neighbors in same series/event
  n_prior / n_prior_valid      # predecessors (all / with valid text)
  top1_row / top1_sim          nearest predecessor (for qualitative audit)
tau is calibrated as the q-th percentile of random valid-pair similarity.

v2 (2026-07-03): inner loop ported to torch (GEMM, masking, topk all
multithreaded — the numpy version spent ~2h single-threaded on masks), and an
optional --mask (bool npy) excludes empty-text markets as focal AND candidate
(their rows get NaN). Interface and outputs unchanged otherwise.

Run AFTER embed_universe.py / embed_fields.py. Example:
  python compute_novelty.py --emb .../emb_context.npy \
      --mask .../emb_context_mask.npy --out .../novelty_context.parquet \
      --neighbors-out .../neighbors_context.npy --meta-out .../novelty_context_meta.json
"""
from __future__ import annotations
import argparse
import json
import os
import time

import numpy as np
import pandas as pd
import torch

K_LIST = (1, 5, 25)
KMAX = 25
NEG = -2.0


def group_codes(series: pd.Series) -> np.ndarray:
    """Factorize with NaN/None -> unique negative codes (never match)."""
    codes, _ = pd.factorize(series, sort=False)
    codes = codes.astype(np.int64)
    miss = codes == -1
    codes[miss] = -(np.arange(miss.sum(), dtype=np.int64) + 1)
    return codes


def naive_us(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s)
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_localize(None)
    return s.astype("datetime64[us]")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default=f"/mnt/data/embedding_difficulty/universe_markets.parquet")
    ap.add_argument("--emb", default="/mnt/data/embedding_difficulty/emb_q.npy")
    ap.add_argument("--mask", default=None, help="bool npy; False rows excluded")
    ap.add_argument("--out", default="/mnt/data/embedding_difficulty/novelty.parquet")
    ap.add_argument("--neighbors-out", default="/mnt/data/embedding_difficulty/neighbors_top25.npy")
    ap.add_argument("--meta-out", default="/mnt/data/embedding_difficulty/novelty_meta.json")
    ap.add_argument("--tau-quantile", type=float, default=0.995)
    ap.add_argument("--block", type=int, default=4096)
    ap.add_argument("--prior-chunk", type=int, default=250_000)
    args = ap.parse_args()

    torch.set_num_threads(os.cpu_count())
    t0 = time.time()
    uni = pd.read_parquet(args.universe).sort_values("market_id").reset_index(drop=True)
    emb = np.load(args.emb)
    assert len(uni) == len(emb), (len(uni), len(emb))
    n = len(uni)
    valid = (np.load(args.mask) if args.mask
             else np.ones(n, dtype=bool))

    birth = naive_us(uni["created_at"])
    fallback = birth.isna()
    birth = birth.fillna(naive_us(uni["first_trade_at"]))
    birth_ts = birth.astype("int64").to_numpy()
    series_c = group_codes(uni["series_slug"])
    event_c = group_codes(uni["event_slug"])

    order = np.argsort(birth_ts, kind="stable")
    emb_o = np.ascontiguousarray(emb[order])
    ts_o = birth_ts[order]
    ser_o = series_c[order]
    evt_o = event_c[order]
    valid_o = valid[order]
    nv_cum = np.concatenate([[0], np.cumsum(valid_o)])

    # tau from random valid cross pairs
    rng = np.random.default_rng(20260703)
    vidx = np.where(valid_o)[0]
    ii = rng.choice(vidx, 400_000)
    jj = rng.choice(vidx, 400_000)
    keep = ii != jj
    pair_sims = np.einsum("ij,ij->i", emb_o[ii[keep]], emb_o[jj[keep]])
    tau = float(np.quantile(pair_sims, args.tau_quantile))
    print(f"tau (q={args.tau_quantile}) = {tau:.4f} | random-pair sim "
          f"median={np.median(pair_sims):.4f} | valid={valid.sum():,}/{n:,}",
          flush=True)

    E = torch.from_numpy(emb_o)
    ser_t = torch.from_numpy(ser_o)
    evt_t = torch.from_numpy(evt_o)
    val_t = torch.from_numpy(valid_o)

    top_sims = torch.full((n, KMAX), NEG)
    top_idx = torch.full((n, KMAX), -1, dtype=torch.int64)
    top_sims_x = torch.full((n, KMAX), NEG)
    cnt_tau = torch.zeros(n, dtype=torch.int64)
    cnt_tau_x = torch.zeros(n, dtype=torch.int64)
    n_prior = np.zeros(n, dtype=np.int64)

    with torch.no_grad():
        for bs in range(0, n, args.block):
            be = min(bs + args.block, n)
            B = be - bs
            bound_np = np.searchsorted(ts_o, ts_o[bs:be], side="left")
            n_prior[bs:be] = bound_np
            pmax = int(bound_np.max())
            if pmax == 0:
                continue
            bound = torch.from_numpy(bound_np).unsqueeze(1)
            q = E[bs:be]
            for cs in range(0, pmax, args.prior_chunk):
                ce = min(cs + args.prior_chunk, pmax)
                S = q @ E[cs:ce].T
                col = torch.arange(cs, ce).unsqueeze(0)
                ok = (col < bound) & val_t[cs:ce].unsqueeze(0)
                S = torch.where(ok, S, torch.tensor(NEG))
                cnt_tau[bs:be] += (S >= tau).sum(1)
                k = min(KMAX, S.shape[1])
                cand_s, ci = torch.topk(S, k, dim=1)
                cand_i = ci + cs
                if k < KMAX:
                    pad_s = torch.full((B, KMAX - k), NEG)
                    pad_i = torch.full((B, KMAX - k), -1, dtype=torch.int64)
                    cand_s = torch.cat([cand_s, pad_s], 1)
                    cand_i = torch.cat([cand_i, pad_i], 1)
                ms = torch.cat([top_sims[bs:be], cand_s], 1)
                mi = torch.cat([top_idx[bs:be], cand_i], 1)
                ts_new, sel = torch.topk(ms, KMAX, dim=1)
                top_sims[bs:be] = ts_new
                top_idx[bs:be] = torch.gather(mi, 1, sel)
                # exclusion variant: same series OR same event never counts
                same = ((ser_t[cs:ce].unsqueeze(0) == ser_t[bs:be].unsqueeze(1))
                        | (evt_t[cs:ce].unsqueeze(0) == evt_t[bs:be].unsqueeze(1)))
                Sx = torch.where(same, torch.tensor(NEG), S)
                cnt_tau_x[bs:be] += (Sx >= tau).sum(1)
                cand_sx, _ = torch.topk(Sx, k, dim=1)
                if k < KMAX:
                    cand_sx = torch.cat(
                        [cand_sx, torch.full((B, KMAX - k), NEG)], 1)
                msx = torch.cat([top_sims_x[bs:be], cand_sx], 1)
                top_sims_x[bs:be], _ = torch.topk(msx, KMAX, dim=1)
            if (bs // args.block) % 20 == 0:
                rate = be / max(time.time() - t0, 1e-9)
                print(f"  {be:,}/{n:,} ({rate:,.0f} mkts/s, "
                      f"{(time.time()-t0)/60:.1f} min)", flush=True)

    top_sims = top_sims.numpy()
    top_idx = top_idx.numpy()
    top_sims_x = top_sims_x.numpy()
    cnt_tau = cnt_tau.numpy()
    cnt_tau_x = cnt_tau_x.numpy()

    def kmean(sims, k):
        s = sims[:, :k].astype(np.float64)
        cntv = (s > NEG + 1).sum(axis=1)
        s = np.where(s > NEG + 1, s, 0.0)
        with np.errstate(invalid="ignore"):
            out = s.sum(axis=1) / np.maximum(cntv, 1)
        out[cntv == 0] = np.nan
        return out

    valid_nb = top_idx >= 0
    same_grp = np.zeros_like(top_idx, dtype=bool)
    vi = np.where(valid_nb)
    same_grp[vi] = ((ser_o[top_idx[vi]] == ser_o[vi[0]])
                    | (evt_o[top_idx[vi]] == evt_o[vi[0]]))
    with np.errstate(invalid="ignore"):
        share_same = same_grp.sum(axis=1) / np.maximum(valid_nb.sum(axis=1), 1)
    share_same[valid_nb.sum(axis=1) == 0] = np.nan

    inv = np.empty(n, dtype=np.int64)
    inv[order] = np.arange(n)
    out = pd.DataFrame({"market_id": uni["market_id"]})
    out["birth_at"] = birth
    out["birth_fallback"] = fallback.to_numpy()
    res = {
        "n_prior": n_prior,
        "n_prior_valid": nv_cum[n_prior],
        "cnt_tau": cnt_tau, "cnt_tau_x": cnt_tau_x,
        "sim_k1": kmean(top_sims, 1), "sim_k5": kmean(top_sims, 5),
        "sim_k25": kmean(top_sims, 25),
        "sim_k1_x": kmean(top_sims_x, 1), "sim_k5_x": kmean(top_sims_x, 5),
        "sim_k25_x": kmean(top_sims_x, 25),
        "share_top25_same_group": share_same,
        "top1_sim": np.where(top_sims[:, 0] > NEG + 1, top_sims[:, 0], np.nan),
    }
    focal_bad = ~valid_o
    for k_, v in res.items():
        v = v.astype(np.float64) if v.dtype != np.float64 else v
        v = v.copy()
        if k_ not in ("n_prior", "n_prior_valid"):
            v[focal_bad] = np.nan
        out[k_] = v[inv]
    nb = np.where(top_idx >= 0, order[np.clip(top_idx, 0, None)], -1)
    out["top1_row"] = nb[:, 0][inv]
    np.save(args.neighbors_out, nb[inv].astype(np.int32))
    out.to_parquet(args.out, index=False)
    with open(args.meta_out, "w") as f:
        json.dump({"tau": tau, "tau_quantile": args.tau_quantile, "n": int(n),
                   "n_valid": int(valid.sum()), "kmax": KMAX,
                   "emb": args.emb, "mask": args.mask,
                   "runtime_min": (time.time() - t0) / 60,
                   "birth_fallback_n": int(fallback.sum())}, f, indent=2)
    print(f"done in {(time.time()-t0)/60:.1f} min -> {args.out}", flush=True)


if __name__ == "__main__":
    main()
