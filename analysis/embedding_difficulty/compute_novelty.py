"""Precedent density / novelty-at-birth from market-text embeddings.

For each market m with birth time t_m, similarity is computed ONLY against
markets born strictly before t_m (no lookahead; equal timestamps excluded —
same-batch listings are not precedents). Birth = native created_at, fallback
first_trade_at (flagged).

Measures per market (following Tetlock 2011 fixed-k, Hoberg-Phillips threshold
count, and rank/percentile practice from the embedding-novelty literature):
  sim_k1, sim_k5, sim_k25      mean cosine similarity to top-k predecessors
  cnt_tau                      # predecessors with sim >= tau
  *_x variants                 excluding same-series / same-event predecessors
  share_top25_same_group       share of top-25 neighbors in same series/event
  n_prior                      # predecessors (control for platform growth)
  top1_idx / top1_sim          nearest predecessor (for qualitative audit)
tau is calibrated as the q-th percentile of random cross-pair similarity
(default 0.995), logged to the meta json.

Outputs: novelty.parquet (row-aligned to sorted-by-market_id universe),
         neighbors_top25.npy (int32 [N,25] indices into the same row order,
         -1 padded; for hubness diagnostics), novelty_meta.json.

Run AFTER embed_universe.py. CPU GEMM-blocked exact search.
"""
from __future__ import annotations
import argparse
import json
import time

import numpy as np
import pandas as pd

K_LIST = (1, 5, 25)
KMAX = 25


def group_codes(series: pd.Series) -> np.ndarray:
    """Factorize with NaN/None -> unique negative codes (never match)."""
    codes, _ = pd.factorize(series, sort=False)
    codes = codes.astype(np.int64)
    miss = codes == -1
    codes[miss] = -(np.arange(miss.sum(), dtype=np.int64) + 1)
    return codes


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="/mnt/data/embedding_difficulty/universe_markets.parquet")
    ap.add_argument("--emb", default="/mnt/data/embedding_difficulty/emb_q.npy")
    ap.add_argument("--out", default="/mnt/data/embedding_difficulty/novelty.parquet")
    ap.add_argument("--neighbors-out", default="/mnt/data/embedding_difficulty/neighbors_top25.npy")
    ap.add_argument("--meta-out", default="/mnt/data/embedding_difficulty/novelty_meta.json")
    ap.add_argument("--tau-quantile", type=float, default=0.995)
    ap.add_argument("--block", type=int, default=2048)
    ap.add_argument("--prior-chunk", type=int, default=200_000)
    args = ap.parse_args()

    t0 = time.time()
    uni = pd.read_parquet(args.universe).sort_values("market_id").reset_index(drop=True)
    emb = np.load(args.emb)
    assert len(uni) == len(emb), (len(uni), len(emb))
    n = len(uni)

    def naive_us(s: pd.Series) -> pd.Series:
        s = pd.to_datetime(s)
        if getattr(s.dt, "tz", None) is not None:
            s = s.dt.tz_localize(None)
        return s.astype("datetime64[us]")

    birth = naive_us(uni["created_at"])
    fallback = birth.isna()
    birth = birth.fillna(naive_us(uni["first_trade_at"]))
    birth_ts = birth.astype("int64").to_numpy()  # microseconds
    series_c = group_codes(uni["series_slug"])
    event_c = group_codes(uni["event_slug"])

    order = np.argsort(birth_ts, kind="stable")
    emb_o = np.ascontiguousarray(emb[order])
    ts_o = birth_ts[order]
    ser_o = series_c[order]
    evt_o = event_c[order]

    # tau from random cross pairs
    rng = np.random.default_rng(20260703)
    ii = rng.integers(0, n, 400_000)
    jj = rng.integers(0, n, 400_000)
    keep = ii != jj
    pair_sims = np.einsum("ij,ij->i", emb_o[ii[keep]], emb_o[jj[keep]])
    tau = float(np.quantile(pair_sims, args.tau_quantile))
    print(f"tau (q={args.tau_quantile}) = {tau:.4f} | random-pair sim "
          f"median={np.median(pair_sims):.4f}", flush=True)

    NEG = -2.0
    top_sims = np.full((n, KMAX), NEG, dtype=np.float32)
    top_idx = np.full((n, KMAX), -1, dtype=np.int64)
    top_sims_x = np.full((n, KMAX), NEG, dtype=np.float32)
    cnt_tau = np.zeros(n, dtype=np.int64)
    cnt_tau_x = np.zeros(n, dtype=np.int64)
    n_prior = np.zeros(n, dtype=np.int64)

    def merge_top(cur_s, cur_i, new_s, new_i):
        """Row-wise merge of running top-K with candidate top-K."""
        s = np.concatenate([cur_s, new_s], axis=1)
        i = np.concatenate([cur_i, new_i], axis=1)
        sel = np.argpartition(-s, KMAX - 1, axis=1)[:, :KMAX]
        r = np.arange(s.shape[0])[:, None]
        return s[r, sel], i[r, sel]

    def merge_top_sims(cur_s, new_s):
        """Row-wise merge of running top-K sims (no index tracking)."""
        s = np.concatenate([cur_s, new_s], axis=1)
        sel = np.argpartition(-s, KMAX - 1, axis=1)[:, :KMAX]
        return s[np.arange(s.shape[0])[:, None], sel]

    done = 0
    for bs in range(0, n, args.block):
        be = min(bs + args.block, n)
        B = be - bs
        # prior boundary per row: first index with ts >= own ts (strict predecessor)
        bound = np.searchsorted(ts_o, ts_o[bs:be], side="left")  # ascending in-block
        n_prior[bs:be] = bound
        pmax = int(bound.max())
        if pmax == 0:
            done = be
            continue
        q = emb_o[bs:be]
        for cs in range(0, pmax, args.prior_chunk):
            ce = min(cs + args.prior_chunk, pmax)
            S = q @ emb_o[cs:ce].T  # B x (ce-cs)
            # mask columns at/after each row's boundary
            col = np.arange(cs, ce)
            valid = col[None, :] < bound[:, None]
            S = np.where(valid, S, NEG).astype(np.float32)
            cnt_tau[bs:be] += (S >= tau).sum(axis=1)
            k = min(KMAX, S.shape[1])
            sel = np.argpartition(-S, k - 1, axis=1)[:, :k]
            r = np.arange(B)[:, None]
            cand_s = S[r, sel]
            cand_i = col[sel]
            if k < KMAX:
                pad = np.full((B, KMAX - k), NEG, dtype=np.float32)
                cand_s = np.concatenate([cand_s, pad], axis=1)
                cand_i = np.concatenate([cand_i, np.full((B, KMAX - k), -1)], axis=1)
            top_sims[bs:be], top_idx[bs:be] = merge_top(
                top_sims[bs:be], top_idx[bs:be], cand_s, cand_i)
            # exclusion variant: same series OR same event never counts
            same = ((ser_o[cs:ce][None, :] == ser_o[bs:be][:, None])
                    | (evt_o[cs:ce][None, :] == evt_o[bs:be][:, None]))
            Sx = np.where(same, NEG, S)
            cnt_tau_x[bs:be] += (Sx >= tau).sum(axis=1)
            selx = np.argpartition(-Sx, k - 1, axis=1)[:, :k]
            cand_sx = Sx[r, selx]
            top_sims_x[bs:be] = merge_top_sims(top_sims_x[bs:be], cand_sx)
        done = be
        if (bs // args.block) % 20 == 0:
            rate = done / max(time.time() - t0, 1e-9)
            print(f"  {done:,}/{n:,} ({rate:,.0f} mkts/s, "
                  f"{(time.time()-t0)/60:.1f} min)", flush=True)

    # sort each row's top-K descending for clean k-prefix means
    ordk = np.argsort(-top_sims, axis=1)
    r = np.arange(n)[:, None]
    top_sims = top_sims[r, ordk]
    top_idx = top_idx[r, ordk]
    top_sims_x = np.sort(top_sims_x, axis=1)[:, ::-1]

    def kmean(sims, k):
        s = sims[:, :k].astype(np.float64)
        cntv = (s > NEG + 1).sum(axis=1)
        s = np.where(s > NEG + 1, s, 0.0)
        with np.errstate(invalid="ignore"):
            out = s.sum(axis=1) / np.maximum(cntv, 1)
        out[cntv == 0] = np.nan
        return out

    # share of top-25 neighbors in same series/event
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
        "cnt_tau": cnt_tau, "cnt_tau_x": cnt_tau_x,
        "sim_k1": kmean(top_sims, 1), "sim_k5": kmean(top_sims, 5),
        "sim_k25": kmean(top_sims, 25),
        "sim_k1_x": kmean(top_sims_x, 1), "sim_k5_x": kmean(top_sims_x, 5),
        "sim_k25_x": kmean(top_sims_x, 25),
        "share_top25_same_group": share_same,
        "top1_sim": np.where(top_sims[:, 0] > NEG + 1, top_sims[:, 0], np.nan),
    }
    for k, v in res.items():
        out[k] = v[inv]
    # neighbor idx: convert sorted-order indices back to universe row indices
    nb = np.where(top_idx >= 0, order[np.clip(top_idx, 0, None)], -1)
    out["top1_row"] = nb[:, 0][inv]
    np.save(args.neighbors_out, nb[inv].astype(np.int32))
    out.to_parquet(args.out, index=False)
    with open(args.meta_out, "w") as f:
        json.dump({"tau": tau, "tau_quantile": args.tau_quantile, "n": int(n),
                   "kmax": KMAX, "runtime_min": (time.time() - t0) / 60,
                   "birth_fallback_n": int(fallback.sum())}, f, indent=2)
    print(f"done in {(time.time()-t0)/60:.1f} min -> {args.out}", flush=True)


if __name__ == "__main__":
    main()
