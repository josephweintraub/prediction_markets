"""Run FLB-per-slice for every slicing scheme in a directory.

A scheme is a parquet file `scheme_<name>.parquet` with columns
  market_id (0x hex), slice (string or int)
Every scheme is joined against the compact base table for the chosen
lifecycle window and measured with flb_engine.compute_slice (signed slope
primary, D10-D1 secondary, CGM 3-way clustered SEs, 5,000-trade floor).

Usage (EC2):
  python run_schemes.py --window mature [--schemes s1 s2 ...]
Outputs to /mnt/data/embedding_difficulty/output/:
  flb_deciles_<scheme>_<window>.parquet
  flb_summary_<scheme>_<window>.parquet
  flb_dropped_<scheme>_<window>.parquet   (slices under the trade floor)
"""
from __future__ import annotations
import argparse
import glob
import os
import time

import duckdb
import numpy as np
import pandas as pd

from flb_engine import compute_slice, sig_stars

BASE_DIR = "/mnt/data/embedding_difficulty"
OUT_DIR = f"{BASE_DIR}/output"
MIN_TRADES = 5000


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", required=True, choices=["mature", "closing"])
    ap.add_argument("--schemes", nargs="*", default=None,
                    help="scheme names (default: every scheme_*.parquet)")
    ap.add_argument("--min-trades", type=int, default=MIN_TRADES)
    args = ap.parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)
    t0 = time.time()

    con = duckdb.connect()
    con.execute(f"SET threads TO {os.cpu_count()}")

    print(f"loading base ({args.window})", flush=True)
    base = con.execute(f"""
        SELECT market_code, token_code, wallet_code, day, price, ret, won, usdc,
               LEAST(FLOOR(price * 10)::INT, 9) + 1 AS decile
        FROM read_parquet('{BASE_DIR}/flb_base_{args.window}.parquet')
    """).fetchdf()
    print(f"  {len(base):,} rows in {time.time()-t0:.0f}s", flush=True)

    mkt_map = con.execute(f"""
        SELECT code AS market_code, value AS market_id
        FROM read_parquet('{BASE_DIR}/code_maps.parquet') WHERE kind = 'market'
    """).fetchdf()
    id2code = dict(zip(mkt_map["market_id"], mkt_map["market_code"]))
    max_code = int(mkt_map["market_code"].max())

    files = sorted(glob.glob(f"{BASE_DIR}/schemes/scheme_*.parquet"))
    if args.schemes:
        want = set(args.schemes)
        files = [f for f in files
                 if os.path.basename(f)[len("scheme_"):-len(".parquet")] in want]
    print(f"{len(files)} schemes: "
          f"{[os.path.basename(f) for f in files]}", flush=True)

    mc = base["market_code"].to_numpy()
    for f in files:
        scheme = os.path.basename(f)[len("scheme_"):-len(".parquet")]
        sm = pd.read_parquet(f)
        sl_labels, sl_codes = np.unique(sm["slice"].astype(str), return_inverse=True)
        lookup = np.full(max_code + 1, -1, dtype=np.int32)
        codes = sm["market_id"].map(id2code)
        ok = codes.notna()
        lookup[codes[ok].astype(int).to_numpy()] = sl_codes[ok.to_numpy()]
        sl = lookup[mc]
        keep = sl >= 0
        df = base.loc[keep].copy()
        df["slice_code"] = sl[keep]
        print(f"[{scheme}|{args.window}] {len(df):,} trades, "
              f"{len(sl_labels)} defined slices", flush=True)

        out_dec, out_sum, dropped = [], [], []
        for code, sub in df.groupby("slice_code", sort=True):
            label = sl_labels[code]
            if len(sub) < args.min_trades:
                dropped.append({"scheme": scheme, "slice": label,
                                "n_trades": int(len(sub))})
                continue
            dec_rows, summary = compute_slice(sub)
            for r in dec_rows:
                out_dec.append({"scheme": scheme, "slice": label, **r})
            out_sum.append({"scheme": scheme, "slice": label, **summary})
            s = out_sum[-1]
            print(f"    {label[:42]:42s} N={s['n_trades']:>11,} "
                  f"slope={s['slope']:+.4f}(t={s['slope_t']:+.1f}"
                  f"{sig_stars(s['slope_t'])}) "
                  f"slope$={s['slope_dol']:+.4f}(t={s['slope_t_dol']:+.1f}) "
                  f"spread={s['spread']:+.4f}", flush=True)

        pd.DataFrame(out_dec).to_parquet(
            f"{OUT_DIR}/flb_deciles_{scheme}_{args.window}.parquet", index=False)
        pd.DataFrame(out_sum).to_parquet(
            f"{OUT_DIR}/flb_summary_{scheme}_{args.window}.parquet", index=False)
        pd.DataFrame(dropped).to_parquet(
            f"{OUT_DIR}/flb_dropped_{scheme}_{args.window}.parquet", index=False)
        print(f"  [{scheme}] done, {len(out_sum)} slices kept, "
              f"{len(dropped)} dropped (< {args.min_trades} trades) "
              f"[{(time.time()-t0)/60:.1f} min]", flush=True)
    print("all schemes done", flush=True)


if __name__ == "__main__":
    main()
