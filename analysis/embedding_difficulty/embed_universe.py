"""Embed the market-text universe for the embedding-difficulty workstream.

Input : universe parquet (from build_universe.py) with one row per market
        (market grain = 0x condition_id), columns at minimum:
          market_id, question, description, created_at
Output: <out_prefix>_<variant>.npy      float32 [n_markets, dim] L2-normalized
        <out_prefix>_ids.parquet        row-aligned market_id + text lengths

Variants:
  q  — question only
  qd — question + " || " + description (rules text), description truncated

Run on EC2 (CPU). Example:
  /home/ubuntu/venv/bin/python embed_universe.py \
      --universe /mnt/data/embedding_difficulty/universe.parquet \
      --out-prefix /mnt/data/embedding_difficulty/emb \
      --model BAAI/bge-small-en-v1.5 --variant q
"""
from __future__ import annotations
import argparse
import time

import numpy as np
import pandas as pd


def build_texts(df: pd.DataFrame, variant: str, desc_chars: int) -> list[str]:
    q = df["question"].fillna("").str.strip()
    if variant == "q":
        return q.tolist()
    if variant == "qd":
        d = df["description"].fillna("").str.strip().str.slice(0, desc_chars)
        return (q + " || " + d).str.strip(" |").tolist()
    raise ValueError(f"unknown variant {variant}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--out-prefix", required=True)
    ap.add_argument("--model", default="BAAI/bge-small-en-v1.5")
    ap.add_argument("--variant", default="q", choices=["q", "qd"])
    ap.add_argument("--batch-size", type=int, default=256)
    ap.add_argument("--desc-chars", type=int, default=1000,
                    help="truncate description to this many chars in qd variant")
    ap.add_argument("--limit", type=int, default=0, help="debug: embed only first N")
    args = ap.parse_args()

    from sentence_transformers import SentenceTransformer

    df = pd.read_parquet(args.universe)
    df = df.sort_values("market_id").reset_index(drop=True)  # stable row order
    if args.limit:
        df = df.head(args.limit)
    texts = build_texts(df, args.variant, args.desc_chars)
    print(f"{len(texts):,} texts | variant={args.variant} | model={args.model}", flush=True)

    model = SentenceTransformer(args.model, device="cpu")
    t0 = time.time()
    emb = model.encode(
        texts,
        batch_size=args.batch_size,
        normalize_embeddings=True,
        show_progress_bar=True,
        convert_to_numpy=True,
    ).astype(np.float32)
    dt = time.time() - t0
    print(f"embedded in {dt/60:.1f} min ({len(texts)/max(dt,1e-9):.0f} texts/s), dim={emb.shape[1]}", flush=True)

    np.save(f"{args.out_prefix}_{args.variant}.npy", emb)
    ids = df[["market_id"]].copy()
    ids["q_len"] = df["question"].fillna("").str.len()
    ids["d_len"] = df["description"].fillna("").str.len()
    ids.to_parquet(f"{args.out_prefix}_ids.parquet", index=False)
    print(f"saved {args.out_prefix}_{args.variant}.npy + _ids.parquet", flush=True)


if __name__ == "__main__":
    main()
