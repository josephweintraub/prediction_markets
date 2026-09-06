"""Embed per-field market texts (rules / context) with unique-text dedup.

Fields:
  rules   — market description (resolution criteria), from universe_markets
  context — EVENT-level description from native_market_meta (event_description):
            the closest native field to "market context"; shared across sibling
            markets of one event

Both fields are truncated to --chars characters and deduplicated before
encoding (rules are heavily templated; context is event-level), then mapped
back to one row per market, row-aligned to universe_markets sorted by
market_id — the same alignment as emb_q.npy.

Markets with an empty field get a ZERO vector and mask=False in
emb_<field>_mask.npy; downstream novelty must exclude them as focal and as
candidates.

Run on EC2:
  python embed_fields.py --field rules
  python embed_fields.py --field context
"""
from __future__ import annotations
import argparse
import time

import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
NATIVE_META = "/mnt/data/learnability/native/native_market_meta.parquet"


def load_texts(field: str) -> pd.Series:
    uni = pd.read_parquet(f"{BASE}/universe_markets.parquet",
                          columns=["market_id", "description"]) \
            .sort_values("market_id").reset_index(drop=True)
    if field == "rules":
        return uni["description"].fillna("").str.strip()
    if field == "context":
        import duckdb
        con = duckdb.connect()
        ev = con.execute(f"""
            SELECT condition_id AS market_id, event_description
            FROM read_parquet('{NATIVE_META}')
        """).fetchdf()
        m = uni.merge(ev, on="market_id", how="left")
        return m["event_description"].fillna("").str.strip()
    raise ValueError(field)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--field", required=True, choices=["rules", "context"])
    ap.add_argument("--model", default="BAAI/bge-small-en-v1.5")
    ap.add_argument("--chars", type=int, default=800)
    ap.add_argument("--batch-size", type=int, default=128)
    args = ap.parse_args()

    texts = load_texts(args.field).str.slice(0, args.chars)
    mask = (texts.str.len() > 0).to_numpy()
    uniq, inv = np.unique(texts.to_numpy(), return_inverse=True)
    print(f"{args.field}: {len(texts):,} markets, {mask.sum():,} non-empty, "
          f"{len(uniq):,} unique texts", flush=True)

    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(args.model, device="cpu")
    t0 = time.time()
    emb_u = model.encode(
        list(uniq), batch_size=args.batch_size, normalize_embeddings=True,
        show_progress_bar=True, convert_to_numpy=True).astype(np.float32)
    print(f"encoded {len(uniq):,} uniques in {(time.time()-t0)/60:.1f} min",
          flush=True)

    emb = emb_u[inv]
    emb[~mask] = 0.0
    np.save(f"{BASE}/emb_{args.field}.npy", emb)
    np.save(f"{BASE}/emb_{args.field}_mask.npy", mask)
    print(f"saved emb_{args.field}.npy {emb.shape} + mask "
          f"({mask.sum():,} valid)", flush=True)


if __name__ == "__main__":
    main()
