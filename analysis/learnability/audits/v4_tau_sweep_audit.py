"""Tau-sweep audit for the qcluster grouping (approach 3).

For each tau in a sweep:
  1. Cluster event_slug-concatenated questions via TF-IDF + HNSW + connected
     components.
  2. Compute summary stats (singleton%, cluster-size distribution, megaclusters).
  3. Randomly sample 30 clusters stratified by size and print 8 questions per
     cluster, written to a markdown audit file.

The user reads the audit, picks tau, then we use that tau in the main run.

No FLB calls here — just the clustering audit.
"""
import sys, os, json, random, time
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, "/home/ubuntu")
sys.path.insert(0, "/home/ubuntu/pipeline/analysis")
sys.path.insert(0, "/home/ubuntu/learnability")

from learnability.dimensions_v4_addons import (
    build_slug_documents, build_qcluster_index, cluster_at_tau,
)

AUG = Path("/mnt/data/learnability/stage2_per_contract_augmented.parquet")
OUT_DIR = Path("/mnt/data/learnability/output")
AUDIT_MD = OUT_DIR / "v4_tau_sweep_audit.md"
AUDIT_JSON = OUT_DIR / "v4_tau_sweep_stats.json"
CACHE_DIR = OUT_DIR / "v4_qcluster_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

TAUS = [0.55, 0.60, 0.65, 0.70, 0.75, 0.80]
SAMPLE_SEED = 42


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def stratified_sample(size_to_clusters: dict, rng: random.Random):
    """Pick 10 singletons + 10 from size 2-10 + 10 from size 50+.

    `size_to_clusters` maps cluster_id → size. Falls back to fewer if a stratum
    is sparse.
    """
    by_band = {"singleton (size=1)": [], "small (size 2-10)": [], "large (size 50+)": []}
    for cid, sz in size_to_clusters.items():
        if sz == 1:
            by_band["singleton (size=1)"].append(cid)
        elif 2 <= sz <= 10:
            by_band["small (size 2-10)"].append(cid)
        elif sz >= 50:
            by_band["large (size 50+)"].append(cid)
    picks = {}
    for band, lst in by_band.items():
        rng.shuffle(lst)
        picks[band] = lst[:10]
    return picks


def main():
    log("Loading augmented parquet…")
    aug = pd.read_parquet(AUG, columns=["token_id", "event_slug", "question"])
    log(f"  {len(aug):,} contracts")
    docs = build_slug_documents(aug)
    log(f"  built {len(docs):,} slug documents")

    rng = random.Random(SAMPLE_SEED)
    all_stats = []
    audit_lines = ["# qcluster τ-sweep audit\n"]
    audit_lines.append(f"_N event_slugs documented_: **{len(docs):,}**\n")
    audit_lines.append(
        "For each τ: summary statistics + a stratified random sample of 30 clusters "
        "(10 singletons / 10 small / 10 large), showing up to 8 distinct member questions per cluster. "
        "The chosen τ should produce clusters that (a) cohere semantically internally and (b) distinguish across clusters.\n\n"
    )

    aug_slug = aug[["event_slug", "question"]].dropna(subset=["event_slug"]).copy()
    aug_slug["question"] = aug_slug["question"].fillna("").astype(str)

    # Build index ONCE — shared across all taus
    INDEX_CACHE = CACHE_DIR / "qindex.npz"
    if INDEX_CACHE.exists():
        log(f"Loading cached qindex from {INDEX_CACHE}")
        d = np.load(INDEX_CACHE, allow_pickle=True)
        idx_slugs = list(d["slugs"]); idx_labels = d["labels"]; idx_sims = d["sims"]
    else:
        log("Building TF-IDF + SVD + HNSW kNN index (once)…")
        idx_slugs, idx_labels, idx_sims = build_qcluster_index(docs, verbose=True)
        np.savez_compressed(INDEX_CACHE, slugs=np.array(idx_slugs, dtype=object),
                            labels=idx_labels, sims=idx_sims)
        log(f"  saved {INDEX_CACHE}")

    for tau in TAUS:
        log(f"=== τ = {tau} ===")
        cache_pkl = CACHE_DIR / f"slug_to_cluster_tau{tau:.2f}.parquet"
        stats_pkl = CACHE_DIR / f"stats_tau{tau:.2f}.json"
        if cache_pkl.exists() and stats_pkl.exists():
            log(f"  loading cached cluster map from {cache_pkl}")
            m = pd.read_parquet(cache_pkl)
            slug_to_cluster = dict(zip(m["event_slug"], m["cluster_id"]))
            stats = json.loads(stats_pkl.read_text())
        else:
            slug_to_cluster, stats = cluster_at_tau(idx_slugs, idx_labels, idx_sims,
                                                    tau=tau, verbose=True)
            pd.DataFrame(
                {"event_slug": list(slug_to_cluster.keys()),
                 "cluster_id": list(slug_to_cluster.values())}
            ).to_parquet(cache_pkl, index=False)
            stats_pkl.write_text(json.dumps(stats, indent=2))

        # Contract-level singleton%: how many of the 1.12M contracts are in singleton clusters?
        slug_clu = pd.Series(slug_to_cluster).rename("cluster_id").reset_index().rename(
            columns={"index": "event_slug"})
        slug_size = slug_clu.groupby("cluster_id")["event_slug"].count().rename("cluster_size_in_slugs")
        # Propagate to contract level
        c2 = aug[["token_id", "event_slug"]].merge(slug_clu, on="event_slug", how="left")
        c2 = c2.merge(slug_size, on="cluster_id", how="left")
        # Re-bucket by contract count per cluster
        contract_size_by_cluster = c2.groupby("cluster_id")["token_id"].count()
        n_contracts = len(c2)
        n_singleton_contracts = int((c2["cluster_id"].map(contract_size_by_cluster) == 1).sum())
        contract_singleton_pct = float(100 * n_singleton_contracts / n_contracts)
        top10_contract_sizes = [int(x) for x in contract_size_by_cluster.sort_values(ascending=False).head(10)]
        stats.update({
            "n_contracts": int(n_contracts),
            "n_singleton_contracts": int(n_singleton_contracts),
            "contract_singleton_pct": contract_singleton_pct,
            "top10_cluster_sizes_contracts": top10_contract_sizes,
        })
        all_stats.append(stats)

        # Audit markdown block
        audit_lines.append(f"## τ = {tau}\n")
        audit_lines.append(f"- n_clusters: **{stats['n_clusters']:,}**\n")
        audit_lines.append(f"- slug-level singleton%: **{stats['singleton_slug_pct']:.2f}%**\n")
        audit_lines.append(f"- contract-level singleton%: **{contract_singleton_pct:.2f}%**\n")
        audit_lines.append(f"- mean cluster size (slugs): {stats['mean_cluster_size_slug']:.2f}, "
                           f"median: {stats['median_cluster_size_slug']:.0f}\n")
        audit_lines.append(f"- top-10 cluster sizes (slugs): {stats['top10_cluster_sizes']}\n")
        audit_lines.append(f"- top-10 cluster sizes (contracts): {top10_contract_sizes}\n")
        audit_lines.append(f"- share of slugs in top-1% largest clusters: {stats['share_slugs_in_top1pct_clusters']:.3f}\n\n")

        # Stratified sample of clusters
        size_map = slug_size.to_dict()
        picks = stratified_sample(size_map, rng)
        for band, cids in picks.items():
            audit_lines.append(f"### τ={tau} — sample: {band}\n")
            if not cids:
                audit_lines.append("_no clusters in this stratum_\n\n")
                continue
            for cid in cids:
                # Pull up to 8 distinct questions from cluster
                cluster_slugs = [s for s, c in slug_to_cluster.items() if c == cid]
                # cap slugs lookup to first 200 for speed
                q_sample = (
                    aug_slug[aug_slug["event_slug"].isin(cluster_slugs[:200])]["question"]
                    .drop_duplicates().head(8).tolist()
                )
                sz = size_map.get(cid, 0)
                audit_lines.append(f"- **{cid}** (size={sz} slugs):\n")
                for q in q_sample:
                    audit_lines.append(f"  - {q}\n")
                audit_lines.append("\n")
        audit_lines.append("\n---\n\n")

    AUDIT_MD.write_text("".join(audit_lines))
    AUDIT_JSON.write_text(json.dumps(all_stats, indent=2))
    log(f"\nAudit markdown → {AUDIT_MD}")
    log(f"Summary JSON   → {AUDIT_JSON}")
    log(f"Cluster maps   → {CACHE_DIR}/slug_to_cluster_tau*.parquet")


if __name__ == "__main__":
    main()
