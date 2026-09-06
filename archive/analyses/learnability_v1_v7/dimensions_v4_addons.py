"""v4 addon dimensions for FLB learnability analysis.

Layered on top of the v3 contract_dimensions parquet, adds:

A) Three new grouping definitions (recurrence robustness, §10.x):
   - dim_group_strict      : event_slug + '|' + market_template
   - dim_group_loose       : event_template with date placeholders stripped
   - dim_group_qcluster    : TF-IDF + HNSW + connected-components clustering on
                             event_slug-concatenated questions, at a chosen tau.

B) A temporal first-occurrence dim per grouping (approach 1, §10.x):
   - dim_prior_settlements__{group_col} for group_col in
     {event_template, event_slug, dim_group_strict, dim_group_loose,
      dim_group_qcluster}.
   Bins: '0', '1-5', '6-50', '50+' — count of same-group contracts whose
   last_trade_ts STRICTLY precedes THIS contract's first_trade_ts.

C) Family-size × dollar-volume cross-tab (§5.x):
   - dim_family_vol_tier              : Low/Mid/High terciles of log(fam_total_vol)
   - dim_family_size_x_vol            : 3x3 cross of size × vol terciles
   - dim_vol_per_contract_tier        : quintiles of log(vol_per_contract)
   - dim_vol_per_contract_residualized: terciles of residual after regressing
                                         log(vol_per_contract) on log(fam_size)

The qcluster step is heavy enough to live behind a tau-sweep audit driver,
not run inline here. This module's qcluster function takes a pre-built
event_slug → cluster_id mapping.
"""
from __future__ import annotations
import re
import numpy as np
import pandas as pd


# ---------------- Grouping definitions (text-only) ----------------

# Loose-grouping strips ALL placeholders. Polymarket templates use 4:
# <DATE>, <TIME>, <NUM>, <TEAM>. The first two are date/time slots; <NUM>
# is overloaded (time slots in updown markets + stat thresholds elsewhere);
# <TEAM> is the team pair in sports games. Loose tier is the maximum-collapse
# sensitivity check, so we erase all of them.
_DATE_PLACEHOLDER_RE = re.compile(r"<(DATE|YEAR|MONTH|DAY|TIME|DATETIME|HOUR|MINUTE|WEEK|QUARTER|NUM|TEAM)>",
                                  re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")


def add_dim_group_strict(df: pd.DataFrame) -> pd.DataFrame:
    """Strict grouping: event_slug + market_template (the LLM's market shape).

    Two contracts share dim_group_strict iff they belong to the same Polymarket
    event AND their per-market template shape matches. Highest singleton% of the
    grouping approaches.

    Also adds dim_group_strict_size = the same family-size bucketing as
    dim_event_family_size, applied to dim_group_strict counts. The raw
    dim_group_strict (hundreds of thousands of unique IDs) is used for
    prior-settlement computation only; the bucketed dim is what gets sliced
    in the FLB SQL.
    """
    es = df["event_slug"].fillna("__NA_SLUG__").astype(str)
    mt = df["market_template"].fillna("__NA_MT__").astype(str)
    df["dim_group_strict"] = es + "|" + mt
    # Per-group size = distinct MARKETS (condition_id), not tokens (YES/NO share one)
    counts = df.groupby("dim_group_strict")["condition_id"].transform("nunique")
    df["dim_group_strict_count"] = counts
    df["dim_group_strict_size"] = pd.cut(
        counts, bins=[-0.5, 1.5, 20.5, 1000.5, np.inf],
        labels=["Singleton 1", "Small 2-20", "Medium 21-1K", "Large 1K+"],
    ).astype(str)
    return df


def add_dim_event_slug_size(df: pd.DataFrame) -> pd.DataFrame:
    """Same family-size bucketing applied to event_slug groupings (free bonus —
    no extra computation, event_slug is already in the parquet)."""
    counts = df.groupby("event_slug")["condition_id"].transform("nunique")
    df["dim_event_slug_count"] = counts
    df["dim_event_slug_size"] = pd.cut(
        counts, bins=[-0.5, 1.5, 20.5, 1000.5, np.inf],
        labels=["Singleton 1", "Small 2-20", "Medium 21-1K", "Large 1K+"],
    ).astype(str)
    return df


def _strip_date_placeholders(s: str) -> str:
    if s is None or not isinstance(s, str):
        return ""
    out = _DATE_PLACEHOLDER_RE.sub("", s)
    out = _WHITESPACE_RE.sub(" ", out).strip()
    return out


def add_dim_group_loose(df: pd.DataFrame) -> pd.DataFrame:
    """Loose grouping: event_template with <DATE>/<YEAR>/<MONTH>/<DAY>/<TIME>...
    stripped. Collapses across calendar time within a series."""
    et = df["event_template"].fillna("").astype(str)
    df["dim_group_loose"] = et.map(_strip_date_placeholders)
    # Empty-string loose template → make each empty-template contract a singleton
    # by giving it a unique ID (use token_id as the fallback).
    mask_empty = df["dim_group_loose"].eq("")
    df.loc[mask_empty, "dim_group_loose"] = "__loose_singleton__" + df.loc[mask_empty, "token_id"].astype(str)
    return df


def add_dim_group_qcluster_from_map(df: pd.DataFrame, slug_to_cluster: dict) -> pd.DataFrame:
    """[deprecated path] Attach cluster IDs from connected-components clustering.

    Replaced by add_dim_text_novelty_from_index — connected components chained at
    every tau tested. Kept here in case a future user wants to retry.
    """
    es = df["event_slug"].fillna("__NA_SLUG__").astype(str)
    cluster = es.map(slug_to_cluster)
    mask_unk = cluster.isna()
    fallback = "__qc_singleton__" + df.loc[mask_unk, "token_id"].astype(str)
    cluster = cluster.astype(object)
    cluster.loc[mask_unk] = fallback
    df["dim_group_qcluster"] = cluster.astype(str)
    return df


def compute_per_slug_novelty(slugs: list[str], labels, sims) -> pd.DataFrame:
    """For each slug, compute its text-novelty score from the cached kNN index.

    Returns a DataFrame with one row per slug, columns:
      - event_slug
      - best_sim     : max cosine similarity to ANY other slug in its top-K
      - n_above_0_50, n_above_0_65, n_above_0_75, n_above_0_85
                     : count of top-K neighbors above each threshold

    Note: position 0 in `labels`/`sims` is the slug itself (self-loop, sim=1.0).
    We skip position 0.
    """
    import numpy as np
    sims_excl = sims[:, 1:]  # skip self-loops
    best_sim = sims_excl.max(axis=1)
    n_above_50 = (sims_excl >= 0.50).sum(axis=1)
    n_above_65 = (sims_excl >= 0.65).sum(axis=1)
    n_above_75 = (sims_excl >= 0.75).sum(axis=1)
    n_above_85 = (sims_excl >= 0.85).sum(axis=1)
    return pd.DataFrame({
        "event_slug": slugs,
        "best_sim": best_sim,
        "n_above_0_50": n_above_50,
        "n_above_0_65": n_above_65,
        "n_above_0_75": n_above_75,
        "n_above_0_85": n_above_85,
    })


def add_dim_text_novelty(df: pd.DataFrame, slug_novelty: pd.DataFrame) -> pd.DataFrame:
    """Bin slugs by best-neighbor similarity quintile → per-contract dim.

    Quintiles are computed across unique slugs (not contracts), so each
    quintile gets roughly the same number of distinct slugs.
    """
    sn = slug_novelty.copy()
    # Quintiles of best_sim across slugs (descending sim = less isolated)
    sn["dim_text_novelty"] = pd.qcut(
        sn["best_sim"], q=5,
        labels=[
            "Q1 most isolated",
            "Q2 moderately isolated",
            "Q3 moderate",
            "Q4 repetitive",
            "Q5 most repetitive",
        ],
        duplicates="drop",
    ).astype(str)

    # Continuous-count dims (for sensitivity reporting)
    # Bin n_above_0_65: 0 / 1 / 2-5 / 6+
    sn["dim_text_neighbors_strict"] = pd.cut(
        sn["n_above_0_75"], bins=[-1, 0, 1, 5, np.inf],
        labels=["0 strict neighbors", "1 strict neighbor", "2-5 strict neighbors", "6+ strict neighbors"],
    ).astype(str)

    out = df.merge(
        sn[["event_slug", "dim_text_novelty", "dim_text_neighbors_strict", "best_sim"]],
        on="event_slug", how="left",
    )
    # Fallbacks for contracts whose event_slug isn't in the slug index
    out["dim_text_novelty"] = out["dim_text_novelty"].fillna("Q1 most isolated")
    out["dim_text_neighbors_strict"] = out["dim_text_neighbors_strict"].fillna("0 strict neighbors")
    out["best_sim"] = out["best_sim"].fillna(0.0)
    return out


# ---------------- TF-IDF + HNSW + connected-components clustering ----------------

def build_slug_documents(augmented_df: pd.DataFrame) -> pd.DataFrame:
    """One row per event_slug; document = ' || '.join(distinct questions).

    Drops slugs with no question text.
    """
    sub = augmented_df[["event_slug", "question"]].dropna(subset=["event_slug"]).copy()
    sub["question"] = sub["question"].fillna("").astype(str)
    sub = sub[sub["question"].str.len() > 0]
    docs = (
        sub.groupby("event_slug")["question"]
           .apply(lambda s: " || ".join(sorted(set(s))))
           .reset_index()
           .rename(columns={"question": "doc"})
    )
    return docs


def tfidf_hybrid(docs: list[str]):
    """Char (3-5) + word (1-2) hybrid TF-IDF, both L2-normalized, hstacked."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from scipy.sparse import hstack
    import numpy as np

    char_vec = TfidfVectorizer(
        analyzer="char_wb", ngram_range=(3, 5),
        min_df=2, max_df=0.5, sublinear_tf=True, norm="l2",
    )
    word_vec = TfidfVectorizer(
        analyzer="word", ngram_range=(1, 2),
        min_df=2, max_df=0.5, sublinear_tf=True, norm="l2",
    )
    X_char = char_vec.fit_transform(docs)
    X_word = word_vec.fit_transform(docs)
    # hstack keeps L2 norm at sqrt(2); we re-normalize so cosine is well-defined
    X = hstack([X_char, X_word]).tocsr()
    # row-normalize to unit L2
    from sklearn.preprocessing import normalize
    X = normalize(X, norm="l2", axis=1, copy=False)
    return X, {"char": char_vec, "word": word_vec, "n_features_char": X_char.shape[1],
               "n_features_word": X_word.shape[1]}


def build_qcluster_index(docs_df: pd.DataFrame, k_neighbors: int = 20,
                         hnsw_M: int = 32, hnsw_ef_construct: int = 200, hnsw_ef: int = 100,
                         svd_dim_target: int = 256, verbose: bool = True):
    """Build the TF-IDF + SVD + HNSW kNN index *once*. Returns (slugs, labels, sims)
    suitable for `cluster_at_tau` to threshold at multiple tau values."""
    import hnswlib
    from sklearn.decomposition import TruncatedSVD

    slugs = docs_df["event_slug"].to_list()
    docs = docs_df["doc"].to_list()
    N = len(slugs)
    if verbose:
        print(f"[qindex] N={N:,} slugs", flush=True)

    X, _ = tfidf_hybrid(docs)
    if verbose:
        print(f"[qindex] TF-IDF features: {X.shape[1]:,}", flush=True)

    svd_dim = min(svd_dim_target, max(64, X.shape[1] // 4))
    svd = TruncatedSVD(n_components=svd_dim, random_state=0)
    Xd = svd.fit_transform(X).astype(np.float32)
    norms = np.linalg.norm(Xd, axis=1, keepdims=True)
    norms[norms == 0] = 1
    Xd = Xd / norms
    if verbose:
        print(f"[qindex] SVD→{svd_dim} dims, explained_var={svd.explained_variance_ratio_.sum():.3f}",
              flush=True)

    p = hnswlib.Index(space="ip", dim=svd_dim)
    p.init_index(max_elements=N, ef_construction=hnsw_ef_construct, M=hnsw_M)
    p.add_items(Xd, np.arange(N))
    p.set_ef(hnsw_ef)

    k = min(k_neighbors + 1, N)
    labels, distances = p.knn_query(Xd, k=k)
    sims = 1.0 - distances  # "ip" returns 1-cos for unit vectors
    if verbose:
        print(f"[qindex] kNN done, top-{k}", flush=True)
    return slugs, labels, sims


def cluster_at_tau(slugs: list[str], labels: np.ndarray, sims: np.ndarray,
                   tau: float, megacluster_cap: int = 50_000, verbose: bool = True):
    """Given a prebuilt kNN index, threshold at tau and run connected components."""
    from scipy.sparse.csgraph import connected_components
    from scipy.sparse import coo_matrix

    N = len(slugs)
    k = labels.shape[1]
    # vectorized edge construction: drop self-loops (j_idx=0) and sim < tau
    rows_all = np.repeat(np.arange(N), k - 1)
    cols_all = labels[:, 1:].reshape(-1)
    sims_all = sims[:, 1:].reshape(-1)
    keep = sims_all >= tau
    rows = rows_all[keep]
    cols = cols_all[keep]
    vals = np.ones(rows.shape[0], dtype=np.int8)
    if verbose:
        print(f"[tau={tau}] edges above threshold: {len(rows):,}", flush=True)

    A = coo_matrix((vals, (rows, cols)), shape=(N, N))
    n_comp, comp_labels = connected_components(A, directed=False)
    if verbose:
        print(f"[tau={tau}] components: {n_comp:,}", flush=True)

    sizes = pd.Series(comp_labels).value_counts()
    big = sizes[sizes > megacluster_cap].index.tolist()
    if big and verbose:
        print(f"[tau={tau}] WARNING: {len(big)} megaclusters >cap ({megacluster_cap}) — not resplit "
              f"in fast path; flag for inspection", flush=True)

    slug_to_cluster = {slug: f"qc_t{int(tau*100)}_{int(c)}" for slug, c in zip(slugs, comp_labels)}
    n_singletons = int((sizes == 1).sum())
    n_top1pct = max(1, int(len(sizes) * 0.01))
    share_top1pct = float(sizes.head(n_top1pct).sum() / len(comp_labels))
    stats = {
        "tau": tau,
        "n_slugs": N,
        "n_clusters": int(n_comp),
        "singleton_slug_pct": float(100 * n_singletons / N),
        "mean_cluster_size_slug": float(sizes.mean()),
        "median_cluster_size_slug": float(sizes.median()),
        "top10_cluster_sizes": [int(x) for x in sizes.head(10).tolist()],
        "share_slugs_in_top1pct_clusters": share_top1pct,
        "n_megaclusters_above_cap": len(big),
    }
    return slug_to_cluster, stats


def cluster_qcluster(docs_df: pd.DataFrame, tau: float, k_neighbors: int = 20,
                     hnsw_M: int = 32, hnsw_ef_construct: int = 200, hnsw_ef: int = 100,
                     megacluster_cap: int = 50_000, verbose: bool = True):
    """Cluster event_slug documents at threshold tau.

    Returns:
      slug_to_cluster (dict[str,str])
      stats (dict): singleton_pct, n_clusters, top10_sizes, mean_size, median_size,
                    share_in_top1pct_clusters
    """
    import hnswlib
    from scipy.sparse.csgraph import connected_components
    from scipy.sparse import coo_matrix
    import numpy as np

    slugs = docs_df["event_slug"].to_list()
    docs = docs_df["doc"].to_list()
    N = len(slugs)
    if verbose:
        print(f"[qcluster] N={N:,} slugs, tau={tau}", flush=True)

    X, _ = tfidf_hybrid(docs)
    if verbose:
        print(f"[qcluster] TF-IDF features: {X.shape[1]:,}; nnz/row mean={X.nnz/N:.1f}", flush=True)

    # HNSW expects dense vectors; sparse TF-IDF is too wide to densify.
    # Workaround: project to a moderate-dim dense space via TruncatedSVD.
    from sklearn.decomposition import TruncatedSVD
    svd_dim = min(256, max(64, X.shape[1] // 4))
    svd = TruncatedSVD(n_components=svd_dim, random_state=0)
    Xd = svd.fit_transform(X).astype(np.float32)
    # re-normalize for cosine in HNSW (which uses inner product on unit vectors)
    norms = np.linalg.norm(Xd, axis=1, keepdims=True)
    norms[norms == 0] = 1
    Xd = Xd / norms
    if verbose:
        print(f"[qcluster] SVD to {svd_dim} dims, explained var sum={svd.explained_variance_ratio_.sum():.3f}",
              flush=True)

    # HNSW index — cosine space (inner-product on unit vectors)
    p = hnswlib.Index(space="ip", dim=svd_dim)
    p.init_index(max_elements=N, ef_construction=hnsw_ef_construct, M=hnsw_M)
    p.add_items(Xd, np.arange(N))
    p.set_ef(hnsw_ef)

    # Query top-K neighbors per node
    k = min(k_neighbors + 1, N)
    labels, distances = p.knn_query(Xd, k=k)
    # hnswlib "ip" distance = 1 - cosine_similarity. Convert.
    sims = 1.0 - distances
    if verbose:
        print(f"[qcluster] kNN done, top-{k}", flush=True)

    # Build sparse adjacency from edges with sim >= tau (skip self-loops)
    rows, cols, vals = [], [], []
    for i in range(N):
        for j_idx in range(1, k):
            j = labels[i, j_idx]
            s = sims[i, j_idx]
            if s >= tau and j != i:
                rows.append(i); cols.append(j); vals.append(1)
    if verbose:
        print(f"[qcluster] edges above tau: {len(rows):,}", flush=True)

    A = coo_matrix((vals, (rows, cols)), shape=(N, N))
    n_comp, comp_labels = connected_components(A, directed=False)
    if verbose:
        print(f"[qcluster] components: {n_comp:,}", flush=True)

    # Optional: split megaclusters (component_size > cap) by re-clustering them
    sizes = pd.Series(comp_labels).value_counts()
    big = sizes[sizes > megacluster_cap].index.tolist()
    if big:
        if verbose:
            print(f"[qcluster] {len(big)} megaclusters >cap ({megacluster_cap}); "
                  f"recursive resplit at tau+0.10", flush=True)
        # Naive resplit: bump tau and recluster within the megacluster only
        next_label = comp_labels.max() + 1
        for c in big:
            idx = np.where(comp_labels == c)[0]
            sub_docs = pd.DataFrame({"event_slug": [slugs[i] for i in idx],
                                     "doc": [docs[i] for i in idx]})
            sub_map, _ = cluster_qcluster(sub_docs, tau=min(tau + 0.10, 0.99),
                                          k_neighbors=k_neighbors, verbose=False)
            for slug, sub_c in sub_map.items():
                ii = slugs.index(slug)  # O(N), acceptable since |megacluster| is small relative to N
                comp_labels[ii] = next_label + hash(sub_c) % 1_000_000
        # Renumber for tidiness
        _, comp_labels = np.unique(comp_labels, return_inverse=True)
        n_comp = int(comp_labels.max()) + 1

    slug_to_cluster = {slug: f"qc_{int(c)}" for slug, c in zip(slugs, comp_labels)}

    # Stats — propagate to contract level later; for slug-level stats:
    size_series = pd.Series(comp_labels).value_counts()
    n_singletons_slug = (size_series == 1).sum()
    n_top1pct = max(1, int(len(size_series) * 0.01))
    share_top1pct = float(size_series.head(n_top1pct).sum() / len(comp_labels))
    stats = {
        "tau": tau,
        "n_slugs": N,
        "n_clusters": int(n_comp),
        "singleton_slug_pct": float(100 * n_singletons_slug / N),
        "mean_cluster_size_slug": float(size_series.mean()),
        "median_cluster_size_slug": float(size_series.median()),
        "top10_cluster_sizes": [int(x) for x in size_series.head(10).tolist()],
        "share_slugs_in_top1pct_clusters": share_top1pct,
    }
    return slug_to_cluster, stats


# ---------------- Family-volume cross-tab ----------------

def add_dim_family_vol_tiers(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-template volume aggregates and derived contract-level dims.

    Assumes df already has columns:
      - event_template
      - dim_event_family_count (per-contract count of same-template contracts;
                                from v3 add_dim_event_family_size)
      - dim_event_family_size  (Singleton/Small/Medium/Large bucket label)
      - dollar_volume          (per-contract sum usdcSize; from v3 trade aggregates)
    """
    # fam_total_vol per template
    fam = df.groupby("event_template").agg(
        fam_total_vol=("dollar_volume", "sum"),
        fam_size=("condition_id", "nunique"),   # markets per family, not tokens (YES/NO share a condition_id)
    ).reset_index()
    fam["vol_per_contract"] = fam["fam_total_vol"] / fam["fam_size"].clip(lower=1)

    # log-vol terciles across templates (drop zero-volume templates)
    log_vol = np.log1p(fam["fam_total_vol"])
    log_vpc = np.log1p(fam["vol_per_contract"])
    log_size = np.log1p(fam["fam_size"])

    def safe_qcut(s: pd.Series, q: int, labels: list[str]) -> pd.Series:
        try:
            return pd.qcut(s, q=q, labels=labels, duplicates="drop")
        except ValueError:
            return pd.Series(pd.Categorical([labels[0]] * len(s), categories=labels), index=s.index)

    fam["dim_family_vol_tier"] = safe_qcut(log_vol, 3, ["Low vol", "Mid vol", "High vol"]).astype(str)
    fam["dim_vol_per_contract_tier"] = safe_qcut(
        log_vpc, 5, ["VPC Q1 (thinnest)", "VPC Q2", "VPC Q3", "VPC Q4", "VPC Q5 (thickest)"]
    ).astype(str)

    # Residualized vol-per-contract: regress log_vpc on log_size, take residuals
    # then tercile.
    valid = log_size.notna() & log_vpc.notna() & np.isfinite(log_size) & np.isfinite(log_vpc)
    if valid.sum() > 10:
        x = log_size[valid].values
        y = log_vpc[valid].values
        # closed-form OLS coefficients
        x_mean, y_mean = x.mean(), y.mean()
        x_var = ((x - x_mean) ** 2).sum()
        beta = ((x - x_mean) * (y - y_mean)).sum() / max(x_var, 1e-12)
        alpha = y_mean - beta * x_mean
        resid = pd.Series(np.nan, index=fam.index)
        resid.loc[valid] = y - (alpha + beta * x)
        fam["dim_vol_per_contract_residualized"] = safe_qcut(
            resid, 3, ["VPC resid Low", "VPC resid Mid", "VPC resid High"]
        ).astype(str)
        meta = {"vpc_resid_alpha": float(alpha), "vpc_resid_beta": float(beta),
                "spearman_logsize_logvol": float(pd.Series(log_size).corr(pd.Series(log_vol), method="spearman"))}
    else:
        fam["dim_vol_per_contract_residualized"] = "VPC resid (insufficient data)"
        meta = {"vpc_resid_alpha": None, "vpc_resid_beta": None, "spearman_logsize_logvol": None}

    out = df.merge(
        fam[["event_template", "dim_family_vol_tier", "dim_vol_per_contract_tier",
             "dim_vol_per_contract_residualized", "fam_total_vol", "vol_per_contract"]],
        on="event_template", how="left",
    )

    # 3x3 cross-tab cell label
    out["dim_family_size_x_vol"] = (
        out["dim_event_family_size"].astype(str) + " × " + out["dim_family_vol_tier"].astype(str)
    )
    return out, meta


# ---------------- Temporal first-occurrence (approach 1) ----------------

def add_dim_prior_settlements(df: pd.DataFrame, group_col: str,
                              first_ts_col: str = "first_ts",
                              last_ts_col: str = "last_ts") -> pd.DataFrame:
    """For each contract, count how many same-group contracts have
    `last_ts < this.first_ts`. Then bin into '0' / '1-5' / '6-50' / '50+'.

    Adds a column `dim_prior_settlements__{group_col}`.
    """
    new_col = f"dim_prior_settlements__{group_col}"
    bin_col = f"dim_prior_settlements_bin__{group_col}"

    work = df[[group_col, first_ts_col, last_ts_col, "token_id", "condition_id"]].copy()
    work[first_ts_col] = pd.to_numeric(work[first_ts_col], errors="coerce")
    work[last_ts_col] = pd.to_numeric(work[last_ts_col], errors="coerce")

    # Count prior MARKETS (condition_id), not tokens: a binary market's YES+NO settle
    # together and must not count as two prior settlements. Reduce to one settlement
    # time per (group, condition_id) = the market's last trade, then searchsorted each
    # contract's first_ts into the group's sorted market-settlement times. side='left'
    # counts markets strictly settled before; a contract's own market (c_last >= its
    # first_ts) is naturally excluded.
    cond = (work.dropna(subset=[last_ts_col])
                .groupby([group_col, "condition_id"], sort=False)[last_ts_col]
                .max().reset_index())
    cond_by_group = {g: np.sort(s.values)
                     for g, s in cond.groupby(group_col, sort=False)[last_ts_col]}

    out_counts = np.zeros(len(work), dtype=np.int64)
    work_index = work.index.to_numpy()
    pos_in_work = {idx: i for i, idx in enumerate(work_index)}
    for grp, sub in work.groupby(group_col, sort=False):
        last_sorted = cond_by_group.get(grp)
        if last_sorted is None or len(last_sorted) == 0:
            continue
        first_vals = sub[first_ts_col].values
        cnts = np.searchsorted(last_sorted, first_vals, side="left")
        nan_mask = pd.isna(first_vals)
        if nan_mask.any():
            cnts = np.where(nan_mask, 0, cnts)
        positions = np.array([pos_in_work[i] for i in sub.index], dtype=np.int64)
        out_counts[positions] = cnts

    df[new_col] = out_counts
    # Bin
    bins = [-1, 0, 5, 50, np.inf]
    labels = ["0", "1-5", "6-50", "50+"]
    df[bin_col] = pd.cut(df[new_col], bins=bins, labels=labels).astype(str)
    return df


# ---------------- Driver ----------------

V4_GROUPING_COLS = [
    "event_template",     # existing (medium)
    "event_slug",         # free bonus
    "dim_group_strict",   # tightest grouping
]

V4_BINNED_PRIOR_COLS = [f"dim_prior_settlements_bin__{c}" for c in V4_GROUPING_COLS]

V4_FAMILY_VOL_DIMS = [
    "dim_family_vol_tier",
    "dim_family_size_x_vol",
    "dim_vol_per_contract_tier",
    "dim_vol_per_contract_residualized",
]

V4_TEXT_NOVELTY_DIMS = [
    "dim_text_novelty",
    "dim_text_neighbors_strict",
]

V4_GROUPING_DIMS_FOR_FLB = [
    "dim_group_strict_size",   # size bucket of strict grouping — singletons appear
    "dim_event_slug_size",     # size bucket of event_slug grouping (free reference)
]


def all_v4_new_dim_cols() -> list[str]:
    return V4_BINNED_PRIOR_COLS + V4_FAMILY_VOL_DIMS + V4_TEXT_NOVELTY_DIMS + V4_GROUPING_DIMS_FOR_FLB
