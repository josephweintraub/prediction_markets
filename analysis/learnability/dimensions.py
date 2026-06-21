"""Per-contract learnability dimension extractors.

Operates on the augmented per-contract DataFrame (no trades scan). Adds 7
`dim_*` columns. Three more (`dim_dollar_volume_tier`, `dim_contract_horizon`,
`dim_recurrence_class`) are added by `dimensions_from_trades.py`.
"""
import re
import numpy as np
import pandas as pd

AUGMENTED_PARQUET = "/mnt/data/learnability/stage2_per_contract_augmented.parquet"


# ---------- info_type supergroup ----------

INFO_TYPE_PATTERNS = [
    ("market_data", re.compile(
        r"(crypto|stock|equity|forex|fx|commodity|treasury|yield|interest_rate|bond)"
        r".*?(price|return|level|yield|rate|data)", re.I)),
    ("sports_data", re.compile(r"sports?|game|team|player|league|tournament|match", re.I)),
    ("weather_data", re.compile(r"weather|temperature|rainfall|snow|hurricane|storm", re.I)),
    ("awards", re.compile(r"award|oscar|emmy|grammy|nobel|hall_of_fame|mvp|cup|championship", re.I)),
    ("politics_governance", re.compile(
        r"election|vote|poll|policy|legislation|congress|senate|president|gov|resignation|appointment", re.I)),
    ("culture_media", re.compile(r"film|movie|music|tv|show|book|art|celebrity|media|streaming", re.I)),
]


def info_type_supergroup(s):
    if not isinstance(s, str) or not s:
        return "other"
    for label, rgx in INFO_TYPE_PATTERNS:
        if rgx.search(s):
            return label
    return "other"


# ---------- dimension extractors ----------

def add_dim_resolution_type(df):
    df["dim_resolution_type"] = df["event_resolution_type"].fillna("unknown")
    return df


def add_dim_info_type_supergroup(df):
    df["dim_info_type_supergroup"] = df["event_info_type"].apply(info_type_supergroup)
    return df


def _first_cat(lst):
    if lst is None:
        return "Uncategorized"
    try:
        if len(lst) == 0:
            return "Uncategorized"
        return str(lst[0])
    except TypeError:
        return "Uncategorized"


def add_dim_primary_category(df):
    df["dim_primary_category"] = df["categories"].apply(_first_cat)
    return df


def _list_len(x):
    if x is None:
        return 0
    try:
        return len(x)
    except TypeError:
        return 0


def add_dim_subject_specificity(df):
    n = df["event_subjects"].apply(_list_len)
    df["dim_subject_specificity"] = pd.cut(
        n, bins=[-0.5, 1.5, 2.5, np.inf],
        labels=["1 subject", "2 subjects", "3+ subjects"],
    ).astype(str)
    return df


def add_dim_event_family_size(df):
    # group by event_template; count distinct MARKETS (condition_id), not tokens —
    # YES/NO of a binary market share one condition_id, so token-counting double-counts.
    counts = df.groupby("event_template")["condition_id"].transform("nunique")
    df["dim_event_family_count"] = counts
    df["dim_event_family_size"] = pd.cut(
        counts, bins=[-0.5, 1.5, 20.5, 1000.5, np.inf],
        labels=["Singleton 1", "Small 2-20", "Medium 21-1K", "Large 1K+"],
    ).astype(str)
    return df


def add_dim_outcomes_per_event(df):
    # outcomes per event: count distinct MARKETS (condition_id) per event_slug.
    # YES/NO share a condition_id, so a standalone binary market = 1 (Binary 1),
    # not 2 tokens (which previously misfiled binaries into "Few 2-5").
    distinct_cond = df.groupby("event_slug")["condition_id"].transform("nunique")
    df["dim_outcomes_per_event_raw"] = distinct_cond
    df["dim_outcomes_per_event"] = pd.cut(
        distinct_cond, bins=[-0.5, 1.5, 5.5, np.inf],
        labels=["Binary 1", "Few 2-5", "Many 6+"],
    ).astype(str)
    return df


def add_dim_market_specificity(df):
    m_subj = df["market_subjects"].apply(_list_len)
    e_subj = df["event_subjects"].apply(_list_len)
    diff = m_subj - e_subj
    label = np.where(diff > 0, "Market narrower",
            np.where(diff == 0, "Market = Event", "Market broader/equal"))
    df["dim_market_specificity"] = label
    return df


PER_CONTRACT_DIMS = [
    "dim_resolution_type",
    "dim_info_type_supergroup",
    "dim_primary_category",
    "dim_subject_specificity",
    "dim_event_family_size",
    "dim_outcomes_per_event",
    "dim_market_specificity",
]


def load_augmented():
    df = pd.read_parquet(AUGMENTED_PARQUET)
    return df


def add_all_per_contract_dimensions(df):
    df = add_dim_resolution_type(df)
    df = add_dim_info_type_supergroup(df)
    df = add_dim_primary_category(df)
    df = add_dim_subject_specificity(df)
    df = add_dim_event_family_size(df)
    df = add_dim_outcomes_per_event(df)
    df = add_dim_market_specificity(df)
    return df


def report_slice_counts(df, dim_cols=None):
    if dim_cols is None:
        dim_cols = PER_CONTRACT_DIMS
    rows = []
    for d in dim_cols:
        for s, n in df[d].value_counts(dropna=False).items():
            rows.append({"dim": d, "slice": s, "n_contracts": int(n)})
    return pd.DataFrame(rows)
