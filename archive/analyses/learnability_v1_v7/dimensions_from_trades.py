"""Trade-aggregated learnability dimension extractors.

Adds 3 `dim_*` columns sourced from a single DuckDB scan over the cleaned
`trades` view (which has bots + updown markets excluded).
"""
import numpy as np
import pandas as pd


def compute_contract_aggregates(con):
    """One scan over trades_buy → per-token dollar volume + time bounds.

    trades.conditionId is actually a per-outcome token_id (77-digit decimal),
    matching the augmented parquet's `token_id` column. Returned keyed by token_id.
    """
    return con.execute("""
        SELECT
            conditionId AS token_id,
            COUNT(*) AS n_trades,
            SUM(usdcSize) AS dollar_volume,
            MIN(timestamp) AS first_ts,
            MAX(timestamp) AS last_ts
        FROM trades_buy
        GROUP BY conditionId
    """).fetchdf()


def add_dim_dollar_volume_tier(df):
    pos = df["dollar_volume"].fillna(0).copy()
    has_vol = pos > 0
    if has_vol.sum() == 0:
        df["dim_dollar_volume_tier"] = "Zero"
        return df

    # Compute quartiles over contracts with positive volume
    qs = pos[has_vol].quantile([0.25, 0.50, 0.75])
    q1, q2, q3 = qs.values

    label = np.full(len(df), "Zero", dtype=object)
    v = pos.values
    label[has_vol & (v <= q1)] = f"Q1 (≤${q1:,.0f})"
    label[has_vol & (v > q1) & (v <= q2)] = f"Q2 (${q1:,.0f}-${q2:,.0f})"
    label[has_vol & (v > q2) & (v <= q3)] = f"Q3 (${q2:,.0f}-${q3:,.0f})"
    label[has_vol & (v > q3)] = f"Q4 (>${q3:,.0f})"
    df["dim_dollar_volume_tier"] = label
    return df


def add_dim_contract_horizon(df):
    span_sec = (df["last_ts"] - df["first_ts"]).fillna(0)
    HOUR = 3600
    DAY = 86400
    WEEK = 7 * DAY
    MONTH = 30 * DAY
    bins = [-1, HOUR, DAY, WEEK, MONTH, np.inf]
    labels = ["<1h", "1h-1d", "1d-1w", "1wk-1mo", ">1mo"]
    df["dim_contract_horizon"] = pd.cut(span_sec, bins=bins, labels=labels).astype(str)
    return df


def add_dim_recurrence_class(df):
    """Heuristic on family size + time span of the event_template's contracts."""
    if "dim_event_family_count" not in df.columns:
        df["dim_event_family_count"] = 1

    fam = df.groupby("event_template").agg(
        fam_first=("first_ts", "min"),
        fam_last=("last_ts", "max"),
        fam_size=("condition_id", "nunique"),   # markets per family, not tokens (so a standalone binary = 1 = One-off)
    ).reset_index()

    fam_span_days = (fam["fam_last"] - fam["fam_first"]).fillna(0) / 86400.0
    fam["fam_span_days"] = fam_span_days

    # Per-template recurrence label
    def label_row(r):
        size, span = r["fam_size"], r["fam_span_days"]
        if size <= 1:
            return "One-off"
        # Daily: medium-large families with broad span and high cadence
        if size >= 100 and span >= 60 and (size / max(span, 1)) >= 0.5:
            return "Daily"
        # Recurring: medium family with broad span
        if size >= 10 and span >= 30:
            return "Recurring"
        return "Episodic"

    fam["dim_recurrence_class"] = fam.apply(label_row, axis=1)
    out = df.merge(fam[["event_template", "dim_recurrence_class", "fam_span_days"]],
                   on="event_template", how="left")
    out["dim_recurrence_class"] = out["dim_recurrence_class"].fillna("One-off")
    out = out.rename(columns={"fam_span_days": "dim_family_span_days"})
    return out


def add_all_trades_aggregated_dimensions(per_contract_df, con):
    agg = compute_contract_aggregates(con)
    merged = per_contract_df.merge(agg, on="token_id", how="left")
    merged["dollar_volume"] = merged["dollar_volume"].fillna(0)
    merged["n_trades"] = merged["n_trades"].fillna(0).astype(int)
    merged = add_dim_dollar_volume_tier(merged)
    merged = add_dim_contract_horizon(merged)
    merged = add_dim_recurrence_class(merged)
    return merged


def report_slice_counts(df, dim_cols=None):
    if dim_cols is None:
        dim_cols = ["dim_dollar_volume_tier", "dim_contract_horizon", "dim_recurrence_class"]
    rows = []
    for d in dim_cols:
        for s, n in df[d].value_counts(dropna=False).items():
            rows.append({"dim": d, "slice": s, "n_contracts": int(n)})
    return pd.DataFrame(rows)
