"""Build the augmented per-contract dataset by adding v2 generic columns to
the existing 1.12M-row dataset.

The existing 18 columns (event_template, market_template, event_subjects, etc.)
are preserved bit-exactly — represents the CURRENT abstraction level (a mix:
generic for NBA/NHL/etc., specific for La Liga / Serie A / etc.).

13 new "_generic" columns are added — represents the CONSISTENT generic
abstraction level (every sports league gets <TEAM>-<TEAM> collapse). LLM
classifications at this level come from:
  * fresh LLM run for the 841 merge templates (so generic templates don't
    inherit team-specific subjects from a single v1 source)
  * carried over from v1 for the UNCHANGED + RENAME pairs (where the content
    is unchanged, just normalization adjusted)

Inputs:
  /Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract.parquet
    - original 18-column per-contract dataset
  /Users/josephweintraub/prediction_markets/analysis/output/stage2_v2_extracted.jsonl
    - fresh LLM extractions for the 841 merge templates (downloaded from EC2)

Output:
  /Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract_augmented.parquet
    - 1.12M rows, 18 original columns + 13 new _generic columns
"""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import importlib, stage0_v2
importlib.reload(stage0_v2)
from stage0_v2 import normalize as norm_v2

SRC          = Path("/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract.parquet")
V2_JSONL     = Path("/Users/josephweintraub/prediction_markets/analysis/output/stage2_v2_extracted.jsonl")
OUT_PARQUET  = Path("/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract_augmented.parquet")

EXT_COLS = ["event_subjects","event_action","event_info_type","event_resolution_type",
            "market_subjects","market_action","market_info_type","market_resolution_type",
            "categories","snippet","extraction_error"]


def main():
    print(f">>> loading {SRC} ...")
    df = pd.read_parquet(SRC)
    print(f"    {len(df):,} contracts, columns: {list(df.columns)}")

    # Apply v2 normalizer
    print(">>> applying v2 normalizer to event_slug and market_slug ...")
    event_slug_for_norm = df["event_slug"].fillna(df["market_slug"])
    df["event_template_generic"]  = event_slug_for_norm.apply(norm_v2)
    df["market_template_generic"] = df["market_slug"].apply(norm_v2)
    n_v2_pairs = df.groupby(["event_template_generic","market_template_generic"]).ngroups
    print(f"    unique v2 generic pairs: {n_v2_pairs:,}")

    # Load fresh LLM extractions for the 841 merge templates
    print(f">>> loading fresh LLM extractions from {V2_JSONL} ...")
    fresh = []
    for line in V2_JSONL.read_text().splitlines():
        r = json.loads(line)
        e = r.get("extracted", {})
        row = {
            "event_template_generic":  r["event_template"],
            "market_template_generic": r["market_template"],
        }
        if "_error" in e:
            row["extraction_error_generic"] = e["_error"]
            for c in EXT_COLS:
                if c != "extraction_error":
                    row[f"{c}_generic"] = None
        else:
            row["extraction_error_generic"] = None
            for c in EXT_COLS:
                if c == "extraction_error":
                    continue
                row[f"{c}_generic"] = e.get(c)
        fresh.append(row)
    fresh_df = pd.DataFrame(fresh).drop_duplicates(["event_template_generic","market_template_generic"])
    print(f"    {len(fresh_df):,} fresh extractions (target: ~841)")

    # Identify which v2 pairs need fresh vs inherit from v1
    print(">>> identifying inheritance vs fresh assignment ...")
    fresh_pairs = set(zip(fresh_df["event_template_generic"], fresh_df["market_template_generic"]))

    # For each contract, decide source:
    #   - if v2 pair is in fresh_pairs: use fresh LLM
    #   - else: inherit from existing v1 extraction (the contract's existing extraction row)
    # Implementation: merge fresh_df by v2 pair (left join). For unmatched rows, carry over v1 cols.
    df = df.merge(fresh_df, on=["event_template_generic","market_template_generic"], how="left")

    # For rows that didn't get a fresh extraction (NaN in generic cols), inherit v1
    needs_inherit = df["extraction_error_generic"].isna() & ~df["event_subjects_generic"].apply(
        lambda x: isinstance(x, list))  # already-fresh rows have a list value

    # Actually cleaner check: pair in fresh_pairs?
    pair_tuples = list(zip(df["event_template_generic"], df["market_template_generic"]))
    has_fresh = pd.Series([p in fresh_pairs for p in pair_tuples], index=df.index)
    inherit_mask = ~has_fresh
    n_fresh = has_fresh.sum()
    n_inherit = inherit_mask.sum()
    print(f"    contracts using fresh LLM:    {n_fresh:,} ({n_fresh/len(df)*100:.2f}%)")
    print(f"    contracts inheriting from v1: {n_inherit:,} ({n_inherit/len(df)*100:.2f}%)")

    # Inherit: copy v1 columns into v2-generic columns
    for c in EXT_COLS:
        gen_col = f"{c}_generic"
        df.loc[inherit_mask, gen_col] = df.loc[inherit_mask, c]

    # Final column order: original 18 + 13 new generic columns
    new_cols = ["event_template_generic","market_template_generic"] + [f"{c}_generic" for c in EXT_COLS]
    final_cols = list(SRC_COLS := pd.read_parquet(SRC, columns=None).columns.tolist()) + new_cols
    df = df[final_cols]

    # Sanity check
    print(f"\n>>> final dataset: {len(df):,} rows × {len(df.columns)} columns")
    print(f"    new columns: {new_cols}")
    n_missing = df["categories_generic"].isna().sum()
    print(f"    rows with NaN categories_generic: {n_missing:,}")
    n_extraction_error = df["extraction_error_generic"].notna().sum()
    print(f"    rows with extraction_error_generic populated: {n_extraction_error:,}")

    print(f"\n>>> writing {OUT_PARQUET} ...")
    df.to_parquet(OUT_PARQUET, index=False)
    print(f"    wrote {OUT_PARQUET} ({OUT_PARQUET.stat().st_size / 1024 / 1024:.1f} MB)")

    # Quick summary stats
    print(f"\n=== SUMMARY ===")
    print(f"  contracts:                          {len(df):,}")
    print(f"  v1 distinct (event,market) pairs:   {df.groupby(['event_template','market_template']).ngroups:,}")
    print(f"  v2 distinct (event,market) pairs:   {df.groupby(['event_template_generic','market_template_generic']).ngroups:,}")
    print(f"  contracts unchanged template:       {(df['event_template'] == df['event_template_generic']).sum():,}")
    print(f"  contracts with template change:     {(df['event_template'] != df['event_template_generic']).sum():,}")


if __name__ == "__main__":
    main()
