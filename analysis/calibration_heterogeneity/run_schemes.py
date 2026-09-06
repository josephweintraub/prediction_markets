"""Run calibration estimates with vintage checks and immutable artifacts.

Each run validates its committed data vintage, creates a unique run directory, and writes
count-, dollar-, and equal-market-weighted estimates. Raw, Bonferroni, and Benjamini-
Hochberg p-values are retained in the machine-readable tables.
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from data_vintage import load_vintage, validate_vintage
from flb_engine import add_multiple_testing_columns, compute_slice, sig_stars
from run_artifacts import create_run, file_fingerprint, finalize_run, write_manifest


REPOSITORY = Path(__file__).resolve().parents[2]
DEFAULT_VINTAGE = REPOSITORY / "configs" / "data_vintages" / "polymarket_2026-07-04.json"
MIN_TRADES = 5000
MIN_DECILE_TRADES = 50


def select_scheme_files(base_dir: str, requested: list[str] | None) -> list[str]:
    """Resolve scheme paths and fail if the selection is empty or incomplete."""
    files = sorted(glob.glob(f"{base_dir}/schemes/scheme_*.parquet"))
    if requested:
        want = set(requested)
        found = {
            os.path.basename(path)[len("scheme_"):-len(".parquet")]
            for path in files
        }
        missing = sorted(want - found)
        if missing:
            raise FileNotFoundError(f"Requested scheme files not found: {missing}")
        files = [
            path
            for path in files
            if os.path.basename(path)[len("scheme_"):-len(".parquet")] in want
        ]
    if not files:
        raise FileNotFoundError(f"No scheme files found under {base_dir}/schemes")
    return files


def _write_table(frame: pd.DataFrame, path: Path, kind: str) -> dict:
    frame.to_parquet(path, index=False)
    artifact = {
        "kind": kind,
        "rows": int(len(frame)),
        "columns": list(frame.columns),
    }
    artifact.update(file_fingerprint(path))
    return artifact


def run_analysis(args: argparse.Namespace) -> Path:
    vintage_path = Path(args.vintage).expanduser().resolve()
    vintage = load_vintage(vintage_path)
    parameters = {
        "window": args.window,
        "schemes": args.schemes,
        "min_trades": args.min_trades,
        "min_decile_trades": args.min_decile_trades,
        "n_bins": args.n_bins,
        "threads": args.threads,
        "random_seed": None,
    }
    run_dir, manifest = create_run(
        args.run_root,
        "calibration-heterogeneity",
        args.analysis_state,
        REPOSITORY,
        sys.argv,
        vintage_path,
        parameters,
        args.run_id,
    )
    started = time.time()
    con = duckdb.connect()
    con.execute(f"SET threads TO {args.threads}")
    con.execute("SET preserve_insertion_order=false")

    try:
        print(f"run directory: {run_dir}", flush=True)
        print(f"validating vintage {vintage['id']}", flush=True)
        validation = validate_vintage(vintage, args.window, con)
        manifest["validations"]["data_vintage"] = validation

        analysis_root = vintage.get("analysis_root")
        if not analysis_root:
            raise ValueError(f"Vintage {vintage['id']!r} does not declare analysis_root")
        files = select_scheme_files(analysis_root, args.schemes)
        manifest["inputs"]["schemes"] = [file_fingerprint(path) for path in files]
        manifest["inputs"]["base"] = validation["artifacts"][f"flb_base_{args.window}"]
        write_manifest(run_dir, manifest)

        base_path = vintage["artifacts"][f"flb_base_{args.window}"]["path"]
        escaped_base = base_path.replace("'", "''")
        print(f"loading {args.window} base", flush=True)
        base = con.execute(f"""
            SELECT market_code, token_code, wallet_code, day, price, ret, won, usdc,
                   LEAST(FLOOR(price * {args.n_bins})::INT, {args.n_bins - 1}) + 1 AS decile
            FROM read_parquet('{escaped_base}')
        """).fetchdf()
        print(f"  {len(base):,} rows in {time.time() - started:.0f}s", flush=True)

        map_path = vintage["artifacts"]["code_maps"]["path"].replace("'", "''")
        market_map = con.execute(f"""
            SELECT code AS market_code, value AS market_id
            FROM read_parquet('{map_path}') WHERE kind = 'market'
        """).fetchdf()
        id_to_code = dict(zip(market_map["market_id"], market_map["market_code"]))
        max_code = int(market_map["market_code"].max())
        market_codes = base["market_code"].to_numpy()

        for scheme_file in files:
            scheme = os.path.basename(scheme_file)[len("scheme_"):-len(".parquet")]
            scheme_map = pd.read_parquet(scheme_file)
            labels, label_codes = np.unique(
                scheme_map["slice"].astype(str), return_inverse=True
            )
            lookup = np.full(max_code + 1, -1, dtype=np.int32)
            codes = scheme_map["market_id"].map(id_to_code)
            matched = codes.notna()
            lookup[codes[matched].astype(int).to_numpy()] = label_codes[matched.to_numpy()]
            slice_codes = lookup[market_codes]
            keep = slice_codes >= 0
            if not keep.any():
                raise ValueError(
                    f"Scheme {scheme!r} has no markets in the {args.window!r} trade base"
                )
            frame = base.loc[keep].copy()
            frame["slice_code"] = slice_codes[keep]
            print(
                f"[{scheme}|{args.window}] {len(frame):,} trades, {len(labels)} defined slices",
                flush=True,
            )

            decile_rows: list[dict] = []
            summary_rows: list[dict] = []
            dropped_rows: list[dict] = []
            for code, subset in frame.groupby("slice_code", sort=True):
                label = labels[code]
                if len(subset) < args.min_trades:
                    dropped_rows.append(
                        {"scheme": scheme, "slice": label, "n_trades": int(len(subset))}
                    )
                    continue
                deciles, summary = compute_slice(
                    subset,
                    n_bins=args.n_bins,
                    min_decile_trades=args.min_decile_trades,
                )
                decile_rows.extend(
                    {"scheme": scheme, "slice": label, **row} for row in deciles
                )
                summary_rows.append({"scheme": scheme, "slice": label, **summary})
                current = summary_rows[-1]
                print(
                    f"    {label[:42]:42s} N={current['n_trades']:>11,} "
                    f"spread={current['spread']:+.4f} "
                    f"(t={current['spread_t']:+.1f}{sig_stars(current['spread_t'])}) "
                    f"spread_mkt={current['spread_mkt']:+.4f}",
                    flush=True,
                )

            decile_frame = pd.DataFrame(decile_rows)
            summary_frame = pd.DataFrame(summary_rows)
            if summary_frame.empty:
                raise ValueError(
                    f"Scheme {scheme!r} produced no slices above the {args.min_trades:,}-trade floor"
                )
            dropped_frame = pd.DataFrame(
                dropped_rows, columns=["scheme", "slice", "n_trades"]
            )
            decile_frame, summary_frame = add_multiple_testing_columns(
                decile_frame, summary_frame
            )
            manifest["outputs"].extend(
                [
                    _write_table(
                        decile_frame,
                        run_dir / "tables" / f"flb_deciles_{scheme}_{args.window}.parquet",
                        "deciles",
                    ),
                    _write_table(
                        summary_frame,
                        run_dir / "tables" / f"flb_summary_{scheme}_{args.window}.parquet",
                        "summary",
                    ),
                    _write_table(
                        dropped_frame,
                        run_dir / "tables" / f"flb_dropped_{scheme}_{args.window}.parquet",
                        "dropped_slices",
                    ),
                ]
            )
            write_manifest(run_dir, manifest)
            print(
                f"  [{scheme}] done: {len(summary_frame)} slices kept, "
                f"{len(dropped_frame)} dropped",
                flush=True,
            )

        manifest["validations"]["nonempty_outputs"] = {
            "status": "passed",
            "summary_tables": sum(item["kind"] == "summary" for item in manifest["outputs"]),
        }
        finalize_run(run_dir, manifest, "completed")
        print(f"completed immutable run {run_dir}", flush=True)
        return run_dir
    except Exception as exc:
        finalize_run(run_dir, manifest, "failed", f"{type(exc).__name__}: {exc}")
        raise
    finally:
        con.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--window", required=True, choices=["mature", "closing", "full"])
    parser.add_argument(
        "--schemes", nargs="*", default=None,
        help="scheme names (default: every scheme_*.parquet)",
    )
    parser.add_argument("--min-trades", type=int, default=MIN_TRADES)
    parser.add_argument("--min-decile-trades", type=int, default=MIN_DECILE_TRADES)
    parser.add_argument("--n-bins", type=int, default=10)
    parser.add_argument(
        "--analysis-state",
        choices=["exploration", "candidate", "confirmatory"],
        default="exploration",
    )
    parser.add_argument("--vintage", default=str(DEFAULT_VINTAGE))
    parser.add_argument("--run-root", default=os.environ.get("PM_RUN_ROOT", "/mnt/data/runs"))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--threads", type=int, default=os.cpu_count() or 1)
    return parser.parse_args(argv)


def main() -> None:
    run_analysis(parse_args())


if __name__ == "__main__":
    main()
