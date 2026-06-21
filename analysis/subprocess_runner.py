"""
Subprocess-based DuckDB execution for guaranteed OS memory reclamation.

When DuckDB runs in a subprocess, all memory (DuckDB's internal allocator +
Python heap) is returned to the OS when the subprocess exits — regardless of
whether DROP TABLE or gc.collect() were called in the parent.

Typical usage (notebook):

    from subprocess_runner import sp_run, register_parquet_view
    from trader_flb import build_experience_tables_to_parquet

    # Large table: built in subprocess, saved to parquet, registered as lazy VIEW
    exp_path = sp_run(build_experience_tables_to_parquet, OUTPUT_DIR / '_exp_all.parquet')
    register_parquet_view(con, '_exp_all', str(exp_path))

    # Aggregation: built in subprocess, small DataFrame returned
    from trader_flb import compute_flb_by_contract_intensity_subprocess
    flb_df = sp_run(compute_flb_by_contract_intensity_subprocess, OUTPUT_DIR)
"""

import multiprocessing as mp
import os
import pickle
import tempfile
from pathlib import Path


# ---------------------------------------------------------------------------
# Module-level worker — MUST be at module level for spawn pickling to work.
# ---------------------------------------------------------------------------

def _worker(fn, args, kwargs, result_path):
    """Subprocess entry point. Runs fn(*args, **kwargs) and pickles result."""
    import sys
    # Ensure the analysis directory is importable (spawn doesn't inherit
    # runtime sys.path additions from the parent Jupyter kernel).
    analysis_dir = os.path.dirname(os.path.abspath(__file__))
    if analysis_dir not in sys.path:
        sys.path.insert(0, analysis_dir)

    result = fn(*args, **kwargs)
    with open(result_path, 'wb') as f:
        pickle.dump(result, f)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sp_run(fn, *args, use_cache=True, **kwargs):
    """
    Run fn(*args, **kwargs) in a subprocess and return the result.

    Memory guarantee: when the subprocess exits, the OS reclaims ALL memory
    it used — DuckDB allocator pages, Python heap, everything.

    Cache: if the first positional arg is a Path-like string pointing to an
    existing file, sp_run returns it immediately without spawning a subprocess.
    Pass use_cache=False to force recomputation.

    Args:
        fn:         A module-level callable (not a lambda or notebook closure).
                    Must be importable so that multiprocessing 'spawn' can
                    pickle it.
        *args:      Positional args forwarded to fn.
        use_cache:  If True (default) and args[0] is an existing path, skip.
        **kwargs:   Keyword args forwarded to fn.

    Returns:
        Whatever fn returns.

    Raises:
        RuntimeError if the subprocess exits with a non-zero code.
    """
    # Cache check
    if use_cache and args:
        first = args[0]
        if isinstance(first, (str, Path)) and Path(first).is_file():
            print(f"[sp_run cache] {Path(first).name} already exists, skipping.")
            return first

    # Temp file for IPC
    fd, result_path = tempfile.mkstemp(suffix='.pkl')
    os.close(fd)

    try:
        ctx = mp.get_context('spawn')
        p = ctx.Process(target=_worker, args=(fn, args, kwargs, result_path))
        p.start()
        p.join()

        if p.exitcode != 0:
            raise RuntimeError(
                f"sp_run: subprocess for {fn.__name__!r} failed "
                f"(exit code {p.exitcode}). Check cell output for traceback."
            )

        with open(result_path, 'rb') as f:
            return pickle.load(f)

    finally:
        if os.path.exists(result_path):
            os.unlink(result_path)


def register_parquet_view(con, view_name: str, parquet_path: str) -> None:
    """
    Register a parquet file as a lazy DuckDB VIEW in con.

    Queries against the view read from parquet on demand — no RAM cost
    until the query actually executes, and even then only the needed pages
    are buffered.

    Args:
        con:          DuckDB connection (main notebook connection).
        view_name:    Name for the VIEW (e.g. '_exp_all').
        parquet_path: Absolute path to the parquet file.
    """
    con.execute(
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT * FROM read_parquet('{parquet_path}')"
    )
    # Read row count from parquet file metadata (footer only — zero data scan)
    try:
        n = con.execute(
            f"SELECT SUM(num_rows) FROM parquet_file_metadata('{parquet_path}')"
        ).fetchone()[0]
        print(f"[register_parquet_view] {view_name}: {n:,} rows  ← {parquet_path}")
    except Exception:
        print(f"[register_parquet_view] {view_name}: registered  ← {parquet_path}")
