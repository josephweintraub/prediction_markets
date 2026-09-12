"""Render audited shared sports calibration artifacts as deterministic offline HTML."""
from __future__ import annotations

import argparse
import html
import json
from pathlib import Path
from typing import Any

import duckdb

from .artifacts import (
    ArtifactError, artifact_fingerprint, fingerprint, fresh_run, matches_artifact_fingerprint,
    quoted, require_exact_schema, require_sport, resolved, write_json,
)
from .estimate_calibration import CLOSING_SCHEMA, PHASE_PROFILE_SCHEMA
from .estimate_flb_tails import TAIL_SCHEMA
from .fixed_bins import fixed_bin_grid, require_complete_unique_grid
from .phase_contract import load_phase_contract, phase_contract_fingerprint


def _read(con: duckdb.DuckDBPyConnection, relation: str) -> list[dict[str, Any]]:
    cursor = con.execute(f"SELECT * FROM {relation}")
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "withheld"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return html.escape(str(value))


def _table(headers: list[str], rows: list[list[Any]], caption: str, css: str = "") -> str:
    head = "".join(f"<th scope=\"col\">{html.escape(value)}</th>" for value in headers)
    body = "".join("<tr>"+"".join(f"<td>{_fmt(value)}</td>" for value in row)+"</tr>" for row in rows)
    return (f'<div class="table-wrap {css}"><table><caption>{html.escape(caption)}</caption>'
            f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>")


def _chart(phase: str, rows: list[dict[str, Any]]) -> str:
    width, height = 640, 230
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-labelledby="chart-{phase}-title chart-{phase}-desc">',
        f'<title id="chart-{phase}-title">{html.escape(phase)} calibration by probability decile</title>',
        f'<desc id="chart-{phase}-desc">Literal and boundary-buffered mean outcome minus probability with nominal 95 percent confidence intervals. Suppressed cells are omitted.</desc>',
        '<line x1="45" y1="110" x2="625" y2="110" class="zero"/>',
    ]
    colors = {"literal": "series-a", "exclude_within_30s": "series-c"}
    offsets = {"literal": -4, "exclude_within_30s": 4}
    for row in rows:
        if row["suppressed"] or row["mean_calibration"] is None:
            continue
        x = 45 + (row["price_decile"]-0.5)*58 + offsets[row["boundary_sample"]]
        scale = 800
        y = 110-row["mean_calibration"]*scale
        low = 110-row["calibration_ci95_low"]*scale
        high = 110-row["calibration_ci95_high"]*scale
        cls = colors[row["boundary_sample"]]
        parts.append(f'<line x1="{x:.1f}" y1="{high:.1f}" x2="{x:.1f}" y2="{low:.1f}" class="whisker {cls}"/>')
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.2" class="{cls}"/>')
    parts.append("</svg>")
    return "".join(parts)


def render_flb_report(
    sport: str,
    calibration_run_dir: str | Path,
    tail_run_dir: str | Path,
    timestamp_declaration_path: str | Path,
    phase_contract_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    sport = require_sport(sport)
    calibration, tails, declaration, contract_path = map(
        resolved, (calibration_run_dir, tail_run_dir, timestamp_declaration_path, phase_contract_path)
    )
    contract = load_phase_contract(contract_path)
    if contract.sport != sport:
        raise ArtifactError("Report sport does not match phase contract")
    provenance = json.loads(declaration.read_text(encoding="utf-8"))
    if (provenance.get("sport") != sport or provenance.get("method") != "polygon_rpc_block_timestamp"
            or provenance.get("missing_blocks") != 0 or provenance.get("fallback_rows") != 0
            or not isinstance(provenance.get("timing_source_provider"), str)
            or not provenance["timing_source_provider"].strip()
            or provenance.get("timing_source_status") not in
               ("official", "third_party_undocumented")):
        raise ArtifactError("Report requires exact sport-scoped timestamp provenance")
    closing_path = calibration/"closing_calibration.parquet"
    profile_path = calibration/"trade_phase_calibration.parquet"
    estimator_summary_path = calibration/"estimator_summary.json"
    tail_path = tails/"flb_tail_summary.parquet"
    tail_summary_path = tails/"flb_summary.json"
    estimator_summary = json.loads(estimator_summary_path.read_text(encoding="utf-8"))
    tail_summary = json.loads(tail_summary_path.read_text(encoding="utf-8"))
    if (estimator_summary.get("sport") != sport
            or estimator_summary.get("method") != "fixed_decile_calibration_v1"
            or tail_summary.get("sport") != sport
            or tail_summary.get("method") != "fixed_d1_d10_tail_summary_v1"
            or tail_summary.get("inputs", {}).get("closing_calibration") != fingerprint(closing_path)
            or tail_summary.get("inputs", {}).get("trade_phase_calibration") != fingerprint(profile_path)
            or estimator_summary.get("inputs", {}).get("timestamp_declaration") != fingerprint(declaration)
            or tail_summary.get("inputs", {}).get("timestamp_declaration") != fingerprint(declaration)
            or tail_summary.get("inputs", {}).get("adapter_provenance")
               != estimator_summary.get("inputs", {}).get("adapter_provenance")
            or estimator_summary.get("inputs", {}).get("phase_contract")
               != phase_contract_fingerprint(contract_path)
            or tail_summary.get("inputs", {}).get("phase_contract")
               != phase_contract_fingerprint(contract_path)
            or not matches_artifact_fingerprint(
                estimator_summary.get("outputs", {}).get("closing_calibration"), closing_path)
            or not matches_artifact_fingerprint(
                estimator_summary.get("outputs", {}).get("trade_phase_calibration"), profile_path)
            or not matches_artifact_fingerprint(
                tail_summary.get("outputs", {}).get("flb_tail_summary"), tail_path)):
        raise ArtifactError("Report summaries do not match the displayed artifacts")
    con = duckdb.connect()
    try:
        for name, path in (("closing", closing_path), ("profiles", profile_path), ("tails", tail_path)):
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{quoted(path)}')")
        require_exact_schema(con, "closing", CLOSING_SCHEMA, "Closing calibration")
        require_exact_schema(con, "profiles", PHASE_PROFILE_SCHEMA, "Phase calibration")
        require_exact_schema(con, "tails", TAIL_SCHEMA, "FLB tails")
        closing_rows, profile_rows, tail_rows = _read(con, "closing"), _read(con, "profiles"), _read(con, "tails")
    finally:
        con.close()
    grid = fixed_bin_grid(contract)
    require_complete_unique_grid(closing_rows, ("close_definition", "profile_scope", "price_decile"),
                                 grid.closing_profile, "report closing profile")
    require_complete_unique_grid(profile_rows, ("boundary_sample", "phase", "price_decile"),
                                 grid.trade_phase_profile, "report phase profile")
    require_complete_unique_grid(tail_rows, ("analysis_scope", "close_definition", "boundary_sample", "phase"),
                                 grid.tail_summary, "report tail summary")

    closing_table = _table(
        ["Definition", "Scope", "Bin", "Games", "Mean p", "Win rate", "Mean y-p", "95% CI", "Brier", "Status"],
        [[row["close_definition"], row["profile_scope"], row["price_bin"], row["game_count"],
          row["mean_probability"], row["win_rate"], row["mean_calibration"],
          None if row["calibration_ci95_low"] is None else f'{row["calibration_ci95_low"]:.3f} to {row["calibration_ci95_high"]:.3f}',
          row["brier_score"], row["status"]] for row in closing_rows],
        "Closing calibration: A includes flagged buyers; C excludes flagged outcome-token buyers.",
    )
    closing_tails = [row for row in tail_rows if row["analysis_scope"] == "closing"]
    closing_tail_table = _table(
        ["Definition", "D1 n", "D10 n", "D1 y-p", "D10 y-p", "D10-D1", "Joint 95% CI", "Status"],
        [[row["close_definition"], row["d1_n"], row["d10_n"], row["d1_mean_calibration"],
          row["d10_mean_calibration"], row["spread_d10_minus_d1"],
          None if row["spread_ci95_low"] is None else f'{row["spread_ci95_low"]:.3f} to {row["spread_ci95_high"]:.3f}',
          row["status"]] for row in closing_tails],
        "Closing D1/D10 contrasts (filter sensitivity, not CLV).",
    )
    panels = []
    for phase in (item.key for item in contract.analysis_phases):
        cells = [row for row in profile_rows if row["phase"] == phase]
        cells.sort(key=lambda row: (row["boundary_sample"], row["price_decile"]))
        cell_table = _table(
            ["Sample", "Bin", "Trades", "Games", "Dollars", "Mean p", "Win rate", "Mean y-p", "95% CI", "Status"],
            [[row["boundary_sample"], row["price_bin"], row["trade_count"], row["game_count"],
              row["dollars"], row["mean_price"], row["win_rate"], row["mean_calibration"],
              None if row["calibration_ci95_low"] is None else f'{row["calibration_ci95_low"]:.3f} to {row["calibration_ci95_high"]:.3f}',
              row["status"]] for row in cells],
            f"Complete D1-D10 profile for {phase}; suppressed cells retain support only.",
        )
        phase_tails = [row for row in tail_rows if row["analysis_scope"] == "trade_phase" and row["phase"] == phase]
        phase_tails.sort(key=lambda row: row["boundary_sample"])
        tail_table = _table(
            ["Sample", "D1 n/games/$", "D10 n/games/$", "D1 y-p", "D10 y-p", "D10-D1", "Joint 95% CI", "Signs", "Status"],
            [[row["boundary_sample"], f'{row["d1_n"]}/{row["d1_games"]}/{row["d1_dollars"]:.2f}',
              f'{row["d10_n"]}/{row["d10_games"]}/{row["d10_dollars"]:.2f}',
              row["d1_mean_calibration"], row["d10_mean_calibration"], row["spread_d10_minus_d1"],
              None if row["spread_ci95_low"] is None else f'{row["spread_ci95_low"]:.3f} to {row["spread_ci95_high"]:.3f}',
              row["point_pattern"], row["status"]] for row in phase_tails],
            f"Adjacent {phase} FLB-tail contrasts; reported only when both tails have n ≥ 50.",
        )
        panels.append(f'<section class="panel"><h2>{html.escape(contract.phase_label(phase))}</h2>{_chart(phase,cells)}{cell_table}{tail_table}</section>')

    global_tail = _table(
        ["Scope", "Definition", "Sample", "Phase", "D1 n", "D10 n", "Spread", "Joint 95% CI", "Pattern", "Status"],
        [[row["analysis_scope"], row["close_definition"], row["boundary_sample"], row["phase"],
          row["d1_n"], row["d10_n"], row["spread_d10_minus_d1"],
          None if row["spread_ci95_low"] is None else f'{row["spread_ci95_low"]:.3f} to {row["spread_ci95_high"]:.3f}',
          row["point_pattern"], row["status"]] for row in tail_rows],
        "All 12 frozen D1/D10 tail rows.",
    )
    css = """
:root{color-scheme:light dark;--bg:#fff;--fg:#17212b;--muted:#56616d;--line:#aab4be;--card:#f5f7f8;--a:#1769aa;--c:#b34b19}
@media(prefers-color-scheme:dark){:root{--bg:#111820;--fg:#edf2f6;--muted:#b3bec8;--line:#65717d;--card:#19232d;--a:#70b7ef;--c:#f39a72}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.45 system-ui,sans-serif}main{max-width:1180px;margin:auto;padding:24px}h1,h2{line-height:1.15}.lede,.caveat{color:var(--muted);max-width:85ch}.panel{margin:28px 0;padding:18px;background:var(--card);border-radius:10px}svg{display:block;width:100%;height:auto;max-height:300px}.zero{stroke:var(--line);stroke-width:1}.series-a{fill:var(--a);stroke:var(--a)}.series-c{fill:var(--c);stroke:var(--c)}.whisker{stroke-width:1.5}.table-wrap{overflow-x:auto;margin:14px 0}table{border-collapse:collapse;width:100%;font-size:.82rem}caption{text-align:left;font-weight:650;margin-bottom:6px}th,td{border-bottom:1px solid var(--line);padding:6px;text-align:right;white-space:nowrap}th:first-child,td:first-child{text-align:left}code{overflow-wrap:anywhere}@media(max-width:600px){main{padding:12px}.panel{padding:10px;margin:18px 0}table{font-size:.72rem}}
@media print{body{color:#000;background:#fff}main{max-width:none;padding:0}.panel{break-inside:avoid;background:#fff;padding:8px}.table-wrap{overflow:visible}table{font-size:7pt;table-layout:fixed}th,td{white-space:normal;overflow-wrap:anywhere}}
"""
    document = (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f'<title>{html.escape(contract.display_name)} moneyline game dynamics</title><style>{css}</style></head><body><main>'
        f'<h1>{html.escape(contract.display_name)} moneyline calibration and FLB profiles</h1>'
        '<p class="lede">Exploratory descriptive results. Calibration is eventual home outcome minus home-normalized trade probability. Closing calibration and CLV are separate concepts.</p>'
        '<p class="caveat">Phase estimates are equal-BUY-fill weighted; dollars are descriptive. They use 0.01&lt;price&lt;0.99 and exclude only flagged outcome-token buyers. The ±30-second sample removes fills at an inclusive distance of 30 seconds from any exact phase boundary without reassignment. Nominal intervals do not establish a discovered effect.</p>'
        '<section><h2>Pregame closing calibration</h2>'+closing_table+closing_tail_table+'</section>'
        +"".join(panels)+f'<section><h2>Cross-phase tail overview</h2>{global_tail}</section>'
        f'<section><h2>Provenance and limitations</h2><p>Exact timestamps: <code>{html.escape(provenance["method"])}</code>; missing blocks: 0; fallback rows: 0. Timing provider: <code>{html.escape(provenance["timing_source_provider"])}</code>; source status: <code>{html.escape(provenance["timing_source_status"])}</code>. Buyer-bot filtering is buyer-centered and does not identify bot counterparties. Sport timing remains provider-specific upstream.</p><p>Contract SHA-256: <code>{phase_contract_fingerprint(contract_path)["sha256"]}</code>.</p></section>'
        '</main></body></html>'
    )
    inputs = (calibration, tails, declaration, contract_path)
    with fresh_run(run_dir, inputs) as staging:
        report_path = staging/"sports_flb_report.html"
        report_path.write_text(document, encoding="utf-8")
        manifest = {"schema_version": 1, "sport": sport,
                    "method": "deterministic_offline_sports_flb_report_v1",
                    "counts": {"closing_profile_rows": len(closing_rows),
                               "phase_profile_rows": len(profile_rows), "tail_rows": len(tail_rows),
                               "phase_panels": len(contract.analysis_phases)},
                    "inputs": {"closing_calibration": fingerprint(closing_path),
                               "trade_phase_calibration": fingerprint(profile_path),
                               "flb_tail_summary": fingerprint(tail_path),
                               "estimator_summary": fingerprint(estimator_summary_path),
                               "flb_summary": fingerprint(tail_summary_path),
                               "timestamp_declaration": fingerprint(declaration),
                               "phase_contract": phase_contract_fingerprint(contract_path)},
                    "output": artifact_fingerprint(report_path)}
        write_json(staging/"report_manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("sport", "calibration_run_dir", "tail_run_dir", "timestamp_declaration", "phase_contract", "run_dir"):
        parser.add_argument("--" + name.replace("_", "-"), required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    print(render_flb_report(args.sport, args.calibration_run_dir, args.tail_run_dir,
                            args.timestamp_declaration, args.phase_contract, args.run_dir))
