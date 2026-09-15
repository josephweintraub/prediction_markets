"""Render the audited nine-cohort sports results as a data-first LaTeX report."""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,
    fingerprint,
    fresh_run,
    quoted,
    write_json,
)


SPORT_ORDER = ("mlb","nfl","nba","nhl","cbb","atp","epl","cfb","wnba")
SPORT_LABELS = {
    "mlb":"MLB","nfl":"NFL","nba":"NBA","nhl":"NHL","cbb":"Men's CBB",
    "atp":"ATP","epl":"EPL","cfb":"College football","wnba":"WNBA",
}
MIN_CELL_N = 500


def _rows(con: duckdb.DuckDBPyConnection, path: Path) -> list[dict[str, Any]]:
    cursor = con.execute(f"SELECT * FROM read_parquet('{quoted(path)}')")
    names = [item[0] for item in cursor.description]
    return [dict(zip(names,row)) for row in cursor.fetchall()]


def _tex(value: Any) -> str:
    if value is None:
        return "--"
    text = str(value)
    replacements = {
        "\\":"\\textbackslash{}","&":"\\&","%":"\\%","$":"\\$","#":"\\#",
        "_":"\\_","{":"\\{","}":"\\}","~":"\\textasciitilde{}","^":"\\textasciicircum{}",
    }
    return "".join(replacements.get(char,char) for char in text)


def _n(value: int | float | None) -> str:
    return "--" if value is None else f"{int(value):,}"


def _money(value: float | None) -> str:
    return "--" if value is None else f"{value:,.0f}"


def _f(value: float | None, digits: int = 2, scale: float = 1.0) -> str:
    return "--" if value is None else f"{value*scale:.{digits}f}"


def _ci(row: dict[str, Any]) -> str:
    if row.get("calibration_ci95_low") is None:
        return "--"
    return f"[{row['calibration_ci95_low']*100:.2f}, {row['calibration_ci95_high']*100:.2f}]"


def _spread_ci(row: dict[str, Any]) -> str:
    if row.get("spread_ci95_low") is None:
        return "--"
    return f"[{row['spread_ci95_low']*100:.2f}, {row['spread_ci95_high']*100:.2f}]"


def _status(row: dict[str, Any]) -> str:
    return f"Withheld (<{MIN_CELL_N})" if row.get("suppressed") else "Reported"


def _validate_sport_domain(rows: list[dict[str, Any]], artifact: str) -> None:
    actual = {str(row["sport"]) for row in rows}
    expected = set(SPORT_ORDER)
    unexpected = sorted(actual - expected)
    missing = sorted(expected - actual)
    if unexpected or missing:
        raise ValueError(
            f"{artifact} sport domain mismatch: unexpected={unexpected}, missing={missing}"
        )


def _plot_panels(rows: list[dict[str, Any]], panels: list[tuple[str,str]], path: Path) -> None:
    selected = [row for row in rows if any(row["phase"] == key for key,_ in panels)
                and not row["suppressed"]]
    max_abs = max(
        [abs(row[value])*100 for row in selected for value in
         ("calibration_ci95_low","calibration_ci95_high") if row[value] is not None] or [5.0]
    )
    limit = max(5.0,math.ceil(max_abs/5)*5)
    columns = 2
    nrows = math.ceil(len(panels)/columns)
    fig,axes = plt.subplots(nrows,columns,figsize=(7.1,2.25*nrows),sharex=True,sharey=True)
    axes_list = list(getattr(axes,"flat",[axes]))
    for axis,(phase,label) in zip(axes_list,panels):
        cells = sorted((row for row in rows if row["phase"] == phase and not row["suppressed"]),
                       key=lambda row: row["price_decile"])
        if cells:
            x = [row["price_decile"] for row in cells]
            y = [row["mean_calibration"]*100 for row in cells]
            low = [value-row["calibration_ci95_low"]*100 for value,row in zip(y,cells)]
            high = [row["calibration_ci95_high"]*100-value for value,row in zip(y,cells)]
            axis.errorbar(x,y,yerr=[low,high],fmt="o",linestyle="none",color="#1f4e79",
                          ecolor="#666666",elinewidth=0.8,capsize=2.3,markersize=3.8)
        axis.axhline(0,color="black",linewidth=0.7)
        axis.set_title(label,fontsize=9)
        axis.set_xlim(0.5,10.5)
        axis.set_ylim(-limit,limit)
        axis.set_xticks(range(1,11))
        axis.tick_params(labelsize=7)
        axis.spines[["top","right"]].set_visible(False)
    for axis in axes_list[len(panels):]:
        axis.set_visible(False)
    for axis in axes_list[-columns:]:
        if axis.get_visible():
            axis.set_xlabel("Bought-price bin (D1--D10)",fontsize=8)
    for index,axis in enumerate(axes_list):
        if axis.get_visible() and index % columns == 0:
            axis.set_ylabel("Outcome - price (pp)",fontsize=8)
    fig.tight_layout(pad=0.7)
    fig.savefig(path,format="pdf",bbox_inches="tight")
    plt.close(fig)


def _plot_closing(rows: list[dict[str, Any]], sample: str, path: Path) -> None:
    panels = [(sport,SPORT_LABELS[sport]) for sport in SPORT_ORDER]
    filtered = [row for row in rows if row["close_sample"] == sample and not row["suppressed"]]
    max_abs = max(
        [abs(row[value])*100 for row in filtered for value in
         ("calibration_ci95_low","calibration_ci95_high") if row[value] is not None] or [5.0]
    )
    limit = max(5.0,math.ceil(max_abs/5)*5)
    columns = 3
    nrows = math.ceil(len(panels) / columns)
    fig,axes = plt.subplots(nrows,columns,figsize=(7.1,2.15*nrows),sharex=True,sharey=True)
    axes_list = list(getattr(axes,"flat",[axes]))
    for axis,(sport,label) in zip(axes_list,panels):
        cells = sorted((row for row in filtered if row["sport"] == sport),
                       key=lambda row: row["price_decile"])
        if cells:
            x = [row["price_decile"] for row in cells]
            y = [row["mean_calibration"]*100 for row in cells]
            low = [value-row["calibration_ci95_low"]*100 for value,row in zip(y,cells)]
            high = [row["calibration_ci95_high"]*100-value for value,row in zip(y,cells)]
            axis.errorbar(x,y,yerr=[low,high],fmt="o",linestyle="none",color="#1f4e79",
                          ecolor="#666666",elinewidth=0.7,capsize=2,markersize=3.2)
        axis.axhline(0,color="black",linewidth=0.6)
        axis.set_title(label,fontsize=8)
        axis.set_xlim(0.5,10.5); axis.set_ylim(-limit,limit)
        axis.set_xticks((1,3,5,7,10)); axis.tick_params(labelsize=6.5)
        axis.spines[["top","right"]].set_visible(False)
    for axis in axes_list[len(panels):]:
        axis.set_visible(False)
    for axis in axes[-1,:]:
        if axis.get_visible(): axis.set_xlabel("Bought-price bin",fontsize=7)
    for axis in axes[:,0]: axis.set_ylabel("Outcome - price (pp)",fontsize=7)
    fig.tight_layout(pad=0.6)
    fig.savefig(path,format="pdf",bbox_inches="tight")
    plt.close(fig)


def _closing_longtable(rows: list[dict[str, Any]], sample: str, caption: str, label: str) -> str:
    body = []
    for sport in SPORT_ORDER:
        for row in sorted((item for item in rows if item["sport"]==sport and item["close_sample"]==sample),
                          key=lambda item:item["price_decile"]):
            body.append(" & ".join((
                _tex(SPORT_LABELS[sport]),f"D{row['price_decile']}",_n(row["close_count"]),
                _n(row["event_count"]),_money(row["dollars"]),_f(row["mean_price"],3),
                _f(row["win_rate"],3),
                _f(row["mean_calibration"],2,100),_tex(_ci(row)),_f(row["brier_score"],3),
                _tex(_status(row)),
            ))+" \\\\")
    return rf"""{{\scriptsize
\begin{{longtable}}{{llrrrrrrrrl}}
\caption{{{_tex(caption)}}}\label{{{label}}}\\
\toprule
Sport & Bin & Closes & Events & Dollars & Mean $p$ & Win rate & Error (pp) & 95\% CI & Brier & Status \\
\midrule
\endfirsthead
\toprule
Sport & Bin & Closes & Events & Dollars & Mean $p$ & Win rate & Error (pp) & 95\% CI & Brier & Status \\
\midrule
\endhead
{chr(10).join(body)}
\bottomrule
\end{{longtable}}
}}"""


def _phase_longtable(rows: list[dict[str, Any]], sport: str) -> str:
    body = []
    ordered = sorted((row for row in rows if row["sport"]==sport),
                     key=lambda row:(row["phase_order"],row["price_decile"]))
    for row in ordered:
        body.append(" & ".join((
            _tex(row["phase_label"]),f"D{row['price_decile']}",_n(row["trade_count"]),
            _n(row["event_count"]),_money(row["dollars"]),_f(row["mean_price"],3),
            _f(row["win_rate"],3),
            _f(row["mean_calibration"],2,100),_tex(_ci(row)),_tex(_status(row)),
        ))+" \\\\")
    timing_label = (
        "scoreboard-start elapsed phase" if sport == "atp"
        else "literal event phase"
    )
    return rf"""{{\scriptsize
\begin{{longtable}}{{llrrrrrrrl}}
\caption{{{_tex(SPORT_LABELS[sport])}: fixed-bin calibration by {_tex(timing_label)}}}\\
\toprule
Phase & Bin & Trades & Events & Dollars & Mean $p$ & Win rate & Error (pp) & 95\% CI & Status \\
\midrule
\endfirsthead
\toprule
Phase & Bin & Trades & Events & Dollars & Mean $p$ & Win rate & Error (pp) & 95\% CI & Status \\
\midrule
\endhead
{chr(10).join(body)}
\bottomrule
\end{{longtable}}
}}"""


def render_report(estimator_run_dir: str | Path, timing_run_dir: str | Path,
                  run_dir: str | Path) -> dict[str, Any]:
    estimator = Path(estimator_run_dir).expanduser().resolve()
    timing = Path(timing_run_dir).expanduser().resolve()
    phase_path = estimator/"phase_calibration.parquet"
    closing_path = estimator/"closing_calibration.parquet"
    tails_path = estimator/"flb_spreads.parquet"
    phase_obs_path = estimator/"normalized_phase_trades.parquet"
    timing_path = timing/"event_timing.parquet"
    inputs = (phase_path,closing_path,tails_path,phase_obs_path,timing_path)
    if any(not path.is_file() for path in inputs):
        raise FileNotFoundError([str(path) for path in inputs if not path.is_file()])
    con = duckdb.connect()
    try:
        phase_rows = _rows(con,phase_path)
        closing_rows = _rows(con,closing_path)
        tail_rows = _rows(con,tails_path)
        _validate_sport_domain(phase_rows,"phase calibration")
        _validate_sport_domain(closing_rows,"closing calibration")
        _validate_sport_domain(tail_rows,"FLB spread")
        cursor = con.execute(
            f"""SELECT sport,count(*)::BIGINT trades,count(DISTINCT event_id)::BIGINT events,
                       sum(usdc)::DOUBLE dollars
                FROM read_parquet('{quoted(phase_obs_path)}') GROUP BY 1"""
        )
        coverage = [dict(zip((item[0] for item in cursor.description),row)) for row in cursor.fetchall()]
    finally:
        con.close()
    coverage_summary = defaultdict(lambda:{"trades":0,"events":0,"dollars":0.0})
    for row in coverage:
        record = coverage_summary[row["sport"]]
        record.update(trades=row["trades"],events=row["events"],dollars=row["dollars"])
    _validate_sport_domain(coverage,"normalized phase trades")
    target = Path(run_dir).expanduser().resolve()
    with fresh_run(target,inputs) as staging:
        figures = staging/"figures"; figures.mkdir()
        _plot_closing(closing_rows,"all_trades",figures/"closing_all_trades.pdf")
        _plot_closing(closing_rows,"filtered_trades",figures/"closing_filtered_trades.pdf")
        for sport in SPORT_ORDER:
            sport_rows = [row for row in phase_rows if row["sport"]==sport]
            panels = []
            seen = set()
            for row in sorted(sport_rows,key=lambda item:item["phase_order"]):
                if row["phase"] not in seen:
                    panels.append((row["phase"],row["phase_label"])); seen.add(row["phase"])
            _plot_panels(sport_rows,panels,figures/f"phase_{sport}.pdf")

        coverage_body = []
        for sport in SPORT_ORDER:
            record = coverage_summary[sport]
            source = "Exact provider phases"
            if sport == "atp":
                source = "ESPN start + archived duration thirds"
            coverage_body.append(" & ".join((
                _tex(SPORT_LABELS[sport]),_n(record["events"]),_n(record["trades"]),
                _f(record["dollars"],1),_tex(source),
            ))+" \\\\")

        spread_body = []
        for row in sorted((item for item in tail_rows if item["analysis_scope"]=="trade_phase"),
                          key=lambda item:(SPORT_ORDER.index(item["sport"]),item["phase_order"])):
            spread_body.append(" & ".join((
                _tex(SPORT_LABELS[row["sport"]]),_tex(row["phase_label"]),
                _n(row["d1_n"]),_f(row["d1_mean_calibration"],2,100),
                _n(row["d10_n"]),_f(row["d10_mean_calibration"],2,100),
                _f(row["spread_d10_minus_d1"],2,100),_tex(_spread_ci(row)),
                _tex(_status(row)),
            ))+" \\\\")

        phase_sections = []
        for sport in SPORT_ORDER:
            phase_description = (
                "scoreboard-start elapsed phase" if sport == "atp"
                else "literal event phase"
            )
            phase_sections.append(rf"""
\section{{{_tex(SPORT_LABELS[sport])}}}
\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.98\textwidth]{{figures/phase_{sport}.pdf}}
\caption{{Bought-contract calibration by {_tex(phase_description)}. Points are fixed-bin estimates; whiskers are 95\% clustered intervals.}}
\end{{figure}}
\clearpage
\begin{{landscape}}
{_phase_longtable(phase_rows,sport)}
\end{{landscape}}
\clearpage
""")

        source = rf"""\documentclass[10pt]{{article}}
\usepackage[margin=0.72in]{{geometry}}
\usepackage{{booktabs,longtable,graphicx,pdflscape}}
\usepackage[T1]{{fontenc}}
\usepackage{{lmodern}}
\usepackage{{microtype}}
\usepackage[hidelinks]{{hyperref}}
\setlength{{\LTpre}}{{4pt}}
\setlength{{\LTpost}}{{4pt}}
\renewcommand{{\arraystretch}}{{0.93}}
\title{{Moneyline Calibration Across Major Sports}}
\author{{}}
\date{{15 September 2026}}
\begin{{document}}
\maketitle
\vspace{{-2em}}
\noindent Calibration error is the eventual outcome of the bought contract minus its purchase price. Closing calibration uses the last pregame BUY fill and is not CLV. Team-sport phases use provider wallclocks; ATP uses the ESPN scoreboard time plus archived elapsed duration. The filtered sample applies $0.01<p<0.99$ and excludes flagged outcome-token buyers. Estimates with fewer than {MIN_CELL_N:,} observations are withheld; support remains shown.

\begin{{table}}[!htbp]
\centering\small
\caption{{Analysis support and timing construction}}
\begin{{tabular}}{{lrrrl}}
\toprule
Cohort & Events & Trades & Dollars & Timing \\
\midrule
{chr(10).join(coverage_body)}
\bottomrule
\end{{tabular}}
\end{{table}}

\clearpage
\section{{Closing calibration}}
\begin{{figure}}[!htbp]
\centering\includegraphics[width=0.98\textwidth]{{figures/closing_all_trades.pdf}}
\caption{{Closing calibration, all valid BUY fills. Points are isolated fixed-bin estimates.}}
\end{{figure}}
\clearpage
\begin{{landscape}}
{_closing_longtable(closing_rows,"all_trades","Closing calibration: all valid trades","tab:closing-all")}
\end{{landscape}}
\clearpage
\begin{{figure}}[!htbp]
\centering\includegraphics[width=0.98\textwidth]{{figures/closing_filtered_trades.pdf}}
\caption{{Closing calibration, filtered BUY fills. Points are isolated fixed-bin estimates.}}
\end{{figure}}
\clearpage
\begin{{landscape}}
{_closing_longtable(closing_rows,"filtered_trades","Closing calibration: filtered trades","tab:closing-filtered")}
\end{{landscape}}

\clearpage
\begin{{landscape}}
\section{{FLB spread by event phase}}
{{\scriptsize
\begin{{longtable}}{{llrrrrrrl}}
\caption{{D10 minus D1 calibration spread; full fixed-bin profiles follow}}\\
\toprule
Sport & Phase & D1 $n$ & D1 err. & D10 $n$ & D10 err. & Spread & 95\% CI & Status \\
\midrule
\endfirsthead
\toprule
Sport & Phase & D1 $n$ & D1 err. & D10 $n$ & D10 err. & Spread & 95\% CI & Status \\
\midrule
\endhead
{chr(10).join(spread_body)}
\bottomrule
\end{{longtable}}
}}
\end{{landscape}}
\clearpage
{''.join(phase_sections)}
\end{{document}}
"""
        tex_path = staging/"major_sports_game_dynamics.tex"
        tex_path.write_text(source,encoding="utf-8")
        manifest = {
            "schema_version":2,
            "stage":"data_first_major_sports_latex_v2",
            "included_sports":list(SPORT_ORDER),
            "suppression_threshold":MIN_CELL_N,
            "counts":{"phase_rows":len(phase_rows),"closing_rows":len(closing_rows),
                      "tail_rows":len(tail_rows),"figures":2+len(SPORT_ORDER)},
            "inputs":{path.name:fingerprint(path) for path in inputs},
            "outputs":{
                "tex":artifact_fingerprint(tex_path),
                "figures":sorted(path.name for path in figures.iterdir()),
            },
        }
        write_json(staging/"report_manifest.json",manifest)
    return manifest


def parse_args(argv: list[str] | None=None) -> argparse.Namespace:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--estimator-run",required=True)
    parser.add_argument("--timing-run",required=True)
    parser.add_argument("--run-dir",required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None=None) -> None:
    args=parse_args(argv)
    print(json.dumps(render_report(args.estimator_run,args.timing_run,args.run_dir),sort_keys=True))


if __name__=="__main__":
    main()
