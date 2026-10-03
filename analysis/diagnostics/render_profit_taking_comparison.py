"""Render a standalone data-first LaTeX report from completed compact summaries.

No trade history, wallet identifiers or estimators are read. Inline PGFPlots
keeps the source portable to the native single-file editor. Source fingerprints
and reconciliation evidence belong in the machine-facing manifest, not the PDF.
"""
from __future__ import annotations

import argparse
from datetime import datetime,timezone
from decimal import Decimal
import itertools
import json
import math
from pathlib import Path
import sys
from typing import Any

import duckdb

from analysis.diagnostics.profit_taking_contribution import SPORTS,SAMPLES,WINDOWS,WEIGHTS,SUPPORT_FLOOR
from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,fingerprint,fresh_run,quoted,write_json,
)


GRAINS=("matched_execution","own_order_event")
FILES=("profiles.parquet","tails.parquet","late_delta.parquet","grain_totals.parquet","mechanism_summary.parquet")
STAGE="sports_profit_taking_two_grain_contributions_v1"
GRAIN_LABELS={"matched_execution":"Trades","own_order_event":"Order events"}
SPORT_LABELS={sport:sport.upper() for sport in SPORTS}
PARTS=("calibration","exit_contribution","hedge_contribution","remaining_contribution")
DELTAS=("spread_delta","exit_spread_delta","hedge_spread_delta","remaining_spread_delta")
MECHANISM_COUNTS=("own_actions","own_buys","own_sells","favorite_sell_events","primary_exit_events","profitable_hedge_events")
MECHANISM_QUANTITIES=("sell_gross_quantity_micro","matched_sale_quantity_micro","prior_matched_sale_quantity_micro",
    "gross_profitable_disposal_quantity_micro","primary_exit_quantity_micro","profitable_hedge_net_quantity_micro",
    "unmatched_sale_quantity_micro","unmatched_favorite_sale_quantity_micro","merge_disposal_gross_quantity_micro")


def number(value: Any,*,signed: bool=True) -> str:
    if value is None:
        return "withheld"
    if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value):
        raise ValueError("Displayed estimates must be finite numeric values or null")
    value=0.0 if round(float(value),2)==0 else float(value)
    return format(value,"+.2f" if signed else ".2f")


def count(value: Any) -> str:
    if isinstance(value,bool) or not isinstance(value,int) or value<0:
        raise ValueError("Support must be a nonnegative integer")
    return f"{value:,}"


def millions(value: Any) -> str:
    if not _close(value,value) or value<0:
        raise ValueError("Mechanism quantities must be finite and nonnegative")
    scaled=value/1e12  # micro-contracts to millions of contracts
    if 0<scaled<.0005:
        return r"$<0.001$"
    return f"{scaled:,.3f}"


def _indexed(rows: list[dict],keys: tuple[str,...]) -> dict[tuple,dict]:
    result={}
    for row in rows:
        try:
            key=tuple(row[name] for name in keys)
        except KeyError as exc:
            raise ValueError(f"Missing saved summary key: {exc}") from exc
        if key in result:
            raise ValueError(f"Duplicate saved summary key: {key}")
        result[key]=row
    return result


def _close(a: Any,b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return (not isinstance(a,bool) and not isinstance(b,bool)
            and isinstance(a,(int,float)) and isinstance(b,(int,float))
            and math.isfinite(a) and math.isfinite(b)
            and math.isclose(a,b,rel_tol=1e-9,abs_tol=1e-10))


def _saved_value(value: Any) -> Any:
    # The producer exports aggregate HUGEINT quantities as DECIMAL(38,0),
    # because DuckDB's direct HUGEINT-to-Parquet conversion becomes DOUBLE.
    # Keep their exact integer values in both validation and displayed JSON.
    if isinstance(value,Decimal):
        if not value.is_finite() or value!=value.to_integral_value():
            raise ValueError("Saved decimal quantities must be finite integers")
        return int(value)
    return value


def validate_saved_evidence(data: dict[str,list[dict]]) -> dict[str,dict]:
    """Reconcile saved displayed estimates, counts, suppression and units."""
    profiles=_indexed(data["profiles"],("grain","sport","sample","window","weighting","price_bin"))
    tails=_indexed(data["tails"],("grain","sport","sample","window","weighting"))
    changes=_indexed(data["late_delta"],("grain","sport","sample","weighting"))
    totals=_indexed(data["grain_totals"],("grain","sport"))
    mechanisms=_indexed(data["mechanism_summary"],("sport","window","role"))
    for observed,expected,label in (
        (profiles,itertools.product(GRAINS,SPORTS,SAMPLES,WINDOWS,WEIGHTS,range(1,11)),"profile"),
        (tails,itertools.product(GRAINS,SPORTS,SAMPLES,WINDOWS,WEIGHTS),"tail"),
        (changes,itertools.product(GRAINS,SPORTS,SAMPLES,WEIGHTS),"late-change"),
        (mechanisms,itertools.product(SPORTS,WINDOWS,("all","passive","active_aggregate")),"mechanism"),
    ):
        if set(observed)!=set(expected):
            raise ValueError(f"Incomplete or unexpected saved {label} grid")
    for row in profiles.values():
        count(row["n_executions"])
        weight=row["weight_total"]
        if not _close(weight,weight) or weight<0:
            raise ValueError("Invalid saved original weight denominator")
        suppressed=row["n_executions"]<SUPPORT_FLOOR or weight<=0
        if not isinstance(row["suppressed"],bool) or row["suppressed"]!=suppressed:
            raise ValueError("Saved profile suppression does not match original support")
        if row.get("uncertainty_status")!="not_estimated_descriptive":
            raise ValueError("This renderer requires explicitly descriptive estimates")
        for field in PARTS:
            raw=row[field+"_raw"]
            if weight>0 and not _close(raw,raw):
                raise ValueError("Nonfinite or missing supported raw profile estimate")
            if not _close(row[field],None if suppressed else raw):
                raise ValueError("Saved profile display estimate conflicts with suppression")
        if weight>0 and not _close(row["calibration_raw"],sum(row[field+"_raw"] for field in PARTS[1:])):
            raise ValueError("Saved profile components are not additive")
    for key,row in tails.items():
        d1=profiles[key+(1,)]
        d10=profiles[key+(10,)]
        suppressed=d1["suppressed"] or d10["suppressed"]
        if row["suppressed"]!=suppressed or row["d1_n_executions"]!=d1["n_executions"] \
                or row["d10_n_executions"]!=d10["n_executions"]:
            raise ValueError("Saved tail support conflicts with the full profile")
        for source,target in zip(PARTS,("spread","exit_spread_contribution","hedge_spread_contribution","remaining_spread_contribution")):
            expected=None if d1[source+"_raw"] is None or d10[source+"_raw"] is None \
                else d10[source+"_raw"]-d1[source+"_raw"]
            if not _close(row[target+"_raw"],expected) or not _close(row[target],None if suppressed else expected):
                raise ValueError("Saved tail contrast differs from its probability-bin points")
    for key,row in changes.items():
        grain,sport,sample,weighting=key
        previous=tails[(grain,sport,sample,"live_95_99",weighting)]
        final=tails[(grain,sport,sample,"live_99_100",weighting)]
        suppressed=previous["suppressed"] or final["suppressed"]
        if row["contrast"]!="live_99_100_minus_live_95_99" or row["suppressed"]!=suppressed:
            raise ValueError("Saved late change has an incorrect contrast or suppression")
        for phase,source in (("previous",previous),("final",final)):
            for bin_name in ("d1","d10"):
                if row[f"{phase}_{bin_name}_n_executions"]!=source[f"{bin_name}_n_executions"]:
                    raise ValueError("Saved late-change support differs from its tail windows")
        for field,tail_field in zip(DELTAS,("spread","exit_spread_contribution","hedge_spread_contribution","remaining_spread_contribution")):
            a,b=previous[tail_field+"_raw"],final[tail_field+"_raw"]
            expected=None if a is None or b is None else b-a
            shown=None if suppressed else expected
            if not _close(row[field+"_raw"],expected) or not _close(row[field],shown) \
                    or not _close(row[field+"_pp"],None if shown is None else 100*shown):
                raise ValueError("Saved late-change values or percentage-point units do not reconcile")
    if set(totals)-set(itertools.product(GRAINS,SPORTS)):
        raise ValueError("Unexpected grain-total keys")
    for sport in SPORTS:
        a=totals.get((GRAINS[0],sport))
        b=totals.get((GRAINS[1],sport))
        if (a is None)!=(b is None):
            raise ValueError("Missing cross-grain total coverage")
        if a is not None:
            for field in ("gross_quantity_micro","gross_cash_micro","n_own_buy_events"):
                count(a[field])
                count(b[field])
                if a[field]!=b[field]:
                    raise ValueError("Cross-grain source exposure fails saved reconciliation")
            a_numerator,b_numerator=a["quantity_residual_numerator"],b["quantity_residual_numerator"]
            if not _close(a_numerator,a_numerator) or not _close(b_numerator,b_numerator) \
                    or abs(a_numerator-b_numerator)>1e-10*max(a["gross_quantity_micro"],1):
                raise ValueError("Cross-grain quantity-weighted residual fails saved reconciliation")
    for key,row in mechanisms.items():
        for field in MECHANISM_COUNTS+MECHANISM_QUANTITIES:
            count(row[field])
        if row["population"]!="all_own_actions_no_price_or_actor_filter" \
                or row["history_status"]!="trade_implied_only_opening_and_nontrade_movements_unknown":
            raise ValueError("Saved mechanism population differs from unfiltered trade-implied history")
        if row["own_actions"]!=row["own_buys"]+row["own_sells"] \
                or not row["primary_exit_events"]<=row["favorite_sell_events"]<=row["own_sells"] \
                or row["profitable_hedge_events"]>row["own_buys"]:
            raise ValueError("Saved mechanism event counts are inconsistent")
        if not row["primary_exit_quantity_micro"]<=row["gross_profitable_disposal_quantity_micro"] \
                <=row["prior_matched_sale_quantity_micro"]<=row["matched_sale_quantity_micro"]<=row["sell_gross_quantity_micro"] \
                or not row["unmatched_favorite_sale_quantity_micro"]<=row["unmatched_sale_quantity_micro"]<=row["sell_gross_quantity_micro"]:
            raise ValueError("Saved mechanism disposal quantities are inconsistent")
        merge=row["allocated_primary_merge_quantity_micro"]
        tolerance=1e-10*max(row["sell_gross_quantity_micro"],1)
        if not _close(merge,merge) or merge<0 \
                or merge>min(row["primary_exit_quantity_micro"],row["merge_disposal_gross_quantity_micro"])+tolerance:
            raise ValueError("Saved allocated MERGE quantity is inconsistent")
        if key[2]=="all":
            parts=[mechanisms[key[:2]+(role,)] for role in ("passive","active_aggregate")]
            for field in MECHANISM_COUNTS+MECHANISM_QUANTITIES:
                if row[field]!=sum(part[field] for part in parts):
                    raise ValueError("Saved mechanism roles do not add to all-history totals")
            if not _close(merge,sum(part["allocated_primary_merge_quantity_micro"] for part in parts)):
                raise ValueError("Saved allocated MERGE roles do not add to their total")
    return {"profiles":profiles,"tails":tails,"late_delta":changes,"grain_totals":totals,"mechanism_summary":mechanisms}


def load_evidence(directory: Path) -> tuple[dict,dict,list[Path]]:
    manifest_path=directory/"manifest.json"
    manifest=json.loads(manifest_path.read_text())
    if manifest.get("completion_status")!="complete" or manifest.get("stage")!=STAGE \
            or manifest.get("schema_version")!=1:
        raise ValueError("Contribution stage is incomplete or has an unsupported contract")
    contract=manifest.get("contract",{})
    for name,values in (("grains",GRAINS),("sports",SPORTS),("samples",SAMPLES),("windows",WINDOWS),("weights",WEIGHTS)):
        if set(contract.get(name,()))!=set(values):
            raise ValueError(f"Contribution contract differs from the approved {name}")
    data={}
    inputs=[manifest_path]
    con=duckdb.connect()
    try:
        for name in FILES:
            path=directory/name
            if manifest.get("outputs",{}).get(name)!=artifact_fingerprint(path):
                raise ValueError(f"Saved contribution fingerprint mismatch: {name}")
            cursor=con.execute(f"SELECT * FROM read_parquet('{quoted(path)}')")
            columns=[column[0] for column in cursor.description]
            records=[dict(zip(columns,(_saved_value(value) for value in row))) for row in cursor.fetchall()]
            stem=path.stem
            if manifest.get("counts",{}).get(stem)!=len(records):
                raise ValueError(f"Saved contribution count mismatch: {name}")
            data[stem]=records
            inputs.append(path)
    finally:
        con.close()
    return manifest,validate_saved_evidence(data),inputs


def _support(row: dict,phase: str) -> str:
    return r"\mbox{"+count(row[phase+"_d1_n_executions"])+" / "+count(row[phase+"_d10_n_executions"])+"}"


def _change_table(changes: dict,sample: str) -> str:
    label="All trades" if sample=="all_trades" else "Filtered trades"
    lines=[r"\begin{table}[ht]\centering\small",r"\setlength{\tabcolsep}{3pt}",
        r"\caption{"+label+r": change from 95--99\% to the final 1\%.}",
        r"\begin{tabular}{llrrrrrr}\toprule",
        r"Sport & Unit & Prior D1 / D10 & Final D1 / D10 & $\Delta S$ & Direct & Hedge & Rest \\\midrule"]
    for sport in SPORTS:
        for grain in GRAINS:
            row=changes[(grain,sport,sample,"fill")]
            values=[SPORT_LABELS[sport],GRAIN_LABELS[grain],_support(row,"previous"),_support(row,"final")]
            values += [number(row[field+"_pp"]) for field in DELTAS]
            lines.append(" & ".join(values)+r" \\")
    lines += [r"\bottomrule\end{tabular}",
        r"\par\vspace{4pt}\footnotesize All effects are percentage points, per observation. Prior and final columns give D1/D10 support. Withheld: at least one of the four tails has fewer than 500 observations or zero weight. Direct, hedge and rest use the original bin denominators and sum to $\Delta S$.",r"\end{table}"]
    return "\n".join(lines)


def _plot_pages(profiles: dict) -> str:
    pages=[]
    for start in range(0,len(SPORTS),3):
        group=SPORTS[start:start+3]
        lines=[r"\clearpage\begin{figure}[ht]\centering",r"\begin{tikzpicture}",
            r"\begin{groupplot}[group style={group size=2 by 3,horizontal sep=.40in,vertical sep=.72in},",
            r"width=.42\textwidth,height=2.00in,scale only axis,",
            r"xmin=.5,xmax=10.5,xtick={1,...,10},xticklabels={D1,D2,D3,D4,D5,D6,D7,D8,D9,D10},",
            r"tick label style={font=\scriptsize},label style={font=\small},title style={font=\small},",
            r"xlabel={Probability bin},ylabel={Calibration (pp)},axis lines=left]",
        ]
        for sport in group:
            available=[100*profiles[(grain,sport,"all_trades",window,"fill",bin_id)]["calibration"]
                       for grain in GRAINS for window in ("live_95_99","live_99_100") for bin_id in range(1,11)
                       if profiles[(grain,sport,"all_trades",window,"fill",bin_id)]["calibration"] is not None]
            low=min([0]+available)
            high=max([0]+available)
            margin=max(.4,(high-low)*.12)
            low=min(-1,low-margin)
            high=max(1,high+margin)
            for grain in GRAINS:
                lines.append(r"\nextgroupplot[title={"+SPORT_LABELS[sport]+": "+GRAIN_LABELS[grain]+"},ymin="+f"{low:.8g},ymax={high:.8g}"+"]")
                lines.append(r"\addplot[gray,densely dashed,no marks] coordinates {(.5,0) (10.5,0)};")
                n=0
                for window,offset,style in (("live_95_99",-.10,"only marks,mark=o,mark size=2pt,gray"),
                                            ("live_99_100",.10,"only marks,mark=triangle*,mark size=2pt,black")):
                    coordinates=[]
                    for bin_id in range(1,11):
                        row=profiles[(grain,sport,"all_trades",window,"fill",bin_id)]
                        if row["calibration"] is not None:
                            coordinates.append(f"({bin_id+offset:.2f},{100*row['calibration']:.10g})")
                    n+=len(coordinates)
                    if coordinates:
                        lines.append(r"\addplot["+style+"] coordinates {"+" ".join(coordinates)+"};")
                if not n:
                    lines.append(r"\node[font=\small,align=center] at (axis description cs:.5,.6) {All bins withheld};")
        lines += [r"\end{groupplot}\end{tikzpicture}",
            r"\caption{All-trade calibration profiles, per observation. Open circles: 95--99\%; filled triangles: final 1\%. The two units share each sport's vertical scale. Suppressed bins are omitted. Estimates are descriptive, without uncertainty intervals.}",r"\end{figure}"]
        pages.append("\n".join(lines))
    return "\n".join(pages)


def _mechanism_table(mechanisms: dict) -> str:
    lines=[r"\begin{table}[ht]\centering\small",r"\setlength{\tabcolsep}{3pt}",
        r"\caption{Final 1\% mechanisms, all history. Quantities are millions of contracts.}",
        r"\begin{tabular}{lrrrrrr}\toprule",
        r"Sport & \shortstack[r]{Qualified\\favorite sales} & \shortstack[r]{Normal\\gross qty.} & \shortstack[r]{Merge\\gross qty.} & \shortstack[r]{Hedge\\purchases} & \shortstack[r]{Hedge\\net qty.} & \shortstack[r]{Unmatched\\gross qty.} \\\midrule"]
    for sport in SPORTS:
        row=mechanisms[(sport,"live_99_100","all")]
        normal=max(0,row["primary_exit_quantity_micro"]-row["allocated_primary_merge_quantity_micro"])
        values=[SPORT_LABELS[sport],count(row["primary_exit_events"]),millions(normal),
                millions(row["allocated_primary_merge_quantity_micro"]),count(row["profitable_hedge_events"]),
                millions(row["profitable_hedge_net_quantity_micro"]),millions(row["unmatched_favorite_sale_quantity_micro"])]
        lines.append(" & ".join(values)+r" \\")
    lines += [r"\bottomrule\end{tabular}",
        r"\par\vspace{4pt}\footnotesize Original own-action events and physical quantities, before BUY price or wallet filters. Qualified sales are profitable against strictly prior FIFO acquisitions and reduce positive net favorite exposure. Normal and merge columns allocate these qualified gross disposals using the same own-action labels; merge has no actual BUY. Hedge volume is qualified net acquired quantity. Unmatched volume is favorite-sale gross quantity without an observed acquisition match, not a global inventory-certification measure.",r"\end{table}"]
    return "\n".join(lines)


def _robustness(changes: dict) -> str:
    lines=[r"\clearpage\begin{table}[ht]\centering\small",r"\setlength{\tabcolsep}{6pt}",
        r"\caption{Terminal spread change under economic-size and equal-market weights (pp).}",
        r"\begin{tabular}{llrrrr}\toprule",r"& & \multicolumn{2}{c}{All trades} & \multicolumn{2}{c}{Filtered trades} \\",
        r"\cmidrule(lr){3-4}\cmidrule(lr){5-6}",
        r"Sport & Unit & Dollar & Equal-market & Dollar & Equal-market \\\midrule"]
    for sport in SPORTS:
        for grain in GRAINS:
            values=[SPORT_LABELS[sport],GRAIN_LABELS[grain]]
            values += [number(changes[(grain,sport,sample,weight)]["spread_delta_pp"])
                       for sample in ("all_trades","filtered") for weight in ("dollar","equal_market")]
            lines.append(" & ".join(values)+r" \\")
    lines += [r"\bottomrule\end{tabular}",
        r"\par\vspace{4pt}\footnotesize Dollar weights use gross execution collateral. Equal-market weights first normalize those dollars inside each original market-by-bin population, then average markets equally. Withheld estimates fail the same four-tail support rule. Grain differences can arise mechanically from price averaging, binning and weights; they are not additional evidence of a mechanism.",r"\end{table}"]
    return "\n".join(lines)


def create_latex(evidence: dict) -> str:
    return "\n".join([
        r"\documentclass[10pt,letterpaper]{article}",
        r"\usepackage[margin=.70in]{geometry}",r"\usepackage{booktabs,amsmath,pgfplots}",
        r"\usepgfplotslibrary{groupplots}",r"\pgfplotsset{compat=1.18}",
        r"\setlength{\parindent}{0pt}",r"\setlength{\parskip}{5pt}",
        r"\begin{document}",r"\begin{center}\Large Terminal calibration and allocated profitable exits\end{center}",
        r"\small Accepted resolved moneyline markets in nine sports; identical all-history own-action FIFO labels under two observation units.",
        r"\footnotesize Producer: \texttt{\detokenize{build_profit_taking_contribution.py}}; saved \texttt{profiles.parquet}, \texttt{tails.parquet}, and \texttt{late\_delta.parquet}.",
        r"\small Calibration is $Y-P$; $S=C_{10}-C_1$ uses fixed-width probability bins. Tables report final $[.99,1]$ minus prior $[.95,.99)$ normalized live time, in percentage points. Trades are genuine matched BUY legs; order events are own \texttt{OrderFilled} BUY logs, including one active aggregate at execution VWAP, not lifetime order hashes.",
        r"\small All trades use $0<P<1$, including flagged wallets. Filtered trades use $.01<P<.99$ and unflagged wallets. Matching history is unfiltered. Direct labels map profitable, net-exposure-reducing favorite sales to actual buyers; hedge labels identify complementary purchases offsetting prior favorite exposure. The same own-action labels are allocated uniformly, not independently certified as profitable per execution leg. Rest is the unallocated contribution.",
        r"\small All estimates are descriptive accounting, not causal price effects or certified holdings. Event windows use inherited accepted clocks, not uniformly observed actual boundaries. Price averaging, binning and weights can mechanically change results across units.",
        _change_table(evidence["late_delta"],"all_trades"),
        r"\clearpage",_change_table(evidence["late_delta"],"filtered"),
        _mechanism_table(evidence["mechanism_summary"]),
        _plot_pages(evidence["profiles"]),_robustness(evidence["late_delta"]),r"\end{document}",
    ])+"\n"


def render_report(analysis_dir: Path,run_dir: Path) -> dict:
    source_manifest,evidence,inputs=load_evidence(analysis_dir)
    initial={path.name:fingerprint(path) for path in inputs}
    with fresh_run(run_dir,inputs) as staging:
        tex_path=staging/"profit_taking_comparison.tex"
        tex_path.write_text(create_latex(evidence),encoding="utf-8")
        displayed={
            "overview":[evidence["late_delta"][(grain,sport,sample,"fill")]
                        for sample in ("all_trades","filtered") for sport in SPORTS for grain in GRAINS],
            "profiles":[evidence["profiles"][(grain,sport,"all_trades",window,"fill",bin_id)]
                        for sport in SPORTS for grain in GRAINS for window in ("live_95_99","live_99_100")
                        for bin_id in range(1,11)],
            "robustness":[evidence["late_delta"][(grain,sport,sample,weight)]
                          for sport in SPORTS for grain in GRAINS for sample in ("all_trades","filtered")
                          for weight in ("dollar","equal_market")],
            "mechanisms":[evidence["mechanism_summary"][(sport,"live_99_100","all")] for sport in SPORTS],
        }
        write_json(staging/"displayed_evidence.json",displayed)
        if {path.name:fingerprint(path) for path in inputs}!=initial:
            raise ValueError("Saved compact source artifacts changed while rendering")
        manifest={
            "stage":"profit_taking_comparison_report_v1","schema_version":1,"completion_status":"complete",
            "created_at_utc":datetime.now(timezone.utc).isoformat(),"command":sys.argv,
            "inputs":initial,"source_stage":source_manifest["stage"],
            "code":fingerprint(Path(__file__)),
            "counts":{name:len(rows) for name,rows in displayed.items()},
            "contract":{"estimation":"none; saved compact evidence only","uncertainty":"descriptive, not estimated",
                        "plots":"fixed bins, isolated marks, suppressed points omitted, zero references",
                        "native_source":"standalone LaTeX with inline PGFPlots; no additional project files",
                        "compilation_status":"not_compiled_by_renderer"},
            "outputs":{path.name:artifact_fingerprint(path) for path in sorted(staging.iterdir())},
        }
        write_json(staging/"manifest.json",manifest)
    return manifest


def parse_args(argv: list[str] | None=None) -> argparse.Namespace:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analysis-dir",type=Path,required=True)
    parser.add_argument("--run-dir",type=Path,required=True)
    return parser.parse_args(argv)


if __name__=="__main__":
    args=parse_args()
    print(json.dumps(render_report(args.analysis_dir,args.run_dir),indent=2,sort_keys=True))
