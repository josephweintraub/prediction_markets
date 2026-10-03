from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
import itertools
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.diagnostics import render_profit_taking_comparison as report
from analysis.sports_game_dynamics.artifacts import artifact_fingerprint


@pytest.fixture(scope="module")
def saved_rows() -> dict[str,list[dict]]:
    """Hand-defined compact evidence, never a production trade-data fixture."""
    profiles=[]
    for grain,sport,sample,window,weight,bin_id in itertools.product(
            report.GRAINS,report.SPORTS,report.SAMPLES,report.WINDOWS,report.WEIGHTS,range(1,11)):
        phase=1 if window=="live_99_100" else 0
        value=(bin_id-5.5)*(.025+.002*phase)
        exit_value=.3*value if bin_id>5 else 0.
        hedge_value=.2*value if bin_id<=5 else 0.
        n=700 if grain=="matched_execution" else 600
        # Preserve a real supported raw estimate while withholding one plotted
        # bin and its terminal contrast. Other sports remain fully supported.
        if sport=="atp" and window=="live_99_100" and bin_id==1:
            n=499
        suppressed=n<500
        row=dict(grain=grain,sport=sport,sample=sample,window=window,weighting=weight,
                 price_bin=bin_id,n_executions=n,weight_total=float(n),suppressed=suppressed,
                 uncertainty_status="not_estimated_descriptive")
        for field,estimate in zip(report.PARTS,(value,exit_value,hedge_value,value-exit_value-hedge_value)):
            row[field+"_raw"]=estimate
            row[field]=None if suppressed else estimate
        profiles.append(row)
    indexed={tuple(row[field] for field in ("grain","sport","sample","window","weighting","price_bin")):row
             for row in profiles}
    tails=[]
    tail_parts=("spread","exit_spread_contribution","hedge_spread_contribution","remaining_spread_contribution")
    for grain,sport,sample,window,weight in itertools.product(
            report.GRAINS,report.SPORTS,report.SAMPLES,report.WINDOWS,report.WEIGHTS):
        key=(grain,sport,sample,window,weight)
        low,high=indexed[key+(1,)],indexed[key+(10,)]
        suppressed=low["suppressed"] or high["suppressed"]
        row=dict(grain=grain,sport=sport,sample=sample,window=window,weighting=weight,
                 d1_n_executions=low["n_executions"],d10_n_executions=high["n_executions"],suppressed=suppressed)
        for source,target in zip(report.PARTS,tail_parts):
            value=high[source+"_raw"]-low[source+"_raw"]
            row[target+"_raw"]=value
            row[target]=None if suppressed else value
        tails.append(row)
    indexed_tails={tuple(row[field] for field in ("grain","sport","sample","window","weighting")):row
                   for row in tails}
    changes=[]
    for grain,sport,sample,weight in itertools.product(report.GRAINS,report.SPORTS,report.SAMPLES,report.WEIGHTS):
        prior=indexed_tails[(grain,sport,sample,"live_95_99",weight)]
        final=indexed_tails[(grain,sport,sample,"live_99_100",weight)]
        suppressed=prior["suppressed"] or final["suppressed"]
        row=dict(grain=grain,sport=sport,sample=sample,weighting=weight,
                 contrast="live_99_100_minus_live_95_99",suppressed=suppressed)
        for phase,source in (("previous",prior),("final",final)):
            for bin_name in ("d1","d10"):
                row[f"{phase}_{bin_name}_n_executions"]=source[f"{bin_name}_n_executions"]
        for field,source in zip(report.DELTAS,tail_parts):
            value=final[source+"_raw"]-prior[source+"_raw"]
            row[field+"_raw"]=value
            row[field]=None if suppressed else value
            row[field+"_pp"]=None if suppressed else 100*value
        changes.append(row)
    totals=[dict(grain=grain,sport=sport,gross_quantity_micro=10**17,
                 gross_cash_micro=5*10**16,n_own_buy_events=1000,quantity_residual_numerator=3e14)
            for grain,sport in itertools.product(report.GRAINS,report.SPORTS)]
    mechanisms=[]
    for sport,window,role in itertools.product(report.SPORTS,report.WINDOWS,("all","passive","active_aggregate")):
        factor=2 if role=="all" else 1
        values=dict(own_actions=10000,own_buys=6000,own_sells=4000,favorite_sell_events=3000,
            primary_exit_events=1200,profitable_hedge_events=800,sell_gross_quantity_micro=8*10**12,
            matched_sale_quantity_micro=55*10**11,prior_matched_sale_quantity_micro=5*10**12,
            gross_profitable_disposal_quantity_micro=4*10**12,primary_exit_quantity_micro=3*10**12,
            profitable_hedge_net_quantity_micro=10**12,unmatched_sale_quantity_micro=25*10**11,
            unmatched_favorite_sale_quantity_micro=2*10**12,merge_disposal_gross_quantity_micro=10**12,
            allocated_primary_merge_quantity_micro=75e10)
        mechanisms.append(dict(sport=sport,window=window,role=role,
            population="all_own_actions_no_price_or_actor_filter",
            history_status="trade_implied_only_opening_and_nontrade_movements_unknown",
            **{key:value*factor for key,value in values.items()}))
    return dict(profiles=profiles,tails=tails,late_delta=changes,grain_totals=totals,mechanism_summary=mechanisms)


def save_stage(path: Path,rows: dict[str,list[dict]]) -> Path:
    path.mkdir()
    outputs={}
    for name,values in rows.items():
        target=path/(name+".parquet")
        pq.write_table(pa.Table.from_pylist(values),target)
        outputs[target.name]=artifact_fingerprint(target)
    manifest=dict(stage=report.STAGE,schema_version=1,completion_status="complete",
                  contract=dict(grains=list(report.GRAINS),sports=list(report.SPORTS),
                                samples=list(report.SAMPLES),windows=list(report.WINDOWS),weights=list(report.WEIGHTS)),
                  counts={name:len(values) for name,values in rows.items()},outputs=outputs)
    (path/"manifest.json").write_text(json.dumps(manifest))
    return path


def test_saved_grids_additive_contrasts_units_and_support_reconcile(saved_rows) -> None:
    evidence=report.validate_saved_evidence(saved_rows)
    assert len(evidence["profiles"])==16200
    assert len(evidence["tails"])==1620
    assert len(evidence["late_delta"])==162
    assert len(evidence["mechanism_summary"])==270
    row=evidence["late_delta"][("matched_execution","mlb","all_trades","fill")]
    assert row["spread_delta_pp"]==pytest.approx(1.8)
    assert row["exit_spread_delta_pp"]==pytest.approx(.27)
    assert row["hedge_spread_delta_pp"]==pytest.approx(.18)
    assert row["remaining_spread_delta_pp"]==pytest.approx(1.35)
    withheld=evidence["late_delta"][("own_order_event","atp","all_trades","fill")]
    assert withheld["spread_delta_pp"] is None
    assert withheld["spread_delta_raw"]==pytest.approx(.018)


def test_mechanism_table_uses_unfiltered_own_actions_and_gross_vs_net(saved_rows) -> None:
    evidence=report.validate_saved_evidence(saved_rows)
    tex=report._mechanism_table(evidence["mechanism_summary"])
    assert "MLB & 2,400 & 4.500 & 1.500 & 1,600 & 2.000 & 4.000" in tex
    assert "before BUY price or wallet filters" in tex
    assert "merge has no actual BUY" in tex
    assert "net acquired quantity" in tex
    assert "without an observed acquisition match" in tex
    assert "millions of contracts" in tex
    assert report.millions(0)=="0.000"
    assert report.millions(1)==r"$<0.001$"
    assert report.millions(1250000000000)=="1.250"


@pytest.mark.parametrize("failure",["missing","role_sum","count","population","history","sale_quantity","merge_quantity"])
def test_mechanism_source_definition_and_reconciliation_are_required(saved_rows,failure: str) -> None:
    rows=deepcopy(saved_rows)
    row=rows["mechanism_summary"][0]
    if failure=="missing": rows["mechanism_summary"].pop()
    elif failure=="role_sum": row["profitable_hedge_events"]+=1
    elif failure=="count": row["primary_exit_events"]=row["own_sells"]+1
    elif failure=="population": row["population"]="filtered_buys"
    elif failure=="history": row["history_status"]="observed_inventory"
    elif failure=="sale_quantity": row["primary_exit_quantity_micro"]=row["sell_gross_quantity_micro"]+1
    else: row["allocated_primary_merge_quantity_micro"]=1e20
    with pytest.raises(ValueError): report.validate_saved_evidence(rows)


def test_quantity_residual_tolerance_matches_producer_not_cancelled_sum(saved_rows) -> None:
    rows=deepcopy(saved_rows)
    # Producer checks absolute residual disagreement against gross quantity,
    # not against a potentially zero aggregate residual after cancellation.
    rows["grain_totals"][0]["quantity_residual_numerator"]=.001
    other=next(row for row in rows["grain_totals"] if row["sport"]==rows["grain_totals"][0]["sport"]
               and row["grain"]!=rows["grain_totals"][0]["grain"])
    other["quantity_residual_numerator"]=.002
    report.validate_saved_evidence(rows)
    other["quantity_residual_numerator"]=2e7
    with pytest.raises(ValueError,match="quantity-weighted residual"):
        report.validate_saved_evidence(rows)


@pytest.mark.parametrize("failure",[
    "duplicate","missing_grid","support","suppressed_zero","raw_components","tail",
    "tail_support","contrast","delta_units","delta_support","uncertainty","negative_weight",
    "one_microtoken","missing_total_grain",
])
def test_displayed_evidence_fail_closed(saved_rows,failure: str) -> None:
    rows=deepcopy(saved_rows)
    if failure=="duplicate": rows["profiles"].append(dict(rows["profiles"][0]))
    elif failure=="missing_grid": rows["tails"].pop()
    elif failure=="support": rows["profiles"][0]["n_executions"]=499
    elif failure=="suppressed_zero":
        suppressed=next(row for row in rows["profiles"] if row["suppressed"])
        suppressed["calibration"]=0.
    elif failure=="raw_components": rows["profiles"][0]["remaining_contribution_raw"]+=.1
    elif failure=="tail": rows["tails"][0]["spread_raw"]+=.1
    elif failure=="tail_support": rows["tails"][0]["d1_n_executions"]+=1
    elif failure=="contrast": rows["late_delta"][0]["contrast"]="reversed_difference"
    elif failure=="delta_units": rows["late_delta"][0]["spread_delta_pp"]/=100
    elif failure=="delta_support": rows["late_delta"][0]["final_d1_n_executions"]+=1
    elif failure=="uncertainty": rows["profiles"][0]["uncertainty_status"]="nominal"
    elif failure=="negative_weight": rows["profiles"][0]["weight_total"]=-1.
    elif failure=="one_microtoken": rows["grain_totals"][0]["gross_quantity_micro"]+=1
    else: rows["grain_totals"].pop()
    with pytest.raises(ValueError):
        report.validate_saved_evidence(rows)


@pytest.mark.parametrize("value,expected",[(None,"withheld"),(.123,"+0.12"),(-.001,"+0.00"),(-.5,"-0.50")])
def test_effect_display(value,expected) -> None:
    assert report.number(value)==expected


@pytest.mark.parametrize("value",[float("nan"),float("inf"),True,"0.1"])
def test_bad_effect_display(value) -> None:
    with pytest.raises(ValueError): report.number(value)


@pytest.mark.parametrize("value",[-1,True,500.,None])
def test_support_display_does_not_coerce_bad_counts(value) -> None:
    with pytest.raises(ValueError): report.count(value)


def test_saved_decimal_quantities_keep_exact_micro_units_in_display_json() -> None:
    quantity=Decimal("100000000000000001")
    assert report._saved_value(quantity)==10**17+1
    assert isinstance(report._saved_value(quantity),int)
    assert json.dumps({"quantity":report._saved_value(quantity)})=='{"quantity": 100000000000000001}'
    for value in (Decimal(".5"),Decimal("NaN"),Decimal("Infinity")):
        with pytest.raises(ValueError): report._saved_value(value)


def test_native_standalone_layout_isolated_marks_and_withheld_points(saved_rows) -> None:
    evidence=report.validate_saved_evidence(saved_rows)
    tex=report.create_latex(evidence)
    assert tex==report.create_latex(evidence)
    assert tex.count(r"\nextgroupplot")==18
    assert tex.count(r"\begin{table}")==4
    assert tex.count(r"\begin{figure}")==3
    assert tex.count("only marks")==36
    assert "withheld" in tex and "+1.80" in tex
    # The withheld ATP final D1 is absent, while its prior D1 remains plotted.
    atp=tex.split("title={ATP: Trades}")[1].split(r"\nextgroupplot")[0]
    assert "(1.10," not in atp and "(0.90," in atp
    assert r"\includegraphics" not in tex and r"\input{" not in tex
    assert "http" not in tex and "wallet_id" not in tex and "sha256" not in tex
    assert r"\section{Conclusion}" not in tex and "Interpretation limits" not in tex
    assert "not causal" in tex and "not independently certified" in tex
    assert "build_profit_taking_contribution.py" in tex
    assert tex.rstrip().endswith(r"\end{document}")


@pytest.mark.parametrize("failure",["partial","wrong_stage","contract","hash","count"])
def test_completed_saved_parent_and_fingerprints_are_required(tmp_path,saved_rows,failure: str) -> None:
    source=save_stage(tmp_path/"analysis",saved_rows)
    path=source/"manifest.json"
    manifest=json.loads(path.read_text())
    if failure=="partial": manifest["completion_status"]="partial"
    elif failure=="wrong_stage": manifest["stage"]="other_stage"
    elif failure=="contract": manifest["contract"]["weights"]=["fill"]
    elif failure=="hash": manifest["outputs"]["profiles.parquet"]["sha256"]="wrong"
    else: manifest["counts"]["profiles"]+=1
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError): report.render_report(source,tmp_path/"report")
    assert not (tmp_path/"report").exists()


def test_immutable_report_reconciles_every_displayed_row(tmp_path,saved_rows) -> None:
    source=save_stage(tmp_path/"analysis",saved_rows)
    target=tmp_path/"report"
    manifest=report.render_report(source,target)
    assert manifest["completion_status"]=="complete"
    assert manifest["counts"]==dict(overview=36,profiles=360,robustness=72,mechanisms=9)
    assert manifest["contract"]["compilation_status"]=="not_compiled_by_renderer"
    assert set(path.name for path in target.iterdir())=={
        "manifest.json","profit_taking_comparison.tex","displayed_evidence.json"}
    shown=json.loads((target/"displayed_evidence.json").read_text())
    source_maps=report.validate_saved_evidence(saved_rows)
    for section in ("overview","robustness"):
        assert all(row==source_maps["late_delta"][(row["grain"],row["sport"],row["sample"],row["weighting"])]
                   for row in shown[section])
    for row in shown["profiles"]:
        assert row==source_maps["profiles"][(row["grain"],row["sport"],row["sample"],row["window"],row["weighting"],row["price_bin"])]
    with pytest.raises(FileExistsError): report.render_report(source,target)


def test_source_mutation_during_render_is_not_published(tmp_path,saved_rows,monkeypatch) -> None:
    source=save_stage(tmp_path/"analysis",saved_rows)
    original=report.create_latex
    def mutate(evidence):
        output=original(evidence)
        path=source/"manifest.json"
        manifest=json.loads(path.read_text());manifest["changed"]=True
        path.write_text(json.dumps(manifest))
        return output
    monkeypatch.setattr(report,"create_latex",mutate)
    target=tmp_path/"report"
    with pytest.raises(ValueError,match="changed while rendering"):
        report.render_report(source,target)
    assert not target.exists()
    assert not list(tmp_path.glob(".report.staging-*"))
