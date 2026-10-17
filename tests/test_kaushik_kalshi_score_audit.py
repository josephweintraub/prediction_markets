"""Independent tiny saved-score fixtures, without estimator calls or raw data."""
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_kalshi_replication import score_audit as a


TARGETS=("payoff_cents","roi_percent")
CELLS=["D"+str(i) for i in range(1,11)]
KEYS=["kalshi:event:1","kalshi:event:10","kalshi:event:2","kalshi:event:20"]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path,value):
    Path(path).write_text(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+"\n")


def interval(value,variance):
    se=math.sqrt(variance)
    z=value/se if se else None
    return {"estimate":float(value),"standard_error":se,"ci95_low":value-1.959963984540054*se,
            "ci95_high":value+1.959963984540054*se,"normal_statistic":z,
            "normal_p_value":math.erfc(abs(z)/math.sqrt(2)) if z is not None else None}


def estimate_row(names,values,scores,weights,name,reasons):
    used=[i for i,v in enumerate(weights) if v]
    why=sorted({reason for i in used for reason in reasons[i]})
    row={"name":name,"suppressed":bool(why),"suppression_reasons":why,
         "CR0":None,"cluster_count_adjusted":None,"influence":None}
    if why:
        return row
    x=scores@np.array(weights)
    variance=float(x@x)
    value=float(np.array(values)@np.array(weights))
    scaled=(x/max(abs(x)))**2 if max(abs(x)) else np.zeros(len(x))
    total=float(scaled.sum())
    effective=total**2/float((scaled**2).sum()) if total else None
    maximum=float(scaled.max()/total) if total else None
    flags=[]
    if effective is not None and effective<30: flags.append("effective_clusters_below_30")
    if maximum is not None and maximum>.25: flags.append("cluster_variance_share_above_25_percent")
    if not total: flags.append("zero_cluster_score_variance")
    row.update(CR0=interval(value,variance),cluster_count_adjusted=interval(value,variance*len(x)/(len(x)-1)),
               supplementary_suppression_reasons=[],influence={"effective_clusters":effective,
               "maximum_cluster_variance_share":maximum,"flags":flags})
    return row


def joint(root,key,names,values,metadata,*,reasons=None,empty_scores=False):
    width=len(names)
    scores=np.outer(np.array([1.,-1.,2.,-2.]),np.arange(1,width+1)/100)
    reasons=reasons or [[] for _ in names]
    for i,why in enumerate(reasons):
        if why: scores[:,i]=0
    keys=KEYS
    if empty_scores:
        scores=np.zeros((0,width))
        keys=[]
    table=pa.table({"cluster_key":pa.array(keys,type=pa.string()),"projected_score":
                    pa.FixedSizeListArray.from_arrays(pa.array(scores.reshape(-1)),width)})
    path=root/(key+"_scores.parquet")
    pq.write_table(table,path)
    footer=pq.ParquetFile(path)
    info={"path":path.name,"bytes":path.stat().st_size,"sha256":digest(path),"rows":len(scores),
          "schema":str(footer.schema_arrow),"row_groups":footer.metadata.num_row_groups,
          "coefficient_order":names,"score_definition":"projected joint coefficient influence; CR0=score.T@score"}
    metadata={**metadata,"cluster_count":len(scores),"score_artifact":info}
    covariance=scores.T@scores
    def masked(multiplier):
        return [[None if reasons[i] or reasons[j] else float(covariance[i,j]*multiplier)
                 for j in range(width)] for i in range(width)]
    rows=[estimate_row(names,values,scores,np.eye(width)[i],name,reasons) for i,name in enumerate(names)]
    result={"names":names,"metadata":metadata,"variance_convention":deepcopy(a.VARIANCE),"estimates":rows,
            "covariance_CR0":masked(1),"covariance_cluster_count_adjusted":masked(4/3 if len(scores)>1 else 1),"contrasts":[]}
    return result,info,scores,reasons


def make_bundle(root,*,empty_bin=False,suppressed_model=False,empty_panel=False):
    inventory={}
    ns={cell:0 if empty_bin and cell=="D5" else 1000 for cell in CELLS}
    gs={cell:4 if n else 0 for cell,n in ns.items()}
    names=[target+"|"+cell for target in ("price","win_rate",*TARGETS) for cell in CELLS]
    price=[.05+.1*i for i in range(10)]
    wins=[.02+.1*i for i in range(10)]
    values=price+wins+[100*(w-p) for w,p in zip(wins,price)]+list(range(1,11))
    reasons=[[] if ns[cell] else ["cluster_support_below_floor","empty_cell","observation_support_below_floor"]
             for _ in range(4) for cell in CELLS]
    j,info,scores,reasons=joint(root,"table1",names,values,{"n":sum(ns.values()),"n_by_cell":ns,
        "cluster_count_by_cell":gs,"weight_sum_by_cell":ns,"cell_names":CELLS,
        "target_names":["price","win_rate",*TARGETS],"minimum_observations":1,"minimum_clusters":2},reasons=reasons)
    inventory["table1"]=info
    def support(cells):
        return {"n_observations":sum(ns[cell] for cell in cells),"n_clusters":min(gs[cell] for cell in cells),
                "contributing_cluster_minimum":min(gs[cell] for cell in cells),
                "cell_support":[{"cell":cell,"n_observations":ns[cell],"n_clusters":gs[cell]} for cell in cells],
                "paper_support":None,"project_support":None}
    def contrast(coefficients,name):
        weights=[coefficients.get(n,0) for n in names]
        return estimate_row(names,values,scores,weights,name,reasons)
    profiles=[]
    for i,cell in enumerate(CELLS):
        for target in TARGETS:
            profiles.append({"scope":"all_markets","phase":None,"bin":i+1,"outcome":target,
                **support([cell]),**contrast({target+"|"+cell:1},cell),
                "mean_price":price[i] if ns[cell] else None,"win_rate":wins[i] if ns[cell] else None})
    gaps=[{"outcome":target,**support(["D1","D10"]),
           **contrast({target+"|D10":1,target+"|D1":-1},"D10_minus_D1")} for target in TARGETS]

    def model(key,spec,n=2000,claim=False,suppressed=False):
        slopes_grid=[(target,clock) for target in TARGETS for clock in spec["report_clocks"]]
        if not n:
            return {"model_id":key,"spec":spec,"n_observations":0,"n_clusters":0,"suppressed":True,
                    "suppression_reasons":["empty_estimation_sample"],"joint":None,
                    "slopes":[{"outcome":target,"clock":clock,"suppressed":True,
                    "suppression_reasons":["empty_estimation_sample"],"CR0":None,"cluster_count_adjusted":None,
                    "influence":None} for target,clock in slopes_grid]}
        terms=(["H","H:xL","xR","H:xR"] if claim else
               [tail+":"+term for tail in ("D1","D10") for term in
                (["level",*spec["clocks"]] if not spec["effects"] else spec["clocks"])])
        names=[target+"|"+term for target in TARGETS for term in terms]
        values=[(i+1)/7 for i in range(len(names))]
        r2={target:.5 for target in TARGETS}
        group={"r_squared_including_fixed_effects_by_target":r2,
               "weighted_tss_by_target":{target:200. for target in TARGETS},
               "weighted_sse_by_target":{target:100. for target in TARGETS}}
        meta={"n":n,"weight_sum":n,"target_names":list(TARGETS),"term_names":terms,
              "structural_column_count":len(terms),"residualized_design_rank":0 if suppressed else len(terms),
              "absorption":{"converged":True} if spec["effects"] else None,
              "r_squared_including_fixed_effects_by_target":None if suppressed else r2,
              "grouped_r_squared":None if claim or suppressed else {"group_summaries":{"D1":deepcopy(group),"D10":deepcopy(group)},
              "stacked_weighted_tss_r_squared_by_target":r2}}
        reasons=[["rank_deficient_residualized_design"] for _ in names] if suppressed else None
        j,info,scores,reasons=joint(root,key,names,values,meta,reasons=reasons,empty_scores=suppressed)
        inventory[key]=info
        slopes=[]
        for target,clock in slopes_grid:
            coeffs={target+"|H:"+clock:1} if claim else {target+"|D10:"+clock:1,target+"|D1:"+clock:-1}
            slopes.append({"outcome":target,"clock":clock,**estimate_row(names,values,scores,
                [coeffs.get(name,0) for name in names],"claim_FE_interaction" if claim else "D10_minus_D1_clock_slope",reasons)})
        return {"model_id":key,"spec":spec,"n_observations":n,"n_clusters":4,"suppressed":suppressed,
                "suppression_reasons":["rank_deficient_residualized_design"] if suppressed else [],"joint":j,"slopes":slopes}

    specs=[(["xL"],[]),(["xL"],["cat_code"]),(["xR"],[]),(["xR"],["cat_code"]),
           (["xL","xR"],["cat_code","price_code","month_code"])]
    t2=[model("table2_c"+str(i),{"column":i,"clocks":clocks,"effects":effects,"report_clocks":clocks},
              suppressed=suppressed_model and i==2) for i,(clocks,effects) in enumerate(specs,1)]
    t3={}
    for panel,clock in (("L_gt1","xL"),("R_gt1","xR")):
        t3[panel]=[model("table3_"+panel+"_c"+str(i),{"column":i,"clocks":[clock] if i<3 else ["xL","xR"],
                    "effects":[] if i==1 else ["cat_code"] if i==2 else ["cat_code","price_code","month_code"],
                    "report_clocks":[clock]},n=0 if empty_panel and panel=="R_gt1" else 1600) for i in range(1,4)]
    claim_support={"claims":10,"observations":2000,"both_tail_claims":6,"both_tail_claim_observations":1500}
    a1=model("appendix_a1",{"column":1,"clocks":["xL","xR"],"report_clocks":["xL","xR"],"effects":["claim_fe_code"]},claim=True)
    a1["claim_support"]=claim_support
    return {"schema_version":"kaushik_kalshi_estimates_v1","status":"estimates_complete","sample_scope":"all_available_dates",
        "definitions":{"observation":"one admitted native trade print","weight":"one per native print",
        "duration_endpoint":"settlement_ts","combos":"excluded","price":"exact normalized binary64 P; no rounding",
        "duration":"open_time/settlement_ts; log2(1+days); common eligible tail sample","outcomes":{"payoff_cents":"100*(Y-P)",
        "roi_percent":"100*(Y/P-1), before averaging"},"uncertainty":deepcopy(a.VARIANCE)},
        "sample":{"baseline_rows":sum(ns.values()),"duration_tail_rows":2000,"cell_populations":{
            "baseline":{"observations":sum(ns.values()),"clusters":4},"duration":{"observations":2000,"clusters":4},
            "claim":{"observations":2000,"clusters":4}},"duration_panel_counts":{"L_gt1":1600,"R_gt1":0 if empty_panel else 1600},
            "claim_support":claim_support},"table1":{"profile_rows":profiles,"gap_rows":gaps,"joint":j},
        "table2":t2,"table3":t3,"appendix_a1":a1,"score_artifacts":inventory,
        "appendix_a2":{"status":"unavailable_from_public_trade_prints"},"sports":{"status":"pending_not_estimated"}}


def publish(root,summary):
    write_json(root/"estimates.json",summary)
    estimate_hash=digest(root/"estimates.json")
    manifest={"schema_version":"kaushik_kalshi_estimate_stage_v1","status":"estimates_complete",
              "source":{"head":"1"*40},"estimates_sha256":estimate_hash,"outputs":summary["score_artifacts"]}
    write_json(root/"manifest.json",manifest)
    manifest_hash=digest(root/"manifest.json")
    write_json(root/"acceptance.json",{"schema_version":"kaushik_kalshi_estimate_acceptance_v1",
        "status":"estimates_reopened_accepted","all_outputs_reopened":True,"source_head":"1"*40,
        "manifest_sha256":manifest_hash,"estimates_sha256":estimate_hash,"independent_saved_score_audit":"pending"})
    return {"estimates_path":root/"estimates.json","estimate_manifest_path":root/"manifest.json",
            "estimate_acceptance_path":root/"acceptance.json","estimates_sha256":estimate_hash,
            "estimate_manifest_sha256":manifest_hash,"acceptance_sha256":digest(root/"acceptance.json")}


class SavedScoreAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix="kalshi-score-audit-fixture-")
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.summary=make_bundle(self.root)

    def run_audit(self):
        return a.audit_saved_estimates(**publish(self.root,self.summary))

    def test_independent_full_joint_and_reported_contrasts(self):
        receipt=self.run_audit()
        self.assertEqual(receipt["status"],"saved_scores_reconciled")
        self.assertEqual(len(receipt["models"]),13)
        self.assertEqual(receipt["models"]["table1"],{"score_rows":4,"coefficient_count":40,"reported_contrasts":22})
        self.assertTrue(all(value is True for value in receipt["checks"].values()))
        covariance=self.summary["table1"]["joint"]["covariance_CR0"]
        self.assertAlmostEqual(covariance[20][29],10*.21*.30)
        self.assertAlmostEqual(self.summary["table1"]["gap_rows"][0]["CR0"]["standard_error"],math.sqrt(10)*.09)
        self.assertEqual(receipt["estimates_sha256"],digest(self.root/"estimates.json"))
        self.assertIn("coefficients from original design moments",receipt["not_reestimated"])
        self.assertEqual(sorted(KEYS),KEYS)

    def test_no_audit_outputs_or_estimator_imports(self):
        paths=publish(self.root,self.summary)
        before={path.name:digest(path) for path in self.root.iterdir()}
        a.audit_saved_estimates(**paths)
        self.assertEqual(before,{path.name:digest(path) for path in self.root.iterdir()})
        source=Path(a.__file__).read_text()
        self.assertNotIn("import estimate",source)
        self.assertNotIn("import estimators",source)
        self.assertNotIn("import run_estimates",source)

    def test_one_row_batches_reconstruct_same_joint_scores(self):
        paths=publish(self.root,self.summary)
        full=a.audit_saved_estimates(**paths)
        streamed=a.audit_saved_estimates(**paths,limits=a.AuditLimits(batch_rows=1))
        self.assertEqual(full["models"],streamed["models"])
        self.assertEqual(full["checks"],streamed["checks"])

    def test_covariance_cross_tail_and_adjustment_tampering_blocked(self):
        baseline=deepcopy(self.summary)
        for name in ("covariance_CR0","covariance_cluster_count_adjusted"):
            self.summary=deepcopy(baseline)
            self.summary["table1"]["joint"][name][20][29]=0
            with self.subTest(name=name),self.assertRaisesRegex(a.AuditBlocked,"covariance"):
                self.run_audit()

    def test_reported_interval_contrast_and_influence_tampering_blocked(self):
        baseline=deepcopy(self.summary)
        for field in ("standard_error","ci95_low","normal_p_value"):
            self.summary=deepcopy(baseline)
            self.summary["table2"][4]["slopes"][0]["CR0"][field]+=1
            with self.subTest(field=field),self.assertRaises(a.AuditBlocked): self.run_audit()
        self.summary=deepcopy(baseline)
        self.summary["table1"]["gap_rows"][0]["influence"]["flags"]=[]
        with self.assertRaisesRegex(a.AuditBlocked,"influence flags"): self.run_audit()
        self.summary=deepcopy(baseline)
        # Independent-tail SE is wrong because the saved event covariance is not zero.
        joint=self.summary["table1"]["joint"]
        self.summary["table1"]["gap_rows"][0]["CR0"]["standard_error"]=math.sqrt(joint["covariance_CR0"][20][20]+joint["covariance_CR0"][29][29])
        with self.assertRaises(a.AuditBlocked): self.run_audit()

    def test_count_support_estimand_and_r_squared_tampering_blocked(self):
        baseline=deepcopy(self.summary)
        edits=[lambda x:x["table1"]["gap_rows"][0].update(n_observations=3),
               lambda x:x["definitions"].update(price="rounded price"),
               lambda x:x["table2"][0]["spec"].update(clocks=["xR"]),
               lambda x:x["table3"]["R_gt1"][1].update(n_observations=1599),
               lambda x:x["appendix_a1"]["claim_support"].update(both_tail_claim_observations=1),
               lambda x:x["table2"][0]["joint"]["metadata"]["grouped_r_squared"]["group_summaries"]["D1"]["weighted_sse_by_target"].update(payoff_cents=101)]
        for i,edit in enumerate(edits):
            self.summary=deepcopy(baseline)
            edit(self.summary)
            with self.subTest(edit=i),self.assertRaises(a.AuditBlocked): self.run_audit()

    def test_valid_empty_bins_rank_suppression_and_empty_panel_preserved(self):
        for options in ({"empty_bin":True},{"suppressed_model":True},{"empty_panel":True}):
            self.summary=make_bundle(self.root,**options)
            with self.subTest(options=options):
                receipt=self.run_audit()
                self.assertEqual(receipt["status"],"saved_scores_reconciled")
        self.assertNotIn("table3_R_gt1_c1",receipt["models"])

    def rewrite_score(self,keys):
        key="table2_c1"
        info=self.summary["score_artifacts"][key]
        path=self.root/info["path"]
        old=pq.read_table(path)
        table=pa.table({"cluster_key":pa.array(keys),"projected_score":old["projected_score"]})
        pq.write_table(table,path)
        info.update(bytes=path.stat().st_size,sha256=digest(path),schema=str(pq.ParquetFile(path).schema_arrow))

    def test_duplicate_and_nonwriter_order_score_keys_blocked(self):
        for keys in ([KEYS[0],KEYS[0],KEYS[2],KEYS[3]],list(reversed(KEYS))):
            self.rewrite_score(keys)
            with self.subTest(keys=keys),self.assertRaisesRegex(a.AuditBlocked,"ordered/unique"): self.run_audit()

    def test_hash_manifest_acceptance_and_body_binding_blocked(self):
        paths=publish(self.root,self.summary)
        wrong={**paths,"estimate_manifest_sha256":"0"*64}
        with self.assertRaisesRegex(a.AuditBlocked,"SHA256"): a.audit_saved_estimates(**wrong)
        acceptance=json.loads((self.root/"acceptance.json").read_text())
        acceptance["estimates_sha256"]="0"*64
        write_json(self.root/"acceptance.json",acceptance)
        paths["acceptance_sha256"]=digest(self.root/"acceptance.json")
        with self.assertRaisesRegex(a.AuditBlocked,"acceptance"): a.audit_saved_estimates(**paths)
        paths=publish(self.root,self.summary)
        path=self.root/self.summary["score_artifacts"]["table1"]["path"]
        path.write_bytes(path.read_bytes()+b"corrupt")
        with self.assertRaisesRegex(a.AuditBlocked,"size/SHA256"): a.audit_saved_estimates(**paths)

    def test_path_escape_and_resource_limits_blocked(self):
        self.summary["score_artifacts"]["table1"]["path"]="../outside.parquet"
        with self.assertRaisesRegex(a.AuditBlocked,"relative"): self.run_audit()
        self.summary=make_bundle(self.root)
        paths=publish(self.root,self.summary)
        with self.assertRaisesRegex(a.AuditBlocked,"row ceiling"):
            a.audit_saved_estimates(**paths,limits=a.AuditLimits(score_rows=3))
        with self.assertRaisesRegex(a.AuditBlocked,"batch memory"):
            a.audit_saved_estimates(**paths,limits=a.AuditLimits(batch_bytes=1))

    def test_actual_native_writer_schema_is_readable_without_using_it_as_oracle(self):
        # This is an interface fixture, not the mathematical oracle above:
        # create small saved outputs through the writer and audit independently.
        import duckdb
        from analysis.kaushik_kalshi_replication import estimate
        from tests.test_kaushik_kalshi_estimate import fixture
        con=duckdb.connect()
        self.addCleanup(con.close)
        raw,baseline=fixture(con)
        output=self.root/"native-writer"
        output.mkdir()
        with patch.object(estimate.shutil,"disk_usage",return_value=type("Disk",(),{"free":10**15})()):
            summary=estimate.estimate_relations(con,baseline_relation="baseline_cells",
                duration_relation="duration_cells",claim_relation="claim_cells",output_dir=output,
                sample_metadata={"baseline_rows":len(baseline),"duration_tail_rows":len(raw)},
                resource_limits=estimate.ResourceLimits())
        receipt=a.audit_saved_estimates(**publish(output,summary))
        self.assertEqual(len(receipt["models"]),13)
        self.assertEqual(receipt["models"]["table1"]["score_rows"],18)


if __name__=="__main__":
    unittest.main()
