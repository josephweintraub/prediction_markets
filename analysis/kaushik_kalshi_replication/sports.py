"""Fresh native sports means from accepted candidate caches and game proofs.

No provider lookup, proposition inference, raw-tape scan or legacy estimate
import occurs here. The relation-level API is for tiny local fixtures; a
production caller must first enforce the production host and admit_saved_inputs.
Only bounded game/cell aggregates and joint scores are written, not a second
all-observation sports cache. Output acceptance and independent QA are separate.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import production_guard

from analysis.kaushik_kalshi_replication import build, estimate
from analysis.kaushik_polymarket_replication import build_inputs as io
from analysis.kaushik_polymarket_replication import estimators as engine
from analysis.kaushik_polymarket_replication import run_estimates as grouped


SPORTS = grouped.SPORTS
SCOPES = ("pooled", *SPORTS)
WINDOWS = grouped.WINDOWS
PHASES = ("pregame", "in_play")
TARGETS = ("payoff_cents", "roi_percent")
CANDIDATE_FIELDS = {"ticker":"string","claim_id":"string","event_cluster":"string","sport":"string",
    "timestamp":"double","P":"double","Y":"integer","payoff":"double","roi":"double","bin":"integer"}
MAP_FIELDS = {"ticker":"string","sport":"string","game_key":"string","actual_start_seconds":"double",
    "actual_end_seconds":"double","timing_quality":"string","provenance":"string","yes_payout":"integer",
    "native_result":"string","native_yes_result_verified":"bool","native_event_ticker":"string",
    "provider_game_id":"string","identity_receipt_sha256":"string","result_receipt_sha256":"string"}
MAP_CHECKS = ("identities_verified","provider_clock_conventions_verified","native_yes_results_verified","source_hashes_reopened")
RUN_ROOT = Path('/mnt/data/runs')
SOURCE_FILES = ('analysis/kaushik_kalshi_replication/sports.py','analysis/kaushik_kalshi_replication/inputs.py',
    'analysis/kaushik_kalshi_replication/build.py','analysis/kaushik_kalshi_replication/estimate.py',
    'analysis/kaushik_polymarket_replication/estimators.py','analysis/kaushik_polymarket_replication/run_estimates.py',
    'analysis/kaushik_polymarket_replication/build_inputs.py','analysis/kaushik_polymarket_replication/exchange_filter_contract.py',
    'production_guard.py','tests/test_kaushik_kalshi_sports.py')


class SportsBlocked(ValueError):
    pass


def require(value, message):
    if not value:
        raise SportsBlocked(message)


@dataclass(frozen=True)
class SportsLimits:
    output_bytes: int = 4_000_000_000
    artifact_bytes: int = 512_000_000
    transient_bytes: int = 1_000_000_000
    spill_bytes: int = 4_000_000_000
    read_bytes: int = 128_000_000_000
    json_bytes: int = 16_000_000
    minimum_free_bytes: int = 20_000_000_000
    aggregate_rows: int = 5_000_000
    numpy_bytes: int = 8_000_000_000
    input_bytes: int = 12_000_000_000

    def validate(self):
        require(all(type(value) is int and value>0 for value in asdict(self).values()),"positive integer sports bounds required")
        require(self.minimum_free_bytes>=20_000_000_000 and self.json_bytes<=self.artifact_bytes<=self.transient_bytes<=self.output_bytes,
                "sports resource bounds disagree")
        require(self.numpy_bytes<=32_000_000_000,"sports NumPy ceiling exceeds numerical core")
        return self


def _relation(name):
    require(isinstance(name,str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",name),"simple sports relation name required")
    return io.ident(name)


def _schema(schema, expected):
    require(set(schema.names)==set(expected),"sports artifact fields differ")
    for name,kind in expected.items():
        actual=schema.field(name).type
        valid=(pa.types.is_string(actual) or pa.types.is_large_string(actual)) if kind=='string' else (
            pa.types.is_integer(actual) if kind=='integer' else pa.types.is_boolean(actual) if kind=='bool'
            else pa.types.is_float64(actual))
        require(valid,"sports artifact type differs: "+name)


def _child(root, value):
    require(isinstance(value,str) and value and '\\' not in value and not Path(value).is_absolute()
            and all(part not in ('','.','..') for part in value.split('/')),"normalized relative sports artifact path required")
    path=root/value
    require(root in path.resolve().parents and path.resolve()==path and path.is_file() and not path.is_symlink(),"sports input escapes accepted root")
    return path


def _record(root, record, *, parquet=False, schema=None, limits=SportsLimits()):
    require(isinstance(record,dict),"explicit sports artifact record required")
    path=_child(root,record.get('path'))
    ceiling=limits.input_bytes if parquet else limits.json_bytes
    require(path.stat().st_size<=ceiling and path.stat().st_size==record.get('bytes') and io.sha256(path)==record.get('sha256'),
            "sports input body size/hash differs")
    if parquet:
        footer=pq.ParquetFile(path)
        require(footer.metadata.num_rows==record.get('rows') and str(footer.schema_arrow)==record.get('schema'),"sports input footer differs")
        _schema(footer.schema_arrow,schema)
        return path
    return io.read_json(path,record['sha256'])[0]


def admit_saved_inputs(con, *, input_manifest_path, input_manifest_sha256,
                       input_acceptance_path, input_acceptance_sha256,
                       map_manifest_path, map_manifest_sha256,
                       map_acceptance_path, map_acceptance_sha256,
                       limits=SportsLimits()):
    """Reopen explicit immutable receipts/bodies; caller enforces production host.

    This does not promote identity candidates. The map producer must publish the
    four explicit independent checks in MAP_CHECKS and bind its per-row receipts.
    Local invocation is limited to synthetic fixture bodies.
    """
    limits.validate()
    require(all(isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) for value in
        (input_manifest_sha256,input_acceptance_sha256,map_manifest_sha256,map_acceptance_sha256)),"explicit sports input/map SHA256 values required")
    def saved(path,digest):
        path=Path(path)
        require(path.is_file() and not path.is_symlink() and path.stat().st_size<=limits.json_bytes,"bounded sports JSON required")
        return io.read_json(path,digest)[0]
    manifest=saved(input_manifest_path,input_manifest_sha256)
    accepted=saved(input_acceptance_path,input_acceptance_sha256)
    mapping=saved(map_manifest_path,map_manifest_sha256)
    map_accepted=saved(map_acceptance_path,map_acceptance_sha256)
    require(manifest.get('schema_version')=='kaushik_kalshi_inputs_v1' and manifest.get('status')=='accepted'
            and accepted.get('schema_version')=='kaushik_kalshi_inputs_acceptance_v1' and accepted.get('status')=='accepted'
            and accepted.get('manifest_sha256')==input_manifest_sha256 and accepted.get('checks')
            and all(accepted['checks'].get(key) is True for key in estimate.REQUIRED_ACCEPTANCE_CHECKS)
            and all(value is True for value in accepted['checks'].values()),"accepted native input manifest/receipt required")
    require(manifest.get('scientific_contract',{}).get('sample_scope')=='all_available_dates' and
            manifest['scientific_contract'].get('combos')=='excluded',"full-date no-combo native input contract required")
    require(mapping.get('schema_version')=='kaushik_kalshi_sports_map_v1' and mapping.get('status')=='sports_map_complete_pending_independent_audit'
            and map_accepted.get('schema_version')=='kaushik_kalshi_sports_map_acceptance_v1'
            and map_accepted.get('status')=='sports_map_reopened_accepted'
            and map_accepted.get('manifest_sha256')==map_manifest_sha256
            and all(map_accepted.get('checks',{}).get(name) is True for name in MAP_CHECKS),
            "independently accepted native YES/result/actual-clock map required; candidates are not admission")
    root=Path(input_manifest_path).resolve().parent
    map_root=Path(map_manifest_path).resolve().parent
    require(Path(input_acceptance_path).resolve().parent==root and Path(map_acceptance_path).resolve().parent==map_root,
            "sports manifests/receipts must share their respective stage roots")
    index=_record(root,manifest.get('daily_receipts'),limits=limits)
    require(index.get('schema_version')=='kaushik_kalshi_receipt_inventory_v1' and isinstance(index.get('receipts'),list),"native daily receipt inventory required")
    require(len(index['receipts'])==manifest.get('counts',{}).get('source_days')
            and len({row.get('path') for row in index['receipts']})==len(index['receipts']),"daily sports receipt inventory incomplete/duplicated")
    paths,days,identities,rows=[],set(),[],0
    for record in index['receipts']:
        day=_record(root,record,limits=limits)
        require(day.get('schema_version')=='kaushik_kalshi_day_cells_v1' and day.get('status')=='durable_pending_global_admission'
                and day.get('source_day') not in days and isinstance(day.get('source_day'),str),"duplicate/missing/unadmitted sports source day")
        days.add(day['source_day'])
        artifact=day.get('artifacts',{}).get('sports_candidates')
        path=_record(root/'cells',artifact,parquet=True,schema=CANDIDATE_FIELDS,limits=limits)
        paths.append(str(path))
        identities.append({'path':str(path),'bytes':artifact['bytes'],'sha256':artifact['sha256'],'rows':artifact['rows']})
        rows+=artifact['rows']
        require(day.get('sports_candidate_rows')==artifact['rows'],"daily candidate count differs")
    require(len(set(paths))==len(paths) and sum(item['bytes'] for item in identities)<=limits.input_bytes
            and rows==manifest.get('sports',{}).get('candidate_rows'),"candidate cache inventory/count/byte ceiling differs")
    map_record=mapping.get('artifacts',{}).get('market_map')
    map_path=_record(map_root,map_record,parquet=True,schema=MAP_FIELDS,limits=limits)
    require(paths,"explicit candidate daily inventory required")
    require(isinstance(mapping.get('timing_qualification'),str) and mapping['timing_qualification'].strip(),
            "explicit saved provider-clock qualification required")
    con.execute('CREATE TEMP VIEW kk_sports_candidates AS SELECT * FROM read_parquet(['+','.join(io.literal(path) for path in paths)+'],hive_partitioning=false)')
    con.execute('CREATE TEMP VIEW kk_sports_market_map AS SELECT * FROM read_parquet('+io.literal(str(map_path))+')')
    return {'input_manifest_sha256':input_manifest_sha256,'input_acceptance_sha256':input_acceptance_sha256,
            'map_manifest_sha256':map_manifest_sha256,'map_acceptance_sha256':map_acceptance_sha256,
            'candidate_rows':rows,'candidate_files':identities,'market_map':{**map_record,'path':str(map_path)},
            'timing_qualification':mapping.get('timing_qualification')}


def _prepare(con,candidates_relation,map_relation,ledger,scan_bytes,limits):
    candidates,mapping=_relation(candidates_relation),_relation(map_relation)
    allowed=','.join(io.literal(sport) for sport in SPORTS)
    _schema(con.execute('SELECT * FROM '+candidates+' LIMIT 0').to_arrow_table().schema,CANDIDATE_FIELDS)
    _schema(con.execute('SELECT * FROM '+mapping+' LIMIT 0').to_arrow_table().schema,MAP_FIELDS)
    # Conservative complete cache-read plan, including validation, both bounded
    # aggregations, exclusion/quality summaries and join-count subqueries.
    ledger.charge(scan_bytes*12,'sports_complete_candidate_cache_read_plan')
    price_bin='CASE '+ ' '.join(f'WHEN P<{upper} THEN {i}' for i,upper in enumerate((.1,.2,.3,.4,.5,.6,.7,.8,.9),1))+' ELSE 10 END'
    bad=[f'NOT {build._valid_identifier(name)}' for name in ('ticker','claim_id','event_cluster')]
    bad += [f"sport IS NULL OR sport NOT IN ({allowed})","timestamp IS NULL OR NOT isfinite(timestamp) OR timestamp<0",
        "P IS NULL OR NOT isfinite(P) OR P<=0 OR P>=1","Y IS NULL OR Y NOT IN (0,1)",
        "payoff IS NULL OR NOT isfinite(payoff)","roi IS NULL OR NOT isfinite(roi)",
        "abs(payoff-100*(Y-P))>1e-12*greatest(1,abs(payoff))",
        "abs(roi-100*(Y/P-1))>1e-12*greatest(1,abs(roi))",f"bin IS NULL OR bin<>({price_bin})",
        "claim_id NOT IN ('kalshi:claim:'||ticker||':yes','kalshi:claim:'||ticker||':no')"]
    require(con.execute(f"SELECT count(*) FROM {candidates} WHERE "+' OR '.join('('+item+')' for item in bad)).fetchone()[0]==0,
            "accepted sports candidate cache contains contradictory native claims/prices/outcomes/bins")
    bad=[f'NOT {build._valid_identifier(name)}' for name in ('ticker','game_key','native_event_ticker','provider_game_id')]
    bad += [f"sport IS NULL OR sport NOT IN ({allowed})","actual_start_seconds IS NULL OR NOT isfinite(actual_start_seconds) OR actual_start_seconds<0",
        "actual_end_seconds IS NULL OR NOT isfinite(actual_end_seconds) OR actual_end_seconds<=actual_start_seconds",
        "timing_quality IS NULL OR trim(timing_quality)=''","provenance IS NULL OR trim(provenance)=''",
        "yes_payout IS NULL OR yes_payout NOT IN (0,1)","native_result IS NULL OR native_result NOT IN ('yes','no')",
        "yes_payout<>CASE WHEN native_result='yes' THEN 1 ELSE 0 END",
        "native_yes_result_verified IS DISTINCT FROM true"]
    bad += [f"{name} IS NULL OR NOT regexp_full_match({name},'[0-9a-f]{{64}}')" for name in ('identity_receipt_sha256','result_receipt_sha256')]
    require(con.execute(f"SELECT count(*) FROM {mapping} WHERE "+' OR '.join('('+item+')' for item in bad)).fetchone()[0]==0,"native sports map proof/clock contract invalid")
    require(con.execute(f"SELECT count(*) FROM (SELECT ticker FROM {mapping} GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]==0,"native sports map ticker is not unique")
    require(con.execute(f"SELECT count(*) FROM (SELECT sport,game_key FROM {mapping} GROUP BY 1,2 HAVING count(DISTINCT actual_start_seconds)<>1 OR count(DISTINCT actual_end_seconds)<>1 OR count(DISTINCT provider_game_id)<>1)").fetchone()[0]==0,
            "native sports game clock/provider identity conflict")
    con.execute(f"""CREATE TEMP VIEW kk_sports_joined AS SELECT c.*,m.game_key,m.actual_start_seconds,m.actual_end_seconds,
        m.timing_quality,m.yes_payout,m.native_event_ticker,m.sport AS map_sport,
        CASE WHEN m.ticker IS NULL THEN 'missing_accepted_map' WHEN c.timestamp>m.actual_end_seconds THEN 'after_actual_end' ELSE 'eligible' END sports_reason
        FROM {candidates} c LEFT JOIN {mapping} m USING(ticker)""")
    counts=con.execute(f"SELECT (SELECT count(*) FROM {candidates}),count(*) FROM kk_sports_joined").fetchone()
    require(counts[0]==counts[1],"sports join fanout")
    require(con.execute("""SELECT count(*) FROM kk_sports_joined WHERE game_key IS NOT NULL AND
        (sport<>map_sport OR event_cluster<>'kalshi:event:'||native_event_ticker OR
        Y<>CASE WHEN claim_id='kalshi:claim:'||ticker||':yes' THEN yes_payout ELSE 1-yes_payout END)""").fetchone()[0]==0,
        "matched sports native event/sport/claim payout contradicts accepted proof")
    con.execute("""CREATE TEMP VIEW kk_sports_base AS SELECT *,
        'kalshi:sport:'||sport||':game:'||game_key AS game_cluster,
        timestamp-actual_start_seconds sport_elapsed_seconds,actual_end_seconds-timestamp sport_remaining_seconds,
        CASE WHEN timestamp<actual_start_seconds THEN 'pregame' ELSE 'in_play' END phase
        FROM kk_sports_joined WHERE sports_reason='eligible'""")
    # One lazy candidate scan, not fourteen Parquet/join branches. Final-hour
    # membership intentionally overlaps the separate since-start partition.
    window_values=','.join('('+io.literal(panel)+','+io.literal(key)+')' for panel,key,_,_ in WINDOWS)
    predicate=' OR '.join('(w.clock_window='+io.literal(key)+' AND ('+condition+'))' for _,key,_,condition in WINDOWS)
    con.execute('CREATE TEMP VIEW kk_sports_windows AS SELECT b.*,w.clock_panel,w.clock_window FROM kk_sports_base b '
                'CROSS JOIN (VALUES '+window_values+') w(clock_panel,clock_window) WHERE '+predicate)
    con.execute("""CREATE TEMP TABLE kk_sports_phase_totals AS SELECT sport,game_cluster,phase,bin,count(*)::BIGINT n_rows,
        sum(payoff)::DOUBLE sum_payoff,sum(roi)::DOUBLE sum_roi FROM kk_sports_base GROUP BY 1,2,3,4""")
    con.execute("""CREATE TEMP TABLE kk_sports_window_totals AS SELECT sport,game_cluster,clock_panel,clock_window,bin,count(*)::BIGINT n_rows,
        sum(payoff)::DOUBLE sum_payoff,sum(roi)::DOUBLE sum_roi FROM kk_sports_windows GROUP BY 1,2,3,4,5""")
    for name in ('kk_sports_phase_totals','kk_sports_window_totals'):
        require(con.execute('SELECT count(*) FROM '+name).fetchone()[0]<=limits.aggregate_rows,"sports aggregate row ceiling exceeded")
    return counts[0]


def _means(con,relation,cells,cell_expression,scope,where,limits):
    predicate='TRUE' if scope=='pooled' else 'sport='+io.literal(scope)
    query=f"SELECT game_cluster,{cell_expression} cell,sum(n_rows)::BIGINT n,sum(sum_payoff)::DOUBLE payoff,sum(sum_roi)::DOUBLE roi FROM {relation} WHERE ({predicate}) AND ({where}) GROUP BY 1,2 ORDER BY 1,2"
    def records():
        for batch in grouped.array_batches(con,query):
            for i in range(len(batch['n'])):
                yield {'cluster':batch['game_cluster'][i],'cell':batch['cell'][i],'count':int(batch['n'][i]),
                       'weight_sum':float(batch['n'][i]),'weighted_sums':[float(batch['payoff'][i]),float(batch['roi'][i])]}
    return engine.joint_clustered_means_from_totals(records(),cells=cells,target_names=TARGETS,
        min_observations=500,min_clusters=30,memory_limit_bytes=limits.numpy_bytes)


def _save_aggregate(con,relation,path,writer):
    writer.reserve(writer.limits.artifact_bytes)
    with io.copy_ceiling(writer.limits.transient_bytes):
        con.execute('COPY (SELECT * FROM '+relation+') TO '+io.literal(str(path))+" (FORMAT PARQUET,COMPRESSION ZSTD)")
    require(path.stat().st_size<=writer.limits.artifact_bytes,"sports aggregate artifact ceiling exceeded")
    writer.ledger.charge(path.stat().st_size*2,'sports_saved_aggregate_hash_and_reopen')
    info=io._output_info(path)
    require(info['rows']==con.execute('SELECT count(*) FROM '+relation).fetchone()[0],"sports saved aggregate row loss")
    writer.reserve()
    return {'path':path.name,**info}


def estimate_relations(con,*,candidates_relation,market_map_relation,output_dir,input_binding,
                       limits=SportsLimits(),ledger=None,scan_bytes=0):
    """Fixture-accessible SQL aggregation; caller owns all source admission.

    Production must supply admitted immutable file bindings from admit_saved_inputs.
    No acceptance is manufactured by this numerical library call.
    """
    limits.validate()
    output_dir=Path(output_dir)
    require(output_dir.is_dir() and not any(output_dir.iterdir()),"new empty sports output directory required")
    require(isinstance(input_binding,dict) and input_binding,"explicit sports input binding required")
    ledger=ledger or grouped.ReadLedger(ceiling=limits.read_bytes)
    writer=estimate._Writer(output_dir,limits,ledger)
    writer.reserve()
    source_n=_prepare(con,candidates_relation,market_map_relation,ledger,scan_bytes,limits)
    require('candidate_rows' not in input_binding or input_binding['candidate_rows']==source_n,
            "sports candidate count differs from accepted immutable binding")
    aggregate_artifacts={family:_save_aggregate(con,'kk_sports_'+family+'_totals',output_dir/(family+'_totals.parquet'),writer)
                         for family in ('phase','window')}
    phase_rows,profile_rows,window_rows,joints=[],[],[],{}
    phase_cells=tuple(tail+':'+phase for phase in PHASES for tail in ('D1','D10'))
    profile_cells=tuple('D'+str(band)+':'+phase for phase in PHASES for band in range(1,11))
    window_cells=tuple(tail+':'+key for _,key,_,_ in WINDOWS for tail in ('D1','D10'))
    phase_counts,scope_counts,window_counts=[],[],[]
    for scope in SCOPES:
        predicate='TRUE' if scope=='pooled' else 'sport='+io.literal(scope)
        table='kk_sports_phase_totals'
        n,g=con.execute(f"SELECT coalesce(sum(n_rows),0),count(DISTINCT game_cluster) FROM {table} WHERE {predicate}").fetchone()
        scope_counts.append({'scope':scope,'n_observations':int(n),'n_games':g})
        for phase in PHASES:
            n,g=con.execute(f"SELECT coalesce(sum(n_rows),0),count(DISTINCT game_cluster) FROM {table} WHERE {predicate} AND phase={io.literal(phase)}").fetchone()
            phase_counts.append({'scope':scope,'phase':phase,'n_observations':int(n),'n_games':g})
        for panel,key,_,_ in WINDOWS:
            n,g=con.execute(f"SELECT coalesce(sum(n_rows),0),count(DISTINCT game_cluster) FROM kk_sports_window_totals WHERE {predicate} AND clock_window={io.literal(key)}").fetchone()
            window_counts.append({'scope':scope,'window':key,'panel':panel,'n_observations':int(n),'n_games':g})
        for family,relation,cells,expression,where in (
            ('phases',table,phase_cells,"'D'||bin||':'||phase",'bin IN (1,10)'),
            ('profiles',table,profile_cells,"'D'||bin||':'||phase",'TRUE'),
            ('windows','kk_sports_window_totals',window_cells,"'D'||bin||':'||clock_window",'bin IN (1,10)')):
            result=_means(con,relation,cells,expression,scope,where,limits)
            require(result.coefficient_cluster_influence.nbytes<=limits.numpy_bytes,"sports joint score allocation ceiling exceeded")
            key='sport_'+scope+'_'+family
            if family=='profiles':
                profile_rows.extend(grouped.profile_rows(result,scope=scope,phases=PHASES,sports=True))
            elif family=='phases':
                phase_rows.extend(grouped.gap_rows(result,scope=scope,phases=PHASES,sports=True))
            else:
                rows=grouped.gap_rows(result,scope=scope,windows=[item[1] for item in WINDOWS],sports=True)
                labels={key:(panel,label,order) for order,(panel,key,label,_) in enumerate(WINDOWS)}
                for row in rows:
                    row['panel'],row['label'],row['window_order']=labels[row['window']]
                window_rows.extend(rows)
            joints[key]=writer.joint(result,key)
    require((len(phase_rows),len(profile_rows),len(window_rows))==(60,400,280),"fresh sports grids incomplete")
    for scope in SCOPES:
        phases={row['phase']:row['n_observations'] for row in phase_counts if row['scope']==scope}
        windows=[row for row in window_counts if row['scope']==scope]
        require(sum(row['n_observations'] for row in windows if row['panel']=='pregame')==phases['pregame']
                and sum(row['n_observations'] for row in windows if row['panel']=='since_start')==phases['in_play'],"sports literal window/phase partition differs")
    exclusions=grouped.rows(con,'SELECT sport,sports_reason reason,count(*) AS "rows" FROM kk_sports_joined GROUP BY 1,2 ORDER BY 1,2')
    eligible=scope_counts[0]['n_observations']
    require(sum(row['rows'] for row in exclusions)==source_n and sum(row['rows'] for row in exclusions if row['reason']=='eligible')==eligible,
            "sports disjoint candidate reconciliation failed")
    output={'schema_version':'kaushik_kalshi_sports_estimates_v1','status':'sports_estimates_complete',
        'sample_scope':'all_available_dates','input_binding':input_binding,'definitions':{
        'observation':'one admitted native trade print','weight':'one per native print','cluster':'sport-namespaced provider game_key',
        'phase':'pregame timestamp<actual_start_seconds; in_play timestamp>=actual_start_seconds; timestamp<=actual_end_seconds',
        'endpoint_grace_seconds':0,'outcomes':{'payoff_cents':'100*(Y-P)','roi_percent':'100*(Y/P-1), before averaging'},
        'minimum_prints_per_cell':500,'minimum_games_per_cell':30,'uncertainty':engine.VARIANCE_NOTE,
        'windows':[{'panel':panel,'key':key,'label':label,'predicate':predicate} for panel,key,label,predicate in WINDOWS]},
        'candidate_rows':source_n,'eligible_rows':eligible,'scope_counts':scope_counts,'phase_counts':phase_counts,
        'window_counts':window_counts,'exclusions':exclusions,'phase_rows':phase_rows,'profile_rows':profile_rows,
        'window_rows':window_rows,'joint_artifacts':joints,'score_artifacts':writer.artifacts,
        'aggregate_artifacts':aggregate_artifacts,
        'timing_coverage':grouped.rows(con,'SELECT sport,timing_quality,count(*) AS "rows" FROM kk_sports_base GROUP BY 1,2 ORDER BY 1,2'),
        'window_reconciliation':{'all_scopes_pregame_partition':True,'all_scopes_since_start_partition':True,
            'final_hour_overlaps_since_start':True,'exact_start_and_endpoint_retained':True},
        'resources':asdict(limits),'read_ledger':json.loads(json.dumps(ledger.to_dict())),
        'independent_saved_score_audit':'pending'}
    writer.json('sports_estimates.json',output)
    return output


def _source_snapshot(expected_head):
    require(isinstance(expected_head,str) and re.fullmatch('[0-9a-f]{40}',expected_head),"explicit committed sports source HEAD required")
    require(subprocess.check_output(['git','rev-parse','HEAD'],cwd=io.REPO,text=True).strip()==expected_head,"sports source HEAD differs")
    files={}
    for relative in SOURCE_FILES:
        tracked=subprocess.run(['git','ls-files','--error-unmatch',relative],cwd=io.REPO,capture_output=True)
        clean=subprocess.run(['git','diff','--quiet','HEAD','--',relative],cwd=io.REPO)
        require(tracked.returncode==clean.returncode==0,"sports source is not committed/clean: "+relative)
        files[relative]=io.sha256(io.REPO/relative)
    return {'head':expected_head,'files':files}


def _available_ram():
    match=re.search(r'^MemAvailable:\s+(\d+) kB$',Path('/proc/meminfo').read_text(),re.MULTILINE)
    require(match is not None,"available sports RAM could not be inspected")
    return int(match.group(1))*1024


def estimate_saved(*,input_manifest_path,input_manifest_sha256,input_acceptance_path,input_acceptance_sha256,
                   map_manifest_path,map_manifest_sha256,map_acceptance_path,map_acceptance_sha256,
                   core_manifest_path,core_manifest_sha256,core_acceptance_path,core_acceptance_sha256,
                   output_dir,expected_source_head,limits=SportsLimits()):
    """Guarded immutable production wrapper; every admitted hash is explicit.

    Separate sports saved-score QA remains required before report release. This
    wrapper must be scheduled serially with other production cache/ID stages.
    """
    production_guard.require_production_host()
    require(Path(sys.executable)==Path('/home/ubuntu/venv/bin/python'),"canonical sports Python required")
    require(all(isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) for value in
            (core_manifest_sha256,core_acceptance_sha256)),"explicit accepted core SHA256 values required")
    limits.validate()
    target=Path(output_dir).resolve()
    require(target.parent==RUN_ROOT and not target.exists(),"new immutable top-level sports output directory required")
    reservation=limits.output_bytes+limits.transient_bytes+limits.spill_bytes+limits.minimum_free_bytes
    require(shutil.disk_usage(target.parent).free>=reservation,"sports output/spill/free-floor reservation failed")
    require((os.cpu_count() or 0)>=2 and _available_ram()>=32_000_000_000,"two sports threads and 32GB available RAM required")
    source=_source_snapshot(expected_source_head)
    for path in (input_manifest_path,input_acceptance_path,map_manifest_path,map_acceptance_path,core_manifest_path,core_acceptance_path):
        resolved=Path(path).resolve()
        require(target!=resolved and target not in resolved.parents and resolved not in target.parents,"sports output overlaps an input")
    core,core_id=io.read_json(core_manifest_path,core_manifest_sha256)
    accepted,core_acceptance_id=io.read_json(core_acceptance_path,core_acceptance_sha256)
    core_hash=accepted.get('estimates_sha256')
    require(core.get('schema_version')=='kaushik_kalshi_estimate_stage_v1' and core.get('status')=='estimates_complete'
        and accepted.get('schema_version')=='kaushik_kalshi_estimate_acceptance_v1' and
        accepted.get('status')=='estimates_reopened_accepted' and accepted.get('all_outputs_reopened') is True and
        accepted.get('manifest_sha256')==core_manifest_sha256 and core.get('estimates_sha256')==core_hash and
        core.get('input_binding',{}).get('manifest',{}).get('sha256')==input_manifest_sha256,
        "accepted core estimate/native input binding differs")
    require(Path(core_acceptance_path).resolve().parent==Path(core_manifest_path).resolve().parent,"core estimate receipt root differs")
    io.read_json(Path(core_manifest_path).parent/'estimates.json',core_hash)
    stage=Path(tempfile.mkdtemp(prefix='.'+target.name+'.staging-',dir=target.parent))
    con,published=None,False
    try:
        con=duckdb.connect()
        for setting in ("SET TimeZone='UTC'","SET memory_limit='16GB'","SET threads=2",
                        "SET preserve_insertion_order=true","SET enable_object_cache=false",
                        "SET disabled_optimizers='join_order,build_side_probe_side'"):
            con.execute(setting)
        con.execute('SET temp_directory='+io.literal(stage/'spill'))
        con.execute('SET max_temp_directory_size='+io.literal(str(limits.spill_bytes)+'B'))
        arguments=dict(input_manifest_path=input_manifest_path,input_manifest_sha256=input_manifest_sha256,
            input_acceptance_path=input_acceptance_path,input_acceptance_sha256=input_acceptance_sha256,
            map_manifest_path=map_manifest_path,map_manifest_sha256=map_manifest_sha256,
            map_acceptance_path=map_acceptance_path,map_acceptance_sha256=map_acceptance_sha256)
        binding=admit_saved_inputs(con,**arguments,limits=limits)
        binding.update(core_estimates_sha256=core_hash,core_manifest_sha256=core_manifest_sha256,
                       core_acceptance_sha256=core_acceptance_sha256)
        sizes=sum(item['bytes'] for item in binding['candidate_files'])
        require(sizes*12+limits.output_bytes*4<=limits.read_bytes,"sports worst-case read plan exceeds ceiling")
        ledger=grouped.ReadLedger(ceiling=limits.read_bytes)
        result=estimate_relations(con,candidates_relation='kk_sports_candidates',market_map_relation='kk_sports_market_map',
            output_dir=stage,input_binding=binding,limits=limits,ledger=ledger,scan_bytes=sizes)
        for item in [*binding['candidate_files'],binding['market_map']]:
            path=Path(item['path'])
            require(path.stat().st_size==item['bytes'] and io.sha256(path)==item['sha256'],"sports admitted input body drift")
        for path,digest in ((input_manifest_path,input_manifest_sha256),(input_acceptance_path,input_acceptance_sha256),
            (map_manifest_path,map_manifest_sha256),(map_acceptance_path,map_acceptance_sha256),
            (core_manifest_path,core_manifest_sha256),(core_acceptance_path,core_acceptance_sha256)):
            io.read_json(path,digest)
        require(_source_snapshot(expected_source_head)==source,"sports source drift before publication")
        digest=io.sha256(stage/'sports_estimates.json')
        artifacts={**result['score_artifacts'],**{'aggregate_'+key:value for key,value in result['aggregate_artifacts'].items()}}
        manifest={'schema_version':'kaushik_kalshi_sports_estimate_stage_v1','status':'sports_estimates_complete',
            'source':source,'sports_sha256':digest,'core_estimates_sha256':core_hash,'input_binding':binding,
            'core_binding':{'manifest':core_id,'acceptance':core_acceptance_id},'outputs':artifacts,
            'resources':asdict(limits),'runtime':{'memory_limit':'16GB','threads':2,'numpy_bytes':limits.numpy_bytes,
                'minimum_available_ram_bytes':32_000_000_000},'independent_saved_score_audit':'pending'}
        writer=estimate._Writer(stage,limits,ledger)
        writer.json('manifest.json',manifest)
        con.close()
        con=None
        io.atomic_publish(stage,target)
        published=True
        require(io.read_json(target/'manifest.json')[0]==manifest and io.sha256(target/'sports_estimates.json')==digest,
                "published sports JSON failed reopen")
        for item in artifacts.values():
            require(io._output_info(target/item['path'])=={key:item[key] for key in ('bytes','sha256','rows','schema','row_groups')},
                    "published sports aggregate/score failed reopen")
        acceptance={'schema_version':'kaushik_kalshi_sports_estimate_acceptance_v1','status':'sports_estimates_reopened_accepted',
            'sports_sha256':digest,'manifest_sha256':io.sha256(target/'manifest.json'),'core_estimates_sha256':core_hash,
            'all_outputs_reopened':True,'independent_saved_score_audit':'pending'}
        estimate._Writer(target,limits,ledger).json('acceptance.json',acceptance)
        return manifest
    except BaseException as error:
        failed=target if published else stage
        if failed.exists() and not (failed/'failure.json').exists():
            io.write_json(failed/'failure.json',{'status':'sports_estimates_incomplete','error_type':type(error).__name__,'reason':str(error)})
        raise
    finally:
        if con is not None: con.close()
