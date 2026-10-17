"""Reader-facing cohort metadata from accepted, immutable saved Kalshi cells.

No raw tape, estimator, network, output file, or publication operation is used.
The caller owns its bounded DuckDB connection and saves the returned values in
its own immutable report stage. Parquet-backed objects are temporary views;
only the two explicitly owned compact caches are temporary tables.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re

import pyarrow.parquet as pq

import production_guard

SCHEMA_VERSION='kaushik_kalshi_reader_metadata_v1'
JSON_LIMIT=16_000_000
CHECKS=('schema_validated','body_hashes_validated','source_commit_validated','global_trade_ids_unique',
        'source_rows_reconciled','no_join_fanout','cell_moments_validated','duration_membership_validated',
        'claim_invariants_validated','codes_validated')
EXECUTION_FIELDS=('baseline_execution_seconds_min','baseline_execution_seconds_max',
                  'duration_tail_execution_seconds_min','duration_tail_execution_seconds_max')
REASONS=('eligible','missing_or_invalid_open_time','missing_or_invalid_settlement_ts',
         'nonpositive_lifespan','outside_recorded_open_settlement_interval')


class SummaryBlocked(ValueError):
    pass


def _require(condition,message):
    if not condition:raise SummaryBlocked(message)


def _count(value):
    _require(type(value) is int and value>=0,'exact nonnegative count required')
    return value


def _literal(value):return "'"+str(value).replace("'","''")+"'"


def _name(value):
    _require(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',value or ''),'simple temporary SQL name required')
    return '"'+value+'"'


def _relative(value):
    _require(isinstance(value,str) and value and not Path(value).is_absolute() and ':' not in value and '\\' not in value
             and all(part not in ('','.','..') for part in value.split('/')),'normalized relative artifact path required')
    return value


def _canonical(path):
    path=Path(path)
    _require(path.is_absolute() and path.resolve(strict=True)==path and path.is_file() and not path.is_symlink(),'canonical regular saved body required')
    return path


def _identity(path):
    value=path.lstat()
    return value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns


def _sha(path):
    value=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1_048_576),b''):value.update(block)
    return value.hexdigest()


def _pairs(pairs):
    output={}
    for key,value in pairs:
        _require(key not in output,'duplicate saved JSON key');output[key]=value
    return output


class _Bindings:
    def __init__(self):self.values={};self.identities={}

    def remember(self,path,size,hashed,rows=None,schema=None,identity_=None):
        value={'path':str(path),'bytes':size,'sha256':hashed,'rows':rows,'schema':schema}
        _require(str(path) not in self.values or self.values[str(path)]==value,'conflicting saved body bindings')
        self.values[str(path)]=value
        self.identities[str(path)]=identity_

    def json(self,path,sha,bytes_=None):
        path=_canonical(path);before=_identity(path)
        _require(re.fullmatch(r'[0-9a-f]{64}',sha or '') and 0<path.stat().st_size<=JSON_LIMIT,'bounded fingerprinted JSON required')
        raw=path.read_bytes()
        _require(hashlib.sha256(raw).hexdigest()==sha and before==_identity(path) and (bytes_ is None or bytes_==len(raw)),'saved JSON size/hash/drift differs')
        value=json.loads(raw,object_pairs_hook=_pairs,parse_constant=lambda token:(_ for _ in ()).throw(SummaryBlocked('nonfinite JSON')))
        _require(type(value) is dict,'saved JSON object required')
        self.remember(path,len(raw),sha,identity_=before)
        return value

    def record_json(self,root,item):
        return self.json(Path(root)/_relative(item['path']),item['sha256'],item['bytes'])

    def parquet(self,root,item):
        path=_canonical(Path(root)/_relative(item['path']));before=_identity(path)
        _require(type(item.get('bytes')) is int and item['bytes']>0 and re.fullmatch(r'[0-9a-f]{64}',item.get('sha256','')),'pinned Parquet body required')
        _require(path.stat().st_size==item['bytes'] and _sha(path)==item['sha256'],'saved Parquet size/hash differs')
        footer=pq.ParquetFile(path)
        _require(footer.metadata.num_rows==item.get('rows') and str(footer.schema_arrow)==item.get('schema') and before==_identity(path),'saved Parquet footer/schema/drift differs')
        self.remember(path,item['bytes'],item['sha256'],footer.metadata.num_rows,str(footer.schema_arrow),before)
        return path


def _one(con,sql):return con.execute(sql).fetchone()[0]


def _same(con,left,right,message):
    mismatch=_one(con,'SELECT count(*) FROM ((('+left+') EXCEPT ALL ('+right+')) UNION ALL (('+right+') EXCEPT ALL ('+left+')))')
    _require(mismatch==0,message)


def _utc(seconds):
    if seconds is None:return None
    _require(type(seconds) in (int,float) and math.isfinite(seconds) and seconds>=0,'finite admitted UTC execution bound required')
    try:return datetime.fromtimestamp(seconds,timezone.utc).isoformat(timespec='microseconds').replace('+00:00','Z')
    except (ValueError,OverflowError,OSError) as error:raise SummaryBlocked('invalid admitted UTC execution bound') from error


def build_reader_metadata(con,*,manifest_path,manifest_sha256,acceptance_path,acceptance_sha256,expected_input_source_head):
    """Return ``(reader_metadata, provenance)`` without writing saved files.

    This is a production saved-body reader. Fixture tests mock only the host
    guard and supply tiny temporary artifacts. Native metadata paths can come
    only from the accepted trade manifest's pinned metadata-stage binding.
    """
    production_guard.require_production_host()
    binder=_Bindings()
    manifest_path,acceptance_path=_canonical(manifest_path),_canonical(acceptance_path)
    root=manifest_path.parent
    _require(manifest_path.name=='manifest.json' and acceptance_path==root/'acceptance.json'
             and not (root/'failure.json').exists(),'canonical nonfailed accepted input pair required')
    manifest=binder.json(manifest_path,manifest_sha256)
    acceptance=binder.json(acceptance_path,acceptance_sha256)
    _require(manifest.get('schema_version')=='kaushik_kalshi_inputs_v1' and manifest.get('status')=='accepted'
             and acceptance.get('schema_version')=='kaushik_kalshi_inputs_acceptance_v1' and acceptance.get('status')=='accepted'
             and acceptance.get('manifest_sha256')==manifest_sha256 and all(acceptance.get('checks',{}).get(key) is True for key in CHECKS),
             'bound independently accepted compact inputs required')
    _require(re.fullmatch(r'[0-9a-f]{40}',expected_input_source_head or '') and manifest.get('source',{}).get('head')==expected_input_source_head,'explicit accepted builder source head differs')
    contract=manifest.get('scientific_contract',{})
    _require(all(contract.get(key)==value for key,value in {'sample_scope':'all_available_dates','combos':'excluded','duration_endpoint':'settlement_ts','equal_print_weight':True,'cutoff_utc':None,'cutoff_seconds':None}.items()),'accepted scientific cohort differs')
    sample=manifest.get('sample_metadata',{})
    _require(all(key in sample for key in EXECUTION_FIELDS),'admitted baseline/duration execution bounds missing; raw archive extrema cannot substitute')
    inventory=binder.record_json(root,manifest['input_inventory'])
    source_days=set()
    for item in inventory.get('entries',[]):
        matched=re.fullmatch(r'trades_all/dt=([0-9]{4}-[0-9]{2}-[0-9]{2})/part-000\.parquet',item.get('path',''))
        _require(matched and matched.group(1) not in source_days,'explicit unique daily source inventory required')
        source_days.add(matched.group(1))
    _require(source_days and len(source_days)==manifest.get('counts',{}).get('source_days'),'accepted source-day inventory/count differs')
    meta_binding=manifest.get('metadata_binding',{}).get('manifest',{})
    meta_path=_canonical(meta_binding.get('path',''))
    meta=binder.json(meta_path,meta_binding.get('sha256'),meta_binding.get('bytes'))
    _require(meta.get('schema_version')=='kaushik_kalshi_metadata_stage_v1' and meta.get('status')=='metadata_complete_pending_independent_audit'
             and not (meta_path.parent/'failure.json').exists(),'bound native metadata stage required')
    proof_record=meta.get('global_metadata_proof',{})
    proof=binder.record_json(meta_path.parent,proof_record)
    _require(meta_binding and manifest.get('metadata_binding',{}).get('proof',{}).get('sha256')==proof_record.get('sha256')
             and proof.get('status')=='structural_global_metadata_keys_passed' and proof.get('source')==meta.get('source'),
             'bound native metadata global proof differs')
    names={name:'reader_'+name for name in ('baseline','duration','claim','categories','markets','claims','metadata_files','metadata','daily_claims','native_day_claims')}
    for name in names.values():
        _require(_one(con,'SELECT count(*) FROM information_schema.tables WHERE table_name='+_literal(name))==0,'reader temporary name already exists')
    created=[]

    def create(name,sql,kind='VIEW'):
        con.execute('CREATE TEMP '+kind+' '+_name(names[name])+' AS '+sql);created.append((names[name],kind))

    def parquet_view(name,paths):
        _require(paths,'explicit nonempty saved-body inventory required')
        create(name,'SELECT * FROM read_parquet(['+','.join(_literal(path) for path in paths)+'],hive_partitioning=false)')

    try:
        for role in ('baseline','duration','claim','categories','markets','claims'):
            item=manifest.get('artifacts',{}).get(role,{})
            _require(item.get('path')=='cells/final/'+role+'.parquet','accepted final role/path differs')
            parquet_view(role,[binder.parquet(root,item)])
        meta_paths=[]
        for key,item in meta.get('artifacts',{}).items():
            if not key.startswith('metadata_'):continue
            _require(item.get('path')=='artifacts/metadata/'+key[len('metadata_'):]+'.parquet','native metadata role/path differs')
            meta_paths.append(binder.parquet(meta_path.parent,item))
        _require(len(meta_paths)==meta.get('counts',{}).get('source_shards'),'bound native metadata file inventory differs')
        parquet_view('metadata_files',meta_paths)
        create('metadata','SELECT ticker,source_version,market_reason,opening_seconds,endpoint_seconds,event_cluster,category,result FROM reader_metadata_files','TABLE')
        _require(_one(con,'SELECT count(*)-count(DISTINCT ticker) FROM reader_metadata')==0,'native metadata ticker join fanout')
        index=binder.record_json(root,manifest['daily_receipts'])
        _require(index.get('schema_version')=='kaushik_kalshi_receipt_inventory_v1','daily receipt inventory contract differs')
        day_paths=[];seen=set();source_rows=0;exclusions={};saved_tail_reasons={};bounds={key:[] for key in EXECUTION_FIELDS}
        for item in index.get('receipts',[]):
            receipt=binder.record_json(root,item);day=receipt.get('source_day')
            _require(day in source_days and day not in seen and item['path']=='cells/day='+day+'/receipt.json'
                     and receipt.get('schema_version')=='kaushik_kalshi_day_cells_v1' and receipt.get('status')=='durable_pending_global_admission','complete unique daily receipt binding required')
            source_rows+=_count(receipt['source_rows'])
            _require(all(key in receipt for key in EXECUTION_FIELDS) and isinstance(receipt.get('duration_tail_exclusions'),list),'cohort-specific daily diagnostics required')
            for key in EXECUTION_FIELDS:
                if receipt[key] is not None:bounds[key].append(receipt[key])
            for row in receipt.get('exclusions',[]):exclusions[row['eligibility_reason']]=exclusions.get(row['eligibility_reason'],0)+_count(row['rows'])
            for row in receipt['duration_tail_exclusions']:
                reason=row['duration_reason']
                if reason in ('trade_before_open_time','trade_after_settlement_ts'):reason='outside_recorded_open_settlement_interval'
                _require(reason in REASONS,'unadmitted tail-only duration reason')
                saved_tail_reasons[reason]=saved_tail_reasons.get(reason,0)+_count(row['rows'])
            claim_record=receipt.get('artifacts',{}).get('claims',{})
            _require(claim_record.get('path')=='day='+day+'/claims.parquet','daily claim role/day path differs')
            day_paths.append(binder.parquet(root/'cells',claim_record));seen.add(day)
        _require(seen==source_days,'complete saved daily claim inventory missing')
        for key,values in bounds.items():
            value=(min(values) if key.endswith('_min') else max(values)) if values else None
            _require(sample[key]==value,'saved cohort execution bounds do not reconcile')
        create('daily_claims',"SELECT * EXCLUDE(filename),regexp_extract(filename,'/day=([0-9]{4}-[0-9]{2}-[0-9]{2})/',1) source_day FROM read_parquet(["+','.join(_literal(path) for path in day_paths)+'],filename=true,hive_partitioning=false)')
        create('native_day_claims','SELECT c.*,m.ticker matched_ticker,m.source_version,m.market_reason,m.opening_seconds,m.endpoint_seconds FROM reader_daily_claims c LEFT JOIN reader_metadata m USING(ticker)','TABLE')
        _require(_one(con,'SELECT count(*) FROM reader_native_day_claims')==_one(con,'SELECT count(*) FROM reader_daily_claims'),'daily/native metadata join fanout')
        _require(_one(con,"SELECT count(*) FROM reader_native_day_claims WHERE matched_ticker IS NULL OR market_reason<>'eligible' OR source_version IS NULL OR n_rows IS NULL OR n_rows<=0 OR n_d1<0 OR n_d10<0 OR n_d1+n_d10>n_rows OR duration_rows<0 OR duration_rows>n_d1+n_d10")==0,'saved daily native claim support invalid')
        _same(con,'SELECT claim_id,ticker,taker_side,event_cluster,category,Y,sum(n_rows)::BIGINT n_rows,sum(n_d1)::BIGINT n_d1,sum(n_d10)::BIGINT n_d10,sum(duration_rows)::BIGINT duration_rows FROM reader_daily_claims GROUP BY ALL','SELECT * FROM reader_claims','final/daily claim support differs')
        _require(_one(con,"SELECT count(*) FROM reader_markets f LEFT JOIN reader_metadata m USING(ticker) WHERE m.ticker IS NULL OR m.market_reason<>'eligible' OR f.event_cluster IS DISTINCT FROM m.event_cluster OR f.category IS DISTINCT FROM m.category OR f.result IS DISTINCT FROM m.result OR f.opening_seconds IS DISTINCT FROM m.opening_seconds OR f.endpoint_seconds IS DISTINCT FROM m.endpoint_seconds")==0,'retained market/native metadata binding differs')
        result=_summarize(con,manifest,meta,source_rows,exclusions,saved_tail_reasons)
        _require(all(_identity(Path(path))==identity for path,identity in binder.identities.items()),'saved input drift during reader summary')
        provenance={'schema_version':'kaushik_kalshi_reader_metadata_provenance_v1',
            'accepted_manifest_sha256':manifest_sha256,'accepted_acceptance_sha256':acceptance_sha256,'input_source_head':expected_input_source_head,
            'native_metadata_manifest':dict(meta_binding),'native_metadata_source_head':meta.get('source',{}).get('head'),
            'body_bindings':[binder.values[path] for path in sorted(binder.values)],
            'execution_bounds':'first/last_execution_utc are admitted baseline extrema; archive_observed_* are all-raw source extrema only',
            'coverage_dates':'UTC source partition dates with duration_rows>0, not actual execution extrema; null when no eligible duration rows',
            'outside_interval':'valid clocks; saved tail_rows-duration_rows combines before-open and after-settlement without guessing which',
            'raw_tape_read':False,'saved_file_writes':False,
            'temporary_relations':'owned DuckDB TEMP views/tables only; caller resource limits and spill location apply'}
        return result,provenance
    finally:
        for name,kind in reversed(created):
            system='duckdb_tables()' if kind=='TABLE' else 'duckdb_views()'
            field='table_name' if kind=='TABLE' else 'view_name'
            matches=con.execute('SELECT temporary FROM '+system+' WHERE '+field+'=?',[name]).fetchall()
            _require(matches==[(True,)],'reader cleanup refused a non-owned temporary relation')
            con.execute('DROP '+kind+' '+_name(name))


def _summarize(con,manifest,meta,source_rows,exclusions,saved_tail_reasons):
    baseline,tail,clusters,native,fallback=con.execute("""SELECT coalesce(sum(n_rows),0),coalesce(sum(n_rows) FILTER(WHERE bin IN(1,10)),0),
       count(DISTINCT event_cluster),count(DISTINCT event_cluster) FILTER(WHERE starts_with(event_cluster,'kalshi:event:')),
       count(DISTINCT event_cluster) FILTER(WHERE starts_with(event_cluster,'kalshi:market:')) FROM reader_baseline""").fetchone()
    duration,duration_clusters,l_gt1,r_gt1=con.execute('SELECT coalesce(sum(n_rows),0),count(DISTINCT event_cluster),coalesce(sum(n_rows) FILTER(WHERE lifespan_gt1),0),coalesce(sum(n_rows) FILTER(WHERE remaining_gt1),0) FROM reader_duration').fetchone()
    _require(clusters==native+fallback,'native/fallback cluster namespace reconciliation failed')
    markets=_one(con,'SELECT count(*) FROM reader_markets');claims=_one(con,'SELECT count(*) FROM reader_claims')
    _require(_one(con,'SELECT count(DISTINCT ticker) FROM reader_markets')==markets and _one(con,'SELECT count(DISTINCT claim_id) FROM reader_claims')==claims,'saved market/claim identities not unique')
    _require((baseline,tail,duration)==con.execute('SELECT coalesce(sum(n_rows),0),coalesce(sum(n_d1+n_d10),0),coalesce(sum(duration_rows),0) FROM reader_claims').fetchone(),'baseline/tail/duration claim supports do not reconcile')
    _require(source_rows==manifest['counts']['source_rows'] and baseline==manifest['counts']['baseline_rows'] and duration==manifest['counts']['duration_tail_rows'],'accepted population counts differ')
    exclusions.setdefault('eligible',0)
    _require(sum(exclusions.values())==source_rows and exclusions['eligible']==baseline,'source first-failure exclusion waterfall differs')
    categories=[]
    for category,n in con.execute('SELECT category,n_rows FROM reader_categories ORDER BY category').fetchall():
        _require(isinstance(category,str) and category and type(n) is int and n>0,'native category support invalid')
        categories.append({'category':category,'n_observations':n,'share':n/baseline if baseline else None})
    _same(con,'SELECT category,sum(n_rows)::BIGINT n_rows FROM reader_claims GROUP BY 1','SELECT * FROM reader_categories','native category population does not reconcile')
    invalid='opening_seconds IS NULL OR endpoint_seconds IS NULL OR endpoint_seconds<=opening_seconds'
    _require(_one(con,'SELECT count(*) FROM reader_native_day_claims WHERE ('+invalid+') AND duration_rows<>0')==0,'invalid metadata clocks have admitted duration rows')
    reason_rows={reason:0 for reason in REASONS};reason_rows['eligible']=duration
    for reason,n in con.execute("""SELECT CASE WHEN opening_seconds IS NULL THEN 'missing_or_invalid_open_time'
       WHEN endpoint_seconds IS NULL THEN 'missing_or_invalid_settlement_ts' WHEN endpoint_seconds<=opening_seconds THEN 'nonpositive_lifespan'
       ELSE 'outside_recorded_open_settlement_interval' END reason,
       sum(CASE WHEN opening_seconds IS NULL OR endpoint_seconds IS NULL OR endpoint_seconds<=opening_seconds THEN n_d1+n_d10 ELSE n_d1+n_d10-duration_rows END)::BIGINT n
       FROM reader_native_day_claims GROUP BY 1""").fetchall():reason_rows[reason]+=n
    _require(sum(reason_rows.values())==tail and all(reason_rows[reason]==saved_tail_reasons.get(reason,0) for reason in REASONS),'tail-only metadata clock/exclusion reconciliation differs')
    coverage_map={source:(n,d,first,last) for source,n,d,first,last in con.execute('SELECT source_version,sum(n_d1+n_d10)::BIGINT,sum(duration_rows)::BIGINT,min(source_day) FILTER(WHERE duration_rows>0),max(source_day) FILTER(WHERE duration_rows>0) FROM reader_native_day_claims GROUP BY 1').fetchall()}
    sources=set(meta.get('source_version_policy',{}));_require(sources and set(coverage_map)<=sources,'unadmitted native metadata source in baseline support')
    coverage=[]
    for source in sorted(sources):
        n,d,first,last=coverage_map.get(source,(0,0,None,None))
        _require((first is None and last is None) if d==0 else isinstance(first,str) and isinstance(last,str) and first<=last,'duration source coverage partition bounds differ')
        coverage.append({'source_version':source,'tail_rows':n,'duration_tail_rows':d,'first_utc_day':first,'last_utc_day':last})
    _require(sum(row['tail_rows'] for row in coverage)==tail and sum(row['duration_tail_rows'] for row in coverage)==duration,'native source coverage population differs')
    support=con.execute('SELECT count(*),coalesce(sum(n),0),count(*) FILTER(WHERE h>0 AND h<n),coalesce(sum(n) FILTER(WHERE h>0 AND h<n),0) FROM (SELECT claim_fe_code,sum(n_rows) n,sum(a0) h FROM reader_claim GROUP BY 1)').fetchone()
    _require(support[1]==duration,'A1 common-duration support differs')
    sample=manifest['sample_metadata']
    _require(baseline>0 and sample['baseline_execution_seconds_min'] is not None and sample['baseline_execution_seconds_max'] is not None,'admitted baseline execution bounds unavailable')
    return {'schema_version':SCHEMA_VERSION,'first_execution_utc':_utc(sample['baseline_execution_seconds_min']),
        'last_execution_utc':_utc(sample['baseline_execution_seconds_max']),
        'duration_first_execution_utc':_utc(sample['duration_tail_execution_seconds_min']),
        'duration_last_execution_utc':_utc(sample['duration_tail_execution_seconds_max']),
        'archive_observed_first_execution_utc':_utc(sample.get('execution_seconds_min')),
        'archive_observed_last_execution_utc':_utc(sample.get('execution_seconds_max')),
        'source_rows':source_rows,'baseline_rows':baseline,'markets':markets,'normalized_claims':claims,
        'clusters':clusters,'unique_event_clusters':native,'market_fallback_clusters':fallback,'tail_rows':tail,
        'duration_tail_rows':duration,'duration_tail_clusters':duration_clusters,'categories':categories,
        'exclusions':[{'reason':reason,'rows':exclusions[reason]} for reason in sorted(exclusions)],
        'duration_exclusions':[{'reason':reason,'rows':reason_rows[reason]} for reason in REASONS],
        'duration_coverage':coverage,'duration_panel_counts':{'L_gt1':l_gt1,'R_gt1':r_gt1},
        'claim_support':dict(zip(('claims','observations','both_tail_claims','both_tail_claim_observations'),map(int,support)))}
