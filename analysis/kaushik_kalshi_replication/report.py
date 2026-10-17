"""Pure LaTeX/string and figure-description rendering of accepted saved outputs.

No files, plots, compilation, trade reads or estimation occur here. Root owns
source/hash admission, the authoring marker, figure creation and page QA. The
fresh sports adapter is deliberately not guessed from legacy sports artifacts.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import PurePosixPath
import re

from analysis.kaushik_polymarket_replication import render_report as grammar
from analysis.kaushik_kalshi_replication import inputs


OUTCOMES = ("payoff_cents", "roi_percent")
FIGURE_PATH = "figures/baseline_profiles.pdf"
SPORT_SCOPES = grammar.SCOPES
SPORT_PHASES = grammar.PHASES
SPORT_WINDOWS = grammar.WINDOWS


class ReportBlocked(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise ReportBlocked(message)


def summary_digest(summary):
    """The exact canonical JSON serialization used by the saved-estimate writer."""
    raw=(json.dumps(summary,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    return hashlib.sha256(raw).hexdigest()


def _count(value, label):
    require(type(value) is int and value>=0,label+' must be a nonnegative integer')
    return f'{value:,}'


def _finite(value):
    return type(value) in (int,float) and math.isfinite(value)


def _index(rows, fields):
    require(type(rows) is list,'saved row list required')
    result={}
    for row in rows:
        require(isinstance(row,dict) and all(name in row for name in fields),'saved row key absent')
        key=tuple(row[name] for name in fields)
        require(key not in result,'duplicate saved result key')
        result[key]=row
    return result


def _estimate(row):
    require(type(row.get('suppressed')) is bool,'explicit saved suppression flag required')
    require(type(row.get('suppression_reasons')) is list,'saved withholding reasons required')
    if row['suppressed']:
        require(row.get('CR0') is None and bool(row['suppression_reasons']),
                'withheld estimate must be null with a reason')
    else:
        value=row.get('CR0')
        require(isinstance(value,dict) and all(_finite(value.get(name)) for name in
                ('estimate','standard_error','ci95_low','ci95_high')),'finite saved CR0 interval required')
        require(value['standard_error']>=0 and value['ci95_low']<=value['estimate']<=value['ci95_high'],
                'saved interval ordering or standard error differs')


def _model(model):
    for field in ('n_observations','n_clusters'):
        _count(model.get(field),field)
    require(type(model.get('suppressed')) is bool and isinstance(model.get('spec'),dict),
            'saved model support/specification required')
    slopes=_index(model.get('slopes'),('outcome','clock'))
    expected={(target,clock) for target in OUTCOMES for clock in model['spec']['report_clocks']}
    require(set(slopes)==expected,'saved model slope grid differs')
    for row in slopes.values(): _estimate(row)
    return slopes


def validate_summary(summary):
    require(summary.get('schema_version')=='kaushik_kalshi_estimates_v1' and
            summary.get('status')=='estimates_complete','fresh Kalshi saved estimates required')
    require(summary.get('sample_scope')=='all_available_dates','full available-date scope required')
    definitions=summary.get('definitions',{})
    require(definitions.get('duration_endpoint')=='settlement_ts' and definitions.get('combos')=='excluded',
            'Kalshi clock/combination definitions differ')
    require(definitions.get('weight')=='one per native print','native equal-print weighting required')
    require(summary.get('appendix_a2',{}).get('status')=='unavailable_from_public_trade_prints',
            'public-print role boundary differs')
    profile=_index(summary.get('table1',{}).get('profile_rows'),('outcome','bin'))
    require(set(profile)=={(target,band) for target in OUTCOMES for band in range(1,11)},
            'complete ten-bin profile required')
    gaps=_index(summary['table1'].get('gap_rows'),('outcome',))
    require(set(gaps)=={(target,) for target in OUTCOMES},'both saved tail contrasts required')
    for row in (*profile.values(),*gaps.values()):
        _estimate(row)
        _count(row.get('n_observations'),'profile print count')
        _count(row.get('n_clusters'),'profile cluster count')
    for band in range(1,11):
        left,right=(profile[(target,band)] for target in OUTCOMES)
        require((left['n_observations'],left['n_clusters'])==(right['n_observations'],right['n_clusters']),
                'paired-outcome profile support differs')
        for key in ('mean_price','win_rate'):
            require(left.get(key)==right.get(key),'paired profile auxiliaries differ')
            value=left.get(key)
            require(value is None or (_finite(value) and 0<=value<=1),'invalid saved price/win rate')
    table2=sorted(summary.get('table2',[]),key=lambda row:row['spec']['column'])
    require([row['spec']['column'] for row in table2]==[1,2,3,4,5],'five duration columns required')
    expected_specs=[(['xL'],[]),(['xL'],['cat_code']),(['xR'],[]),(['xR'],['cat_code']),
                    (['xL','xR'],['cat_code','price_code','month_code'])]
    for model,(clocks,effects) in zip(table2,expected_specs):
        require(model['spec']['clocks']==clocks and model['spec']['effects']==effects and
                model['spec']['report_clocks']==clocks,'duration column specification differs')
        _model(model)
    require(len({(row['n_observations'],row['n_clusters']) for row in table2})==1,'Table 2 common sample differs')
    for panel,clock in (('L_gt1','xL'),('R_gt1','xR')):
        models=sorted(summary.get('table3',{}).get(panel,[]),key=lambda row:row['spec']['column'])
        require([row['spec']['column'] for row in models]==[1,2,3],'three threshold-panel columns required')
        for model,(clocks,effects) in zip(models, [([clock],[]),([clock],['cat_code']),
                                   (['xL','xR'],['cat_code','price_code','month_code'])]):
            require(model['spec']['clocks']==clocks and model['spec']['effects']==effects and
                    model['spec']['report_clocks']==[clock],'threshold model specification differs')
            _model(model)
        require(len({row['n_observations'] for row in models})==1,'Table 3 common panel sample differs')
    a1=summary.get('appendix_a1',{})
    require(a1.get('spec',{}).get('effects')==['claim_fe_code'],'claim-FE specification differs')
    _model(a1)
    support=a1.get('claim_support',{})
    for name in ('claims','observations','both_tail_claims','both_tail_claim_observations'):
        _count(support.get(name),'claim support '+name)
    require(support['observations']==a1['n_observations']==table2[0]['n_observations'],
            'claim-FE/common duration sample differs')
    require(a1['n_clusters']==table2[0]['n_clusters'],'claim-FE/common duration clusters differ')
    require(support['both_tail_claims']<=support['claims'] and
            support['both_tail_claim_observations']<=support['observations'],'both-tail claim support differs')
    return profile,gaps


def _gate(summary,estimate_acceptance,score_audit,estimates_sha256,estimate_manifest_sha256):
    for value in (estimates_sha256,estimate_manifest_sha256):
        require(isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value),'explicit saved SHA-256 binding required')
    require(summary_digest(summary)==estimates_sha256,'supplied summary differs from accepted saved JSON')
    require(estimate_acceptance.get('schema_version')=='kaushik_kalshi_estimate_acceptance_v1' and
            estimate_acceptance.get('status')=='estimates_reopened_accepted' and
            estimate_acceptance.get('all_outputs_reopened') is True,'reopened estimate acceptance required')
    require(score_audit.get('schema_version')=='kaushik_kalshi_score_audit_v1' and
            score_audit.get('status')=='saved_scores_reconciled','completed independent saved-score audit required')
    for receipt in (estimate_acceptance,score_audit):
        require(receipt.get('estimates_sha256')==estimates_sha256,'saved estimate/audit content binding differs')
    require(estimate_acceptance.get('manifest_sha256')==estimate_manifest_sha256 and
            score_audit.get('estimate_manifest_sha256')==estimate_manifest_sha256,'estimate manifest/audit binding differs')
    checks=score_audit.get('checks')
    require(isinstance(checks,dict) and checks and all(type(value) is bool and value for value in checks.values()),
            'independent audit checks incomplete')


def _metadata(summary,metadata,profile,gaps):
    fields=('source_rows','baseline_rows','markets','normalized_claims','clusters','unique_event_clusters',
            'market_fallback_clusters','tail_rows','duration_tail_rows','duration_tail_clusters')
    for field in fields: _count(metadata.get(field),field)
    require(metadata['source_rows']>=metadata['baseline_rows']>=metadata['duration_tail_rows'],
            'sample population ordering differs')
    require(metadata['clusters']==metadata['unique_event_clusters']+metadata['market_fallback_clusters'],
            'native/fallback cluster support does not reconcile')
    require(sum(profile[(OUTCOMES[0],band)]['n_observations'] for band in range(1,11))==metadata['baseline_rows'],
            'profile/baseline sample differs')
    require(all(row['n_observations']==metadata['tail_rows'] for row in gaps.values()),'tail count differs')
    require(summary['table2'][0]['n_observations']==metadata['duration_tail_rows'], 'duration sample metadata differs')
    require(summary['table2'][0]['n_clusters']==metadata['duration_tail_clusters'] and
            metadata['duration_tail_rows']<=metadata['tail_rows'],'duration support metadata differs')
    require(summary.get('sample',{}).get('baseline_rows')==metadata['baseline_rows'] and
            summary.get('sample',{}).get('duration_tail_rows')==metadata['duration_tail_rows'],
            'saved sample counts differ from reader metadata')
    for key in ('first_execution_utc','last_execution_utc'):
        require(inputs._seconds(metadata.get(key),'ISO8601_UTC') is not None,'observed canonical UTC execution range required')
    require(inputs._seconds(metadata['last_execution_utc'],'ISO8601_UTC')>=
            inputs._seconds(metadata['first_execution_utc'],'ISO8601_UTC'),'observed execution range is reversed')
    archive_fields=('archive_observed_first_execution_utc','archive_observed_last_execution_utc')
    if any(key in metadata for key in archive_fields):
        require(all(inputs._seconds(metadata.get(key),'ISO8601_UTC') is not None for key in archive_fields),
                'observed raw-archive UTC execution range required')
        require(inputs._seconds(metadata[archive_fields[0]],'ISO8601_UTC')<=inputs._seconds(metadata['first_execution_utc'],'ISO8601_UTC')
                and inputs._seconds(metadata[archive_fields[1]],'ISO8601_UTC')>=inputs._seconds(metadata['last_execution_utc'],'ISO8601_UTC'),
                'raw-archive execution bounds do not contain baseline bounds')
    categories=_index(metadata.get('categories'),('category',))
    require(bool(categories),'native category population required')
    for row in categories.values():
        require(isinstance(row['category'],str) and bool(row['category']),'native category label required')
        _count(row.get('n_observations'),'category print count')
        share=row.get('share')
        require((share is None if metadata['baseline_rows']==0 else
                _finite(share) and math.isclose(share,row['n_observations']/metadata['baseline_rows'],rel_tol=0,abs_tol=1e-12)),
                'saved category share/count differs')
    require(sum(row['n_observations'] for row in categories.values())==metadata['baseline_rows'],
            'categories do not exhaust baseline')
    reasons=_index(metadata.get('exclusions'),('reason',))
    for row in reasons.values(): _count(row.get('rows'),'first-failure print count')
    require(sum(row['rows'] for row in reasons.values())==metadata['source_rows'] and
            reasons.get(('eligible',),{}).get('rows')==metadata['baseline_rows'],'first-failure source waterfall differs')
    if 'yearly_coverage' in metadata:
        years=_index(metadata['yearly_coverage'],('source_year',))
        require(bool(years),'nonempty yearly source coverage required')
        for row in years.values():
            require(type(row['source_year']) is int and 1<=row['source_year']<=9999,'integer UTC source-partition year required')
            for field in ('source_days','source_rows','baseline_rows','duration_tail_rows','excluded_rows',
                          'unadmitted_market_rows','unadmitted_combo_classification_rows'):_count(row.get(field),'year '+field)
            require(row['source_days']>0 and row['source_rows']==row['baseline_rows']+row['excluded_rows']
                    and row['duration_tail_rows']<=row['baseline_rows']
                    and row['unadmitted_market_rows']+row['unadmitted_combo_classification_rows']<=row['excluded_rows'],
                    'yearly source/eligibility row reconciliation differs')
        expected={'source_rows':metadata['source_rows'],'baseline_rows':metadata['baseline_rows'],
            'duration_tail_rows':metadata['duration_tail_rows'],'excluded_rows':metadata['source_rows']-metadata['baseline_rows'],
            'unadmitted_market_rows':reasons.get(('unadmitted_market',),{}).get('rows',0),
            'unadmitted_combo_classification_rows':reasons.get(('unadmitted_combo_classification',),{}).get('rows',0)}
        if 'source_days' in metadata:
            _count(metadata['source_days'],'source days');expected['source_days']=metadata['source_days']
        require(all(sum(row[key] for row in years.values())==value for key,value in expected.items()),
                'yearly source/eligibility totals differ')
    if 'duration_exclusions' in metadata or 'duration_coverage' in metadata:
        reasons=_index(metadata.get('duration_exclusions'),('reason',))
        for row in reasons.values(): _count(row.get('rows'),'tail-only clock count')
        require(('not_tail_bin',) not in reasons and sum(row['rows'] for row in reasons.values())==metadata['tail_rows'] and
                reasons.get(('eligible',),{}).get('rows',0)==metadata['duration_tail_rows'],
                'tail-only clock exclusion reconciliation differs')
        coverage=_index(metadata.get('duration_coverage'),('source_version',))
        for row in coverage.values():
            _count(row.get('tail_rows'),'source tail prints')
            _count(row.get('duration_tail_rows'),'source duration prints')
            require(row['duration_tail_rows']<=row['tail_rows'],'source duration population exceeds tails')
            first,last=row.get('first_utc_day'),row.get('last_utc_day')
            require((first is None and last is None) if row['duration_tail_rows']==0 else
                    inputs._day(first) is not None and inputs._day(last) is not None and first<=last,
                    'duration source-partition dates differ')
        require(sum(row['tail_rows'] for row in coverage.values())==metadata['tail_rows'] and
                sum(row['duration_tail_rows'] for row in coverage.values())==metadata['duration_tail_rows'],
                'source duration coverage totals differ')


def validate_sports(summary):
    require(isinstance(summary,dict) and summary.get('schema_version')=='kaushik_kalshi_sports_estimates_v1'
            and summary.get('status')=='sports_estimates_complete' and summary.get('sample_scope')=='all_available_dates',
            'fresh native Kalshi sports JSON required; legacy estimates cannot supply it')
    definition=summary.get('definitions',{})
    require(definition.get('observation')=='one admitted native trade print' and
            definition.get('weight')=='one per native print' and definition.get('cluster')=='sport-namespaced provider game_key'
            and definition.get('endpoint_grace_seconds')==0 and definition.get('minimum_prints_per_cell')==500
            and definition.get('minimum_games_per_cell')==30 and definition.get('phase')==
            'pregame timestamp<actual_start_seconds; in_play timestamp>=actual_start_seconds; timestamp<=actual_end_seconds',
            'fresh sports claim, clock, weighting or support definition differs')
    binding=summary.get('input_binding',{})
    require(all(isinstance(binding.get(key),str) and re.fullmatch('[0-9a-f]{64}',binding[key]) for key in
            ('input_manifest_sha256','input_acceptance_sha256','map_manifest_sha256','map_acceptance_sha256')),
            'fresh sports native input/map receipt hashes required')
    require(isinstance(binding.get('timing_qualification'),str) and bool(binding['timing_qualification'].strip()),
            'saved provider clock qualification required')
    require([(row.get('panel'),row.get('key')) for row in definition.get('windows',[])]==
            [(panel,key) for panel,key,_ in SPORT_WINDOWS],'literal sports window grid differs')
    for field in ('candidate_rows','eligible_rows'): _count(summary.get(field),'sports '+field)
    scope=_index(summary.get('scope_counts'),('scope',))
    phases=_index(summary.get('phase_counts'),('scope','phase'))
    windows=_index(summary.get('window_counts'),('scope','window'))
    require(set(scope)=={(key,) for key in SPORT_SCOPES} and
            set(phases)=={(key,phase) for key in SPORT_SCOPES for phase in SPORT_PHASES} and
            set(windows)=={(scope,key) for scope in SPORT_SCOPES for _,key,_ in SPORT_WINDOWS},'complete sports count grids required')
    for row in (*scope.values(),*phases.values(),*windows.values()):
        _count(row.get('n_observations'),'sports prints'); _count(row.get('n_games'),'sports games')
        require(row['n_games']<=row['n_observations'],'sports games exceed prints')
    require(scope[('pooled',)]['n_observations']==summary['eligible_rows'] and
            sum(scope[(sport,)]['n_observations'] for sport in SPORT_SCOPES[1:])==summary['eligible_rows'] and
            sum(scope[(sport,)]['n_games'] for sport in SPORT_SCOPES[1:])==scope[('pooled',)]['n_games'],
            'pooled/sport game counts differ')
    profiles=_index(summary.get('profile_rows'),('scope','phase','bin','outcome'))
    phase_rows=_index(summary.get('phase_rows'),('scope','phase','outcome'))
    window_rows=_index(summary.get('window_rows'),('scope','window','outcome'))
    require(set(profiles)=={(scope,phase,band,target) for scope in SPORT_SCOPES for phase in SPORT_PHASES
                            for band in range(1,11) for target in OUTCOMES} and
            set(phase_rows)=={(scope,phase,target) for scope in SPORT_SCOPES for phase in (*SPORT_PHASES,'in_play_minus_pregame') for target in OUTCOMES} and
            set(window_rows)=={(scope,key,target) for scope in SPORT_SCOPES for _,key,_ in SPORT_WINDOWS for target in OUTCOMES},
            'complete fresh sports estimate grids required')
    for row in (*profiles.values(),*phase_rows.values(),*window_rows.values()):
        _estimate(row)
        _count(row.get('n_observations'),'sports estimate prints'); _count(row.get('n_clusters'),'sports contributing games')
        support=row.get('cell_support')
        require(isinstance(support,list) and support,'saved sports required-cell support missing')
        for cell in support:
            _count(cell.get('n_observations'),'sports cell prints'); _count(cell.get('n_clusters'),'sports cell games')
            require(cell['n_clusters']<=cell['n_observations'],'sports cell games exceed prints')
        paper=all(cell['n_clusters']>=30 for cell in support)
        project=all(cell['n_observations']>=500 for cell in support)
        require(row.get('paper_support') is paper and row.get('project_support') is project and
                row['n_observations']==sum(cell['n_observations'] for cell in support) and
                row['n_clusters']==min(cell['n_clusters'] for cell in support) and
                row.get('contributing_cluster_minimum')==row['n_clusters'] and
                (row['suppressed'] or (paper and project)),'sports separate print/game guard or support differs')
    for key in SPORT_SCOPES:
        require(sum(phases[(key,phase)]['n_observations'] for phase in SPORT_PHASES)==scope[(key,)]['n_observations'],
                'sports phase/scope count differs')
        for phase in SPORT_PHASES:
            require(sum(profiles[(key,phase,band,OUTCOMES[0])]['n_observations'] for band in range(1,11))==
                    phases[(key,phase)]['n_observations'],'sports full-bin/phase counts differ')
        for panel,phase in (('pregame','pregame'),('since_start','in_play')):
            require(sum(windows[(key,window)]['n_observations'] for p,window,_ in SPORT_WINDOWS if p==panel)==
                    phases[(key,phase)]['n_observations'],'sports window/phase partition differs')
    exclusions=summary.get('exclusions')
    require(isinstance(exclusions,list),'sports candidate reconciliation required')
    for row in exclusions: _count(row.get('rows'),'sports excluded prints')
    require(sum(row['rows'] for row in exclusions)==summary['candidate_rows'] and
            sum(row['rows'] for row in exclusions if row.get('reason')=='eligible')==summary['eligible_rows'],
            'sports disjoint candidate reconciliation differs')
    quality=_index(summary.get('timing_coverage'),('sport','timing_quality'))
    for row in quality.values():
        require(row['sport'] in SPORT_SCOPES[1:] and isinstance(row['timing_quality'],str) and row['timing_quality'],
                'saved per-sport provider timing quality required')
        _count(row.get('rows'),'timing-quality prints')
    require(sum(row['rows'] for row in quality.values())==summary['eligible_rows'],
            'per-sport timing-quality coverage differs')
    for sport in SPORT_SCOPES[1:]:
        require(sum(row['rows'] for row in quality.values() if row['sport']==sport)==scope[(sport,)]['n_observations'],
                'sport timing-quality counts differ')
    return profiles,phase_rows,window_rows


def _sports_gate(bundle,core_hash):
    require(isinstance(bundle,dict),'explicit fresh sports summary/receipts required')
    data=bundle.get('summary')
    validate_sports(data)
    digest,manifest=bundle.get('sports_sha256'),bundle.get('sports_manifest_sha256')
    require(isinstance(digest,str) and re.fullmatch('[0-9a-f]{64}',digest) and summary_digest(data)==digest
            and isinstance(manifest,str) and re.fullmatch('[0-9a-f]{64}',manifest),'sports saved content binding differs')
    accepted,audit=bundle.get('acceptance',{}),bundle.get('score_audit',{})
    require(accepted.get('schema_version')=='kaushik_kalshi_sports_estimate_acceptance_v1' and
            accepted.get('status')=='sports_estimates_reopened_accepted' and accepted.get('all_outputs_reopened') is True
            and accepted.get('sports_sha256')==digest and accepted.get('manifest_sha256')==manifest and
            accepted.get('core_estimates_sha256')==core_hash,'reopened fresh sports acceptance/core binding required')
    require(audit.get('schema_version')=='kaushik_kalshi_sports_score_audit_v1' and audit.get('status')=='saved_scores_reconciled'
            and audit.get('sports_sha256')==digest and audit.get('sports_manifest_sha256')==manifest and
            isinstance(audit.get('checks'),dict) and audit['checks'] and all(value is True for value in audit['checks'].values()),
            'completed independently bound sports score audit required')
    return data


def _r2(model,target,group='stacked'):
    meta=(model.get('joint') or {}).get('metadata',{})
    grouped=meta.get('grouped_r_squared') or {}
    if group=='stacked':
        return (grouped.get('stacked_weighted_tss_r_squared_by_target') or
                meta.get('r_squared_including_fixed_effects_by_target') or {}).get(target)
    return grouped.get('group_summaries',{}).get(group,{}).get('r_squared_including_fixed_effects_by_target',{}).get(target)


def _interval(row):
    if row['suppressed']: return ''
    value=row['CR0']
    return '['+grammar.number(value['ci95_low'],signed=True)+', '+grammar.number(value['ci95_high'],signed=True)+']'


VARIANCE_NOTE=(r'Parentheses are event/market-clustered CR0 standard errors; brackets and figure whiskers are saved pointwise normal 95\% intervals. '
               r'$\dagger$ marks fewer than 30 effective influence clusters or a largest cluster variance share above 25\%.')


def _slopes_table(models,title,targets,clocks,headers,*,r2_tails=False):
    indexed=[_model(model) for model in models]
    rows=[]
    labeled=[]
    for target in targets:
        for clock in clocks:
            selected=[index.get((target,clock)) for index in indexed]
            label=('Payoff' if target==OUTCOMES[0] else 'Return')+' / '+('Original' if clock=='xL' else 'Remaining')
            rows.extend([[label,*[grammar.cell(row) if row else '--' for row in selected]],
                         ['',*[grammar.se(row) if row else '' for row in selected]],
                         ['',*[_interval(row) if row else '' for row in selected]]])
            labeled.extend((f'({model["spec"]["column"]}) {label}',row) for model,row in zip(models,selected) if row)
    rows.extend([['Prints',*[_count(model['n_observations'],'model prints') for model in models]],
                 ['Event/market clusters',*[_count(model['n_clusters'],'model clusters') for model in models]]])
    for target in targets:
        for group in (('D1','D10','stacked') if r2_tails else ('stacked',)):
            rows.append([('$R^2$ '+group+' / '+('payoff' if target==OUTCOMES[0] else 'return')),
                         *[grammar.number(_r2(model,target,group),4) for model in models]])
    for effect,label in (('cat_code','Category FE'),('price_code','Exact price FE'),('month_code','UTC month FE')):
        rows.append([label,*['Yes' if effect in model['spec']['effects'] else 'No' for model in models]])
    note=(r'D10-minus-D1 slopes per unit of $\log_2(1+\mathrm{days})$; payoff is cents, return is percentage points. '
          r'Opening is native \texttt{open\_time}; endpoint is \texttt{settlement\_ts}. Fixed effects vary by tail. '
          r'Each table shares its saved sample across columns; full controls hold the other clock fixed. '
          r'$R^2$ includes controls; stacked TSS uses the overall outcome mean. A withheld $R^2$ is undefined in the saved fit. '+VARIANCE_NOTE+
          grammar.withholding_note(labeled))
    return grammar.table(title,headers,rows,note)


def figure_specs(summary):
    """Return deterministic isolated-mark descriptions; creates no figure/file."""
    profile,_=validate_summary(summary)
    panels=[]
    for target,label,marker,color in ((OUTCOMES[0],'Mean payoff (cents)','o','#225E7A'),
                                      (OUTCOMES[1],'Mean individual return (%)','s','#9B5F32')):
        points=[]
        withheld=[]
        for band in range(1,11):
            row=profile[(target,band)]
            if row['suppressed']:
                withheld.append({'bin':band,'n_observations':row['n_observations'],
                                 'n_clusters':row['n_clusters'],'reasons':list(row['suppression_reasons'])})
            else:
                points.append({'bin':band,**{key:row['CR0'][key] for key in ('estimate','ci95_low','ci95_high')},
                               'n_observations':row['n_observations'],'n_clusters':row['n_clusters']})
        panels.append({'outcome':target,'ylabel':label,'points':points,'withheld':withheld,
                       'ticks':list(range(1,11)),'tick_labels':[f'D{i}' for i in range(1,11)],
                       'linestyle':'none','marker':marker,'color':color,'capsize':3,'zero_reference':0})
    return {'filename':FIGURE_PATH,'panels':panels,'interval':'saved pointwise normal 95% CR0',
            'x_label':'Fixed ten-cent probability bin','connect_points':False}


def sports_figure_specs(summary):
    """Fresh saved sports figures only; no plot creation or acceptance claim."""
    profiles,_,windows=validate_sports(summary)
    figures={}
    for scope in SPORT_SCOPES:
        for family in ('profiles','windows'):
            panels=[]
            for target in OUTCOMES:
                series=[]
                if family=='profiles':
                    groups=[(phase,[(band,profiles[(scope,phase,band,target)]) for band in range(1,11)]) for phase in SPORT_PHASES]
                else:
                    groups=[(panel,[(i,windows[(scope,key,target)]) for i,(p,key,_) in enumerate(SPORT_WINDOWS) if p==panel])
                            for panel in ('pregame','since_start','final_hour')]
                for i,(label,rows) in enumerate(groups):
                    points=[{'x':x,**{name:row['CR0'][name] for name in ('estimate','ci95_low','ci95_high')}}
                            for x,row in rows if not row['suppressed']]
                    withheld=[{'x':x,'n_observations':row['n_observations'],'n_clusters':row['n_clusters'],
                               'reasons':list(row['suppression_reasons'])} for x,row in rows if row['suppressed']]
                    series.append({'label':label,'points':points,'withheld':withheld,'linestyle':'none',
                                   'marker':('o','s','^')[i],'color':('#225E7A','#9B5F32','#586A3B')[i],'capsize':3})
                panels.append({'outcome':target,'series':series,'zero_reference':0,
                    'ticks':list(range(1,11)) if family=='profiles' else list(range(14)),
                    'tick_labels':['D'+str(i) for i in range(1,11)] if family=='profiles' else [row[2] for row in SPORT_WINDOWS]})
            key='sports_'+scope+'_'+family
            figures[key]={'filename':'figures/'+key+'.pdf','scope':scope,'family':family,'panels':panels,
                          'connect_points':False,'interval':'saved pointwise normal 95% game-cluster CR0'}
    return figures


def _duration_tables(metadata):
    if 'duration_exclusions' not in metadata: return []
    coverage=grammar.table('Duration sample. Source and clock coverage',
        ['Source vintage','Tail prints','Duration prints','First UTC day','Last UTC day'],
        [[grammar.tex(row['source_version']),_count(row['tail_rows'],'source tails'),_count(row['duration_tail_rows'],'source duration'),
          grammar.tex(row['first_utc_day']) if row['first_utc_day'] is not None else '--',
          grammar.tex(row['last_utc_day']) if row['last_utc_day'] is not None else '--'] for row in metadata['duration_coverage']],
        'Dates are admitted UTC source partitions containing duration-eligible prints, not execution-time extrema. Missing clocks affect duration only.')
    exclusions=grammar.table('Duration sample. Baseline-tail clock reconciliation',['Clock status','Prints'],
        [[grammar.tex(row['reason'].replace('_',' ')),_count(row['rows'],'tail clock prints')] for row in metadata['duration_exclusions']],
        'Disjoint reasons within baseline D1/D10 only. Eligible is the common Table 2 sample; non-tail prints are not counted here.')
    return [coverage,exclusions]


def _yearly_table(metadata):
    rows=sorted(metadata['yearly_coverage'],key=lambda row:row['source_year'])
    fields=('source_rows','baseline_rows','duration_tail_rows','excluded_rows',
            'unadmitted_market_rows','unadmitted_combo_classification_rows')
    body=[[str(row['source_year']),*[_count(row[key],'year '+key) for key in fields]] for row in rows]
    body.append(['Total',*[_count(sum(row[key] for row in rows),'year total '+key) for key in fields]])
    return grammar.table('Sample. Yearly source and eligibility coverage',
        ['UTC year','Source','Baseline','Duration tails','Excluded','No market match','Unknown type'],body,
        'Years are UTC source partitions, not execution-time extrema. No market match means no match in retained noncompound metadata; '
        'Unknown type means no independently admitted single-contract classification. Counts reflect the observed first exclusion, '
        'not an exhaustive diagnosis of missing raw metadata; other exclusions remain in the excluded total.')


def _sports_tables(data,paths):
    profiles,phases,windows=validate_sports(data)
    text=[r'\clearpage']
    scope_counts={row['scope']:row for row in data['scope_counts']}
    phase_counts={(row['scope'],row['phase']):row for row in data['phase_counts']}
    text.append(grammar.table('Sports. Fresh native winner-claim support',
        ['Scope','Prints','Games','Pregame prints','In-play prints'],
        [[grammar.tex(scope),_count(scope_counts[scope]['n_observations'],'sports prints'),_count(scope_counts[scope]['n_games'],'sports games'),
          *[_count(phase_counts[(scope,phase)]['n_observations'],'phase prints') for phase in SPORT_PHASES]] for scope in SPORT_SCOPES],
        r'Native YES/NO claim payouts are independently bound to provider game identity and results. '
        r'Game clusters are namespaced by sport. Pregame is $t<s$; in play is $s\le t\le e$, with no endpoint grace. '
        r'Each required bin/tail cell needs at least 500 prints and 30 games; both guards apply separately. '
        r'$G_{\min}$ is the minimum game count across the required cells, not total matched games. '
        r'Provider clock conventions and their source quality are retained from the independently accepted map; settlement is not a game clock.'))
    text.append(grammar.table('Sports. Provider clock conventions',['Sport','Recorded timing quality','Prints'],
        [[grammar.tex(row['sport']),grammar.tex(row['timing_quality']),_count(row['rows'],'timing-quality prints')]
         for row in data['timing_coverage']],grammar.tex(data['input_binding']['timing_qualification']),widths='lp{9cm}r'))
    text.append(grammar.table('Sports. Candidate reconciliation',['Sport','Status','Prints'],
        [[grammar.tex(row['sport']),grammar.tex(row['reason'].replace('_',' ')),_count(row['rows'],'sports exclusions')]
         for row in data['exclusions']], 'Disjoint candidate-cache counts, including eligible; missing accepted mappings are not guessed.'))
    for scope in SPORT_SCOPES:
        rows=[]
        for phase in (*SPORT_PHASES,'in_play_minus_pregame'):
            selected=[phases[(scope,phase,target)] for target in OUTCOMES]
            rows.append([grammar.tex(phase.replace('_',' ')),*[grammar.cell(row) for row in selected],
                         _count(selected[0]['n_observations'],'phase tails'),_count(selected[0]['n_clusters'],'tail games')])
            rows.append(['',*[grammar.se(row) for row in selected],'',''])
        text.extend([r'\clearpage',grammar.table('Sports. '+grammar.tex(scope)+': joint tail contrasts',
            ['Phase','Payoff gap','Return gap','Tail prints',r'$G_{\min}$'],rows,
            r'D10 minus D1; the change is in-play gap minus pregame gap using the joint game scores. '
            r'Payoff is cents, return gaps are percentage points. Parentheses are CR0 standard errors.'+
            grammar.withholding_note((phase+' '+target,phases[(scope,phase,target)])
                for phase in (*SPORT_PHASES,'in_play_minus_pregame') for target in OUTCOMES))])
        for family in ('profiles','windows'):
            text.extend([r'\begin{figure}[!htbp]\centering',r'\includegraphics[width=\linewidth]{'+paths['sports_'+scope+'_'+family]+'}',
                r'\caption*{Sports. '+grammar.tex(scope)+': '+family+r'}',
                r'{\footnotesize Isolated estimates and capped saved normal 95\% game-cluster intervals. Withheld cells have no marks.}\end{figure}'])
        rows=[]
        for phase in SPORT_PHASES:
            for band in range(1,11):
                selected=[profiles[(scope,phase,band,target)] for target in OUTCOMES]
                rows.append(['Pre' if phase=='pregame' else 'In','D'+str(band),*[grammar.cell(row) for row in selected],
                    _count(selected[0]['n_observations'],'bin prints'),_count(selected[0]['n_clusters'],'bin games')])
                rows.append(['','',*[grammar.se(row) for row in selected],'',''])
        text.append(grammar.table('Sports. '+grammar.tex(scope)+': complete fixed-bin support',
            ['Phase','Bin','Payoff','Return','Prints','Games'],rows,
            'Payoff is cents; individual return is percent. Both the 500-print and 30-game floors apply to each cell. '+
            grammar.withholding_note((phase+' D'+str(band)+' '+target,profiles[(scope,phase,band,target)])
                for phase in SPORT_PHASES for band in range(1,11) for target in OUTCOMES)))
        rows=[]
        for _,window,label in SPORT_WINDOWS:
            selected=[windows[(scope,window,target)] for target in OUTCOMES]
            support=selected[0]['cell_support']
            rows.append([grammar.tex(label),*[grammar.cell(row) for row in selected],
                _count(support[0]['n_observations'],'window D1'),_count(support[1]['n_observations'],'window D10'),
                _count(selected[0]['n_clusters'],'window tail games')])
            rows.append(['',*[grammar.se(row) for row in selected],'','',''])
        text.append(grammar.table('Sports. '+grammar.tex(scope)+': literal-window tail support',
            ['Window','Payoff gap','Return gap','D1 prints','D10 prints',r'$G_{\min}$'],rows,
            'Final-hour windows overlap since-start windows; pregame and since-start windows separately partition their phases. '+
            grammar.withholding_note((window+' '+target,windows[(scope,window,target)]) for _,window,_ in SPORT_WINDOWS for target in OUTCOMES)))
    return text


def render_source(summary,metadata,*,estimate_acceptance,score_audit,estimates_sha256,
                  estimate_manifest_sha256,figure_paths=None,draft=False,sports=None,
                  sports_release_decision=None):
    """Render core saved evidence; no actual TeX/PDF/file authoring takes place.

    Metadata is an explicit saved-summary adapter: observed first/last UTC,
    source/baseline/market/claim/cluster/tail counts, native categories containing
    category,n_observations,share, and disjoint reason,rows exclusions (including
    eligible). A draft does not bypass acceptance or the independent score audit.
    Fresh sports need a separately admitted JSON adapter, not a legacy zip.
    """
    require(type(draft) is bool,'explicit draft mode required')
    profile,gaps=validate_summary(summary)
    _gate(summary,estimate_acceptance,score_audit,estimates_sha256,estimate_manifest_sha256)
    _metadata(summary,metadata,profile,gaps)
    fresh_sports=_sports_gate(sports,estimates_sha256) if sports is not None else None
    if fresh_sports is not None:
        require(fresh_sports['candidate_rows']<=metadata['baseline_rows'],'sports candidates exceed their native baseline')
    user_scope=(isinstance(sports_release_decision,dict) and
                sports_release_decision.get('status')=='user_approved_non_sports_scope' and
                isinstance(sports_release_decision.get('authorization_reference'),str) and
                bool(sports_release_decision['authorization_reference'].strip()))
    require(draft or user_scope or fresh_sports is not None,'full fresh report release requires admitted sports or an explicit user scope decision')
    require(draft or ('duration_exclusions' in metadata and 'duration_coverage' in metadata),
            'released report requires bound tail-only clock/source coverage')
    require(draft or 'yearly_coverage' in metadata,'released report requires bound yearly source/eligibility coverage')
    expected_paths={'baseline':FIGURE_PATH}
    if fresh_sports is not None:
        expected_paths.update({key:value['filename'] for key,value in sports_figure_specs(fresh_sports).items()})
    paths=expected_paths if figure_paths is None else figure_paths
    require(isinstance(paths,dict) and set(paths)==set(expected_paths),'explicit complete figure mapping required')
    for filename in paths.values():
        require(isinstance(filename,str),'portable relative PDF figure path required')
        path=PurePosixPath(filename)
        require(not path.is_absolute() and path.as_posix()==filename and
                all(part not in ('.','..') for part in filename.split('/')) and
                re.fullmatch(r'[A-Za-z0-9_./-]+\.pdf',filename),'portable relative PDF figure path required')
    filename=paths['baseline']
    text=[r'\documentclass[11pt]{article}',r'\usepackage[margin=0.65in]{geometry}',
          r'\usepackage{booktabs,graphicx,caption,array,longtable}',r'\usepackage[T1]{fontenc}',
          r'\setlength{\parindent}{0pt}\setlength{\parskip}{5pt}\setlength{\tabcolsep}{4pt}',
          r'\makeatletter\setlength{\@fptop}{0pt}\makeatother',r'\captionsetup{font=small}\begin{document}',
          r'{\Large Favorite--longshot bias: native Kalshi replication}\par']
    if draft: text.append(r'\textbf{DRAFT -- not a released complete report.}\par')
    text.extend([r'All available execution dates in the pinned native archive through October 1, 2026; combinations excluded. '
                 r'One admitted native print has weight one. The recorded YES/NO taker price selects a finalized binary \$1 claim, with $0<P<1$. '
                 r'Payoff is $100(Y-P)$ cents; individual return is $100(Y/P-1)$ percent, averaged before fees. '
                 r'D1--D10 are fixed ten-cent bins and gaps are signed D10 minus D1.\par',
                 r'Native event categories are retained; missing categories are Unclassified. Opening is \texttt{open\_time}; '
                 r'the endpoint is \texttt{settlement\_ts}; missing clocks affect duration only. Public prints lack wallet-role observations, '
                 r'so the Appendix A2 maker/taker comparison is unavailable.\par'])
    if user_scope and not draft: text.append(r'Sports results are outside this report under the explicit user scope decision.\par')
    sample=[['First baseline execution UTC',grammar.tex(metadata['first_execution_utc'])],
            ['Last baseline execution UTC',grammar.tex(metadata['last_execution_utc'])]]
    for key,label in (('archive_observed_first_execution_utc','First raw-archive execution UTC'),
                      ('archive_observed_last_execution_utc','Last raw-archive execution UTC')):
        if key in metadata:sample.append([label,grammar.tex(metadata[key])])
    for key,label in (('source_rows','Source prints'),('baseline_rows','Baseline prints'),('markets','Markets'),
                      ('normalized_claims','YES/NO claims'),('clusters','Event/market clusters'),
                      ('unique_event_clusters','Native event clusters'),('market_fallback_clusters','Market fallback clusters'),
                      ('tail_rows','Baseline D1 + D10 prints'),('duration_tail_rows','Duration tail prints'),
                      ('duration_tail_clusters','Duration tail clusters')):
        sample.append([label,_count(metadata[key],key)])
    text.append(grammar.table('Sample. Coverage and reconciliation',['Quantity','Kalshi'],sample,
        'The admitted metadata and eventual resolution requirement define coverage; this is not an exchange-wide completeness certificate.'))
    if 'yearly_coverage' in metadata:text.append(_yearly_table(metadata))
    text.append(grammar.table('Sample. Disjoint first-failure eligibility',['Reason','Prints'],
        [[grammar.tex(row['reason'].replace('_',' ')),_count(row['rows'],'exclusions')] for row in metadata['exclusions']],
        'Every source print receives exactly one reason, including eligible. Missing settlement times do not remove otherwise eligible baseline prints.'))
    text.append(grammar.table('Categories. Native event labels',['Category','Prints',r'Share (\%)'],
        [[grammar.tex(row['category']),_count(row['n_observations'],'category'),
          grammar.number(100*row['share']) if row['share'] is not None else 'withheld'] for row in metadata['categories']],
        'Mutually exclusive native labels; Unclassified retains missing category evidence.'))
    text.extend([r'\clearpage',r'\begin{figure}[!htbp]\centering',
        r'\includegraphics[width=\linewidth]{'+filename+'}',
        r'\caption*{Figure 1. Full fixed-bin payoff and return profiles}',
        r'{\footnotesize Isolated estimates and capped saved pointwise normal 95\% CR0 intervals; withheld cells have no plotted marks.}\end{figure}'])
    rows=[]
    for band in range(1,11):
        payoff,roi=(profile[(target,band)] for target in OUTCOMES)
        rows.append([f'D{band}',grammar.number(payoff.get('mean_price'),4),grammar.number(payoff.get('win_rate'),4),
                     grammar.cell(payoff),grammar.cell(roi),_count(payoff['n_observations'],'prints'),_count(payoff['n_clusters'],'clusters')])
        rows.append(['','','',grammar.se(payoff),grammar.se(roi),'',''])
    pg,rg=(gaps[(target,)] for target in OUTCOMES)
    rows.extend([[r'D10$-$D1','--','--',grammar.cell(pg),grammar.cell(rg),_count(metadata['tail_rows'],'tails'),_count(pg['n_clusters'],'tail clusters')],
                 ['','','',grammar.se(pg),grammar.se(rg),'','']])
    text.append(grammar.table('Table 1. Complete fixed-bin evidence',['Bin','Mean price','Win rate','Payoff','Return','N','G'],rows,
        r'Mean price is dollars; payoff is cents; return is percent, with gap in percentage points. N is prints and G contributing event/market clusters. '+VARIANCE_NOTE+
        grammar.withholding_note([(f'D{band} {target}',profile[(target,band)]) for target in OUTCOMES for band in range(1,11)]+
                                 [(f'D10-D1 {target}',gaps[(target,)]) for target in OUTCOMES])))
    text.append(r'\clearpage')
    text.extend(_duration_tables(metadata))
    text.extend([r'Duration models have tail-specific coefficients for $q\in\{D1,D10\}$:',
        r'\[ z_i=\alpha_q+\beta_{Lq}x_{Li}+\beta_{Rq}x_{Ri}+F_i^\top\gamma_q+\varepsilon_i,\qquad i\in q. \]',
        r'Here $z_i$ is payoff or individual return; $x_{Li}=\log_2(1+L_i)$ and $x_{Ri}=\log_2(1+R_i)$. '
        r'In days, $L_i=(\tau_i-o_i)/86400$ and $R_i=(\tau_i-t_i)/86400$, where $o_i$ is native \texttt{open\_time}, '
        r'$\tau_i$ is \texttt{settlement\_ts}, and $t_i$ is execution time, all in UTC seconds. '
        r'Omitted clocks and controls follow each column; control coefficients vary by tail. '
        r'Reported gradients are $\beta_{\cdot,D10}-\beta_{\cdot,D1}$. Remaining time decreases as execution time advances.\par'])
    models=sorted(summary['table2'],key=lambda row:row['spec']['column'])
    for target,suffix in zip(OUTCOMES,('A','B')):
        text.append(_slopes_table(models,'Table 2'+suffix+'. Duration gradients',[target],('xL','xR'),
            ['Clock / controls','(1) Original','(2) Category','(3) Remaining','(4) Category','(5) Both full'],r2_tails=True))
    text.append(r'\clearpage')
    for panel,clock,title in (('L_gt1','xL','A. Original lifespan greater than one day'),
                              ('R_gt1','xR','B. Remaining time greater than one day')):
        models=sorted(summary['table3'][panel],key=lambda row:row['spec']['column'])
        text.append(_slopes_table(models,'Table 3'+title[0]+'. '+title[3:],OUTCOMES,(clock,),
                                 ['Outcome / controls','(1) Raw','(2) Category FE','(3) Both + full FE']))
    if fresh_sports is not None: text.extend(_sports_tables(fresh_sports,paths))
    a1=summary['appendix_a1']
    slopes=_model(a1)
    rows=[]
    for target,label in zip(OUTCOMES,('Payoff (cents)','Return (pp)')):
        selected=[slopes[(target,clock)] for clock in ('xL','xR')]
        rows.extend([[label,*[grammar.cell(row) for row in selected]],['',*[grammar.se(row) for row in selected]],
                     ['',*[_interval(row) for row in selected]]])
    support=a1['claim_support']
    rows.extend([['Prints / clusters',_count(a1['n_observations'],'A1 prints'),_count(a1['n_clusters'],'A1 clusters')],
                 ['Claims / both-tail claims',_count(support['claims'],'claims'),_count(support['both_tail_claims'],'both-tail claims')],
                 ['Prints in both-tail claims',_count(support['both_tail_claim_observations'],'both-tail prints'),'--']])
    text.extend([r'\clearpage',grammar.table('Appendix A1. Claim fixed effects: price-path diagnostic',
        ['Outcome / support',r'Original $\times$ D10',r'Remaining $\times$ D10'],rows,
        r'Claim effects absorb standalone original duration. Regressors are $H,Hx_L,x_R,Hx_R$, with $H=1$ in D10. '
        r'Within claim, $\widetilde{Y-P}=-\widetilde P$; payoff coefficients describe price paths. '+VARIANCE_NOTE+
        grammar.withholding_note((target+' '+clock,row) for (target,clock),row in slopes.items())),r'\end{document}'])
    return '\n'.join(text)+'\n'
