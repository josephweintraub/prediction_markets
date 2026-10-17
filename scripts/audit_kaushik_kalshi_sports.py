#!/usr/bin/env python3
"""Independent, bounded saved-source sports-map and game-score release audit.

No collection, raw trade replay, map production or sports estimation is done
here. Fixture workers receive a connection; the CLI always guards production.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import unicodedata

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from scripts import audit_kaushik_kalshi_inputs as bodies
from scripts import audit_kaushik_replication_saved_scores as numerical

DATA_ROOT = Path('/mnt/data')
RUN_ROOT = DATA_ROOT / 'runs'
PRODUCTION_PYTHON = Path('/home/ubuntu/venv/bin/python')
METADATA_FILES, METADATA_ROWS, MLB_GAMES = 80, 166_961_153, 3696
SPORTS = ('mlb','nfl','nba','nhl','cbb','atp','epl','cfb','wnba')
SCOPES = ('pooled', *SPORTS)
SERIES = {'KXMLBGAME':'mlb','KXNFLGAME':'nfl','KXNBAGAME':'nba','KXNHLGAME':'nhl',
          'KXNCAAMBGAME':'cbb','KXATPMATCH':'atp','KXEPLGAME':'epl','KXNCAAFGAME':'cfb','KXWNBAGAME':'wnba'}
LEAGUES = {'mlb':'MLB','nfl':'NFL','nba':'NBA','nhl':'NHL','cbb':'NCAAMB','cfb':'NCAAFB','wnba':'WNBA'}
MAP_CHECKS = ('identities_verified','provider_clock_conventions_verified','native_yes_results_verified','source_hashes_reopened')
PROVIDER_FAILURE_REASONS = {'independent_provider_completed_result_missing','provider_result_pair_unproved',
    'provider_winner_or_draw_identity_unproved','provider_result_source_field_binding_missing',
    'provider_result_participant_name_identity_ambiguous','provider_result_participant_mapping_not_bijective',
    'native_provider_yes_result_disagreement'}
CLOCK_PROOF = ('Exact clock values and stated conventions match the hash-bound retained provider timing bodies and reviewed binding, '
    'with per-sport raw quality retained and ATP\u2019s scheduled-start plus archived-duration qualification explicit. '
    'It is NOT a new play-by-play boundary reconstruction or a guarantee of observed actual starts/ends.')
MAX_BOUND_BYTES, MAX_CACHE_BYTES, MAX_ROWS = 32_000_000_000, 12_000_000_000, 5_000_000
MAX_NUMPY_BYTES, MAX_OUTPUT_BYTES, FLOOR = 8_000_000_000, 1_000_000_000, 20_000_000_000
DUCKDB_MEMORY_LIMIT = '16GB'
MINIMUM_AVAILABLE_RAM_BYTES = 32_000_000_000
DISABLED_OPTIMIZERS = 'join_order,build_side_probe_side'
SOURCE_FILES = ('scripts/audit_kaushik_kalshi_sports.py','tests/test_kaushik_kalshi_sports_audit.py',
    'scripts/audit_kaushik_kalshi_inputs.py','scripts/audit_kaushik_replication_saved_scores.py',
    'analysis/kaushik_polymarket_replication/render_report.py','production_guard.py')
WINDOWS = (
    ('pregame','pre_lt24h','u < -86400'),
    ('pregame','pre_24to6h','u>=-86400 AND u< -21600'),
    ('pregame','pre_6to1h','u>=-21600 AND u< -3600'),
    ('pregame','pre_60to15m','u>=-3600 AND u< -900'),
    ('pregame','pre_15to0m','u>=-900 AND u<0'),
    ('since_start','live_0to15m','u>=0 AND u<900'),
    ('since_start','live_15to30m','u>=900 AND u<1800'),
    ('since_start','live_30to60m','u>=1800 AND u<3600'),
    ('since_start','live_1to2h','u>=3600 AND u<7200'),
    ('since_start','live_2hplus','u>=7200'),
    ('final_hour','final_60to30m','u>=0 AND r>1800 AND r<=3600'),
    ('final_hour','final_30to15m','u>=0 AND r>900 AND r<=1800'),
    ('final_hour','final_15to5m','u>=0 AND r>300 AND r<=900'),
    ('final_hour','final_5to0m','u>=0 AND r>=0 AND r<=300'))

AuditBlocked = bodies.AuditBlocked
require, literal, ident = bodies.require, bodies.literal, bodies.ident


def _decode(raw):
    def finite(value):
        result = float(value)
        require(math.isfinite(result), 'nonfinite sports JSON number')
        return result
    return json.loads(raw, object_pairs_hook=bodies._unique, parse_float=finite,
        parse_constant=lambda value: (_ for _ in ()).throw(AuditBlocked('nonfinite sports JSON constant')))


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _name(value):
    require(isinstance(value,str) and value.strip() and not any(ord(c)<32 or ord(c)==127 for c in value), 'literal participant name invalid')
    return ' '.join(unicodedata.normalize('NFC',value).split()).casefold()


def _opaque(value):
    return isinstance(value,str) and bool(value) and value==value.strip() and not any(
        ord(c)<32 or ord(c)==127 or (c.isspace() and c!=' ') for c in value)


def _utc(value):
    if isinstance(value,str):
        require(re.fullmatch(bodies.UTC_PATTERN,value), 'provider/native UTC shape invalid')
        try:
            value=datetime.strptime(value[:19].replace(' ','T'),'%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc) + timedelta(
                microseconds=int((re.search(r'\.(\d+)',value).group(1)+'000000')[:6]) if '.' in value else 0)
        except (ValueError,OverflowError) as error:
            raise AuditBlocked('provider/native UTC calendar invalid') from error
    require(isinstance(value,datetime) and value.tzinfo is not None and value.utcoffset().total_seconds()==0
        and 0<=value.timestamp()<253402300800, 'provider/native UTC value invalid')
    return value.astimezone(timezone.utc)


def _historical_source(snapshot, expected_head, required):
    require(re.fullmatch('[0-9a-f]{40}',expected_head or '') and snapshot.get('head')==expected_head
        and type(snapshot.get('files')) is dict and set(required)<=set(snapshot['files']), 'explicit historical sports source inventory required')
    require(subprocess.check_output(['git','rev-parse',expected_head+'^{commit}'],cwd=REPO,text=True).strip()==expected_head,'historical sports commit differs')
    for relative,hashed in snapshot['files'].items():
        bodies._relative_path(relative)
        require(re.fullmatch('[0-9a-f]{64}',hashed or ''),'historical source SHA invalid')
        blob=subprocess.check_output(['git','show',expected_head+':'+relative],cwd=REPO)
        require(len(blob)<=2_000_000 and hashlib.sha256(blob).hexdigest()==hashed,'historical sports source blob differs: '+relative)
    return {'head':expected_head,'files':len(snapshot['files']),'proof_mode':'git_show_each_explicit_historical_commit_blob'}


class Bound:
    """Existing independent body verifier plus explicit sports byte ceilings."""
    def __init__(self):
        self.binder=bodies.Binder()
        self.stats={}

    def absolute(self,record,*,parquet=False,maximum=200_000_000):
        require(type(record) is dict and type(record.get('bytes')) is int and 0<record['bytes']<=maximum,'bounded sports body identity required')
        path=Path(record.get('path',''))
        require(path.is_absolute() and path.resolve(strict=True)==path and path.is_relative_to(DATA_ROOT),'canonical explicit sports source path required')
        previous=self.binder.records.get(str(path))
        planned=sum(item['bytes'] for item in self.binder.records.values())+(0 if previous else record['bytes'])
        require(planned<=MAX_BOUND_BYTES,'total sports source-body ceiling exceeded before read')
        self.binder.file(path.parent,{**record,'path':path.name},parquet=parquet or ('rows' in record and 'schema' in record))
        self.stats[str(path)]=bodies.identity(path)
        return path

    def relative(self,root,record,*,parquet=False,maximum=200_000_000):
        path=bodies.child(Path(root),record.get('path'))
        return self.absolute({**record,'path':str(path)},parquet=parquet,maximum=maximum)

    def json(self,record,*,maximum=16_000_000):
        path=self.absolute(record,maximum=maximum)
        value=_decode(path.read_bytes())
        require(type(value) is dict,'sports JSON object required')
        return value

    def check_drift(self):
        require(all(bodies.identity(Path(path))==stat for path,stat in self.stats.items()),'sports audit source body drift')

    def records(self):
        return list(self.binder.records.values())


def _json_record(path,sha):
    path=Path(path)
    require(path.is_file() and not path.is_symlink() and 0<path.stat().st_size<=16_000_000,'bounded regular sports manifest required')
    return {'path':str(path),'bytes':path.stat().st_size,'sha256':sha}


def _bound_core_json(bound,record):
    """Reopen the unchanged shared-reader identity saved by the producer.

    The sports producer uses io.read_json's path/sha256/stat record, not the
    auditor's flat path/bytes/sha256 body record. Normalize only at this read
    boundary, retaining every original filesystem identity check.
    """
    require(type(record) is dict and set(record)=={'path','sha256','stat'},
        'producer core JSON binding shape differs')
    saved=record['stat'];fields=('device','inode','bytes','mtime_ns','ctime_ns')
    require(type(saved) is dict and set(saved)==set(fields)
        and all(type(saved[name]) is int and saved[name]>=0 for name in fields)
        and saved['inode']>0 and 0<saved['bytes']<=16_000_000,
        'producer core JSON stat identity invalid')
    require(isinstance(record['path'],str) and isinstance(record['sha256'],str)
        and re.fullmatch('[0-9a-f]{64}',record['sha256']), 'producer core JSON path/hash identity invalid')
    path=Path(record['path'])
    require(path.is_absolute() and path.resolve(strict=True)==path and path.is_relative_to(DATA_ROOT)
        and path.is_file() and not path.is_symlink(), 'canonical producer core JSON path required')
    expected=tuple(saved[name] for name in fields)
    require(bodies.identity(path)==expected,'producer core JSON recorded stat identity differs')
    value=bound.json({'path':record['path'],'bytes':saved['bytes'],'sha256':record['sha256']})
    require(bodies.identity(path)==expected,'producer core JSON stat drift during reopen')
    return value


def _rows(con,path,columns='*',where='TRUE'):
    query='SELECT '+columns+' FROM read_parquet('+literal(path)+',hive_partitioning=false) WHERE '+where
    require(con.execute('SELECT count(*) FROM ('+query+')').fetchone()[0]<=MAX_ROWS,'bounded sports metadata/aggregate row ceiling exceeded')
    cursor=con.execute(query);names=[item[0] for item in cursor.description]
    return [dict(zip(names,row)) for row in cursor.fetchall()]


def _index(rows,key,reason):
    output={}
    for row in rows:
        value=key(row)
        require(value not in output,reason)
        output[value]=row
    return output


def _object(value,kind):
    result=_decode(value) if isinstance(value,str) else value
    require(type(result) is kind,'native structured field type differs')
    return result


def _participant_labels(entity):
    detail=_object(entity['details'],dict)
    return {_name(value) for value in [entity.get('name'),*[detail.get(k) for k in ('market','team_name','short_name')]]
        if isinstance(value,str) and value.strip()}


def _entity_sport(entity,sport):
    detail=_object(entity['details'],dict)
    if sport=='atp':return entity.get('type')=='tennis_competitor' and detail.get('gender')=='male'
    kinds={'mlb':'baseball_team','nfl':'football_team','cfb':'football_team','nba':'basketball_team',
        'wnba':'basketball_team','cbb':'basketball_team','nhl':'hockey_team','epl':'soccer_team'}
    return entity.get('type')==kinds[sport] and (detail.get('league') in (None,'EPL') if sport=='epl' else detail.get('league')==LEAGUES[sport])


def _source_equality(actual,expected,label):
    require(type(actual) is dict and actual==expected,'exact '+label+' primary source membership differs')


def _scoreboard_snapshots(payload,sport,source):
    """Read every literal completed snapshot, including malformed completions.

    The latter are deliberately retained as invalid, never dropped before
    cross-snapshot reconciliation. In-progress snapshots are not outcomes.
    """
    output=[]
    require(type(payload.get('events')) is list,'primary scoreboard events absent')
    for event_index,event in enumerate(payload['events']):
        require(type(event) is dict,'primary scoreboard event malformed')
        groups=event.get('groupings',[]) if sport=='atp' else [event]
        require(type(groups) is list,'primary scoreboard groupings malformed')
        for group_index,group in enumerate(groups):
            require(type(group) is dict,'primary scoreboard grouping malformed')
            if sport=='atp':
                label=group.get('grouping',group)
                require(type(label) is dict,'primary ATP grouping label malformed')
                if str(label.get('name',label.get('displayName',''))).casefold()!="men's singles":continue
            competitions=group.get('competitions',[])
            require(type(competitions) is list,'primary scoreboard competitions malformed')
            for competition_index,competition in enumerate(competitions):
                require(type(competition) is dict,'primary scoreboard competition malformed')
                game=competition.get('id')
                if not _opaque(game):continue
                status=competition.get('status',event.get('status',{}))
                kind=status.get('type',{}) if type(status) is dict else {}
                kind=kind if type(kind) is dict else {}
                pointer=f'events[{event_index}]'+(f'.groupings[{group_index}]' if sport=='atp' else '')+f'.competitions[{competition_index}]'
                row={'sport':sport,'provider_game_id':game,'source_binding':source,'source_pointer':pointer,
                    'completed_status':kind,'participants':[],'completed':kind.get('state')=='post' and kind.get('completed') is True,
                    'reason':None,'winner_participant_id':None,'is_draw':False}
                competitors=competition.get('competitors')
                if type(competitors) is not list or len(competitors)!=2:row['reason']='provider_result_participant_pair_missing'
                else:
                    for index,competitor in enumerate(competitors):
                        if type(competitor) is not dict:row['reason']='provider_result_participant_object_invalid';break
                        person=competitor.get('team',competitor.get('athlete',competitor))
                        if type(person) is not dict:row['reason']='provider_result_participant_object_invalid';break
                        pid=person.get('id',competitor.get('id'))
                        labels={_name(person[k]):person[k] for k in ('displayName','fullName','name','shortName','shortDisplayName','location','nickname','abbreviation')
                            if isinstance(person.get(k),str) and person[k].strip()}
                        winner=competitor.get('winner');score=competitor.get('score')
                        if type(score) is dict:score=score.get('value',score.get('displayValue'))
                        if type(pid) not in (str,int) or not _opaque(str(pid)) or not labels:row['reason']='provider_result_participant_identity_missing';break
                        row['participants'].append({'id':str(pid),'name_options':list(labels.values()),'winner':winner,'score':score,
                            'source_pointer':pointer+f'.competitors[{index}]'})
                people=row['participants'];winners=[p['id'] for p in people if p.get('winner') is True]
                draw=sport=='epl' and len(people)==2 and all(p.get('winner') is False for p in people) and all(
                    re.fullmatch('[0-9]+',str(p.get('score'))) for p in people) and int(people[0]['score'])==int(people[1]['score'])
                if row['reason'] is None:
                    if len({p['id'] for p in people})!=2:row['reason']='provider_result_duplicate_participant_id'
                    elif not row['completed']:row['reason']='provider_result_not_completed'
                    elif not all(type(p['winner']) is bool for p in people):row['reason']='provider_result_winner_flags_missing'
                    elif len(winners)==1:row['winner_participant_id']=winners[0]
                    elif draw:row['is_draw']=True
                    else:row['reason']='provider_result_winner_or_draw_ambiguous'
                row['invalid']=row['reason'] is not None
                output.append(row)
    return output


def _reconcile_snapshots(snapshots):
    snapshots=[row for row in snapshots if row.get('completed',True)]
    require(snapshots and not any(row['invalid'] for row in snapshots),'malformed completed primary result snapshot cannot be discarded')
    signatures={(tuple(sorted((p['id'],str(p.get('score'))) for p in row['participants'])),row['winner_participant_id'],row['is_draw']) for row in snapshots}
    require(len(signatures)==1,'completed primary result snapshots conflict')
    first=dict(snapshots[0]);labels=defaultdict(set)
    for row in snapshots:
        for person in row['participants']:labels[person['id']].update(_name(value) for value in person['name_options'])
    first['labels']=dict(labels)
    return first


def _expected_provider_result(snapshots):
    """Independent full reconciliation record, including failed/incomplete proof."""
    if not snapshots:return None
    complete=[row for row in snapshots if row.get('completed') is True]
    fields=('source_binding','source_pointer','completed_status','reason','participants','winner_participant_id','is_draw')
    evidence=[{field:row.get(field) for field in fields} for row in complete]
    ignored=[{field:row.get(field) for field in fields[:4]} for row in snapshots if row.get('completed') is not True]
    for row in ignored:row['reason']=row['reason'] or 'snapshot_not_completed'
    review={'completed_source_snapshots':evidence,'ignored_incomplete_source_snapshots':ignored}
    key={field:snapshots[0][field] for field in ('sport','provider_game_id')}
    if not complete:return {**key,'completed':False,'reason':'no_unambiguous_completed_provider_result_snapshot',**review}
    if any(row.get('reason') is not None for row in complete):
        return {**key,'completed':False,'reason':'contradictory_or_ambiguous_completed_provider_result_snapshot',**review}
    signatures={(tuple(sorted((p['id'],str(p.get('score'))) for p in row['participants'])),row.get('winner_participant_id'),row.get('is_draw')) for row in complete}
    if len(signatures)!=1:return {**key,'completed':False,'reason':'conflicting_completed_provider_result_snapshots',**review}
    first={field:value for field,value in complete[0].items() if field!='invalid'}
    people=[]
    for person in first['participants']:
        labels={_name(label):label for row in complete for candidate in row['participants'] if candidate['id']==person['id'] for label in candidate['name_options']}
        people.append({**person,'name_options':list(labels.values())})
    return {**first,'participants':people,**review}


def _moneyline_snapshot(game):
    return {'sport':game['sport'],'provider_game_id':game['game_id'],'completed':True,'reason':None,'is_draw':False,
        'winner_participant_id':game['winner'],'participants':[{'id':pid,'name_options':[name]} for pid,name in zip(game['ids'],game['names'])],
        'completed_status':'retained audited finalized provider moneyline identity/result/timing contract',
        'source_binding':game['source'],'source_pointer':('game_pk=' if game['sport']=='mlb' else 'game_id=')+game['game_id']}


def _native_proposition(market,people_by_native,sport):
    """Independent full-game grammar: no period/set/points proposition fallback."""
    title,label=market.get('title'),market.get('yes_sub_title')
    require(isinstance(title,str) and isinstance(label,str) and label and label==label.strip(),'native named-YES primary fields invalid')
    draw=sport=='epl' and _name(label) in ('tie','draw')
    matches=[key for key,labels in people_by_native.items() if _name(label) in labels]
    require(draw or len(matches)==1,'native YES literal does not identify one proved participant')
    target=matches[0] if not draw else None
    def named(value):return [key for key,labels in people_by_native.items() if _name(value) in labels]
    pair_title=re.sub(r'^Basketball Play-In: ','',re.sub(r'^Game [0-9]+: ','',title))
    if pair_title.endswith(' Winner?'):
        pair_title=re.sub(r'(?:: Game [0-9]+)? Winner\?$','',pair_title)
        pairs=[pair_title.split(separator) for separator in (' at ',' vs ') if separator in pair_title]
        require(len(pairs)==1 and len(pairs[0])==2,'native full-game pair title ambiguous')
        ids=[named(value.strip()) for value in pairs[0]]
        require(all(len(values)==1 for values in ids) and len({values[0] for values in ids})==2,'native winner pair title disagrees with participant proof')
    elif draw:
        require(title=='Tie is the result','native draw proposition title unsupported')
    else:
        captured=None
        if title.endswith(' wins'):captured=title[:-5]
        elif sport=='atp':
            require(not re.search(r'\b(set|sets|game|games|point|points|tiebreak|half|quarter)\b',title,re.IGNORECASE),
                    'ATP sub-match/period proposition is not a full-game winner')
            question=re.fullmatch(r'Will (.+?) win the .+ match\?',title)
            round_question=re.fullmatch(r'Will (.+?) be a winner of the round of [0-9]+ of .+ Singles\?',title)
            captured=(question or round_question).group(1) if question or round_question else None
        require(captured is not None and named(captured)==[target],'native full-game named winner title unsupported or contradictory')
    return 'draw' if draw else 'named_participant',target


def _provider_catalog(con,config,files,bound):
    binding=bound.json(files['provider_binding'])
    require(binding.get('schema_version')=='kaushik_replication_sports_binding_v1' and binding.get('provider_cohort_admitted') is True
        and binding.get('retained_identity_result_timing_proof') is True,'reviewed retained provider binding required')
    roles=_index(binding.get('inputs',[]),lambda r:r['role'],'duplicate retained provider role')
    require({'six_timing','six_proof','nfl_moneylines','nba_moneylines','mlb_phase'}<=set(roles),'retained provider source roles missing')
    for role in ('six_timing','six_proof','nfl_moneylines','nba_moneylines','mlb_phase'):
        item=roles[role]
        path=Path(item['path'])
        bound.absolute({**item,'bytes':path.stat().st_size},maximum=200_000_000)
    games={}
    def add(row):
        key=(row['sport'],row['game_id'])
        require(key not in games,'duplicate primary retained sport/game identity')
        require(len(row['names'])==2 and len({_name(n) for n in row['names']})==2,'primary retained participant pair invalid')
        try:
            start,end=_utc(row['start']),_utc(row['end']);scheduled=_utc(row['scheduled']) if row['scheduled'] is not None else start
            row.update(start=start,end=end,scheduled=scheduled,valid=end>start)
            day=row.get('market_date')
            if type(day) is date:row['valid'] &= abs((start.date()-day).days)<=1
            elif isinstance(day,str) and re.fullmatch('[0-9]{4}-[0-9]{2}-[0-9]{2}',day):
                row['valid'] &= abs((start.date()-date.fromisoformat(day)).days)<=1
        except AuditBlocked:row['valid']=False
        games[key]=row
    timings=_rows(con,roles['six_timing']['path'],'sport,event_slug,game_id,market_date,actual_start_utc,actual_end_utc,timing_quality',where="sport IN('atp','cbb','cfb','epl','nhl','wnba')")
    proofs=_index(_rows(con,roles['six_proof']['path'],'sport,event_slug,provider_competition_id,provider_start_utc,provider_participants,provider_event_name',where="eligible AND sport IN('atp','cbb','cfb','epl','nhl','wnba')"),lambda r:(r['sport'],r['event_slug']),'retained timing/identity join fanout')
    for row in timings:
        proof=proofs.get((row['sport'],row['event_slug']))
        require(proof is not None and proof['provider_competition_id']==row['game_id'],'retained timing/source ID proof differs')
        quality=row.get('timing_quality')
        require(isinstance(quality,str) and quality.strip(),'six-sport raw provider timing quality missing')
        names=[v.strip() for v in proof['provider_participants'].split('|')]
        oriented=proof['provider_event_name'].split(' at ') if isinstance(proof.get('provider_event_name'),str) else []
        oriented=oriented if len(oriented)==2 and {_name(v) for v in oriented}=={_name(v) for v in names} else [None,None]
        add({'sport':row['sport'],'game_id':row['game_id'],'names':names,'away_name':oriented[0],'home_name':oriented[1],
            'start':row['actual_start_utc'],'end':row['actual_end_utc'],'scheduled':proof['provider_start_utc'],'market_date':row.get('market_date'),
            'quality':quality,'convention':'scheduled_start_plus_archived_duration' if row['sport']=='atp' else 'retained_provider_live_play_boundaries',
            'sources':{'timing':bound.binder.records[str(Path(roles['six_timing']['path']))],'identity':bound.binder.records[str(Path(roles['six_proof']['path']))]},
            'raw_sources':{'timing':{**roles['six_timing'],'bytes':Path(roles['six_timing']['path']).stat().st_size},
                           'identity':{**roles['six_proof'],'bytes':Path(roles['six_proof']['path']).stat().st_size}}})
    mlb_path=bound.absolute(files['mlb_results'])
    outcome_fields='official_date,away_team_id,away_team_name,home_team_id,home_team_name,winning_team_id,scheduled_start_utc,actual_start_utc,actual_end_utc'
    mlb=_index(_rows(con,mlb_path,'game_pk,'+outcome_fields),lambda r:str(r['game_pk']),'duplicate primary MLB outcome ID')
    retained=_rows(con,roles['mlb_phase']['path'],'DISTINCT market_id,game_pk,official_date,away_team_id,away_team_name,home_team_id,home_team_name,actual_start_utc,actual_end_utc')
    require(len(retained)==len({r['game_pk'] for r in retained})==len({r['market_id'] for r in retained})==MLB_GAMES,'retained MLB nine-column subset proof differs')
    for sport,role in (('mlb','mlb_phase'),('nfl','nfl_moneylines'),('nba','nba_moneylines')):
        source=files['mlb_results'] if sport=='mlb' else {**roles[role],'bytes':Path(roles[role]['path']).stat().st_size}
        rows=[]
        if sport=='mlb':
            for old in retained:
                row=mlb.get(str(old['game_pk']))
                require(row is not None and all(row[k]==v for k,v in old.items() if k!='market_id'),'retained MLB primary subset fields differ')
                rows.append(row)
        else:rows=_rows(con,source['path'],'game_id,'+outcome_fields)
        for row in rows:
            require(all(type(row[k]) in (str,int) for k in ('away_team_id','home_team_id','winning_team_id')),'literal primary moneyline participant IDs invalid')
            game_id=str(row['game_pk'] if sport=='mlb' else row['game_id'])
            ids=[str(row['away_team_id']),str(row['home_team_id'])];winner=str(row['winning_team_id'])
            require(len(set(ids))==2 and winner in ids and all(_opaque(value) for value in ids),'primary moneyline winner/participant identities invalid')
            add({'sport':sport,'game_id':game_id,'names':[row['away_team_name'],row['home_team_name']],
                'away_name':row['away_team_name'],'home_name':row['home_team_name'],
                'ids':ids,'winner':winner,'start':row['actual_start_utc'],'end':row['actual_end_utc'],'scheduled':row['scheduled_start_utc'],
                'quality':None,'convention':'retained_provider_live_play_boundaries','source':source,
                'raw_sources':{'identity_result_timing':source,**({'retained_subset_nine_metadata_columns':{**roles[role],'bytes':Path(roles[role]['path']).stat().st_size}} if sport=='mlb' else {})}})
    return games


def audit_map(con,*,manifest_path,manifest_sha256,expected_producer_head):
    """Verify the map against native identities and retained provider bodies."""
    bound=Bound();root=Path(manifest_path).parent
    require(not (root/'failure.json').exists(),'failed sports map cannot be audited')
    manifest=bound.json(_json_record(manifest_path,manifest_sha256))
    require(manifest.get('schema_version')=='kaushik_kalshi_sports_map_v1' and manifest.get('status')=='sports_map_complete_pending_independent_audit','pending sports map contract required')
    producer=_historical_source(manifest.get('source',{}),expected_producer_head,['analysis/kaushik_kalshi_replication/sports_binding.py'])
    config=bound.json(manifest['input_config'],maximum=1_000_000);files=config.get('files',{})
    require(config.get('schema_version')=='kaushik_kalshi_sports_map_build_config_v1' and config.get('status')=='reviewed'
        and set(files)=={'metadata_manifest','native_events','native_milestones','native_entities','provider_binding','provider_index','mlb_results','alias_receipt'},'exact reviewed sports source configuration required')
    require(manifest.get('input_metadata')==files['metadata_manifest'] and manifest.get('timing_qualification')==config.get('timing_qualification')
        and isinstance(config.get('timing_qualification'),str) and config['timing_qualification'].strip(),'native metadata/clock qualification binding differs')
    source_bindings=manifest.get('input_bindings',{})
    require(type(source_bindings) is dict and source_bindings,'producer source-body inventory required')
    for path,record in source_bindings.items():
        require(record.get('path')==path,'producer source inventory path differs')
        bound.absolute(record,maximum=200_000_000)
    metadata=bound.json(files['metadata_manifest'])
    require(metadata.get('schema_version')=='kaushik_kalshi_metadata_stage_v1' and metadata.get('status')=='metadata_complete_pending_independent_audit'
        and metadata.get('counts')=={'source_rows':METADATA_ROWS,'source_shards':METADATA_FILES},'full native metadata inventory required')
    metadata_source=_historical_source(metadata.get('source',{}),config.get('metadata_source_head'),['analysis/kaushik_kalshi_replication/metadata.py'])
    metadata_root=Path(files['metadata_manifest']['path']).parent
    proof_record=metadata.get('global_metadata_proof',{});proof_path=bound.relative(metadata_root,proof_record)
    proof=bound.json({**proof_record,'path':str(proof_path)});health=proof.get('health',{})
    require(proof.get('status')=='structural_global_metadata_keys_passed' and proof.get('source')==metadata['source']
        and health.get('rows')==health.get('ticker_groups')==health.get('nonnull_ticker_groups')==METADATA_ROWS
        and all(health.get(k)==0 for k in ('duplicate_ticker_keys','invalid_ticker_rows','conflicting_result_keys','conflicting_status_keys')),'native metadata global key proof differs')
    shards=health.get('expected_source_shards',[])
    require(len(shards)==len(set(shards))==METADATA_FILES and set(shards)==set(health.get('observed_source_shards',[])),'complete native sports metadata shard proof required')
    require(set(metadata.get('artifacts',{}))=={role+'_'+Path(shard).stem for shard in shards for role in ('metadata','sports','exclusions')},'complete metadata role inventory differs')
    markets,market_sources={},{}
    for shard in shards:
        item=metadata['artifacts']['sports_'+Path(shard).stem]
        require(item.get('path')=='artifacts/sports/'+Path(shard).name,'native sports metadata source path differs')
        path=bound.relative(metadata_root,item,parquet=True)
        for row in _rows(con,path):
            ticker=row.get('ticker');require(_opaque(ticker) and ticker not in markets,'native sports ticker duplicate/invalid')
            markets[ticker]=row;market_sources[ticker]={**item,'path':str(path)}
    events_path=bound.absolute(files['native_events']);milestones_path=bound.absolute(files['native_milestones']);entities_path=bound.absolute(files['native_entities'])
    events=_index(_rows(con,events_path,'event_ticker,series_ticker,title,sub_title',where='series_ticker IN('+','.join(literal(k) for k in SERIES)+')'),lambda r:r['event_ticker'],'native event join fanout')
    milestones=_rows(con,milestones_path,'id,primary_event_tickers,title,details,start_date');links=defaultdict(list)
    require(len({r['id'] for r in milestones})==len(milestones),'native milestone ID duplicated')
    for row in milestones:
        primary=_object(row['primary_event_tickers'],list)
        for event in primary:links[event].append((row,primary))
    entities=_index(_rows(con,entities_path,'id,name,type,details'),lambda r:r['id'],'native participant entity ID duplicated')
    games=_provider_catalog(con,config,files,bound)
    alias_review=bound.json(files['alias_receipt'],maximum=1_000_000)
    require(alias_review.get('schema_version')=='kaushik_kalshi_sports_alias_semantic_review_v1' and alias_review.get('status')=='root_semantic_review_recorded_pending_field_reopen','explicit reviewed alias source required')
    aliases={};reverse={};alias_targets={}
    for row in alias_review.get('approved_aliases',[]):
        key=(row.get('sport'),row.get('native_entity_id'))
        require(row.get('reviewed') is True and _opaque(row.get('review_id')) and row.get('evidence') and key[1] in entities,'reviewed alias identity absent')
        require(_name(row['native_name']) in _participant_labels(entities[key[1]]),'alias native literal field absent')
        native_source=row.get('native_source',{});provider_source=row.get('provider_source',{})
        require(native_source.get('path')==files['native_entities']['path'] and native_source.get('sha256')==files['native_entities']['sha256']
            and provider_source.get('path') in source_bindings and provider_source.get('sha256')==source_bindings[provider_source['path']]['sha256'],'reviewed alias source fingerprint differs')
        for source in (native_source,provider_source):
            require(type(source.get('fields')) is list and source['fields'] and all(isinstance(f,str) and f for f in source['fields']),'reviewed alias source fields absent')
        target=_name(row['provider_name'])
        require(any(g['sport']==key[0] and target in {_name(n) for n in g['names']} for g in games.values()),'alias provider literal field absent')
        require(key not in alias_targets or alias_targets[key]==target,'alias native UUID has conflicting provider name')
        require((key[0],target) not in reverse or reverse[(key[0],target)]==key[1],'alias provider name is not bijective')
        alias_key=(*key,_name(row['native_name']))
        require(alias_key not in aliases,'duplicate reviewed alias native literal')
        aliases[alias_key]=target;alias_targets[key]=target;reverse[(key[0],target)]=key[1]
    def entity_options(sport,key):
        return {aliases.get((sport,key,label),label) for label in _participant_labels(entities[key])}
    native_by_day=defaultdict(list)
    for event in events.values():
        found=links[event['event_ticker']]
        if len(found)!=1:continue
        milestone,primary=found[0]
        if len(primary)!=len(set(primary)):continue
        detail=_object(milestone['details'],dict);sport=SERIES[event['series_ticker']]
        valid_league=(detail.get('tour')=='ATP' and detail.get('gender')=='men') if sport=='atp' else detail.get('league') in (('EPL','English Premier') if sport=='epl' else (LEAGUES[sport],))
        ids=[detail.get('first_competitor_id'),detail.get('second_competitor_id')] if sport=='atp' else [detail.get('away_team_id'),detail.get('home_team_id')]
        title=milestone.get('title','')
        pairs=[title.split(separator) for separator in (' at ',' vs ') if isinstance(title,str) and separator in title]
        if (not valid_league or len(set(ids))!=2 or not all(k in entities and _entity_sport(entities[k],sport) for k in ids)
            or len(pairs)!=1 or len(pairs[0])!=2 or not all(value.strip() for value in pairs[0])):continue
        try:day=_utc(milestone.get('start_date')).date()
        except AuditBlocked:continue
        native_by_day[(sport,day)].append((event['event_ticker'],[entity_options(sport,k) for k in ids]))
    artifact_names={'market_map':'sports_market_map.parquet','binding_proofs':'sports_binding_proofs.parquet','exclusions':'sports_binding_exclusions.parquet'}
    require(set(manifest.get('artifacts',{}))==set(artifact_names),'sports map artifact roles differ')
    artifacts={}
    for role,name in artifact_names.items():
        record=manifest['artifacts'][role];require(record.get('path')==name,'sports map artifact path differs')
        path=bound.relative(root,record,parquet=True,maximum=1_000_000_000)
        fields=({'ticker':'string','sport':'string','game_key':'string','actual_start_seconds':'double','actual_end_seconds':'double','timing_quality':'string',
            'provenance':'string','yes_payout':'integer','native_result':'string','native_yes_result_verified':'bool','native_event_ticker':'string',
            'provider_game_id':'string','identity_receipt_sha256':'string','result_receipt_sha256':'string'} if role=='market_map' else
            {key:'string' for key in (('ticker','identity_receipt_json','identity_receipt_sha256','result_receipt_json','result_receipt_sha256') if role=='binding_proofs' else
                ('ticker','native_event_ticker','sport','reason','provider_result_receipt_json','provider_result_receipt_sha256'))})
        _schema(path,fields)
        artifacts[role]=_rows(con,path)
    mapped=_index(artifacts['market_map'],lambda r:r['ticker'],'duplicate mapped native ticker')
    receipts=_index(artifacts['binding_proofs'],lambda r:r['ticker'],'duplicate market proof ticker')
    excluded=_index(artifacts['exclusions'],lambda r:r['ticker'],'duplicate excluded native ticker')
    require(set(mapped)==set(receipts) and not set(mapped)&set(excluded) and set(mapped)|set(excluded)==set(markets),'sports map/proof/exclusion source partition differs')
    require(manifest.get('counts')=={'source_markets':len(markets),'mapped_markets':len(mapped),'excluded_markets':len(excluded)},'sports map body/count conservation differs')
    failure_receipts={}
    for ticker,row in excluded.items():
        require(row.get('native_event_ticker')==markets[ticker].get('event_ticker') and row.get('sport')==markets[ticker].get('sport')
            and isinstance(row.get('reason'),str) and row['reason'],'sports exclusion native membership/reason missing')
        encoded,hashed=row.get('provider_result_receipt_json'),row.get('provider_result_receipt_sha256')
        if row['reason'] not in PROVIDER_FAILURE_REASONS:
            require(encoded is None and hashed is None,'unrelated exclusion must have paired-null provider failure receipt')
            continue
        require(isinstance(encoded,str) and re.fullmatch('[0-9a-f]{64}',hashed or '') and hashlib.sha256(encoded.encode()).hexdigest()==hashed,
            'provider failure canonical receipt/SHA pair required')
        receipt=_decode(encoded)
        require(type(receipt) is dict and set(receipt)=={'exclusion_reason','sport','provider_game_id','provider_result'} and _canonical(receipt)==encoded
            and receipt['exclusion_reason']==row['reason'] and receipt['sport']==row['sport'] and _opaque(receipt['provider_game_id']),
            'provider failure canonical receipt identity differs')
        failure_receipts[ticker]=receipt
    snapshots=defaultdict(list);index=bound.json(files['provider_index'],maximum=5_000_000)
    entries=[r for r in index.get('files',[]) if len(r.get('path','').split('/'))==3 and r['path'].split('/')[0]=='scoreboards' and r['path'].split('/')[1] in set(SPORTS)-{'mlb','nfl','nba'}]
    require(entries and len({r['path'] for r in entries})==len(entries) and sum(r['bytes'] for r in entries)<=1_000_000_000,'bounded complete scoreboard inventory required')
    cache=Path(config.get('provider_cache_root',''))
    require(cache.is_absolute() and cache.resolve(strict=True)==cache and cache.is_relative_to(DATA_ROOT),'canonical saved scoreboard cache required')
    for item in entries:
        bodies._relative_path(item['path']);source={**item,'path':str(cache/item['path'])}
        payload=bound.json(source,maximum=5_000_000)
        for row in _scoreboard_snapshots(payload,item['path'].split('/')[1],source):
            snapshots[(row['sport'],row['provider_game_id'])].append(row)
    for ticker,receipt in failure_receipts.items():
        market=markets[ticker];game_key=(receipt['sport'],receipt['provider_game_id']);game=games.get(game_key)
        require(game is not None and game.get('valid') is True,'provider failure retained game identity/clock absent')
        native_options=[pair for event,pair in native_by_day[(game_key[0],game['scheduled'].date())] if event==market['event_ticker']]
        names=[_name(value) for value in game['names']]
        require(len(native_options)==1 and sum(all(names[order[i]] in native_options[0][i] for i in range(2)) for order in ((0,1),(1,0)))==1,
            'provider failure native UUID/retained pair/date membership differs')
        primary=[_moneyline_snapshot(game)] if game_key[0] in ('mlb','nfl','nba') else snapshots[game_key]
        expected=_expected_provider_result(primary)
        require(receipt['provider_result']==expected,'provider failure full completed/incomplete snapshots differ from bound primary bodies')
        if receipt['exclusion_reason']=='independent_provider_completed_result_missing':
            require(expected is None or expected.get('completed') is not True or expected.get('reason') is not None,
                'provider missing/blocked exclusion contradicts valid completed primary result')
    native_game_members={};clock_members={};qualities=defaultdict(int)
    for ticker,row in mapped.items():
        require(all(value is not None for value in row.values()) and all(_opaque(row.get(k)) for k in ('ticker','game_key','native_event_ticker','provider_game_id'))
            and all(re.fullmatch('[0-9a-f]{64}',row.get(k,'')) for k in ('identity_receipt_sha256','result_receipt_sha256')),'mapped native proof required fields invalid')
        market=markets[ticker];receipt=receipts[ticker];event=events.get(market.get('event_ticker'))
        require(event is not None and SERIES[event['series_ticker']]==market.get('sport')==row.get('sport'),'native winner-series sport/event membership differs')
        require(market.get('market_reason')=='eligible' and market.get('status')=='finalized' and market.get('market_type')=='binary'
            and market.get('combo_state')=='single' and market.get('unit_payoff_admitted') is True and market.get('face_value_dollars')==1
            and market.get('result') in ('yes','no'),'native final binary single payout source not admitted')
        require(len(links[event['event_ticker']])==1,'native primary milestone identity not unique')
        milestone,primary=links[event['event_ticker']][0]
        require(len(primary)==len(set(primary)),'duplicate native primary milestone link')
        detail=_object(milestone['details'],dict);sport=row['sport']
        require((detail.get('tour')=='ATP' and detail.get('gender')=='men') if sport=='atp' else
            detail.get('league') in (('EPL','English Premier') if sport=='epl' else (LEAGUES[sport],)),'native milestone league/tour differs')
        ids=[detail.get('first_competitor_id'),detail.get('second_competitor_id')] if sport=='atp' else [detail.get('away_team_id'),detail.get('home_team_id')]
        require(len(set(ids))==2 and all(_opaque(k) and k in entities for k in ids),'native UUID participant pair unproved')
        require(all(_entity_sport(entities[key],sport) for key in ids),'native UUID participant sport/league differs')
        for name in ('identity','result'):
            encoded=receipt.get(name+'_receipt_json');hashed=receipt.get(name+'_receipt_sha256')
            require(isinstance(encoded,str) and hashlib.sha256(encoded.encode()).hexdigest()==hashed==row.get(name+'_receipt_sha256'),'canonical per-market proof fingerprint differs')
            parsed=_decode(encoded);require(type(parsed) is dict and _canonical(parsed)==encoded,'canonical proof JSON encoding differs')
        identity=_decode(receipt['identity_receipt_json']);result_receipt=_decode(receipt['result_receipt_json'])
        game_key=(sport,row.get('provider_game_id'));game=games.get(game_key)
        require(game is not None and game.get('valid') is True and game['end']>game['start'],'mapped primary provider game clocks absent/invalid')
        require(_utc(milestone['start_date']).date()==game['scheduled'].date(),'native/provider scheduled UTC date differs')
        require(row.get('native_event_ticker')==event['event_ticker'] and row.get('game_key')=='provider:'+sport+':'+game['game_id'],'mapped exact game/event key differs')
        display_quality=game['convention']+(' | provider_quality='+game['quality'] if game['quality'] is not None else '')
        require(row.get('actual_start_seconds')==game['start'].timestamp() and row.get('actual_end_seconds')==game['end'].timestamp()
            and row.get('timing_quality')==display_quality,'retained provider clock values/convention differ')
        require(identity.get('event_ticker')==event['event_ticker'] and identity.get('sport')==sport and identity.get('provider_game_id')==game['game_id']
            and identity.get('milestone_id')==milestone['id'] and identity.get('match_utc_date')==game['scheduled'].date().isoformat()
            and _utc(identity.get('actual_start_utc'))==game['start'] and _utc(identity.get('actual_end_utc'))==game['end']
            and identity.get('timing_qualification')==game['convention'] and identity.get('raw_provider_timing_quality')==game['quality'],'identity clock/date/native proof differs')
        _source_equality(identity.get('provider_source_bindings'),game['raw_sources'],'provider identity/clock')
        _source_equality(identity.get('native_source_bindings'),{role:files[role] for role in ('native_events','native_milestones','native_entities','alias_receipt')},'native identity')
        assignment=identity.get('participant_mapping',[])
        require(len(assignment)==2 and {r.get('native_participant_id') for r in assignment}==set(ids),'receipt UUID participant membership differs')
        mapping={r['native_participant_id']:r['provider_name_key'] for r in assignment}
        require(all(r.get('native_name')==entities[r['native_participant_id']]['name'] for r in assignment),'identity native literal participant name receipt differs')
        options={key:entity_options(sport,key) for key in ids}
        require(len(set(mapping.values()))==2 and set(mapping.values())=={_name(n) for n in game['names']}
            and all(mapping[key] in options[key] for key in ids),'exact or reviewed-alias participant assignment differs')
        native_orientation={'away_name':entities[ids[0]]['name'] if sport!='atp' else None,'home_name':entities[ids[1]]['name'] if sport!='atp' else None,
            'away_native_id':ids[0] if sport!='atp' else None,'home_native_id':ids[1] if sport!='atp' else None}
        provider_orientation={key:game.get(key) for key in ('away_name','home_name')}
        orientation='unknown'
        if sport!='atp' and game.get('away_name') is not None:
            orientation='verified_away_home' if mapping[ids[0]]==_name(game['away_name']) and mapping[ids[1]]==_name(game['home_name']) else 'conflicting_away_home'
        require(identity.get('native_orientation')==native_orientation and identity.get('provider_orientation')==provider_orientation
            and identity.get('orientation')==orientation and identity.get('orientation_conflict') is (orientation=='conflicting_away_home')
            and identity.get('home_away_proposition_admissible') is (orientation=='verified_away_home'),'native/provider orientation and conflict receipt differs')
        choices=[]
        for candidate_key,candidate in games.items():
            if candidate_key[0]!=sport or not candidate.get('valid') or candidate['scheduled'].date()!=game['scheduled'].date():continue
            names=[_name(n) for n in candidate['names']]
            orders=[order for order in ((0,1),(1,0)) if all(names[order[i]] in options[ids[i]] for i in range(2))]
            require(len(orders)<=1,'participant assignment is ambiguous')
            if orders:choices.append(candidate_key)
        require(choices==[game_key],'exact participant/date identity is not unique across retained provider games')
        provider_names=[_name(value) for value in game['names']]
        matching_native=[key for key,options_pair in native_by_day[(sport,game['scheduled'].date())] if any(
            all(provider_names[order[i]] in options_pair[i] for i in range(2)) for order in ((0,1),(1,0)))]
        require(matching_native==[event['event_ticker']],'retained provider game is not one-to-one across all native event candidates')
        require(game_key not in native_game_members or native_game_members[game_key]==event['event_ticker'],'multiple native events map to one retained provider game')
        native_game_members[game_key]=event['event_ticker']
        primary=[_moneyline_snapshot(game)] if sport in ('mlb','nfl','nba') else snapshots[game_key]
        expected_provider_result=_expected_provider_result(primary)
        require(expected_provider_result is not None and expected_provider_result.get('completed') is True and expected_provider_result.get('reason') is None,
            'mapped primary result is incomplete, contradictory or ambiguous')
        require(result_receipt.get('provider_result')==expected_provider_result,'full provider completed/incomplete snapshot receipts differ from bound primary bodies')
        if sport in ('mlb','nfl','nba'):
            result={'winner_participant_id':game['winner'],'is_draw':False,'labels':{pid:{_name(name)} for pid,name in zip(game['ids'],game['names'])}}
            source=game['source'];provider_result=result_receipt.get('provider_result',{})
            _source_equality(provider_result.get('source_binding'),source,'provider moneyline result')
            pointer=('game_pk=' if sport=='mlb' else 'game_id=')+game['game_id']
            require(provider_result.get('source_pointer')==pointer,'provider moneyline primary row pointer differs')
        else:
            result=_reconcile_snapshots(snapshots[game_key]);provider_result=result_receipt.get('provider_result',{})
        require(provider_result.get('completed') is True and provider_result.get('reason') is None and provider_result.get('winner_participant_id')==result['winner_participant_id']
            and provider_result.get('is_draw') is result['is_draw'],'provider winner/draw receipt contradicts primary source fields')
        result_people=_index(provider_result.get('participants',[]),lambda r:r.get('id'),'duplicate result participant ID')
        require(set(result_people)==set(result['labels']) and all({_name(v) for v in person.get('name_options',[])}==result['labels'][pid]
            for pid,person in result_people.items()),'provider result literal participant labels differ')
        native_to_provider={key:next((pid for pid,labels in result['labels'].items() if mapping[key] in labels),None) for key in ids}
        require(None not in native_to_provider.values() and len(set(native_to_provider.values()))==2,'native UUID to provider participant IDs is not bijective')
        labels={key:_participant_labels(entities[key])|result['labels'][native_to_provider[key]] for key in ids}
        kind,target=_native_proposition(market,labels,sport);prop=result_receipt.get('native_yes_proposition',{})
        require(prop.get('field')=='yes_sub_title' and prop.get('text')==market.get('yes_sub_title') and prop.get('native_title')==market.get('title')
            and prop.get('kind')==kind and prop.get('native_participant_id')==target,'native YES proposition receipt differs from literal primary fields')
        _source_equality(prop.get('source_binding'),market_sources[ticker],'native YES metadata shard')
        payout=int(result['is_draw']) if kind=='draw' else int(not result['is_draw'] and result['winner_participant_id']==native_to_provider[target])
        require(type(result_receipt.get('yes_payout')) is int and result_receipt.get('native_ticker')==ticker and result_receipt.get('native_result')==market['result']
            and result_receipt.get('native_to_provider_participant_ids')==native_to_provider and result_receipt.get('yes_payout')==payout
            and row.get('yes_payout')==payout==int(market['result']=='yes') and row.get('native_result')==market['result']
            and row.get('native_yes_result_verified') is True,'native YES payout/provider outcome mismatch')
        provenance=_decode(row.get('provenance',''))
        require(provenance=={'identity_receipt_sha256':row['identity_receipt_sha256'],'result_receipt_sha256':row['result_receipt_sha256'],
            'raw_provider_timing_quality':game['quality']},'raw provider timing quality/proof provenance differs')
        clock=(row['actual_start_seconds'],row['actual_end_seconds'],row['timing_quality'])
        require(game_key not in clock_members or clock_members[game_key]==clock,'same provider game has inconsistent mapped clocks')
        clock_members[game_key]=clock;qualities[(sport,game['convention'],game['quality'])]+=1
    bound.check_drift()
    return {'schema_version':'kaushik_kalshi_sports_map_audit_v1','status':'sports_map_sources_reconciled',
        'manifest_sha256':manifest_sha256,'manifest_path':str(manifest_path),'producer_source':producer,'metadata_source':metadata_source,
        'checks':{key:True for key in MAP_CHECKS},'counts':manifest['counts'],'provider_failure_receipts_reopened':len(failure_receipts),
        'proof_modes':{'provider_clock_conventions_verified':CLOCK_PROOF,'identity':'exact native UUID/literal or explicitly reviewed alias bijection; unique retained sport/pair/UTC date',
            'outcomes':'literal primary moneyline winner IDs or every indexed completed scoreboard snapshot; no candidate-label winner inference',
            'exclusions':'reopened complete disjoint native market inventory and stated reasons; not a maximal possible-match certificate'},
        'clock_coverage':[{'sport':s,'convention':c,'raw_quality':q,'mapped_markets':n} for (s,c,q),n in sorted(qualities.items(),key=lambda x:str(x[0]))],
        'raw_tape_read':False},bound.records()


def _schema(path,fields):
    schema=pq.ParquetFile(path).schema_arrow
    require(set(schema.names)==set(fields),'independent sports artifact field inventory differs')
    for name,kind in fields.items():
        actual=schema.field(name).type
        valid=(pa.types.is_string(actual) or pa.types.is_large_string(actual)) if kind=='string' else (
            pa.types.is_integer(actual) if kind=='integer' else pa.types.is_boolean(actual) if kind=='bool' else pa.types.is_float64(actual))
        require(valid,'independent sports field type differs: '+name)


def _grid_rows(summary):
    phases={(scope,outcome,phase) for scope in SCOPES for outcome in ('payoff_cents','roi_percent') for phase in ('pregame','in_play','in_play_minus_pregame')}
    expected=(('phase_rows',('scope','outcome','phase'),phases),
        ('profile_rows',('scope','outcome','phase','bin'),{(s,o,p,b) for s in SCOPES for o in ('payoff_cents','roi_percent') for p in ('pregame','in_play') for b in range(1,11)}),
        ('window_rows',('scope','outcome','window'),{(s,o,key) for s in SCOPES for o in ('payoff_cents','roi_percent') for _,key,_ in WINDOWS}))
    for role,keys,grid in expected:
        values=summary.get(role,[])
        require(type(values) is list and len(values)==len(grid) and {tuple(row.get(k) for k in keys) for row in values}==grid,'complete unique sports estimate grid differs: '+role)
    expected_windows={key:(panel,index) for index,(panel,key,_) in enumerate(WINDOWS)}
    for row in summary['window_rows']:
        require((row.get('panel'),row.get('window_order'))==expected_windows[row['window']],'sports window panel/order differs')


def _verify_statistics(joint,checks,scores,direct_means):
    """Independent score algebra; preserve the declared binary64 point dot.

    In cancelling multi-cell contrasts fsum point substitution or projecting all
    contrasts in one BLAS matrix product can change roundoff and near-zero-SE
    statistics. Keep individual score matrix-vector and coefficient-vector dots.
    Direct means and scores have already passed their unchanged parity checks.
    """
    numerical.support(joint,checks)
    names=joint['names'];coefficients=joint['estimates'];d=len(names);g=len(scores)
    require([row.get('name') for row in coefficients]==names and not joint.get('contrasts'),'sports coefficient/contrast inventory differs')
    meta=joint['metadata'];cells=meta['cell_names']
    require((meta.get('minimum_observations'),meta.get('minimum_clusters'))==(500,30),'sports joint support floors differ')
    supported=np.array([meta['n_by_cell'][cells[i%len(cells)]]>=500 and meta['cluster_count_by_cell'][cells[i%len(cells)]]>=30 for i in range(d)])
    with np.errstate(over='ignore',invalid='ignore'):
        covariance=scores.T@scores
    factor=g/(g-1) if g>1 else 1.
    for field,multiplier in (('covariance_CR0',1.),('covariance_cluster_count_adjusted',factor)):
        matrix=joint.get(field)
        require(type(matrix) is list and len(matrix)==d and all(type(row) is list and len(row)==d for row in matrix),'sports covariance dimensions differ')
        for i in range(d):
            for j in range(d):
                value=float(covariance[i,j]*multiplier)
                numerical.close(matrix[i][j],value if supported[i] and supported[j] and math.isfinite(value) else None,field)
    points=np.array([row['CR0']['estimate'] if row.get('CR0') is not None else direct_means[i] for i,row in enumerate(coefficients)])
    all_checks=[(row,np.eye(d)[i],None) for i,row in enumerate(coefficients)]+checks
    for row,weights,_ in all_checks:
        used=weights!=0
        if not np.all(supported[used]):
            reasons=sorted({reason for i in np.flatnonzero(used) for reason in coefficients[i]['suppression_reasons']})
            require(row.get('suppressed') is True and row.get('suppression_reasons')==reasons
                and all(row.get(k) is None for k in ('CR0','cluster_count_adjusted','influence')),'unsupported sports contrast fields/reasons differ')
            continue
        projected=scores@weights
        with np.errstate(over='ignore',invalid='ignore'):
            variance=float(projected@projected);point=float(points[used]@weights[used])
        if not math.isfinite(variance) or not math.isfinite(point):
            require(row.get('suppressed') is True and row.get('suppression_reasons')==['nonfinite_estimate_or_cluster_variance']
                and all(row.get(k) is None for k in ('CR0','cluster_count_adjusted','influence')),'nonfinite sports contrast withholding differs')
            continue
        require(row.get('suppressed') is False and row.get('suppression_reasons')==[] and g>1,'finite supported sports estimate was incorrectly withheld')
        numerical.interval(row.get('CR0'),point,variance,'sports CR0 binary64 point dot')
        adjusted=variance*factor
        if math.isfinite(adjusted):
            numerical.interval(row.get('cluster_count_adjusted'),point,adjusted,'sports G/(G-1)')
            require(row.get('supplementary_suppression_reasons')==[],'sports finite adjusted interval withholding differs')
        else:require(row.get('cluster_count_adjusted') is None and row.get('supplementary_suppression_reasons')==['nonfinite_adjusted_cluster_variance'],'sports adjusted overflow withholding differs')
        scale=float(np.max(np.abs(projected)))
        if scale:
            normalized=projected/scale;squared=normalized*normalized;sum2=float(np.sum(squared))
            effective=sum2*sum2/float(np.sum(squared*squared));share=float(np.max(squared))/sum2
        else:effective=share=None
        influence=row.get('influence',{})
        numerical.close(influence.get('effective_clusters'),effective,'sports effective game clusters')
        numerical.close(influence.get('maximum_cluster_variance_share'),share,'sports maximum game variance share')
        flags=[]
        if effective is not None and effective<30:flags.append('effective_clusters_below_30')
        if share is not None and share>.25:flags.append('cluster_variance_share_above_25_percent')
        if effective is None:flags.append('zero_cluster_score_variance')
        require(influence.get('flags')==flags,'sports influence diagnostic flags differ')
    convention=joint.get('variance_convention',{})
    require(convention.get('primary')=='event_cluster_CR0' and convention.get('supplementary')=='event_cluster_CR0_times_G_over_G_minus_1'
        and convention.get('full_N_minus_k_CR1_used') is False,'sports CR0/adjustment declaration differs')
    return len(all_checks)


def _audit_joint(con,root,key,joint,scope,family,summary,artifacts,outputs,reads):
    cells=([f'D{band}:{phase}' for phase in ('pregame','in_play') for band in (1,10)] if family=='phases' else
        [f'D{band}:{phase}' for phase in ('pregame','in_play') for band in range(1,11)] if family=='profiles' else
        [f'D{band}:{window}' for _,window,_ in WINDOWS for band in (1,10)])
    relation='ssa_expected_window' if family=='windows' else 'ssa_expected_phase'
    expression="'D'||bin||':'||"+('clock_window' if family=='windows' else 'phase')
    predicate='TRUE' if scope=='pooled' else 'sport='+literal(scope)
    where=predicate+(' AND bin IN (1,10)' if family!='profiles' else '')
    grouped=con.execute('SELECT game_cluster,'+expression+' cell,sum(n_rows)::BIGINT n,sum(sum_payoff) payoff,sum(sum_roi) roi FROM '+relation+
        ' WHERE '+where+' GROUP BY 1,2 ORDER BY 1,2').fetchall()
    require(len(grouped)<=MAX_ROWS,'independent game/cell row ceiling exceeded')
    ids=sorted({row[0] for row in grouped});lookup={value:index for index,value in enumerate(ids)};cell_index={c:i for i,c in enumerate(cells)}
    g,c=len(ids),len(cells)
    require(g*c*8*8<=MAX_NUMPY_BYTES,'independent sports game-score NumPy ceiling before allocation')
    counts=np.zeros((g,c),dtype=np.int64);sums=np.zeros((g,c,2))
    for game,cell,n,payoff,roi in grouped:
        require(cell in cell_index and type(n) is int and 0<n<=2**53 and math.isfinite(payoff) and math.isfinite(roi),'independent game-cell values invalid')
        counts[lookup[game],cell_index[cell]]=n;sums[lookup[game],cell_index[cell],:]=[payoff,roi]
    ns=counts.sum(axis=0);gs=(counts>0).sum(axis=0)
    means=np.zeros((c,2))
    for i in range(c):
        for target in range(2):
            if ns[i]:means[i,target]=math.fsum(float(v) for v in sums[:,i,target])/int(ns[i])
    scores=np.zeros((g,c*2))
    for target in range(2):
        for i in range(c):
            if ns[i]:scores[:,target*c+i]=(sums[:,i,target]-counts[:,i]*means[i,target])/int(ns[i])
    names=[target+'|'+cell for target in ('payoff_cents','roi_percent') for cell in cells]
    require(joint.get('names')==names,'independent sports coefficient/cell order differs')
    meta=joint.get('metadata',{})
    require(meta.get('cell_names')==cells and meta.get('target_names')==['payoff_cents','roi_percent']
        and meta.get('n_by_cell')==dict(zip(cells,ns.tolist())) and meta.get('cluster_count_by_cell')==dict(zip(cells,gs.tolist()))
        and meta.get('n')==int(ns.sum()) and meta.get('cluster_count')==g,'independent sports mean/support populations differ')
    estimates=joint.get('estimates',[])
    require(len(estimates)==len(names),'sports coefficient inventory differs')
    for i,row in enumerate(estimates):
        if not row.get('suppressed'):numerical.close(row.get('CR0',{}).get('estimate'),float(means[i%c,i//c]),'independent direct game-cell mean')
    artifact=artifacts.get(key,{})
    require(artifact==meta.get('score_artifact') and artifact.get('path')==key+'_scores.parquet','sports joint score role/path differs')
    path=root/artifact['path'];footer=pq.ParquetFile(path)
    require(footer.schema_arrow.names==['cluster_key','projected_score'] and pa.types.is_fixed_size_list(footer.schema_arrow.field('projected_score').type)
        and footer.schema_arrow.field('projected_score').type.list_size==len(names),'independent sports projected-score schema differs')
    offset=0;saved_scores=np.empty_like(scores)
    for batch in footer.iter_batches(batch_size=2048):
        cluster=batch.column(0);values=batch.column(1)
        require(cluster.null_count==values.null_count==values.values.null_count==0,'NULL independent sports score/cluster')
        actual_ids=cluster.to_pylist()
        require(actual_ids==ids[offset:offset+len(batch)],'independent score game identity/order differs')
        actual=values.values.slice(values.offset*len(names),len(batch)*len(names)).to_numpy(zero_copy_only=False).reshape(len(batch),len(names))
        require(np.isfinite(actual).all() and np.allclose(actual,scores[offset:offset+len(batch)],rtol=numerical.RTOL,atol=numerical.ATOL),'independent direct game-centered scores differ')
        saved_scores[offset:offset+len(batch)]=actual
        offset+=len(batch)
    require(offset==g,'independent saved score game count differs')
    profiles=[r for r in summary['profile_rows'] if r['scope']==scope] if family=='profiles' else []
    gaps=[r for r in summary['phase_rows' if family=='phases' else 'window_rows'] if r['scope']==scope] if family!='profiles' else []
    checks=numerical.mean_checks(joint,profiles,gaps,phases=family!='windows',windows=family=='windows')
    info=outputs.get(artifact['path'],{})
    require(all(info.get(k)==artifact.get(k) for k in ('bytes','rows','schema','sha256','row_groups')) and info.get('rows')==g
        and info.get('row_groups')==footer.metadata.num_row_groups and info.get('schema')==str(footer.schema_arrow),'sports score footer/manifest binding differs')
    hashed,_=reads.digest(path,numerical.MAX_SCORE)
    require(hashed==artifact.get('sha256'),'sports score fingerprint differs')
    checked=_verify_statistics(joint,checks,saved_scores,means.T.reshape(-1))
    result={'artifact':artifact['path'],'sha256':hashed,'union_clusters':g,'coefficient_dimension':len(names),'checked_estimates_and_contrasts':checked,
        'mean_and_score_proof':'independent count-weighted primary cache game/cell means and (sum_y_game-n_game*mean)/N_cell',
        'contrast_point_arithmetic':'binary64 dot of already-verified coefficients and individual score matrix-vector projection; no fsum/fused multi-contrast substitution'}
    return result


def audit_game_scores(con,*,candidates_relation,map_relation,summary,root,outputs):
    """Tiny-fixture worker; all real bodies must first pass saved admission.

    Rebuilds game cells from the accepted cache and clock map, not from any
    producer annotation/estimation function. No raw full-tape equality claim.
    """
    root=Path(root);candidates=ident(candidates_relation);mapping=ident(map_relation)
    require(summary.get('schema_version')=='kaushik_kalshi_sports_estimates_v1' and summary.get('status')=='sports_estimates_complete','complete sports estimate schema required')
    require(not con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE 'ssa_%'").fetchone()[0]
        and not con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name LIKE 'ssa_%'").fetchone()[0],'owned sports-audit temporary relation collision')
    bodies._validate_saved_fields(con,candidates_relation,'sports_candidates')
    required=('ticker','sport','game_key','actual_start_seconds','actual_end_seconds','timing_quality','provenance','yes_payout','native_result',
        'native_yes_result_verified','native_event_ticker','provider_game_id','identity_receipt_sha256','result_receipt_sha256')
    require(set(bodies._schema(con,map_relation))==set(required),'independent sports map fields differ')
    bad=[ident(k)+' IS NULL' for k in required]
    bad += ["sport NOT IN("+','.join(literal(s) for s in SPORTS)+')',"yes_payout NOT IN(0,1)","native_result NOT IN('yes','no')",
        "native_yes_result_verified IS DISTINCT FROM true","yes_payout IS DISTINCT FROM (native_result='yes')::INTEGER",
        'NOT isfinite(actual_start_seconds)','NOT isfinite(actual_end_seconds)','actual_start_seconds<0','actual_end_seconds<=actual_start_seconds']
    bad += ['NOT '+bodies._opaque_identifier(ident(k)) for k in ('ticker','game_key','native_event_ticker','provider_game_id')]
    require(con.execute('SELECT count(*) FROM '+mapping+' WHERE '+' OR '.join('('+x+')' for x in bad)).fetchone()[0]==0,'independent accepted map required fields invalid')
    require(con.execute('SELECT count(*) FROM (SELECT ticker FROM '+mapping+' GROUP BY 1 HAVING count(*)<>1)').fetchone()[0]==0,'independent sports map join fanout')
    con.execute('CREATE TEMP VIEW ssa_joined AS SELECT c.*,m.game_key,m.actual_start_seconds,m.actual_end_seconds,m.timing_quality,m.yes_payout,m.native_event_ticker,m.sport map_sport,'+
        "CASE WHEN m.ticker IS NULL THEN 'missing_accepted_map' WHEN c.timestamp>m.actual_end_seconds THEN 'after_actual_end' ELSE 'eligible' END reason "+
        'FROM '+candidates+' c LEFT JOIN '+mapping+' m USING(ticker)')
    require(con.execute("SELECT count(*) FROM ssa_joined WHERE game_key IS NOT NULL AND (sport IS DISTINCT FROM map_sport OR event_cluster IS DISTINCT FROM 'kalshi:event:'||native_event_ticker OR "+
        "Y IS DISTINCT FROM CASE WHEN claim_id='kalshi:claim:'||ticker||':yes' THEN yes_payout ELSE 1-yes_payout END)").fetchone()[0]==0,'independent sports native claim/map payout mismatch')
    con.execute("CREATE TEMP VIEW ssa_base AS SELECT *,timestamp-actual_start_seconds u,actual_end_seconds-timestamp r,'kalshi:sport:'||sport||':game:'||game_key game_cluster,"+
        "CASE WHEN timestamp<actual_start_seconds THEN 'pregame' ELSE 'in_play' END phase,100.0*(Y-P) direct_payoff,100.0*(Y/P-1) direct_roi FROM ssa_joined WHERE reason='eligible'")
    con.execute('CREATE TEMP TABLE ssa_expected_phase AS SELECT sport,game_cluster,phase,bin,count(*)::BIGINT n_rows,sum(direct_payoff)::DOUBLE sum_payoff,sum(direct_roi)::DOUBLE sum_roi,'+
        'sum(abs(direct_payoff)) absolute_payoff,sum(abs(direct_roi)) absolute_roi FROM ssa_base GROUP BY 1,2,3,4')
    values=','.join('('+literal(panel)+','+literal(key)+')' for panel,key,_ in WINDOWS)
    predicates=' OR '.join('(w.clock_window='+literal(key)+' AND ('+predicate+'))' for _,key,predicate in WINDOWS)
    con.execute('CREATE TEMP VIEW ssa_windows AS SELECT b.*,w.* FROM ssa_base b CROSS JOIN (VALUES '+values+') w(clock_panel,clock_window) WHERE '+predicates)
    con.execute('CREATE TEMP TABLE ssa_expected_window AS SELECT sport,game_cluster,clock_panel,clock_window,bin,count(*)::BIGINT n_rows,sum(direct_payoff)::DOUBLE sum_payoff,sum(direct_roi)::DOUBLE sum_roi,'+
        'sum(abs(direct_payoff)) absolute_payoff,sum(abs(direct_roi)) absolute_roi FROM ssa_windows GROUP BY 1,2,3,4,5')
    for family in ('phase','window'):
        require(con.execute('SELECT count(*) FROM ssa_expected_'+family).fetchone()[0]<=MAX_ROWS,'independent sports aggregate row ceiling exceeded')
        record=summary.get('aggregate_artifacts',{}).get(family,{})
        require(record.get('path')==family+'_totals.parquet' and outputs.get('aggregate_'+family)==record,'sports game aggregate manifest binding differs')
        path=bodies.child(root,record['path']);pf=pq.ParquetFile(path)
        fields={'sport':'string','game_cluster':'string','bin':'integer','n_rows':'integer','sum_payoff':'double','sum_roi':'double',
            **({'phase':'string'} if family=='phase' else {'clock_panel':'string','clock_window':'string'})}
        _schema(path,fields)
        require(pf.metadata.num_rows==record.get('rows')<=MAX_ROWS,'sports saved aggregate footer bound differs')
        relation='ssa_saved_'+family;bodies.view(con,relation,[path])
        required_fields=' OR '.join(ident(k)+' IS NULL'+(' OR NOT isfinite('+ident(k)+')' if v=='double' else '') for k,v in fields.items())
        require(con.execute('SELECT count(*) FROM '+relation+' WHERE '+required_fields+' OR n_rows<=0 OR n_rows>9007199254740992 OR bin NOT BETWEEN 1 AND 10').fetchone()[0]==0,'saved game aggregate required fields invalid')
        keys=['sport','game_cluster',*(['phase'] if family=='phase' else ['clock_panel','clock_window']),'bin']
        require(con.execute('SELECT count(*) FROM (SELECT '+','.join(keys)+' FROM '+relation+' GROUP BY ALL HAVING count(*)<>1)').fetchone()[0]==0,'saved game aggregate grain duplicated')
        bodies._same_aggregate(con,'SELECT * FROM ssa_expected_'+family,'SELECT * FROM '+relation,keys,['n_rows'],['sum_payoff','sum_roi'],
            'independent sports saved game-cell means/counts differ',scale_columns={'sum_payoff':'absolute_payoff','sum_roi':'absolute_roi'})
    candidate_rows=con.execute('SELECT count(*) FROM '+candidates).fetchone()[0]
    exclusions=[{'sport':s,'reason':r,'rows':n} for s,r,n in con.execute('SELECT sport,reason,count(*) FROM ssa_joined GROUP BY 1,2 ORDER BY 1,2').fetchall()]
    eligible=con.execute('SELECT count(*) FROM ssa_base').fetchone()[0]
    require(summary.get('candidate_rows')==candidate_rows and summary.get('eligible_rows')==eligible and summary.get('exclusions')==exclusions,'independent sports disjoint exclusion/count reconciliation differs')
    timing=[{'sport':s,'timing_quality':q,'rows':n} for s,q,n in con.execute('SELECT sport,timing_quality,count(*) FROM ssa_base GROUP BY 1,2 ORDER BY 1,2').fetchall()]
    require(summary.get('timing_coverage')==timing,'independent sports clock quality coverage differs')
    expected_scope=[];expected_phase=[];expected_window=[]
    for scope in SCOPES:
        predicate='TRUE' if scope=='pooled' else 'sport='+literal(scope)
        n,g=con.execute('SELECT coalesce(sum(n_rows),0),count(DISTINCT game_cluster) FROM ssa_expected_phase WHERE '+predicate).fetchone()
        expected_scope.append({'scope':scope,'n_observations':n,'n_games':g})
        for phase in ('pregame','in_play'):
            n,g=con.execute('SELECT coalesce(sum(n_rows),0),count(DISTINCT game_cluster) FROM ssa_expected_phase WHERE '+predicate+' AND phase='+literal(phase)).fetchone()
            expected_phase.append({'scope':scope,'phase':phase,'n_observations':n,'n_games':g})
        for panel,key,_ in WINDOWS:
            n,g=con.execute('SELECT coalesce(sum(n_rows),0),count(DISTINCT game_cluster) FROM ssa_expected_window WHERE '+predicate+' AND clock_window='+literal(key)).fetchone()
            expected_window.append({'scope':scope,'panel':panel,'window':key,'n_observations':n,'n_games':g})
    require(summary.get('scope_counts')==expected_scope and summary.get('phase_counts')==expected_phase and summary.get('window_counts')==expected_window,'independent sports full-scope phase/window support counts differ')
    _grid_rows(summary)
    keys={'sport_'+scope+'_'+family for scope in SCOPES for family in ('phases','profiles','windows')}
    require(set(summary.get('joint_artifacts',{}))==set(summary.get('score_artifacts',{}))==keys and set(outputs)==keys|{'aggregate_phase','aggregate_window'},'exact 30-joint/32-body sports inventory differs')
    definitions=summary.get('definitions',{})
    require(definitions.get('endpoint_grace_seconds')==0 and definitions.get('minimum_prints_per_cell')==500 and definitions.get('minimum_games_per_cell')==30,'frozen sports endpoint/support convention differs')
    windows=definitions.get('windows',[])
    require(len(windows)==14 and [(row.get('panel'),row.get('key')) for row in windows]==[(panel,key) for panel,key,_ in WINDOWS],'frozen sports window order differs')
    reconciled=summary.get('window_reconciliation',{})
    require(set(reconciled)=={'all_scopes_pregame_partition','all_scopes_since_start_partition','final_hour_overlaps_since_start','exact_start_and_endpoint_retained'}
        and all(value is True for value in reconciled.values()),'sports window/endpoint reconciliation declarations missing')
    reads=numerical.Reads();result=[]
    output_by_name={record['path']:record for record in outputs.values()}
    require(len(output_by_name)==len(outputs),'duplicate sports artifact filename')
    for scope in SCOPES:
        for family in ('phases','profiles','windows'):
            key='sport_'+scope+'_'+family
            result.append(_audit_joint(con,root,key,summary['joint_artifacts'][key],scope,family,summary,summary['score_artifacts'],output_by_name,reads))
    return {'candidate_rows':candidate_rows,'eligible_rows':eligible,'joints':result,'score_reads_bytes':reads.bytes,
        'game_aggregate_replay':'all accepted saved sports candidate prints against accepted map, not a raw native-tape replay'}


def _reopen_audit(record,bound,*,status):
    report=bound.json(record)
    require(report.get('status')==status and type(report.get('checks')) is dict and report['checks']
        and all(value is True for value in report['checks'].values()),'passing independent sports audit receipt required')
    _historical_source(report.get('audit_source',{}),report.get('audit_source',{}).get('head'),SOURCE_FILES)
    inventory=report.get('body_bindings',{})
    path=bound.relative(Path(record['path']).parent,inventory,parquet=True,maximum=32_000_000)
    rows=pq.ParquetFile(path).read().to_pylist()
    require(len(rows)<=50_000 and len({r['path'] for r in rows})==len(rows),'bounded unique independent body inventory required')
    for row in rows:
        record={key:row[key] for key in ('path','bytes','sha256')}
        if row.get('rows') is not None:
            path=Path(row['path'])
            require(path.is_absolute() and path.resolve(strict=True)==path and path.is_relative_to(DATA_ROOT),'canonical explicit audit body path required')
            footer=pq.ParquetFile(path)
            require(footer.metadata.num_rows==row['rows'] and hashlib.sha256(str(footer.schema_arrow).encode()).hexdigest()==row.get('schema_sha256'),'independent proof body footer/schema changed')
            record.update(rows=row['rows'],schema=str(footer.schema_arrow))
        bound.absolute(record,maximum=MAX_CACHE_BYTES)
    return report


def audit_scores(con,*,sports_dir,sports_manifest_sha256,sports_sha256,expected_producer_head,
                 input_manifest_path,input_acceptance_path,map_manifest_path,map_acceptance_path):
    bound=Bound();root=Path(sports_dir)
    require(not (root/'failure.json').exists(),'failed sports estimate stage cannot be audited')
    manifest=bound.json(_json_record(root/'manifest.json',sports_manifest_sha256))
    require(manifest.get('schema_version')=='kaushik_kalshi_sports_estimate_stage_v1' and manifest.get('status')=='sports_estimates_complete'
        and manifest.get('sports_sha256')==sports_sha256,'explicit sports manifest/summary binding differs')
    producer=_historical_source(manifest.get('source',{}),expected_producer_head,['analysis/kaushik_kalshi_replication/sports.py'])
    summary=bound.json(_json_record(root/'sports_estimates.json',sports_sha256))
    binding=summary.get('input_binding',{})
    require(binding==manifest.get('input_binding'),'sports summary/manifest input binding differs')
    core_binding=manifest.get('core_binding',{})
    core=_bound_core_json(bound,core_binding.get('manifest',{}));core_accepted=_bound_core_json(bound,core_binding.get('acceptance',{}))
    require(core.get('schema_version')=='kaushik_kalshi_estimate_stage_v1' and core.get('status')=='estimates_complete'
        and core_accepted.get('schema_version')=='kaushik_kalshi_estimate_acceptance_v1' and core_accepted.get('status')=='estimates_reopened_accepted'
        and core_accepted.get('all_outputs_reopened') is True and core_accepted.get('manifest_sha256')==core_binding['manifest']['sha256']==binding.get('core_manifest_sha256')
        and core_binding['acceptance']['sha256']==binding.get('core_acceptance_sha256')
        and core.get('estimates_sha256')==core_accepted.get('estimates_sha256')==binding.get('core_estimates_sha256')==manifest.get('core_estimates_sha256')
        and core.get('input_binding',{}).get('manifest',{}).get('sha256')==binding.get('input_manifest_sha256'),'accepted core/native input lineage differs')
    bound.json(_json_record(Path(core_binding['manifest']['path']).parent/'estimates.json',binding['core_estimates_sha256']))
    inputs=bound.json(_json_record(input_manifest_path,binding.get('input_manifest_sha256')))
    accepted=bound.json(_json_record(input_acceptance_path,binding.get('input_acceptance_sha256')))
    require(Path(input_manifest_path).parent==Path(input_acceptance_path).parent and inputs.get('schema_version')=='kaushik_kalshi_inputs_v1' and inputs.get('status')=='accepted'
        and accepted.get('schema_version')=='kaushik_kalshi_inputs_acceptance_v1' and accepted.get('status')=='accepted'
        and accepted.get('manifest_sha256')==binding['input_manifest_sha256'] and type(accepted.get('checks')) is dict
        and set(bodies.CHECKS)<=set(accepted['checks']) and all(value is True for value in accepted['checks'].values()),'accepted native input pair differs')
    require(inputs.get('scientific_contract',{}).get('sample_scope')=='all_available_dates' and inputs['scientific_contract'].get('combos')=='excluded','native all-dates no-combo sports source required')
    mapping=bound.json(_json_record(map_manifest_path,binding.get('map_manifest_sha256')))
    map_accepted=bound.json(_json_record(map_acceptance_path,binding.get('map_acceptance_sha256')))
    require(Path(map_manifest_path).parent==Path(map_acceptance_path).parent and mapping.get('schema_version')=='kaushik_kalshi_sports_map_v1'
        and mapping.get('status')=='sports_map_complete_pending_independent_audit' and map_accepted.get('schema_version')=='kaushik_kalshi_sports_map_acceptance_v1'
        and map_accepted.get('status')=='sports_map_reopened_accepted' and map_accepted.get('manifest_sha256')==binding['map_manifest_sha256']
        and set(map_accepted.get('checks',{}))==set(MAP_CHECKS) and all(v is True for v in map_accepted['checks'].values())
        and map_accepted.get('proof_modes',{}).get('provider_clock_conventions_verified')==CLOCK_PROOF,'independently accepted retained-clock native sports map required')
    map_audit=_reopen_audit(map_accepted.get('audit',{}),bound,status='sports_map_sources_reconciled')
    require(map_audit.get('manifest_sha256')==binding['map_manifest_sha256'],'accepted map audit manifest identity differs')
    map_root=Path(map_manifest_path).parent
    for role,record in mapping.get('artifacts',{}).items():bound.relative(map_root,record,parquet=True,maximum=1_000_000_000)
    require(set(mapping.get('artifacts',{}))=={'market_map','binding_proofs','exclusions'},'accepted map complete artifact inventory required')
    map_record=mapping['artifacts']['market_map'];map_path=bodies.child(map_root,map_record['path'])
    require(binding.get('market_map')=={**map_record,'path':str(map_path)} and binding.get('timing_qualification')==mapping.get('timing_qualification'),'sports accepted map body/clock qualification differs')
    input_root=Path(input_manifest_path).parent;index_path=bound.relative(input_root,inputs['daily_receipts'])
    index=bound.json({**inputs['daily_receipts'],'path':str(index_path)})
    require(index.get('schema_version')=='kaushik_kalshi_receipt_inventory_v1' and type(index.get('receipts')) is list,'accepted daily sports source inventory required')
    paths=[];days=set();cache_records=[]
    for item in index['receipts']:
        path=bound.relative(input_root,item);day=bound.json({**item,'path':str(path)})
        source_day=day.get('source_day')
        require(source_day not in days and item.get('path')=='cells/day='+str(source_day)+'/receipt.json'
            and day.get('schema_version')=='kaushik_kalshi_day_cells_v1' and day.get('status')=='durable_pending_global_admission','accepted daily sports partition contract differs')
        days.add(source_day);record=day.get('artifacts',{}).get('sports_candidates',{})
        require(record.get('path')=='day='+source_day+'/sports_candidates.parquet' and record.get('rows')==day.get('sports_candidate_rows'),'daily native sports cache identity/count differs')
        path=bound.relative(input_root/'cells',record,parquet=True,maximum=MAX_CACHE_BYTES)
        _schema(path,{'ticker':'string','claim_id':'string','event_cluster':'string','sport':'string','timestamp':'double','P':'double','Y':'integer','payoff':'double','roi':'double','bin':'integer'})
        paths.append(path);cache_records.append({'path':str(path),'bytes':record['bytes'],'sha256':record['sha256'],'rows':record['rows']})
    require(days==bodies.expected_days() and len(days)==inputs.get('counts',{}).get('source_days'),'complete accepted native sports daily coverage required')
    require(cache_records==binding.get('candidate_files') and len(cache_records)==binding.get('candidate_files_count',len(cache_records))
        and sum(r['rows'] for r in cache_records)==binding.get('candidate_rows')==inputs.get('sports',{}).get('candidate_rows')
        and sum(r['bytes'] for r in cache_records)<=MAX_CACHE_BYTES,'all accepted daily cache bindings/counts/bytes differ')
    outputs=manifest.get('outputs',{})
    require(type(outputs) is dict and sum(r.get('bytes',0) for r in outputs.values())<=4_000_000_000,'bounded complete sports output inventory required')
    cache_bytes=sum(r['bytes'] for r in cache_records);output_bytes=sum(r['bytes'] for r in outputs.values())
    other_bytes=sum(r['bytes'] for r in bound.records())-cache_bytes
    require(cache_bytes*12+output_bytes*4+other_bytes*2<=128_000_000_000,'sports independent full-cache/score read plan exceeds 128GB')
    for record in outputs.values():bound.relative(root,record,parquet=True,maximum=512_000_000)
    bodies.view(con,'ss_input_candidates',paths);bodies.view(con,'ss_input_map',[map_path])
    details=audit_game_scores(con,candidates_relation='ss_input_candidates',map_relation='ss_input_map',summary=summary,root=root,outputs=outputs)
    bound.check_drift()
    return {'schema_version':'kaushik_kalshi_sports_score_audit_v1','status':'saved_scores_reconciled','sports_sha256':sports_sha256,
        'sports_manifest_sha256':sports_manifest_sha256,'producer_source':producer,'checks':{
            'source_hashes_reopened':True,'accepted_native_map_verified':True,'saved_game_cells_replayed':True,'direct_means_verified':True,
            'game_scores_verified':True,'joint_CR0_covariance_verified':True,'contrasts_intervals_influence_verified':True,'complete_grids_support_verified':True},
        'proof_modes':{'provider_clock_conventions_verified':CLOCK_PROOF,'saved_cache_boundary':'Accepted input cache bodies are fully replayed; individual native raw trade identity/price/time equality remains the separate fixed eight-day input-audit replay, not a second full-history tape proof.'},
        'details':details,'raw_tape_read':False},bound.records()


def _own_source(expected):
    require(subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()==expected,'sports audit current committed HEAD differs')
    files={}
    for relative in SOURCE_FILES:
        require(subprocess.run(['git','ls-files','--error-unmatch',relative],cwd=REPO,capture_output=True).returncode==0
            and subprocess.run(['git','diff','--quiet','HEAD','--',relative],cwd=REPO).returncode==0,'sports audit source must be tracked/clean: '+relative)
        files[relative]=bodies.digest(REPO/relative)
    snapshot={'head':expected,'files':files};_historical_source(snapshot,expected,SOURCE_FILES)
    return snapshot


def _write_json(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    require(len(raw)<=16_000_000,'sports audit JSON output ceiling')
    with Path(path).open('xb') as stream:
        stream.write(raw);stream.flush();os.fsync(stream.fileno())
    require(Path(path).read_bytes()==raw,'sports audit output exact reopen differs')


def _save_report(target,report,records,source):
    require(len(records)<=50_000,'sports audit body inventory row ceiling')
    table=pa.Table.from_pylist(records)
    require(table.nbytes<=32_000_000,'sports audit body inventory memory ceiling')
    path=target/'body_bindings.parquet'
    with path.open('xb') as stream:
        pq.write_table(table,stream,compression='zstd');stream.flush();os.fsync(stream.fileno())
    require(path.stat().st_size<=32_000_000 and pq.ParquetFile(path).read().equals(table),'sports audit body inventory exact reopen differs')
    report={**report,'audit_source':source,'body_bindings':bodies._record(target,path,parquet=True),
        'resources':{'memory_limit':DUCKDB_MEMORY_LIMIT,'minimum_available_ram_bytes':MINIMUM_AVAILABLE_RAM_BYTES,
                     'disabled_optimizers':DISABLED_OPTIMIZERS,
                     'threads':2,'numpy_bytes':MAX_NUMPY_BYTES,'spill_bytes':4_000_000_000,
                     'output_bytes':MAX_OUTPUT_BYTES,'minimum_free_bytes':FLOOR,'saved_cache_bytes':MAX_CACHE_BYTES,'planned_read_bytes_ceiling':128_000_000_000}}
    _write_json(target/'audit_candidate.json',report)
    require(sum(p.stat().st_size for p in target.rglob('*') if p.is_file() and 'spill' not in p.relative_to(target).parts)<=MAX_OUTPUT_BYTES
        and shutil.disk_usage(target).free>=FLOOR,'sports audit output/free-space floor exceeded')
    return report


def _guard(expected):
    import production_guard
    production_guard.require_production_host()
    require(REPO==Path('/home/ubuntu/prediction_markets') and Path(sys.executable).resolve()==PRODUCTION_PYTHON.resolve(),'canonical sports-audit source/Python required')
    return _own_source(expected)


def publish_map(*,audit_dir,audit_sha256,expected_audit_head):
    source=_guard(expected_audit_head);bound=Bound();target=Path(audit_dir)
    require(target.is_absolute() and target.resolve()==target and target.parent==RUN_ROOT and not (target/'failure.json').exists(),'complete immutable top-level sports map audit required')
    report=_reopen_audit(_json_record(target/'audit_candidate.json',audit_sha256),bound,status='sports_map_sources_reconciled')
    require(report.get('schema_version')=='kaushik_kalshi_sports_map_audit_v1' and set(report['checks'])==set(MAP_CHECKS)
        and report.get('audit_source')==source and report.get('proof_modes',{}).get('provider_clock_conventions_verified')==CLOCK_PROOF,'all explicit independent map release proofs required')
    manifest_path=Path(report['manifest_path']);root=manifest_path.parent
    require(root.parent==RUN_ROOT and not (root/'failure.json').exists() and not os.path.lexists(root/'acceptance.json'),'new map acceptance only; failed/preexisting receipt forbidden')
    require(shutil.disk_usage(root).free>=FLOOR+16_000_000,'map acceptance/free-floor reservation failed')
    require(bodies.digest(manifest_path)==report['manifest_sha256'],'map manifest changed before acceptance')
    bound.check_drift();require(_own_source(expected_audit_head)==source,'sports audit source drift')
    result={'schema_version':'kaushik_kalshi_sports_map_acceptance_v1','status':'sports_map_reopened_accepted',
        'manifest_sha256':report['manifest_sha256'],'audit':_json_record(target/'audit_candidate.json',audit_sha256),
        'checks':report['checks'],'proof_modes':report['proof_modes'],'audit_source':source}
    _write_json(root/'acceptance.json',result)
    return result


def run_production(args):
    source=_guard(args.expected_audit_head)
    target=Path(args.output_dir)
    require(target.is_absolute() and target.resolve()==target and target.parent==RUN_ROOT and not os.path.lexists(target),'new immutable top-level sports audit stage required')
    require(shutil.disk_usage(target.parent).free>=FLOOR+MAX_OUTPUT_BYTES+1_000_000_000+4_000_000_000,'sports audit output/spill/free-floor reservation failed')
    available=re.search(r'^MemAvailable:\s+(\d+) kB$',Path('/proc/meminfo').read_text(),re.MULTILINE)
    require((os.cpu_count() or 0)>=2 and available and int(available.group(1))*1024>=MINIMUM_AVAILABLE_RAM_BYTES,'sports audit two CPUs/32GB available RAM required')
    target.mkdir();con=None
    try:
        con=duckdb.connect()
        for sql in ("SET TimeZone='UTC'",'SET memory_limit='+literal(DUCKDB_MEMORY_LIMIT),"SET threads=2","SET enable_object_cache=false",
                    'SET disabled_optimizers='+literal(DISABLED_OPTIMIZERS)):
            con.execute(sql)
        con.execute('SET temp_directory='+literal(target/'spill'));con.execute("SET max_temp_directory_size='4GB'")
        if args.command=='map-audit':
            report,records=audit_map(con,manifest_path=args.manifest,manifest_sha256=args.manifest_sha256,expected_producer_head=args.expected_producer_head)
        else:
            report,records=audit_scores(con,sports_dir=args.sports_dir,sports_manifest_sha256=args.sports_manifest_sha256,sports_sha256=args.sports_sha256,
                expected_producer_head=args.expected_producer_head,input_manifest_path=args.input_manifest,input_acceptance_path=args.input_acceptance,
                map_manifest_path=args.map_manifest,map_acceptance_path=args.map_acceptance)
        require(_own_source(args.expected_audit_head)==source,'sports audit source drift before saved proof')
        return _save_report(target,report,records,source)
    except BaseException as error:
        if not (target/'failure.json').exists():_write_json(target/'failure.json',{'status':'sports_audit_incomplete','error_type':type(error).__name__,'reason':str(error)})
        raise
    finally:
        if con is not None:con.close()


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);commands=parser.add_subparsers(dest='command',required=True)
    mapping=commands.add_parser('map-audit')
    mapping.add_argument('--manifest',required=True);mapping.add_argument('--manifest-sha256',required=True)
    scores=commands.add_parser('scores')
    for flag in ('sports-dir','sports-manifest-sha256','sports-sha256','input-manifest','input-acceptance','map-manifest','map-acceptance'):
        scores.add_argument('--'+flag,required=True)
    for command in (mapping,scores):
        for flag in ('expected-producer-head','expected-audit-head','output-dir'):command.add_argument('--'+flag,required=True)
    publish=commands.add_parser('map-publish')
    for flag in ('audit-dir','audit-sha256','expected-audit-head'):publish.add_argument('--'+flag,required=True)
    args=parser.parse_args(argv)
    result=publish_map(audit_dir=args.audit_dir,audit_sha256=args.audit_sha256,expected_audit_head=args.expected_audit_head) if args.command=='map-publish' else run_production(args)
    print(json.dumps({'status':result['status'],'checks':result['checks']},sort_keys=True));return 0


if __name__=='__main__':
    raise SystemExit(main())
