"""Report-only rendering from explicit accepted saved JSON; never estimate or scan.

The reviewed config binds all ten JSON roles and supplied canonical source
identities. Matching local bytes is not a claim about the Mac Git repository.
Actual TeX/PDF authoring is a separate root-authorized operation; fixture tests
mock the publishing boundary and plotting backend.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

from analysis.kaushik_kalshi_replication import report


REPO=Path(__file__).resolve().parents[1]
PRIMARY_NAME='kalshi_flb_replication.tex'
JSON_LIMIT=16_000_000
TOTAL_JSON_LIMIT=128_000_000
OUTPUT_LIMIT=100_000_000
SOURCE_FILES=(
    'scripts/render_kaushik_kalshi_report.py',
    'analysis/kaushik_kalshi_replication/__init__.py',
    'analysis/kaushik_kalshi_replication/report.py','analysis/kaushik_kalshi_replication/inputs.py',
    'analysis/kaushik_polymarket_replication/__init__.py',
    'analysis/kaushik_polymarket_replication/render_report.py',
    'analysis/kaushik_polymarket_replication/exchange_filter_contract.py',
    'analysis/kaushik_polymarket_replication/build_inputs.py')
ROLES=('core_estimates','core_manifest','core_acceptance','core_score_audit',
       'reader_metadata','reader_provenance','sports_estimates','sports_manifest',
       'sports_acceptance','sports_score_audit')


class RenderBlocked(ValueError):
    pass


def require(value,message):
    if not value:raise RenderBlocked(message)


def _sha(path):
    value=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1_048_576),b''):value.update(block)
    return value.hexdigest()


def _identity(path):
    value=path.stat()
    return (value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns)


def _regular(value):
    path=Path(value)
    require(path.is_absolute() and path.resolve(strict=True)==path and path.is_file()
            and not path.is_symlink(),'explicit canonical regular input required')
    return path


def _pairs(pairs):
    result={}
    for key,value in pairs:
        require(key not in result,'duplicate saved JSON key');result[key]=value
    return result


def _bound(record,*,maximum,read_json=False):
    require(type(record) is dict and set(record)=={'path','bytes','sha256'}
            and type(record['bytes']) is int and 0<record['bytes']<=maximum
            and isinstance(record['sha256'],str) and re.fullmatch('[0-9a-f]{64}',record['sha256']),
            'explicit bounded path/bytes/SHA-256 record required')
    path=_regular(record['path']);before=_identity(path)
    require(before[2]==record['bytes'],'saved input byte size differs')
    raw=path.read_bytes() if read_json else None
    require((hashlib.sha256(raw).hexdigest() if raw is not None else _sha(path))==record['sha256']
            and _identity(path)==before,'saved input hash/drift differs')
    bound={**record,'identity':before}
    if not read_json:return bound
    value=json.loads(raw,object_pairs_hook=_pairs,
        parse_constant=lambda token:(_ for _ in ()).throw(RenderBlocked('nonfinite saved JSON')))
    require(type(value) is dict,'saved JSON object required')
    return value,bound


def _recheck(bindings):
    _import_origins()
    for record in bindings:
        path=_regular(record['path'])
        require(_identity(path)==record['identity'] and _sha(path)==record['sha256'],
                'render input/source drift')


def _analysis_namespace():
    """Bind the actual canonical namespace; never invent or ignore a marker."""
    path=REPO/'analysis';module=sys.modules.get('analysis')
    require(path.is_dir() and path.resolve(strict=True)==path and
            not os.path.lexists(path/'__init__.py'),'canonical analysis namespace initializer must be absent')
    require(module is not None and getattr(module,'__file__',None) is None and
            list(getattr(module,'__path__',()))==[str(path)],'imported analysis namespace origin differs')
    return {'analysis':{'path':str(path),'initializer':'absent','search_paths':[str(path)]}}


def _import_origins():
    namespace_packages=_analysis_namespace()
    for name,relative in (
        ('analysis.kaushik_kalshi_replication','analysis/kaushik_kalshi_replication/__init__.py'),
        ('analysis.kaushik_kalshi_replication.report','analysis/kaushik_kalshi_replication/report.py'),
        ('analysis.kaushik_kalshi_replication.inputs','analysis/kaushik_kalshi_replication/inputs.py'),
        ('analysis.kaushik_polymarket_replication','analysis/kaushik_polymarket_replication/__init__.py'),
        ('analysis.kaushik_polymarket_replication.render_report','analysis/kaushik_polymarket_replication/render_report.py'),
        ('analysis.kaushik_polymarket_replication.exchange_filter_contract','analysis/kaushik_polymarket_replication/exchange_filter_contract.py')):
        require(Path(getattr(sys.modules.get(name),'__file__','')).resolve()==REPO/relative,
                'imported renderer source origin differs: '+name)
    return namespace_packages


def prepare_render(*,config_path,config_sha256):
    """Validate bounded saved inputs and return pure source/specs; no writes."""
    path=_regular(config_path)
    config,config_binding=_bound({'path':str(path),'bytes':path.stat().st_size,'sha256':config_sha256},
        maximum=1_000_000,read_json=True)
    require(set(config)=={'schema_version','status','canonical_sources','files'} and
            config['schema_version']=='kaushik_kalshi_report_config_v1' and config['status']=='reviewed',
            'exact reviewed full-report config required')
    canonical=config['canonical_sources']
    require(type(canonical) is dict and set(canonical)=={'head','files'} and
            isinstance(canonical['head'],str) and re.fullmatch('[0-9a-f]{40}',canonical['head'])
            and type(canonical['files']) is dict and set(canonical['files'])==set(SOURCE_FILES),
            'exact supplied canonical renderer-source inventory required')
    source_bindings=[]
    for relative in SOURCE_FILES:
        item=canonical['files'][relative]
        require(type(item) is dict and set(item)=={'bytes','sha256'},'canonical renderer-source identity differs')
        source_bindings.append(_bound({'path':str(REPO/relative),**item},maximum=1_000_000))
    namespace_packages=_import_origins()
    files=config['files']
    require(type(files) is dict and set(files)==set(ROLES),'all ten explicit accepted JSON roles required')
    require(all(type(item) is dict and type(item.get('bytes')) is int and item['bytes']>0 for item in files.values()),
            'exact report JSON records required')
    require(len({item.get('path') for item in files.values() if type(item) is dict})==len(ROLES),
            'report input roles share a path or are malformed')
    require(sum(item.get('bytes',TOTAL_JSON_LIMIT+1) for item in files.values())<=TOTAL_JSON_LIMIT,
            'total saved report JSON ceiling exceeded')
    values,bindings={},[config_binding,*source_bindings]
    for role in ROLES:
        values[role],binding=_bound(files[role],maximum=JSON_LIMIT,read_json=True);bindings.append(binding)
    core,manifest,accepted=values['core_estimates'],values['core_manifest'],values['core_acceptance']
    core_hash,manifest_hash=files['core_estimates']['sha256'],files['core_manifest']['sha256']
    require(manifest.get('schema_version')=='kaushik_kalshi_estimate_stage_v1' and
            manifest.get('status')=='estimates_complete' and manifest.get('estimates_sha256')==core_hash and
            accepted.get('source_head')==manifest.get('source',{}).get('head') and
            isinstance(accepted.get('source_head'),str) and re.fullmatch('[0-9a-f]{40}',accepted['source_head']),
            'core estimate manifest/source binding differs')
    require(values['core_score_audit'].get('estimate_acceptance_sha256')==files['core_acceptance']['sha256'],
            'core score-audit acceptance binding differs')
    provenance=values['reader_provenance'];native=manifest.get('input_binding',{})
    require(provenance.get('schema_version')=='kaushik_kalshi_reader_metadata_provenance_v1' and
            provenance.get('raw_tape_read') is False and provenance.get('saved_file_writes') is False,
            'saved-body reader provenance required')
    input_hashes=(provenance.get('accepted_manifest_sha256'),provenance.get('accepted_acceptance_sha256'))
    require(all(isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) for value in input_hashes) and
            input_hashes==(native.get('manifest',{}).get('sha256'),native.get('acceptance',{}).get('sha256')),
            'reader/core accepted input binding differs')
    sports,sm=values['sports_estimates'],values['sports_manifest']
    sports_hash,sports_manifest_hash=files['sports_estimates']['sha256'],files['sports_manifest']['sha256']
    require(sm.get('schema_version')=='kaushik_kalshi_sports_estimate_stage_v1' and
            sm.get('status')=='sports_estimates_complete' and sm.get('sports_sha256')==sports_hash
            and sm.get('core_estimates_sha256')==core_hash and
            sm.get('input_binding')==sports.get('input_binding'),
            'sports estimate manifest/core binding differs')
    sports_inputs=sports.get('input_binding',{})
    require(input_hashes==(sports_inputs.get('input_manifest_sha256'),sports_inputs.get('input_acceptance_sha256')),
            'reader/sports accepted input binding differs')
    bundle={'summary':sports,'sports_sha256':sports_hash,'sports_manifest_sha256':sports_manifest_hash,
        'acceptance':values['sports_acceptance'],'score_audit':values['sports_score_audit']}
    source=report.render_source(core,values['reader_metadata'],estimate_acceptance=accepted,
        score_audit=values['core_score_audit'],estimates_sha256=core_hash,
        estimate_manifest_sha256=manifest_hash,draft=False,sports=bundle)
    specs=[report.figure_specs(core),*report.sports_figure_specs(sports).values()]
    require(len({spec['filename'] for spec in specs})==len(specs)==21,'complete unique vector figure inventory required')
    for spec in specs:plot_geometry(spec)
    _recheck(bindings)
    return {'source':source,'figure_specs':specs,'bindings':bindings,
        'canonical_sources':canonical,'namespace_packages':namespace_packages,'config_sha256':config_sha256,
        'input_bindings':{role:dict(files[role]) for role in ROLES}}


def plot_geometry(spec):
    """Translate saved figure descriptions into isolated-interval axes only."""
    require(spec.get('connect_points') is False,'connected bin/window figures are forbidden')
    axes=[]
    for row,panel in enumerate(spec['panels']):
        require(panel.get('zero_reference')==0,'defined zero reference required')
        series=panel.get('series',[panel]);window=spec.get('family')=='windows'
        all_values=[0.0]
        for item in series:
            require(item.get('linestyle')=='none' and item.get('capsize')==3,'isolated capped intervals required')
            observed=set()
            for point in item['points']:
                x=point.get('x',point.get('bin'))
                require(type(x) is int and x in panel['ticks'] and x not in observed,'unique fixed-bin/window position required')
                observed.add(x)
                low,value,high=(point[key] for key in ('ci95_low','estimate','ci95_high'))
                require(all(type(v) in (int,float) and math.isfinite(v) for v in (low,value,high)) and low<=value<=high,
                        'finite ordered saved figure interval required')
                all_values.extend((low,high))
            require(not observed.intersection(item.get('x',item.get('bin')) for item in item.get('withheld',[])),
                    'withheld estimate appears among plotted marks')
        spread=max(all_values)-min(all_values);padding=.07*spread if spread else 1.
        limits=(min(all_values)-padding,max(all_values)+padding)
        require(all(math.isfinite(value) for value in limits),'figure range overflow')
        groups=[[item] for item in series] if window else [series]
        for col,selected in enumerate(groups):
            ticks=panel['ticks']
            if window:
                label=selected[0]['label'];positions={'pregame':range(0,5),'since_start':range(5,10),'final_hour':range(10,14)}
                require(label in positions,'literal window family required');ticks=list(positions[label])
            axes.append({'row':row,'column':col,'outcome':panel['outcome'],'series':selected,'ticks':ticks,
                'tick_labels':[panel['tick_labels'][panel['ticks'].index(tick)] for tick in ticks],
                'ylim':limits,'window':window,'xlabel':spec.get('x_label','Fixed ten-cent probability bin')})
    return axes


def draw_figure(spec,path,*,pyplot=None):
    """Write one deterministic vector PDF; tests supply a non-writing backend."""
    if pyplot is None:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as pyplot
    geometry=plot_geometry(spec);columns=3 if spec.get('family')=='windows' else 1
    # The report's 0.65in margins give 7.2in on standard letter paper. Window
    # figures retain their 10in canvas/aspect, so 11pt text prints about 7.9pt
    # at linewidth (rather than scaling the old 8pt ticks to about 5.8pt).
    style={'font.family':'DejaVu Sans','font.size':11 if columns==3 else 9,
           'pdf.fonttype':42,'pdf.compression':9,'text.usetex':False}
    if columns==3:style.update({'axes.labelsize':11,'axes.titlesize':11,'xtick.labelsize':11,'ytick.labelsize':11})
    with pyplot.rc_context(style):
        figure,axes=pyplot.subplots(2,columns,figsize=(10,5) if columns==3 else (8,4.6),squeeze=False)
        try:
            for item in geometry:
                axis=axes[item['row']][item['column']];marks=0
                for series in item['series']:
                    points=series['points'];marks+=len(points)
                    if points:
                        axis.errorbar([p.get('x',p.get('bin')) for p in points],[p['estimate'] for p in points],
                            yerr=[[p['estimate']-p['ci95_low'] for p in points],[p['ci95_high']-p['estimate'] for p in points]],
                            fmt=series['marker'],linestyle='none',color=series['color'],capsize=3,
                            markersize=4,elinewidth=.85,label=series.get('label'))
                axis.axhline(0,color='0.45',linewidth=.7,zorder=0)
                axis.set_ylim(*item['ylim']);axis.set_xlim(min(item['ticks'])-.5,max(item['ticks'])+.5)
                axis.set_xticks(item['ticks'],[label.replace(' to ','\nto ') if item['window'] else label for label in item['tick_labels']],fontsize=11 if item['window'] else 8)
                axis.spines[['top','right']].set_visible(False);axis.grid(axis='y',alpha=.2)
                if item['column']==0:
                    axis.set_ylabel(('Payoff gap (cents)' if item['window'] else 'Mean payoff (cents)') if item['outcome']=='payoff_cents'
                        else ('Return gap (pp)' if item['window'] else 'Mean individual return (%)'))
                if item['window'] and item['row']==0:
                    axis.set_title({'pregame':'Before start','since_start':'Since start','final_hour':'Final recorded hour'}[item['series'][0]['label']])
                if not item['window'] and item['row']==1:axis.set_xlabel(item['xlabel'])
                if not item['window'] and len(item['series'])>1 and marks:axis.legend(frameon=False,loc='best',ncol=2)
                if not marks:axis.text(.5,.5,'No supported cells',ha='center',transform=axis.transAxes,color='0.4')
            figure.tight_layout()
            figure.savefig(path,format='pdf',bbox_inches='tight',metadata={
                'Creator':'Native Kalshi replication renderer','CreationDate':None,'ModDate':None})
        finally:pyplot.close(figure)


def _publish_render(plan,target):
    from analysis.kaushik_polymarket_replication import build_inputs as io
    require(Path(io.__file__).resolve()==REPO/'analysis/kaushik_polymarket_replication/build_inputs.py',
            'imported publication helper source origin differs')
    _recheck(plan['bindings'])
    require(shutil.disk_usage(target.parent).free>=OUTPUT_LIMIT,'report output reservation unavailable')
    stage=Path(tempfile.mkdtemp(prefix='.'+target.name+'.staging-',dir=target.parent));published=False
    try:
        (stage/'figures').mkdir();artifacts={};written=0
        for relative in [PRIMARY_NAME,*[spec['filename'] for spec in plan['figure_specs']]]:
            path=stage/relative
            with io.copy_ceiling(OUTPUT_LIMIT-written):
                if relative==PRIMARY_NAME:
                    with path.open('xb') as stream:stream.write(plan['source'].encode());stream.flush();os.fsync(stream.fileno())
                else:draw_figure(next(spec for spec in plan['figure_specs'] if spec['filename']==relative),path)
            with path.open('rb') as stream:os.fsync(stream.fileno())
            size=path.stat().st_size;written+=size
            require(size>0 and written<OUTPUT_LIMIT,'report output byte ceiling exceeded')
            artifacts[relative]={'path':relative,'bytes':size,'sha256':_sha(path)}
        import matplotlib
        manifest={'schema_version':'kaushik_kalshi_report_render_v1','status':'rendered_pending_compile_and_page_qa',
            'config_sha256':plan['config_sha256'],'canonical_sources':plan['canonical_sources'],
            'namespace_packages':plan['namespace_packages'],
            'source_binding_scope':'local bytes match supplied canonical identities; no local Git equivalence inferred',
            'input_bindings':plan['input_bindings'],'artifacts':artifacts,'primary_source':PRIMARY_NAME,
            'figure_count':len(plan['figure_specs']),'all_saved_release_gates_passed':True,
            'raw_data_read':False,'estimated':False,'compiled':False,'page_qa':'pending',
            'environment':{'python':sys.version,'matplotlib':matplotlib.__version__},'output_byte_limit':OUTPUT_LIMIT}
        raw=(json.dumps(manifest,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
        require(written+len(raw)<=OUTPUT_LIMIT,'render manifest exceeds report byte ceiling')
        with (stage/'render_manifest.json').open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        _recheck(plan['bindings'])
        require(all((stage/name).stat().st_size==item['bytes'] and _sha(stage/name)==item['sha256'] for name,item in artifacts.items()),
                'render artifact failed exact reopen')
        descriptor=os.open(stage,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
        io.atomic_publish(stage,target);published=True
        descriptor=os.open(target.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
        _recheck(plan['bindings'])
        require((target/'render_manifest.json').read_bytes()==raw and
                all((target/name).stat().st_size==item['bytes'] and _sha(target/name)==item['sha256'] for name,item in artifacts.items()),
                'published render failed exact reopen')
        return manifest
    except BaseException as error:
        failed=target if published else stage
        with (failed/'failure.json').open('x') as stream:
            json.dump({'status':'render_incomplete','error_type':type(error).__name__,'reason':str(error)},stream,sort_keys=True)
        raise


def render(*,config_path,config_sha256,output_dir):
    target=Path(output_dir)
    require(target.is_absolute() and target.resolve()==target and target.parent.is_dir() and not os.path.lexists(target),
            'new absolute immutable report directory required')
    plan=prepare_render(config_path=config_path,config_sha256=config_sha256)
    require(all(not Path(item['path']).is_relative_to(target) for item in plan['bindings']),
            'report output overlaps an input/source')
    return _publish_render(plan,target)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True,type=Path)
    parser.add_argument('--config-sha256',required=True)
    parser.add_argument('--output-dir',required=True,type=Path)
    args=parser.parse_args(argv)
    try:result=render(config_path=args.config,config_sha256=args.config_sha256,output_dir=args.output_dir)
    except (OSError,ValueError,KeyError,TypeError) as error:parser.exit(2,str(error)+'\n')
    print(json.dumps({'status':result['status'],'output_dir':str(args.output_dir)},sort_keys=True))


if __name__=='__main__':main()
