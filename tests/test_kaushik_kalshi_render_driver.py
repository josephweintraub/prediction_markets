"""Saved JSON/string and fake-drawing tests; never author TeX or PDF files."""
from contextlib import ExitStack,nullcontext
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import patch

from scripts import render_kaushik_kalshi_report as driver
from analysis.kaushik_kalshi_replication import report
from tests.test_kaushik_kalshi_report import fixture,gates,sports_bundle
from tests.test_kaushik_replication_report import suppress


class Axis:
    def __init__(self):
        self.calls=[];self.transAxes='axes';self.spines=self
    def __getitem__(self,key):return self
    def set_visible(self,*args,**kwargs):self.calls.append(('visible',args,kwargs))
    def __getattr__(self,name):
        if name=='plot':raise AssertionError('connected plotting is forbidden')
        return lambda *args,**kwargs:self.calls.append((name,args,kwargs))


class Backend:
    def __init__(self):self.axes=[];self.saved=[];self.closed=[]
    def rc_context(self,options):self.options=options;return nullcontext()
    def subplots(self,rows,columns,**kwargs):
        self.subplot_kwargs=kwargs
        self.axes=[[Axis() for _ in range(columns)] for _ in range(rows)]
        return self,self.axes
    def tight_layout(self):pass
    def savefig(self,*args,**kwargs):self.saved.append((args,kwargs))
    def close(self,figure):self.closed.append(figure)


class KalshiRenderDriverTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory(prefix='kalshi-render-json-fixture-')
        self.addCleanup(self.folder.cleanup);self.root=Path(self.folder.name).resolve()
        # Simulate the real canonical namespace layout, not the Mac-only
        # analysis/__init__.py. Source bytes remain exact temporary copies.
        self.source_root=self.root/'canonical-source-fixture'
        for relative in driver.SOURCE_FILES:
            path=self.source_root/relative;path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes((driver.REPO/relative).read_bytes())
        self.origins=ExitStack();self.addCleanup(self.origins.close)
        self.origins.enter_context(patch.object(driver,'REPO',self.source_root))
        self.namespace=ModuleType('analysis');self.namespace.__file__=None
        self.namespace.__path__=[str(self.source_root/'analysis')]
        self.origins.enter_context(patch.dict(sys.modules,{'analysis':self.namespace}))
        for name in ('analysis.kaushik_kalshi_replication','analysis.kaushik_kalshi_replication.report',
                     'analysis.kaushik_kalshi_replication.inputs','analysis.kaushik_polymarket_replication',
                     'analysis.kaushik_polymarket_replication.render_report',
                     'analysis.kaushik_polymarket_replication.exchange_filter_contract'):
            module=sys.modules[name]
            relative=name.replace('.','/')+('/__init__.py' if hasattr(module,'__path__') else '.py')
            self.origins.enter_context(patch.object(module,'__file__',str(self.source_root/relative)))
        core,metadata=fixture();gate=gates(core);sports=sports_bundle(core)
        core_record=self.save('core_estimates',core)
        native={'manifest':{'sha256':'1'*64},'acceptance':{'sha256':'2'*64}}
        manifest={'schema_version':'kaushik_kalshi_estimate_stage_v1','status':'estimates_complete',
            'estimates_sha256':core_record['sha256'],'source':{'head':'a'*40},'input_binding':native}
        manifest_record=self.save('core_manifest',manifest)
        accepted={**gate['estimate_acceptance'],'manifest_sha256':manifest_record['sha256'],'source_head':'a'*40}
        accepted_record=self.save('core_acceptance',accepted)
        audit={**gate['score_audit'],'estimate_manifest_sha256':manifest_record['sha256'],
            'estimate_acceptance_sha256':accepted_record['sha256']}
        self.files={'core_estimates':core_record,'core_manifest':manifest_record,'core_acceptance':accepted_record,
            'core_score_audit':self.save('core_score_audit',audit),'reader_metadata':self.save('reader_metadata',metadata),
            'reader_provenance':self.save('reader_provenance',{'schema_version':'kaushik_kalshi_reader_metadata_provenance_v1',
                'accepted_manifest_sha256':'1'*64,'accepted_acceptance_sha256':'2'*64,'raw_tape_read':False,'saved_file_writes':False})}
        sports['summary']['input_binding']['input_acceptance_sha256']='2'*64
        sports_record=self.save('sports_estimates',sports['summary'])
        sports_manifest={'schema_version':'kaushik_kalshi_sports_estimate_stage_v1','status':'sports_estimates_complete',
            'sports_sha256':sports_record['sha256'],'core_estimates_sha256':core_record['sha256'],
            'input_binding':sports['summary']['input_binding']}
        sm_record=self.save('sports_manifest',sports_manifest)
        sa={**sports['acceptance'],'sports_sha256':sports_record['sha256'],'manifest_sha256':sm_record['sha256']}
        score={**sports['score_audit'],'sports_sha256':sports_record['sha256'],'sports_manifest_sha256':sm_record['sha256']}
        self.files.update(sports_estimates=sports_record,sports_manifest=sm_record,
            sports_acceptance=self.save('sports_acceptance',sa),sports_score_audit=self.save('sports_score_audit',score))
        self.config={'schema_version':'kaushik_kalshi_report_config_v1','status':'reviewed','files':self.files,
            'canonical_sources':{'head':'b'*40,'files':{relative:{'bytes':(driver.REPO/relative).stat().st_size,
                'sha256':driver._sha(driver.REPO/relative)} for relative in driver.SOURCE_FILES}}}

    def save(self,name,value):
        path=self.root/(name+'.json');raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
        path.write_bytes(raw)
        return {'path':str(path),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}

    def rewrite(self,role,change):
        value=json.loads(Path(self.files[role]['path']).read_text());change(value);self.files[role]=self.save(role,value)

    def prepare(self):
        config=self.save('config',self.config)
        return driver.prepare_render(config_path=config['path'],config_sha256=config['sha256'])

    def test_complete_accepted_json_prepares_portable_source_and_21_vector_specs_without_writes(self):
        before=set(self.root.iterdir());plan=self.prepare()
        self.assertIn('Table 1.',plan['source']);self.assertIn('sports_atp_profiles.pdf',plan['source'])
        self.assertNotIn('DRAFT',plan['source']);self.assertEqual(len(plan['figure_specs']),21)
        self.assertEqual(plan['canonical_sources']['head'],'b'*40)
        self.assertEqual(len(plan['bindings']),1+len(driver.SOURCE_FILES)+10)
        self.assertEqual(set(self.root.iterdir())-before,{self.root/'config.json'})
        self.assertFalse(any(p.suffix in ('.tex','.pdf') for p in self.root.rglob('*')))

    def test_real_canonical_namespace_absence_and_two_package_initializers_are_bound(self):
        self.assertNotIn('analysis/__init__.py',driver.SOURCE_FILES)
        self.assertEqual(len(driver.SOURCE_FILES),8)
        self.assertFalse((self.source_root/'analysis/__init__.py').exists())
        for relative in ('analysis/kaushik_kalshi_replication/__init__.py',
                         'analysis/kaushik_polymarket_replication/__init__.py'):
            self.assertIn(relative,driver.SOURCE_FILES);self.assertTrue((self.source_root/relative).is_file())
        plan=self.prepare()
        self.assertEqual(plan['namespace_packages'],{'analysis':{'path':str(self.source_root/'analysis'),
            'initializer':'absent','search_paths':[str(self.source_root/'analysis')]}})

    def test_namespace_extra_misdirected_or_regular_package_origins_are_refused(self):
        for paths in ([str(self.source_root/'analysis'),'/different/analysis'],['/different/analysis']):
            with self.subTest(paths=paths),patch.object(self.namespace,'__path__',paths),\
                 self.assertRaisesRegex(driver.RenderBlocked,'namespace origin'):self.prepare()
        with patch.object(self.namespace,'__file__',str(self.source_root/'analysis/__init__.py')),\
             self.assertRaisesRegex(driver.RenderBlocked,'namespace origin'):self.prepare()
        plan=self.prepare()
        (self.source_root/'analysis/__init__.py').write_text('"""Unexpected regular-package marker: negative fixture only."""\n')
        with self.assertRaisesRegex(driver.RenderBlocked,'initializer must be absent'):self.prepare()
        with self.assertRaisesRegex(driver.RenderBlocked,'initializer must be absent'):driver._recheck(plan['bindings'])

    def test_publication_boundary_is_mocked_and_existing_target_never_reused(self):
        record=self.save('config',self.config);target=self.root/'new-report'
        with patch.object(driver,'_publish_render',return_value={'status':'fixture_not_authored'}) as publish:
            result=driver.render(config_path=record['path'],config_sha256=record['sha256'],output_dir=target)
            self.assertEqual(result['status'],'fixture_not_authored');publish.assert_called_once()
        self.assertFalse(target.exists())
        target.mkdir()
        with patch.object(driver,'prepare_render') as unopened,self.assertRaisesRegex(driver.RenderBlocked,'new absolute immutable'):
            driver.render(config_path=record['path'],config_sha256=record['sha256'],output_dir=target)
        unopened.assert_not_called()

    def test_config_hash_source_identity_and_import_origin_fail_closed(self):
        record=self.save('config',self.config)
        with self.assertRaisesRegex(driver.RenderBlocked,'hash/drift'):
            driver.prepare_render(config_path=record['path'],config_sha256='0'*64)
        key=driver.SOURCE_FILES[0];self.config['canonical_sources']['files'][key]['sha256']='f'*64
        with self.assertRaisesRegex(driver.RenderBlocked,'hash/drift'):self.prepare()
        self.config['canonical_sources']['files'][key]['sha256']=driver._sha(driver.REPO/key)
        with patch.object(report,'__file__','/different/source/report.py'),self.assertRaisesRegex(driver.RenderBlocked,'source origin'):
            self.prepare()

    def test_duplicate_nonfinite_unknown_config_and_incomplete_sports_are_refused(self):
        for raw in ('{"a":1,"a":2}','{"a":NaN}'):
            path=self.root/'bad.json';path.write_text(raw)
            with self.subTest(raw=raw),self.assertRaises(driver.RenderBlocked):
                driver.prepare_render(config_path=path,config_sha256=hashlib.sha256(raw.encode()).hexdigest())
        self.config['draft']=True
        with self.assertRaisesRegex(driver.RenderBlocked,'exact reviewed full-report'):self.prepare()
        del self.config['draft'];del self.files['sports_score_audit']
        with self.assertRaisesRegex(driver.RenderBlocked,'all ten'):self.prepare()

    def test_hash_bound_json_drift_and_oversized_inventory_reject_before_publish(self):
        Path(self.files['reader_metadata']['path']).write_bytes(b'changed')
        with self.assertRaisesRegex(driver.RenderBlocked,'byte size'):self.prepare()
        self.files['reader_metadata']['bytes']=driver.TOTAL_JSON_LIMIT+1
        with self.assertRaisesRegex(driver.RenderBlocked,'total saved report JSON'):self.prepare()

    def test_reader_core_input_binding_and_score_acceptance_binding_are_exact(self):
        self.rewrite('reader_provenance',lambda x:x.update(accepted_acceptance_sha256='3'*64))
        with self.assertRaisesRegex(driver.RenderBlocked,'reader/core accepted input'):self.prepare()
        self.rewrite('reader_provenance',lambda x:x.update(accepted_acceptance_sha256='2'*64))
        self.rewrite('core_score_audit',lambda x:x.update(estimate_acceptance_sha256='4'*64))
        with self.assertRaisesRegex(driver.RenderBlocked,'score-audit acceptance'):self.prepare()

    def test_existing_full_release_gates_not_bypassed(self):
        self.rewrite('core_score_audit',lambda x:x.update(status='pending'))
        with self.assertRaisesRegex(report.ReportBlocked,'completed independent'):self.prepare()
        self.rewrite('core_score_audit',lambda x:x.update(status='saved_scores_reconciled'))
        self.rewrite('reader_metadata',lambda x:x.pop('yearly_coverage'))
        with self.assertRaisesRegex(report.ReportBlocked,'bound yearly'):self.prepare()

    def test_sports_full_release_and_same_native_input_bindings_are_required(self):
        self.rewrite('sports_score_audit',lambda x:x.update(status='pending'))
        with self.assertRaisesRegex(report.ReportBlocked,'independently bound sports score audit'):self.prepare()
        self.rewrite('sports_score_audit',lambda x:x.update(status='saved_scores_reconciled'))
        sports=json.loads(Path(self.files['sports_estimates']['path']).read_text())
        sports['input_binding']['input_manifest_sha256']='3'*64
        self.files['sports_estimates']=self.save('sports_estimates',sports)
        self.rewrite('sports_manifest',lambda x:x.update(input_binding=sports['input_binding'],
            sports_sha256=self.files['sports_estimates']['sha256']))
        with self.assertRaisesRegex(driver.RenderBlocked,'reader/sports accepted input'):self.prepare()

    def test_source_or_input_drift_after_prepare_is_detected(self):
        plan=self.prepare();path=Path(self.files['reader_metadata']['path']);path.write_bytes(path.read_bytes()+b' ')
        with self.assertRaisesRegex(driver.RenderBlocked,'render input/source drift'):driver._recheck(plan['bindings'])

    def test_child_import_origin_drift_after_prepare_is_rejected_even_with_identical_bytes(self):
        plan=self.prepare();foreign=self.root/'foreign_report.py'
        expected=self.source_root/'analysis/kaushik_kalshi_replication/report.py'
        foreign.write_bytes(expected.read_bytes())
        self.assertEqual(driver._sha(foreign),driver._sha(expected))
        with patch.object(report,'__file__',str(foreign)),\
             self.assertRaisesRegex(driver.RenderBlocked,'imported renderer source origin differs'):
            driver._recheck(plan['bindings'])

    def test_fake_backend_draws_only_saved_isolated_capped_intervals(self):
        core,_=fixture();suppress(core['table1']['profile_rows'][0],'fixture withheld')
        spec=report.figure_specs(core);backend=Backend()
        driver.draw_figure(spec,self.root/'not-created.pdf',pyplot=backend)
        first=[call for call in backend.axes[0][0].calls if call[0]=='errorbar']
        self.assertEqual(len(first),1);self.assertNotIn(1,first[0][1][0]);self.assertEqual(len(first[0][1][0]),9)
        for axes in backend.axes:
            for axis in axes:
                for name,args,kwargs in axis.calls:
                    if name=='errorbar':self.assertEqual((kwargs['linestyle'],kwargs['capsize']),('none',3))
                    if name=='axhline':self.assertEqual(args,(0,))
        saved=backend.saved[0][1]
        self.assertEqual(saved['format'],'pdf');self.assertIsNone(saved['metadata']['CreationDate'])
        self.assertIsNone(saved['metadata']['ModDate']);self.assertEqual(len(backend.closed),1)
        self.assertFalse((self.root/'not-created.pdf').exists())

    def test_window_geometry_keeps_literal_families_and_common_outcome_axes(self):
        core,_=fixture();spec=report.sports_figure_specs(sports_bundle(core)['summary'])['sports_pooled_windows']
        geometry=driver.plot_geometry(spec);self.assertEqual(len(geometry),6)
        self.assertEqual([x['ticks'] for x in geometry[:3]],[list(range(5)),list(range(5,10)),list(range(10,14))])
        self.assertEqual(len({x['ylim'] for x in geometry[:3]}),1)
        backend=Backend();driver.draw_figure(spec,self.root/'not-created.pdf',pyplot=backend)
        self.assertEqual(len(backend.axes),2);self.assertEqual(len(backend.axes[0]),3)
        self.assertFalse((self.root/'not-created.pdf').exists())

    def test_window_ticks_use_exact_compact_two_line_labels_at_unchanged_positions(self):
        core,_=fixture();spec=report.sports_figure_specs(sports_bundle(core)['summary'])['sports_pooled_windows']
        before=deepcopy(spec)
        expected=[['>24\nh','24-6\nh','6-1\nh','60-15\nmin','15-0\nmin'],
                  ['0-15\nmin','15-30\nmin','30-60\nmin','1-2\nh','2+\nh'],
                  ['60-30\nmin','30-15\nmin','15-5\nmin','5-0\nmin']]
        ticks=[list(range(5)),list(range(5,10)),list(range(10,14))]
        backend=Backend();driver.draw_figure(spec,self.root/'not-created.pdf',pyplot=backend)
        for row in backend.axes:
            for column,axis in enumerate(row):
                calls=[(args,kwargs) for name,args,kwargs in axis.calls if name=='set_xticks']
                self.assertEqual(len(calls),1)
                self.assertEqual(calls[0][0],(ticks[column],expected[column]))
                self.assertEqual(calls[0][1]['fontsize'],11)
        self.assertEqual(spec,before)
        self.assertFalse((self.root/'not-created.pdf').exists())

    def test_window_fonts_remain_legible_at_actual_report_linewidth(self):
        plan=self.prepare()
        self.assertIn(r'\documentclass[11pt]{article}',plan['source'])
        self.assertIn(r'\usepackage[margin=0.65in]{geometry}',plan['source'])
        self.assertIn(r'\includegraphics[width=\linewidth]',plan['source'])
        spec=next(spec for spec in plan['figure_specs'] if spec.get('family')=='windows')
        backend=Backend();driver.draw_figure(spec,self.root/'not-created.pdf',pyplot=backend)
        self.assertEqual(backend.subplot_kwargs['figsize'],(10,5))
        fonts=[backend.options[key] for key in ('font.size','axes.labelsize','axes.titlesize','xtick.labelsize','ytick.labelsize')]
        fonts.extend(kwargs['fontsize'] for row in backend.axes for axis in row
            for name,_,kwargs in axis.calls if name=='set_xticks')
        self.assertEqual(min(fonts),11)
        letter_linewidth=8.5-2*.65
        self.assertAlmostEqual(letter_linewidth,7.2)
        self.assertGreaterEqual(min(fonts)*letter_linewidth/backend.subplot_kwargs['figsize'][0],7)
        self.assertFalse((self.root/'not-created.pdf').exists())

    def test_all_withheld_panel_is_not_plotted_at_zero(self):
        core,_=fixture()
        for row in core['table1']['profile_rows']:
            if row['outcome']=='payoff_cents':suppress(row,'fixture withheld')
        backend=Backend();driver.draw_figure(report.figure_specs(core),self.root/'not-created.pdf',pyplot=backend)
        calls=backend.axes[0][0].calls;self.assertFalse(any(name=='errorbar' for name,_,_ in calls))
        self.assertTrue(any(name=='text' and 'No supported cells' in args for name,args,_ in calls))

    def test_bad_plot_specs_refuse_connected_or_withheld_marks(self):
        core,_=fixture();spec=report.figure_specs(core)
        bad=deepcopy(spec);bad['connect_points']=True
        with self.assertRaisesRegex(driver.RenderBlocked,'connected'):driver.plot_geometry(bad)
        bad=deepcopy(spec);bad['panels'][0]['capsize']=0
        with self.assertRaisesRegex(driver.RenderBlocked,'isolated capped'):driver.plot_geometry(bad)
        bad=deepcopy(spec);bad['panels'][0]['withheld']=[{'bin':1}]
        with self.assertRaisesRegex(driver.RenderBlocked,'withheld estimate'):driver.plot_geometry(bad)


if __name__=='__main__':unittest.main()
