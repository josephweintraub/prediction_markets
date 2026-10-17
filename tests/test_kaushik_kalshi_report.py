"""Pure string/data fixtures only: no TeX, PDF, figure or other file writes."""
from copy import deepcopy
import unittest
from unittest.mock import patch

from analysis.kaushik_kalshi_replication import report as r
from tests.test_kaushik_replication_report import synthetic_estimates,suppress


def fixture():
    data=synthetic_estimates()
    data.update(schema_version='kaushik_kalshi_estimates_v1',sample_scope='all_available_dates',
                definitions={'duration_endpoint':'settlement_ts','combos':'excluded','weight':'one per native print'},
                sample={'baseline_rows':90000,'duration_tail_rows':18000},
                appendix_a2={'status':'unavailable_from_public_trade_prints'},
                sports={'status':'pending_not_estimated'})
    metadata={'first_execution_utc':'2021-06-30T20:09:14.185137Z','last_execution_utc':'2026-10-01T23:59:59Z',
              'source_rows':90010,'baseline_rows':90000,'markets':400,'normalized_claims':800,'clusters':360,
              'unique_event_clusters':350,'market_fallback_clusters':10,'tail_rows':18000,
              'duration_tail_rows':18000,'duration_tail_clusters':360,
              'categories':[{'category':'Sports','n_observations':45000,'share':.5},
                            {'category':'Native & Finance_1','n_observations':45000,'share':.5}],
              'exclusions':[{'reason':'eligible','rows':90000},{'reason':'invalid_size','rows':10}]}
    metadata.update(duration_exclusions=[{'reason':'eligible','rows':18000}],
        duration_coverage=[{'source_version':'fixture_native','tail_rows':18000,'duration_tail_rows':18000,
                            'first_utc_day':'2026-03-25','last_utc_day':'2026-03-26'}])
    return data,metadata


def gates(data):
    digest=r.summary_digest(data)
    manifest='a'*64
    return {'estimates_sha256':digest,'estimate_manifest_sha256':manifest,
            'estimate_acceptance':{'schema_version':'kaushik_kalshi_estimate_acceptance_v1',
                'status':'estimates_reopened_accepted','all_outputs_reopened':True,
                'estimates_sha256':digest,'manifest_sha256':manifest,'independent_saved_score_audit':'pending'},
            'score_audit':{'schema_version':'kaushik_kalshi_score_audit_v1','status':'saved_scores_reconciled',
                'estimates_sha256':digest,'estimate_manifest_sha256':manifest,
                'checks':{'scores':True,'covariance':True,'intervals':True,'support':True}}}


def sports_bundle(core):
    data=synthetic_estimates()['sports']
    data.update(schema_version='kaushik_kalshi_sports_estimates_v1',status='sports_estimates_complete',
        sample_scope='all_available_dates',candidate_rows=90000,eligible_rows=90000,
        definitions={'observation':'one admitted native trade print','weight':'one per native print',
            'cluster':'sport-namespaced provider game_key','endpoint_grace_seconds':0,
            'minimum_prints_per_cell':500,'minimum_games_per_cell':30,
            'phase':'pregame timestamp<actual_start_seconds; in_play timestamp>=actual_start_seconds; timestamp<=actual_end_seconds',
            'windows':[{'panel':panel,'key':key} for panel,key,_ in r.SPORT_WINDOWS]},
        input_binding={key:'1'*64 for key in ('input_manifest_sha256','input_acceptance_sha256','map_manifest_sha256','map_acceptance_sha256')},
        exclusions=[{'sport':sport,'reason':'eligible','rows':10000} for sport in r.SPORT_SCOPES[1:]],window_counts=[])
    data['input_binding']['timing_qualification']='ATP uses scheduled start plus archived match duration; other provider qualities remain as recorded.'
    data['timing_coverage']=[{'sport':sport,'timing_quality':'scheduled_start_plus_archived_duration' if sport=='atp' else 'fixture_provider_clock',
                              'rows':10000} for sport in r.SPORT_SCOPES[1:]]
    for scope in r.SPORT_SCOPES:
        n=4500 if scope=='pooled' else 500
        for panel,window,_ in r.SPORT_WINDOWS:
            data['window_counts'].append({'scope':scope,'window':window,'panel':panel,'n_observations':2*n,
                                          'n_games':360 if scope=='pooled' else 40})
    for row in data['profile_rows']:
        row['cell_support'][0]['cell']='D'+str(row['bin'])+':'+row['phase']
    for row in data['phase_rows']:
        phases=r.SPORT_PHASES if row['phase']=='in_play_minus_pregame' else (row['phase'],)
        for support,label in zip(row['cell_support'],[tail+':'+phase for phase in phases for tail in ('D1','D10')]): support['cell']=label
    for row in data['window_rows']:
        for support,tail in zip(row['cell_support'],('D1','D10')): support['cell']=tail+':'+row['window']
    digest=r.summary_digest(data)
    manifest='d'*64
    return {'summary':data,'sports_sha256':digest,'sports_manifest_sha256':manifest,
        'acceptance':{'schema_version':'kaushik_kalshi_sports_estimate_acceptance_v1','status':'sports_estimates_reopened_accepted',
            'all_outputs_reopened':True,'sports_sha256':digest,'manifest_sha256':manifest,'core_estimates_sha256':r.summary_digest(core)},
        'score_audit':{'schema_version':'kaushik_kalshi_sports_score_audit_v1','status':'saved_scores_reconciled',
            'sports_sha256':digest,'sports_manifest_sha256':manifest,'checks':{'independent_saved_game_scores':True}}}


class KalshiReportTests(unittest.TestCase):
    def setUp(self):
        self.data,self.metadata=fixture()

    def render(self,**options):
        return r.render_source(self.data,self.metadata,**{**gates(self.data),'draft':True,**options})

    def test_complete_core_tables_render_native_saved_values_and_end_with_evidence(self):
        source=self.render()
        for expected in ('Table 1.','Table 2A.','Table 2B.','Table 3A.','Table 3B.','Appendix A1.',
                         '90,010','90,000','18,000','360','0.1234','0.2345','0.3456',
                         'Native \\& Finance\\_1','\\texttt{settlement\\_ts}','\\begin{tabular}'):
            self.assertIn(expected,source)
        for band in range(1,11):
            self.assertEqual(sum(line.startswith(f'D{band} &') for line in source.splitlines()),1)
        self.assertIn('(0.50)',source)
        self.assertIn('[+0.27, +2.23]',source)
        self.assertTrue(source.endswith('\\end{document}\n'))
        self.assertNotIn('\\section{Conclusion}',source)
        self.assertNotIn('Interpretation limits',source)
        self.assertNotIn('Fingerprint',source)
        self.assertIn('Appendix A2 maker/taker comparison is unavailable',source)

    def test_immutable_pending_acceptance_is_superseded_only_by_completed_bound_audit(self):
        self.assertIn('Table 1.',self.render())
        for status in ('pending','complete','unknown'):
            supplied=gates(self.data)
            supplied['score_audit']['status']=status
            with self.subTest(status=status),self.assertRaisesRegex(r.ReportBlocked,'completed independent'):
                self.render(**supplied)
        supplied=gates(self.data)
        supplied['score_audit']['checks']['scores']=False
        with self.assertRaisesRegex(r.ReportBlocked,'checks incomplete'):
            self.render(**supplied)
        supplied=gates(self.data)
        supplied['score_audit']['estimate_manifest_sha256']='b'*64
        with self.assertRaisesRegex(r.ReportBlocked,'manifest/audit binding'):
            self.render(**supplied)

    def test_content_hash_and_original_reopened_acceptance_are_required(self):
        supplied=gates(self.data)
        self.data['table1']['profile_rows'][0]['CR0']['estimate']+=.001
        with self.assertRaisesRegex(r.ReportBlocked,'summary differs'):
            self.render(**supplied)
        supplied=gates(self.data)
        supplied['estimate_acceptance']['all_outputs_reopened']=False
        with self.assertRaisesRegex(r.ReportBlocked,'reopened estimate'):
            self.render(**supplied)

    def test_plot_descriptions_have_isolated_capped_intervals_and_no_suppressed_marks(self):
        row=self.data['table1']['profile_rows'][0]
        suppress(row,'empty_cell')
        spec=r.figure_specs(self.data)
        self.assertFalse(spec['connect_points'])
        panel=spec['panels'][0]
        self.assertEqual(panel['linestyle'],'none')
        self.assertEqual(panel['capsize'],3)
        self.assertEqual(panel['zero_reference'],0)
        self.assertEqual(panel['ticks'],list(range(1,11)))
        self.assertEqual(len(panel['points']),9)
        self.assertNotIn(1,[point['bin'] for point in panel['points']])
        self.assertEqual(panel['withheld'][0]['reasons'],['empty_cell'])
        source=self.render()
        self.assertIn('withheld',source)
        self.assertIn('empty cell',source)

    def test_complete_grid_and_disjoint_counts_fail_closed(self):
        self.data['table1']['profile_rows'].pop()
        with self.assertRaisesRegex(r.ReportBlocked,'ten-bin'):
            self.render()
        self.data,self.metadata=fixture()
        self.metadata['exclusions'][0]['rows']-=1
        with self.assertRaisesRegex(r.ReportBlocked,'waterfall'):
            self.render()
        self.data,self.metadata=fixture()
        self.metadata['categories'][0]['n_observations']-=1
        with self.assertRaisesRegex(r.ReportBlocked,'share/count'):
            self.render()

    def test_model_membership_and_control_contract_are_not_reinterpreted(self):
        self.data['table2'][0]['spec']['effects']=['price_code']
        with self.assertRaisesRegex(r.ReportBlocked,'specification differs'):
            self.render()
        self.data,self.metadata=fixture()
        self.data['table3']['L_gt1'][1]['n_observations']-=1
        with self.assertRaisesRegex(r.ReportBlocked,'common panel sample'):
            self.render()

    def test_full_report_release_blocks_pending_sports_and_legacy_payloads(self):
        with self.assertRaisesRegex(r.ReportBlocked,'full fresh report release'):
            self.render(draft=False)
        with self.assertRaisesRegex(r.ReportBlocked,'fresh native Kalshi sports JSON'):
            self.render(sports={'platform':'kalshi','source':'legacy_zip'})
        decision={'status':'user_approved_non_sports_scope','authorization_reference':'explicit user message'}
        source=self.render(draft=False,sports_release_decision=decision)
        self.assertNotIn('DRAFT',source)
        self.assertIn('explicit user scope decision',source)

    def test_fresh_sports_release_and_all_native_support_tables_are_pure(self):
        bundle=sports_bundle(self.data)
        before=deepcopy(bundle)
        with patch('builtins.open',side_effect=AssertionError('renderer attempted I/O')):
            source=self.render(draft=False,sports=bundle)
            specs=r.sports_figure_specs(bundle['summary'])
        self.assertNotIn('DRAFT',source)
        self.assertEqual(len(specs),20)
        for scope in r.SPORT_SCOPES:
            self.assertIn(scope+': complete fixed-bin support',source)
            self.assertIn(scope+': literal-window tail support',source)
            self.assertIn(scope+': joint tail contrasts',source)
        self.assertIn('500 prints and 30 games',source)
        self.assertIn('no endpoint grace',source)
        self.assertIn('ATP uses scheduled start plus archived match duration',source)
        self.assertIn('scheduled\\_start\\_plus\\_archived\\_duration',source)
        self.assertIn('Duration sample. Baseline-tail clock reconciliation',source)
        self.assertIn('UTC source partitions',source)
        self.assertTrue(source.endswith('\\end{document}\n'))
        self.assertEqual(bundle,before)

    def test_sports_receipt_pending_or_wrong_core_binding_blocks_release(self):
        bundle=sports_bundle(self.data)
        bundle['score_audit']['status']='pending'
        with self.assertRaisesRegex(r.ReportBlocked,'sports score audit'): self.render(sports=bundle)
        bundle=sports_bundle(self.data)
        bundle['acceptance']['core_estimates_sha256']='0'*64
        with self.assertRaisesRegex(r.ReportBlocked,'acceptance/core'): self.render(sports=bundle)
        bundle=sports_bundle(self.data)
        bundle['summary']['profile_rows'].pop()
        with self.assertRaisesRegex(r.ReportBlocked,'estimate grids'): self.render(sports=bundle)

    def test_sports_figures_omit_withheld_and_never_connect_bins_or_windows(self):
        data=sports_bundle(self.data)['summary']
        row=data['profile_rows'][0]
        suppress(row,'nonfinite_estimate_or_cluster_variance')
        specs=r.sports_figure_specs(data)
        panel=specs['sports_pooled_profiles']['panels'][0]
        self.assertEqual(len(panel['series'][0]['points']),9)
        self.assertEqual(panel['series'][0]['withheld'][0]['x'],1)
        for figure in specs.values():
            self.assertFalse(figure['connect_points'])
            for panel in figure['panels']:
                self.assertEqual(panel['zero_reference'],0)
                for series in panel['series']:
                    self.assertEqual(series['linestyle'],'none')
                    self.assertEqual(series['capsize'],3)

    def test_tail_only_clock_adapter_and_source_partition_dates_are_checked(self):
        self.metadata['duration_exclusions'].append({'reason':'not_tail_bin','rows':1})
        with self.assertRaisesRegex(r.ReportBlocked,'tail-only clock'): self.render()
        self.data,self.metadata=fixture()
        self.metadata['duration_coverage'][0]['first_utc_day']='2026-03-25T01:00:00Z'
        with self.assertRaisesRegex(r.ReportBlocked,'source-partition'): self.render()

    def test_portable_figure_paths_only_and_no_io_or_mutation(self):
        for filename in ('/tmp/a.pdf','../a.pdf','https://example.test/a.pdf','figures/a.png','figures//a.pdf'):
            with self.subTest(path=filename),self.assertRaisesRegex(r.ReportBlocked,'relative PDF'):
                self.render(figure_paths={'baseline':filename})
        before=deepcopy((self.data,self.metadata))
        with patch('builtins.open',side_effect=AssertionError('renderer attempted I/O')):
            source=self.render()
            spec=r.figure_specs(self.data)
        self.assertIn(r'\includegraphics[width=\linewidth]{figures/baseline_profiles.pdf}',source)
        self.assertEqual(spec['filename'],'figures/baseline_profiles.pdf')
        self.assertEqual((self.data,self.metadata),before)

    def test_metadata_preserves_observed_source_range_and_saved_claim_support(self):
        self.assertIn('2021-06-30T20:09:14.185137Z',self.render())
        self.assertIn('2,500',self.render())
        self.metadata['last_execution_utc']='2020-01-01T00:00:00Z'
        with self.assertRaisesRegex(r.ReportBlocked,'range is reversed'):
            self.render()

    def test_saved_influence_and_withholding_are_displayed_without_reestimation(self):
        slope=self.data['table2'][4]['slopes'][0]
        slope['influence']['flags']=['effective_clusters_below_30']
        suppressed=self.data['table3']['L_gt1'][0]['slopes'][0]
        suppress(suppressed,'rank_deficient_residualized_design')
        source=self.render()
        self.assertIn(r'+1.25$^{\dagger}$',source)
        self.assertIn('rank deficient residualized design',source)
        self.assertIn('withheld',source)
        self.assertIn('18,000',source)


if __name__=='__main__':
    unittest.main()
