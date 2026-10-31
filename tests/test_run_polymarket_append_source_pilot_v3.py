"""Tiny local composition/faults only, never a production-host bypass."""
import ast
from collections import namedtuple
from contextlib import ExitStack
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
import zlib
import base64

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import run_polymarket_append_source_pilot_v3 as m
from tests import test_decode_polymarket_append_legacy_v2_v2 as bridge_fixtures

BASE=Path('output/polymarket_oct1_finalization_2026-10-27_v1')
PRIOR_ROOT_FIXTURE_BASE64='eyJSVU5fcGx1c19GVUxMX3Byb2R1Y2VyX3N0ZG91dF9ieXRlcyI6OTE1NzUsImFjdHVhbF93YWl0X2FuZF9zaGFyZWRfVjVfbGVkZ2VyX2JvdW5kX2xvY2FsbHkiOnRydWUsImNhbm9uaWNhbF9SVU5fYnl0ZXMiOjg5NDY2LCJjaGVja2VyX2VudmVsb3BlIjp7ImJ5dGVzIjoxMzg0MTYyLCJzaGEyNTYiOiJmOTZkOTMxZTI3YzljMWZiYjRmYzI3NTIwNjI5ZTFhNTIyYzBhOTdlZGM2NzQxZTdhN2EzMGZmZDlhZjYyMWI4In0sImNoZWNrZXJfc291cmNlIjp7ImJ5dGVzIjo2NjIzNywic2hhMjU2IjoiN2IxNDcxNWYyYmU0MDQ0OWE2OTY1NTc1NzJhMjA0ZTRjYjVlZmJiNTVlMDlmYTA4Mzc2NGIxMzFmYjEzNTE3MCJ9LCJjdXJyZW50X293bmVkX2RyYWluX3ZlcmlmaWVkIjpmYWxzZSwiZmluYWxfZGF0YXNldF9hZG1pdHRlZCI6ZmFsc2UsImZsb3ciOnsiZnVsbF9wcm9kdWNlcl9zdGRvdXQiOnsiYnl0ZXMiOjIxMDksInNoYTI1NiI6ImY4ZmQxM2ZhZWI2ZDQ1MDJhZDhjNmNkZTFhNDk5Y2JkMzBiMmJmZGNmZTBjNzkwODRiNDQ2MzU1ZjFlYThlMWYifSwicHJlZmxpZ2h0X2VudmVsb3BlIjp7ImJ5dGVzIjoxNDgxNzksInNoYTI1NiI6ImRmY2EwZTcyNWVkMTFjYzk0ZWM5ODJlMTg3MzJhYjYwZTRlNmM0MzA4ZjE1MzliM2JiMTJkY2QwMWNhMDhmNWIifSwicHJpb3JfdGVybWluYWxfcmVvcGVuX2ZhaWx1cmUiOnsiYnl0ZXMiOjIwMjIsInNoYTI1NiI6IjkyMzg4MTY5MjM4MzUxNThjZDA4NzA3Y2NlMTY1M2ZhZWM3YzQ0YTFjODBmY2IzNjg0MWEzYTllZDgwNjJlYmEifSwicm9vdF9mbG93X2xvZyI6eyJieXRlcyI6MTE0NDIwLCJzaGEyNTYiOiJkNDAyZWM0ZjcyZGY2ZGY4N2U1YWY0NDAxMjgxZmEwNDU1NDEwN2ZkOGM2ODhjMTRkYTRmNmU3NGRkMTA0MDhjIn0sInNoYXJlZF9WNV9yZWFkX2J5dGVzIjoxMTc0NjI0MCwid2FpdCI6eyJPU19yZWFkX2J5dGVzIjoxOTUwNDYsImVsYXBzZWRfc2Vjb25kcyI6MC4wMjEzMTM2MzQ5OTkzODc0MzIsImZyb3plbl9WNV9pbnZvY2F0aW9uc19pbnRlbmRlZCI6MSwibGFzdF9vZmZfYW5jZXN0cnlfU1NIX2NvdW50IjowLCJwb2xscyI6MSwic2NoZWR1bGluZ19vbmx5X25vdF9ob3N0X2FkbWlzc2lvbiI6dHJ1ZSwic2NoZW1hX3ZlcnNpb24iOiJyb290X3plcm9fb2ZmX2FuY2VzdHJ5X1NTSF93YWl0X3YxIiwic3RhdHVzIjoiemVyb19vYnNlcnZlZF9iZWZvcmVfc2luZ2xlX1Y1In19LCJmdWxsX3Byb2R1Y2VyX3N0ZG91dF9hdXRoZW50aWNhdGVkX2xvY2FsbHkiOnRydWUsIm5ld19yZXNvdXJjZV9hZG1pc3Npb24iOmZhbHNlLCJwaHlzaWNhbF9yb290X29yaWdpbmFsc19yZW9wZW5lZF9iZWZvcmVfYW5kX2FmdGVyIjo5LCJyZW1vdGVfaGVscGVyX2ZpbGVzX3dyaXR0ZW4iOmZhbHNlLCJyb290X2N1cnJlbnRfZXhhY3RfYmlydGhfZ3JvdXBfc2Vzc2lvbl9hbmRfZnVsbF9jZW5zdXNfZHJhaW5fcmVxdWlyZWQiOnRydWUsInNjaGVtYV92ZXJzaW9uIjoicG9seW1hcmtldF9hcHBlbmRfcGlsb3Rfcm9vdF90ZXJtaW5hbF9pbnRlZ3JhdGlvbl92MiIsInN0YXR1cyI6InJlYWRfb25seV90ZXJtaW5hbF9yZW9wZW5fdmVyaWZpZWRfbm90X2ZpbmFsX2FkbWlzc2lvbiIsInRlcm1pbmFsX2NoZWNrZXJfc3Rkb3V0Ijp7ImJ5dGVzIjoyNzMzLCJzaGEyNTYiOiJjMzJiM2I4NGZiYTk2NDEzYjY2ZTlhYmFjMDRiMzE1MzY0MDczZGZhMWM3Mjk2YmEyMDU4ZDBkNjBkYjM2NGE1In0sInRlcm1pbmFsX2NoZWNrZXJfc3Rkb3V0X1VURjgiOiJ7XG4gIFwiUlVOX2RpcmVjdG9yeV9mc3luY19wZXJmb3JtZWRcIjogdHJ1ZSxcbiAgXCJSVU5fZmlsZXNcIjoge1xuICAgIFwiQ09NUExFVEUuanNvblwiOiB7XG4gICAgICBcImJ5dGVzXCI6IDQ5MSxcbiAgICAgIFwic2hhMjU2XCI6IFwiMmVjMjEwZjg0YTYyMjhkNThjNjhlNGQ0NzYyYWIxMWRiNWM1ZTQxYmQyY2YzNWYzYTQ4ODg2MmVlMTYyOWYzY1wiXG4gICAgfSxcbiAgICBcIlNUQVJULmpzb25cIjoge1xuICAgICAgXCJieXRlc1wiOiA0NDE3LFxuICAgICAgXCJzaGEyNTZcIjogXCIxMmQzNTkzODkzYTk3ZDhmZDQ2Y2ZkYjkxNDdhMzhmNWM4NWI2MmU0NGYyNzJmN2ViMDk4ZTQ0MmE0MTA3NTA0XCJcbiAgICB9LFxuICAgIFwicHJvZmlsZS5qc29uXCI6IHtcbiAgICAgIFwiYnl0ZXNcIjogODQ1NTgsXG4gICAgICBcInNoYTI1NlwiOiBcIjVmYTAyMTliODM5MjdkZGQzYjVlMzRiMTYwMTg3ODY1ODQ5OWQ0ZGJiNDY0MGVlODM3ZTJhOTgwNTM4Y2RhZDdcIlxuICAgIH1cbiAgfSxcbiAgXCJTU0hfY2xhc3NpZmllcl9vcl9leGVtcHRpb25fYWRkZWRcIjogZmFsc2UsXG4gIFwiYWRtaXNzaW9uX3NoYTI1NlwiOiBcIjRiZWRjMGYxNzc2N2VhNTA1YmYxYzgyZjhmYzA3OTM2MjAwMTNjZTVhNjA5NTg4ZjNiYTI2ZjI1YzllYjhkY2VcIixcbiAgXCJhdXRoX2xvZ19zdWJzdHJpbmdzX3VzZWRfZm9yX2FkbWlzc2lvblwiOiBmYWxzZSxcbiAgXCJhdXRoZW50aWNhdGVkX2VtYmVkZGVkX2xvY2FsX29yaWdpbmFsc1wiOiA5LFxuICBcImJvb3RfaWRcIjogXCJlZmZhMDA4Ni01NmU4LTRjMzAtOWNhZi1iNTRlZjAyNDdiODBcIixcbiAgXCJjb250cm9sX2ZpbGVzX3Jlb3BlbmVkXCI6IDMxLFxuICBcImRlbWFuZGVkX2tleXNfcHJvZHVjZXJfcmVjZWlwdFwiOiAyMDAsXG4gIFwiZGVyaXZlZF9zZWxlY3RlZF9jb250cm9sX2NvdW50XCI6IDgsXG4gIFwiZGVyaXZlZF9zZWxlY3RlZF9jb250cm9sX3JlZnNfc2hhMjU2XCI6IFwiM2E2MTQ3YjkyYTQ5OGYxMjcyZWQyMzdiYWEwNTg5OWQ0ZGUwMGQ2YjUyNGU2NTVjYWEzZGZhMzU1MTUzMTc5Y1wiLFxuICBcImZ1bGxfcHJvZHVjZXJfc3Rkb3V0X2F1dGhlbnRpY2F0ZWRfaGVyZVwiOiBmYWxzZSxcbiAgXCJmdWxsX3Byb2R1Y2VyX3N0ZG91dF9iaW5kaW5nX2FuZF9SVU5fcGx1c19mdWxsX3N0ZG91dF9jYXBfcmVxdWlyZWRcIjogdHJ1ZSxcbiAgXCJoaXN0b3JpY2FsX2ZhaWxlZF9wcmVmbGlnaHRfbm90X2N1cnJlbnRfcGlsb3RfZHJhaW5cIjogdHJ1ZSxcbiAgXCJpbmRlcGVuZGVudF9ieXRlc19ieV9raW5kXCI6IHtcbiAgICBcIkdpdF9zb3VyY2VfcmVvcGVuX3Jlc2VydmVkXCI6IDE4MzE1MjAsXG4gICAgXCJhdXRoZW50aWNhdGVkX3N0ZGluXCI6IDEzODQxNjMsXG4gICAgXCJjb250cm9sXCI6IDk5OTE0NDAsXG4gICAgXCJjdXJyZW50X2Jvb3RcIjogMTI5LFxuICAgIFwiZW1iZWRkZWRfcm9vdF9jb250cm9sc19maW5hbFwiOiA1MDMyMDAsXG4gICAgXCJmaW5hbF9jb250cm9sXCI6IDUwNTM0OTQsXG4gICAgXCJwb3N0X2ZzeW5jX1JVTl9yZW9wZW5cIjogODk0NjksXG4gICAgXCJzZWxlY3RlZF9wYXlsb2FkX2Z1bGxfU0hBXCI6IDIzNzY5NzM1XG4gIH0sXG4gIFwiaW5kZXBlbmRlbnRfZWxhcHNlZF9zZWNvbmRzXCI6IDEuMjAyNjY1MTAyOTk5MTYyNixcbiAgXCJpbmRlcGVuZGVudF9yZWFkX2J5dGVzXCI6IDQyNjIzMTUwLFxuICBcImxvY2FsX29yaWdpbmFsc19waHlzaWNhbGx5X3Jlb3BlbmVkX2hlcmVcIjogZmFsc2UsXG4gIFwibG9jYWxfcGh5c2ljYWxfb3JpZ2luYWxfcmVvcGVuX3JlcXVpcmVkXCI6IHRydWUsXG4gIFwibmV3X3Jlc291cmNlX2FkbWlzc2lvblwiOiBmYWxzZSxcbiAgXCJvYnNlcnZlZF9hYnNlbnRfY29udHJvbHNfcmVjaGVja2VkXCI6IDQsXG4gIFwib3duZWRfcHJvY2Vzc19kcmFpbl92ZXJpZmllZFwiOiBmYWxzZSxcbiAgXCJyYXdfZmlsZXNfcmVoYXNoZWRcIjogNCxcbiAgXCJyYXdfcm93c19wcm9kdWNlcl9yZWNlaXB0XCI6IDIzODg3LFxuICBcInJlYWRfYnl0ZXNfYXJlX2NvbnNlcnZhdGl2ZV9jaGFyZ2VzXCI6IHRydWUsXG4gIFwicmVhZF9jYXBfYnl0ZXNcIjogMjAwMDAwMDAwMCxcbiAgXCJyb290X2FjdHVhbF93YWl0X291dGNvbWVfYXV0aGVudGljYXRlZF9oZXJlXCI6IGZhbHNlLFxuICBcInJvb3RfYWN0dWFsX3dhaXRfb3V0Y29tZV9iaW5kaW5nX3JlcXVpcmVkXCI6IHRydWUsXG4gIFwicm9vdF9wcmVmbGlnaHRfY2Fycnlfc2Vjb25kc1wiOiAyMjEwMDgsXG4gIFwicm9vdF9wcmVmbGlnaHRfY29udGludWF0aW9uX3NoYTI1NlwiOiBcIjQ0NjJmOTQxZDYzZTJlOGUxMTg3NzExZDJjNmQwZTAyMmVmNjU0YTRkMmVlMDg4ODcyOTRhNWY2YzgwMWRhZjVcIixcbiAgXCJydW50aW1lX2NhcF9zZWNvbmRzXCI6IDYwMCxcbiAgXCJzY2hlbWFfdmVyc2lvblwiOiBcInBvbHltYXJrZXRfYXBwZW5kX3BpbG90X3Rlcm1pbmFsX3Jlb3Blbl92NFwiLFxuICBcInNlbGVjdGVkX2hlYWRlcl9maWxlc19yZWhhc2hlZFwiOiAzLFxuICBcInNlbGVjdGVkX2hlYWRlcl9pbmRpY2VzXCI6IFtcbiAgICAyNzEsXG4gICAgMjczLFxuICAgIDMyOFxuICBdLFxuICBcInNvdXJjZV9jb21taXRcIjogXCJhZDYxMzNlMGRjMGNkYjBhYjNjODQ4MzAxODYwYzFkZjE3NjRlNmI5XCIsXG4gIFwic3RhdHVzXCI6IFwidGVybWluYWxfYnl0ZXNfaWRlbnRpdGllc19hbmRfcmVjZWlwdF9sYXdzX3ZlcmlmaWVkX25vdF9hZG1pdHRlZFwiLFxuICBcInRlcm1pbmFsX3NoYTI1NlwiOiBcImQwMjk3YWU0OGIwZTM4OGFkOTlhNWI2ZjhlOTBjMjQ5MzhmNzY2ODMyNjIwZWVmZjFlYjhkNWUzZDBhNzY2N2FcIixcbiAgXCJ0cmFkZV9yb3dzX29yX2Nsb2NrX2pvaW5fcmVwbGF5ZWRcIjogZmFsc2UsXG4gIFwid2hvbGVfZGF0YXNldF9vcl9mdWxsX2hlYWRlcl9yb3V0aW5nX3ZlcmlmaWVkXCI6IGZhbHNlXG59XG4ifQo='


class PilotTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='append_pilot_fixture_')
        self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve()
        self.space=patch.object(m.host.shutil,'disk_usage',return_value=namedtuple('Space','total used free')(10**12,0,10**12))
        self.space.start();self.addCleanup(self.space.stop)

    def meter(self):
        now=time.monotonic();return m.Meter(now,now+100,1)

    def file(self,rows,name,schema,index,group=None):
        path=self.root/name
        pq.write_table(pa.Table.from_pylist(rows,schema=schema),path,compression='zstd',row_group_size=41)
        raw=path.read_bytes();ref={'path':str(path),'bytes':len(raw),'sha256':m.digest(raw),'rows':len(rows),'source_file_index':index}
        if group is not None:ref.update(source_group=group)
        return ref

    def raw(self,number,index=0,*,count=1):
        row=bridge_fixtures.NativeBridgeTests().row(index%4)
        row.update(blockNumber=hex(number),block_number=number,blockHash='0x'+format(number,'064x'),
                   blockTimestamp='not-an-execution-clock',provider_block_timestamp_present=True)
        rows=[deepcopy(row) for _ in range(count)]
        ref=self.file(rows,f'raw-{index}.parquet',m.native.native_schema(),index,'append_probe_v2' if index==0 else 'append_campaign_v2')
        return ref|{'first_block':number,'last_block':number},rows

    def header(self,numbers,index=0,*,timestamp=1790000000):
        rows=[{'block_number':n,'block_hash':bytes.fromhex(format(n,'064x')),'parent_hash':bytes.fromhex(format(n-1,'064x')),
               'timestamp':timestamp+i} for i,n in enumerate(numbers)]
        return self.file(rows,f'header-{index}.parquet',m.clocks.header_schema(),index)

    def project(self,raws,headers,indices=None):
        with patch.object(m,'DATA_ROOT',self.root),patch.object(m,'_header_reuse_contract',return_value={'fixture_only':True}):
            return m._project_pilot(raws,headers,[r['source_file_index'] for r in headers] if indices is None else indices,self.meter(),{},())

    def test_actual_parent_carry_is_single_existing_full_envelope(self):
        self.assertEqual(m.PARENT,{'bytes':3231,'sha256':'3d50b848938b54e4d9ebdbe74aeb7aebd83c8dfdb6a743527e20031ea8d834ba'})
        self.assertEqual(m.PARENT_END,'2026-10-31T01:56:30Z')
        self.assertEqual(m.OLD_PARENT_END,'2026-10-31T00:36:30Z')

    def test_actual_parent_derived_four_selection_not_a01_guess_or_full_scope(self):
        seed=BASE/'append_only_source_controls_and_fourth_profile_drain_2026-10-30_v1.json'
        self.assertEqual(seed.stat().st_size,m.inputs.CAPTURE_BYTES)
        raw=m.inputs.captured_controls(seed.read_bytes())
        child=BASE/'append_selected_child_original_controls_2026-10-30_v1.json'
        self.assertEqual(child.stat().st_size,422849)
        capture=child.read_bytes();self.assertEqual(m.digest(capture),'e6efdaa73626fb4ff52d3fdc42fa4eb728a58b845258d873b4e3f0a6298de060')
        value=json.loads(capture)
        records=value.get('records',value.get('controls'))
        if type(records) is dict:records=list(records.values())
        self.assertIsInstance(records,list)
        for item in records:
            body=zlib.decompress(base64.b64decode(item['raw_zlib_base64']))
            self.assertEqual(len(body),item['bytes']);self.assertEqual(m.digest(body),item['sha256']);raw[item['path']]=body
        class Bound:
            callback=staticmethod(lambda r:raw.get(r['path']))
        plan=m.inputs.prepare_plan(Bound.callback)
        # Probe REQUESTS/SUCCESS bodies are not part of these nine seed mirrors;
        # their mandatory physical byte checks are separately fault-tested.
        with patch.object(m,'selected_controls') as receipt_check:chosen,children=m.selection(plan,Bound())
        self.assertEqual(len(receipt_check.call_args.args[0]),4)
        self.assertEqual([r['rows'] for r in chosen],[7634,7150,5782,3321])
        self.assertEqual(sum(r['rows'] for r in chosen),23887)
        self.assertEqual([c['chunk']['chunk'] for c in children],['c000001','c000018','c000582'])
        self.assertTrue(all(r['source_file_index']==i for i,r in enumerate(chosen)))

    def test_fixed_selection_row_cap_fails_before_payload_reader(self):
        base={'path':'/mnt/data/runs/x/probe/payload.parquet','bytes':1,'sha256':'a'*64,'rows':200001,
              'first_block':m.inputs.FIRST,'last_block':m.inputs.FIRST}
        groups=[{'collector_version':1},{'collector_version':2}]
        plan={'campaign_children_to_resolve':groups,'probe':{'payloads':[base]}}
        def child(r):return {'payloads':[base|{'path':base['path']+str(id(r))}]}
        reader=type('Reader',(),{'callback':lambda *_:None})()
        with patch.object(m.inputs,'resolve_child',side_effect=lambda r,_:child(r)),self.assertRaisesRegex(m.Blocked,'selection_row_limit'):
            m.selection(plan,reader)

    def test_native_physical_ordinals_case_schema_and_full_hash(self):
        record,rows=self.raw(m.inputs.FIRST,count=131);observed=[]
        original=m.strict_row
        def observe(row,locator,admission):
            observed.append((deepcopy(row),deepcopy(locator)));return original(row,locator,admission)
        with patch.object(m,'DATA_ROOT',self.root),patch.object(m,'strict_row',side_effect=observe):
            result=m.stream_native([record],self.meter(),m._NativeConsumer([record],{}))
        self.assertEqual([r for r,_ in observed],rows)
        self.assertEqual([l['physical_row_ordinal'] for _,l in observed],list(range(131)))
        self.assertEqual(result['usage']['rows'],131);self.assertEqual(result['usage']['source_bytes'],record['bytes'])
        self.assertEqual(result['usage']['hash_read_bytes'],record['bytes'])

    def test_bad_body_with_valid_footer_never_emits(self):
        record,_=self.raw(m.inputs.FIRST);path=Path(record['path']);raw=bytearray(path.read_bytes());raw[12]^=1;path.write_bytes(raw)
        with patch.object(m,'DATA_ROOT',self.root),self.assertRaisesRegex(m.Blocked,'full_native_SHA'):
            m.stream_native([record],self.meter(),m._NativeConsumer([record],{}))

    def test_native_callback_mutation_blocks_final_identity(self):
        record,_=self.raw(m.inputs.FIRST)
        original=m.strict_row
        def mutate(row,locator,admission):
            value=original(row,locator,admission);Path(record['path']).touch();return value
        with patch.object(m,'DATA_ROOT',self.root),patch.object(m,'strict_row',side_effect=mutate),self.assertRaises(m.native.StreamBlocked):
            m.stream_native([record],self.meter(),m._NativeConsumer([record],{}))

    def test_tiny_full_raw_and_clock_composition_preserves_multiplicity(self):
        first=m.inputs.FIRST;raws=[self.raw(first+i,i,count=i+1)[0] for i in range(4)]
        headers=[self.header(range(first,first+4))]
        result=self.project(raws,headers)
        self.assertEqual((result['raw_rows'],result['demanded_block_keys'],result['matched_block_keys']),(10,4,4))
        self.assertEqual(sum(r['rows'] for r in result['protocol_month_counts']),10)
        self.assertFalse(result['deduplication']);self.assertFalse(result['economic_projection'])
        self.assertFalse(result['provider_timestamp_fallback']);self.assertFalse(result['final_dataset_admitted'])
        self.assertLess(result['consumer_logical_bytes'],result['consumer_reserved_bytes'])

    def test_missing_clock_and_wrong_hash_and_cutoff_fail_closed(self):
        first=m.inputs.FIRST;raw=self.raw(first)[0]
        for what in ('missing','hash','cutoff'):
            header=self.header([first+1 if what=='missing' else first],index=1,timestamp=m.inputs.CUTOFF if what=='cutoff' else 1790000000)
            if what=='hash':
                rows=pq.read_table(header['path']).to_pylist();rows[0]['block_hash']=b'\xff'*32
                header=self.file(rows,'header-1.parquet',m.clocks.header_schema(),1)
            with self.subTest(what=what),self.assertRaises(m.Blocked):self.project([raw],[header])

    def test_unknown_emitter_bad_wire_and_observed_hash_conflict_block(self):
        row=bridge_fixtures.NativeBridgeTests().row();locator={'source_path':'/tmp/fixture.parquet','source_file_index':0,'physical_row_ordinal':0}
        for changed in (row|{'address':'0x'+'f'*40},row|{'data':'0x'},row|{'removed':True}):
            with self.subTest(changed=changed),self.assertRaises((m.Blocked,ValueError)):m.strict_row(changed,locator,{})
        raw=self.raw(m.inputs.FIRST)[0];other=self.raw(m.inputs.FIRST,1)[0]
        rows=pq.read_table(other['path']).to_pylist();rows[0]['blockHash']='0x'+'f'*64
        other=self.file(rows,'raw-1.parquet',m.native.native_schema(),1,'append_campaign_v2')|{'first_block':m.inputs.FIRST,'last_block':m.inputs.FIRST}
        with self.assertRaisesRegex(m.Blocked,'conflicting_native_block_hash'):self.project([raw,other],[self.header([m.inputs.FIRST])])

    def test_footer_routing_reads_no_unselected_payload_and_full_selected_SHA(self):
        first=m.inputs.FIRST;raw=self.raw(first)[0]
        headers=[self.header([first],0),self.header([first+100],1)]
        meter=self.meter();seen=[];original=m.clocks._Source.read
        def spy(source,size=-1):
            at=source.tell();body=original(source,size);seen.append((str(source.path),at,len(body)));return body
        with patch.object(m.clocks._Source,'read',spy):selected,captures=m.footer_hints(headers,[(first,first)],meter)
        self.assertEqual(selected,[0]);self.assertEqual(len(captures),2)
        self.assertTrue(all(offset==0 and size==4 or offset>=headers[0]['bytes']-2048 for _,offset,size in seen))
        # Corrupt only an unselected body: hints remain hints, no false full-SHA claim.
        path=Path(headers[1]['path']);body=bytearray(path.read_bytes());body[12]^=1;path.write_bytes(body)
        self.assertEqual(m.footer_hints(headers,[(first,first)],self.meter())[0],[0])
        result=self.project([raw],headers,[0]);self.assertEqual(result['clock_component']['unselected_file_indices'],[1])
        # Actual accepted campaign footer: no invented string-hash schema.
        diagnostic=BASE/'append_pilot_first_header_footer_diagnostic_2026-10-30_v1.json'
        self.assertEqual(diagnostic.stat().st_size,m.DIAGNOSTIC['bytes'])
        evidence=m.decode(diagnostic.read_bytes())
        envelope=base64.b64decode(evidence['original_footer_envelope_base64'],validate=True)
        metadata=pq.read_metadata(pa.BufferReader(envelope))
        self.assertEqual(metadata.schema.to_arrow_schema(),m.clocks.header_schema())
        path=self.root/'actual-footer-only.parquet';size=evidence['reference']['bytes']
        with path.open('xb') as stream:
            stream.write(envelope[:4]);stream.truncate(size);stream.seek(size-len(envelope)+4);stream.write(envelope[4:])
        record=evidence['reference']|{'path':str(path),'source_file_index':0,'rows':evidence['actual_num_rows']}
        first=metadata.row_group(0).column(0).statistics.min
        self.assertEqual(m.footer_hints([record],[(first,first)],self.meter())[0],[0])

    def test_footer_wrong_schema_bad_tail_and_noninteger_stats_reject(self):
        first=m.inputs.FIRST
        for change in ('schema','tail'):
            header=self.header([first],0)
            if change=='schema':
                schema=m.clocks.header_schema().set(0,pa.field('block_number',pa.string()))
                header=self.file([{'block_number':str(first),'block_hash':b'\x11'*32,'parent_hash':b'\x00'*32,'timestamp':1}],
                                 'header-0.parquet',schema,0)
            else:
                path=Path(header['path']);raw=bytearray(path.read_bytes());raw[-1]^=1;path.write_bytes(raw)
            with self.subTest(change=change),self.assertRaises(m.Blocked):m.footer_hints([header],[(first,first)],self.meter())

    def test_component_counters_separate_but_total_read_cap_shared(self):
        parent=self.meter();a=m.Component(parent,'native',100,4,4);b=m.Component(parent,'clock',100,4,4)
        a.read(100,'hash');b.read(200,'hash')
        self.assertEqual((a.usage['hash_read_bytes'],b.usage['hash_read_bytes']),(100,200))
        self.assertEqual(sum(parent.reads.values()),301)
        parent.reads['precharged']=m.READ_LIMIT-301
        with self.assertRaises(ValueError):b.read(1,'arrow')

    def test_consumer_and_inline_working_reserve_combines_with_component(self):
        parent=self.meter();component=m.Component(parent,'native',100,4,4)
        component.hold('buffer',1000);self.assertEqual(parent.peak_working,parent.shared+1000)
        with self.assertRaises(m.Blocked):component.hold('too_large',m.WORKING-parent.shared+1)

    def test_real_control_final_reopen_and_observed_absence(self):
        path=self.root/'control.json';path.write_bytes(b'{}');meter=self.meter();reader=m.Reader(meter)
        with patch.object(m,'DATA_ROOT',self.root):
            self.assertEqual(reader.callback({'path':str(path),'maximum_bytes':20}),b'{}')
            absent=self.root/'EXIT.json';self.assertIsNone(reader.callback({'path':str(absent),'maximum_bytes':20}))
        meter.reserve(100);path.write_bytes(b'[]')
        with self.assertRaisesRegex(m.Blocked,'control_final_changed'):reader.full_reopen()

    def primitive_fixture(self):
        path=self.root/'control.json';path.write_bytes(b'{}')
        reader=m.Reader(self.meter());reader.files[str(path)]={'identity':m.host.identity(path),'bytes':2,'sha256':m.digest(b'{}')}
        output=self.root/'output';output.mkdir();publisher=type('Pub',(),{'root':output,'files':{}})()
        return reader,publisher,path

    def test_final_primitive_detects_late_QA_input_absence_and_namespace(self):
        for change in ('input','absent','namespace','alias','deadline'):
            with self.subTest(change=change):
                sub=self.root/change;sub.mkdir();old=self.root;self.root=sub
                reader,publisher,path=self.primitive_fixture();missing=sub/'late_EXIT.json';reader.absent.add(str(missing))
                if change=='input':path.write_bytes(b'[]')
                elif change=='absent':missing.write_bytes(b'{}')
                elif change=='namespace':(publisher.root/'late.json').write_bytes(b'{}')
                elif change=='alias':path.rename(sub/'original');path.symlink_to(sub/'original')
                with self.assertRaises(m.Blocked):m._primitive(reader,{}, {},publisher,{},time.monotonic()-1 if change=='deadline' else time.monotonic()+10)
                self.root=old

    def test_final_optional_Git_absence_and_root_alias_reject(self):
        reader,publisher,_=self.primitive_fixture();absent=self.root/'packed-refs'
        m._primitive(reader,{}, {},publisher,{str(absent):None},time.monotonic()+10)
        absent.write_bytes(b'late')
        with self.assertRaisesRegex(m.Blocked,'absence_changed'):m._primitive(reader,{}, {},publisher,{str(absent):None},time.monotonic()+10)

    def test_full_final_payload_SHA_and_reserve_failed_read_charge(self):
        path=self.root/'data';path.write_bytes(b'abcd');saved={'identity':m.host.identity(path),'bytes':4,'sha256':m.digest(b'abcd')}
        meter=self.meter();meter.reserve(5);m._full_hash(path,saved,meter,'payload',final=True)
        self.assertEqual(meter.reads['payload'],5);self.assertEqual(meter.reserved_final_reads,0)
        with self.assertRaises(ValueError):m._full_hash(path,saved,meter,'payload',final=True)

    def test_timer_and_hard_AS_arm_before_stdin_and_project_import(self):
        wrapper=m.stdin_wrapper('1'*64,'2'*64,'3'*64);ast.parse(wrapper)
        self.assertLess(wrapper.index('signal.setitimer'),wrapper.index('sys.stdin.buffer.read'))
        self.assertLess(wrapper.index('resource.setrlimit'),wrapper.index('import hashlib'))
        self.assertIn('8*1024**3,8*1024**3',wrapper)
        self.assertLess(wrapper.index('pilot_bootstrap_source_binding_before_import'),wrapper.index('from scripts import'))
        self.assertIn('1793411790-time.time()',wrapper);self.assertIn("old[0] if old[0]>0",wrapper)
        self.assertIn('pilot_unbundled_project_import',wrapper)

    def test_public_guard_blocks_before_bootstrap_or_any_IO(self):
        with patch.object(m,'require_production_host',side_effect=RuntimeError('not_production')),patch.object(m,'admission',side_effect=AssertionError('I/O')):
            with self.assertRaisesRegex(RuntimeError,'not_production'):m.run_production()

    def test_exact_module_and_cap_contract_no_old_history_driver(self):
        self.assertEqual(len(m.RUNTIME_FILES),25);self.assertEqual(len(m.QA_MODULES),17)
        self.assertIn('scripts/prepare_polymarket_append_inputs_v2.py',m.RUNTIME_FILES)
        self.assertNotIn('scripts/stream_polymarket_append_native_rows_v1.py',m.RUNTIME_FILES)
        self.assertEqual(m.OUTPUT_CAP,1024**2);self.assertEqual(m.READ_LIMIT,2000000000)
        tree=ast.parse(Path(m.__file__).read_text());calls=[ast.unparse(n.func) for n in ast.walk(tree) if isinstance(n,ast.Call)]
        self.assertNotIn('host._execute',calls);self.assertNotIn('host.lineage',calls)

    def context(self):
        """Syntactically realistic but deliberately unauthenticated fixture."""
        pin=lambda raw:{'bytes':len(raw),'sha256':m.digest(raw)}
        ref=lambda path,raw:{'path':path,**pin(raw)}
        modules={name:b'# fixture only\n' for name in m.RUNTIME_FILES}
        tests={name.replace('.','/')+'.py':pin(b'# fixture test\n') for name in m.QA_MODULES}
        auxiliary={name:pin(b'# auxiliary fixture\n') for name in m.QA_AUXILIARY}
        source={name:pin(raw) for name,raw in modules.items()}
        added={name.replace('tests/test_','scripts/',1):source.get(name.replace('tests/test_','scripts/',1),auxiliary.get(name.replace('tests/test_','scripts/',1))) for name in tests}
        boot='00000000-0000-0000-0000-000000000001';head='a'*40
        preflight={'schema_version':'native_clock_readonly_host_inspection_v5',
            'status':'read_only_inspection_complete_not_stage_admission','boot_id':boot,
            'old_process_receipts_checked':603,'receipt_absence_relative_to_same_final_census':True,
            'checked_utc_at_last_census_start':'2026-10-31T00:40:00Z','own_process':{'pid':12},'mounts':{'data_st_dev':5,'root_st_dev':3}}
        pre=m.encode(preflight)
        ended=m.encode({'schema_version':'polymarket_native_profile_preflight_exit_v1','status':'root_verified_preflight_exited',
            'exit_code':0,'boot_id':boot,'preflight_sha256':m.digest(pre),'own_process':preflight['own_process']})
        fixture_map={name:{'bytes':v[0],'sha256':v[1]} for name,v in m.QA_FIXTURES.items()}
        fixture_digest=m.digest((json.dumps(fixture_map,sort_keys=True,separators=(',',':'))+'\n').encode())
        qa={'schema_version':'append_source_git_result_v3','status':'source_commit_prepared_not_producer_admission',
            'commit':head,'parent':m.HEAD,'host_boot':boot,'sources':added|tests,'test_suite_executed':True,'producer_dispatched':False,
            'QA_fixture_files_sha256':fixture_digest,'QA_fixture_file_count':11,
            'optimized_continuation':dict(m.CONTINUATION),'additional_runtime_reservation_seconds':0,
            'native_upper_after_full_reservations_bytes':49330839510,
            'Linux_QA':{'command_modules':list(m.QA_MODULES),'tests_run':m.QA_TESTS,'skipped':0,'failures':0,'errors':0,
                'owned_child_drained':True,'child_identity':{'available':True,'boot_id':boot,'pid':55,'parent_pid':44,
                    'process_group':55,'session':55,'start_ticks':123,'uid':0,'executable':'/usr/bin/python3.12'},
                'terminal_absence':{'available':True,'pid_absent':True,'process_group_absent':True,'session_absent':True,'process_stat_rows_checked':80}}}
        parent=m.encode({'conservative_carry_seconds':219808,'combined_absolute_deadline_utc':m.OLD_PARENT_END,
            'combined_preparation_reservation_seconds':15000,'components_seconds':{'new_reviewed_append_only_control_or_bounded_pilot':1200,'other':13800},
            'all_four_failed_footer_attempt_runtime_seconds_retained':4800,'failed_append_pilot_runtime_seconds_retained':1200,
            'failed_append_pilot_RUN_and_stdout_reservation_bytes_retained':m.OUTPUT_CAP})
        parent=(BASE/'append_only_preparation_reservation_2026-10-30_v5.json').read_bytes()
        failures={k:('failure '+k).encode() for k in m.FAILURES};build=b'{}'
        path=BASE/'append_source_pilot_actual_execution_2026-10-30_v1.jsonl'
        self.assertEqual(path.stat().st_size,m.FAILURES['five'][0]);failures['five']=path.read_bytes()
        path=BASE/'append_pilot_first_header_footer_diagnostic_2026-10-30_v1.json'
        self.assertEqual(path.stat().st_size,m.DIAGNOSTIC['bytes']);diagnostic=path.read_bytes()
        history={k:{**pin(raw),'source_head':m.FAILURES[k][2]} for k,raw in failures.items()}
        contract={'schema_version':m.wire.archive.SCHEMA,'status':'root_accepted_exchange_builds_and_era','chain_id':137,
            'exchange_address':m.wire.archive.EXCHANGE,'build_receipt':ref('/mnt/data/runs/build/RESULT.json',build),
            'builds':deepcopy(m.wire.archive.BUILDS),'upgrade':deepcopy(m.wire.archive.UPGRADE)}
        context={'modules':modules,'preflight':pre,'preflight_exit':ended,'linux_QA':m.encode(qa),'prior_failures':failures,
            'prior_pilot_format_diagnostic':diagnostic,'v3_build_receipt':build,'parent_reservation':parent}
        value={'schema_version':'polymarket_append_source_clock_pilot_admission_v3','status':'root_prospectively_admitted_four_source_optimized_clock_pilot_v3',
            'run_dir':str(m.RUN),'live_checkout_head':m.HEAD,'operator_source_head':head,'boot_id':boot,
            'issued_utc':'2026-10-31T00:40:00Z','valid_until_utc':'2026-10-31T00:50:00Z','cumulative_runtime_seconds':225808,
            'runtime_ceiling_seconds':259200,'caps':dict(m.CAPS),'preflight':ref('/mnt/data/runs/preflight.json',pre),
            'preflight_exit':ref('/mnt/data/runs/preflight_exit.json',ended),'volume_identity':preflight['mounts'],
            'native_accounting':{'retained_and_scratch_ceiling_bytes':64000000000,'observed_retained_and_scratch_bytes':49296236502,
                'spill_ceiling_bytes':16000000000,'observed_spill_bytes':0,'new_RUN_reservation_bytes':m.OUTPUT_CAP,'prior_failed_profile_runtime_seconds':4800,
                'prior_failed_append_pilot_runtime_seconds':1200,'prior_failed_append_pilot_artifact_reservation_bytes':m.OUTPUT_CAP,
                'qualified_upper_with_all_reservations_bytes':49330839510},
            'source_bundle':source,'fixture_test_sources':tests,'qa_auxiliary_sources':auxiliary,'parent_reservation':pin(parent),
            'linux_QA':ref('/mnt/data/runs/qa/RESULT.json',context['linux_QA']),'prior_failures':history,'prior_pilot_format_diagnostic':dict(m.DIAGNOSTIC),
            'v3_wire_admission':contract,'external_pool':{'ceiling_bytes':2097152,'ceiling_refs':256,'old_future_bytes':786432,
                'old_future_refs':9,'reserved_bytes':2094547,'reserved_refs':95,'new_OP_refs':0}}
        path=BASE/'append_optimized_pilot_continuation_reservation_2026-10-31_v1.json'
        self.assertEqual(path.stat().st_size,m.CONTINUATION['bytes'])
        context['optimized_continuation']=path.read_bytes();value['optimized_continuation']=dict(m.CONTINUATION)
        context['prior_successful_pilot_root']=base64.b64decode(PRIOR_ROOT_FIXTURE_BASE64,validate=True)
        value['prior_successful_pilot_root']=dict(m.PRIOR_ROOT_SUCCESS)
        self.bind(context,value)
        return context,value,preflight

    def bind(self,context,value):
        context['admission']=m.encode(value);context['admission_sha']=m.digest(context['admission'])

    def admit(self,context,value):
        fail={k:(len(raw),m.digest(raw),value['prior_failures'][k]['source_head']) for k,raw in context['prior_failures'].items()}
        with patch.object(m,'PARENT',value['parent_reservation']),patch.object(m,'BUILD_REF',value['v3_wire_admission']['build_receipt']),\
             patch.object(m,'FAILURES',fail),patch.object(m.time,'time',return_value=1793407201):
            return m.admission(context,self.meter())

    def test_realistic_preparer_RESULT_nested_Linux_QA_and_admission_positive(self):
        context,value,preflight=self.context();accepted,actual=self.admit(context,value)
        self.assertEqual(accepted,value);self.assertEqual(actual,preflight)
        self.assertLess(len(context['admission']),m.ADMISSION_CAP)
        self.assertEqual(len(m.decode(context['linux_QA'])['sources']),34)

    def test_QA_wrong_epoch_count_bool_missing_fixture_and_owned_birth_block(self):
        for kind in ('epoch','count','bool','fixture','fixturehash','birth','absence','source'):
            context,value,_=self.context();qa=m.decode(context['linux_QA'])
            if kind=='epoch':qa['commit']='b'*40
            elif kind=='count':qa['Linux_QA']['tests_run']+=1
            elif kind=='bool':qa['Linux_QA']['errors']=False
            elif kind=='fixture':qa['QA_fixture_file_count']=9
            elif kind=='fixturehash':qa['QA_fixture_files_sha256']='0'*64
            elif kind=='birth':qa['Linux_QA']['child_identity']['session']+=1
            elif kind=='absence':qa['Linux_QA']['terminal_absence']['session_absent']=False
            else:qa['sources'].pop(next(iter(qa['sources'])))
            context['linux_QA']=m.encode(qa);value['linux_QA'].update(bytes=len(context['linux_QA']),sha256=m.digest(context['linux_QA']))
            self.bind(context,value)
            with self.subTest(kind=kind),self.assertRaises((m.Blocked,ValueError)):self.admit(context,value)

    def test_stale_V5_changed_exit_wrong_boot_and_public_boolean_block(self):
        for kind in ('stale','exit','boot','bool'):
            context,value,_=self.context()
            if kind=='exit':body=m.decode(context['preflight_exit']);body['exit_code']=2;role='preflight_exit'
            else:
                body=m.decode(context['preflight']);role='preflight'
                if kind=='stale':body['checked_utc_at_last_census_start']='2026-10-30T21:00:00Z'
                elif kind=='boot':body['boot_id']='1'*36
                else:body['old_process_receipts_checked']=True
            context[role]=m.encode(body);value[role].update(bytes=len(context[role]),sha256=m.digest(context[role]));self.bind(context,value)
            with self.subTest(kind=kind),self.assertRaises((m.Blocked,ValueError)):self.admit(context,value)

    def test_no_refund_cap_bool_unknown_field_and_cold_expired_admission_block(self):
        for kind in ('refund','pool','bool','extra','expiry','negative'):
            context,value,_=self.context()
            if kind=='refund':value['cumulative_runtime_seconds']=225807
            elif kind=='pool':value['external_pool']['old_future_bytes']=0
            elif kind=='bool':value['caps']['workers']=True
            elif kind=='extra':value['authenticated']=True
            elif kind=='expiry':value['valid_until_utc']='2026-10-31T02:00:00Z'
            else:value['native_accounting']['observed_retained_and_scratch_bytes']=-1
            self.bind(context,value)
            with self.subTest(kind=kind),self.assertRaises((m.Blocked,ValueError)):self.admit(context,value)

    def test_source_missing_changed_and_oversized_inline_admission_block(self):
        for kind in ('source','module','large'):
            context,value,_=self.context()
            if kind=='source':value['source_bundle'].pop(next(iter(value['source_bundle'])))
            elif kind=='module':context['modules'][m.DRIVER_PATH]=b'evil'
            else:value['status']='x'*m.ADMISSION_CAP
            self.bind(context,value)
            with self.subTest(kind=kind),self.assertRaises(m.Blocked):self.admit(context,value)

    def test_selected_REQUESTS_SUCCESS_full_bytes_required(self):
        parent='/mnt/data/runs/fixture/node';record={'path':parent+'/payload.parquet'}
        bodies={parent+'/REQUESTS.json':b'[]',parent+'/SUCCESS.json':b'{}'}
        ref=lambda p:{'path':p,'bytes':len(bodies[p]),'sha256':m.digest(bodies[p])}
        group={'request_ledgers':[ref(parent+'/REQUESTS.json')],'success_and_split_controls':[ref(parent+'/SUCCESS.json')]}
        reader=type('Reader',(),{'callback':lambda _,r:bodies.get(r['path'])})()
        m.selected_controls([group],[record],reader)
        bodies[parent+'/SUCCESS.json']=b'[]'
        with self.assertRaisesRegex(m.Blocked,'selected_control_full_SHA'):m.selected_controls([group],[record],reader)

    def test_Git_proof_requires_parent443e_and_exact28_ADD_paths(self):
        allowed={name.replace('.','/')+'.py' for name in m.QA_MODULES}
        allowed|={name.replace('tests/test_','scripts/',1) for name in allowed}
        def git(_,args,*a,**k):return (m.HEAD+'\n').encode() if args[0]=='rev-parse' else ('\n'.join('A\t'+p for p in sorted(allowed))+'\n').encode()
        with patch.object(m.host,'git_state'),patch.object(m.host,'prove_git_blobs'),patch.object(m.host,'git',side_effect=git):
            m.git_proof(self.meter(),[],'a'*40)
        with patch.object(m.host,'git_state'),patch.object(m.host,'prove_git_blobs'),patch.object(m.host,'git',return_value=b'b'*40),self.assertRaises(m.Blocked):
            m.git_proof(self.meter(),[],'a'*40)

    def compose(self,mutation=None):
        run=self.root/'RUN';repo=self.root/'repo';repo.mkdir();(repo/'.git/refs/heads').mkdir(parents=True)
        for name in ('production_guard.py','.git/HEAD','.git/index','.git/config','.git/refs/heads/main'):(repo/name).write_bytes(b'fixture')
        control=self.root/'bound.json';control.write_bytes(b'{}')
        qa_path=self.root/'bound_QA.json';qa_path.write_bytes(b'{}')
        payload=self.root/'payload';payload.write_bytes(b'payload')
        record={'path':str(payload),'bytes':7,'sha256':m.digest(b'payload'),'rows':1,'source_file_index':0,
            'source_group':'append_probe_v2','first_block':m.inputs.FIRST,'last_block':m.inputs.FIRST}
        context,value,preflight=self.context();value['qa_auxiliary_sources']={}
        meter=self.meter();context.update(started=meter.started,deadline=meter.deadline,bootstrap_bytes=1)
        process={'uid':0,'full_exe':'/usr/bin/python3.12'}
        def plan(callback):
            for p in (control,qa_path):callback({'path':str(p),'maximum_bytes':10})
            callback({'path':str(self.root/'absent_EXIT.json'),'maximum_bytes':10})
            return {'header_parquets':[]}
        censuses=[0]
        def census(*a,**k):
            censuses[0]+=1
            if mutation:mutation(run,control,qa_path,context)
            return {},[]
        with ExitStack() as stack:
            for obj,name,new in [(m,'RUN',run),(m,'REPO',repo),(m,'DATA_ROOT',self.root),
                (m.host,'process_record',lambda *_:process),(m.host,'proc_bytes',lambda *_:value['boot_id'].encode()),
                (m.host,'host_checks',lambda *_a,**_k:{'process_count':1}),(m,'git_proof',lambda *_a,**_k:None),
                (m.host,'git',lambda *_a,**_k:b'refs/heads/main\n'),(m.inputs,'prepare_plan',plan),
                (m,'selection',lambda *_:([record],[])),(m,'footer_hints',lambda *_:([],{})),
                (m,'_header_controls',lambda *_:()),
                (m,'_project_pilot',lambda *_:{'raw_rows':23887,'demanded_block_keys':200,'matched_block_keys':200}),
                (m.host,'current_census',census)]:stack.enter_context(patch.object(obj,name,new))
            stack.enter_context(patch('sys.stdout',io.TextIOWrapper(io.BytesIO())))
            output=m._compose(context,meter,value,preflight)
        return run,m.decode(output)

    def test_positive_private_full_publication_real_Reader_Publisher_and_primitive(self):
        run,output=self.compose()
        self.assertEqual({p.name for p in run.iterdir()},{'START.json','profile.json','COMPLETE.json'})
        self.assertEqual(output['status'],'four_source_clock_pilot_verified_not_full_dataset')
        self.assertTrue(output['root_independent_reopen_directory_sync_and_owned_drain_required'])

    def test_late_complete_callback_input_QA_pending_and_absence_mutations_withhold(self):
        for kind in ('input','QA','pending','absence','inline','namespace'):
            sub=self.root/kind;sub.mkdir();old=self.root;self.root=sub
            def mutation(run,control,qa,context):
                if kind=='input':control.write_bytes(b'[]')
                elif kind=='QA':qa.write_bytes(b'[]')
                elif kind=='pending':
                    pending=run/'.COMPLETE.json.pending';raw=pending.read_bytes();pending.write_bytes(raw.replace(b'four',b'xxxx',1))
                elif kind=='absence':(self.root/'absent_EXIT.json').write_bytes(b'{}')
                elif kind=='inline':context['linux_QA']+=b' '
                else:(run/'unclassified').write_bytes(b'x')
            with self.subTest(kind=kind),self.assertRaises(ValueError):self.compose(mutation)
            self.assertFalse((sub/'RUN/COMPLETE.json').exists());self.assertTrue((sub/'RUN/.COMPLETE.json.pending').exists())
            self.root=old

    def test_bootstrap_and_actual_entry_cannot_accept_public_context_flag(self):
        with patch.object(m,'require_production_host'),patch.object(m,'admission',side_effect=AssertionError('before auth')):
            with self.assertRaisesRegex(m.Blocked,'authenticated_bootstrap'):m._execute({'authenticated':True})
        tree=ast.parse(Path(m.__file__).read_text());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_compose')
        source=ast.unparse(fn)
        self.assertLess(source.index('process_record'),source.index('host_checks'))
        self.assertLess(source.index('host_checks'),source.index('prepare_plan'))

    def test_actual_first_pilot_failure_epoch_stdout_and_original_format_full_pins(self):
        failed=BASE/'append_source_pilot_actual_execution_2026-10-30_v1.jsonl'
        diagnostic=BASE/'append_pilot_first_header_footer_diagnostic_2026-10-30_v1.json'
        self.assertEqual(failed.stat().st_size,m.FAILURES['five'][0]);self.assertEqual(diagnostic.stat().st_size,m.DIAGNOSTIC['bytes'])
        raw=failed.read_bytes();info=diagnostic.read_bytes();m.first_pilot_history(raw,info)
        for left,right in ((raw+b' ',info),(raw,info+b' ')):
            with self.subTest(failure_changed=len(left)!=len(raw)),self.assertRaises(m.Blocked):m.first_pilot_history(left,right)
        context,value,_=self.context();value['native_accounting']['prior_failed_append_pilot_runtime_seconds']=0;self.bind(context,value)
        with self.assertRaises(m.Blocked):self.admit(context,value)

    def test_frozen_V1_pair_and_original_clock_unchanged_not_union_schema(self):
        files={
            'scripts/run_polymarket_append_source_pilot_v1.py':'4047e5d4f96f9fe13571e7a17edac83789fcf5b72e567f126af7cf37b060c052',
            'tests/test_run_polymarket_append_source_pilot_v1.py':'c9bc6325196fe310b358f65c3f6ffddb34fb33695cc210ad32a2360be29b7668'}
        for name,sha in files.items():self.assertEqual(m.digest(Path(name).read_bytes()),sha)
        self.assertEqual(m.clocks.header_schema(),pa.schema([('block_number',pa.int64()),('block_hash',pa.binary(32)),
            ('parent_hash',pa.binary(32)),('timestamp',pa.int64())]))

    def test_frozen_V2_pairs_and_core_final_gates_unchanged(self):
        pins={
            'scripts/run_polymarket_append_source_pilot_v2.py':'d0da1261d0ad372d87dda0eaaf10337ceac0ed8c3c4cdc0834c1b6ae89931501',
            'tests/test_run_polymarket_append_source_pilot_v2.py':'04e206cdde8280711de5b7e55ff06020f6ae68f790e793ffa8ed8517750096f1',
            'scripts/read_polymarket_append_clocks_v2.py':'8f29cba467914a16d3f6097ba884e8c62283e30bbe21a30d3aca4c750bef3817'}
        for path,sha in pins.items():self.assertEqual(m.digest(Path(path).read_bytes()),sha)
        old=ast.parse(Path('scripts/run_polymarket_append_source_pilot_v2.py').read_text())
        new=ast.parse(Path(m.__file__).read_text())
        for name in ('selection','selected_controls','strict_row','footer_hints','_full_hash','_primitive','git_proof'):
            a=next(n for n in old.body if isinstance(n,ast.FunctionDef) and n.name==name)
            b=next(n for n in new.body if isinstance(n,ast.FunctionDef) and n.name==name)
            self.assertEqual(ast.dump(a),ast.dump(b),name)

    def test_exact_original_header_receipt_manifest_and_complete_not_boolean(self):
        capture=BASE/'append_only_source_controls_and_fourth_profile_drain_2026-10-30_v1.json'
        self.assertEqual(capture.stat().st_size,m.inputs.CAPTURE_BYTES)
        bodies=m.inputs.captured_controls(capture.read_bytes())
        controls=tuple(bodies[m.inputs.ANCHORS[k]['path']] for k in ('header_acceptance','headers','headers_complete'))
        derived=m.inputs._headers(controls[0],m.decode(controls[1]),m.decode(controls[2]))
        headers=[{**{k:r[k] for k in ('path','bytes','sha256')},'rows':r['keys'],'source_file_index':r['chunk']} for r in derived]
        proof=m._header_reuse_contract(controls,headers)
        self.assertEqual(proof['whole_campaign_accepted_keys'],32901442)
        self.assertIs(proof['whole_campaign_reverified_here'],False)
        for changed in ({'accepted':True},tuple([controls[0]+b' ',*controls[1:]])):
            with self.subTest(changed=type(changed)),self.assertRaises(ValueError):m._header_reuse_contract(changed,headers)
        bad=deepcopy(headers);bad[0]['sha256']='0'*64
        with self.assertRaisesRegex(m.Blocked,'reference_not_accepted'):m._header_reuse_contract(controls,bad)
        with patch.object(m,'stream_native',side_effect=AssertionError('body I/O')),self.assertRaises(ValueError):
            m._project_pilot([],headers,[0],self.meter(),{},({'accepted':True},))

    def test_sealed_native_consumer_all_rows_and_batch_not_row_roster_complexity(self):
        first=m.inputs.FIRST
        records=[self.raw(first+i,i,count=256)[0] for i in range(4)]
        observations=[];rosters=[];original=m.strict_row;reopen=m.native._reopen
        def observe(row,locator,admission):
            observations.append(deepcopy(locator));return original(row,locator,admission)
        def reopen_spy(captured):rosters.append(len(captured));return reopen(captured)
        with patch.object(m,'DATA_ROOT',self.root),patch.object(m,'strict_row',side_effect=observe),\
                patch.object(m.native,'_reopen',side_effect=reopen_spy):
            result=m.stream_native(records,self.meter(),m._NativeConsumer(records,{}))
        self.assertEqual(len(observations),1024)
        self.assertEqual(result['physical_rows_newly_wire_checked'],1024)
        self.assertEqual(result['usage']['rows'],1024)
        self.assertEqual(sum(n==4 for n in rosters),2)
        self.assertLessEqual(len(rosters),2+2*result['usage']['batches']+len(records))
        for file in range(4):
            self.assertEqual([r['physical_row_ordinal'] for r in observations if r['source_file_index']==file],list(range(256)))
        with self.assertRaisesRegex(m.Blocked,'sealed_native_consumer'):
            m.stream_native(records,self.meter(),lambda *_:None)

    def test_vector_lookup_checks_only_demands_and_preserves_physical_ordinals(self):
        first=m.inputs.FIRST
        raws=[self.raw(first,0,count=2)[0],self.raw(first+1000,1,count=3)[0]]
        headers=[self.header(range(first,first+1024))]
        original=m.clocks._header;checked=[]
        def spy(row,previous):checked.append(row['block_number']);return original(row,previous)
        with patch.object(m.clocks,'_header',side_effect=spy):result=self.project(raws,headers)
        self.assertEqual(checked,[first,first+1000])
        clock=result['clock_component']
        self.assertEqual(clock['vectorized_payload_rows_read'],1024)
        self.assertEqual(clock['physical_accepted_header_rows_reused'],1024)
        self.assertEqual(clock['demanded_header_rows_newly_checked'],2)
        self.assertIs(clock['full_new_order_parent_replay_performed'],False)
        self.assertEqual(result['raw_rows'],5)
        self.assertEqual([r['physical_row_ordinal'] for r in result['matched_header_physical_locators']],[0,1000])
        from scripts import run_polymarket_append_source_pilot_v2 as old
        with patch.object(old,'DATA_ROOT',self.root):
            previous=old._project_pilot(raws,headers,[0],self.meter(),{})
        for key in ('raw_rows','demanded_block_keys','matched_block_keys','protocol_counts','protocol_month_counts'):
            self.assertEqual(result[key],previous[key],key)

    def test_demand_duplicates_schema_null_and_full_SHA_remain_blocking(self):
        first=m.inputs.FIRST;raw=self.raw(first)[0]
        for kind in ('same_file_duplicate','two_file_duplicate','null_hash','string_schema','body_SHA'):
            sub=self.root/kind;sub.mkdir();saved=self.root;self.root=sub
            if kind=='same_file_duplicate':headers=[self.header([first,first])]
            elif kind=='two_file_duplicate':headers=[self.header([first],0),self.header([first],1)]
            else:
                headers=[self.header([first])]
                rows=pq.read_table(headers[0]['path']).to_pylist()
                if kind=='null_hash':rows[0]['block_hash']=None
                if kind=='string_schema':
                    schema=m.clocks.header_schema().set(1,pa.field('block_hash',pa.string()))
                    rows[0]['block_hash']='0x'+'00'*32
                    headers=[self.file(rows,'header-0.parquet',schema,0)]
                elif kind=='null_hash':headers=[self.file(rows,'header-0.parquet',m.clocks.header_schema(),0)]
                else:
                    path=Path(headers[0]['path']);body=bytearray(path.read_bytes());body[12]^=1;path.write_bytes(body)
            # Raw remains a local literal file outside this subfixture folder.
            with patch.object(m,'DATA_ROOT',saved),patch.object(m,'_header_reuse_contract',return_value={'fixture_only':True}),\
                    self.subTest(kind=kind),self.assertRaises(ValueError):
                m._project_pilot([raw],headers,[r['source_file_index'] for r in headers],self.meter(),{},())
            self.root=saved

    def test_batch_memory_fresh_state_and_late_other_file_change_block(self):
        first=m.inputs.FIRST;records=[self.raw(first,i,count=65)[0] for i in range(2)]
        original=m.strict_row;seen=[0]
        def mutate_other(row,locator,admission):
            value=original(row,locator,admission);seen[0]+=1
            if locator['source_file_index']==1:Path(records[0]['path']).touch()
            return value
        with patch.object(m,'DATA_ROOT',self.root),patch.object(m,'strict_row',side_effect=mutate_other),\
                self.assertRaises(m.native.StreamBlocked):
            m.stream_native(records,self.meter(),m._NativeConsumer(records,{}))
        self.assertGreater(seen[0],65)
        consumer=m._NativeConsumer(records,{})
        with patch.object(m,'ROW_LIMIT',1),self.assertRaisesRegex(m.Blocked,'retained_consumer_working'):
            consumer.demands[1]='0x'+'a'*2000;consumer.measure()

    def test_new_root_budget_exact_arithmetic_scope_and_prior_success_are_not_admission(self):
        path=BASE/'append_optimized_pilot_continuation_reservation_2026-10-31_v1.json'
        self.assertEqual(path.stat().st_size,m.CONTINUATION['bytes'])
        raw=path.read_bytes();m.optimized_continuation(raw)
        prior=base64.b64decode(PRIOR_ROOT_FIXTURE_BASE64,validate=True);m.prior_successful_pilot_root(prior)
        for value in (raw+b' ',m.encode({'accepted':True})):
            with self.assertRaises(ValueError):m.optimized_continuation(value)
        for field,bad in (('conservative_carry_after_seconds',221008),('conservative_carry_after_seconds',True),
                          ('additional_prospective_runtime_seconds',0),('new_OP_refs',1),
                          ('native_upper_after_full_reservations_bytes',49296236502),
                          ('new_window_start_utc','2026-10-31T00:00:00Z')):
            value=m.decode(raw);value[field]=bad;changed=m.encode(value)
            with patch.object(m,'CONTINUATION',{'bytes':len(changed),'sha256':m.digest(changed)}),\
                    self.subTest(field=field),self.assertRaises(ValueError):m.optimized_continuation(changed)
        with self.assertRaises(ValueError):m.prior_successful_pilot_root(prior+b' ')
        context,value,_=self.context()
        for field,bad in (('cumulative_runtime_seconds',221008),('schema_version','polymarket_append_source_pilot_admission_v2')):
            changed=deepcopy(value);changed[field]=bad;self.bind(context,changed)
            with self.subTest(field=field),self.assertRaises(ValueError):self.admit(context,changed)

    def test_new_window_bootstrap_pins_and_fixed_child_peak_memory(self):
        wrapper=m.stdin_wrapper('1'*64,'2'*64,'3'*64)
        self.assertIn('optimized_continuation_utf8',wrapper)
        self.assertIn('prior_successful_pilot_root_utf8',wrapper)
        self.assertIn('1793411790-time.time()',wrapper)
        self.assertIn('run_polymarket_append_source_pilot_v3 as driver',wrapper)
        context,value,_=self.context()
        with patch.object(m.time,'time',return_value=1793406000),self.assertRaises(ValueError):
            m.admission(context,self.meter())
        parent=self.meter();child=m.Component(parent,'native',100,4,4)
        child.hold('one',1000);peak=parent.peak_working
        child.drop('one');child.primitive_working()
        self.assertEqual(parent.peak_working,peak)
        self.assertEqual(child.usage['peak_working_bytes'],1000)



if __name__=='__main__':unittest.main()


