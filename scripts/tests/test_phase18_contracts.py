"""Adversarial independent semantic evidence checks; no service/build dependency."""
import copy,importlib.util,json,pathlib,tempfile,unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('phase18_offline',ROOT/'phase18/verify_evidence.py');verify=importlib.util.module_from_spec(spec);spec.loader.exec_module(verify)

class Phase18Contracts(unittest.TestCase):
    def setUp(self):
        self.fixture=json.loads((ROOT/'phase18/test-fixture.json').read_text())
    def test_independent_canonical_properties_and_choice(self):
        verify.oracle.decision(self.fixture['plan'],self.fixture['decision'])
    def test_property_and_candidate_forgery_survive_no_outer_rehash(self):
        for mutate in [lambda d:d['properties'].update(termination='finite'),lambda d:d['properties'].update(delivery='local_committed_prefix_v3'),lambda d:d['candidates'][0].update(estimated_units=1),lambda d:d.update(chosen='local'),lambda d:d.update(candidates=[])]:
            d=copy.deepcopy(self.fixture['decision']);mutate(d)
            with self.assertRaises(ValueError):verify.oracle.decision(self.fixture['plan'],d)
    def test_source_cut_semantics_change_identity(self):
        changed=copy.deepcopy(self.fixture['plan']);changed['computation']['source']['mode']['cut'][0]['offset']+=1
        with self.assertRaises(ValueError):verify.oracle.decision(changed,self.fixture['decision'])
    def test_negative_case_requires_the_intended_admission_refusal(self):
        case=dict(name='v3-hybrid-refused',exit=1)
        valid=[dict(type='failure',reason='stream admission: strict logical plan')]
        verify.refusal(case,valid,b'')
        for trace,out in [([dict(type='failure',reason='broker failure')],b''),(valid,b'leaked'),(valid+[dict(type='admitted')],b'')]:
            with self.assertRaises(ValueError):verify.refusal(case,trace,out)
    def test_negative_labels_cannot_replace_invalid_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);path=root/'plan.json';path.write_text(json.dumps(self.fixture['plan']))
            case=dict(name='unknown-schema',logical=path.name)
            with self.assertRaises(ValueError):verify.negative_input(root,case)
            plan=copy.deepcopy(self.fixture['plan']);plan['computation']['schema']=[];path.write_text(json.dumps(plan));verify.negative_input(root,case)
    def test_actual_producer_payload_and_phase_oracle(self):
        fixture=self.fixture;plan=fixture['plan'];source=plan['computation']['source']
        c=dict(version=1,bootstrap='127.0.0.1:1',expected_cluster_id=source['cluster'],topic=source['topic'],starts=source['original_starts'],security=dict(mode='loopback_dev'),key_less_than=plan['computation']['key_less_than'],allowance_ms=10,payload_bytes=4096,client_queue_kib=16,sink_stall_ms=30000,timers=[])
        event=dict(version=1,key=1,value_i64=5,event_time_ms=0);receipt=dict(version=1,session='fixture',partition=0,offset=0,event_time_ms=0,late=False,projected=dict(value_i64=5,key=1,event_time_ms=0))
        progress=dict(forwarded_records=1,late_records=0,frontier=None,partitions={'0':dict(next_offset_suggestion=1,watermark=-10),'1':dict(next_offset_suggestion=0,watermark=None)})
        for part,cursor in progress['partitions'].items():cursor.update(start=0,forwarded=0 if part=='0' else None,seen=0 if part=='0' else None)
        audit=dict(semantic_digest=fixture['decision']['semantic_digest'],partition=0,offset=0,phase='historical')
        ledger={source['topic']:[dict(partition=0,offset=0,event=event)]};case=dict(name='case',mode='stateless',config='config.json',logical='plan.json')
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);(root/'config.json').write_text(json.dumps(c));(root/'plan.json').write_text(json.dumps(plan))
            def write(out,record):
                (root/'case.stdout').write_text(json.dumps(out)+'\n');(root/'case.stderr').write_text('\n'.join(map(json.dumps,[dict(type='unified_decision',decision=fixture['decision']),dict(type='admitted',session='fixture',progress=dict(forwarded_records=0,partitions={str(o['partition']):dict(start=o['offset']) for o in source['original_starts']})),dict(type='unified_confirmed',record=record),dict(type='forwarded',progress=dict(progress,session='fixture')),dict(type='terminal',progress=progress)]))+'\n')
            write(receipt,audit);verify.oracle.replay(root,case,ledger)
            changed=copy.deepcopy(receipt);changed['projected']['value_i64']=6;write(changed,audit)
            with self.assertRaises(ValueError):verify.oracle.replay(root,case,ledger)
            changed=dict(audit,phase='live');write(receipt,changed)
            with self.assertRaises(ValueError):verify.oracle.replay(root,case,ledger)

if __name__=='__main__':unittest.main()
