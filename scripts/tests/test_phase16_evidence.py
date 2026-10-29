"""Independent cost/state and archive attack tests; no Kafka/MinIO needed."""
import copy, hashlib, importlib.util, json, pathlib, struct, sys, tempfile, unittest
ROOT=pathlib.Path(__file__).resolve().parents[1]/'phase16'
def module(name,path):
    s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
oracle=module('phase16_contract',ROOT/'oracle.py')
saved=sys.modules.get('oracle');sys.modules['oracle']=oracle
verifier=module('phase16_verifier',ROOT/'verify_evidence.py')
if saved is None:del sys.modules['oracle']
else:sys.modules['oracle']=saved

def inputs():
    p=dict(placements=['local','remote','both'],max_depth=2,min_samples=2,prices=dict(local_write_byte=1,remote_write_byte=5,remote_request=3,compute_ms=1,capacity_byte_ms=0,replay_record=1),constraints=dict(min_copies=1,require_remote=False,max_loss_ppm=1000000,local_loss_ppm=100000,remote_loss_ppm=100,both_loss_ppm=1,failure_ppm=1000,retention_ms=1000,recovery_slo_ms=10000))
    o=dict(samples=2,logical_bytes=1000,changed_bytes=10,changed_shards=1,depth=0,local_available_bytes=1000000000,remote_available_bytes=1000000000,remote_available_objects=8192,remaining_requests=65536,remaining_response_bytes=68719476736,scan_bytes_per_second=1000000,local_bytes_per_second=1000000,remote_bytes_per_second=1000000,replay_ns_per_record=10000)
    a=dict(format='full',placement='local',records=10,ms=1000);return p,o,a

class PolicyTests(unittest.TestCase):
    def test_independent_integer_equation_and_physical_delta(self):
        p,o,a=inputs();self.assertEqual(oracle.evaluate(p,o,a)['score'],100206000)
        a['format']='incremental';self.assertEqual(oracle.evaluate(p,o,a)['score'],1206000)
        a.update(format='full',placement='remote');self.assertEqual(oracle.evaluate(p,o,a)['score'],530806000)
    def test_risk_slo_depth_and_replication_are_constraints(self):
        p,o,a=inputs();p['constraints'].update(min_copies=2,max_loss_ppm=1,require_remote=True)
        self.assertEqual(oracle.evaluate(p,o,a)['rejection'],'durability_floor');a['placement']='both'
        self.assertIsNone(oracle.evaluate(p,o,a)['rejection']);p['constraints']['recovery_slo_ms']=1
        self.assertEqual(oracle.evaluate(p,o,a)['rejection'],'recovery_slo');p['constraints']['recovery_slo_ms']=10000
        a['format']='incremental';o['depth']=2;self.assertEqual(oracle.evaluate(p,o,a)['rejection'],'full_required')
    def test_checksum_consistent_state_semantic_forgery(self):
        config=dict(source=dict(starts=[dict(partition=0,offset=0)],allowance_ms=10,key_less_than=1000),window_ms=100,job_id='job')
        m=oracle.Model(config);m.state={(0,1):(5,2)};m.maxima={0:0};m.offsets={0:2};m.frontier=-10;m.total=2
        progress=dict(forwarded_records=2,late_records=0,frontier=-10,partitions={'0':dict(next_offset_suggestion=2,max_forwarded_time=0,watermark=-10)})
        manifest=dict(version=2,progress=progress,pending_closures=0,closed_window_late=0,owners=1,windows=1,generation='new',shards=[dict(shard=i,owner=0,object_generation='new',bytes=64 if i==0 else 0,count=1 if i==0 else 0) for i in range(32)])
        raw=struct.pack('<qQqQ',0,1,5,2)+bytes(32);get=lambda g,n:raw if n=='shard-00.bin' else b''
        self.assertEqual(oracle.check_snapshot(None,manifest,m,get),64)
        forged=struct.pack('<qQqQ',0,1,999,2)+bytes(32)
        # Updating the enclosing SHA inventory cannot make changed semantics valid.
        self.assertNotEqual(hashlib.sha256(raw).hexdigest(),hashlib.sha256(forged).hexdigest())
        with self.assertRaisesRegex(ValueError,'canonical v2 state'):oracle.check_snapshot(None,manifest,m,lambda g,n:forged if n=='shard-00.bin' else b'')
    def test_archive_version_and_one_byte_tamper_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);archive=root/'archive';manifest=root/'manifest';archive.write_bytes(b'original')
            manifest.write_text(json.dumps(dict(version=2,archive_sha256=hashlib.sha256(b'original').hexdigest(),files={})))
            with self.assertRaisesRegex(ValueError,'evidence version'):verifier.archive_verify(archive,manifest)
            manifest.write_text(json.dumps(dict(version=1,archive_sha256=hashlib.sha256(b'original').hexdigest(),files={})));archive.write_bytes(b'Original')
            with self.assertRaisesRegex(ValueError,'archive checksum'):verifier.archive_verify(archive,manifest)

if __name__=='__main__':unittest.main()
