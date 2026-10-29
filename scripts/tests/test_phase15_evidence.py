"""Phase15 independent oracle and archive refusal regressions."""
import copy
import importlib.util
import io
import json
import pathlib
import sys
import tarfile
import tempfile
import unittest

DIRECTORY=pathlib.Path(__file__).resolve().parents[1]/'phase15'
sys.path.insert(0,str(DIRECTORY))
import oracle
import verify_evidence


def config():
    return dict(job_id='test-job',window_ms=100,source=dict(starts=[dict(partition=0,offset=0),dict(partition=1,offset=0)],key_less_than=10,allowance_ms=0))

def input_frame(partition,offset,key,value,ts,late=False,closed=False):
    return dict(type='input',closed_window_late=closed,receipt=dict(version=1,session='session',partition=partition,offset=offset,key=key,event_time_ms=ts,late=late,projected=dict(key=key,value_i64=value,event_time_ms=ts)))

def event(key,value,ts):return dict(version=1,key=key,value_i64=value,event_time_ms=ts)

class Phase15Evidence(unittest.TestCase):
    def test_cross_partition_replay_keeps_logical_id_but_marks_conflicting_payload(self):
        c=config();c['source']['key_less_than']=2
        ledger={(0,0):event(1,2,0),(1,0):event(1,3,0),
                (0,1):event(9,0,200),(0,2):event(1,7,50),(1,1):event(9,0,200)}
        orders=[[(0,0),(1,0),(0,1),(0,2),(1,1)],
                [(0,0),(1,0),(0,1),(1,1),(0,2)]]
        outputs=[]
        for order in orders:
            m=oracle.Model(c);frames=[]
            for p,o in order:
                e=ledger[p,o]
                accepted=e['key']<c['source']['key_less_than']
                maximum=m.maxima.get(p)
                late=maximum is not None and e['event_time_ms']<maximum-c['source']['allowance_ms']
                start=(e['event_time_ms']//c['window_ms'])*c['window_ms']
                closed=accepted and m.frontier is not None and start+c['window_ms']<=m.frontier
                frame=input_frame(p,o,e['key'],e['value_i64'],e['event_time_ms'],late=late,closed=closed)
                if not accepted:frame['receipt']['projected']=None
                m.consume(frame,ledger)
                while m.due():
                    start,key=m.due()[0];amount,count=m.state[(start,key)];stop=start+c['window_ms']
                    output=dict(type='window',output_version=1,job_id=c['job_id'],key=key,
                                window_start=start,window_end=stop,sum_i64=amount,count_u64=count,
                                output_id=f"{c['job_id']}:{key}:{start}:{stop}:1",
                                payload_fingerprint=f'window-payload-v1:{amount}:{count}')
                    m.consume(output,ledger);frames.append(output)
            outputs.append(frames)
        self.assertEqual(len(outputs[0]),1);self.assertEqual(len(outputs[1]),1)
        self.assertEqual(outputs[0][0]['output_id'],outputs[1][0]['output_id'])
        self.assertEqual((outputs[0][0]['sum_i64'],outputs[0][0]['count_u64']),(12,3))
        self.assertEqual((outputs[1][0]['sum_i64'],outputs[1][0]['count_u64']),(5,2))
        self.assertNotEqual(outputs[0][0]['payload_fingerprint'],outputs[1][0]['payload_fingerprint'])

    def test_quiet_partition_signed_alignment_and_exact_close(self):
        m=oracle.Model(config());ledger={(0,0):event(1,2,-1),(1,3):event(1,3,-50),(0,4):event(2,0,100),(1,9):event(2,0,100)}
        for p,o in [(0,0),(1,3),(0,4),(1,9)]:
            e=ledger[p,o];m.consume(input_frame(p,o,e['key'],e['value_i64'],e['event_time_ms']),ledger)
            if p==0 and o==0:self.assertIsNone(m.frontier)
        self.assertEqual(m.due(),[(-100,1)])
        m.consume(dict(type='window',output_version=1,job_id='test-job',key=1,window_start=-100,window_end=0,sum_i64=5,count_u64=2,output_id='test-job:1:-100:0:1'),ledger)
        self.assertEqual(m.due(),[])

    def test_forged_native_projection_refused(self):
        m=oracle.Model(config());ledger={(0,0):event(1,2,0)};frame=input_frame(0,0,1,999,0)
        with self.assertRaisesRegex(ValueError,'native transform'):m.consume(frame,ledger)

    def test_visible_record_skip_refused_but_legal_hole_accepted(self):
        m=oracle.Model(config());ledger={(0,0):event(1,2,0),(0,5):event(1,2,0)}
        with self.assertRaisesRegex(ValueError,'lost visible'):m.consume(input_frame(0,5,1,2,0),ledger)
        m.consume(input_frame(0,0,1,2,0),ledger);m.consume(input_frame(0,5,1,2,0),ledger)
        self.assertEqual(m.offsets[0],6)

    def test_closed_window_lateness_separate_from_partition_late(self):
        m=oracle.Model(config());ledger={(0,0):event(1,1,100),(0,1):event(1,2,-1)}
        m.consume(input_frame(0,0,1,1,100),ledger)
        # Unknown global frontier: the old record is partition-late but must update state.
        m.consume(input_frame(0,1,1,2,-1,late=True),ledger);self.assertIn((-100,1),m.state)
        bad=oracle.Model(config());bad.frontier=100
        with self.assertRaisesRegex(ValueError,'closed-window late'):bad.consume(input_frame(0,0,1,2,-1),{(0,0):event(1,2,-1)})

    def test_authored_pass_labels_do_not_satisfy_cases(self):
        with tempfile.TemporaryDirectory() as temp:
            root=pathlib.Path(temp);(root/'cases.json').write_text(json.dumps([dict(name='invented',status='PASS')]))
            with self.assertRaisesRegex(ValueError,'mandatory process'):verify_evidence.verify(root)

    def test_tar_traversal_symlink_and_duplicate_refusals(self):
        for mode in ('traversal','symlink','duplicate'):
            stream=io.BytesIO()
            with tarfile.open(fileobj=stream,mode='w:gz') as archive:
                member=tarfile.TarInfo('../escape' if mode=='traversal' else 'file');member.size=1
                if mode=='symlink':member.type=tarfile.SYMTYPE;member.linkname='/etc/passwd';member.size=0
                archive.addfile(member,io.BytesIO(b'x'))
                if mode=='duplicate':archive.addfile(member,io.BytesIO(b'x'))
            with tempfile.TemporaryDirectory() as temp:
                with self.assertRaisesRegex(ValueError,'unsafe/duplicate'):verify_evidence.safe_extract(stream.getvalue(),pathlib.Path(temp),100)

    def test_tar_byte_quota_is_enforced_before_extraction(self):
        stream=io.BytesIO()
        with tarfile.open(fileobj=stream,mode='w:gz') as archive:
            member=tarfile.TarInfo('file');member.size=10;archive.addfile(member,io.BytesIO(b'x'*10))
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaisesRegex(ValueError,'size/count'):verify_evidence.safe_extract(stream.getvalue(),pathlib.Path(temp),9)

    def test_duplicate_window_output_is_refused_inside_one_lineage(self):
        m=oracle.Model(config());m.frontier=100;m.state[(0,1)]=(3,1)
        frame=dict(type='window',output_version=1,job_id='test-job',key=1,window_start=0,window_end=100,sum_i64=3,count_u64=1,output_id='test-job:1:0:100:1')
        m.consume(frame,{})
        with self.assertRaisesRegex(ValueError,'unexpected/reopened'):m.consume(frame,{})

if __name__=='__main__':unittest.main()
