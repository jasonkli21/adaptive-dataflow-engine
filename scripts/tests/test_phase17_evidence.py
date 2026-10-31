"""Independent digest, trace, tamper and semantic oracle regressions."""
import importlib.util,pathlib,sys,unittest
BASE=pathlib.Path(__file__).resolve().parents[1]/'phase17'
def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path);result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result
digest=module('phase17_digest',BASE/'digest.py')
service=module('phase17_service_fixture',BASE/'service_fixture.py')
saved={name:sys.modules.get(name) for name in ['digest','service_fixture']}
sys.modules['digest']=digest;sys.modules['service_fixture']=service
oracle=module('phase17_contract',BASE/'oracle.py')
for name,value in saved.items():
    if value is None:del sys.modules[name]
    else:sys.modules[name]=value
Model=oracle.Model
class Phase17(unittest.TestCase):
    def test_blake3_public_vectors(self):
        self.assertEqual(digest.digest(b''),'af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262')
        self.assertEqual(digest.digest(b'abc'),'6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85')
    def test_native_window_contract_negative_time_and_late_close(self):
        c={'state':{'source':{'starts':[{'partition':0,'offset':0},{'partition':1,'offset':0}],'allowance_ms':10,'key_less_than':1000},'window_ms':100,'job_id':'job'}}
        m=Model(c);ledger={(0,0):{'key':1,'value_i64':2,'event_time_ms':-50},(1,0):{'key':1,'value_i64':3,'event_time_ms':-50},(0,1):{'key':9999,'value_i64':0,'event_time_ms':200},(1,1):{'key':9999,'value_i64':0,'event_time_ms':200},(0,2):{'key':1,'value_i64':7,'event_time_ms':-50}}
        for p,off in ledger:m.consume({'partition':p,'offset':off},ledger)
        self.assertEqual(m.outputs[0]['sum_i64'],5);self.assertEqual(m.outputs[0]['window_start'],-100);self.assertEqual(m.closed_late,1);self.assertEqual(m.state,{})
    def test_arithmetic_fails_closed(self):
        checked=oracle.checked
        with self.assertRaises(ValueError):checked(2**63)
        with self.assertRaises(ValueError):checked(-2**63-1)
if __name__=='__main__':unittest.main()
