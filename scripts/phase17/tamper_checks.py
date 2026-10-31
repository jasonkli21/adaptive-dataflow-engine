#!/usr/bin/env python3
"""Hash-consistent output/state forgeries must fail the separate source-ledger oracle."""
import argparse,copy,json,pathlib,shutil,struct,tempfile
from digest import digest,raw
from oracle import verify_trace
from service_fixture import require,rows,dump

def attacks(campaign):
    root=pathlib.Path(campaign);observations=json.loads((root/'observations.json').read_text());ledgers=json.loads((root/'ledgers.json').read_text());attempts={}
    for path in root.glob('*.stderr'):
        trace=rows(path);admitted=[r for r in trace if r.get('type')=='admitted']
        if admitted:attempts[admitted[0]['session']]=trace
    outcomes=[]
    for kind,label in [('output','kill-before_stage-final'),('state','atomic-seed')]:
        observation=next(o for o in observations if o['name']==label);config=json.loads((root/observation['config']).read_text());observed=[json.loads(line) for line in (root/observation['reader']).read_bytes().splitlines()]
        with tempfile.TemporaryDirectory(prefix='ade17-forgery-') as tmp:
            check=pathlib.Path(tmp)/'check';shutil.copytree(root/observation['snapshot'],check);selector=json.loads((check/'CURRENT3').read_text());folder=check/selector['id'];manifest=json.loads((folder/'epoch.json').read_text())
            if kind=='output':
                lines=[json.loads(line) for line in (folder/'output.jsonl').read_bytes().splitlines()];require(lines,'chosen snapshot has no output');lines[0]['sum_i64']+=999;lines[0]['payload_fingerprint']=f"window-payload-v1:{lines[0]['sum_i64']}:{lines[0]['count_u64']}";data=b''.join(json.dumps(o,separators=(',',':')).encode()+b'\n' for o in lines);(folder/'output.jsonl').write_bytes(data);manifest['output'].update(bytes=len(data),blake3=digest(data));observed=copy.deepcopy(observed);observed[0]=lines[0]
            else:
                ref=next(s for s in manifest['shards'] if s['count']);path=folder/f"shard-{ref['shard']:02}.bin";data=bytearray(path.read_bytes());amount=struct.unpack('<q',data[16:24])[0];data[16:24]=struct.pack('<q',amount+999);data[32:64]=raw(data[:32]);path.write_bytes(data);ref['object']['blake3']=digest(data)
                canonical=b''.join((folder/f'shard-{s:02}.bin').read_bytes() for s in range(32));manifest['state_blake3']=digest(canonical)
            data=json.dumps(manifest,separators=(',',':')).encode();(folder/'epoch.json').write_bytes(data);selector['blake3']=digest(data);dump(check/'CURRENT3',selector)
            try:verify_trace(check,config,ledgers,attempts,observed)
            except ValueError as error:
                require('differs' in str(error),'forgery failed hashes/shape instead of independent semantics: '+str(error));outcomes.append(dict(kind=kind,result='REFUSED_BY_INDEPENDENT_SEMANTICS',reason=str(error)))
            else:raise ValueError('hash-consistent forged '+kind+' accepted')
    return outcomes
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--campaign',required=True,type=pathlib.Path);a=p.parse_args();print(json.dumps(attacks(a.campaign),sort_keys=True))
