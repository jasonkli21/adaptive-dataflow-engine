#!/usr/bin/env python3
"""Rehash archive metadata after semantic attacks; the independent oracle must refuse."""
import argparse,copy,hashlib,io,json,pathlib,tarfile,tempfile
from verify_evidence import archive,load,oracle

def run(archive_path,manifest_path,output):
    output.mkdir(parents=True,exist_ok=False);results=[]
    with tarfile.open(archive_path,'r:gz') as tar:original={m.name:tar.extractfile(m).read() for m in tar if m.isfile()}
    def encode(value):return json.dumps(value,separators=(',',':')).encode()
    def trace_attack(files,field):
        name='hybrid2.stderr';rows=[json.loads(line) for line in files[name].splitlines()];r=next(r for r in rows if r.get('type')==('unified_confirmed' if field=='phase' else 'unified_decision'))
        if field=='phase':r['record']['phase']='live'
        elif field=='property':r['decision']['properties']['termination']='finite'
        else:r['decision']['candidates'][0]['estimated_units']=1
        files[name]=b'\n'.join(encode(r) for r in rows)+b'\n'
    def state_attack(files):
        folder='restart-prefix.checkpoint/';pointer=json.loads(files[folder+'CURRENT']);path=folder+pointer['generation']+'/manifest.json';manifest=json.loads(files[path]);ref=next(s for s in manifest['shards'] if s['count']);shard=folder+pointer['generation']+f"/shard-{ref['shard']:02}.bin";b=bytearray(files[shard]);import struct
        amount=struct.unpack('<q',b[16:24])[0];b[16:24]=struct.pack('<q',amount+1);b[32:64]=oracle.raw(b[:32]);files[shard]=bytes(b);ref['blake3']=oracle.digest(b);files[path]=encode(manifest);pointer['manifest_blake3']=oracle.digest(files[path]);files[folder+'CURRENT']=encode(pointer)
    for label in ['phase','property','cost','state-rehashed','reader-output','missing-ack','source-rehashed','unknown-plan-field']:
        files=dict(original)
        if label in ['phase','property','cost']:trace_attack(files,label)
        elif label=='state-rehashed':state_attack(files)
        elif label in ['source-rehashed','unknown-plan-field']:
            name='empty-prefix-stateless.logical.json';plan=json.loads(files[name])
            if label=='source-rehashed':plan['computation']['source']['topic']='forged-topic'
            else:plan['computation']['forged_property']='trusted'
            files[name]=encode(plan);trace_name='empty-prefix-stateless.stderr';records=[json.loads(line) for line in files[trace_name].splitlines()];new_digest=oracle.digest(oracle.canonical(plan))
            for record in records:
                if record.get('type')=='unified_decision':record['decision']['semantic_digest']=new_digest
                if record.get('type')=='unified_confirmed':record['record']['semantic_digest']=new_digest
            files[trace_name]=b'\n'.join(encode(record) for record in records)+b'\n'
        elif label=='reader-output':
            name='v3-reader.stdout';rows=[json.loads(line) for line in files[name].splitlines()];rows[0]['sum_i64']+=1;rows[0]['payload_fingerprint']=f"window-payload-v1:{rows[0]['sum_i64']}:{rows[0]['count_u64']}";files[name]=b'\n'.join(encode(r) for r in rows)+b'\n'
        else:
            name='hybrid2.stderr';rows=[json.loads(line) for line in files[name].splitlines()];index=next(i for i,r in enumerate(rows) if r.get('type')=='unified_confirmed');rows.pop(index);files[name]=b'\n'.join(encode(r) for r in rows)+b'\n'
        with tempfile.TemporaryDirectory(prefix='ade18-tamper-') as tmp:
            root=pathlib.Path(tmp);path=root/'attack.tar.gz';manifest=root/'manifest.json'
            with tarfile.open(path,'w:gz') as tar:
                for name,b in files.items():m=tarfile.TarInfo(name);m.size=len(b);tar.addfile(m,io.BytesIO(b))
            metadata=load(manifest_path);metadata['files']={n:hashlib.sha256(b).hexdigest() for n,b in files.items()};metadata['archive_sha256']=hashlib.sha256(path.read_bytes()).hexdigest();manifest.write_text(json.dumps(metadata))
            try:archive(path,manifest)
            except ValueError as error:results.append(dict(attack=label,result='REFUSED',reason=str(error)))
            else:raise ValueError('semantic tamper accepted: '+label)
    (output/'results.json').write_text(json.dumps(results,indent=2)+'\n');print(json.dumps(results,sort_keys=True))
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--archive',required=True,type=pathlib.Path);p.add_argument('--manifest',required=True,type=pathlib.Path);p.add_argument('--output',required=True,type=pathlib.Path);a=p.parse_args();run(a.archive,a.manifest,a.output)
