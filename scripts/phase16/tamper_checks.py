#!/usr/bin/env python3
"""Attack copies, regenerating inner BLAKE3 pins AND outer SHA inventories.
Normal evidence verification is stdlib-only. Attack generation additionally needs
rustc and cached blake3 artifacts from cargo build --workspace --all-features.
Never modifies the accepted archive.
"""
import argparse, copy, hashlib, json, pathlib, struct, subprocess, tarfile, tempfile
from verify_evidence import archive_verify

def encoded(value):return json.dumps(value,separators=(',',':'),ensure_ascii=False).encode()
def run(archive,manifest):
    results={};repo=pathlib.Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix='ade16-attacks-') as temp:
        root=pathlib.Path(temp);source=root/'hash.rs';binary=root/'hash'
        source.write_text('use std::io::Read; fn main(){let mut b=Vec::new();std::io::stdin().read_to_end(&mut b).unwrap();println!("{}",blake3::hash(&b).to_hex());}')
        libraries=sorted((repo/'target/debug/deps').glob('libblake3-*.rlib'),key=lambda p:p.stat().st_mtime,reverse=True)
        if not libraries:raise ValueError('build cached Rust blake3 artifacts first')
        subprocess.run(['rustc','--edition=2024',str(source),'--extern','blake3='+str(libraries[0]),'-L','dependency='+str(repo/'target/debug/deps'),'-o',str(binary)],check=True)
        def b3(b):return subprocess.check_output([str(binary)],input=b).decode().strip()
        with tarfile.open(archive,'r:gz') as t:original={m.name:t.extractfile(m).read() for m in t.getmembers()}
        for attack in ['receipt-semantic-forgery','checkpoint-semantic-forgery','policy-cost-forgery']:
            files=dict(original)
            if attack=='receipt-semantic-forgery':
                name='local-adaptive-0.output.jsonl';frames=[json.loads(x) for x in files[name].splitlines()];frames[0]['receipt']['projected']['value_i64']+=999;files[name]=b'\n'.join(encoded(x) for x in frames)+b'\n'
            elif attack=='policy-cost-forgery':
                name='local-adaptive-0.trace.jsonl';frames=[json.loads(x) for x in files[name].splitlines()];decision=next(f for f in frames if f['type']=='adaptive_decision');decision['decision']['candidates'][0]['score']+=1;files[name]=b'\n'.join(encoded(x) for x in frames)+b'\n'
            else:
                prefix='local-adaptive-0.checkpoints/';selector=json.loads(files[prefix+'CURRENT2']);cert=selector['certificate'];generation=cert['manifest']['generation'];mpath=prefix+generation+'/manifest.json';m=json.loads(files[mpath])
                # Alter a real payload and recompute record/object/manifest/certificate pins.
                shard=next(s for s in m['shards'] if s['object_generation']==generation and s['count']>0)
                path=prefix+generation+f"/shard-{shard['shard']:02}.bin";raw=bytearray(files[path]);amount=struct.unpack('<q',raw[16:24])[0];raw[16:24]=struct.pack('<q',amount+999);raw[32:64]=bytes.fromhex(b3(raw[:32]));files[path]=bytes(raw);shard['blake3']=b3(raw)
                files[mpath]=encoded(m);mh=b3(files[mpath]);cert['manifest'].update(blake3=mh,bytes=len(files[mpath]));cb=encoded(cert);ch=b3(cb);files[prefix+generation+'/certificate.json']=cb;selector['certificate_blake3']=ch;files[prefix+'CURRENT2']=encoded(selector)
                name='local-adaptive-0.trace.jsonl';frames=[json.loads(x) for x in files[name].splitlines()]
                for frame in frames:
                    if frame.get('type')=='adaptive_decision' and frame['certificate']['manifest']['generation']==generation:frame['certificate']=copy.deepcopy(cert);frame['certificate_blake3']=ch
                    for field in ['pointer','generation']:
                        if isinstance(frame.get(field),dict) and frame[field].get('generation')==generation:frame[field]['manifest_blake3']=mh
                files[name]=b'\n'.join(encoded(x) for x in frames)+b'\n'
            folder=root/attack;folder.mkdir()
            paths={}
            for name,b in files.items():p=folder/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b);paths[name]=p
            forged=root/(attack+'.tar.gz')
            with tarfile.open(forged,'w:gz',compresslevel=1) as t:
                for name,path in sorted(paths.items()):t.add(path,arcname=name,recursive=False)
            meta=root/(attack+'.json');meta.write_text(json.dumps(dict(version=1,archive_sha256=hashlib.sha256(forged.read_bytes()).hexdigest(),files={k:hashlib.sha256(v).hexdigest() for k,v in files.items()})))
            try:archive_verify(forged,meta)
            except (ValueError,KeyError,StopIteration) as e:results[attack]=dict(refused=True,reason=str(e))
            else:raise ValueError('semantic forgery accepted: '+attack)
        raw=bytearray(pathlib.Path(archive).read_bytes());raw[len(raw)//2]^=1;mutated=root/'one-byte.tar.gz';mutated.write_bytes(raw)
        try:archive_verify(mutated,manifest)
        except ValueError as e:results['one-byte-archive']=dict(refused=True,reason=str(e))
        else:raise ValueError('one-byte tampering accepted')
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--archive',type=pathlib.Path,required=True);p.add_argument('--manifest',type=pathlib.Path,required=True);a=p.parse_args();print(json.dumps(run(a.archive,a.manifest),sort_keys=True))
