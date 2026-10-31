#!/usr/bin/env python3
"""Portable independent semantic verification. Never trust authored PASS labels."""
import argparse,hashlib,io,json,pathlib,tarfile,tempfile,re
from digest import digest,raw
from oracle import chain,verify_trace,shard
from service_fixture import require,rows
import struct

def load(p):return json.loads(pathlib.Path(p).read_text())
def sha(b):return hashlib.sha256(b).hexdigest()
def source(root):
    inventory=load(root/'source-inventory.json')
    with tarfile.open(root/'source.tar.gz','r:gz') as tar:
        members=tar.getmembers();require(len(members)<=2048 and sum(m.size for m in members)<=33554432 and all(m.isfile() and m.size<=4194304 for m in members),'source archive cap/type');files={m.name:tar.extractfile(m).read() for m in members}
    require(set(files)==set(inventory),'source inventory coverage')
    for p,h in inventory.items():require(sha(files[p])==h,'source hash mismatch: '+p)
    runtime=load(root/'matched-runtime-inventory.json');require(all(inventory.get(p)==h for p,h in runtime.items()) and len(runtime)==139,'matched runtime source inventory')
    env=load(root/'environment.json');require(env['base']==load(root/'criteria.json')['starting_head'],'wrong baseline')
    for name,h in env['binaries'].items():
        p=root/(name+'-pinned')
        if p.exists():require(sha(p.read_bytes())==h,'pinned executable hash')
    require(len(env['binaries'])==4 and len(set(env['binaries'].values()))==4,'missing executable identities')
    return len(inventory)

def prerequisite(root,ledgers):
    service=load(root/'services.json');require(service['kafka']=='3.9.1' and service['minio_pid']!=service['broker_pid'] and service['minio_pid']!=service['proxy_pid'],'services not separate')
    require(len(service['minio_sha256'])==64 and (root/'minio.stderr').exists() and (root/'broker.stderr').exists(),'service identities/logs absent')
    wire=rows(root/'wire.jsonl');puts={};gets={}
    for r in wire:
        require(r['method'] in ['PUT','HEAD','GET'] and r['signed'],'broad/unsigned remote request')
        for field in ['request','response']:
            b=(root/'wire-bodies'/r[field+'_sha256']).read_bytes();require(len(b)==r[field+'_bytes'] and sha(b)==r[field+'_sha256'],'wire body integrity')
        if r['method']=='PUT':
            require(r['conditional_create'],'remote overwrite attempt')
            if r['status']==200:puts[r['path']]=r
        if r['method']=='GET' and r['status']==200:gets.setdefault(r['path'],[]).append(r)
    def fetch(remote,generation,name):
        key=f"/{remote['bucket']}/{remote['prefix']}/checkpoint-v2/{remote['namespace']}/{generation}/{name}"
        require(key in puts and key in gets,'required remote object lacks PUT/GET')
        put=puts[key];b=(root/'wire-bodies'/put['request_sha256']).read_bytes();require(any(g['response_sha256']==put['request_sha256'] and g['serial']>put['serial'] for g in gets[key]),'remote readback differs');return b
    def v2(config_path):
        c=load(config_path);check=root/pathlib.Path(c['checkpoint_root']).parent.name/'check'
        # Archives retain original absolute configs; resolve owned roots by their basename pair.
        if not check.exists():check=root/pathlib.Path(c['checkpoint_root']).parent.name/'check'
        selected=load(check/'CURRENT2');cert=selected['certificate'];g=cert['manifest']['generation'];cert_raw=(check/g/'certificate.json').read_bytes();require(digest(cert_raw)==selected['certificate_blake3'] and json.loads(cert_raw)==cert,'v2 certificate binding')
        manifest_path=check/g/'manifest.json';b=manifest_path.read_bytes() if manifest_path.exists() else fetch(cert['remote'],g,'manifest.json');require(digest(b)==cert['manifest']['blake3'] and len(b)==cert['manifest']['bytes'],'v2 manifest pin');m=json.loads(b)
        actual={}
        for s,ref in enumerate(m['shards']):
            p=check/ref['object_generation']/f'shard-{s:02}.bin';b=p.read_bytes() if p.exists() else fetch(cert['remote'],ref['object_generation'],f'shard-{s:02}.bin');require(digest(b)==ref['blake3'] and len(b)==ref['bytes']==ref['count']*64,'v2 shard pin')
            for off in range(0,len(b),64):
                record=b[off:off+64];require(raw(record[:32])==record[32:],'v2 record checksum');start,key,amount,count=struct.unpack('<qQqQ',record[:32]);require(shard(key)==s,'v2 shard owner');require((start,key) not in actual,'v2 duplicate state');actual[(start,key)]=(amount,count)
        expected={};consumed=0
        for r in ledgers[c['source']['topic']]:
            if r['offset']<m['progress']['partitions'][str(r['partition'])]['next_offset_suggestion']:
                e=r['event'];require(e['event_time_ms']==0,'prerequisite oracle expects declared t0 fixture');key=(0,e['key']);amount,count=expected.get(key,(0,0));expected[key]=(amount+e['value_i64'],count+1);consumed+=1
        require(actual==expected and consumed==m['progress']['forwarded_records'],'real-service full-state/source oracle mismatch')
        return m
    v2(root/'p16-retention-seed.config.json');m=v2(root/'p16-retained-saved-offsets.config.json');require([v['next_offset_suggestion'] for v in m['progress']['partitions'].values()]==[20,20],'retained saved vector differs')
    trace=rows(root/'p16-retained-saved-offsets.stderr');decision=next(r for r in trace if r.get('type')=='recovery_decision');sources=decision['model']['sources'];require(not sources['replay']['eligible'] and sources['restore']['eligible'] and decision['path']=='checkpoint_restore','feasibility-first restore absent');require(not (root/'p16-retained-saved-offsets.stdout').read_bytes(),'unchanged restore emitted output')
    require(b'PREFLIGHT_COMPLETE' in (root/'p16-race.stdout').read_bytes() and b'offset range/retention/truncation' in (root/'p16-race.stderr').read_bytes(),'preflight/assignment race not refused')
    require(not (root/'p16-both-vectors-expired.stdout').read_bytes() and any(r.get('type')=='failure' and 'recovery unavailable' in r.get('reason','') for r in rows(root/'p16-both-vectors-expired.stderr')),'both-expired source not refused')
    # Independent policy arithmetic is shared with Phase16's separate Python contract evaluator.
    import importlib.util
    p=pathlib.Path(__file__).resolve().parents[1]/'phase16/oracle.py';spec=importlib.util.spec_from_file_location('phase16_independent',p);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    for mode in ['ample','veto']:
        trace=rows(root/('p16-placement-'+mode+'.stdout'));cuts=[r for r in trace if r.get('type')=='cut'];control=next(r for r in trace if r.get('type')=='before_transition');d=cuts[-1]['decision'];module.check_decision(control['policy'],d)
        require(len(cuts)==3 and d['observation']['samples']>=2,'unmeasured placement campaign');require(d['chosen']['format']=='full' and d['chosen']['placement']==('remote' if mode=='ample' else 'local'),'wrong effective placement')
        v2(root/('p16-placement-'+mode+'.config.json'))
        if mode=='veto':
            require(any(c['action']['format']=='full' and c['action']['placement']=='remote' and c['rejection']=='capacity_floor' for c in d['candidates']),'capacity veto absent')
            remote=cuts[0]['certificate']['remote'];prefix=f"/{remote['bucket']}/{remote['prefix']}/checkpoint-v2/{remote['namespace']}/";allowed={c['certificate']['manifest']['generation'] for c in cuts[:2]}
            writes=[r for r in wire if r['method']=='PUT' and r['path'].startswith(prefix)];extra=[r for r in writes if not any(g in r['path'] for g in allowed)];require(len(extra)==2 and all('/probe-' in r['path'] for r in extra),'PUT occurred after placement capacity veto')
    return dict(cases=5,requests=len(wire),remote_objects=len(puts))

def exports(root,ledgers,attempts):
    count=0
    for label in ['atomic-export','lifecycle-export','previous-export-safe']:
        c=load(root/(label+'.config3.json'));cap=next(r for r in rows(root/(label+'.stderr')) if r.get('status')=='EXPORTED');path=root/'savepoints'/pathlib.Path(cap['path']).name
        top=(path/'SAVEPOINT3').read_bytes();require(digest(top)==cap['blake3'],'retained export digest mismatch');m=json.loads(top);require(m['version']==3,'export version')
        cuts=chain(path,c,m['selected']);expected=[];observed=[]
        for epoch,entries,output in cuts:
            prefix=epoch['id']+'/';expected.extend([prefix+'epoch.json',prefix+'output.jsonl']+[prefix+f'shard-{s:02}.bin' for s in range(32)]);observed.extend(output)
        require([f['path'] for f in m['files']]==expected,'export self-contained exact path closure')
        for f in m['files']:
            data=(path/f['path']).read_bytes();require(len(data)==f['object']['bytes'] and digest(data)==f['object']['blake3'],'export object inventory hash')
        verify_trace(path,c,ledgers,attempts,observed,m['selected']);count+=1
    return count

def checks(root):
    v=root/'validation';rust=(v/'ade17-final-workspace-tests-unrestricted.log').read_text();summaries=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored',rust)
    require(sum(int(r[0]) for r in summaries)==301 and all(int(r[1])==0 for r in summaries) and sum(int(r[2]) for r in summaries)==10 and 'test result: FAILED' not in rust,'workspace correctness log mismatch')
    require('Ran 54 tests' in (v/'ade17-final-python-3.log').read_text() and (v/'ade17-final-python-3.log').read_text().rstrip().endswith('OK'),'Python correctness log mismatch')
    for label in ['check','clippy']:require('Finished' in (v/('ade17-final-'+label+'.log')).read_text(),'locked build gate log missing')
    require(not (v/'ade17-final-fmt.log').read_bytes(),'format gate log not clean')
    require('"validated": true' in (v/'ade17-final-smoke.log').read_text(),'smoke result missing')
    require(load(root/'required-checks.json')['runtime_inventory_sha256']==sha((root/'matched-runtime-inventory.json').read_bytes()),'validation source binding')

def verify(root):
    root=pathlib.Path(root);sources=source(root);criteria=load(root/'criteria.json');env=load(root/'environment.json');ledgers=load(root/'ledgers.json');cases=load(root/'cases.json');observations=load(root/'observations.json')
    require(load(root/'outcome.json')['status']=='CAMPAIGN_FINISHED','incomplete campaign')
    names={c['name']:c for c in cases};require(len(names)==len(cases),'duplicate case identity')
    attempts={}
    for path in root.glob('*.stderr'):
        trace=rows(path);admitted=[r for r in trace if r.get('type')=='admitted']
        if admitted:require(admitted[0]['session'] not in attempts,'reused attempt identity');attempts[admitted[0]['session']]=trace
    cuts=0
    for record in observations:
        c=load(root/record['config']);observed=[json.loads(line) for line in (root/record['reader']).read_bytes().splitlines()];proof=verify_trace(root/record['snapshot'],c,ledgers,attempts,observed);require(proof==record['proof'],'authored proof disagrees with actual records');cuts+=proof['cuts']
    obs={r['name']:r for r in observations}
    for point in criteria['crash_points']:
        require(names['kill-'+point]['exit']==-9,'missing actual SIGKILL');old=point not in criteria['new_selection_points']
        for suffix in ['-concurrent-reader','-after-kill']:
            proof=obs['kill-'+point+suffix]['proof'];require(proof['records']==(80 if old else 82) and proof['outputs']==(0 if old else 80) and proof['state_entries']==(80 if old else 0),'atomic old/new witness mismatch')
        require(obs['kill-'+point+'-final']['proof']['outputs']==80,'crash resume duplicate/missing output')
    for label in ['migration-kill','export-kill','import-kill']:
        require(names[label]['exit']==-9,'missing real lifecycle kill')
    require(not (root/'import-kill-reader.stdout').read_bytes(),'partial import reader exposed output')
    require(names['competing-writer']['exit']==1 and names['signal-pause']['exit']==0 and names['slow-local-io']['exit']==1,'pause/fence/demand witness missing')
    before=[r for r in rows(root/'slow-local-io.stderr') if r.get('type')=='input'];require(len(before)==2,'source-demand pause advanced during local I/O');require(names['file-size-io-failure']['rlimit_fsize']==1024 and names['file-size-io-failure']['exit']==1,'real kernel I/O refusal missing')
    for owner in [2,4,1]:require(obs[f'transition-2-{owner}']['proof']['owners']==owner and obs[f'transition-2-{owner}']['proof']['state_entries']==80,'actual full-state owner transfer missing')
    conflict=load(root/'conflicting-payloads.json');require(conflict[0]['output_id']==conflict[1]['output_id'] and conflict[0]['payload_fingerprint']!=conflict[1]['payload_fingerprint'],'conflicting legal-order payload experiment absent')
    negatives=['wrong-history','wrong-job','wrong-origin','wrong-codec','wrong-owners','epoch-quota','stage-quota','output-quota','history-quota','corrupt-selector','missing-output','modified-output','symlink-output','hardlink-output','wrong-shard-owner','broken-parent','failed-migration','failed-export','import-kill-replay-refused']
    for label in negatives:
        require(names[label]['exit']==1,'missing negative '+label);trace=rows(root/(label+'.stderr'));require(any(r.get('type')=='failure' for r in trace) and not any(r.get('status') in ['PAUSED','TRANSITIONED','IMPORTED','EXPORTED'] for r in trace),'false operation success '+label);require(not (root/(label+'.stdout')).read_bytes(),'negative business output '+label)
    repeats=load(root/'ordered-repeats.json');require(len(repeats)==2 and repeats[0]==repeats[1],'ordered trace repeat differs');require(all(f'ordered-repeat-{i}-repeat-check' in obs for i in range(2)),'missing real ordered repeat observations')
    costs=load(root/'costs.json');require(len(costs)==6 and {r['mode'] for r in costs}=={'txn','static_v1'},'descriptive baseline samples absent')
    export_count=exports(root,ledgers,attempts);checks(root)
    prereq=prerequisite(root,ledgers)
    extra=root/'supplemental/enospc';enospc=load(extra/'outcome.json');require(enospc['errno']==28 and enospc['binary_sha256']==env['binaries']['txn'],'actual ENOSPC matched executable missing')
    require(load(extra/'statvfs-full.json')['available']==0 and 'No space left on device' in (extra/'fill-error.stderr').read_text(),'filesystem was not actually full')
    require((extra/'reader-before.stdout').read_bytes()==(extra/'reader-after.stdout').read_bytes(),'ENOSPC output visibility changed')
    require(sha((extra/'selected-and-orphans/CURRENT3').read_bytes())==enospc['selector_sha256'],'ENOSPC selected pointer changed')
    ec=load(extra/'config3.json');eo=[json.loads(line) for line in (extra/'reader-after.stdout').read_bytes().splitlines()];ep=verify_trace(extra/'selected-and-orphans',ec,ledgers,attempts,eo);require(ep['records']==80 and ep['state_entries']==80,'ENOSPC old state/source closure missing')
    return dict(cases=len(cases),observations=len(observations),matched_cuts=cuts,sessions=len(attempts),source_files=sources,prerequisite=prereq,enospc=ep,exports=export_count)

def archive(path,manifest):
    raw_bytes=pathlib.Path(path).read_bytes();m=load(manifest);require(sha(raw_bytes)==m['archive_sha256'],'archive hash')
    with tempfile.TemporaryDirectory(prefix='ade17-offline-') as tmp:
        root=pathlib.Path(tmp)
        with tarfile.open(fileobj=io.BytesIO(raw_bytes),mode='r:gz') as tar:
            members=tar.getmembers();require(len(members)<=30000,'archive count cap');total=0;found=set()
            for member in members:
                path=pathlib.PurePosixPath(member.name);require(member.isfile() and not path.is_absolute() and '..' not in path.parts and member.name in m['files'] and member.name not in found,'unsafe archive member');total+=member.size;require(total<=536870912,'archive byte cap');b=tar.extractfile(member).read();require(sha(b)==m['files'][member.name],'archive member hash');dest=root/member.name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(b);found.add(member.name)
            require(found==set(m['files']),'archive inventory mismatch')
        return verify(root)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--campaign',type=pathlib.Path);p.add_argument('--archive',type=pathlib.Path);p.add_argument('--manifest',type=pathlib.Path);a=p.parse_args();print(json.dumps(verify(a.campaign) if a.campaign else archive(a.archive,a.manifest),sort_keys=True))
