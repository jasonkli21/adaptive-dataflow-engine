#!/usr/bin/env python3
"""Portable source-bound semantic verifier: producer/state/cost/wire/atomicity.
Only Python stdlib. Checksums alone are insufficient; no authored PASS is trusted.
"""
import argparse, copy, hashlib, io, json, pathlib, tarfile, tempfile
from oracle import Model, check_decision, check_recovery, check_snapshot, require, rows
POINTS=['policy_decided','stage_created','shard_write','shard_synced','remote_shard_put','remote_shard_verified','manifest_write','manifest_synced','remote_manifest_put','remote_manifest_verified','stage_synced','generation_renamed','generation_synced','copies_verified','remote_certificate_put','remote_certificate_verified','certificate_synced','before_selector','selector_synced','selector_renamed','selector_durable']
REMOTE_FAILURES=['put-shard','head-shard','get-shard','put-manifest','put-certificate','ambiguous-shard','ambiguous-certificate','redirect','oversize-error','timeout']
BASE=[f'local-{m}-{i}' for i in range(3) for m in ['static','adaptive']]+['remote-adaptive-0','both-adaptive-0']
NEGATIVE=['wrong-consent','no-consent','untrusted-history','wrong-topic','wrong-origin','weaken-copies','corrupt-selector','slow-control','remote-objects','requests','response-budget','trace-budget','generation-budget','retention-lost','retention-both-lost','service-outage','missing-credentials','wrong-credentials','capability-unavailable','disk-loss-reuse-refused','checkpoint-byte-budget','cold-budget']
_COMMON_NEGATIVE=[name for name in NEGATIVE if name not in ('retention-lost','retention-both-lost')]
_COMMON=['cadence-low-risk','cadence-high-risk','adaptive-placement','restore-local','restore-remote','clean-scratch','full-replay','fault-seed','retention-seed','outage-local-copy','window-a-seed','window-a','window-b-seed','window-b','clean-scratch-resume','remote-rebuild-local-copy']
_TAIL=['kill-'+p+s for p in POINTS for s in ('','-restore')]+['remote-'+p+s for p in REMOTE_FAILURES for s in ('','-injected','-restore')]
EXPECTED=set(BASE+_COMMON+_COMMON_NEGATIVE+['retention-lost']+_TAIL)
REMEDIATED_EXPECTED=(EXPECTED-{'retention-lost'})|{'retention-both-lost','retention-restore-after-expiry'}

def load(p):return json.loads(pathlib.Path(p).read_text())
def identity(c,p):return (c['job_id'],c['source']['topic'],p['forwarded_records'],tuple((k,v['next_offset_suggestion']) for k,v in sorted(p['partitions'].items())))
def output(root,name):
    b=(root/(name+'.output.jsonl')).read_bytes()
    if b and not b.endswith(b'\n'):b=b.rsplit(b'\n',1)[0] if b'\n' in b else b''
    return [json.loads(x) for x in b.splitlines() if x]

class Wire:
    def __init__(self,root):
        self.root=root;self.rows=rows(root/'wire.jsonl');self.puts={};self.by_key={}
        serials=[r['serial'] for r in self.rows];require(len(serials)==len(set(serials)),'duplicate wire request')
        for r in self.rows:
            self.by_key.setdefault(r['path'].split('?')[0],[]).append(r)
            require(r['method'] in ['PUT','HEAD','GET'] and r['signed'],'unsigned/broad/unsupported service request')
            if r['method']=='PUT':
                require(r['conditional_create'],'mutable remote create')
                if r['status']==200 and r['fault'] not in ['500','503','redirect','oversize']:
                    self.puts[r['path'].split('?')[0]]=r
            for field in ['request','response']:
                raw=(root/'wire-bodies'/r[field+'_sha256']).read_bytes()
                require(len(raw)==r[field+'_bytes'] and hashlib.sha256(raw).hexdigest()==r[field+'_sha256'],'wire body length/hash')
        require(any(r['method']=='PUT' and r['status']==412 and '/probe-' in r['path'] for r in self.rows),'real conditional capability conflict absent')
        require(not any('never' in r['path'] for r in self.rows),'redirect was followed')
    @staticmethod
    def key(remote,g,name):return '/ade-phase16/'+remote['prefix']+'/checkpoint-v2/'+remote['namespace']+'/'+g+'/'+name
    def get(self,remote,g,name,before=None):
        key=self.key(remote,g,name);require(key in self.puts,'required remote object was never created')
        put=self.puts[key];limit=before or 10**18
        require(put['serial']<limit,'remote object created after eligibility')
        body=(self.root/'wire-bodies'/put['request_sha256']).read_bytes()
        heads=[r for r in self.by_key[key] if r['method']=='HEAD' and r['status']==200 and put['serial']<r['serial']<limit]
        gets=[r for r in self.by_key[key] if r['method']=='GET' and r['status']==200 and r['response_sha256']==put['request_sha256'] and put['serial']<r['serial']<limit]
        require(heads and gets,'required copy lacks independent HEAD/GET/readback before eligibility')
        return body
    def certificate_boundary(self,cert):
        key=self.key(cert['remote'],cert['manifest']['generation'],'certificate.json');require(key in self.puts,'remote certificate absent');return self.puts[key]['serial']


def ancestry(c,latest,get,known,states):
    chain=[latest];seen={latest['generation']}
    while chain[-1]['prior'] is not None:
        parent=chain[-1]['prior'];require(parent['generation'] not in seen and len(chain)<5,'cycle/unbounded ancestry')
        raw=get(parent['generation'],'manifest.json');require(len(raw)==parent['bytes'],'ancestor length pin')
        m=json.loads(raw);require(m['generation']==parent['generation'] and m['depth']+1==chain[-1]['depth'] and m['progress']['forwarded_records']<chain[-1]['progress']['forwarded_records'],'ancestor identity/progress/depth')
        for field in ['logical_fingerprint','job_id','origin','placement','remote','min_copies','require_remote','max_loss_ppm']:require(m[field]==latest[field],'foreign ancestor semantic identity')
        seen.add(m['generation']);chain.append(m)
    require(chain[-1]['depth']==0,'unclosed ancestry')
    for index,m in enumerate(chain):
        for shard in m['shards']:
            if shard['object_generation']!=m['generation']:
                require(index+1<len(chain),'full references outside generation')
                old=chain[index+1]['shards'][shard['shard']]
                for field in ['object_generation','bytes','count','blake3']:require(shard[field]==old[field],'foreign ancestor object pin')
        if index:
            key=identity(c,m['progress']);model=known.get(key) or states.get(m['progress']['forwarded_records'])
            require(model is not None,'ancestor lacks independent confirmed prefix');check_snapshot(None,m,model,get)


def verify(root):
    root=pathlib.Path(root);cases=load(root/'cases.json');names=[c['name'] for c in cases]
    require(len(names)==len(set(names)),'duplicate process contract')
    case_names=set(names)
    if case_names==EXPECTED:remediated=False
    elif case_names==REMEDIATED_EXPECTED:remediated=True
    else:require(False,'missing/extra mandatory process contracts')
    env=load(root/'environment.json');outcome=load(root/'outcome.json');require(env['binary_sha256']==outcome['binary_sha256'],'executable changed during campaign')
    service=load(root/'service.json');require(service['kind']=='real-minio-source-build' and 'MinIO' in (root/'minio.log').read_text(),'real separate MinIO absent')
    inventory=load(root/'source-inventory.json')
    with tarfile.open(root/'source.tar.gz','r:gz') as t:
        members={m.name:m for m in t.getmembers()};require(set(members)==set(inventory),'source inventory differs')
        for name,h in inventory.items():require(members[name].isfile() and hashlib.sha256(t.extractfile(members[name]).read()).hexdigest()==h,'source bytes differ')
    wire=Wire(root);ledgers={topic:{(p,off):e for p,off,e in ledger} for topic,ledger in load(root/'ledgers.json').items()}
    known={};sessions=[];decisions=[];cuts=[];models={};measured=[]
    for case in cases:
        name=case['name'];c=load(root/(name+'.config.json'));p=load(root/(name+'.policy.json'));ts=rows(root/(name+'.trace.jsonl'));frames=output(root,name)
        admits=[r for r in ts if r['type']=='admitted'];terms=[r for r in ts if r['type']=='terminal'];fails=[r for r in ts if r['type']=='failure'];successes=[r for r in ts if r['type']=='checkpoint_success'];adecs=[r for r in ts if r['type']=='adaptive_decision']
        killed=name.startswith('kill-') and not name.endswith('-restore')
        negative=name in NEGATIVE or name in {'remote-'+prefix+suffix for prefix in REMOTE_FAILURES for suffix in ('','-injected')}
        if killed:
            require(case['exit']==-9 and not terms and not fails and not successes,'crashed process claimed success')
            require(any(t['type']=='checkpoint_step' and t['point']==name[5:] for t in ts),'crash boundary not reached')
        elif negative:
            require(case['exit']==1 and fails and not any(t['status']=='STOPPED' for t in terms),'negative case false success')
            if name in ['wrong-consent','no-consent','untrusted-history','wrong-topic','wrong-origin','weaken-copies','corrupt-selector','retention-lost','retention-both-lost','missing-credentials','wrong-credentials','capability-unavailable','service-outage']:
                require(not admits and not frames,'identity/consent/source refusal was after output')
        else:require(case['exit']==0 and len(terms)==1 and terms[0]['status']=='STOPPED' and not fails,'positive case incomplete')
        for recovery in [r for r in ts if r['type']=='recovery_decision' and r.get('model')]:
            # Source/copy choice is bound by exact CLI argv, not inferred by label.
            argv=next(t for t in load(root/'commands.json') if t['label']==name)['argv'];remote_copy='--restore-copy' in argv and argv[argv.index('--restore-copy')+1]=='remote'
            selected_offsets=None
            selector_path=root/(name+'.checkpoints/CURRENT2')
            if selector_path.is_file():
                selected=load(selector_path);generation=selected['certificate']['manifest']['generation']
                manifest_path=root/(name+'.checkpoints')/generation/'manifest.json'
                if manifest_path.is_file():
                    selected_manifest=load(manifest_path)
                    selected_offsets={int(k):v['next_offset_suggestion'] for k,v in selected_manifest['progress']['partitions'].items()}
            check_recovery(p,recovery,c,remote_copy,selected_offsets)
        if not admits:continue
        require(len(admits)==1,'multiple admissions');admit=admits[0];sessions.append(admit['session'])
        if admit['progress']['forwarded_records']==0:m=Model(c)
        else:
            key=identity(c,admit['progress']);require(key in known,'restore has no independently verified prior matched state');m=known[key].clone();m.config=c
        m.check_progress(admit['progress']);states={m.total:m.clone()}
        for frame in frames:
            if frame['type']=='input':require(frame['receipt']['session']==admit['session'],'stale session frame')
            m.consume(frame,ledgers[c['source']['topic']])
            if not m.due():
                states[m.total]=m.clone()
                # Key ignores session only; all semantic output was independently confirmed.
                state_progress=next((r['progress'] for r in ts if r.get('progress',{}).get('forwarded_records')==m.total and r['type'] in ['forwarded','checkpoint_success','terminal']),None)
                if state_progress:known[identity(c,state_progress)]=m.clone()
        if terms:
            if terms[-1]['pending_input'] is False and not terms[-1]['pending_window_output']:m.check_progress(terms[-1]['progress'])
            met=terms[-1]['metrics'];sm=terms[-1]['state'];require(met['in_flight_peak_records']<=1 and met['downstream_channel_depth']==0 and met['sink_frame_peak_bytes']<=2048,'unbounded sink queue')
            require(sm['hot_peak_bytes']<=c['hot_bytes'] and sm['disk_peak_bytes']<=c['cold_bytes'] and sm['checkpoint_peak_bytes']<=c['checkpoint_bytes'],'state/checkpoint quota exceeded')
        require(len(successes)==len(adecs),'checkpoint success without exact policy/certificate evidence')
        folder=root/(name+'.checkpoints')
        for success,decision in zip(successes,adecs):
            check_decision(p,decision['decision']);decisions.append(decision['decision']['chosen']);total=success['progress']['forwarded_records'];require(total in states,'cut ahead of ACK/closures');model=states[total]
            cert=decision['certificate'];generation=cert['manifest']['generation'];require(generation==success['pointer']['generation'] and success['pointer']['version']==2,'v1/v2 pointer mixed')
            mb=(folder/generation/'manifest.json').read_bytes();manifest=json.loads(mb)
            require(len(mb)==cert['manifest']['bytes'] and cert['logical_fingerprint']==admit['logical_fingerprint'],'certificate manifest size/identity')
            local=lambda g,n:(folder/g/n).read_bytes()
            remote=lambda g,n:wire.get(cert['remote'],g,n,wire.certificate_boundary(cert))
            physical=None
            if cert['placement']!='remote':
                ancestry(c,manifest,local,known,states);physical=check_snapshot(root,manifest,model,local)
            if cert['placement']!='local':
                require(remote(generation,'manifest.json')==mb,'remote manifest differs from local receipt')
                ancestry(c,manifest,remote,known,states);remote_physical=check_snapshot(root,manifest,model,remote)
                if physical is not None:require(physical==remote_physical,'replicated logical state differs')
                physical=remote_physical
                cb=wire.get(cert['remote'],generation,'certificate.json');require(json.loads(cb)==cert,'remote certificate content differs')
            metrics=decision['metrics'];require(physical==metrics['physical_shard_bytes'],'claimed incremental bytes were not physically written')
            require(metrics['canonical_scanned_bytes']==sum(s['bytes'] for s in manifest['shards']),'canonical byte accounting')
            require(manifest['depth']<=p['max_depth'] and (manifest['depth']==0)==(manifest['prior'] is None),'ancestry/full bound')
            if manifest['depth']==0:require(all(s['object_generation']==generation for s in manifest['shards']),'full is not self-contained')
            require(all(s['object_generation']==generation or manifest['prior'] is not None for s in manifest['shards']),'unreferenced ancestor')
            cuts.append((name,total,manifest,metrics));known[identity(c,success['progress'])]=model.clone()
        # Every selected CURRENT2 after a crash must match an entire old or new cut.
        current=folder/'CURRENT2'
        if killed:
            selection=load(current);g=selection['certificate']['manifest']['generation'];manifest=load(folder/g/'manifest.json')
            want=80 if name[5:] in ['selector_renamed','selector_durable'] else 40
            require(manifest['progress']['forwarded_records']==want,'crash selected incomplete/wrong generation')
            model=states[want] if want in states else known[identity(c,manifest['progress'])]
            check_snapshot(root,manifest,model,lambda generation,file:(folder/generation/file).read_bytes())
        if name.startswith('remote-') and name.endswith('-injected'):
            require(not successes and load(current)['certificate']['manifest']['generation']==load(root/'fault-frozen-roots/check/CURRENT2')['certificate']['manifest']['generation'],'remote outage changed eligibility')
        if terms:models[name]=m.clone();measured.append((name,terms[-1]['elapsed_ms']))
    require(len(sessions)==len(set(sessions)),'reused session epoch')
    require({d['placement'] for d in decisions}=={'local','remote','both'},'placement coverage missing')
    for label in ['local-adaptive-0','remote-adaptive-0','both-adaptive-0']:
        group=[(manifest,metrics) for n,_,manifest,metrics in cuts if n==label]
        require(any(m['physical_shard_bytes']<m['canonical_scanned_bytes'] and manifest['depth']>0 for manifest,m in group),'no actual incremental savings '+label)
        require(any(manifest['depth']==p['max_depth'] for manifest,_ in group),'incremental depth bound not reached')
        depths=[m['depth'] for m,_ in group];require(any(a==2 and b==0 for a,b in zip(depths,depths[1:])),'periodic self-contained full absent')
    cadences={n:{r['decision']['chosen']['records'] for r in rows(root/(n+'.trace.jsonl')) if r['type']=='adaptive_decision'} for n in ['cadence-low-risk','cadence-high-risk']}
    require(80 in cadences['cadence-low-risk'] and cadences['cadence-high-risk']=={20},'controlled cadence choices not executed')
    for n in ['restore-local','restore-remote','clean-scratch']:
        rec=next(t for t in rows(root/(n+'.trace.jsonl')) if t['type']=='recovery_decision');require(rec['path']=='checkpoint_restore' and rec['progress']['forwarded_records']==240,'restore was only a recommendation')
    replay=next(t for t in rows(root/'full-replay.trace.jsonl') if t['type']=='recovery_decision');require(replay['path']=='full_replay_explicit_conflict_consent' and replay['progress']['forwarded_records']==0,'full replay path not executed')
    if remediated:
        restored=next(t for t in rows(root/'retention-restore-after-expiry.trace.jsonl') if t['type']=='recovery_decision')
        require(restored['path']=='checkpoint_restore' and restored['progress']['forwarded_records']==40,'retained checkpoint was not restored after replay-origin expiry')
        require(restored['model']['chosen_reason']=='replay_ineligible_origin_retention' and restored['model']['restore_eligible'] and not restored['model']['replay_eligible'],'recovery did not record eligibility-first fallback')
        require(not output(root,'retention-restore-after-expiry'),'retention fallback replayed or emitted duplicate output')
        decisions_in_transition=[r['decision'] for r in rows(root/'adaptive-placement.trace.jsonl') if r['type']=='adaptive_decision']
        require(any(d['chosen']['format']=='full' and d['chosen']['placement']=='local' and 'local' not in d['incremental_placements'] and any(c['action']['format']=='incremental' and c['action']['placement']=='local' and c['rejection']=='self_contained_required' for c in d['candidates']) for d in decisions_in_transition),'placement transition did not score an executable self-contained action')
    require(not list((root/'clean-scratch.checkpoints').rglob('shard-*')),'clean remote scratch retained local state shards')
    slow=[t for t in rows(root/'slow-control.trace.jsonl') if t['type']=='checkpoint_blocked'];require(len(slow)>=4 and all(t['progress']==slow[0]['progress'] for t in slow),'source data advanced during cut')
    require(slow[-1]['source']['checkpoint_control']['stats_callbacks']>slow[0]['source']['checkpoint_control']['stats_callbacks'],'control pump stopped during I/O')
    windows=[[r for r in output(root,n) if r['type']=='window'] for n in ['window-a','window-b']]
    require(all(len(v)==1 for v in windows) and windows[0][0]['output_id']==windows[1][0]['output_id'] and windows[0][0]['payload_fingerprint']!=windows[1][0]['payload_fingerprint'],'trace-relative payload conflict not witnessed')
    for n in BASE:require(models[n].state==models[BASE[0]].state,'matched controls differ')
    saved=sum(m['canonical_scanned_bytes']-m['physical_shard_bytes'] for n,_,_,m in cuts if n=='local-adaptive-0')
    return dict(cases=len(cases),sessions=len(sessions),source_files=len(inventory),cuts=len(cuts),local_adaptive_shard_bytes_saved=saved,measured_elapsed_ms=dict(measured),cadence_coverage={k:sorted(v) for k,v in cadences.items()})


def archive_verify(archive,manifest):
    meta=load(manifest);require(meta['version']==1,'evidence version');raw=pathlib.Path(archive).read_bytes();require(hashlib.sha256(raw).hexdigest()==meta['archive_sha256'],'archive checksum')
    with tempfile.TemporaryDirectory() as tmp:
        root=pathlib.Path(tmp)
        with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz') as t:
            members=t.getmembers();require(len(members)==len(meta['files']) and {m.name for m in members}==set(meta['files']),'member inventory/count');require(sum(m.size for m in members)<=512*1024*1024 and all(m.size<=64*1024*1024 for m in members),'archive size cap')
            for member in members:
                require(member.isfile() and member.name in meta['files'] and not pathlib.PurePosixPath(member.name).is_absolute() and '..' not in pathlib.PurePosixPath(member.name).parts,'unsafe/foreign archive member')
                b=t.extractfile(member).read();require(hashlib.sha256(b).hexdigest()==meta['files'][member.name],'member checksum');p=root/member.name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
        return verify(root)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--campaign',type=pathlib.Path);p.add_argument('--archive',type=pathlib.Path);p.add_argument('--manifest',type=pathlib.Path);a=p.parse_args()
    print(json.dumps(verify(a.campaign) if a.campaign else archive_verify(a.archive,a.manifest),sort_keys=True))
