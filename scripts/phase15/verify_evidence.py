#!/usr/bin/env python3
"""Portable SHA256 evidence validation + independent ordered window/prefix replay.
Uses only Python standard library; no broker, ADE executable or private files.
Inner Rust BLAKE3 integrity is exercised by process refusals and fault tests; this
verifier independently binds those exact bytes with SHA256 and checks semantics.
"""
import argparse, hashlib, io, json, pathlib, tarfile, tempfile
from oracle import Model, require, rows

POINTS=['stage_created','shard_write','shard_checksum','shard_synced','manifest_write','manifest_checksum','manifest_synced','staging_verified','stage_synced','generation_renamed','generation_synced','pointer_write','pointer_synced','pointer_renamed','pointer_durable','cleanup']
NEGATIVE={
'wrong-job':'checkpoint identity','wrong-window':'checkpoint identity','wrong-hash':'stateful version',
'wrong-cluster':'checkpoint identity','wrong-topic':'checkpoint identity','wrong-filter':'checkpoint identity','wrong-partitions':'checkpoint identity',
 'truncated-shard':'shard length','corrupt-shard':'record checksum','corrupt-manifest':'manifest checksum',
 'pointer-traversal':'strict checkpoint','symlink-shard':'open regular','oversized-pointer':'regular private file',
 'slow-checkpoint-control':'cancelled','cancel-blocked':'cancelled','cancel-checkpoint-write':'cancelled','cancel-checkpoint-promote':'cancelled','orphan-working':'working root not empty','broker-outage':('broker failure','unexpected main-queue data/error'),'retention-lost':'offset range/retention'}
MAIN=['owner1','owner2','owner4','owner1-return','quiet-frontier','close','late-open','final-close']
EXPECTED_BASE=set(MAIN+['ample-control','window-kill','window-replay','backpressure','outage-seed','broker-restart'])|set(NEGATIVE)|{'kill-'+p+s for p in POINTS for s in ('','-restore')}
CROSS_ORDER={'cross-order-a-seed','cross-order-a','cross-order-b-seed','cross-order-b'}

def config(root,label):return json.loads((root/(label+'.config.json')).read_text())
def traces(root,label):return rows(root/(label+'.trace.jsonl'))
def output(root,label,partial=False):
    data=(root/(label+'.output.jsonl')).read_bytes()
    if partial and data and not data.endswith(b'\n'):data=data.rsplit(b'\n',1)[0]+b'\n' if b'\n' in data else b''
    return [json.loads(line) for line in data.splitlines() if line.strip()]
def current(root,label):
    folder=root/(label+'.checkpoints');p=json.loads((folder/'CURRENT').read_text());return folder/p['generation']

def check_metrics(c,t):
    m=t['metrics'];require(m['sink_frame_peak_bytes']<=2048 and m['in_flight_peak_records']<=1 and m['downstream_channel_depth']==0,'frame/queue bounds')
    if 'state' in t:
        s=t['state'];require(s['hot_peak_bytes']<=c['hot_bytes'] and s['disk_peak_bytes']<=c['cold_bytes'],'hot/cold quota')
        require(s['io_buffer_bytes']==4096 and s['checkpoint_peak_bytes']<=c['checkpoint_bytes'] and s['checkpoint_metadata_peak_bytes']<=131072,'I/O/checkpoint charge')

def replay(root,label,model,ledger,check_terminal=True):
    states={model.total:model.clone()}
    admitted=[t for t in traces(root,label) if t['type']=='admitted'];require(len(admitted)==1,label+' missing admission')
    model.check_progress(admitted[0]['progress'])
    for frame in output(root,label):
        if frame['type']=='input':require(frame['receipt']['session']==admitted[0]['session'],'stale session input')
        model.consume(frame,ledger)
        if not model.due():states[model.total]=model.clone()
    for t in traces(root,label):
        if t['type']=='checkpoint_success':
            total=t['progress']['forwarded_records'];require(total in states,label+' unmatched checkpoint cut');states[total].check_progress(t['progress'])
            directory=root/(label+'.checkpoints')/t['pointer']['generation'];states[total].check_checkpoint(directory)
            check_metrics(config(root,label),dict(metrics={'sink_frame_peak_bytes':0,'in_flight_peak_records':0,'downstream_channel_depth':0},state=t['state']))
    if check_terminal:
        terminal=next(t for t in reversed(traces(root,label)) if t['type']=='terminal');model.check_progress(terminal['progress']);check_metrics(config(root,label),terminal)
    return model

def verify(root):
    cases=json.loads((root/'cases.json').read_text());names=[c['name'] for c in cases]
    env_path=root/'environment.json'
    env=json.loads(env_path.read_text()) if env_path.exists() else {}
    expected=EXPECTED_BASE|CROSS_ORDER if env.get('base')=='7f838db7bfb0275f6f8e7c65aab8da90e4d6bd79' else EXPECTED_BASE
    require(len(names)==len(set(names)) and set(names)==expected,'missing/extra mandatory process cases')
    ledgers={topic:{(p,off):event for p,off,event in ledger} for topic,ledger in json.loads((root/'ledgers.json').read_text()).items()}
    sessions=[]
    for case in cases:
        name=case['name'];ts=traces(root,name);c=config(root,name)
        admitted=[t for t in ts if t['type']=='admitted'];sessions.extend(t['session'] for t in admitted)
        terms=[t for t in ts if t['type']=='terminal'];failures=[t for t in ts if t['type']=='failure']
        if name in NEGATIVE:
            require(case['exit']==1 and failures and any(part in failures[-1]['reason'] for part in (NEGATIVE[name] if isinstance(NEGATIVE[name],tuple) else [NEGATIVE[name]])),name+' wrong failure contract')
            allowed_classes={'slow-checkpoint-control':('cancelled',),'cancel-blocked':('cancelled',),'cancel-checkpoint-write':('cancelled',),'cancel-checkpoint-promote':('cancelled',),'broker-outage':('broker','source_identity_or_offset'),'retention-lost':('source_identity_or_offset',)}.get(name,('state_or_checkpoint',))
            require(failures[-1]['failure_class'] in allowed_classes,name+' wrong reached failure class')
            if name not in ('cancel-blocked','broker-outage','cancel-checkpoint-write','cancel-checkpoint-promote','slow-checkpoint-control'):
                require(not admitted and not terms and not (root/(name+'.output.jsonl')).read_bytes(),name+' output before full validation')
            else:require(terms and terms[-1]['status']=='FAILED',name+' failure became success')
        elif name=='window-kill' or name.startswith('kill-') and not name.endswith('-restore'):
            point='stage_created' if name=='window-kill' else name[5:]
            require(case['exit']==-9 and not terms and not failures and any(t['type']=='checkpoint_step' and t['point']==point for t in ts),name+' did not reach kill boundary')
            require(not any(t['type']=='checkpoint_success' for t in ts),name+' premature checkpoint success')
        else:require(case['exit']==0 and len(terms)==1 and terms[0]['status']=='STOPPED' and not failures,name+' did not complete')
        for t in ts:
            if t['type'] in ('terminal','sink_blocked'):check_metrics(c,t)
    require(len(sessions)==len(set(sessions)),'restore reused a session epoch')
    c=config(root,'owner1');ledger=ledgers[c['source']['topic']];m=Model(c);main_states={}
    for label in MAIN:
        m=replay(root,label,m,ledger);main_states[label]=m.clone()
        if label in ('owner1','owner2','owner4','owner1-return','quiet-frontier'):m.check_checkpoint(current(root,label));require(m.frontier is None,'quiet frontier advanced')
    require(not m.state and m.closed_late==1,'final closed/late semantics')
    require([config(root,label)['owners'] for label in MAIN[:4]]==[1,2,4,1],'logical rescale sequence')
    for label in MAIN[:4]:
        s=next(t for t in reversed(traces(root,label)) if t['type']=='terminal')['state'];require(s['evictions']>0,'rescale lacks actual cold state')
    control=Model(config(root,'ample-control'));replay(root,'ample-control',control,ledger)
    require(sorted(m.outputs,key=lambda r:r['output_id'])==sorted(control.outputs,key=lambda r:r['output_id']),'ample uninterrupted/restore output mismatch')
    for suffix,point in [('write','shard_write'),('promote','pointer_synced')]:
        label='cancel-checkpoint-'+suffix;before=main_states['owner1'].clone();failed=before.clone();replay(root,label,failed,ledger,False)
        require(any(t['type']=='checkpoint_step' and t['point']==point for t in traces(root,label)),label+' missing cancellation point')
        require(not any(t['type']=='checkpoint_success' for t in traces(root,label)),label+' false success');before.check_checkpoint(current(root,label))
    slow=[t for t in traces(root,'slow-checkpoint-control') if t['type']=='checkpoint_blocked']
    require(len(slow)>=4 and all(t['progress']==slow[0]['progress'] for t in slow),'slow snapshot progressed input')
    counters=[t['source']['checkpoint_control'] for t in slow]
    require(counters[-1]['stats_callbacks']>=6 and counters[-1]['stats_callbacks']>counters[0]['stats_callbacks'] and counters[-1]['poll_ticks']>counters[0]['poll_ticks']+100,'native control queue was not pumped during snapshot')
    require(all(t['replyq_peak']<=16 and t['requested_stack_bytes']==1048576 for t in counters),'control/thread envelope')
    before=main_states['owner1'].clone();failed=before.clone();replay(root,'slow-checkpoint-control',failed,ledger,False);before.check_checkpoint(current(root,'slow-checkpoint-control'))
    require(not any(t['type']=='checkpoint_success' for t in traces(root,'slow-checkpoint-control')),'slow cancelled snapshot succeeded')
    # Visible offsets have real holes, validated by the producer ledger and oracle.
    require(any(any(b>a+1 for a,b in zip(offsets,offsets[1:])) for offsets in [sorted(off for p,off in ledger if p==part) for part in (0,1)]),'legal Kafka holes missing')
    for point in POINTS:
        label='kill-'+point;before=main_states['owner1'].clone();after=before.clone();replay(root,label,after,ledger,False)
        require(after.total==120,'kill cut is not nontrivial progress')
        manifest=json.loads((current(root,label)/'manifest.json').read_text());selected=manifest['progress']['forwarded_records']
        want=120 if point in ('pointer_renamed','pointer_durable','cleanup') else 80
        require(selected==want,'partial/new generation selected early')
        chosen=after if selected==120 else before;chosen.check_checkpoint(current(root,label))
        recovered=replay(root,label+'-restore',chosen.clone(),ledger);require(recovered.total==120,'restore skipped/doubled input');recovered.check_checkpoint(current(root,label+'-restore'))
        # The prior immutable generation is still present and independently valid.
        seed_current=json.loads((current(root,'owner1')/'manifest.json').read_text())['generation']
        before.check_checkpoint(root/(label+'.checkpoints')/seed_current)
    killed=main_states['quiet-frontier'].clone();replay(root,'window-kill',killed,ledger,False)
    resumed=main_states['quiet-frontier'].clone();replay(root,'window-replay',resumed,ledger)
    ids={r['output_id'] for r in output(root,'window-kill') if r['type']=='window'}
    require(ids and ids=={r['output_id'] for r in output(root,'window-replay') if r['type']=='window'},'stable at-least-once duplicates absent')
    pc=config(root,'backpressure');pm=Model(pc);replay(root,'backpressure',pm,ledgers[pc['source']['topic']]);require(pm.total==1600,'backpressure lost rows')
    blocks=[t for t in traces(root,'backpressure') if t['type']=='sink_blocked'];require(len(blocks)>=30 and blocks[-1]['elapsed_ms']-blocks[0]['elapsed_ms']>=30000,'missing 30s stall')
    require(all(t['progress']==blocks[0]['progress'] for t in blocks),'progress advanced during stalled frame')
    require(len({t['source']['queue_bytes_peak'] for t in blocks[-10:]})==1,'native queue did not plateau')
    require(all(t['source']['queue_bytes_peak']<=4*(16384+65536) for t in blocks),'native prefetch exceeded admitted envelope')
    require(all(t['source']['backlog'][str(p)]>500 for t in blocks[-5:] for p in (0,1)),'broker backlog witness missing')
    # Cancellation has checkpoint pressure; every successful cut must match a fully emitted input prefix.
    cc=config(root,'cancel-blocked');cancel=Model(cc);states={0:cancel.clone()}
    for frame in output(root,'cancel-blocked',True):cancel.consume(frame,ledgers[cc['source']['topic']]);states[cancel.total]=cancel.clone()
    successes=[t for t in traces(root,'cancel-blocked') if t['type']=='checkpoint_success'];require(successes,'checkpoint pressure absent')
    for t in successes:
        saved=states[t['progress']['forwarded_records']];saved.check_checkpoint(root/'cancel-blocked.checkpoints'/t['pointer']['generation'])
    require(any(t['type']=='sink_blocked' for t in traces(root,'cancel-blocked')),'cancellation did not block')
    oc=config(root,'outage-seed');om=Model(oc);ol=ledgers[oc['source']['topic']];replay(root,'outage-seed',om,ol);replay(root,'broker-restart',om,ol);om.check_checkpoint(current(root,'broker-restart'))
    require(om.total==4,'broker restore did not preserve inputs')
    if env['base']=='7f838db7bfb0275f6f8e7c65aab8da90e4d6bd79':
        cross=[]
        for label in ('cross-order-a','cross-order-b'):
            seed=label+'-seed';cc=config(root,seed);mm=Model(cc);ll=ledgers[cc['source']['topic']]
            replay(root,seed,mm,ll);replay(root,label,mm,ll)
            windows=[r for r in mm.outputs if r['type']=='window']
            require(len(windows)==1,label+' expected one window result')
            cross.append(windows[0])
        require(cross[0]['output_id']==cross[1]['output_id'],'legal orders changed logical output ID')
        require((cross[0]['sum_i64'],cross[0]['count_u64'])==(12,3),'legal order A result mismatch')
        require((cross[1]['sum_i64'],cross[1]['count_u64'])==(5,2),'legal order B result mismatch')
        require(cross[0]['payload_fingerprint']!=cross[1]['payload_fingerprint'],'payload conflict is not detectable')
    require(env['base'] in ('dc767c7c4e9e08c7b79ae034411fd9218f30a8e6','7f838db7bfb0275f6f8e7c65aab8da90e4d6bd79') and env['kafka']=='3.9.1','source/environment mismatch')
    return dict(cases=len(cases),sessions=len(sessions),windows=len(m.outputs),stall_ms=blocks[-1]['elapsed_ms']-blocks[0]['elapsed_ms'],queue_plateau=blocks[-1]['source']['queue_bytes_peak'],source_files=len(json.loads((root/'source-inventory.json').read_text())))


def safe_extract(data,directory,max_bytes):
    total=0;seen=set()
    with tarfile.open(fileobj=io.BytesIO(data),mode='r:gz') as archive:
        for member in archive:
            path=pathlib.PurePosixPath(member.name)
            require(member.isfile() and not path.is_absolute() and '..' not in path.parts and member.name not in seen,'unsafe/duplicate archive member')
            seen.add(member.name);total+=member.size;require(total<=max_bytes and member.size<=64*1024*1024 and len(seen)<=12000,'archive size/count cap')
            target=directory/path;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(archive.extractfile(member).read())

def main():
    p=argparse.ArgumentParser();p.add_argument('--campaign',type=pathlib.Path);p.add_argument('--archive',type=pathlib.Path);p.add_argument('--manifest',type=pathlib.Path);a=p.parse_args()
    if a.campaign:
        result=verify(a.campaign.resolve())
    else:
        require(a.archive is not None and a.manifest is not None,'archive and manifest required')
        manifest=json.loads(a.manifest.read_text());data=a.archive.read_bytes();require(manifest['version']==1 and hashlib.sha256(data).hexdigest()==manifest['archive_sha256'],'archive identity/version')
        with tempfile.TemporaryDirectory(prefix='ade15-verify-') as temp:
            root=pathlib.Path(temp);safe_extract(data,root,128*1024*1024)
            actual={str(f.relative_to(root)):hashlib.sha256(f.read_bytes()).hexdigest() for f in root.rglob('*') if f.is_file()}
            require(actual==manifest['files'],'file identity/inventory mismatch')
            source=root/'extracted-source';source.mkdir();safe_extract((root/'source.tar.gz').read_bytes(),source,64*1024*1024)
            source_actual={str(f.relative_to(source)):hashlib.sha256(f.read_bytes()).hexdigest() for f in source.rglob('*') if f.is_file()}
            require(source_actual==json.loads((root/'source-inventory.json').read_text()),'source snapshot mismatch')
            result=verify(root)
    print(json.dumps(dict(status='PASS',**result),sort_keys=True))

if __name__=='__main__':main()
