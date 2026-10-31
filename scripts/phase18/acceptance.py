#!/usr/bin/env python3
"""Owned real Kafka/MinIO Phase18 campaigns; every process/fixture is recorded."""
import argparse,copy,hashlib,json,os,pathlib,shutil,signal,subprocess,sys,tarfile,time,uuid
HERE=pathlib.Path(__file__).resolve();REPO=HERE.parents[2]
sys.path.insert(0,str(REPO/'scripts/phase17'))
from service_fixture import Services,private,dump,rows,require
from prerequisite import native,event,policy

def wait_for(proc,path,predicate,timeout=30):
    until=time.monotonic()+timeout
    while time.monotonic()<until:
        if predicate(rows(path)):return
        require(proc.poll() is None,'process exited before barrier: '+str(path))
        time.sleep(.05)
    raise ValueError('barrier timeout: '+str(path))
def freeze(root,binaries):
    files=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],cwd=REPO,text=True).splitlines()
    files=[p for p in files if not p.startswith('docs/evidence/') and p not in ['sol-medium-phase-0-bootstrap-implementation-prompt.md','docs/reports/phase-18-implementation-report.md'] and (REPO/p).is_file()]
    inventory={p:hashlib.sha256((REPO/p).read_bytes()).hexdigest() for p in files};dump(root/'source-inventory.json',inventory)
    dump(root/'runtime-inventory.json',{p:h for p,h in inventory.items() if p.endswith('.rs') or pathlib.Path(p).name in ['Cargo.toml','Cargo.lock']})
    with tarfile.open(root/'source.tar.gz','w:gz') as tar:
        for p in files:tar.add(REPO/p,arcname=p)
    dump(root/'environment.json',dict(base=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),status=subprocess.check_output(['git','status','--short'],cwd=REPO,text=True),rustc=subprocess.check_output(['rustc','-Vv'],text=True),platform=list(os.uname()),binaries={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in binaries.items()},started=time.time()))
    shutil.copy2(HERE.parent/'criteria.json',root/'criteria.json')
def logical(c,mode='history_then_live',cut=None,state=True,committed=False):
    src=c['source'] if state else c;parts=c['topic_partitions'] if state else [s['partition'] for s in src['starts']]
    source=dict(cluster=src['expected_cluster_id'],topic=src['topic'],topic_partitions=parts,original_starts=copy.deepcopy(src['starts']),immutable_history='owned-immutable-'+src['topic'],mode=dict(kind=mode,**(dict(cut=cut) if mode=='history_then_live' else {})))
    return dict(version=1,computation=dict(kind='kafka',source=source,job=c['job_id'] if state else 'stateless-job',key_less_than=src['key_less_than'],allowance_ms=src['allowance_ms'],projection=['value_i64','key','event_time_ms'],window_ms=c['window_ms'] if state else None,late_policy=c['late_policy'] if state else None,hash_version=1,timers=copy.deepcopy(src['timers'])),sink='committed_local_v3' if committed else 'receipts')
def main(a):
    root=a.output.resolve();private(root);bins={}
    for name in ['ade-plan','ade-stream','ade-state-stream','ade-state-stream-txn','ade-query','ade-worker','ade-bench']:
        dest=root/(name+'-pinned');shutil.copy2(a.binary_dir/name,dest);bins[name]=dest
    dest=root/'phase18-oracle-pinned';shutil.copy2(a.binary_dir/'examples/phase18-oracle',dest);bins['phase18-oracle']=dest
    freeze(root,bins);cases=[];s=None
    def record(case):cases.append(case);dump(root/'cases.json',cases)
    try:
        s=Services.start(a,root)
        s.call([str(a.java_home/'bin/javac'),'-cp',s.cp,'-d',str(s.fixture),str(HERE.with_name('Cuts.java'))],'javac-cuts')
        dump(root/'service-inputs.json',dict(kafka_jars={f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted((a.kafka_home/'libs').glob('*.jar'))},minio_sha256=hashlib.sha256(a.minio.read_bytes()).hexdigest()))
        captures=[]
        def capture(topic,label):
            out=s.call([s.java,'-cp',s.fixture_cp,'Cuts',s.address,topic],label+'-cut-capture')
            offsets=[dict(partition=int(p),low=int(lo),high=int(hi)) for p,lo,hi in (line.split('\t') for line in out.decode().splitlines())]
            offsets.sort(key=lambda o:o['partition']);captures.append(dict(name=label,topic=topic,ranges=offsets));dump(root/'cut-captures.json',captures)
            return [dict(partition=o['partition'],offset=o['high']) for o in offsets]

        def cfg(topic,label,parts=2):
            c=native(s,topic,label);c['source']['starts']=[dict(partition=p,offset=0) for p in range(parts)];c['topic_partitions']=list(range(parts));return c
        def prepare(label,c,p=None,mode='stateful'):
            cp=root/(label+'.config.json');dump(cp,c);lp=None
            if p is not None:
                lp=root/(label+'.logical.json');dump(lp,p)
                probe=json.loads(s.call([str(bins['ade-plan']),str(lp)],label+'-plan'))
                if mode=='stateful':
                    c['job_id']=str(uuid.UUID(hex=probe['bound_job_hex']));dump(cp,c)
            return cp,lp
        def run(label,c,p=None,total=None,restore=False,expected=0,mode='stateful',previous=None,extra=()):
            cp,lp=prepare(label,c,p,mode);command=[str(bins['ade-stream' if mode=='stateless' else 'ade-state-stream']),str(cp)]
            if lp:command+=['--logical-plan',str(lp)]
            if restore:command+=['--restore']
            command+=['--test-stop-records',str(total)] if total else ['--test-stop-ms','1300']
            command+=list(extra);s.call(command,label,expected=expected,timeout=90)
            case=dict(name=label,mode=mode,config=cp.name,logical=lp.name if lp else None,exit=expected,previous=previous,total=total)
            if expected==0 and mode=='stateful' and pathlib.Path(c['checkpoint_root'],'CURRENT').exists():
                snap=root/(label+'.checkpoint');shutil.copytree(c['checkpoint_root'],snap);case['snapshot']=snap.name
            record(case);return case
        # Prefix and tail share open windows and one source session. Tail is released
        # only after an actual ACK barrier, ensuring the boundary is exercised live.
        for parts in [2,4]:
            topic=s.topic('hybrid'+str(parts),parts);c=cfg(topic,'hybrid'+str(parts)+'-roots',parts)
            prefix=[(i%parts,event(i%8,i-20,-10 if i%3==0 else 5)) for i in range(40)]
            s.produce(topic,prefix);cut=capture(topic,'hybrid'+str(parts));p=logical(c,cut=cut)
            cp,lp=prepare('hybrid'+str(parts),c,p)
            out=open(root/f'hybrid{parts}.stdout','wb');proc=s.spawn([str(bins['ade-state-stream']),str(cp),'--logical-plan',str(lp),'--test-stop-records',str(81+parts*2)],f'hybrid{parts}',stdout=out);out.close()
            wait_for(proc,root/f'hybrid{parts}.stderr',lambda rs:any(r.get('type')=='forwarded' and r['progress']['forwarded_records']==40 for r in rs))
            time.sleep(1.2);require(proc.poll() is None,'idle prefix became EOF')
            s.produce(topic,[(i%parts,event(i%8,3,5)) for i in range(40)]+[(part,event(1001,0,t)) for t in [200,500] for part in range(parts)])
            wait_for(proc,root/f'hybrid{parts}.stderr',lambda rs:any(r.get('type')=='forwarded' and r['progress']['forwarded_records']==80+parts*2 for r in rs))
            s.produce(topic,[(0,event(1,999,5))])
            require(proc.wait(90)==0,'hybrid run failed');shutil.copytree(c['checkpoint_root'],root/f'hybrid{parts}.checkpoint')
            record(dict(name=f'hybrid{parts}',mode='stateful',config=cp.name,logical=lp.name,exit=0,total=81+parts*2,snapshot=f'hybrid{parts}.checkpoint',asynchronous_tail=True))
        # Empty history, actual aborted Kafka transaction holes, stateless parity.
        for label,gaps,empty in [('empty-prefix',False,True),('legal-gaps',True,False)]:
            topic=s.topic(label);c=cfg(topic,label+'-roots')
            if gaps:
                raw=''.join(str(i)+'\t'+json.dumps(event(9,999,0))+'\n' for i in range(2)).encode();s.admin('abort',topic,data=raw)
            produced=s.produce(topic,[(i%2,event(i%8,1,0)) for i in range(40)])
            cut=[dict(partition=part,offset=0 if empty else max(r['offset'] for r in produced if r['partition']==part)+1) for part in range(2)]
            p=logical(c,cut=cut);run(label,c,p,40)
            plain=copy.deepcopy(c['source']);p=logical(plain,mode='assigned',state=False);plain['timers']=[dict(kind='event',deadline_ms=-10)];p['computation']['timers']=copy.deepcopy(plain['timers'])
            run(label+'-stateless',plain,p,40,mode='stateless');run(label+'-legacy-stateless',plain,None,40,mode='stateless')
        topic=s.topic('live-state');c=cfg(topic,'live-state-roots');s.produce(topic,[(i%2,event(i%8,1,0)) for i in range(40)]+[(part,event(1001,0,200)) for part in range(2)])
        p=logical(c,mode='assigned');run('live-state',c,p,42)
        old=cfg(topic,'live-state-legacy-roots');old['job_id']=c['job_id'];run('live-state-legacy',old,None,42)
        # Existing v2 native snapshot/recovery is composed, never replaced.
        for place in ['local','both']:
            topic=s.topic('v2-'+place);c=cfg(topic,'v2-'+place+'-roots');s.produce(topic,[(i%2,event(i%8,1,0)) for i in range(40)])
            p=logical(c,cut=[dict(partition=part,offset=20) for part in range(2)]);cp,lp=prepare('v2-'+place,c,p)
            pol=policy(s,c,place);pol['mode']='static';pol['recovery']['allow_replay']=False;polpath=root/('v2-'+place+'.policy.json');dump(polpath,pol)
            s.call([str(bins['ade-state-stream']),str(cp),'--logical-plan',str(lp),'--adaptive-policy',str(polpath),'--test-stop-records','40'],'v2-'+place,env=s.env,timeout=90)
            shutil.copytree(c['checkpoint_root'],root/('v2-'+place+'.snapshot'));record(dict(name='v2-'+place,mode='v2',config=cp.name,logical=lp.name,policy=polpath.name,exit=0,snapshot='v2-'+place+'.snapshot',total=40))
            s.produce(topic,[(i%2,event(i%8,2,0)) for i in range(40)])
            label='v2-'+place+'-recover';s.call([str(bins['ade-state-stream']),str(cp),'--logical-plan',str(lp),'--adaptive-policy',str(polpath),'--recover','--test-stop-records','80'],label,env=s.env,timeout=90)
            shutil.copytree(c['checkpoint_root'],root/(label+'.snapshot'));record(dict(name=label,mode='v2',config=cp.name,logical=lp.name,policy=polpath.name,exit=0,snapshot=label+'.snapshot',total=80,previous='v2-'+place))
        topic=s.topic('assigned-subset');c=cfg(topic,'assigned-subset-roots');c['source']['starts']=[dict(partition=1,offset=0)];s.produce(topic,[(1,event(i%8,1,0)) for i in range(40)]+[(1,event(1001,0,200))]);p=logical(c,mode='assigned');run('assigned-subset',c,p,41)
        # Cold process restore at an exact selected cut; semantic tampering refuses.
        topic=s.topic('restart');c=cfg(topic,'restart-roots');s.produce(topic,[(i%2,event(i%20,1,0)) for i in range(40)])
        p=logical(c,cut=[dict(partition=part,offset=20) for part in range(2)])
        run('restart-prefix',c,p,40)
        s.produce(topic,[(i%2,event(i%20,2,0)) for i in range(40)])
        run('restart-tail',c,p,80,True,previous='restart-prefix')
        for label,change in [('changed-cut',lambda q:q['computation']['source']['mode']['cut'][0].update(offset=21)),('changed-history',lambda q:q['computation']['source'].update(immutable_history='different-history'))]:
            q=copy.deepcopy(p);change(q);cp=root/(label+'.config.json');lp=root/(label+'.logical.json');dump(cp,c);dump(lp,q)
            before={str(f.relative_to(c['checkpoint_root'])):hashlib.sha256(f.read_bytes()).hexdigest() for f in pathlib.Path(c['checkpoint_root']).rglob('*') if f.is_file()}
            s.call([str(bins['ade-state-stream']),str(cp),'--logical-plan',str(lp),'--restore','--test-stop-records','80'],label,expected=1)
            after={str(f.relative_to(c['checkpoint_root'])):hashlib.sha256(f.read_bytes()).hexdigest() for f in pathlib.Path(c['checkpoint_root']).rglob('*') if f.is_file()};require(before==after,'semantic refusal mutated checkpoint root');dump(root/(label+'.root-refusal.json'),dict(before=before,after=after))
            record(dict(name=label,mode='refusal',config=cp.name,logical=lp.name,exit=1,no_root_mutation=True))
        q=copy.deepcopy(p);q['computation']['source']['mode']['cut'][0]['offset']+=1;changed=copy.deepcopy(c);cp,lp=prepare('changed-cut-rebound',changed,q)
        s.call([str(bins['ade-state-stream']),str(cp),'--logical-plan',str(lp),'--restore','--test-stop-records','80'],'changed-cut-rebound',expected=1)
        record(dict(name='changed-cut-rebound',mode='refusal',config=cp.name,logical=lp.name,exit=1))
        # A kill during next checkpoint must preserve the previous selected cut.
        s.produce(topic,[(i%2,event(i%20,3,0)) for i in range(40)])
        cp,lp=prepare('kill-at-checkpoint',c,p);before=pathlib.Path(c['checkpoint_root'],'CURRENT').read_bytes()
        out=open(root/'kill-at-checkpoint.stdout','wb');proc=s.spawn([str(bins['ade-state-stream']),str(cp),'--logical-plan',str(lp),'--restore','--test-stop-records','120','--test-pause-at','pointer_synced'],'kill-at-checkpoint',stdout=out);out.close()
        wait_for(proc,root/'kill-at-checkpoint.stderr',lambda rs:any(r.get('point')=='pointer_synced' for r in rs));proc.kill();require(proc.wait(10)==-9,'no actual SIGKILL');require(pathlib.Path(c['checkpoint_root'],'CURRENT').read_bytes()==before,'kill selected incomplete state')
        record(dict(name='kill-at-checkpoint',mode='kill',config=cp.name,logical=lp.name,exit=-9,prior_selector_sha256=hashlib.sha256(before).hexdigest()))
        # Owned process has been killed/waited. Explicit cleanup of lossy working
        # and stale v1 lock only; every checkpoint orphan remains charged.
        for child in pathlib.Path(c['working_root']).iterdir():shutil.rmtree(child)
        pathlib.Path(c['checkpoint_root'],'LOCK').unlink()
        run('restart-after-kill',c,p,120,True,previous='restart-tail')
        s.admin('retain',topic,[0,1]);run('expired-origin',c,p,120,True,expected=1)
        # Real queue/sink stress and cancellation. No source polls while blocked.
        topic=s.topic('stall');c=cfg(topic,'stall-roots');c['checkpoint_every_records']=1000;c['source']['sink_stall_ms']=60000
        s.produce(topic,[(i%2,event(i%8,1,0)) for i in range(1200)]);p=logical(c,cut=[dict(partition=part,offset=300) for part in range(2)])
        cp,lp=prepare('sink-stall',c,p);proc=s.spawn([str(bins['ade-state-stream']),str(cp),'--logical-plan',str(lp),'--test-stop-records','1200'],'sink-stall',stdout=subprocess.PIPE)
        wait_for(proc,root/'sink-stall.stderr',lambda rs:any(r.get('type')=='sink_blocked' for r in rs))
        time.sleep(a.stall_seconds);out,_=proc.communicate(timeout=90);(root/'sink-stall.stdout').write_bytes(out);require(proc.returncode==0,'stall did not resume');record(dict(name='sink-stall',mode='stateful',config=cp.name,logical=lp.name,exit=0,total=1200,stall_seconds=a.stall_seconds))
        # Idle cancellation remains a failed attempt, never an artificial frontier.
        topic=s.topic('idle');c=cfg(topic,'idle-roots');p=logical(c,cut=[dict(partition=part,offset=0) for part in range(2)]);run('idle-pending',c,p)
        cp,lp=prepare('idle-cancel',c,p);out=open(root/'idle-cancel.stdout','wb');proc=s.spawn([str(bins['ade-state-stream']),str(cp),'--logical-plan',str(lp)],'idle-cancel',stdout=out);out.close();wait_for(proc,root/'idle-cancel.stderr',lambda rs:any(r.get('type')=='admitted' for r in rs));proc.send_signal(signal.SIGINT);require(proc.wait(10)==1,'cancel was not failure');record(dict(name='idle-cancel',mode='refusal',config=cp.name,logical=lp.name,exit=1,cancel=True))
        # Shared descriptor reaches unchanged v3 native transactions/reader.
        topic=s.topic('v3');c=cfg(topic,'v3-roots');s.produce(topic,[(i%2,event(i%8,1,0)) for i in range(40)]+[(part,event(1001,0,200)) for part in range(2)])
        p=logical(c,mode='assigned',committed=True);base=json.loads((REPO/'benchmarks/examples/phase17-config3.json').read_text());base.update(state=c,trusted_history=p['computation']['source']['immutable_history']);cp=root/'v3.config.json';lp=root/'v3.logical.json';dump(cp,base);dump(lp,p)
        s.call([str(bins['ade-state-stream-txn']),str(cp),'run','--logical-plan',str(lp),'--pause-after-records','42'],'v3',timeout=90);s.call([str(bins['ade-state-stream-txn']),str(cp),'read','--logical-plan',str(lp)],'v3-reader')
        shutil.copytree(c['checkpoint_root'],root/'v3.snapshot');record(dict(name='v3',mode='v3',config=cp.name,logical=lp.name,exit=0,snapshot='v3.snapshot',reader='v3-reader.stdout'))
        q=copy.deepcopy(p);q['computation']['source']['mode']=dict(kind='history_then_live',cut=[dict(partition=part,offset=0) for part in range(2)]);dump(root/'v3-hybrid.logical.json',q)
        s.call([str(bins['ade-state-stream-txn']),str(cp),'run','--logical-plan',str(root/'v3-hybrid.logical.json')],'v3-hybrid-refused',expected=1);record(dict(name='v3-hybrid-refused',mode='refusal',config=cp.name,logical='v3-hybrid.logical.json',exit=1))
        q=copy.deepcopy(p);q['sink']='receipts';dump(root/'v3-downgrade.logical.json',q)
        s.call([str(bins['ade-state-stream-txn']),str(cp),'run','--logical-plan',str(root/'v3-downgrade.logical.json')],'v3-downgrade-refused',expected=1);record(dict(name='v3-downgrade-refused',mode='refusal',config=cp.name,logical='v3-downgrade.logical.json',exit=1))
        # Malformed shared descriptors refuse before any native roots/network.
        for label,mutate in [
            ('forged-properties',lambda q:q.update(properties={'termination':'finite'})),
            ('unknown-schema',lambda q:q['computation'].update(schema=[])),
            ('unsafe-source-token',lambda q:q['computation']['source'].update(immutable_history='secret@untrusted/path')),
            ('missing-cut-partition',lambda q:q['computation']['source'].update(mode=dict(kind='history_then_live',cut=[]))),
            ('unbounded-range',lambda q:q['computation']['source'].update(mode=dict(kind='range',end=[]))),
            ('wrong-delivery',lambda q:q.update(sink='collect')),
            ('forged-operator',lambda q:q['computation'].update(sort=['key'])),
        ]:
            q=copy.deepcopy(p);mutate(q);lp=root/(label+'.logical.json');dump(lp,q)
            s.call([str(bins['ade-state-stream-txn']),str(cp),'run','--logical-plan',str(lp)],label,expected=1)
            record(dict(name=label,mode='refusal',config=cp.name,logical=lp.name,exit=1))
        for label,data in [('duplicate-json',json.dumps(p).replace('"version": 1','"version": 1,"version": 1',1).encode()),('oversized-json',b' '*262145)]:
            lp=root/(label+'.logical.json');lp.write_bytes(data);s.call([str(bins['ade-state-stream-txn']),str(cp),'run','--logical-plan',str(lp)],label,expected=1);record(dict(name=label,mode='refusal',config=cp.name,logical=lp.name,exit=1))
        # Batch matched static/optimized descriptive distributions, no speed threshold.
        measurements=[]
        for n in [128,12000]:
            dataset=root/f'batch-{n}';s.call([str(bins['ade-bench']),'data','generate','synthetic','--dataset-id','phase18','--output',str(dataset),'--rows',str(n),'--files','2','--key-cardinality','17'],f'generate-{n}')
            s.call([str(bins['phase18-oracle']),str(dataset),'SELECT key,SUM(value_i64) AS sum,COUNT(*) AS count FROM input GROUP BY key'],f'batch-{n}-oracle')
            for sample in range(3):
                for optimized in [False,True]:
                    label=f'batch-{n}-{sample}-{int(optimized)}';cmd=[str(bins['ade-query']),'execute','--dataset',str(dataset),'--sql','SELECT key,SUM(value_i64) AS sum,COUNT(*) AS count FROM input GROUP BY key','--unified-planner','--worker-bin',str(bins['ade-worker'])]
                    if not optimized:cmd+=['--disable-optimizer']
                    start=time.monotonic_ns();s.call(cmd,label);measurements.append(dict(name=label,rows=n,sample=sample,optimized=optimized,elapsed_ns=time.monotonic_ns()-start))
        dump(root/'measurements.json',measurements);dump(root/'outcome.json',dict(status='CAMPAIGN_FINISHED',cases=len(cases),finished=time.time()))
    finally:
        if s:s.close()
        dump(root/'cases.json',cases)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True,type=pathlib.Path);p.add_argument('--binary-dir',type=pathlib.Path,default=REPO/'target/debug');p.add_argument('--kafka-home',type=pathlib.Path,required=True);p.add_argument('--java-home',type=pathlib.Path,required=True);p.add_argument('--minio',type=pathlib.Path,required=True);p.add_argument('--service-python',type=pathlib.Path,required=True);p.add_argument('--stall-seconds',type=int,default=30);main(p.parse_args())
