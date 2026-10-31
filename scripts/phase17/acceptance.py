#!/usr/bin/env python3
"""Source-bound Phase17 real Kafka/MinIO acceptance. Owns all disposable resources."""
import argparse,copy,hashlib,json,os,pathlib,shutil,signal,subprocess,tarfile,time,uuid
from service_fixture import Services,dump,private,require,rows
from prerequisite import native,event,run as prerequisite
from oracle import verify_trace,chain

P=argparse.ArgumentParser();P.add_argument('--output',required=True,type=pathlib.Path);P.add_argument('--kafka-home',required=True,type=pathlib.Path);P.add_argument('--java-home',required=True,type=pathlib.Path);P.add_argument('--minio',required=True,type=pathlib.Path);P.add_argument('--service-python',required=True,type=pathlib.Path);P.add_argument('--binary-dir',type=pathlib.Path,default=pathlib.Path('target/debug'))

def campaign(a):
    root=a.output.resolve();private(root);repo=pathlib.Path(__file__).resolve().parents[2];s=None;cases=[];observations=[]
    files=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],text=True).splitlines()
    files=[p for p in files if not p.startswith('docs/evidence/') and p not in ['sol-medium-phase-0-bootstrap-implementation-prompt.md','docs/reports/phase-17-implementation-report.md'] and pathlib.Path(p).is_file()]
    inventory={p:hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for p in files};dump(root/'source-inventory.json',inventory)
    with tarfile.open(root/'source.tar.gz','w:gz') as tar:
        for p in files:tar.add(p,arcname=p)
    pinned={}
    for name,relative in [('txn','ade-state-stream-txn'),('legacy','ade-state-stream'),('race','examples/phase16-preflight'),('placement','examples/phase16-placement')]:
        target=root/(name+'-pinned');shutil.copy2(a.binary_dir/relative,target);pinned[name]=target
    dump(root/'environment.json',dict(base=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),status=subprocess.check_output(['git','status','--short'],text=True),platform=list(os.uname()),rustc=subprocess.check_output(['rustc','-Vv'],text=True),binaries={k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in pinned.items()},started=time.time()))
    shutil.copy2(repo/'scripts/phase17/criteria.json',root/'criteria.json')
    try:
        s=Services.start(a,root);prerequisite(s,pinned['legacy'],pinned['race'],pinned['placement'])
        def config(topic,label,job=None):
            c=dict(version=3,state=native(s,topic,label),codec=1,trusted_history='owned-immutable-'+topic,limits=dict(root_bytes=32*1048576,stage_bytes=1048576,output_bytes=262144,history_bytes=1048576,epochs=64,diagnostic_bytes=16*1048576,savepoint_bytes=64*1048576,savepoints=8))
            if job:c['state']['job_id']=job
            return c
        def cfgfile(label,c):p=root/(label+'.config3.json');dump(p,c);return p
        def execute(label,c,command,extra=(),expected=0,observe=True):
            start=time.monotonic_ns();out=s.call([str(pinned['txn']),str(cfgfile(label,c)),command,*map(str,extra)],label,expected=expected,timeout=90)
            case=dict(name=label,config=label+'.config3.json',command=command,extra=list(map(str,extra)),exit=expected,elapsed_ns=time.monotonic_ns()-start);cases.append(case);dump(root/'cases.json',cases)
            if command!='read':require(not out,'writer exposed business output on stdout')
            if observe and expected==0 and pathlib.Path(c['state']['checkpoint_root'],'CURRENT3').exists():snapshot(label,c)
            return rows(root/(label+'.stderr')),out
        def attempts():
            result={}
            for p in root.glob('*.stderr'):
                records=rows(p);admitted=[r for r in records if r.get('type')=='admitted']
                if admitted:result[admitted[0]['session']]=records
            return result
        def snapshot(label,c):
            out=s.call([str(pinned['txn']),str(cfgfile(label+'-reader',c)),'read'],label+'-reader')
            output=[json.loads(line) for line in out.splitlines()];dest=root/(label+'.snapshot');shutil.copytree(c['state']['checkpoint_root'],dest)
            proof=verify_trace(dest,c,s.ledgers,attempts(),output);observations.append(dict(name=label,snapshot=dest.name,config=label+'-reader.config3.json',reader=label+'-reader.stdout',proof=proof));dump(root/'observations.json',observations);return output
        def launch(label,c,command,extra=()):
            return s.spawn([str(pinned['txn']),str(cfgfile(label,c)),command,*map(str,extra)],label)
        def wait(label,p,kind='txn_step',point=None,total=None):
            until=time.monotonic()+40
            while time.monotonic()<until:
                trace=rows(root/(label+'.stderr'))
                found=[r for r in trace if r.get('type')==kind and (point is None or r.get('point')==point) and (total is None or r.get('sequence',0)>=total)]
                if found:return found[-1]
                require(p.poll() is None,f'{label} exited early: {trace[-2:]}');time.sleep(.02)
            raise ValueError(f'{label} barrier timeout')
        def finish(label,c,p,expected=0):
            rc=p.wait(30);require(rc==expected,f'{label} exit {rc}: {rows(root/(label+".stderr"))[-2:]}');require(not (root/(label+'.stdout')).read_bytes(),'writer stdout leak')
            cases.append(dict(name=label,config=label+'.config3.json',command='process',exit=rc));dump(root/'cases.json',cases)
            if expected==0:require(any(r.get('status')=='PAUSED' for r in rows(root/(label+'.stderr'))),'missing durable pause');snapshot(label,c)
        def clean_work(c,label):
            work=pathlib.Path(c['state']['working_root']);removed=[]
            for p in work.iterdir():removed.append(p.name);shutil.rmtree(p)
            dump(root/(label+'.dead-working-cleanup.json'),dict(reason='SIGKILL child waited; kernel writer lock released; disposable lossy working files only',removed=removed))
        def imported(base,label,path,digest):
            c=config(base['state']['source']['topic'],label,base['state']['job_id']);c['trusted_history']=base['trusted_history'];c['codec']=base['codec'];c['state']['owners']=base['state']['owners'];execute(label+'-import',c,'import',[path,digest]);return c
        # Full-state/cold tier seed and self-contained export used for each isolated crash branch.
        topic=s.topic('atomic');c=config(topic,'atomic-roots');s.produce(topic,[(i%2,event(i,1,-50)) for i in range(80)])
        execute('atomic-seed',c,'run',['--pause-after-records',80]);exports=private(root/'savepoints')
        trace,_=execute('atomic-export',c,'export',[exports,'seed']);cap=next(r for r in trace if r.get('status')=='EXPORTED');path,digest=cap['path'],cap['blake3']
        s.produce(topic,[(0,event(9999,0,200)),(1,event(9999,0,200))])
        criteria=json.loads((root/'criteria.json').read_text())
        for point in criteria['crash_points']:
            label='kill-'+point;d=imported(c,label+'-roots',path,digest)
            old=json.loads(pathlib.Path(d['state']['checkpoint_root'],'CURRENT3').read_text());p=launch(label,d,'resume',['--pause-after-records',82,'--test-pause-at',point]);wait(label,p,point=point)
            visible=snapshot(label+'-concurrent-reader',d);require(len(visible)==(80 if point in criteria['new_selection_points'] else 0),'mixed prekill reader prefix')
            p.kill();finish(label,d,p,-9);snapshot(label+'-after-kill',d)
            current=json.loads(pathlib.Path(d['state']['checkpoint_root'],'CURRENT3').read_text());require((current==old)==(point not in criteria['new_selection_points']),'kill selected mixed epoch')
            clean_work(d,label);execute(label+'-resume',d,'resume',['--pause-after-records',82]);require(len(snapshot(label+'-final',d))==80,'missing/duplicate recovered output')
        # Combined fresh-process lifecycle: real codec conversion and actual separate owner stores.
        d=imported(c,'lifecycle-roots',path,digest)
        for codec,owners in [(2,2),(2,4),(2,1)]:
            label=f'transition-{codec}-{owners}';execute(label,d,'transition',[codec,owners],observe=False)
            d['codec']=codec;d['state']['owners']=owners;snapshot(label,d)
            execute(label+'-cold-resume',d,'resume',['--pause-after-records',80])
        execute('lifecycle-close',d,'resume',['--pause-after-records',82]);trace,_=execute('lifecycle-export',d,'export',[exports,'complete']);cap2=next(r for r in trace if r.get('status')=='EXPORTED');complete=imported(d,'complete-import-roots',cap2['path'],cap2['blake3']);execute('complete-import-resume',complete,'resume',['--pause-after-records',82])
        # Failed migration and failed export retain old visibility and the first safe savepoint.
        d=imported(c,'failed-migration-roots',path,digest);execute('failed-migration',d,'transition',[2,2,'--test-fail-at','epoch_verified'],expected=1,observe=False);snapshot('failed-migration-visible',d)
        execute('failed-export',c,'export',[exports,'failed','--test-fail-at','export_object'],expected=1,observe=False);require(not (exports/'failed').exists(),'failed export became named');execute('previous-export-safe',c,'export',[exports,'second'])
        # Real lifecycle kills use the same single commit boundary and separate reader.
        q=imported(c,'migration-kill-roots',path,digest);label='migration-kill';p=launch(label,q,'transition',[2,2,'--test-pause-at','epoch_verified']);wait(label,p,point='epoch_verified');snapshot(label+'-old',q);p.kill();finish(label,q,p,-9);clean_work(q,label);execute(label+'-retry',q,'transition',[2,2],observe=False);q['codec']=2;q['state']['owners']=2;snapshot(label+'-new',q)
        label='export-kill';p=launch(label,c,'export',[exports,'killed','--test-pause-at','export_object']);wait(label,p,point='export_object');p.kill();finish(label,c,p,-9);require(not (exports/'killed').exists(),'killed export acquired valid name')
        q=config(c['state']['source']['topic'],'import-kill-roots',c['state']['job_id']);q['trusted_history']=c['trusted_history'];label='import-kill';p=launch(label,q,'import',[path,digest,'--test-pause-at','import_before_selector']);wait(label,p,point='import_before_selector');out=s.call([str(pinned['txn']),str(cfgfile(label+'-reader',q)),'read'],label+'-reader');require(not out,'partial import exposed output');p.kill();finish(label,q,p,-9);execute(label+'-replay-refused',q,'import',[path,digest],expected=1,observe=False)
        q=imported(c,'slow-local-io-roots',path,digest);label='slow-local-io';p=launch(label,q,'resume',['--pause-after-records',82,'--test-pause-at','before_output_sync']);wait(label,p,point='before_output_sync');before=[r for r in rows(root/(label+'.stderr')) if r.get('type')=='input'];time.sleep(3);after=[r for r in rows(root/(label+'.stderr')) if r.get('type')=='input'];require(before==after,'source data polled during slow local sink');p.send_signal(signal.SIGINT);finish(label,q,p,1);snapshot(label+'-old',q);execute(label+'-resume',q,'resume',['--pause-after-records',82])
        # A real kernel file-size I/O refusal after a partial staged output write.
        import resource
        q=imported(c,'file-limit-roots',path,digest);label='file-size-io-failure';cp=cfgfile(label,q)
        def limited_file():
            resource.setrlimit(resource.RLIMIT_FSIZE,(1024,1024));signal.signal(signal.SIGXFSZ,signal.SIG_IGN)
        command=[str(pinned['txn']),str(cp),'resume','--pause-after-records','82'];start=time.monotonic_ns();r=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,preexec_fn=limited_file,timeout=40);(root/(label+'.stdout')).write_bytes(r.stdout);(root/(label+'.stderr')).write_bytes(r.stderr);require(r.returncode==1 and not r.stdout,'kernel I/O failure falsely succeeded');cases.append(dict(name=label,config=cp.name,command='resume',exit=r.returncode,rlimit_fsize=1024));dump(root/'cases.json',cases);snapshot(label+'-old',q)
        # Quiet source: SIGINT is an actual pause operation, including a competing writer.
        quiet=s.topic('quiet');q=config(quiet,'quiet-roots');p=launch('signal-pause',q,'run');wait('signal-pause',p,point='source_assigned');execute('competing-writer',q,'run',['--pause-after-ms',100],expected=1,observe=False);p.send_signal(signal.SIGINT);finish('signal-pause',q,p);execute('idle-cold-resume',q,'resume',['--pause-after-ms',250])
        # Multi-partition legal merges produce conflicting IDs across distinct lineages, never hide them.
        conflicts=[]
        for suffix,late_first in [('a',True),('b',False)]:
            topic=s.topic('order-'+suffix);q=config(topic,'order-'+suffix+'-roots',str(uuid.UUID(int=17)));s.produce(topic,[(0,event(1,2,0)),(1,event(1,3,0))]);execute('order-'+suffix+'-seed',q,'run',['--pause-after-records',2]);label='order-'+suffix;p=launch(label,q,'resume',['--pause-after-records',5]);wait(label,p,point='source_assigned')
            s.produce(topic,[(0,event(9999,0,200))]);wait(label,p,kind='input',total=3)
            if late_first:
                s.produce(topic,[(0,event(1,7,50))]);wait(label,p,kind='input',total=4);s.produce(topic,[(1,event(9999,0,200))])
            else:
                s.produce(topic,[(1,event(9999,0,200))]);wait(label,p,kind='input',total=4);s.produce(topic,[(0,event(1,7,50))])
            finish(label,q,p);conflicts.append(snapshot(label+'-check',q)[0])
        require(conflicts[0]['output_id']==conflicts[1]['output_id'] and conflicts[0]['payload_fingerprint']!=conflicts[1]['payload_fingerprint'],'legal-order conflicting payload experiment absent');dump(root/'conflicting-payloads.json',conflicts)
        repeats=[]
        for sample in range(2):
            topic=s.topic('repeat-'+str(sample));q=config(topic,'repeat-'+str(sample)+'-roots',str(uuid.UUID(int=1717)));label='ordered-repeat-'+str(sample);p=launch(label,q,'run',['--pause-after-records',6]);wait(label,p,point='source_assigned')
            for index,(part,e) in enumerate([(0,event(1,2,-50)),(1,event(1,3,-50)),(0,event(9999,0,200)),(0,event(1,7,-25)),(1,event(9999,0,200)),(1,event(1,9,-50))]):
                s.produce(topic,[(part,e)]);wait(label,p,kind='input',total=index+1)
            finish(label,q,p);repeats.append(snapshot(label+'-repeat-check',q))
        require(repeats[0]==repeats[1],'causally ordered trace was not repeatable');dump(root/'ordered-repeats.json',repeats)
        # Resource/security refusals use fresh imports; no fallback to an older selector.
        for label,mutate in [('wrong-history',lambda x:x.update(trusted_history='foreign')),('wrong-job',lambda x:x['state'].update(job_id=str(uuid.uuid4()))),('wrong-origin',lambda x:x['state']['source']['starts'][0].update(offset=1)),('wrong-codec',lambda x:x.update(codec=2)),('wrong-owners',lambda x:x['state'].update(owners=2))]:
            q=copy.deepcopy(c);mutate(q);execute(label,q,'resume',['--pause-after-records',82],expected=1,observe=False)
        for label,change in [('epoch-quota',lambda x:x['limits'].update(epochs=2)),('stage-quota',lambda x:x['limits'].update(stage_bytes=65536,output_bytes=32768)),('output-quota',lambda x:x['limits'].update(output_bytes=2048)),('history-quota',lambda x:x['limits'].update(history_bytes=2048,output_bytes=2048))]:
            q=imported(c,label+'-roots',path,digest);change(q);execute(label,q,'resume',['--pause-after-records',82],expected=1,observe=False);snapshot(label+'-old-visible',dict(q,limits=c['limits']))
        # Preserve physical evidence for missing, modified, symlink and hash-consistent corruption.
        for label in ['corrupt-selector','missing-output','modified-output','symlink-output','hardlink-output','wrong-shard-owner','broken-parent']:
            q=imported(c,label+'-roots',path,digest);check=pathlib.Path(q['state']['checkpoint_root']);selector=json.loads((check/'CURRENT3').read_text());folder=check/selector['id']
            if label=='corrupt-selector':(check/'CURRENT3').write_text('{}')
            elif label=='missing-output':(folder/'output.jsonl').unlink()
            elif label=='modified-output':(folder/'output.jsonl').write_bytes(b'{}\n')
            elif label=='symlink-output':(folder/'output.jsonl').unlink();(folder/'output.jsonl').symlink_to('/dev/null')
            elif label=='hardlink-output':os.link(folder/'output.jsonl',check/'extra-link')
            else:
                from digest import digest as b3
                m=json.loads((folder/'epoch.json').read_text())
                if label=='wrong-shard-owner':m['shards'][0]['owner']=9
                else:m['parent']=dict(selector)
                b=json.dumps(m,separators=(',',':')).encode();(folder/'epoch.json').write_bytes(b);selector['blake3']=b3(b);(check/'CURRENT3').write_text(json.dumps(selector))
            execute(label,q,'resume',['--pause-after-records',82],expected=1,observe=False)
        # Three honest static (v1) vs transactional same-fixture descriptive samples.
        costs=[];topic=s.topic('cost');s.produce(topic,[(i%2,event(i%40,1,0)) for i in range(120)])
        for sample in range(3):
            q=config(topic,f'cost-txn-{sample}-roots');trace,_=execute(f'cost-txn-{sample}',q,'run',['--pause-after-records',120]);case=cases[-1];costs.append(dict(mode='txn',sample=sample,elapsed_ns=case['elapsed_ns'],commit_ns=[r['elapsed_ns'] for r in trace if r.get('type')=='committed'],retained_bytes=sum(p.stat().st_size for p in pathlib.Path(q['state']['checkpoint_root']).rglob('*') if p.is_file())))
            old=native(s,topic,f'cost-static-{sample}-roots');cp=root/f'cost-static-{sample}.config.json';dump(cp,old);start=time.monotonic_ns();s.call([str(pinned['legacy']),str(cp),'--test-stop-records','120'],f'cost-static-{sample}',timeout=90);costs.append(dict(mode='static_v1',sample=sample,elapsed_ns=time.monotonic_ns()-start))
        dump(root/'costs.json',costs);dump(root/'outcome.json',dict(status='CAMPAIGN_FINISHED',cases=len(cases),observations=len(observations),prerequisite='fresh real-service probes',finished=time.time()))
    finally:
        if s:s.close()
        dump(root/'cases.json',cases);dump(root/'observations.json',observations)

if __name__=='__main__':campaign(P.parse_args())
