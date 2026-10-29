#!/usr/bin/env python3
"""Owned isolated Kafka3.9.1 + native ADE state/checkpoint campaign. No cloud."""
import argparse, base64, copy, hashlib, json, os, pathlib, shutil, signal, socket, subprocess, tarfile, time, uuid
from oracle import Model, require, rows

P=argparse.ArgumentParser()
P.add_argument('--kafka-home',type=pathlib.Path,required=True)
P.add_argument('--java-home',type=pathlib.Path,required=True)
P.add_argument('--output',type=pathlib.Path,required=True)
P.add_argument('--binary',type=pathlib.Path,default=pathlib.Path('target/debug/ade-state-stream'))
a=P.parse_args();root=a.output.resolve();root.mkdir(parents=True,exist_ok=False)
repo=pathlib.Path(__file__).resolve().parents[2]
java=str(a.java_home/'bin/java');javac=str(a.java_home/'bin/javac');cp=str(a.kafka_home.resolve()/'libs/*');binary=str(root/'ade-state-stream-pinned');shutil.copy2(a.binary.resolve(),binary)
cluster=base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=');children=[];cases=[];commands=[];ledgers={};models={};control_process=None

def dump(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True))
def command(cmd,label,data=None,timeout=30):
    commands.append(dict(label=label,argv=cmd));result=subprocess.run(cmd,input=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
    (root/(label+'.stdout')).write_bytes(result.stdout);(root/(label+'.stderr')).write_bytes(result.stderr)
    require(result.returncode==0,f'{label} failed: {result.stderr[-1000:]}');return result.stdout

def free_port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]

port,controller=free_port(),free_port();address=f'127.0.0.1:{port}'
props=root/'broker.properties'
props.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{controller}
listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://127.0.0.1:{port}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs={root/'broker-data'}
num.partitions=3
offsets.topic.replication.factor=1
offsets.topic.num.partitions=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
transaction.state.log.num.partitions=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
message.max.bytes=32768
log.retention.hours=24
''')
source_paths=sorted(set(subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],text=True).splitlines()))
source_paths=[p for p in source_paths if not p.startswith('docs/evidence/') and p!='sol-medium-phase-0-bootstrap-implementation-prompt.md']
inventory={p:hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in source_paths if (repo/p).is_file()}
dump(root/'source-inventory.json',inventory)
with tarfile.open(root/'source.tar.gz','w:gz') as tar:
    for p in inventory:tar.add(repo/p,arcname=p)
dump(root/'environment.json',dict(base=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),status=subprocess.check_output(['git','status','--short'],text=True),platform=list(os.uname()),rustc=subprocess.check_output(['rustc','-Vv'],text=True),cargo=subprocess.check_output(['cargo','-V'],text=True),binary_sha256=hashlib.sha256(pathlib.Path(binary).read_bytes()).hexdigest(),cluster=cluster,kafka='3.9.1',started=time.time()))
fixture_dir=root/'fixture';fixture_dir.mkdir();command([javac,'-cp',cp,'-d',str(fixture_dir),str(repo/'scripts/phase15/Fixture.java')],'javac');fixture_cp=str(fixture_dir)+os.pathsep+cp

def fixture(mode,topic,data=None,extra=()):return command([java,'-cp',fixture_cp,'Fixture',address,mode,topic,*map(str,extra)],f'{len(commands):03}-{mode}',data)
def start_broker(label):
    log=open(root/f'broker-{label}.log','wb');p=subprocess.Popen([java,'-Xmx512m','-Xms256m','-cp',cp,'kafka.Kafka',str(props)],stdout=log,stderr=subprocess.STDOUT);log.close();children.append(p)
    until=time.monotonic()+30
    while time.monotonic()<until:
        require(p.poll() is None,'broker exited')
        try:
            with socket.create_connection(('127.0.0.1',port),timeout=.1):time.sleep(2);return p
        except OSError:time.sleep(.1)
    raise ValueError('broker start timeout')

def topic(label,partitions=3):
    name='ade15-'+uuid.uuid4().hex[:8]+'-'+label;fixture('create',name,extra=[partitions]);ledgers[name]=[];return name

def event(key,value,time_ms):return dict(version=1,key=key,value_i64=value,event_time_ms=time_ms)
def produce(topic,events,mode='produce'):
    data=''.join(f'{p}\t'+json.dumps(e,separators=(',',':'))+'\n' for p,e in events).encode();out=fixture(mode,topic,data)
    if mode=='produce':
        ledger=[(int(p),int(off),json.loads(payload)) for p,off,payload in (line.split('\t',2) for line in out.decode().splitlines())];ledgers[topic]+=ledger
        dump(root/'ledgers.json',ledgers);return ledger
    return []

def descriptor(topic,label,owners=1,partitions=3):
    folder=root/label;folder.mkdir();work=folder/'work';check=folder/'check';work.mkdir(mode=0o700);check.mkdir(mode=0o700)
    return dict(version=1,job_id=str(uuid.uuid4()),source=dict(version=1,bootstrap=address,expected_cluster_id=cluster,topic=topic,starts=[dict(partition=p,offset=0) for p in range(partitions)],security=dict(mode='loopback_dev'),key_less_than=1000,allowance_ms=10,payload_bytes=4096,client_queue_kib=16,sink_stall_ms=90000,timers=[]),topic_partitions=list(range(partitions)),operator='tumbling_sum_count_v1',window_ms=100,late_policy='receipt_drop_closed_v1',hash_version=1,owners=owners,hot_bytes=384,cold_bytes=1024*1024,checkpoint_bytes=16*1024*1024,max_generations=64,checkpoint_every_records=40,checkpoint_every_ms=60000,working_root=str(work),checkpoint_root=str(check))

def fork_config(c,label):
    d=descriptor(c['source']['topic'],label,partitions=len(c['topic_partitions']));d.update(copy.deepcopy(c));d['working_root']=str(root/label/'work');d['checkpoint_root']=str(root/label/'check')
    for path in pathlib.Path(c['checkpoint_root']).iterdir():
        if path.name=='LOCK':continue
        if path.is_dir():shutil.copytree(path,pathlib.Path(d['checkpoint_root'])/path.name)
        else:shutil.copy2(path,pathlib.Path(d['checkpoint_root'])/path.name)
    return d

def launch(label,c,args=(),pipe=False):
    dump(root/(label+'.config.json'),c);out=None if pipe else open(root/(label+'.output.jsonl'),'wb');err=open(root/(label+'.trace.jsonl'),'wb')
    cmd=[binary,str(root/(label+'.config.json')),*map(str,args)];commands.append(dict(label=label,argv=cmd));p=subprocess.Popen(cmd,stdout=subprocess.PIPE if pipe else out,stderr=err);children.append(p)
    if out:out.close()
    err.close();return p

def traces(label):return rows(root/(label+'.trace.jsonl'))
def wait_trace(label,kind,point=None,timeout=20):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        found=[r for r in traces(label) if r['type']==kind and (point is None or r.get('point')==point)]
        if found:return found[-1]
        time.sleep(.03)
    raise ValueError(f'{label}: missing {kind} {point}: {traces(label)[-2:]}')

def save_checkpoints(label,c):
    dest=root/(label+'.checkpoints');dest.mkdir()
    for path in pathlib.Path(c['checkpoint_root']).iterdir():
        if path.name=='LOCK':continue
        if path.is_dir():shutil.copytree(path,dest/path.name,symlinks=True)
        else:shutil.copy2(path,dest/path.name)

def finish(p,label,c,expected=0,reason=None,killpoint=None,model=None,timeout=30):
    rc=p.wait(timeout);require(rc==expected,f'{label} exit {rc}: {traces(label)[-3:]}')
    case=dict(name=label,exit=rc,reason=reason,killpoint=killpoint,topic=c['source']['topic']);cases.append(case);dump(root/'cases.json',cases)
    ts=traces(label)
    if expected==0:require(any(t['type']=='terminal' and t['status']=='STOPPED' for t in ts),label+' no success terminal')
    elif expected==-9:require(not any(t['type']=='terminal' for t in ts),label+' killed but terminal')
    else:
        require(any(t['type']=='failure' and any(part in t['reason'] for part in (reason if isinstance(reason,tuple) else [reason])) for t in ts),label+' wrong failure reason')
    if model is not None:
        ledger={(p,off):e for p,off,e in ledgers[c['source']['topic']]}
        for r in rows(root/(label+'.output.jsonl')):model.consume(r,ledger)
        if expected==0:model.check_progress(next(t for t in reversed(ts) if t['type']=='terminal')['progress'])
        models[label]=model.clone()
    save_checkpoints(label,c);return case

def current(c):
    folder=pathlib.Path(c['checkpoint_root']);pointer=json.loads((folder/'CURRENT').read_text());return folder/pointer['generation']

def wait_total(label,total):
    until=time.monotonic()+30
    while time.monotonic()<until:
        if any(t.get('progress',{}).get('forwarded_records',0)>=total for t in traces(label)):return
        time.sleep(.03)
    raise ValueError(f'{label}: did not reach {total}')

def run_ok(label,c,total,restore=False,model=None):
    p=launch(label,c,(['--restore'] if restore else [])+['--test-stop-records',total]);finish(p,label,c,model=model)
    if control_process is not None and label in ['owner1','owner2','owner4','owner1-return','quiet-frontier','close','late-open','final-close']:wait_total('ample-control',total)
    return p

try:
    command([java,'-cp',cp,'kafka.tools.StorageTool','format','--cluster-id',cluster,'--config',str(props)],'format')
    broker=start_broker('initial');require(fixture('cluster','unused').decode().strip()==cluster,'cluster differs')
    stream=topic('state');c=descriptor(stream,'state-roots');c['checkpoint_every_records']=80
    control=descriptor(stream,'control');control['job_id']=c['job_id'];control['hot_bytes']=192*1000;control['checkpoint_every_records']=209
    control_process=launch('ample-control',control,['--test-stop-records',209]);wait_trace('ample-control','admitted')
    produce(stream,[(i%2,event(i%61,(i%19)-9,(i%3)*100-120)) for i in range(80)])
    m=Model(c);run_ok('owner1',c,80,model=m);m.check_checkpoint(current(c));require(m.frontier is None,'quiet partition advanced frontier');seed=m.clone()
    frozen=fork_config(c,'frozen-seed');frozen_model=seed.clone()
    produce(stream,[(i%2,event(i%61,3,(i%3)*100-120)) for i in range(40)])
    c['owners']=2;c['checkpoint_every_records']=40
    run_ok('owner2',c,120,True,m);m.check_checkpoint(current(c))
    # Real aborted transaction offsets produce legal holes under read_committed.
    produce(stream,[(0,event(999,999,999)),(1,event(999,999,999))],mode='abort')
    produce(stream,[(i%2,event(i%61,-2,(i%3)*100-120)) for i in range(40)])
    c['owners']=4;run_ok('owner4',c,160,True,m);m.check_checkpoint(current(c))
    c['owners']=1;produce(stream,[(i%2,event(i%61,5,(i%3)*100-120)) for i in range(40)])
    run_ok('owner1-return',c,200,True,m);m.check_checkpoint(current(c))
    # Advance both active partitions first; quiet third still prevents closure.
    produce(stream,[(0,event(9999,0,1000)),(1,event(9999,0,1000))]);c['checkpoint_every_records']=2
    run_ok('quiet-frontier',c,202,True,m);require(m.frontier is None,'idle exclusion');m.check_checkpoint(current(c))
    closure_seed=fork_config(c,'closure-seed');closure_model=m.clone()
    c['checkpoint_every_records']=1
    produce(stream,[(2,event(9999,0,1000))]);run_ok('close',c,203,True,m)
    # Kill after window delivery but before promotion. The retained cut causes stable-ID replay.
    d=fork_config(closure_seed,'duplicate-roots');d['checkpoint_every_records']=1;d['checkpoint_every_ms']=3600000
    p=launch('window-kill',d,['--restore','--test-stop-records',203,'--test-pause-at','stage_created']);wait_trace('window-kill','checkpoint_step','stage_created');p.kill();finish(p,'window-kill',d,-9,killpoint='stage_created')
    first=rows(root/'window-kill.output.jsonl');require(any(r['type']=='window' for r in first),'kill after close missing')
    pathlib.Path(d['checkpoint_root'],'LOCK').unlink()
    for orphan in pathlib.Path(d['working_root']).iterdir():shutil.rmtree(orphan)
    replay=closure_model.clone();run_ok('window-replay',d,203,True,replay)
    replay_rows=rows(root/'window-replay.output.jsonl');ids={r['output_id'] for r in first if r['type']=='window'};require(ids.issubset({r['output_id'] for r in replay_rows if r['type']=='window'}),'stable replay IDs missing')
    produce(stream,[(0,event(1,999,-1)),(1,event(1,5,1000)),(0,event(1,7,1000))]);run_ok('late-open',c,206,True,m)
    require(m.closed_late==1,'late closed arrival not explicit')
    produce(stream,[(0,event(9999,0,2000)),(1,event(9999,0,2000)),(2,event(9999,0,2000))]);run_ok('final-close',c,209,True,m);require(not m.state,'final state should be empty')
    m.check_checkpoint(current(c)) if json.loads((current(c)/'manifest.json').read_text())['progress']['forwarded_records']==209 else None
    # An ample-memory uninterrupted run must have the exact same complete window results.
    cm=Model(control);finish(control_process,'ample-control',control,model=cm);control_process=None
    require(sorted(m.outputs,key=lambda r:r['output_id'])==sorted(cm.outputs,key=lambda r:r['output_id']),'restore/tiering differs from uninterrupted control')
    # Actual SIGKILL at every publication boundary, with a distinct post-seed cut.
    points=['stage_created','shard_write','shard_checksum','shard_synced','manifest_write','manifest_checksum','manifest_synced','staging_verified','stage_synced','generation_renamed','generation_synced','pointer_write','pointer_synced','pointer_renamed','pointer_durable','cleanup']
    for point in points:
        label='kill-'+point;d=fork_config(frozen,label+'-roots');d['checkpoint_every_records']=40
        p=launch(label,d,['--restore','--test-stop-records',120,'--test-pause-at',point]);wait_trace(label,'checkpoint_step',point);p.kill();finish(p,label,d,-9,killpoint=point)
        selected=json.loads((current(d)/'manifest.json').read_text())['progress']['forwarded_records'];require(selected in (80,120),'partial selected generation')
        require(selected==(120 if point in ('pointer_renamed','pointer_durable','cleanup') else 80),'wrong publication boundary')
        # Operator-proved dead process: remove only its stale lock, never adopt working files.
        pathlib.Path(d['checkpoint_root'],'LOCK').unlink()
        for orphan in pathlib.Path(d['working_root']).iterdir():shutil.rmtree(orphan)
        recovered=seed.clone()
        if selected==120:
            for r in rows(root/(label+'.output.jsonl')):recovered.consume(r,{(p,o):e for p,o,e in ledgers[stream]})
        d['source']['starts']=[dict(partition=p,offset=999999) for p in range(3)]
        run_ok(label+'-restore',d,120,True,recovered);recovered.check_checkpoint(current(d))
    # SIGINT inside snapshot writes/pointer preparation leaves the prior selection.
    for suffix,point in [('write','shard_write'),('promote','pointer_synced')]:
        label='cancel-checkpoint-'+suffix;d=fork_config(frozen,label+'-roots');d['checkpoint_every_records']=40
        p=launch(label,d,['--restore','--test-stop-records',120,'--test-pause-at',point]);wait_trace(label,'checkpoint_step',point);p.send_signal(signal.SIGINT);finish(p,label,d,1,reason='cancelled',timeout=5)
        require(json.loads((current(d)/'manifest.json').read_text())['progress']['forwarded_records']==80,'cancelled checkpoint advanced')
        require(not any(t['type']=='checkpoint_success' for t in traces(label)),'cancelled checkpoint success')
    d=fork_config(frozen,'orphan-working-roots');pathlib.Path(d['working_root'],str(uuid.uuid4())).mkdir(mode=0o700)
    p=launch('orphan-working',d,['--restore','--test-stop-ms',1000]);finish(p,'orphan-working',d,1,reason='working root not empty');require(not (root/'orphan-working.output.jsonl').read_bytes(),'orphan adopted')
    # Slow snapshot I/O surrogate: demand owner is stationary while control stats drain.
    d=fork_config(frozen,'slow-checkpoint-roots');d['checkpoint_every_records']=40
    p=launch('slow-checkpoint-control',d,['--restore','--test-stop-records',120,'--test-pause-at','shard_write']);wait_trace('slow-checkpoint-control','checkpoint_step','shard_write');time.sleep(5.2)
    paused=[t for t in traces('slow-checkpoint-control') if t['type']=='checkpoint_blocked'];require(len(paused)>=4,'slow checkpoint witness short')
    require(paused[-1]['source']['checkpoint_control']['stats_callbacks']>=6,'statistics not drained during I/O')
    p.send_signal(signal.SIGINT);finish(p,'slow-checkpoint-control',d,1,reason='cancelled',timeout=5)
    # Admission must refuse before output; retain typed descriptors and exact diagnostics.
    for label,change,reason in [
        ('wrong-job',lambda d:d.update(job_id=str(uuid.uuid4())),'checkpoint identity'),
        ('wrong-window',lambda d:d.update(window_ms=99),'checkpoint identity'),
        ('wrong-hash',lambda d:d.update(hash_version=2),'stateful version'),
        ('wrong-cluster',lambda d:d['source'].update(expected_cluster_id='wrong'),'checkpoint identity'),
        ('wrong-topic',lambda d:d['source'].update(topic='wrong-topic'),'checkpoint identity'),
        ('wrong-filter',lambda d:d['source'].update(key_less_than=1),'checkpoint identity'),
        ('wrong-partitions',lambda d:d['source'].update(starts=[dict(partition=0,offset=0)]),'checkpoint identity'),
    ]:
        d=fork_config(frozen,label+'-roots');change(d);p=launch(label,d,['--restore','--test-stop-ms',1000]);finish(p,label,d,1,reason=reason);require(not (root/(label+'.output.jsonl')).read_bytes(),'output before refusal')
    # Corrupt every integrity domain; no automatic fallback from corrupt current.
    for label,mutate in [('truncated-shard','truncate'),('corrupt-shard','flip'),('corrupt-manifest','manifest'),('pointer-traversal','pointer'),('symlink-shard','symlink'),('oversized-pointer','oversized')]:
        d=fork_config(frozen,label+'-roots');gen=current(d);files=[f for f in gen.glob('shard-*.bin') if f.stat().st_size];target=files[0]
        if mutate=='truncate':target.write_bytes(target.read_bytes()[:-1])
        elif mutate=='flip':b=bytearray(target.read_bytes());b[0]^=1;target.write_bytes(b)
        elif mutate=='manifest':(gen/'manifest.json').write_text('{}')
        elif mutate=='pointer':pathlib.Path(d['checkpoint_root'],'CURRENT').write_text(json.dumps(dict(version=1,generation='../../escape',manifest_blake3='0'*64)))
        elif mutate=='symlink':target.unlink();target.symlink_to('/etc/passwd')
        else:pathlib.Path(d['checkpoint_root'],'CURRENT').write_bytes(b' '*513)
        p=launch(label,d,['--restore','--test-stop-ms',1000]);finish(p,label,d,1,reason={'truncate':'shard length','flip':'record checksum','manifest':'manifest checksum','pointer':'strict checkpoint','symlink':'open regular','oversized':'regular private file'}[mutate]);require(not (root/(label+'.output.jsonl')).read_bytes(),'corrupt restore output')
    # A real output pipe stays blocked >30s while a separate producer grows backlog.
    pressure=topic('pressure',2);pc=descriptor(pressure,'pressure-roots',partitions=2);pc['checkpoint_every_records']=100000;pc['checkpoint_every_ms']=3600000;pc['hot_bytes']=192*32
    produce(pressure,[(i%2,event(i%10,(i%17)-8,0)) for i in range(1400)])
    p=launch('backpressure',pc,['--test-stop-records',1600],pipe=True);wait_trace('backpressure','sink_blocked');began=time.monotonic()
    produce(pressure,[(i%2,event(i%10,1,0)) for i in range(200)])
    while time.monotonic()-began<35:require(p.poll() is None,'blocked reader exited');time.sleep(.2)
    out,_=p.communicate(timeout=40);(root/'backpressure.output.jsonl').write_bytes(out);finish(p,'backpressure',pc,model=Model(pc))
    blocks=[t for t in traces('backpressure') if t['type']=='sink_blocked'];require(len(blocks)>=30,'short blocked witness')
    require(all(t['progress']==blocks[0]['progress'] for t in blocks),'progress advanced while blocked');require(blocks[-1]['elapsed_ms']-blocks[0]['elapsed_ms']>=30000,'stall <30 seconds')
    require(len({t['source']['queue_bytes_peak'] for t in blocks[-10:]})==1,'queue did not plateau')
    # Cancellation of a blocked sink with checkpoint pressure must not publish an unmatched cut.
    pc['checkpoint_every_records']=8
    p=launch('cancel-blocked',pc,['--test-stop-ms',60000],pipe=True);wait_trace('cancel-blocked','sink_blocked');p.send_signal(signal.SIGINT);out,_=p.communicate(timeout=5);(root/'cancel-blocked.output.jsonl').write_bytes(out);finish(p,'cancel-blocked',pc,1,reason='cancelled')
    # Broker outage and restart: failed attempt then explicit new session at its saved cut.
    outage=topic('outage');oc=descriptor(outage,'outage-roots');oc['checkpoint_every_records']=2
    produce(outage,[(0,event(1,2,0)),(1,event(1,3,0))]);om=Model(oc);run_ok('outage-seed',oc,2,model=om)
    p=launch('broker-outage',oc,['--restore','--test-stop-ms',15000]);wait_trace('broker-outage','admitted');broker.terminate();broker.wait(10);finish(p,'broker-outage',oc,1,reason=('broker failure','unexpected main-queue data/error'),timeout=15)
    broker=start_broker('restart');produce(outage,[(0,event(2,4,0)),(1,event(2,5,0))]);run_ok('broker-restart',oc,4,True,om);om.check_checkpoint(current(oc))
    # Retention invalidates the seed's saved next offset, even if initial config offsets differ.
    high=max(o for p,o,e in ledgers[stream] if p==0)+1;fixture('retain',stream,extra=[0,high])
    d=fork_config(frozen,'retention-roots');p=launch('retention-lost',d,['--restore','--test-stop-ms',1000]);finish(p,'retention-lost',d,1,reason='offset range/retention');require(not (root/'retention-lost.output.jsonl').read_bytes(),'retention output')
    dump(root/'commands.json',commands);dump(root/'cases.json',cases);dump(root/'ledgers.json',ledgers)
    print(json.dumps(dict(cases=len(cases),status='completed; run independent verifier',binary=hashlib.sha256(pathlib.Path(binary).read_bytes()).hexdigest())))
finally:
    dump(root/'commands.json',commands);dump(root/'cases.json',cases);dump(root/'ledgers.json',ledgers)
    for child in reversed(children):
        if child.poll() is None:
            child.terminate()
            try:child.wait(5)
            except subprocess.TimeoutExpired:child.kill();child.wait(5)
