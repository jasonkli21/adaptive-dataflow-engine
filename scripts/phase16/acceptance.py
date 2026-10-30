#!/usr/bin/env python3
"""Owned isolated Kafka3.9.1 + native ADE state/checkpoint campaign. No cloud."""
import argparse, base64, copy, hashlib, json, os, pathlib, shutil, signal, socket, subprocess, tarfile, time, uuid
from oracle import Model, require, rows
import threading

P=argparse.ArgumentParser()
P.add_argument('--kafka-home',type=pathlib.Path,required=True)
P.add_argument('--java-home',type=pathlib.Path,required=True)
P.add_argument('--output',type=pathlib.Path,required=True)
P.add_argument('--minio',type=pathlib.Path,required=True)
P.add_argument('--service-python',type=pathlib.Path,required=True)
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
source_paths=[p for p in source_paths if not p.startswith('docs/evidence/') and p not in ('docs/reports/phase-16-implementation-report.md','sol-medium-phase-0-bootstrap-implementation-prompt.md')]
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
    name='ade16-'+uuid.uuid4().hex[:8]+'-'+label;fixture('create',name,extra=[partitions]);ledgers[name]=[];return name

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

def launch(label,c,args=(),pipe=False,policy=None):
    dump(root/(label+'.config.json'),c);out=None if pipe else open(root/(label+'.output.jsonl'),'wb');err=open(root/(label+'.trace.jsonl'),'wb')

    if policy is not None:dump(root/(label+'.policy.json'),policy)
    cmd=[binary,str(root/(label+'.config.json')),*(['--adaptive-policy',str(root/(label+'.policy.json'))] if policy is not None else []),*map(str,args)];commands.append(dict(label=label,argv=cmd));p=subprocess.Popen(cmd,stdout=subprocess.PIPE if pipe else out,stderr=err);children.append(p)
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

# Predeclared criteria: all exact oracle/copy/failpoint checks, no speedup gate.
# Three matched240-record full/local and adaptive/local samples; one remote and
# one replicated sample; real incremental bytes < canonical bytes; depth<=2.
# Preserve every failed campaign directory; never overwrite a candidate.

def policy(c,placement='local',mode='adaptive'):
    return dict(version=1,mode=mode,min_records=40,max_records=40,min_ms=3600000,max_ms=3600000,max_depth=2,min_samples=2,cooldown_ms=0,hysteresis_nano=0,placements=[placement],static_placement=placement,
        constraints=dict(min_copies=2 if placement=='both' else 1,require_remote=placement!='local',max_loss_ppm=1000000,local_loss_ppm=100000,remote_loss_ppm=100,both_loss_ppm=1,recovery_slo_ms=86400000,failure_ppm=1000,retention_ms=1000),
        prices=dict(local_write_byte=1,local_read_byte=0,remote_read_byte=0,remote_write_byte=5,remote_request=3,capacity_byte_ms=0,compute_ms=1,replay_record=1),
        limits=dict(object_bytes=1048576,remote_objects=8192,remote_write_bytes=268435456,requests=65536,response_bytes=68719476736,io_timeout_ms=5000,trace_bytes=67108864),
        remote=dict(endpoint=proxy_endpoint,bucket='ade-phase16',prefix='private16',region='us-east-1',namespace=str(uuid.uuid4()),dev_allow_http=True) if placement!='local' else None,
        recovery=dict(original_starts=copy.deepcopy(c['source']['starts']),trusted_immutable_history=True,allow_replay=False,accept_duplicate_and_conflicting_outputs=False,replay_certificate_blake3=None,replay_estimated_ms=1000,restore_estimated_ms=100))

def run(label,c,p,total,recover=False,extra=(),expected=0,reason=None):
    proc=launch(label,c,(['--recover'] if recover else [])+['--test-stop-records',total,*extra],policy=p)
    finish(proc,label,c,expected,reason,timeout=120)
    return proc

def selected(c):return json.loads((pathlib.Path(c['checkpoint_root'])/'CURRENT2').read_text())
def clean_lock(c):
    folder=pathlib.Path(c['checkpoint_root']);lock=folder/'LOCK'
    if lock.exists():lock.unlink()
    for f in pathlib.Path(c['working_root']).iterdir():
        if f.is_dir():shutil.rmtree(f)
        else:f.unlink()
def fault(rule):
    request=urllib.request.Request(proxy_endpoint+'/__control',json.dumps(rule).encode(),{'Content-Type':'application/json'})
    with urllib.request.urlopen(request,timeout=5) as response:require(response.status==200,'proxy control')

def service_ready(port,p):
    until=time.monotonic()+30
    while time.monotonic()<until:
        require(p.poll() is None,'service exited')
        try:
            with socket.create_connection(('127.0.0.1',port),timeout=.2):return
        except OSError:time.sleep(.1)
    raise ValueError('service startup timeout')

import urllib.request
minio_port,console_port,proxy_port=free_port(),free_port(),free_port()
proxy_endpoint=f'http://127.0.0.1:{proxy_port}'
minio_endpoint=f'http://127.0.0.1:{minio_port}'
# Disposable generated credentials remain environment-only and are never archived.
os.environ['AWS_ACCESS_KEY_ID']='ade16'+uuid.uuid4().hex
os.environ['AWS_SECRET_ACCESS_KEY']=uuid.uuid4().hex+uuid.uuid4().hex
service_env=dict(os.environ,MINIO_ROOT_USER=os.environ['AWS_ACCESS_KEY_ID'],MINIO_ROOT_PASSWORD=os.environ['AWS_SECRET_ACCESS_KEY'],MINIO_BROWSER='off')
# Verify proxy bypass behavior for the engine; test-control urllib uses explicit no-proxy.
os.environ['HTTP_PROXY']='http://127.0.0.1:1';os.environ['HTTPS_PROXY']='http://127.0.0.1:1';os.environ['NO_PROXY']='127.0.0.1'
try:
    minio_log=open(root/'minio.log','wb');minio=subprocess.Popen([str(a.minio.resolve()),'server','--address',f'127.0.0.1:{minio_port}','--console-address',f'127.0.0.1:{console_port}',str(root/'minio-data')],env=service_env,stdout=minio_log,stderr=subprocess.STDOUT);children.append(minio);minio_log.close();service_ready(minio_port,minio)
    # Provision only this owned, disposable local service bucket. No LIST.
    provision="""import boto3,os,sys
from botocore.config import Config
s=boto3.client('s3',endpoint_url=sys.argv[1],region_name='us-east-1',aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],config=Config(proxies={},retries={'max_attempts':0}))
s.create_bucket(Bucket='ade-phase16')
"""
    command([str(a.service_python),'-c',provision,minio_endpoint],'bucket')
    log=open(root/'proxy.log','wb');proxy=subprocess.Popen([str(a.service_python),str(repo/'scripts/phase16/service.py'),'--backend',minio_endpoint,'--port',str(proxy_port),'--root',str(root)],stdout=log,stderr=subprocess.STDOUT);children.append(proxy);log.close();service_ready(proxy_port,proxy)
    dump(root/'service.json',dict(kind='real-minio-source-build',tag='RELEASE.2025-09-07T16-13-09Z',server_sha256=hashlib.sha256(a.minio.read_bytes()).hexdigest(),pid=minio.pid,proxy_pid=proxy.pid,endpoint=minio_endpoint,proxy_endpoint=proxy_endpoint,bucket='ade-phase16'))
    command([java,'-cp',cp,'kafka.tools.StorageTool','format','--cluster-id',cluster,'--config',str(props)],'format')
    broker=start_broker('initial');require(fixture('cluster','unused').decode().strip()==cluster,'cluster differs')
    stream=topic('matched');events=[(i%2,event(i,1,0)) for i in range(80)]+[(i%2,event(1,2,0)) for i in range(160)];produce(stream,events)
    job=str(uuid.uuid4());runs={}
    for placement,mode,sample in [('local',m,i) for i in range(3) for m in ['static','adaptive']]+[('remote','adaptive',0),('both','adaptive',0)]:
        label=f'{placement}-{mode}-{sample}';c=descriptor(stream,label+'-roots');c['job_id']=job;p=policy(c,placement,mode);run(label,c,p,240);runs[label]=(c,p)
    for label,regime in [('cadence-low-risk','low'),('cadence-high-risk','high'),('adaptive-placement','place')]:
        c=descriptor(stream,label+'-roots');c['job_id']=job;p=policy(c,'both' if regime=='place' else 'local');p.update(min_records=20,max_records=80)
        if regime=='high':p['constraints']['failure_ppm']=1000000;p['prices']['replay_record']=10000000
        if regime=='low':p['constraints']['failure_ppm']=0
        if regime=='place':p['placements']=['local','remote','both'];p['constraints'].update(min_copies=1,require_remote=False)
        run(label,c,p,240)
    seed,seed_policy=runs['both-adaptive-0'];seed_selected=selected(seed);seed_cert=seed_selected['certificate'];seed_hash=seed_selected['certificate_blake3']
    # Independently restore each required copy with physical logical owner changes.
    for place,owners in [('local',2),('remote',4)]:
        c=fork_config(seed,'restore-'+place+'-roots');c['owners']=owners;run('restore-'+place,c,seed_policy,240,True,['--restore-copy',place])
    # True disk-loss: empty new check/work, only operator-retained certificate/hash.
    remote_seed,remote_policy=runs['remote-adaptive-0'];remote_selection=selected(remote_seed);remote_cert=remote_selection['certificate'];remote_hash=remote_selection['certificate_blake3']
    c=descriptor(stream,'clean-scratch-roots');c['job_id']=job;c['owners']=1;p=copy.deepcopy(remote_policy)
    certificate_file=root/'operator-certificate.json';certificate_file.write_bytes((pathlib.Path(remote_seed['checkpoint_root'])/remote_cert['manifest']['generation']/'certificate.json').read_bytes())
    run('clean-scratch',c,p,240,True,['--restore-copy','remote','--certificate',str(certificate_file),'--certificate-blake3',remote_hash])
    require(not any(path.name.startswith('shard-') for path in pathlib.Path(c['checkpoint_root']).rglob('*')),'remote-only restore used local shards')
    # Both recovery recommendations are actual execution branches.
    replay=fork_config(seed,'replay-roots');rp=copy.deepcopy(seed_policy);rp['recovery'].update(allow_replay=True,accept_duplicate_and_conflicting_outputs=True,replay_certificate_blake3=seed_hash,replay_estimated_ms=1,restore_estimated_ms=10000)
    run('full-replay',replay,rp,240,True,['--restore-copy','remote'])
    # Wrong consent/newer cut, corrupt selector and source identity refuse pre-output.
    for label,mutate in [('wrong-consent',lambda c,p:p['recovery'].update(allow_replay=True,accept_duplicate_and_conflicting_outputs=True,replay_certificate_blake3='a'*64)),('no-consent',lambda c,p:p['recovery'].update(allow_replay=True)),('untrusted-history',lambda c,p:p['recovery'].update(trusted_immutable_history=False)),('wrong-topic',lambda c,p:c['source'].update(topic='foreign-topic')),('wrong-origin',lambda c,p:p['recovery']['original_starts'][0].update(offset=1)),('weaken-copies',lambda c,p:p['constraints'].update(min_copies=1))]:
        d=fork_config(seed,label+'-roots');pp=copy.deepcopy(seed_policy);mutate(d,pp)
        run(label,d,pp,240,True,expected=1,reason={'wrong-consent':'replay consent','no-consent':'replay requires','untrusted-history':'trusted original','wrong-topic':'certificate/explicit copy/durability','wrong-origin':'exact original','weaken-copies':'certificate/explicit copy/durability'}[label])
    d=fork_config(seed,'corrupt-selector-roots');(pathlib.Path(d['checkpoint_root'])/'CURRENT2').write_text('{}');run('corrupt-selector',d,seed_policy,240,True,expected=1,reason='strict v2')
    # A causal40-record seed, then40 updates for every crash/failure boundary.
    fault_topic=topic('faults');produce(fault_topic,[(i%2,event(i,1,0)) for i in range(40)])
    base=descriptor(fault_topic,'fault-seed-roots');bp=policy(base,'both');run('fault-seed',base,bp,40)
    frozen=fork_config(base,'fault-frozen-roots');frozen_selected=selected(frozen)
    produce(fault_topic,[(i%2,event(1,2,0)) for i in range(40)])
    points=['policy_decided','stage_created','shard_write','shard_synced','remote_shard_put','remote_shard_verified','manifest_write','manifest_synced','remote_manifest_put','remote_manifest_verified','stage_synced','generation_renamed','generation_synced','copies_verified','remote_certificate_put','remote_certificate_verified','certificate_synced','before_selector','selector_synced','selector_renamed','selector_durable']
    for point in points:
        d=fork_config(frozen,'kill-'+point+'-roots');label='kill-'+point
        proc=launch(label,d,['--recover','--test-stop-records','80','--test-pause-at',point],policy=bp);wait_trace(label,'checkpoint_step',point,timeout=120);proc.kill();finish(proc,label,d,-9,killpoint=point);clean_lock(d)
        run(label+'-restore',d,bp,80,True)
    # Remote errors, ambiguity, corrupt/read loss, timeout and refusal at each object domain.
    failures=[('put-shard','PUT','shard-','500'),('head-shard','HEAD','shard-','500'),('get-shard','GET','shard-','corrupt'),('put-manifest','PUT','manifest.json','500'),('put-certificate','PUT','certificate.json','500'),('ambiguous-shard','PUT','shard-','drop-after'),('ambiguous-certificate','PUT','certificate.json','drop-after'),('redirect','PUT','shard-','redirect'),('oversize-error','PUT','shard-','oversize'),('timeout','PUT','shard-','delay')]
    for label,method,contains,action in failures:
        d=fork_config(frozen,label+'-roots');label='remote-'+label
        # Set fault only after pre-output restore so read failures challenge new copy verification.
        proc=launch(label,d,['--recover','--test-stop-records','80','--test-pause-at','policy_decided'],policy=bp);wait_trace(label,'checkpoint_step','policy_decided',timeout=120)
        # Pause hook is intentionally indefinite; use a new process with an armed
        # rule after restore, excluding old-generation object paths by matching
        # the next new generation via request sequencing in the proxy.
        proc.send_signal(signal.SIGINT);finish(proc,label,d,1,reason='cancelled');clean_lock(d)
        fault(dict(method=method,contains=contains,action=action,remaining=1,delay_ms=6000))
        run(label+'-injected',d,bp,80,True,expected=1,reason='remote operation')
        fault(None);require(selected(d)['certificate_blake3']==frozen_selected['certificate_blake3'],'incomplete remote cut selected')
        run(label+'-restore',d,bp,80,True)
    # Cancel during slow remote I/O; no source data polling and native control alive.
    d=fork_config(frozen,'slow-control-roots');proc=launch('slow-control',d,['--recover','--test-stop-records','80','--test-pause-at','remote_shard_put'],policy=bp)
    wait_trace('slow-control','checkpoint_step','remote_shard_put',timeout=120);time.sleep(5);proc.send_signal(signal.SIGINT);finish(proc,'slow-control',d,1,reason='cancelled');clean_lock(d)
    # Missing/invalid environment credentials and capability-probe errors are pre-output.
    for label,kind in [('missing-credentials','missing'),('wrong-credentials','wrong'),('capability-unavailable','probe')]:
        d=descriptor(stream,label+'-roots');d['job_id']=job;pp=policy(d,'both')
        saved_secret=os.environ['AWS_SECRET_ACCESS_KEY']
        if kind=='missing':del os.environ['AWS_SECRET_ACCESS_KEY']
        if kind=='wrong':os.environ['AWS_SECRET_ACCESS_KEY']=uuid.uuid4().hex
        if kind=='probe':fault(dict(method='PUT',contains='probe-',action='503',remaining=1))
        try:run(label,d,pp,240,expected=1,reason='credentials required' if kind=='missing' else 'remote operation')
        finally:os.environ['AWS_SECRET_ACCESS_KEY']=saved_secret;fault(None)
    # Finite local/remote resources refuse; no hidden copy degradation.
    for label,change in [('remote-objects',lambda p:p['limits'].update(remote_objects=40)),('requests',lambda p:p['limits'].update(requests=128)),('response-budget',lambda p:p['limits'].update(response_bytes=1048576)),('trace-budget',lambda p:p['limits'].update(trace_bytes=8192))]:
        d=descriptor(stream,label+'-roots');d['job_id']=job;pp=policy(d,'both');change(pp);run(label,d,pp,240,expected=1,reason=('remote operation','sink failure','resource quota','static/fallback violates hard constraints'))
    for label in ['checkpoint-byte-budget','cold-budget']:
        d=descriptor(stream,label+'-roots');d['job_id']=job;pp=policy(d)
        if label=='checkpoint-byte-budget':d['checkpoint_bytes']=262144;pp.update(min_records=10,max_records=10)
        else:d['cold_bytes']=128
        run(label,d,pp,240,expected=1,reason='retained plus staging quota' if label=='checkpoint-byte-budget' else 'cold disk quota')
    d=descriptor(stream,'generation-budget-roots');d['job_id']=job;d['max_generations']=2;pp=policy(d);run('generation-budget',d,pp,240,expected=1,reason='generation quota')
    # Remote-only clean restore can continue only with a fresh write namespace;
    # an explicit remote copy can rebuild a lost required local copy on its next full cut.
    produce(stream,[(i%2,event(1,3,0)) for i in range(40)])
    scratch_config=json.loads((root/'clean-scratch.config.json').read_text());scratch_policy=copy.deepcopy(remote_policy)
    reuse=fork_config(scratch_config,'disk-loss-reuse-roots');run('disk-loss-reuse-refused',reuse,scratch_policy,280,True,['--restore-copy','remote'],expected=1,reason='fresh remote namespace')
    scratch_policy['remote']['namespace']=str(uuid.uuid4());run('clean-scratch-resume',scratch_config,scratch_policy,280,True,['--restore-copy','remote'])
    lost=fork_config(seed,'lost-local-copy-roots')
    for path in pathlib.Path(lost['checkpoint_root']).rglob('shard-*.bin'):path.unlink()
    run('remote-rebuild-local-copy',lost,seed_policy,280,True,['--restore-copy','remote'])
    # Causally released legal interleavings exercise actual windows/closures and
    # same logical ID with conflicting payload fingerprints in the new v2 mode.
    for suffix,accept_late in [('a',True),('b',False)]:
        label='window-'+suffix;topic_name=topic(label,2);wc=descriptor(topic_name,label+'-roots',partitions=2);wc['job_id']=str(uuid.UUID(int=16));wp=policy(wc);wp.update(min_records=1,max_records=1)
        produce(topic_name,[(0,event(1,2,0)),(1,event(1,3,0))]);run(label+'-seed',wc,wp,2)
        proc=launch(label,wc,['--recover','--test-stop-records','5'],policy=wp);wait_trace(label,'admitted')
        produce(topic_name,[(0,event(9999,0,200))]);wait_total(label,3)
        if accept_late:
            produce(topic_name,[(0,event(1,7,50))]);wait_total(label,4)
            produce(topic_name,[(1,event(9999,0,200))])
        else:
            produce(topic_name,[(1,event(9999,0,200))]);wait_trace(label,'window_confirmed')
            produce(topic_name,[(0,event(1,7,50))])
        finish(proc,label,wc)
    # The selected local required copy restores independently during a real service outage.
    minio.terminate();minio.wait(10)
    d=fork_config(seed,'service-outage-roots');run('service-outage',d,seed_policy,240,True,['--restore-copy','remote'],expected=1,reason='remote operation')
    d=fork_config(seed,'outage-local-copy-roots');run('outage-local-copy',d,seed_policy,240,True,['--restore-copy','local'])
    # Expiring the replay origin must leave the verified 20/20 checkpoint usable.
    retention_topic=topic('retention',2);rc=descriptor(retention_topic,'retention-roots',partitions=2);rr=policy(rc);produce(retention_topic,[(i%2,event(1,1,0)) for i in range(40)]);run('retention-seed',rc,rr,40)
    retained=selected(rc);rr['recovery'].update(allow_replay=True,accept_duplicate_and_conflicting_outputs=True,replay_certificate_blake3=retained['certificate_blake3'],replay_estimated_ms=1,restore_estimated_ms=10000)
    fixture('retain',retention_topic,extra=[0,20]);d=fork_config(rc,'retention-expired-roots')
    run('retention-restore-after-expiry',d,rr,40,True,['--restore-copy','local'])
    # If the saved vector is also expired, both candidates must be refused before admission/output.
    fixture('retain',retention_topic,extra=[0,21]);d=fork_config(rc,'retention-both-lost-roots')
    run('retention-both-lost',d,rr,40,True,['--restore-copy','local'],expected=1,reason=('recovery unavailable','replay_ineligible_origin_retention','restore_ineligible_saved_offset_retention'))
    dump(root/'outcome.json',dict(status='CAMPAIGN_FINISHED',cases=len(cases),criteria='predeclared exact gates; no speedup threshold',binary_sha256=hashlib.sha256(pathlib.Path(binary).read_bytes()).hexdigest()))
finally:
    dump(root/'cases.json',cases);dump(root/'commands.json',commands);dump(root/'ledgers.json',ledgers)
    for proc in reversed(children):
        if proc.poll() is None:
            proc.terminate()
            try:proc.wait(5)
            except subprocess.TimeoutExpired:proc.kill();proc.wait()
