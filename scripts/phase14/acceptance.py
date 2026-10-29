#!/usr/bin/env python3
"""Real isolated Kafka witness. No shared ADE oracle code. Keeps every raw artifact.
Requires an unpacked Apache Kafka 3.9.1 distribution and Java 17 (no Docker/cloud).
"""
import argparse, base64, hashlib, json, os, pathlib, signal, socket, subprocess, tarfile, tempfile, time, uuid
from evidence_contracts import build_descriptor, validate_backpressure, validate_case_contract

if not __debug__:
    raise SystemExit('Evidence checks require Python assertions enabled; do not use -O/PYTHONOPTIMIZE.')

P = argparse.ArgumentParser()
P.add_argument('--kafka-home', type=pathlib.Path, required=True)
P.add_argument('--java-home', type=pathlib.Path, required=True)
P.add_argument('--output', type=pathlib.Path, required=True)
P.add_argument('--binary', type=pathlib.Path, default=pathlib.Path('target/debug/ade-stream'))
P.add_argument('--stall-seconds', type=int, default=35)
a = P.parse_args()
assert a.stall_seconds >= 30
root = a.output.resolve(); root.mkdir(parents=True, exist_ok=False)
repo = pathlib.Path(__file__).resolve().parents[2]
java = str(a.java_home / 'bin/java'); javac = str(a.java_home / 'bin/javac')
cp = str(a.kafka_home.resolve() / 'libs/*')
binary = str(a.binary.resolve())
cluster = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=')
source_paths = sorted(set(subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],cwd=repo,text=True).splitlines()))
source_paths = [p for p in source_paths if p != 'sol-medium-phase-0-bootstrap-implementation-prompt.md' and not p.startswith('docs/evidence/')]
source_inventory = {p:hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in source_paths if (repo/p).is_file()}
(root/'source-inventory.json').write_text(json.dumps(source_inventory,indent=2,sort_keys=True))
with tarfile.open(root/'source.tar.gz','w:gz') as tar:
    for p in source_inventory: tar.add(repo/p,arcname=p)
(root/'environment.json').write_text(json.dumps({'parent_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
    'rustc':subprocess.check_output(['rustc','-Vv'],text=True),'cargo':subprocess.check_output(['cargo','-V'],text=True),
    'platform':os.uname(),'java_home':str(a.java_home),'kafka_home':str(a.kafka_home),'started_unix':time.time()},indent=2))


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0)); return s.getsockname()[1]
port, controller, tls_port = free_port(), free_port(), free_port()
address = f'127.0.0.1:{port}'
config = root / 'broker.properties'
config.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{controller}
listeners=PLAINTEXT://127.0.0.1:{port},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://127.0.0.1:{port}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs={root / 'broker-data'}
num.partitions=2
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
commands = []; cases = []; children = []; broker = None

def command(cmd, name, data=None, timeout=30):
    commands.append({'name':name, 'argv':cmd})
    result = subprocess.run(cmd, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    (root / (name+'.stdout')).write_bytes(result.stdout)
    (root / (name+'.stderr')).write_bytes(result.stderr)
    assert result.returncode == 0, (name,result.returncode,result.stderr[-2000:])
    return result.stdout

# Ephemeral fixture credentials/keys stay outside raw evidence and are never logged.
private_security = tempfile.TemporaryDirectory(prefix='ade14-security-')
security_root = pathlib.Path(private_security.name)
os.environ['ADE14_KEYSTORE_PASSWORD'] = uuid.uuid4().hex
os.environ['ADE14_SASL_USER'] = 'fixtureuser'
os.environ['ADE14_SASL_PASSWORD'] = uuid.uuid4().hex
os.environ['ADE14_WRONG_PASSWORD'] = uuid.uuid4().hex
os.environ['ADE_TEST_USER'] = 'fixture-user'
os.environ['ADE_TEST_PASSWORD'] = 'fixture-secret'
command(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=127.0.0.1',
    '-addext','subjectAltName=IP:127.0.0.1','-keyout',str(security_root/'key.pem'),'-out',str(security_root/'ca.pem')],'tls-cert')
command(['openssl','pkcs12','-export','-in',str(security_root/'ca.pem'),'-inkey',str(security_root/'key.pem'),
    '-out',str(security_root/'server.p12'),'-passout','env:ADE14_KEYSTORE_PASSWORD'],'tls-keystore')
text = config.read_text().replace(f'CONTROLLER://127.0.0.1:{controller}\n', f'CONTROLLER://127.0.0.1:{controller},SASL_SSL://127.0.0.1:{tls_port}\n',1)
text = text.replace(f'advertised.listeners=PLAINTEXT://127.0.0.1:{port}',f'advertised.listeners=PLAINTEXT://127.0.0.1:{port},SASL_SSL://127.0.0.1:{tls_port}')
text = text.replace('CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT','CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_SSL:SASL_SSL')
text += f"""ssl.keystore.type=PKCS12
ssl.keystore.location={security_root/'server.p12'}
ssl.keystore.password={os.environ['ADE14_KEYSTORE_PASSWORD']}
ssl.key.password={os.environ['ADE14_KEYSTORE_PASSWORD']}
ssl.client.auth=none
sasl.enabled.mechanisms=PLAIN
listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="fixtureuser" password="{os.environ['ADE14_SASL_PASSWORD']}" user_fixtureuser="{os.environ['ADE14_SASL_PASSWORD']}";
"""
# Save only redacted broker configuration as reproducible evidence.
redacted=text.replace(os.environ['ADE14_KEYSTORE_PASSWORD'],'<fixture-keystore-password>').replace(os.environ['ADE14_SASL_PASSWORD'],'<fixture-sasl-password>')
config.write_text(redacted)
config = security_root/'broker.properties'; config.write_text(text)

fixture_dir = root / 'fixture'; fixture_dir.mkdir()
command([javac,'-cp',cp,'-d',str(fixture_dir),str(repo/'scripts/phase14/Fixture.java')], 'javac')
fixture_cp = str(fixture_dir)+os.pathsep+cp

def fixture(mode,topic,data=None,extra=()):
    name = f'{len(commands):03}-{mode}-{topic}'
    return command([java,'-cp',fixture_cp,'Fixture',address,mode,topic,*map(str,extra)],name,data)

def start_broker(suffix):
    log = open(root / ('broker-'+suffix+'.log'),'wb')
    proc = subprocess.Popen([java,'-Xmx512m','-Xms256m','-cp',cp,'kafka.Kafka',str(config)],stdout=log,stderr=subprocess.STDOUT)
    log.close(); children.append(proc)
    limit=time.monotonic()+30
    while time.monotonic()<limit:
        if proc.poll() is not None: raise AssertionError('broker exited')
        try:
            with socket.create_connection(('127.0.0.1',port),timeout=.1): return proc
        except OSError: time.sleep(.1)
    raise AssertionError('broker startup timeout')

prefix='ade14-'+uuid.uuid4().hex[:8]
def topic(label):
    name=prefix+'-'+label;fixture('create',name);return name

def event(key,value,ts): return dict(version=1,key=key,value_i64=value,event_time_ms=ts)
def produce(topic,items,mode='produce'):
    data=''.join(str(p)+'\t'+(json.dumps(e,separators=(',',':')) if isinstance(e,dict) else e)+'\n' for p,e in items).encode()
    output=fixture(mode,topic,data)
    return [(int(p),int(off),json.loads(payload)) for p,off,payload in (line.split('\t',2) for line in output.decode().splitlines())] if mode=='produce' and all(isinstance(e,dict) for _,e in items) else []

def descriptor(topic,offsets=(0,0),*,explicit_starts=None,**changes):
    bootstrap=changes.pop('bootstrap',address)
    return build_descriptor(topic,bootstrap,cluster,offsets=offsets,explicit_starts=explicit_starts,**changes)

def launch(label,c,args=(),pipe=False):
    path=root/(label+'.config.json');path.write_text(json.dumps(c,sort_keys=True))
    out=None if pipe else open(root/(label+'.output.jsonl'),'wb')
    err=open(root/(label+'.trace.jsonl'),'wb')
    cmd=[binary,str(path),*args];commands.append({'name':label,'argv':cmd})
    p=subprocess.Popen(cmd,stdout=subprocess.PIPE if pipe else out,stderr=err)
    if out:out.close()
    err.close();children.append(p);return p

def traces(label):
    path=root/(label+'.trace.jsonl')
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
def terminal(label):return next(t for t in reversed(traces(label)) if t['type']=='terminal')
def wait_trace(label,kind,timeout=10):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if any(t['type']==kind for t in traces(label)):return
        time.sleep(.05)
    raise AssertionError((label,'missing trace',kind))
def wait(p,label,expected=0,timeout=30):
    rc=p.wait(timeout);wanted=(-signal.SIGKILL if label=='kill' else expected)
    assert rc==wanted,(label,rc,traces(label)[-3:])
    cases.append({'case':label,'status':'PASS','pid':p.pid,'exit':rc})

def oracle(label,ledger,starts=(0,0)):
    rows=[json.loads(line) for line in (root/(label+'.output.jsonl')).read_text().splitlines()]
    expected={(p,off):e for p,off,e in ledger if off>=starts[p]}
    assert len(rows)==len(expected),(label,len(rows),len(expected))
    seen=set();last={};maxima={};watermarks={};forwarded=[]
    for r in rows:
        k=(r['partition'],r['offset']);assert k in expected and k not in seen
        seen.add(k);p,off=k;e=expected[k]
        assert off>last.get(p,-1);last[p]=off
        assert r['event_time_ms']==e['event_time_ms']
        assert r['late']==(p in watermarks and e['event_time_ms']<watermarks[p])
        want={k:e[k] for k in ('value_i64','key','event_time_ms')} if e['key']<10 else None
        assert r['projected']==want,(r,want)
        maxima[p]=max(maxima.get(p,e['event_time_ms']),e['event_time_ms']);watermarks[p]=maxima[p]-10
        forwarded.append((dict(last),dict(watermarks)))
    progress=[t['progress'] for t in traces(label) if t['type']=='forwarded']
    assert len(progress)==len(rows)
    for pr,(last,wm) in zip(progress,forwarded):
        for p,off in last.items():
            cursor=pr['partitions'][str(p)];assert cursor['forwarded']==off and cursor['watermark']==wm[p]
        assert pr['frontier']==(min(wm.values()) if len(wm)==2 else None)
    term=terminal(label);assert term['progress']['forwarded_records']==len(rows)
    assert term['metrics']['in_flight_peak_records']<=1 and term['metrics']['sink_frame_peak_bytes']<=2048
    assert term['decoded_peak_bytes']<=4096 and term['arrow_peak_bytes']<=8192
    current_frontier=None
    for trace in traces(label):
        if 'progress' in trace: current_frontier=trace['progress']['frontier']
        if trace['type']=='timer' and trace['timer']['kind']=='processing':
            assert trace['processing_ms']>=trace['timer']['deadline_ms']
        if trace['type']=='timer' and trace['timer']['kind']=='event':
            assert current_frontier is not None and current_frontier>=trace['timer']['deadline_ms']
    ids=[t['timer']['id'] for t in traces(label) if t['type']=='timer'];assert len(ids)==len(set(ids))
    assert ids[:2]==[0,1]
    return rows

try:
    command([java,'-cp',cp,'kafka.tools.StorageTool','format','--cluster-id',cluster,'--config',str(config)],'format')
    broker=start_broker('initial')
    # Wait for metadata readiness, not merely TCP bind.
    time.sleep(2)
    assert fixture('cluster','unused').decode().strip()==cluster
    native=topic('native')
    values=[(0,event(0,-5,-100)),(1,event(5,0,-50)),(0,event(9,2,100)),(1,event(11,-7,70)),(0,event(1,-2,20)),(1,event(2,9,500)),(0,event(10,0,700)),(1,event(3,-9,30))]
    ledger=produce(native,values)
    p=launch('native',descriptor(native),['--test-stop-records',str(len(ledger))]);wait(p,'native');oracle('native',ledger)
    p=launch('middle-replay',descriptor(native,(2,1)),['--test-stop-records','5']);wait(p,'middle-replay');oracle('middle-replay',ledger,(2,1))
    secure = dict(mode='sasl_ssl',username_env='ADE14_SASL_USER',password_env='ADE14_SASL_PASSWORD',ca_path=str(security_root/'ca.pem'))
    p=launch('sasl-ssl',descriptor(native,bootstrap=f'127.0.0.1:{tls_port}',security=secure),['--test-stop-records','8']);wait(p,'sasl-ssl');oracle('sasl-ssl',ledger)
    wrong=dict(secure,password_env='ADE14_WRONG_PASSWORD')
    p=launch('bad-credentials',descriptor(native,bootstrap=f'127.0.0.1:{tls_port}',security=wrong),['--test-stop-ms','3000']);wait(p,'bad-credentials',expected=1)
    assert not (root/'bad-credentials.output.jsonl').read_bytes()

    gap=topic('gap');g=produce(gap,[(0,event(1,2,100)),(1,event(2,-3,200))])
    produce(gap,[(0,event(99,999,999)),(1,event(99,999,999))],mode='abort')
    g+=produce(gap,[(0,event(3,-2,400)),(1,event(4,5,300))])
    assert g[2][1]>g[0][1]+1
    p=launch('legal-gap',descriptor(gap),['--test-stop-records','4']);wait(p,'legal-gap');oracle('legal-gap',g)
    quiet=topic('quiet');q=produce(quiet,[(0,event(1,-1,100))])
    p=launch('quiet',descriptor(quiet),['--test-stop-ms','2500']);wait(p,'quiet');oracle('quiet',q)
    t=terminal('quiet');assert t['progress']['frontier'] is None and t['metrics']['pending_polls']>=10 and t['metrics']['polls']<50
    assert all(t['timer']['kind']!='event' for t in traces('quiet') if t['type']=='timer')
    assert any(t['timer']['id']==3 and t['processing_ms']>=1000 for t in traces('quiet') if t['type']=='timer')
    # A genuine output pipe stalls. Independent producer increases broker backlog
    # while no source demands data; regular bounded traces witness the plateau.
    pressure=topic('pressure');bulk=[(i%2,event(i%13,(i%17)-8,1000+(i%23)*10)) for i in range(1400)]
    bulk_ledger=produce(pressure,bulk)
    p=launch('backpressure',descriptor(pressure),['--test-stop-records','1600'],pipe=True)
    wait_trace('backpressure','sink_blocked')
    before=traces('backpressure')[-1]['progress'];began=time.monotonic()
    extra=produce(pressure,[(i%2,event(i%12,-i,500)) for i in range(200)]);bulk_ledger+=extra
    while time.monotonic()-began<a.stall_seconds:
        assert p.poll() is None
        time.sleep(.2)
    blocks=[t for t in traces('backpressure') if t['type']=='sink_blocked']
    assert len(blocks)>=25
    assert all(t['progress']==before for t in blocks)
    assert all(t['source']['backlog']['0']>500 and t['source']['backlog']['1']>500 for t in blocks[-5:])
    peaks=[t['source']['queue_bytes_peak'] for t in blocks[-10:]];assert max(peaks)==min(peaks)
    assert max(peaks)<=4*(16*1024+65536)
    out,_=p.communicate(timeout=40);(root/'backpressure.output.jsonl').write_bytes(out)
    assert p.returncode==0;cases.append({'case':'backpressure','status':'PASS','pid':p.pid,'exit':0,'blocked_seconds':time.monotonic()-began,'plateau_bytes':max(peaks)})
    oracle('backpressure',bulk_ledger)
    validate_backpressure(cases[-1],traces('backpressure'),out)
    # Cancellation during an idle poll and a blocked write; sink EPIPE.
    for label,conf,blocked in [('cancel-poll',descriptor(quiet,(1,0)),False),('cancel-stall',descriptor(pressure),True),('epipe',descriptor(pressure),True),('kill',descriptor(pressure),True)]:
        p=launch(label,conf,pipe=blocked)
        wait_trace(label,'sink_blocked' if blocked else 'admitted')
        if label=='epipe':p.stdout.close()
        elif label=='kill':p.kill()
        else:p.send_signal(signal.SIGINT)
        began=time.monotonic();wait(p,label,expected=1,timeout=5)
        assert time.monotonic()-began<3
        if blocked and label!='epipe':
            output=p.stdout.read();(root/(label+'.output.jsonl')).write_bytes(output);p.stdout.close()
        if label!='kill':assert terminal(label)['status']=='FAILED'
        else:assert all(t['type']!='terminal' for t in traces(label))
    # Hard death followed by explicit replay includes known duplicates, no recovery claim.
    p=launch('kill-replay',descriptor(pressure),['--test-stop-records','1600']);wait(p,'kill-replay');rows=oracle('kill-replay',bulk_ledger)
    killed=[json.loads(l) for l in (root/'kill.output.jsonl').read_text().splitlines()]
    assert killed and {(r['partition'],r['offset']) for r in killed}<={(r['partition'],r['offset']) for r in rows}
    # Fail-closed schema/oversize/underflow; a poison first message never gets forwarded.
    for label,payload in [('bad-schema','{"version":2,"key":0,"value_i64":1,"event_time_ms":1}'),('oversize','x'*5000),('underflow',event(0,1,-9223372036854775808))]:
        name=topic(label);produce(name,[(0,payload)])
        p=launch(label,descriptor(name),['--test-stop-ms','5000']);wait(p,label,expected=1)
        t=terminal(label);assert t['progress']['partitions']['0']['seen']==0 and t['progress']['partitions']['0']['forwarded'] is None
        assert not (root/(label+'.output.jsonl')).read_bytes()
    fifo=root/'config.fifo';os.mkfifo(fifo)
    proc=subprocess.Popen([binary,str(fifo)],stdout=subprocess.PIPE,stderr=subprocess.PIPE);children.append(proc)
    output,diagnostic=proc.communicate(timeout=2)
    (root/'config-fifo.stdout').write_bytes(output);(root/'config-fifo.stderr').write_bytes(diagnostic)
    assert proc.returncode==1 and not output
    assert json.loads(diagnostic)['failure_class']=='admission' and b'regular file' in diagnostic
    cases.append({'case':'config-fifo','status':'PASS','pid':proc.pid,'exit':1})
    fifo.unlink()
    strict=descriptor(native);strict['security']['unexpected']=1
    strict_path=root/'nested-unknown.config.json';strict_path.write_text(json.dumps(strict))
    proc=subprocess.Popen([binary,str(strict_path)],stdout=subprocess.PIPE,stderr=subprocess.PIPE);children.append(proc)
    output,diagnostic=proc.communicate(timeout=2)
    (root/'nested-unknown.stdout').write_bytes(output);(root/'nested-unknown.stderr').write_bytes(diagnostic)
    assert proc.returncode==1 and not output
    assert json.loads(diagnostic)['failure_class']=='admission' and b'strict config JSON' in diagnostic
    cases.append({'case':'nested-unknown','status':'PASS','pid':proc.pid,'exit':1})

    # Admission and metadata faults before output, including no implicit topic creation.
    for label,c in [('out-of-range',descriptor(native,(9999,0))),('missing-topic',descriptor(prefix+'-absent')),('missing-partition',descriptor(native,explicit_starts=[dict(partition=3,offset=0)])),('wrong-cluster',descriptor(native,expected_cluster_id='wrong')),('remote-plaintext',descriptor(native,bootstrap='192.0.2.1:9092')),('timer-cap',descriptor(native,timers=[dict(kind='event',deadline_ms=0)]*65)),('tls-refusal',descriptor(native,security=dict(mode='sasl_ssl',username_env='ADE_TEST_USER',password_env='ADE_TEST_PASSWORD',ca_path='/does/not/exist')))]:
        p=launch(label,c,['--test-stop-ms','2000']);wait(p,label,expected=1)
        assert not (root/(label+'.output.jsonl')).read_bytes()
        assert 'fixture-secret' not in (root/(label+'.trace.jsonl')).read_text()
    unsafe=prefix+'-unsafe';fixture('unsafe',unsafe)
    p=launch('compression-refusal',descriptor(unsafe),['--test-stop-ms','2000']);wait(p,'compression-refusal',expected=1)
    assert not (root/'compression-refusal.output.jsonl').read_bytes()
    # Retention drift after admission, and partition-set changes.
    for label,action in [('retention-drift','retain'),('partition-drift','expand'),('compression-drift','compression-drift')]:
        drift=topic(label);produce(drift,bulk)
        p=launch(label,descriptor(drift),pipe=True);wait_trace(label,'sink_blocked')
        if action=='retain':fixture('retain',drift,extra=(0,700))
        else:fixture(action,drift)
        wait(p,label,expected=1,timeout=6);p.stdout.close();assert terminal(label)['status']=='FAILED'
    # Actual service outage: fail the active reader, restart service, explicit reader restart.
    p=launch('broker-outage',descriptor(quiet,(1,0)))
    wait_trace('broker-outage','admitted');broker.terminate();broker.wait(15)
    wait(p,'broker-outage',expected=1,timeout=6)
    broker=start_broker('restart');time.sleep(2)
    p=launch('broker-restart-explicit',descriptor(native),['--test-stop-records','8']);wait(p,'broker-restart-explicit');oracle('broker-restart-explicit',ledger)

    # Recheck every negative case from its captured files, independent of the
    # inline scenario assertions above. Pipe failures may truthfully retain a
    # prefix; pre-admission and first-poison cases must have empty output.
    negative_labels=['bad-credentials','cancel-poll','cancel-stall','epipe','kill','bad-schema','oversize','underflow',
        'config-fifo','nested-unknown','out-of-range','missing-topic','missing-partition','wrong-cluster','remote-plaintext',
        'timer-cap','tls-refusal','compression-refusal','retention-drift','partition-drift','compression-drift','broker-outage']
    case_by_label={case['case']:case for case in cases}
    for label in negative_labels:
        config_path=root/(label+'.config.json')
        config=json.loads(config_path.read_text()) if config_path.exists() else None
        trace_path=root/(label+'.trace.jsonl')
        if trace_path.exists():
            trace_raw=trace_path.read_bytes()
        elif label in ('config-fifo','nested-unknown'):
            trace_raw=(root/(label+'.stderr')).read_bytes()
        else:
            trace_raw=None
        output_path=root/(label+'.output.jsonl')
        if output_path.exists():
            output=output_path.read_bytes()
        elif label in ('config-fifo','nested-unknown'):
            output=(root/(label+'.stdout')).read_bytes()
        else:
            output=None
        validate_case_contract(label,case_by_label[label],config,trace_raw,output)

    source_inventory_bytes=(root/'source-inventory.json').read_bytes()
    source_archive_bytes=(root/'source.tar.gz').read_bytes()
    result={'status':'PASS','cases':cases,'cluster_id':cluster,'broker':'Apache Kafka 3.9.1','client':'rdkafka 0.39.0 / librdkafka 2.12.1','seed':'deterministic index formulas v1','commands':commands,'binary_sha256':hashlib.sha256(pathlib.Path(binary).read_bytes()).hexdigest(),
        'source_inventory_sha256':hashlib.sha256(source_inventory_bytes).hexdigest(),'source_archive_sha256':hashlib.sha256(source_archive_bytes).hexdigest()}
    (root/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps({'status':'PASS','cases':len(cases),'root':str(root)}),flush=True)
finally:
    for p in children:
        if p.poll() is None:
            p.terminate()
            try:p.wait(10)
            except subprocess.TimeoutExpired:p.kill();p.wait()
    for path in root.rglob('*'):
        if path.is_file() and 'broker-data' not in path.parts and path.suffix in ('.log','.json','.jsonl','.stdout','.stderr','.properties'):
            raw=path.read_bytes()
            assert all(os.environ[name].encode() not in raw for name in ('ADE14_KEYSTORE_PASSWORD','ADE14_SASL_PASSWORD','ADE14_WRONG_PASSWORD')), 'fixture credential exposure'
    private_security.cleanup()
    (root/'commands.json').write_text(json.dumps(commands,indent=2))
    inventory={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file() and 'broker-data' not in p.parts and p.name!='inventory.json'}
    (root/'inventory.json').write_text(json.dumps(inventory,indent=2,sort_keys=True))
