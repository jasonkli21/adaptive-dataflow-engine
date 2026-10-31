"""Fresh post-fix Phase16 live probes. Historical archives are never relabeled."""
import copy,json,pathlib,shutil,subprocess,time,uuid
from service_fixture import dump,private,require,rows

def event(key,value,time_ms):return dict(version=1,key=key,value_i64=value,event_time_ms=time_ms)
def native(s,topic,label):
    root=private(s.root/label);work=private(root/'work');check=private(root/'check')
    return dict(version=1,job_id=str(uuid.uuid4()),source=dict(version=1,bootstrap=s.address,expected_cluster_id=s.cluster,topic=topic,starts=[dict(partition=p,offset=0) for p in range(2)],security=dict(mode='loopback_dev'),key_less_than=1000,allowance_ms=10,payload_bytes=4096,client_queue_kib=16,sink_stall_ms=30000,timers=[]),topic_partitions=[0,1],operator='tumbling_sum_count_v1',window_ms=100,late_policy='receipt_drop_closed_v1',hash_version=1,owners=1,hot_bytes=128,cold_bytes=1048576,checkpoint_bytes=16*1048576,max_generations=64,checkpoint_every_records=40,checkpoint_every_ms=60000,working_root=str(work),checkpoint_root=str(check))
def policy(s,c,place='local'):
    return dict(version=1,mode='adaptive',min_records=40,max_records=40,min_ms=3600000,max_ms=3600000,max_depth=2,min_samples=2,cooldown_ms=0,hysteresis_nano=0,placements=[place],static_placement=place,
        constraints=dict(min_copies=1,require_remote=False,max_loss_ppm=1000000,local_loss_ppm=100000,remote_loss_ppm=100,both_loss_ppm=1,recovery_slo_ms=86400000,failure_ppm=1000,retention_ms=1000),prices=dict(local_write_byte=1,local_read_byte=0,remote_read_byte=0,remote_write_byte=5,remote_request=3,capacity_byte_ms=0,compute_ms=1,replay_record=1),
        limits=dict(object_bytes=1048576,remote_objects=8192,remote_write_bytes=268435456,requests=65536,response_bytes=68719476736,io_timeout_ms=5000,trace_bytes=67108864),remote=dict(endpoint=s.endpoint,bucket='ade-phase16',prefix='private17-prereq',region='us-east-1',namespace=str(uuid.uuid4()),dev_allow_http=True) if place!='local' else None,
        recovery=dict(original_starts=copy.deepcopy(c['source']['starts']),trusted_immutable_history=True,allow_replay=False,accept_duplicate_and_conflicting_outputs=False,replay_certificate_blake3=None,replay_estimated_ms=1,restore_estimated_ms=10000))
def fork(s,c,label):
    d=copy.deepcopy(c);root=private(s.root/label);private(root/'work');shutil.copytree(c['checkpoint_root'],root/'check');d.update(working_root=str(root/'work'),checkpoint_root=str(root/'check'));return d

def run(s,binary,race_binary,placement_binary):
    results=[]
    def execute(label,c,p,total,recover=False,expected=0):
        cfg=s.root/(label+'.config.json');pol=s.root/(label+'.policy.json');dump(cfg,c);dump(pol,p)
        out=s.call([str(binary),str(cfg),'--adaptive-policy',str(pol),*(['--recover'] if recover else []),'--test-stop-records',str(total)],label,env=s.env,expected=expected,timeout=90)
        trace=rows(s.root/(label+'.stderr'));results.append(dict(name=label,exit=expected,trace=label+'.stderr',config=cfg.name,policy=pol.name));dump(s.root/'prerequisite-cases.json',results);return trace,out
    topic=s.topic('retention');c=native(s,topic,'prereq-retention');p=policy(s,c)
    s.produce(topic,[(i%2,event(i,1,0)) for i in range(40)]);execute('p16-retention-seed',c,p,40)
    selected=json.loads((pathlib.Path(c['checkpoint_root'])/'CURRENT2').read_text());p['recovery'].update(allow_replay=True,accept_duplicate_and_conflicting_outputs=True,replay_certificate_blake3=selected['certificate_blake3'])
    s.produce(topic,[(0,event(1,2,0))]);s.admin('retain',topic,[0,20]);d=fork(s,c,'prereq-retained')
    trace,out=execute('p16-retained-saved-offsets',d,p,40,True);decision=next(r for r in trace if r['type']=='recovery_decision')
    require(decision['path']=='checkpoint_restore' and not decision['model']['sources']['replay']['eligible'] and decision['model']['sources']['restore']['eligible'],'expired-origin restore not executed');require(not out,'unchanged restore unexpectedly emitted receipts')
    d=fork(s,c,'prereq-race');cfg=s.root/'p16-race.config.json';pol=s.root/'p16-race.policy.json';dump(cfg,d);dump(pol,p)
    err=open(s.root/'p16-race.stderr','wb');proc=subprocess.Popen([str(race_binary),str(cfg),str(pol)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,env=s.env);s.children.append(proc);err.close()
    # Separate blocking pipe barrier, bounded by process/runner deadline.
    import selectors
    sel=selectors.DefaultSelector();sel.register(proc.stdout,selectors.EVENT_READ);require(sel.select(20),'preflight barrier timeout');line=proc.stdout.readline();require(line==b'PREFLIGHT_COMPLETE\n','preflight failed')
    s.admin('retain',topic,[0,21]);proc.stdin.write(b'x');proc.stdin.flush();out,_=proc.communicate(timeout=20);(s.root/'p16-race.stdout').write_bytes(line+out)
    require(proc.returncode==1 and b'offset range/retention/truncation' in (s.root/'p16-race.stderr').read_bytes(),'assignment race failed open');results.append(dict(name='p16-assignment-race',exit=1))
    d=fork(s,c,'prereq-both-expired');trace,out=execute('p16-both-vectors-expired',d,p,40,True,1);require(not out and any(r.get('type')=='failure' and 'recovery unavailable' in r.get('reason','') for r in trace),'both expired did not refuse before output')
    topic=s.topic('placement')
    s.produce(topic,[(i%2,event(i,1,0)) for i in range(80)]+[(i%2,event(1,2,0)) for i in range(160)])
    for mode in ['ample','veto']:
        c=native(s,topic,'prereq-placement-'+mode);p=policy(s,c,'both');p.update(min_records=80,max_records=80)
        cfg=s.root/('p16-placement-'+mode+'.config.json');pol=s.root/('p16-placement-'+mode+'.policy.json');dump(cfg,c);dump(pol,p)
        before=len(rows(s.root/'wire.jsonl'))
        out=s.call([str(placement_binary),str(cfg),str(pol),mode],'p16-placement-'+mode,env=s.env,timeout=120)
        trace=[json.loads(line) for line in out.splitlines()];decisions=[r['decision'] for r in trace if r['type']=='cut'];d=decisions[-1]
        require(d['observation']['samples']>=2,'missing real warmup measurements')
        require(d['chosen']['format']=='full' and d['chosen']['placement']==('remote' if mode=='ample' else 'local'),'effective full placement decision differs')
        require(all(v['rejection']=='self_contained_required' for v in d['candidates'] if v['action']['placement'] in ['local','remote'] and v['action']['format']=='incremental' and v['rejection'] not in ['capacity_floor']),'placement-ineligible incremental scored')
        if mode=='veto':
            require(any(v['action']['placement']=='remote' and v['action']['format']=='full' and v['rejection']=='capacity_floor' for v in d['candidates']),'missing full capacity veto')
            cuts=[r for r in trace if r['type']=='cut'];first_two={r['certificate']['manifest']['generation'] for r in cuts[:2]}
            wire=rows(s.root/'wire.jsonl')[before:]
            require(not any(r['method']=='PUT' and not any(g in r['path'] for g in first_two) and '/probe-' not in r['path'] for r in wire),'veto wrote third remote generation')
            # The reservation ledger itself must not grow across the final local-only cut.
            require(cuts[-1]['metrics']['remote']['put']==0 if 'remote' in cuts[-1]['metrics'] else True,'veto remote effects')
        results.append(dict(name='p16-placement-'+mode,exit=0,config=cfg.name,policy=pol.name))
    dump(s.root/'prerequisite-cases.json',results);return results
