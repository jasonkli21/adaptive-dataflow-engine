#!/usr/bin/env python3
"""Identity/lease/terminal correctness first; frozen local timing guards second."""
import hashlib,importlib.util,json,os,pathlib,statistics,subprocess,sys,uuid

def check_record(record,run,manifest,slot,warmup,pids,worker_bin,dataset_root):
    n=int(run['engine']['configuration']['workers']);config=run['engine']['configuration'];leased=config['scheduler_policy']!='static'
    assert record['sidecar_version']==(3 if leased else 2)
    assert record['run_id']==run['run_id'] and record['iteration_index']==slot and record['warmup']==warmup
    assert record['effective_config']==config,'effective config binding'
    pid=record['process_id'];assert isinstance(pid,int) and pid>0 and pid not in pids;pids.add(pid)
    source=record['source'];assert source['version']==1 and source['replayable']==(not leased)
    assert source['manifest_fingerprint']==run['dataset_manifest_fingerprint']
    assert source['schema_fingerprint']==manifest['schema_fingerprint'] and source['logical_fingerprint']==manifest['logical_fingerprint']
    expected=[dict(id=i,path=str((dataset_root/f['relative_path']).resolve()),fingerprint=f['fingerprint'],bytes=f['bytes']) for i,f in enumerate(manifest['files'])]
    assert source['morsels']==expected,'manifest-derived source inventory'
    m=record['metrics'];assert m['worker_count']==n and m['exchange_mode']=='spill' and m['topology'] is None and not m['inactive'] and m['recovery']['recovery_attempts']==0
    assert set(m['producers'])==set(m['finals'])==set(map(str,range(n)))
    assert m['gather']['transfers']==n
    for field in ['transfers','rows','payload_bytes','batches']:
        sent=sum(p['sent'][field] for p in m['producers'].values());received=sum(f['received'][field] for f in m['finals'].values());assert sent==received,field
        if field=='transfers':assert sent==n*n
    assert sum(p['input_rows'] for p in m['producers'].values())==manifest['row_count']
    for role in ['producers','finals']:
        for wid,task in m[role].items():
            assert task['managed_peak']<=int(config['memory_limit_bytes'])
            if role=='producers':
                assert task['sent']['transfers']==n and len(task['manifests'])==n
                for dest,terminal in task['manifests'].items():
                    assert terminal['logical']==dict(job=0,exchange=0,partition=int(dest),producer=int(wid))
                    assert terminal['attempt']==0 and len(terminal['fingerprint'])==64
                    for field,key in [('segments','batches'),('rows','rows'),('payload_bytes','payload_bytes')]:assert terminal[field]==task['destinations'][dest][key]
            else:
                assert task['executed_final'] and task['received']['transfers']==n
                assert task['retention']['current_buffer_bytes']==task['retention']['current_spill_backlog_bytes']==0
    if not leased:
        assert m['scheduler'] is None
        for w,p in m['producers'].items():assert p['source_files']==len(expected[int(w)::n])
        return
    s=m['scheduler'];assert s['source']==source
    uuid.UUID(s['fence']['execution']);assert s['fence']['job']==0 and s['fence']['epoch']==1
    assert s['config']['policy']==config['scheduler_policy']
    assert s['task']['scheduler']==s['config'] and s['task']['partial']['mode']=='fixed_hash' and s['task']['topology']['policy']=='static'
    for key in ['memory_limit_bytes','batch_size','prefetch_batches']:assert str(s['task'][key])==config[key]
    for key,value in s['config'].items():
        if key=='recovery_enabled':assert not value
        else:
            declared=config['scheduler_'+key]
            if isinstance(value,str):assert value==declared
            else:assert value==json.loads(declared)
    subprocess.run([str(worker_bin),'--verify-scheduler'],input=json.dumps(s).encode(),check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    for wid,p in m['producers'].items():
        entries=[e for e in s['entries'] if e['lease']['worker']==int(wid)]
        assert p['source_files']==len(entries)
        for field in ['input_rows','output_rows']:assert p[field]==sum(e['completion'][field] for e in entries)
        assert p['sent']['rows']==p['output_rows']
        assert p['production']['source_bytes']==sum(e['morsel']['bytes'] for e in entries)
    if config['scheduler_policy']=='leased_no_steal':assert all(e['preferred']==e['lease']['worker'] for e in s['entries'])
    assert sum(e['completion']['input_rows'] for e in s['entries'])==manifest['row_count']

def verify(root):
    root=pathlib.Path(root);load=lambda f:json.loads((root/f).read_text());identity=load('source-identity.json')
    spec=importlib.util.spec_from_file_location('phase9verify',pathlib.Path(__file__).with_name('phase9-verify.py'));legacy=importlib.util.module_from_spec(spec);spec.loader.exec_module(legacy)
    legacy.check_source_identity(root,identity['source_commit'],identity['runtime_tree'])
    binary=pathlib.Path(os.environ.get('ADE_BENCH_BIN','target/release/ade-bench')).resolve();worker=binary.with_name('ade-worker')
    assert load('binary-sha256.json')=={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [binary,worker]},'binary identity'
    statuses=load('statuses.json');inventory=load('inventory.json');pids=set();summaries={};environment=None;datasets={}
    expected={f'{r}-{n}-{m}' for n in [2,4] for r,m in [('imbalanced','static'),('imbalanced','leased_no_steal'),('imbalanced','leased_v1'),('uniform','static'),('uniform','leased_v1')]}|{'capacity-unweighted','capacity-weighted','participation'}
    assert {i['stem'] for i in inventory}==expected and len(inventory)==13
    assert set(statuses)==expected|{'negative-exit','negative-expiry','negative-notice'}
    for item in inventory:
        stem=item['stem'];assert statuses[stem]==0,stem
        run=load(stem+'-result.json');assert run['schema_version']==1 and run['valid']
        subprocess.run([str(binary),'check-result',str(root/(stem+'-result.json'))],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        assert run['repository']==dict(git_commit=identity['source_commit'],git_dirty=False)
        assert run['build']['profile']=='release' and not run['build']['debug_assertions']
        current=[run['build'],run['environment']]
        if environment is None:environment=current
        assert current==environment
        manifest=load(item['regime']+'-manifest-stdout.log');assert run['dataset']==manifest
        expected_weights=[15,1,1,1]*8 if item['regime']=='imbalanced' else []
        assert manifest['synthetic'].get('file_row_weights',[])==expected_weights
        assert manifest['row_count']==4000000 and manifest['synthetic']['files']==32 and manifest['synthetic']['seed']==42 and manifest['synthetic']['key_cardinality']==1024 and manifest['synthetic']['distribution']=='uniform'
        previous=datasets.setdefault(item['regime'],run['dataset_manifest_fingerprint']);assert previous==run['dataset_manifest_fingerprint']
        oracle=load(item['regime']+'-oracle-stdout.log');assert oracle['validated']
        c=run['engine']['configuration'];assert run['engine']['name']=='ade-distributed' and c['workers']==str(item['workers']) and c['scheduler_policy']==item['policy']
        assert c['control_protocol']=='ADETCP12' and c['hash_version']=='1' and c['exchange_wire_version']=='2'
        assert c['aggregate_policy']=='fixed_hash' and c['recovery_enabled']=='false' and c['partition_policy']=='static' and c['exchange_mode']=='spill' and c['exchange_buffer_bytes']=='1048576' and c['memory_limit_bytes']=='33554432' and c['batch_size']=='8192' and c['prefetch_batches']=='0'
        # The command inventory must bind each effective override, not just a label.
        for opt in item['options']:
            key,value=opt.split('=',1)
            if key=='scheduler_capacities':assert json.loads(c[key])==list(map(int,value.split(',')))
            elif key=='scheduler_participation':assert json.loads(c[key])==json.loads(value)
            else:assert c[key]==value
        count=5 if item['workers']==2 else 3
        assert run['run_config']['warmups']==1 and run['run_config']['iterations']==count and run['run_config']['dataset_verification']=='full'
        assert len(run['warmups'])==1 and len(run['iterations'])==count
        records=[json.loads(l) for l in (root/(stem+'-metrics.jsonl')).read_text().splitlines()];iters=run['warmups']+run['iterations'];assert len(records)==len(iters)==count+1
        command=load(stem+'-command.json');dataset_root=pathlib.Path(command[command.index('--dataset')+1])
        samples=[]
        for index,(it,record) in enumerate(zip(iters,records)):
            assert it['warmup']==(index==0) and it['exit_code']==0 and not it['error']
            out=it['outcome'];assert out['validated'] and out['engine']==run['engine'] and out['result_fingerprint']==oracle['result_fingerprint'] and out['rows_output']==oracle['rows_output']
            check_record(record,run,manifest,it['index'],index==0,pids,worker,dataset_root)
            m=record['metrics'];assert m['gather']['rows']==out['rows_output']
            s=m['scheduler']
            if stem.startswith('imbalanced-') and item['policy']=='leased_v1':assert any(e['preferred']!=e['lease']['worker'] for e in s['entries'])
            if stem=='participation':
                assert m['producers']['3']['source_files']==0 and m['producers']['2']['source_files']>0
                notice=s['events'][1];assert notice['in_flight'] is not None and notice['drained_ms'] is not None
                assert all(e['lease']['worker']!=0 or e['granted_sequence']<=notice['sequence'] for e in s['entries'])
            if index:
                samples.append(dict(query_ns=out['query_time_ns'],max_producer_completion_ns=max(t['coordinator_finished_ns'] for t in m['producers'].values()),producer_files=[m['producers'][str(w)]['source_files'] for w in range(item['workers'])],producer_bytes=[m['producers'][str(w)]['production']['source_bytes'] for w in range(item['workers'])],sent_rows=sum(t['sent']['rows'] for t in m['producers'].values()),sent_bytes=sum(t['sent']['payload_bytes'] for t in m['producers'].values()),control_messages=s['control_messages'] if s else 0,control_bytes=s['control_bytes'] if s else 0,renewals=s['renewals'] if s else 0,rss_bytes=it['peak_rss_bytes']))
        summaries[stem]=dict(samples=samples,median_query_ns=statistics.median(s['query_ns'] for s in samples),median_max_producer_completion_ns=statistics.median(s['max_producer_completion_ns'] for s in samples),query_range_ns=[min(s['query_ns'] for s in samples),max(s['query_ns'] for s in samples)])
    for stem in ['negative-exit','negative-expiry','negative-notice']:
        assert statuses[stem]!=0 and (root/(stem+'-stderr.log')).read_text().strip()
        stdout=(root/(stem+'-stdout.log')).read_text().strip()
        if stdout:
            try:assert not json.loads(stdout).get('validated',False)
            except json.JSONDecodeError:pass
    assert len(pids)==62
    gates={}
    for n in [2,4]:
        for label,regime,key,limit in [('imbalanced_job','imbalanced','median_query_ns',.90),('imbalanced_tail','imbalanced','median_max_producer_completion_ns',.90),('uniform_job','uniform','median_query_ns',1.15)]:
            ratio=summaries[f'{regime}-{n}-leased_v1'][key]/summaries[f'{regime}-{n}-static'][key];gates[f'{label}-{n}']=dict(ratio=ratio,limit=limit,passed=ratio<=limit)
    return dict(correctness_passed=True,passed=all(g['passed'] for g in gates.values()),children=62,measured=49,gates=gates,summaries=summaries,source=identity)
if __name__=='__main__':
    root=pathlib.Path(sys.argv[1])
    try:result=verify(root)
    except Exception as error:result=dict(correctness_passed=False,passed=False,error=f'{type(error).__name__}: {error}')
    (root/'verification.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='summaries'},indent=2));sys.exit(0 if result['passed'] else 1)
