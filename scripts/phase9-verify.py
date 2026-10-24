#!/usr/bin/env python3
"""V1 exact inventory, oracle, source/strategy/traffic and all-sample timing checks."""
import json,pathlib,statistics,subprocess,os,sys

def verify(root):
    load=lambda name:json.loads((root/name).read_text())
    commit=(root/'source-commit.txt').read_text().strip()
    runtime=(root/'runtime-tree.txt').read_text().strip()
    arms={2:[('tiny',s) for s in ['forced_repartition','forced_broadcast','observed_v1']]+[('balanced',s) for s in ['forced_repartition','observed_v1']]+[('pressure',s) for s in ['auto','grace']]+[(r,s) for r in ['low','high'] for s in ['fixed_hash','raw_partial','adaptive_v1']],4:[('tiny',s) for s in ['forced_repartition','forced_broadcast','observed_v1']]+[('balanced',s) for s in ['forced_repartition','observed_v1']]+[('pressure','auto')]+[(r,s) for r in ['low','high'] for s in ['fixed_hash','adaptive_v1']]}
    statuses=load('statuses.json');summaries={};pids=set();identity=None;datasets={};children=0;measured=0
    expected={f'{r}-{n}-{m}' for n,cases in arms.items() for r,m in cases}|{'negative-balanced-forced_broadcast','negative-pressure-hash_only','negative-hot-auto'}
    assert set(statuses)==expected,'exact arm inventory'
    for n,cases in arms.items():
        for regime,mode in cases:
            stem=f'{regime}-{n}-{mode}';assert statuses[stem]==0,stem
            subprocess.run([os.environ.get('ADE_BENCH_BIN','target/release/ade-bench'),'check-result',str(root/(stem+'-result.json'))],check=True,stdout=subprocess.DEVNULL)
            run=load(stem+'-result.json');assert run['schema_version']==1 and run['valid'],stem
            assert run['repository']==dict(git_commit=commit,git_dirty=False),stem
            assert run['build']['profile']=='release' and not run['build']['debug_assertions'],stem
            current=[run[k] for k in ['repository','build','environment']]
            if identity is None:identity=current
            assert current==identity,stem
            manifest=load(regime+'-manifest-stdout.log');assert run['dataset']==manifest,stem
            dataset_identity=run['dataset_manifest_fingerprint']
            if regime not in datasets:datasets[regime]=dataset_identity
            assert datasets[regime]==dataset_identity
            oracle=load(regime+'-oracle-stdout.log');assert oracle['validated'] and oracle['schema_version']==1
            c=run['engine']['configuration'];assert c['workers']==str(n) and c['control_protocol']=='ADETCP11' and c['exchange_wire_version']=='2' and c['hash_version']=='1'
            assert c['exchange_mode']=='spill' and c['recovery_enabled']=='false'
            agg=regime in ['low','high'];assert run['engine']['name']==('ade-distributed' if agg else 'ade-distributed-join')
            if agg:
                assert c['aggregate_policy']==mode and c['batch_size']=='1024' and c['memory_limit_bytes']=='33554432' and c['exchange_buffer_bytes']=='1048576' and c['prefetch_batches']=='0' and c['partition_policy']=='static'
                assert manifest['synthetic']['seed']==42 and manifest['synthetic']['files']==4 and manifest['row_count']==100000 and manifest['synthetic']['key_cardinality']==(16 if regime=='low' else 100000)
            else:
                sizes={'tiny':(65536,128,65536),'balanced':(65536,65536,1048576),'pressure':(8192,8192,8192)}[regime]
                j=manifest['join'];assert (j['left_rows'],j['right_rows'],j['left_key_domain'])==sizes and j['right_key_domain']==sizes[2] and j['seed']==42 and j['files_per_side']==4 and j['row_group_rows']==4096
                assert c['policy']==(mode if regime in ['tiny','balanced'] else 'forced_repartition') and c['build']=='null'
                local=json.loads(c['local']);assert local['memory_limit_bytes']==(262144 if regime=='pressure' else 33554432) and local['hash_rows']==(512 if regime=='pressure' else 65536)
                assert local['algorithm']==(mode if regime=='pressure' else 'auto')
                subprocess.run([os.environ.get('ADE_BENCH_BIN','target/release/ade-bench'),'__check-join-metrics',str(root/(stem+'-metrics.jsonl'))],check=True,stdout=subprocess.DEVNULL)
            k=5 if n==2 else 3
            assert run['run_config']['warmups']==1 and run['run_config']['iterations']==k and run['run_config']['dataset_verification']=='full'
            assert len(run['warmups'])==1 and len(run['iterations'])==k and run['summary']['count']==k
            records=[json.loads(l) for l in (root/(stem+'-metrics.jsonl')).read_text().splitlines()]
            iters=run['warmups']+run['iterations'];assert len(records)==len(iters)==k+1
            samples=[]
            for i,(it,record) in enumerate(zip(iters,records)):
                pid=record['process_id'];assert pid not in pids;pids.add(pid)
                assert it['warmup']==(i==0) and it['exit_code']==0 and not it['error'] and it['outcome']['validated']
                out=it['outcome'];assert out['engine']==run['engine'] and out['result_fingerprint']==oracle['result_fingerprint'] and out['rows_output']==oracle['rows_output']
                m=record['metrics'];assert m['gather']['rows']==out['rows_output'] and m['gather']['transfers']==n
                assert set(m['producers'])==set(m['finals'])=={str(p) for p in range(n)}
                if agg:
                    assert not m['topology'] and not m['inactive'] and m['recovery']['recovery_attempts']==0
                    sent=sum(p['sent']['rows'] for p in m['producers'].values());received=sum(f['received']['rows'] for f in m['finals'].values());assert sent==received
                    assert sum(p['input_rows'] for p in m['producers'].values())==100000
                    for p in m['producers'].values():
                        policy=p['partial']
                        if mode=='fixed_hash':assert policy is None
                        else:
                            assert policy['policy']==mode
                            if mode=='adaptive_v1':assert policy['hash_to_raw']==(regime=='high') and policy['prefix_rows']==1024 and policy['distinct_exact']
                            if mode=='raw_partial' or regime=='high':assert policy['state_released_before_tail']
                        assert p['sent']['transfers']==n and p['managed_peak']<=33554432
                    for f in m['finals'].values():assert f['managed_peak']<=33554432 and f['received']['transfers']==n and f['retention']['current_spill_backlog_bytes']==0
                    sample=dict(query_ns=out['query_time_ns'],coordinator_ns=m['elapsed_ms']*1000000,operator_work_ns=sum(t['operator_work_ns'] for role in ['producers','finals'] for t in m[role].values()),max_final_work_ns=max(t['operator_work_ns'] for t in m['finals'].values()),sent_rows=sent,sent_bytes=sum(p['sent']['payload_bytes'] for p in m['producers'].values()),spill_bytes=sum(t['spill_bytes'] for role in ['producers','finals'] for t in m[role].values()),rss_bytes=it['peak_rss_bytes'])
                else:
                    d=m['decision'];assert len(d['observations'])==n*2 and len(d['digest'])==64
                    assert m['spec']['config']['local']==local
                    assert d['distribution']==('broadcast_build' if regime=='tiny' and mode!='forced_repartition' else 'hash_repartition')
                    if regime=='tiny':assert d['build']=='right' and d['complete']==[False,True]
                    if regime=='balanced':assert d['complete']==[False,False]
                    for f in m['finals'].values():
                        l=f['local'];assert l['input_batch_peak_bytes']<=65536 and l['output_batch_peak_bytes']<=65536
                        if regime=='pressure':assert l['used_grace'] and l['spill_write_bytes']>0 and l['spill_read_bytes']>0 and l['transitioned']==(mode=='auto')
                    sample=dict(query_ns=out['query_time_ns'],coordinator_ns=m['elapsed_ns'],probe_ns=m['probe_ns'],commit_ns=m['commit_ns'],source_verify_ns=sum(p['source_verify_ns'] for p in m['producers'].values()),operator_work_ns=sum(f['local']['operator_work_ns'] for f in m['finals'].values()),max_final_work_ns=max(f['local']['operator_work_ns'] for f in m['finals'].values()),sent_rows=[sum(p['sent'][s]['rows'] for p in m['producers'].values()) for s in range(2)],sent_bytes=[sum(p['sent'][s]['payload_bytes'] for p in m['producers'].values()) for s in range(2)],spill_bytes=sum(f['local']['spill_write_bytes'] for f in m['finals'].values()),managed_peak=max(f['local']['managed_peak'] for f in m['finals'].values()),rss_bytes=it['peak_rss_bytes'])
                if i:samples.append(sample)
            walls=[s['query_ns'] for s in samples];median=statistics.median(walls)
            assert run['summary']['median']==median and run['summary']['min']==min(walls) and run['summary']['max']==max(walls),'all measured samples must enter summary'
            summaries[stem]=dict(median_query_ns=median,min_query_ns=min(walls),max_query_ns=max(walls),samples=samples)
            children+=k+1;measured+=k
    for stem,fragment in [('negative-balanced-forced_broadcast','lacks complete bounded build proof'),('negative-pressure-hash_only','spill disabled'),('negative-hot-auto','capacity 32')]:
        assert statuses[stem]!=0 and fragment in (root/(stem+'-stderr.log')).read_text(),stem
    gates={};ratios={}
    get=lambda r,n,m:summaries[f'{r}-{n}-{m}']['median_query_ns']
    for n in [2,4]:
        tiny=get('tiny',n,'observed_v1')/min(get('tiny',n,'forced_repartition'),get('tiny',n,'forced_broadcast'))
        balanced=get('balanced',n,'observed_v1')/get('balanced',n,'forced_repartition')
        low=get('low',n,'adaptive_v1')/get('low',n,'fixed_hash')
        high=get('high',n,'adaptive_v1')/min([get('high',n,'fixed_hash')]+([get('high',n,'raw_partial')] if n==2 else []))
        ratios[str(n)]=dict(tiny=tiny,balanced=balanced,low=low,high=high)
        for name,value,guard in [('tiny',tiny,1.15),('balanced',balanced,1.20),('low',low,1.15),('high',high,1.15)]:gates[f'{name}-{n}']=value<=guard
    assert children==118 and measured==95
    return dict(passed=all(gates.values()),correctness_passed=True,source_commit=commit,runtime_tree=runtime,children=children,measured=measured,gates=gates,ratios=ratios,summaries=summaries)
if __name__=='__main__':
    root=pathlib.Path(sys.argv[1]).resolve()
    try:result=verify(root)
    except Exception as e:
        result=dict(passed=False,correctness_passed=False,error=str(e));(root/'verification.json').write_text(json.dumps(result,indent=2)+'\n');raise
    (root/'verification.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='summaries'},indent=2));sys.exit(0 if result['passed'] else 1)
