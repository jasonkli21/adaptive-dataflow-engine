#!/usr/bin/env python3
"""Portable raw-source/choice/producer/window/checkpoint verifier; no services."""
import argparse,copy,hashlib,importlib.util,io,json,pathlib,struct,sys,tarfile,tempfile,uuid
HERE=pathlib.Path(__file__).resolve();sys.path.insert(0,str(HERE.parents[1]/'phase17'))
from digest import digest,raw
from service_fixture import require,rows
spec=importlib.util.spec_from_file_location('phase18_reference',HERE.with_name('oracle.py'));oracle=importlib.util.module_from_spec(spec);spec.loader.exec_module(oracle)
def load(p):return json.loads(pathlib.Path(p).read_text())
def sha(b):return hashlib.sha256(b).hexdigest()
def source(root):
    inv=load(root/'source-inventory.json')
    with tarfile.open(root/'source.tar.gz','r:gz') as tar:
        members=tar.getmembers();require(len(members)<=2048 and sum(m.size for m in members)<=33554432 and all(m.isfile() and m.size<=4194304 for m in members),'source archive bound');actual={m.name:sha(tar.extractfile(m).read()) for m in members}
    require(actual==inv,'frozen source inventory mismatch');runtime=load(root/'runtime-inventory.json');require(runtime and all(inv.get(p)==h for p,h in runtime.items()),'runtime source pin')
    env=load(root/'environment.json');require(env['base']=='fbe0e2b7739fd7d61c45c58706faa56b63a945be','unexpected base');require(len(env['binaries'])==8,'executable coverage')
    for name,h in env['binaries'].items():
        p=root/(name+'-pinned')
        if p.exists():require(sha(p.read_bytes())==h,'executable hash mismatch')
    return len(inv)
def table(path):
    text=path.read_text();_,end=json.JSONDecoder().raw_decode(text);lines=text[end:].splitlines();data=[]
    for line in lines:
        if line.startswith('|'):
            vals=[v.strip() for v in line.strip('|').split('|')]
            if vals[0]!='key':data.append(vals)
    return sorted(data)
def v2(root,case,model):
    c=load(root/case['config']);folder=root/case['snapshot'];sel=load(folder/'CURRENT2');cert=sel['certificate'];cert_bytes=(folder/cert['manifest']['generation']/'certificate.json').read_bytes();require(digest(cert_bytes)==sel['certificate_blake3'] and json.loads(cert_bytes)==cert,'v2 certificate hash')
    ref=cert['manifest'];b=(folder/ref['generation']/'manifest.json').read_bytes();require(digest(b)==ref['blake3'],'v2 manifest hash');m=json.loads(b);entries={}
    for sid,ref in enumerate(m['shards']):
        b=(folder/ref['object_generation']/f'shard-{sid:02}.bin').read_bytes();require(len(b)==ref['bytes'] and digest(b)==ref['blake3'],'v2 shard object pin')
        for off in range(0,len(b),64):
            r=b[off:off+64];require(raw(r[:32])==r[32:],'v2 record checksum');start,key,amount,count=struct.unpack('<qQqQ',r[:32]);require(raw(struct.pack('<Q',key))[0]&31==sid,'v2 ownership');entries[(start,key)]=(amount,count)
    model.compare(m,entries,model.outputs)
    if cert['placement']=='both':
        require(cert['remote'] is not None,'two-copy capability missing');wire=rows(root/'wire.jsonl');require(any(r['method']=='PUT' and r['status']==200 and r['signed'] for r in wire),'real remote write absent')
        remote=cert['remote']
        for sid,ref in enumerate(m['shards']):
            key=f"/{remote['bucket']}/{remote['prefix']}/checkpoint-v2/{remote['namespace']}/{ref['object_generation']}/shard-{sid:02}.bin"
            puts=[r for r in wire if r['path']==key and r['method']=='PUT' and r['status']==200];gets=[r for r in wire if r['path']==key and r['method']=='GET' and r['status']==200]
            require(puts and gets,'two-copy wire object absent')
            body=(root/'wire-bodies'/puts[0]['request_sha256']).read_bytes();require(digest(body)==ref['blake3'] and any(g['response_sha256']==sha(body) for g in gets),'remote/local state differs')
def negative_input(root,case):
    generic={'v3-hybrid-refused','forged-properties','unknown-schema','unsafe-source-token','missing-cut-partition','unbounded-range','wrong-delivery','forged-operator','duplicate-json','oversized-json'}
    if case['name'] not in generic:return
    body=(root/case['logical']).read_bytes()
    def unique(pairs):
        result={}
        for key,value in pairs:
            require(key not in result,'duplicate JSON field');result[key]=value
        return result
    try:
        require(len(body)<=262144,'plan byte cap');plan=json.loads(body,object_pairs_hook=unique);oracle.strict(plan)
    except (ValueError,TypeError,KeyError,UnicodeError):return
    raise ValueError('negative fixture is actually an admissible plan: '+case['name'])

def refusal(case,trace,stdout):
    name=case['name'];generic={'v3-hybrid-refused','forged-properties','unknown-schema','unsafe-source-token','missing-cut-partition','unbounded-range','wrong-delivery','forged-operator','duplicate-json','oversized-json'}
    expected=('stream admission: strict logical plan' if name in generic else {'changed-cut':'stream admission: logical job/cut identity mismatch','changed-history':'stream admission: logical job/cut identity mismatch','changed-cut-rebound':'state/checkpoint: checkpoint identity/codec/owner inventory','expired-origin':'stream admission: logical origin retention/truncation','v3-downgrade-refused':'stream admission: logical/native delivery mismatch','idle-cancel':'cancelled'}.get(name))
    require(expected is not None,'undeclared negative case '+name)
    failures=[r for r in trace if r.get('type')=='failure'];require(case['exit']==1 and not stdout and len(failures)==1 and failures[0].get('reason')==expected,'wrong refusal semantics '+name)
    admitted=[r for r in trace if r.get('type')=='admitted']
    if name!='idle-cancel':require(not admitted,'refusal reached assignment '+name)
    else:
        terminal=next(r for r in trace if r.get('type')=='terminal');require(len(admitted)==1 and terminal['status']=='FAILED' and terminal['failure_class']=='cancelled' and terminal['progress']['forwarded_records']==0 and terminal['progress']['frontier'] is None,'idle cancellation changed progress')

def verify(root):
    root=pathlib.Path(root);sources=source(root);criteria=load(root/'criteria.json');require(criteria['version']==1 and criteria['optimizer']==dict(local_row_units=4,distributed_start_units=20000,distributed_row_units=2,workers=[2,3,4],samples=3,timing='descriptive_only_no_speedup_threshold'),'predeclared science changed');require(load(root/'outcome.json')['status']=='CAMPAIGN_FINISHED','incomplete campaign')
    service_inputs=load(root/'service-inputs.json');require(len(service_inputs['kafka_jars'])>30 and all(len(h)==64 for h in service_inputs['kafka_jars'].values()),'service input identity missing')
    services=load(root/'services.json');require(services['broker_pid']!=services['minio_pid'] and services['kafka']=='3.9.1','separate services missing')
    cases=load(root/'cases.json');names={c['name']:c for c in cases};require(len(names)==len(cases),'duplicate cases');ledgers=load(root/'ledgers.json');models={};records=0;windows=0;classifications=0;peaks={}
    expected={'hybrid2','hybrid4','empty-prefix','legal-gaps','empty-prefix-stateless','empty-prefix-legacy-stateless','legal-gaps-stateless','legal-gaps-legacy-stateless','live-state','live-state-legacy','assigned-subset','v2-local','v2-local-recover','v2-both','v2-both-recover','restart-prefix','restart-tail','changed-cut','changed-history','changed-cut-rebound','kill-at-checkpoint','restart-after-kill','expired-origin','sink-stall','idle-pending','idle-cancel','v3','v3-hybrid-refused','v3-downgrade-refused','forged-properties','unknown-schema','unsafe-source-token','missing-cut-partition','unbounded-range','wrong-delivery','forged-operator','duplicate-json','oversized-json'}
    require(set(names)==expected,'required real case inventory differs')
    for case in cases:
        name=case['name'];trace=rows(root/(name+'.stderr'));stdout=(root/(name+'.stdout')).read_bytes()
        if case['exit']==1:
            negative_input(root,case);refusal(case,trace,stdout)
            if name in ['changed-cut','changed-history']:
                root_refusal=load(root/(name+'.root-refusal.json'));require(root_refusal['before']==root_refusal['after'],'cut/history refusal mutated root')
            continue
        if case['mode']=='kill':require(case['exit']==-9,'missing actual SIGKILL');continue
        if case['mode']=='v3':
            p=load(root/case['logical']);oracle.decision(p,next(r['decision'] for r in trace if r.get('type')=='unified_decision'));c=load(root/case['config']);oracle.native_binding(p,c['state'],'v3');require(p['computation']['source']['immutable_history']==c['trusted_history'],'v3 history binding');admit=next(r for r in trace if r.get('type')=='admitted');observed=rows(root/case['reader']);proof=oracle.verify_trace(root/case['snapshot'],c,ledgers,{admit['session']:trace},observed);require(proof['records']==42 and proof['outputs']==8,'unified v3 committed prefix');require(not stdout,'v3 writer exposed stdout');records+=proof['records'];windows+=proof['outputs'];continue
        initial=models.get(case.get('previous'));model,inputs=oracle.replay(root,case,ledgers,initial);models[name]=model;records+=len(inputs);windows+=len(model.outputs)-(len(initial.outputs) if initial else 0)
        if case.get('total') is not None:require(model.total==case['total'],'declared input stop did not execute')
        terminal=next(r for r in reversed(trace) if r.get('type')=='terminal');metrics=terminal['metrics'];require(metrics['in_flight_peak_records']<=1 and metrics['downstream_channel_depth']==0 and metrics['sink_frame_peak_bytes']<=2048,'native retention envelope')
        for key in ['hot_peak_bytes','disk_peak_bytes','checkpoint_peak_bytes']:
            if key in terminal.get('state',{}):peaks[key]=max(peaks.get(key,0),terminal['state'][key])
        if case.get('logical'):
            classifications+=len(inputs);p=load(root/case['logical']);c=load(root/case['config'])
            if case['mode']!='stateless':require(c['job_id']==str(uuid.UUID(bytes=raw(digest(oracle.canonical(p)).encode())[:16])),'state semantic digest not bound to native job')
        if case.get('snapshot'):
            if case['mode']=='v2':v2(root,case,model)
            else:oracle.checkpoint(root,case,model)
        if name.startswith('hybrid'):
            capture=next(c for c in load(root/'cut-captures.json') if c['name']==name);require(p['computation']['source']['mode']['cut']==[dict(partition=o['partition'],offset=o['high']) for o in capture['ranges']] and all(o['low']==0 for o in capture['ranges']),'cut was not actual observed source head')
            require(case['asynchronous_tail'] and terminal['metrics']['pending_polls']>0,'boundary forced completion rather than Pending')
            audits=[r['record'] for r in trace if r.get('type')=='unified_confirmed'];require({a['phase'] for a in audits}=={'historical','live'},'both regimes not executed');require(model.closed_late==1,'causally released closed-window late record was not dropped')
        if name=='legal-gaps':require(any(off>i for part in [0,1] for i,(p,off) in enumerate([x for x in inputs if x[0]==part])),'legal offsets were dense fixture')
        if name=='empty-prefix':require(all(r['record']['phase']=='live' for r in trace if r.get('type')=='unified_confirmed'),'empty history misclassified')
        if name=='sink-stall':
            blocked=[r for r in trace if r.get('type')=='sink_blocked'];require(len(blocked)>=25 and blocked[-1]['elapsed_ms']-blocked[0]['elapsed_ms']>=28000,'sustained sink stall absent');require(len({r['metrics']['polls'] for r in blocked})==1 and len({r['progress']['forwarded_records'] for r in blocked})==1,'data polling/progress bypassed sink demand');require(terminal['metrics']['sink_blocked_ticks']>2000,'backpressure not executed');peaks['kafka_queue_bytes']=terminal['broker']['queue_bytes_peak'];require(peaks['kafka_queue_bytes']<=16384+4*65536,'Kafka response domain bound')
        if name=='idle-pending':require(model.total==0 and model.frontier is None and metrics['pending_polls']>0,'idle manufactured progress')
    for prefix in ['empty-prefix','legal-gaps']:
        require(models[prefix+'-stateless'].cursors==models[prefix+'-legacy-stateless'].cursors,'old/unified stateless parity')
        unified_timers=[r['timer'] for r in rows(root/(prefix+'-stateless.stderr')) if r.get('type')=='timer'];legacy_timers=[r['timer'] for r in rows(root/(prefix+'-legacy-stateless.stderr')) if r.get('type')=='timer'];require(unified_timers==legacy_timers and len(unified_timers)==1,'native timer parity')
    require(models['live-state'].state==models['live-state-legacy'].state and models['live-state'].outputs==models['live-state-legacy'].outputs,'old/unified keyed state parity')
    measurements=load(root/'measurements.json');require(len(measurements)==12,'full descriptive distributions absent')
    choices=set();verified_datasets=set()
    for m in measurements:
        name=m['name'];tr=rows(root/(name+'.stderr'));entry=next(r for r in tr if r.get('type')=='unified_decision');oracle.decision(entry['plan'],entry['decision']);choice=entry['decision']['chosen'];choices.add(choice)
        require(choice==('distributed_aggregate' if m['optimized'] and m['rows']>10000 else 'local'),'two actual choices not realized');require(entry['decision']['controls']['enabled']==m['optimized'],'static control did not disable optimization');require(table(root/(name+'.stdout'))==load(root/f"batch-{m['rows']}-oracle.stdout"),'independent DataFusion aggregate differs');require(m['elapsed_ns']>0,'missing actual measurement')
        diagnostics,_=json.JSONDecoder().raw_decode((root/(name+'.stdout')).read_text());require(diagnostics['target']==choice,'optimizer target not executed')
        require(diagnostics['plan_hash']==oracle.physical_digest(entry['plan'],choice),'executed native PlanV1 differs from shared lowering')
        require(diagnostics['sources']==entry['plan']['computation']['sources'] and diagnostics['output_schema']==entry['decision']['properties']['schema'],'logical/native finite source/schema differs')
        if m['rows'] not in verified_datasets:
            dataset=root/f"batch-{m['rows']}";manifest_bytes=(dataset/'manifest.json').read_bytes();manifest=json.loads(manifest_bytes);bound=entry['plan']['computation']['sources'][0]
            require(bound['manifest']==digest(manifest_bytes) and bound['logical']==manifest['logical_fingerprint'] and bound['schema_identity']==manifest['schema_fingerprint'],'actual manifest/source identity differs')
            require(sum(f['rows'] for f in bound['files'])==m['rows'] and manifest['row_count']==m['rows'],'predeclared finite source rows differ')
            for f,ref in zip(bound['files'],manifest['files']):
                require(f['name']==ref['relative_path'] and f['rows']==ref['rows'] and f['bytes']==ref['bytes'] and f['fingerprint']==ref['fingerprint'],'source inventory differs')
                path=pathlib.PurePosixPath(f['name']);require(not path.is_absolute() and '..' not in path.parts,'source traversal');body=(dataset/f['name']).read_bytes();require(len(body)==f['bytes'] and digest(body)==f['fingerprint'],'actual Parquet content differs')
            require(len(bound['files'])==len(manifest['files']),'source coverage');verified_datasets.add(m['rows'])

        native=diagnostics['runtime'];require(native['native_entrypoint']==('ade_distributed::coordinator::execute' if choice=='distributed_aggregate' else 'ade_exec::physical::execute'),'wrong native implementation executed')
        if choice=='distributed_aggregate':require(native['metrics']['worker_count']==2 and len(native['metrics']['producers'])==2 and native['metrics']['gather']['payload_bytes']>0,'real worker/exchange witness absent')
    require(choices=={'local','distributed_aggregate'},'two native execution branches missing')
    return dict(cases=len(cases),records=records,classifications=classifications,windows=windows,source_files=sources,measurements=len(measurements),peaks=peaks)
def archive(path,manifest):
    m=load(manifest);require(sha(pathlib.Path(path).read_bytes())==m['archive_sha256'],'archive hash')
    with tempfile.TemporaryDirectory(prefix='ade18-verify-') as tmp:
        root=pathlib.Path(tmp);found=set();total=0
        with tarfile.open(path,'r:gz') as tar:
            for member in tar:
                rel=pathlib.PurePosixPath(member.name);require(member.isfile() and not rel.is_absolute() and '..' not in rel.parts and member.name not in found and member.name in m['files'],'unsafe archive member');total+=member.size;require(total<=536870912 and len(found)<30000,'archive bounds');b=tar.extractfile(member).read();require(sha(b)==m['files'][member.name],'member hash');dest=root/member.name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(b);found.add(member.name)
        require(found==set(m['files']),'archive inventory');return verify(root)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--campaign',type=pathlib.Path);p.add_argument('--archive',type=pathlib.Path);p.add_argument('--manifest',type=pathlib.Path);a=p.parse_args();print(json.dumps(verify(a.campaign) if a.campaign else archive(a.archive,a.manifest),sort_keys=True))
