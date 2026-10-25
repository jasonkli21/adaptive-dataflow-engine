#!/usr/bin/env python3
"""Frozen v1 matrix. Preserve each result including failed/expected negative arms."""
import hashlib,json,os,pathlib,subprocess,sys,time
root=pathlib.Path(os.environ.get('ADE_PHASE9_RESULTS','benchmarks/results/phase9-v1')).resolve()
data=pathlib.Path(os.environ.get('ADE_PHASE9_DATA','benchmarks/datasets/phase9-v1')).resolve()
bin=pathlib.Path(os.environ.get('ADE_BENCH_BIN','target/release/ade-bench')).resolve()
if subprocess.check_output(['git','status','--porcelain','--untracked-files=all']).strip():raise SystemExit('clean source required')
root.mkdir(parents=True,exist_ok=False);data.mkdir(parents=True,exist_ok=False)
(root/'source-commit.txt').write_bytes(subprocess.check_output(['git','rev-parse','HEAD']))
(root/'runtime-tree.txt').write_bytes(subprocess.check_output(['git','rev-parse','HEAD:crates']))
source_commit=(root/'source-commit.txt').read_text().strip()
runtime_tree=(root/'runtime-tree.txt').read_text().strip()
source_archive=subprocess.check_output(['git','archive','--format=tar',source_commit])
(root/'source-identity.json').write_text(json.dumps(dict(
    schema_version=1,
    source_commit=source_commit,
    runtime_tree=runtime_tree,
    source_archive_sha256=hashlib.sha256(source_archive).hexdigest(),
),indent=2)+'\n')
(root/'binary-sha256.json').write_text(json.dumps({str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [bin,bin.with_name('ade-worker')]},indent=2)+'\n')
def command(args,stem,env=None):
    (root/f'{stem}-command.json').write_text(json.dumps([str(bin),*map(str,args)],indent=2)+'\n')
    with (root/f'{stem}-stdout.log').open('w') as out,(root/f'{stem}-stderr.log').open('w') as err:
        r=subprocess.run([str(bin),*map(str,args)],stdout=out,stderr=err,env=env,timeout=900)
    return r.returncode
command(['env','--format','json'],'environment')
# Readiness must precede all release measurements, including warmups.
readiness=[]
for i in range(3):
    readings={k:subprocess.check_output(v,text=True) for k,v in {'top':['top','-l','1','-n','0'],'vm':['vm_stat'],'uptime':['uptime'],'processes':['ps','-A','-o','pid=,comm=']}.items()}
    readings['time_ns']=time.time_ns();readiness.append(readings)
    (root/'readiness.json').write_text(json.dumps(readiness,indent=2)+'\n')
    if i<2:time.sleep(10)
import re
idle=[float(re.search(r'([\d.]+)% idle',r['top'])[1]) for r in readiness]
load=[float(re.search(r'load averages?:\s*([\d.]+)',r['uptime'])[1]) for r in readiness]
swap=[int(re.search(r'Swapouts:\s*(\d+)',r['vm'])[1]) for r in readiness]
others=[line for r in readiness for line in r['processes'].splitlines() if re.search(r'/(cargo|rustc|ade-bench|ade-worker)$',line)]
if not(min(idle)>=70 and max(load)<=8 and swap[-1]-swap[0]<=1000 and not others):raise SystemExit('readiness failed; no measurement; preserve and postpone')
join_spec='benchmarks/specs/inner-equi-join.toml';agg_spec='benchmarks/specs/aggregate-high-cardinality.toml'
for name,l,r,domain in [('tiny',65536,128,65536),('balanced',65536,65536,1048576),('pressure',8192,8192,8192),('hot',128,128,1)]:
    assert command(['data','generate','join','--dataset-id',f'phase9-{name}-v1','--left-rows',l,'--right-rows',r,'--left-key-domain',domain,'--right-key-domain',domain,'--seed',42,'--files-per-side',4,'--row-group-rows',4096,'--output',data/name],f'{name}-manifest')==0
for name,card in [('low',16),('high',100000)]:
    assert command(['data','generate','synthetic','--dataset-id',f'phase9-{name}-v1','--rows',100000,'--files',4,'--row-group-rows',8192,'--key-cardinality',card,'--seed',42,'--output',data/name],f'{name}-manifest')==0
for name in ['tiny','balanced','pressure','hot','low','high']:
    assert command(['validate','--engine','datafusion','--spec',join_spec if name not in ['low','high'] else agg_spec,'--dataset',data/name],f'{name}-oracle')==0
arms={2:[('tiny',s) for s in ['forced_repartition','forced_broadcast','observed_v1']]+[('balanced',s) for s in ['forced_repartition','observed_v1']]+[('pressure',s) for s in ['auto','grace']]+[(r,s) for r in ['low','high'] for s in ['fixed_hash','raw_partial','adaptive_v1']],4:[('tiny',s) for s in ['forced_repartition','forced_broadcast','observed_v1']]+[('balanced',s) for s in ['forced_repartition','observed_v1']]+[('pressure','auto')]+[(r,s) for r in ['low','high'] for s in ['fixed_hash','adaptive_v1']]}
def options(regime,mode,workers):
    if regime in ['low','high']:
        return ['--engine','ade-distributed',*sum((['--engine-option',s] for s in [f'workers={workers}','partition_policy=static','exchange_mode=spill','exchange_buffer_bytes=1048576','memory_limit_bytes=33554432','batch_size=1024','prefetch_batches=0','recovery_enabled=false',f'aggregate_policy={mode}']),[])]
    opts=[f'workers={workers}',f'join_policy={mode if regime in ["tiny","balanced"] else "forced_repartition"}']
    if regime in ['pressure','hot']:opts += [f'hash_rows={512 if regime=="pressure" else 32}','memory_limit_bytes=262144',f'join_algorithm={mode}']
    return ['--engine','ade-distributed-join',*sum((['--engine-option',s] for s in opts),[])]
statuses={}
for workers,cases in arms.items():
    for regime,mode in cases:
        stem=f'{regime}-{workers}-{mode}';metric=root/f'{stem}-metrics.jsonl';metric.write_text('');env=dict(os.environ,ADE_DISTRIBUTED_METRICS_FILE=str(metric))
        args=['run',*options(regime,mode,workers),'--spec',agg_spec if regime in ['low','high'] else join_spec,'--dataset',data/regime,'--verify-data','full','--warmups',1,'--iterations',5 if workers==2 else 3,'--output',root/f'{stem}-result.json']
        statuses[stem]=command(args,stem,env);(root/'statuses.json').write_text(json.dumps(statuses,indent=2)+'\n')
        print(stem,statuses[stem],flush=True)
for regime,mode in [('balanced','forced_broadcast'),('pressure','hash_only'),('hot','auto')]:
    stem=f'negative-{regime}-{mode}';statuses[stem]=command(['validate',*options(regime,mode,2),'--spec',join_spec,'--dataset',data/regime],stem)
(root/'statuses.json').write_text(json.dumps(statuses,indent=2)+'\n')
(root/'post-vm.txt').write_bytes(subprocess.check_output(['vm_stat']))
(root/'post-top.txt').write_bytes(subprocess.check_output(['top','-l','1','-n','0']))
# The verifier is authoritative, not runner exit codes or hand-picked successful rows.
subprocess.run([sys.executable,'scripts/phase9-verify.py',str(root)],check=True)
