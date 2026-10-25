#!/usr/bin/env python3
"""Phase10 v1; exact source, paired controls, every child and failure preserved."""
import hashlib,json,os,pathlib,re,subprocess,sys,time
root=pathlib.Path(os.environ.get('ADE_PHASE10_RESULTS','benchmarks/results/phase10-v1')).resolve()
data=pathlib.Path(os.environ.get('ADE_PHASE10_DATA','benchmarks/datasets/phase10-v1')).resolve()
bin=pathlib.Path(os.environ.get('ADE_BENCH_BIN','target/release/ade-bench')).resolve()
assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=all']).strip(),'clean source required'
root.mkdir(parents=True,exist_ok=False);data.mkdir(parents=True,exist_ok=False)
commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip();tree=subprocess.check_output(['git','rev-parse','HEAD:crates'],text=True).strip()
archive=subprocess.check_output(['git','archive','--format=tar',commit]);(root/'source.tar').write_bytes(archive)
subprocess.run(['git','bundle','create',str(root/'source.bundle'),'HEAD'],check=True)
(root/'source-identity.json').write_text(json.dumps(dict(schema_version=1,source_commit=commit,runtime_tree=tree,source_archive_sha256=hashlib.sha256(archive).hexdigest()),indent=2)+'\n')
(root/'binary-sha256.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [bin,bin.with_name('ade-worker')]},indent=2)+'\n')
(root/'declaration.md').write_bytes(pathlib.Path('docs/research/phase-10-experiment.md').read_bytes())
def command(args,stem,env=None):
    cmd=[str(bin),*map(str,args)];(root/f'{stem}-command.json').write_text(json.dumps(cmd,indent=2)+'\n')
    with (root/f'{stem}-stdout.log').open('w') as out,(root/f'{stem}-stderr.log').open('w') as err:
        return subprocess.run(cmd,stdout=out,stderr=err,env=env,timeout=900).returncode
command(['env','--format','json'],'environment')
readiness=[]
for i in range(3):
    r={k:subprocess.check_output(v,text=True) for k,v in {'top':['top','-l','1','-n','0'],'vm':['vm_stat'],'uptime':['uptime'],'processes':['ps','-A','-o','pid=,comm=']}.items()};r['time_ns']=time.time_ns();readiness.append(r)
    (root/'readiness.json').write_text(json.dumps(readiness,indent=2)+'\n')
    if i<2:time.sleep(10)
idle=[float(re.search(r'([\d.]+)% idle',r['top'])[1]) for r in readiness];load=[float(re.search(r'load averages?:\s*([\d.]+)',r['uptime'])[1]) for r in readiness];swap=[int(re.search(r'Swapouts:\s*(\d+)',r['vm'])[1]) for r in readiness]
others=[line for r in readiness for line in r['processes'].splitlines() if re.search(r'/(cargo|rustc|ade-bench|ade-worker)$',line)]
if not(min(idle)>=70 and max(load)<=8 and swap[-1]-swap[0]<=1000 and not others):raise SystemExit('readiness failed; ZERO measurements; preserve attempt')
spec='benchmarks/specs/aggregate-low-cardinality.toml'
for regime in ['imbalanced','uniform']:
    args=['data','generate','synthetic','--dataset-id',f'phase10-{regime}-v1','--rows',4000000,'--files',32,'--row-group-rows',8192,'--key-cardinality',1024,'--seed',42,'--output',data/regime]
    if regime=='imbalanced':args+=['--file-row-weights',','.join(map(str,[15,1,1,1]*8))]
    assert command(args,f'{regime}-manifest')==0
    assert command(['validate','--engine','datafusion','--spec',spec,'--dataset',data/regime],f'{regime}-oracle')==0
statuses={};inventory=[]
def run(regime,workers,policy,extra=(),label=None):
    stem=label or f'{regime}-{workers}-{policy}';k=5 if workers==2 else 3
    opts=[f'workers={workers}','exchange_mode=spill','exchange_buffer_bytes=1048576','memory_limit_bytes=33554432','batch_size=8192','prefetch_batches=0','recovery_enabled=false',f'scheduler_policy={policy}',*extra]
    metric=root/f'{stem}-metrics.jsonl';metric.write_text('');env=dict(os.environ,ADE_DISTRIBUTED_METRICS_FILE=str(metric))
    args=['run','--engine','ade-distributed',*sum((['--engine-option',s] for s in opts),[]),'--spec',spec,'--dataset',data/regime,'--verify-data','full','--warmups',1,'--iterations',k,'--output',root/f'{stem}-result.json']
    statuses[stem]=command(args,stem,env);inventory.append(dict(stem=stem,regime=regime,workers=workers,policy=policy,options=opts))
    (root/'statuses.json').write_text(json.dumps(statuses,indent=2)+'\n');(root/'inventory.json').write_text(json.dumps(inventory,indent=2)+'\n');print(stem,statuses[stem],flush=True)
for n in [2,4]:
    for policy in ['static','leased_no_steal','leased_v1']:run('imbalanced',n,policy)
    for policy in ['static','leased_v1']:run('uniform',n,policy)
for label,weights in [('unweighted','1,1,1,1'),('weighted','1,4,4,4')]:run('uniform',4,'leased_v1',[f'scheduler_capacities={weights}','scheduler_research_delay_worker=0','scheduler_research_delay_ms=100'],f'capacity-{label}')
events=json.dumps([dict(after_completed=1,worker=2,action='activate'),dict(after_completed=2,worker=0,action=dict(spot_notice=dict(grace_ms=5000))),dict(after_completed=3,worker=0,action='drain')],separators=(',',':'))
run('uniform',4,'leased_v1',['scheduler_initial_active=2','scheduler_research_delay_worker=0','scheduler_research_delay_ms=1000',f'scheduler_participation={events}'],'participation')
negatives={
 'exit':['scheduler_research_exit_after_emit_worker=0'],
 'expiry':['scheduler_research_delay_worker=0','scheduler_research_delay_ms=350','scheduler_research_no_heartbeat_worker=0','scheduler_lease_ms=80','scheduler_heartbeat_ms=20'],
 'notice':['scheduler_research_delay_worker=0','scheduler_research_delay_ms=1000','scheduler_participation='+json.dumps([dict(after_completed=1,worker=0,action=dict(spot_notice=dict(grace_ms=1)))],separators=(',',':'))],
}
for name,extra in negatives.items():
    opts=['workers=2','exchange_mode=spill','recovery_enabled=false','scheduler_policy=leased_v1',*extra]
    statuses['negative-'+name]=command(['validate','--engine','ade-distributed',*sum((['--engine-option',s] for s in opts),[]),'--spec',spec,'--dataset',data/'uniform'],'negative-'+name)
(root/'statuses.json').write_text(json.dumps(statuses,indent=2)+'\n')
for name,cmd in [('post-vm',['vm_stat']),('post-top',['top','-l','1','-n','0'])]:(root/f'{name}.txt').write_bytes(subprocess.check_output(cmd))
subprocess.run([sys.executable,'scripts/phase10-verify.py',str(root)],check=True)
