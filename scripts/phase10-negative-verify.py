#!/usr/bin/env python3
"""Copy-only attacks on Phase10 evidence; preserve originals and rejections."""
import copy,importlib.util,json,os,pathlib,shutil,sys
root=pathlib.Path(sys.argv[1]).resolve();out=root/'negative-verifier';out.mkdir(exist_ok=False)
module_spec=importlib.util.spec_from_file_location('phase10verify',pathlib.Path(__file__).with_name('phase10-verify.py'));module=importlib.util.module_from_spec(module_spec);module_spec.loader.exec_module(module)
stem='imbalanced-2-leased_v1';run=json.loads((root/(stem+'-result.json')).read_text());manifest=run['dataset'];record=json.loads((root/(stem+'-metrics.jsonl')).read_text().splitlines()[0]);command=json.loads((root/(stem+'-command.json')).read_text());dataset=pathlib.Path(command[command.index('--dataset')+1]);worker=pathlib.Path(os.environ.get('ADE_BENCH_BIN','target/release/ade-bench')).resolve().with_name('ade-worker')
module.check_record(record,run,manifest,0,True,set(),worker,dataset)
attacks={
 'run-id':lambda r:r.update(run_id='foreign'),
 'slot':lambda r:r.update(iteration_index=999),
 'source-inventory':lambda r:r['source']['morsels'][0].update(fingerprint='f'*64),
 'effective-config':lambda r:r['effective_config'].update(scheduler_lease_ms='9999'),
 'nonce':lambda r:r['metrics']['scheduler']['entries'][0]['lease'].update(nonce=999),
 'terminal-rows':lambda r:r['metrics']['producers']['0']['sent'].update(rows=r['metrics']['producers']['0']['sent']['rows']+1),
}
rejections={}
for label,mutate in attacks.items():
    forged=copy.deepcopy(record);mutate(forged);(out/(label+'.json')).write_text(json.dumps(forged,indent=2)+'\n')
    try:module.check_record(forged,run,manifest,0,True,set(),worker,dataset)
    except Exception as error:rejections[label]=f'{type(error).__name__}: {error}'
    else:raise AssertionError('forgery accepted: '+label)
# A consistent summary over fewer favorable iterations must still fail the fixed sample count.
with __import__('tempfile').TemporaryDirectory(prefix='ade-p10-forged-') as directory:
    clone=pathlib.Path(directory)/'evidence';shutil.copytree(root,clone,ignore=shutil.ignore_patterns('negative-verifier'))
    forged=copy.deepcopy(run);forged['iterations']=sorted(forged['iterations'],key=lambda i:i['outcome']['query_time_ns'])[:1];forged['run_config']['iterations']=1
    selected=forged['iterations'][0]
    original_index=selected['index'];selected['index']=0
    value=selected['outcome']['query_time_ns']
    forged['summary']=dict(count=1,min=value,max=value,mean=value,median=value,stddev=0,p90=value,p95=value)
    records=[json.loads(line) for line in (root/(stem+'-metrics.jsonl')).read_text().splitlines()]
    retained=copy.deepcopy(records[original_index+1]);retained['iteration_index']=0
    (clone/(stem+'-metrics.jsonl')).write_text('\n'.join(map(json.dumps,[records[0],retained]))+'\n')
    (clone/(stem+'-result.json')).write_text(json.dumps(forged,indent=2)+'\n')
    (out/'best-only-result.json').write_text(json.dumps(forged,indent=2)+'\n')
    (out/'best-only-metrics.jsonl').write_bytes((clone/(stem+'-metrics.jsonl')).read_bytes())
    # The modified result passes general schema/summary correctness. Only the
    # frozen release sample count must reject this internally consistent subset.
    __import__('subprocess').run([str(worker.with_name('ade-bench')),'check-result',str(clone/(stem+'-result.json'))],check=True,stdout=__import__('subprocess').DEVNULL)

    try:module.verify(clone)
    except Exception as error:rejections['best-only']=f'{type(error).__name__}: {error}'
    else:raise AssertionError('best-only forgery accepted')
(out/'rejections.json').write_text(json.dumps(rejections,indent=2)+'\n');print(json.dumps(rejections,indent=2))
