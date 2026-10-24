#!/usr/bin/env python3
"""Fixed A/B/C stability confirmation: retain A, require B+C and all9 medians."""
import json
import pathlib
import statistics
import subprocess
import sys


def verify(roots):
    assert len(roots) == 3, 'exactly A, B, C are required'
    checks = []
    pids = set()
    reference = None
    for root in roots:
        subprocess.run([sys.executable, str(pathlib.Path(__file__).with_name('phase8-verify.py')), str(root), "ADETCP10"], stdout=subprocess.DEVNULL, check=False)
        v = json.loads((root/'verification.json').read_text())
        assert v['correctness_passed']
        assert v['source_commit'] == 'ab7404b1c7095ce3e8b60d79f944a78cd9787c34'
        assert v['runtime_tree'] == '432cc7dd212ec41ad6fdec93bc97f957df5a514d'
        identity = {}
        children = 0
        for case in v['summaries']:
            run = json.loads((root/(case+'-result.json')).read_text())
            identity[case] = {k: run[k] for k in ('repository','build','environment','dataset','workload','engine','run_config')}
            for line in (root/(case+'-metrics.jsonl')).read_text().splitlines():
                pid = json.loads(line)['process_id']
                assert pid not in pids, 'process isolation identity reused'
                pids.add(pid)
                children += 1
        assert children == 39
        if reference is None:
            reference = identity
        assert identity == reference, 'cross-suite identity drift'
        checks.append(v)
    pooled = {}
    for case in checks[0]['summaries']:
        count = 3 if case.endswith('-normal') and not case.startswith('tiny-') else 1
        wall = [x for v in checks for x in v['summaries'][case]['wall_ns']]
        work = [x for v in checks for x in v['summaries'][case]['max_final_work_ns']]
        assert len(wall) == len(work) == count * 3
        pooled[case] = dict(wall_ns=wall,max_final_work_ns=work,
                            median_wall_ns=statistics.median(wall),
                            median_max_final_work_ns=statistics.median(work))
    def ratio(distribution, metric, workers=2):
        return pooled[f'{distribution}-{workers}-sampled_v1-normal'][metric]/pooled[f'{distribution}-{workers}-static-normal'][metric]
    ratios = {f'{d}-{n}': {k: ratio(d,k,n) for k in ('median_wall_ns','median_max_final_work_ns')}
              for d,n in [('uniform',2),('hash-bucket-skew',2),('hot-key',2),('hash-bucket-skew',4)]}
    gates = dict(skew_exclusive_work_20pct=ratio('hash-bucket-skew','median_max_final_work_ns')<=.8,
                 skew_end_to_end_no_increase=ratio('hash-bucket-skew','median_wall_ns')<=1,
                 uniform_overhead_30pct=ratio('uniform','median_wall_ns')<=1.3,
                 independent_B=checks[1]['passed'],independent_C=checks[2]['passed'])
    return dict(passed=all(gates.values()),correctness_passed=True,source_commit=checks[0]['source_commit'],
                runtime_tree=checks[0]['runtime_tree'],children=117,primary_measured_per_arm=9,
                gates=gates,ratios=ratios,per_suite=[dict(path=str(p),passed=v['passed'],ratios=v['ratios']) for p,v in zip(roots,checks)],summaries=pooled)


if __name__ == '__main__':
    output = pathlib.Path(sys.argv[1])
    try:
        result = verify([pathlib.Path(p) for p in sys.argv[2:]])
    except (AssertionError,KeyError,ValueError,OSError) as error:
        result = dict(passed=False,correctness_passed=False,error=repr(error))
    output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='summaries'},indent=2))
    raise SystemExit(0 if result['passed'] else 1)
