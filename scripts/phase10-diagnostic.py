#!/usr/bin/env python3
"""Short paired diagnosis, never release acceptance. Run with no concurrent build.

Uses the frozen layouts/config, plus a 32k-row fixture. The data directory can be
reused by the candidate so both compare the very same manifest bytes. Operator
timers are exclusive worker wall time, not CPU; producers/finals overlap.
"""
import argparse
import importlib.util
import json
import os
import pathlib
import statistics
import subprocess
import time
import uuid
import phase10_evidence as evidence

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('output', type=pathlib.Path)
parser.add_argument('data', type=pathlib.Path)
parser.add_argument('--binary', type=pathlib.Path, required=True)
args = parser.parse_args()
root, data, binary = args.output.resolve(), args.data.resolve(), args.binary.resolve()
root.mkdir(parents=True, exist_ok=False)
data.mkdir(parents=True, exist_ok=True)
(root / 'driver.py').write_bytes(pathlib.Path(__file__).read_bytes())
(root / 'phase10_evidence.py').write_bytes(pathlib.Path(evidence.__file__).read_bytes())
session = str(uuid.uuid4())
evidence.write(root, 'identity.json', dict(source=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
    status=subprocess.check_output(['git', 'status', '--porcelain'], text=True),
    binary_sha256={p.name: evidence.sha(p) for p in [binary, binary.with_name('ade-worker')]}))

def run(argv, stem, env=None):
    evidence.write(root, stem + '-command.json', argv)
    with (root / (stem + '-stdout.log')).open('w') as out, (root / (stem + '-stderr.log')).open('w') as err:
        subprocess.run(argv, stdout=out, stderr=err, env=env, check=True)

# Let source/binary identity I/O settle before the three frozen readings.
time.sleep(10)
samples = []
for i in range(3):
    samples.append(evidence.capture(session))
    evidence.write(root, 'readiness.json', samples)
    if i < 2: time.sleep(10)
evidence.check_readiness(samples, session, samples[0]['host'])
spec = 'benchmarks/specs/aggregate-low-cardinality.toml'
summaries = {}
for size, rows in [('frozen', 4_000_000), ('small', 32_000)]:
    for regime in ['imbalanced', 'uniform']:
        dataset = data / f'{size}-{regime}'
        if not dataset.exists():
            argv = [str(binary), 'data', 'generate', 'synthetic', '--dataset-id', f'phase10-{regime}-v1',
                    '--rows', str(rows), '--files', '32', '--row-group-rows', '8192',
                    '--key-cardinality', '1024', '--seed', '42', '--output', str(dataset)]
            if regime == 'imbalanced': argv += ['--file-row-weights', ','.join(map(str, [15, 1, 1, 1] * 8))]
            run(argv, f'{size}-{regime}-generate')
        run([str(binary), 'validate', '--engine', 'datafusion', '--dataset', str(dataset), '--spec', spec],
            f'{size}-{regime}-oracle')
        for workers in [2, 4]:
            for policy in ['static', 'leased_v1']:
                stem = f'{size}-{regime}-{workers}-{policy}'
                metric = root / (stem + '-metrics.jsonl')
                options = [f'workers={workers}', 'exchange_mode=spill', 'exchange_buffer_bytes=1048576',
                    'memory_limit_bytes=33554432', 'batch_size=8192', 'prefetch_batches=0',
                    'recovery_enabled=false', f'scheduler_policy={policy}']
                argv = [str(binary), 'run', '--engine', 'ade-distributed',
                    *sum((['--engine-option', o] for o in options), []), '--dataset', str(dataset),
                    '--spec', spec, '--verify-data', 'full', '--warmups', '1', '--iterations', '3',
                    '--output', str(root / (stem + '-result.json'))]
                run(['/usr/bin/time', '-l', *argv], stem, dict(os.environ, ADE_DISTRIBUTED_METRICS_FILE=str(metric)))
                run([str(binary), 'check-result', str(root / (stem + '-result.json'))], stem + '-check')
                result = json.loads((root / (stem + '-result.json')).read_text())
                records = [json.loads(line) for line in metric.read_text().splitlines()]
                evidence.require(result['valid'] and len(result['warmups']) == 1
                    and len(result['iterations']) == 3 and len(records) == 4, 'diagnostic sample inventory')
                measured = []
                for iteration, record in zip(result['iterations'], records[1:]):
                    m = record['metrics']
                    if m['scheduler']:
                        subprocess.run([str(binary.with_name('ade-worker')), '--verify-scheduler'],
                            input=json.dumps(m['scheduler']).encode(), check=True, stdout=subprocess.DEVNULL)
                    measured.append(dict(query_ns=iteration['outcome']['query_time_ns'],
                        peak_rss_bytes=iteration['peak_rss_bytes'],
                        last_producer_ns=max(p['coordinator_finished_ns'] for p in m['producers'].values()),
                        coordinator={k: m.get(k) for k in ['source_admission_ns', 'prepare_ns', 'gather_collect_ns']},
                        producers=[{k: p.get(k) for k in ['scan_decode_ns', 'aggregate_ns', 'source_verify_ns',
                            'operator_work_ns', 'routing_ns', 'sending_ns', 'lease_wait_ns', 'lease_ack_ns',
                            'elapsed_ms', 'sent', 'managed_peak', 'spill_bytes']} for p in m['producers'].values()],
                        finals=[{k: p.get(k) for k in ['operator_work_ns', 'source_pull_ns',
                            'elapsed_ms', 'spill_bytes']} for p in m['finals'].values()]))
                summaries[stem] = dict(samples=measured, median_query_ns=statistics.median(s['query_ns'] for s in measured),
                    median_tail_ns=statistics.median(s['last_producer_ns'] for s in measured))
                evidence.write(root, 'summary.json', summaries)
                print(stem, summaries[stem]['median_query_ns'], flush=True)
evidence.write(root, 'postflight.json', evidence.capture(session))
