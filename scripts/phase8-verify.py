#!/usr/bin/env python3
"""Validate child identity/oracle, actual topology and predeclared empirical gates."""
import json
import pathlib
import statistics
import subprocess
import os
import sys


def verify(root):
    def load(name):
        return json.loads((root / name).read_text())
    commit = (root / 'source-commit.txt').read_text().strip()
    summaries = {}
    identity = None
    cases = [(d, 2, p, 'normal', 3, 1) for d in ('uniform', 'hash-bucket-skew', 'hot-key') for p in ('static', 'sampled_v1')]
    cases += [('hash-bucket-skew', 4, p, 'normal', 3, 1) for p in ('static', 'sampled_v1')]
    cases += [('hash-bucket-skew', 2, 'static_packed', 'normal', 3, 1)]
    cases += [('tiny', 2, 'sampled_v1', 'normal', 1, 0)]
    cases += [('hash-bucket-skew', 2, 'sampled_v1', p, 1, 0) for p in ('replay', 'restore')]
    for distribution, workers, policy, profile, iterations, warmups in cases:
        stem = f'{distribution}-{workers}-{policy}-{profile}'
        run = load(stem + '-result.json')
        assert run['valid'] and run['schema_version'] == 1, stem
        assert run['repository'] == {'git_commit': commit, 'git_dirty': False}, stem
        assert run['build']['profile'] == 'release', stem
        current = {k: run[k] for k in ('repository', 'build', 'environment', 'workload')}
        if identity is None:
            identity = current
        assert current == identity, stem
        manifest = load(distribution + '-manifest.json')
        assert run['dataset'] == manifest, stem
        assert manifest['synthetic']['seed'] == 42 and manifest['synthetic']['files'] == 4
        assert manifest['row_count'] == (100 if distribution == 'tiny' else 1000000)
        assert manifest['synthetic']['key_cardinality'] == (4 if distribution == 'tiny' else 131072)
        config = run['engine']['configuration']
        assert config['partition_policy'] == policy and config['workers'] == str(workers)
        assert config['control_protocol'] == 'ADETCP08' and config['exchange_wire_version'] == '2' and config['hash_version'] == '1'
        assert config['memory_limit_bytes'] == '33554432' and config['exchange_buffer_bytes'] == '1048576'
        assert config['prefetch_batches'] == '1' and config['research_slow_partition'] == 'off'
        assert config['exchange_mode'] == ('adaptive' if profile == 'restore' else 'spill')
        assert run['run_config']['warmups'] == warmups and run['run_config']['iterations'] == iterations
        records = [json.loads(line) for line in (root / (stem + '-metrics.jsonl')).read_text().splitlines()]
        all_iterations = run['warmups'] + run['iterations']
        assert len(records) == len(all_iterations) == iterations + warmups
        assert len({r['process_id'] for r in records}) == len(records)
        oracle = load(distribution + '-oracle.json')['result_fingerprint']
        walls, work, wait, completion, per_final, probes, commits = [], [], [], [], [], [], []
        for index, (it, record) in enumerate(zip(all_iterations, records)):
            assert it['exit_code'] == 0 and not it['error'] and it['outcome']['validated'], stem
            assert it['outcome']['result_fingerprint'] == oracle and it['outcome']['engine'] == run['engine'], stem
            m = record['metrics']
            active = set(m['finals'])
            assert len(m['producers']) == workers and len(m['finals']) + len(m['inactive']) == workers
            assert set(m['recovery']['committed_attempts']) == active
            assert m['gather']['transfers'] == len(active)
            decision = m['topology']
            if policy != 'sampled_v1':
                assert decision is None and len(active) == workers and not m['inactive']
                digest = None
            else:
                layout = decision['layout']
                digest = layout['digest']
                assert layout['version'] == layout['hash_version'] == 1 and layout['workers'] == workers
                assert all(isinstance(p,int) and 0 <= p < workers for p in layout['owners'])
                assert len(layout['owners']) == 48 and active == {str(p) for p in layout['owners']}
                assert len(digest) == 64 and len(decision['observations']) == workers
                topology_config = {key.removeprefix('partition_'): (value if key == 'partition_policy' else int(value))
                                   for key,value in config.items() if key.startswith('partition_')}
                subprocess.run([os.environ.get('ADE_WORKER_BIN','target/release/ade-worker'),'--verify-topology'],
                               input=json.dumps(dict(config=topology_config,decision=decision)),text=True,check=True)
                counts = [sum(o['counts'][s] for o in decision['observations']) for s in range(48)]
                static_load = [sum(c for s,c in enumerate(counts) if s % workers == p) for p in range(workers)]
                planned_load = [sum(c for s,c in enumerate(counts) if layout['owners'][s] == p) for p in range(workers)]
                assert decision['static_load'] == static_load and decision['planned_load'] == planned_load
                assert decision['moved_shards'] == sum(p != s % workers for s,p in enumerate(layout['owners']))
                for p, observation in enumerate(decision['observations']):
                    assert observation['producer'] == p and len(observation['counts']) == 48
                    assert sum(observation['counts']) <= observation['rows'] <= 16384
                    assert observation['files'] <= 4 and observation['read_bytes'] <= 8388608
                    assert isinstance(observation['source_complete'], bool)
                if distribution == 'hash-bucket-skew':
                    assert decision['reason'] == 'split_oversized_bucket' and decision['moved_shards'] > 0
                    assert len(active) == workers
                    assert max(decision['planned_load']) < max(decision['static_load'])
                if distribution == 'tiny':
                    assert decision['reason'] == 'coalesce_tiny' and active == {'0'}
                    assert all(o['source_complete'] for o in decision['observations'])
                if distribution == 'hash-bucket-skew' and workers == 2:
                    assert not any(o['source_complete'] for o in decision['observations'])
                if distribution == 'hot-key':
                    assert decision['reason'] == 'insufficient_sample_coverage'
                    assert decision['moved_shards'] == 0 and len(active) == workers
            for task in m['producers'].values():
                assert task['routing_buffer_peak_rows'] <= task['routing_buffer_capacity_rows'] <= workers * 1024
                assert (task['routing_buffer_capacity_rows'] > 0) == (policy != 'static')
                assert task['layout_digest'] == digest and len(task['manifests']) == workers
                for fact in task['materializations'].values():
                    assert fact['topology_digest'] == digest
                    if fact['receipt']:
                        assert fact['receipt']['topology_digest'] == digest
            for task in m['inactive'].values():
                assert not task['executed_final'] and task['layout_digest'] == digest
                assert task['received']['transfers'] == workers
                assert task['received']['rows'] == task['received']['batches'] == task['received']['payload_bytes'] == 0
            for task in m['finals'].values():
                assert task['executed_final'] and task['layout_digest'] == digest
                assert task['received']['transfers'] == workers
            for field in ('transfers','batches','rows','payload_bytes'):
                sent = sum(p['sent'][field] for p in m['producers'].values())
                received = sum(f['received'][field] for f in list(m['finals'].values()) + list(m['inactive'].values()))
                assert sent == received, (stem,field)
            assert sum(p['sent']['transfers'] for p in m['producers'].values()) == workers * workers
            if distribution == 'hash-bucket-skew':
                loads = [f['received']['rows'] for f in m['finals'].values()]
                if policy != 'sampled_v1':
                    assert sum(x > 0 for x in loads) == 1
                else:
                    assert min(loads) > 0 and max(loads) <= 1.2 * statistics.mean(loads)
            r = m['recovery']
            assert r['recovered_partitions'] == int(profile != 'normal')
            if profile != 'normal':
                assert r['plan']['layout_digest'] == digest and r['committed_attempts']['0'] == 1
                assert r['lost_segments'] == r['restored_segments'] + r['recomputed_segments']
                assert (r['restored_segments'] > 0) == (profile == 'restore')
                for replay in r['replay_manifests']:
                    original = m['producers'][str(replay['logical']['producer'])]['manifests']['0']
                    assert {k:v for k,v in replay.items() if k != 'attempt'} == {k:v for k,v in original.items() if k != 'attempt'}
            if index < warmups:
                continue
            walls.append(it['outcome']['query_time_ns'])
            work.append(max(f['operator_work_ns'] for f in m['finals'].values()))
            wait.append(max(f['source_pull_ns'] for f in m['finals'].values()))
            completion.append(max(f['coordinator_finished_ns'] for f in m['finals'].values()))
            per_final.append({p: {k: f[k] for k in ('received','output_rows','operator_work_ns','source_pull_ns','elapsed_ms','coordinator_finished_ns','managed_peak','spill_bytes')} for p,f in m['finals'].items()})
            probes.append(m['topology_probe_ns']); commits.append(m['topology_commit_ns'])
        summaries[stem] = dict(wall_ns=walls, max_final_work_ns=work, max_source_pull_ns=wait,
                               last_final_completion_ns=completion, per_final=per_final,
                               probe_ns=probes, commit_ns=commits,
                               median_wall_ns=statistics.median(walls), median_max_final_work_ns=statistics.median(work))
    def ratio(distribution, metric, workers=2):
        return summaries[f'{distribution}-{workers}-sampled_v1-normal'][metric] / summaries[f'{distribution}-{workers}-static-normal'][metric]
    gates = {'skew_exclusive_work_20pct': ratio('hash-bucket-skew','median_max_final_work_ns') <= .80,
             'skew_end_to_end_no_increase': ratio('hash-bucket-skew','median_wall_ns') <= 1,
             'uniform_overhead_30pct': ratio('uniform','median_wall_ns') <= 1.30}
    return dict(source_commit=commit, runtime_tree=(root/'runtime-tree.txt').read_text().strip(),
                correctness_passed=True, passed=all(gates.values()), gates=gates,
                ratios={f'{d}-{n}': {metric: ratio(d,metric,n) for metric in ('median_wall_ns','median_max_final_work_ns')}
                        for d,n in [('uniform',2),('hash-bucket-skew',2),('hot-key',2),('hash-bucket-skew',4)]},
                packing_diagnostic={metric: summaries['hash-bucket-skew-2-sampled_v1-normal'][metric] / summaries['hash-bucket-skew-2-static_packed-normal'][metric]
                                    for metric in ('median_wall_ns','median_max_final_work_ns')},
                summaries=summaries)


if __name__ == '__main__':
    root = pathlib.Path(sys.argv[1])
    try:
        verification = verify(root)
    except (AssertionError, KeyError, ValueError, OSError, subprocess.CalledProcessError) as error:
        verification = dict(passed=False, correctness_passed=False, error=repr(error))
    (root/'verification.json').write_text(json.dumps(verification,indent=2)+'\n')
    print(json.dumps(verification,indent=2))
    raise SystemExit(0 if verification['passed'] else 1)
