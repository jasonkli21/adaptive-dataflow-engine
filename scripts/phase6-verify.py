"""Verify commit-bound oracle, terminal manifests, selective replay and failure evidence."""
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
commit = sys.argv[2]
load = lambda name: json.loads((root / name).read_text())
identity = load('environment.json')
assert identity['repository'] == {'git_commit': commit, 'git_dirty': False}
assert identity['build']['profile'] == 'release'
oracle = load('datafusion.json')['result_fingerprint']
manifest = load('manifest.json')
expected_cases = {
    '2-normal-local_materialized', '2-recover-local_materialized',
    '2-normal-direct', '2-normal-buffered', '2-normal-spill',
    '2-normal-object_store', '2-recover-adaptive', '4-normal-adaptive',
}
assert {p.name.removesuffix('-result.json') for p in root.glob('*-result.json')} == expected_cases | {'unrecoverable'}
summary = {}
manifest_id = None
workload = None
for case in sorted(expected_cases):
    workers, profile, mode = case.split('-')
    workers = int(workers)
    result = load(f'{case}-result.json')
    assert result['valid'] and result['schema_version'] == 1
    for key in ['repository', 'build', 'environment']:
        assert result[key] == identity[key]
    assert result['dataset'] == manifest
    assert manifest_id is None or result['dataset_manifest_fingerprint'] == manifest_id
    manifest_id = result['dataset_manifest_fingerprint']
    assert workload is None or workload == result['workload']
    workload = result['workload']
    assert result['run_config']['warmups'] == 0 and result['run_config']['iterations'] == 1
    assert not result['warmups'] and len(result['iterations']) == 1
    assert result['iterations'][0]['outcome']['validated']
    assert result['iterations'][0]['outcome']['result_fingerprint'] == oracle
    assert load(f'{case}-validation.json')['result_fingerprint'] == oracle
    config = result['engine']['configuration']
    for key, value in {
        'workers': str(workers), 'exchange_mode': mode,
        'recovery_enabled': 'true', 'recovery_policy': 'single_final_loss_v1',
        'research_exit_final_partition': '0' if profile == 'recover' else 'off',
        'exchange_buffer_bytes': '1048576', 'memory_limit_bytes': '33554432',
        'prefetch_batches': '1', 'exchange_wire_version': '2', 'hash_version': '1',
    }.items():
        assert config[key] == value, (case, key)
    log = (root / f'{case}-metrics.log').read_text()
    lines = [line for line in log.splitlines() if 'ADE distributed execution' in line]
    assert len(lines) == 1  # One logical job, no adapter-level retry.
    metrics = json.JSONDecoder().raw_decode(lines[0].split('metrics=', 1)[1])[0]
    recovery = metrics['recovery']
    producers = metrics['producers']
    assert set(producers) == {str(p) for p in range(workers)}
    assert set(metrics['finals']) == set(producers)
    assert metrics['gather']['transfers'] == workers
    original_job = None
    for p, task in producers.items():
        assert set(task['manifests']) == set(producers)
        assert task['sent']['transfers'] == workers
        assert task['source_files'] == len(manifest['files'][int(p)::workers])
        for d, terminal in task['manifests'].items():
            logical = terminal['logical']
            assert logical['producer'] == int(p) and logical['partition'] == int(d)
            assert logical['exchange'] == 0 and terminal['attempt'] == 0
            assert original_job is None or original_job == logical['job']
            original_job = logical['job']
            counts = task['destinations'][d]
            assert terminal['segments'] == counts['batches']
            assert terminal['rows'] == counts['rows']
            assert terminal['payload_bytes'] == counts['payload_bytes']
            assert len(terminal['fingerprint']) == 64
    assert recovery['committed_attempts'] == {str(p): (1 if profile == 'recover' and p == 0 else 0) for p in range(workers)}
    exits = [line for line in log.splitlines() if 'research worker process exit' in line]
    if profile == 'recover':
        assert len(exits) == 1 and 'attempt=0 gather_batches_before_exit=1' in exits[0]
        assert recovery['worker_failures_detected'] == recovery['recovery_attempts'] == recovery['recovered_partitions'] == 1
        assert recovery['full_job_restart_avoided']
        assert recovery['planner_reason'] == 'single_final_worker_loss_recompute_file_lineage_no_durable_copy'
        plan = recovery['plan']
        assert plan['job'] == original_job == 0 and plan['partition'] == 0 and plan['host'] == 1
        assert plan['reused_partitions'] == list(range(1, workers))
        assert len(plan['closures']) == len(plan['ranges']) == workers
        assert recovery['replayed_producer_tasks'] == workers
        assert recovery['replayed_morsels'] == len(manifest['files'])
        assert metrics['finals']['0']['sent']['batches'] > 1
        assert len(recovery['replay_manifests']) == workers
        for p, (closure, lost, replay) in enumerate(zip(plan['closures'], plan['ranges'], recovery['replay_manifests'])):
            original = producers[str(p)]['manifests']['0']
            assert closure['producer'] == p and closure['original'] == original
            assert 'morsels' not in closure  # RecoverFinal references the prepared snapshot.
            assert len(manifest['files'][p::workers]) == recovery['replayed_morsels'] // workers
            assert lost == {'transfer': original['logical'], 'start': 0, 'end_exclusive': original['segments']}
            assert replay == dict(original, attempt=1)
        assert recovery['lost_segments'] == recovery['recomputed_segments'] == sum(c['original']['segments'] for c in plan['closures'])
        assert recovery['lost_segments'] > workers  # Multi-segment coverage.
        assert recovery['lost_payload_bytes'] == recovery['recomputed_payload_bytes']
        assert metrics['finals']['0']['retention']['local_materialized_files'] == workers
    else:
        assert not exits and recovery['plan'] is None
        assert recovery['recovery_attempts'] == recovery['worker_failures_detected'] == 0
    # Successful resident state drains; recovery uses local materialization.
    for partition, task in metrics['finals'].items():
        received = task['received']
        for key in ['transfers', 'batches', 'rows', 'payload_bytes']:
            assert received[key] == sum(p['destinations'][partition][key] for p in producers.values())
        retained = task['retention']
        assert retained['current_buffer_bytes'] == retained['current_spill_backlog_bytes'] == 0
        if mode == 'adaptive' and not (profile == 'recover' and partition == '0'):
            adaptive = retained['adaptive']
            assert len(adaptive['transitions']) <= 3 and adaptive['extents'] <= 4
            assert sum(adaptive['segments_by_tier']) == task['received']['batches']
    summary[case] = {'oracle': oracle, 'attempts': recovery['committed_attempts'],
                     'replayed_morsels': recovery['replayed_morsels'],
                     'lost_segments': recovery['lost_segments'],
                     'recovery_elapsed_ms': recovery['recovery_elapsed_ms']}
negative = load('unrecoverable-result.json')
assert not negative['valid'] and negative['iterations'][0]['outcome'] is None
for key in ['repository', 'build', 'environment']:
    assert negative[key] == identity[key]
assert negative['dataset'] == manifest and negative['workload'] == workload
assert negative['engine']['configuration']['recovery_enabled'] == 'false'
assert 'unrecoverable' in negative['iterations'][0]['error']
assert 'missing source snapshot' in negative['iterations'][0]['error']
log = (root / 'unrecoverable-metrics.log').read_text()
assert log.count('research worker process exit') == 1
assert 'unrecoverable' in log and 'missing source snapshot' in log
assert 'ADE distributed execution metrics=' not in log
print(json.dumps({'source_commit': commit, 'cases': summary, 'negative': 'missing_source_snapshot_rejected'}, indent=2))
