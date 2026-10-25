#!/usr/bin/env python3
"""Identity/lease/terminal correctness first; frozen local timing guards second."""
import hashlib, json, os, pathlib, statistics, subprocess, sys, tempfile, uuid

class VerificationError(RuntimeError):
    pass

def require(condition, context):
    if not condition:
        raise VerificationError(str(context))

def check_source_identity(root, commit, runtime):
    identity = json.loads((root / 'source-identity.json').read_text())
    require(identity['schema_version'] == 1, 'unsupported source identity schema')
    require(identity['source_commit'] == commit, 'source commit differs from release identity')
    require(identity['runtime_tree'] == runtime, 'runtime tree differs from release identity')
    digest = identity['source_archive_sha256']
    require(len(digest) == 64, 'invalid source archive digest')
    archived_source = root / 'source.tar'
    source_bundle = root / 'source.bundle'
    if source_bundle.is_file():
        with tempfile.TemporaryDirectory(prefix='ade-phase10-source-check-') as directory:
            repository = pathlib.Path(directory) / 'source.git'
            subprocess.run(
                ['git', 'clone', '--bare', str(source_bundle), str(repository)],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            resolved = subprocess.check_output(
                ['git', '-C', str(repository), 'rev-parse', commit], text=True
            ).strip()
            tree = subprocess.check_output(
                ['git', '-C', str(repository), 'rev-parse', f'{commit}:crates'], text=True
            ).strip()
            require(resolved == commit and tree == runtime, 'source bundle commit/tree mismatch')
            source = subprocess.check_output(
                ['git', '-C', str(repository), 'archive', '--format=tar', commit]
            )
    elif archived_source.is_file():
        source = archived_source.read_bytes()
    else:
        resolved = subprocess.check_output(['git', 'rev-parse', commit], text=True).strip()
        tree = subprocess.check_output(['git', 'rev-parse', f'{commit}:crates'], text=True).strip()
        require(resolved == commit and tree == runtime, 'local source commit/tree mismatch')
        source = subprocess.check_output(['git', 'archive', '--format=tar', commit])
    require(hashlib.sha256(source).hexdigest() == digest, 'source archive digest mismatch')
    if archived_source.is_file():
        require(hashlib.sha256(archived_source.read_bytes()).hexdigest() == digest, 'bundled source archive digest mismatch')
    bundled_identity = root / 'implementation-identity.json'
    if bundled_identity.is_file():
        require(json.loads(bundled_identity.read_text()) == identity, 'implementation identity mismatch')

def check_record(record, run, manifest, slot, warmup, pids, worker_bin, dataset_root):
    n = int(run['engine']['configuration']['workers'])
    config = run['engine']['configuration']
    leased = config['scheduler_policy'] != 'static'
    require(record['sidecar_version'] == (3 if leased else 2), 'assertion failed at line 7')
    require(record['run_id'] == run['run_id'] and record['iteration_index'] == slot and (record['warmup'] == warmup), 'assertion failed at line 8')
    require(record['effective_config'] == config, 'effective config binding')
    pid = record['process_id']
    require(isinstance(pid, int) and pid > 0 and (pid not in pids), 'assertion failed at line 10')
    pids.add(pid)
    source = record['source']
    require(source['version'] == 1 and source['replayable'] == (not leased), 'assertion failed at line 11')
    require(source['manifest_fingerprint'] == run['dataset_manifest_fingerprint'], 'assertion failed at line 12')
    require(source['schema_fingerprint'] == manifest['schema_fingerprint'] and source['logical_fingerprint'] == manifest['logical_fingerprint'], 'assertion failed at line 13')
    expected = [dict(id=i, path=str((dataset_root / f['relative_path']).resolve()), fingerprint=f['fingerprint'], bytes=f['bytes']) for (i, f) in enumerate(manifest['files'])]
    require(source['morsels'] == expected, 'manifest-derived source inventory')
    m = record['metrics']
    require(m['worker_count'] == n and m['exchange_mode'] == 'spill' and (m['topology'] is None) and (not m['inactive']) and (m['recovery']['recovery_attempts'] == 0), 'assertion failed at line 16')
    require(set(m['producers']) == set(m['finals']) == set(map(str, range(n))), 'assertion failed at line 17')
    require(m['gather']['transfers'] == n, 'assertion failed at line 18')
    for field in ['transfers', 'rows', 'payload_bytes', 'batches']:
        sent = sum((p['sent'][field] for p in m['producers'].values()))
        received = sum((f['received'][field] for f in m['finals'].values()))
        require(sent == received, field)
        if field == 'transfers':
            require(sent == n * n, 'assertion failed at line 21')
    require(sum((p['input_rows'] for p in m['producers'].values())) == manifest['row_count'], 'assertion failed at line 22')
    for role in ['producers', 'finals']:
        for (wid, task) in m[role].items():
            require(task['managed_peak'] <= int(config['memory_limit_bytes']), 'assertion failed at line 25')
            if role == 'producers':
                require(task['sent']['transfers'] == n and len(task['manifests']) == n, 'assertion failed at line 27')
                for (dest, terminal) in task['manifests'].items():
                    require(terminal['logical'] == dict(job=0, exchange=0, partition=int(dest), producer=int(wid)), 'assertion failed at line 29')
                    require(terminal['attempt'] == 0 and len(terminal['fingerprint']) == 64, 'assertion failed at line 30')
                    for (field, key) in [('segments', 'batches'), ('rows', 'rows'), ('payload_bytes', 'payload_bytes')]:
                        require(terminal[field] == task['destinations'][dest][key], 'assertion failed at line 31')
            else:
                require(task['executed_final'] and task['received']['transfers'] == n, 'assertion failed at line 33')
                require(task['retention']['current_buffer_bytes'] == task['retention']['current_spill_backlog_bytes'] == 0, 'assertion failed at line 34')
    if not leased:
        require(m['scheduler'] is None, 'assertion failed at line 36')
        for (w, p) in m['producers'].items():
            require(p['source_files'] == len(expected[int(w)::n]), 'assertion failed at line 37')
        return
    s = m['scheduler']
    require(s['source'] == source, 'assertion failed at line 39')
    uuid.UUID(s['fence']['execution'])
    require(s['fence']['job'] == 0 and s['fence']['epoch'] == 1, 'assertion failed at line 40')
    require(s['config']['policy'] == config['scheduler_policy'], 'assertion failed at line 41')
    require(s['task']['scheduler'] == s['config'] and s['task']['partial']['mode'] == 'fixed_hash' and (s['task']['topology']['policy'] == 'static'), 'assertion failed at line 42')
    for key in ['memory_limit_bytes', 'batch_size', 'prefetch_batches']:
        require(str(s['task'][key]) == config[key], 'assertion failed at line 43')
    for (key, value) in s['config'].items():
        if key == 'recovery_enabled':
            require(not value, 'assertion failed at line 45')
        else:
            declared = config['scheduler_' + key]
            if isinstance(value, str):
                require(value == declared, 'assertion failed at line 48')
            else:
                require(value == json.loads(declared), 'assertion failed at line 49')
    subprocess.run([str(worker_bin), '--verify-scheduler'], input=json.dumps(s).encode(), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    for (wid, p) in m['producers'].items():
        entries = [e for e in s['entries'] if e['lease']['worker'] == int(wid)]
        require(p['source_files'] == len(entries), 'assertion failed at line 53')
        for field in ['input_rows', 'output_rows']:
            require(p[field] == sum((e['completion'][field] for e in entries)), 'assertion failed at line 54')
        require(p['sent']['rows'] == p['output_rows'], 'assertion failed at line 55')
        require(p['production']['source_bytes'] == sum((e['morsel']['bytes'] for e in entries)), 'assertion failed at line 56')
    if config['scheduler_policy'] == 'leased_no_steal':
        require(all((e['preferred'] == e['lease']['worker'] for e in s['entries'])), 'assertion failed at line 57')
    require(sum((e['completion']['input_rows'] for e in s['entries'])) == manifest['row_count'], 'assertion failed at line 58')

def verify(root):
    root = pathlib.Path(root)
    load = lambda f: json.loads((root / f).read_text())
    identity = load('source-identity.json')
    check_source_identity(root, identity['source_commit'], identity['runtime_tree'])
    binary = pathlib.Path(os.environ.get('ADE_BENCH_BIN', 'target/release/ade-bench')).resolve()
    worker = binary.with_name('ade-worker')
    require(load('binary-sha256.json') == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in [binary, worker]}, 'binary identity')
    statuses = load('statuses.json')
    inventory = load('inventory.json')
    pids = set()
    summaries = {}
    environment = None
    datasets = {}
    expected = {f'{r}-{n}-{m}' for n in [2, 4] for (r, m) in [('imbalanced', 'static'), ('imbalanced', 'leased_no_steal'), ('imbalanced', 'leased_v1'), ('uniform', 'static'), ('uniform', 'leased_v1')]} | {'capacity-unweighted', 'capacity-weighted', 'participation'}
    require({i['stem'] for i in inventory} == expected and len(inventory) == 13, 'assertion failed at line 68')
    require(set(statuses) == expected | {'negative-exit', 'negative-expiry', 'negative-notice'}, 'assertion failed at line 69')
    for item in inventory:
        stem = item['stem']
        require(statuses[stem] == 0, stem)
        run = load(stem + '-result.json')
        require(run['schema_version'] == 1 and run['valid'], 'assertion failed at line 72')
        subprocess.run([str(binary), 'check-result', str(root / (stem + '-result.json'))], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        require(run['repository'] == dict(git_commit=identity['source_commit'], git_dirty=False), 'assertion failed at line 74')
        require(run['build']['profile'] == 'release' and (not run['build']['debug_assertions']), 'assertion failed at line 75')
        current = [run['build'], run['environment']]
        if environment is None:
            environment = current
        require(current == environment, 'assertion failed at line 78')
        manifest = load(item['regime'] + '-manifest-stdout.log')
        require(run['dataset'] == manifest, 'assertion failed at line 79')
        expected_weights = [15, 1, 1, 1] * 8 if item['regime'] == 'imbalanced' else []
        require(manifest['synthetic'].get('file_row_weights', []) == expected_weights, 'assertion failed at line 81')
        require(manifest['row_count'] == 4000000 and manifest['synthetic']['files'] == 32 and (manifest['synthetic']['seed'] == 42) and (manifest['synthetic']['key_cardinality'] == 1024) and (manifest['synthetic']['distribution'] == 'uniform'), 'assertion failed at line 82')
        previous = datasets.setdefault(item['regime'], run['dataset_manifest_fingerprint'])
        require(previous == run['dataset_manifest_fingerprint'], 'assertion failed at line 83')
        oracle = load(item['regime'] + '-oracle-stdout.log')
        require(oracle['validated'], 'assertion failed at line 84')
        c = run['engine']['configuration']
        require(run['engine']['name'] == 'ade-distributed' and c['workers'] == str(item['workers']) and (c['scheduler_policy'] == item['policy']), 'assertion failed at line 85')
        require(c['control_protocol'] == 'ADETCP12' and c['hash_version'] == '1' and (c['exchange_wire_version'] == '2'), 'assertion failed at line 86')
        require(c['aggregate_policy'] == 'fixed_hash' and c['recovery_enabled'] == 'false' and (c['partition_policy'] == 'static') and (c['exchange_mode'] == 'spill') and (c['exchange_buffer_bytes'] == '1048576') and (c['memory_limit_bytes'] == '33554432') and (c['batch_size'] == '8192') and (c['prefetch_batches'] == '0'), 'assertion failed at line 87')
        for opt in item['options']:
            (key, value) = opt.split('=', 1)
            if key == 'scheduler_capacities':
                require(json.loads(c[key]) == list(map(int, value.split(','))), 'assertion failed at line 91')
            elif key == 'scheduler_participation':
                require(json.loads(c[key]) == json.loads(value), 'assertion failed at line 92')
            else:
                require(c[key] == value, 'assertion failed at line 93')
        count = 5 if item['workers'] == 2 else 3
        require(run['run_config']['warmups'] == 1 and run['run_config']['iterations'] == count and (run['run_config']['dataset_verification'] == 'full'), 'assertion failed at line 95')
        require(len(run['warmups']) == 1 and len(run['iterations']) == count, 'assertion failed at line 96')
        records = [json.loads(l) for l in (root / (stem + '-metrics.jsonl')).read_text().splitlines()]
        iters = run['warmups'] + run['iterations']
        require(len(records) == len(iters) == count + 1, 'assertion failed at line 97')
        command = load(stem + '-command.json')
        dataset_root = pathlib.Path(command[command.index('--dataset') + 1])
        samples = []
        for (index, (it, record)) in enumerate(zip(iters, records)):
            require(it['warmup'] == (index == 0) and it['exit_code'] == 0 and (not it['error']), 'assertion failed at line 101')
            out = it['outcome']
            require(out['validated'] and out['engine'] == run['engine'] and (out['result_fingerprint'] == oracle['result_fingerprint']) and (out['rows_output'] == oracle['rows_output']), 'assertion failed at line 102')
            check_record(record, run, manifest, it['index'], index == 0, pids, worker, dataset_root)
            m = record['metrics']
            require(m['gather']['rows'] == out['rows_output'], 'assertion failed at line 104')
            s = m['scheduler']
            if stem.startswith('imbalanced-') and item['policy'] == 'leased_v1':
                require(any((e['preferred'] != e['lease']['worker'] for e in s['entries'])), 'assertion failed at line 106')
            if stem == 'participation':
                require(m['producers']['3']['source_files'] == 0 and m['producers']['2']['source_files'] > 0, 'assertion failed at line 108')
                notice = s['events'][1]
                require(notice['in_flight'] is not None and notice['drained_ms'] is not None, 'assertion failed at line 109')
                require(all((e['lease']['worker'] != 0 or e['granted_sequence'] <= notice['sequence'] for e in s['entries'])), 'assertion failed at line 110')
            if index:
                samples.append(dict(query_ns=out['query_time_ns'], max_producer_completion_ns=max((t['coordinator_finished_ns'] for t in m['producers'].values())), producer_files=[m['producers'][str(w)]['source_files'] for w in range(item['workers'])], producer_bytes=[m['producers'][str(w)]['production']['source_bytes'] for w in range(item['workers'])], sent_rows=sum((t['sent']['rows'] for t in m['producers'].values())), sent_bytes=sum((t['sent']['payload_bytes'] for t in m['producers'].values())), control_messages=s['control_messages'] if s else 0, control_bytes=s['control_bytes'] if s else 0, renewals=s['renewals'] if s else 0, rss_bytes=it['peak_rss_bytes']))
        summaries[stem] = dict(samples=samples, median_query_ns=statistics.median((s['query_ns'] for s in samples)), median_max_producer_completion_ns=statistics.median((s['max_producer_completion_ns'] for s in samples)), query_range_ns=[min((s['query_ns'] for s in samples)), max((s['query_ns'] for s in samples))])
    for stem in ['negative-exit', 'negative-expiry', 'negative-notice']:
        require(statuses[stem] != 0 and (root / (stem + '-stderr.log')).read_text().strip(), 'assertion failed at line 115')
        stdout = (root / (stem + '-stdout.log')).read_text().strip()
        if stdout:
            try:
                require(not json.loads(stdout).get('validated', False), 'assertion failed at line 118')
            except json.JSONDecodeError:
                pass
    require(len(pids) == 62, 'assertion failed at line 120')
    gates = {}
    for n in [2, 4]:
        for (label, regime, key, limit) in [('imbalanced_job', 'imbalanced', 'median_query_ns', 0.9), ('imbalanced_tail', 'imbalanced', 'median_max_producer_completion_ns', 0.9), ('uniform_job', 'uniform', 'median_query_ns', 1.15)]:
            ratio = summaries[f'{regime}-{n}-leased_v1'][key] / summaries[f'{regime}-{n}-static'][key]
            gates[f'{label}-{n}'] = dict(ratio=ratio, limit=limit, passed=ratio <= limit)
    return dict(correctness_passed=True, passed=all((g['passed'] for g in gates.values())), children=62, measured=49, gates=gates, summaries=summaries, source=identity)
if __name__ == '__main__':
    root = pathlib.Path(sys.argv[1])
    try:
        result = verify(root)
    except Exception as error:
        result = dict(correctness_passed=False, passed=False, error=f'{type(error).__name__}: {error}')
    (root / 'verification.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for (k, v) in result.items() if k != 'summaries'}, indent=2))
    sys.exit(0 if result['passed'] else 1)
