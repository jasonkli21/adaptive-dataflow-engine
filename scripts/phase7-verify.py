#!/usr/bin/env python3
"""Fixed Phase 7 criterion. Monetary scores use observations, never policy alternatives.

Suite win: expected aggregate empirical score strictly below both always and never
(and threshold); all oracle/identity/recovery checks pass. Normal-case overhead:
each regime's mean isolated expected query wall time <= 5x threshold's mean.
Two samples per success/failure arm; no best-iteration selection.
Prices are nano-USD; p is configured experiment probability. Source requests are
charged on replay; each restore segment uses HEAD + range GET (two requests).
Original work/source is charged once per execution. Staging wall time is charged
at the compute proxy rate too, conservatively, though it includes I/O waits.
SLO scores use observed whole-job wall time; this suite uses zero penalty. Unit
coverage establishes deadline/throughput flips independently.
"""
import json
import pathlib
import statistics
import sys
import uuid

root = pathlib.Path(sys.argv[1])
def load(name):
    return json.loads((root / name).read_text())
def metrics(name):
    return [json.loads(line)['metrics'] for line in (root / name).read_text().splitlines()]

regimes = {
    'low': (1000, 0, 1000, 1),
    'high': (900000, 1000, 1, 1),
    'expensive_restore': (500000, 1, 1, 100000),
}
policies = ['expected', 'always', 'never', 'threshold']
oracle = load('datafusion.json')['result_fingerprint']
manifest = load('manifest.json')
commit = (root / 'source-commit.txt').read_text().strip()
totals = dict.fromkeys(policies, 0)
summary = {}
identity = None

def observation_score(m, prices):
    _, source, write, read = prices
    score = 0
    for producer in m['producers'].values():
        production = producer['production']
        score += production['operator_work_ns'] + production['source_bytes'] * source
        for fact in producer['materializations'].values():
            score += fact['staging_write_ns'] + fact['staging_read_ns']
            receipt = fact['receipt']
            if receipt:
                score += receipt['write_bytes'] * write + receipt['write_requests'] * 1000
    r = m['recovery']
    score += r['recomputed_operator_work_ns'] + r['recomputed_source_bytes'] * source
    score += r['restored_payload_bytes'] * read + r['object_read_requests'] * 1000
    # Legacy threshold suffix objects remain delivery I/O, never complete copies.
    for task in m['finals'].values():
        retention = task['retention']
        score += retention['object_write_bytes'] * write + retention['object_count'] * 1000
        score += retention['object_read_bytes'] * read + retention['object_count'] * 2000
    return score

def ceil_div(a, b):
    return (a + b - 1) // b

def estimates(o, c):
    if o['output_segments']:
        replay_work = o['operator_work_ns']
        reread_bytes = o['source_bytes']
        reread_requests = o['source_requests']
    else:
        replay_work = reread_bytes = reread_requests = 0
    recompute_ms = ceil_div(replay_work, 1_000_000) + ceil_div(
        reread_bytes * 1000, c['source_read_bytes_per_second'])
    restore_ms = ceil_div(o['output_bytes'] * 1000, c['object_read_bytes_per_second'])
    write_ms = ceil_div(o['output_bytes'] * 1000, c['object_write_bytes_per_second'])
    recompute_base = (replay_work * c['compute_nano_usd_per_work_ns']
                      + reread_bytes * c['source_read_nano_usd_per_byte']
                      + reread_requests * c['source_read_nano_usd_per_request'])
    restore_base = (o['output_bytes'] * c['object_read_nano_usd_per_byte']
                    + 2 * o['output_segments'] * c['object_read_nano_usd_per_request'])
    write_base = (o['output_bytes'] * c['object_write_nano_usd_per_byte']
                  + o['output_segments'] * c['object_write_nano_usd_per_request']
                  + o['output_bytes'] * c['object_capacity_nano_usd_per_byte_ms']
                    * c['expected_retention_ms'])
    return recompute_base, restore_base, write_base, recompute_ms, restore_ms, write_ms

def penalty(c, elapsed_ms):
    return max(0, elapsed_ms - c['deadline_slack_ms']) * c['slo_penalty_nano_usd_per_ms']

def recovery_actions(m, closure, elapsed_ms):
    producer = m['producers'][str(closure['producer'])]
    fact = producer['materializations'].get(str(closure['original']['logical']['partition']))
    assert fact is not None
    o, c = fact['decision']['observation'], fact['decision']['config']
    recompute_base, restore_base, _, recompute_ms, restore_ms, _ = estimates(o, c)
    recompute = recompute_base + penalty(c, elapsed_ms + recompute_ms)
    restore = restore_base + penalty(c, elapsed_ms + restore_ms)
    return fact, recompute, restore, recompute_ms, restore_ms

for regime, prices in regimes.items():
    p, source, write, read = prices
    outcomes = {}
    walls = {}
    for policy in policies:
        arms = {}
        for profile in ['normal', 'loss']:
            stem = f'{regime}-{policy}-{profile}'
            result = load(f'{stem}-result.json')
            assert result['valid'] and result['schema_version'] == 1
            assert result['repository']['git_commit'] == commit and not result['repository']['git_dirty']
            current_identity = {k: result[k] for k in ['repository', 'build', 'environment', 'workload', 'dataset_manifest_fingerprint']}
            if identity is None:
                identity = current_identity
            assert identity == current_identity
            assert result['dataset'] == manifest
            assert result['run_config']['warmups'] == 0 and result['run_config']['iterations'] == 2
            assert len(result['iterations']) == 2
            assert load(f'{stem}-validation.json')['result_fingerprint'] == oracle
            ms = metrics(f'{stem}-metrics.jsonl')
            assert len(ms) == 2, (stem, len(ms))
            scores = []
            for iteration, m in zip(result['iterations'], ms):
                assert iteration['outcome']['validated'] and iteration['outcome']['result_fingerprint'] == oracle
                assert len(m['producers']) == len(m['finals']) == 2
                r = m['recovery']
                loss = profile == 'loss'
                assert r['worker_failures_detected'] == r['recovery_attempts'] == int(loss)
                assert r['committed_attempts'] == {'0': int(loss), '1': 0}
                assert r['recomputed_segments'] + r['restored_segments'] == r['lost_segments']
                assert r['recomputed_payload_bytes'] + r['restored_payload_bytes'] == r['lost_payload_bytes']
                run_namespaces = set()
                for producer in m['producers'].values():
                    assert len(producer['materializations']) == (0 if policy == 'threshold' else 2)
                    for d, fact in producer['materializations'].items():
                        terminal = producer['manifests'][d]
                        decision = fact['decision']
                        o = decision['observation']
                        c = decision['config']
                        assert decision['object_request_cost_scope'] == 'put_head_get_proxy_v1'
                        assert o['consumer_count'] == 1
                        assert o['source_bytes'] == producer['production']['source_bytes']
                        assert o['operator_work_ns'] == producer['production']['operator_work_ns']
                        assert o['output_bytes'] == terminal['payload_bytes']
                        assert o['output_segments'] == terminal['segments']
                        assert c['failure_probability_ppm'] == p
                        assert (c['source_read_nano_usd_per_byte'], c['object_write_nano_usd_per_byte'], c['object_read_nano_usd_per_byte']) == (source, write, read)
                        # Independently reconstruct estimates and compose the complete
                        # write-before-loss plus restore-after-loss deadline path.
                        recompute_base, restore_base, now, recompute_ms, restore_ms, write_ms = estimates(o, c)
                        recompute = recompute_base + penalty(c, recompute_ms)
                        restore = restore_base + penalty(c, write_ms + restore_ms)
                        persist_cost = (now * 1_000_000
                                        + (1_000_000 - p) * penalty(c, write_ms)
                                        + p * restore)
                        assert decision['recompute_ms'] == recompute_ms
                        assert decision['restore_ms'] == restore_ms
                        assert decision['write_ms'] == write_ms
                        assert decision['recompute_nano_usd'] == recompute
                        assert decision['restore_nano_usd'] == restore
                        assert decision['write_nano_usd'] == now
                        assert decision['no_persist_nano_usd_ppm'] == p * recompute
                        assert decision['persist_nano_usd_ppm'] == persist_cost
                        expected_persist = policy == 'always' or (policy == 'expected' and persist_cost < p * recompute)
                        assert decision['persist'] == expected_persist
                        assert (fact['receipt'] is not None) == expected_persist
                        assert fact['staging_write_bytes'] == terminal['payload_bytes']
                        if fact['receipt']:
                            receipt = fact['receipt']
                            assert receipt['original'] == terminal
                            assert receipt['backend']['durability'] == 'worker_process_surviving'
                            assert receipt['namespace'] == 'complete-v1'
                            parsed_namespace = str(uuid.UUID(receipt['execution_namespace']))
                            assert parsed_namespace == receipt['execution_namespace']
                            run_namespaces.add(parsed_namespace)
                            assert receipt['write_bytes'] == terminal['payload_bytes']
                            assert receipt['write_requests'] == terminal['segments']
                assert len(run_namespaces) <= 1
                if policy == 'expected':
                    decisions = [f['decision']['persist'] for t in m['producers'].values() for f in t['materializations'].values()]
                    assert all(decisions) if regime == 'high' else not any(decisions)
                if loss:
                    # Producer uploads finish before the injected loss. They overlap
                    # between producers but each producer emits destination copies in
                    # sequence, so seed recovery with the slowest producer's total.
                    write_ms_by_producer = []
                    for producer in m['producers'].values():
                        total_ns = sum(
                            fact['receipt']['write_elapsed_ns']
                            for fact in producer['materializations'].values()
                            if fact['receipt'])
                        write_ms_by_producer.append(ceil_div(total_ns, 1_000_000))
                    elapsed_ms = max(write_ms_by_producer, default=0)
                    expected_restored_segments = 0
                    replayed_morsels = 0
                    for closure in r['plan']['closures']:
                        producer = m['producers'][str(closure['producer'])]
                        fact = producer['materializations'].get(str(closure['original']['logical']['partition']))
                        if fact is None:  # Legacy threshold has no cost facts or copies.
                            assert closure['restore'] is None
                            if closure['original']['segments']:
                                replayed_morsels += 2
                            continue
                        fact, recompute_cost, restore_cost, recompute_ms, restore_ms = recovery_actions(m, closure, elapsed_ms)
                        should_restore = bool(fact['receipt'] and restore_cost < recompute_cost)
                        assert (closure['restore'] is not None) == should_restore
                        if should_restore:
                            expected_restored_segments += closure['original']['segments']
                            elapsed_ms += restore_ms
                        elif closure['original']['segments']:
                            replayed_morsels += 2
                            elapsed_ms += recompute_ms
                        else:
                            elapsed_ms += recompute_ms
                    assert r['restored_segments'] == expected_restored_segments
                    assert r['replayed_morsels'] == replayed_morsels
                    assert r['object_read_requests'] == r['restored_segments'] * 2
                    if regime == 'high' and policy in ['expected', 'always']:
                        assert r['restored_segments'] > 0 and r['replayed_morsels'] == r['recomputed_source_bytes'] == 0
                    for original, replay in zip(r['plan']['closures'], r['replay_manifests']):
                        expected = dict(original['original'], attempt=1)
                        assert replay == expected
                    if regime == 'expensive_restore' and policy == 'always':
                        assert all(c['restore'] is None and c['action_reason'] == 'valid_copy_recompute_cheaper_or_tie' for c in r['plan']['closures'])
                scores.append(observation_score(m, prices))
            if profile == 'loss':
                log = (root / f'{stem}-validation.log').read_text()
                assert log.count('gather_batches_before_exit=1') == 1
            arms[profile] = statistics.mean(scores)
            if profile == 'normal':
                walls[policy] = statistics.mean(i['outcome']['query_time_ns'] / 1_000_000 for i in result['iterations'])
        empirical = ((1_000_000 - p) * arms['normal'] + p * arms['loss']) / 1_000_000
        outcomes[policy] = {'empirical_expected_nano_usd': empirical, 'observed_arms_nano_usd': arms}
        totals[policy] += empirical
    ratio = walls['expected'] / walls['threshold']
    assert ratio <= 5, (regime, 'normal overhead bound exceeded', ratio)
    summary[regime] = {'outcomes': outcomes, 'normal_mean_query_ms': walls, 'normal_overhead_ratio': ratio}
assert all(totals['expected'] < totals[baseline] for baseline in ['always', 'never', 'threshold']), totals
verification = {'passed': True, 'source_commit': commit, 'criterion': 'oracle, regimes, aggregate below always/never/threshold, normal <=5x', 'object_request_cost_scope': 'put_head_get_proxy_v1', 'suite_totals_nano_usd': totals, 'regimes': summary}
(root / 'verification.json').write_text(json.dumps(verification, indent=2) + '\n')
print(json.dumps(verification, indent=2))
