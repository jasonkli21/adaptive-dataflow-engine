#!/usr/bin/env python3
"""Verify every predeclared Phase 13 sample against independent service traffic.

No best-run selection, inferred topology prices, credentials or paid-provider API.
"""
import argparse
import json
from pathlib import Path
import statistics
import subprocess


def require(ok, message):
    if not ok:
        raise ValueError(message)


def verify_remote_cost(receipt, cleanup, evidence_version):
    rates = receipt['effective']['rates']
    counters = receipt['counters']
    if evidence_version == 1:
        require({k: rates[k] for k in ['put_nano_usd', 'head_nano_usd', 'get_nano_usd', 'delete_nano_usd', 'egress_byte_nano_usd', 'retention_ms']} ==
                {'put_nano_usd': 5000, 'head_nano_usd': 400, 'get_nano_usd': 400, 'delete_nano_usd': 0, 'egress_byte_nano_usd': 1, 'retention_ms': 60000}, 'historical predeclared prices')
        require(all(rates[k] == 0 for k in ['write_byte_nano_usd', 'read_byte_nano_usd', 'storage_byte_ms_nano_usd']), 'historical undeclared byte/capacity price')
        cost = counters['put'] * 5000 + (counters['head'] + counters['get']) * 400 + counters['read_bytes']
        require(receipt['estimated_nano_usd'] == cost, 'historical configured execution cost reconstruction')
        return

    require(evidence_version == 2, 'unknown campaign evidence version')
    require({k: rates[k] for k in ['put_nano_usd', 'head_nano_usd', 'get_nano_usd', 'delete_nano_usd', 'egress_byte_nano_usd', 'storage_byte_ms_nano_usd', 'retention_ms']} ==
            {'put_nano_usd': 5000, 'head_nano_usd': 400, 'get_nano_usd': 400, 'delete_nano_usd': 3, 'egress_byte_nano_usd': 1, 'storage_byte_ms_nano_usd': 2, 'retention_ms': 60000}, 'remediation price matrix')
    require(rates['write_byte_nano_usd'] == 0 and rates['read_byte_nano_usd'] == 0, 'unexpected request-byte price')
    execution_cost = (counters['put'] * rates['put_nano_usd']
                      + counters['head'] * rates['head_nano_usd']
                      + counters['get'] * rates['get_nano_usd']
                      + counters['delete'] * rates['delete_nano_usd']
                      + counters['read_bytes'] * rates['egress_byte_nano_usd'])
    require(receipt['estimated_nano_usd'] == execution_cost, 'pre-cleanup execution proxy')
    gc_counters = cleanup['counters']
    total = {key: counters[key] + gc_counters[key] for key in counters}
    retained = cleanup['verified_retained_bytes']
    require(cleanup['assumed_retention_ms'] == rates['retention_ms'], 'retention assumption identity')
    require(retained > 0, 'verified retained object bytes required')
    lifecycle_cost = (total['put'] * rates['put_nano_usd']
                      + total['head'] * rates['head_nano_usd']
                      + total['get'] * rates['get_nano_usd']
                      + total['delete'] * rates['delete_nano_usd']
                      + total['read_bytes'] * rates['egress_byte_nano_usd']
                      + retained * rates['retention_ms'] * rates['storage_byte_ms_nano_usd'])
    require(cleanup['post_cleanup_lifecycle_proxy_nano_usd'] == lifecycle_cost,
            'post-cleanup lifecycle proxy reconstruction')


def verify(samples, trace):
    require(len(samples) == 32, 'all 32 warmup/measured executions required')
    expected = {(d, n, m, r) for d in ['uniform', 'hot-key'] for n in [2, 4]
                for m in ['local_materialized', 'remote_object'] for r in range(4)}
    seen, sources, fingerprints, grouped = set(), {}, {}, {}
    require(len({r['coordinator_pid'] for r in samples}) == 32, 'fresh coordinator per sample required')
    for row in samples:
        d, n, m, r = (row[k] for k in ['distribution', 'workers', 'mode', 'repetition'])
        key = (d, n, m, r)
        require(key in expected and key not in seen, 'duplicate/foreign sample')
        seen.add(key)
        require(row['warmup'] == (r == 0) and row['evidence_version'] in [1, 2], 'warmup/version')
        v = row['validation']
        require(v['schema_version'] == 1 and v['validated'] is True, 'independent oracle required')
        identity = (row['manifest_hash'], row['plan_hash'])
        require(sources.setdefault(d, identity) == identity, 'source/plan comparison mismatch')
        require(fingerprints.setdefault(d, v['result_fingerprint']) == v['result_fingerprint'], 'result mismatch')
        wall = row['execution']['native_wall_ns']
        require(isinstance(wall, int) and wall > 0, 'whole native wall required')
        if r:
            grouped.setdefault((d, n, m), []).append(wall)
        if m == 'remote_object':
            receipt, gc = row['execution']['remote'], row['cleanup']['observed']
            c, context = receipt['counters'], receipt['context']
            require(context['workers'] == n and receipt['effective']['readers'] == n, 'fixed topology')
            require(context['plan'] == row['plan_hash'] and receipt['effective']['plan']['sources'][0]['manifest'] == row['manifest_hash'], 'source-bound receipt')
            require(len(receipt['producers']) == n and len(receipt['readers']) == n, 'actual two cohorts')
            boots = receipt['producers'] + receipt['readers']
            require(len({b['instance'] for b in boots}) == 2*n and len({b['scratch'] for b in boots}) == 2*n, 'fresh boot/scratch')
            require(receipt['rate_provenance'] == 'CONFIGURED' and receipt['observation_provenance'] == 'MEASURED', 'provenance')
            verify_remote_cost(receipt, gc, row['evidence_version'])
            execution = context['execution']
            calls = [q for q in trace if execution in q['path'] or any(execution in k for k in q.get('keys', []))]
            require(calls and all(q['fault'] is None for q in calls), 'uncontrolled/faulted cost sample')
            require(all(q['status'] in [200, 204, 206, 412] for q in calls), 'unsuccessful sample request')
            total = {k: c[k]+gc['counters'][k] for k in c}
            count = lambda method: sum(q['method'] == method for q in calls)
            require((count('PUT'), count('HEAD'), count('GET'), count('POST')) ==
                    tuple(total[k] for k in ['put', 'head', 'get', 'delete']), 'actual PUT/HEAD/GET/DeleteObjects request conservation')
            deleted_keys = [key for call in calls if call['method'] == 'POST'
                            for key in call.get('keys', [])]
            capability_keys = [key for key in deleted_keys if key.endswith('/CAPABILITY')]
            require(len(capability_keys) == 1
                    and capability_keys[0].startswith(f"{receipt['effective']['store']['prefix']}/remote-v1/{execution}/attempt-0/"),
                    'exact capability probe delete trace')
            require(sorted(key for key in deleted_keys if not key.endswith('/CAPABILITY'))
                    == sorted(gc['keys']), 'exact cleanup key trace')
            require(len(calls) == total['http_requests'], 'actual HTTP attempt count')
            require(sum(q['sent_body_bytes'] for q in calls if q['method'] == 'PUT') == total['write_bytes'], 'actual PUT object body bytes')
            require(sum(q['received_body_bytes'] for q in calls if q['method'] == 'GET') == total['read_bytes'], 'actual GET object body bytes')
            require(sum(q['sent_body_bytes'] for q in calls) == total['http_request_body_bytes'], 'actual HTTP request body bytes')
            require(sum(q['received_body_bytes'] for q in calls) == total['http_response_body_bytes'], 'actual HTTP response body bytes')
            require(gc['applied'] is True and gc['counters']['delete'] == len(gc['keys']), 'quiesced exact-key cleanup')
            if row['evidence_version'] == 2:
                require(gc['verified_retained_bytes'] > 0
                        and gc['post_cleanup_lifecycle_proxy_nano_usd'] is not None,
                        'verified post-cleanup lifecycle estimate')
    require(seen == expected, 'missing samples')
    versions = {row.get('evidence_version') for row in samples}
    require(len(versions) == 1 and next(iter(versions)) in [1, 2], 'mixed/unknown campaign evidence versions')
    require(not any('/never-follow' in q['path'] for q in trace), 'redirect was followed')
    summary = []
    for d in ['uniform', 'hot-key']:
        for n in [2, 4]:
            local, remote = (grouped[(d, n, m)] for m in ['local_materialized', 'remote_object'])
            summary.append({'distribution': d, 'workers': n,
                            'local_ns': {'median': statistics.median(local), 'min': min(local), 'max': max(local), 'all': local},
                            'remote_ns': {'median': statistics.median(remote), 'min': min(remote), 'max': max(remote), 'all': remote},
                            'remote_over_local': statistics.median(remote)/statistics.median(local)})
    return {'schema_version': 2, 'status': 'PASS', 'samples': 32, 'measured': 24, 'summary': summary}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('samples', type=Path)
    parser.add_argument('requests', type=Path)
    parser.add_argument('--native-verifier', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    samples = json.loads(args.samples.read_text())
    trace = [json.loads(line) for line in args.requests.read_text().splitlines() if line]
    result = verify(samples, trace)
    for row in samples:
        if row['mode'] == 'remote_object':
            process = subprocess.run([str(args.native_verifier.resolve()), '--verify-receipt'], input=json.dumps(row['execution']['remote']), text=True, capture_output=True, check=False)
            require(process.returncode == 0, 'native receipt identity/cost verifier refused: ' + process.stderr)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()
