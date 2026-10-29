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
        require(row['warmup'] == (r == 0) and row['evidence_version'] == 1, 'warmup/version')
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
            rates = receipt['effective']['rates']
            require({k: rates[k] for k in ['put_nano_usd', 'head_nano_usd', 'get_nano_usd', 'delete_nano_usd', 'egress_byte_nano_usd', 'retention_ms']} ==
                    {'put_nano_usd': 5000, 'head_nano_usd': 400, 'get_nano_usd': 400, 'delete_nano_usd': 0, 'egress_byte_nano_usd': 1, 'retention_ms': 60000}, 'predeclared prices')
            require(all(rates[k] == 0 for k in ['write_byte_nano_usd', 'read_byte_nano_usd', 'storage_byte_ms_nano_usd']), 'undeclared byte/capacity price')
            cost = c['put']*5000 + (c['head']+c['get'])*400 + c['read_bytes']
            require(receipt['estimated_nano_usd'] == cost, 'configured cost reconstruction')
            execution = context['execution']
            calls = [q for q in trace if execution in q['path'] or any(execution in k for k in q.get('keys', []))]
            require(calls and all(q['fault'] is None for q in calls), 'uncontrolled/faulted cost sample')
            require(all(q['status'] in [200, 204, 206, 412] for q in calls), 'unsuccessful sample request')
            total = {k: c[k]+gc['counters'][k] for k in c}
            count = lambda method: sum(q['method'] == method for q in calls)
            require((count('PUT'), count('HEAD'), count('GET'), count('POST')) ==
                    tuple(total[k] for k in ['put', 'head', 'get', 'delete']), 'actual PUT/HEAD/GET/DeleteObjects request conservation')
            require(len(calls) == total['http_requests'], 'actual HTTP attempt count')
            require(sum(q['sent_body_bytes'] for q in calls if q['method'] == 'PUT') == total['write_bytes'], 'actual PUT object body bytes')
            require(sum(q['received_body_bytes'] for q in calls if q['method'] == 'GET') == total['read_bytes'], 'actual GET object body bytes')
            require(sum(q['sent_body_bytes'] for q in calls) == total['http_request_body_bytes'], 'actual HTTP request body bytes')
            require(sum(q['received_body_bytes'] for q in calls) == total['http_response_body_bytes'], 'actual HTTP response body bytes')
            require(gc['applied'] is True and gc['counters']['delete'] == len(gc['keys']), 'quiesced exact-key cleanup')
    require(seen == expected, 'missing samples')
    require(not any('/never-follow' in q['path'] for q in trace), 'redirect was followed')
    summary = []
    for d in ['uniform', 'hot-key']:
        for n in [2, 4]:
            local, remote = (grouped[(d, n, m)] for m in ['local_materialized', 'remote_object'])
            summary.append({'distribution': d, 'workers': n,
                            'local_ns': {'median': statistics.median(local), 'min': min(local), 'max': max(local), 'all': local},
                            'remote_ns': {'median': statistics.median(remote), 'min': min(remote), 'max': max(remote), 'all': remote},
                            'remote_over_local': statistics.median(remote)/statistics.median(local)})
    return {'status': 'PASS', 'samples': 32, 'measured': 24, 'summary': summary}


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
