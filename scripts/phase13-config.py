#!/usr/bin/env python3
"""Create a credential-free remote_object run from the bounded native frontend IR."""
import argparse
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--query-binary', type=Path, required=True)
    parser.add_argument('--worker-binary', type=Path, required=True)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--prefix', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--dev-loopback-http', action='store_true')
    parser.add_argument('--workers', type=int, choices=[2, 3, 4], default=2)
    parser.add_argument('--readers', type=int)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    dataset = args.dataset.resolve(strict=True)
    process = subprocess.run([str(args.query_binary.resolve(strict=True)), 'plan-sql', '--dataset', str(dataset),
                              '--target', 'distributed-aggregate', '--max-output-rows', '1000000',
                              '--max-output-bytes', '67108864', '--sql',
                              'SELECT key, SUM(value_i64) AS sum, COUNT(*) AS count FROM input GROUP BY key'],
                             text=True, capture_output=True, check=True)
    if len(process.stdout) > 1024 * 1024:
        raise ValueError('frontend output cap')
    plan, end = json.JSONDecoder().raw_decode(process.stdout.lstrip())
    if not process.stdout.lstrip()[end:].startswith('\nplan_hash='):
        raise ValueError('unexpected frontend output envelope')
    # The fixed SQL planner may rename aggregate expressions then restore their
    # aliases. Remove the composed identity only when every ordinal/type survives.
    if len(plan['nodes']) > 2:
        base = plan['nodes'][1]
        for i, node in enumerate(plan['nodes'][2:], 2):
            op = node['operator']
            if op.get('op') != 'project' or op.get('input') != i-1 or [c['column'] for c in op.get('columns', [])] != [0, 1, 2]:
                raise ValueError('remote v1 refuses transformed or reordered projections')
            if [(f['data_type'], f['nullable']) for f in node['schema']] != [(f['data_type'], f['nullable']) for f in base['schema']]:
                raise ValueError('remote v1 refuses changed projection types')
        if plan['nodes'][-1]['schema'] != base['schema']:
            raise ValueError('remote v1 requires the original aggregate output names')
        plan['nodes'] = plan['nodes'][:2]
        plan['output'] = 1
    if plan['nodes'][1]['operator'] != {'op': 'group_sum_count', 'input': 0, 'key': 1, 'value': 3}:
        raise ValueError('remote v1 requires original static aggregate')
    args.scratch.mkdir(mode=0o700, parents=False, exist_ok=True)
    scratch = args.scratch.resolve(strict=True)
    if scratch.stat().st_mode & 0o777 != 0o700:
        raise ValueError('scratch must have mode 0700')
    request = {'job': {'version': 1, 'job': 1, 'plan': plan,
                       'files': [str((dataset/f['name']).resolve(strict=True)) for f in plan['sources'][0]['files']],
                       'manifest': str((dataset/'manifest.json').resolve(strict=True)),
                       'workers': args.workers, 'readers': args.readers or args.workers,
                       'store': {'endpoint': args.endpoint, 'bucket': args.bucket, 'prefix': args.prefix,
                                 'region': args.region, 'dev_allow_http': args.dev_loopback_http},
                       'limits': {'segments': 256, 'write_bytes': 16777216, 'objects': 272, 'requests': 4096,
                                  'timeout_ms': 120000, 'io_timeout_ms': 5000, 'read_retries': 1},
                       'rates': {'worker_placement': 'unspecified-compute', 'store_placement': 'unspecified-store',
                                 'put_nano_usd': 0, 'head_nano_usd': 0, 'get_nano_usd': 0, 'delete_nano_usd': 0,
                                 'write_byte_nano_usd': 0, 'read_byte_nano_usd': 0, 'egress_byte_nano_usd': 0,
                                 'storage_byte_ms_nano_usd': 0, 'retention_ms': 60000}},
               'worker_binary': str(args.worker_binary.resolve(strict=True)), 'scratch_parent': str(scratch)}
    with args.output.open('x') as output:
        json.dump(request, output, indent=2)
        output.write('\n')
    print(args.output.resolve())

if __name__ == '__main__':
    main()
