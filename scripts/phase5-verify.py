"""Verify independently validated results, bounded transitions, and predeclared guards."""
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
fingerprint = json.loads((root / 'datafusion.json').read_text())['result_fingerprint']
summary = {}
for path in sorted(root.glob('*-result.json')):
    case = path.name.removesuffix('-result.json')
    workers, profile, mode = case.split('-')
    result = json.loads(path.read_text())
    assert result['valid']
    config = result['engine']['configuration']
    assert config['exchange_mode'] == mode
    assert config['research_delay_ms'] == ('750' if profile == 'slow' else '0')
    for iteration in result['warmups'] + result['iterations']:
        assert iteration['outcome']['validated']
        assert iteration['outcome']['result_fingerprint'] == fingerprint
    validation = json.loads((root / f'{case}-validation.json').read_text())
    assert validation['result_fingerprint'] == fingerprint
    log = (root / f'{case}-metrics.log').read_text()
    line = next(line for line in log.splitlines() if 'ADE distributed execution' in line)
    metrics = json.JSONDecoder().raw_decode(line.split('metrics=', 1)[1])[0]
    retention = {k: t['retention'] for k, t in metrics['finals'].items()}
    for partition, r in retention.items():
        assert r['current_buffer_bytes'] == r['current_spill_backlog_bytes'] == 0
        assert r['peak_buffer_bytes'] <= 1048576
        assert r['spill_write_bytes'] == r['spill_read_bytes']
        assert r['object_write_bytes'] == r['object_read_bytes']
        if mode == 'adaptive':
            a = r['adaptive']
            assert a['policy'] == 'threshold_v1' and a['extents'] <= 4
            assert len(a['transitions']) <= 3
            received = metrics['finals'][partition]['received']
            assert sum(a['segments_by_tier']) == received['batches']
            assert sum(a['bytes_by_tier']) == received['payload_bytes']
            for transition in a['transitions']:
                assert transition['transfer']['partition'] == int(partition)
    if profile == 'slow' and mode == 'adaptive':
        slow = retention['0']['adaptive']
        assert sum(n > 0 for n in slow['segments_by_tier']) >= 2
        assert any(t['new'] == 'spill' for t in slow['transitions'])
        assert any(len(r['adaptive']['transitions']) < len(slow['transitions'])
                   for p, r in retention.items() if p != '0'), 'partition histories did not diverge'
    summary[case] = {
        'median_ms': result['summary']['median'] / 1e6,
        'raw_ms': [i['outcome']['query_time_ns'] / 1e6 for i in result['iterations']],
        'retention': retention,
        'adaptive': metrics['adaptive'],
    }
assert len(summary) == 6
normal_ratio = summary['2-normal-adaptive']['median_ms'] / summary['2-normal-buffered']['median_ms']
slow_ratio = summary['2-slow-adaptive']['median_ms'] / summary['2-slow-direct']['median_ms']
output = {'fingerprint': fingerprint, 'normal_ratio': normal_ratio, 'slow_direct_ratio': slow_ratio, 'cases': summary}
(root / 'summary.json').write_text(json.dumps(output, indent=2) + '\n')
print(json.dumps({k: v for k, v in output.items() if k != 'cases'}, indent=2))
for case, row in summary.items():
    print(case, 'median_ms=', round(row['median_ms'], 3), 'raw_ms=', row['raw_ms'])
assert normal_ratio <= 1.25, 'predeclared normal guard failed'
assert slow_ratio < 1, 'predeclared adversarial comparison failed'
