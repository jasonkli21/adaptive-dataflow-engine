#!/usr/bin/env bash
# Release correctness/mechanism evidence, with no timing threshold or cloud credentials.
set -euo pipefail
bin=${ADE_BENCH_BIN:-target/release/ade-bench}
data=${ADE_PHASE4_DATA:-benchmarks/datasets/phase4-acceptance-20261016}
results=${ADE_PHASE4_RESULTS:-benchmarks/results/phase4-acceptance-20261016}
mkdir -p "$results"
"$bin" env --format json > "$results/environment.json"
"$bin" data generate synthetic --dataset-id phase4-uniform-v1 --rows 100000 --files 4 --row-group-rows 8192 --key-cardinality 10000 --seed 42 --output "$data" > "$results/manifest.json"
spec=benchmarks/specs/aggregate-high-cardinality.toml
"$bin" validate --engine datafusion --spec "$spec" --dataset "$data" > "$results/datafusion.json"
for workers in 2 4; do
  for mode in direct buffered spill local_materialized object_store; do
    if [[ $workers == 4 && $mode != direct && $mode != object_store ]]; then continue; fi
    buffer=1048576
    if [[ $mode == spill ]]; then buffer=1; fi
    options=(--engine ade-distributed --engine-option "workers=$workers" --engine-option "exchange_mode=$mode" --engine-option "exchange_buffer_bytes=$buffer" --engine-option memory_limit_bytes=2097152 --engine-option prefetch_batches=1)
    stem="$results/$workers-$mode"
    ADE_LOG=debug "$bin" validate "${options[@]}" --spec "$spec" --dataset "$data" > "$stem-validation.json" 2> "$stem-metrics.log"
    "$bin" run "${options[@]}" --spec "$spec" --dataset "$data" --verify-data full --warmups 1 --iterations 3 --output "$stem-result.json" > "$stem-run.log"
    "$bin" check-result "$stem-result.json" > "$stem-check.json"
  done
done
python3 - "$results" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
fingerprint = json.loads((root/'datafusion.json').read_text())['result_fingerprint']
summary = []
for path in sorted(root.glob('*-metrics.log')):
    name = path.name.removesuffix('-metrics.log')
    workers, mode = name.split('-',1)
    n = int(workers)
    validation = json.loads((root/f'{name}-validation.json').read_text())
    assert validation['result_fingerprint'] == fingerprint
    result = json.loads((root/f'{name}-result.json').read_text())
    assert result['valid'] and result['engine']['configuration']['exchange_mode'] == mode
    for iteration in result['warmups'] + result['iterations']:
        assert iteration['outcome']['validated'] and iteration['outcome']['result_fingerprint'] == fingerprint
    if mode == 'object_store':
        assert result['engine']['configuration']['object_store_backend'] == 'local_filesystem_ephemeral'
    line = next(line for line in path.read_text().splitlines() if 'ADE distributed execution' in line)
    m = json.JSONDecoder().raw_decode(line.split('metrics=',1)[1])[0]
    assert m['exchange_mode'] == mode and m['worker_count'] == n
    sent = {k:sum(t['sent'][k] for t in m['producers'].values()) for k in ['transfers','batches','rows','payload_bytes']}
    accepted = {k:sum(t['received'][k] for t in m['finals'].values()) for k in sent}
    assert sent == accepted and sent['transfers'] == n*n and sent['batches'] > n*n
    assert sent['rows'] == sum(t['input_rows'] for t in m['finals'].values())
    assert m['gather']['rows'] == 10000
    retention = [t['retention'] for t in m['finals'].values()]
    for r in retention:
        assert r['peak_buffer_bytes'] <= (1 if mode == 'spill' else 1048576)
        if mode in ['direct','buffered']:
            assert r['peak_buffer_bytes'] > 0
            assert r['spill_write_bytes']+r['local_materialized_bytes']+r['object_write_bytes'] == 0
        elif mode == 'spill':
            assert r['spill_write_bytes'] == r['spill_read_bytes'] > 0
            assert r['local_materialized_bytes']+r['object_write_bytes'] == 0
        elif mode == 'local_materialized':
            assert r['local_materialized_files'] == n and r['local_materialized_bytes'] > 0
            assert r['spill_write_bytes']+r['object_write_bytes'] == 0
        else:
            assert r['object_write_bytes'] == r['object_read_bytes'] > 0 and r['object_count'] > n
            assert r['spill_write_bytes']+r['local_materialized_bytes'] == 0
    if mode == 'object_store':
        assert sum(r['object_count'] for r in retention) == accepted['batches']
        assert sum(r['object_write_bytes'] for r in retention) == accepted['payload_bytes']
    summary.append(dict(workers=n,mode=mode,fingerprint=fingerprint,elapsed_ms=m['elapsed_ms'],**sent,
        peak_buffer_bytes=max(r['peak_buffer_bytes'] for r in retention),
        **{k:sum(r[k] for r in retention) for k in ['spill_write_bytes','spill_read_bytes','local_materialized_bytes','local_materialized_files','object_write_bytes','object_read_bytes','object_count']}))
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
PY
