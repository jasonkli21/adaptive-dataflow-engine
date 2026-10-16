#!/usr/bin/env bash
set -euo pipefail
bin=${ADE_BENCH_BIN:-target/debug/ade-bench}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
"$bin" env --format json > "$work/env.json"
"$bin" data generate synthetic --dataset-id ci-skew-v1 --rows 100000 --files 4 --row-group-rows 8192 --key-cardinality 10000 --distribution hot-key --output "$work/data" > "$work/manifest.json"
"$bin" data inspect "$work/data" > "$work/inspect.json"
for name in scan scan-filter-project aggregate-low-cardinality aggregate-high-cardinality aggregate-skew sort-key-rowid; do
  "$bin" validate --spec "benchmarks/specs/$name.toml" --dataset "$work/data" > "$work/$name-validation.json"
  "$bin" validate --engine ade-local --spec "benchmarks/specs/$name.toml" --dataset "$work/data" > "$work/$name-ade-validation.json"
done
"$bin" run --spec benchmarks/specs/aggregate-skew.toml --dataset "$work/data" --verify-data full --warmups 1 --iterations 1 --output "$work/result.json"
"$bin" check-result "$work/result.json"
"$bin" compare "$work/result.json" "$work/result.json" --format json
"$bin" run --engine ade-local --spec benchmarks/specs/aggregate-skew.toml --dataset "$work/data" --verify-data full --warmups 0 --iterations 1 --output "$work/ade-result.json"
"$bin" check-result "$work/ade-result.json"
"$bin" compare "$work/result.json" "$work/ade-result.json" --format json

# Constrained states must execute the local spill paths and retain oracle validation.
ADE_LOG=debug "$bin" validate --engine ade-local --engine-option memory_limit_bytes=2097152 --engine-option prefetch_batches=1 --spec benchmarks/specs/aggregate-skew.toml --dataset "$work/data" > "$work/spill-aggregate.json" 2> "$work/spill-aggregate.log"
ADE_LOG=debug "$bin" validate --engine ade-local --engine-option memory_limit_bytes=4194304 --engine-option prefetch_batches=1 --spec benchmarks/specs/sort-key-rowid.toml --dataset "$work/data" > "$work/spill-sort.json" 2> "$work/spill-sort.log"
python3 - "$work" <<'PYTHON'
import pathlib, re, sys
root = pathlib.Path(sys.argv[1])
for name, limit in [("aggregate", 2097152), ("sort", 4194304)]:
    log = (root / f"spill-{name}.log").read_text()
    metrics = dict((key, int(value)) for key, value in re.findall(r"(\w+)=(\d+)", log))
    assert metrics["spill_events"] > 0 and metrics["spill_read_bytes"] > 0, metrics
    assert metrics["managed_peak"] <= limit, metrics
    assert 0 < metrics["prefetch_queue_peak"] <= 1, metrics
    if name == "sort":
        assert metrics["sort_runs"] > 1 and metrics["sort_merges"] > 0, metrics
PYTHON

# Real process/network path, independently validated against the same DataFusion fixture.
ADE_LOG=debug "$bin" validate --engine ade-distributed --engine-option workers=2 --spec benchmarks/specs/aggregate-skew.toml --dataset "$work/data" > "$work/distributed.json" 2> "$work/distributed.log"
python3 - "$work" <<'PYTHON'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
assert json.loads((root / "distributed.json").read_text())["result_fingerprint"] == json.loads((root / "aggregate-skew-validation.json").read_text())["result_fingerprint"]
line = next(line for line in (root / "distributed.log").read_text().splitlines() if "ADE distributed execution" in line)
metrics = json.JSONDecoder().raw_decode(line.split("metrics=", 1)[1])[0]
assert metrics["worker_count"] == 2
assert metrics["gather"]["rows"] > 0 and metrics["gather"]["payload_bytes"] > 0
for worker in ("0", "1"):
    assert metrics["producers"][worker]["source_files"] == 2
    assert metrics["producers"][worker]["sent"]["payload_bytes"] > 0
    assert metrics["received"][worker]["transfers"] == 2
PYTHON
