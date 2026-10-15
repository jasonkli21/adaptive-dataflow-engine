#!/usr/bin/env bash
set -euo pipefail
bin=${ADE_BENCH_BIN:-target/debug/ade-bench}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
"$bin" env --format json > "$work/env.json"
"$bin" data generate synthetic --dataset-id ci-skew-v1 --rows 100000 --files 3 --row-group-rows 8192 --key-cardinality 10000 --distribution hot-key --output "$work/data" > "$work/manifest.json"
"$bin" data inspect "$work/data" > "$work/inspect.json"
for name in scan scan-filter-project aggregate-low-cardinality aggregate-high-cardinality aggregate-skew; do
  "$bin" validate --spec "benchmarks/specs/$name.toml" --dataset "$work/data" > "$work/$name-validation.json"
  "$bin" validate --engine ade-local --spec "benchmarks/specs/$name.toml" --dataset "$work/data" > "$work/$name-ade-validation.json"
done
"$bin" run --spec benchmarks/specs/aggregate-skew.toml --dataset "$work/data" --verify-data full --warmups 1 --iterations 1 --output "$work/result.json"
"$bin" check-result "$work/result.json"
"$bin" compare "$work/result.json" "$work/result.json" --format json
"$bin" run --engine ade-local --spec benchmarks/specs/aggregate-skew.toml --dataset "$work/data" --verify-data full --warmups 0 --iterations 1 --output "$work/ade-result.json"
"$bin" check-result "$work/ade-result.json"
"$bin" compare "$work/result.json" "$work/ade-result.json" --format json
