#!/usr/bin/env bash
# Local release evidence, never a CI performance gate.
# Predeclared: normal adaptive <= 1.25x buffered median; delayed adaptive < direct median.
# Final configuration uses 100% occupancy to avoid escalating a normal late burst.
# The initial 80% experiment and its failed divergence check remain preserved.
# Spill is reported as a second adversarial baseline, not selected after measuring.
set -euo pipefail
bin=${ADE_BENCH_BIN:-target/release/ade-bench}
data=${ADE_PHASE5_DATA:-benchmarks/datasets/phase5-acceptance-20261016}
results=${ADE_PHASE5_RESULTS:-benchmarks/results/phase5-acceptance-20261016}
mkdir -p "$results"
"$bin" env --format json > "$results/environment.json"
"$bin" data generate synthetic --dataset-id phase5-uniform-v1 --rows 1000000 --files 4 --row-group-rows 8192 --key-cardinality 100000 --seed 42 --output "$data" > "$results/manifest.json"
spec=benchmarks/specs/aggregate-high-cardinality.toml
"$bin" validate --engine datafusion --spec "$spec" --dataset "$data" > "$results/datafusion.json"
for case in 2-normal-buffered 2-normal-adaptive 2-slow-direct 2-slow-spill 2-slow-adaptive 4-slow-adaptive; do
  IFS=- read -r workers profile mode <<< "$case"
  options=(--engine ade-distributed --engine-option "workers=$workers" --engine-option "exchange_mode=$mode" --engine-option exchange_buffer_bytes=1048576 --engine-option memory_limit_bytes=33554432 --engine-option prefetch_batches=1)
  if [[ $mode == adaptive ]]; then
    options+=(--engine-option adaptive_policy=threshold_v1 --engine-option adaptive_buffer_high_pct=100 --engine-option adaptive_remote_after_bytes=1048576)
  fi
  if [[ $profile == slow ]]; then
    options+=(--engine-option research_slow_partition=0 --engine-option research_delay_after_segments=1 --engine-option research_delay_ms=750)
  fi
  stem="$results/$case"
  ADE_LOG=debug "$bin" validate "${options[@]}" --spec "$spec" --dataset "$data" > "$stem-validation.json" 2> "$stem-metrics.log"
  "$bin" run "${options[@]}" --spec "$spec" --dataset "$data" --verify-data full --warmups 1 --iterations 5 --output "$stem-result.json" > "$stem-run.log"
  "$bin" check-result "$stem-result.json" > "$stem-check.json"
done
python3 scripts/phase5-verify.py "$results"
