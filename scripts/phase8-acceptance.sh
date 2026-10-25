#!/usr/bin/env bash
# Revised supported-domain experiment in ADR-0015; original failed run is retained.
set -euo pipefail
repo=$(git rev-parse --show-toplevel)
cd "$repo"
if [[ -n $(git status --porcelain --untracked-files=all) ]]; then
  echo 'Phase 8 acceptance requires a clean source checkout' >&2
  exit 1
fi
bin=${ADE_BENCH_BIN:-target/release/ade-bench}
data=${ADE_PHASE8_DATA:-benchmarks/datasets/phase8-acceptance-packed}
results=${ADE_PHASE8_RESULTS:-benchmarks/results/phase8-acceptance-packed}
mkdir -p "$results" "$data"
git rev-parse HEAD > "$results/source-commit.txt"
git rev-parse HEAD:crates > "$results/runtime-tree.txt"
"$bin" env --format json > "$results/environment.json"
spec=benchmarks/specs/aggregate-high-cardinality.toml
for distribution in uniform hash-bucket-skew hot-key tiny; do
  actual=$distribution
  rows=1000000
  cardinality=131072
  if [[ $distribution == tiny ]]; then actual=uniform; rows=100; cardinality=4; fi
  "$bin" data generate synthetic --dataset-id "phase8-$distribution" --rows "$rows" --files 4 --row-group-rows 8192 --key-cardinality "$cardinality" --distribution "$actual" --hot-key-fraction 1 --hot-key-count 1 --seed 42 --output "$data/$distribution" > "$results/$distribution-manifest.json"
  "$bin" validate --engine datafusion --spec "$spec" --dataset "$data/$distribution" > "$results/$distribution-oracle.json"
done
run_case() {
  local distribution=$1 workers=$2 policy=$3 profile=$4 iterations=$5 warmups=$6
  local stem="$results/$distribution-$workers-$policy-$profile"
  local options=(--engine ade-distributed --engine-option "workers=$workers" --engine-option "partition_policy=$policy" --engine-option exchange_mode=spill --engine-option exchange_buffer_bytes=1048576 --engine-option memory_limit_bytes=33554432 --engine-option prefetch_batches=1)
  if [[ $profile == replay ]]; then options+=(--engine-option research_exit_final_partition=0); fi
  if [[ $profile == restore ]]; then
    options=(--engine ade-distributed --engine-option "workers=$workers" --engine-option "partition_policy=$policy" --engine-option exchange_mode=adaptive --engine-option adaptive_policy=expected_cost_v1 --engine-option cost_mode=always --engine-option cost_source_read_nano_usd_per_byte=1000 --engine-option exchange_buffer_bytes=1048576 --engine-option memory_limit_bytes=33554432 --engine-option prefetch_batches=1 --engine-option research_exit_final_partition=0)
  fi
  printf '%q ' "$bin" run "${options[@]}" --spec "$spec" --dataset "$data/$distribution" --verify-data full --warmups "$warmups" --iterations "$iterations" --output "$stem-result.json" > "$stem-command.txt"
  : > "$stem-metrics.jsonl"
  ADE_DISTRIBUTED_METRICS_FILE="$stem-metrics.jsonl" "$bin" run "${options[@]}" --spec "$spec" --dataset "$data/$distribution" --verify-data full --warmups "$warmups" --iterations "$iterations" --output "$stem-result.json" > "$stem-run.log" 2> "$stem-stderr.log"
  "$bin" check-result "$stem-result.json" > "$stem-check.json"
}
for distribution in uniform hash-bucket-skew hot-key; do
  for policy in static sampled_v1; do run_case "$distribution" 2 "$policy" normal 3 1; done
done
for policy in static sampled_v1; do run_case hash-bucket-skew 4 "$policy" normal 3 1; done
run_case hash-bucket-skew 2 static_packed normal 3 1
run_case tiny 2 sampled_v1 normal 1 0
run_case hash-bucket-skew 2 sampled_v1 replay 1 0
run_case hash-bucket-skew 2 sampled_v1 restore 1 0
python3 scripts/phase8-verify.py "$results" "${ADE_CONTROL_PROTOCOL:-ADETCP12}" | tee "$results/verifier.stdout.txt"
