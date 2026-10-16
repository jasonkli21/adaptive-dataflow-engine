#!/usr/bin/env bash
# Correctness and selective recovery evidence; no timing threshold.
set -euo pipefail
repo=$(git rev-parse --show-toplevel)
cd "$repo"
source_commit=$(git rev-parse HEAD)
if [[ -n $(git status --porcelain --untracked-files=all) ]]; then
  echo "Phase 6 acceptance requires a clean source checkout" >&2
  exit 1
fi
bin=${ADE_BENCH_BIN:-target/release/ade-bench}
data=${ADE_PHASE6_DATA:-benchmarks/datasets/phase6-acceptance-20261016}
results=${ADE_PHASE6_RESULTS:-benchmarks/results/phase6-acceptance-20261016}
mkdir -p "$results"
"$bin" env --format json > "$results/environment.json"
"$bin" data generate synthetic --dataset-id phase6-uniform-v1 --rows 200000 --files 4 --row-group-rows 8192 --key-cardinality 32768 --seed 42 --output "$data" > "$results/manifest.json"
spec=benchmarks/specs/aggregate-high-cardinality.toml
"$bin" validate --engine datafusion --spec "$spec" --dataset "$data" > "$results/datafusion.json"
# One normal and recovered run, five-mode static refresh, limited adaptive 2/4-worker refresh.
for case in 2-normal-local_materialized 2-recover-local_materialized 2-normal-direct 2-normal-buffered 2-normal-spill 2-normal-object_store 2-recover-adaptive 4-normal-adaptive; do
  IFS=- read -r workers profile mode <<< "$case"
  options=(--engine ade-distributed --engine-option "workers=$workers" --engine-option "exchange_mode=$mode" --engine-option exchange_buffer_bytes=1048576 --engine-option memory_limit_bytes=33554432 --engine-option prefetch_batches=1)
  if [[ $profile == recover ]]; then
    options+=(--engine-option research_exit_final_partition=0)
  fi
  stem="$results/$case"
  ADE_LOG=debug "$bin" validate "${options[@]}" --spec "$spec" --dataset "$data" > "$stem-validation.json" 2> "$stem-metrics.log"
  "$bin" run "${options[@]}" --spec "$spec" --dataset "$data" --verify-data full --warmups 0 --iterations 1 --output "$stem-result.json" > "$stem-run.log"
  "$bin" check-result "$stem-result.json" > "$stem-check.json"
done
# Same genuine worker exit, but source lineage was deliberately not supplied.
options=(--engine ade-distributed --engine-option workers=2 --engine-option research_exit_final_partition=0 --engine-option recovery_enabled=false --engine-option exchange_buffer_bytes=1048576 --engine-option memory_limit_bytes=33554432 --engine-option prefetch_batches=1)
if ADE_LOG=debug "$bin" validate "${options[@]}" --spec "$spec" --dataset "$data" > "$results/unrecoverable-validation.json" 2> "$results/unrecoverable-metrics.log"; then
  echo "Unrecoverable case unexpectedly succeeded" >&2
  exit 1
fi
if "$bin" run "${options[@]}" --spec "$spec" --dataset "$data" --verify-data full --warmups 0 --iterations 1 --output "$results/unrecoverable-result.json" > "$results/unrecoverable-run.log" 2>&1; then
  echo "Unrecoverable child unexpectedly succeeded" >&2
  exit 1
fi
python3 scripts/phase6-verify.py "$results" "$source_commit"
