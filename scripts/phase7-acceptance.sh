#!/usr/bin/env bash
# Predeclared suite: 3 regimes, 4 policies, paired success/loss, two isolated samples.
# Scoring and the fixed win/overhead criterion are in phase7-verify.py and ADR-0014.
set -euo pipefail
repo=$(git rev-parse --show-toplevel)
cd "$repo"
if [[ -n $(git status --porcelain --untracked-files=all) ]]; then
  echo 'Phase 7 release acceptance requires a clean source checkout' >&2
  exit 1
fi
bin=${ADE_BENCH_BIN:-target/release/ade-bench}
data=${ADE_PHASE7_DATA:-benchmarks/datasets/phase7-acceptance}
results=${ADE_PHASE7_RESULTS:-benchmarks/results/phase7-acceptance}
mkdir -p "$results"
git rev-parse HEAD > "$results/source-commit.txt"
"$bin" env --format json > "$results/environment.json"
"$bin" data generate synthetic --dataset-id phase7-uniform-v1 --rows 200000 --files 4 --row-group-rows 8192 --key-cardinality 32768 --seed 42 --output "$data" > "$results/manifest.json"
spec=benchmarks/specs/aggregate-high-cardinality.toml
"$bin" validate --engine datafusion --spec "$spec" --dataset "$data" > "$results/datafusion.json"
for regime in low high expensive_restore; do
  case "$regime" in
    low) probability=1000; source_price=0; write_price=1000; read_price=1 ;;
    high) probability=900000; source_price=1000; write_price=1; read_price=1 ;;
    expensive_restore) probability=500000; source_price=1; write_price=1; read_price=100000 ;;
  esac
  for policy in expected always never threshold; do
    for profile in normal loss; do
      options=(--engine ade-distributed --engine-option workers=2 --engine-option exchange_mode=adaptive --engine-option exchange_buffer_bytes=1048576 --engine-option memory_limit_bytes=33554432 --engine-option prefetch_batches=1)
      if [[ "$policy" == threshold ]]; then
        options+=(--engine-option adaptive_policy=threshold_v1)
      else
        options+=(--engine-option adaptive_policy=expected_cost_v1 --engine-option "cost_mode=$policy" --engine-option "cost_failure_probability_ppm=$probability" --engine-option "cost_source_read_nano_usd_per_byte=$source_price" --engine-option "cost_object_write_nano_usd_per_byte=$write_price" --engine-option "cost_object_read_nano_usd_per_byte=$read_price")
      fi
      if [[ "$profile" == loss ]]; then options+=(--engine-option research_exit_final_partition=0); fi
      stem="$results/$regime-$policy-$profile"
      ADE_LOG=debug "$bin" validate "${options[@]}" --spec "$spec" --dataset "$data" > "$stem-validation.json" 2> "$stem-validation.log"
      : > "$stem-metrics.jsonl"
      ADE_DISTRIBUTED_METRICS_FILE="$stem-metrics.jsonl" ADE_LOG=debug "$bin" run "${options[@]}" --spec "$spec" --dataset "$data" --verify-data full --warmups 0 --iterations 2 --output "$stem-result.json" > "$stem-run.log" 2> "$stem-metrics.log"
      "$bin" check-result "$stem-result.json" > "$stem-check.json"
    done
  done
done
python3 scripts/phase7-verify.py "$results" | tee "$results/verifier.stdout.txt"
