# Benchmark workflow

Build once with `cargo build --release -p ade-bench`. Commands below run from the repository root. Data and result directories are ignored; output datasets and result artifacts must use new paths.

```sh
BIN=target/release/ade-bench
$BIN env --format json
$BIN data generate synthetic --dataset-id skew-v1 --rows 100000 --files 4 --row-group-rows 8192 --key-cardinality 10000 --distribution hot-key --hot-key-fraction 0.5 --hot-key-count 1 --seed 42 --output benchmarks/datasets/skew-v1
$BIN data inspect benchmarks/datasets/skew-v1
$BIN validate --spec benchmarks/specs/aggregate-skew.toml --dataset benchmarks/datasets/skew-v1
$BIN run --spec benchmarks/specs/aggregate-skew.toml --dataset benchmarks/datasets/skew-v1 --verify-data full --warmups 1 --iterations 5 --output benchmarks/results/baseline.json
$BIN run --spec benchmarks/specs/aggregate-skew.toml --dataset benchmarks/datasets/skew-v1 --engine-option target_partitions=4 --verify-data full --iterations 5 --output benchmarks/results/candidate.json
$BIN check-result benchmarks/results/baseline.json
$BIN compare benchmarks/results/baseline.json benchmarks/results/candidate.json --format json
```

`--engine datafusion` is the default; `--engine ade-local` selects ADE's independent local runtime. The selected engine and each raw `--engine-option` are forwarded to the isolated child; unsupported engines fail before a run is recorded. DataFusion alone parses and validates its options: `target_partitions`, `batch_size`, and `collect_statistics`, each `key=value`. All effective options are recorded. Build metadata includes the executable's build-bound Git identity and known effective Cargo profile/codegen settings. `run` defaults to metadata verification for immutable data; use `--verify-data full` to check file checksums once before child execution. `data inspect` and `validate` always perform full verification. Engine overrides never belong in semantic TOML specs. Seven supplied specs cover scan, filter/project, three aggregation stress labels, ordered key/row_id sort and TPC-H-derived Q1. ADE supports the six synthetic specs; Q1 uses DataFusion. Synthetic validation can run every synthetic spec on any synthetic fixture. Sort uses an order-preserving oracle; other integer workloads retain their unordered canonical validation.

ADE options are `batch_size=8192`, `memory_limit_bytes=268435456`, `spill_enabled=true` and `prefetch_batches=0` by default. Managed memory includes retained collection, so choose a budget that can hold the result as well as bounded operator state; it is not a total-process RSS cap. `ADE_LOG=debug` on standalone `validate` exposes spill/run/prefetch counters. See [ADR-0006](../docs/adr/0006-managed-local-memory-and-spill.md) for resource semantics.

Synthetic controls include rows, files, row-group-rows, compression (`none`, `snappy`, `zstd`), key-cardinality, distribution (`uniform`, `hot-key`), hot-key-fraction, hot-key-count, payload-size and seed. Shape is unpartitioned row order; no optional sorted-output mode is provided. Choose cardinality 100 for low state, roughly rows/2 for high state, and hot-key distribution for skew. Presets are reproducible recipes:

| Recipe | Rows | Payload | Keys | Purpose |
|---|---:|---:|---:|---|
| CI | 100000 | 16 | 100 | correctness/smoke |
| local-small | 10000000 | 128 | 1000000 | about 2 GB logical; compression changes physical size |
| local-memory-pressure | user sized | user sized | rows/2 | choose logical size above host RAM; baseline may fail without a configured spill/memory policy |

The generator writes bounded 8192-row batches; Parquet buffers depend on configured row-group size. The harness collects results and its oracle in memory. Scan/high-cardinality validation may need substantial memory. ADE managed memory and local spill apply to its runtime; harness/oracle collection remains external.

## TPC-H-derived local integration

Install the tested upstream tool with `cargo install tpchgen-cli --version 3.0.0 --locked`. The wrapper captures its reported version, invokes public `parquet --scale-factor --output-dir` flags, checks all eight table files, counts Parquet rows and fingerprints files. Nothing large is vendored.

```sh
$BIN data generate tpch --scale-factor 1 --output benchmarks/datasets/tpch-sf1
$BIN data inspect benchmarks/datasets/tpch-sf1
$BIN validate --spec benchmarks/specs/tpch-q1.toml --dataset benchmarks/datasets/tpch-sf1
$BIN run --spec benchmarks/specs/tpch-q1.toml --dataset benchmarks/datasets/tpch-sf1 --iterations 5 --output benchmarks/results/tpch-q1.json
```

Use `--generator /path/to/tpchgen-cli` if it is not on PATH; SF 0.001 is suitable for a development smoke run. Q1 is selected with the same `--spec` mechanism as synthetic cases; there is no separate `--workload/--query` alias. This is derived from TPC-H, not an official TPC result.

## Microbenchmarks

```sh
cargo bench -p ade-bench --bench smoke
cargo bench -p ade-bench --bench smoke -- --test
```

Criterion measures a stable BLAKE3 buffer operation independently of macrobench orchestration. Read [methodology](../docs/research/benchmarking-methodology.md) before interpreting results. `examples/result-v1.json` is an intentionally small schema example, not a performance claim.

## Phase 3 static distributed adapter

Build with `cargo build --release --workspace` so `ade-worker` is present beside
`ade-bench`. Use `--engine ade-distributed --engine-option workers=2` (or 4) with
any existing synthetic aggregate spec. Scan, filter/project, sort and Q1 are
rejected by this adapter. The four local options listed above also apply; each
worker runs one producer and one final task. Files come only from the manifest,
assigned round robin. Gather concatenates disjoint partitions. No retries.

`ADE_LOG=debug` standalone validation emits structured per-worker source/input/
partial/final rows, destination payload bytes/transfers/peak frames, durations,
local managed peak/spill and gather totals. These diagnostics do not change result
schema v1 or measured-child logging. Worker memory budgets exclude separately
bounded network buffers and coordinator/harness collection. Use at least four
files to demonstrate work on every worker. Local timings have no acceptance
threshold and do not establish speedup.
