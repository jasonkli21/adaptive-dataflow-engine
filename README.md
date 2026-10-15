# Adaptive Dataflow Engine (ADE)

ADE is a research-oriented Rust project exploring a distributed execution engine where **materialization and recomputation are adaptive decisions rather than fixed properties of the processing model**.

The long-term goal is a general-purpose data-processing substrate with Spark/Flink-class breadth, but the project deliberately starts much smaller. The early research spine is:

```text
benchmark substrate
    -> local execution
    -> bounded memory / spill
    -> static distribution
    -> unified exchange
    -> adaptive exchange
    -> lineage-aware recovery
    -> cost-aware materialization
    -> adaptive partitioning
```

## Current status

**Phase 1 local execution implemented.** `ade-exec` owns synchronous Parquet, filter, projection, hash aggregation, limit and collection, with cancellation, deterministic IDs and observational memory/operator metrics. `ade-local` executes all five synthetic specs through the existing independent correctness oracle.

The completed Phase 0 architecture/benchmark lab in `crates/ade-bench` provides deterministic datasets, engine-independent workload definitions, a DataFusion baseline, process-isolated macrobenchmarks, versioned result artifacts, correctness validation, comparison tooling, and microbenchmark infrastructure.

## Read first

Implementation work should use this order:

1. [`AGENTS.md`](AGENTS.md) — repository working rules and handoff conventions.
2. [`docs/product/prd.md`](docs/product/prd.md) — product/research thesis and long-term phases.
3. [`docs/architecture/architecture.md`](docs/architecture/architecture.md) — architectural boundaries and invariants.
4. [`docs/plans/phase-1-implementation-plan.md`](docs/plans/phase-1-implementation-plan.md) — active Phase 1 implementation contract.
5. [`docs/adr/`](docs/adr/) — concrete accepted design decisions.
6. [`docs/reports/`](docs/reports/) — implementation state and phase reports.

## Repository shape

```text
.
├── AGENTS.md
├── Cargo.toml
├── crates/
│   ├── ade-exec/               # Phase 1 local runtime
│   └── ade-bench/              # Phase 0 benchmark harness
├── benchmarks/
│   ├── specs/
│   ├── datasets/               # generated; ignored
│   ├── results/                # generated; ignored
│   └── examples/
├── docs/
│   ├── product/prd.md
│   ├── architecture/architecture.md
│   ├── plans/phase-0-implementation-plan.md
│   ├── adr/
│   ├── research/
│   └── reports/
└── .github/workflows/ci.yml
```

The architecture describes additional future crates. **Do not create them until a phase gives them real ownership.** Empty future modules increase context cost and prematurely freeze interfaces.

## Development philosophy

- correctness before performance;
- measure before optimizing;
- mechanism separate from policy;
- explicit memory/resource accounting;
- static baselines for adaptive features;
- observable optimizer decisions;
- reuse Arrow/Parquet/open ecosystem infrastructure where it is not differentiating;
- expand scope only after the current phase is validated.

## Phase 0 quick start

Use stable Rust 1.94 or newer. The implemented workflow is:

```bash
cargo run -p ade-bench --release -- env
cargo run -p ade-bench --release -- data generate synthetic --help
cargo run -p ade-bench --release -- validate --help
cargo run -p ade-bench --release -- run --help
cargo run -p ade-bench --release -- compare --help
cargo bench -p ade-bench
```

See [`benchmarks/README.md`](benchmarks/README.md) for executable generation/run/compare examples, TPC-H-derived setup, and dataset size recipes. Read the [methodology](docs/research/benchmarking-methodology.md) before interpreting local measurements. The [implementation report](docs/reports/phase-0-implementation-report.md) records acceptance evidence and limitations.

Run `bash scripts/smoke.sh` after `cargo build -p ade-bench` for the CI correctness flow. Result artifacts are immutable: choose a new output path for every run.

## ADE local execution

After generating a synthetic fixture using the benchmark quick start:

```bash
target/release/ade-bench validate --engine ade-local --engine-option batch_size=8192 --spec benchmarks/specs/scan-filter-project.toml --dataset benchmarks/datasets/example
target/release/ade-bench run --engine ade-local --spec benchmarks/specs/aggregate-skew.toml --dataset benchmarks/datasets/example --verify-data full --warmups 1 --iterations 3 --output benchmarks/results/ade-example.json
```

`batch_size` defaults to 8192 and must be positive. TPC-H Q1 remains supported
only by DataFusion. The local plan API is provisional. Aggregation and collected
output reside in memory; memory counters are estimates, without hard limits or
spill. Detailed metrics are returned by `ade_exec::execute` and logged at debug
by the adapter; benchmark artifacts keep their existing schema. See the
[Phase 1 report](docs/reports/phase-1-implementation-report.md) for validation and
comparison evidence. Phase 2 has not started.

## License

No open-source license has been selected in this scaffold. Add one intentionally before relying on public reuse semantics.
