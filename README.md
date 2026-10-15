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

**Phase 0 benchmark infrastructure implemented. No ADE physical execution engine exists.**

Phase 0 builds the architecture/benchmark lab before ADE execution operators are introduced. The initial implementation target is `crates/ade-bench`, which provides deterministic datasets, engine-independent workload definitions, a DataFusion baseline, process-isolated macrobenchmarks, versioned result artifacts, correctness validation, comparison tooling, and microbenchmark infrastructure.

## Read first

Implementation work should use this order:

1. [`AGENTS.md`](AGENTS.md) — repository working rules and handoff conventions.
2. [`docs/product/prd.md`](docs/product/prd.md) — product/research thesis and long-term phases.
3. [`docs/architecture/architecture.md`](docs/architecture/architecture.md) — architectural boundaries and invariants.
4. [`docs/plans/phase-0-implementation-plan.md`](docs/plans/phase-0-implementation-plan.md) — exact Phase 0 implementation contract.
5. [`docs/adr/`](docs/adr/) — concrete accepted design decisions.
6. [`docs/reports/`](docs/reports/) — implementation state and phase reports.

## Repository shape

```text
.
├── AGENTS.md
├── Cargo.toml
├── crates/
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

## License

No open-source license has been selected in this scaffold. Add one intentionally before relying on public reuse semantics.
