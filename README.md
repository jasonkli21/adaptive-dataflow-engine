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

**Phase 5 adaptive exchange v1 implemented.** `ade-distributed` runs scan → partial aggregate → network hash exchange → final merge aggregate → gather across 2–4 static worker processes. Worker failure fails the job; no retry or recovery.

Phase 2 local execution remains available. `ade-exec` owns managed memory, partitioned aggregate spill, external sort and bounded Parquet prefetch alongside scan/filter/project/limit/collect. `ade-local` executes six synthetic specs through the independent oracle, including order-aware sort validation. Distributed execution currently supports only synthetic GroupBySum.

The completed Phase 0 architecture/benchmark lab in `crates/ade-bench` provides deterministic datasets, engine-independent workload definitions, a DataFusion baseline, process-isolated macrobenchmarks, versioned result artifacts, correctness validation, comparison tooling, and microbenchmark infrastructure.

## Read first

Implementation work should use this order:

1. [`AGENTS.md`](AGENTS.md) — repository working rules and handoff conventions.
2. [`docs/product/prd.md`](docs/product/prd.md) — product/research thesis and long-term phases.
3. [`docs/architecture/architecture.md`](docs/architecture/architecture.md) — architectural boundaries and invariants.
4. [`docs/plans/phase-5-implementation-plan.md`](docs/plans/phase-5-implementation-plan.md) — Phase 5 implementation contract.
5. [`docs/adr/`](docs/adr/) — concrete accepted design decisions.
6. [`docs/reports/`](docs/reports/) — implementation state and phase reports.

## Repository shape

```text
.
├── AGENTS.md
├── Cargo.toml
├── crates/
│   ├── ade-distributed/        # static coordinator, workers, TCP/IPC exchange
│   ├── ade-exec/               # Phase 2 local runtime
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

Run `bash scripts/smoke.sh` after `cargo build --workspace` for the CI correctness flow. Result artifacts are immutable: choose a new output path for every run.

## ADE local execution

After generating a synthetic fixture using the benchmark quick start:

```bash
target/release/ade-bench validate --engine ade-local --engine-option batch_size=8192 --spec benchmarks/specs/scan-filter-project.toml --dataset benchmarks/datasets/example
target/release/ade-bench run --engine ade-local --spec benchmarks/specs/aggregate-skew.toml --dataset benchmarks/datasets/example --verify-data full --warmups 1 --iterations 3 --output benchmarks/results/ade-example.json
```

The effective options are recorded in benchmark engine configuration:

| Option | Default | Meaning |
|---|---:|---|
| `batch_size` | 8192 | Positive Parquet scan batch size |
| `memory_limit_bytes` | 268435456 | Positive execution managed budget (256 MiB) |
| `spill_enabled` | true | Allow local aggregate/sort spill; false fails at state target |
| `prefetch_batches` | 0 | Queue depth 0–64; zero reads synchronously |

For example:

```bash
ADE_LOG=debug target/release/ade-bench validate --engine ade-local --engine-option memory_limit_bytes=4194304 --engine-option prefetch_batches=1 --spec benchmarks/specs/sort-key-rowid.toml --dataset benchmarks/datasets/example
```

`ADE_LOG=debug` exposes pressure, managed memory, spill/run and prefetch counters
on stderr. Programmatic `ade_exec::execute_with_config` also returns per-operator
metrics and accepts an output batch cap. Result artifacts retain schema v1.

The budget covers reserved blocking state, explicit queued batches and retained
collection; it is **not a total-process RSS cap**. Decoder/kernel/IPC internals,
synchronous input and allocator overhead are external/transient. A collected
result that cannot fit fails explicitly even if its operator spills. Aggregate
uses 16 local partitions and fails clearly if a partition still cannot fit;
external sort uses bounded IPC chunks and two-way multi-pass merging. Temp spill
is cleaned on normal success/error/cancellation, and has no recovery durability.
Prefetch preserves pull backpressure and joins its producer on teardown.

The physical plan is provisional. Sort supports non-null UInt64 key/row_id and
Int64 value, ascending key then row_id. TPC-H Q1 is supported only by DataFusion.
The local plan has no joins or SQL frontend. Static distribution is provided by the separate distributed crate below.
See [ADR-0006](docs/adr/0006-managed-local-memory-and-spill.md) and the
[Phase 2 report](docs/reports/phase-2-implementation-report.md) for contracts,
validation and comparison evidence.

## License

No open-source license has been selected in this scaffold. Add one intentionally before relying on public reuse semantics.

## ADE distributed execution

Build both binaries with `cargo build --workspace --release`. The benchmark
adapter starts loopback workers on OS-selected ports, owns their lifecycle, and
validates the collected result with the existing independent oracle.

```sh
ADE_LOG=debug target/release/ade-bench validate --engine ade-distributed \
  --engine-option workers=2 --spec benchmarks/specs/aggregate-skew.toml \
  --dataset benchmarks/datasets/skew-v1
```

Options: `workers=2` (2–4), `batch_size=8192`, `memory_limit_bytes=268435456`,
`spill_enabled=true`, `prefetch_batches=0` (0–64),
`exchange_mode=local_materialized`, `exchange_buffer_bytes=4194304`.
Hash exchange modes are `direct`, `buffered`, `spill`, `local_materialized` and
`object_store`, plus explicit `adaptive`. Buffer limits are 1 byte–256 MiB; `buffered` requires at least
1 MiB. `spill` can use a tiny limit to force disk. Direct/local/object modes
validate/report the buffer setting but do not use it as retention capacity.
Effective configuration and
fixed protocol limits appear in engine metadata; standalone debug validation
prints per-worker/network metrics. `ADE_WORKER_BIN` can select the worker binary;
otherwise `ade-worker` is resolved beside `ade-bench`.

The Rust coordinator API also accepts static endpoints. `ade-worker WORKER_ID
TEMP_PARENT` binds loopback control/exchange ports and writes a framed readiness
record to stdout; logs use stderr. Gather currently binds loopback, so this is a
local multi-process deployment. Final consumers and producers overlap, with at
most one task of each role per worker and no retry/recovery. The local memory
limit applies per task; two task budgets may coexist. Exchange retention and
bounded frame transients (1 MiB, 1024 rows) are separate from the local ledger;
this is not a process RSS cap. Direct uses rendezvous handoff, buffered uses a
byte-bounded FIFO, spill uses that FIFO plus a fixed-threshold local disk log,
and materialized modes wait for every producer to finish. Local storage is lossy.
Object-store mode defaults to the public provider-neutral API with an ephemeral
local filesystem backend. Embedded worker callers can install an
`ObjectStoreBackend` through `worker::serve_with_object_store`; job configuration
names the backend and its declared durability capability. ADE reports that
capability but does not verify it. The default does not demonstrate
cloud/multi-host durability or recovery. A retention operation that outlives its
deadline poisons the worker, which then rejects new jobs. All storage is
execution-scoped. Gather stays fixed and bounded in
transit; coordinator gathering/oracle validation collect the final result. See
[Phase 4 report](docs/reports/phase-4-implementation-report.md) and
[unified exchange ADR](docs/adr/0010-static-unified-exchange.md).

Adaptive mode runs deterministic `threshold_v1` at validated segment boundaries.
Destinations independently escalate through direct → buffered → spill → object
store. `adaptive_policy=threshold_v1`, `adaptive_buffer_high_pct=80` (1–100) and
`adaptive_remote_after_bytes=67108864` (0 disables remote, maximum 1 TiB) are
accepted only with `exchange_mode=adaptive`. Typed static configurations ignore
the serialized adaptive defaults. Equality at the high watermark escalates;
remote escalation also requires an unconsumed spill backlog. No downgrade or
routing change occurs. Live local prefixes drain before finalized object suffixes;
all tiers feed the same aggregate. At most three transition records per
partition expose exact ordinal/transfer/sequence, reason and observation values.
Debug metrics include tier byte/segment totals and job transition summaries.

The delay hook is compiled only for the `ade-bench` research-hooks build; the
default `ade-distributed` library API has no delay option. Benchmark options are
`research_slow_partition=N`,
`research_delay_after_segments=1`, `research_delay_ms=750`. They delay that final
consumer once, default off, with a maximum two-second cancellation-aware delay.
Effective metadata records these options. They change no query semantics.

Run `bash scripts/phase5-acceptance.sh` after a release workspace build for the
predeclared normal buffered/adaptive comparison, delayed direct/spill/adaptive
comparison, and limited four-worker adaptive evidence. It preserves raw oracle
validated iterations and checks the local 1.25× normal median guard plus the
narrow adversarial win over direct; these timing checks do not run in CI.
See [ADR-0012](docs/adr/0012-adaptive-exchange-v1.md) and the
[Phase 5 report](docs/reports/phase-5-implementation-report.md).
Phase 6 requires a new explicit instruction.
