# Adaptive Dataflow Engine (ADE) — Phase 1 Detailed Implementation Plan
## Minimal Single-Node Physical Engine

**Status:** Implemented and acceptance verified; see `docs/reports/phase-1-implementation-report.md`
**Date:** 2026-10-15
**Repository reference:** `jasonkli21/adaptive-dataflow-engine`
**Phase 0 baseline reviewed:** `f6beacb1a5db23b2399c4305445889df3128740d` (`Address Phase 0 review findings`)
**Depends on:** `docs/product/prd.md`, `docs/architecture/architecture.md`, `docs/reports/phase-0-implementation-report.md`
**Primary objective:** Implement ADE's first real execution engine locally, keep it small enough to understand end-to-end, and plug it into the existing Phase 0 benchmark/correctness harness as `ade-local`.

---

# 1. Phase 1 Objective

Phase 1 is the transition from **measurement infrastructure** to **engine implementation**.

At the end of the phase, ADE must own a small local physical execution path capable of running the existing synthetic benchmark semantics without delegating execution to DataFusion:

```text
ParquetScan
    -> optional Filter
    -> optional Project
    -> optional HashAggregate
    -> CollectSink
```

The engine must support the Phase 1 operator set from the PRD:

- Parquet scan;
- filter;
- projection;
- hash aggregate;
- limit;
- local collect/sink.

The runtime must also establish the first versions of:

- Arrow-compatible batch flow;
- execution context;
- cancellation;
- deterministic operator/execution identity;
- operator/runtime metrics;
- explicit memory-accounting hooks;
- natural backpressure through the execution model.

The exit condition is:

> The existing Phase 0 harness can run the supported synthetic workload specs using `--engine ade-local`, independently validate the results against the existing oracle, and compare those validated results with DataFusion without changing workload semantics or weakening Phase 0 guarantees.

Phase 1 is **not** a performance-optimization phase. It should produce a clear baseline that later phases can improve.

---

# 2. Repository State This Plan Assumes

The current repository already provides a strong Phase 0 foundation and should be reused rather than redesigned.

Important existing properties:

- `ade-bench` is the only current workspace crate.
- DataFusion is pinned as the production baseline adapter.
- the benchmark harness has an explicit `BenchmarkEngine` boundary with engine-owned configuration;
- `EngineRequest` already forwards the engine name and raw options into isolated child processes;
- the harness owns canonical output validation rather than trusting engine output;
- `compare` can compare validated results from different engines while warning about engine differences;
- benchmark child executions are process-isolated;
- synthetic workloads already cover scan, filter/project, and low/high/skew aggregation;
- result and dataset identity/provenance were hardened during the Phase 0 review remediation;
- Phase 0 is complete and no ADE execution operators currently exist.

This means Phase 1 should be mostly additive:

```text
new ade-exec crate
        |
        +-> ade-local adapter in ade-bench
        |
        +-> existing workload/correctness/result pipeline
```

Do **not** replace the Phase 0 harness with a new benchmark path.

---

# 3. Efficiency Goal for Phase 1 Development

Phase 0 took roughly 50 minutes for the initial implementation and more than 44 minutes for review remediation. Phase 1 should attempt to reduce redundant work without reducing correctness.

The intended approach is:

1. **reuse the hardened Phase 0 contracts** rather than reopening them;
2. implement Phase 1 as a small number of vertical slices;
3. run focused tests for the touched crate/module during each slice;
4. delay full-workspace lint/test validation until the implementation is integrated;
5. build release binaries and run comparative macrobenchmarks only near the end;
6. avoid optional Phase 2+ abstractions, generic frameworks, and premature performance tuning;
7. have the review focus on correctness/architecture defects rather than style or speculative enhancements.

This is an efficiency strategy, not a hard time limit. If a correctness issue requires deeper investigation, correctness wins.

### Recommended validation cadence

During implementation:

```text
edit ade-exec core
  -> focused ade-exec unit tests

edit one operator cluster
  -> focused operator tests

wire ade-local adapter
  -> focused ade-bench adapter/integration test

complete integration
  -> workspace tests once
  -> clippy/fmt/check once
  -> release benchmark acceptance once
```

Avoid repeatedly running:

- the entire Phase 0 acceptance suite after every operator;
- TPC-H generation/Q1 when unrelated files are untouched;
- Criterion when benchmark infrastructure is unchanged;
- release builds before correctness is stable;
- `cargo clean` unless a concrete build issue demands it.

Incremental Cargo builds and narrow test filters should do most iteration work.

---

# 4. Phase 1 Scope

## 4.1 Required

### New local execution crate

Create:

```text
crates/ade-exec/
```

This crate owns the Phase 1 runtime and must **not depend on DataFusion or `ade-bench`**.

It may depend directly on:

- Arrow;
- Parquet;
- `thiserror`;
- `tracing`;
- small standard-purpose crates only when genuinely needed.

Prefer synchronous execution in Phase 1. Tokio is not required in `ade-exec` unless a concrete implementation need is discovered and documented.

### Local physical plan/operator model

Implement a small provisional local physical-plan representation sufficient to build:

```text
scan
scan -> project
scan -> filter -> project
scan -> hash aggregate
scan -> limit
```

The plan representation is intentionally **not the final stable physical IR**. Stable broader physical IR is a later roadmap milestone.

### Runtime

Implement:

- synchronous pull-based batch execution;
- `ExecContext` or equivalent;
- deterministic operator IDs;
- cancellation checks;
- local metrics;
- explicit memory-accounting hooks;
- collect sink / executor entrypoint.

### Operators

Implement:

1. Parquet scan;
2. filter required by the current synthetic filter/project benchmark;
3. zero-copy projection where Arrow permits it;
4. local hash aggregate for `GROUP BY key, SUM(value_i64), COUNT(*)`;
5. limit;
6. collection/sink action.

### Benchmark integration

Add `ade-local` as a production `BenchmarkEngine` adapter in `ade-bench`.

It must support the existing synthetic cases:

- `scan`;
- `scan-filter-project`;
- `aggregate-low-cardinality`;
- `aggregate-high-cardinality`;
- `aggregate-skew`.

It should reject unsupported TPC-H Q1 cleanly in Phase 1.

### Documentation

- add/update Phase 1 ADR(s) only for durable decisions;
- update `AGENTS.md` so active-phase guidance is no longer Phase-0-only;
- update README current status/workflow;
- create `docs/reports/phase-1-implementation-report.md`.

---

## 4.2 Explicitly out of scope

Do **not** implement:

- hard memory limits;
- spilling;
- external sort;
- joins;
- shuffle/exchange;
- coordinator/worker processes;
- distributed scheduling;
- async operator pipelines merely for future-proofing;
- general SQL parsing;
- DataFusion physical operators inside `ade-exec`;
- broad expression language;
- TPC-H support in `ade-local`;
- object stores;
- lineage/recovery;
- adaptive execution;
- streaming/state/checkpoints;
- JIT/SIMD/custom allocators;
- performance tuning beyond obvious correctness-preserving implementation choices.

Those belong to later phases.

---

# 5. Crate and Dependency Architecture

## 5.1 Workspace

Target Phase 1 workspace:

```text
crates/
  ade-exec/
  ade-bench/
```

Do not create `ade-array`, `ade-physical`, `ade-memory`, `ade-expr`, or other future crates yet unless implementation reveals an unavoidable ownership boundary. Phase 1 is still small enough for the local runtime to remain cohesive.

## 5.2 Dependency direction

Required:

```text
Arrow / Parquet
      ^
      |
   ade-exec
      ^
      |
   ade-bench ----> DataFusion
```

Forbidden:

```text
ade-exec -> ade-bench
ade-exec -> DataFusion
```

This preserves ADE ownership of the execution engine while allowing the Phase 0 harness to benchmark it.

## 5.3 Shared dependency versions

Where practical, move Arrow/Parquet versions into workspace dependencies or otherwise ensure `ade-exec` and `ade-bench` use compatible pinned versions to avoid duplicate Arrow type universes.

Do not perform a dependency-upgrade project during Phase 1 unless required for correctness.

---

# 6. Phase 1 Execution Model

## 6.1 Recommended model: synchronous pull

Use a pull-based operator contract for Phase 1.

Illustrative, not mandatory exact syntax:

```rust
pub trait Operator: Send {
    fn id(&self) -> OperatorId;
    fn schema(&self) -> SchemaRef;
    fn next_batch(&mut self, cx: &mut ExecContext) -> Result<Option<RecordBatch>>;
}
```

A downstream operator requests the next batch from upstream only when it is ready.

Advantages for this phase:

- natural backpressure;
- no unbounded channels;
- no async scheduling complexity;
- easy cancellation checks between batches;
- easy operator-local metrics;
- clear ownership/lifetimes;
- simpler debugging and review.

This is a Phase 1 decision, not a permanent commitment. Phase 3+ networking or future streaming may justify a different internal model.

## 6.2 No queue required

The PRD mentions bounded queues/backpressure. A synchronous pull pipeline satisfies the backpressure requirement **without a queue**:

```text
consumer asks -> producer computes one next batch -> consumer processes
```

Do not add channels merely to claim a bounded queue exists.

## 6.3 Blocking operators

`HashAggregate` is blocking in Phase 1:

```text
consume all input batches
        -> build local aggregate state
        -> emit result batch(es)
```

This is acceptable. Phase 2 will address memory pressure/spilling.

---

# 7. Provisional Physical Plan

Phase 1 needs enough plan structure to construct the operator tree deterministically, but should not pretend the final cross-phase IR has been solved.

A reasonable model is an enum/tree similar to:

```rust
pub enum LocalPlan {
    ParquetScan { ... },
    Filter { input: Box<LocalPlan>, predicate: Predicate },
    Project { input: Box<LocalPlan>, columns: Vec<ColumnRef> },
    HashAggregate { input: Box<LocalPlan>, ... },
    Limit { input: Box<LocalPlan>, limit: usize },
}
```

The exact representation may differ.

Requirements:

- plan construction must be deterministic;
- operator IDs must be assigned deterministically from plan structure/build order;
- engine execution must not depend on DataFusion planning;
- unsupported constructs fail clearly;
- no exchange/distribution concepts are required yet;
- do not freeze a large public API surface.

### 7.1 Predicate surface

Implement only what current Phase 1 workloads require.

At minimum:

```text
UInt64 column < UInt64 literal
```

A tiny expression/predicate enum is preferable to a hard-coded benchmark-case switch inside the Filter operator, but do not build a general SQL expression engine.

### 7.2 Projection surface

Projection should support selecting/reordering input columns required by the benchmark cases.

Use zero-copy `ArrayRef` reuse where possible.

---

# 8. Operator Requirements

## 8.1 ParquetScan

Responsibilities:

- consume an explicit ordered list of file paths supplied by the caller;
- read Arrow `RecordBatch` values through the public Parquet Arrow reader;
- obey configured batch size;
- expose schema;
- advance across files deterministically;
- check cancellation between batches/files;
- record scan metrics.

Important boundary:

`ade-exec` should **not know about `DatasetManifest`**. The `ade-local` adapter in `ade-bench` converts the validated manifest into explicit file inputs.

This avoids coupling the engine to benchmark infrastructure.

### Phase 1 simplification

Full Parquet projection/predicate pushdown is optional. It is acceptable to decode the required synthetic input columns and perform filter/project in ADE operators. Performance is measured but not a Phase 1 acceptance gate.

Do not spend substantial time matching DataFusion scan optimizations yet.

---

## 8.2 Filter

Required semantic case:

```text
secondary < threshold
```

Requirements:

- preserve row order from input;
- preserve schema unless the operator contract explicitly says otherwise;
- handle empty input/output batches;
- reject unsupported types/predicates cleanly;
- use Arrow kernels/public APIs where they reduce implementation risk;
- record input/output rows/batches and elapsed execution metrics.

Current synthetic data is non-null for the required columns, but the operator must not silently produce incorrect results if nulls appear. It may reject unsupported null semantics explicitly in Phase 1 if that is simpler and documented.

---

## 8.3 Project

Requirements:

- select/reorder columns;
- preserve row count;
- validate column references when the operator is built;
- reuse arrays without copying when possible;
- produce a correct output schema.

---

## 8.4 HashAggregate

Required semantics:

```text
GROUP BY key
SUM(value_i64)
COUNT(*)
```

Recommended state:

```text
HashMap<u64, AggState>
AggState { sum, count }
```

Requirements:

- deterministic correct output;
- detect or safely handle integer overflow rather than silently wrap;
- sort group keys before final output unless another deterministic order is guaranteed;
- expose aggregate-state memory accounting as an **estimate/tracked value**, not false allocator-precise memory;
- check cancellation at least once per input batch;
- record rows/batches/state-size/time metrics.

Using a wider accumulator such as `i128` internally and checked conversion to the chosen Arrow output type is recommended.

No spill or hard memory cap is required.

---

## 8.5 Limit

Requirements:

- `limit = 0` returns no rows without unnecessary upstream work where practical;
- stop reading upstream once the limit is satisfied;
- slice Arrow batches without copying when possible;
- handle limit crossing a batch boundary.

Limit does not need a benchmark workload in Phase 1. Focused unit/integration tests are sufficient.

---

## 8.6 Collect sink / executor

Provide one clear local execution entrypoint, conceptually:

```rust
ExecutionOutput {
    batches: Vec<RecordBatch>,
    metrics: ExecutionMetrics,
}
```

This sink/action may collect output in memory because Phase 1 datasets are bounded and the existing benchmark harness already collects outputs for validation.

Do not claim that sink collection is scalable for arbitrary result sizes.

---

# 9. Execution Context

Implement a small `ExecContext` or equivalent.

Phase 1 context should carry only real responsibilities:

- cancellation;
- metrics registry/collector;
- memory accounting hooks;
- execution identity/configuration needed by operators.

Do not add placeholders for:

- spill manager;
- state backend;
- object-store client;
- scheduler;
- lineage;
- distributed worker identity.

Those can be added when phases require them.

---

# 10. Deterministic Identity

Implement deterministic local operator identity.

A simple newtype is sufficient:

```rust
pub struct OperatorId(u32);
```

Properties:

- identical plan construction should produce identical operator-ID ordering;
- retry/random UUID semantics are not needed locally;
- IDs are used in metrics/errors/traces;
- no dependency on benchmark run UUIDs.

Do not over-design distributed task/attempt identity in Phase 1.

---

# 11. Cancellation

A lightweight cancellation token is sufficient, e.g. an `Arc<AtomicBool>`-backed type owned by the runtime.

Required behavior:

- executor can be cancelled;
- operators check at batch-level safe points;
- cancellation returns a specific error rather than partial success;
- blocking aggregate checks between input batches;
- no need for preemptive interruption in the middle of an Arrow kernel.

One focused cancellation integration test is enough.

---

# 12. Metrics

## 12.1 Required metrics

At minimum, per operator where applicable:

- input batches;
- input rows;
- output batches;
- output rows;
- elapsed/operator work time;
- tracked/estimated memory contribution where meaningful.

Execution-level:

- operators by `OperatorId`;
- total output rows/batches;
- tracked current/peak memory.

## 12.2 Benchmark artifact scope

Do **not** redesign the Phase 0 benchmark result schema solely to persist ADE operator metrics in Phase 1.

The local engine should return metrics programmatically and may emit them through tracing/debug output. The `ade-local` benchmark adapter may discard detailed metrics after optional tracing while the benchmark harness continues to record query timing and process RSS using the existing protocol.

A future phase can version the benchmark schema if engine-specific structured runtime metrics become a required artifact.

This intentionally avoids turning Phase 1 into another result-schema migration.

---

# 13. Memory Accounting Hooks

Phase 1 must establish **honest hooks**, not Phase 2 enforcement.

Recommended model:

```text
MemoryTracker
  current_tracked_bytes
  peak_tracked_bytes
```

Optionally use RAII reservation/guards if it remains simple.

Track categories that can be estimated reliably enough in Phase 1:

- active RecordBatch buffers observed by an operator;
- aggregate logical state / estimated entry footprint;
- output batch allocations.

Requirements:

- documentation must call this `tracked` or `estimated` memory, not exact allocator RSS;
- no hard memory limit is required;
- no spill response is required;
- counters cannot underflow;
- peak is monotonic;
- Phase 2 should be able to evolve this into enforceable reservations without replacing every operator API.

The existing harness's process RSS remains a separate whole-process measurement that includes validation/harness overhead.

---

# 14. `ade-local` Benchmark Adapter

## 14.1 Integration point

Extend the existing adapter model rather than creating a separate execution path.

Conceptually:

```rust
ResolvedEngine {
    DataFusion(DataFusionConfig),
    AdeLocal(AdeLocalConfig),
}
```

and:

```rust
struct AdeLocalEngine { ... }
```

implementing the existing `BenchmarkEngine` trait.

## 14.2 Configuration

Keep configuration tiny.

Required option:

```text
batch_size=<positive usize>
```

Optional only if clearly useful:

```text
trace_metrics=true|false
```

Do not add tuning knobs for future memory/spill behavior.

`EngineInfo` should identify:

```text
name = "ade-local"
version = ADE crate/package version
configuration = effective supported options
```

## 14.3 Workload-to-plan translation

The adapter translates existing semantic benchmark cases into ADE local plans.

### Scan

```text
ParquetScan -> Project(row_id, key, value_i64)
```

### Filter/project

```text
ParquetScan
 -> Filter(secondary < threshold)
 -> Project(key, value_i64)
```

### Aggregate

```text
ParquetScan
 -> Project(key, value_i64)  # optional if scan output already narrowed
 -> HashAggregate(group=key, sum=value_i64, count=*)
```

Low/high/skew benchmark specs share the same aggregate semantics; their datasets stress different state shapes.

### TPC-H Q1

Reject with an actionable unsupported-workload error.

Do not quietly delegate Q1 back to DataFusion under `ade-local`.

## 14.4 Manifest discipline

Use only files listed by the already validated `DatasetManifest`.

Reject:

- non-synthetic schemas for Phase 1 `ade-local`;
- missing input files;
- incompatible required schema;
- unsupported benchmark query variants.

---

# 15. Error Model

Add an ADE runtime error type in `ade-exec` with meaningful categories such as:

- I/O;
- Parquet;
- Arrow;
- invalid plan/schema;
- unsupported type/predicate;
- overflow;
- cancelled;
- internal invariant violation.

In `ade-bench`, preserve error context when crossing the adapter boundary.

Avoid:

- converting every ADE error into an opaque string too early;
- adding DataFusion error types to `ade-exec`;
- panics for malformed external plans/configuration.

Panics remain acceptable only for impossible internal test-only invariants where justified.

---

# 16. Testing Strategy

Testing should be focused and sufficient, not exhaustive for its own sake.

The quality target is to cover semantic boundaries and architecture invariants with a small number of high-value tests.

## 16.1 `ade-exec` focused unit tests

Recommended tests:

### Identity/context

1. deterministic operator IDs for the same plan;
2. cancellation stops execution;
3. memory tracker current/peak behavior.

### Scan

4. read a small multi-batch/multi-file Parquet fixture and preserve row count/order.

### Filter/project

5. filter + project returns expected rows/schema;
6. empty filter output or zero-row edge case.

### Aggregate

7. known group/sum/count result;
8. empty input and/or checked overflow behavior if easily isolated.

### Limit

9. limit across a batch boundary;
10. limit zero avoids unnecessary output/upstream progress where testable without fragile instrumentation.

These can be combined where sensible. Do not create a huge combinatorial test matrix.

## 16.2 Local pipeline integration

At least one in-crate pipeline test should execute:

```text
scan -> filter -> project -> collect
```

and one:

```text
scan -> aggregate -> collect
```

using real Parquet input.

## 16.3 `ade-bench` integration

Add one table-driven integration test that loops over the supported synthetic specs and verifies `ade-local` through the existing independent canonical validator.

This single table-driven test is preferable to five near-identical integration tests.

Also verify:

- `ade-local` engine resolution;
- invalid option rejection;
- TPC-H Q1 rejection;
- DataFusion path still works.

## 16.4 Existing Phase 0 regression coverage

Do not duplicate Phase 0 tests in `ade-exec`.

At final acceptance, run the existing workspace tests unchanged. If they pass, the harness regression surface is covered.

---

# 17. Development Validation Cadence

The following cadence is specifically intended to reduce implementation time while preserving quality.

## Checkpoint A — crate/core compiles

After `ade-exec` core types/context/errors exist:

```bash
cargo check -p ade-exec --locked
cargo test -p ade-exec <focused-test-filter> --locked
```

Do not run full workspace clippy yet.

## Checkpoint B — operators work

After scan/filter/project/aggregate/limit:

```bash
cargo test -p ade-exec --locked
```

This is the first complete crate test pass.

## Checkpoint C — adapter works

After `ade-local` integration:

```bash
cargo test -p ade-bench <phase1-or-adapter-filter> --locked
```

Run direct debug CLI validation against a tiny synthetic fixture.

## Checkpoint D — full correctness

Once implementation is functionally complete:

```bash
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
```

Do this full pass once, then only rerun commands affected by actual fixes.

## Checkpoint E — release evidence

Build once:

```bash
cargo build --release --workspace --locked
```

Then run only the focused Phase 1 acceptance benchmarks described below.

Existing Phase 0 TPC-H and Criterion acceptance do not need to be rerun unless Phase 1 changes their relevant code/dependencies or the full tests expose a regression.

---

# 18. Phase 1 Acceptance Benchmark

Performance is evidence, not a gate.

Use one deterministic synthetic dataset suitable for all supported specs, or separate low/high/skew fixtures if the existing workflow naturally requires them.

For development acceptance, keep iteration count modest:

```text
warmups = 1
measured iterations = 3
```

Recommended release comparisons:

1. `scan-filter-project` — `datafusion` vs `ade-local`;
2. `aggregate-high-cardinality` — `datafusion` vs `ade-local`;
3. `aggregate-skew` — `datafusion` vs `ade-local`.

Use the existing `compare` command so correctness fingerprint and logical dataset identity are checked before timing arithmetic.

Record:

- median/mean/stddev;
- peak process RSS when available;
- engine/config warnings;
- qualitative explanation of obvious performance differences.

Do not optimize merely to win these comparisons.

### Performance sanity rule

There is no required speed ratio in Phase 1.

However, investigate obviously pathological behavior such as:

- repeated full-file rereads inside one query;
- whole-dataset copying between every operator;
- quadratic aggregation behavior;
- accidental one-row batches;
- unnecessary serialization/deserialization.

A slower but structurally sound first engine is acceptable.

---

# 19. CI Changes

Preserve all Phase 0 CI checks.

Extend the tiny smoke path to validate `ade-local` on the supported synthetic semantics, preferably reusing the synthetic fixture already generated by `scripts/smoke.sh`.

Efficient CI pattern:

```text
existing DataFusion validations
+ ade-local validate scan/filter/aggregate specs on same small fixture
+ one ade-local 1-iteration run/check-result if useful
```

Do not add:

- performance thresholds;
- full release benchmarking;
- TPC-H `ade-local` tests;
- redundant generated fixtures.

---

# 20. Documentation / ADR Work

## 20.1 Required plan

Add this document as:

```text
docs/plans/phase-1-implementation-plan.md
```

if it is not already present when implementation begins.

## 20.2 ADRs

Expected durable decision:

### ADR-0005 — Phase 1 local runtime crate and pull execution model

Record:

- why `ade-exec` is a separate crate;
- dependency direction;
- why synchronous pull is used now;
- why it is not treated as a permanent streaming/distributed commitment;
- why memory accounting is observational in Phase 1 and enforceable limits are deferred to Phase 2.

Create additional ADRs only if implementation encounters another genuinely cross-cutting decision. Do not create ADRs for routine module layout choices.

## 20.3 `AGENTS.md`

Replace Phase-0-only active guidance with phase-aware language and a Phase 1 section covering:

- own local execution path;
- preserve harness contracts;
- no spill/distribution/streaming;
- focused test cadence;
- Phase 1 report path.

Do not delete still-useful benchmark-integrity guidance.

## 20.4 README

Update current status from Phase 0 complete to Phase 1 implementation complete when acceptance passes.

Add example:

```bash
ade-bench validate --engine ade-local ...
ade-bench run --engine ade-local ...
```

---

# 21. Implementation Workstreams

Use these as vertical slices. Complete each to a coherent state before widening scope.

## Workstream 1 — Add `ade-exec` core

Implement:

- crate/workspace wiring;
- error type;
- `OperatorId`;
- cancellation;
- metrics structures;
- memory tracker;
- execution context;
- minimal plan/operator contracts.

Focused tests only for these contracts.

**Exit:** `cargo check -p ade-exec` and focused core tests pass.

---

## Workstream 2 — Streaming unary operators

Implement:

- Parquet scan;
- filter;
- projection;
- limit.

Add focused unit/pipeline tests.

**Exit:** real Parquet `scan -> filter -> project -> limit/collect` works locally.

---

## Workstream 3 — Hash aggregate and local executor

Implement:

- aggregate state;
- checked sum/count output;
- deterministic result ordering;
- collect execution entrypoint;
- execution metrics return.

Add focused aggregate and memory-metric tests.

**Exit:** real Parquet `scan -> aggregate -> collect` works.

---

## Workstream 4 — `ade-local` adapter

Implement:

- config;
- engine resolution;
- benchmark-case-to-ADE-plan translation;
- ADE error propagation;
- supported synthetic workloads;
- Q1 rejection.

Add one table-driven benchmark-harness integration test.

**Exit:** all required synthetic specs validate through the existing independent oracle with `--engine ade-local`.

---

## Workstream 5 — CI/docs/acceptance

- update smoke script minimally;
- update AGENTS/README;
- add ADR-0005;
- run full workspace validation once;
- run focused release comparisons;
- write Phase 1 implementation report.

**Exit:** all acceptance criteria below are satisfied.

---

# 22. Acceptance Criteria

Phase 1 is complete when all required criteria are met.

## Architecture

- [x] `ade-exec` exists as a real local runtime crate.
- [x] `ade-exec` has no DataFusion or `ade-bench` dependency.
- [x] no Phase 2+ crates are added without real Phase 1 ownership.
- [x] execution is ADE-owned rather than delegated through DataFusion.
- [x] pull execution provides natural backpressure without hidden unbounded queues.
- [x] physical-plan API is explicitly provisional rather than falsely declared stable.

## Runtime

- [x] Parquet scan works across explicit manifest-listed files.
- [x] filter works for the required predicate.
- [x] projection works and is zero-copy where practical.
- [x] hash aggregate produces correct `key/sum/count` results.
- [x] limit works across batch boundaries and for zero.
- [x] collect sink/executor returns Arrow batches.
- [x] cancellation is observable and tested.
- [x] deterministic operator IDs are tested.
- [x] operator/execution metrics exist.
- [x] memory tracking exposes current/peak tracked values without claiming exact RSS or enforcing Phase 2 limits.

## Benchmark integration

- [x] `--engine ade-local` resolves through the existing engine adapter boundary.
- [x] `ade-local` records explicit effective configuration.
- [x] all five synthetic specs validate against the existing independent oracle.
- [x] unsupported Q1 fails clearly.
- [x] existing DataFusion adapter behavior remains intact.
- [x] cross-engine compare works on validated equivalent runs.

## Tests / quality

- [x] focused `ade-exec` semantic tests pass.
- [x] one table-driven `ade-local` harness test covers supported synthetic cases.
- [x] full workspace tests pass once final integration is complete.
- [x] final fmt/check/clippy pass succeeds.
- [x] existing Phase 0 smoke remains green.
- [x] CI includes focused `ade-local` correctness smoke without timing gates.

## Evidence / docs

- [x] ADR-0005 or equivalent decision record exists.
- [x] AGENTS active-phase guidance is updated.
- [x] README documents `ade-local` usage.
- [x] focused release comparisons are recorded as development evidence, not performance claims.
- [x] `docs/reports/phase-1-implementation-report.md` exists.
- [x] no Phase 2 spill/distribution work was started.

---

# 23. Phase 1 Implementation Report

Create:

```text
docs/reports/phase-1-implementation-report.md
```

Required sections:

1. summary;
2. implemented runtime architecture;
3. supported operators/workloads;
4. deviations from the plan and rationale;
5. ADRs/important decisions;
6. commands actually run;
7. focused test results;
8. final full-suite validation;
9. release benchmark comparison evidence;
10. memory/metrics behavior and limitations;
11. known limitations;
12. deferred Phase 2+ work;
13. recommended Phase 2 considerations.

Include approximate testing/validation strategy used so future phases can continue optimizing the workflow.

Do not report local benchmark numbers as product claims.

---

# 24. Known Phase 1 Limitations That Are Acceptable

These are expected and should not generate review findings by themselves:

- hash aggregate can consume memory proportional to group cardinality;
- no spill;
- no hard memory cap;
- no joins;
- no SQL frontend;
- no TPC-H support in `ade-local`;
- no async execution;
- no distributed workers;
- no scan pushdown requirement;
- collected final output resides in memory;
- metrics are not persisted into benchmark result schema;
- memory metrics are tracked/estimated rather than allocator-exact;
- local physical plan is provisional.

A reviewer should flag these only if the implementation falsely claims otherwise or creates architecture that blocks the intended next phases.

---

# 25. Handoff to Phase 2

Phase 2 will make memory a true managed resource.

The most important Phase 1 architectural gift to Phase 2 is not raw speed; it is a runtime where:

- each operator already receives an execution context;
- aggregate state has explicit tracked memory;
- batch flow is naturally backpressured;
- metrics identify where memory is consumed;
- engine execution is independent from the benchmark harness;
- DataFusion remains only a comparison/reference implementation.

Phase 2 should then be able to evolve:

```text
observational memory tracking
        -> enforceable reservations
        -> pressure response
        -> spillable aggregate
        -> external sort
```

without replacing the Phase 1 operator runtime wholesale.

---

# 26. Completion Definition

Phase 1 is successful when ADE can execute its first real query path itself:

```text
Parquet -> ADE operators -> Arrow results
```

and the existing Phase 0 harness independently proves those results correct.

The code should still be small enough for one developer to understand the entire local execution loop in a focused reading session.

That clarity is more valuable at this stage than broad SQL coverage, aggressive optimization, or generalized abstractions.
