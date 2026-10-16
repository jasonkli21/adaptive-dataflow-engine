# Adaptive Dataflow Engine (ADE) — Phase 2 Detailed Implementation Plan
## Spillable Local Engine

**Status:** Implemented and accepted; Sol High review remediation verified (see Phase 2 report)\
**Date:** 2026-10-15\
**Repository:** `jasonkli21/adaptive-dataflow-engine`\
**Repository baseline reviewed:** `985e1a9b1a7406de2f7004cc21e097ce99d40942` (`Implement Phase 1 ADE local execution engine`)\
**Depends on:** `docs/product/prd.md`, `docs/architecture/architecture.md`, `docs/plans/phase-1-implementation-plan.md`, `docs/reports/phase-1-implementation-report.md`, and accepted ADRs through ADR-0005\
**Primary objective:** make local ADE execution operate under an explicit managed-memory budget by spilling blocking operator state to local storage, while adding an external sort, bounded asynchronous scan prefetch, and truthful pressure/spill metrics without broadening into distributed execution.

---

# 1. Phase 2 Objective

Phase 1 proved that ADE owns a real, frontend-independent local physical runtime. Phase 2 turns that runtime from a bounded-data prototype into a resource-managed local engine.

The Phase 2 PRD contract is:

- worker memory manager;
- spillable aggregation;
- external sort;
- local partitioned spill;
- async prefetch;
- pressure metrics.

The central Phase 2 behavior is:

```text
operator needs managed memory
        |
        +-- reservation succeeds --> continue
        |
        +-- pressure / reservation denied
                    |
                    +-- spillable operator --> spill bounded state and retry
                    |
                    +-- non-spillable retained state --> return structured memory error
```

At the end of the phase, ADE must be able to run a high-cardinality aggregate and an external sort through `ade-local` under a deliberately constrained managed-memory budget, spill to execution-scoped local storage, clean up spill state on success/failure/cancellation, and return the same validated semantics as the independent benchmark oracle/DataFusion baseline.

Phase 2 is still a **single-node** phase. It creates the local memory/spill mechanisms that later exchange and adaptive-materialization phases will reuse, but it must not implement distributed exchange, scheduling, lineage recovery, or adaptive policy.

---

# 2. Source-of-Truth Order

Implementation must follow repository precedence:

1. `docs/product/prd.md` — phase outcome and product scope;
2. `docs/architecture/architecture.md` — long-lived invariants and boundaries;
3. this Phase 2 plan — detailed implementation contract;
4. accepted ADRs — concrete decisions/refinements;
5. implementation reports/current code — observed state.

Important architecture invariants for this phase include:

- **INV-4 — No hidden unbounded memory.** Queues, hash tables, buffers, state, and exchange memory require accounting hooks.
- **INV-5 — Runtime behavior is observable.** Resource pressure and spill decisions must be diagnosable.
- **INV-6 — Physical runtime remains frontend-independent.** `ade-exec` must not depend on DataFusion or `ade-bench`.
- **INV-9 — Optimization can be disabled.** Static/no-prefetch and no-spill baselines must remain available where meaningful.
- **INV-10 — Reuse adequate open formats/protocols rather than reinventing external standards.**

ADR-0005 remains the Phase 1 baseline: synchronous pull execution and the provisional `LocalPlan` remain valid unless Phase 2 discovers a concrete reason to refine them. Phase 2 should refine memory semantics; it should not casually replace the runtime model.

---

# 3. Current Repository State and Phase 2 Gaps

As of the reviewed baseline:

- workspace crates are `ade-bench` and `ade-exec`;
- `ade-exec` owns the local runtime and has no DataFusion dependency;
- `LocalPlan` currently supports Parquet scan, filter, project, hash aggregate, and limit;
- `execute` collects bounded output and returns `ExecutionOutput`;
- execution is synchronous and recursively pull-based;
- cancellation is cooperative;
- deterministic postorder `OperatorId`s exist;
- operator/execution metrics exist;
- memory tracking is observational only;
- aggregate state is one in-memory `HashMap`;
- aggregate finalization builds all output vectors at once;
- aggregate output is one batch;
- collected output remains in memory;
- Parquet scan has no prefetch;
- no spill files or spill lifecycle exist;
- no sort operator exists;
- `ade-local` supports the five synthetic Phase 1 cases and rejects TPC-H Q1;
- the benchmark harness owns canonical correctness and process isolation.

The Phase 1 report explicitly identifies Phase 2 needs:

- evolve memory hooks into reservations before aggregate allocation;
- distinguish ownership from shared-batch observation;
- bound aggregate output batches;
- account relevant finalization/decoder temporaries honestly;
- test pressure/error cleanup;
- introduce spill with an explicit lifecycle;
- retain the independent oracle and cross-engine contracts.

Phase 2 should build directly on those gaps rather than redesign successful Phase 0/1 infrastructure.

---

# 4. Development-Efficiency Goal

Phase 2 is materially more complex than Phase 1 because it introduces resource enforcement, local I/O, and limited concurrency. Correctness wins over speed, but iteration should stay disciplined.

Use vertical slices and narrow checks:

```text
memory manager
  -> focused memory tests

spill manager/lifecycle
  -> focused spill cleanup tests

spillable aggregate
  -> focused aggregate tests

external sort
  -> focused sort tests

prefetch
  -> focused scan/prefetch/cancellation tests

ade-local + benchmark semantics
  -> focused adapter/integration tests

integrated implementation
  -> workspace gate once
  -> focused release acceptance evidence once
```

Avoid repeatedly running:

- the full workspace suite after every edit;
- TPC-H generation/Q1 unless touched by an actual regression;
- Criterion microbenchmarks when microbenchmark infrastructure is unchanged;
- full release builds before semantics stabilize;
- large performance matrices;
- `cargo clean`;
- duplicated tests that exercise the same behavior at multiple layers.

A few high-value tests that force spill, cancellation, and cleanup are preferred over many shallow tests.

---

# 5. Required Phase 2 Scope

## 5.1 Enforced worker-local memory manager

Replace the Phase 1 observational-only memory model with an execution-scoped memory manager that has:

- a configured managed-memory limit;
- current managed bytes;
- peak managed bytes;
- reservation attempts;
- reservation denials / pressure events;
- explicit ownership attribution sufficient for operator metrics;
- safe release semantics;
- overflow/underflow protection;
- RAII or equivalently reliable release on early return;
- no process-global mutable pool.

Large ADE-owned state must reserve before allocation/growth where the code controls allocation.

The manager does **not** need to claim allocator-exact RSS. Phase 2 must clearly distinguish:

1. **enforced managed memory** — ADE-controlled retained state for which reservation semantics are meaningful; and
2. **observed/external/transient memory** — memory allocated inside Arrow/Parquet/public library calls where ADE cannot pre-reserve exactly.

Do not present the configured limit as a total-process RSS cap.

## 5.2 Spill manager

Add an execution-scoped local spill manager that:

- owns a unique temporary spill root;
- creates operator-scoped spill paths/files;
- prevents filename collisions;
- records bytes/files/read/write activity;
- cleans spill artifacts on normal success;
- cleans spill artifacts on operator error;
- cleans spill artifacts on cancellation;
- has no durable/recovery semantics;
- never treats local spill as surviving a worker/process loss.

The spill manager belongs to `ExecContext` or an equivalent execution-scoped service. Operators must not directly invent arbitrary global temp paths.

## 5.3 Spillable hash aggregation

Evolve the Phase 1 aggregate so state growth is reservation-backed and can spill.

Required semantic behavior remains:

```text
GROUP BY key
SUM(value_i64)
COUNT(*)
```

with the same checked arithmetic and null/type behavior unless an intentional ADR changes it.

Required spill behavior:

- aggregate state begins in memory;
- before hash-table growth, reserve managed bytes;
- when the target growth cannot fit or a defined pressure threshold is crossed, write partial aggregate state to local partitioned spill;
- clear/release in-memory state after successful spill;
- continue consuming input;
- merge partial states at finalization;
- bound finalization memory;
- emit output in bounded batches rather than constructing one unbounded result batch;
- preserve deterministic/reproducible output where practical;
- if a partition still cannot fit, repartition recursively or fail with a clear, deterministic “budget too small” error rather than silently exceeding the limit.

Do not implement distributed shuffle. “Partitioned spill” here is strictly worker-local temporary state.

## 5.4 External sort

Add a real external sort operator.

A correct implementation must be able to:

1. accumulate an in-memory sort run within a managed-memory target;
2. sort that run;
3. spill one or more sorted runs when required;
4. merge spilled runs with bounded memory;
5. emit sorted output in bounded batches.

The Phase 2 sort surface should remain intentionally narrow. Do not introduce a general expression engine just for sorting.

Recommended initial local-plan representation:

```rust
LocalPlan::Sort {
    input: Box<LocalPlan>,
    columns: Vec<SortKey>,
}
```

with a small supported `SortKey` surface sufficient for the Phase 2 synthetic workload. The benchmark workload should use a total deterministic order such as:

```text
ORDER BY key ASC, row_id ASC
```

where `row_id` breaks ties.

Use existing Arrow sort/take kernels where practical. Spill run encoding should reuse an existing Arrow-compatible local format (prefer Arrow IPC if the pinned public API is practical). If implementation evidence strongly favors a smaller internal format, record the decision in an ADR; do not invent a public storage format.

## 5.5 Local partitioned spill

Partitioning is a first-class requirement, not merely “write a file when full.”

At minimum the aggregate path must distribute spilled partial state by a deterministic hash partition so finalization can process partitions independently.

Requirements:

- deterministic partition function for a given key/config;
- bounded per-partition writer buffering;
- multiple spill generations/runs may contribute to the same logical partition;
- merge must combine repeated keys across batches and spill generations;
- skew must not create an unbounded in-memory finalization step;
- a too-small budget must terminate with a structured error instead of looping or continually repartitioning;
- spill paths/metadata are execution-local and disposable.

## 5.6 Bounded asynchronous Parquet prefetch

Add asynchronous prefetch to overlap scan/decode with downstream CPU work without converting the whole runtime to an async operator graph.

Preferred Phase 2 shape:

- retain synchronous downstream `next_batch` pull semantics;
- use a bounded background producer for Parquet reads when prefetch is enabled;
- use a bounded queue/channel;
- make queue depth configurable;
- `prefetch_batches=0` remains the synchronous baseline;
- propagate producer I/O/Parquet errors to the consumer;
- honor cancellation;
- ensure producer shutdown cannot deadlock on a full channel;
- join/clean up producer work before execution teardown;
- shut down an upstream subtree before returning a terminal `Limit` batch, and snapshot prefetch metrics after the producer joins;
- expose queue/prefetch metrics.

A standard-library bounded channel/thread is acceptable and preferable to introducing Tokio solely for local-file prefetch. If Tokio is introduced, there must be a concrete reason documented in an ADR.

The queue must be bounded. Any retained batch bytes should be observed and, where reasonably enforceable, integrated with the managed-memory model. Document unavoidable Parquet/Arrow allocation timing limitations rather than making a false hard-RSS claim.

## 5.7 Pressure and spill metrics

Extend metrics so Phase 2 behavior is observable.

At execution level, expose at least:

- configured managed-memory limit;
- managed current bytes;
- managed peak bytes;
- reservation attempts;
- reservation denials / pressure events;
- spill file count;
- spill write bytes;
- spill read bytes;
- spill events/runs;
- prefetch batches produced/consumed;
- prefetch queue peak depth;
- cancellation/error cleanup outcome if useful.

At operator level, expose useful attribution such as:

- peak managed state;
- spill count;
- spilled bytes;
- output rows/batches;
- existing work/input/output counters.

Do not change the stable benchmark result schema merely to persist every internal metric unless there is a demonstrated need. Runtime metrics can remain part of `ade_exec::ExecutionOutput` and debug tracing in Phase 2. Engine configuration must still be recorded in benchmark artifacts.

## 5.8 Benchmark integration

Extend `ade-local` configuration with a minimal set of real Phase 2 controls.

Recommended options:

```text
batch_size=<positive usize>
memory_limit_bytes=<positive usize>
spill_enabled=<true|false>
prefetch_batches=<non-negative usize>
```

Do not expose low-level knobs unless needed for reproducible experiments. Partition/run sizing should preferably be derived from the memory budget.

`EngineInfo.configuration` must record the effective values.

Default behavior should remain practical for all existing Phase 1 synthetic workloads. Choose and document a conservative finite default memory limit rather than silently reverting to “unlimited.”

## 5.9 New benchmark sort semantics

Add one engine-independent synthetic sort workload to make external sort benchmarkable.

Recommended semantic query:

```text
SELECT key, row_id, value_i64
FROM input
ORDER BY key ASC, row_id ASC
```

Required harness changes:

- add a narrow Phase 2 `Query` variant;
- add a strict TOML benchmark spec;
- implement matching DataFusion SQL;
- implement matching `ade-local` plan translation;
- extend the independent synthetic oracle;
- use an order-aware validation contract for this workload so the harness does not sort away an incorrect engine order;
- preserve existing workload semantic identity/version rules;
- do not weaken canonical validation for pre-existing workloads.

The existing five Phase 1 synthetic specs must continue to work unchanged.

## 5.10 Documentation and report

Update:

- `AGENTS.md` active phase and Phase 2 boundaries;
- `README.md` current status and Phase 2 local options/limitations;
- ADR(s) for durable Phase 2 decisions;
- `docs/plans/phase-2-implementation-plan.md` if the handed-off plan is not already present;
- `docs/reports/phase-2-implementation-report.md`.

---

# 6. Explicitly Out of Scope

Do **not** implement in Phase 2:

- distributed workers;
- network shuffle;
- exchange edges;
- remote shuffle service;
- object-store spill;
- peer replication;
- lineage or recomputation;
- coordinator/scheduler;
- worker placement;
- distributed fault recovery;
- adaptive materialization policy;
- cost model;
- joins;
- broad SQL support;
- TPC-H execution in ADE;
- streaming/watermarks/checkpoints/state backends;
- stable public physical IR;
- general expression language;
- vectorized code generation/JIT;
- SIMD-specific paths;
- custom allocator;
- GPU support;
- multi-tenant arbitration;
- spill durability across process failure.

Also avoid speculative subcrates such as `ade-memory`, `ade-spill`, or `ade-sort` unless implementation reveals a real ownership boundary that cannot remain cohesive inside `ade-exec`.

---

# 7. Target Code Organization

Phase 2 makes the current single `lib.rs` large enough that modest modularization is justified.

Recommended target:

```text
crates/ade-exec/src/
  lib.rs
  error.rs
  plan.rs
  runtime.rs
  memory.rs
  spill.rs
  operators/
    mod.rs
    scan.rs
    filter.rs
    project.rs
    aggregate.rs
    sort.rs
    limit.rs
  tests.rs
```

This is a recommendation, not a requirement. Do not spend time moving code only for aesthetics. A smaller module split is fine if responsibilities remain clear.

Keep:

```text
ade-bench -> ade-exec
ade-exec -X-> ade-bench
ade-exec -X-> DataFusion
```

No new crate is required for Phase 2.

---

# 8. Execution Configuration

Introduce an execution configuration owned by `ade-exec` rather than passing individual services ad hoc.

Illustrative only:

```rust
pub struct ExecutionConfig {
    pub memory_limit_bytes: usize,
    pub spill_enabled: bool,
    pub prefetch_batches: usize,
    pub output_batch_rows: usize,
}
```

Possible entrypoint:

```rust
pub fn execute(
    plan: LocalPlan,
    config: ExecutionConfig,
    cancellation: CancellationToken,
) -> Result<ExecutionOutput>
```

If preserving a compatibility helper is useful, keep a default-config wrapper, but avoid two independent execution paths.

Validation:

- memory limit > 0;
- output batch rows > 0;
- prefetch depth has a reasonable upper bound;
- invalid combinations return structured configuration errors;
- `spill_enabled=false` under insufficient memory returns a memory-limit error rather than spilling.

Do not make spill directory a required user-facing option. Execution-scoped temporary storage is safer by default.

---

# 9. Memory Manager Design

## 9.1 Reservation semantics

A reservation represents ADE-owned retained bytes.

Preferred semantics:

```rust
reservation = memory.try_reserve(owner, bytes)?
reservation.grow(additional)?
reservation.shrink(bytes)?
drop(reservation) // releases remainder
```

Equivalent APIs are fine.

Requirements:

- reservation success must be atomic with respect to the manager;
- failed growth must leave existing reservation unchanged;
- manager counters never underflow;
- integer overflow is checked;
- drop/early-return releases owned bytes;
- metrics remain truthful after failures.

Because prefetch introduces a producer thread, choose primitives that are safe for the concurrency actually used. Avoid a complicated global allocator.

## 9.2 What must be enforced

Enforce reservations for major ADE-owned retained structures where allocation is under ADE control:

- aggregate hash-table capacity;
- aggregate merge/finalization buffers;
- sort run accumulation;
- sort merge output buffers;
- retained collected output, unless the API is changed to transfer ownership in a clearly bounded way;
- explicit prefetch buffering where feasible.

## 9.3 What may remain observed/approximate

Be explicit for:

- Parquet decoder internals;
- Arrow kernel temporaries created inside public APIs;
- allocator metadata;
- transient capacity differences not knowable before an upstream allocation.

These still need observation where practical, but they must not be mislabeled as hard-enforced bytes.

## 9.4 Pressure threshold/headroom

Do not let spillable state consume the entire configured limit before trying to spill.

Reserve headroom for:

- one input batch;
- one output batch;
- spill serialization;
- merge state;
- bounded prefetch.

A simple fixed fraction/derived target is acceptable for Phase 2 if documented and deterministic. Avoid a sophisticated adaptive policy; policy is later.

---

# 10. Spill Manager Design

Illustrative responsibilities:

```rust
SpillManager
  create_file(operator_id, kind, sequence)
  note_write(bytes)
  note_read(bytes)
  remove/close
  cleanup_all_on_drop
```

Use an execution-owned temporary directory (`tempfile::TempDir` or equivalent).

Requirements:

- spill root creation failure is a structured I/O error;
- partial file creation is cleaned;
- file handles close before cleanup;
- no path traversal from workload input;
- filenames may include operator ID/kind/sequence but do not need to be user-stable;
- successful execution should leave no spill artifacts;
- failed/cancelled execution should leave no spill artifacts under normal process teardown;
- tests may inject/use a known parent temp directory to verify cleanup.

---

# 11. Spillable Aggregate Algorithm

A contained Phase 2 algorithm is sufficient.

## 11.1 Build phase

For each input batch:

1. validate cancellation;
2. inspect keys/values with existing type/null semantics;
3. before hash-table growth, ensure sufficient reservation;
4. update `(sum: i128, count: u64)` with checked arithmetic;
5. when state reaches the spill target or growth reservation fails:
   - if spill disabled: return structured memory error;
   - otherwise partition and spill current partial groups;
   - only release/clear state after spill data is safely written;
   - continue.

Do not call an allocation-growing `HashMap` API first and “account later.”

## 11.2 Partitioning

Use a deterministic hash/partition function.

The number of partitions may be derived from estimated state size and target per-partition memory. Prefer a power-of-two partition count for simple routing if appropriate.

Multiple spills may append/create runs per partition.

## 11.3 Spill encoding

Aggregate spill state is internal and fixed-schema. A small versioned internal codec for:

```text
u64 key
i128 partial_sum
u64 partial_count
```

is acceptable.

If using a custom codec:

- include magic/version;
- define endian explicitly;
- detect truncated records;
- use buffered I/O;
- do not expose it as a public interchange format.

## 11.4 Finalization

For each logical partition in deterministic order:

1. read its partial records/runs;
2. merge into a fresh reservation-backed hash map;
3. if it cannot fit:
   - repartition the partition at a deeper level, with a bounded maximum depth; or
   - return a deterministic budget-too-small error if safe progress cannot be made;
4. finalize that partition;
5. emit output in bounded batches;
6. release partition state before moving to the next partition.

If no spill occurred, use the same bounded finalization path where practical rather than maintaining two very different algorithms.

## 11.5 Correctness traps to test

- same key appears in multiple source batches;
- same key appears in multiple spill generations;
- negative `value_i64`;
- sum output overflows final `i64`;
- count overflow paths remain checked;
- skew/hot key;
- empty input/filter result;
- cancellation during build;
- cancellation during merge;
- terminal `Limit` releases upstream reservations before returning its last batch;
- completed execution metrics include producer activity through shutdown/join;
- spill write/read failure;
- too-small budget;
- cleanup after every error path.

---

# 12. External Sort Algorithm

## 12.1 Run generation

Accumulate input batches/rows while reservations remain under the sort run target.

When target reached:

1. concatenate/materialize the run using bounded/reserved state;
2. sort using Arrow public kernels;
3. spill the sorted run;
4. release run memory;
5. continue.

If all data fits, sort in memory and emit in bounded batches.

## 12.2 Spill format

Prefer Arrow IPC stream/file for sorted runs if the pinned Arrow 59.3.1 public API is straightforward.

Reasons:

- schema is preserved;
- avoids a benchmark-specific row codec;
- matches ADE's Arrow boundary;
- reusable for future local operators without creating a public format contract.

Keep dependency features narrow.

## 12.3 Merge

Use a bounded k-way merge.

Requirements:

- one bounded read buffer/current batch per active run, or a similarly bounded design;
- deterministic comparator;
- lexicographic key order;
- tie-breaker included in the benchmark semantics;
- output batches capped by `output_batch_rows`;
- no loading all runs back into memory;
- cancellation checks during merge;
- read errors propagate;
- run files clean up.

If the number of runs becomes too large for the selected merge fan-in/memory budget, perform multi-pass merging rather than opening/holding unbounded run state.

## 12.4 Scope surface

Do not build a universal SQL sort implementation. Support only the types/order semantics necessary for the Phase 2 local-plan API and synthetic benchmark, using Arrow primitives where possible.

---

# 13. Bounded Prefetch Design

Keep the existing consumer pull model.

A possible shape:

```text
Parquet reader producer
      |
      v
bounded channel (N batches)
      |
      v
scan.next_batch()
      |
      v
downstream pull operators
```

Requirements:

- depth 0 = current synchronous behavior;
- positive depth starts a bounded producer;
- queue capacity is exact and finite;
- producer errors are transmitted once and terminate the stream;
- cancellation reaches producer;
- dropping the scan/receiver cannot leave producer permanently blocked on send;
- producer thread is joined or otherwise deterministically finished during teardown;
- no detached background thread survives execution;
- file order and row order remain identical to synchronous scan;
- schema mismatch remains an error;
- metrics record produced/consumed/queue-peak activity.

Do not introduce speculative I/O scheduling or remote-object abstractions yet.

---

# 14. Collection and Output Memory

Phase 1 `execute` returns all `RecordBatch` outputs, so collection itself can be a memory consumer.

Phase 2 must not ignore this.

Preferred rule:

- collected output participates in managed memory;
- each retained output batch must be accounted;
- if collection alone exceeds the configured managed budget, execution returns a structured memory-limit error.

This means spill can make blocking operators bounded, but a user can still request a result too large to collect under the chosen budget. That is correct and explicit.

Do not silently exempt collection from the memory limit just to make tests pass.

A streaming/non-collecting sink can be a later extension; do not redesign the benchmark adapter around it in Phase 2 unless absolutely necessary.

---

# 15. Error and Cancellation Semantics

Add structured errors for at least:

- invalid execution/memory configuration;
- managed memory limit exceeded / reservation denied where spill is unavailable;
- spill disabled under pressure;
- spill codec corruption/truncation;
- spill recursion/fan-in cannot make progress;
- prefetch producer failure;
- existing I/O/Parquet/Arrow/cancellation errors.

Cancellation rules:

- no partial successful `ExecutionOutput`;
- check during long aggregate/sort loops, not only between whole files;
- prefetch producer observes cancellation;
- spill files are cleaned;
- reservations are released;
- channels/threads terminate.

Avoid panics for expected resource pressure.

---

# 16. Metrics Semantics

Make metrics precise enough that a reviewer can answer:

- Did spill actually happen?
- How much managed memory did the execution peak at?
- Was the configured limit respected by managed reservations?
- How many reservation denials/pressure events occurred?
- How much spill data was written/read?
- Did sort create multiple runs?
- Was prefetch enabled and how deep did it get?
- Which operator spilled?

Keep “managed peak” separate from “observed Arrow bytes” if both exist.

Do not aggregate unlike concepts into a number named simply `memory` without documenting semantics.

---

# 17. Benchmark Harness Changes

## 17.1 `ade-local` adapter

Extend configuration parsing and `EngineInfo`.

Examples:

```text
--engine-option batch_size=8192
--engine-option memory_limit_bytes=8388608
--engine-option spill_enabled=true
--engine-option prefetch_batches=1
```

Reject:

- duplicate/unknown options if existing conventions do;
- zero memory limit;
- invalid booleans;
- unreasonable overflow values.

Build `ExecutionConfig` from the parsed adapter config.

## 17.2 Sort workload

Add one TOML spec, for example:

```text
benchmarks/specs/sort-key-rowid.toml
```

Use the existing synthetic dataset.

The independent oracle must compute the expected sorted sequence directly from deterministic generated rows.

For this query, canonicalization must preserve row order and compare that ordered sequence. Do not reuse the pre-existing “sort canonical rows before hashing” behavior because that would fail to test sort correctness.

DataFusion and ADE must produce the same ordered semantic output.

## 17.3 Existing workloads

Existing Phase 1 workload files and semantics must remain unchanged.

Aggregate workloads should be usable in:

- normal finite-memory mode;
- forced-spill mode using a lower `memory_limit_bytes`;
- no-spill baseline (`spill_enabled=false`) that fails clearly if the budget is insufficient.

---

# 18. CI / Smoke Strategy

Keep CI correctness-oriented.

Update the existing smoke flow only enough to cover high-value Phase 2 behavior.

Recommended:

1. existing Phase 1 synthetic validations remain;
2. new sort spec validates on DataFusion and ADE;
3. one deliberately constrained ADE aggregate run forces spill and validates;
4. optionally one constrained ADE sort run forces multiple runs and validates if this stays fast.

Do not add:

- performance thresholds;
- large fixtures;
- large iteration counts;
- TPC-H reruns unrelated to Phase 2;
- extensive matrix combinations.

The smoke suite should remain a correctness gate, not a benchmark lab.

---

# 19. Implementation Sequence

## Slice A — reconcile docs and baseline

Before code:

- inspect current HEAD and working tree;
- read PRD/architecture/Phase 1 report/ADR-0005;
- confirm whether the baseline moved after this plan was written;
- add the Phase 2 plan to `docs/plans/` if needed;
- update `AGENTS.md` only when implementation begins.

No broad refactor yet.

## Slice B — memory manager

Implement:

- finite execution memory config;
- reservation object;
- manager metrics;
- structured pressure errors;
- compatibility wiring through `ExecContext`.

Focused tests only.

Do not spill yet.

## Slice C — spill manager

Implement:

- execution temp root;
- file lifecycle;
- spill metrics;
- cleanup guarantees.

Focused success/failure/cancellation cleanup tests.

## Slice D — spillable aggregate

Refactor aggregate state to use reservations and local partition spill.

Acceptance for this slice:

- existing aggregate semantics still pass;
- a small deterministic fixture forced under low memory spills;
- output equals in-memory result;
- spill metrics are nonzero;
- managed peak does not exceed the configured managed limit;
- spill files are gone after execution.

## Slice E — bounded aggregate output

Ensure finalization emits batches capped by `output_batch_rows` and avoids one all-groups batch.

Test cross-batch output.

## Slice F — external sort

Add local sort plan/operator, run spill, merge, bounded output, metrics.

Focused tests force:

- no-spill sort;
- multi-run external sort;
- duplicate key/tie handling;
- merge across several runs;
- cancellation/error cleanup.

## Slice G — prefetch

Add bounded scan prefetch with depth 0 baseline.

Test:

- output identity/order with and without prefetch;
- bounded depth/metrics;
- cancellation;
- producer error;
- teardown without hangs.

## Slice H — benchmark integration

Add:

- adapter options;
- new sort semantic spec/oracle/DataFusion/ADE path;
- focused integration coverage;
- forced-spill aggregate/sort validation.

Preserve result schema unless a concrete blocker requires a versioned change.

## Slice I — documentation and final validation

Update:

- ADR(s);
- README;
- AGENTS;
- Phase 2 implementation report.

Then run the full gate once.

---

# 20. Focused Test Plan

The goal is coverage of Phase 2 failure boundaries, not test-count maximization.

## 20.1 `ade-exec` focused tests

A compact set can cover:

### Memory manager
- reserve/grow/shrink/drop;
- exact-limit success;
- over-limit denial without counter mutation;
- overflow/underflow protection;
- peak/pressure metrics.

### Spill lifecycle
- temp root/files removed after success;
- removed after injected error;
- removed after cancellation.

### Aggregate
- existing in-memory expected values;
- forced spill equals in-memory output;
- key repeated across multiple spill generations merges once;
- high-cardinality multi-partition spill;
- skewed/hot key;
- bounded output batches;
- too-small budget returns structured error;
- final `i64` sum overflow remains an error.

### Sort
- in-memory deterministic order;
- forced multi-run spill/merge order;
- ties resolved by second key;
- output batch bound;
- too-small budget/fan-in failure is structured;
- cleanup on error/cancel.
- an exhausted sort returns EOF without reserving output-builder memory;
- the final in-memory sort batch releases its retained run reservation before return;

### Prefetch
- depth 0 and positive depth produce identical batches/order;
- queue depth never exceeds config;
- producer error propagates;
- cancellation/early consumer drop terminates producer.
- terminal `Limit` stops and joins prefetch before returning its last batch.

Where practical, combine related assertions into table-driven tests rather than many single-assertion tests.

## 20.2 `ade-bench` focused integration

One table-driven Phase 2 integration test should be enough to cover:

- all existing synthetic workloads still validate on ADE;
- sort validates on ADE and DataFusion;
- cross-engine sort fingerprint/order contract;
- constrained aggregate visibly spills;
- constrained sort creates external runs;
- configuration parse/reject cases.

Do not duplicate all Phase 0 protocol tests.

---

# 21. Final Validation Commands

Use repository/toolchain conventions. Prefer locked/offline after dependencies are present.

During implementation, run narrow commands such as:

```bash
cargo test -p ade-exec <focused_filter> --locked --offline
cargo test -p ade-bench --test phase2 --locked --offline
cargo check -p ade-exec --locked --offline
```

Near completion, run once:

```bash
cargo fmt --all --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo test --workspace --all-features --locked --offline
bash scripts/smoke.sh
git diff --check
```

Run a release build only after correctness is stable:

```bash
cargo build --release --workspace --locked --offline
```

Do not repeatedly rebuild release during normal iteration.

If a dependency/feature addition requires one unlocked fetch/update, do it intentionally, inspect the lockfile delta, then return to locked checks.

---

# 22. Focused Release Acceptance Evidence

Phase 2 needs evidence that the new mechanisms execute correctly, not a broad performance study.

Use a small number of deterministic runs.

Recommended evidence:

## A. Spillable high-cardinality aggregate

Run the same fixture/workload with:

1. `ade-local` generous memory / spill enabled;
2. `ade-local` constrained memory that forces spill;
3. DataFusion baseline.

Requirements:

- all pass independent validation;
- ADE constrained and unconstrained canonical fingerprints match;
- constrained run reports nonzero spill activity;
- result artifacts pass `check-result`;
- cross-engine comparison is semantically valid.

## B. External sort

Run:

1. `ade-local` constrained memory that forces multiple runs;
2. DataFusion baseline.

Requirements:

- ordered oracle validation passes;
- spill/run metrics confirm external path;
- output batch bound is respected;
- cross-engine semantic result matches.

Use approximately:

```text
1 warmup
3 measured iterations
```

only if consistent with existing harness defaults/reporting. The purpose is reproducible evidence and rough regression visibility, not a statistically exhaustive performance claim.

No performance gate is required.

---

# 23. Documentation / ADR Requirements

At least one new ADR is likely justified.

Suggested topic:

`ADR-0006: Managed local memory, spill lifecycle, and external sort`

It should record:

- what the Phase 2 memory limit does and does not guarantee;
- reservation semantics;
- local spill is execution-scoped/lossy;
- spillable aggregate partition strategy;
- sort run format/merge approach;
- bounded prefetch mechanism;
- why synchronous pull remains the operator model;
- any new Arrow feature/dependency;
- deviations from architecture recommendations.

A separate ADR is warranted only if prefetch or spill encoding introduces a distinct durable decision.

Avoid writing ADRs for trivial naming or file-layout choices.

---

# 24. Phase 2 Implementation Report

Create:

```text
docs/reports/phase-2-implementation-report.md
```

Include:

1. summary;
2. baseline commit and final commit;
3. implemented scope;
4. memory-manager semantics and explicit non-guarantees;
5. spill lifecycle/format;
6. aggregate spill algorithm;
7. external sort algorithm;
8. prefetch design;
9. metrics added;
10. benchmark-harness changes;
11. deviations from this plan and why;
12. ADRs;
13. commands actually run;
14. focused test results;
15. final workspace validation;
16. smoke results;
17. focused release acceptance evidence;
18. cleanup/cancellation evidence;
19. known limitations;
20. deferred Phase 3+ work;
21. recommended Phase 3 considerations;
22. acceptance audit against this plan.

Do not claim a command passed if it was not run.

---

# 25. Acceptance Criteria

Phase 2 is complete only when all applicable items below are true.

## Architecture / boundaries

- [x] `ade-exec` remains independent of `ade-bench` and DataFusion.
- [x] No distributed exchange/scheduler/recovery code was introduced.
- [x] No unnecessary future crates were added.
- [x] Synchronous pull remains viable; prefetch does not force the whole runtime async without justification.
- [x] No hidden unbounded queue/state was introduced.
- [x] Phase 2 decisions are documented where durable.

## Memory manager

- [x] Execution has a finite configured managed-memory limit.
- [x] Major ADE-owned blocking state reserves before controlled growth.
- [x] Reservation failure does not corrupt counters/state.
- [x] Managed current/peak counters are correct on success and failure.
- [x] Collection/output retained memory is not silently exempt.
- [x] Non-enforced Arrow/Parquet/transient memory is clearly distinguished from managed memory.
- [x] Managed peak never exceeds configured limit in forced-pressure tests.

## Spill

- [x] Spill manager is execution-scoped.
- [x] Spill files are cleaned after success.
- [x] Spill files are cleaned after failure.
- [x] Spill files are cleaned after cancellation.
- [x] Spill bytes/files/events are observable.
- [x] Spill is explicitly local/lossy, not recovery-durable.

## Aggregation

- [x] Existing aggregate semantics remain correct.
- [x] Hash state is reservation-backed.
- [x] Low-memory execution actually spills.
- [x] Spill is partitioned.
- [x] Repeated keys across spill generations merge correctly.
- [x] Skew does not trigger silent unbounded finalization.
- [x] Too-small budgets fail clearly.
- [x] Aggregate output is emitted in bounded batches.
- [x] Arithmetic overflow behavior remains correct.

## External sort

- [x] Real external run generation exists.
- [x] Multi-run merge is bounded.
- [x] Low-memory test forces the external path.
- [x] Ordered output is correct, including ties.
- [x] Output is emitted in bounded batches.
- [x] Multi-pass/fan-in behavior or a safe explicit bound prevents unbounded run state.
- [x] Sort spill cleanup works on success/error/cancel.

## Prefetch

- [x] Prefetch depth is configurable and bounded.
- [x] Depth 0 preserves synchronous baseline behavior.
- [x] Prefetch preserves file/row semantics.
- [x] Producer errors propagate.
- [x] Cancellation terminates producer.
- [x] No background producer leaks/deadlocks after execution.
- [x] Prefetch metrics are exposed.

## Benchmark integration

- [x] Existing five synthetic specs still validate on `ade-local`.
- [x] New sort spec is engine-independent.
- [x] DataFusion supports the sort semantic baseline.
- [x] The independent oracle validates sort order rather than sorting the result away.
- [x] `ade-local` config records memory/spill/prefetch settings.
- [x] A forced-spill aggregate passes independent validation.
- [x] A forced-spill external sort passes independent validation.
- [x] Benchmark result/protocol integrity is preserved or any versioned change is explicitly justified.

## Quality / validation

- [x] Focused Phase 2 tests cover pressure, spill, cleanup, sort, prefetch, and configuration.
- [x] Tests are not duplicated excessively across layers.
- [x] `cargo fmt --all --check` passes.
- [x] workspace check passes.
- [x] Clippy with `-D warnings` passes.
- [x] workspace tests pass.
- [x] smoke passes.
- [x] focused release acceptance evidence is captured.
- [x] no performance thresholds were added to CI.

## Documentation / phase boundary

- [x] `AGENTS.md` reflects Phase 2.
- [x] README reflects Phase 2.
- [x] ADR(s) capture durable decisions.
- [x] Phase 2 implementation report is complete and truthful.
- [x] No Phase 3 distributed execution work was started as “cleanup.”

---

# 26. Known Design Risks to Watch

## Risk: the “hard limit” is overstated

Arrow/Parquet may allocate transient buffers that ADE cannot reserve before the library call.

**Mitigation:** call it a managed-memory limit, enforce what ADE owns, separately observe/describe external/transient memory, and never claim allocator-exact RSS.

## Risk: spill happens after allocation

Accounting a `HashMap` only after it grows defeats the purpose.

**Mitigation:** explicitly reserve growth before calling allocation-growing APIs.

## Risk: aggregate finalization becomes the new unbounded step

Spilling build state but loading all partitions/results at once is not sufficient.

**Mitigation:** finalize one partition at a time and emit bounded output batches.

## Risk: skewed partition still does not fit

A fixed partition count can fail under extreme skew/cardinality.

**Mitigation:** bounded recursive repartition or a clear no-progress/budget-too-small error.

## Risk: external sort is actually “spill then reload everything”

That is not external sort.

**Mitigation:** bounded run merge with bounded active buffers and multi-pass merge/fan-in control.

## Risk: prefetch deadlocks under cancellation/early drop

A producer blocked on a full bounded channel can outlive the consumer.

**Mitigation:** explicit shutdown/cancellation, receiver-drop handling, and a focused teardown test.

## Risk: benchmark canonicalization hides broken ordering

Existing integer canonicalization sorts output rows.

**Mitigation:** add order-aware validation for the sort semantic contract.

## Risk: Phase 2 becomes an architecture rewrite

Memory/spill pressure is enough complexity.

**Mitigation:** keep the provisional plan and pull engine, add only the mechanisms required by the PRD, and defer distribution/adaptation.

---

# 27. Phase Boundary / Handoff to Phase 3

Phase 2 should leave the engine with reusable local mechanisms:

```text
managed memory
local spill
spillable blocking operators
external sort
bounded prefetch
resource-pressure metrics
```

but **not** a distributed runtime.

The likely next phase can then introduce distribution/exchange using these local guarantees rather than simultaneously solving local memory, spill, network transport, and scheduling.

Do not pre-build Phase 3 abstractions in this phase.
