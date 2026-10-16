# Adaptive Dataflow Engine (ADE) — Phase 3 Detailed Implementation Plan
## Static Distributed Batch v0

**Status:** Implemented; acceptance verified (see Phase 3 implementation report)\
**Date:** 2026-10-15\
**Repository:** `jasonkli21/adaptive-dataflow-engine`\
**Repository baseline reviewed:** `bc0dbb0759f941f188686d73dc3190645f30ef89` (`Fix Phase 2 terminal resource lifecycle`)\
**Depends on:** `docs/product/prd.md`, `docs/architecture/architecture.md`, `docs/plans/phase-2-implementation-plan.md`, `docs/reports/phase-2-implementation-report.md`, `AGENTS.md`, and accepted ADRs through ADR-0006\
**Primary objective:** prove that ADE can execute one real multi-process distributed batch graph with static workers and a real hash exchange while preserving Phase 0–2 correctness, bounded-resource discipline, observability, and clean failure/cancellation behavior.

---

# 1. Phase 3 Objective

Phase 0 proved measurement. Phase 1 proved ADE-owned local execution. Phase 2 proved that the local runtime can manage memory pressure and spill blocking state.

Phase 3 must prove **distribution**.

The PRD contract is intentionally narrow:

```text
one coordinator
      |
      +-------------------------------+
      |                               |
      v                               v
 static worker 0                 static worker N-1

scan
  -> partial aggregate
  -> hash exchange
  -> final aggregate
  -> gather
```

The required graph is:

```text
scan -> partial aggregate -> hash exchange -> final aggregate -> gather
```

The exit condition is correct execution with **2–4 worker processes/nodes** and observable network/work distribution.

Phase 3 is not the adaptive-exchange phase. It should establish the smallest trustworthy distributed runtime on which Phase 4 can build.

At completion, ADE should be able to:

1. start or connect to a fixed set of workers;
2. deterministically assign input files to producer tasks;
3. run local partial aggregation using `ade-exec`;
4. repartition partial aggregate rows by a deterministic hash function;
5. transfer partitions over a real network transport;
6. materialize the received Phase 3 exchange in bounded, execution-scoped local state;
7. run final aggregation on the destination worker;
8. gather final results at the coordinator;
9. validate the result against the existing independent oracle/DataFusion baseline;
10. report per-worker and network distribution metrics;
11. detect a worker/control/data-plane failure and fail the job cleanly;
12. cancel/clean up surviving work rather than hanging or silently returning partial success.

No retry or distributed recovery is required.

---

# 2. Source-of-Truth Order

Implementation follows repository precedence:

1. `docs/product/prd.md` — product scope and Phase 3 outcome;
2. `docs/architecture/architecture.md` — long-lived invariants and boundaries;
3. this Phase 3 plan — detailed implementation contract;
4. accepted ADRs — concrete decisions/refinements;
5. implementation reports/current code — observed state.

Relevant architecture invariants:

- **INV-1 — Logical identity is separate from execution attempt.**
- **INV-2 — Distribution semantics are separate from storage mechanism.**
- **INV-3 — Policy cannot bypass correctness.**
- **INV-4 — No hidden unbounded memory.**
- **INV-5 — Runtime behavior is observable.**
- **INV-6 — Physical runtime remains frontend-independent.**
- **INV-7 — Worker-local state is assumed lossy unless explicitly promoted.**
- **INV-9 — Optimization can be disabled / static baselines remain available.**
- **INV-10 — Reuse adequate open formats/protocols rather than inventing public standards unnecessarily.**

Phase 3 may resolve the architecture backlog around the first control-plane protocol and first exchange transport. Any material refinement should be recorded in ADRs.

---

# 3. Current Repository State

The reviewed baseline is Phase 2-complete.

Current facts that Phase 3 should build on rather than replace:

- workspace crates are `ade-bench` and `ade-exec`;
- `ade-exec` owns synchronous pull-based local execution;
- `LocalPlan` supports:
  - Parquet scan;
  - filter;
  - project;
  - spillable hash aggregate;
  - external sort;
  - limit;
- local execution has:
  - deterministic postorder `OperatorId`s;
  - cooperative cancellation;
  - finite managed-memory accounting;
  - execution-scoped local spill;
  - bounded Parquet prefetch;
  - bounded aggregate/sort output batches;
  - idempotent subtree shutdown;
- `execute_with_config` currently collects the whole final result;
- `ade-local` remains independent from DataFusion;
- `ade-bench` owns canonical workload semantics, independent correctness validation, process isolation, and result artifacts;
- Phase 2 intentionally contains no exchange, scheduler, coordinator, worker service, lineage, recovery, or distributed transport.

The Phase 2 report calls out several Phase 3-relevant constraints:

- the managed-memory ledger is **not** a process-wide allocator/RSS cap;
- exchange ownership transfer must be explicit;
- cancellation must cross worker boundaries cleanly;
- a streaming/non-collecting sink is desirable before claiming arbitrary larger-than-memory distributed results;
- local spill remains worker-local and disposable.

---

# 4. Phase 3 Design Principles

## 4.1 Prove distribution, not the entire future architecture

Phase 3 should not try to build:

- a general scheduler;
- dynamic task stealing;
- adaptive exchange;
- durable shuffle;
- lineage recovery;
- retries;
- elastic workers;
- stable public physical IR;
- broad SQL.

A small static distributed path is preferable to a speculative framework.

## 4.2 Distribution semantics must not equal one storage mechanism

The graph requires a **hash distribution**. Phase 3 may use one fixed transport/retention mechanism, but the code should not encode:

```text
Hash(keys) == local materialized shuffle
```

Instead:

```text
logical distribution: Hash(key)
Phase 3 mechanism: one fixed, explicit exchange implementation
```

This leaves Phase 4 free to add direct stream, buffering, spill, full local materialization, and durable materialization behind a common exchange boundary.

## 4.3 Backpressure must remain bounded

Do not add an unbounded in-memory network queue.

A Phase 3 producer should block/yield when the destination cannot accept more data. The implementation may use blocking I/O for this phase if the boundary is clean and documented; it does not need to convert the local operator runtime to async.

## 4.4 No hidden durability

Any received exchange file/state is:

```text
execution-scoped
worker-local
ephemeral
lost if the worker/process is lost
```

Do not describe it as recoverable cluster state.

## 4.5 Failure means job failure in Phase 3

Basic failure detection is required.

Recovery is not.

If a worker disappears, a task times out, a protocol is invalid, or an exchange stream fails:

```text
coordinator marks job failed
  -> cancel surviving worker work
  -> close network work
  -> clean execution-scoped temp state
  -> return structured failure
```

No retry is required.

---

# 5. Development-Efficiency Goal

Phase 3 adds networking and processes, so careless test strategy can become slow and flaky.

Use vertical slices and focused validation:

```text
ade-exec streaming sink + merge aggregate
  -> focused ade-exec tests

protocol / partitioning
  -> focused unit tests

exchange sender/receiver
  -> focused loopback tests

worker service
  -> one focused worker lifecycle test

coordinator + 2 workers
  -> one high-value end-to-end correctness test

failure detection
  -> one targeted worker-loss/unreachable-worker test

ade-bench adapter
  -> focused adapter/config validation

integrated implementation
  -> workspace gate once
  -> smoke once
  -> focused 2-worker and 4-worker acceptance evidence
```

Avoid:

- repeatedly running the full workspace after every edit;
- exhaustive worker-count × memory-budget × workload matrices;
- large network fuzz suites;
- Criterion runs when no microbenchmark mechanism changed;
- TPC-H generation/Q1;
- repeated release builds before correctness stabilizes;
- repeated identical multi-process tests at several layers;
- performance gating in CI;
- `cargo clean`.

The normal test suite should prove semantics and lifecycle. A small acceptance run should prove actual 2–4-process distribution and metrics.

---

# 6. Required Phase 3 Scope

## 6.1 One real distributed runtime ownership boundary

Add **one** new cohesive crate for Phase 3 distributed execution.

Recommended name:

```text
crates/ade-distributed/
```

Recommended initial module ownership:

```text
crates/ade-distributed/
  Cargo.toml
  src/
    lib.rs
    ids.rs
    protocol.rs
    partition.rs
    exchange.rs
    worker.rs
    coordinator.rs
    metrics.rs
    bin/
      ade-worker.rs
      ade-coordinator.rs      # optional but recommended if small
```

Do not create the full future crate map (`ade-common`, `ade-proto`, `ade-exchange`, `ade-scheduler`, etc.) merely because the architecture lists it.

Phase 4 is the natural point to extract a dedicated exchange crate if multiple mechanisms create a real ownership boundary.

Required dependency direction:

```text
ade-bench -> ade-distributed -> ade-exec
ade-distributed -X-> ade-bench
ade-exec        -X-> ade-distributed
ade-exec        -X-> DataFusion
```

The distributed runtime must use public `ade-exec` APIs.

## 6.2 Static logical and attempt identity

Introduce small explicit distributed IDs.

At minimum:

```text
JobId
WorkerId
TaskId
AttemptId
ExchangeId
PartitionId
```

Requirements:

- logical task identity must not be derived from a socket/OS PID;
- `AttemptId` is distinct even though Phase 3 normally uses attempt 0 only;
- source assignment is deterministic for the same dataset + worker topology;
- partition function is deterministic across processes;
- protocol messages carry enough identity to reject stale/mismatched data;
- no retry semantics need to be implemented.

Avoid prematurely freezing full Phase 6 lineage/segment identity.

A small Phase 3 transfer identity such as:

```text
(job, exchange, partition, producer_task, attempt)
```

is enough to detect duplicates and missing producers.

## 6.3 Static job shape

Phase 3 may use a purpose-built job description rather than inventing the final physical IR.

A contained structure is preferred, conceptually:

```rust
StaticAggregateJob {
    job_id,
    input_files,
    workers,
    local_execution_config,
    hash_partition_count,
}
```

The initial distributed engine only needs to support the existing synthetic `GroupBySum` semantics:

```text
GROUP BY key
SUM(value_i64)
COUNT(*)
```

Do not add joins, generic expressions, SQL, or arbitrary DAG submission merely to make the shape appear general.

The job must preserve the graph:

```text
producer stage:
  assigned Parquet files
    -> partial hash aggregate
    -> hash partition partial rows
    -> exchange

consumer stage:
  received partial rows
    -> final aggregate
    -> gather
```

## 6.4 Deterministic source assignment

Assign manifest-listed input files exactly once.

Recommended initial rule:

```text
sort by manifest order
file index i -> producer worker (i % worker_count)
```

Requirements:

- no file is processed twice;
- no file is silently omitted;
- extra files in the dataset directory remain ignored, preserving Phase 0 semantics;
- worker count may be 2–4 for Phase 3 acceptance;
- if some worker receives no source file, either:
  - support an idle producer cleanly, or
  - skip creating that producer task while keeping final partition ownership explicit.

The benchmark fixture should contain at least as many files as workers so normal acceptance demonstrates work across workers.

## 6.5 Partial aggregate and final aggregate

The first distributed graph depends on two aggregation modes.

### Producer partial aggregation

Existing Phase 2 behavior:

```text
input:
  key: UInt64
  value_i64: Int64

output:
  key: UInt64
  sum: Int64
  count: UInt64
```

### Consumer final aggregation

Add a narrow merge-aggregate path that consumes:

```text
key: UInt64
sum: Int64
count: UInt64
```

and produces the same schema while combining repeated partials.

Required semantics:

```text
final_sum   = checked sum(partial_sum)
final_count = checked sum(partial_count)
```

Requirements:

- reuse the Phase 2 spillable aggregate implementation rather than writing an unrelated unbounded distributed hash map;
- preserve checked arithmetic;
- preserve managed-memory/spill behavior;
- preserve bounded output;
- handle the same key appearing from multiple producer workers;
- preserve cleanup/cancellation.

A good implementation is to refactor aggregate input contribution logic so the same bounded state machine supports:

```text
raw contribution:     (value, 1)
partial contribution: (partial_sum, partial_count)
```

Do not duplicate the entire aggregate algorithm.

## 6.6 Streaming/non-collecting `ade-exec` output path

Phase 3 should not require every producer/final worker to collect all operator output before sending it.

Add one reusable sink-style execution API to `ade-exec`.

Conceptually:

```rust
execute_to_sink(
    plan,
    config,
    cancellation,
    |batch| -> Result<()> { ... }
) -> Result<ExecutionMetrics>
```

Exact signature is implementation-owned.

Requirements:

- the internal operator loop remains one code path;
- existing `execute` / `execute_with_config` collection behavior remains supported;
- sink execution does not reserve/retain the whole result;
- one batch is handed to the sink at a time;
- sink failure aborts execution and triggers normal subtree/spill cleanup;
- cancellation still returns failure, never partial success;
- final metrics are captured only after operator shutdown;
- no hidden unbounded queue is inserted between engine and sink.

The distributed producer should use this API to partition/send partial aggregate batches.

The distributed final stage should use it to send final batches to gather.

## 6.7 Exchange input for final aggregation

The destination worker needs a bounded way to feed received exchange data back into `ade-exec`.

Recommended Phase 3 mechanism:

1. receive network batches;
2. write them into execution-scoped Arrow IPC stream files, one logical transfer/producer per destination partition;
3. after producer exchange completion, scan those files locally;
4. merge aggregate them.

Add a narrow local IPC scan path to `ade-exec`, for example:

```rust
LocalPlan::IpcScan {
    files: Vec<PathBuf>,
}
```

or an equivalent internal source boundary.

Requirements:

- use Arrow IPC, already present in the runtime, instead of inventing a new public format;
- preserve input file order for deterministic behavior where practical;
- validate schemas;
- stream one batch at a time;
- do not read all exchange files into memory;
- treat exchange files as external to local spill semantics but execution-scoped and lossy;
- cleanup is owned by the distributed job lifecycle.

This is intentionally a Phase 3 exchange staging mechanism, not a claim that future exchange must always materialize locally.

## 6.8 Hash partitioning

Use a deterministic, versioned partition function for the Phase 3 key type.

For `UInt64 key`:

```text
stable_hash_v1(key) % partition_count
```

Do not use a per-process randomized `HashMap`/`RandomState` seed.

Requirements:

- same key always routes to the same destination for a fixed version/count;
- partition count is positive;
- Phase 3 normally uses one destination partition per final worker;
- routing is testable independently;
- rows are neither duplicated nor dropped;
- empty partitions are represented explicitly enough for completeness checks.

The hash version should be protocol/ADR-visible so later changes cannot silently mix incompatible workers.

## 6.9 Initial control plane

Phase 3 needs a small versioned control protocol.

Required operations are roughly:

```text
Ping / Health
RunProducerTask
RunFinalTask
CancelJob
Shutdown          # only for locally managed test/benchmark workers if useful
```

The implementation may use a minimal blocking TCP RPC-style protocol for Phase 3 if it remains:

- versioned;
- bounded;
- validated;
- private/internal;
- isolated from physical execution semantics;
- documented in an ADR.

Recommended contained framing:

```text
magic
protocol version
bounded message length
serialized request
bounded response length
serialized response
```

`serde` + a strict internal encoding is acceptable.

If implementation evidence strongly favors an established RPC framework without materially expanding scope/build complexity, that is also acceptable, but record the decision. Do not introduce a large framework merely for aesthetics.

Control protocol requirements:

- maximum frame size;
- read/write/connect timeouts;
- unknown version/request rejection;
- no silent partial decode;
- structured worker-side error responses;
- no user/cloud credentials in messages;
- job/task identity on task requests;
- concurrency safe enough that `CancelJob` can be processed while a task is active.

This protocol is not a stable public API.

## 6.10 Initial exchange data plane

Use a separate explicit exchange transport boundary.

A contained Phase 3 design:

```text
producer worker
  -> bounded partition batch
  -> Arrow IPC encode
  -> bounded network frame
  -> destination worker
  -> validate identity/schema/sequence
  -> append to execution-scoped Arrow IPC file
  -> ACK completion
```

A blocking TCP implementation is acceptable for Phase 3 and has the advantage that socket backpressure naturally blocks the producer instead of requiring an asynchronous unbounded queue.

Requirements:

- each producer/destination transfer has explicit identity;
- bounded frame length with hard rejection of oversized frames;
- bounded sender-side buffering;
- no `Vec` containing the whole exchange;
- explicit stream completion;
- destination ACK only after its transfer is safely finalized locally;
- duplicate transfer identity rejected;
- truncated/disconnected transfers do not become valid completed exchange inputs;
- temporary partial files are removed/ignored;
- receiver validates schema kind/version;
- expected producers are known before final aggregation;
- final worker verifies all expected producer transfers for its partition are complete;
- TCP read/write timeouts prevent infinite blocking;
- cancellation/failure closes sockets within bounded time;
- metrics record logical rows plus wire/payload bytes.

Do not add compression, multiplexing, connection pooling, TLS, or a remote shuffle service in Phase 3 unless needed to make the required path correct.

## 6.11 Exchange ownership and memory accounting

The Phase 2 managed-memory budget remains scoped to `ade-exec`-owned retained state. Phase 3 must not falsely claim that network/protocol buffers are charged to that manager.

Instead:

- keep exchange frame/batch sizes explicitly bounded;
- record exchange peak frame bytes / in-flight bytes where practical;
- avoid retaining multiple unsent frames;
- document which allocations are:
  - local execution managed memory;
  - exchange bounded-but-separate memory;
  - protocol/library transient memory.

If the implementation shares the Phase 2 `MemoryManager` safely without creating backwards dependencies or false accounting, that is allowed, but it is not required for Phase 3 acceptance.

## 6.12 Worker service

Each static worker must own:

- a stable `WorkerId`;
- control listener;
- exchange listener;
- execution-scoped job state;
- active cancellation token;
- exchange temp root;
- completed exchange registry;
- worker metrics.

Phase 3 may allow only one active job per worker.

That is preferable to implementing multi-job arbitration.

Required lifecycle:

```text
start
 -> announce/listen
 -> Ping succeeds
 -> producer task
 -> receive exchange transfers
 -> final task
 -> job cleanup
 -> next job or shutdown
```

Worker task execution should call public `ade-exec` APIs.

Do not duplicate the local operator runtime inside the worker crate.

## 6.13 Coordinator

The Phase 3 coordinator is deliberately static.

Responsibilities:

1. validate worker topology;
2. ping workers before execution;
3. create deterministic task/partition assignments;
4. start producer tasks;
5. wait for all producer task completions;
6. fail immediately on worker/task/control failure;
7. once all producer exchanges are acknowledged, start final tasks;
8. gather one completed final transfer per final worker/partition;
9. verify completeness;
10. return batches + distributed metrics;
11. on failure, issue best-effort cancellation to surviving workers and clean coordinator-local gather state.

No retry, work stealing, placement scoring, or dynamic worker count.

A simple state machine is recommended:

```text
Prepared
  -> ProducersRunning
  -> ExchangeComplete
  -> FinalsRunning
  -> GatherComplete
  -> Succeeded

any state
  -> Failed
```

## 6.14 Gather

Gather is a real final distribution edge, but Phase 3 can keep it simple.

Recommended:

- coordinator hosts a bounded gather receiver using the same exchange framing concepts;
- each final worker sends one completed final stream;
- coordinator verifies one stream from every expected final task;
- benchmark adapter collects the gathered final result because its existing interface is `Vec<RecordBatch>`.

The benchmark harness collection is not part of a worker managed-memory guarantee.

Document this distinction.

## 6.15 Basic failure detection

Required failure cases:

- worker endpoint unavailable before job;
- worker process/control connection disappears while a task is running;
- exchange connection fails;
- task returns structured execution error;
- task exceeds a configured timeout;
- destination rejects invalid/mismatched transfer.

Coordinator behavior:

```text
mark job failed
cancel surviving workers best-effort
close/stop gather
wait/clean locally owned worker processes if applicable
return structured error
```

No recovery is required.

A worker hang must not block forever.

Use generous deterministic test timeouts rather than very small timing-sensitive values.

## 6.16 Local process topology

Localhost multi-process execution is a first-class Phase 3 deployment mode.

For benchmark/test-managed workers:

- bind to `127.0.0.1`;
- support port `0` so the OS chooses free ports;
- worker reports actual bound control/exchange addresses through a small readiness handshake;
- logs belong on stderr;
- readiness/control stdout must not be polluted if stdout is used as a process handshake;
- parent owns child process handles;
- child guards kill/wait on error or drop;
- no orphan worker processes.

Avoid "pick a random port, close it, and hope the child binds before someone else" races.

## 6.17 Standalone worker/coordinator CLI (optional, deferred)

A manual CLI may be useful, but it is not required for Phase 3. The public Rust
coordinator API and benchmark adapter provide the Phase 3 execution entrypoint,
including the required 2–4-worker localhost process topology. A standalone
`ade-coordinator` CLI is deferred unless a later phase has an independent need
for it. Do not turn this into an operations platform.

## 6.18 Distributed metrics

Phase 3 must make distribution observable.

At minimum record:

### Coordinator/job

- worker count;
- producer task count;
- final task count;
- job wall time;
- failed worker/task if any (reported as structured errors; failed jobs do not
  return success metrics);
- gather rows/batches/bytes, with one batch counted per non-empty data frame.

### Per worker

- assigned source files;
- producer input rows;
- partial aggregate rows;
- producer task duration;
- final input partial rows;
- final output rows;
- final task duration.

### Exchange

- transfers/segments;
- rows sent/received;
- payload bytes sent/received;
- wire bytes if practical;
- per-destination bytes/rows;
- peak frame bytes;
- rejected/failed transfers are surfaced through structured worker/task errors;
  no success metrics object is returned for a failed job.

Producer/final task counts are the corresponding task-map sizes. `elapsed_ms`
measures the complete coordinator call, including best-effort worker cleanup.
Explicit empty transfers count as completed transfers and zero data batches.

Metrics can initially remain runtime/debug diagnostics rather than changing benchmark result schema v1.

`EngineInfo.configuration` must record the effective distributed configuration.

## 6.19 Benchmark integration

Add `ade-distributed` as a benchmark engine adapter.

Phase 3 adapter support should be deliberately narrow:

```text
supported:
  GroupBySum synthetic workload

unsupported:
  Scan
  ScanFilterProject
  SortKeyRowid
  TpchQ1
```

Do not implement distributed pass-through versions of unrelated workloads only to increase apparent coverage.

Recommended engine options:

```text
workers=<2..4>
batch_size=<positive usize>
memory_limit_bytes=<positive usize>
spill_enabled=<true|false>
prefetch_batches=<0..64>
```

Keep low-level network knobs internal unless needed for reproducibility/debugging.

Record effective values in `EngineInfo.configuration`.

The adapter should:

- preserve manifest-only input semantics;
- validate the synthetic schema;
- start/connect workers;
- run the static aggregate graph;
- return gathered `RecordBatch`es;
- let the existing independent oracle validate semantics.

Do not move correctness logic into the distributed runtime.

---

# 7. Explicitly Out of Scope

Do **not** implement in Phase 3:

- adaptive exchange selection;
- direct-stream vs spill policy switching;
- durable/object-store exchange;
- peer replication;
- remote shuffle service;
- lineage;
- minimal-lineage recovery;
- worker/task retry;
- coordinator HA/restart;
- dynamic scheduling;
- morsels/work stealing;
- elastic workers;
- worker decommission;
- skew split/coalesce;
- dynamic partition count;
- speculative execution;
- joins;
- broadcast/range exchange;
- general physical DAG submission;
- stable public physical IR;
- SQL/DataFrame frontend;
- TPC-H execution in ADE;
- streaming;
- checkpointing/state;
- exactly-once sinks;
- multi-tenancy/admission control;
- authentication/authorization;
- TLS/mTLS;
- Kubernetes;
- cloud deployment;
- cost model;
- adaptive policy;
- public RPC compatibility guarantees;
- performance tuning of Phase 2 sort;
- recursive aggregate spill repartitioning unless independently required to fix a Phase 3 correctness issue.

---

# 8. Proposed Code Changes

## 8.1 `ade-exec`

Expected changes:

```text
crates/ade-exec/src/lib.rs
crates/ade-exec/src/aggregate.rs
crates/ade-exec/src/ipc_scan.rs       # if useful
crates/ade-exec/src/tests.rs
```

Required capability changes:

- merge-aggregate input mode;
- IPC scan source or equivalent;
- streaming/non-collecting sink execution entrypoint;
- no regression to existing collected execution.

Do not make `ade-exec` depend on distributed types.

## 8.2 `ade-distributed`

Expected new crate:

```text
crates/ade-distributed/
```

Own:

- static distributed IDs;
- private Phase 3 protocol;
- hash partition function;
- exchange sender/receiver;
- job-local exchange store;
- worker server;
- coordinator;
- distributed metrics;
- worker binary;
- optional small coordinator binary.

## 8.3 `ade-bench`

Expected changes:

```text
crates/ade-bench/src/ade_distributed.rs
crates/ade-bench/src/engine.rs
crates/ade-bench/src/lib.rs
crates/ade-bench/tests/phase3.rs      # if process setup remains focused/reliable
```

Do not change canonical correctness ownership.

## 8.4 Documentation

Update:

- `AGENTS.md` — active Phase 3 boundary;
- `README.md` — Phase 3 status/manual run limitations;
- `benchmarks/README.md` if adapter options belong there;
- ADR(s) for control protocol/exchange transport;
- `docs/plans/phase-3-implementation-plan.md`;
- `docs/reports/phase-3-implementation-report.md`.

Only update architecture/PRD if a real contradiction is discovered.

---

# 9. Recommended Protocol Shape

This section is concrete enough to guide implementation but is not a public compatibility promise.

## 9.1 Control message envelope

Conceptually:

```text
ControlEnvelope {
  magic/version
  request_id
  request
}
```

Requests:

```text
Ping
RunProducerTask
RunFinalTask
CancelJob
Shutdown
```

Response:

```text
request_id
status
structured error or task metrics
```

Hard requirements:

- bounded encoded size;
- unknown fields/version behavior defined;
- no indefinite reads;
- request/response IDs checked;
- worker errors remain structured enough for coordinator diagnostics.

## 9.2 Exchange header

Conceptually:

```text
ExchangeHeader {
  protocol_version
  hash_version
  job_id
  exchange_id
  partition_id
  producer_task_id
  attempt_id
  schema_kind
}
```

Then:

```text
repeated:
  frame_len
  Arrow IPC payload for one bounded RecordBatch

frame_len = 0
ACK
```

The exact encoding may differ.

Required semantics matter more than exact bytes.

## 9.3 Transfer completeness

Destination records a transfer as complete only after:

1. header validates;
2. all frames decode and match schema;
3. stream termination is observed;
4. local IPC writer finishes successfully;
5. temp file is finalized atomically;
6. completed-transfer registry is updated.

Producer receives success ACK only after this point.

A truncated stream must never appear complete.

---

# 10. Exchange Store Lifecycle

Use a job-scoped temporary root.

Conceptually:

```text
job-temp/
  exchange-0/
    partition-0/
      producer-0-attempt-0.arrow
      producer-1-attempt-0.arrow
    partition-1/
      ...
  gather/
    final-worker-0.arrow
    ...
```

Requirements:

- paths generated only from validated numeric/internal IDs;
- use `create_new` or equivalent collision protection;
- receive into partial/temp name;
- rename/finalize only after complete decode/write;
- duplicate completed identity rejected;
- normal success cleans after gathered result is no longer needed;
- failure/cancellation cleans best-effort through RAII;
- worker process loss may leave OS temp files until process cleanup and is not a recovery guarantee.

---

# 11. Correctness Invariants for the Distributed Aggregate

The following are Phase 3 acceptance invariants.

## 11.1 Source coverage

For all manifest-listed files:

```text
processed_count(file) == 1
```

## 11.2 Partial aggregation

For each producer worker and key:

```text
partial_sum(key)   == sum(values from that worker's assigned files)
partial_count(key) == count(rows from those files)
```

## 11.3 Hash routing

For every partial row:

```text
destination == stable_hash_v1(key) % final_partition_count
```

Each partial row is emitted exactly once.

## 11.4 Final aggregation

For each key:

```text
final_sum   == sum(all producer partial_sum)
final_count == sum(all producer partial_count)
```

## 11.5 Gather

Each key appears in exactly one final partition because routing is key-based.

Gather concatenates final partitions; it does not re-aggregate.

## 11.6 Failure semantics

Any incomplete required producer transfer means the final partition is not runnable/successful.

The coordinator must not return a partial result.

---

# 12. Focused Test Plan

Testing must be sufficient, not exhaustive.

## 12.1 `ade-exec` focused tests

Add a small number of high-value tests:

1. **merge aggregate correctness**
   - repeated key appears in several partial batches/files;
   - sums/counts merge exactly;
   - checked overflow remains an error.

2. **IPC scan streaming**
   - two small IPC files;
   - schema/order/batch iteration correct;
   - corrupt/truncated input fails.

3. **streaming sink lifecycle**
   - sink receives bounded batches without collection;
   - sink failure triggers operator/spill teardown;
   - cancellation remains clean;
   - final metrics have released operator state.

Do not duplicate every existing local operator test.

## 12.2 Protocol/partition unit tests

High-value tests:

- deterministic hash version vectors;
- every input row routes once;
- wrong protocol version rejected;
- oversized/truncated control or exchange frame rejected;
- duplicate transfer identity rejected.

## 12.3 Exchange loopback test

One loopback exchange test should verify:

- multiple batches;
- one empty partition/transfer;
- receiver ACK after finalization;
- resulting IPC file reads correctly;
- rows/bytes metrics are nonzero and consistent;
- partial file cleanup on disconnect.

## 12.4 Multi-process correctness test

One high-value end-to-end test with **2 worker processes**:

- at least two input files;
- repeated keys across workers;
- coordinator runs full graph;
- exact expected aggregate rows;
- both workers do real producer work;
- both receive exchange data where fixture permits;
- network sent/received metrics are nonzero;
- worker processes terminate/clean up.

This can be the main expensive integration test.

## 12.5 Failure-detection test

One targeted test is enough.

Preferred case:

- start two workers;
- begin a job or verify readiness;
- terminate one worker / point one worker at an unavailable endpoint;
- coordinator returns a structured failure within a generous timeout;
- surviving worker is cancelled/cleaned;
- no hang/orphan process.

Do not build a full chaos suite in Phase 3.

## 12.6 Benchmark adapter test

Validate:

- options parse/reject correctly;
- only `GroupBySum` is accepted;
- distributed result fingerprint equals the independent baseline on a small fixture if doing so does not duplicate the expensive process test.

If full adapter execution is already covered by the multi-process/smoke path, avoid another identical long test.

---

# 13. Integrated Validation Strategy

During implementation, use focused commands.

Examples:

```bash
cargo test -p ade-exec <phase3-focused-tests>
cargo test -p ade-distributed <partition-or-protocol-tests>
cargo test -p ade-distributed <exchange-loopback-test>
cargo test -p ade-distributed <two-worker-e2e-test>
cargo test -p ade-distributed <failure-detection-test>
cargo test -p ade-bench --test phase3
```

Near completion, run the normal repository gate once:

```bash
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
git diff --check
```

Run `scripts/smoke.sh` once after it is updated.

If offline/cached dependency constraints require `--offline`, follow the environment and record it.

Do not repeatedly rebuild release mode.

---

# 14. Smoke / Acceptance Evidence

Phase 3 needs direct evidence for the PRD exit condition.

Use one deterministic synthetic aggregate dataset with:

- enough files for four producers;
- repeated keys spanning files/workers;
- nontrivial cardinality;
- modest size so the acceptance run remains quick.

Required acceptance cases:

## Case A — 2 workers

Run:

```text
DataFusion baseline
ADE distributed, 2 workers
```

Require:

- independent validation passes;
- fingerprints match;
- producer work occurs on >1 worker;
- exchange network bytes > 0;
- final work occurs on expected workers;
- no temp/process leaks.

## Case B — 4 workers

Run:

```text
ADE distributed, 4 workers
```

Require:

- independent validation passes;
- fingerprint matches the baseline / 2-worker result;
- work/network distribution is observable across the topology.

No performance claim is required.

If timings are recorded by the harness, classify them as development evidence only.

Do not add an absolute performance threshold.

A single final release build is enough once correctness is stable.

---

# 15. CI Guidance

CI remains correctness-oriented.

Reasonable CI additions:

- workspace compile/lint/test;
- focused distributed unit tests;
- one small 2-worker localhost process integration test if it is reliable and quick.

Avoid in normal CI:

- 4-worker benchmark matrices;
- timing gates;
- large datasets;
- repeated failure scenarios;
- network performance assertions.

If process tests are flaky due to CI sandbox constraints, keep one deterministic local-process smoke path outside strict timing assertions and document the limitation.

---

# 16. Documentation / ADR Expectations

At minimum, record durable Phase 3 decisions around:

1. **control-plane protocol/framework**;
2. **initial exchange transport and retention behavior**.

Architecture backlog labels ADR-0008 and ADR-0009 for these areas. Preserve numbering intent if practical; do not renumber existing accepted ADRs.

ADR content should explicitly state:

- why the choice is sufficient for Phase 3;
- what is intentionally provisional;
- how distribution semantics remain separate from transport/retention;
- how backpressure is bounded;
- what failure/durability guarantees do **not** exist;
- what Phase 4 is expected to replace/generalize.

Do not create an ADR for every small code organization decision.

---

# 17. Phase 3 Implementation Sequence

Recommended order:

## Step 1 — Re-verify baseline

Read:

```text
AGENTS.md
PRD
architecture
Phase 2 plan
Phase 2 report
ADR-0005
ADR-0006
relevant ade-exec / ade-bench code
```

Verify current `main` has not moved materially from the reviewed baseline.

If it has, adapt this plan to current reality before coding.

## Step 2 — Add `ade-exec` distributed prerequisites

Implement:

- merge aggregate mode;
- IPC scan;
- streaming sink execution.

Run only focused `ade-exec` tests.

## Step 3 — Add distributed IDs and partitioning

Create the new crate.

Implement:

- IDs;
- deterministic hash v1;
- static task definitions;
- metrics structs.

Run unit tests.

## Step 4 — Implement protocol and exchange loopback

Implement:

- bounded control framing;
- bounded exchange framing;
- exchange store lifecycle;
- completion ACK;
- duplicate/truncation checks.

Run focused loopback tests.

## Step 5 — Implement worker

Implement:

- listeners/readiness;
- Ping;
- producer task;
- final task;
- cancellation;
- cleanup.

Test one worker lifecycle.

## Step 6 — Implement coordinator

Implement:

- topology validation;
- deterministic source assignment;
- producer barrier;
- final barrier;
- gather;
- failure propagation/cancellation.

Run one 2-worker end-to-end test.

## Step 7 — Failure test

Add one worker-loss/unreachable-worker test.

Fix lifecycle issues before continuing.

## Step 8 — Benchmark adapter

Add `ade-distributed` support for `GroupBySum` only.

Preserve independent validation.

## Step 9 — Docs and report

Update active phase docs/ADRs/README/report.

## Step 10 — Final gates and acceptance

Run:

- one workspace correctness gate;
- one smoke;
- one final release build if acceptance uses release;
- focused 2-worker and 4-worker acceptance runs.

Do not expand into Phase 4 cleanup.

---

# 18. Phase 3 Completion Checklist

Phase 3 is complete only if all of the following are true.

## Distributed execution

- [x] one coordinator can drive N static workers;
- [x] 2-worker multi-process execution works;
- [x] 4-worker multi-process execution works;
- [x] source files are assigned exactly once;
- [x] producer partial aggregate uses `ade-exec`;
- [x] real network hash exchange occurs;
- [x] destination final aggregate merges partial sums/counts correctly;
- [x] gather returns the complete result.

## Correctness

- [x] result matches independent oracle/DataFusion baseline;
- [x] same job result is invariant across 2 and 4 workers;
- [x] stable partition function is deterministic;
- [x] duplicate/missing transfer cannot silently change results;
- [x] incomplete exchange prevents final success.

## Resources/lifecycle

- [x] no unbounded exchange queue;
- [x] network frame sizes are bounded;
- [x] received data is streamed to execution-scoped local files;
- [x] `ade-exec` producer/final output can stream without whole-result collection;
- [x] exchange temp state cleans on success;
- [x] exchange temp state cleans on ordinary error/cancellation;
- [x] locally spawned worker processes do not become orphans;
- [x] local exchange files are documented as lossy/non-durable.

## Failure behavior

- [x] unavailable/dead worker is detected;
- [x] task/network timeout is bounded;
- [x] coordinator fails the job rather than returning partial output;
- [x] surviving workers receive best-effort cancellation;
- [x] no retry/recovery is falsely claimed.

## Observability

- [x] per-worker work metrics exist;
- [x] exchange row/byte metrics exist;
- [x] total coordinator elapsed time and non-empty data-frame counts exist;
- [x] empty transfers count as completed transfers and zero data frames;
- [x] rejected/failed transfers surface through structured errors on failed jobs;
- [x] job topology/configuration is visible;
- [x] acceptance evidence demonstrates nonzero network/work distribution.

## Architecture

- [x] distributed runtime remains frontend-independent;
- [x] `ade-exec` has no distributed dependency;
- [x] hash distribution is not encoded as a permanent storage policy;
- [x] no Phase 4 adaptive policy is introduced;
- [x] no speculative future crate explosion.

## Validation

- [x] focused tests pass;
- [x] workspace fmt/check/clippy/test pass;
- [x] smoke passes;
- [x] 2-worker acceptance passes;
- [x] 4-worker acceptance passes;
- [x] implementation report records exact commands/results/limitations.

---

# 19. Required Phase 3 Implementation Report

Create:

```text
docs/reports/phase-3-implementation-report.md
```

Include:

1. baseline commit and final commit;
2. summary;
3. exact implemented scope;
4. distributed topology;
5. source/task/partition identity model;
6. control protocol;
7. exchange transport and retention behavior;
8. backpressure/boundedness;
9. `ade-exec` changes;
10. worker/coordinator lifecycle;
11. failure semantics;
12. metrics;
13. benchmark adapter changes;
14. ADRs;
15. deviations from this plan;
16. commands actually run;
17. focused test results;
18. workspace gate;
19. 2-worker acceptance evidence;
20. 4-worker acceptance evidence;
21. known limitations;
22. deferred Phase 4+ items;
23. recommended next-phase considerations;
24. acceptance audit.

The report must clearly say that:

- Phase 3 exchange is static;
- worker-local received exchange state is non-durable;
- worker failure causes job failure;
- no retry/recovery exists;
- network/protocol buffering is bounded separately from the Phase 2 local managed-memory ledger;
- benchmark output gathering may collect the final result because the harness interface still requires collected batches.

---

# 20. Stop Boundary

After Phase 3 acceptance:

**stop.**

Do not use "cleanup" to begin:

- unified exchange;
- direct-stream exchange;
- adaptive spill/persist decisions;
- lineage;
- retries;
- cost modeling;
- skew adaptation.

Phase 4 should begin with a fresh plan grounded in the actual Phase 3 implementation.

The purpose of Phase 3 is narrow and important:

> **Prove that ADE can distribute a real aggregation graph across static worker processes with correct hash routing, bounded exchange behavior, observable work/network distribution, and clean failure semantics.**
