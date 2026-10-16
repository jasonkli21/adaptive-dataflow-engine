# Adaptive Dataflow Engine (ADE) — Phase 4 Detailed Implementation Plan
## Unified Exchange Mechanism

**Status:** Implemented; see Phase 4 implementation report
**Date:** 2026-10-15
**Repository:** `jasonkli21/adaptive-dataflow-engine`
**Repository baseline reviewed:** `2fa71e372d64323e499ece3421c6cd2ef5c74bfa` (`Fix Phase 3 review findings`)
**Depends on:** `docs/product/prd.md`, `docs/architecture/architecture.md`, `docs/plans/phase-3-implementation-plan.md`, `docs/reports/phase-3-implementation-report.md`, `docs/adr/0008-static-control-protocol.md`, `docs/adr/0009-static-tcp-ipc-exchange.md`, `AGENTS.md`, and the current Phase 3 implementation
**Primary objective:** replace Phase 3's single fixed exchange retention path with one trustworthy exchange contract that can execute the same hash distribution using multiple statically selected transport/retention mechanisms without changing query semantics.

---

# 1. Phase 4 Objective

Phase 3 proved static distribution:

```text
scan
  -> partial aggregate
  -> hash exchange
  -> final aggregate
  -> gather
```

It intentionally hard-coded one exchange behavior: bounded TCP transfer followed by execution-local Arrow IPC materialization on the destination worker. That was the right Phase 3 boundary, but it is not yet the architecture ADE needs for its research thesis.

The PRD defines Phase 4 as:

> One exchange API supports direct stream, bounded buffering, local spill, full local materialization, and durable object-store materialization.

The Phase 4 exit condition is:

> The same logical distribution can execute under each mechanism without changing correctness.

Phase 4 therefore proves **mechanism plurality without policy adaptation**.

At completion, the existing distributed GroupBySum graph must be able to run its hash-distribution edge under each of these statically selected mechanisms:

```text
direct stream
bounded in-memory buffer
bounded buffer + local spill
full local materialization
object-store materialization
```

The logical distribution remains:

```text
Hash(key)
```

The mechanism becomes an independent runtime choice.

No runtime heuristic should switch mechanisms in Phase 4. That is Phase 5.

---

# 2. Source-of-Truth Order

Implementation follows repository precedence:

1. `docs/product/prd.md` — Phase 4 outcome and long-term research scope;
2. `docs/architecture/architecture.md` — long-lived invariants and exchange/storage boundaries;
3. this Phase 4 plan — detailed Phase 4 implementation contract;
4. accepted ADRs — concrete decisions/refinements;
5. implementation reports and current code — observed implementation state.

The current code is not automatically authoritative when it conflicts with the PRD or architecture, but it must be understood before changing it.

The most relevant long-lived principles are:

- correctness before adaptation;
- mechanism and policy remain separate;
- distribution semantics are separate from retention/transport;
- bounded resources and backpressure are explicit;
- worker-local storage is lossy unless a stronger capability is explicitly provided;
- runtime behavior is observable;
- static baselines remain available;
- open formats/storage libraries should be reused where they are not ADE's differentiator.

The user's explicit Phase 4 instruction supersedes the Phase 3 stop boundary currently recorded in `AGENTS.md`.

---

# 3. Current Repository State

Phase 4 starts from a remediated Phase 3 implementation, not the original Phase 3 plan.

Important current facts:

- Workspace crates are:
  - `ade-bench`;
  - `ade-distributed`;
  - `ade-exec`.
- `ade-distributed` owns:
  - static worker/coordinator lifecycle;
  - control protocol;
  - distributed IDs;
  - deterministic hash partitioning;
  - TCP/Arrow IPC exchange;
  - distributed metrics.
- `ade-exec` owns:
  - pull-based local execution;
  - managed local memory;
  - spillable hash aggregation;
  - external sort;
  - bounded prefetch;
  - IPC scan;
  - merge aggregation;
  - non-collecting sink execution.
- Phase 3 exchange currently:
  - uses bounded frames;
  - uses blocking TCP as backpressure;
  - accepts only a prepared transfer identity set;
  - writes each completed transfer to an execution-scoped IPC file;
  - requires all producer transfers before final aggregation;
  - treats local materialization as non-durable;
  - fails the whole job on worker/control/data-plane failure.
- Each worker currently allows one active task at a time.
- The coordinator currently executes a producer barrier followed by a final-task barrier.
- The Phase 3 remediation added:
  - stage-wide deadlines that include connect;
  - monotonic worker `JobId` high-watermarks;
  - explicit non-empty network batch counts;
  - total distributed elapsed time.
- The benchmark adapter currently supports distributed synthetic GroupBySum only.
- Gather and oracle validation still collect the final result.
- The local managed-memory budget is per local task, not a total worker/process RSS cap.
- Exchange frame/batch memory is a separate bounded category.

These facts are Phase 4 constraints. Do not silently regress them.

---

# 4. Why Phase 4 Requires a Real Lifecycle Change

A true direct-stream mode cannot be implemented by simply adding an enum around Phase 3's current store.

Phase 3 has this lifecycle:

```text
run all producers
  -> fully receive/materialize exchange
  -> run all final consumers
```

If final consumers do not exist while producers are sending, then a "direct" mode must either:

- retain the whole exchange somewhere, or
- deadlock/block indefinitely.

Therefore Phase 4 must support producer/consumer overlap.

The preferred unified lifecycle is:

```text
PrepareJob
  -> initialize exchange endpoints/session
  -> start final consumers
  -> start producers
  -> producers finish their transfer streams
  -> exchange readers observe completion
  -> final consumers drain and finish
  -> gather completes
```

For full-materialization modes, the reader may internally wait until all required transfer materialization is complete before yielding data. The coordinator does not need a separate execution model for those modes.

This keeps one distributed graph lifecycle while letting the mechanism determine when data becomes consumable.

---

# 5. Phase 4 Design Principles

## 5.1 Static mechanism selection only

A job/exchange is configured with a mechanism before execution.

Allowed:

```text
exchange_mode = direct
exchange_mode = buffered
exchange_mode = spill
exchange_mode = local_materialized
exchange_mode = object_store
```

Not allowed:

```text
start direct
 -> observe lag
 -> switch to spill
```

That is Phase 5 policy.

A spill-mode implementation may use a fixed, deterministic memory threshold intrinsic to the selected mechanism. That is not policy adaptation as long as the threshold and behavior are static and observable.

## 5.2 Preserve logical distribution

Hash routing must remain independent from mechanism selection.

The same key, topology, hash version, and partition count must route identically in every mode.

Do not create mode-specific partitioning semantics.

## 5.3 One consumer contract

The final aggregate should consume a common batch-source abstraction rather than know whether data came from:

- a live socket;
- an in-memory queue;
- a local spill file;
- a finalized local IPC file;
- an object store.

Mechanism-specific code belongs behind the exchange boundary.

## 5.4 One producer contract

The producer should push partitioned batches through a common writer/session abstraction rather than call one class per mechanism from `worker.rs`.

A conceptual shape is:

```rust
trait ExchangeWriter {
    fn push(&mut self, batch: RecordBatch) -> Result<()>;
    fn finish(self) -> Result<ExchangeWriteSummary>;
}

trait ExchangeReader {
    fn next(&mut self) -> Result<Option<RecordBatch>>;
}
```

The exact Rust signatures are implementation-owned. The important invariant is ownership separation, not freezing a public trait prematurely.

## 5.5 Explicit boundedness

Every mode must state what is bounded and where pressure propagates.

No implementation may introduce:

- an unbounded `Vec<RecordBatch>`;
- an unbounded channel;
- unbounded per-transfer thread creation;
- a whole-exchange in-memory collection;
- object-store upload buffering proportional to the exchange.

## 5.6 Truthful durability

Capabilities must be described precisely.

- direct/buffered: volatile;
- local spill/materialization: worker/host local and lossy;
- object-store mode: durable only to the capability of the configured backend.

A local filesystem backend used for deterministic tests must not be described as multi-host/cloud durable merely because it implements an object-store API.

## 5.7 No recovery semantics yet

Object-store materialization in Phase 4 is a retention mechanism, not a recovery plan.

A worker failure still fails the job.

Do not add:

- retries;
- replay;
- lineage;
- lost-segment recovery;
- coordinator failover.

Those remain Phase 6+ work.

---

# 6. Required Phase 4 Scope

## 6.1 Introduce a unified exchange configuration

Add an explicit static exchange mechanism to the distributed job/task configuration.

Conceptually:

```rust
enum ExchangeMode {
    Direct,
    Buffered,
    Spill,
    LocalMaterialized,
    ObjectStoreMaterialized,
}

struct ExchangeConfig {
    mode: ExchangeMode,
    buffer_bytes: usize,
    // additional narrowly required settings
}
```

Requirements:

- strict validation;
- deterministic serialization through the existing bounded private control protocol;
- default should preserve the existing Phase 3 behavior unless there is a compelling compatibility reason otherwise;
- effective configuration must be reported by the benchmark adapter;
- unsupported/irrelevant options must fail or be clearly ignored by a documented rule, not silently change semantics.

Recommended default:

```text
local_materialized
```

This preserves the Phase 3 static baseline.

## 6.2 Refactor the current exchange module into a mechanism boundary

Phase 3 intentionally kept `exchange.rs` monolithic because only one mechanism existed.

Phase 4 now has a real mechanism boundary.

Refactor toward a structure such as:

```text
crates/ade-distributed/src/exchange/
  mod.rs
  codec.rs
  session.rs
  direct.rs
  buffered.rs
  spill.rs
  local_materialized.rs
  object_store.rs
```

Exact filenames are not normative.

Do **not** create a speculative multi-crate runtime merely for aesthetics.

A new `ade-exchange` crate is acceptable only if it produces a clean one-way dependency without requiring a speculative `ade-common`/`ade-proto` crate or duplicating distributed identity types. Otherwise keep the boundary inside `ade-distributed` for Phase 4.

The architectural requirement is a replaceable mechanism boundary, not a particular crate count.

## 6.3 Preserve and reuse the bounded wire codec

Do not rewrite the proven Phase 3 framing without reason.

Preserve or strengthen:

- transfer identity validation;
- hash/schema version validation;
- frame-size bound;
- row bound;
- prevalidation before Arrow allocation;
- explicit transfer completion;
- bounded connection handlers;
- total frame/header/ACK deadlines;
- cancellation checks.

If the wire protocol changes to add segment sequence/completion metadata, version it explicitly and retain strict decoding.

## 6.4 Add minimal segment identity/sequence

Phase 5 needs a smaller unit than a whole transfer if ADE will eventually choose different mechanisms for different segments.

Phase 4 should introduce the smallest useful bridge:

```text
TransferId
  + monotonically increasing segment sequence
```

A non-empty exchange batch/frame can be treated as one Phase 4 segment.

Requirements:

- sequence starts deterministically;
- no duplicate sequence accepted;
- no gaps may silently produce success;
- completion records the final segment count;
- empty transfers remain representable;
- per-producer segment order is preserved;
- no global cross-producer order is promised;
- segment identity is not yet lineage identity.

Do not implement Phase 6 lineage descriptors.

## 6.5 Add a bounded external batch source boundary to `ade-exec`

Final merge aggregation must be able to consume a live exchange without first receiving a completed file list.

Add the narrowest reusable batch-source boundary consistent with the current pull runtime.

Conceptually:

```rust
trait BatchSource: Send {
    fn schema(&self) -> SchemaRef;
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}
```

Then allow the existing merge aggregate to consume that source through the same operator drive loop.

Requirements:

- no second aggregation implementation;
- no unbounded queue hidden in `ade-exec`;
- cancellation propagates into a blocked source;
- source errors fail execution;
- subtree shutdown/spill cleanup remains correct;
- existing `LocalPlan::IpcScan` remains supported;
- collected and sink execution behavior remains unchanged.

The exact public API can differ if a smaller shape fits the code better.

## 6.6 Allow one producer and one final consumer task concurrently per worker

The current worker-wide `busy` flag is incompatible with streaming exchange.

Replace it with bounded, role-aware execution state.

A worker may have at most:

```text
one prepared job
one producer task for that job
one final task for that job
bounded control handlers
bounded exchange handlers
```

Requirements:

- no arbitrary task concurrency;
- duplicate producer/final starts fail;
- attempt identity remains explicit;
- cancellation can interrupt/unblock both roles;
- shutdown waits/joins bounded task lifecycles;
- job cleanup cannot remove exchange state while a producer/final task still owns it;
- the monotonic `JobId` high-watermark remains enforced;
- stale traffic from prior jobs is rejected.

Memory truthfulness:

- `memory_limit_bytes` remains a local-task budget unless intentionally renamed;
- producer and final task may overlap, so a worker can have two local-task budgets active at once;
- exchange buffer memory is a separate explicit budget/category;
- docs/metrics must not imply the existing local memory limit is a total worker/process cap.

## 6.7 Change coordinator execution to consumer-first overlap

The coordinator should stop assuming that all producers must finish before any final task starts.

Recommended order:

```text
prepare all workers
  -> launch final-task RPCs
  -> launch producer-task RPCs
  -> wait for producer and final completion under one bounded job/stage deadline model
  -> cancel all surviving work on first required failure
  -> gather result
```

Requirements:

- no deadlock if a producer or final task fails early;
- first failure triggers shared cancellation;
- final readers unblock on cancellation;
- deadline ownership is explicit;
- connect time remains inside the relevant deadline;
- cleanup waves remain bounded;
- successful result is returned only after all required producer/final completions.

For materialized modes, the final task may start early but block inside the exchange reader until materialization completeness is reached.

## 6.8 Direct-stream mechanism

Semantics:

```text
producer network frame
  -> destination exchange reader
  -> final aggregate
```

No ADE-owned retained exchange beyond the minimum bounded in-flight handoff.

Requirements:

- explicit bounded/rendezvous handoff;
- producer backpressure when consumer is not ready;
- no disk write on the normal data path;
- per-transfer completion and segment sequence verified;
- cancellation unblocks sender and receiver;
- metrics prove that local/object spill bytes are zero.

TCP/kernel buffering exists and should be described as external transport buffering, not ADE-managed exchange retention.

## 6.9 Bounded-buffer mechanism

Semantics:

```text
producer
  -> bounded ADE-owned memory queue
  -> consumer
```

Requirements:

- enforce an explicit byte/batch bound;
- block producer when the bound is full;
- release accounting when the consumer takes a segment;
- track current/peak retained bytes;
- queue capacity is finite and validated;
- one segment cannot cause silent unbounded growth;
- cancellation wakes blocked producers/consumers.

Prefer byte-oriented accounting. Batch-count limits may also exist as a secondary guard.

## 6.10 Local-spill mechanism

Semantics:

```text
producer
  -> bounded in-memory exchange buffer
  -> deterministic spill after fixed threshold
  -> consumer drains in-memory/spilled segments
```

This is a static mechanism, not adaptive mode switching.

Requirements:

- a fixed configured memory threshold;
- spill is execution-scoped and worker-local;
- spill uses bounded Arrow IPC or another already justified bounded format;
- no whole-exchange memory retention;
- per-transfer sequence/completeness is preserved across memory and disk;
- consumer sees each segment exactly once;
- partial spill files are not visible as completed data;
- spill cleanup occurs on success/error/cancellation;
- metrics distinguish:
  - in-memory bytes;
  - spill write bytes;
  - spill read bytes;
  - peak resident exchange bytes.

The implementation may choose to spill the oldest queued segments or new arrivals, but the rule must be deterministic and documented.

## 6.11 Full local-materialization mechanism

This should preserve the Phase 3 baseline behavior behind the new exchange interface.

Requirements:

- one completed materialization per required transfer;
- final consumer does not observe partial transfer files;
- all expected transfers must complete;
- worker-local and non-durable semantics remain explicit;
- current duplicate/missing/stale transfer protections remain;
- current cleanup behavior is preserved;
- Phase 3 mode remains usable as a static benchmark baseline.

Avoid a rewrite that loses already-tested Phase 3 invariants.

## 6.12 Object-store materialization mechanism

Add a provider-neutral object-store retention path.

Preferred approach:

- use the Rust `object_store` ecosystem already transitively present through the existing data stack if compatible with the locked dependency set;
- depend on public APIs only;
- use a deterministic local filesystem-backed object-store configuration for tests/acceptance;
- do not require AWS/GCP credentials or external services in normal tests.

Semantics:

```text
producer transfer
  -> bounded serialization/upload
  -> final object becomes visible as completed
  -> consumer reads object through common exchange reader
```

Requirements:

- object keys include sufficient job/exchange/partition/producer/attempt identity;
- stale job data cannot be mistaken for current data;
- partial/in-progress upload is not treated as completed;
- completion metadata is explicit;
- upload/download is bounded and does not read the whole object into memory;
- backend errors fail the job;
- object-store read/write/object counts are observable;
- cleanup/retention behavior is explicit.

Phase 4 durability statement:

> The mechanism can persist exchange output through an object-store interface and consume it independently of worker-local exchange temp state. Durability is exactly the guarantee of the configured backend. The local filesystem backend used for tests is not evidence of cloud or multi-host durability.

Do not implement recovery from those objects yet.

## 6.13 Gather scope

The Phase 4 acceptance target is the **hash exchange edge** between partial and final aggregation.

Do not expand scope merely to make every future exchange edge configurable.

The final gather path may remain a fixed bounded mechanism, while sharing codec/lifecycle utilities where convenient.

If making gather use the same abstraction is genuinely simpler after refactoring, it is allowed, but it is not required for Phase 4 exit.

## 6.14 Metrics and observability

Every successful distributed result should record the selected hash-exchange mode.

Add enough metrics to prove which mechanism actually ran.

Recommended exchange-level metrics:

```text
mode
transfers
segments/batches
rows
payload bytes
peak frame bytes

peak managed exchange-buffer bytes
producer blocked/wait time
consumer wait time

spill write bytes
spill read bytes

local materialized bytes/files

object-store write bytes
object-store read bytes
object count

first-data / completion timing where cheap to measure
```

Exact schema is implementation-owned.

Required conservation checks:

```text
producer sent rows == exchange accepted rows
exchange accepted rows == final input rows
required transfers completed exactly once
segment counts are consistent with completion metadata
```

Do not silently merge failed-transfer diagnostics into a successful metrics object.

## 6.15 Benchmark adapter

Extend `ade-distributed` engine options narrowly.

At minimum:

```text
exchange_mode=
  direct
  buffered
  spill
  local_materialized
  object_store
```

Add one explicit exchange buffer limit if required, for example:

```text
exchange_buffer_bytes=4194304
```

Requirements:

- strict parsing and validation;
- options appear in effective engine configuration;
- default preserves the Phase 3 local-materialized baseline;
- benchmark result schema should not be bumped merely for debug metrics unless a real artifact contract changes;
- unsupported query families remain rejected before spawning workers;
- independent oracle validation remains mandatory.

The local multi-process cluster may provision a coordinator-owned shared object-store root for `object_store` mode.

---

# 7. Explicit Non-Goals

Phase 4 must **not** implement:

- runtime mode switching;
- heuristic exchange policy;
- expected-cost policy;
- lineage;
- recomputation;
- retry/recovery;
- worker replacement;
- dynamic membership;
- work stealing;
- skew split/coalesce;
- dynamic partition count;
- joins;
- SQL/DataFrame frontend;
- streaming/event time/state;
- exactly-once;
- TLS/authentication;
- cloud-provider credential management;
- coordinator HA;
- cross-job materialization reuse.

Do not pull Phase 5–7 work forward as cleanup.

---

# 8. Suggested Implementation Sequence

The order below is designed to reduce debugging scope and avoid repeatedly running broad tests.

## Step 1 — Baseline and documentation reconciliation

Before code changes:

- read PRD Phase 4;
- read architecture exchange/storage/backpressure sections;
- read Phase 3 report including review remediation;
- inspect current `exchange.rs`, `worker.rs`, `coordinator.rs`, `protocol.rs`, metrics, adapter, and `ade-exec` source/sink code;
- record the actual baseline commit;
- confirm no newer commit changed the assumptions in this plan.

If the repository moved, adapt the plan rather than forcing stale file shapes.

## Step 2 — Behavior-preserving exchange refactor

Refactor the Phase 3 materialized path behind the new internal producer/reader/session boundary.

At the end of this step:

- existing local-materialized mode should behave exactly as Phase 3;
- existing focused exchange/process tests should still pass;
- no new mechanism is required yet.

This is the highest-value regression checkpoint.

## Step 3 — Segment sequence and unified completion

Add minimal segment sequence/completion semantics and conservation checks.

Focus tests on:

- duplicate segment;
- gap/missing completion;
- empty transfer;
- stale transfer;
- multi-batch transfer.

## Step 4 — `ade-exec` external source + worker dual-role lifecycle

Add the bounded external batch source and allow one producer plus one final task to overlap.

Validate:

- consumer can block before producer begins;
- producer can feed final merge aggregation;
- cancellation unblocks both;
- shutdown/cleanup joins both roles;
- Phase 3 local-materialized behavior still works through the new lifecycle.

## Step 5 — Direct and bounded-buffer modes

Implement direct first, then buffered.

Focused tests:

- handoff/backpressure;
- exact row/segment conservation;
- bounded peak queue bytes;
- cancellation while producer is blocked;
- cancellation while consumer is blocked.

## Step 6 — Spill mode

Build spill on the same reader/writer contract.

Focused tests:

- force spill with a tiny but valid exchange buffer;
- prove nonzero spill read/write;
- verify exact output and segment sequence;
- verify temp cleanup;
- verify no deadlock with a deliberately slow consumer.

## Step 7 — Object-store mode

Add provider-neutral object-store materialization with deterministic local backend tests.

Focused tests:

- multi-segment round-trip;
- no partial visibility as completed transfer;
- stale job/key isolation;
- backend error propagation;
- bounded upload/read path;
- cleanup/retention semantics.

## Step 8 — Adapter, metrics, end-to-end matrix

Add strict engine option parsing and a compact process-level matrix.

Recommended end-to-end correctness coverage:

- one 2-worker synthetic distributed GroupBySum run for **each of the five modes**;
- one 4-worker release acceptance for a streaming mode and the object-store mode;
- reuse the same fixture and oracle;
- assert mechanism-specific metrics so a mode cannot accidentally fall back to local materialization.

Do not multiply this by many memory budgets, datasets, and worker counts.

## Step 9 — Documentation and final gate

Update:

- `AGENTS.md` active boundary to Phase 4;
- `README.md` current status/options;
- one or more ADRs for the unified exchange contract and object-store capability boundary;
- `docs/reports/phase-4-implementation-report.md`.

Run final workspace checks once after focused tests are green.

---

# 9. Focused Test Strategy

Testing should be sufficient to falsify the design without becoming a matrix explosion.

## 9.1 Unit/mechanism tests

Required high-value cases:

- stable hash routing unchanged across modes;
- transfer/segment identity and sequence validation;
- empty transfer;
- duplicate/stale transfer rejection;
- direct backpressure;
- bounded-buffer capacity enforcement;
- spill threshold and exact-once readback;
- local materialization completeness;
- object-store complete-object visibility;
- cancellation unblocks producer/consumer;
- cleanup after error.

Prefer table-driven tests where the same correctness contract applies to all mechanisms.

## 9.2 Local-runtime tests

Only add tests needed for the new external source boundary:

- normal batch stream;
- source error;
- cancellation while blocked;
- merge aggregate correctness;
- shutdown/spill cleanup.

Do not duplicate the existing aggregate/spill suite.

## 9.3 Distributed process test

Use one high-value process integration test that can iterate exchange modes.

It should verify:

- real worker processes;
- exact query result;
- producer/final overlap where applicable;
- all required transfers complete;
- no stale job reuse;
- cancellation/cleanup still works.

Avoid one large bespoke test per mode if a parameterized path can prove the same lifecycle.

## 9.4 Acceptance evidence

Use release binaries only after correctness stabilizes.

Recommended acceptance fixture:

- 4 Parquet files;
- enough cardinality for keys to cross every producer;
- large enough to create multiple exchange segments;
- small enough to run quickly.

For each mode:

- independent oracle passes;
- result fingerprint is identical;
- row conservation passes;
- mode-specific metrics prove the intended path.

For spill mode, use a buffer limit guaranteed to spill.

No absolute performance threshold is required in Phase 4.

Timing may be recorded as exploratory evidence only.

---

# 10. Validation Commands

Use focused commands during implementation.

Examples:

```bash
cargo check -p ade-exec -p ade-distributed
cargo test -p ade-exec <new_source_tests>
cargo test -p ade-distributed <exchange_tests>
cargo test -p ade-distributed --test static_job
cargo clippy -p ade-exec -p ade-distributed --all-targets -- -D warnings
```

After implementation stabilizes, run the full gate once:

```bash
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo build --workspace --locked
bash scripts/smoke.sh
git diff --check
```

Use `--offline` when the required dependency set is already available.

If adding a direct `object_store` dependency requires a lockfile adjustment, keep it minimal and explain it in the implementation report.

Do not run:

- `cargo clean`;
- exhaustive mode × worker × memory matrices;
- Criterion unless a microbenchmark is directly needed;
- TPC-H regeneration/Q1;
- cloud integration tests requiring credentials;
- noisy CI performance gates.

---

# 11. Documentation / ADR Requirements

At least one Phase 4 ADR should record:

1. static `ExchangeMode` semantics;
2. the common writer/reader/session boundary;
3. producer/final overlap;
4. segment sequence/completion semantics;
5. bounded-buffer accounting;
6. local-spill guarantees;
7. local-materialization guarantees;
8. object-store capability/durability boundary;
9. why this is mechanism work, not Phase 5 policy adaptation.

A second ADR is warranted only if object-store provider/visibility semantics become a separately durable decision.

Update the Phase 4 implementation report with:

- exact baseline/final commit;
- implemented mode semantics;
- deviations from this plan;
- lifecycle changes;
- memory/boundedness statement;
- durability statement;
- commands/tests;
- acceptance evidence;
- known limitations;
- Phase 5 considerations.

---

# 12. Phase 4 Acceptance Criteria

Phase 4 is complete only if all of the following hold.

## Correctness

- The same synthetic distributed GroupBySum workload produces the same independently validated result in all five modes.
- Hash routing/version is unchanged by mode.
- Missing, duplicate, stale, truncated, or invalid transfer/segment data cannot silently produce success.
- Empty partitions/transfers remain correct.

## Unified mechanism boundary

- Producer code does not hard-code local IPC materialization.
- Final merge aggregation consumes a common exchange-reader/source boundary.
- Mode selection is static job/exchange configuration.
- No Phase 5 heuristic selects or switches modes.

## Concurrency/lifecycle

- Final consumers can start before producers.
- Each worker has bounded producer/final concurrency.
- Cancellation/error unblocks all relevant roles.
- First required failure prevents partial success.
- Cleanup is bounded and does not reuse stale job state.

## Resource discipline

- Direct mode has no ADE-owned whole-exchange retention.
- Buffered mode enforces a finite configured buffer.
- Spill mode enforces finite memory and uses bounded disk spill.
- Local materialization is streaming-to-disk, not whole-exchange memory collection.
- Object-store upload/read is bounded.
- The implementation does not misrepresent task memory limits as process RSS limits.

## Durability truthfulness

- Direct/buffer/spill/local materialization are not called durable.
- Object-store durability is scoped to the configured backend.
- No recovery guarantee is claimed.

## Observability

- Selected mode is recorded.
- Mechanism-specific bytes/counts/peaks are visible.
- Row and transfer/segment conservation are testable.
- A test can distinguish direct, spill, local-materialized, and object-store paths by metrics.

## Regression safety

- Existing Phase 0–3 correctness contracts remain intact.
- The Phase 3 local-materialized baseline still works.
- Independent oracle validation remains mandatory.
- Worker `JobId` monotonicity and stale-traffic rejection remain.
- Existing total deadline protections remain.

## Scope discipline

No implementation of:

- adaptive exchange;
- lineage/recovery;
- cost model;
- skew adaptation;
- SQL/joins;
- streaming/state.

---

# 13. Phase 4 Exit Demonstration

The minimum convincing Phase 4 demonstration is one fixture executed through:

```text
Hash(key) + direct
Hash(key) + buffered
Hash(key) + spill
Hash(key) + local materialized
Hash(key) + object store
```

All five must have:

```text
same logical input
same hash version
same worker topology
same query semantics
same independently validated result
```

while their mechanism metrics visibly differ.

That is the Phase 4 research result:

> **Distribution semantics no longer imply one retention/transport mechanism.**

---

# 14. Phase 5 Handoff

Phase 4 should leave Phase 5 with mechanism APIs and observations, not policy.

Phase 5 should be able to consume:

- buffer occupancy/peak;
- producer blocked time;
- consumer lag/wait;
- produced bytes;
- spill bytes;
- materialization bytes;
- object-store write/read cost proxies;
- worker health;
- intermediate size;
- segment identity.

Then Phase 5 can add deterministic policy such as:

```text
if consumer keeps up -> direct
if short lag -> buffer
if buffer pressure -> spill
if decoupling/durability threshold -> materialize
```

Phase 4 must not implement that selector.

The cleaner Phase 4 keeps the mechanism/policy boundary, the more meaningful the Phase 5 adaptive-exchange experiment will be.
