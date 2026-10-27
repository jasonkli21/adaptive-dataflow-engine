# Adaptive Dataflow Engine (ADE) — Architecture

**Status:** Refined architecture baseline for repository scaffolding  
**Date:** 2026-10-15  
**Companion document:** `docs/product/prd.md`  
**Near-term companion:** `docs/plans/phase-0-implementation-plan.md`  
**Working name:** Adaptive Dataflow Engine (ADE)

---

## Document Role and Authority

This document defines ADE's long-lived **technical boundaries, invariants, and architectural direction**. It is subordinate to the product scope in `docs/product/prd.md` and more authoritative than phase plans for cross-phase architecture.

Document precedence for implementation work is:

1. product scope and phase outcomes in the PRD;
2. architecture invariants and system boundaries in this document;
3. the active phase implementation plan for detailed deliverables;
4. accepted ADRs for concrete decisions;
5. implementation reports for the observed state of the codebase.

An accepted ADR may intentionally refine or supersede a recommendation here, but it must identify the affected section/invariant and explain why. **Illustrative Rust APIs in this document are not frozen interfaces.** They show responsibility boundaries, not exact signatures.

An accepted ADR does not silently widen an active phase plan; changes to a phase deliverable or boundary must be reconciled in that plan while preserving PRD outcomes and architecture invariants.

## 1. Purpose

This document translates the ADE product requirements into a durable system architecture.

The architecture is designed around one research thesis:

> **Build a distributed execution engine where materialization and recomputation are adaptive decisions rather than fixed properties of the processing model.**

The immediate implementation scope is intentionally much smaller than the long-term system. Phase 0 establishes the benchmark and experimentation substrate. Phase 1 begins a minimal single-node physical engine. Later phases grow outward into distributed batch, adaptive exchange, lineage-aware recovery, cost-aware scheduling, streaming state, incremental computation, and eventually a Spark/Flink-class extensible data-processing platform.

This architecture therefore has two responsibilities:

1. define the long-lived boundaries and invariants that the early implementation must not accidentally violate; and
2. avoid prematurely implementing abstractions whose requirements have not yet been validated experimentally.

The architecture favors **stable concepts with replaceable mechanisms**.

---

## 2. Architectural Thesis

Traditional distributed engines typically make several decisions at coarse boundaries:

- pipeline or shuffle;
- persist or recompute;
- local or remote;
- batch or streaming;
- checkpoint every fixed interval;
- execute using a chosen stage/task topology.

ADE treats those choices as a related optimization problem.

For any recoverable unit of intermediate work, the runtime should eventually be able to choose among:

```text
do not retain / lineage only
        |
        v
in-flight pipeline
        |
        v
bounded memory buffer
        |
        v
local NVMe spill
        |
        v
peer replica / remote ephemeral shuffle
        |
        v
durable object storage
```

The decision is not necessarily permanent. The same segment can transition between mechanisms as runtime conditions change.

Examples:

- a partition starts as a direct stream, then spills because its consumer becomes slow;
- a local spill becomes remotely durable after a worker receives a decommission signal;
- an intermediate is deliberately discarded because its lineage is cheap to recompute;
- a skewed partition is split after production has begun;
- streaming state is checkpointed more frequently because replay cost rises;
- a reusable intermediate remains cached across jobs because expected reuse exceeds storage cost.

The architecture must therefore keep **execution, storage, scheduling, lineage, recovery, progress, and cost accounting** connected while preventing them from collapsing into one inseparable module.

---

## 3. Architectural Principles

### 3.1 Correctness before adaptation

Adaptive execution is permitted to alter:

- placement;
- task count;
- partition topology;
- execution order where semantics allow;
- join strategy;
- retention;
- checkpoint cadence;
- resource allocation.

It must not silently alter results or the declared delivery/recovery guarantees.

Correctness rules belong in the mechanism layer. Policy may choose among valid mechanisms.

---

### 3.2 Mechanism and policy are separate

The runtime provides mechanisms such as:

- stream;
- buffer;
- spill;
- persist;
- replicate;
- discard;
- recompute;
- split;
- coalesce;
- migrate;
- checkpoint;
- restore.

Policy answers:

- when;
- where;
- how much;
- how durable;
- at what expected cost.

A policy implementation must be replaceable without rewriting exchange or storage code.

---

### 3.3 ADE owns the physical runtime

ADE may reuse:

- Apache Arrow;
- Parquet;
- object-store clients;
- SQL parsers;
- logical planning;
- connector libraries;
- open protocols such as Arrow Flight/Substrait where useful.

ADE must own the parts that express the research thesis:

- physical execution IR;
- local operator runtime;
- memory accounting semantics;
- distributed scheduling;
- exchange;
- segment identity;
- lineage;
- recovery planning;
- runtime statistics;
- adaptive policy;
- cost model;
- state/checkpoint integration.

DataFusion can become an important frontend and library dependency without becoming the architecture of the distributed runtime.

---

### 3.4 Fine-grained data, bounded control overhead

ADE should eventually adapt below whole-stage granularity, but the control plane cannot manage one RPC or database row per batch.

Therefore:

- work is fine-grained in the data plane;
- metadata is aggregated where possible;
- workers may make safe local decisions;
- global decisions are batched;
- coordinator metadata uses ranges/extents rather than naive per-buffer records.

The architecture deliberately distinguishes:

- **morsel:** schedulable input work;
- **partition:** routing domain;
- **segment:** recoverable exchange/state unit;
- **extent:** metadata aggregation of adjacent compatible segments.

---

### 3.5 Boundedness is a property, not a second engine

Long-term physical operators and exchanges must not assume all input terminates.

A source can be:

```text
bounded
unbounded
bounded-at-snapshot
hybrid historical + live
```

Streaming support is deferred, but early interfaces should avoid making it impossible.

---

### 3.6 Cost is an explicit objective

Performance does not have one scalar objective.

Long-term job objectives include:

```text
minimize latency
minimize compute cost
minimize total dollar cost
minimize cost under deadline
minimize latency under budget
balance cost / latency / recovery risk
```

Policies must be able to explain decisions in terms of observed inputs and declared objective.

---

### 3.7 Observability is part of correctness

An adaptive engine that cannot explain its decisions is not operable and is difficult to research.

All adaptive actions must eventually produce decision records containing:

- relevant observations;
- estimated alternatives;
- selected action;
- confidence/reason;
- actual later outcome where measurable.

---

### 3.8 Benchmarkability is a first-class architectural requirement

The system must make it possible to disable each optimization independently.

Every adaptive mechanism needs:

- static baseline;
- controlled experiment;
- deterministic workload where practical;
- comparable result schema;
- regression history.

This is why Phase 0 precedes engine implementation.

---

## 4. System Context

Long-term system context:

```text
+-------------------------------------------------------------+
|                         Clients                             |
| SQL | Python | Rust | JVM | Flight SQL | Plan submission   |
+-------------------------------+-----------------------------+
                                |
                                v
+-------------------------------------------------------------+
|                       Frontend Layer                        |
| parser | catalog binding | logical plans | API translation |
+-------------------------------+-----------------------------+
                                |
                                v
+-------------------------------------------------------------+
|                    Planning / Optimization                  |
| logical optimizer | physical planner | physical IR         |
+-------------------------------+-----------------------------+
                                |
                                v
+-------------------------------------------------------------+
|                     Adaptive Control Plane                  |
| scheduler | cost model | runtime stats | lineage catalog   |
| materialization catalog | recovery planner | job state     |
+----------+----------------------+---------------------------+
           |                      |
     control plane          metadata / decisions
           |                      |
           v                      v
+-------------------------------------------------------------+
|                     Worker Runtime (N)                      |
| operator pipelines | memory | exchange | spill | state     |
| metrics | local policy | peer data service                 |
+------------+--------------------------+---------------------+
             |                          |
        peer network              storage hierarchy
             |                          |
             v                          v
          workers          RAM | NVMe | shuffle | object store
```

Phase 0 implements only the **benchmark/experiment boundary** around this future system.

---

## 5. Major Architectural Layers

### 5.1 API and frontend layer

Responsibilities:

- SQL parsing;
- DataFrame APIs;
- plan submission;
- catalog lookup;
- type/schema validation;
- logical expression construction.

Not responsible for:

- deciding segment placement;
- worker scheduling;
- recovery;
- memory pressure;
- storage tier selection.

Long-term frontends translate into ADE's logical/physical representations.

Initial recommendation:

- do not implement SQL in Phase 0 or Phase 1;
- manually build simple physical graphs first;
- add DataFusion/Substrait frontend integration only after the local/distributed runtime boundaries are understood.

---

### 5.2 Logical plan

The logical plan expresses **what** computation should happen.

Examples:

- scan;
- filter;
- projection;
- join;
- aggregate;
- window;
- union;
- sink.

Logical properties include:

- schema;
- expressions;
- ordering requirements;
- boundedness;
- determinism;
- estimated statistics;
- key properties.

The logical plan must not encode execution placement such as:

```text
write this shuffle to S3
```

That belongs lower in the stack.

---

### 5.3 Physical IR

The physical IR is ADE's stable execution contract.

It should express:

- concrete operator algorithms;
- data distributions;
- execution regions;
- partitioning requirements;
- input/output schemas;
- ordering;
- resource hints;
- statefulness;
- retry/determinism properties.

It should **not** hard-code a single physical storage behavior for an exchange.

Illustrative structure:

```rust
PhysicalGraph {
    regions: Vec<ExecutionRegion>,
    exchanges: Vec<ExchangeEdge>,
    sinks: Vec<SinkNode>,
}
```

An exchange edge specifies the required logical distribution:

```rust
enum Distribution {
    Hash(Vec<PhysicalExpr>),
    Range(Vec<SortExpr>),
    Broadcast,
    Gather,
    RoundRobin,
    Custom(...),
}
```

Storage/transport is runtime policy.

---

## 6. Core Execution Concepts

### 6.1 Execution region

An **execution region** is a connected set of operators that can be executed as a pipeline without a mandatory global materialization boundary.

Example:

```text
ParquetScan
    |
Filter
    |
Project
    |
PartialAggregate
```

A region may have local buffers or spill internally. The key property is that the planner does not require the entire upstream region to finish before any downstream processing can begin.

Later physical optimization may fuse operators inside a region.

---

### 6.2 Morsel

A **morsel** is the normal unit of schedulable source/input work.

Examples:

- Parquet row-group range;
- file byte range;
- object-store object/range;
- Kafka offset interval;
- state-shard update range.

Properties:

```text
MorselId
source identity
input range
estimated bytes
estimated rows
locality hints
lineage identity
epoch/progress metadata when applicable
```

Why morsels:

- smaller scheduling granularity;
- improved load balancing;
- easier work stealing;
- easier straggler mitigation;
- less penalty from skew.

The morsel size must remain large enough to amortize scheduling and metadata overhead.

---

### 6.3 Partition

A **partition** is a logical routing domain.

Examples:

- hash bucket;
- range bucket;
- single gather partition;
- broadcast destination set.

Partitions determine which records are semantically grouped, but a large partition need not be represented by one task or one materialized file.

---

### 6.4 Segment

A **segment** is the smallest independently recoverable/tracked portion of exchange or state output.

Potential properties:

```text
SegmentId
ExchangeId / StateId
PartitionId
sequence/range
schema fingerprint
row count
byte count
checksum
producer attempt
lineage reference
consumer progress
known locations
durability properties
```

The segment is central to the research thesis.

A segment may be:

- streamed without retention;
- held in memory;
- spilled locally;
- replicated to a peer;
- uploaded remotely;
- dropped after consumption;
- regenerated from lineage.

The architecture intentionally separates **partition** from **segment** so skewed partitions can be split into multiple independently managed units.

---

### 6.5 Extent

A future **extent** groups segments with identical metadata/placement characteristics.

Example:

```text
exchange E2
partition 43
segments 120..157
location worker-7:/spill/e2/p43/...
durability local-only
```

This prevents segment-level adaptivity from creating unbounded control-plane metadata.

Extents are not needed in the earliest implementation, but metadata structures should leave room for range compaction.

---

## 7. Operator Runtime

### 7.1 Goals

The local operator runtime must provide:

- bounded memory;
- predictable cancellation;
- backpressure;
- streaming batches;
- spill hooks;
- metrics;
- deterministic task identity;
- no implicit global runtime state.

Conceptual interface:

```rust
trait PhysicalOperator: Send {
    fn name(&self) -> &'static str;

    fn poll_next(
        &mut self,
        cx: &mut ExecContext,
    ) -> Poll<Result<Option<RecordBatch>>>;
}
```

The final interface may be push-, pull-, async-stream-, or hybrid-oriented. Phase 1 should experiment before this shape is frozen.

---

### 7.2 ExecContext

`ExecContext` should eventually provide scoped access to:

- cancellation token;
- memory reservation;
- metrics;
- task/attempt identity;
- spill manager;
- I/O handles;
- state handle;
- tracing span;
- runtime configuration.

Operators must not directly access a process-global memory pool or storage singleton.

---

### 7.3 CPU and async execution

Recommended split:

**Async runtime**
- control plane;
- remote object reads/writes;
- network transport;
- checkpoint upload;
- prefetch.

**CPU worker pool**
- decoding;
- filtering;
- joins;
- aggregation;
- expression execution;
- compression where CPU-heavy.

Do not force CPU-bound operator loops through async scheduling merely because networking uses Tokio.

---

## 8. Memory Architecture

### 8.1 Explicit reservations

Large memory consumers must reserve before allocating.

Conceptual flow:

```text
operator requests reservation
        |
        +-- granted --> allocate/use
        |
        +-- denied --> spill / yield / reduce batch / wait
```

Tracked categories should eventually include:

- input batches;
- output batches;
- hash tables;
- sort buffers;
- exchange buffers;
- state;
- decompression;
- metadata.

---

### 8.2 Hierarchy

Long-term memory scopes:

```text
process / worker
    |
    +-- job
         |
         +-- execution region
         |    |
         |    +-- operator
         |
         +-- exchange
         |
         +-- state backend
```

A child reservation cannot exceed its parent.

---

### 8.3 Lifetime-oriented allocation

Rust removes garbage collection, not allocation cost.

Potential later arenas:

- per batch;
- per task;
- per query/job;
- per hash partition;
- per exchange segment;
- per state shard.

The benchmark harness should make allocator/runtime metadata visible before custom allocators are adopted.

---

## 9. Exchange Architecture

### 9.1 Required separation

An exchange has two conceptually separate decisions.

#### Distribution semantics

```text
hash
range
broadcast
round-robin
gather
```

#### Retention/transport mechanism

```text
direct stream
buffer
local spill
peer copy
remote shuffle
object-store materialization
```

The physical graph fixes the distribution semantics. Runtime policy may adapt
retention. ADR-0015 refines hash distribution for Phase 8: bounded source
observations choose one immutable key-shard ownership map and active final set
inside a prepared running job, before the first exchange data. The observations
come from bounded source-prefix probes. The Phase8 remediation also ingests one
original partial-aggregate input batch and retains its exact state/cursor before
commitment; it resumes after every worker ACK without repeating that prefix.
No mid-exchange routing-epoch switch is supported; hash v1 and fixed worker processes remain.

ADR-0016 adds a separate bounded two-source join graph in Phase9. Pure observed
policy chooses broadcast-build versus two-sided hash1%N repartition and a build
side, after bounded real-source probes and before all-worker strategy ACK. The
digest binds both source snapshots, execution, strategy, stages and hash version.
An incomplete prefix cannot authorize broadcast. The owned local join genuinely
converts reserved hash state to grace spill on capacity pressure before output;
it does not change distribution after commitment. Both sides reuse the existing
bounded exchange; spill retention accepts an unconsumed side without a bilateral
bounded-channel cycle. Join failures always cancel, while the accepted default
aggregate-only recovery envelope is preserved. Opt-in non-fixed partial
aggregation keeps the original partial codec but refuses replay/copy combinations
until byte-identical adaptive replay is established. This refines physical
operator choices, not the Phase10 scheduler or Phase11 general frontend/IR.


---

### 9.2 Exchange writer/reader

Conceptual interface:

```rust
trait ExchangeWriter {
    async fn push(&mut self, batch: RecordBatch) -> Result<()>;
    async fn finish(self) -> Result<ExchangeOutput>;
}

trait ExchangeReader {
    async fn next(&mut self) -> Result<Option<RecordBatch>>;
}
```

A writer should produce segments and placement metadata through internal services rather than exposing a permanent "shuffle mode."

---

### 9.3 Exchange capabilities

A segment may possess multiple capabilities simultaneously:

```text
stream-readable
memory-resident
locally-recoverable
peer-recoverable
durably-recoverable
lineage-recomputable
```

This is preferable to a single state enum because:

```text
streaming + local spill
```

is a valid simultaneous state.

---

### 9.4 Backpressure

Backpressure must flow from consumer to producer through bounded queues.

Possible responses:

1. producer slows;
2. exchange buffers;
3. spill begins;
4. remote persistence begins;
5. scheduler moves/splits consumers;
6. policy alters segment retention.

Backpressure is therefore both an execution mechanism and an optimizer signal.

---

## 10. Storage Hierarchy

Storage is modeled through capabilities rather than provider-specific assumptions.

### 10.1 Tier classes

#### In-flight
- lowest persistence;
- lowest extra I/O;
- tied to producer/consumer liveness.

#### Worker RAM
- very fast;
- scarce;
- volatile.

#### Local disk/NVMe
- fast;
- bounded;
- lost with worker/host.

#### Peer replica
- protects against process/host loss depending on placement;
- consumes network + peer capacity.

#### Remote ephemeral shuffle
- decouples executor lifecycle;
- potentially high throughput;
- separate service cost.

#### Durable object storage
- highly durable;
- elastic;
- request/latency overhead;
- useful for serverless/ephemeral workers.

---

### 10.2 Storage interface

Long-term abstraction:

```rust
trait SegmentStore {
    async fn put(...);
    async fn get(...);
    async fn delete(...);
    async fn stat(...);
    fn capabilities(&self) -> StoreCapabilities;
}
```

Capabilities should include:

- durability domain;
- latency class;
- expected throughput;
- range-read support;
- locality;
- capacity;
- configured cost model.

Do not make the storage interface itself responsible for deciding whether a segment should be written.

---

## 11. Lineage Architecture

### 11.1 Goal

Lineage makes recomputation a first-class recovery option.

Each recoverable output needs enough identity to answer:

> What is the minimal deterministic computation that can regenerate this data?

---

### 11.2 Lineage descriptor

Potential fields:

```text
LineageId
physical graph version
operator region ID
source snapshot / offsets
input lineage references
morsel/partition range
partition function + version
UDF artifact/version
deterministic seeds
catalog snapshot/schema fingerprints
```

---

### 11.3 Lineage constraints

Recomputation is safe only if:

- source snapshot is available;
- operator behavior is deterministic or deterministically seeded;
- external UDF/artifact version is available;
- relevant catalog/table version is stable;
- side effects are excluded from recomputable regions.

The planner should mark non-replayable boundaries.

---

## 12. Materialization Catalog

Long-term coordinator metadata tracks recoverable data.

Logical record:

```text
Segment / Extent identity
lineage identity
partition
size / rows
locations
durability class
producer status
consumer progress
expected reuse
estimated recompute cost
estimated restore cost
expiry/retention
checksums
```

The catalog is not necessarily a single database table. Early implementations can use in-memory structures.

Important invariant:

> Materialization metadata describes facts. Policy decides desired state.

---

## 13. Scheduler Architecture

### 13.1 Early scheduler

First distributed scheduler:

- one coordinator;
- static workers;
- static physical graph;
- coarse tasks;
- deterministic partition assignments.

This is intentionally simple.

---

### 13.2 Morsel scheduler

Later scheduler operates on leased work.

```text
Coordinator:
  assign lease of N morsels
       |
Worker:
  process morsels
  report progress
  request more
       |
Other workers:
  steal / receive remaining work
```

Benefits:

- dynamic balancing;
- lower straggler sensitivity;
- worker elasticity;
- heterogeneous hardware support.

---

### 13.3 Scheduler inputs

Long-term scoring inputs:

- locality;
- worker CPU speed;
- memory availability;
- network saturation;
- queue depth;
- storage proximity;
- current exchange placement;
- worker price;
- worker failure risk;
- job deadline slack;
- critical path.

The scheduler and storage policy exchange information through well-defined model inputs, not direct module coupling.

---

### 13.4 Phase10 bounded implementation refinement

ADR0017 implements file-morsel leasing for fixed-hash GroupBySum in a fixed
2–4-process loopback registry. The coordinator owns exclusive Pending/Leased/
Completed state; a pure policy chooses pending preferred work or steals it using
bounded byte-work/capacity/locality/remaining-deadline observations. Producer
activation/drain changes participation only. Final ownership and original
producer transfer identities stay fixed, and every worker closes N terminals.
One independent heartbeat thread renews the same lease while source/aggregate/
exchange work blocks. Ambiguous completion, expiry or any process loss cancels;
leased mode refuses legacy modulo-lineage recovery and complete-copy decisions.
Source fingerprints verify at admission and before/after each leased read.
Schema4 adds schedule context and terminal hashes to the original aggregate
arrays on wire2; schema1/static behavior is unchanged. Catalog/control/audit and
ingress limits are explicit; memory ledgers remain separate from process RSS.
Physical process add/remove and state migration are still unmet PRD Phase10
goals; elastic producer participation must not be described as full elasticity.
This refines §§6.2,9,13,17,22,27–31 without weakening INV1–10.
The 2026-10-26 stabilization isolates partial control peers with a single-thread,
nonblocking set capped at 2*N+1 sockets and 500ms per frame; saturation or malformed
traffic still aborts. Continued remediation retains full admission/pre/post
content hashes with a scoped pool capped at eight hash threads per job stage.
File-level catalog hashing uses at most eight 256KiB buffers (2MiB total);
a producer retains one 1MiB buffer and hashes each file in its scoped pool. These are separate from task/exchange ledgers. Persistent main and
renewal sockets fence every request; the 500ms deadline covers each in-progress
frame, while acknowledged idle sessions remain subject to stage/lease deadlines.
The provisional sender session packs exact contributions into at most N*1024
rows plus one transient frame. Per-file acceptance conserves sent+pending rows;
post-read verification precedes ACK and pending rows flush before terminal hashes.
The fixed table uses positive counts as occupancy, preserving bucket and spill
thresholds while reserving compact storage. See ADR-0017 for failed candidates,
source/ACK invariants, evidence-v2 and liveness limits.
The re-review adds a separately named static_projected diagnostic sharing the
projected per-file operator/hash/packing path with fixed modulo ownership, no
lease service and no recovery. Leased_no_steal retains lease/control/ACK framing
for policy attribution. Static defaults and frontend admission stay unchanged.
The trusted-host operating envelope requires timely renewal scheduling; minimum
lease admission is not availability under CPU starvation or hostile-peer load.
See ADR-0017 and the dated re-review validation for matched/local confirmation
and independent host/day limitations.

---

## 14. Adaptive Policy Engine

### 14.1 Inputs

The policy layer consumes observations from:

- worker resources;
- operator statistics;
- segment metadata;
- consumer lag;
- scheduler state;
- storage throughput;
- historical estimates;
- configured prices;
- job objective.

---

### 14.2 Actions

Examples:

```text
spill segments
persist remotely
drop replica
create peer replica
split partition
coalesce partitions
move consumer
change worker count
switch join algorithm
change checkpoint interval
```

Actions must be:

- idempotent or safely retried;
- auditable;
- validated against correctness constraints;
- rate limited/hysteretic where oscillation is possible.

---

### 14.3 Policy evolution

Recommended order:

1. static policy;
2. threshold heuristic;
3. expected-cost heuristic;
4. calibration from observed performance;
5. search/optimization;
6. learned model only if justified.

Do not begin with machine learning.

---

## 15. Cost Model

### 15.1 Cost dimensions

Long-term expected total cost:

```text
ComputeDollarCost
+ NetworkDollarCost
+ StorageRequestCost
+ StorageCapacityCost
+ ExpectedRecoveryCost
+ SLOPenalty
```

Expected recovery:

```text
Σ failure_scenario
  P(scenario)
  × min(recompute, restore, surviving replica)
```

---

### 15.2 Runtime calibration

Static cloud price configuration is insufficient.

The engine should measure:

- CPU ns/row;
- bytes/s per storage tier;
- network bytes/s;
- observed compression ratio;
- spill throughput;
- source re-read cost;
- operator state build cost.

Price and measured performance are separate model inputs.

---

### 15.3 Explainability

Every model-based action should eventually expose something equivalent to:

```text
action: persist segment range 41..55 remotely

estimated write:
  1.2 s
  $0.00042

estimated recomputation if producer is lost:
  8.4 s
  $0.0049

failure probability before final consumption:
  0.17

deadline slack:
  4.1 s

reason:
  expected recovery-adjusted persistence cost lower
```

---

## 16. Statistics

### 16.1 Operator statistics

Track:

- input rows/bytes;
- output rows/bytes;
- selectivity;
- execution CPU time;
- allocations;
- spill;
- state size;
- compression;
- wait/backpressure time.

---

### 16.2 Exchange statistics

Track:

- produced bytes;
- streamed bytes;
- buffered bytes;
- spill bytes;
- remote bytes;
- restored bytes;
- recomputed bytes;
- consumer lag;
- partition size distribution;
- segment lifetime.

---

### 16.3 Sampling

ADE should not require full upstream completion to learn.

Future sampling strategies:

- initial morsels;
- reservoir samples;
- producer progress samples;
- partial histograms;
- count-min/heavy-hitter summaries;
- approximate cardinality.

Statistics must include confidence/coverage where used for plan changes.

---

## 17. Fault Tolerance

### 17.1 Stable identity

Separate logical work identity from attempt identity.

```text
TaskId / MorselId / SegmentId
        !=
AttemptId
```

Retries must not create new logical data identities.

---

### 17.2 Recovery order

For a missing segment:

```text
1. use surviving in-memory/local/peer copy
2. restore durable materialization
3. recompute minimal lineage closure
4. restart larger execution region
5. restart job
```

The recovery planner chooses the cheapest valid action subject to SLO/correctness.

---

### 17.3 Sinks

Side effects are a correctness boundary.

Batch sink pattern:

```text
attempt writes temporary output
        |
coordinator validates completed partition set
        |
atomic/transactional manifest commit
```

Streaming exactly-once is deferred until checkpoint/state milestones.

---

## 18. Progress, Streaming, and State

Streaming is a future capability, but architecture decisions now must not block it.

### 18.1 Progress

ADE will eventually need a logical progress/frontier model capable of representing:

- event time;
- processing progress;
- nested/cyclic scopes;
- completion;
- watermark/frontier advancement.

This is intentionally inspired by timely dataflow concepts without prematurely adopting an external API.

---

### 18.2 State

State is another form of recoverable materialized computation.

State tiers:

```text
hot memory
local SSD
remote backing
checkpoint snapshot
reconstructible input log
```

The eventual optimizer can compare:

```text
checkpoint vs replay
replicate vs restore
retain hot vs evict
```

using the same general cost framework as batch intermediates.

---

### 18.3 Checkpoints

A checkpoint is a consistent materialization of execution progress.

The architecture should eventually allow:

- full snapshot;
- incremental snapshot;
- asynchronous upload;
- adaptive cadence;
- operator-specific cost estimation.

Checkpoint policy remains separate from checkpoint correctness.

---

## 19. Incremental and Iterative Execution

Long term, ADE adds a third alternative:

```text
restore
full recompute
incrementally update
```

Differential/DBSP-style incremental maintenance can become a physical strategy rather than a separate product.

The physical IR must eventually support:

- changelog/delta collections;
- retained indexed arrangements;
- cyclic regions;
- iterative progress.

These capabilities are explicitly **not** Phase 0 concerns.

---

## 20. Frontend Integration Strategy

### 20.1 First stages

Phase 1–3:

- hand-authored physical graphs;
- no SQL dependency required.

This keeps the runtime understandable.

---

### 20.2 Middle stages

Introduce an adapter:

```text
DataFusion logical/physical concepts
          |
          v
ADE planner/translator
          |
          v
ADE physical IR
```

Potentially support Substrait as an external plan protocol.

---

### 20.3 Long term

Frontends:

- SQL;
- Rust DataFrame;
- Python DataFrame;
- Flight SQL;
- JVM client;
- low-level Rust dataflow API.

All target ADE-owned physical/runtime abstractions.

---

## 21. Connector Architecture

Connectors are extension modules, not hard-coded scheduler cases.

Source contract eventually needs:

- schema;
- split/morsel discovery;
- boundedness;
- snapshot identity;
- pushdown capabilities;
- locality;
- offset/checkpoint semantics.

Sink contract eventually needs:

- schema;
- partitioning requirements;
- idempotence;
- prepare/commit/abort;
- exactly-once integration.

Phase 0 uses local synthetic and Parquet datasets only.

---

## 22. Observability Architecture

### 22.1 Three layers

#### Metrics
Aggregatable numeric values.

#### Traces
Timing and causal relationships.

#### Decision log
Why adaptation happened.

All three matter.

---

### 22.2 IDs carried through telemetry

At minimum:

```text
JobId
GraphVersion
RegionId
OperatorId
ExchangeId
PartitionId
SegmentId/ExtentId
TaskId
AttemptId
WorkerId
```

The earliest phases will implement only a subset.

---

## 23. Security and Multi-Tenancy

Not early scope, but boundaries should be compatible with:

- tenant/job identity;
- credential delegation;
- secret isolation;
- connector-specific auth;
- worker authorization;
- encrypted transport;
- storage access scopes.

Do not put user/cloud credentials into physical plans or lineage records.

---

## 24. Deployment Model

### 24.1 Local development

```text
one process
or
coordinator + multiple worker processes on localhost
```

This must be a first-class supported topology for tests and research.

---

### 24.2 Cluster

Later:

```text
coordinator service
worker pool
object storage
optional remote shuffle
metadata store
```

Kubernetes is one deployment target, not an architectural dependency.

---

### 24.3 Ephemeral/serverless

Long-term architecture supports:

- workers with no durable local state;
- object-store-backed recovery;
- dynamic worker acquisition;
- scale-to-zero.

ADE should not require this model for all deployments.

---

## 25. Repository / Module Architecture

The following is the **target ownership map**, not a requirement to create every crate during scaffolding. The repository scaffold intentionally creates only `ade-bench`; future crates are added when a phase gives them real ownership.

```text
crates/
  ade-common/        stable IDs, errors, shared primitives
  ade-array/         Arrow adapters and batch utilities
  ade-expr/          physical expressions
  ade-physical/      physical IR
  ade-exec/          local execution runtime/operators
  ade-memory/        reservations/accounting
  ade-spill/         local spill
  ade-exchange/      exchange mechanisms
  ade-storage/       segment-store abstractions
  ade-lineage/       lineage/recovery identity
  ade-cost/          cost models/policies
  ade-scheduler/     scheduler
  ade-worker/        worker service
  ade-coordinator/   control plane
  ade-proto/         RPC/control protocol
  ade-observe/       metrics/tracing/decision records
  ade-state/         future state/checkpoint runtime
  ade-bench/         benchmark harness

benchmarks/
  specs/
  fixtures/
  results/           ignored except canonical examples

docs/
  product/
  architecture/
  plans/
  adr/
  research/
  reports/
```

### Important scaffolding guidance

Do **not** create a large collection of empty future crates merely because they appear above.

Initial scaffold should create only modules required by Phase 0 plus documentation/testing/CI structure. Add future crates when the implementation acquires real ownership boundaries.

This reduces:
- empty abstraction churn;
- LLM context noise;
- circular placeholder APIs;
- premature dependency commitments.

---

## 26. Dependency Rules

Long-term directional dependencies:

```text
common
  ^
  |
array / expr
  ^
  |
physical
  ^
  |
exec ---- memory ---- spill
  |
exchange ---- storage
  |
lineage
  |
scheduler / cost
  |
worker / coordinator
```

Rules:

1. execution operators do not depend on coordinator implementation;
2. storage does not depend on cost policy;
3. cost policy consumes abstract measurements/capabilities;
4. physical IR does not depend on a concrete RPC layer;
5. worker and coordinator depend on interfaces, not vice versa;
6. benchmark harness may depend on public engine APIs, never internal private modules merely to extract data;
7. frontend adapters do not own execution semantics.

---

## 27. Configuration Architecture

Configuration falls into three categories.

### 27.1 Correctness constraints

Examples:

- memory hard limit;
- delivery guarantee;
- maximum data-loss policy;
- allowed storage regions.

These are not optimizer suggestions.

### 27.2 Objective

Examples:

- deadline;
- budget;
- cost weights.

### 27.3 Mechanism/debug overrides

Examples:

```text
force_exchange_mode=local_spill
disable_adaptive_partitioning=true
```

These exist for debugging and benchmarks, not normal user tuning.

Principle:

> Prefer goals/constraints over implementation knobs.

---

## 28. Testing Architecture

### 28.1 Unit tests

For:
- IDs;
- partition functions;
- reservation logic;
- cost calculations;
- serialization;
- state machines.

---

### 28.2 Property tests

Useful invariants:

- repartition preserves all rows exactly once;
- segment range merge/split preserves identity coverage;
- retries do not alter logical IDs;
- memory reservations never exceed parent budget;
- recovery selects only semantically valid lineage.

---

### 28.3 Integration tests

Use local multi-process topology.

Examples later:

- worker loss;
- exchange recovery;
- scheduler rebalance;
- durable restore;
- coordinator restart.

---

### 28.4 Fault injection

Fault injection is part of feature acceptance, not a final hardening phase.

Inject:

- process kill;
- delayed messages;
- dropped connection;
- disk full;
- object-store failure;
- slow consumer;
- corrupt segment/checksum mismatch.

---

### 28.5 Performance tests

Performance results do not belong in normal unit-test pass/fail unless a stable dedicated environment exists.

Instead:

- collect benchmark results;
- compare against baseline;
- report regression;
- gate only controlled CI microbenchmarks if noise is acceptable.

---

## 29. Benchmark Architecture

Phase 0 implements this layer first.

### 29.1 Two benchmark classes

### Microbenchmarks

Purpose:
- isolate kernels/data structures;
- high iteration count;
- Criterion-style statistical measurement.

Examples:
- hash insertion;
- partition hashing;
- Arrow filtering;
- encoding conversion.

### Macrobenchmarks

Purpose:
- whole query/workload;
- process isolation;
- wall-clock/resource measurements;
- engine comparisons;
- JSON result artifacts.

Examples:
- Parquet scan;
- filter/project;
- group-by;
- skew workload;
- derived TPC-H subset.

ADE needs both. They must not share assumptions about iteration/warmup.

---

### 29.2 Macrobenchmark engine adapter

Conceptual:

```rust
trait BenchmarkEngine {
    fn engine_name(&self) -> &'static str;
    fn engine_version(&self) -> String;

    async fn prepare(
        &self,
        dataset: &DatasetManifest,
        case: &BenchmarkCase,
    ) -> Result<PreparedCase>;

    async fn run(
        &self,
        prepared: &PreparedCase,
    ) -> Result<RunMetrics>;
}
```

Phase 0 adapters:

- DataFusion baseline;
- optional Polars baseline for semantically equivalent cases.

Later:

- ADE local;
- ADE distributed;
- Spark;
- Flink;
- Ballista;
- Trino.

External engines may use process-command adapters rather than linking libraries.

---

### 29.3 Result schema

Every macrobenchmark result should contain:

```text
schema_version
run_id
timestamp
git commit / dirty state
benchmark harness version
engine name/version
case ID/version
dataset manifest/hash
hardware fingerprint
OS/architecture
Rust compiler/build profile when relevant
engine config
warmups
iterations
per-iteration wall time
summary statistics
peak RSS when supported
CPU time when supported
bytes/rows when available
status
error
notes/tags
```

Later metrics may add:
- storage I/O;
- network bytes;
- spill;
- object-store requests;
- cost.

The schema must be versioned from Phase 0.

---

## 30. Phase Evolution

### Phase 0 — Architecture Lab and Benchmark Harness

Implements:

- benchmark contracts;
- dataset generators/manifests;
- DataFusion baseline adapter;
- macrobenchmark JSON;
- comparison tooling;
- initial Criterion microbenchmark support;
- architecture/ADR discipline.

Does not implement ADE execution.

---

### Phase 1 — Minimal Single-Node Physical Engine

Adds:

- Arrow batch execution;
- scan/filter/project/aggregate;
- explicit memory hooks;
- metrics.

---

### Phase 2 — Spillable Local Engine

Adds:

- bounded memory;
- spill;
- external sort;
- memory-pressure behavior.

---

### Phase 3 — Static Distributed Batch

Adds:

- coordinator/worker;
- static tasks;
- hash exchange.

---

## Phases 4–8 — Research Core

Adds:

- unified exchange;
- adaptive exchange;
- lineage/recovery;
- expected-cost persistence;
- runtime repartition/skew.

These phases validate the central thesis.

---

## Later phases

Broaden into:

- adaptive operators;
- morsel scheduler;
- SQL/DataFrame;
- reliability/HA;
- disaggregated cloud execution;
- streaming;
- state/checkpointing;
- exactly-once;
- incremental/iterative dataflow;
- connector ecosystem;
- multi-tenancy;
- heterogeneous hardware;
- shared materialization.

---

## 31. Architecture Invariants

The following should be treated as non-negotiable unless an ADR explicitly supersedes them.

### INV-1 — Logical identity is separate from execution attempt

Retries never change the identity of the logical data/work.

### INV-2 — Distribution semantics are separate from storage mechanism

`Hash(keys)` cannot mean "write a blocking shuffle file."

### INV-3 — Policy cannot bypass correctness

Adaptive decisions select among valid mechanisms only.

### INV-4 — No hidden unbounded memory

Queues, hash tables, buffers, state, and exchange memory must have accounting hooks.

### INV-5 — Runtime behavior is observable

Adaptive actions and key resource decisions emit structured diagnostics.

### INV-6 — Physical runtime remains frontend-independent

SQL/DataFusion integration cannot become a required dependency for core execution.

### INV-7 — Worker-local state is assumed lossy unless explicitly promoted

Local disk/RAM does not silently become durable cluster state.

### INV-8 — Replayability is explicit

Operators/sources that cannot be deterministically replayed create lineage boundaries.

### INV-9 — Optimization can be disabled

Static policy baselines remain available for testing.

### INV-10 — External system compatibility beats unnecessary reinvention

Arrow/Parquet/open protocols are reused where adequate.

---

## 32. Initial ADR Backlog (Historical Proposal)

This list records the original Phase 0 proposal, not the authoritative accepted
ADR titles. The accepted records and current numbering are indexed in
`docs/adr/README.md`. In particular, the accepted ADR-0012 is Adaptive Exchange
v1, ADR-0013 is Lineage Recovery v1, and ADR-0014 is Expected-Cost Materialization
v1; the proposed “Cost model API” item below was never accepted under ADR-0012.

These decisions should be recorded as ADRs when implementation reaches them.

1. **ADR-0001:** Rust workspace and minimal crate boundary.
2. **ADR-0002:** Macrobenchmark result schema.
3. **ADR-0003:** Dataset manifest/versioning strategy.
4. **ADR-0004:** Initial Arrow/DataFusion dependency strategy.
5. **ADR-0005:** Operator execution model: pull/push/hybrid.
6. **ADR-0006:** Memory reservation interface.
7. **ADR-0007:** Spill-file format and lifecycle.
8. **ADR-0008:** Control-plane RPC framework.
9. **ADR-0009:** Initial exchange data transport.
10. **ADR-0010:** Segment identity and checksum model.
11. **ADR-0011:** Lineage representation.
12. **Proposed, unaccepted:** Cost model API. Accepted ADR-0012 is [Adaptive Exchange v1](../adr/0012-adaptive-exchange-v1.md).
13. **Proposed, unaccepted:** Streaming progress model. Accepted ADR-0013 is [Lineage Recovery v1](../adr/0013-lineage-recovery-v1.md).
14. **Proposed, unaccepted:** State/checkpoint consistency model. Accepted ADR-0014 is [Expected-Cost Materialization v1](../adr/0014-expected-cost-materialization-v1.md).

Phase 0 should resolve only ADRs 0001–0004.

---

## 33. Phase 0 Architecture Boundary

Phase 0 should produce the following concrete architecture:

```text
                    CLI
                     |
                     v
             +---------------+
             | ade-bench     |
             +-------+-------+
                     |
          +----------+-----------+
          |                      |
          v                      v
 +------------------+   +------------------+
 | Dataset Manager  |   | Benchmark Runner |
 +--------+---------+   +---------+--------+
          |                       |
          v                       v
 manifests + files        Engine Adapter API
                                  |
                       +----------+----------+
                       |                     |
                       v                     v
                 DataFusion             optional Polars
                  baseline                baseline

                     |
                     v
              Result Recorder
                     |
                     v
          versioned JSON artifact
                     |
                     v
             Comparison Tool
```

No ADE physical execution path exists yet. The `ade-bench` crate may contain benchmark orchestration and baseline-engine integration, but it must not quietly become the ADE execution engine.

This is deliberate and is the Phase 0 stop boundary.

---

## 34. Architecture Quality Bar

Before beginning Phase 1, the repository should make it straightforward to answer:

- What exact workload changed?
- What exact engine/build ran?
- On what hardware?
- With what dataset?
- What commit/configuration?
- How variable were the results?
- How much memory did it use?
- Can the run be reproduced?
- Can a future ADE backend produce the same result schema?
- Can a regression report compare ADE against a fixed baseline?

If the benchmark substrate cannot answer these questions, performance claims made during later phases will be unreliable.

---

## 35. References

The architecture is derived primarily from the ADE PRD and its research survey. Particularly relevant external references for implementation choices include:

- Apache DataFusion benchmark tooling:  
  https://github.com/apache/datafusion/tree/main/benchmarks
- Apache DataFusion Benchmarks repository:  
  https://github.com/apache/datafusion-benchmarks
- Apache DataFusion:  
  https://datafusion.apache.org/
- Apache DataFusion Ballista shuffle design:  
  https://datafusion.apache.org/ballista/contributors-guide/shuffle.html
- Apache Flink Hybrid Shuffle:  
  https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/ops/batch/batch_shuffle/
- Timely Dataflow / Naiad:  
  https://www.microsoft.com/en-us/research/publication/naiad-a-timely-dataflow-system-2/
- Differential Dataflow:  
  https://www.microsoft.com/en-us/research/publication/differential-dataflow/
- DBSP / Feldera:  
  https://docs.feldera.com/
- Velox memory management:  
  https://facebookincubator.github.io/velox/develop/memory.html
- Exoshuffle:  
  https://arxiv.org/abs/2203.05072
- Pocket:  
  https://www.usenix.org/conference/osdi18/presentation/klimovic

---

## 36. Summary

ADE should be architected around a simple long-term idea:

> **Intermediate computation has value, cost, location, lineage, and risk. The runtime should manage it dynamically rather than forcing every workload through one fixed materialization model.**

The first implementation phases must resist the temptation to build the full system.

Phase 0 proves the measurement foundation.

Phase 1 proves local execution.

Phase 2 proves bounded memory.

Phase 3 proves distribution.

Phases 4–8 prove the research thesis.

Only after those foundations are credible should ADE expand toward the breadth of Spark and Flink.

The architecture is successful if the initial code remains small while the fundamental abstractions leave a coherent path to that end state.
