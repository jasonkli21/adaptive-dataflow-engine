# Adaptive Dataflow Engine (ADE) — Product Requirements Document

**Status:** Refined baseline for implementation  
**Date:** 2026-10-15  
**Working name:** Adaptive Dataflow Engine (ADE)  
**Primary implementation language:** Rust  
**Project horizon:** Multi-year, milestone-gated  
**Core research thesis:** **Build a distributed execution engine where materialization and recomputation are adaptive decisions rather than fixed properties of the processing model.**

---

## 1. Document Role and Authority

This PRD defines **why ADE exists, what the long-term product is, what is in and out of scope, and the milestone sequence**.

Companion documents have narrower authority:

1. `docs/product/prd.md` — product scope, research thesis, milestone outcomes.
2. `docs/architecture/architecture.md` — system boundaries, invariants, and long-lived technical direction.
3. `docs/plans/phase-0-implementation-plan.md` — exact implementation contract for Phase 0.
4. ADRs — concrete implementation decisions. An ADR may supersede an architectural recommendation only when it explicitly records the reason and affected invariant/section.
5. Implementation reports — what was actually built and what remains.

Where an illustrative API in this PRD conflicts with the architecture or an accepted ADR, the architecture/ADR wins. Code examples in this PRD are conceptual unless explicitly marked normative.

---

## 2. Executive Summary

ADE is a long-term effort to build a general-purpose distributed data-processing engine in Rust that can eventually occupy the same architectural category as Apache Spark and Apache Flink: a fundamental execution substrate for large-scale batch, streaming, SQL, stateful, incremental, iterative, and data-intensive workloads.

The project must **not** begin as a feature-for-feature rewrite of Spark or Flink. The initial system is deliberately narrow. The first real distributed engine milestone should execute a graph such as:

```text
scan -> partial aggregate -> hash exchange -> final aggregate -> output
```

with bounded resource usage and an exchange abstraction capable of supporting multiple transport/retention mechanisms.

The project's differentiating idea is that an intermediate edge should not permanently be classified as a “pipelined edge,” “blocking shuffle,” “cached result,” or “durable checkpoint.” Instead, the engine should eventually make those choices dynamically and at finer granularity than an entire job or stage.

For a recoverable segment of intermediate work, ADE should be able to choose among:

```text
lineage only / discard
        |
        v
direct pipeline
        |
        v
bounded memory buffer
        |
        v
local NVMe spill
        |
        v
peer replica / remote shuffle
        |
        v
durable object storage
```

The choice may change while the job runs.

The same principle should later govern:

- recompute versus restore;
- partition count and skew splitting;
- worker count and placement;
- broadcast versus repartition;
- join/aggregation algorithm;
- checkpoint frequency;
- streaming state placement;
- incremental update versus full recomputation;
- cross-job reuse of materialized computation.

The long-term product is therefore:

> **A cost-performance-native adaptive dataflow runtime in which execution, storage, scheduling, and recovery are jointly optimized under explicit correctness, latency, budget, and durability constraints.**

---

## 3. Problem Statement

Distributed processing repeatedly confronts a core tension.

### 3.1 Pipelining is efficient but coupled

Pipelining can:

- avoid writing and rereading intermediates;
- reduce latency;
- let consumers start before producers finish;
- reduce storage demand.

But it can also:

- couple producer and consumer lifetimes;
- amplify backpressure;
- require more simultaneous capacity;
- complicate fault recovery;
- perform poorly when intermediates exceed memory;
- make elastic scheduling harder.

### 3.2 Materialization is robust but expensive

Materialization can:

- decouple producer and consumer scheduling;
- simplify retries;
- support jobs wider than the active worker pool;
- make elastic or serverless compute practical;
- preserve expensive work.

But it can also:

- create barriers;
- add serialization and I/O;
- increase latency;
- consume storage and request budget;
- write data that is read only once or never needed again.

### 3.3 Existing engines adapt, but usually within a chosen substrate

Spark provides adaptive query execution, skew handling, partition coalescing, dynamic allocation, lineage-based recomputation, and multiple shuffle mechanisms. Flink provides streaming-first pipelining, backpressure, checkpoints, savepoints, and hybrid shuffle. Trino can spool exchanges for fault-tolerant execution. Ray and Dask dynamically schedule fine-grained work and spill data. DataFusion/Ballista offer modern Rust/Arrow foundations. RisingWave and newer Flink state architectures embrace disaggregated storage. Timely/Differential Dataflow and DBSP demonstrate deeper unification of streaming, iteration, and incremental computation.

The opportunity is not that hybrid mechanisms do not exist. The opportunity is to make **the choice among mechanisms the primary runtime optimization problem**.

---

## 4. Product Vision

### 4.1 Long-term user experience

A mature ADE should allow a user to describe a computation and an objective while leaving most mechanism selection to the runtime.

Example objective:

```text
Run this transformation in <= 90 seconds and minimize total cloud cost.
```

A possible runtime response might be:

```text
- 6 baseline workers + 2 short-lived workers on critical path
- Parquet predicate/projection pushdown
- broadcast one small dimension input
- 384 initial hash buckets
- stream most exchange segments
- spill slow-consumer segments locally
- remotely persist only expensive/high-risk segments
- split four skewed partitions
- remove excess workers after the join
```

A latency-first objective might instead choose more simultaneous compute, more replication, less durable materialization, and speculative execution.

### 4.2 Programming model

Long-term relational API:

```python
events = ade.read_parquet("s3://lake/events/")
users = ade.read_table("catalog.prod.users")

result = (
    events
      .filter(col("event_type") == "purchase")
      .join(users, on="user_id")
      .group_by("country")
      .agg(sum("amount"))
)

result.write_table("catalog.analytics.revenue")
```

The same logical concepts should extend to unbounded input:

```python
events = ade.read_kafka("events")

result = (
    events
      .with_event_time("ts")
      .window("5 minutes")
      .join(users, on="user_id")
      .group_by("country")
      .agg(sum("amount"))
)
```

The runtime should share a deep dataflow foundation rather than implementing an unrelated batch engine and streaming engine behind similar APIs.

---

## 5. Goals

### 5.1 Primary goals

1. **Optimize cost-performance rather than peak speed alone.**
2. Make **materialization, recomputation, placement, and durability adaptive runtime decisions**.
3. Support **fine-grained adaptation**, eventually at segment/morsel granularity where useful.
4. Build a **Rust-native physical runtime** with explicit ownership and resource accounting.
5. Co-optimize **execution, storage, scheduling, and recovery**.
6. Support **bounded and unbounded computation** on one conceptual dataflow foundation.
7. Preserve a path to the **breadth and extensibility of Spark/Flink**.
8. Reuse open standards and ecosystems where they are not differentiating: Arrow, Parquet, object stores, SQL frontends, open table formats, and open RPC/plan protocols when appropriate.
9. Make all major optimizations **benchmarkable, disable-able, and observable**.
10. Keep mechanisms modular enough that research policies can be replaced without rewriting the engine.

### 5.2 Secondary goals

- excellent single-node execution so distribution is used only when worthwhile;
- efficient execution on small clusters as well as large ones;
- strong support for preemptible/spot workers;
- compatibility with ephemeral/serverless workers without requiring them;
- embeddable library and service deployment modes;
- strong observability and explainability;
- multi-language clients without putting another runtime on the hot path.

---

## 6. Non-Goals

### 6.1 Early non-goals

The first phases do not attempt to provide:

- broad SQL coverage;
- streaming or exactly-once sinks;
- Kubernetes operators;
- ML or graph libraries;
- hundreds of connectors;
- multi-tenant security;
- a web UI;
- Spark API compatibility;
- GPU execution;
- a custom table format;
- a custom object store;
- a custom SQL parser;
- a custom universal columnar format.

### 6.2 Long-term non-goals / biases against reinvention

ADE should generally avoid:

- owning commodity storage services;
- inventing another durable lakehouse file format without a compelling reason;
- coupling correctness to one cloud provider;
- requiring the JVM in the core runtime;
- making SQL the only API;
- making one specific cluster manager mandatory.

---

## 7. Core Product Principles

### 7.1 Correctness before adaptation

Policies may alter mechanism, not semantics.

### 7.2 Mechanism and policy remain separate

The runtime exposes valid mechanisms; the optimizer chooses among them.

### 7.3 Storage tiers form a continuum

RAM, local disk, peer copies, remote shuffle, and object storage are placements with different cost, latency, capacity, and failure properties.

### 7.4 Failure cost is part of execution cost

A cheap plan that loses minutes of work on one worker failure may not be cheap in expectation.

### 7.5 Avoid barriers unless correctness or economics justify them

Barriers are useful but must not be required simply to obtain runtime statistics.

### 7.6 Boundedness is a property

Batch is a bounded computation, not a reason to fork the core runtime architecture.

### 7.7 Arrow at boundaries, specialization internally

Arrow is the default interoperability contract; hot internal operators may use specialized representations if measured benefit justifies complexity.

### 7.8 Users specify constraints and objectives more often than implementation knobs

Prefer:

```text
memory <= 64 GiB
finish <= 5 min
minimize dollar cost
```

over:

```text
shuffle.mode=foo
spill.threshold=bar
```

Debug overrides still exist to create static baselines.

---

## 8. Core Research Thesis

### 8.1 Materialization should be granular

A multi-terabyte exchange should not need one global durability decision.

Partitions differ in:

- skew;
- size;
- consumer lag;
- fanout;
- lineage cost;
- expected reuse;
- producer failure exposure.

Therefore retention should eventually be decided per independently recoverable unit.

### 8.2 Durability is a set of capabilities, not one state

A segment may simultaneously be:

- actively streamed;
- buffered in RAM;
- spilled locally;
- replicated to a peer;
- reconstructible from lineage.

The implementation should model capabilities rather than a single mutually exclusive “shuffle state.”

### 8.3 Recomputation has an explicit price

For a segment `s`:

```text
ExpectedRecomputeCost(s)
  = P(loss before final consumption)
    * Cost(minimal lineage closure(s))
```

Persistence also has a price:

```text
PersistCost(s)
  = write cost
  + storage/request cost
  + expected future read cost
```

The optimizer should preserve data when the expected value of preservation exceeds the expected cost of recomputation and SLO risk.

### 8.4 The scheduler is part of the decision

Whether data should be persisted depends on:

- worker lifetime/risk;
- spare capacity;
- producer rescheduling cost;
- network load;
- consumer readiness;
- source reread cost;
- deadline slack.

Storage policy and scheduling therefore exchange information through a common cost/observation model.

---

## 9. Core Domain Model

The architecture document defines these concepts normatively; this section states the product intent.

### Execution Region

A pipeline-compatible set of physical operators without a mandatory global materialization boundary.

### Morsel

A fine-grained unit of schedulable input work such as a Parquet row-group range or future stream offset interval.

### Partition

A logical routing domain, such as a hash bucket.

### Segment

The smallest independently tracked/recoverable portion of exchange or state output. This is the central adaptive object.

### Extent

A compact metadata representation grouping adjacent segments with identical placement/durability properties.

### Lineage Descriptor

Enough deterministic identity to reconstruct a lost segment or state shard from a known source snapshot and operator version.

### Materialization Record

Facts about where recoverable data exists, its durability, size, consumption progress, and estimated recovery alternatives.

### Objective

Explicit user/system target such as min latency, min cost, or min cost under deadline.

---

## 10. Landscape: What ADE Should Learn and Where It Should Push Further

### 10.1 Apache Spark

**Learn from:** lineage-based recomputation, broad ecosystem, DataFrame/SQL adoption, AQE, skew handling, dynamic allocation, mature connectors, extension libraries.

**Push further:** make persistence/recomputation a continuous runtime policy; make columnar/native execution foundational; jointly reason about shuffle placement, worker lifecycle, and recovery cost.

### 10.2 Apache Flink

**Learn from:** stateful streaming, event time, watermarks, checkpoints/savepoints, backpressure, rescaling, bounded/unbounded abstractions, hybrid shuffle, disaggregated state direction.

**Push further:** integrate hybrid exchange with lineage/recomputation economics; use the same adaptive policy framework for batch intermediates and streaming state.

### 10.3 DataFusion / Ballista

**Learn from:** Rust + Arrow viability, vectorized operators, clean logical/physical separation, extensibility, simple distributed architecture, explicit discussion of blocking shuffle tradeoffs.

**ADE distinction:** the distributed runtime, exchange semantics, recovery, and cost optimizer are the product rather than a thin distributed wrapper around a local query engine.

### 10.4 DataFusion Distributed

**Learn from:** pipelined exchanges, distributed planning hooks, dynamic tasking, early runtime statistics.

**Extend with:** durable intermediate catalog, multi-tier materialization, recovery-aware policy, stateful streaming, explicit dollar-cost objectives.

### 10.5 Arroyo

**Learn from:** modern Rust-native streaming, checkpointing, SQL streaming, cloud-oriented operations.

**ADE distinction:** general execution substrate beginning from adaptive batch exchange and expanding into streaming.

### 10.6 RisingWave

**Learn from:** disaggregated compute/storage and remote state architecture.

**ADE distinction:** not a streaming database; durability can vary according to workload correctness and recovery requirements.

### 10.7 Timely / Differential Dataflow / Materialize

**Learn from:** logical progress, cyclic dataflow, nested iteration, incremental updates, arrangements/shared indexes.

**Extend with:** elastic placement/materialization and an explicit recovery/cost optimizer.

### 10.8 DBSP / Feldera

**Learn from:** algebraic incrementalization and incremental maintenance of rich query languages.

**Long-term ADE opportunity:** add `incrementally update` as a third alternative to `restore` and `full recompute`.

### 10.9 Trino

**Learn from:** interactive distributed SQL, connector architecture, durable exchange spooling for fault-tolerant execution.

**Extend with:** dynamically selected durability rather than a coarse execution mode.

### 10.10 Ray Data and Dask

**Learn from:** fine-grained work, locality, work stealing, adaptive clusters, object spilling, dynamic Python ecosystem integration.

**Extend with:** dataflow-aware semantics, schema/cardinality knowledge, native memory accounting, and optimizer-visible lineage.

### 10.11 Velox and Polars

**Learn from:** explicit memory arbitration, spill, vector encodings, high-quality native execution, lazy/streaming local processing.

**Product lesson:** a distributed runtime must not erase strong single-node efficiency.

### 10.12 Beam

**Learn from:** portable bounded/unbounded programming model and runner separation.

**Product lesson:** ADE-specific physical optimization should coexist with portable logical plan boundaries.

### 10.13 Research systems

Important prior art includes:

- RDDs — lineage/recomputation;
- Naiad/Timely — progress and cyclic dataflow;
- Differential Dataflow — incremental maintenance;
- Eddies — continuously adaptive execution;
- Pocket — cost-aware heterogeneous storage;
- Exoshuffle — shuffle as an extensible data/control-plane abstraction;
- Drizzle — separating processing from coordination intervals;
- Deca — data lifetime/layout as a systems performance concern;
- Quincy — scheduling as an explicit cost optimization problem.

ADE should treat this literature as a design toolbox, not a checklist to reproduce.

---

## 11. Cost Model Product Requirements

### 11.1 Required observations

Per worker, eventually:

- CPU utilization and effective throughput;
- memory reserved/resident;
- local disk throughput/pressure;
- network throughput/RTT;
- object-store throughput/errors;
- available cores/slots;
- decommission/interruption risk.

Per operator:

- input/output rows and bytes;
- selectivity;
- CPU time per unit;
- state/hash-table size;
- spill bytes;
- compression/encoding behavior;
- wait/backpressure time.

Per segment:

- production cost;
- lineage depth;
- consumer count/progress;
- replicas/locations;
- estimated reuse.

### 11.2 Objective function

An initial long-term model:

```text
TotalExpectedCost =
    ComputeDollarCost
  + NetworkDollarCost
  + StorageRequestCost
  + StorageCapacityCost
  + ExpectedRecoveryCost
  + SLOPenalty
```

The first implementation must use interpretable heuristics before learned models.

---

## 12. Rust and Native Systems Opportunities

Rust is an enabling technology, not the novelty claim.

Potential advantages to pursue and measure:

- deterministic ownership and lack of GC pauses;
- explicit memory accounting and RAII reservations;
- lifetime-oriented arenas;
- Arrow-native columnar execution;
- SIMD/vectorized kernels;
- reduced serialization/copying;
- efficient asynchronous I/O;
- explicit topology/NUMA control;
- safe concurrency boundaries;
- optional kernel fusion/JIT only where measurement justifies it.

Rust does not remove allocator fragmentation, cache misses, NUMA penalties, temporary allocations, or poor data structure design. Those remain benchmarked systems problems.

---

## 13. Research Agenda

Key research avenues include:

1. online materialization policy;
2. minimal-lineage partial recovery;
3. dynamic partition split/coalesce;
4. segment/morsel-level speculative execution;
5. runtime operator algorithm switching;
6. cross-query materialization reuse;
7. incremental recomputation;
8. adaptive checkpoint/coordination intervals;
9. network- and egress-aware scheduling;
10. disaggregated shuffle;
11. encoded/compressed execution;
12. workload-aware single-node-versus-distributed choice;
13. hardware-aware scheduling;
14. learned policy only after sufficient interpretable baseline data exists.

Every research feature must ship with a static baseline and an experiment capable of falsifying its claimed benefit.

---

## 14. Platform Surface: Long-Term Requirements

To reach the practical category occupied by Spark/Flink, ADE eventually needs:

- SQL;
- DataFrame APIs;
- lower-level dataflow/operator APIs;
- bounded and unbounded sources;
- keyed state, windows, timers, event time;
- checkpoints/savepoints and rescaling;
- exactly-once-capable sinks;
- lakehouse/table integration;
- connector SDK;
- Rust/Python/SQL and language-neutral plan submission;
- UDF model;
- multi-tenant scheduling/admission control;
- observability and job history;
- deployment on local, cluster, Kubernetes, and elastic cloud environments;
- extension libraries for graph/ML/CEP/other domains rather than embedding all domains in core.

This breadth is a long-term direction, not permission to dilute the early research core.

---

## 15. Benchmark and Evaluation Strategy

### 15.1 Baselines over time

Use the systems appropriate to the capability being evaluated:

- DataFusion / Polars for local native execution;
- Spark / Flink / Ballista / DataFusion Distributed / Trino for distributed batch where semantically comparable;
- Flink / Arroyo / RisingWave for stateful streaming;
- specialized comparisons for incremental execution.

### 15.2 Standard workloads

- TPC-H-derived workloads;
- TPC-DS-derived workloads later;
- Nexmark for streaming;
- sort;
- hash aggregate;
- repartition;
- joins;
- windows.

### 15.3 Adversarial workloads

The thesis requires workloads that expose execution/storage/recovery tradeoffs:

- extreme skew;
- intermediate data larger than RAM;
- slow consumers;
- source reread cheap versus expensive;
- object-store throttling;
- worker loss mid-exchange;
- spot interruption warnings;
- heterogeneous workers;
- variable network bandwidth;
- high fanout/reuse;
- bursty streaming load;
- state larger than worker memory;
- rescaling under load.

### 15.4 North-star metrics

Performance:
- wall time;
- p50/p95/p99 latency;
- throughput.

Resource efficiency:
- CPU-seconds;
- GB-RAM-seconds;
- network bytes;
- disk bytes;
- object-store bytes and request counts.

Cost:
- dollars/job;
- dollars/TB;
- dollars/million events;
- cost under a fixed deadline.

Recovery:
- time to recovery;
- recomputed bytes;
- restored bytes;
- lost work;
- SLO violation duration.

North-star claim format:

> For workload W under deadline D and correctness guarantees G, ADE achieves a lower total expected cost than baseline X without violating D or G.

---

## 16. Phase Roadmap

Phases are capability gates, not time estimates. The project should stop and validate at each phase.

## Phase 0 — Architecture Lab and Benchmark Harness

**Goal:** Establish trustworthy measurement before engine implementation.

**Deliverables:**
- Rust workspace/scaffold;
- deterministic synthetic datasets and manifests;
- DataFusion baseline;
- process-isolated macrobenchmark runner;
- versioned JSON results;
- correctness validation;
- comparison tooling;
- Criterion microbenchmark path;
- initial TPC-H-derived path;
- ADR and benchmark methodology discipline.

**Non-goal:** implement ADE operators.

**Exit:** a future ADE engine can plug into the same workload/result contract.

---

## Phase 1 — Minimal Single-Node Physical Engine

**Goal:** Own and understand the local execution loop.

Operators:
- Parquet scan;
- filter;
- projection;
- hash aggregate;
- limit/sink.

Runtime:
- Arrow-compatible batches;
- explicit memory hooks;
- backpressure/bounded queues;
- operator metrics;
- deterministic execution IDs.

**Exit:** correct supported workloads run through `ade-local` and are measured by the Phase 0 harness.

---

## Phase 2 — Spillable Local Engine

**Goal:** make memory a managed resource rather than a failure boundary.

Deliver:
- worker memory manager;
- spillable aggregation;
- external sort;
- local partitioned spill;
- async prefetch;
- pressure metrics.

**Exit:** workloads materially larger than configured memory complete while respecting the memory contract.

---

## Phase 3 — Static Distributed Batch v0

**Goal:** execute the first real distributed graph.

Topology:
- one coordinator;
- N static workers;
- static graph;
- basic failure detection.

Graph:

```text
scan -> partial aggregate -> hash exchange -> final aggregate -> gather
```

**Exit:** correct execution on 2–4 worker processes/nodes with observable network/work distribution.

---

## Phase 4 — Unified Exchange Mechanism

**Goal:** one exchange API supports:
- direct stream;
- bounded buffering;
- local spill;
- full local materialization;
- durable object-store materialization.

**Exit:** the same logical distribution can execute under each mechanism without changing correctness.

---

## Phase 5 — Adaptive Exchange v1

**Goal:** runtime chooses among streaming, buffering, spill, and persistence using deterministic heuristics.

Inputs:
- buffer occupancy;
- consumer lag;
- memory pressure;
- intermediate size;
- estimated lineage cost;
- worker health.

These are candidate signals for the adaptive policy as it evolves, not a
requirement that every phase implement every signal. Phase 5 v1 may use a
truthful measured subset: exchange-buffer occupancy, consumer readiness/lag,
spill backlog, and observed intermediate production. It must not label exchange
pressure as process memory pressure or invent lineage-cost or nonfatal worker
health estimates. Lineage-cost signals belong with the Phase 6 lineage model;
worker failures continue to fail the job. Any broader Phase 5 signal requirement
must be added explicitly to this PRD before expanding the phase contract.

**Exit:** one job can use different mechanisms for different partitions/segments and outperform at least one static policy on a defined adversarial workload without unacceptable normal-case regression.

---

## Phase 6 — Lineage and Recovery Planner

**Goal:** recomputation becomes an explicit recovery mechanism.

Deliver:
- lineage graph;
- deterministic morsel/segment identity;
- lost-segment detection;
- minimal lineage closure;
- recovery choice and metrics.

**Exit:** worker failure can recover selected missing data without restarting the entire job.

---

## Phase 7 — Expected-Cost Materialization

**Goal:** replace threshold-only persistence with interpretable expected-cost decisions.

Inputs include measured production cost, source reread cost, storage throughput/cost, consumer count, failure probability, and deadline slack.

**Exit:** traceable cost-aware policy wins on a suite spanning different failure/storage regimes.

---

## Phase 8 — Runtime Repartitioning and Skew Adaptation

**Goal:** partition topology can change while execution proceeds.

Features:
- sample distribution;
- split oversized partitions;
- coalesce tiny partitions;
- change downstream task count;
- skew-key strategies where legal;
- downstream progress without unnecessary full barriers.

**Exit:** severe-skew workloads avoid single-partition tail domination with controlled overhead on normal workloads.

---

## Phase 9 — Adaptive Join and Aggregation Algorithms

**Goal:** runtime can change physical operator strategy using observed data.

Examples:
- broadcast vs repartition;
- hash vs sort-merge;
- in-memory vs grace hash;
- build-side switching;
- adaptive partial aggregation.

---

## Phase 10 — Morsel Scheduler, Work Stealing, and Elasticity

**Goal:** move beyond coarse fixed tasks.

Features:
- morsel leases;
- work stealing;
- locality/critical-path scoring;
- add/remove workers;
- graceful decommission;
- spot hooks;
- heterogeneous worker awareness.

---

## Phase 11 — SQL/DataFrame Frontend and Stable Physical IR

**Goal:** become useful beyond hand-authored graphs without surrendering runtime ownership.

Likely approach:
- ADE owns physical IR;
- DataFusion and/or Substrait can feed plans;
- Rust and Python DataFrame APIs build toward that IR.

---

## Phase 12 — Batch Reliability and Operations

Features:
- durable job metadata;
- coordinator recovery/HA design;
- retries/backoff;
- output commit protocol;
- cancellation;
- quotas;
- job history;
- fault-injection suite.

---

## Phase 13 — Cloud-Native / Disaggregated Execution

Features:
- remote shuffle plugin;
- object-store-native shuffle;
- ephemeral workers;
- independent compute/storage scaling;
- cross-AZ/egress awareness;
- optional serverless experiments.

---

## Phase 14 — Unbounded Sources and Progress

Features:
- Kafka first;
- offsets;
- logical timestamps;
- watermarks/frontiers;
- timers;
- long-lived backpressure.

---

## Phase 15 — Keyed State and Checkpointing

Features:
- keyed/window state;
- tiered state;
- consistent checkpoints;
- restore;
- state redistribution on rescale.

---

## Phase 16 — Adaptive Checkpoint and State Placement

**Goal:** apply the original thesis to streaming state.

Decide:
- checkpoint cadence;
- incremental/full snapshot;
- local/remote placement;
- replication;
- replay versus restore.

---

## Phase 17 — Exactly-Once and Savepoint-Class Operations

Features:
- exactly-once-capable sinks;
- transactional commit integration;
- savepoints;
- pause/resume;
- upgrade;
- rescale;
- state migration hooks.

---

## Phase 18 — Unified Bounded/Unbounded Optimizer

**Goal:** remove remaining planner bifurcation and support historical + live pipelines under one logical representation.

---

## Phase 19 — Incremental Computation / Maintained Dataflows

Add:
- changelog/delta collections;
- differential operators;
- reusable arrangements;
- maintained views;
- incremental recomputation.

Optimizer choice expands to:

```text
restore vs full recompute vs incremental update
```

---

## Phase 20 — Iterative and Cyclic Dataflow

Features:
- cycles in physical graph;
- loop progress;
- convergence;
- graph/iterative ML primitives.

---

## Phase 21 — Connector and Catalog Ecosystem

Priorities:
- object stores/Parquet;
- Iceberg;
- Delta;
- Kafka;
- JDBC/Postgres;
- Arrow/Flight;
- generic source/sink SDK.

---

## Phase 22 — Multi-Language and UDF Platform

Features:
- Python client;
- Arrow batch UDF protocol;
- SQL gateway/Flight SQL;
- JVM client;
- optional WASM isolation;
- language-neutral plans.

---

## Phase 23 — Multi-Tenant Resource Manager

Features:
- queues;
- priorities;
- quotas;
- fairness;
- admission control;
- preemption;
- per-job cost;
- autoscaling.

---

## Phase 24 — Specialized Libraries / Extension Ecosystem

Possible external libraries:
- graph;
- ML data prep / iterative algorithms;
- batch inference;
- CEP;
- geospatial;
- vector/embedding transforms.

---

## Phase 25 — Heterogeneous Hardware and Advanced Code Generation

Features, only when measured:
- SIMD specialization;
- NUMA-aware scheduling;
- ARM/x86 economics;
- optional GPU operators;
- JIT/fusion.

---

## Phase 26 — Cross-Job Adaptive Shared Materialization

Generalize temporary intermediate state into a lineage-addressed computation cache with:
- snapshot/catalog validation;
- expected-value eviction;
- security boundaries;
- cross-job reuse;
- optional arrangements/indexes.

This is a natural long-term extension of the original thesis: materialized computation becomes an autonomously managed asset.

---

## 17. Phase Dependency Spine

The core dependency is intentionally simple:

```text
0 benchmark substrate
 -> 1 local execution
 -> 2 bounded memory/spill
 -> 3 distribution
 -> 4 unified exchange
 -> 5 adaptive exchange
 -> 6 lineage/recovery
 -> 7 cost-aware persistence
 -> 8 adaptive partitioning
```

Phases 0–8 are the **research core**. If the project reaches Phase 8 with strong results, it is already a meaningful engine/research platform even before Spark/Flink-scale breadth exists.

Later platform breadth should not be allowed to bypass this spine.

---

## 18. First Publication-Sized Research Target

### Working title

**Adaptive Materialization for Distributed Dataflow**

### Prototype scope

- Rust;
- bounded batch;
- hash repartition;
- aggregation and join;
- segment-level exchange;
- RAM/local NVMe/object storage;
- lineage recomputation;
- one coordinator;
- static worker cluster;
- heuristic and cost-aware policies.

### Compare against

1. always pipeline;
2. always local materialize;
3. always remote durable materialize;
4. pressure-only spill;
5. ADE expected-cost policy.

### Claim to test

> A runtime that adaptively chooses retention and recomputation at segment granularity can produce a better latency/cost/recovery frontier than any single static exchange policy across heterogeneous workloads.

The claim is intentionally falsifiable.

---

## 19. Success Metrics

### Foundation success

- understandable by a small development team;
- deterministic/reproducible benchmark artifacts;
- bounded local memory;
- reliable small-cluster execution.

### Research-core success

- different segments in one job use different retention strategies;
- adaptive policy beats static strategies across a mixed workload suite, not only a hand-tuned case;
- recovery chooses between restore and recompute based on measured/estimated economics;
- severe skew can be corrected during execution.

### Batch-platform success

- credible TPC-H/TPC-DS-derived cost-performance;
- elastic workers;
- robust retries/output commit;
- useful SQL/DataFrame APIs;
- operational observability.

### Streaming success

- event time;
- state;
- checkpoints/recovery;
- exactly-once mode;
- rescaling/savepoints;
- adaptive checkpoint/state placement.

### General-platform success

- one foundation supports batch, streaming, incremental, and iterative execution;
- stable extension APIs;
- broad connectors/languages;
- shared-cluster operation.

---

## 20. Major Risks and Mitigations

### 20.1 Control-plane explosion

Fine-grained segment tracking can create unacceptable metadata volume.

**Mitigate with:** extents/ranges, worker-local safe decisions, batched updates, adaptive metadata granularity.

### 20.2 Adaptation overhead and oscillation

Continuous optimization can consume CPU or thrash.

**Mitigate with:** hysteresis, minimum decision intervals, confidence thresholds, safe adaptation points, static-control comparisons.

### 20.3 Incorrect cost model

Cloud performance is noisy and prices are only one part of effective cost.

**Mitigate with:** online calibration, conservative heuristics, observed-versus-estimated traces, explicit confidence, simple fallbacks.

### 20.4 Scope explosion

Spark/Flink breadth represents years of engineering.

**Mitigate with:** protect Phases 0–8; treat later breadth as extension milestones; reject features that do not validate or depend on the core.

### 20.5 Reusing too much

ADE could become a scheduler wrapper around another engine.

**Mitigate with:** own physical IR, runtime, exchange, memory semantics, distributed scheduler, lineage, recovery, and adaptive policy.

### 20.6 Reimplementing too much

The project could spend years on parsers, codecs, and table formats before testing the thesis.

**Mitigate with:** aggressively reuse Arrow, Parquet, object-store clients, planners/frontends, and open table formats.

### 20.7 Streaming requirements distort early batch design

**Mitigate with:** reserve boundedness/progress identity in architecture, but defer state/checkpoint implementation until the batch substrate is credible.

---

## 21. Open Research Questions

Track these explicitly in `docs/research/` as experiments mature:

1. What is the best adaptive unit: batch, segment, partition, morsel, or region?
2. How small can morsels/segments become before scheduler/control overhead dominates?
3. Which materialization decisions are safely worker-local?
4. What minimum lineage metadata enables partial recovery?
5. When is remote object storage cheaper than local spill plus recomputation risk?
6. How should worker failure probability be modeled?
7. How should deadline slack alter durability?
8. Can consumers progress while partition topology is still changing?
9. How are deterministic identities preserved across partition split/coalesce?
10. When can operator algorithms switch after partial execution?
11. When should intermediates outlive a job for reuse?
12. Can streaming checkpoint policy share one model with batch intermediate materialization?
13. Which Arrow encodings should be preserved versus normalized?
14. When does a custom data transport justify itself over existing standards?
15. When does JIT/fusion outperform vectorized interpretation enough to justify complexity?
16. Which workloads should remain on one machine?
17. How should spot/heterogeneous workers be priced into recovery decisions?
18. Can incremental maintenance become a recovery choice rather than a separate mode?

---

## 22. Scope Boundary: Core vs Ecosystem

### Core must eventually own

- physical IR;
- local execution runtime;
- memory accounting;
- spill;
- exchange;
- distributed scheduling;
- adaptive policy;
- lineage/recovery;
- runtime statistics;
- progress/state integration;
- checkpoint correctness;
- cost model;
- distributed observability.

### Ecosystem modules may own

- SQL frontend;
- language clients;
- connectors;
- table formats/catalogs;
- UDF runtimes;
- graph/ML/CEP libraries;
- deployment integrations;
- UI.

This separation is necessary to obtain Spark/Flink-like breadth without turning the core into a monolith.

---

## 23. Near-Term Stop Point That Is Already Valuable

Phase 5 alone should produce a serious project:

```text
Parquet
  |
distributed scan
  |
partial aggregate
  |
ADAPTIVE HASH EXCHANGE
  |-- stream ordinary segments
  |-- buffer bursty segments
  |-- spill slow/skewed segments
  `-- persist expensive/high-risk segments
  |
final aggregate
  |
output
```

Even without SQL breadth, streaming, or HA, this is:

- a real distributed system;
- a substantial Rust engine;
- a research platform;
- a benchmarkable alternative to fixed exchange semantics.

Phases 6–8 determine whether the deeper thesis holds.

---

## 24. Development Rules

1. Every optimization must be disable-able.
2. Every adaptive decision must be observable.
3. Every adaptive policy must have a static baseline.
4. Correctness must not depend on timing.
5. Fault-injection belongs in feature acceptance.
6. Prefer measured policy to configuration knobs where possible.
7. Configuration should express constraints/objectives more often than mechanisms.
8. Do not add a distributed abstraction until its local mechanism is understood.
9. Measure before introducing unsafe Rust or custom low-level protocols.
10. Preserve open interoperability at boundaries.
11. Do not widen phase scope merely because a dependency makes a feature easy to expose.
12. Every phase ends with an implementation report and explicit deferred work.

---

## 25. Research and System References

The product direction was informed by the following systems and literature. These are reference points, not dependencies unless separately adopted by ADR.

- Apache Spark: https://spark.apache.org/docs/latest/
- Spark RDD Programming Guide: https://spark.apache.org/docs/latest/rdd-programming-guide.html
- Apache Flink: https://flink.apache.org/
- Flink Hybrid Shuffle: https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/ops/batch/batch_shuffle/
- Apache DataFusion: https://datafusion.apache.org/
- Ballista shuffle design: https://datafusion.apache.org/ballista/contributors-guide/shuffle.html
- DataFusion Distributed: https://github.com/datafusion-contrib/datafusion-distributed
- Arroyo: https://github.com/ArroyoSystems/arroyo
- RisingWave: https://risingwave.com/
- Trino fault-tolerant execution: https://trino.io/docs/current/admin/fault-tolerant-execution.html
- Ray Data internals: https://docs.ray.io/en/latest/data/data-internals.html
- Dask Distributed: https://distributed.dask.org/
- Velox memory management: https://facebookincubator.github.io/velox/develop/memory.html
- Polars streaming: https://docs.pola.rs/user-guide/concepts/streaming/
- Apache Beam basics: https://beam.apache.org/documentation/basics/
- Naiad: https://www.microsoft.com/en-us/research/publication/naiad-a-timely-dataflow-system-2/
- Differential Dataflow: https://www.microsoft.com/en-us/research/publication/differential-dataflow/
- DBSP/Feldera: https://docs.feldera.com/
- RDD paper: https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia
- Eddies: https://db.cs.berkeley.edu/papers/sigmod00-eddy.pdf
- Pocket: https://www.usenix.org/conference/osdi18/presentation/klimovic
- Exoshuffle: https://arxiv.org/abs/2203.05072
- Drizzle: https://dblp.org/rec/conf/sosp/VenkataramanPOA17.html
- Deca: https://arxiv.org/abs/1602.01959
- Quincy: https://www.microsoft.com/en-us/research/?p=157807
- Apache Arrow Flight: https://arrow.apache.org/docs/format/Flight.html

---

## 26. Final Product Direction

ADE should become broad **by growing outward from a small number of unusually strong primitives**, not by starting with a giant surface area.

The critical sequence is:

1. establish trustworthy measurement;
2. own a small local runtime;
3. make memory bounded;
4. distribute one simple graph;
5. unify exchange mechanisms;
6. make exchange adaptive;
7. make recomputation an explicit alternative to persistence;
8. add an interpretable cost model and dynamic topology;
9. only then broaden into SQL, elasticity, streaming, state, incremental computation, and ecosystem features.

The project should continuously protect the original thesis:

> **Materialization is not a permanent property of the processing model. It is a runtime economic and correctness decision over recoverable computation.**
