# Adaptive Dataflow Engine (ADE) — Phase 5 Detailed Implementation Plan
## Adaptive Exchange v1

**Status:** Implemented — see `docs/reports/phase-5-implementation-report.md`
**Date:** 2026-10-16
**Repository:** `jasonkli21/adaptive-dataflow-engine`
**Repository baseline reviewed:** `56ac1daa22ac56f8d64bcef934e3bd0bc3672740` (`Address Phase 4 review findings`)
**Depends on:** `docs/product/prd.md`, `docs/architecture/architecture.md`, `docs/plans/phase-4-implementation-plan.md`, `docs/reports/phase-4-implementation-report.md`, ADR-0010, ADR-0011, `AGENTS.md`, and the current Phase 4 implementation
**Primary objective:** introduce the first deterministic adaptive exchange policy so one distributed job can choose different retention mechanisms for different partitions/segment ranges based on live observations, while preserving Phase 4 correctness, boundedness, static baselines, and the no-recovery boundary.

---

# 1. Phase 5 Objective

Phase 4 established one trustworthy hash-exchange contract with five statically selected mechanisms:

```text
direct
buffered
spill
local_materialized
object_store
```

and proved that the same logical distribution produces the same result under each mechanism.

Phase 5 is the first phase where **policy may choose among those mechanisms while the job is running**.

The PRD defines Phase 5 as:

> Runtime chooses among streaming, buffering, spill, and persistence using deterministic heuristics.

Relevant intended inputs include:

```text
buffer occupancy
consumer lag
memory pressure
intermediate size
estimated lineage cost
worker health
```

The PRD exit condition is:

> One job can use different mechanisms for different partitions/segments and outperform at least one static policy on a defined adversarial workload without unacceptable normal-case regression.

This phase should satisfy that outcome without pulling Phase 6 lineage/recovery or Phase 7 expected-cost materialization forward.

A successful Phase 5 implementation should make this possible:

```text
partition A:
  early segments -> direct
  short lag      -> buffered
  sustained lag  -> spill

partition B:
  early segments -> direct
  growing output -> buffered
  large/sustained backlog -> object-store persistence

partition C:
  stays direct for the whole exchange
```

while preserving the same hash distribution, transfer/segment sequence rules, final rows, and failure semantics.

---

# 2. Source-of-Truth Order

Use repository precedence:

1. `docs/product/prd.md` — Phase 5 outcome and long-term scope;
2. `docs/architecture/architecture.md` — long-lived invariants and boundaries;
3. this Phase 5 plan — detailed Phase 5 implementation contract;
4. accepted ADRs — concrete design decisions/refinements;
5. implementation reports and current code — observed implementation state.

Do not blindly force this plan onto the repository if the baseline moved. Reconcile it against the actual current code first.

Important long-lived invariants:

- correctness before adaptation;
- mechanism and policy remain separate;
- distribution semantics remain independent from retention/transport;
- adaptive actions are observable and explainable;
- static baselines remain available and independently selectable;
- queues, retained data, threads, and control metadata remain bounded;
- cancellation/deadline behavior remains explicit;
- local storage remains lossy unless a stronger backend capability is explicitly provided;
- object presence is **not** recovery;
- the local task memory ledger is **not** a worker/process RSS limit;
- no policy may silently change query semantics.

---

# 3. Baseline Repository State

Phase 5 begins from the remediated Phase 4 baseline at `56ac1daa...`, not the original Phase 4 implementation commit.

The current system has these important properties.

## 3.1 Workspace and dependency direction

The core workspace remains:

```text
ade-bench
   |
   v
ade-distributed
   |
   v
ade-exec
```

Do not introduce a speculative frontend/common/runtime crate split just to host the Phase 5 policy.

A small internal policy module inside `ade-distributed` is preferred unless a genuinely cleaner one-way dependency emerges.

## 3.2 Distributed execution

The current distributed graph remains:

```text
scan
  -> partial aggregate
  -> hash exchange
  -> final aggregate
  -> gather
```

with:

- one coordinator;
- N static workers;
- fixed worker membership;
- static hash partitioning;
- one prepared job per worker;
- at most one producer and one final role per worker;
- consumer-first producer/final overlap;
- whole-job failure on required worker/control/data-plane failure;
- no retry/recovery.

## 3.3 Phase 4 exchange contract

The current implementation provides:

- `ExchangeMode::{Direct, Buffered, Spill, LocalMaterialized, ObjectStoreMaterialized}`;
- strict `ExchangeConfig`;
- transfer identity and version validation;
- per-transfer monotonically increasing segment sequence;
- explicit terminal segment count;
- bounded one-batch-per-segment IPC frames;
- no global cross-producer order;
- common `Sender` producer behavior;
- a common `BatchSource` path into the existing spillable merge aggregate;
- destination-side retention in `retention.rs`;
- fixed Phase 4 mode selected for the job/session.

Do not fork the logical exchange protocol into an “adaptive protocol.” Adaptive placement belongs behind the same validated transfer/segment boundary.

## 3.4 Current retention facts

Current Phase 4 retention includes:

- direct rendezvous;
- bounded destination-side in-memory FIFO;
- local spill with bounded in-memory payload and a constant-sized spool metadata model;
- local finalized materialization;
- provider-neutral object-store materialization;
- live current/peak buffer bytes;
- current spill backlog bytes;
- spill/local/object read/write counters;
- producer and consumer wait observations;
- object-store backend/durability metadata.

The remediation also ensures:

- spill file I/O is not performed while holding the shared queue mutex;
- spill and object-store operations have cancellation/deadline containment;
- stuck non-preemptible storage work poisons the worker;
- poisoned workers reject later `PrepareJob`;
- object-store backends can be injected by worker callers;
- the default object-store fixture is still an execution-scoped ephemeral local filesystem backend.

These contracts must survive adaptive selection.

## 3.5 Current limitations that remain intentional

Phase 5 starts with no:

- lineage graph;
- recomputation;
- worker replacement;
- retry;
- coordinator HA;
- remote shuffle service;
- disk quota manager;
- process-wide RSS memory manager;
- dynamic partition count;
- skew split/coalesce;
- joins;
- SQL/DataFrame frontend;
- streaming/checkpoint/state runtime.

Do not “solve” missing Phase 6+ capabilities as part of adaptive exchange.

---

# 4. Phase 5 Design Boundary

Phase 5 should introduce **policy**, not a second exchange implementation.

Recommended conceptual decomposition:

```text
validated segment arrives
        |
        v
+----------------------+
| Observation Snapshot |
+----------------------+
        |
        v
+----------------------+
| Adaptive Policy v1   |  <-- pure/deterministic decision logic
+----------------------+
        |
        v
+----------------------+
| Retention Mechanisms |  <-- Phase 4 mechanisms/correctness
+----------------------+
        |
        v
 common ExchangeReader / BatchSource
```

Policy decides a valid retention action. Mechanisms remain responsible for:

- exact storage/transport semantics;
- capacity enforcement;
- sequence/completion validation;
- cancellation;
- storage I/O;
- cleanup;
- reader correctness.

The policy must not:

- parse network frames;
- own Arrow decoding;
- mutate hash routing;
- bypass sequence validation;
- implement recovery;
- directly manipulate spill files/object keys.

---

# 5. Recommended Phase 5 Policy Model

## 5.1 Preserve all static modes

Existing static modes remain first-class:

```text
exchange_mode=direct
exchange_mode=buffered
exchange_mode=spill
exchange_mode=local_materialized
exchange_mode=object_store
```

Add a new explicit mode:

```text
exchange_mode=adaptive
```

Do **not** make adaptive the default in Phase 5. Preserve the existing compatibility baseline unless a later product decision explicitly changes it.

Static mode behavior should remain behaviorally identical except for narrowly necessary refactoring and additional observability.

## 5.2 Deterministic threshold policy, not learned or expected-cost policy

Phase 5 policy should be interpretable and deterministic.

Recommended initial policy family:

```text
threshold_v1
```

Do not add:

- ML;
- stochastic selection;
- cloud price optimization;
- expected recovery cost;
- failure probability models;
- lineage-based recompute estimates.

Those belong later.

## 5.3 Use monotone retention escalation

The safest Phase 5 v1 transition model is a **monotone escalation path**:

```text
direct
  -> buffered
      -> spill
          -> object_store
```

`local_materialized` remains a static compatibility mechanism; it does not need to participate in the adaptive chain.

Why monotone escalation:

1. it allows real runtime switching at segment boundaries;
2. it avoids oscillation and tier thrashing;
3. it keeps decision metadata small;
4. it preserves a simple reader lifecycle;
5. it leaves more complex de-escalation/replication for later phases;
6. it naturally implements hysteresis: once a partition escalates, it does not immediately downgrade.

A policy may skip tiers, for example:

```text
buffered -> object_store
```

if configured thresholds demand it.

The exact implementation may choose a different bounded monotone representation if it is demonstrably simpler, but arbitrary bidirectional switching is not required and should not be introduced casually.

## 5.4 Decision granularity

The minimum acceptable adaptive granularity is **per destination partition/session with segment-boundary transitions**.

That means:

- different destination partitions may be in different tiers simultaneously;
- a partition may use one tier for an early segment range and a later tier for subsequent segments;
- each transition becomes effective only between complete validated segments;
- an individual encoded segment is never split across two mechanisms.

This satisfies the PRD requirement that one job can use different mechanisms for different partitions/segments without requiring one coordinator metadata record per segment.

Per-transfer policy state is allowed if the current implementation makes it substantially cleaner, but it must retain bounded metadata and preserve unspecified cross-producer ordering.

## 5.5 Bounded extent-style decision metadata

Do not retain an unbounded in-memory `Vec<DecisionRecord>` for every segment.

Represent placement history as compact ranges/extents.

Conceptually:

```rust
enum AdaptiveTier {
    Direct,
    Buffered,
    Spill,
    ObjectStore,
}

struct TierExtent {
    tier: AdaptiveTier,
    first_segment_ordinal: u64,
    end_exclusive: Option<u64>,
}
```

Exact identity can use existing transfer/partition sequence information rather than a new global segment ID.

For monotone v1 policy, a partition should have only a small fixed number of tier extents.

Required properties:

- decision metadata is O(number of allowed transitions), not O(number of segments);
- transition boundaries are observable;
- no segment can disappear between extents;
- no segment can be consumed twice;
- current tier can be reconstructed from compact state;
- completed job metrics can summarize counts/bytes per selected tier.

Do not introduce Phase 6 lineage identity.

---

# 6. Observation Model

Phase 5 needs a small explicit observation structure rather than letting policy reach into arbitrary mutex-protected mechanism internals.

Conceptual shape:

```rust
struct AdaptiveObservation {
    incoming_segment_bytes: usize,

    buffer_bytes: usize,
    buffer_capacity_bytes: usize,
    spill_backlog_bytes: usize,

    producer_wait_ns: u64,
    consumer_wait_ns: u64,

    produced_bytes: u64,
    produced_segments: u64,
    consumed_segments: u64,

    consumer_ready: bool,

    // Truthful v1 signals only; not process RSS.
    exchange_memory_pressure: PressureLevel,

    // Optional/configured hint, not lineage.
    production_cost_hint: Option<u64>,

    worker_health: WorkerHealth,
}
```

The exact struct may differ.

## 6.1 Required Phase 5 observations

At minimum, policy should be able to reason about:

- incoming segment size;
- current exchange buffer occupancy;
- configured exchange buffer capacity;
- current spill backlog;
- whether a consumer is immediately ready / observable consumer lag;
- cumulative producer wait;
- cumulative consumer wait;
- bytes/segments already produced for the adaptive partition;
- current tier.

## 6.2 Memory pressure must be truthful

The current `memory_limit_bytes` is a per-local-task retained-state budget and can be active twice per worker because producer/final tasks overlap.

It is **not** process RSS.

Therefore Phase 5 may use:

```text
exchange buffer occupancy / adaptive exchange capacity
```

as an exchange-memory-pressure signal.

Do not name that signal `worker_memory_pressure` unless the implementation actually introduces a truthful worker-wide memory scope.

A future phase can replace or augment it with process/job-level memory arbitration.

## 6.3 Consumer lag

Use a bounded, cheap signal.

Acceptable v1 signals include:

- buffered-but-unconsumed bytes;
- producer wait caused by consumer readiness/capacity;
- consumer-ready flag;
- segment production minus consumption progress;
- bounded EWMA of wait/lag if implemented without per-segment history.

Do not require a coordinator round trip for every segment.

## 6.4 Intermediate size

Use observed produced bytes/segments, not speculative whole-job cardinality.

Thresholds should act on measured current output.

## 6.5 Estimated lineage cost

Phase 6 owns real lineage.

Phase 5 must not invent a fake lineage graph merely to satisfy the PRD input list.

Allowed:

- an optional explicit production-cost/replay-cost hint;
- `Unknown` / absent lineage estimate;
- record that the threshold policy did not use a lineage signal.

Not allowed:

- claiming a segment is recomputable when no Phase 6 lineage descriptor exists;
- using object presence as a recovery substitute.

## 6.6 Worker health

Phase 5 may expose a narrowly truthful signal such as:

```text
healthy
at_risk (only when explicitly supplied)
poisoned
```

A poisoned worker still fails the job under the existing contract.

Do not add worker replacement or recovery.

If no meaningful nonfatal health signal exists in the current runtime, record `healthy` and keep health-driven transitions disabled rather than inventing unreliable health inference.

---

# 7. Adaptive Policy Configuration

Extend exchange configuration narrowly.

Conceptual shape:

```rust
enum ExchangeMode {
    Direct,
    Buffered,
    Spill,
    LocalMaterialized,
    ObjectStoreMaterialized,
    Adaptive,
}

struct AdaptivePolicyConfig {
    policy: AdaptivePolicyKind,       // ThresholdV1
    buffer_high_watermark_pct: u8,
    spill_backlog_remote_bytes: usize,
    remote_persist_after_bytes: u64,
    min_segments_before_remote: u64,
}
```

Exact fields are implementation-owned.

Requirements:

- strict validation;
- deterministic serialization through the existing bounded control protocol;
- finite upper bounds;
- no duplicate/unknown option acceptance;
- effective configuration recorded by benchmark artifacts;
- defaults must be conservative and documented;
- settings irrelevant to static modes must either be clearly rejected or reported as ignored by a documented rule;
- object-store adaptive persistence must continue to validate backend identity/capability.

Avoid a large bag of tuning knobs. Phase 5 should have only the thresholds needed to falsify the research idea.

Recommended benchmark-facing options:

```text
exchange_mode=adaptive
adaptive_policy=threshold_v1
adaptive_buffer_high_pct=...
adaptive_remote_after_bytes=...
```

Only add more if the implementation truly requires them.

---

# 8. Adaptive Action Semantics

## 8.1 Direct

Use direct/rendezvous when:

- the consumer is ready;
- there is no retained backlog that must be preserved ahead of the incoming segment;
- current policy tier has not escalated past direct.

Direct retains the Phase 4 bounded handoff behavior.

## 8.2 Buffered

Escalate from direct to buffered when a segment would otherwise incur meaningful producer blocking because the consumer is not ready.

Buffered mode must retain:

- byte capacity enforcement;
- producer backpressure at capacity;
- cancellation wakeups;
- exact accounting.

Adaptive mode should not silently enlarge configured buffer capacity.

## 8.3 Spill

Escalate to spill when bounded memory pressure or sustained lag exceeds configured thresholds.

Examples of acceptable deterministic conditions:

```text
buffer occupancy >= high watermark
OR
next segment cannot fit without exceeding high watermark
OR
producer wait exceeds configured threshold after minimum observations
```

Once spill is selected for a partition in Phase 5 v1, do not downgrade to buffered/direct.

Reuse Phase 4 spill containment and poisoning behavior.

## 8.4 Object-store persistence

Escalate to object-store persistence only through a clearly configured deterministic rule, for example:

```text
spill backlog exceeds threshold
OR
observed intermediate bytes exceed persistence threshold
OR
explicit at-risk worker hint is active
```

Object-store persistence remains **retention only**.

Required statements remain:

- backend capability is caller-supplied;
- default local filesystem backend is ephemeral;
- object presence does not permit retry/recovery;
- worker failure still fails the job.

## 8.5 Local materialization

Keep `local_materialized` as a static Phase 3/4 compatibility baseline.

It need not be part of `threshold_v1`.

Adding it to the adaptive chain is allowed only if it is materially simpler than not doing so and the policy/reader semantics remain clear. Do not add it merely to make all enum values appear symmetric.

---

# 9. Reader and Ordering Contract

Adaptive retention must preserve the Phase 4 ordering contract:

- per-producer segment sequence is strict;
- duplicates/gaps fail;
- terminal segment count must match;
- empty transfers remain valid;
- no global cross-producer order is promised.

The adaptive reader may consume from different retention tiers, but it must behave as one logical `BatchSource`.

Required invariants:

```text
each accepted segment becomes visible exactly once
no segment is visible before its integrity/identity checks pass
no tier transition can reorder segments within one producer transfer
completion is not reported until all required transfer terminals are valid
cancellation wakes blocked readers/writers across every tier
```

Do not implement a second final aggregation path for adaptive mode.

Reuse the existing merge-source/sink path.

---

# 10. Policy Evaluation Location

Prefer **worker-local partition decisions** for Phase 5 v1.

Reasons:

- current observations already exist at the destination retention boundary;
- one coordinator decision per segment would violate bounded-control intent;
- direct/buffer/spill decisions are latency-sensitive local mechanism decisions;
- the coordinator does not yet maintain the long-term materialization catalog;
- global cost/scheduler coordination belongs later.

Coordinator responsibilities in Phase 5 should remain roughly:

- distribute validated adaptive policy config;
- start/cancel tasks;
- collect final metrics/decision summaries;
- verify conservation and completion;
- benchmark/report effective behavior.

If a small coordinator aggregate is useful for final reporting, keep it post-hoc rather than on the hot path.

---

# 11. Policy/Mechanism Interface

Add the narrowest internal boundary that makes policy independently testable.

Conceptual API:

```rust
trait ExchangePolicy {
    fn decide(
        &mut self,
        current: AdaptiveTier,
        observation: &AdaptiveObservation,
    ) -> AdaptiveDecision;
}

struct AdaptiveDecision {
    tier: AdaptiveTier,
    reason: AdaptiveReason,
}
```

Requirements:

- pure/deterministic for the same prior policy state + observation sequence;
- no file/network I/O;
- no global state;
- no direct storage access;
- no Arrow ownership;
- no unbounded history.

The mechanism layer applies the decision and returns structured failure if the action cannot be executed.

Static modes should bypass adaptive policy completely.

---

# 12. Decision Records and Observability

Architecture requires adaptive decisions to be explainable.

Every actual transition should emit a compact record containing at least:

```text
job/exchange/partition identity
transition ordinal
old tier
new tier
segment boundary
observation snapshot / normalized relevant values
reason enum
policy version
```

Do not record every non-transitioning segment as a permanent record unless metrics are aggregated.

Recommended reason enum:

```text
ConsumerNotReady
BufferHighWatermark
ProducerWait
SpillBacklog
IntermediateSize
WorkerAtRisk
ConfiguredStart
```

Exact names may differ.

Metrics should include:

```text
adaptive_policy
adaptive_partitions

direct_segments / bytes
buffered_segments / bytes
spill_segments / bytes
object_segments / bytes

direct_to_buffered transitions
buffered_to_spill transitions
spill_to_object transitions
other allowed monotone transitions

transition reason counts
max extents/partition
decision evaluation count
```

Also preserve all Phase 4 mechanism metrics and conservation checks.

A completed adaptive job should allow a reviewer to answer:

1. Which partitions transitioned?
2. At which segment boundaries?
3. Why?
4. How many bytes/segments used each tier?
5. Did any static mechanism unexpectedly run outside its declared adaptive decisions?
6. Were all current buffer/spill backlog counters zero at successful completion?

---

# 13. Boundedness and Resource Discipline

Phase 5 must not introduce unbounded adaptive metadata.

Forbidden:

- `Vec<DecisionRecord>` growing with every segment;
- one thread/task per decision;
- one coordinator message per segment;
- unbounded channels between policy and retention;
- retaining decoded batches merely for policy;
- retaining all observations for later analysis in memory;
- storing one heap object per spilled segment indefinitely.

Preferred:

- current observation snapshot;
- cumulative counters;
- constant/fixed transition state;
- compact extents;
- bounded queues already owned by mechanism layer.

The Phase 4 I/O containment contract remains mandatory.

Policy evaluation must happen outside expensive I/O critical sections and should not hold retention locks across object-store/filesystem operations.

---

# 14. Failure and Cancellation Semantics

Phase 5 does not change fault tolerance.

Any required producer/final/control/data-plane failure still fails the job.

Adaptive policy must not:

- retry a failed segment;
- replay from object storage;
- restart a producer;
- select another worker;
- infer a valid backup from a retained copy.

Transition failures:

- failure while buffering/spilling/persisting is a job failure;
- cancellation must unblock adaptive writers/readers;
- non-preemptible storage timeout continues to poison the worker as in ADR-0011;
- worker poisoning is not an adaptive action;
- cleanup must remove execution-scoped retained state according to existing rules.

---

# 15. Benchmark and Adversarial Evaluation

Phase 5 needs a defined experiment capable of falsifying the policy.

## 15.1 Preserve static baselines

Benchmark the same workload/configuration under selected static modes:

```text
direct
buffered
spill
object_store
adaptive
```

`local_materialized` remains useful as a compatibility baseline but does not need to participate in every adaptive experiment.

## 15.2 Add one deterministic slow-consumer/bursty-consumer pressure profile

The Phase 5 exit condition needs an adversarial workload where static choices expose a real tradeoff.

Add a **benchmark-only/debug pressure-injection hook** that can deterministically delay one final consumer after a known amount of progress.

Conceptual parameters:

```text
target_worker
delay_after_segments
delay_duration_ms
repeat_count (prefer 0 or a very small fixed count)
```

This hook:

- defaults off;
- is recorded in effective benchmark configuration;
- must not alter row values or routing;
- exists only to create reproducible consumer lag;
- must not be treated as a production scheduling feature.

Do not use OS/network shaping that makes the acceptance path platform-specific if a small explicit harness hook is clearer.

## 15.3 Required adversarial behavior

Construct one workload where:

- at least two destination partitions exist;
- one consumer is slowed;
- another remains normal;
- the normal partition stays at a lower tier;
- the slow partition escalates at least once;
- at least two retention tiers are used in one job;
- correctness remains identical to the oracle.

A stronger target is:

```text
normal partition: direct/buffered only
slow partition: buffered -> spill
```

or:

```text
slow partition: buffered -> spill -> object_store
```

if the object path is stable enough.

## 15.4 Performance claim

The required claim should be narrow:

> On the defined slow-consumer adversarial workload, adaptive mode is faster than at least one predeclared static policy while preserving correctness and boundedness.

Prefer predeclaring comparison policies before measuring.

Do not cherry-pick whichever static mode happened to lose after seeing results.

Recommended primary comparison:

- adaptive vs static direct under slow consumer, because direct should expose producer blocking;
- optionally adaptive vs static spill to show avoidance of unnecessary disk I/O on unaffected partitions.

Also report the best static result for context without requiring Phase 5 adaptive to beat the globally best static policy in every case.

## 15.5 Normal-case regression guard

Run the same data without injected delay.

Define a broad local acceptance guard before measuring, for example:

```text
adaptive median query time <= 1.25x a designated normal-case static baseline
```

Use enough isolated iterations to reduce noise (for example 1 warmup + 5 measured) but do not turn CI into a performance gate.

If local variability makes the guard unreliable, report distributions and treat the guard as release evidence rather than CI correctness.

The implementation report must clearly separate correctness gates from exploratory machine-local performance evidence.

---

# 16. Benchmark Adapter Changes

Extend `ade-bench` narrowly.

Required:

- parse `exchange_mode=adaptive`;
- parse the small Phase 5 policy config;
- expose effective policy config in result metadata;
- expose adaptive summary metrics;
- retain independent oracle validation;
- retain child-process isolation;
- retain result schema v1 unless a real artifact contract change requires a bump.

Avoid schema churn solely for debug details.

If adaptive decision detail is too rich for existing result metadata, store a compact summary in the result and emit detailed per-run diagnostics separately under ignored benchmark artifacts.

The slow-consumer injection configuration must be explicit and recorded.

---

# 17. Documentation / ADR Work

Phase 5 should add an ADR covering:

```text
adaptive policy boundary
threshold_v1 semantics
monotone escalation
decision granularity
bounded extent metadata
observation truthfulness
no-recovery boundary
```

Likely:

```text
docs/adr/0012-adaptive-exchange-v1.md
```

Also update:

- `AGENTS.md` active/completed boundary to Phase 5;
- `README.md` current adaptive exchange status and options;
- benchmark documentation for Phase 5 acceptance;
- `docs/reports/phase-5-implementation-report.md`.

Do not rewrite the original PRD/architecture merely to mirror code unless an actual product/architecture contradiction is discovered.

If implementation reveals a real architecture conflict, record it explicitly rather than silently editing around it.

---

# 18. Explicit Non-Goals

Phase 5 must **not** implement:

- lineage descriptors or lineage graph;
- recomputation;
- recovery from retained exchange;
- worker retry/replacement;
- coordinator HA;
- failure probability estimation;
- expected-cost materialization;
- cloud price optimization;
- dynamic partition split/coalesce;
- changing hash partition count;
- work stealing;
- elastic workers;
- peer replication;
- remote shuffle service;
- joins/adaptive joins;
- SQL/DataFrame frontend;
- streaming/event-time/state/checkpointing;
- exactly-once;
- cross-job materialization reuse;
- learned policy;
- arbitrary bidirectional tier oscillation;
- process-wide memory accounting unless deliberately scoped as a separate, justified prerequisite.

Do not pull Phase 6 or Phase 7 work forward as “needed for adaptivity.”

---

# 19. Suggested Implementation Sequence

The sequence is designed to keep debugging focused and avoid broad test loops.

## Step 1 — Baseline reconciliation

Before edits:

- confirm `main` baseline and record the exact SHA;
- read PRD Phase 5;
- read architecture sections on mechanism/policy, exchange, adaptive policy, statistics, bounded control metadata, and fault tolerance;
- read Phase 4 plan/report and ADR-0010/0011;
- inspect current `retention.rs`, `exchange.rs`, `worker.rs`, `coordinator.rs`, metrics, protocol, adapter, tests, and acceptance script;
- identify any code changes since `56ac1daa...`.

If the repository moved, update the implementation approach rather than forcing stale file names.

## Step 2 — Extract a behavior-preserving mechanism boundary if needed

Refactor only enough to let adaptive mode select among existing Phase 4 mechanisms.

At the end of this step:

- all five static modes behave exactly as before;
- no adaptive decision is required yet;
- existing focused five-mode tests pass.

This is the most important regression checkpoint.

## Step 3 — Add policy types/config + pure threshold policy

Implement:

- adaptive mode/config parsing;
- `AdaptiveObservation`;
- policy state;
- deterministic `threshold_v1`;
- compact transition/extent state;
- reason enum.

Test the policy as a pure unit independent of I/O.

Key cases:

- stays direct when consumer is ready;
- direct -> buffered;
- buffered -> spill;
- spill -> object;
- no downgrade;
- threshold equality boundaries;
- overflow-safe counters;
- same observation sequence => same decisions;
- bounded transition count.

## Step 4 — Add adaptive retention application

Wire policy decisions into the existing retention/session path.

Requirements:

- transition only at segment boundaries;
- one logical reader;
- exact once consumption;
- all Phase 4 sequence/completion checks preserved;
- no unbounded per-segment placement map;
- cancellation across mixed tiers;
- successful cleanup leaves current retained counters at zero.

Start with:

```text
direct -> buffered -> spill
```

before adding object-store escalation.

## Step 5 — Add object-store escalation

Reuse the Phase 4 injected backend and I/O containment.

Validate:

- backend identity/capability still matches config;
- no partial object visibility;
- mixed prior spill + later object segments are consumed exactly once;
- object operation timeout/poisoning semantics are unchanged;
- no recovery claim is introduced.

## Step 6 — Adaptive metrics and decision records

Add:

- tier bytes/segments;
- transition counts/reasons;
- compact transition detail;
- policy version/effective config;
- decision evaluation counters if useful.

Verify successful completion conservation and zero live retained backlog.

## Step 7 — Benchmark adapter + deterministic pressure injection

Add the smallest reproducible slow-consumer hook.

Test that:

- disabled path has no behavior effect;
- enabled delay is applied to exactly the targeted consumer;
- effective config records it;
- it does not alter result rows/hash routing.

## Step 8 — Process integration

Add one focused process-level adaptive integration case:

- real worker processes;
- at least two workers;
- deterministic slow consumer;
- one job uses more than one tier;
- partitions show different tier histories;
- oracle correctness;
- conservation metrics;
- cancellation/failure path.

Do not multiply this across many worker counts and thresholds.

## Step 9 — Release acceptance

Create `scripts/phase5-acceptance.sh` or equivalent.

Recommended compact matrix:

Normal case, 2 workers:
- designated static baseline;
- adaptive.

Adversarial slow-consumer case, 2 workers:
- direct;
- spill;
- adaptive;
- optionally object-store if needed for the chosen transition proof.

Limited 4-worker case:
- one adaptive adversarial run proving partition divergence at larger topology.

Use one deterministic dataset and fixed policy thresholds.

## Step 10 — Documentation and final gate

Update ADR/report/README/AGENTS.

Then run the final workspace checks once after focused tests are green.

---

# 20. Focused Test Strategy

Testing should falsify the design without creating a matrix explosion.

## 20.1 Pure policy tests

High-value cases:

- deterministic identical decisions for identical input sequence;
- exact threshold boundaries;
- monotone transitions;
- no impossible downgrade;
- no invalid tier;
- zero/maximum config validation;
- overflow-safe byte/wait counters;
- object escalation disabled when policy config disallows it;
- decision metadata stays within fixed bound.

## 20.2 Adaptive retention tests

Required:

- direct then buffer exact readback;
- buffer then spill exact readback;
- spill then object exact readback;
- empty transfer under adaptive mode;
- multi-producer transfer sequence correctness;
- duplicate/gap/terminal mismatch still fail;
- cancellation while adaptive direct writer waits;
- cancellation while buffer writer waits;
- cancellation during spill;
- cancellation/deadline during object put/get;
- poisoned worker semantics unchanged;
- cleanup after transition failure;
- successful current buffer/spill backlog = 0;
- no double-read or lost segment at each boundary.

Prefer table-driven sequence tests.

## 20.3 Static regression tests

Do not duplicate the full Phase 4 matrix.

Run enough existing focused coverage to prove:

- five static modes still function;
- static metrics remain truthful;
- static option parsing unchanged;
- existing process smoke still passes.

## 20.4 Process adaptive test

One high-value process test should verify:

- real child workers;
- independent oracle validation;
- actual partition divergence;
- actual tier transition;
- tier-specific metrics;
- failure/cancellation propagation.

## 20.5 Performance evidence

Keep machine-local performance out of normal CI pass/fail.

Release acceptance should record:

- all raw iterations;
- median and distribution;
- effective config;
- source/environment identity;
- adaptive transition evidence;
- static comparison results.

---

# 21. Acceptance Criteria

Phase 5 is complete only if all of the following are true.

## Correctness

- adaptive mode produces exactly the oracle result;
- hash routing/distribution semantics are unchanged;
- each accepted segment is consumed exactly once;
- sequence/completion validation remains strict;
- no adaptive action changes query semantics.

## Adaptive behavior

- at least one job uses more than one retention mechanism;
- at least two partitions/segment ranges can follow different retention histories;
- transitions are deterministic under the same observations/config;
- transition metadata is bounded and explainable.

## Resource discipline

- exchange memory remains explicitly bounded;
- adaptive metadata remains bounded independently of segment count;
- spill/object operations preserve Phase 4 cancellation/deadline containment;
- no unbounded threads/channels/queues are added;
- successful completion has no retained in-memory/spill backlog.

## Static compatibility

- all five Phase 4 static modes remain selectable;
- default compatibility behavior remains documented;
- static mode correctness regressions are absent.

## Fault boundary

- worker failure still fails the job;
- no retry/recovery/recompute is implemented or implied;
- object-store retention is not described as a recovery feature.

## Observability

- effective policy/config is recorded;
- transitions include reason/observation summaries;
- bytes/segments per adaptive tier are visible;
- conservation checks still hold.

## Research claim

- on one predeclared deterministic adversarial workload, adaptive mode beats at least one predeclared static policy;
- normal-case regression remains within the predeclared acceptable release bound or is explicitly documented as a failed Phase 5 exit criterion;
- no correctness or resource guarantee is weakened to obtain the result.

---

# 22. Implementation Report Requirements

Create:

```text
docs/reports/phase-5-implementation-report.md
```

Include:

1. baseline and final commit SHA;
2. repository/dependency changes;
3. exact adaptive policy/version;
4. configuration defaults and validation;
5. observation model;
6. transition model and decision granularity;
7. bounded metadata argument;
8. reader/order/correctness invariants;
9. static mode compatibility;
10. failure/cancellation behavior;
11. memory/boundedness truthfulness;
12. object-store durability statement;
13. ADRs added/updated;
14. focused tests and final workspace commands;
15. release acceptance setup;
16. raw/summary normal and adversarial evidence;
17. which partitions/tier ranges actually adapted;
18. limitations/deviations;
19. explicit Phase 6 handoff.

Do not claim a performance win without preserved raw evidence and independently validated results.

---

# 23. Phase 6 Handoff Boundary

Phase 5 should leave Phase 6 with:

- stable validated segment sequence/identity;
- adaptive placement history summarized as bounded extents;
- observed segment/partition production size;
- transition reasons;
- retention locations/capabilities as facts;
- producer/consumer wait metrics;
- static and adaptive policy baselines.

Phase 6 can then add:

- lineage descriptors;
- recomputability;
- lost-segment detection;
- minimal lineage closure;
- restore/recompute recovery choices.

Phase 5 must **not** pretend those exist.

---

# 24. Expected File Touches

Exact files are implementation-owned, but likely areas include:

```text
crates/ade-distributed/src/lib.rs
crates/ade-distributed/src/retention.rs
crates/ade-distributed/src/exchange.rs
crates/ade-distributed/src/metrics.rs
crates/ade-distributed/src/worker.rs
crates/ade-distributed/src/coordinator.rs
crates/ade-distributed/tests/...

crates/ade-bench/src/ade_distributed.rs
crates/ade-bench/src/...

docs/adr/0012-adaptive-exchange-v1.md
docs/reports/phase-5-implementation-report.md
AGENTS.md
README.md
scripts/phase5-acceptance.sh
```

A small `policy.rs`/`adaptive.rs` inside `ade-distributed` is reasonable if it creates a clean independent decision boundary.

Do not create new crates unless the dependency graph clearly improves.

---

# 25. Final Implementation Guidance

The purpose of Phase 5 is not to build the final optimizer.

It is to prove one narrow but foundational statement:

> Given trustworthy exchange mechanisms and live bounded observations, ADE can make deterministic runtime retention decisions at a finer granularity than the whole job, preserve correctness and resource bounds, explain those decisions, and win on at least one workload where a fixed policy is predictably wrong.

Prefer the smallest design that proves that statement and leaves Phase 6/7 clean.
