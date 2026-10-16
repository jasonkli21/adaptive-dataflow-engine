# Adaptive Dataflow Engine (ADE) — Phase 6 Detailed Implementation Plan
## Lineage and Recovery Planner v1

**Status:** Implemented; see ADR-0013 and Phase 6 implementation report\
**Date:** 2026-10-16\
**Repository:** `jasonkli21/adaptive-dataflow-engine`\
**Repository baseline reviewed:** `602cb3aab1a97360c15b6db61965a5b64c325e79` (`Address Phase 5 independent review findings`)\
**Primary references:** `docs/product/prd.md`, `docs/architecture/architecture.md`, `docs/plans/phase-5-implementation-plan.md`, `docs/reports/phase-5-implementation-report.md`, ADR-0010, ADR-0011, ADR-0012, `AGENTS.md`, and current code on `main`\
**Primary objective:** make recomputation an explicit recovery mechanism by introducing stable lineage/segment identity, truthful lost-segment detection, a minimal-lineage recovery planner, and one bounded worker-loss recovery path that restores only selected missing work rather than restarting the whole job.

---

# 1. Phase 6 Objective

The PRD defines Phase 6 as:

> **Lineage and Recovery Planner**
>
> Goal: recomputation becomes an explicit recovery mechanism.
>
> Deliver:
> - lineage graph;
> - deterministic morsel/segment identity;
> - lost-segment detection;
> - minimal lineage closure;
> - recovery choice and metrics.
>
> Exit: worker failure can recover selected missing data without restarting the entire job.

Phase 5 established a unified/adaptive exchange mechanism but intentionally retained whole-job failure semantics. Phase 6 changes that boundary for a deliberately narrow, well-defined class of worker loss.

A successful Phase 6 implementation should be able to demonstrate a flow like:

```text
original distributed aggregate job
        |
        +-- producer contributions for partition 0 survive
        |
        +-- producer contributions for partition 1 survive
        |
        +-- worker owning final partition 1 dies before committing final output
                        |
                        v
              detect missing logical output
                        |
                        v
              derive lost exchange ranges
                        |
                        v
              compute minimal replay closure
                        |
                        v
              replay only partition 1 inputs
              from deterministic source lineage
                        |
                        v
              recompute final partition 1
                        |
                        v
              commit one successful attempt
                        |
                        v
              produce the same oracle result
```

The job must **not** rerun all unaffected partitions or silently launch a fresh whole-job execution and call that recovery.

Phase 6 is still an early research-core milestone. It is not the general reliability/HA phase. It should prove the identity, lineage, planning, fencing, and recovery semantics that later phases can generalize.

---

# 2. Source-of-Truth and Baseline Discipline

Use repository precedence:

1. `docs/product/prd.md` — product scope and Phase 6 outcome;
2. `docs/architecture/architecture.md` — long-lived invariants and system boundaries;
3. this Phase 6 plan — detailed Phase 6 implementation contract;
4. accepted ADRs — concrete design decisions/refinements;
5. implementation reports and current code — observed state.

Before editing:

- confirm the current `main` head and record the exact SHA;
- compare it with `602cb3aab1a97360c15b6db61965a5b64c325e79`;
- inspect any commits added after this plan was written;
- reconcile this plan to the actual repository rather than forcing stale file names or assumptions.

If a better design satisfies the PRD/architecture more cleanly than the recommended implementation below, it may be used, but the implementation report must explain the deviation and why it preserves or improves the required invariants.

Do **not** reinterpret Phase 6 as permission to pull Phase 7 expected-cost materialization, Phase 8 repartitioning, Phase 10 elasticity, or Phase 12 general reliability into scope.

---

# 3. Relevant Architectural Invariants

The following architecture requirements are especially important in Phase 6.

## 3.1 Logical identity is not attempt identity

The architecture explicitly requires:

```text
TaskId / MorselId / SegmentId != AttemptId
```

A retry/recompute attempt must preserve logical data identity. Attempt numbers describe execution history, not new logical data.

Phase 6 must remove the current practical assumption that every distributed task/transfer is permanently `AttemptId(0)`.

## 3.2 A segment is the smallest independently recoverable tracked exchange unit

The architecture distinguishes:

- **morsel** — schedulable source/input work;
- **partition** — logical routing domain;
- **segment** — independently recoverable/tracked exchange output;
- **extent/range** — bounded metadata over adjacent segments.

Phase 6 should make these identities concrete enough to support recovery without creating one permanent coordinator object per segment.

## 3.3 Lineage answers a specific question

For missing output:

> What is the minimal deterministic computation that can regenerate this data?

The answer must be based on actual physical execution structure and source snapshot identity. It must not be an aspirational “we could probably rerun this.”

## 3.4 Replayability is a correctness property

Recomputation is valid only if required inputs and semantics are stable, including the applicable subset of:

- source snapshot identity;
- physical graph/operator version;
- partition/hash version;
- schema version/fingerprint;
- deterministic execution/configuration;
- UDF/artifact identity when those exist later;
- no non-replayable side effects in the closure.

Current ADE has a narrow deterministic synthetic/Parquet aggregate path. Phase 6 should model the facts actually needed by that path without pretending the general problem is solved.

## 3.5 Materialization metadata describes facts; recovery policy chooses an action

Phase 5 placement information is evidence about what was produced/retained. It is not itself a recovery policy.

Keep a separation similar to:

```text
facts / lineage / availability
          |
          v
+----------------------+
| Recovery Planner v1  |  pure / bounded / explainable
+----------------------+
          |
          v
+----------------------+
| Recovery Mechanisms  |  replay / restore / fail
+----------------------+
```

## 3.6 Recovery order must be semantically valid

The architecture's long-term order is:

```text
1. surviving copy
2. durable restore
3. minimal-lineage recompute
4. larger region restart
5. job restart
```

Phase 6 does **not** need to implement every option. It must represent unsupported options truthfully.

In particular:

- the Phase 5 default “object store” is an execution-scoped local filesystem fixture and is **not** proof of durable recovery;
- local worker spill is lost with a failed worker;
- peer replicas do not exist yet;
- Phase 7 expected-dollar-cost comparison does not exist yet.

For the supported Phase 6 failure, recomputation may therefore be the first valid recovery mechanism.

## 3.7 Recovery metadata and control overhead remain bounded

Do not solve lineage by retaining an unbounded in-memory record for every accepted segment.

Prefer:

- logical transfer descriptors;
- compact segment ranges such as `[0, terminal_count)`;
- aggregate/fingerprint metadata;
- bounded per-attempt records;
- one recovery plan for the current failure episode;
- fixed or topology-bounded maps.

---

# 4. Current Repository State at the Reviewed Baseline

The baseline already contains Phase 5 plus its independent-review remediation.

## 4.1 Workspace

Current dependency direction remains:

```text
ade-bench
   |
   v
ade-distributed
   |
   v
ade-exec
```

Do not create speculative empty crates merely because the architecture's long-term ownership map includes `ade-lineage`, `ade-scheduler`, `ade-storage`, etc.

For Phase 6, small focused modules such as `lineage.rs` and `recovery.rs` inside `ade-distributed` are preferable unless implementation reveals a real reusable ownership boundary that justifies a new crate.

## 4.2 Current distributed graph

The current graph remains:

```text
scan
  -> project
  -> partial hash aggregate
  -> hash exchange
  -> final merge aggregate
  -> gather
```

with:

- one coordinator;
- 2–4 static worker processes;
- fixed membership;
- one producer and one final role per worker;
- producer/final overlap;
- one prepared job per worker;
- fixed hash partition count equal to worker count;
- no worker replacement;
- no scheduler/work stealing;
- no recovery.

## 4.3 Current identities and protocols

Current `ids.rs` contains:

```text
JobId
WorkerId
TaskId
AttemptId
ExchangeId
PartitionId
TransferId {
  job,
  exchange,
  partition,
  producer,
  attempt
}
```

but helper construction and worker validation effectively assume `AttemptId(0)`.

The private control protocol is currently `ADETCP02`.

The exchange segment wire is version 2. A transfer carries full `TransferId`; each non-empty frame carries a monotonically increasing sequence number, and terminal completion carries the final segment count.

Phase 6 should avoid changing the exchange wire unless required by a real contract change. A control-message schema change should bump the private control protocol version so incompatible peers fail before JSON decoding.

## 4.4 Phase 5 exchange state

Phase 5 provides:

- five static retention mechanisms;
- `adaptive` mode with deterministic `threshold_v1`;
- validated transfer sequence/completion;
- bounded adaptive transition metadata;
- buffer/spill/object accounting;
- cancellation/deadline containment;
- poisoning after uncontained retention I/O;
- injectable object-store backend;
- common reader path into the final aggregate;
- static/adaptive release evidence.

Important Phase 5 boundary:

> object presence and placement history do not establish recovery.

That remains true unless Phase 6 explicitly proves a recovery-capable store and restore path. This plan does not require that.

## 4.5 Current coordinator failure behavior

The coordinator currently:

- starts final RPCs and producer RPCs together;
- waits for task responses;
- cancels the entire job on the first required task error;
- returns no partial output.

Phase 6 must carefully refactor this behavior. It should preserve fail-fast cleanup for unrecoverable cases while retaining enough successful task facts to plan an eligible recovery.

## 4.6 Current benchmark source identity

The benchmark harness already has a useful source snapshot substrate:

- versioned `DatasetManifest`;
- manifest identity;
- logical/schema fingerprints;
- per-file relative path, byte/row counts, row-group count, fingerprint;
- deterministic generator configuration.

Use these facts rather than inventing a second unrelated dataset identity scheme.

---

# 5. Recommended Phase 6 v1 Failure Boundary

The PRD exit condition requires a real worker failure, but Phase 6 does not require arbitrary failure timing.

The recommended v1 recovery envelope is:

> **Exactly one worker becomes unavailable after all original producer tasks have successfully completed their exchange outputs, but before that worker's final partition output has been successfully committed to the coordinator gather. ADE detects that the logical final partition is missing, derives the lost input segment ranges for that partition, replays only the required producer lineage on surviving capacity, recomputes the missing final partition, and completes the original job without rerunning unaffected final partitions.**

Why this is a useful first boundary:

- it is a genuine process/worker loss, not an injected Rust error;
- exchange state on the failed worker is genuinely lost;
- all source-producing work is known to have completed, so lost segment coverage is knowable from terminal manifests;
- unaffected final partitions can remain committed;
- recovery requires real lineage replay and attempt fencing;
- it avoids simultaneously solving partially produced transfers, producer lease reassignment, dynamic cluster membership, and general scheduling.

The implementation may support a broader safe failure window if it naturally falls out of the design, but the report must distinguish what is actually proven from what is merely structurally possible.

---

# 6. Stable Identity Model

Phase 6 should add explicit attempt-independent identity.

Exact Rust names may differ, but the model should contain equivalents of the following.

## 6.1 Logical transfer identity

```rust
struct LogicalTransferId {
    job: JobId,
    exchange: ExchangeId,
    partition: PartitionId,
    producer: TaskId,
}
```

`AttemptId` is intentionally absent.

A physical execution transfer is then conceptually:

```text
(LogicalTransferId, AttemptId)
```

The current `TransferId` may remain the concrete wire identity if that avoids churn.

## 6.2 Deterministic segment identity

```rust
struct SegmentId {
    transfer: LogicalTransferId,
    sequence: u64,
}
```

The same logical data regenerated by attempt 1 should have the same `SegmentId` sequence as attempt 0 when replay determinism holds.

Do not define `SegmentId` using:

- socket identity;
- worker process ID;
- temp file path;
- attempt number;
- wall-clock time;
- random UUID.

## 6.3 Morsel identity

Current source scheduling is file-granular and round-robin. Phase 6 may therefore use one deterministic morsel per manifest file.

Conceptual descriptor:

```rust
struct SourceMorsel {
    id: MorselId,              // stable manifest ordinal or equivalent
    relative_path: ...,
    file_fingerprint: ...,
    rows: u64,
    bytes: u64,
}
```

Do not claim row-group/range-level scheduling if the runtime still replays whole assigned files.

## 6.4 Source snapshot identity

A compact descriptor should bind replay to the verified dataset state. It can reuse benchmark manifest identity/fingerprints.

Conceptually:

```text
source snapshot identity
schema fingerprint
logical dataset fingerprint
manifest/file fingerprint(s)
```

The exact fields should be the minimum needed to prevent accidental replay from a changed input snapshot.

---

# 7. Lineage Graph v1

Phase 6 needs a real lineage graph, but the graph should reflect the current physical engine rather than an imagined future planner.

For the current aggregate graph, a useful lineage structure is:

```text
SourceSnapshot
    |
    +-- Morsel 0 ----+
    +-- Morsel 4 ----+--> ProducerTask 0
    |                         |
    |                         +--> logical transfer to partition 0
    |                         +--> logical transfer to partition 1
    |
    +-- Morsel 1 ----+
    +-- Morsel 5 ----+--> ProducerTask 1
                              |
                              +--> logical transfer to partition 0
                              +--> logical transfer to partition 1
```

A producer's current partial hash aggregate is built across all files assigned to that producer. Therefore the truthful minimal closure for one lost producer→partition transfer is currently:

> the full set of source morsels assigned to that producer task, followed by the same project/partial-aggregate/partition pipeline, while emitting only the target partition.

This is still a meaningful minimal closure because recovery can exclude:

- unaffected producer tasks when not needed;
- unaffected destination partitions;
- already successful final partitions;
- the rest of the job.

Do **not** claim that Phase 6 can replay a single original output segment from a row-level source slice unless the execution engine genuinely has that property.

## 7.1 Required lineage facts

For the current supported graph, lineage should capture enough to validate at least:

- source snapshot identity;
- producer logical `TaskId`;
- input morsel IDs/descriptors;
- physical graph/lineage version;
- relevant execution configuration that affects deterministic batching/output;
- hash/partition function version;
- exchange/schema version as needed;
- target logical transfer/partition;
- replayability/non-replayable reason.

Avoid serializing arbitrary internal Rust objects into a “lineage blob.” Prefer explicit versioned fields.

## 7.2 Bounded lineage metadata

The lineage graph should be bounded by job topology and input-morsel count, not output segment count.

The current file-level morsel list is acceptable because it already corresponds to job input cardinality. Do not add a persistent lineage node for every Arrow batch or accepted exchange segment.

---

# 8. Transfer Manifest and Replay Verification

Lost-segment detection needs more than “worker disappeared.” The coordinator should know what logical exchange output was successfully produced before the failure.

Recommended: add a compact **transfer manifest** to producer results.

Conceptually:

```rust
struct TransferManifest {
    logical: LogicalTransferId,
    attempt: AttemptId,
    segment_count: u64,
    row_count: u64,
    payload_bytes: u64,
    content_fingerprint: ...,
}
```

The existing `NetworkMetrics` may continue to hold counters; do not overload it if doing so obscures semantics.

## 8.1 Deterministic fingerprint

A replay fingerprint is strongly recommended because it provides a cheap integrity check that attempt 1 regenerated the same logical transfer as attempt 0.

A practical implementation may hash, in sequence order:

```text
logical transfer identity/version
segment sequence
encoded segment bytes
terminal count/version
```

Requirements:

- deterministic across attempts for identical logical input/config;
- does not include attempt ID, socket address, timestamps, or temp paths;
- computed incrementally with bounded memory;
- replay must match the original manifest before recovered output is considered valid.

A small dependency such as `blake3` is acceptable if justified; it already exists elsewhere in the workspace lockfile through `ade-bench`.

If an alternative verification scheme provides equivalent deterministic coverage with less change, document it.

## 8.2 Empty transfers

Empty transfers still have logical identity and terminal count zero. Recovery must not accidentally omit them from expected transfer coverage.

---

# 9. Lost-Segment Detection

For the recommended v1 failure window, all original producer tasks have completed successfully. That gives the coordinator terminal manifests for every producer→destination transfer.

If final partition `P` is lost with its worker, then the missing input coverage is compactly describable as:

```text
for each producer logical transfer to P:
    segments [0, original.segment_count)
```

This is preferable to storing all `SegmentId`s in memory.

Conceptual type:

```rust
struct SegmentRange {
    transfer: LogicalTransferId,
    start: u64,
    end_exclusive: u64,
}
```

Requirements:

- reject malformed ranges/overflow;
- zero-length range is valid for an empty transfer but should not create fake missing data;
- ranges must be derived from terminally completed manifests;
- do not infer completion from “connection closed” alone;
- do not mark unaffected partitions as lost merely because one worker disappeared;
- keep lost ranges deterministic and ordered for diagnostics.

---

# 10. Minimal Lineage Closure

Implement a pure/bounded closure computation that maps lost logical transfer ranges to the smallest valid replay work supported by the current physical graph.

Conceptual flow:

```text
lost segment ranges
      |
      v
logical transfer(s)
      |
      v
producer lineage node(s)
      |
      v
required source morsel set(s)
      |
      v
replay tasks restricted to target destination partition
```

For the current graph:

- each missing producer→partition transfer maps to that producer task's assigned source morsels;
- multiple lost ranges belonging to the same logical transfer should collapse to one replay of the producer lineage when the current execution cannot regenerate individual output segments independently;
- only the missing destination partition is emitted during replay;
- a producer with an empty original transfer may still need lineage validation, but the replay may prove zero emitted segments.

The planner must be explicit about granularity. “Minimal” means minimal under current valid replay primitives, not theoretical minimality.

---

# 11. Recovery Planner v1

Add a pure recovery planner separated from execution mechanisms.

Potential module:

```text
crates/ade-distributed/src/recovery.rs
```

Conceptual inputs:

```text
failure/liveness facts
successful logical outputs / attempts
transfer manifests
lineage graph + replayability
known surviving materialization capabilities
job topology
```

Conceptual decision:

```rust
enum RecoveryAction {
    UseSurvivingOutput(...),
    Recompute { ... },
    Unrecoverable { reason: ... },
}
```

Exact representation may differ.

## 11.1 Required planner behavior

For the supported v1 scenario:

- unaffected successfully committed final partitions are reused;
- missing final partition is identified by logical task/partition identity;
- local data on the dead worker is treated as lost;
- the planner evaluates whether a valid surviving/durable copy exists;
- absent such a proven copy, it chooses recomputation if lineage is replayable;
- otherwise it returns an explicit unrecoverable reason and the job fails cleanly.

## 11.2 No Phase 7 expected-cost policy

Do not add:

- failure probability estimation;
- storage dollar prices;
- deadline slack economics;
- expected recompute/restore dollar cost;
- learned policy.

If multiple valid Phase 6 mechanisms exist, use an explicit deterministic preference/capability rule and record why. Phase 7 is where expected-cost selection becomes the product goal.

---

# 12. Recommended Recovery Execution Path

The following design is recommended because it minimizes changes to the existing exchange correctness machinery while establishing clean attempt boundaries. Equivalent safer designs are acceptable if documented.

## 12.1 Step A — collect original task facts without immediately destroying recovery state

Refactor coordinator task collection so it can distinguish:

- successful producer results;
- successful final results;
- failed/unreachable worker final;
- other task failures;
- timeout/cancellation.

Do not simply ignore failures. Preserve the current cleanup discipline for unrecoverable cases.

Eligibility for v1 recovery should require, at minimum:

- all original producer tasks succeeded and their transfer manifests validated;
- exactly one worker is unavailable / one logical final partition is missing;
- all other required final tasks succeeded;
- the failed final output was not committed as successful;
- lineage for the lost partition is replayable;
- at least one survivor can host recovery;
- only one recovery episode is attempted in v1.

A task error from a still-alive worker is not automatically equivalent to worker loss. Classify errors carefully.

## 12.2 Step B — choose a deterministic recovery host

For v1, pick a surviving worker deterministically, for example the lowest `WorkerId` satisfying capability constraints.

This is **not** dynamic cluster scheduling. It is a bounded recovery-placement rule.

The recovery host should run after its normal final task has completed, avoiding the need to solve arbitrary simultaneous final/recovery resource contention.

## 12.3 Step C — create a fresh recovery exchange session/store

Prefer a fresh recovery store/session rather than mutating the failed worker's old `Store` semantics to merge multiple attempts.

The recovery store should:

- expect replay transfers for only the lost partition;
- expect the original logical producer IDs with a new `AttemptId`;
- use a deterministic static retention mechanism such as `LocalMaterialized` unless a different existing mechanism is demonstrably simpler/correct;
- preserve sequence/terminal validation;
- have independent cancellation/cleanup;
- be bounded by the existing retention/resource contracts.

This isolates replay attempt state from the original exchange session.

## 12.4 Step D — replay producer lineage for only the lost destination

Add a replay request that carries enough validated information to rerun an original producer logical task with:

- the same logical `TaskId`;
- a new bounded `AttemptId` (typically 1 for the only v1 retry);
- the same source morsels/snapshot facts;
- the same relevant execution/hash/schema configuration;
- only one target destination partition/endpoint.

The replay may execute producer closures on surviving workers. Do not require replay to run on the same original worker.

Concurrency should be bounded by topology. Avoid one thread/RPC per segment.

If replaying all producer closures sequentially is simpler and still practical for v1, that is acceptable. If parallelized, cap it by worker/replay-task count and preserve deterministic manifests.

## 12.5 Step E — verify replay transfer identity/content

Before recovered final output can commit:

- every replay transfer must complete with valid terminal count;
- its logical transfer ID must equal the original logical ID;
- replay attempt ID must be the expected attempt;
- rows/bytes/segment count should match the original transfer manifest where semantically stable;
- content fingerprint must match if implemented;
- unexpected/duplicate/stale transfers must fail the recovery episode.

A mismatch indicates source/config drift or nondeterministic replay and must not be silently accepted.

## 12.6 Step F — recompute the missing final partition

Run the existing final merge aggregate over the recovery store. Do not create a separate aggregate algorithm.

The recovered final task must preserve its logical final task/partition identity and use a new attempt ID.

## 12.7 Step G — commit exactly one successful final attempt

The coordinator's gather path must become attempt-safe.

Recommended approach:

- isolate gather state by logical final task and attempt;
- a failed/incomplete attempt is never promoted to the committed job output;
- a recovery attempt uses a fresh gather attempt/store;
- once one attempt for a logical final task validates fully, exactly that attempt is committed;
- late/stale traffic from a superseded attempt is rejected or quarantined and cannot duplicate output.

This establishes an internal attempt-commit boundary for the current gather path.

Do not claim this is the general durable sink transaction protocol from later phases.

---

# 13. Failure Injection for Acceptance

Phase 6 requires real worker-loss evidence.

Add the smallest deterministic **research-only process-failure hook** capable of creating the supported recovery window.

Recommended injection point:

> target worker exits after it has consumed/validated the complete original exchange input for its final partition, but before it sends a successful final aggregate output to the coordinator gather.

This creates a real process disappearance while ensuring all original producer transfers have terminal manifests.

Requirements:

- compiled only under `ade-distributed`'s `research-hooks` feature;
- enabled by `ade-bench`, not present in the default distributed library API/path;
- explicit target worker/partition and failure point;
- defaults off;
- recorded in benchmark effective configuration;
- cannot trigger accidentally in normal builds;
- must terminate the worker process, not just return a synthetic error;
- must not corrupt source rows or routing before the declared failure point.

Do not add a broad production fault-injection framework in this phase.

---

# 14. Control Protocol and Attempt Fencing

Phase 6 will likely add requests/replies for recovery preparation, replay, and recovered final execution.

Because the serialized control schema changes, bump the private control magic/version from `ADETCP02` to the next version (expected `ADETCP03`) unless implementation avoids any serialized schema change.

Add a focused test proving the new implementation rejects the prior control protocol before decoding incompatible JSON.

The exchange wire may remain version 2 if its serialized header/frame format does not change.

Attempt fencing requirements:

- workers reject unexpected attempt IDs;
- v1 permits a small bounded attempt set (normally 0 and at most one recovery attempt);
- stale attempt traffic cannot enter a fresh recovery store;
- a late original final output cannot overwrite/duplicate a committed recovery attempt;
- reused workers retain the current monotonically increasing job-ID protection;
- recovery does not allocate a new `JobId` merely to avoid handling attempt identity.

The recovery must remain part of the original logical job.

---

# 15. Metrics and Decision Records

Add compact recovery observability sufficient to answer what happened.

A `RecoveryMetrics`/`RecoverySummary` equivalent should cover a useful subset of:

```text
worker_failures_detected
recovery_plans
recovery_attempts
recovered_final_partitions
reused_final_partitions
lost_transfer_count
lost_segment_count
lost_payload_bytes
replayed_producer_tasks
replayed_morsels
recomputed_segment_count
recomputed_payload_bytes
recovery_host
recovery_elapsed_ms
full_job_restart_avoided
planner outcome / reason
```

Do not store one permanent metric/log entry per segment.

A compact decision record should let a reviewer answer:

1. What worker/logical partition was lost?
2. Which original transfer ranges were considered missing?
3. Why was recomputation valid?
4. What source lineage was replayed?
5. Which work was explicitly *not* replayed?
6. Which worker hosted recovery?
7. Which attempts were committed/rejected?
8. Did replay match the original transfer manifests?
9. Did the job complete without whole-job restart?

Keep Phase 5 adaptive metrics intact. Recovery metrics are additive and should not relabel exchange pressure as failure risk/cost.

---

# 16. Conservation and Correctness Checks

A recovered job should pass stronger invariants than “oracle rows happened to match.”

Required checks should include the applicable set below.

## 16.1 Original production

- all original producer tasks succeeded;
- original producer transfer manifests cover every destination;
- transfer terminal counts and existing exchange conservation remain valid.

## 16.2 Lost partition

- exactly the expected logical final partition is missing;
- lost ranges derive from original completed transfer manifests;
- unaffected partitions are not marked lost.

## 16.3 Replay

- replay logical task/transfer IDs equal the originals;
- only attempt changes;
- replay uses the same source snapshot/lineage version;
- replay segment terminal counts match expected;
- replay transfer manifest/fingerprint matches original when implemented;
- no replay emits to an unaffected partition.

## 16.4 Gather commit

- exactly one successful attempt is committed for each logical final task;
- no final partition is omitted or duplicated;
- incomplete/stale attempts do not contribute output;
- aggregate rows still match the independent oracle.

## 16.5 Resource lifecycle

- original survivor stores drain/clean as before;
- recovery store drains/cleans;
- cancellation unblocks recovery writers/readers;
- poisoned worker semantics remain intact;
- failed recovery returns no partial job output;
- no worker/recovery thread leaks after job/cluster cleanup.

---

# 17. Failure Classification and Negative Paths

Phase 6 must not turn every error into a retry.

At minimum, distinguish these cases:

## Recoverable v1 candidate

- all producers completed;
- exactly one worker process is unreachable;
- its logical final output has no committed successful attempt;
- other finals succeeded;
- lineage is valid/replayable.

## Explicitly unrecoverable in v1

Examples:

- a producer task did not complete, so lost segment coverage is incomplete;
- more than one worker is lost;
- source snapshot identity cannot be validated;
- replay manifest mismatches the original;
- recovery host fails;
- recovery attempt fails or times out;
- lineage marks the work non-replayable;
- coordinator/gather correctness is uncertain;
- task error occurs without evidence that the relevant worker is actually lost and there is no safe retry contract.

These should fail cleanly and cancel survivors, preserving the no-partial-output behavior.

Do not loop through arbitrary retries in Phase 6.

---

# 18. Benchmark Harness Changes

Extend `ade-bench` narrowly.

Recommended additions:

- pass a source snapshot/manifest descriptor into `StaticAggregateJob` or the equivalent public API;
- expose recovery configuration in effective engine metadata;
- expose compact recovery summary diagnostics;
- add the feature-gated worker-failure injection option;
- retain child-process isolation;
- retain independent oracle validation;
- retain benchmark result schema v1 unless a real artifact-contract change requires a version bump.

Avoid hiding recovery by constructing a brand-new `AdeDistributedEngine` instance after failure. The acceptance evidence must prove the original job recovered selected data within the distributed runtime.

---

# 19. Phase 6 Acceptance Experiment

Phase 6 acceptance is primarily a correctness/fault-recovery experiment, not a speed contest.

## 19.1 Predeclare one deterministic dataset/workload

Use the existing synthetic `GroupBySum` path with enough rows/cardinality to produce multiple exchange segments per producer/destination.

Keep:

- deterministic generator seed;
- fixed worker count/config;
- independent oracle;
- clean source identity;
- isolated child execution.

## 19.2 Required cases

Keep the matrix compact.

### Case A — normal baseline

Two workers, no failure.

Purpose:

- prove unchanged result;
- capture baseline segment/transfer manifests;
- prove recovery instrumentation is dormant.

### Case B — one recoverable worker failure

Two workers, deterministic worker exit at the supported Phase 6 failure point.

Required evidence:

- real target worker process exited;
- all original producer tasks completed successfully;
- one logical final partition was missing;
- lost transfer/segment ranges were derived;
- only the lost partition's upstream producer closures were replayed;
- unaffected final partition was not rerun;
- replay manifests matched originals;
- recovered final attempt committed exactly once;
- complete result matched oracle;
- job retained the original logical `JobId`;
- no whole-job restart occurred.

### Case C — unrecoverable boundary

One focused negative case, such as failure before producer completion or forced replay-manifest mismatch.

Required:

- planner rejects recovery;
- job fails with a specific reason;
- survivors are cancelled/cleaned;
- no partial output escapes;
- no hang/retry loop.

### Optional Case D — Phase 5 compatibility

If implementation touches adaptive exchange substantially, add one **limited** adaptive-mode recovery case proving the lineage/recovery layer does not depend on static retention.

Do not create a five-mode × failure-mode matrix unless a concrete bug requires it.

## 19.3 Performance reporting

Report recovery elapsed time/replayed bytes/morsels as diagnostics.

Do not require Phase 6 recovery to outperform whole-job restart in timing yet unless the experiment is stable and explicitly declared as research evidence. The Phase 6 product gate is selective recovery correctness.

---

# 20. Focused Test Strategy

Testing should target invariants and failure modes, not enumerate every combinatorial configuration.

## 20.1 Identity tests

High-value cases:

- logical transfer equality ignores attempt while concrete transfer identity includes it;
- `SegmentId` is stable across attempt 0/1;
- segment range validation/coverage;
- attempt overflow/unexpected attempt rejection;
- deterministic morsel IDs from manifest ordering;
- source snapshot mismatch rejection.

## 20.2 Lineage/closure tests

- one lost transfer maps to the correct producer source morsels;
- two ranges for the same transfer collapse to one replay closure when appropriate;
- unrelated producer/partition lineage is excluded;
- empty original transfer handled correctly;
- non-replayable lineage produces explicit planner refusal;
- closure metadata remains topology/input bounded.

## 20.3 Transfer-manifest tests

- deterministic identical transfer fingerprint on replay;
- attempt ID does not change logical fingerprint;
- content/sequence change changes fingerprint;
- zero-segment terminal manifest is stable;
- duplicate/gap/terminal mismatch still fails before manifest acceptance.

## 20.4 Recovery-planner tests

- one eligible worker/final loss => recompute plan;
- unaffected partitions => reuse/no action;
- producer incomplete => unrecoverable;
- two failed workers => unrecoverable in v1;
- changed snapshot/config => unrecoverable;
- planner is deterministic for identical facts;
- no Phase 7 cost/failure-probability inputs required.

## 20.5 Attempt-safe gather tests

- attempt 0 completes => commit once;
- attempt 0 incomplete + attempt 1 complete => only attempt 1 contributes;
- late attempt 0 data after attempt 1 commit is rejected/quarantined;
- duplicate successful attempts cannot double output;
- gather cancellation/cleanup after failed recovery.

## 20.6 Worker/recovery-session tests

- recovery store expects only target partition replay transfers;
- replay producer preserves logical `TaskId`, changes attempt only;
- targeted replay emits no other destination;
- recovery final consumes replay store through existing aggregate path;
- recovery session cleanup is bounded;
- poisoned worker cannot be selected as recovery host;
- cancellation unblocks replay and final recovery.

## 20.7 Process integration

One primary real-process test should prove the Phase 6 exit condition.

A second focused negative process case is enough unless debugging identifies additional necessary coverage.

## 20.8 Regression

Run focused existing tests around:

- static exchange sequence/completion;
- adaptive transitions/conservation;
- retention cancellation/poisoning;
- distributed oracle correctness;
- worker lifecycle/control protocol.

Do not duplicate the entire historical Phase 4/5 acceptance matrix during every development step.

---

# 21. Suggested Implementation Sequence

The sequence below is designed to expose correctness issues early and keep iteration focused.

## Step 1 — baseline reconciliation

- record current SHA;
- read PRD Phase 6 and architecture sections on identity, lineage, materialization catalog, fault tolerance, observability, testing;
- read Phase 5 report and ADR-0012 remediation details;
- inspect coordinator/worker/exchange/IDs/protocol/benchmark adapter;
- update the implementation approach for any current-repo changes.

## Step 2 — stable IDs and source/lineage descriptors

Implement attempt-independent logical transfer/segment/morsel IDs and versioned source/lineage descriptors.

Focused tests only.

Do not change recovery behavior yet.

## Step 3 — producer transfer manifests

Add terminal transfer manifests/fingerprints and return them to the coordinator.

Prove normal execution is behavior-preserving before adding retry.

## Step 4 — pure lost-range + lineage-closure + recovery planner

Implement the planning layer with no worker orchestration side effects.

Test eligible/unrecoverable scenarios thoroughly at unit level.

## Step 5 — attempt-safe gather/commit boundary

Refactor gather so an incomplete failed final attempt cannot contaminate a replacement attempt.

This should be proven before process failure injection.

## Step 6 — worker recovery session and targeted replay

Add the minimal requests/state for:

- preparing recovery store;
- replaying producer lineage to one lost partition;
- running recovered final attempt;
- cleaning recovery state.

Keep one recovery episode/job in v1.

## Step 7 — coordinator recovery orchestration

Refactor task result collection to retain successful facts, classify one eligible worker loss, build a recovery plan, execute it, validate replay, and commit recovered output.

All unrecoverable branches should still cancel/clean promptly.

## Step 8 — deterministic research worker-loss hook

Add feature-gated process exit at the proven recovery boundary.

Verify default build has no hook/config path.

## Step 9 — process integration and acceptance verifier

Add the compact normal/recoverable/unrecoverable experiment plus verifier.

Prefer assertions over timing thresholds.

## Step 10 — documentation and final gate

Update ADR/report/README/AGENTS and only then run the full final workspace gate once focused tests are green.

---

# 22. Documentation / ADR Work

Phase 6 should add an ADR covering the actual accepted design, likely:

```text
docs/adr/0013-lineage-recovery-v1.md
```

It should record at least:

- logical vs attempt identity;
- current morsel/segment granularity;
- source snapshot/replayability contract;
- transfer manifest/fingerprint semantics;
- compact lost-range representation;
- minimal closure semantics under the current aggregate graph;
- recovery planner/mechanism boundary;
- supported worker-failure window;
- recovery host/attempt policy;
- attempt-safe gather/commit semantics;
- bounded metadata/retry count;
- control protocol version change;
- no Phase 7 expected-cost selection;
- remaining unrecoverable scenarios.

Also update:

- `AGENTS.md` — Phase 6 active/completed boundary;
- `README.md` — current recovery support and narrow failure envelope;
- benchmark/research docs if needed;
- `docs/reports/phase-6-implementation-report.md`.

Do not rewrite the PRD/architecture simply to match implementation details. If the implementation exposes a genuine architectural contradiction, record and resolve it explicitly.

---

# 23. Phase 6 Implementation Report Requirements

The report should make independent review possible without reconstructing the entire coding session.

Include:

- baseline and final commit identity;
- exact supported failure envelope;
- logical/attempt/morsel/segment identity model;
- source snapshot and replayability contract;
- lineage graph/closure semantics;
- lost-segment detection semantics;
- recovery planner decisions and unsupported alternatives;
- replay manifest/fingerprint checks;
- coordinator/worker recovery state machine;
- attempt-safe gather semantics;
- protocol/version changes;
- resource/boundedness analysis;
- failure/cancellation/poisoning behavior;
- research hook isolation;
- focused tests and final commands actually run;
- acceptance artifacts and verifier results;
- one normal and one recovered partition history/metrics summary;
- negative unrecoverable evidence;
- known limitations and Phase 7 handoff;
- deviations from this plan and rationale.

Do not claim unsupported arbitrary worker recovery based only on the one accepted failure window.

---

# 24. Explicit Non-Goals

Phase 6 does **not** require:

- arbitrary worker failure during partially completed producer exchange;
- recovery from incomplete/non-terminal original producer transfers;
- multiple simultaneous worker failures;
- arbitrary retry loops;
- worker replacement or elastic cluster membership;
- work stealing or a general scheduler;
- peer replication;
- remote shuffle service;
- proven durable object-store restore;
- expected-cost materialization/recovery economics;
- failure probability estimation;
- deadline/budget optimization;
- Phase 7 expected-cost policy;
- dynamic partition split/coalesce or skew adaptation;
- changing partition count during recovery;
- coordinator HA/restart;
- persistent lineage catalog across coordinator restart;
- general output-commit protocol for arbitrary external sinks;
- row-group/range morsel scheduling if file-level morsels are the real current primitive;
- SQL/DataFrame frontend;
- joins/adaptive joins;
- streaming/event-time/state/checkpoints;
- exactly-once external sinks;
- cross-job materialization reuse;
- incremental computation;
- learned recovery policy;
- process-wide RSS memory management or disk quota unless independently justified as a correctness prerequisite.

These are not forbidden forever. They are outside the Phase 6 proof.

---

# 25. Phase 6 Exit Criteria

Phase 6 is complete only when all of the following are true.

## Identity and lineage

- [x] Logical work/data identity is explicitly separated from `AttemptId`.
- [x] Deterministic current-scope `MorselId` and `SegmentId` equivalents exist.
- [x] Source snapshot/replayability facts are explicit and validated.
- [x] Current physical lineage graph is represented without per-segment metadata explosion.

## Detection and planning

- [x] Completed original producer transfers expose compact terminal manifests.
- [x] One lost final partition can be translated into exact compact lost segment ranges.
- [x] Minimal lineage closure excludes unaffected final partitions and destination outputs.
- [x] A pure/bounded recovery planner selects only semantically valid actions.
- [x] Unsupported recovery cases fail with explicit reasons.

## Recovery mechanism

- [x] A real worker-process failure in the supported window is detected.
- [x] The original job does not restart from scratch.
- [x] Only required producer lineage is replayed for the lost destination partition.
- [x] Replay preserves logical IDs and changes attempt identity only.
- [x] Replay content/terminal manifests are validated against original output.
- [x] Missing final partition is recomputed through the existing aggregate path.
- [x] Exactly one successful final attempt is committed for every logical partition.
- [x] Late/stale/partial attempts cannot duplicate result output.

## Correctness and boundedness

- [x] Recovered complete result matches the independent oracle.
- [x] Existing exchange sequence/completion/conservation invariants remain intact.
- [x] Phase 5 static/adaptive mechanisms still work in focused regression coverage.
- [x] Recovery metadata/control state is bounded by topology/input/retry limits, not output segment count.
- [x] Cancellation, timeout, cleanup, and poisoning semantics remain explicit.

## Evidence

- [x] Deterministic real-process recovery acceptance passes on a clean reviewed commit.
- [x] One explicit unrecoverable case fails cleanly with no partial output/hang.
- [x] Research failure injection is feature-gated and absent from the default library path.
- [x] Phase 6 ADR/report/README/AGENTS documentation is current.
- [x] Final workspace formatting/check/clippy/tests/build/smoke run or any environment limitation is recorded truthfully.

The key product claim should be no broader than:

> **For the documented Phase 6 worker-loss window, ADE can detect the missing logical partition, derive its lost exchange ranges, recompute the minimal currently supported lineage closure on surviving capacity, and finish the original job without restarting unaffected work.**
