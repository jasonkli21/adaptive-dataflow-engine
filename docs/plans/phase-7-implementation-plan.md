# Adaptive Dataflow Engine (ADE) — Phase 7 Detailed Implementation Plan
## Expected-Cost Materialization v1

**Status:** Implemented with reconciliations in ADR-0014; see Phase 7 implementation report\
**Date:** 2026-10-16\
**Repository:** `jasonkli21/adaptive-dataflow-engine`\
**Repository baseline reviewed:** `ae5d0f03d9532320e7644d1d8fa34849947fe8c5` (`Address Phase 6 review findings`)\
**Primary references:** `docs/product/prd.md`, `docs/architecture/architecture.md`, `docs/plans/phase-5-implementation-plan.md`, `docs/plans/phase-6-implementation-plan.md`, `docs/reports/phase-5-implementation-report.md`, `docs/reports/phase-6-implementation-report.md`, ADR-0011, ADR-0012, ADR-0013, `AGENTS.md`, and current code on `main`\
**Primary objective:** replace threshold-only remote persistence with a truthful, interpretable expected-cost materialization decision that can produce and use a verified worker-loss-surviving copy of exchange output, while preserving the existing exchange, lineage, recovery, correctness, boundedness, and benchmark contracts.

---

# 1. Phase 7 Objective

The PRD defines Phase 7 as:

> **Expected-Cost Materialization**
>
> Goal: replace threshold-only persistence with interpretable expected-cost decisions.
>
> Inputs include measured production cost, source reread cost, storage throughput/cost, consumer count, failure probability, and deadline slack.
>
> Exit: traceable cost-aware policy wins on a suite spanning different failure/storage regimes.

Phase 5 established a deterministic threshold policy over exchange retention mechanisms. Phase 6 added stable logical identity, terminal manifests, replayable file lineage, and one bounded final-worker-loss recovery episode. Phase 7 should connect these two foundations without collapsing mechanism, policy, recovery, and benchmarking into one module.

The central Phase 7 question is:

> Given a completed, replayable unit of intermediate work, is the expected value of retaining a worker-loss-surviving copy lower than the expected value of relying on lineage and recomputation, after accounting for storage cost, compute/source reread cost, recovery probability, recovery latency, and deadline pressure?

A successful v1 should make a flow like this possible:

```text
producer executes deterministic partial aggregate
        |
        +-- emits normal exchange data for final consumer
        |
        +-- measures actual production work and source facts
        |
        v
terminal boundary for logical transfer
        |
        v
expected-cost policy compares:
  A) do not persist; recompute if worker-local copy is lost
  B) retain a worker-loss-surviving copy; restore if useful
        |
        +-- decision = do not persist
        |       -> acknowledge terminal with traceable decision record
        |
        +-- decision = persist
                -> complete verified durable copy
                -> acknowledge terminal with receipt + decision record

later: final worker is lost
        |
        v
recovery planner validates facts/capabilities
        |
        +-- valid retained copy + restore cheaper -> restore
        |
        +-- no valid copy or recompute cheaper -> replay lineage
        |
        v
same attempt-safe gather commit boundary as Phase 6
```

The implementation should remain deliberately narrow. This is the first interpretable cost-aware materialization policy, not the final global optimizer.

---

# 2. Source-of-Truth and Baseline Discipline

Use repository precedence:

1. `docs/product/prd.md` — product scope and Phase 7 outcome;
2. `docs/architecture/architecture.md` — cross-phase invariants and system boundaries;
3. this Phase 7 plan — detailed Phase 7 implementation contract;
4. accepted ADRs — concrete decisions/refinements;
5. implementation reports and current code — observed state.

Before editing:

- confirm the current `main` head and record the exact SHA;
- compare it with `ae5d0f03d9532320e7644d1d8fa34849947fe8c5`;
- inspect any commits added after this plan was written;
- reconcile this plan with the actual repository rather than forcing stale file names or signatures;
- read the Phase 6 implementation report and ADR-0013 in full, including its review-remediation notes and limitations.

This plan proposes a concrete v1 architecture, but it is not permission to ignore a cleaner design discovered while implementing. An equivalent design is acceptable if it satisfies the PRD/architecture more directly and the implementation report explains the deviation. Do not preserve a recommendation merely because it appears here if it creates a correctness, boundedness, provenance, or maintainability problem.

Do **not** reinterpret Phase 7 as permission to pull in:

- Phase 8 dynamic repartitioning/skew adaptation;
- Phase 9 join/operator switching;
- Phase 10 work stealing/elasticity;
- Phase 12 coordinator HA/general reliability;
- Phase 13 production disaggregated shuffle service;
- streaming/checkpoint/state work;
- learned policies.

---

# 3. Current Repository State at the Reviewed Baseline

Phase 7 begins from the remediated Phase 6 state at `ae5d0f03...`, not from the original Phase 6 implementation commit.

## 3.1 Workspace and dependency direction

The workspace remains:

```text
ade-bench
   |
   v
ade-distributed
   |
   v
ade-exec
```

Preserve this direction. A focused `cost.rs`/materialization module inside `ade-distributed` is preferable to introducing speculative crates unless the implementation reveals a real reusable ownership boundary.

## 3.2 Current distributed graph

The supported graph remains:

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
- fixed membership and hash partition count;
- one producer and one final role per worker;
- producer/final overlap;
- one active job per worker;
- no worker replacement;
- no general scheduler or work stealing;
- one narrow Phase 6 recovery episode.

## 3.3 Phase 5 adaptive exchange remains an accepted baseline

Current exchange modes are:

```text
direct
adaptive
buffered
spill
local_materialized
object_store
```

`adaptive` currently means deterministic `threshold_v1` behavior over:

```text
direct -> buffered -> spill -> object_store
```

using truthful flow-control observations such as consumer readiness, buffer pressure, spill backlog, and produced size. The transition metadata is bounded.

Phase 7 must preserve:

- every existing static mode;
- the existing `threshold_v1` policy as an independently selectable baseline;
- the policy/mechanism separation established in Phase 5;
- existing sequence, terminal, cancellation, poison, and cleanup semantics.

Do not silently redefine `threshold_v1` to mean the new expected-cost policy.

## 3.4 Phase 6 identity and recovery are the foundation

Current Phase 6 establishes:

- `LogicalTransferId` independent of execution attempt;
- `SegmentId` as logical transfer + sequence;
- concrete `TransferId` with attempt identity;
- source snapshot/morsel identity;
- validated transfer terminal manifests containing segment/row/byte counts and a BLAKE3 content fingerprint;
- one full-transfer lost range per producer for a missing final partition;
- replay of only the missing destination;
- attempt-safe gather quarantine/promotion;
- exactly one bounded recovery episode for one final-worker loss after every producer terminal is acknowledged;
- no durable restore path.

The existing recovery planner is intentionally capability-based and contains no cost model.

## 3.5 Useful current execution timing boundary

In the current producer path, the local partial aggregate completes and yields `ExecutionMetrics` before the producer finalizes each exchange sender and receives the terminal ACK.

That gives Phase 7 a useful, truthful boundary:

```text
measured local production work available
        |
        v
terminal/materialization decision
        |
        v
terminal ACK
```

Phase 7 should prefer this existing ordering over inventing a speculative pre-run compute estimate when the measured value is already available.

## 3.6 Existing object-store mechanism is not yet a Phase 7 durable fact

The object-store retention mechanism already supports an injected `ObjectStore` backend and exposes backend/durability configuration. However, the benchmark's default worker process uses an execution-scoped local filesystem fixture. It does not survive the worker process lifecycle in a way Phase 6 can prove and use for restore.

Therefore:

> Existing object placement is not automatically a worker-loss-surviving materialization.

Phase 7 must define the precise failure domain that a retained copy survives and prove it in acceptance. Do not call a shared-local test fixture “cloud durable” or imply host/AZ durability that was never tested.

---

# 4. Phase 7 Architectural Invariants

## 4.1 Correctness remains independent of cost policy

The cost policy may choose among already-correct mechanisms. It must not:

- change hash routing;
- change query semantics;
- weaken transfer identity or terminal validation;
- accept a corrupt/incomplete materialization because it is cheaper;
- bypass gather attempt fencing;
- turn an unsupported recovery case into an optimistic retry.

If the model fails, overflows, sees invalid configuration, or lacks a required capability, the runtime must fail explicitly or use an explicitly documented safe fallback. It must never fabricate a cost-derived correctness guarantee.

## 4.2 Facts, policy, and mechanism remain separate

Use a conceptual boundary like:

```text
measured facts + configured objective
             |
             v
+------------------------------+
| Expected-Cost Policy v1      |  pure, deterministic, explainable
+------------------------------+
             |
             v
+------------------------------+
| Materialization Mechanism    |  stage / write / verify / receipt
+------------------------------+
             |
             v
+------------------------------+
| Materialization Catalog Fact |  what copy actually exists
+------------------------------+
             |
             v
+------------------------------+
| Recovery Planner             |  restore/recompute capability + cost
+------------------------------+
```

A receipt describes an existing verified copy. It does not assert that the copy should be used.

## 4.3 Delivery tier and recoverability are not the same state

The architecture explicitly allows simultaneous capabilities such as:

```text
stream-readable + locally spilled + remotely recoverable
```

Phase 7 should avoid modeling “persisted” as if it must replace the delivery mechanism.

Recommended v1 interpretation:

- `threshold_v1` remains the legacy exclusive tier progression baseline;
- `expected_cost_v1` may use the existing direct/buffer/spill mechanisms for delivery flow control;
- worker-loss-surviving materialization is an **orthogonal copy capability** whose bytes and costs are accounted separately.

This prevents durable copy bytes from being mislabeled as normal consumer-delivery bytes.

## 4.4 Cost inputs must state their provenance

Every cost input must be distinguishable as one of:

- **measured this execution**;
- **configured price/objective**;
- **configured/calibrated estimate**;
- **topology fact**;
- **derived value**.

Do not label a configured failure probability as “observed worker health.” Do not label exchange pressure as process memory pressure. Do not label a configured throughput estimate as measured throughput.

## 4.5 Decisions and metadata must remain bounded

Phase 7 must not introduce one permanent coordinator record or RPC per segment.

Prefer:

- one expected-cost decision record per logical transfer;
- one compact materialization receipt per retained logical transfer;
- deterministic object naming from logical identity/sequence;
- constant-size running hashes/counters;
- topology-bounded recovery choices;
- streaming/sequential reads and writes.

Disk/object bytes may scale with data size; control metadata must not.

---

# 5. Phase 7 v1 Decision Granularity

The architecture's long-term goal permits segment/morsel-level adaptation, but the current recovery primitive is coarser: Phase 6 replays a full producer closure for a lost producer-to-destination transfer.

Therefore the recommended Phase 7 v1 granularity is:

> **one cost/materialization decision per complete logical producer-to-destination transfer.**

Why this is a good current boundary:

- terminal manifests already define complete transfer identity/content;
- recovery already reasons about `[0, terminal_count)` for each lost transfer;
- a full retained transfer can replace one replay closure cleanly;
- metadata remains O(workers²), not O(segments);
- Phase 8+ can later refine topology/granularity without making Phase 7 pretend those mechanisms exist today.

A different granularity is acceptable if it remains independently recoverable, bounded, and fully integrated with lineage/recovery. A reviewer should reject a design that claims segment-level recoverability while still requiring full producer replay and lacking segment-level source lineage.

---

# 6. Expected-Cost Policy v1

## 6.1 Keep the model interpretable

Do not use ML or opaque search. The policy should be a deterministic pure function over a validated observation/configuration object.

Recommended module:

```text
crates/ade-distributed/src/cost.rs
```

Conceptual inputs:

```text
MeasuredProduction {
  operator_work_ns
  wall_elapsed_ns              // diagnostic; not silently substituted for CPU cost
  source_bytes
  source_objects/files
  output_bytes
  output_segments
}

CostConfig {
  compute_nano_usd_per_cpu_ns
  source_read_nano_usd_per_byte
  source_read_nano_usd_per_request
  object_write_nano_usd_per_byte
  object_write_nano_usd_per_request
  object_read_nano_usd_per_byte
  object_read_nano_usd_per_request
  object_capacity_nano_usd_per_byte_ms
  expected_retention_ms

  source_read_bytes_per_second_estimate
  object_write_bytes_per_second_estimate
  object_read_bytes_per_second_estimate

  failure_probability_ppm
  deadline_slack_ms
  slo_penalty_nano_usd_per_ms
  min_expected_savings_nano_usd
}

TopologyFacts {
  consumer_count
  replayable
  materialization_capability
  failure_domain
}
```

Exact names may differ. Keep unit names explicit in types/field names.

## 6.2 Fixed-point arithmetic

Prefer integer/fixed-point units instead of floating-point money/probability decisions.

Recommended conventions:

- monetary unit: `nano_usd` or another named fixed-point unit;
- probability: parts per million (`ppm`, 0..=1,000,000);
- time: integer ns/ms with explicit conversion;
- intermediate multiplication in checked `u128`;
- reject configurations whose validated bounds cannot be evaluated safely.

Do not allow silent saturation to change a materialization decision. Saturating counters are acceptable for diagnostics where already established, but policy arithmetic should fail on overflow rather than produce a false comparison.

## 6.3 Production cost

Use measured execution work from the existing local producer where practical.

A useful v1 compute proxy is:

```text
sum(operator exclusive work_time)
```

from `ade-exec::ExecutionMetrics`, because it represents measured operator work rather than scheduler/wait wall time. Keep wall elapsed separately as a diagnostic.

The implementation report must state exactly what `production_cost` includes and excludes. Do not call it full cloud compute billing if idle/control/network time is not included.

## 6.4 Source reread cost

The Phase 6 source snapshot already has the full producer file/morsel closure and byte sizes.

For the current whole-producer replay primitive, the no-persist recovery alternative should account for the source bytes/files that would actually be reopened for that producer closure.

Current local benchmark sources may have configured zero monetary reread price. That is valid if explicit; the field must not be fabricated merely to force persistence.

## 6.5 Consumer count

The current physical graph gives each producer-to-destination transfer one logical downstream final consumer.

For v1:

```text
consumer_count = 1
```

as a topology fact is truthful. Record it. Do not invent future fanout/reuse in order to satisfy the PRD input list.

## 6.6 Failure probability

Phase 7 needs failure probability as an input to expected cost, but current ADE has no calibrated production failure estimator.

Therefore v1 should use an explicit **configured experiment/objective probability** with clear provenance, e.g.:

```text
failure_probability_ppm=...
```

Do not claim it is learned from worker health. Runtime worker health still governs correctness/failure detection, not statistical estimation.

Later phases may calibrate this input from real operational history.

## 6.7 Deadline slack and SLO penalty

A full distributed deadline optimizer is not required.

Use an explicit job/scenario input such as:

```text
deadline_slack_ms
slo_penalty_nano_usd_per_ms
```

and clearly state that v1 models expected penalty; it does not yet provide a hard deadline scheduler.

Do not depend on cross-process wall-clock synchronization for the decision.

## 6.8 Recommended comparison

A minimal interpretable comparison is:

```text
recompute_now_cost =
    measured_production_compute_cost
  + source_reread_cost

persist_now_cost =
    object_write_cost
  + object_request_cost
  + expected_capacity_cost

restore_if_failed_cost =
    object_read_cost
  + object_read_request_cost

expected_no_persist =
    P(failure)
      * (recompute_now_cost
         + SLOPenalty(estimated_recompute_time, deadline_slack))

expected_persist =
    persist_now_cost
  + (1 - P(failure)) * SLOPenalty(estimated_write_time, deadline_slack)
  + P(failure)
      * (restore_if_failed_cost
         + SLOPenalty(estimated_write_time + estimated_restore_time,
                      deadline_slack))
```

The slack input is the remaining budget at the start of this transfer's
materialization decision path. The write finishes before the supported loss
boundary and restore happens after it, so the failure penalty must clamp their
combined critical-path time once. It must not add two independently clamped
penalties. This per-transfer model does not claim to model overlap among
producers. After a real loss, recovery compares direct action costs using elapsed
time accumulated across the sequential replay closures, seeded by the slowest
producer's measured selected-copy write total.

Persist only when:

```text
expected_persist + minimum_savings_margin < expected_no_persist
```

Recommended tie behavior: **do not persist**. This avoids paying an irreversible write cost for an equal estimate.

If a better v1 formula is used, it must remain interpretable, dimensionally valid, benchmarkable, and documented.

## 6.9 Throughput estimates versus measurements

Architecture requires runtime calibration over time, but Phase 7 v1 should avoid arrival-order-dependent online learning that makes a deterministic experiment hard to reason about.

Recommended split:

- decision-time throughput estimates are explicit configured/calibrated inputs;
- actual materialization/restore elapsed time and throughput are measured after the action;
- decision records preserve both estimated and actual values where available;
- acceptance evaluates whether the model made useful choices across declared regimes.

An implementation may safely add deterministic calibration if it can prove provenance and ordering semantics, but it is not required to build the full calibration subsystem in Phase 7.

---

# 7. Decision Records

Every `expected_cost_v1` transfer should produce one compact decision record.

Recommended fields:

```text
policy_version
logical_transfer_id
terminal_manifest_fingerprint
consumer_count
failure_domain

observations:
  production_operator_work_ns
  production_wall_ns
  source_bytes
  source_files
  output_bytes
  output_segments

configured_inputs:
  failure_probability_ppm
  price units
  throughput estimates
  deadline slack
  SLO penalty
  retention horizon

alternatives:
  expected_no_persist_nano_usd
  expected_persist_nano_usd
  estimated_recompute_ms
  estimated_write_ms
  estimated_restore_ms

selected_action:
  do_not_persist | persist
reason

actual_if_selected:
  write_bytes
  write_requests
  write_elapsed_ms
  observed_write_bytes_per_second
```

Decision records must be bounded by transfer count. Do not preserve one policy observation for every segment.

---

# 8. Full-Transfer Materialization Mechanism

A core practical issue is that the expected-cost decision occurs after measured producer work is available, but the consumer may already have consumed earlier exchange segments.

A complete recoverable copy cannot be constructed at terminal time unless the transfer bytes remain available somewhere.

## 8.1 Recommended bounded staging design

For `expected_cost_v1`, maintain a **local append-only staging log** for every accepted encoded frame of the logical transfer while normal delivery continues.

Properties:

- one length-prefixed sequential log per transfer or destination;
- no in-memory per-segment index required;
- bounded metadata independent of segment count;
- encoded bytes are preserved exactly as validated on the wire;
- normal final-consumer delivery can still use direct/buffer/spill behavior;
- at terminal, the cost policy can either discard the staged bytes or upload all staged segments in sequence;
- staging is cleaned after the terminal/materialization decision or after recovery/lifecycle cleanup.

This staging write is **real overhead**. It must be separately measured and disclosed. Do not hide it inside spill bytes or claim `expected_cost_v1` has zero normal-case cost.

If implementation finds a cleaner way to preserve a complete transfer for a post-production decision—without unbounded memory or replaying source during the decision—it may use it and document why.

## 8.2 Do not treat a Phase 5 object-store suffix as full recoverability

The Phase 5 adaptive path may write only a suffix after escalation to object store.

A suffix is **not** a complete recoverable transfer for Phase 6's lost range `[0, terminal_count)`.

Phase 7 eligibility for restore must require a complete verified copy of the logical transfer, not merely `object_write_bytes > 0` or an adaptive transition to object store.

---

# 9. Worker-Loss-Surviving Backend for Acceptance

The benchmark needs a backend that survives the exact failure domain Phase 6 injects: one child worker process exits while the coordinator and another worker remain alive.

## 9.1 Shared local fixture is sufficient for v1 research acceptance

Recommended test fixture:

- `LocalCluster` owns one parent-scoped object-store directory;
- all child workers are configured to use that same directory/backend for Phase 7 materializations;
- the directory remains alive after one child process exits;
- a surviving worker can read objects written by the lost worker;
- the parent removes the directory after the cluster/job lifecycle ends.

Call the capability something truthful such as:

```text
worker_process_surviving
shared_local_fixture
```

Do **not** describe this fixture as surviving host loss, AZ loss, or providing production cloud durability.

## 9.2 Durability capability should express failure domain

The current `Ephemeral`/`Durable` distinction may be too coarse for Phase 7.

Prefer an explicit capability/failure-domain representation that can distinguish at least:

```text
worker_local_ephemeral
worker_process_surviving
```

and leaves room for future host/AZ/object-store semantics.

If the existing enum is extended rather than replaced, document exactly what each value means.

## 9.3 Keep production/provider abstraction

The shared local fixture is acceptance infrastructure, not a hard-coded storage architecture.

The mechanism should still use the existing provider-neutral object-store boundary so a real remote backend can be injected later without rewriting cost/recovery logic.

---

# 10. Materialization Receipt

A successful persistent action should yield a compact fact record that can survive loss of the producer's destination worker process.

Recommended `MaterializationReceipt` facts:

```text
logical_transfer_id
original_attempt
terminal_manifest_fingerprint
segments
rows
payload_bytes
backend_name
failure_domain
namespace/version
write_bytes
write_requests
write_elapsed_ms
```

The exact object layout should be deterministic and derivable from the receipt + sequence, so the coordinator does not hold one key per segment.

Recommended object identity basis:

```text
logical transfer identity
+ manifest/content fingerprint
+ segment sequence
```

Avoid making the logical recovery namespace depend only on temporary paths or worker IDs.

The receipt is valid only if:

- the full transfer was written;
- write completion was acknowledged by the backend;
- terminal manifest identity/count/fingerprint matches the retained copy;
- the backend capability survives the failure being planned for.

---

# 11. Terminal ACK / Receipt Provenance

The coordinator must still know which copies exist if the **destination final worker later dies**.

Therefore the materialization decision/receipt cannot exist only inside destination-local final metrics.

Recommended v1 protocol flow:

```text
producer -> destination:
  data frames
  terminal + measured production observation

 destination:
  validates terminal
  makes expected-cost decision
  optionally materializes full staged transfer
  validates resulting receipt

 destination -> producer ACK:
  transfer identity
  materialization decision record
  optional receipt

 producer:
  includes decision/receipt in TaskMetrics returned to coordinator
```

This preserves the Phase 6 recovery window:

> the injected final-worker failure is released only after all producer RPCs/ACKs completed, so the coordinator has the terminal evidence and any receipts before the worker disappears.

If a different propagation path is used, it must prove the same property.

---

# 12. Protocol Versioning

Phase 7 is likely to change both:

- control/config/metrics structs; and
- exchange terminal/ACK payload semantics.

Recommended version discipline:

- bump private control framing from `ADETCP04` to `ADETCP05` for the original receipt schema, and to `ADETCP06` when the execution namespace is added;
- bump exchange wire from v2 to v3 if terminal/ACK bytes change;
- add focused tests proving old versions are rejected before incompatible decoding;
- do not bump unrelated public benchmark result schema unless the artifact schema itself truly changes.

Do not smuggle new terminal metadata into an old version without an explicit compatibility contract.

---

# 13. Recovery Planner v2: Restore Versus Recompute

Phase 7 should extend recovery without broadening the supported failure envelope.

The supported failure remains:

- all original producers completed;
- exactly one final worker becomes unavailable;
- all other finals succeeded;
- gather has no committed successful attempt for the lost partition;
- one bounded recovery episode on a surviving worker.

## 13.1 Recovery facts

For each lost producer-to-destination transfer, recovery has:

- original terminal manifest;
- full lost range;
- replay closure/source snapshot;
- optional validated materialization receipt;
- configured recovery cost inputs.

## 13.2 Per-transfer recovery action

Recommended action enum:

```text
Restore(receipt)
Recompute(replay_closure)
```

Mixed recovery is valid: one producer transfer may restore while another recomputes.

At actual failure time, failure probability is no longer part of the choice—the failure occurred. Compare the direct remaining alternatives:

```text
estimated_restore_cost/time
vs
estimated_recompute_cost/time
```

including applicable SLO penalty.

A retained copy is not automatically preferred merely because it exists.

## 13.3 Restore validation

A restore action must require:

- exact logical transfer match;
- original manifest fingerprint/count/rows/bytes match;
- expected backend/failure-domain capability;
- object sequence coverage `[0, terminal_count)`;
- each restored encoded frame passes existing frame/schema/sequence constraints;
- reconstructed attempt-1 transfer manifest validates against original attempt 0 after changing only attempt identity.

Prefer feeding restored frames through the same recovery store/common final merge path used by recomputation rather than introducing a second final aggregate implementation.

## 13.4 Missing/corrupt materialization

The planner/executor must not trust a receipt blindly.

If the copy is missing/corrupt before recovery begins, either:

- safely fall back to recomputation **only if** the implementation can re-plan before any ambiguous partial restore has contaminated the recovery attempt; or
- fail the recovery episode cleanly and return no partial result.

Pick one explicit behavior and test it. Do not silently mix corrupt restored data with recomputed data.

---

# 14. Materialization Catalog Scope

Phase 7 does not require a persistent distributed catalog service.

A topology-bounded in-memory fact set is sufficient for v1:

```text
logical transfer -> terminal manifest + decision + optional receipt
```

The coordinator obtains facts from successful producer task replies. Workers retain only the local/session state needed to execute the current job.

Do not build cross-job reuse, TTL policy, persistent coordinator metadata, or a new metadata database in this phase.

---

# 15. Cleanup and Retention Lifecycle

Expected-cost materialization creates storage that may outlive the worker that wrote it. Cleanup ownership therefore must be explicit.

Requirements:

- materialization objects are scoped to one logical job/transfer and cannot collide across jobs;
- successful job completion releases unneeded materializations after final output commit;
- failed/unrecoverable jobs do not leak unbounded object data across subsequent benchmark jobs;
- cleanup must still be possible when the original writer process is dead;
- cleanup failure is observable and has an explicit policy; it must not corrupt a successfully committed job result;
- acceptance should prove the parent/shared fixture drains after job/cluster teardown.

For the local research fixture, parent-owned directory teardown may provide the final safety net, but normal successful cleanup should still exercise the intended lifecycle where practical.

---

# 16. Metrics and Accounting

Keep accounting dimensions separate.

## 16.1 Existing exchange delivery metrics

Preserve:

- network rows/batches/bytes;
- direct/buffer/spill/object delivery metrics;
- Phase 5 adaptive summary;
- wait/pressure metrics.

## 16.2 New Phase 7 materialization metrics

Add a compact summary covering a useful subset of:

```text
expected_cost_decisions
persist_selected
persist_skipped
materialized_transfers
materialized_segments
materialized_payload_bytes
staging_write_bytes
staging_read_bytes
object_write_bytes
object_write_requests
object_write_elapsed_ms
object_read_bytes
object_read_requests
object_read_elapsed_ms
restored_transfers
restored_segments
restored_payload_bytes
recomputed_transfers
recomputed_segments
recomputed_payload_bytes
estimated_cost totals by alternative
actual cost proxy totals where measurable
```

Do not double-count durable-copy bytes as normal exchange-delivery bytes.

## 16.3 Decision summaries

Keep full decision records bounded by transfer count. A job-level summary should make it possible to answer:

1. Why was each transfer persisted or skipped?
2. Which values were measured versus configured?
3. Which retained copies actually existed at the failure boundary?
4. Which lost transfers restored versus recomputed and why?
5. How much extra normal-case I/O did staging/materialization add?
6. Did the outcome match the independent oracle?

---

# 17. Benchmark Adapter and Configuration

Extend `ade-bench` narrowly.

Recommended options:

```text
exchange_mode=adaptive
adaptive_policy=threshold_v1 | expected_cost_v1

cost_failure_probability_ppm=...
cost_compute_nano_usd_per_cpu_ns=...
cost_source_read_nano_usd_per_byte=...
cost_source_read_nano_usd_per_request=...
cost_object_write_nano_usd_per_byte=...
cost_object_write_nano_usd_per_request=...
cost_object_read_nano_usd_per_byte=...
cost_object_read_nano_usd_per_request=...
cost_object_capacity_nano_usd_per_byte_ms=...
cost_expected_retention_ms=...
cost_source_read_bytes_per_second=...
cost_object_write_bytes_per_second=...
cost_object_read_bytes_per_second=...
cost_deadline_slack_ms=...
cost_slo_penalty_nano_usd_per_ms=...
cost_min_expected_savings_nano_usd=...
```

This exact list may be simplified if the implementation can satisfy the model with fewer parameters. Avoid an enormous configuration surface solely for theoretical completeness.

Requirements:

- `threshold_v1` options continue to parse exactly as before;
- expected-cost options are rejected unless the expected-cost policy is selected;
- duplicate/unknown options remain errors;
- effective engine metadata records all model inputs and their units;
- the backend/failure-domain used for Phase 7 is recorded;
- snapshot/lineage remains available when expected-cost recovery needs it;
- independent oracle ownership remains in `ade-bench`.

Default engine behavior should remain unchanged unless there is a strong explicit reason to alter it.

---

# 18. Recommended Module/File Changes

This is a likely map, not a frozen prescription.

```text
crates/ade-distributed/src/
  adaptive.rs        preserve threshold_v1; select policy version cleanly
  cost.rs            new pure fixed-point expected-cost model/decision records
  retention.rs       staging/materialization mechanism + truthful capabilities
  exchange.rs        terminal observation, ACK receipt, complete-copy validation
  lineage.rs         reuse terminal fingerprint/identity contracts
  recovery.rs        restore/recompute recovery action planning
  worker.rs          producer observations, restore/recompute execution
  coordinator.rs     receipt validation/catalog facts, recovery action orchestration
  protocol.rs        versioned payloads as needed
  metrics.rs         compact Phase 7 summaries
  lib.rs             configuration/public boundary

crates/ade-bench/src/
  ade_distributed.rs expected-cost options/effective config/shared backend fixture

scripts/
  phase7-acceptance.sh
  phase7-verify.py

docs/adr/
  0014-expected-cost-materialization-v1.md

docs/reports/
  phase-7-implementation-report.md
```

Update `README.md` and `AGENTS.md` where the accepted phase boundary/configuration changes.

Do not create empty future crates or placeholder services.

---

# 19. Focused Unit and Integration Testing

Testing should be strong but not excessive. Favor tests that protect architectural boundaries and known failure modes.

## 19.1 Cost-model unit tests

Cover:

- deterministic same-input same-decision behavior;
- persist/no-persist cases;
- exact tie behavior;
- minimum-savings margin;
- probability endpoints 0 and 1,000,000 ppm;
- deadline/SLO penalty flip case;
- unit conversion and checked arithmetic boundaries;
- invalid/overflowing configuration rejection;
- explicit provenance/serialization round trip where applicable.

Do not create thousands of near-identical numeric cases.

## 19.2 Materialization mechanism tests

Cover:

- full staged transfer can be uploaded and reconstructed;
- empty transfer receipt;
- staging does not retain unbounded metadata;
- Phase 5 object suffix is not accepted as a full receipt;
- wrong logical ID/fingerprint/count rejects receipt;
- cancellation/timeout/poison behavior remains contained;
- cleanup removes job-scoped objects.

## 19.3 Recovery tests

Cover:

- restore selected when valid and cheaper;
- recompute selected when no valid copy;
- recompute selected when valid copy exists but restore is more expensive/slower;
- mixed restore + recompute closure set;
- corrupt/missing object behavior;
- attempt-1 manifest exactly validates against attempt 0;
- unaffected partition reuse and gather attempt fencing remain intact.

## 19.4 Protocol tests

Cover old control/exchange version rejection if schemas changed.

## 19.5 Regression tests

Run focused regressions for:

- five static modes;
- legacy `threshold_v1` adaptive behavior;
- Phase 6 recomputation-only worker-loss recovery;
- no-default-features/default-library surface where research hooks remain feature-gated.

---

# 20. Phase 7 Acceptance Experiment

Phase 7 exit is not satisfied by “the cost function unit tests pass.” The policy must be falsifiable against actual executions.

## 20.1 Predeclare a deterministic workload

Use the existing synthetic `GroupBySum` path and independent oracle, with enough data to produce multiple segments per producer/destination.

Keep fixed:

- generator seed;
- dataset manifest/fingerprint;
- worker count for each declared case;
- task/exchange limits;
- release build identity;
- child-process isolation;
- source/config identity.

## 20.2 Compare against real baselines

At minimum include:

1. **never worker-loss-surviving persist** — e.g. local/static or expected-cost configuration guaranteed to skip;
2. **always worker-loss-surviving persist** — same complete-copy mechanism forced for baseline measurement;
3. **legacy `threshold_v1`**;
4. **`expected_cost_v1`**.

Do not compare only against a strawman threshold deliberately configured to lose.

## 20.3 Regime suite

Use a small predeclared suite spanning meaningfully different economics. Recommended examples:

### Regime A — low failure probability / expensive storage

Expected outcome:

```text
expected_cost_v1 skips full durable materialization
```

and avoids most durable-write cost/overhead.

### Regime B — high failure probability / expensive recompute / cheap fast storage

Expected outcome:

```text
expected_cost_v1 persists
```

Then inject the existing real final-worker process loss. Recovery should use the retained copy for eligible transfers and avoid source replay where restore is selected.

### Regime C — copy exists but restore is expensive/slow

Force or baseline-create a valid worker-loss-surviving copy, then configure recovery economics such that recomputation is cheaper.

Expected outcome:

```text
recovery planner recomputes despite copy availability
```

This proves materialization facts and recovery policy are separate.

### Regime D — tight deadline slack

Use a scenario where SLO penalty changes the decision relative to a relaxed-deadline run.

This validates that deadline slack is not a dead configuration field.

The final suite may use three regimes if that is sufficient to demonstrate all required dimensions without excessive runtime.

## 20.4 Evaluate expected cost with observed outcomes

Avoid a circular acceptance test that declares success because the policy's own estimates say it was cheapest.

For each regime, retain actual observations such as:

- operator work proxy;
- source bytes reopened during recompute;
- staging bytes/time;
- object writes/reads/requests;
- actual materialization/restore elapsed time;
- total job wall time;
- independent correctness result.

A useful evaluation is a paired success/failure experiment under configured `p`:

```text
empirical_expected_score =
    (1 - p) * observed_no_failure_score
  + p       * observed_failure_score
```

where `observed_*_score` is computed from the declared price model using **actual observed quantities**, plus actual SLO penalty where applicable.

This is not claiming a production failure distribution. It is a controlled research evaluation under the declared probability.

## 20.5 Phase 7 win criterion

Predeclare a clear win criterion before running final acceptance. A reasonable criterion is:

- all runs match the independent oracle;
- every expected-cost decision is traceable from retained inputs;
- the expected-cost policy chooses differently across the declared regimes for the intended economic reasons;
- on the aggregate predeclared suite, its empirical expected score is lower than at least one meaningful static/threshold baseline, preferably both always-persist and never-persist where the suite is designed to require both behaviors;
- normal-case regression from staging/decision overhead is measured and remains within a predeclared acceptable bound, or the report explicitly records a failed Phase 7 exit rather than moving the bound after seeing results.

Do not use a single best iteration. Keep sample counts small but credible and use release builds.

---

# 21. Negative Acceptance / Adversarial Cases

Include a small set of high-value negative checks:

- default ephemeral local object backend is not accepted as worker-loss-surviving;
- forged/mismatched receipt fingerprint is rejected;
- missing/corrupt retained object triggers the documented safe behavior;
- cost arithmetic/configuration overflow is rejected before execution;
- old protocol version is rejected if version changed;
- worker-loss recovery still returns no partial output on unrecoverable failure;
- materialization cleanup does not depend on the dead worker process.

Avoid turning Phase 7 into a broad chaos-testing campaign.

---

# 22. Workspace Validation

During implementation, run focused tests after each coherent change rather than the full workspace repeatedly.

Before final handoff, run the available final gate, typically:

```bash
cargo fmt --all --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo check -p ade-distributed --no-default-features --locked --offline
cargo test --workspace --all-features --locked --offline
cargo build --workspace --locked --offline
bash scripts/smoke.sh
cargo build --release --workspace --locked --offline
git diff --check
```

If the environment cannot run a command, record that limitation in the implementation report rather than claiming it passed.

Do not put noisy absolute performance gates into CI. Keep Phase 7 economic/performance acceptance as controlled release evidence unless/until measurement stability is established.

---

# 23. Documentation and ADR Requirements

Create an ADR for the accepted Phase 7 mechanism/policy boundary. It should record at least:

- decision granularity;
- cost units/formula/provenance;
- staging strategy;
- worker-loss-surviving backend/failure domain;
- terminal receipt propagation;
- restore-versus-recompute planning;
- protocol version changes;
- metadata/resource bounds;
- limitations.

The implementation report should include:

- baseline and final source identity;
- exact implemented cost model;
- measured versus configured fields;
- materialization receipt contract;
- storage failure domain actually proven;
- recovery behavior and limitations;
- acceptance suite definition before results;
- raw result/artifact locations;
- actual decisions and empirical scores;
- normal-case overhead;
- failed experiments/fixes;
- deviations from this plan;
- Phase 8 handoff.

Update `AGENTS.md` so it no longer says Phase 6 is the active boundary after Phase 7 is complete.

---

# 24. Explicit Non-Goals

Phase 7 does **not** need:

- segment-level source replay;
- dynamic repartition/split/coalesce;
- producer-stage worker-loss recovery;
- multiple simultaneous worker losses;
- recursive retry loops;
- dynamic worker replacement;
- coordinator HA or persistent job catalog;
- production S3/GCS/Azure deployment;
- remote shuffle service;
- cross-job materialization reuse;
- automatic cloud price discovery;
- learned failure probability;
- learned cost model;
- scheduler placement optimization;
- joins or alternative aggregation algorithms;
- SQL/DataFrame frontend;
- streaming/checkpoints/state;
- external transactional sink protocol.

Do not add these as “cleanup.”

---

# 25. Phase 7 Exit Criteria

Phase 7 is complete only if all of the following are true:

## Correctness

- existing supported workloads still match the independent oracle;
- static exchange modes remain correct;
- legacy `threshold_v1` remains independently selectable and correct;
- Phase 6 recomputation recovery still works;
- restore/recompute paths preserve logical identity, manifest integrity, and exactly one committed final attempt.

## Policy

- `expected_cost_v1` is an explicit policy, not a renamed threshold;
- decision inputs have truthful provenance;
- arithmetic is deterministic and bounded;
- decisions expose alternatives and reasons;
- failure probability, consumer count, storage economics, production/source cost, and deadline slack have real modeled roles or an explicitly documented reason for any intentionally deferred component.

## Materialization

- a selected materialization is a **complete** verified transfer copy;
- the copy demonstrably survives the Phase 6 worker-process loss domain;
- the coordinator retains a receipt even if that destination worker later exits;
- suffix-only Phase 5 object data is not mislabeled as complete recovery state.

## Recovery

- recovery can choose restore or recompute using valid facts;
- at least one acceptance case restores after real process loss;
- at least one case recomputes because persistence was skipped or restore is less favorable;
- corrupt/missing retained data follows the documented safe behavior;
- no unsupported failure envelope is silently broadened.

## Boundedness/lifecycle

- control metadata is bounded by topology/source descriptors rather than segment count;
- staging/object I/O is streaming/sequential rather than unbounded in memory;
- cancellation/poisoning behavior remains contained;
- retained data has explicit cleanup ownership;
- no leaked shared fixture accumulates across jobs.

## Evidence

- final workspace gate passes where the environment permits;
- controlled release acceptance spans multiple failure/storage regimes;
- expected-cost decisions and empirical outcomes are both retained;
- the policy wins the predeclared suite criterion without moving the target after measurement;
- normal-case overhead is measured and reported.

---

# 26. Phase 8 Handoff

Phase 7 should leave Phase 8 with:

- an interpretable, replaceable expected-cost policy layer;
- explicit cost input provenance and fixed-point units;
- full-transfer materialization facts/receipts;
- a tested worker-loss-surviving storage capability;
- restore-versus-recompute recovery actions;
- measured cost/outcome records;
- unchanged logical partition topology.

Phase 8 can then focus on changing partition topology and skew handling rather than having to repair an ambiguous materialization/recovery contract first.

The main limitations to carry forward should remain explicit:

- transfer-level rather than segment-level materialization economics;
- whole-producer source replay primitive;
- one final-worker-loss recovery episode;
- static 2–4 worker topology;
- configured rather than operationally learned failure probability/throughput estimates;
- shared-local worker-process-survival fixture rather than production remote durability;
- no cross-job catalog/reuse.

## Implementation reconciliation

The original implementation accepts producer-side staging and post-destination-
ACK complete-copy upload, with receipts in producer control replies before the
failure gate. The independent review added two contract corrections: SLO penalties
use whole sequential paths and recovery carries elapsed time across closures;
complete-copy keys carry a coordinator-created execution UUID, independent of
logical transfer IDs. These control schema changes advance framing to ADETCP06;
exchange wire stays v2. The complete-copy prefix is
`complete-v1/{execution-uuid}/job-J/`, and cleanup/restore validate the same
namespace. Missing/corrupt objects still fail before final aggregation, with no
fallback retry.

The decision and verifier expose `put_head_get_proxy_v1` as the request-cost
scope: per-transfer writes use PUT prices and restores use HEAD+GET prices.
Cleanup LIST/DELETE costs are excluded, so this is not a full provider lifecycle
request-cost objective. The report qualifies the older ignored benchmark evidence
and the bundle script packages future successful clean-checkout runs for sharing.
