# Adaptive Dataflow Engine — Phase 14 Implementation Plan

## Unbounded sources and progress: bounded Kafka-first streaming foundation v1

**Status:** Implemented — `COMPLETE_BOUNDED_STREAMING_V1`. The original candidate-7 archive remains immutable and is historical-qualified after independent review found two evidence P2s; a fresh source-bound 30-case campaign and strict portable verifier resolve both (see the 2026-10-29 remediation report). This does not claim full PRD streaming, checkpoints or exactly-once.
**Prepared:** 2026-10-28 (America/Los_Angeles).  
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine  
**Inspected main HEAD:** `9fc6e35643b2d758ed5a06d00c4d4896f8ab2601` (Phase 13 independent-review remediation).  
**Authority:** `AGENTS.md` → `docs/product/prd.md` → `docs/architecture/architecture.md` → this scoped plan → relevant ADRs → reports and actual code. A newer HEAD requires a documented delta review before implementation.

## 0. Product interpretation and scope decision

The PRD Phase 14 (`docs/product/prd.md`, lines under “Phase 14 — Unbounded Sources and Progress”) calls for **Kafka first, offsets, logical timestamps, watermarks/frontiers, timers, and long-lived backpressure**. The architecture (§3.5, §18, INV-1–10) calls for the same runtime foundation across bounded and unbounded data, with explicit progress and bounded resources. These capabilities need source and time semantics before they can safely support keyed state, checkpointing, exactly-once sinks or incremental computation.

**Recommended bounded v1:** a default-off, genuinely unbounded **Kafka → ADE-native stateless transform → bounded streaming sink** execution path with explicit fixed-partition offsets, event/processing-time progress, observable timers, cancellation and sustained backpressure. The workload may be stopped by the operator or a test harness, but polling an empty Kafka partition must never be mistaken for source EOF. This is a functional streaming foundation, not a disguised finite batch replay, and not a general streaming SQL engine.

**Completion vocabulary:** `PARTIAL` if a mandatory gate is unverified/failed; `COMPLETE_BOUNDED_STREAMING_V1` only if G0–G9 pass against a real broker and documented live native execution; `FULL_PRD_PHASE14_COMPLETE` is not claimed. Phase 15–17 requirements remain separate.

### Current source reality and dependencies

- **Phase 13** is `COMPLETE_BOUNDED_REMOTE_V1` in `docs/reports/phase-13-implementation-report.md` and ADR-0020. It is a dedicated remote-object static aggregate executor, **not** a reusable remote exchange plugin or proof of physical elasticity. Review remediation at the inspected HEAD records source-bound local verification and an explicit raw-evidence portability limit. Do not re-label historical Phase 13 G0–G8 samples.
- **Phase 12** is a bounded local durable-operations path, not production HA. **Phase 10** remains `PARTIAL`: fixed 2–4-process membership despite successful local leased-scheduler timing guards. Neither is a prerequisite for one explicitly fixed Kafka source reader, and this phase must not silently widen their claims.
- **Phase 11** has strict versioned `ade-physical::PlanV1` admission for bounded local/distributed aggregate/join targets. `crates/ade-physical/src/lib.rs` currently uses manifest-listed immutable files; do not pretend its v1 source model already represents Kafka or silently change its serialized hashes or parsing.
- **`ade-exec`** already offers `BatchSource` and `CancellationToken` (`crates/ade-exec/src/lib.rs`), but `next_batch -> Option<RecordBatch>` treats `None` as terminal. Introduce a distinct streaming poll/progress contract so *temporarily empty* and *permanently finished* are not conflated. Reuse ADE-owned Arrow/native stateless execution and accounting rather than creating a parallel query engine.
- Existing `ade-bench`, `ade-distributed`, `ade-ops`, `ade-frontend`, `ade-query`, legacy protocols, manifests and result schema v1 retain their defaults and acceptance contracts. No Kafka or streaming dependency becomes a default requirement for existing binaries.

### Implemented admission refinements (ADR-0021)

The maintained client cannot bound native decompression using its compressed
fetch-response limit alone. V1 therefore requires trusted immutable topic history
with `compression.type=uncompressed` and `max.message.bytes<=32768`, verifies
current effective configuration before assignment and periodically, and refuses
other configurations. The client cannot attest historical topic incarnation or
undetected truncate-and-regrow; unchanged incarnation/history is an explicit
operator assumption. These restrictions preserve boundedness rather than expand
source/recovery claims. Config and CA inputs are bounded regular files.

Backpressure stops ADE partition-queue polling while allowing bounded native
prefetch to fill; control/statistics remain serviced. Native pause/resume was
rejected after reproducible destruction hangs, retained as negative evidence.
One in-flight record, zero downstream channel and bounded output remain. The
original G0–G9 criteria are unchanged; no missing broker witness is promoted.

## 1. Explicit v1 support and refusal matrix

| Dimension | Mandatory v1 | Explicitly refused/deferred |
|---|---|---|
| Source | One Kafka-compatible cluster, one topic, 1–4 **explicitly assigned, fixed** partitions; real broker in acceptance | Consumer-group balancing/rebalances, arbitrary topic discovery, broker failover guarantee, connectors other than Kafka |
| Start/replay | Explicit absolute starting offset per partition; bounded poll, distinct seen/forwarded positions; manual restart from supplied offsets | Durable job offset checkpoint, exactly-once, automatic failover/recovery, arbitrary consumer-group committed-offset adoption |
| Data | Strict versioned JSON event payload or similarly explicit schema (`key: u64`, `value_i64: i64`, `event_time_ms: i64`) converted to Arrow batches; independently check content/identity | Schema Registry, arbitrary Avro/Protobuf/DDL, joins, windows, keyed aggregation, late-data corrections |
| Native work | At least a demonstrated ADE-owned stateless filter/projection (or equivalently narrow existing native operation), with backpressured Arrow batches | A stand-alone Kafka echo loop claimed as ADE execution; DataFusion runtime fallback; general SQL streaming |
| Progress | Offset vector and logical event timestamp; per-partition monotone watermark and global min frontier with `unknown` if any assigned partition has no valid bound | Timely-style nested/cyclic frontiers, source-independent completeness guarantees, idle-timeout heuristic advertised as correctness |
| Timers | Bounded in-process processing-time and event-time timer registration/firing with ordered, observable events | Durable keyed timers, window triggers, restored timer state, output semantics coupled to checkpoints |
| Sink | Explicit bounded streaming sink (e.g. NDJSON with stable metadata), delivery acknowledgment only after successful emission | Transactional/exactly-once external sink, durable publication or no-duplicate restart promise |
| Execution | One local process, explicit cancellation/finite test stop, no data on idle, controlled slow-sink backpressure | Multi-worker streaming, repartition, streaming remote shuffle, elastic membership, remote operations, HA |
| Security | Bounded credentials/config and redacted diagnostics; TLS required for non-loopback brokers unless a justified explicit dev override | Automated cloud provisioning, production credential management, public unauthenticated control plane |

## 2. Semantic invariants to design and prove before coding

### 2.1 Source ownership and offset truth

1. Canonical source identity includes broker/cluster identity where available, topic, assigned partition set, user-supplied start offsets, effective schema/transform hash and process/session epoch. Validate partition/topic metadata, offset range and configuration **before any output**. Do not invent a stable topic UUID if the client/broker does not expose one; document the remaining identity caveat.
2. Disable consumer auto-commit and auto-offset-store for this experiment. Prefer manual fixed assignment rather than subscription/rebalance; initialize each partition at its explicit absolute offset. An empty poll emits `Pending`, never EOF. The caller deliberately chooses an operator stop; the broker's high watermark is **not** the last processed offset.
3. Track separately **read/seen offset** and **successfully emitted/forwarded offset** for each partition. Advance the forwarded cursor only after successful sink acknowledgment; emit an honest `next_offset` as a diagnostic replay suggestion, **not** a durable checkpoint. A crash can replay data, and no durable external offset commit is made.
4. Kafka offsets can have legal gaps (retention/compaction/control records). Require increasing delivered offsets within the fixed assignment epoch, but **do not require arithmetic contiguity** or equate a gap with data loss. Reject backward/out-of-epoch records and overflow, and distinguish broker truncation/offset-out-of-range from temporary empty polling. Never skip malformed events silently; fail closed or expose an explicitly gated dead-letter policy only in a future phase.
5. Broker disconnect, partition disappearance, retention drift, unexpected reassignment and poll/decode errors must not produce success or silent offset advancement. On failure, stop cleanly and report the last **confirmed forwarded** vector; no automatic replay in v1.

### 2.2 Logical time, frontiers and timers

1. Each input record carries a parsed event timestamp (`event_time_ms`) and distinct source offset/ingest metadata. Never use Kafka offset or wall-clock arrival as a substitute for event time. Support out-of-order event timestamps without rewinding established progress.
2. Define a configurable, nonnegative allowed-out-of-orderness bound. Per-partition candidate watermark is `max_event_time_successfully_forwarded - allowance` using checked arithmetic; it never decreases. **Only successfully forwarded records** may advance the published downstream watermark. Global frontier is the minimum across all assigned partitions, and remains `unknown` until every partition has an observed bound. Unit tests must cover asymmetric partitions, empty partitions, equal timestamps, negative times, overflow and reordered records.
3. A watermark is a **declared event-time progress policy**, not a guarantee Kafka will never later deliver an older event. Stateless v1 must not silently drop a late record: forward it with clear late/progress diagnostics. Idle polls alone must never exclude a partition from the global minimum or assert completion. If manual idleness is introduced, it must have an explicit source/policy contract, reactivation rules and separate negative tests; otherwise defer it.
4. Introduce an in-process timer queue with identity, owner epoch, deadline and kind. Processing timers use a monotonic testable clock; event-time timers fire only after the published global frontier reaches the deadline (define inclusive comparison). Fired timers have deterministic tie-breaking and fire at most once within a live session. Limit count, registration rate and retained bytes. Timer events are *observability/control events only* in Phase 14; they cannot mutate keyed/window state or imply durability.
5. A distinct progress event is needed for idle/pending/stop, with bounded serialization; no unbounded event log or inferred source completion. Cancellation must release the consumer, sink and timers promptly.

### 2.3 Backpressure, memory and cancellation

- Poll Kafka only as downstream demand allows. Bound Arrow batch rows/bytes, decoded Kafka payload size, channel depth, client prefetch/fetch queue, pending timer registrations, sink output buffer, total in-flight bytes and optional duration/request retry budgets. Verify the Kafka client's **internal** fetch limits, not just an ADE-facing channel limit. Advertise a managed/queue bound rather than an unprovable process RSS cap.
- A blocked or slow sink must **propagate demand upstream**; never drop records, advance forwarded offsets, spin on empty polls or accumulate unbounded user-space backlog. Broker-side backlog is expected, and must be observed separately from local queue occupancy. If `pause/resume` is used, its hysteresis and cancellation behavior are explicit.
- Use bounded waiting and a real cancellation wakeup path, including during poll, sink blockage and timers. No detached poller/producer may outlive shutdown and later advance progress. Separate transient client/kernel allocations from ADE-managed retention in metrics.
- Operator-limited stop and SIGINT are not source EOF. Define the final status (`STOPPED_BY_OPERATOR`, `FAILED`, etc.), drained versus discarded in-flight work and which offsets are confirmed. Handle output failure without partial *success* claims.

## 3. Architecture/implementation approach

**WP0 — Baseline and design gate.** Capture clean `git status`, exact HEAD, toolchain and relevant tests. Independently inspect the current Phase 13 remediation/report, Phase 12/10 limits, existing native execution contracts and `AGENTS.md`. Record a small correctness/readiness inventory; block only for a concrete shared P0/P1 invariant that would make this opt-in path unsafe. No Phase 13 raw-data rerun is required just to start Phase 14, but preserve its provenance boundaries.

**WP1 — ADR and contract tests first.** Add `docs/adr/0021-unbounded-source-and-progress-v1.md` with state machine, offset ownership, time/frontier semantics, sink acknowledgment, boundedness and refusal table. Decide a thin `ade-stream` crate/module with real ownership, or an equivalent source/progress module integrated into `ade-exec`; state why. Prefer a new **explicitly versioned streaming admission descriptor** over widening/breaking `PlanV1` v1. Implement deterministic in-memory source/sink/clock traits and unit tests independent of Kafka.

**WP2 — Kafka source adapter.** Behind an opt-in `kafka`/`streaming` feature, use a maintained public Kafka Rust client with pinned lockfile. Verify its manual `assign`, start offset, polling, queue and auto-commit controls against the installed version. Fail on missing real broker or security/config violations; do not substitute fixture records while claiming Kafka acceptance. Convert strict validated payloads to Arrow with stable source metadata; enforce payload/batch budgets and distinguish `Pending`, data, failure and stop.

**WP3 — ADE-native live data path.** Wire the new source through ADE-owned stateless execution (filter/projection or comparable native operator) into a bounded sink. Preserve Arrow schema, ordering per source partition and deterministic payload semantics. Do not reuse `BatchSource::None` for temporary idle. If extending native execution reveals a broad refactor, choose the smallest safely reusable operator seam and explicitly record exclusions rather than rewriting batch runtimes.

**WP4 — Progress and timers.** Implement independently testable offset vector, event-time watermark frontier and timer scheduler with structured telemetry. Assert monotonicity after *successful sink emission*, distinguish read versus forwarded offsets, test gaps and restart duplicates, and provide explainable policy/config provenance. Negative tests must cover late data, overflow, unset partition frontier and clock jumps.

**WP5 — Long-running backpressure/failure.** Prove non-terminating idle behavior, a slow and blocked sink for a sustained local window, queue plateau at configured limits, broker backlog, recovery of throughput after unblocking, no drops/reordering within fixed partitions, bounded cancellation and no progress beyond sink success. Faults: broker outage/restart, invalid/missing topic/partition, offset out-of-range, malformed/oversized payload, sink write error, timed-out poll, hard process kill and manual replay from explicit offsets.

**WP6 — Real broker, regression and evidence.** Execute a real separate Kafka-compatible broker (e.g. local Redpanda/Kafka container or operator-provided broker; no paid cloud) with at least two active partitions, out-of-order event times, deliberate quiet partition and skewed/slow sink. Compare the complete emitted record multiset plus per-partition sequence to an independently constructed producer/oracle ledger for a declared finite **test stop condition**; do not equate test finite stop with source EOF. Capture traffic, offset vectors, frontier transitions, timer fire traces, queue high-water marks, errors and lifecycle on clean, slow, restarted and negative runs. Run old workspace/smoke/Phase 10–13 correctness regressions; do not rewrite historical evidence.

**WP7 — Runbook and report.** Add a concrete opt-in CLI/example, operator guide, `docs/reports/phase-14-implementation-report.md`, machine-readable supported/refused matrix and source-bound acceptance evidence. Update README/AGENTS only to truthful final bounded status. Explicitly disclose missing raw artifacts or unavailable broker infrastructure. Use `PARTIAL` if any mandatory real-broker gate is unverified.

## 4. Mandatory independent acceptance gates

| Gate | Witness | Pass condition |
|---|---|---|
| G0 baseline/authority | Recorded source SHA, changed-file review, legacy smoke + relevant prior reports | No accidental regression or hidden broader authorization; Phase10/12/13 claims unchanged |
| G1 Kafka truth | Real separate broker, fixed assignment, recorded producer ledger | Real records received through Kafka; no fixture-only fallback or implicit offset commit |
| G2 offsets/replay | Two-partition start, legal holes, failure/restart and explicit replay | Seen vs forwarded truth; order and no loss during uninterrupted run; duplicates across restart disclosed |
| G3 time/frontier | Deterministic out-of-order/asymmetric/quiet-partition cases | Monotone per-partition watermarks, correct min/unknown frontier, no idle-as-EOF or false completion |
| G4 timers | Fake-clock unit suite + live broker trace | Exactly-once *within one session* firing at/after deadlines; correct tie-break and cap refusals; no restored timer claim |
| G5 live native semantics | Nontrivial filter/projection, Arrow schema and independent oracle | ADE-owned transform actually executes; no frontend/DataFusion fallback, exact expected output |
| G6 backpressure | Sustained slow/blocked sink, queue measurements, resume, idle interval | All local queues/buffers capped, input slows, backlog stays broker-side, resumes without silent loss, finite cancellation |
| G7 faults/security | Broker disconnect/out-of-range, bad/large payload, sink failure, kill, bad TLS/config | Closed failure states, no credential leak, no misleading success/forwarded offsets, bounded resource teardown |
| G8 regressions | Locked workspace fmt/check/clippy/tests, smoke, selected Phase10–13 proofs | Defaults/protocols/legacy schema/bench/oracle and batch operations unchanged; any unavailable gate stated as unverified |
| G9 reproducibility | Exact source/binaries/toolchain, broker client/service versions, commands, config, seed, outputs/hashes | Fresh session can replay small public/contained artifacts; raw inaccessible evidence clearly labeled, no unmeasured speedup/production claims |

**Measurement integrity:** Predeclare configurations and stop conditions; run fresh processes; preserve all successful and failed attempts. Use an independent producer ledger, not the same parser/transform implementation as the oracle. No Phase 14 throughput speedup is required, and performance figures are descriptive unless separately powered and predeclared. A 30–60 second blocked/slow-sink run is appropriate as one local stress witness; unit/property tests must carry the deterministic correctness burden. A memory bound refers to explicitly tracked in-process queues, not RSS.

**Required checks when environment supports them:**

    cargo fmt --all -- --check
    cargo check --workspace --all-targets --locked
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
    cargo test --workspace --all-features --locked
    bash scripts/smoke.sh

Add targeted broker integration and Python evidence-validator commands documented by the implementation. Separate correctness CI (with broker service only if reproducibly provisioned) from noisy performance evidence. Mark failures/ignored tests; never use an unavailable service as a passing gate.

## 5. Stop conditions and later phases

Stop and mark `PARTIAL` or `BLOCKED` if real Kafka integration cannot be witnessed, the source cannot distinguish idle from completion, upstream backpressure cannot be bounded, sink acknowledgment/progress are conflated, or meaningful native execution requires rewriting Phase 11/12/13 contracts. Do **not** broaden scope to paper over those gaps. Report the narrow next fix.

Explicitly deferred: consumer-group rebalances/reassignment, source offset snapshots and consistent checkpoint barriers (**Phase 15**); adaptive checkpoint/state placement (**16**); transactional sinks, end-to-end exactly-once and savepoints (**17**); full unified bounded/unbounded optimizer (**18**); incremental/iterative work (**19–20**); broader connector/catalog ecosystem (**21**); Phase10 physical elasticity, Phase12 HA and Phase13 production cloud/disaggregation debt. The Phase14 bounded foundation should expose honest extension seams, not pre-implement these phases.

## 6. Read-first source map (inspected main SHA)

- `AGENTS.md`; `docs/product/prd.md` §§4,7–9,14,16 Phase14,17,19,23; `docs/architecture/architecture.md` §§3.5,5.3,7–9,13,17–18,21–22,25–31 and INV-1–10.
- `docs/plans/phase-10-implementation-plan.md`, `phase-11-implementation-plan.md`, `phase-12-implementation-plan.md`, `phase-13-implementation-plan.md`; corresponding reports and `docs/reports/repo-wide-readiness-2026-10-26.md`.
- `docs/adr/0017-morsel-scheduler-v1.md`, `0018-physical-ir-and-frontend-v1.md`, `0019-durable-batch-operations.md`, `0020-remote-disaggregated-shuffle-v1.md`; `docs/phase-13-operations-guide.md`.
- `crates/ade-exec/src/{lib.rs,physical.rs,memory.rs,partial.rs}`, `crates/ade-physical/src/lib.rs`, `crates/ade-distributed/src/{lib.rs,coordinator.rs,protocol.rs,remote/mod.rs}`, `crates/ade-frontend/src/execution.rs`, `crates/ade-bench/src/{engine.rs,runner.rs}`, workspace Cargo manifests, CI and `scripts/smoke.sh`.
- Kafka client API reference to reverify at implementation time: https://docs.rs/rdkafka/latest/rdkafka/consumer/trait.Consumer.html (manual assignment/seek/commit behavior); official Kafka documentation for broker offset and partition semantics. API compatibility is version-specific, so do not select the library only from this reference.

**This plan is a bounded proposal, not a substitute for the independent review.** The review must independently derive obligations from PRD, architecture, code, source semantics and actual failure evidence and is authorized to find defects in this plan itself.
