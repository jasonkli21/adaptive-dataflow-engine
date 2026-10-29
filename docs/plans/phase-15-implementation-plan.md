# Adaptive Dataflow Engine — Phase 15 Implementation Plan

## Keyed state and checkpointing: bounded, recoverable streaming v1

**Prepared:** 2026-10-29 (America/Los_Angeles)
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine
**Inspected `main`:** `dc767c7c4e9e08c7b79ae034411fd9218f30a8e6` (`fix(phase14): validate negative acceptance evidence`). Reinspect HEAD and document its delta before implementing.
**Status:** `COMPLETE_BOUNDED_STATE_V1`, explicitly authorized by the user on 2026-10-29; candidate8 local G0–G11 evidence accepted in the implementation report. This document alone never supplies authorization.
**Authority:** `docs/product/prd.md` > `docs/architecture/architecture.md` > active phase plan > accepted ADRs > reports/current code, with `AGENTS.md` governing agent workflow. If these disagree, reconcile explicitly; this plan cannot silently relax an invariant.

## 0. Why this phase, and what counts as completion

The PRD Phase 15 names five outcomes: **keyed/window state, tiered state, consistent checkpoints, restore, and state redistribution on rescale**. Architecture §18 treats state as recoverable materialized computation; §18.3 requires a consistent cut of progress with state. INV-1–10 require stable logical identity, bounded resource ownership, honest replayability, observable state decisions, frontend independence, and lossy worker-local tiers unless promoted. The product's *adaptive* checkpoint cadence/placement/full-versus-incremental selection belongs to Phase 16, and transactional sinks, savepoints, upgrades, and operational pause/resume belong to Phase 17.

**Accepted bounded v1:** an **opt-in, local, single-process but genuinely Kafka-fed** stateful pipeline supporting `key: u64` event-time **tumbling-window sum/count**, bounded hot memory with **real local-disk state eviction/read-through**, fixed-policy **atomic full checkpoints** of state+input progress+window deadlines, **checkpoint-based restart**, and **offline key-shard redistribution across 1–4 logical state owners on restore**. Use shared ADE-native Arrow/stateless kernels for admission; do not fork a parallel engine. Window results go to a backpressured, nontransactional sink with explicit at-least-once replay caveats. Logical owners must be distinct state-store instances with demonstrable key transfer, not a merely relabeled owner count.

This is a *bounded research milestone*, not a claim of full scalable/distributed checkpointing: no remote durability, live rescale, physical worker handoff, concurrent unaligned barriers, transactional sink, generalized state backend, or general streaming SQL. The PRD's redistribution requirement is met **only for logical owners at a quiesced restore boundary**; multi-process/host rescaling remains **PARTIAL** until independently implemented. Do not call the full PRD Phase 15 complete. Report `COMPLETE_BOUNDED_STATE_V1` only when all gates below pass; otherwise `PARTIAL` or `BLOCKED` with honest reasons.

### Reality of the inspected tree

- `crates/ade-stream/src/lib.rs` owns `StreamConfigV1`, `Source`, `Poll::{Data,Pending,Stopped}`, `Engine::prepare/acknowledge`, source `seen` versus sink-confirmed `forwarded`, declared watermarks and the min/unknown frontier. `Engine::prepare` can advance `seen` before an acknowledgment; an unconfirmed record **must never** be included in a checkpoint cut.
- `crates/ade-stream/src/runtime.rs` is a single-owner demand loop with one pending NDJSON frame, cooperative nonblocking sink writes, no downstream channel, bounded stalls, health checks and cancellation. The existing `timers.rs` timers are session-local diagnostic events, **not durable keyed triggers**.
- `crates/ade-stream/src/kafka.rs` uses manual `rdkafka` assignment at absolute offsets; no group commits, automatic offsets, or background durable source ownership. `StreamConfigV1` hashes its whole descriptor including `starts`; a restored job needs a separate stable **logical job/transform identity** that does *not* confuse initial starts with checkpoint-resume starts.
- Phase 14 (`docs/adr/0021-unbounded-source-and-progress-v1.md`, `docs/reports/phase-14-implementation-report.md`, `docs/reports/phase-14-review-remediation-2026-10-29.md`) is `COMPLETE_BOUNDED_STREAMING_V1` with independent negative-evidence remediation. The final review source is the HEAD above; the fresh 30-case broker campaign is source-bound, not a Phase 15 correctness witness. Preserve immutable Phase 14 archives and the historical-qualified original archive.
- Phase 10 is still **PARTIAL** for physical membership; Phase 12 local operations do **not** confer production HA; Phase 13 remote S3 shuffle is a dedicated static path, not a durable streaming-state backend. Do not claim these debts closed or import their assumptions silently.

## 1. Support/refusal and minimal public contract

| Dimension | Must work in bounded v1 | Refuse or label deferred |
|---|---|---|
| Admission | Separate strictly versioned stateful descriptor/CLI or feature, default-off; identical Kafka security/immutable-history and fixed-topic/partition requirements as Phase 14 | Silent widening of `StreamConfigV1`, `ade-physical::PlanV1`, existing batch CLI, Kafka groups or implicit offset reset |
| State operators | UInt64 key; checked Int64 sum and UInt64 count per **event-time tumbling window**; deterministic output identity and close order | General keyed user functions, joins, sessions/sliding windows, changelogs, iterative dataflow |
| Time | Signed event timestamps, Euclidean window boundaries, global frontier; explicit late-to-closed-window action and audit count; no idle-only advancement | Treat watermark as true completeness, silent late drop, wall-clock-as-event-time |
| Tiering | Configured hot-memory limit plus real bounded local-disk read-through/eviction; explicit disk quota; cancel/error cleanup | Memory-only map with snapshot file called “tiering”; unbounded compaction/cache, remote state or shared distributed storage |
| Checkpoint | Full immutable, integrity-checked local checkpoint at quiescent cut of state, next offsets, progress/watermarks and event-time window obligations; atomic current-pointer publication | Incremental/async adaptive snapshots, unaligned distributed checkpoints, durability claimed without fsync/verified storage |
| Restore | Exact compatible job/schema/hash/topic/cluster/partition policy and historical-source checks; manual restart from saved next offsets; no output before validation | Automatic HA, retention-lost replay, unverified topic incarnation, group offsets, silently reset state |
| Rescale | 1↔2↔4 *logical local state owners* on restore, stable virtual shards, no lost/duplicated keys, deterministic hash/serialization | Live physical membership, cross-host migration, Phase 10 worker elasticity, savepoint/upgrade API |
| Sink | Backpressured window records and input/late receipts; at-least-once outputs across crash/replay, stable output IDs for downstream dedupe | Exactly-once, atomic external offsets/sink commit, retract/update after window close |
| Security/ops | Local private checkpoint roots, no symlink escape/path traversal, redacted logs, bounded metadata/cleanup, operator-visible recovery refusal | Production multi-tenant state service, cloud secrets, process-level HA |

Use a schema whose fields distinguish: immutable **logical job identity** (`job_id`, schema/operator/window/late policy/hash version, source cluster/topic/partition inventory), runtime **attempt/session identity**, and restart **checkpoint generation**. A `--restore` path must take precedence over configured initial offsets, but only after strict compatibility validation; do not silently combine CLI offsets and checkpoint offsets. If adding a new stateful CLI, leave the original Phase 14 binary behavior intact. Persist a canonical codec version and canonical numeric byte order; avoid Rust `DefaultHasher`/unstable iteration as an ownership or serialization contract.

## 2. Required correctness invariants

### 2.1 State, windows, and progress

1. The same `(key, event-time window)` owns one accumulator across source partitions. Window bounds use `div_euclid` / `rem_euclid` and checked math for negative event times, window-end overflow, sum/count overflow. Document whether close uses `frontier >= window_end` (recommended). The global frontier is **unknown** until every assigned partition has a watermark. A quiet partition cannot be declared idle without a new, separately proved contract.
2. A record whose target window has *already closed* must never recreate mutable state. Emit an explicit late/drop receipt and metric, or fail closed under an explicit policy; do not silently discard it. Do not conflate the preexisting Phase 14 `late` diagnostic (event timestamp below a partition watermark) with the **closed-window** decision. At equality/boundary, assert the chosen policy in tests. A record for an open window must be processed even when it is otherwise out of order.
3. State changes, input receipt delivery, and progress advancement must have a single-owner, documented commit/acknowledgment order. The sink can stall/fail/partially write: no checkpoint may include any unacknowledged input or undelivered close record. Define a finite queue for potentially many windows closing at a frontier advance and drain it under downstream demand before establishing a checkpoint. Do not materialize all closed windows into an unbounded output `Vec`.
4. Restored progress must preserve source `next_offset` per partition, per-partition max-forwarded-time/watermark, global min/unknown frontier, late counters as appropriate and the set of pending/completed window closures. Prior `seen` beyond confirmed output is *not durable progress*. On restart, use a **new session UUID**; translate checkpointed logical cursors to the new epoch without accepting stale frames.
5. Event-time window deadlines/closure obligations must survive restore and fire once **within a checkpoint lineage**, subject to explicitly disclosed at-least-once sink replay. Separate them from Phase 14's disposable processing-time/diagnostic timers; do not restore a transient clock deadline as if the monotonic origin persisted.

### 2.2 Tiered state and boundedness

1. Specify ownership of every hot map/cache, local cold index/file, compaction buffer, write/restore buffer, outstanding close record, checkpoint staging buffer, Kafka client queue and diagnostic frame. Charge bytes **before** retention and reject impossible single-key/window records. Distinguish managed limits, native Kafka/Arrow buffers, page cache, filesystem free space and RSS. The state store must remain usable when hot memory is forced below live key cardinality and correctly read evicted state back.
2. Use deterministic **virtual shards** (e.g. 32 or 64, fixed for the format) assigned by stable hash of the unsigned key. `owner = shard % logical_owner_count` is a suitable initial policy if versioned. Key ownership must be exclusive, independent of Kafka partition. A checkpoint records shard inventory, canonical key ranges/records and hashes. Repartition on restore only from a completed checkpoint; no in-flight ownership or migration acknowledgments are inferred.
3. Disk state is a **working tier**, not itself a promoted checkpoint. Crash restart must rebuild from the last validated published checkpoint rather than accidentally adopting writes/evictions made after it. Disk quota, partial write, disk full, corrupt local file, cleanup failure and slow I/O have explicit fail-closed paths. Do not use the Phase 2 ephemeral spill contract as a false durability guarantee; reusable IO/accounting helpers are fine if semantics fit.

### 2.3 Checkpoint consistency and atomic publication

1. For this one-owner source loop, the checkpoint barrier is a **quiescent logical cut**: stop source demand; finish/refuse the pending input receipt; drain all closure outputs through the finite sink path; settle frontier/window timers; then snapshot a matched source-offset vector and keyed state. The checkpoint must never advance source offsets ahead of state, nor state ahead of persisted source progress. During checkpoint I/O, do not continue source polling or mutating state. Source metadata/retention verification before/after the cut must remain bounded.
2. Full checkpoints are versioned, immutable generations under an operator-provided local root. Write state shards plus normalized manifest to a unique staging generation, integrity-hash and verify lengths/counts, `fsync` (files/parent directories where supported), then publish an atomic no-clobber generation and only afterward atomically update a recoverable current pointer/manifest. A crash at any failpoint leaves the previous checkpoint loadable; incomplete generations are ignored/quarantined and never selected as latest. Explicit operator policy controls retention/GC; retain at least one verified prior generation until new promotion is proved. Do not treat rename alone as power-loss durability.
3. Include stable schema/codec/hash/window policy identifiers, immutable source identity, source offsets, watermarks and progress, state-shard counts/digests, logical owner assignment, close obligations, checkpoint ID and prior generation linkage. On restore verify **all** data before any network assignment/output; reject wrong job/config, missing files, truncated/corrupted data, inconsistent offset frontier, duplicate key ownership, unsupported version and foreign checkpoint. Guard against path traversal/symlinks, size bombs and unbounded manifest loading. Error messages must be useful but redact credentials and payloads.
4. If Kafka retention has passed a checkpoint offset, or topic/partition history/identity is uncertain under Phase 14's trusted-history contract, fail closed. Checkpointing cannot retroactively attest Kafka topic incarnation; retain that explicit operator assumption. Do not commit Kafka consumer-group offsets.
5. State/window closure outputs are nontransactional. A crash after emitting to the sink but before checkpoint promotion can replay outputs. A stable `(job, window, key, semantic output version)` identity allows consumers to dedupe but is **not** an exactly-once guarantee. Checkpoint restoration must be mathematically equivalent to clean replay modulo these disclosed duplicate output frames.

## 3. Implementation work packages

**WP0 — Preflight / source pin.** Follow `AGENTS.md` read order, capture `git status`, `HEAD`, toolchain and diff from `dc767c7`. Read Phase 14 guide/report/remediation, ADR-0021, stream tests and Kafka adapter, Phase 10/12/13 boundaries. Run focused baseline. Resolve any newly landed Phase 15 code before editing; do not overwrite it or assume a clean tree. Identify actual feature-graph/OS constraints.

**WP1 — Contract/ADR and deterministic model.** Write `docs/adr/0022-keyed-state-and-checkpoints-v1.md` covering identity, event-time window close/late rules, state lifecycle, storage/promoted-durability distinction, quiescent cut, checkpoint file schema/atomicity, restore and logical-rescale semantics. Add an opt-in strict versioned stateful descriptor and tests for unknown fields, resource caps, incompatible policy, owner count, signed timestamps and schema. Use no frontend/DataFusion runtime dependency. Decide file format with deterministic encoding and bounded readers.

**WP2 — Stateful operator and window scheduler.** Integrate a **real** keyed/window aggregation after existing ADE-native event projection/filter. Route keyed records by stable virtual shard. Implement deterministic signed tumbling windows, count/sum overflow checks, explicit late/closed decision, bounded closure traversal and sink backpressure. Define source input receipts vs window-output frames and output IDs; no output before admission and no frontier advancement on failed sink emission.

**WP3 — Hot/cold state.** Provide an actual two-tier state backend with charged hot budget, bounded cold reads/writes and total disk budget; demonstrate eviction/read-through, repeated updates to evicted windows, cancellation cleanup, and failure of exhausted budgets. Keep checkpoint generations separate from mutable working files. Expose state bytes, hot/cold hits, evictions, window count, disk bytes and outstanding close work in bounded telemetry.

**WP4 — Consistent checkpoint writer.** Add finite record/time/operator-triggered *fixed* checkpoint policy (not adaptive). State cut must be provably quiescent with one in-flight source and backpressured window closures. Snapshot complete state (hot+cold), event-time deadline/closure obligations, per-partition acknowledged next offsets, per-partition progress and policy. Implement immutable staging, checksums, durable promotion/current pointer, cleanup, fault injection at every publication boundary and retention policy. A source or sink failure cannot yield `CHECKPOINT_SUCCESS` for an incomplete cut.

**WP5 — Restore / state redistribution.** Add explicit operator restore mode using checkpoint input offsets after validating every byte/identity before side effects. Reconstruct state and timers, resume Kafka with a new session, exercise 1→2, 2→4, 4→1 logical-owner changes and key routing with no skipped/multiplied accumulators. Refuse unsupported topology/schema/source changes. Disclose that no physical worker relocation or online rescale occurs.

**WP6 — Live fault integration.** Extend `scripts/phase14/` or create `scripts/phase15/` with independent producer-ledger oracle and real local Kafka-compatible broker campaign. Test failure/kill at input emission, closure emission, snapshot staging, pointer promotion and restore; corrupt/truncate/tamper manifests and state shards; verify no checkpoint ahead of sink confirmation, bounded backpressure and failure diagnostics. Verify retained Kafka history and explicit gap semantics across restart. Preserve failed candidates and their source identities.

**WP7 — Operator surface and documentation.** Add an actionable local guide (`docs/phase-15-state-guide.md`), support/refusal JSON, reproducible examples, stable `docs/reports/phase-15-implementation-report.md`, source-bound raw evidence plus portable verifier. Update `README.md`, `AGENTS.md` and plan status only after gates pass. Do not rewrite old phase reports, numeric acceptance guards or immutable archives. A coherent local commit is preferred; no push/PR/cloud mutation without separate authorization.

## 4. Mandatory acceptance gates (independent and source-bound)

| Gate | Required independent witness | Pass condition |
|---|---|---|
| G0 — Authority/defaults | Pinned baseline, clean/diff inventory, Phase14 contract replay, default dependency tree, batch/ops/remote smoke | Old stateless CLI, PlanV1, v1 protocols, defaults and historical evidence unchanged; known Phase10/12/13 limits remain honest |
| G1 — Keyed/window semantics | Separate Python/reference model, property tests, multiple Kafka partitions, negative timestamps, crossed keys, empty/quiet partition | Exact sum/count, deterministic close order, unknown/frontier behavior, overflow refusal, late closed-window receipts and no reopened output |
| G2 — State tiering/budgets | Hot cap far below active cardinality, repeated evict/reload, disk cap/faults, peak accounting | Same outputs as ample-memory run; bounded hot/cache/IO/close queues and disk usage; fail closed rather than OOM or silent loss |
| G3 — Checkpoint cut | Injected stall/failure at each receipt/window-output stage, inspect serialized offsets/state/frontiers | No published cut includes unacknowledged inputs or undelivered windows; no snapshot while pending; exact matched cut across partitions |
| G4 — Atomicity/integrity | Kill/fault at each file write/fsync/rename/pointer phase; corrupt/truncate/replace/schema/version attacks | Prior checkpoint remains restorable or cleanly refused; never load partial/newer corrupt generation silently; validated hashes/size/ownership |
| G5 — Restore equivalence | Real broker restart from checkpoint, independent full replay oracle, legal offset holes, out-of-order and quiet partition | Restored logical state/final windows match uninterrupted baseline; correct new session/offset vector; duplicates in sink observable and disclosed |
| G6 — Redistribute on restore | 1→2→4→1 owner transitions with skewed keys, loaded cold tiers and active windows | Real key/state transfers between separate owner stores; canonical shard inventory unchanged, no lost/duplicated state, no online/physical claim |
| G7 — Identity/refusals/security | Wrong cluster/topic/config/hash/schema/partition, expired offsets, path traversal/symlink, poison metadata, bad permissions | Refuse before output; no secrets/payloads in logs, no silent resets/repair, bounded parse/resource use |
| G8 — Demand/cancellation | 30–60s blocked sink plus checkpoint pressure, SIGINT/kill during read/write/promotion, per-domain peaks | No unbounded retained outputs or source-poll bypass; bounded teardown; no false success; broker backlog isolated from ADE buffers |
| G9 — Real source & independent oracle | Fresh 2+ partition real broker, fixed stop criterion, producer ledger, exact window-output and late receipts | Actual ADE-native stateful execution and deterministic correctness; synthetic/unit-only acceptance explicitly insufficient |
| G10 — Regression/toolchain | Locked fmt/check/Clippy/full-feature workspace tests, smoke, selected real Phase14 and Phase12/13 tests | Every run reported with pass/fail/ignore, unavailable gate `UNVERIFIED`; no reclassification of old timing campaigns |
| G11 — Reproducible evidence | Source commit/snapshot, binary and dependency versions, command/env/config identities, raw case traces, hashes, standalone offline verifier | Independent fresh review can replay oracle and all negative contracts; tamper/version mismatch refused; failures preserved, not overwritten |

If any mandatory gate fails or cannot independently run, status stays **PARTIAL**. For G1/G5/G9 use a reference implementation derived from the *contract*, not copied from ADE's state/window code; include a finite true input prefix and separate assertions for output duplicate envelopes. A passing unit suite cannot stand in for a real broker or a crash boundary. Do not make a speedup claim without a predeclared comparable campaign. Record environment limits separately from code defects.

Required general checks when supported:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
bash scripts/smoke.sh
```

The implementation must additionally document and execute its precise Phase 15 unit/property, fault-injection, broker-acceptance and offline-verifier commands. CI can run deterministic correctness tests; noisy throughput assertions stay outside CI.

## 5. Stop conditions and sequencing

Stop before expansion and report a blocker if: the existing sink receipt semantics cannot support a consistent state/progress cut; the store cannot enforce hot/disk quotas; a source log is no longer replayable; checkpoint publication can select a partial generation; mutable working-tier files are mistaken for committed checkpoints; logical rescale cannot prove exclusive key ownership; or real Kafka/failure evidence is unavailable. Document narrow, safe changes or mark `PARTIAL`; never conceal a missing proof with broader claims.

The proposal requested a read-only independent review. The user explicitly required no subagents and all work in this session; that stronger instruction is reconciled to documented same-session self-review and independently written producer-ledger/Python oracles. Do not claim a separate reviewer. Do not begin Phase 16 adaptive checkpoint policy/placement, Phase 17 transactional sinks/savepoints, Phase 18 optimizer unification or Phase 10 physical elasticity under the Phase 15 authorization. Any follow-on P0/P1 correction must be reviewed on its own evidence and source revision.

## 6. Exact read-first map

- `AGENTS.md`; `docs/product/prd.md` (Phase 14–18, dependency spine); `docs/architecture/architecture.md` (§§3,7–9,17–18,21–22,28,31).
- `docs/plans/phase-14-implementation-plan.md`; `docs/adr/0021-unbounded-source-and-progress-v1.md`; `docs/reports/phase-14-implementation-report.md`; `docs/reports/phase-14-review-remediation-2026-10-29.md`; `docs/phase-14-streaming-guide.md`.
- `crates/ade-stream/{Cargo.toml,src/lib.rs,src/runtime.rs,src/kafka.rs,src/timers.rs,src/bin/ade-stream.rs,tests/core.rs,tests/demand.rs}`.
- `crates/ade-exec/src/{stateless.rs,memory.rs,spill.rs,lib.rs}`; `crates/ade-physical/src/lib.rs`; `crates/ade-ops/src/{store.rs,publication.rs}` for *patterns only*, not inherited cross-phase guarantees.
- `scripts/phase14/{acceptance.py,evidence_contracts.py,verify_evidence.py}`; `docs/reports/phase-14-support.json`; `docs/reports/repo-wide-readiness-2026-10-26.md`.

**Pinning note:** Paths and behavior above were inspected on `dc767c7`. If newer commits exist, this plan's first implementation task is to reconcile changed reality while preserving PRD/architecture authority. It is a recommendation to build a bounded Phase 15 milestone, not a preapproved claim of complete distributed state semantics.

## 7. Implementation reconciliation — 2026-10-29

ADR-0022 and the report record the exact bounded implementation. Each owner has a
separate fixed-capacity hot allocation, cold inventory and private directory;
canonical32-shard files physically move to the new owner stores on restore. Full
checkpoints settle pending closure before publication; open records persist their
event-time deadlines. Retained generations and total staging bytes have explicit
finite quotas; operator GC preserves current/prior cuts. Working roots must be
empty at admission, so crash orphans require proved-dead operator cleanup.

An owned stateful-only control poller drains native Kafka statistics/errors during
synchronous I/O, with a requested1MiB stack and joined lifecycle. It never polls
partition data or owns the state/sink cut. This preserves the single data-owner
loop and stateless defaults while closing the slow-checkpoint control-queue hole.
No runtime input changed after candidate8 acceptance. Source snapshots, failed
candidates, the full gate matrix and all qualifications are in the report.
