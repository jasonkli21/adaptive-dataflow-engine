# Adaptive Dataflow Engine — Phase 12 Implementation Plan

## Batch reliability and operations: bounded, crash-consistent v1

**Planning baseline:** `jasonkli21/adaptive-dataflow-engine`, `main` at `fb82b9b454e918f11dcac0776139beb293056acf` (2026-10-28). **Status: COMPLETE bounded v1**, explicitly authorized by the user's 2026-10-28 request, without subagents and with one local commit. The original outcome and acceptance obligations below remain open where unimplemented; historical evidence is unchanged.

### Implementation reconciliation (2026-10-28)

ADR-0019 accepts a bounded transactional checksummed snapshot instead of SQLite.
WP0–5 are implemented for both distributed targets and verified by the implementation
report. The initial foundation at fcffd77b was PARTIAL; its evidence remains
historical. Completion adds managed ADEOPS01/v1 controls alongside unchanged
legacy ADETCP12, durable engine/namespace mapping, native streaming terminal proof,
worker drain/fence, bounded retries/backoff, cancellation and publication/GC.

Reconciled implementation choices: snapshot v2 migrates checksum-valid v1;
32 retained jobs/8 pending/1 active/32 events/3 attempts are fixed caps. Global
execution identity is a UUID namespace plus deployment and checked deployment-local
numeric JobId, never a claim that raw u64 is globally unique. Native runtime/source
and concrete config are pinned. Two retries use deterministic200/400ms backoff;
no jitter is needed for the single local coordinator. HashOnly join and fixed-hash
aggregate disable operator spill; admitted exchange/gather writes share per-worker/
coordinator cumulative attempt budgets. The byte-budgeted join input exchange may
spill, while coordinator gather materializes locally. This deliberately refuses
unaccounted operator/grace/object-retention modes without changing old defaults.

Whole-job replay of immutable pinned join inputs is distinct from the IR's
nonreplayable **in-place lineage** contract: no partial join state is replayed and
no IR flag is changed. Every old session must prove quiescence first; any missing
proof requires NEEDS_OPERATOR. Coordinator SIGKILL can leave bounded scratch debt;
terminal GC deletes only recorded private namespaces, preserving committed refs.
Publication/cancel serialize at the metadata transaction and no-clobber link;
post-link ambiguity is reconciled from the exact persisted native candidate.

The detailed outcome/acceptance obligations below are retained. Documentation of
full PRD Phase12 gaps (replicated/cross-host HA, worker replacement, general sinks)
does not widen this accepted same-host bounded slice. No Phase13 is authorized.
See the report for actual commands, exact quota tests, native reached-stage proofs,
negative development runs, static overhead diagnostic and untested power-loss limits.

**PRD mandate:** `docs/product/prd.md` §16, Phase 12: durable job metadata; coordinator recovery/HA design; retries/backoff; output commit; cancellation; quotas; job history; fault-injection suite. The architecture additionally requires stable logical versus attempt identity, explicit replayability, bounded resources, observable decisions, and attempt-staged output followed by an atomic/transactional manifest commit (`docs/architecture/architecture.md` §§17, 22, 27–28, INV-1–10). Phase 12 is **batch reliability**, not a backdoor for Phase 10 physical elasticity, Phase 13 cloud disaggregation, Phase 17 streaming exactly-once, or Phase 23 multi-tenant scheduling.

## 0. Authority, baseline and reconciliation

Read in this order: `AGENTS.md`; PRD; architecture; this plan; ADR-0010/0011/0013/0017/0018 (plus ADR-0014/0015/0016 where the chosen runner touches their semantics); Phase 10/11 reports, `docs/reports/repo-wide-readiness-2026-10-26.md` and `docs/reports/phase-10-rereview-validation-2026-10-26.md`; current source/tests. Record exact base commit, worktree state, Rust toolchain and selected feature set before changes. The current `AGENTS.md` says “Do not begin Phase 12” because its last authorized work was Phase 10 remediation. A user instruction to **implement** Phase 12 supersedes that historical authorization boundary for the requested work only; amend status documentation explicitly. This *planning* artifact alone is not that instruction.

Current reality, rather than a hypothetical mature cluster:

- Phase 11 bounded v1 is complete: `ade-physical` owns IR v1 and `ade-frontend` binds SQL/typed Rust DataFrame to native local, static aggregate, or bounded inner-join targets. No DataFusion execution fallback. Manifest/source and output bounds apply only to the admitted frontend contract (`docs/adr/0018-physical-ir-and-frontend-v1.md`, Phase 11 query guide/report).
- Phase 10 remains **PARTIAL** despite all six unchanged *local* release guards passing at the distinct `ee9f8da` source. Static is default. Leased file morsels, steals, spot/drain controls, and the `static_projected` diagnostic remain opt-in, fixed 2–4 physical workers; **no** physical add/remove, migration, leased recovery, or production HA claim (`AGENTS.md`, ADR-0017, dated readiness/re-review reports). The `fb82b9b` HEAD is not the frozen timing source.
- `ade-distributed/src/coordinator.rs` runs fixed-stage jobs with in-memory `JobState` and gathers Arrow batches; `gather.rs` commits **internal** final-partition attempts, expressly not external sink transactions. Existing Phase 6 recovery is one final-worker loss after producers complete, in the stated replay/copy envelope only. Other producer/multiple failures abort. Join has its own bounded no-recovery contract.
- `worker.rs` hosts one active job with volatile cancellation/high-water state. `protocol.rs` uses private `ADETCP12`, exchange wire v2 and hash v1; worker state and `LocalCluster`'s temporary directory/parent-owned child processes are **not** restart-persistent. `ade-frontend/src/execution.rs` spawns `LocalCluster` for distributed queries and uses `JobId(0)`; it has bounded postflight validation/collection but no durable job service.
- Existing per-task memory reservations and exchange bounds do not imply an RSS or total temporary-disk bound. Filesystem object-store fixtures are ephemeral, and a durability label from an injected object store is only caller asserted. Preserve benchmark result schema v1, independent oracle and historical empirical gates.

### Phase 12 v1 outcome and honest status

Build **one opt-in, local-host, single-active-coordinator batch operations path** over a fixed, operator-managed 2–4-worker registry. Its durable metadata and output root survive *coordinator-process* crashes. It accepts a frozen, source-verified **ADE physical IR v1** job using the Phase 11 supported **static distributed aggregate and distributed inner join** shapes, stores staged output, atomically publishes one verified result, exposes status/history/cancellation, supports bounded whole-job retries after *provable quiescence*, and repairs ambiguous crash windows on restart. An interactive `ade-query` invocation and all existing benchmark entrypoints retain their behavior. It is useful even without automatic host failover.

The goal is **crash-consistent at-most-once visible publication per durable logical job**, with a successful retry only from immutable verified inputs and disjoint execution attempts. It is *not* generalized exactly-once execution, transactional external sinks, service HA across machines, process membership elasticity, or arbitrary existing API compatibility. If both distributed targets cannot be made correct, do not silently narrow advertised success: explicitly mark the result **PARTIAL**, preserve refusal paths, and describe the unimplemented target and why. Local unary durable execution is a follow-on, not an implied promise of this first distributed slice.

### Contract table (decide and test before coding)

| Surface | Admitted bounded v1 | Explicitly refused/deferred |
|---|---|---|
| Execution | Validated IR v1, manifest-listed immutable local Parquet; Phase 11 static fixed-hash distributed aggregate and bounded two-source inner join; 2–4 *already running* workers | Leased/`static_projected`, physical elasticity, sampled/split/coalesce where retry fencing is unproven, remote sources, arbitrary DAGs, unsupported SQL, streaming |
| Coordinator | One fenced owner of a local durable store; restart/reconcile on same host with same durable root and configured worker registry | Concurrent active coordinators, cross-host automatic failover, leader election/consensus, unverified shared filesystem durability |
| Retry | Finite whole-job fresh execution on verified transient failure, with old attempt definitively quiesced and source fingerprints stable | Blind retry after ambiguous worker state, failed integrity check, nonreplayable input, unknown side effect, arbitrary Phase 6/leased replay expansion |
| Output | Attempt-scoped immutable files; independently validated result manifest; one atomic no-clobber publication marker on one local filesystem | External transactional tables, arbitrary object-store atomic rename, uncommitted partial results, public intermediate streams |
| Limits | Enforced active/admission/history/retry/deadline/output/staging bounds; explicit accounting scope | Pretending ADE-managed memory is process RSS or unchecked spill is subject to a hard quota |
| Operations | Submit/run-or-recover/status/history/cancel/verify/GC; bounded structured events; actionable refusal codes | Multi-tenant auth/priority/fairness, production SLA/availability guarantees, live remote dashboard |

## 1. Architecture and state machine

### 1.1 Real ownership, no speculative scaffolding

Add a small `ade-ops` crate **only if** it actually owns the durable store, state machine, output publisher and `ade-batch` CLI. It may depend on `ade-frontend`, `ade-physical`, and `ade-distributed`; the reverse must remain false. Minimal, surgical hooks in `ade-frontend`/`ade-distributed` may accept externally managed fixed worker endpoints and a caller-owned cancellation/deadline without changing existing API defaults. Keep physical IR independent of SQL, coordinator, persistence and protocol; keep benchmark adapters independent. If a smaller module boundary satisfies these rules, explain it in ADR-0019. No empty `ade-coordinator`/`ade-observe` crate tree.

**Suggested operator interface** (names can change if documented and tested):

```text
ade-batch init --state-root ROOT --output-root ROOT
ade-batch submit --manifest ... --dataset ... --sql-file ... --target distributed-aggregate|distributed-join --config ...
ade-batch run [--once] --workers FIXED_ENDPOINT_CONFIG
ade-batch recover --workers FIXED_ENDPOINT_CONFIG
ade-batch status JOB_ID
ade-batch history [--limit N] [--cursor CURSOR]
ade-batch cancel JOB_ID [--reason TEXT]
ade-batch verify-output JOB_ID
ade-batch gc [--dry-run]
```

An ordinary foreground runner plus explicit restart command is acceptable for v1. There is no need for a bespoke network daemon if a second CLI invocation can durably request cancellation and the active runner observes it within a bounded interval. All commands must have a programmatic Rust API and structured, versioned machine-readable errors/results. No external service or user auth is claimed; restrict state/IPC access to the local operator and document trust/permissions.

### 1.2 Stable durable identity versus engine execution scope

Define a never-reused **DurableJobId** (stable across retries), **ExecutionInstanceId/attempt ordinal** (increases), **fencing generation/owner token**, and an engine `JobId`/`execution_namespace` pair unique across *all* instances and restarts. Persist the mapping transactionally. Within one engine instance, retain existing `LogicalTransferId`, `AttemptId`, hash/wire semantics and Phase 6 recovery. A whole-job restart is a **new engine execution**, not a hidden continuation of the original exchange; it must never merge old transfer/gather data. Document this two-level identity carefully in ADR-0019 so it does not misrepresent architecture INV-1. Do not use `JobId(0)` for durable submissions. Reject integer wrap, ID reuse, and stale ownership/response/commit tokens.

### 1.3 Durable state and crash recovery

An explicitly selected, durable-on-one-host metadata implementation is required. **SQLite with transactions and WAL** is a reasonable default (a narrowly scoped bundled dependency is acceptable); a filesystem journal is valid only with equivalent documented atomicity, checksumming, exclusive ownership, fsync and recovery properties. Record the choice, portability and failure model in ADR-0019. Persist at least:

1. schema/protocol version and migration policy; stable job id, creation order, immutable user request/plan digest, manifest *bytes* hash, table/source file fingerprints, resolved schemas, target/config, worker-registry identity, limits/deadline and output location;
2. attempt ordinal, unique engine job id/namespace, owner generation, state transitions with timestamp/sequence and classified error; finite retry/backoff schedule and cancellation intent;
3. staged file inventory/digests, terminal partition/attempt coverage, publish marker identity, final status/history and cleanup tombstones.

No credentials, opaque absolute-source data dumps, arbitrary SQL blobs or unbounded verbose logs in the journal. Cap metadata sizes and retained events; preserve enough provenance to reconstruct/verify admitted inputs. Journal transitions must be atomic and durable **before** side effects that rely on them. Corrupt/incompatible records fail closed, never silently reset. Explicit migration/version refusal; backup/restore documented if supported.

Required public state machine (internal substates may be richer):

```text
PENDING -> CLAIMED -> RUNNING -> STAGED -> COMMITTING -> SUCCEEDED
              |         |          |           |
              +---------+----------+-----------+-> CANCELLING -> CANCELLED
              |         |          |
              +---------+----------+-> RETRY_WAIT -> CLAIMED (fresh instance)
              +---------+----------+-> FAILED / NEEDS_OPERATOR
```

All transitions use transactional compare-and-swap with owner generation/attempt and terminal monotonicity; there is exactly one active owner. `SUCCEEDED`, `CANCELLED`, and `FAILED` must be monotonic, except explicit operator-supervised recovery from `NEEDS_OPERATOR` through a *new* execution with recorded intent. A verified durable publish marker is authoritative across the crash window between publication and the state-store success write. Cancellation races with commit need one documented linearization order; never flip an already published success to cancelled.

### 1.4 Fenced owner and worker quiescence

The coordinator lock must be **OS/process enforced**, not just a timestamp. Acquire it before mutating the state store or attaching to workers; release on process exit. On restart, increment the durable owner generation, inspect every nonterminal attempt, verify the configured worker registry and **prove** the old attempt has stopped accepting/writing tasks before a fresh instance starts. This likely requires a narrow, versioned worker control addition for session inspection and cancel-and-drain acknowledgements (or an equally strong, tested mechanism). A `CancelJob` RPC acknowledgement by itself is **not** proof that a stuck task, old gather, retention I/O or poisoned worker has quiesced. Bound waits; if worker/process identity cannot be established, classify `NEEDS_OPERATOR`, expose cleanup instructions and do not automatically retry.

Existing `LocalCluster::spawn` child ownership/temporary roots cannot simply be persisted or adopted across a crashed parent. For v1, require **externally supervised, fixed, explicitly configured worker processes and persistent, scoped scratch roots**; do not assume orphaned children are safe to reuse. Worker restart and replacement are not automatic membership changes. A permanently dead worker may be replaced by an operator in a controlled new fixed cohort; this must have a distinct instance identity and explicit test/refusal. Fencing applies to control replies, exchange/gather, staged files and publisher. Never relax existing worker high-water, poison or cancellation protection.

### 1.5 Input replay and deadline

Admission stores validated IR v1, canonical manifest/source identities, schema and bound sizes **without copying arbitrary source data**. Each retry rebinds/revalidates the same immutable source inventory before any worker preparation and after native completion, using Phase 11 source checks. Reject source drift, replacement-by-rename, symlink/path escape, changed manifest/schema, invalid plan, unsupported target/config, altered worker registry and expired total deadline. A successful source hash is bounded verification, not transactional isolation against change-and-restore or malicious FS access. One **job-wide absolute deadline** includes queueing (if configured), admission, backoff, worker execution, postflight, publication and teardown. Persist wall-clock expiry and use monotonic time within a live process; document clock skew/restart handling and never extend the original budget by restarting.

## 2. Output commit and resource safety

### 2.1 Transaction-like publication for a local batch sink

The durable result contract is a directory of versioned attempt files (prefer Arrow IPC or Parquet with declared schema and checksums) plus exactly one canonical, **immutable**, small committed manifest/marker. Stage under an attempt-unique path on the **same filesystem** as the marker. Verify native completion (all required final partitions and their attempt-safe gather commits), plan output schema/row and byte limits, input postflight, file length/digest and manifest consistency; fsync staged bytes and parent directory before publishing. Commit by a documented truly atomic **no-replace** operation (e.g. tested same-filesystem hard-linking a fully written manifest to a canonical marker, then syncing the directory). A plain overwrite-capable rename is insufficient unless a separate atomic ownership/CAS protocol makes replacement impossible. Publication marker is the consumer visibility boundary; never expose temporary files through successful query APIs.

After a crash, validate the marker **and all referenced immutable files**: if valid, reconcile metadata to `SUCCEEDED` without a second publish; if absent, determine safe retry/cleanup; if corrupt or conflicting, freeze job `NEEDS_OPERATOR` and quarantine. Do not delete data referenced by a committed marker. A stage failure/oversize/cancellation leaves **no visible committed result**; temporary debris may exist until guarded GC. Pin logical job/plan/source/execution IDs, worker terminal coverage, schema, row/byte counts and file hashes in the manifest. Avoid stale-path traversal, hard-link/symlink attacks and cross-device assumptions; require private directories and validated owned paths. Document local filesystem, process and power-loss guarantees honestly (including fsync assumptions); **no** cloud object-store durability or transactional table claim.

### 2.2 Retries, cancellation and budget enforcement

- Retry only explicitly classified transient failures after output is known uncommitted, old attempt drained/poisoned workers excluded, input replayability reverified and new instance identity reserved. A bounded retry policy (default e.g. 0 or 1 retry; maximum 2) with exponential backoff, capped jitter, timestamps and a finite global deadline must be fully recorded. Do not turn Phase 6's single-final loss into arbitrary producer or join recovery; those still fail the *engine attempt* and may, if safe, trigger a clean whole-job rerun.
- `cancel` is durably recorded idempotently and delivered to active runner/worker tasks. Verify cancellation promptly at queue, admission, input hashing, staging, backoff, gather and publication boundaries. Cancelled/failed attempts cannot create a new publish marker. An already committed job returns `SUCCEEDED` to a late cancel; no success-to-cancel regression. Document blocking syscall limitations, operator escalation and eventual cleanup.
- Enforce admission caps for active jobs (v1 exactly 1), queued jobs, history entries, source/plan bytes, attempts, per-job timeout and output rows/bytes. Implement real byte-counted staging and cleanup budget; either instrument **every admitted runtime write path** (operator spill, exchange spill/materialization and staged output) for a hard managed disk quota or **explicitly refuse** any configuration whose writes escape that quota. Preflight free-space checks alone are not a hard limit. Keep task memory, exchange buffers, OS RSS, native decoder transients, and scratch disk measurements separately labelled. Quota exhaustion must fail closed without a success marker.
- All file and metadata I/O is bounded or deadline-checked as reasonably possible; do not hold a global lock across unbounded worker/network/file operations. Event history, listing pagination, logs and error payloads have fixed size limits; no unbounded polling, channels, or retained result batches. Limit submitted output paths and avoid unsanitized identifiers.

## 3. Work packages (dependency order)

### WP0 — Gate, contract and fault model

1. Produce a concise *source-to-requirement map* and supported/refused matrix covering both targets, worker failure stages, source mutability, store/sink failures, restart and cancellation.
2. Identify actual public runner seams (`ade_distributed::physical::execute`, `Catalog::execute_until`, worker protocol, gather, join) and how they will be reused without weakening current checks. Freeze baseline tests/evidence, exact compatibility contracts, negative cases and known Phase 10 debt.
3. Write ADR-0019 (before risky edits): store atomicity/locking; durable/engine identities; recovery/quiescence protocol; publication linearization; retry matrix; quotas and trust boundary. Record intentional architecture refinements, alternatives and residual gaps.

### WP1 — Durable store and state-machine core

Implement schema/migration refusal, finite history, atomic CAS state transitions, exclusive coordinator owner, unique ID allocation and attempt creation. Serialize validated job recipe with checked version/hash. CLI/API for init/submit/status/list/history; deterministic unit/property tests for invalid transitions, duplicate submit/idempotency token, corrupt DB/journal, two coordinators and crash points. No worker I/O before a durable claim.

### WP2 — Fixed-worker adapter, quiescence, cancellation and retry

Provide externally managed worker-endpoint execution without changing existing `LocalCluster` defaults. Add narrowly scoped worker inspect/drain/fence as needed, with explicit version negotiation or version refusal; if private protocol changes, document the new `ADETCP` marker and test old/new rejection (do **not** quietly change exchange wire v2, hash v1, or benchmark result v1). Integrate exact Phase 11 aggregate **and** join sources/outputs, job-wide deadline and postflight. Retry only safe whole-job instances. Test coordinator `SIGKILL` at prepare, producer, final/gather, postflight and backoff. A stuck/poisoned or unreachable worker yields `NEEDS_OPERATOR`/failure, never unsafe reuse.

### WP3 — Staging, atomic publication, replay reconciliation

Implement a streaming bounded attempt sink reusing existing native gathered output/schema adaptation without collecting unbounded in-memory copies. Add file-level checksums, terminal-set manifest, syncing, atomic no-replace marker and reconciliation. Test crash immediately before/after every durable boundary, foreign marker, partial/corrupt staged data, repeated recovery/commit, duplicate late frames, source drift after staging, cross-device paths and no-partial-visibility. Verify both aggregate exact grouping and join **bag** multiplicity against independent DataFusion/oracle outputs, not just row counts.

### WP4 — Quotas, history, cleanup, ergonomics

Implement bounded metadata and per-job output/staging/managed spill quotas with explicit enforcement coverage; controlled GC that never erases committed data or a live attempt. CLI for cancel, recover, history, verify-output and dry-run GC. Report lifecycle events, durable/instance IDs, worker/plan/config/source hashes, attempt count, retry reason, cost/resource counters, terminal coverage, final manifest digest, operation durations and cleanup status. Redact raw inputs and secrets; include troubleshooting docs. Test all limits at boundary and one-over, plus concurrent status/cancel while run is blocked.

### WP5 — Comprehensive validation and closeout

1. Add deterministic store/sink failure injection (transaction refusal, disk full/short write, fsync/rename/link, checksum flip, corrupt record), worker/IPC failure injection (process kill, timeout, delayed/reordered replies, stale epoch, malformed frames, loss during gather), and crash/restart integration tests with *real processes*. Injection must reach the intended stage; a preflight refusal does not count.
2. Run independent aggregate and join correctness oracles for normal/empty/duplicate/skew/overflow cases, including failed/cancelled jobs with **no** published marker. Add an evidence manifest binding commit/source, toolchain, command/config, test source, fixtures, reached fault stage, expected and observed state, output digests and negative outcomes; keep local timing claims separate from correctness.
3. Required workspace checks: `cargo fmt --all -- --check`; `cargo check --workspace --all-targets`; `cargo clippy --workspace --all-targets --all-features -- -D warnings`; `cargo test --workspace --all-features`; `bash scripts/smoke.sh`; focused real-worker/front-end/operations tests and relevant Phase 6–11 regressions. Run offline/locked if reproducibility requires. Record skipped commands and environmental failures as **not run**, not passes.
4. Keep `.github/workflows/ci.yml` correctness-only, with bounded deterministic tests. Re-run relevant legacy static exchange/recovery/join and Phase 11 tests. Do not change Phase 8/9 frozen timing acceptance, Phase 10 six thresholds or relabel old failed campaigns. If new native hot-path changes threaten their contracts, run source-bound fresh comparisons *separately* and report outcome without retroactively moving gates.
5. Update `README.md`, `AGENTS.md`, `docs/adr/README.md`, user ops guide, and `docs/reports/phase-12-implementation-report.md`. Include exact base/final commit and diff scope, version/contracts, supported/refused matrix, fault evidence, quotas/retry/commit semantics, clean-state reproduction, regressions, limitations and honest **COMPLETE bounded v1 / PARTIAL / BLOCKED** disposition. No production HA assertion without corresponding evidence.

## 4. Acceptance matrix and hard stop conditions

| Gate | Required observation; no proxy accepted |
|---|---|
| Correctness | Both admitted distributed targets execute through ADE-owned operators, matching independent oracle results **and** schema/bag/order contracts; no SQL/DataFusion execution fallback |
| Durability | Persistent job metadata survives `SIGKILL` and restart; repeated recovery yields one terminal state, no lost or duplicated publish, no guessed success |
| Fencing | Two active coordinators rejected; stale worker response/old attempt/duplicate commit refused; undrainable old worker cannot trigger fresh execution |
| Commit | One immutable visible marker only after all partitions, exact output bounds and source postflight; crash-before/after publication reconciles correctly |
| Retry | Attempt count, unique engine identities, safe failure classification, original absolute deadline, finite backoff and `NEEDS_OPERATOR` refusals all demonstrated |
| Cancellation | Pending/running/retry/staged cancellation idempotent, deadline bounded, no marker; commit race linearized and documented |
| Quotas | Enumerated **enforced** resource domains with cap and cap+1 tests; unmanaged RSS/transients and unsupported spill paths plainly disclosed/refused |
| Operations | Useful status/history/cancel/recover/verify/GC, bounded pagination/logs, structured failure codes, corruption and migration refusal |
| Fault suite | Reached-stage real-process faults, metadata/disk faults, adversarial source/stale messages, repeated restart and cleanup correctness; source-bound evidence |
| Compatibility | Static default, accepted Phase 6–9 behavior, Phase 10 bounded research status, Phase 11 API/IR semantics, oracle/result schema and wire/hash boundaries preserved or explicitly versioned |

**P0 stop and disclose:** partial/duplicate visible output; unsafe retry with unquiesced worker; corrupted metadata reinitialized as success; id/epoch reuse; source drift accepted; unbounded resource use falsely advertised as quota-controlled; default benchmark/frontend behavior silently changed. Fix such issues before claiming bounded-v1 success. If a source or runtime constraint makes a gate impossible, refuse that path, retain a failing test/evidence, mark Phase 12 *partial*, and explain the narrow next step. An implementation report saying “the tests pass” without exercised crash windows, independent oracle and source identities is insufficient.

## 5. Intentionally deferred, and phase handoff

- Cross-host coordinator leader election and automatic HA failover, replicated metadata, multi-site durability, network partitions under adversarial multi-writer storage.
- Automatic replacement/add/remove/migration of physical workers; Phase 10 leased failover/stealing recovery or generic scheduler rework.
- General-purpose external sinks, object-store manifest transactions, exactly-once side effects, multi-user auth/tenant fairness/priority and cloud connectors.
- General SQL/nullable types/UDFs, Python/Substrait, plan DAG expansion, streaming progress/state/checkpointing, remote shuffle, stable arbitrary process RSS guarantee.

**Handoff:** preserve the clear difference between (a) an accepted, opt-in local *batch-operations slice* and (b) full Phase 12/production HA. Carry a machine-readable unsupported matrix, a fault/restart runbook, and an independently reviewable evidence bundle into the next phase. The separate review prompt must independently derive obligations from PRD/architecture/current code before consulting this plan.

## 6. Source map for implementer/reviewer

All links are pinned to the inspected `main` snapshot; recheck a changed checkout.

- [PRD](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/docs/product/prd.md), especially §16 Phase 12; [architecture](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/docs/architecture/architecture.md), especially §§17, 22, 27–28 and INV-1–10.
- [AGENTS](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/AGENTS.md), [ADR-0013](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/docs/adr/0013-lineage-recovery-v1.md), [ADR-0017](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/docs/adr/0017-morsel-scheduler-v1.md), [ADR-0018](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/docs/adr/0018-physical-ir-and-frontend-v1.md).
- [Phase 11 report](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/docs/reports/phase-11-implementation-report.md), [Phase 10 readiness](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/docs/reports/repo-wide-readiness-2026-10-26.md), [Phase 10 re-review](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/docs/reports/phase-10-rereview-validation-2026-10-26.md).
- [Coordinator](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/crates/ade-distributed/src/coordinator.rs), [gather](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/crates/ade-distributed/src/gather.rs), [worker](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/crates/ade-distributed/src/worker.rs), [protocol](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/crates/ade-distributed/src/protocol.rs), [frontend execution](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/fb82b9b454e918f11dcac0776139beb293056acf/crates/ade-frontend/src/execution.rs).
