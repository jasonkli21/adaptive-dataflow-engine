# Adaptive Dataflow Engine — Phase 10 Implementation Plan
## Morsel scheduler, work stealing, and bounded worker elasticity v1

**Prepared:** 2026-10-24 (America/Los_Angeles)
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine
**Inspected `main`:** [`3e89edfd0230674f0e06437cccb58fa99e369422`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/3e89edfd0230674f0e06437cccb58fa99e369422) — Phase 9 review fixes
**Status:** Implementation explicitly authorized by the user on 2026-10-24; acceptance remains evidence-gated.

## 0. Authority, gate, and honest outcome

Read in this order at implementation time: `AGENTS.md`; `docs/product/prd.md` (especially §§7–9, 11, 15–17 and Phase 10); `docs/architecture/architecture.md` (§§6, 9, 11–17, 22, 25–31); this plan; ADRs 0013–0016; Phase 8/9 reports and Phase 9 experiment; then the actual source and focused tests. Follow the repo's precedence: PRD outcome > architecture invariants > active plan > accepted ADRs > observed code/reports. An ADR refining architecture must name the affected invariant/section; it cannot silently expand a phase.

**Planning prerequisite:** At inspected HEAD, Phase 9 is marked **COMPLETE (bounded v1)** in `README.md`, `docs/reports/phase-9-implementation-report.md` and ADR-0016. The Phase 9 frozen measurements belong to source `9b0330aa533f0ed403dc7856349ff36952ef4214` / `crates` tree `fab08a06457e2f3f1e62877e3223f6c502afecc9`; later review fixes were verified for correctness but **not re-timed**. Phase 8 was independently closed at source `ab7404b1c7095ce3e8b60d79f944a78cd9787c34` / runtime tree `432cc7dd212ec41ad6fdec93bc97f957df5a514d`, retaining failed experiments and immutable evidence. Do not rewrite or reinterpret either baseline.

**Operational gate:** `AGENTS.md` still says Phase 9 is active and “Do not begin Phase10.” This request authorizes *planning*, not changing code or declaring Phase 10 active. The subsequent implementation instruction should be treated as the user's explicit Phase 10 authorization; before source changes reconcile the status in `AGENTS.md` with Phase 9 closure, pin fresh HEAD, inspect uncommitted work, and record any intervening changes. If Phase 9 has regressed or its accepted closure is materially invalidated, stop and report the blocker rather than burying remediation in Phase 10.

**PRD Phase 10 goal:** Move beyond coarse fixed tasks with morsel leases, work stealing, locality/critical-path scheduling, worker add/remove, graceful decommission, spot/preemption hooks and heterogeneous-worker awareness. The v1 acceptance here is a **genuine dynamic source scheduler** for ADE's bounded distributed aggregate: file-morsel leases can move among producer roles while exchange/final ownership remains pinned. Within a **pre-registered, already-running 2–4-process topology**, producer participation can activate or drain during a job. This is *elastic participation*, **not** live physical-node provisioning, arbitrary worker replacement, final-owner migration or host failure recovery. Record those as explicit incomplete parts of the broader Phase 10 vision, not silently as delivered features. If an independently defensible implementation can safely add real process membership within this phase, first revise this plan/ADR and acceptance matrix; do not call logical activation a real autoscaler.

## 1. Current baseline, as actually implemented

| Surface | Baseline reality at `3e89edf` | Phase 10 consequence |
|---|---|---|
| `coordinator.rs` | `StaticAggregateJob`; 2–4 contiguous `WorkerId`s; input files assigned `i % workers`; one `RunProducerTask` and one final task per worker; all N×N producer/final terminals; attempt-safe gather and final-only recovery | Add opt-in leased production without changing default or presumed N×N transfer identities. Never mutate final ownership mid-exchange. |
| `worker.rs` | Per-job producer/final busy guards; one producer task opens N senders, executes aggregate over its file list, closes every destination, and records manifests; one active job/worker | One *long-lived producer session per worker* must process a sequence of leased morsels before exactly one terminal per destination. Cannot invoke the existing whole-producer function for each morsel under the same transfer ID. |
| `protocol.rs` | Private control `ADETCP11`, bounded JSON control frame (1 MiB); synchronous coordinator→worker task RPCs | Design a bounded worker→coordinator lease request/reply endpoint, or a comparably explicit mechanism. Version changed private messages. Keep exchange wire v2 unless independently necessary. |
| `lineage.rs` / `recovery.rs` | `SourceMorsel` is **one complete source file**, stable index/fingerprint/size; replay maps file indices round-robin to original `TaskId`; one original final-worker loss can recover in fixed graph | Leased reassignment invalidates existing producer lineage closure. **V1 leased mode refuses recovery, expected-cost full-copy semantics and all replay/copy combinations** rather than guessing. Keep static graph recovery unchanged. |
| `topology.rs` / ADR-0015 | Static/static-packed or single immutable sampled 48-shard layout after bounded observation and ACK; final owners pinned | V1 leased mode uses **static** hash layout only. Sampled layout/prefix resumption cannot be combined until explicitly proven under movable source ownership. |
| `ade-exec` / `partial.rs` | `GroupBySum` local plan; fixed hash default; `adaptive_v1` is a distinct opt-in algorithm with replay restrictions | First prove leased mode with `fixed_hash`; exclude `adaptive_v1`/`raw_partial` until per-morsel state/reset and exactness are proven. Never double-ingest when obtaining/acking a lease. |
| Phase 9 join | Two-source fixed join, observed broadcast/repartition, bounded spill; no join replay | Keep join out of v1 scheduler. It must remain correct and reject any scheduler option it does not implement. |
| `ade-bench` | Independent oracle/process-isolated macrobenchmarks; result JSON schema v1; diagnostic sidecar v2 binds run/slot/child/source | Expose explicit scheduler controls and measured source-bound diagnostics without changing result v1 or claiming unmatched sidecars as evidence. |

No general physical IR, SQL frontend, persistent coordinator, cloud autoscaler, durable metadata store, arbitrary retry, process RSS cap or cross-host durability exists. Do not infer one.

## 2. Concrete v1 scope and admission

### 2.1 Supported execution

- `Query::GroupBySum` on existing `synthetic-events-v1` Parquet inputs; same output schema and independent DataFusion oracle, including exact checked sum/count semantics.
- File-granular `SourceMorsel` records from the immutable ordered manifest/snapshot. Each file is one atomic lease; **do not** suggest one large file is splittable in v1. Require multiple morsels to demonstrate actual redistribution. Row-group/byte-range morsels are a follow-on if measured file granularity fails the phase outcome.
- Pre-registered **2–4 workers**, unchanged final hash layout and same existing fixed N logical producer identities. All workers remain alive and configured as exchange destinations throughout the job. A producer on worker `w` may process source morsels whose original list index would have been owned by another worker, but sends under its own `TaskId(w)`; no two live tasks share one logical producer ID.
- Leased mode is **opt in** (`scheduler_policy=leased_v1`); `scheduler_policy=static` is default, behavior/bytes/metrics for old workflows remain comparable. Initial participating workers (e.g. 2 of 4), activation of parked pre-registered producers, and drain of an active producer are explicit controls. Every logical producer still closes N transfer terminals, even with zero morsels.
- Pure deterministic policy over *observed or declared* inputs: `SourceMorsel.bytes`, stable ID, optional explicit locality label, advertised validated worker capacity weight, completed-morsel throughput estimate, pending queue, in-flight count, and deadline/remaining-work estimate. Local machine's shared filesystem is **not evidence of remote locality**; record hints and actual observed work separately.
- A bounded source queue, bounded lease ledger/decision log, one outstanding lease per producer (initial v1), capped total morsels/files and bounded control traffic. Keep source file-count admission explicit and harmonize with existing manifest limits; no unbounded retries, threads, per-row metadata or lease speculation.

### 2.2 Required refusal matrix

Reject before any producer output in leased mode:

1. Recovery/replay snapshots being used for final-loss recovery, expected-cost complete copies and other retention modes that require fixed producer lineage. **Separate** immutable source identity used for scheduler verification from permission to replay/recover; `recovery_enabled=false` must not imply unverified inputs.
2. Sampled 48-shard layout/prefix-resume, non-fixed aggregate modes not explicitly admitted, Phase 9 join, and arbitrary SQL/types/state/unbounded sources.
3. Changed/missing source fingerprints, duplicate/missing `MorselId`, invalid lease owner, stale job/epoch/token, unknown worker, non-contiguous topology, stale control version, invalid weights/locality input, impossible bounds, insufficient worker capacity or incompatible drain requests.
4. In-flight worker crash, lease timeout after possibly producing bytes, lost/ambiguous completion, poisoned store, final-owner loss, or deadline exhaustion. These **cancel and quarantine the whole job**; no reassigning an ambiguous in-flight lease, no unsupported exactly-once/producer recovery claim.

The existing static final-only recovery behavior must remain intact when scheduling is disabled. Worker graceful **drain** is not a crash: it stops granting future leases, finishes and ACKs the current morsel, emits all terminals, and leaves its final/receiver duties running until job cleanup. Spot/preemption notice can trigger this same drain *if there is sufficient time*; forced termination before drain completion is fatal. Do not claim spot survival guarantees.

## 3. Scheduling correctness protocol

### 3.1 Separate logical work, ownership and attempts

Recommended conceptual records (exact Rust APIs are implementation decisions):

```text
MorselDescriptor { id, source_snapshot_identity, file_path, file_fingerprint,
                   size_bytes, optional_locality, estimated_work }
Lease { job, execution_namespace, epoch, morsel_id, assignee_worker,
        nonce_or_lease_id, lease_deadline, source_identity }
MorselState = Pending | Leased(owner,lease_id,deadline) | Completed(owner,lease_id,observed_work)
WorkerState = Parked | Active | Draining | Drained | Failed
```

Morsel identity and content remain stable across ownership choices. `LeaseId`/nonce represents the assignment, **not** a new logical source identity or exchange attempt. Never make a physical socket or filename alone authoritative. Only one outstanding lease per producer and one active owner per morsel in v1. The coordinator owns bounded ledger transitions under one synchronization boundary (or an equivalent serialized actor).

### 3.2 Lifecycle / transport

1. **Admission/freeze:** Validate manifest identity, complete ordered file coverage, duplicate IDs/paths, worker registry, configured resource limits and incompatibility matrix. Create an execution namespace and a deterministic schedule-domain digest binding job, ordered file identities, worker registry/capabilities, static final layout, policy/config and epoch. Give all workers the same frozen spec; require ACK before opening leases/exchange.
2. **Prepare:** Start all final readers (or explicit inactive verifier if the chosen pinned static graph requires one) and exactly one long-lived producer task per worker. Start a bounded, localhost-scoped coordinator lease service with explicit deadline/identity checks. Coordinator waits for registered worker readiness, not a producer-completion barrier.
3. **Acquire:** Active producer requests next lease. The scheduler first picks work from its locality/preferred-owner pending queue; if empty or worse under the documented scoring rule, it **steals only unleased work** from another queue. Record original preference, actual owner, reason/score, and decision inputs. Parked workers receive no work until explicitly activated; draining workers receive none.
4. **Execute:** Validate source size and fingerprint *before* reading (and record any post-read drift checks/assumptions). Execute the existing `fixed_hash` partial operator once for that morsel with the existing memory/spill/cancellation constraints, streaming contributions into the worker's one persistent exchange sender set. Do not buffer all morsel outputs in coordinator memory. Same hash1/routing and partial codec as static path.
5. **ACK:** Mark a morsel complete only after its contributions are accepted into that producer's provisional sender/session and the worker reports the exact lease ID, identity and truthful counters. Idempotent retransmission of the *same* ACK can return prior status; conflicting duplicate/late/stale ACK fails closed. An ACK is **not** a durable cluster commit; if it becomes ambiguous, fail the job rather than reclaiming it.
6. **Finish:** On global exhaustion, every active/parked/drained producer must close its exactly N logical outgoing streams, including empty terminals; all finals drain and return manifests, gather only commits the declared final set after **every morsel completed once**, all producer terminals and row/byte/frame conservation checks. A process-level error anywhere yields no partial output and cancels survivors.
7. **Cleanup:** Stop leasing, reject late requests with the cancelled job watermark, close network/listeners, release local reservations/spill/receivers, await child exit where owned. Repeated cancel/drain/cleanup is idempotent; a poisoned store is never silently reused.

**Fencing detail:** The existing exchange treats `producer TaskId` as the stream owner. Do **not** accidentally create an exchange transfer per file under the same logical transfer ID or advance exchange attempt on a steal. Lease progress lives alongside (not in place of) the N×N terminal manifest. Prove that a producer unable to complete a lease cannot emit a successful terminal/gather, and no pending source is silently omitted.

**Timeout behavior:** Use one finite job/stage deadline, finite lease RPC timeouts and bounded heartbeats/renewals during long morsels. Normal progress renews the *same* exclusive lease. Expiry or heartbeat ambiguity during actual output is a failure, not permission for duplicate execution. Avoid deadlock where a blocking source or exchange sender starves renewals (use a bounded independent control path); document the pre-existing noninterruptible OS I/O caveat.

### 3.3 Policy vs mechanism

- Put a pure, testable decision function in a narrowly scoped `scheduler` module, with typed `Observation`, `Score` and `Decision`, and a separate ledger/lease service. No scheduler decision inside `ade-exec` or a storage backend.
- Start with deterministic, interpretable weighted remaining-work scoring: prefer locality where *declared and meaningful*; estimate finish time as `(queued + candidate estimated work) / admissible worker capacity` with saturation/overflow guards; prioritize risk of longest remaining path near deadline; tie-break consistently by `MorselId` then `WorkerId`. Define units and score ordering precisely in ADR before coding. Worker capacity is a validated declared weight or bounded EWMA of actually completed morsels, never fabricated CPU speed.
- Add simple hysteresis/minimum work threshold for steal and activation to prevent thrash. A worker requests new work when free; other workers' *pending* queues can be stolen, not leased work in progress. A single global queue with no preferred-owner provenance is insufficient to prove real work stealing.
- Include `forced_static`, `leased_no_steal`, `leased_steal`/`leased_v1` diagnostic controls with identical semantic output and resource bounds; do not use these as separate engines with different source identities.
- Do not couple scheduler to the Phase 7 materialization cost model yet. Expose honest schedule observations through a typed bridge suitable for later economic integration; no new worker-cost or failure-probability signal may be invented.

### 3.4 Elastic participation and spot hooks

- Maintain an **immutable registry of 2–4 live endpoints** for a given job, but allow changing the `Active` subset. Prove at least one actual mid-job transition from parked→active and one active→draining→drained. Draining producer completes current lease/terminals; its *final-owner* service stays alive.
- Add a locally injected, deterministic `DrainWorker`/`ActivateWorker` (names TBD) control request or equivalent bounded coordinator API. A preemption-notice hook maps to a drain request; record notice time, grant cutoff, in-flight duration, drain completion or failure. If the notice window expires, cancel; do not silently kill an in-flight owner and reschedule.
- Heterogeneity: test at least two different advertised capacity weights and optionally observed throughput calibration; demonstrate actual differences in assigned bytes/lease count and bounded decision overhead, while preserving identical output. No claim of GPU/cloud heterogeneity.
- **Explicit limitation:** pre-registered process count, receiver topology and logical producer slot count are fixed throughout a job. Joining a *new physical endpoint*, shrinking final-owner set, movement of retained state and true spot replacement require a later, separately justified epoch/membership protocol. Label this limitation clearly in ADR, README and report; if those capabilities are necessary to claim full PRD Phase 10, report v1 as partial rather than asserting full completion.

## 4. Work packages / efficient development order

### WP0 — Rebaseline, compatibility matrix, ADR

1. Pin fresh HEAD/dirty status, inspect `AGENTS.md` and the specific source paths above; verify Phase 9 accepted status and source identity. Do not revert user changes.
2. Define `docs/adr/0017-morsel-scheduler-v1.md` (if 0017 remains next free ADR): file-morsel boundary; lease/fencing state machine; exact exchange identity; refusal matrix; scoring units; control protocol/version; active/drain semantics; recovery impossibility under current fixed producer-lineage; explicit PRD v1 versus future elasticity reconciliation. Put unresolved architecture conflicts into a decision table before code.
3. Add a reproducible Phase 10 *functional* acceptance matrix and a pre-measurement `docs/research/phase-10-experiment.md` template, without outcome numbers.

### WP1 — Pure policy and bounded lease ledger

Implement typed config and validation, deterministic source catalog/identity, preferred queues, pure scoring/steal and active/drain transitions. Add compact tests for ordering, one-owner invariants, full coverage, duplicate/stale ACK, lease expiry, zero-work worker, invalid weights, deadline, bounded metadata, and no-steal control. Keep per-job state bounded by accepted `max_morsels`, workers and a fixed-size audit ring plus aggregate counters (report dropped event count).

### WP2 — Worker-pull production and preserved exchange graph

Add the lease service/protocol, frozen spec ACK, producer-session cursor and per-morsel `fixed_hash` execution. Reuse existing N×N exchange, exact partial codec, final merge, gather and cancellation; keep static code path default. Ensure source validation each lease, no double processing, accurate producer metrics, and bounded control/service threads. End-to-end test two/four processes with intentionally imbalanced file sizes and a real steal, then a parked activation and graceful drain; prove no duplicate/missing rows through independent oracle and terminal/lease accounting.

### WP3 — Harness controls and observability

Expose narrowly namespaced `scheduler_*` options via `ade-bench/src/ade_distributed.rs`; strict parse/duplicate rejection; effective config in engine identity; `recovery_enabled=false` enforced for leased mode. Record policy, digest, source inventory, `MorselId→worker/lease` terminal coverage, actual stolen/owned work, queue wait, work wall, lease/renewal/ACK time, control messages/bytes, activations/drains, throughput estimator and bounded peaks. Bind diagnostics to `run_id`, child PID, warmup/iteration and source identity as in Phase 9 sidecar v2; extend sidecar version if semantics change. Keep benchmark result v1, independent oracle and process isolation intact.

### WP4 — Faults, economic tradeoffs, release evidence and handoff

Test actual worker-process death mid-morsel, expiry/heartbeat loss, duplicate/stale control frames, corrupted input, cancellation after provisional exchange bytes, poison/error cleanup and final-owner loss; **all leased-mode failures must produce no successful output**. Run focused old-path recovery/retention/topology/join regressions. Complete one final workspace quality gate and smoke after meaningful edits stabilize; build a clean, source-bound release for separately predeclared experiments. Preserve failed attempts/raw sidecars. Update `README.md`, `AGENTS.md` phase state (only after implementation succeeds), ADR index, architecture with only genuine refinements, and `docs/reports/phase-10-implementation-report.md` with a documented Phase 11 boundary.

Likely touch map: `crates/ade-distributed/src/{scheduler.rs,coordinator.rs,worker.rs,protocol.rs,ids.rs,metrics.rs,lib.rs}`, `crates/ade-bench/src/ade_distributed.rs`, focused process/unit tests and `scripts/phase10-{acceptance,verify,bundle}.*`, `docs/{adr,research,reports}`. Touch `ade-exec` only for an essential narrow reusable per-morsel source/sink API; do not invent a general SQL/IR or a standalone empty scheduler crate.

## 5. Correctness matrix and measurable exit

### 5.1 Focused CI-safe correctness tests

| Case | Required assertion |
|---|---|
| Exact coverage | Every manifest morsel assigned and completed **once**; 0 missing/extra; same independent oracle/checked aggregate result as static for varying file sizes, key distributions, 2/3/4 workers, empty and singleton-file cases. |
| Real steal | Worker with no eligible local queue leases a still-**pending** morsel preferred elsewhere; owner and decision log prove change, not post-hoc simulated counters; no in-flight reassignment. |
| ACKs and fencing | Duplicate identical completion idempotent; different nonce/owner/stale epoch/source/job/worker rejects. Missing/ambiguous ACK or expired in-flight lease causes full failure, not speculative retry. |
| Exactly N×N terminal closure | One sender per producer/destination including parked/drained empty producer; exact transfer manifest/context, row/byte/frame conservation and no partial gather. |
| Locality/critical path | Deterministic pure scoring examples with meaningful hints, declared worker weights and deadline risk; tie-breaks and score overflow; diagnostics distinguish hint from measured placement. |
| Live participation | Parked→active obtains real subsequent lease; active→drain completes current lease, gets none afterward, closes terminals; final service remains alive; repeated control idempotent. |
| Fail closed | Real producer/final death, stale frame, timeout, stale source fingerprint, poison/cancel during spill or send, dead lease service; no false success or leaked background processes. |
| Compatibility | Default static five retention modes/Phase 6 one-final replay; Phase 7 expected-cost facts; Phase 8 static/sampled layouts, pinned recovery and skew; Phase 9 join/partial controls remain unchanged. All disallowed leased combinations reject **at admission**, not silently coerce. |

Use deterministic state assertions, controlled hooks and exact counters in CI—not noisy wall-clock performance pass/fail. Target genuine boundaries, avoid combinatorial repetitions of equivalent fixtures. Keep older acceptance evidence immutable.

### 5.2 Predeclare release experiment **before first Phase 10 timing**

Write and freeze an experiment manifest (version, git source SHA and `crates` tree, toolchain/host readiness, exact manifest seeds/files/workload sizes, policy/control flags, worker weights/hints, preemption schedules, warmups, measured repetitions, timeouts, validation and calculations). Use at least **one warmup + five independent process-isolated measurements** per primary 2-worker arm and **one + three** for limited 4-worker confirmations; no best-only selection or unpaired sources. Each child passes the independent oracle. Emit source-bound versioned diagnostic sidecars, raw manifest/results, SHA256 bundle/inventory and a verifier that rejects mismatched run/iteration/source/config/lease coverage or incomplete samples. CI remains correctness-only.

Proposed *advance-of-results* gates to freeze or explicitly revise **before** measuring; do not tune them after observing results:

| Workload / arms | Actual mechanism required | Proposed gate |
|---|---|---|
| Imbalanced 16–32 file-morsel aggregate, 2 workers | Static round-robin vs leased-no-steal vs leased-steal with a real off-preference lease | All oracle/lease/terminal checks; leased-steal median whole-job time **≤0.90× static** and maximum producer work completion **≤0.90× static**; show its value versus leased-no-steal. |
| Balanced/uniform same-source aggregate, 2 workers | Static vs leased-steal | Same oracle; leased median whole-job time **≤1.15× static**; report absolute scheduling/control overhead. |
| Heterogeneous/slow-worker 2–4 processes | Frozen declared capacity vs unweighted control; deterministic slow-worker hook and optional honest measured rates | Correct output, actual reassignment toward available capacity, no claimed physical locality/speed unsupported by inputs; quantitative difference, no mandated speed win if hook dominates. |
| Mid-job activate / graceful drain / spot notice | Real lease and terminal transitions on 4 registered processes; failure control with inadequate notice | No loss/duplication or partial gather; successful graceful case completes; forced exit fails closed. |
| Missing/ambiguous lease and source drift | Unrecoverable negatives | Deterministic refusal, bounded teardown, no successful measurement/result. |

If clean-room host readiness or expected timing stability cannot be achieved, **report the timing acceptance as unverified**, not passed. Record all readings and protocol deviations; never weaken historical Phase 6–9 thresholds to clear Phase 10. Avoid calling local processes independent failure domains. `operator_work_ns` is exclusive wall time, **not CPU**, and source logical bytes, network payload, ledger allocations, Arrow decode, control queues and RSS must remain clearly separated.

### 5.3 Completion decision

Mark **Phase 10 bounded v1 accepted** only when all required functional cases, allowed/refused combinations, final quality/smoke regressions and predeclared empirical guards pass on source-bound raw evidence. Document limits: file-sized morsels, initial static destination topology, pre-registered worker pool, no producer/final failover for leased mode, no new process join, no streaming/state/SQL. A PRD feature not met is a known gap; if the phrase “Phase 10 complete” would imply live physical scale-out, use **Phase 10 partial — bounded scheduler slice complete** and propose a separately gated remainder. Do not force a success label.

Required final report: baseline/final SHA and dirty state; code/ADR reconciliation; source snapshot identity; lease state machine/fencing; actual steal and drain traces; independent-oracle/conservation evidence; memory/control/thread/FD bounds; policy diagnostics; failure matrix; complete raw release protocol and all pass/fail gates; historical regressions; deviations and residual risks; explicit Phase 11 handoff. No Phase 11 IR, SQL, unbounded scheduling, cloud autoscaler or generalized recovery as incidental cleanup.

## 6. Review independence

The independent Phase 10 reviewer must reconstruct the current PRD/architecture demands *before* reading this plan as a checklist; identify missing or mis-scoped requirements, assumptions, false outcome claims, failure modes, architecture drift and benchmark artifacts. Passing the exact WP list is not proof that this plan was sufficient. The accompanying review prompt is deliberately broader than this implementation recipe.

## Implementation reconciliation (before source changes)

The implementation request supersedes the planning-only operational gate above.
Baseline HEAD is exactly 3e89edf; Phase9 closure remains valid. ADR0017 fixes the
concrete contract before coding: integer capacity weights and byte-work scores;
completion-count-triggered activation/drain/spot notices are deterministic local
control hooks applied DURING the live job. Leased v1 admits spill delivery only,
with explicit ingress row/byte/frame ceilings, context-bound aggregate schema4
on existing wire2, and no replay. Static five-mode behavior stays unchanged.
Declared capacity is used without an invented throughput calibration. Locality
is an explicit preference label, never physical localhost placement evidence.
The fixed process registry is an incomplete PRD Phase10 outcome, even if the
bounded scheduler slice passes. All empirical gates above remain unchanged.

A necessary exactness refinement discovered before release: fixed-hash file
partials use the existing bounded hash/spill contribution drain so an i128 file
sum can split into several Int64 contributions while conserving count. A final
representable cross-file sum must not be falsely refused by the new file lease
boundary. This is fixed-hash build over the complete file, without adaptive/raw
tail execution; static operators remain unchanged. Admission also verifies the
full immutable catalog and caps physical bytes before any producer output.

Independent review remediation keeps the predeclared experiment and its numeric
guards unchanged. Actual catalog size now bounds completion-count events before
preparation; infeasible last-active drains reject during that same admission
check. Scheduled activations keep their producer session open until the event is
applied. `min_steal_bytes` remains an opportunistic preference: when it excludes
all off-preference candidates, an active requester has no local pending work,
and all pending preferred owners are parked/draining/drained, leased_v1 takes
the best pending morsel for progress. If a pending preferred owner remains
active, the requester waits. The no-steal control keeps its refusal contract.
See ADR-0017 and the implementation report for the review fixes and evidence;
Phase10 acceptance remains gated on a new passing release.


## Stabilization scope (2026-10-26)

The separately authorized readiness handoff permits up to two cohesive measured
remediation candidates, versioned release-evidence hardening and focused legacy
correctness checks. The original declaration, 13 arms, repetitions and six numeric
guards remain frozen. ADR-0017 records bounded control isolation and larger hash
reads with both content checks intact. Phase 11 bounded v1 remains COMPLETE;
original “No Phase11” wording above records historical scope. Physical process
elasticity, leased failover and Phase 12 remain deferred. Failed guards prohibit
promotion; static scheduling remains the supported default.
