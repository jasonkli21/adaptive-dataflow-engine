# Adaptive Dataflow Engine — Phase 23 Implementation Plan
## Multi-Tenant Resource Manager — bounded, durable, same-host v1

**Prepared:** 2026-11-14
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine
**Inspected `main` HEAD:** [`a9ccf823f320c683d68c05d7d27ad25c3deb6f14`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/a9ccf823f320c683d68c05d7d27ad25c3deb6f14)
**Status:** proposal, **not implemented or accepted**. Recheck `HEAD`, worktree, feature state, CI and reports before coding; reconcile changes rather than transplanting this plan blindly.

## 0. Authority, intent, and actual starting point

Read in order and preserve precedence: `docs/product/prd.md` (especially §§5–9, 11, 14, 16 Phase 23, 22, 24), `docs/architecture/architecture.md` (especially §§8, 13, 15, 17, 22–24, 26–27, 29, 31/INV-1–10), `AGENTS.md`, `docs/plans/phase-12-implementation-plan.md`, `docs/adr/0019-durable-batch-operations.md`, `docs/phase-12-operations-guide.md`, recent predecessor ADRs/reports, then affected code and tests. The PRD's Phase 23 outcome is **queues, priorities, quotas, fairness, admission control, preemption, per-job cost and autoscaling**; these are not optional headings to tick off with no working mechanism. The project requires correctness over adaptation, bounded resources, explicit replayability, observable and disable-able policy, genuine process/fault evidence and trustworthy claims.

### Verified repository constraints at the inspected HEAD

| Surface | Observed reality | Phase 23 implication |
|---|---|---|
| `crates/ade-ops/{src/store.rs,src/runner.rs,src/recipe.rs,src/main.rs}` | `ade-batch` uses a bounded checksummed snapshot store (v2), immutable `Recipe` v1, fixed 2–4 local managed workers, `runner.lock` and `owner.lock`, durable cancel/fence/reconcile, atomic no-clobber output; one unresolved native job per store, one active native instance | **Do not** multiplex competing executions into one existing store or reuse its workers concurrently. Build an opt-in resource-manager layer with distinct, pinned execution cohorts and serialized ownership per cohort. |
| `crates/ade-distributed/src/scheduler.rs`, `.../scheduler/control.rs` | Phase 10 file-morsel leasing/stealing is within a **single** fixed registry; phase-10 physical membership is deferred | Per-tenant multi-job queueing belongs above the native job scheduler; no fictitious dynamic physical membership. |
| `crates/ade-exec/src/memory.rs`, Phase 12 runtime/disk limits | Managed reservation bytes are **not** process RSS; write budgets are cumulative, and output/metadata have separate limits | Define quota dimensions and units honestly; distinguish guaranteed admissions from observed OS resource usage. |
| `crates/ade-gateway/src/service.rs`, Phase 22 | One active query, separate reader/ticket caps, trusted registered Python, loopback Flight SQL; not a multi-tenant gateway | **Do not** add tenant headers to the existing Flight gateway and call it isolation. It remains out of Phase 23 v1 execution scope. |
| `docs/reports/phase-18-*`, `phase-20-*`, `phase-21-*`, `phase-22-*` and current support JSON | Phase 18 is partial; Phase 20 awaits independent reacceptance; Phase 21 has independent-remediation reacceptance pending; Phase 22 is `PARTIAL_REMEDIATION_PENDING_REACCEPTANCE` (final-SHA CI and current-source fault campaign pending) | Phase 23 must neither inherit a green predecessor label nor silently claim unified planning/streaming/connector/UDF multi-tenancy. Keep their evidence and qualifications intact. |

Baseline acceptance target: **trusted local Unix operator**, explicitly authenticated tenant-facing control API, **finite immutable local Parquet-backed Phase-12-compatible aggregate and bounded HashOnly join recipes**, two independent worker cohorts (each 2–4 fixed managed workers) for real overlap, with a bounded number of tenants/jobs. This is multi-tenant *resource scheduling* inside a single trusted-host administrative domain, **not** hard multi-user OS/container security or production cloud HA. If another substrate is objectively better, document the proof, compatibility implications and revised acceptance before implementation (see §10).

### Proposed contract name and completion discipline

Target status *only when every mandatory gate passes on the final source*: `COMPLETE_BOUNDED_LOCAL_MULTITENANT_MANAGER_V1`. Otherwise use a precise `PARTIAL_...` status with individually marked gates and blockers. No prior phase is reaccepted merely because Phase 23's regressions pass.

## 1. Supported contract and non-goals

| Capability | Mandatory, meaningful v1 | Explicit deferred scope |
|---|---|---|
| Tenant identity/authorization | Operator provisions distinct opaque tenant identities and credentials; local authenticated request path; resource and result ownership checks; operator-only administration | Hostile same-UID adversaries, cross-host IAM/RBAC, delegated cloud secrets, secrets in plans/lineage |
| Queue/priority/fairness | Durable bounded per-tenant queues; deterministic scheduling; tiered priority **and** cross-tenant starvation guard; explainable decisions; static FIFO policy as control | Distributed cluster-wide optimization; unverifiable probabilistic fairness |
| Admission/quotas | Atomic tenant and global limits for queued, active and reserved resource units; reject/queue typed reasons before worker effects; no double booking | Hard CPU/RSS isolation without OS primitives, limitless jobs, speculative oversubscription |
| Concurrency | At least two independent, operator-owned fixed worker cohorts, one native job per cohort, genuine simultaneous process execution; durable exclusivity | Worker stealing across cohorts; worker reassignment while unresolved; elastic Phase10 membership |
| Preemption | Bounded, prioritized **cooperative whole-job cancellation + safe drain + fresh internal subjob** for eligible replayable jobs; preserve one logical submission and publication; preserve spent cost | Preempting arbitrary in-place operators/state, resuming at a morsel offset, forcibly reusing poisoned workers |
| Cost | Per-logical-job and per-native-attempt usage ledger, with measured/estimated fields, retry/preempt cost and explicit optional price model | Claiming exact cloud dollars, hard CPU cost, inferred bytes from missing proof |
| Autoscale | Bounded **activation/deactivation of already provisioned, healthy independent worker cohorts** (e.g. 1→2→1) based on real queue pressure, with no dispatch during drain; observable effect on concurrent capacity | Automatically spawning/replacing machines, Kubernetes scaling, physical worker membership changes, scaling into an ambiguous cohort. Call the supported behavior *standby-cohort activation*, not cloud autoscaling. |
| Results | Existing ADE-native execution, source/plan validation, output marker and `verify-output`; tenants only access their own verified logical result | New sink semantics, external exactly-once, in-place recovery, partial output streaming |

Use a **default-off** standalone `ade-resource` crate/binary (`ade-manager`, name provisional), independent of existing default CLIs, wire protocols, PlanV1 and native crates. Avoid a mandatory new dependency on `ade-gateway`, SQL, Python/JVM, Kafka, cloud catalogs or streaming. If a minimal `ade-ops` public wrapper is required, keep it additive and audited.

The v1 contract must state concrete finite bounds. Suggested starting bounds, **subject to ADR review and measured validation**: ≤8 tenants, ≤64 logical jobs in manager history, ≤32 queued, ≤2 active cohorts, each cohort 2–4 fixed managed workers, ≤3 logical preempt/restart launches, strict per-cohort inherited 32-job/8-queued/8 MiB metadata limits, ≤64 KiB control frames and ≤1 MiB request payload/metadata if justified. These are proposals, **not repository facts**. Cap history/events and refuse exhaustion rather than erase unverified records or recycle worker/store IDs. Config/derived counts must use checked arithmetic and size-before-allocation checks. Provide deterministic capacity tests at each boundary.

## 2. Design decisions to lock before coding (proposed ADR-0030)

### 2.1 Control plane and underlying executor

Proposed layout:

```text
tenant CLI / local credentialed Unix socket             operator-only CLI
                 |                                            |
                 +----------------+---------------------------+
                                  v
                         ADE resource manager
                  authn/authz -> immutable enqueue
                   durable state + deterministic policy
                  reservations / job & attempt cost ledger
                    |                          |
             cohort A (fixed)            cohort B (fixed)
             own ade-ops root            own ade-ops root
             own STORE_UUID             own STORE_UUID
             2–4 managed workers        2–4 managed workers
             exactly one runner         exactly one runner
                  |                          |
              native PlanV1 and proven Phase12 output publication
```

Each cohort has a distinct canonical private operations root and worker registry/boot identities pinned to its `ade-ops` store. Workers are **externally supervised**, exactly as Phase 12 requires, and must survive manager/coordinator crash for reconciliation. `runner::run` owns its own `runner.lock`; the manager never holds its durable state transaction lock while hashing, spawning, blocking, waiting, canceling, running or draining. A cohort cannot be reassigned until its previous internal subjob's terminal state and drain/publication outcome are reconciled. `NEEDS_OPERATOR`, unreachable workers, boot change, incomplete drain, state corruption or unknown dispatch outcome **quarantine** that cohort and all dependent logical jobs; other healthy cohorts may continue if global metadata invariants hold. Never auto-reset/adopt workers.

A manager logical job is distinct from each `ade-ops` job UUID, engine `JobId`, owner generation, cohort store UUID, boot instance and internal idempotency key. One logical submission may have multiple **serial**, independently fenced subjobs after preemption. Mapping and generation are append-only. Only a single verified committed subjob may become the logical success result; later canceled/failed attempts cannot replace it. An atomic manager result mapping/receipt is necessary; it must never claim that writing the manager snapshot is a replacement for the native `COMMITTED.json` proof.

`ade-ops` currently cannot run two unresolved native instances in the **same store**. Do not "fix" this by removing `runner.lock` or the one-unresolved predicate. The scheduler must use a *different root/cohort* for independent work. New jobs should be submitted to `ade-ops` **only at dispatch**, not while waiting in the manager queue: the ops store has a small pending cap and registration expiry starts immediately. Queue-time identity/source authorization must be checked up front; native source-content verification and compatibility pins are rechecked at dispatch, with a typed source-drift refusal if anything changed.

### 2.2 Authenticated tenant boundary

Operator provisions a versioned private tenant registry and a **high-entropy, non-user-selected credential** for each tenant. Expose only a bounded local Unix-domain-socket control endpoint (or an equivalently well-justified authenticated local IPC mechanism); no new TCP listener. Associate the authenticated principal with the request **server-side**; reject user-supplied tenant/owner impersonation. Store only nonreversible credential fingerprints, check in constant time, rotate/revoke without permitting accidental reauthorization of old jobs. Protect private manager/ops roots (mode 0700/0600 as applicable), avoid credential contents in errors/logs/plan/lineage, and document the same-UID/operator trust assumption. A local operator-only administrative path can configure tenants/cohorts/prices and inspect aggregate metrics; tenant API can submit, poll/list *own* jobs, cancel *own* work and obtain **only a verified** result reference. Check authorization **before any existence-dependent lookup**; unknown vs foreign IDs must not leak existence or file paths. Do not expose `ade-ops` internal roots, scratch paths or arbitrary worker API to tenants.

Bind each tenant to configured canonical, owner-approved source roots and output access policy, not arbitrary caller-supplied paths. Validate symlinks/traversal, aliases and mutations at both manager admission and `ade-ops` dispatch; reject races rather than follow a newly swapped root. Existing Phase12 filesystem pinning is part of, but not a substitute for, tenant-level authorization. All raw local-host files remain trusted-operator assets; this is not hostile same-UID isolation. Do **not** accept untrusted Python UDFs or cross-tenant use of Phase22 tickets.

### 2.3 Durable state machine and crash boundaries

Define a versioned, bounded manager ledger with checksummed atomic replacement, private-root ownership, exclusive manager-instance lock and corruption refusal. Borrow `ade-ops` safety concepts without blindly copying its v2 wire/schema. Suggested *logical* states:

`QUEUED -> RESERVED -> STARTING -> RUNNING -> (PREEMPT_REQUESTED -> DRAINING -> REQUEUED | SUCCEEDED | QUARANTINED) -> [SUCCEEDED | FAILED | CANCELLED | EXPIRED | QUARANTINED]`.

This is illustrative; choose a legal transition table and document exactly which are terminal. Record immutable submission idempotency `(tenant_id, request_key, recipe_digest)`, request deadline, enqueue order, effective priority, replay class, cohort generation, reservation vector, intended internal `ade-ops` key, internal job ID, start/stop/fence, output proof and cost accrual. A repeated key with the same recipe/tenant must return the original logical ID without new source reads/execution; a changed digest must refuse. Different tenants may reuse the same opaque key without collision.

Persist **intent before effects**. Crash windows to design explicitly: reserve->submit; `ade-ops` submit->manager mapping; child launch->RUNNING receipt; preempt intent->cancel; drain ACK->requeue; native marker link->manager success; meter update->cleanup; daemon loss during standby activation. Recovery must use the immutable internal `ade-ops` request key to rediscover an already submitted subjob and `runner::recover`/verified marker reconciliation. Never infer safe replay from timeout, socket disconnect, child death or metadata-only `CANCELLED`. A valid native commit races with and **wins** cancellation; the logical result is exactly once *within the bounded local output mechanism*. Unknown worker state, unproved quiescence or unreadable receipt remains `QUARANTINED`/`NEEDS_OPERATOR` with reservations held conservatively and an operator runbook; **never** release capacity optimistically. A manager restart preserves logical deadlines, aging and already spent cost; wall-clock rollback refuses rather than granting extra time.

### 2.4 Resource vector and accounting

Specify `ResourceRequest` and `PoolCapacity` units and where hard guarantees originate. At minimum: **cohort slots** (exclusive), queued jobs, per-tenant running jobs, max one active job/cohort, worst-case managed runtime bytes per worker/coordinator, max reserved IPC staging bytes, committed output allowance, and bounded metadata/event/control bytes. For native writes, classify inherited budgets as *per-attempt cumulative write allocations*, not live disk occupancy. For memory, an ADE-managed ledger is **not RSS**. If no OS-enforced resident-memory or CPU budget exists, state `UNENFORCED` and collect actual observations; do not pretend to meet a "hard memory quota" by checking a config field. Use checked `u64/u128` arithmetic; reject negative, zero, NaN, overflow, unknown resource units and late reservations. Keep coordinator, worker, journal/metadata, temp, output and historical attempt debris accounted separately.

Admission must atomically test global and tenant limits and reserve before dispatch. All release transitions have one audited owner and are safe under retries/cancel/crash; a committed result may retain bytes after running slots release. Preemption does **not** refund consumed work or delete committed bytes. Recompute reservations on restart from durable active assignments; compare against recorded ledger and refuse mismatch. Quotas should include per-tenant queued/running counts, total pending/active jobs, cumulative admitted work/cost budgets as configured, max output/retained bytes and tenant-authorized source scope. Support config changes only under a versioned, atomic, no-overcommit protocol; tightening below in-use quotas never silently evicts valid committed output.

### 2.5 Deterministic priority, fairness and admission policy

Separate a pure `decide(snapshot, config, monotonic_now) -> Decision{reason, preconditions}` policy from durable mechanism. Suggested policy: bounded priority tiers with **finite aging** plus per-tenant weighted deficit round robin (or another explainable work-conserving scheme) among eligible tenants; tenant concurrency/cost caps take precedence over priority. Use **declared work units**, not raw job counts alone, when jobs differ materially. Document tie-breaking (tenant ID, submission sequence), age saturation, time source, starvation assumptions, and policy nonoscillation. A static policy (e.g., FIFO, no preemption, one active cohort) remains available for attribution.

Fairness acceptance is a **predeclared objective oracle** using equal-weight/equal-cost backlogged tenants (e.g., prefixes of grants differ by at most one after initial transients) and weighted 2:1 examples on identical-cost jobs. For high-priority continuous arrivals, demonstrate the chosen finite-aging policy actually allows an eligible lower-priority tenant to make progress in a bounded number of completed dispatches, not a fairness claim based on a unit test that merely selects alternating IDs. Run the policy on real, overlapping native jobs to show its choices are acted upon.

### 2.6 Preemption contract (real but intentionally narrow)

Preempt only finite, source-pinned, **whole-job restartable** Phase12-native recipes whose external effects are limited to the proved no-clobber local output. Require explicit `preemptible` opt-in, bounded logical launch/preemption counts, original logical deadline and a policy-specific benefit/cooldown. A higher-priority eligible queued job may preempt a lower-priority active job **only after** a durable preempt intent. Send native `cancel`, observe resolution, prove worker drain, and classify committed-vs-canceled correctly. If the job committed before cancellation's linearization point, record **SUCCEEDED**, never `PREEMPTED`; if cancellation and drain succeed without committed output, create a new *internal* `ade-ops` job later, with a new fenced identity and refreshed source verification. Preserve logical ID, request key, original deadline and cumulative usage. `NEEDS_OPERATOR`/unproved drain permanently blocks automatic requeue and that cohort. Never force-kill a worker as a substitute for a drain proof. Preemption of nonreplayable/UDF/streaming/connector/external-effect jobs is **unsupported** and must be refused explicitly.

### 2.7 Per-job cost and bounded capacity activation

Cost must be attributable and auditably finite. Record queue latency, admit/start/end times, worker/cohort slot-milliseconds, number of dispatched native attempts and preemptions, observed native worker/coordinator write counters where available, and committed output bytes. Record **unknown** counters as `null`/`UNMEASURED`, never as zero. Price conversion is opt-in, versioned configuration: e.g. priced slot-ms plus measured/upper-bounded byte quantities, with currency, units and model hash in the receipt; distinguish **estimate at admission**, **reserved budget**, **actual metered usage**, **charged failed/preempted work** and **unpriced/unknown remainder**. Do not conflate job-cost accounting with the original Phase 7 expected-recovery model. Provide exact recomputation from an independent attempt log and verify no double-charge across restart or duplicate requests.

Make pool activation a genuine deterministic policy changing **which already supervised cohorts can accept jobs**: start with one active cohort, activate one healthy standby when waiting demand and budgets warrant, drain/deactivate when idle and no unresolved reservation or native job exists. Prove two independent jobs really overlap and that 1->2->1 decisions leave no reused busy worker, lost work, dangling charge or lost output. Standby workers may still consume host resources; measure/report them. **Do not call this physical autoscaling, scale-to-zero or worker provisioning**. If real dynamic process provisioning is chosen instead, it needs an independent boot/fence/supervision/stop/restart proposal, real-process tests and a reconciled scope; never smuggle it in as a script that restarts a poisoned worker.

## 3. Implementation work packages, ordered by risk

| Package | Deliverable | Proof before advancing |
|---|---|---|
| WP0 — baseline and research | Pin baseline SHA/status/CI; read full code paths; sketch alternatives to manager-as-sidecar vs `ade-ops` extension vs standalone daemon; write ADR-0030 scope, state machine, resource units, threat/replay model and source-bound acceptance matrix | Contradictions and open predecessor qualifications explicitly recorded; no silent relaxation of `ade-ops` fences or PRD feature list |
| WP1 — pure contracts | Versioned tenant/config/job/decision/receipt structures; finite validators; pure policy and quota calculators; checked arithmetic; deterministic virtual-clock and state-transition tests | Source-independent fairness, overflow, policy tie-break and quota oracle; no unbounded collections |
| WP2 — durable control state | Private/checksummed manager store, ownership/locking, idempotency, stable logical/attempt IDs, atomic reservations, admin config changes, bounded event log and explicit quarantine/reconciliation | Crash/tear/corruption/state-drift and duplicate-submission tests; no phantom credits/jobs |
| WP3 — cohort adapter | One cohort = one `ade-ops` root/store/worker group; lifecycle owner and durable dispatch keys; actual `ade-ops` submission, `runner::run`/`recover`, native marker verification, immutable logical-result mapping | Two cohorts can run simultaneously with separate workers; legacy single-store exclusivity stays unchanged; native aggregate/join outputs independently correct |
| WP4 — authenticated API | UDS endpoint/CLI, credential provisioning and tenant scope, refusal codes, bounded frames/connections, tenant-filtered list/status/cancel/result and admin-only operations | Foreign-ID/key, forged token, revoke/rotate, path/symlink/race, oversized request, slow-peer and leak tests |
| WP5 — scheduling and preemption | Durable queue, priorities, fairness, ready-state/aging, quota-first admission, real cancellation/drain and fresh-subjob restart | Real process high-priority preemption, committed-race and `NEEDS_OPERATOR` refusals; no duplicate outputs/worker claims |
| WP6 — cost and standby activation | Versioned pricing/usage ledger, meter reconciliation, global/tenant budgets, deterministic 1->2->1 activation/deactivation | Exact offline cost recomputation, charged failed/preempt attempts, observed overlapping jobs, measured standby footprint |
| WP7 — evidence and release | Fault-injection harness, fresh independent fixtures/oracle, offline detached verifier, fully rehashed tamper suite, matched baselines, docs/guide/ADR/support matrix/CI | All G0–G17 mandatory gates source+binary matched; otherwise publish explicit PARTIAL with blockers |

Avoid unbounded new abstractions and copy-pasted `ade-ops` store logic. Prefer minimal new modules `contract`, `policy`, `store`, `auth`, `cohort`, `dispatch`, `meter`, `service`, plus targeted tests, and keep physical/native crates untouched unless justified by a finding. Illustrative paths: `crates/ade-resource/`, `crates/ade-resource/tests/`, `scripts/phase23/{campaign.py,verify.py,tamper.py}`, `docs/adr/0030-bounded-local-multitenant-resource-manager-v1.md`, `docs/phase-23-resource-manager-guide.md`, `docs/reports/{phase-23-implementation-report.md,phase-23-support.json}`, `docs/evidence/phase23/`. Exact module names can change after ADR reconciliation.

## 4. Non-negotiable safety and semantic invariants

1. **At most one native owner per cohort**; no worker overlaps between cohorts; no worker reuse before its original native drain and owner proof. A worker boot or store ID cannot be reassigned to bypass a fence.
2. **Logical identity survives attempts**; a native subjob is never reused as a new logical request. Tenant identity and idempotency key participate in every authorization and lookup.
3. **No partial or duplicate committed output**; native `COMMITTED.json` is authoritative; cancel-after-commit wins as success; verification precedes tenant-visible success.
4. **A decision cannot weaken safety constraints**; priority, fairness, capacity activation or preemption must not grant nonreplayable execution or free unsafe resources.
5. **No hidden unbounded memory/disk/control queues**; all accepted counts/bytes and error payloads are bounded; label tracked vs observed vs OS-enforced limits.
6. **Persist before effects; recover via facts**. Never turn crash ambiguity into a guessed failure, an unearned refund or a duplicate dispatch.
7. **Authentication precedes existence queries**. Cross-tenant status/list/cancel/result/idempotency leakage is denied; credentials never travel in physical plans or lineage.
8. **Keep all original Phase 12 and default behavior**: `ade-batch`, legacy `ade-query`, PlanV1, worker protocol, local publication and regression evidence remain compatible.
9. **No scope laundering**. Phase 18/20/21/22 remain explicitly qualified; Phase 23 cannot assert arbitrary SQL/streaming, UDF, remote, HA or hardened multi-user isolation.
10. **Every adaptive action is observable/disable-able**, with reason, inputs, policy/config version, immutable IDs and a static baseline; deterministic tests do not rely on timing for correctness.

## 5. Mandatory acceptance gates: predeclare before candidate execution

| Gate | Required evidence, not just a code assertion |
|---|---|
| **G0 — attribution/baseline** | Exact baseline, `HEAD`/dirty input inventory, toolchain, feature flags, executable hashes, host/process identity, criteria/fixture lock before runs; immutable earlier evidence; independent acceptance witness. |
| **G1 — architecture/scope** | ADR decision compares alternatives; precise supported/unsupported matrix maps all eight PRD Phase23 features, INV1–10 and Phase12 constraints; no hidden widening. |
| **G2 — identity/authz** | Two real principals/tokens, authenticated UDS, per-tenant visibility/cancel/result policy, revocation; cross-tenant key and ID oracle; symlink/path/race/forgery refusals; no secrets in logs. |
| **G3 — durable submission** | New, duplicate, changed-key, concurrent duplicate, crash-between-fsync and acknowledgment, overflow/exhaustion; original deadline, enqueue order, immutable request persist across restart. |
| **G4 — priority/fairness** | Pure policy model/property checks plus real multi-tenant backlogged native runs; predeclared equal/weighted share and finite-aging witness; no starvation or undocumented priority inversion. |
| **G5 — quotas/admission** | At every boundary: tenant/global queued/active/slot/managed-byte/staging/output/cost constraints; reject vs wait distinction; checked arithmetic; no worker effects on refused admission; refunds exactly once. |
| **G6 — cohort exclusivity/concurrency** | Two separate externally supervised 2–4 worker cohorts actually overlap; cohort-store/boot pins and all `runner.lock`/drain invariants; negative same-cohort double-claim and worker reuse tests. |
| **G7 — safe preemption** | Running low-priority preemptible native aggregate/join canceled/drained then restarted as fresh native job with stable logical ID; high priority progresses; nonpreemptible refusal; native committed-vs-cancel race; poison/unreachable cohort quarantine. |
| **G8 — crash recovery** | SIGKILL at every persisted-effect boundary, manager restart, native coordinator death, worker death, slow/unresponsive control peer, metadata ENOSPC, bad/checksummed snapshots, clock rollback, duplicate notifications; no replay before drain. |
| **G9 — cost/usage** | Independently recompute per-job and per-attempt measured/unknown cost, retries/preemptions, reservation lifetimes, billable/idle pool usage, pinned price model; no double billing, fabricated zero or budget underflow. |
| **G10 — standby activation** | Real 1->2->1 active-cohort change under queue pressure/idle, observed simultaneous native execution and safe downshift; standby remains provisioned explicitly, no physical-autoscale claim. |
| **G11 — end-to-end native correctness** | Two tenants, at least one nontrivial aggregate and join, expected outputs from **independently authored literal fixture/oracle**, empty/boundary outputs, no foreign/partial reads; exact schema/multiplicity/committed hash and worker closure. |
| **G12 — security/resource faults** | Oversized/slow/forged request, auth brute/resource exhaustion bounds, unauthorized source, symlink swaps, state corruption, disk-full/permission errors, cancellation while output link races, failure-message redaction; demonstrate safe denial. |
| **G13 — diagnostics/ops** | Stable structured reason codes for enqueue/admit/defer/preempt/activate/quarantine; monotonic lifecycle transitions, tenant-filtered views, counters with explicit unknowns; operator recovery runbook and true capability surface. |
| **G14 — measurements** | Predeclared matched static FIFO/single-cohort versus adaptive scheduling campaign on same executable/data/host: throughput, p50/p95 queue latency, fairness, cost/slot-ms, actual overlapping processes, peak RSS, disk, CPU where measured. Correctness guards and raw samples, no unjustified speedup claim. |
| **G15 — compatibility/CI** | `cargo fmt --all -- --check`; `cargo check --workspace --all-targets --locked`; strict all-feature `clippy -D warnings`; all-feature workspace build/test with feature-matched workers; new crate no-default where applicable; `bash scripts/smoke.sh`; repository Python unittest suite; Rust 1.94 MSRV; final-SHA CI status. Do not hide service-dependent skips. |
| **G16 — detached verification/tamper** | Offline verifier validates source/binary/fixture/criteria/receipt integrity, independent positive and negative case counts, stage-reached witnesses, schedules and output semantics; fully rehashed tamper archives (duplicate job, swapped tenant, missing failure, forged cost/share, altered worker boot, stale SHA, relabeled support) **must be rejected**. |
| **G17 — release reporting** | Accepted ADR, operator guide, implementation report, support JSON, evidence manifest/archives and README/AGENTS updates; `PASS/FAIL/PARTIAL/NOT_RUN` for each gate, blockers and deferred scope; historical predecessor claims unchanged. |

**Mandatory live campaign:** do not call a mocked scheduler proof a multi-tenant runtime test. Execute *two concurrent local native jobs* against two independent supervised cohorts; verify actual worker endpoints/PIDs and result bytes. Fault cases must identify the exact reached stage, injected fault, expected refusal/terminal result and post-recovery quiescence. Process `SIGKILL` is not power-cut evidence; disposable filesystem ENOSPC is not simulated `write()` failure. If any required real fixture is unavailable, mark affected gates **PARTIAL/NOT_RUN**, not PASS.

## 6. Release evidence and measurements

Freeze criteria and fixtures **before** candidate runs; keep failed attempts immutable (candidateN), including changes to requirements and reasons for each. Record baseline SHA/branch/dirty state; final source input inventory with relative paths and hashes; compiler/OS/CPU/memory, build features, worker/coordinator/manager executable hashes, worker boot IDs, distinct stores, all command lines/config/price models without credentials, service versions, native source/plan/manifest hashes, independent oracle, per-gate JSON receipt, archive seal and verifier provenance. Keep a separate immutable receipt pin outside the candidate's self-described contents. The detached verifier must distinguish `current-source` from `historical-archive`, require an explicit source+binary match for current-source claims, and fail when any required proof is missing, inconsistent or unobserved. Publish a current-source tamper suite that **rehashes** modified archives to test semantic checks.

Run fixed baseline/adaptive workloads with the same pinned immutable inputs and cohorts. Measure real queueing/admission, preemption waste, cohort idle time, throughput, latency distributions, managed byte ledgers, total IPC staging/durable output, actual file lengths and RSS/CPU if accessible; explicitly label unmanaged/unknown costs. Use performance thresholds only when predeclared and defensible against an actual baseline. No rewrite of older benchmark JSON, previous PRD gates or historical acceptance archives.

## 7. Failure envelope, deferred work, and stop conditions

Completion may truthfully claim **bounded local resource management with tenant-aware scheduling across separately fenced fixed managed-worker cohorts**. It may **not** claim production multi-tenant security, tenant OS isolation, cross-node/region deployment, Kubernetes provisioning, physical worker autoscaling, streaming/UDF/connector multi-tenancy, shared state migration, global fairness, high availability, arbitrary dataflow or exact cloud billing. Optional features cannot silently satisfy a mandatory PRD feature with a label change. Real safe preemption and real concurrent worker cohorts are mandatory; if either cannot be delivered, mark the corresponding gate incomplete and status partial.

Stop automatic scheduling and demand operator intervention when the manager cannot prove unique ownership, source integrity, preserved tenant identity, bounded resource reservation, native drain, committed output or reproducible cost identity. Work may proceed on independent safe cohorts only if their state and budgets remain provably isolated.

## 8. Required implementation outputs

1. `docs/adr/0030-bounded-local-multitenant-resource-manager-v1.md` (or documented next free ADR number) with considered alternatives, exact invariants, precedence reconciliation and threat model.
2. Opt-in `ade-resource` manager library/CLI with finite authenticated IPC, durable queue/state, scheduling/quotas, safe native cohort adapter, real preemption, accounting and standby activation; isolated source/worker fixtures and tests.
3. `scripts/phase23/` repeatable fault/benchmark/evidence scripts with independent oracles, strict detached verifier and fully rehashed tamper attempts.
4. `docs/phase-23-resource-manager-guide.md`, `docs/reports/phase-23-implementation-report.md`, `docs/reports/phase-23-support.json`, `docs/evidence/phase23/` and accurate README/AGENTS update.
5. One final source-bound completion or partial-status report containing exact checks, commands, results, known source/CI discrepancies and a prioritized outstanding ledger. No implicit authorization of Phase 24.

## 9. Recommended sequence and non-regression checklist

First prove new manager's policy/state logic in isolation; second prove one true `ade-ops` integration with unchanged Phase12 receipt; third prove two independent cohorts overlap; fourth harden auth, replay and preempt/commit races; fifth add cost/activation; last freeze full live campaign and detached verifier. Re-run source-binding tests **after** any code changes. Keep clean worktree discipline and do not commit generated build products or unsealed evidence.

Before declaring success, re-read `AGENTS.md`'s **current** phase status sections; recheck prior Phase 18, 20, 21 and 22 current-source qualification rather than repeating a stale README headline. No acceptance evidence from older source revisions can upgrade a new revision without a new matched campaign.

## 10. Open design questions the implementation and reviewer must challenge

- **Substrate:** Could a dedicated bounded local manager on top of Phase12 `ade-ops` be less safe/correct than extending an existing manager interface? Compare complexity, one-active-store invariant, auth surface, live proof cost and future portability before selecting.
- **Tenancy:** Does local UDS + high-entropy credentials actually prevent the claimed cross-tenant operations? Which same-UID, root, symlink, credential-file, log and socket attacks remain? Prefer a narrower truthful claim over fictitious isolation.
- **Fairness:** Does the chosen priority/aging/weighted policy guarantee progress for each *eligible* tenant under continuous contention and unequal job costs? Is the implementation algorithmically work-conserving, bounded and independently model-checkable?
- **Preemption:** Is cancel/drain/fresh-job restart a safe and worthwhile implementation? Does the native commit race allow duplicate logical publication? Is there a workload that cannot be replayed despite apparently finite sources?
- **Cohort utilization:** Does a second `ade-ops` store actually give independent concurrent execution, or can worker boot, global process budgets, source access or output-root naming still collide?
- **Cost:** What are hard reservations vs best-effort measurements, and can an unknown/late native counter cause an uncharged partial attempt or budget reset?
- **Autoscaling:** Would standby activation meet the Phase23 research objective, or must a separately authorized physical provisioning follow-up remain? Prove capability without mislabeling.
- **Evidence:** Can an independent verifier, with no trust in candidate-produced expected rows or gate statuses, disprove the claimed result using mutated but rehashed receipts? Are adversarial cases truly reached in live processes?

**This plan is deliberately challengeable.** An independent reviewer must inspect the PRD, architecture, changed files, runtime and tests from first principles and identify missed Phase 23 requirements, unsafe design decisions, untested boundaries and incorrect claims **even when this document says a gate passed**. Reconcile a superior solution through a reasoned ADR/plan amendment before implementation; never narrow a PRD outcome merely to satisfy the plan.

## 11. Implementation reconciliation

ADR-0030 selects one in-process native coordinator per cohort, leaving worker
processes independently supervised. One native registration can run once.
After a manager kill, Phase 12 proves the old session drained or holds the
cohort. Safe preemption supports one fresh whole-job launch under the original
logical deadline. The wrapper adds narrow Phase 12 APIs for an absolute-expiry
registration, source verification and worker boot probing. The existing
relative-time registration and CLI retain their contracts. The queue, pricing,
scheduling and accounting choices are specified in ADR-0030.

Final-SHA GitHub Actions is post-commit evidence. The checked-in matrix records
the local source-bound campaign and the at-recording CI status. A final CI receipt
must refer to the exact release commit and satisfy every required CI job. The
receipt is external to that commit so it can attest the commit's immutable ID.

## 12. Independent review reconciliation — 2026-11-14

The additive review remediation is recorded in
`docs/reports/phase-23-independent-remediation-2026-11-14.md`. It corrects
deadline-aware aging, permanently exhausted native history, adds an initial
bounded durable decision receipt, and strengthens current-source CI and attempt
verification. These are within the existing Phase23 contract. This amendment
does not waive any G0–G17 evidence requirement. All gates remain partial until
a fresh campaign matches the committed source, release documents, executables
and fixtures. The prior candidate11 measurements and acceptance report are
historical. An out-of-archive receipt pin must be obtained independently after
all exact-final-SHA CI jobs complete.
