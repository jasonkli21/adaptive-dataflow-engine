# Adaptive Dataflow Engine — Phase 13 Implementation Plan

## Disaggregated batch shuffle: bounded remote-storage v1

**Status:** Implementation authorized by the user on 2026-10-28, in this session, without subagents and with one local commit. G0 independently passed at baseline `474f8c9`; COMPLETE_BOUNDED_REMOTE_V1 at clean acceptance source `ead9de6`, with final docs/evidence in the implementation report. Full PRD/cloud production completeness is deferred.
**Prepared:** 2026-10-28.
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine
**Source baseline inspected:** `474f8c92ff13ffb59c63d8fd2edd3be4ce6019bb` (`main`, Phase 12 review remediation).
**Source-of-truth order:** `AGENTS.md` → `docs/product/prd.md` → `docs/architecture/architecture.md` → this plan → accepted ADRs → reports → actual code/tests. A newer HEAD requires an explicit delta/reconciliation before work.

This plan is an intentionally bounded implementation of the PRD's **Phase 13 — Cloud-Native / Disaggregated Execution** (`docs/product/prd.md`, Phase 13). It does **not** silently reclassify the full PRD phase as complete. It selects a meaningful first slice: *real remote object-backed batch shuffle, proof of compute/storage lifecycle separation, and honest cost/failure evidence*. A production cross-host cluster, autoscaling and cloud HA are not implied by that slice.

## 0. Baseline reality and non-negotiable entry gate

At the inspected commit:

- Phase 10 is **PARTIAL**: fixed 2–4 workers; leased/static_projected are constrained research modes, and dynamic physical membership/migration are absent. Historical failures remain historical; later local timing passes do not establish cross-host performance.
- Phase 11 is **COMPLETE bounded v1**: versioned ADE-owned physical IR, native frontend-independent runtime, narrow SQL/DataFrame, static aggregate and bounded inner join. Preserve the independent oracle, dataset/result schemas and static defaults.
- Phase 12 is **COMPLETE bounded v1 in its own report**, but not production HA: `ade-batch`, one trusted same-host coordinator, fixed externally supervised loopback workers, durable local metadata, fenced whole-job retries, no-clobber local output, limited quotas and failure-domain refusals. `ade-batch` recipes reject non-loopback endpoints and operator spill. This path must not become accidentally remote-capable.
- Existing `ade-distributed` already has `ObjectStoreBackend` injection and `ExchangeMode::ObjectStoreMaterialized` / complete-copy materialization (`retention.rs`, `materialization.rs`, ADR-0011/0014). The default is an **ephemeral local filesystem**; the worker CLI's shared-filesystem fixture proves at most process-surviving storage, not cloud durability. `object_store = 0.13.2` currently enables `fs` only. The current object-store durability enum is a *caller assertion*; do not relabel it proof. Existing per-operation remote I/O handling is cancellation/deadline bounded at the caller but may leave detached work; poisoning and resource containment must be revisited before repeated cloud failures.
- Phase 12 independent review found F12-01 (owner-lock contention caused unrelated cancellation), F12-02 (coordinator/frontend semantic compatibility), and F12-03 (unverified list success). `474f8c9` contains fixes plus locally reported checks, including 43 `ade-ops` tests and 242 workspace tests; there is **no independently witnessed post-fix re-review in the inspected record**.

**Hard entry gate (G0):** Before Phase 13 code changes, independently inspect/reproduce the Phase 12 F12-01/F12-02/F12-03 fixes and the remaining V12 verification gaps against the actual new HEAD, using a clean, feature-matched build and real processes. If a P0/P1 reliability/cancellation/publication issue remains, **STOP Phase 13 implementation** and deliver a source-pinned Phase 12 blocker handoff. P2 evidence gaps must be explicitly dispositioned; do not conflate local tests with independent proof. The user must explicitly authorize Phase 13 implementation in the execution session; creation of these planning files is not that authorization. Read `AGENTS.md`'s phase discipline and update status only when authorization and G0 are satisfied.

## 1. Phase 13 interpretation and completion vocabulary

The PRD lists remote shuffle plugin, object-store-native shuffle, ephemeral workers, independent compute/storage scaling, cross-AZ/egress awareness and optional serverless experiments. These are broad, distinct deliverables. This slice targets only a coherent **remote-disaggregated batch path**, leaving the rest visible.

| PRD item | Bounded Phase 13 v1 obligation | Still open after bounded v1 |
|---|---|---|
| Remote shuffle plugin | Explicit opt-in provider-neutral storage-backed exchange with actual separately running S3-compatible endpoint; no local-filesystem masquerade | Dedicated remote shuffle service and general transport selector |
| Object-store-native shuffle | Immutable, integrity-checked partition segments and complete committed manifest, read by another process after producer exits | General DAGs, shared cache, cloud-wide lifecycle/reuse |
| Ephemeral workers | Show fresh final-consumer process(es) with fresh scratch and boot identity can consume completed remote intermediates; stale producers fenced | Arbitrary in-flight replacement, automatic decommission, autoscaler |
| Compute/storage scaling | Remote store lifecycle independent of worker scratch; demonstrate separate consumer cohort/scale experiment with stated fixed-topology constraints | Independent elastic physical membership inside a running job (Phase 10 debt) |
| Cross-AZ/egress awareness | Per-transfer placement labels and explicit request/byte/egress price inputs; deterministic cost estimates and observed counters, clearly marked estimates | Measured cloud networking and production placement scheduler |
| Optional serverless | Document an experiment design only if environment permits | Serverless deployment/execution; optional, never required for acceptance |

**Completion terms:** (a) `PARTIAL` = implementation or independently reproduced acceptance incomplete; (b) `COMPLETE_BOUNDED_REMOTE_V1` = all mandatory gates below pass for an external S3-compatible service and real process turnover; (c) `FULL_PRD_PHASE13_COMPLETE` must **not** be claimed from this slice. A local in-memory/local-FS fake alone cannot earn (b). No user credentials, cloud account or paid infrastructure are presumed.

## 2. Supported domain / out-of-scope contract

**Mandatory target:** One static fixed-hash distributed batch aggregate with 2–4 fixed worker IDs and immutable, verified local Parquet inputs; explicit `remote_object` (name chosen by implementer) exchange selector. Run the existing independent oracle and exact terminal/aggregate validations. A production-mode path must reject unsupported conditions before any external side effect. The normal `ade-bench` / `ade-query` / `ade-batch` defaults stay unchanged.

**Potentially supported after proof:** The Phase 9 bounded HashOnly two-source join may reuse the new mechanism only if independently tested for both sides, duplicate/bag semantics, ordering-independent results, and bounded failure cleanup; otherwise explicitly reject it. No implicit `ade-batch` remote capability: its Phase 12 publication and worker-fencing contract is separate. If integrated, that requires a separately demonstrated local-durable to remote interface contract, new ADR and full crash matrix—not a flags-only change.

**Explicit exclusions:** leased scheduler, `static_projected` diagnostic, sampled topology, adaptive partial, cost-based complete-copy automatic restore, legacy one-final-loss recovery, dynamic worker add/remove/migration, multi-job shared shuffle, remote source/catalog, arbitrary SQL/DAGs, streaming, checkpoints, exactly-once external sinks, replicated metadata, cross-host coordinator HA, multi-tenancy, process RSS hard cap. Any later widening requires independently enumerated admission and failure proofs. Preserve current refusals.

**Trust/security:** a trusted test coordinator/workers, isolated private bucket/prefix, TLS and credentials provided via ordinary provider configuration, no credential serialization; for a local S3-compatible test endpoint only, explicit non-TLS opt-in. An untrusted store is an integrity and availability threat, not assumed to execute code. No broad IAM/bucket delete or opportunistic cloud setup. A missing remote endpoint or credentials is a clear refusal, not fallback to `LocalFileSystem`.

## 3. Architecture and safety contracts

### 3.1 Mechanism separate from policy

Keep hash/bag distribution, logical transfer identity, attempt number, topology digest and destination ownership distinct from placement. Reuse existing `WorkerEndpoint`, `TransferId`, manifests, `Objects` and object-store injection where valid; introduce a narrowly owned segment-store adapter if their current APIs cannot meet remote shuffle semantics. Do not turn the frontend or benchmark adapter into the core runtime, or rewrite the distributed engine around a new storage API. Keep `ade-bench → ade-distributed → ade-exec` and `ade-frontend → ade-physical`; runtime remains DataFusion-free.

Remote placement must be explicit and disable-able; default static/local baselines remain source- and schema-compatible. A worker must never silently fall back from a configured remote backend to a worker-local disk/temporary object store.

### 3.2 Immutable remote transfer and commit protocol

Before code, write `docs/adr/0020-remote-disaggregated-shuffle-v1.md` with the selected object-store semantics and a protocol table. Minimum properties:

1. Namespaces are unique per job/execution/attempt and never reused after cancellation; key components are validated, bounded, canonical and not attacker-controlled paths. A completed prefix from an old attempt is never eligible for a new attempt.
2. Bounded encoded segments retain existing exact logical transfer (job/exchange/partition/producer), monotone sequence, row/byte counts, independent BLAKE3 checksums and wire/codec version identity; track actual encoded bytes, object count and a hard total budget. No unbounded in-memory `Vec<Batch>` aggregation.
3. Writer uploads data to attempt-private immutable keys, verifies responses and writes a **terminal manifest** with exact expected segments and hash chain. The manifest is the only eligibility signal; readers never infer completeness from prefix LIST or object count. Define conditional-create/CAS (or an equally rigorous single-writer proof) and visibility ordering against the chosen S3-compatible backend; don't assume atomic rename, multi-object transactions or universal provider consistency.
4. Consumer validates manifest schema/version, source/plan/topology, store identity, attempt fence, complete expected producer/partition terminal set, content hashes, exact counts and every read. Missing/partial/old/extra/unexpected segments, stale manifest, wrong layout and corrupt bytes fail closed with **no partial query/publication success**.
5. A published manifest must be immutable. A duplicate write can only be recognized as exactly identical/idempotent; divergent content or out-of-order/late attempts fail. A cancelled/timed-out writer cannot later make its attempt visible. No GET after an unverifiable PUT and no success based on backend durability labels alone.
6. Remote objects have a declared retention lifecycle: terminal success/failure, crash, retry, TTL and operator-driven garbage collection. Delete only an exact namespace or exact recorded keys after validation; default GC dry-run, never broad prefix/bucket deletion. Preserve committed data while readers/recovery still reference it. Explicitly account for object versioning, orphan writes and concurrent readers.

If the provider lacks required create/visibility semantics, **refuse the configured mode** or implement a proven coordination fence; don't weaken correctness to gain portability.

### 3.3 Bounded I/O, cancellation, recovery envelope

Separate per-task memory, queues, object request concurrency, bytes, requests, local staging, time and external storage quotas. Specify concrete defaults and maxima in ADR/CLI, with rejection tests at boundaries; all resource and timeout failures are errors, never partial output. Check remote operations for cancellation, deadline, retryable versus permanent error, idempotency, and late completion. If detached I/O can outlive a cancelled task, bound outstanding operations; poison/quarantine until they drain and guarantee no later manifest publish. Do not assume a process RSS ceiling.

**Supported worker-turnover window:** after a complete, verified remote transfer manifest exists but before final consumption completes, a producer can exit and a *fresh final reader process* with new boot identity and scratch path reads the same committed remote data. This is a **stage-boundary handoff/replay**, not arbitrary in-flight reassignment. Prefer an explicit two-stage test driver/opt-in experimental path over mutating the old Phase 6 recovery or Phase 12 managed-ops protocols without proof. Final visible query result is committed only once all expected final terminals pass; failure returns no partial output. Any ambiguity/stale actor/read error refuses or cancels the run. If this end-to-end experiment cannot be integrated safely, Phase 13 remains `PARTIAL` and the blocker is described; a storage-unit test is not sufficient.

### 3.4 Provider, networking and security

The code base currently depends on `object_store` 0.13.2 with `fs` only. Prefer its public S3-compatible API through a narrowly gated feature, with stable dependency resolution and `Cargo.lock`. Use a separate MinIO/S3-compatible service fixture for mandatory integration; do not require paid AWS. Test that the endpoint is genuinely separate from worker-owned temp storage and survives worker exit/restart. The fixture can be loopback network service; that proves **process/store separation**, not multi-host/AZ durability. A real managed provider run is optional and must have explicit user-provided authorization/configuration.

Credentials are not part of receipts, logs, CLI echoed JSON, build hashes or manifests. Validate bucket/prefix/endpoint configuration; require TLS by default, redact sensitive errors, avoid accidental public ACL/anonymous fallback. Explicitly label remote store **provider-declared** durability, observed lifecycle survival, or independently verified failure-domain durability; do not conflate them. Preserve the legacy loopback control path and avoid exposing unauthenticated ADE control ports to public networks as part of this slice.

### 3.5 Placement and cost evidence (no speculative optimizer)

Record immutable per-job effective config: declared worker/store region or zone label, object request/byte prices, egress-per-byte price, measured bytes/requests/retries/latency, transfer counts, wall-time, cleanup, estimated dollar cost and whether rates/placements were **declared**, **measured** or **provider-billed**. Test deterministic policy/model calculations and overflow/saturation. Optional advisory policy may compare local versus remote on observed input; don't auto-switch without independent correctness gates. Egress/latency are environmental inputs, not proof of actual cloud topology. Reuse Phase 7 cost vocabulary where feasible but don't falsify older phase evidence.

## 4. Work packages, in dependency order

**WP0 — Preconditions and independent Phase 12 gate.** Capture exact SHA, dirty state, toolchain, features and existing smoke. Re-run targeted F12 tests and independently witness hash contention, compatibility mismatch, marker-list behavior and relevant V12 crash windows. Document PASS/BLOCKED/UNVERIFIED. Stop on P0/P1. Read Phase10/11/12 reports/ADRs and explicitly reconcile changed sources.

**WP1 — ADR and contract-first proof harness.** Commit ADR-0020 only after contract design review; define support/refusal matrix, protocol identities/capabilities, failure model, object consistency assumptions, resource budgets, source/attempt ownership, witness format. Build deterministic S3-compatible fixture without cloud cost and seed independent reference aggregate dataset. Preserve historical wire/result schemas unless versioning requires a new opt-in message.

**WP2 — Provider plug-in and immutable segment store.** Add opt-in provider wiring (no default changes), exact namespaced keys, checksummed bounded segments, conditional/guarded terminal manifest, strict read verification and structured counters. Unit/property tests for manifest/hash/attempt identity and malicious/malformed store responses. Explicit concurrency/cancellation/cleanup coverage. No silent locally backed implementation under the remote name.

**WP3 — Real static aggregate remote exchange.** Execute a full real 2-worker and 4-worker static GroupBySum via remote-backed distribution, with a separate store process and independent oracle. Preserve original terminal coverage, no-partial semantics, schema and result contracts. Test both empty/nonempty partitions, skew, duplicate rows, overflow/error and source drift. Existing `direct/buffered/spill/local_materialized/object_store` baseline tests remain unchanged.

**WP4 — Disaggregation and ephemeral read proof.** Introduce minimum safe stage-boundary reader turnover: produce/commit remote shuffle; stop producer process(es); start fresh reader process(es) with fresh scratch/boot but same validated logical manifest; complete final aggregate and gather. Prove no local producer artifact is used. Test stale reader, double-start, cancelled owner, store outage/restart, missing/corrupt data, worker crash before/after terminal manifest. No in-flight worker replacement claim.

**WP5 — Cost observations and controlled experiments.** Add declared regions/egress/request prices and measured request/byte/latency counters. Conduct identical input/config reference-local versus remote runs, 2/4-worker and balanced/skew. Predeclare conditions and capture complete raw per-run outputs and error samples; show overhead and cost honestly, not guaranteed speedups or provider-verified cross-AZ behavior. If real cloud/AZ environments are unavailable, mark those conclusions `UNVERIFIED`.

**WP6 — Documentation, release gates and clean handoff.** Update `README.md`, benchmark docs, ADR/readiness matrix, operations guide (including strict refusal in `ade-batch`), `docs/reports/phase-13-implementation-report.md`, `docs/reports/phase-13-unsupported.json` and raw source-bound evidence. Final clean check; no performance gates moved and no old results relabeled. Explicit conclusion `COMPLETE_BOUNDED_REMOTE_V1` / `PARTIAL` / `BLOCKED`.

**Scope-control rule:** if WP4 needs generalized dynamic worker replacement, cross-host coordinator, nonlocal source routing or an unstable rewrite of Phase12 fencing, stop and mark PARTIAL with a narrow recommended next milestone. Do not hide a broad redesign inside Phase13 and claim it done.

## 5. Mandatory independent acceptance matrix

| Gate | Required witness | Pass condition |
|---|---|---|
| G0 prior reliability | Clean builds, targeted real-process F12-01/02/03 and V12 fault cases | No unresolved P0/P1; independent output/restart checks |
| G1 provider truth | Separately running S3-compatible endpoint + strict configuration | Remote backend actually served all exchange PUT/HEAD/GET; no FS fallback; no credentials leaked |
| G2 exact semantics | Real static 2/4-worker aggregate versus independent oracle | Bitwise/canonical result equality, complete terminal manifest, no extra/partial output |
| G3 worker turnover | Fresh reader boot/scratch after producer death and remote publication | Result succeeds only with exact committed remote manifest; stale/ambiguous readers refuse |
| G4 integrity / faults | Corrupt/missing/truncated/stale/duplicate/late writes; outage, timeout, cancellation | Fail closed, no visible partial result, no stale attempt promotion |
| G5 bounds / cleanup | Too many segments/bytes/requests, contention, disk budget, GC, retry storms | Enforced admission/runtime caps; no runaway background I/O or broad deletion |
| G6 existing regressions | Prior `ade-bench`/frontend/managed ops tests and static/local modes | Defaults and independently oracle-validated outputs unchanged; Phase12 publications remain safe |
| G7 reproducible evidence | Source SHA, toolchain/features, service version, exact commands, configs and hashes | Independently replayable, failures retained; claimed results correspond to source |
| G8 cost truth | Identical-source local/remote comparisons and instrumented model | Measured overhead and model assumptions separated; no unverified cloud cost/HA claims |

Final checks when tooling exists: `cargo fmt --all -- --check`; `cargo check --workspace --all-targets --locked`; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo test --workspace --all-features --locked`; `bash scripts/smoke.sh`; selected Phase 4–12 correctness regressions; Python unittest suite. Run cloud-feature-focused tests separately. Use clean builds and source-bound artifacts; do not call local timing noise a release performance win. If native tests/service cannot run, document each as `UNVERIFIED` and withhold the bounded completion claim.

## 6. Explicit post-v1 backlog (no laundering into acceptance)

- Real multi-host and cross-AZ deployment, service authentication/TLS for ADE control/data, network addressability beyond loopback, cross-host stable source/data access.
- Full production remote shuffle service and memory/tier routing, general SQL/joins/complex DAGs, multi-tenant namespaces and billing.
- Physical add/remove/replace with lease/state migration and spot-drain (outstanding **Phase 10**).
- Replicated metadata and cross-host coordinator failover, backup/restore, truly atomic external sinks (outstanding **Phase 12**).
- Automatic cloud cost/egress-aware placement, capacity scaling, isolation, service SLOs, K8s/serverless orchestration and measured cross-AZ costs (remaining **Phase 13**).
- Unbounded/streaming progress and checkpoints (Phases **14+**).

## 7. Read-before-implement source map

Pinned links, all at inspected SHA `474f8c92ff13ffb59c63d8fd2edd3be4ce6019bb`:

- `AGENTS.md`, `docs/product/prd.md` (`Phase 13` and success criteria), `docs/architecture/architecture.md` (``9–10, 12–14, 24, 28–31`; INV-1–10).
- `docs/adr/0010-static-unified-exchange.md`, `0011-retention-io-containment-and-backend-injection.md`, `0013-lineage-recovery-v1.md`, `0014-expected-cost-materialization-v1.md`, `0017-morsel-scheduler-v1.md`, `0018-physical-ir-and-frontend-v1.md`, `0019-durable-batch-operations.md`.
- `docs/plans/phase-10-implementation-plan.md`, `phase-11-implementation-plan.md`, `phase-12-implementation-plan.md`; `docs/reports/repo-wide-readiness-2026-10-26.md`, `phase-10-rereview-validation-2026-10-26.md`, `phase-11-implementation-report.md`, `phase-12-implementation-report.md`, `phase-12-unsupported.json`, `docs/phase-12-operations-guide.md`.
- `crates/ade-distributed/src/{retention.rs,materialization.rs,exchange.rs,worker.rs,worker_managed.rs,coordinator.rs,gather.rs,protocol.rs,managed.rs,topology.rs}`, `crates/ade-distributed/src/bin/ade-worker.rs`, `crates/ade-ops/src/{recipe.rs,runner.rs,publication.rs,store.rs}`, `crates/ade-physical/src/lib.rs`, `crates/ade-bench` and associated process/fault tests.
- `crates/ade-distributed/Cargo.toml` currently pins `object_store 0.13.2` with `fs` feature. Verify exact public S3 builder/conditional-write APIs and provider semantics against installed version before coding; avoid unreviewed dependency upgrades.

**Use this plan with a separate, explicitly authorized implementation prompt.** The independent review prompt should first derive obligations from PRD, architecture, code and actual fault model; this plan is evidence to be critiqued, not the acceptance oracle.
