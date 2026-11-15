# Phase 26 Implementation Plan — Bounded Cross-Job Adaptive Shared Materialization v1

**Status:** Implemented bounded v1. Local acceptance passed. Exact-commit hosted CI pending.

**Date:** 2026-11-14 (America/Los_Angeles)

**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine

**Planning inspection baseline:** `main` at `b364f63c27dc866a78116dea832419cd46d1ba46` ("Remediate Phase 25 independent review"). **Not an implementation-source freeze.**

**Primary requirement:** [PRD Phase 26](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/main/docs/product/prd.md): lineage-addressed computation cache, snapshot/catalog validation, expected-value eviction, security boundaries, cross-job reuse; optional arrangements/indexes.

**Long-lived authority:** `docs/product/prd.md` > `docs/architecture/architecture.md` > this phase plan > accepted ADRs > observed implementation/code. Follow `AGENTS.md`. Any detected conflict must be reconciled explicitly, not ignored.

## 0. Source-state inventory and qualification boundary

This plan is grounded in the checked repository state, **not** in an assumed fully accepted Phase 25 or in an idealized future runtime.

| Source | Present contract / relevance to Phase 26 |
|---|---|
| `docs/product/prd.md`, §16 Phase 26, §§8–9, 13, 17, 21–24 | Cross-job materialization, lineage/snapshots, explicit economics, recoverability and bounded adaptive decisions. The optional arrangements/indexes item is genuinely optional, not an automatic v1 deliverable. |
| `docs/architecture/architecture.md`, §§10–12, 14–17, 22–23, 26–29, INV-1–10 | Separation of metadata facts from policy; snapshots and replayability; physical/runtime ownership; bounded resources; auditable decisions; security and isolation. |
| `AGENTS.md` and `docs/research/benchmarking-methodology.md` | Correctness and independent oracle before timing; versioned, source-bound evidence, default-preserving change and accurate release qualifications. |
| `crates/ade-physical/src/lib.rs` | Versioned native `PlanV1`, `Source`/file fingerprint inventory, local `Scan`, `Filter`, `Project`, `GroupSumCount`, `Sort`, `Limit`; bounded local tree, max 64 nodes; a source can be replayable. **Do not** add a generic DAG/PlanV2 just for caching. |
| `crates/ade-exec/src/physical.rs` | Public `lower`, native `execute`, local source `Catalog::verify` hashes actual Parquet inputs and preflights physical schema/rows/nulls; execution postflight verifies source drift. Use this real native path and maintain verification guarantees. |
| `crates/ade-exec/src/lib.rs` | `LocalPlan`, `execute_to_sink` and bounded `ExecutionConfig` (finite managed budget, not RSS cap). Streaming a prefix to a bounded materialization writer avoids collecting an unbounded result. |
| `crates/ade-frontend/src/catalog.rs` / `execution.rs` | Manifest-bound local sources; query routes through ADE-owned physical IR and executor, not DataFusion execution. Existing public/CLI behavior must remain compatible. |
| `docs/adr/0014-expected-cost-materialization-v1.md` and `crates/ade-distributed/src/materialization.rs` | Phase 7 has an explicit, **single-job complete-transfer** economic model and no durable cross-job cache. Reuse ideas, **not** assumptions about API/remote survival or source acceptance. |
| `docs/adr/0026-incremental-maintained-dataflows-v1.md` and `crates/ade-incremental/` | Phase 19 has local generations/arrangements and independently pinned history; it is **not** a general shared computation cache. Do not silently inherit Phase 19's data model or claim its guarantees. |
| `docs/adr/0028-connector-catalog-v1.md` and `crates/ade-connectors/` | Phase 21 stages pinned source bytes for a **single handle**, explicitly not a durable cache. No implicit reuse of unpinned live Postgres/Flight/Kafka or unchecked table versions. |
| `docs/adr/0030-bounded-local-multitenant-resource-manager-v1.md`, Phase 23 support matrix | Phase 23's locally authenticated tenant manager is a qualified, independent capability. Do not infer cross-UID or cross-tenant cache authorization from it. |
| `docs/adr/0031-specialized-extension-sdk-v1.md`, `docs/adr/0032-measured-cpu-specialization-v1.md` | Trusted extension binaries and ISA/profile hints must not silently change cache identity, deterministic semantics or isolation. Dynamic/untrusted plugins and hardware-specific cross-job execution are out of scope. |

**Known predecessor qualifications at planning HEAD:** Phase 18 `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`; Phase 20 pending independent reacceptance; Phase 21 `PARTIAL_REMEDIATION_PENDING_REACCEPTANCE`; Phase 22 `PARTIAL_REMEDIATION_PENDING_REACCEPTANCE`; Phase 23 `PARTIAL_PHASE23_INDEPENDENT_REVIEW_REMEDIATION_PENDING_REACCEPTANCE`; Phase 24 `REMEDIATION_REQUIRED`; Phase 25 `PARTIAL_PHASE25_REVIEW_REMEDIATION_PENDING_REACCEPTANCE`. See the exact `docs/reports/phase-{18,20,21,22,23,24,25}-support.json` files at the implementation checkout; **recheck before making any new assertions**. Historical Phase 7/11/19 evidence is source-bound and must not be relabeled as Phase 26 acceptance.

**Dependency carve-out:** A bounded Phase 26 local study may use Phase 0/1/2 native/benchmark substrate and the Phase 11 **local** `PlanV1`/native executor with fresh, phase-specific regression proof. Do not require or silently activate unsettled Phase 18–25 paths. A Phase 21 pinned connector may become a later integration only after a separately bounded contract and proof. Distributed shuffle, state, incremental arrangements and resource-manager authentication are **not** v1 prerequisites.

### 0.1 Freeze/authority checklist before implementation

Capture: `git rev-parse HEAD`, tree hash, branch/dirty state, `git status --porcelain`, workspace deps/lockfile, `rustc --version`, `cargo --version`, relevant features, `cargo test` baseline, and path inventory. Record changed source and evidence files. The planning HEAD above must not be mistaken for the commit eventually tested. If `main` moves, inspect the diff, review new ADRs/support statuses, update the scope mapping and pin a fresh start point. Do not rewrite prior phase archives or result claims.

## 1. Outcome, falsifiable claim and explicit v1 slice

**Outcome:** A native, default-off, local ADE facility can persist a deterministic intermediate **after a producer job ends**, validate it under a pinned source/catalog/semantic identity, reuse it in a **different job/process** with a different legal downstream query, and adapt retention/eviction to expected measured value subject to bounded capacity and an explicit security domain. A cache hit must skip **actual ADE prefix operator work**, not merely label a query as a hit.

**Primary falsifiable hypothesis:** For some repeatable local workloads, validated cross-job reuse saves more end-to-end time/declared cost than admission, write, read, validation, storage and eviction overhead; for workloads without expected reuse, adaptive policy declines or evicts the artifact. Record a negative finding if measurements do not support the hypothesis.

### 1.1 Supported narrow native template (required)

- **Input:** one local, immutable, explicitly pinned Parquet `input` source already admitted by ADE's native source/manifest constraints; `Source::replayable == true`. No mutable external database snapshot, streaming offsets, remote namespace or user-supplied nondeterministic source.
- **Reusable intermediate:** a **connected unary** `PlanV1` prefix rooted at `Scan` with an actual `Filter { UInt64 less_than }` and optional pure `Project` (or a clearly justified equivalently useful native unary prefix). Prefer `Scan -> Filter(secondary < threshold) -> Project(key, value_i64, ...)`. Preserve schema, names, nulls, row order and operator identity. Reject unsupported prefix shapes and side effects.
- **Consumers:** at least two **distinct legal downstream `PlanV1` local queries**, e.g. one `GroupSumCount(key, value_i64)` and another separately projected/filter-derived output, that share the same pinned prefix. These must be independent job invocations (prefer separate subprocesses, with root re-open/restart) and must not be two calls in one in-memory session.
- **Miss:** verify source, run the prefix through **ADE's native physical lowering and `ade-exec` pull operator path**, stream bounded Arrow batches to a finite Parquet artifact, attest/commit it, then run the requested native suffix from the committed or freshly staged equivalent. No DataFusion execution fallback and no benchmark-only executor.
- **Hit:** independently verify the requested original source snapshot/catalog and semantic prefix; validate the committed cache entry + physical bytes; compile/rebind a suffix against the **verified cached Parquet as a new native `input` source**, using the existing local `PlanV1`/`ade-exec::physical` path. The suffix is genuinely executed by ADE. Preserve the public/original logical plan identity in decision diagnostics; cache physical source identity is separately identified and never masquerades as the original source.
- A plan with no supported cut, incompatible aliases/schema/ordering, changed semantics, nonreplayable source, unsupported algorithm/UDF, unsupported `PlanV1` target or mixed security domain must **not hit**. Refusal or native bypass must be explicit and tested. In particular, do not rewrite a `Sort`/`Limit` boundary where an output/order/tie-breaker invariant is not proven.

**Implementation choice, not a frozen API:** Add one small ownership module/crate (e.g. feature-gated `ade-cache`) for catalog, identity, policy and filesystem lifecycle; add a minimal opt-in integration to the ADE local executor/frontend and an explicit CLI or SDK entrypoint. Avoid a new general storage hierarchy, PlanV2, service fabric, operator registry or giant future scaffold. Reuse `ade-exec::physical::lower`/`Catalog::verify` and `execute_to_sink`; for the suffix, construct a validated derived `PlanV1` and native binding from a checked cache record, not a second unchecked interpreter. If current interfaces cannot express this safely, add **minimal** internal APIs with compatibility tests and document the limitation instead of faking reuse.

### 1.2 Deliberately out of scope

- Arbitrary-subgraph/DAG equivalence or algebraic query matching, multiple-input joins, partial aggregate reuse, whole-cluster caching, remote/S3 durability, replicas, cross-host/HA, physical worker elasticity or distributed recovery semantics.
- Cross-principal, cross-tenant, cross-UID shared cache access; per-tenant admission under the Phase 23 manager; untrusted callers submitting their own effective principal; hosted cache service or live credentials inside lineage keys. **Security boundary v1 = one operator-controlled local OS identity/trust domain**. Explicitly refuse or isolate other domains; never advertise this as a hostile-same-UID sandbox.
- Streaming, mutable snapshots, incremental/CDC arrangements, automatic retained-index construction, externally visible table/catalog version inference, multi-language or arbitrary Phase 24 UDF artifact reuse, nondeterministic kernels, GPU/ISA-specific cache identity.
- Serving final results as the sole proof of reuse (a final-result optimization may be auxiliary only); zero-copy promises, global RSS guarantees, transactional external sinks, online learned policy, guaranteed minimum cost, broad optimizer integration.

Optional indexes/arrangements remain **DEFERRED** unless the core reuse and economics gates already pass and a separate measured amendment justifies them. There is no default obligation to implement all PRD examples in one phase.

## 2. Semantics, identity and admission

### 2.1 Canonical cache identity (versioned; fail closed)

Define a canonical, unambiguous, bounded serialized `SharedMaterializationKeyV1` and derive `BLAKE3` or SHA-256 digest with domain separation. It must bind at least:

1. semantic contract version and explicit prefix operator implementation/behavior version (including null, type, comparison, ordering and aggregation semantics where applicable);
2. canonical **subplan** with exact nodes, field identities/schema/nullability, projection aliases/order and filter threshold (not just human-readable SQL or a plan-output hash);
3. ordered physical source file inventory (names, byte sizes, rows, verified content digests), manifest/logical/schema fingerprints, snapshot/catalog authority identity and replayability; carry provenance of the source binding;
4. logical security domain from a **trusted** operator/OS credential boundary, not a mutable requester string; identity and capability version of any allowed deterministic native transform;
5. deterministic seed, format/encoding compatibility and any dependency/version known to alter output bytes/semantics; separately record physical artifact build/format version.

Cache **entry key** and **entry/content digest** are different concepts. Cache key cannot include incidental job/attempt IDs (would defeat cross-job reuse); execution/job IDs belong in audit only. Hardware/CPU/ISA and optimizer timings are not semantic inputs when proven equivalent, but unsafe semantically different code must be refused/versioned. Reject unknown identity/version fields, noncanonical serialization, duplicate source entries, missing metadata or collision/inconsistent manifests; never match by untrusted path, table name, timestamps, row count, or caller-selected digest alone.

### 2.2 Snapshot/catalog verification and trust model

- On **both hits and misses**, establish a trusted source inventory independently of cached data. Revalidate immutable original source bytes (hash), schema, row counts, file ordering and catalog/prefix binding before authorizing reuse; if a trusted external snapshot authority is later added, it needs explicit versioned proof and independent tests. A copied expected fingerprint in a request is **not** verification.
- Verify the cache payload's own digest, exact schema/row count, Parquet footer/row-group bounds, format version and key/manifest association before exposing it as a source. Use native `Catalog::verify` for bound execution and postflight where possible.
- Address TOCTOU: use owner-controlled immutable roots and stable open handles/dirfd-style checked accesses when feasible; otherwise define and test the narrower trusted-operator boundary and abort on observed pre/post drift. A `canonicalize()` precheck alone is not race-safe against hostile concurrent replacement.
- Same-size source mutation, directory/symlink swap, stale catalog revision, altered filter/projection, changed operator version, corrupted cached bytes, absent files and mismatched private namespace must all be detected **before successful result publication**. Distinguish **safe cache miss / recompute** from **source integrity refusal**. Never serve old materialization after a revoked/mutated source merely because the cache checksum is valid.
- Cache hits must not silently depend on availability of previous worker processes, live source handles, inherited lock state or a preloaded in-memory index; run a second invocation from a fresh process/root reopen.

### 2.3 Native rewrite proof obligation

The cut/rewrite algorithm must give a short, testable equivalence argument: the materialized prefix has **exactly** the same logical Arrow rows/field values/schema/order as the original prefix for the pinned snapshot; the rewritten suffix receives the identical stream and remains on the native physical path. Every rewritten `PlanV1` must pass `validate()`; every bound source passes the existing native pre/post checks. Preserve original query limits/observable result schema and error behavior. Reject rather than approximate unsupported cases; no bypass of source/output caps, uniqueness, overflow, cancellation or deadlines. The cache may change physical execution work, **never query meaning**.

## 3. Durable local catalog, reader lifetime and crash behavior

### 3.1 Filesystem contract

A single private, operator-owned, non-symlink cache root on a documented local filesystem with standard atomic-link/rename + `fsync` semantics is the only supported backend. Use `0700` root/dirs, `0600` regular data/metadata (subject to platform), ownership/UID and ancestor/path checks; refuse foreign, unsafe, symlink or untrusted writable roots. A separately trusted external operator pin may be needed to prove rollback; **do not claim tamper-proof history from local files alone**.

Versioned entry metadata describes **facts**: key, security scope, immutable snapshot references, native prefix/schema, artifact file inventory and hash, row count/size, commit generation, capability, provenance, observed work and expiry. Policy records desired retention separately. Keep file counts and metadata sizes capped; strictly parse/validate metadata with `deny_unknown_fields` or equivalent. Never store credentials, arbitrary source paths, secrets or raw unbounded caller strings in public diagnostics/artifacts. Record validated paths only where needed in trusted private metadata.

### 3.2 Publish/read/evict state machine

Use an explicit state machine, e.g. `ABSENT -> STAGING -> VERIFIED -> COMMITTED -> PINNED_READ -> EVICTABLE -> DELETED`; incomplete stages are **never** hits. Stage under unique bounded temporary names on the same filesystem; stream bytes while enforcing quota; sync data and manifest; publish via atomic **no-clobber** operation and sync the parent directory. Multiple writers for one key must yield one valid immutable object: an identical independently verified entry may win/idempotently confirm, conflict refuses; never clobber a different key/content under a supposedly equal identity. No successful acknowledgement before durable, independently re-readable commit.

- Readers atomically pin the specific committed generation/artifact before any eviction; validate before and during consumption as permitted by the trust model; eviction/GC cannot unlink live data until readers release. Define bounded lock timeouts/cancellation and deterministic lock ordering. No deadlocks between lookup, writer, lock recovery, cleanup and suffix execution.
- On crash/kill/ENOSPC at every stage, a new process sees either the old valid committed generation or no hit; never partial output, corrupt manifest or unbounded leaked temporary state. Orphan inventory and bounded GC must not erase another active writer's stage.
- If a checksum mismatch or ambiguous state is discovered, **refuse or quarantine** safely (never silently return stale data); determine whether native recomputation can proceed only after source and security admission succeed. If a full trusted cache rollback cannot be externally ruled out, state that limitation.
- Distinguish local process-surviving durability from whole-disk, host, cloud or adversarial durability; no cross-job cache promises beyond the documented failure domain.

## 4. Memory, disk, concurrency, cancellation and API constraints

Establish hard v1 admission caps (defaults proposed below may be adjusted *before* experiments with an ADR and recorded rationale):

| Resource | Proposed bounded contract |
|---|---|
| Original native source/IR | Retain physical `PlanV1` 1,000,000-row / 64 MiB source and 64-node limits; 1 supported source; original runtime schema checks. |
| Cache entry | At most 64 MiB **encoded** payload + capped metadata (e.g. 1 MiB), <=1 output Parquet artifact in v1, <=1,000,000 rows. Limit decoded batch and Parquet row group independently. |
| Cache root | Default 256 MiB total charged bytes including metadata, staged objects and committed payloads; <=32 committed entries; reserved space for writes; no silent quota borrowing. |
| Concurrent operations | Initially at most one writer per key / bounded overall writer count; explicit bounded readers and lock wait. Quota includes concurrent stages and protected pinned readers. |
| Runtime | Existing `ExecutionConfig` bounded managed memory, `execute_to_sink` one batch at a time, bounded I/O chunks and prefetch. Native output reservations and Arrow/Parquet allocator/RSS are **separately observed**, not presumed covered. |
| Time and diagnostics | One shared finite deadline (prefer <=120s for local physical execution); cooperative cancellation across admission/hash, stage, decode, suffix, sync, locks, GC; bounded audit record and file path/name lengths. |

Use checked arithmetic for every byte count and utility score; no saturating arithmetic that turns overflow into a pro-cache decision. Refuse on quotas or uncertain free space before/while streaming; report **actual** written/read/verified bytes and disk occupancy separately. A cache failure must not produce partially successful query output, leak an untracked reservation, or bypass native request limits. Do not equate filesystem quota accounting with allocator/RSS or claim synchronous filesystem calls can always be preempted.

Preserve original public `ExecutionConfig`, `AdeLocalConfig`, `PlanV1` JSON, `ade-query`, `ade-bench`, existing result schema, static/adaptive defaults and prior feature-off behavior. Introduce **additive** feature-gated types/entrypoints or a distinct `ade-cache` binary if needed. Test external Rust consumers with existing struct literals. Phase 25's CPU selection must be disabled/not required in Phase 26 smoke; semantic equivalence across hardware is a separate invariant.

## 5. Pure adaptive value policy, retention and eviction

### 5.1 Mechanism vs policy

Provide two independent components:

- **Mechanism:** verify, materialize, lookup, pin, read, evict, GC, recover staged state; never decides whether an entry is economically worthwhile.
- **Policy:** deterministic pure function over versioned **admitted** facts, measured calibration and explicit trusted config. At least `disabled` (no lookup/write), `always` (cache if legal/capacity permits), `never` (recompute, no read/write), and `expected_value_v1` (decline/persist/evict, with reason). Forced paths still pass all correctness/security/resource checks. The default remains **disabled**.

A model is a declared proxy, not accurate cloud billing or infallible demand forecast. Prefer conservative, checked fixed-point/integer values. Track independently:

- measured current prefix production/scan cost and expected future recomputation (`C_recompute`);
- measured/estimated cache read + source revalidation + decode (`C_restore`);
- write+sync+checksum + capacity/rent until expected expiry (`C_store`);
- bounded expected eligible reuse count/probability and horizon, configured by a **trusted operator**, not inferred from arbitrary request text;
- expected invalidation/expiry risk and future storage opportunity cost;
- entry bytes, pinned status, exact quota headroom and competing candidates.

A simple first model is `expected_net_value = E[eligible_uses] * max(0, C_recompute - C_restore) - C_store - C_hold - C_invalidation`; if an uncalibrated dimension materially changes the decision, conservatively **skip** and explain. If evidence shows a better bounded model, document it in ADR-0033 (if unallocated) before recording release results. No learned model/online feedback loop unless explicitly designed and tested.

Admission/verification work occurs on both cache/no-cache paths and must **not** be omitted from end-to-end economics. A hit's source snapshot rehash may dominate its savings; report this honestly. Record a decomposed work-time and byte ledger (source admission, cache lookup/verify, native prefix, materialization write/sync, native suffix, GC/eviction, postflight) to distinguish observed vs estimated and prevent double counting. No pre-decision leak of private tenant/cached-entry facts.

### 5.2 Expected-value eviction is a **mandatory** demonstration

When quota is insufficient, score the candidate plus all **eligible** incumbents under the same bounded pure value function, account for bytes and expected net future benefit (and optionally value density for greedy selection), produce deterministic action/order/tie behavior, then run actual removal under reader pin/lease constraints. Existing live readers and staged entries are never evictable. A valid candidate may be **rejected** when existing inventory is worth more. Expiration/TTL is a safety/pruning constraint, **not** a substitute for expected-value eviction. If pure policy chooses an action that is infeasible at commit time, re-admit/replan or refuse; never evict blind.

Test at least: two entries competing for a tight quota; expected-value ordering opposite LRU ordering; changed reuse demand; identical/tied values; zero reuse; expired but pinned; overflow; missing calibration; a protected in-flight reader; concurrent publisher conflict; restart with retained decision facts. An eviction audit must record alternatives and actual bytes freed without exposing another principal's keys.

## 6. Security and privacy contract

- Single-user local operator-owned cache root with effective-UID ownership and a fixed operator-managed namespace. Cross-job does **not** imply cross-principal. Disallow caller-controlled `tenant_id`/scope as an auth proof; never use a passed string as authority to access another principal's cache.
- On every lookup/publish/pin/evict, check effective scope **before** revealing hit/miss, existence, sizes, or timing-sensitive per-entry decision details. A separate-principal request should reject at admission or be routed to a separate private root; **do not** claim hostile same-UID isolation. If OS-UID impersonation cannot be tested in the environment, classify that security gate as not run/partial.
- Refuse foreign ownership, symlinks, traversal, hard-link anomalies where applicable, unsafe ancestors and world/group writable roots; prevent injection through names, paths, JSON metadata and diagnostics. Guard TOCTOU with checked handle-based access and revalidation; document residual local trust assumptions.
- Entries must not embed credentials, cleartext source payload samples in logs, remote tokens or arbitrary user-provided strings; protect the source and intermediate at least to the documented local permission boundary. A content hash is integrity metadata, **not** authorization or authenticity.
- Do not claim Phase 23 tenant isolation or multi-tenant resource accounting for this cache. Any future tenant-aware shared cache requires an explicit authenticated service/credential delegation design, revocation and a separate acceptance campaign.

## 7. Work packages, order and expected artifacts

| WP | Implementation | Exit witness |
|---|---|---|
| **W0 — Forensics** | Pin actual starting source, statuses, native query/manifest contract, filesystem primitives and original release checks. Identify one supported prefix + two distinct suffixes. | Baseline/reconciliation section in final report, saved original native outputs. |
| **W1 — Semantic cut and ADR** | Define key/version, prefix cut, eligibility, verified rewrite, principal, failure domain, policy cost units, storage and lock model. Draft ADR-0033 if available. | Independent side-by-side no-cache/prefix/suffix equivalence fixture; ADR with explicit alternatives. |
| **W2 — Cache storage mechanism** | Implement bounded metadata, checksums, stage/commit, reopen/verify, reader pin, quota reservation, eviction/GC, crash recovery. | Cross-process reopen/kill/ENOSPC tests; directory integrity inventory. |
| **W3 — Native materialization and reuse** | Integrate with `PlanV1`/`ade-exec::physical` native lowering + streaming Parquet writer and native suffix. No frontend/DataFusion fallback. | Two separately launched jobs with identical prefix/different suffix, observed scan/filter suppression on hit and independent exact output oracle. |
| **W4 — Policy and explanation** | Pure checked expected-value selection; always/never/static/adaptive arms; deterministic competing-entry eviction and audit. | Independent policy oracle and adversarial unit tests. |
| **W5 — Safety/adversarial** | Source/content/catalog drift, changed semantics, unsafe paths/scope, race, corruption, kill between stages, concurrent readers/publishers, cancel/deadline/quota. | Genuine process and filesystem fault campaign with positive + negative outcomes. |
| **W6 — Reproducible research** | Predeclare and run release-mode fresh-process arms; separately measure cold first write, warm hit, no reuse, eviction pressure. | Raw samples, independent correctness, actual work/byte ledgers, calibrated alternatives, negative results retained. |
| **W7 — Release and docs** | Existing compatibility tests, full available Rust/MSRV/CI, guide, ADR, research note, report, support JSON, detached evidence verifier. | Exact final-commit source/artifact/CI provenance or honest partial qualification. |

Do not start with a general-purpose cache service or a speculative multi-level storage abstraction. First get source identity and native equivalence right; then publish atomically; then add policy; finally claim economics only after source-bound whole-job measurement.

## 8. Acceptance matrix: G0–G21

**Scoring rule:** `PASS` requires independently reproducible evidence from the **exact final source**, not an assertion in a report. Each gate may be `PASS`, `FAIL`, `PARTIAL`, `NOT_RUN`, `BLOCKED` with cause and scope; a single failed critical correctness/security gate prevents completion. Independent reviewers can add tests and gates when the plan is insufficient.

| Gate | Mandatory acceptance evidence |
|---|---|
| **G0 Source/authority** | Frozen baseline + final SHA/tree, clean/dirty states, compiler/build/toolchain/feature flags, changed inventory; main moved/reconciled; prior support statuses unchanged or explicitly updated on their own evidence. |
| **G1 Product/architecture** | Explicit map to PRD Phase 26 and architecture INV-1–10; accepted ADR with documented deviations; no misleading HA, multitenancy, generalized optimizer or prerequisite promotion. |
| **G2 Native-path reality** | A hit skips ADE's **real** prefix scan/filter work; a miss executes that prefix and suffix through native lowering/execution; distinct downstream native queries and separate process invocations; no DataFusion/shadow engine. |
| **G3 Correct semantic rewrite** | All derived `PlanV1`s validate; independent original-source oracle checks exact schema, rows/bags, values, aliases, order where specified, limits/aggregates, null/empty/boundary cases; rejected rewrites demonstrate fail-closed behavior. |
| **G4 Source/catalog identity** | Every hit validates source bytes + schema + catalog authority/snapshot; source, operator/version, projection, threshold and source-file order changes invalidate/reject; same-size mutation and revoked source do not hit. |
| **G5 Cache artifact integrity** | Reopen validates key/manifest/data digest, Parquet schema/row count/footer/group constraints; forged metadata, missing/changed bytes, collision/cross-entry substitution refuse. |
| **G6 Durable atomic publish** | Tests prove no uncommitted/torn entry becomes visible; fsync/no-clobber semantics, writer conflict, idempotent same-key publish and clean restart after termination at each commit boundary. |
| **G7 Reader pin/concurrency** | A live reader survives or safely refuses concurrent eviction; bounded locks, deterministic ordering, no deadlock/starvation within declared bounds; parallel readers, publishers, GC and reboot/reopen tested. |
| **G8 Security boundary** | Trusted effective-principal binding before lookup, 0700/0600/owner/path protections, no caller-supplied impersonation, cross-domain access refusal; accurate same-UID trust limitation; no secrets disclosed. |
| **G9 Resource accounting** | Payload/stage/metadata/quota arithmetic checked; source/output/row-group/decode/memory/deadline/cancellation bounds measured; actual ENOSPC/quota tests; no successful partial query outputs. |
| **G10 Source mutation/race** | Pre/post mutation, symlink/path swap and stale pinned manifests cause refusal, not stale results; demonstrate and document remaining TOCTOU assumption. |
| **G11 Static controls/default compatibility** | Feature-off build, existing `PlanV1`, native/CLI/query/batch benchmarks and public Rust consumers unchanged; cache disabled by default; never/disabled/always operate as declared. |
| **G12 Pure policy correctness** | Checked deterministic inputs, score, rationale, tie/overflow/no-calibration behavior; independent reference policy oracle recomputes candidate actions. |
| **G13 Expected-value eviction** | Real quota pressure with >=2 entries and measured eviction outcome **contrary to LRU**, protected reader and infeasible candidate; exact byte accounting and deterministic evidence. |
| **G14 Audit/telemetry** | Structured versioned per-job decisions (`miss`, `hit`, `publish`, `skip`, `evict`, `refuse`) and observed-vs-estimated ledger; no private cross-domain leakage or double-counted costs. |
| **G15 Independent process/oracle** | At least two distinct OS-process jobs sharing one pinned prefix, distinct legal suffixes, restart/reopen, independent source-row oracle; no cache produced output used as its own ground truth. |
| **G16 Adversarial/detached verifier** | Immutable raw source/fixture/artifact inventory and source-bound fresh-checkout verifier; rehashed malicious mutations targeting payload, key, catalog, policy and result receipts are rejected for substantive reasons. |
| **G17 Economics** | Predeclared fresh-process no-cache/always/adaptive experiments, valid matched inputs and hardware, all samples and negative outcomes, source verification overhead included; at least one credibly positive reuse case and an adaptive no-reuse refusal, or classify research outcome partial/negative. |
| **G18 Crash/fault campaign** | Real subprocess kill/fault stages, disk-full, corrupt/truncated, lock interruption, restart/GC, writer/read race; no false hit, partial successful result, orphan runaway or wrong-principal delete. |
| **G19 Release checks** | `cargo fmt --check`, `cargo check --workspace --all-targets`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace --all-features`, `bash scripts/smoke.sh`, applicable MSRV; recorded actual outcomes. |
| **G20 Exact-commit CI** | Hosted required repository CI jobs and independently retrieved receipt for **final Phase 26 commit**; no substituting historical predecessor jobs or a local green status. If unavailable, `NOT_RUN` / partial. |
| **G21 Reporting/completion** | ADR, operator guide, research experiment, implementation report, versioned support JSON with every gate, limitations/deferred list, reproducible commands/evidence, source/baseline qualification matrix. |

### 8.1 Economics protocol and release decision (predeclare before measurement)

Construct deterministic pinned fixtures including: (A) selective prefix reused by different expensive native suffix queries; (B) cheap prefix/low-or-zero reuse, where adaptive should skip; (C) competing entries under capacity pressure; (D) empty/all/none/selectivity edge regimes. Prefer at least three input sizes/three selectivities where practical. Repeat each macro arm in **fresh process** (at least one warmup + five measured complete job sequences per primary regime), with the *same* source inputs and true independent oracle; randomize/interleave arm order if practical and retain cold population, warm hit and restart costs separately. Predeclare cache reset/hit state, release profile, hardware and driver source. Measure full **sequence** wall time and per-job wall time; first-job materialization cost must count in multi-job benefit, not just second-job hit.

A conservative target is median >=10% **sequence** wall-time improvement over `disabled` for at least one measured repeated-prefix regime, while adaptive does not exceed `disabled` by >20% median sequence wall time in the declared no-reuse control and makes the required correct skip/eviction decisions. This is a **falsifiable research target, not permission to weaken correctness**. If unavailable/noisy/failed, retain raw data and mark `G17=PARTIAL`/`FAIL` or `NEGATIVE_RESULT`; do not retroactively change thresholds or claim general economics. Report p50/p95, all raw samples, confidence/noise caveats, total bytes, process peak RSS (scope) and estimated vs actual cost. No CI timing gates.

### 8.2 Release status taxonomy

`COMPLETE_BOUNDED_SHARED_MATERIALIZATION_V1` only if mandatory G0–G21 pass for matched final source (including economics and exact-commit hosted CI) and evidence is externally/detached verifiable. Otherwise use a truthful scoped `PARTIAL_PHASE26_*` or `NEGATIVE_PHASE26_ECONOMICS` status with failed/not-run reasons. The code can be useful even if the economics hypothesis fails; **the research claim cannot be promoted**. Preserve all predecessor matrices, failure reports, candidate archives and historical source identities.

## 9. Outputs and documentation

Proposed new files (final names may be slightly reconciled with repository conventions):

- `docs/adr/0033-cross-job-shared-materialization-v1.md` — accepted/revised only once implementation design and limitations are actually verified; never pretend an ADR exists before writing it.
- `docs/phase-26-shared-materialization-guide.md` — supported templates, CLI/SDK examples, credential/root setup, security assumptions, capacity/quota, restart/GC, refusals, observability, runbook.
- `docs/research/phase-26-experiment.md` — predeclared hypothesis/arms/cost units/results, including any negative economics.
- `docs/reports/phase-26-implementation-report.md`, `docs/reports/phase-26-support.json` — exact qualification and per-gate evidence.
- `docs/evidence/phase26/` — deliberately finite source/fixture/result/archive hashes, raw measured samples, verifier/tamper records, detached bundle/readme; preserve historical artifacts elsewhere.
- Minimal native library/feature/CLI and dedicated focused tests/scripts under existing repo conventions; no empty speculative crates.

## 10. Design questions to resolve **during W0/W1**, not through extra user prompting

1. Which `PlanV1` unary prefix shape is legal and high-value without changing the public physical IR? **Prefer `Scan -> Filter(secondary < threshold) -> Project(key, value_i64)`**, with a first `GroupSumCount` suffix and another distinct local suffix; implement an explicit typed eligibility checker.
2. Can a valid derived prefix `PlanV1` use existing `ade-exec::physical::lower` and `execute_to_sink` with intact pre/post verified source handles? If an internal API is needed, add a bounded native `execute_plan_to_sink` that retains native verification/cancellation/deadline; **do not** bypass verification because it is inconvenient.
3. Which portable local fs primitives meet no-clobber publish/read-pin semantics on the available OS? Prefer well-audited standard primitives with a documented failure-domain assumption; record exactly what `fsync` does and does not guarantee.
4. What cost units and input features can be measured fairly across distinct jobs? Start with observed wall time and encoded/source bytes as separate fields; estimates must be explicitly labeled and checked.
5. What external authorization is genuinely available? Use OS-owner private scope and refuse wider claims rather than interpreting a free-form tenant name as identity.
6. Is the candidate cache faster enough **after original-source revalidation**, not just after a kernel microbenchmark? If not, publish a negative result and remain bounded; do not disable integrity checking to manufacture a speedup.

**Stop condition:** If safe cross-process source identity, native rewrite equivalence, atomic publish/pin, or trusted principal admission cannot be demonstrated with actual code/tests, finish with a narrow experimental implementation and `PARTIAL` report. Correctness/security are not negotiable in exchange for performance.

## 11. Implementation reconciliation

The authorized implementation uses ADR-0033. The catalog is a separate strict
v1 descriptor and exact externally supplied digest, allowing a frontend-free
native binding and explicit revision without activating a connector. The cut is
node 1 (scan/filter) or 2 (scan/filter/project) over non-null fixed-width integer
fields. Suffixes support native GroupSumCount with optional post-aggregate
projections. Distinct grouping columns provide the two independent consumers.
This bounded contract refuses sorting and limiting in the accelerated path;
original row/byte caps still govern native results.

An exclusive root pin serializes all fills and removals, with eight concurrent
shared readers. Storage reserves conservatively before staging, so a failed
fill can follow successful individual quota evictions. Retention forecasts
remain fixed for each publication horizon and expire before reassessment.
Actual-byte accounting covers plain file lengths, with filesystem block overhead
and native allocator state measured separately. A reader invokes original native
preflight even on a hit. Gates G0–G21 and the economic criteria are unchanged.
