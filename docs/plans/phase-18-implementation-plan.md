# Adaptive Dataflow Engine — Phase 18 Implementation Plan

## Unified bounded/unbounded optimizer and historical-to-live planning (bounded v1)

**Prepared:** 2026-10-30 (America/Los_Angeles)  
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine  
**Inspected `main`:** `fbe0e2b7739fd7d61c45c58706faa56b63a945be` — *Fix Phase 17 transactional review findings*. Recheck HEAD, worktree, status and source contracts at implementation time.  
**Planning status:** Proposed, not implemented. The user requested planning artifacts, not a repository mutation.  
**Prerequisite:** Phase 17 is still `PARTIAL_VERIFICATION` at inspected HEAD. Local review fixes pass, but fresh current-source G15 Kafka/MinIO reacceptance and a matched final-source service campaign were **NOT_RUN** (`docs/reports/phase-17-review-remediation-2026-10-30.md`). Earlier candidate5 G0–G14 evidence is historical only. Keep this qualification explicit.

## 0. Executive decision and scope

PRD Phase 18 (`docs/product/prd.md`, §16) calls for **one logical representation for bounded, unbounded and historical+live pipelines**. Architecture §§3.5, 5, 14–15, 18, 20, 26–31 and INV-1–10 require a frontend-independent ADE physical runtime, explicit boundedness/progress/replay semantics, correctness-constrained optimization, finite resource envelopes and an optimization-disable control.

The smallest credible Phase 18 release is an **opt-in, versioned, shared logical plan and deterministic optimizer** actually exercised by ADE-native **batch** and **Kafka streaming** paths, plus one executable **same-topic Kafka historical-prefix → live-tail** pipeline with proven per-partition handoff. Every path must use the *same* semantic admission/property derivation and optimizer interface, lowering into existing native mechanisms. A JSON-only explain command or two independent planners with common-looking structs is **not** completion.

The historical-prefix demonstration uses Kafka's pinned immutable log; it is not arbitrary Parquet↔Kafka CDC continuity. General streaming SQL, hybrid cross-source joins, changelogs/differential maintenance and iterative dataflow remain deferred. No external sink exactly-once, physical rescale or production HA is added.

**Do not reinterpret `ade-physical::PlanV1`** as an unbounded graph: it is a strict bounded tree, validates `bounded=true`, uses allowlisted local Parquet sources and preserves Phase 11 semantics. `ade-frontend` currently compiles DataFusion SQL and Rust DataFrame to PlanV1. `ade-stream` separately owns `StreamConfigV1`, `EventV1`, demand/ACK, frontiers, keyed windows and v1/v2/v3 state/checkpoint/transaction modes. Phase 18 introduces a neutral *logical* layer that lowers eligible finite shapes into **unchanged PlanV1** and eligible Kafka shapes into **existing stream mechanisms**. Native operators, state and commit remain authoritative.

**Proposed release status:** `COMPLETE_BOUNDED_UNIFIED_PLANNER_V1` only if all mandatory gates pass on matched source/executable/services; otherwise `PARTIAL` / `PARTIAL_VERIFICATION` with exact unrun items. This is not a Spark/Flink-class general optimizer.

### 0.1 Read-first map and source reality

1. `AGENTS.md`; `docs/product/prd.md` §§4.2, 7–9, 11, 14–17, 19 and Phase 18/19/21; `docs/architecture/architecture.md` §§3, 5, 7–9, 14–18, 20–22, 26–31 and INV-1–10.
2. `docs/adr/0018-physical-ir-and-frontend-v1.md`, `0021-unbounded-source-and-progress-v1.md`, `0022-keyed-state-and-checkpoints-v1.md`, `0023-adaptive-checkpoint-placement-v1.md`, `0024-transactional-output-savepoints-v1.md`; relevant joins/exchange/scheduler ADRs.
3. `docs/plans/phase-{11,14,15,16,17}-implementation-plan.md`; `docs/phase-{11-query,14-streaming,15-state,16-adaptive-checkpoint,17-transactional-state}-guide.md`; corresponding phase reports and Phase 15–17 review remediation; Phase 10 readiness debt.
4. `crates/ade-physical/src/lib.rs`; `crates/ade-frontend/src/{catalog,dataframe,sql,execution}.rs`; `crates/ade-stream/src/{lib,kafka,runtime}.rs`, `src/state/{mod,checkpoint,store}.rs`, `src/transactional/{mod,epoch,sink,savepoint}.rs`; `ade-exec`/`ade-distributed` native physical adapters, tests and feature graph.
5. `docs/reports/phase-17-review-remediation-2026-10-30.md` and `scripts/phase17/{criteria.json,acceptance.py,verify_evidence.py}`: independently establish whether current-source G15 was completed after this plan was prepared.

Inspected reality: PlanV1's `Target::{Local,DistributedAggregate,DistributedJoin}`, operators `Scan/Filter/Project/GroupSumCount/Sort/Limit/InnerJoin`, 64 nodes/256KiB, and allowlisted Parquet manifests (≤64 files, 1m rows/64MiB per source). `ade-stream` has default-off `kafka`, `stateful`, `adaptive-checkpoint`, `remote-checkpoint`, `transactional-state`; fixed manually assigned 1–4 Kafka partitions and `EventV1 {key,value_i64,event_time_ms}`. Phase17 v3 exactly-once is only the **committed-visible output prefix within one pinned local job lineage**; do not downgrade it or assert external exactly once. Review fixes add `ACTIVATED3` fail-closed admission, terminal PAUSED, operator-attested retired head and atomic no-replace savepoint exports.

## 1. Supported product / refusal matrix

| Axis | Required Phase 18 bounded support | Must refuse/defer |
|---|---|---|
| Logical semantics | One versioned canonical plan with schema, expression, source, termination/boundedness, event-time/frontier, determinism/replay, ordering, keys/distribution and sink/delivery properties | Forged derived properties; arbitrary shared DAG/cycles; treating a `bounded: bool` as sufficient |
| Batch | Existing accepted Phase11 SQL/Rust shapes through common logical admission and optimizer, then unchanged native PlanV1 | New general SQL/joins, unbounded PlanV1, DataFusion physical execution, weakening manifest/source checks |
| Stream | Opt-in structured logical source/operators for existing Kafka stateless filter/projection and keyed tumbling sum/count, executed through native demand/state | General streaming SQL, streaming joins, unbounded sort/collect, side-effecting UDFs, changed window/late semantics |
| Transaction | Existing v3 local committed-output sink is an explicitly constrained native capability; preserve CURRENT3 and reader/fencing semantics | Silent fallback from committed visibility to stdout/at-least-once, cross-sink 2PC, implicit v3 migration |
| Historical→live | One pinned topic/partition inventory; operator-attested immutable Kafka history, exclusive per-partition cutoff vector, then same-source live tail in one source/state/progress lineage | Arbitrary Parquet+Kafka CDC boundary, multi-source hybrid join, globally atomic cross-partition snapshot, source swaps |
| Optimizer | Deterministic pure legal-property pass, bounded alternatives, hard constraints first, static-disable control, candidate/rejection/selection trace, ≥1 real two-choice target decision | Cost-ranked illegal plans, randomized learned policies, invented price/performance measurements, optimizing guarantees |
| Resources | Caps for plan, search, trace, batch output, native queues/state/checkpoints and deadlines; fail-closed cancellation | Unbounded memo/history buffering, arbitrary automatic replay, hidden process-RSS claims |
| Compatibility | Legacy CLI/API/serialized formats and feature defaults unchanged; new entrypoint default-off/versioned | Silent changes to PlanV1, StreamConfigV1, state config, CURRENT/CURRENT2/CURRENT3, wire/benchmark/evidence |

**Historical/live semantics:** For each pinned Kafka partition `p`, declare and verify exclusive cut `H[p]`. Historical records actually admitted have offsets `< H[p]`; live records have offsets `>= H[p]`. The **same** Engine, watermark/frontier, state, downstream demand and source cursor process both. No synthetic EOF, forced watermark, window-close, source reset or second writer at boundary. Kafka offset gaps are legal: audit *actual* message identities, not a fictitious dense offset sequence. A vector of per-partition offsets is not a globally atomic event-time snapshot; trusted immutable history/incarnation attestation and retention checks remain required.

**Transactional hybrid is conditional.** It can execute only if the cut and semantic plan compatibility remain authoritative across v3 crash, pause/resume, savepoint, codec migration and logical owner transitions, without changing CURRENT3 semantics. If this would require an unsafe new unbound sidecar or breaking v3 format, **explicitly refuse v3 hybrid in v1** and retain existing v3 pure-live support. Real at-least-once hybrid is mandatory; do not call an unsupported transactional hybrid a failure of existing v3.

## 2. Contracts and design

### 2.1 Versioned semantic plan, separate from physical IR

Prefer a focused `ade-logical` crate with real ownership, no DataFusion/Kafka/frontend/runtime dependencies. It defines strict `LogicalPlanV1`, schema/expression types, checked property inference, immutable semantic digest, legal operator/sink contracts and the pure optimizer. If a small `ade-physical::logical` module is substantially cleaner after dependency review, record the choice in **ADR-0025**; **do not mutate PlanV1**. Backend lowering stays in batch/stream composition modules, and runtime libraries never depend on `ade-frontend`.

Proposed source forms: `ParquetManifest` (finite), `KafkaAssigned` (nonterminating), `KafkaRange` (finite bounded-at-snapshot), `KafkaHistoryThenLive` (nonterminating with observable historical prefix/tail). The last two reuse one native Kafka owner. Model **termination** (`Finite` or `NonTerminating`), **phase** (`SnapshotPrefix` or `LiveTail`), and **event-time/frontier** separately; do not confuse offset progress with event time. Bind source/job/operator/window/late/hash, original Kafka starts, historical cut, schema, delivery and sink capabilities to canonical identity, but exclude credentials, endpoint addresses, mutable resource settings and local absolute paths. Independently recompute node properties; reject forged serialized derived fields even if checksums are recomputed.

Admit only existing semantically proven families: finite `Scan/Filter/Project/GroupSumCount/InnerJoin/Sort/Limit`; Kafka `Scan/KeyFilter/Project/TumblingSumCount` with native EventV1 and `receipt_drop_closed_v1`. Batch group aggregation and streaming event-time windows are **not** one interchangeable operator. Finite collect, at-least-once diagnostic stream and v3 committed-output are distinct typed sinks. Unknown replayability, invalid time scope, unbounded sort/global limit/collect, cross-stream join, nonreplayable source with required restore and incompatible sink/delivery combinations refuse *before* effects. Constrain graph/node/size/depth and JSON duplicate/unknown keys.

### 2.2 Genuine optimizer, legality-first

Shared procedure: strict parse → derive properties → enumerate a small allowlist of semantics-preserving native choices/rewrites → reject on correctness, delivery, capability and hard resource ceilings → choose deterministically by documented integer/fixed-point heuristic or measured cost → lower to current native runtime. Emit bounded decision records with semantic hash, optimizer version, source-stat provenance, all candidates/refusals, static baseline, estimate vs observation and chosen target. Cap enumeration count, depth, memo/trace bytes, and cooperative CPU/deadline work. A forced target remains a **hard constraint**, not a hint. Disable switch must produce an executable static baseline.

**Mandatory real choice:** for an eligible *unfiltered finite GroupSumCount* with the existing manifest schema, allow local or fixed-worker distributed aggregate when both meet resource/worker/capability admission. Use source-verified bounded stats and a predeclared conservative rule; show fixtures choosing **both** and matching independent bag-result oracle. No fabricated dollar costs, guaranteed speedup or production claim. Optional batch filter/projection rewrites need independent equivalence proof. **Never** move filters across Kafka watermark/frontier/state/checkpoint boundaries: filtered/late events still advance progress and affect window closure. Streaming checkpoint policies and delivery modes remain correctness-bound subordinate decisions, not cheaper equivalent algorithms.

### 2.3 Actual execution, not planner theater

New opt-in CLI/API must `explain` the strict logical plan **and execute** admitted finite and Kafka workloads through native operators. Existing SQL and typed DataFrame need a unified compilation route `SQL/Rust -> LogicalPlanV1 -> optimizer -> unchanged PlanV1 -> ade-exec/ade-distributed`; new runtime path must actually consume common plan and choices, not merely display them while directly compiling old PlanV1. Structured streaming descriptors likewise use the *same* property/optimizer path before native Kafka dispatch. Native source verification, Arrow kernels, checkpoint and transactional commits remain unchanged. A thin opt-in composition binary (`ade-plan`, name to settle in ADR) is preferable to making `ade-stream` depend on `ade-frontend` or creating another scheduler.

Separate *semantic plan hash* from physical choice, mutable source connection settings and session identity. A semantic change cannot reuse checkpoint/output lineage. A legal physical replan may vary only within proven equivalent semantics. On restart restore exact persisted cursor/state, verify source inventory/history and historical cut; refuse changed cut, lost origin or unknown incarnation. Never infer a missing v3 selector, adopt root clones, bypass ACTIVATED3/lock, expose provisional output, or merge independent imported branches.

## 3. Work packages and sequencing

| WP | Implementation | Adversarial proof |
|---|---|---|
| **WP0 — reconcile & baseline** | Pin HEAD/worktree/features, old query/state/transaction tests and Phase17 G15; freeze acceptance criteria before performance work. | Every claim attached to correct source; no unowned changes or reuse of candidate5 as current evidence. |
| **WP1 — ADR and semantic IR** | ADR-0025, strict versioned `LogicalPlanV1`, source/sink/termination/progress/identity/property derivation and size caps. | Forged schema/boundedness/order/replay/delivery, unsupported combinations, duplicate JSON fields, DAG/cycle and oversized inputs refuse pre-effects. |
| **WP2 — pure optimizer** | Finite deterministic alternative enumeration, legality filters, cost/heuristic trace and static override. | Both existing aggregate targets chosen on fixed eligible fixtures; independently recomputed candidates/decision; no illegal candidate wins. |
| **WP3 — batch lowering** | Opt-in SQL/DataFrame → logical → native PlanV1 → existing execution, differential legacy tests and explain. | All currently admitted Phase11 query families, bags/ordering/overflows/source/postflight parity. |
| **WP4 — streaming lowering** | Native Kafka stateless, keyed windows and explicit v3 local transaction descriptor through common admission, then existing source/runtime. | Real Kafka offset/frontier/late/output equality; commit fences untouched; unsupported combinations refuse pre-network. |
| **WP5 — Kafka history→tail** | Record explicit per-partition snapshot cut and process one same-topic source continuously; prove handoff, restart, concurrent partitions, gaps and idle. | No gap/duplicate in observed record ledger or window state; no invented EOF, reset frontier or lossy cutoff recovery. |
| **WP6 — safety & observability** | Typed errors, resource budgets, cancellation, trace provenance, compatibility and operator guide. | Cap exhaustion/path/tamper/selector/timeout fail closed; no secrets in trace; no defaults/legacy ABI regressions. |
| **WP7 — acceptance & closure** | `scripts/phase18/` evidence runner, independent oracle/verifier, report, support matrix, docs/README/AGENTS. | Pinned source+runtime+real services, raw negative evidence and replayable offline verification. |

Suggested touches: one new **real** neutral logical owner (`crates/ade-logical/`) if justified; `crates/ade-frontend/src/{sql,dataframe,execution,catalog}.rs`; narrow adapters in `crates/ade-stream/src/` or a new opt-in `unified` module; opt-in CLI; existing native lowering APIs; `crates/*/tests/`; `scripts/phase18/{criteria.json,acceptance.py,oracle.py,verify_evidence.py,package_evidence.py}` and Python tests. Create `docs/adr/0025-unified-bounded-unbounded-planning-v1.md` with an accepted/refused decision table. Do not create a general future connector or Phase19 state engine speculatively.

## 4. Mandatory release gates (source-bound, falsifiable)

| Gate | Required witness / pass criterion |
|---|---|
| **G0 Provenance/prerequisite** | Exact base/final source tree, dirty inventory, feature graph, service/executable identity; check and reaccept Phase17 current-source G15 with fresh Kafka/MinIO, or keep a blocking `PARTIAL_VERIFICATION` and explicitly NOT_RUN. Historical archives unchanged. |
| **G1 Contract/property security** | Strict bounded parse/roundtrip and canonical digest; independent property oracle; forged schema, boundedness, order, replay, event-time, delivery, source identity/cut and sink properties refuse even after outer rehash. Unknown/duplicate JSON, excessive graph/plan/expressions/depth/cycles refuse. |
| **G2 Optimizer** | Independent reference candidate ledger checks hard constraints, deterministic ranking/trace, disable/forced target, both real aggregate choices, integer overflow, uncertainty fallback and finite time/memory/search limits. |
| **G3 Batch parity** | Existing Phase11 SQL and typed Rust supported shapes through common path vs legacy `PlanV1`, equivalent exact/bag/ordered results vs independent DataFusion reference; source tamper, overflow, output cap and deadline still fail. No DataFusion execution fallback. |
| **G4 Streaming parity** | Same **real Kafka** fixture, recorded partition trace, common vs old stateless/stateful/v3 paths: ACK/forwarded offsets, progress/frontier, filtered/late receipts, windows, payload fingerprints, snapshots and committed output match within each contract. No extra background polling/buffer. |
| **G5 Hybrid continuity** | Real broker with 2–4 partitions, verified starts and exclusive cutoffs; controlled interleavings, legal Kafka offset gaps, no historical records, idle Pending, source lag, downstream stall and resumed process. Independent producer-ledger checks every admitted record appears in exactly one historical/tail regime, with exact state/outputs and no synthetic watermark/EOF. Changed cutoff/incarnation or expired source refuses. |
| **G6 Recovery/transactions** | Real kill/cancel at handoff/checkpoint/savepoint/selector boundaries; v1/v2 and v3 restoration, terminal PAUSED, ACTIVATED3 refusal and retired-head attestations remain correct. Unsupported v3 hybrid refuses before root/broker effects; if supported, its committed-reader prefix is proven through crashes. |
| **G7 Adversarial refusal** | Malformed plan, source drift, unsafe roots, corrupt manifest/closure, missing selector, stale source head, forbidden physical target, incompatible event-time/window/delivery and optimizer trace tamper fail closed; no implicit downgrade. |
| **G8 Resource envelope** | Exhaust plan/memo/trace, batch result, Kafka client queue, hot/cold/checkpoint/output, historical handoff bookkeeping and cancellation; report actual peaks and OS/decoder/allocator/RSS exclusions; no unbounded buffering. |
| **G9 Measurement honesty** | Predeclare static vs optimized target fixtures and empirical criteria; store all raw matched results (including losses); distinguish prediction from observed timing/bytes and estimated from billed dollars; no post-hoc thresholds. |
| **G10 Compatibility** | `PlanV1`, CLI/API, benchmark/child/wire formats, StreamConfigV1, state v1/v2/v3 config/selector/savepoint semantics unchanged. No-default, relevant individual features, and all-features build/test matrix. |
| **G11 Toolchain/regression** | `cargo fmt --all -- --check`; `cargo check --workspace --all-targets --locked`; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo test --workspace --all-features --locked`; `bash scripts/smoke.sh`; relevant Phase11/14–17 Rust+Python suites. Record exact omissions. |
| **G12 Independent evidence** | Pinned raw Kafka ledger, source, executable, service, output/reader, plan/decision, failure scripts and resource inventory; portable offline verifier independently recomputes records/outputs and rejects **semantic** tampering even if checksum labels are regenerated. PASS labels alone are not proof. |
| **G13 Honest closure** | Reconciled ADR/plan, versioned operator guide, supported/refused JSON, source-bound implementation report, README/AGENTS updates and accurate status/remaining debt. |

G0 current-source G15, G4 live Kafka, G5 real hybrid and G6 transactional regressions are **release-critical**; mocks or historical archives cannot replace them. Use disposable owned Kafka and, for Phase17 reacceptance, a separate MinIO process. Predeclare case inventory, refusal reasons, quantitative decision thresholds and fixtures **before** final measurement. Preserve every failed/partial candidate and immutable previous evidence. Freeze one final source/executable/config/service identity before claiming the gates; verify archival contents offline rather than trusting authored status strings.

## 5. Stop conditions, handoff and next-phase boundary

Sequence: (1) reconcile Phase17 blocker and pin baseline; (2) same-session adversarial review of ADR/semantic plan/acceptance criteria before coding; (3) prove property/optimizer and batch parity first; (4) wire native streaming and measured Kafka hybrid; (5) crash/negative/resource campaigns; (6) final matched tests, evidence verification and truthful closure. Do not obscure Phase17's partial state by proceeding with Phase18.

**Stop as `PARTIAL`** for planner-only metadata without executed common routes; changed PlanV1/legacy defaults; optimizer reorders event-time/progress or selects a cheaper illegal sink; lost/duplicated hybrid record or invalid cutoff; missing v3 selector accepted as fresh, weakened writer fence or output prefix; unbounded buffering/search; unknown source identity; absent live service/independent oracle/matched evidence; or missing Phase17 prerequisite. Fix within scope when safe; do not broaden product claims to pass a gate.

Implementation report and handoff must include exact baseline/final SHA, worktree, source/runtime/service identities, supported/refused matrix, semantic plan and decisions, source/handoff cut and independent message ledger, each G0–G13 result (PASS/FAIL/NOT_RUN), crash/restart/output observations, resource peaks, reproducible commands, preserved negatives and debt. Remain within Phase18: **defer** Phase19 delta/arrangement/maintained views, Phase20 cycles, Phase21 connectors/cross-source CDC, new UDFs, multi-tenant schedulers, physical rescale/HA and external two-phase commits. A Parquet-history + Kafka live pipeline requires a separately proved cross-source continuity protocol, not an optimistic assumption.

## 6. Implementation reconciliation — 2026-10-30 Pacific

The user now explicitly authorizes implementation in this session without
subagents, one local commit. The supplied plan above is preserved. HEAD is
fbe0e2b; Phase17 G15 is still NOT_RUN and requires fresh acceptance. ADR-0025
records the same-session challenge and concrete ownership/API decisions.

Use `ade-logical` depending only on the existing portable ade-physical contract,
serde/BLAKE3/errors. Reuse finite schema inference and SQL binding while stripping
all physical properties and rebuilding PlanV1 after shared optimizer selection.
No stream rewrites or delivery/cadence optimization. Unfiltered finite aggregate
has two executable choices using the predeclared integer proxy model. Native
capability admission remains mandatory. Reserve/refuse KafkaRange explicitly:
a finite offset range needs a distinct source-completion contract; no cut forces
EOF in the mandatory historical+live path. Transactional hybrid is refused.
Hybrid state binds the complete cut/history digest into the unchanged job UUID
and verifies original retention on every source verification, including restart.
This is conservative and may refuse a saved cut whose origin expired.

## 7. Acceptance reconciliation — 2026-10-31

COMPLETE_BOUNDED_UNIFIED_PLANNER_V1 at candidate5's frozen runtime/executable
identities in the implementation report. All G0–G13 pass. The new real campaign
has38 cases,2,061 records,1,897 offset classifications,64 windows and12 descriptive
samples; the exact same147-input runtime reaccepts Phase17 G0–G15 on fresh
Kafka/MinIO plus actual ENOSPC. Every historical archive and failed/premeasurement
candidate is preserved. The fixed cost proxy selected a slower distributed case;
no benefit threshold was introduced or changed, and all raw samples remain.

Same-session hardening sealed compiled decisions, bounded programmatic metadata
before copies, separated output constraints from semantic hashing, bound pure-live
v1/v2 origins as well as cuts, and admitted fixed assigned subsets of the pinned
full topic inventory. Independent verification reconstructs native PlanV1 hashes,
checks every source receipt/ACK/cursor/session/window and selected state, and
rejects eight rehashed semantic attacks. KafkaRange and transactional hybrid remain
the explicit ADR refusals. No Phase19, physical rescale, automatic recovery/GC,
external transaction or production HA is inferred.
