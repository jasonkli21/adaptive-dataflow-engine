# Adaptive Dataflow Engine — Phase 19 Implementation Plan

## Incremental computation and maintained dataflows (bounded local v1)

**Prepared:** 2026-11-10 (America/Los_Angeles)

**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine

**Inspected `main`:** `510671b2b72272d25495a719ab45c168b38c54f7` (clean shallow checkout; “Address Phase 18 independent review”)

**Document status:** Authorized 2026-11-11. The user explicitly waived current-source Phase 18 acceptance for Phase 19. Baseline `2af859a26ba1ed559f383dcb129858c4d0ad31f5`; Phase 18 retains its partial status.

## 0. Decision, prerequisites, and exit claim

The PRD's Phase 19 outcome (§16) is **changelog/delta collections, differential operators, reusable arrangements, maintained views, and incremental recomputation**, extending the architecture's (§19) legal choice between **restore, full recomputation, and incremental update**. The product's cost/recovery thesis is more important than feature breadth. Deliver a small **real, native, default-off implementation** with persisted maintained state and an independently provable result—not a JSON-only simulation, SQL shortcut, or separate general-purpose row engine.

### 0.1 Phase 18 qualification and user waiver

At the inspected HEAD, `README.md`, `AGENTS.md`, `docs/phase-18-unified-planner-guide.md`, `docs/reports/phase-18-support.json`, and `docs/reports/phase-18-review-remediation-2026-10-31.md` say `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`:

| Prerequisite | Current status | Required before Phase 19 implementation |
| --- | --- | --- |
| Phase 18 G14 | **BLOCKED** | Native lowering is implemented at the current baseline. Matched real Kafka parity and semantic-mutation evidence remain open. |
| Phase 18 G15 | **BLOCKED** | Matched optimizer-selected native worker join and refusal evidence remain open. |
| Phase 18 G16 | **BLOCKED** | Real Kafka V2 quota evidence at 8 and 16 KiB remains open. |
| Phase 18 G0–G13 | Historical/partial for revised runtime | Fresh **matched final-source** acceptance, Phase 17 prerequisites, toolchain/service and independent evidence where affected. |

**Stop rule:** No Phase 19 coding, new source/format acceptance, or claim of completion until the latest Phase 18 report/support matrix shows all G0–G16 passed with matched source/executable/service identities, or a *separate explicit user decision* changes the dependency. Do not quietly fold Phase 18 remediation into Phase 19. Prepare an exact Phase 18 blocker handoff if still open; preserve immutable candidate5/review archives and earlier failed evidence.

The user exercised the separate-decision exception on 2026-11-11. Phase 19 G0 now requires baseline, source and executable provenance, the recorded waiver, and preservation of historical evidence. Phase 18 current-source acceptance remains open. Phase 19 uses its own feature-gated native path and makes no claim about Phase 18 acceptance.

### 0.2 Bounded Phase 19 claim

Proposed release label: `COMPLETE_BOUNDED_INCREMENTAL_MAINTENANCE_V1`, only if all gates below pass. It means:

1. A **versioned weighted multiset of finite changes**, with positive and negative deltas, deterministic epoch/order identity, checked arithmetic and exact duplicate/retraction semantics.
2. **Actual ADE-native incremental** filter/project and keyed sum/count updates, retaining a **versioned, bounded, reusable keyed arrangement**, with at least two independently named maintained views that share the same arrangement rather than duplicate it.
3. A **durable, source-bound, atomic local generation** containing logical epoch, upstream input digest/cursor, arrangement, both view states, and committed output/manifest identity. A fresh process restores and continues without silently applying an epoch twice.
4. One **legality-first, deterministic, disable-able policy** choosing executable `restore`, `full_recompute`, or `incremental_update` for the *same requested target epoch and semantic result*, with decision provenance, candidate refusals and actual costs reported separately.
5. An independent full-replay oracle and crash/fault/size/performance evidence showing meaningful incremental work avoidance for sparse changes, correctness on dense changes, and a clean static baseline.

This is a **single-host, finite-epoch** maintained dataflow with sequential named immutable local batches. It is an incremental capability on ADE's existing logical/native foundation, **not** general CDC, Kafka retractions, streaming SQL, external exactly-once, a distributed MV service or a new v3 transaction mode.

### 0.3 Read-first / architectural precedence

1. `AGENTS.md`; `docs/product/prd.md` §§1–9, 13–20 and Phase 19/20/21; `docs/architecture/architecture.md` §§3, 5–9, 11–19, 26–31, notably INV-1–10.
2. Current `docs/plans/phase-18-implementation-plan.md` including G14–G16 amendment; `docs/adr/0025-unified-bounded-unbounded-planning-v1.md`, `docs/phase-18-unified-planner-guide.md`, Phase 18 implementation and remediation reports/support JSON. Record the present qualification and the waiver before implementation.
3. `docs/adr/0018-physical-ir-and-frontend-v1.md`, `0021-unbounded-source-and-progress-v1.md`, `0022-keyed-state-and-checkpoints-v1.md`, `0023-adaptive-checkpoint-placement-v1.md`, `0024-transactional-output-savepoints-v1.md`, ADR-0019 and relevant storage/lineage/expected-cost ADRs.
4. `crates/ade-logical/src/lib.rs`, `crates/ade-physical/src/lib.rs`, `crates/ade-frontend/src/{catalog,dataframe,sql,unified,execution}.rs`; `crates/ade-exec/src/{physical,aggregate,stateless,lib}.rs`; `crates/ade-stream/src/{unified,runtime,state/*,transactional/*}.rs`; `crates/ade-ops/src/{publication,store,runner}.rs` and actual tests/features.
5. `scripts/phase17/`, `scripts/phase18/`, their offline verifiers/evidence fixtures; `docs/reports/phase-10*`/readiness debt, current guides and CI/smoke scripts.

Authority: PRD scope/outcome > architecture invariants > this active plan > accepted ADR refinements > code/reports. Record and resolve real conflicts explicitly. Do not relabel historical evidence or infer undocumented guarantees.

## 1. Contract and refusal matrix

| Axis | Phase 19 bounded v1 must execute | Explicit refusal/deferred |
| --- | --- | --- |
| Sources | Immutable, digest-verified **finite baseline** through existing native Parquet/physical adapters; ordered, immutable, locally supplied finite `DeltaBatchV1` epochs, with full replay provenance | Arbitrary mutable Parquet scans, tailing changed files, database CDC, multi-topic Kafka joins, untrusted mutable histories, unknown replay or missing epoch |
| Delta algebra | Signed weights for `(key, value_i64)` row multiplicity; insert/delete and update as `-old/+new`; strict epoch order, duplicate detection, checked magnitude/count/sum | Negative resulting multiplicity, impossible retractions, arithmetic wrap, implicit last-write-wins semantics, unordered auto-merge, timestamp-as-identity |
| Operators | ADE-owned filter/project (allowlisted simple integer predicates/projections), keyed `sum_i64` and `count_u64`, no hidden full-scan for the incremental route | General SQL, arbitrary UDF, non-invertible aggregate, general join, cyclic/fixpoint logic, unsupported null/float algebra |
| Arrangement | One canonical indexed multiset keyed by `(key,value)` or a proved equivalent physical index; bounded hot/disk use, exact canonical snapshot/restore, shared by at least two named view readers | “Arrangement” that is merely the output aggregate; two copied indexes advertised as shared; unbounded key growth or unsafe eviction |
| Views | Two named views over the same arrangement: e.g. `all_sum_count_by_key` and `value_ge_threshold_sum_count_by_key`; stable per-key output IDs and update versions; read committed generation only | Cross-job global cache, arbitrary view DAG, spontaneous source-dependent view mutation, externally visible provisional updates |
| Planner | One common semantic identity and legal target epoch; deterministic `restore`/`full_recompute`/`incremental_update` selection with disable/forced controls and finite trace/budget | Ranking incompatible outputs, using stale arrangements, treating an unverified checkpoint as a valid restore, fabricated resource or dollar estimates |
| Publication | Atomic local pointer/generation with exact-digest verified arrangement, state, output, source chain and epoch; one writer, read-only committed view; crash-safe no-clobber semantics | Treating Phase 17 `CURRENT3` as Phase 19 format; external transactional sink, production/remote HA, automatic old-root adoption or destructive GC |
| Scope | Opt-in single local job, one owner, serial epochs, explicit bounded admission, no default-path behavior change | Physical rescale, cross-host durability, live differential Kafka, automatic recovery, Phase20 loops or Phase21 connector SDK |

**Important modeling details:**

- **Bag semantics:** `DeltaRowV1 { key:u64, value_i64:i64, weight:i64 }` is a *proposed logical algebra*, not a public API chosen in advance. A signed weight expresses multiplicity change, not a replacement value. Normalize equal rows by checked addition at an epoch boundary; remove zero multiplicities. Enforce nonnegative committed multiplicities and nonnegative `u64` counts. Use widened intermediate arithmetic (`i128`/`u128` or equivalently checked higher precision) with explicit refusal if the agreed `i64` sum/`u64` count result cannot be represented. Increments and decrements must be symmetric under replay. An update is two changes in one atomic epoch, not separate externally committed steps.
- **Epochs and identity:** an epoch has a monotonically contiguous logical generation, canonical payload hash, exact parent generation/hash, original source baseline digest, row/sequence ordering and semantic view-plan digest; duplicate identical epoch submission is an idempotent no-op **only after** verifying the existing committed fingerprint, and duplicate epoch ID with different bytes refuses. A gap, fork or wrong base refuses before effects. No reliance on wall-clock arrival order or Kafka offsets where the source is local.
- **Lineage/semantic hash:** logical plan binds baseline source schema, allowed predicates, keyed aggregation semantics, thresholds, signed algebra/version, source sequence and view IDs. Physical choice, paths, credentials, worker knobs, attempt/session IDs and resource ceilings are not semantic identity. Changing semantics cannot read a prior root as compatible; a valid physical replan may not alter the mathematical bag result.
- **Durability:** local atomic rename/fsync on a single honest filesystem and exclusive writer/reader-fencing protocol; assume process crashes, not arbitrary disk loss or lying storage. If atomic update fails or source verification is ambiguous, retain previous committed generation and poison the attempt. Readers never observe provisional arrangement/view/output; do not delete a still-referenced generation. A new distinct format and selector namespace is safer than modifying `CURRENT`, `CURRENT2`, `CURRENT3` or `PlanV1`/`StreamConfigV1`. Document exact names in a new ADR after inspecting code, including root collision/tamper rules.
- **Baseline:** establish using the existing ADE-native immutable Parquet scan/physical adapter and strict manifest verification. Do not use DataFusion as the implementation executor; it is permitted as an *independent* reference oracle. A separate bounded local delta-file admission is acceptable as a narrow **testable source contract**, not as a general connector. Reuse native cancellation, budget, batch/Arrow and publication primitives where they actually fit; if an adapter must be added, prove it is inside the native execution ownership/dependency boundary.
- **Views and arrangement reuse:** choose two independently specified named view semantics, one all-rows and one deterministic filtered subset, that can be recomputed from the canonical multiset. Both consume **one physical arrangement and delta traversal** with explicit reader/ownership metrics. Demonstrate arrangement reuse after restart, and independently calculate each result from the full source history. Mere reuse of a plan description or one output count is insufficient.

## 2. Semantic integration and route design

### 2.1 Preserve current ADE boundaries

`ade-logical::LogicalPlanV1` currently holds `ParquetManifest` and `Kafka` computations and a pure, capped optimizer. It owns admission; `ade-physical::PlanV1` is an immutable **finite tree**, not a live delta engine. The stream v1/v2/v3 modes own receipt/frontier/window/checkpoint/transactional semantics and cannot be silently rewritten. Extend with an explicitly versioned **incremental descriptor/plan family** (e.g. `MaintainedPlanV1`, not an unchecked extra variant accepted by every old consumer), with independent property derivation and strict unknown-field refusal. Reuse neutral semantic digests and optimizer legality discipline; do not pass a user-authored “decision” as executable authority. Native incremental operators and durable generations must be consumed by actual execution.

Suggested ownership, subject to ADR and code audit:

- `ade-logical`: delta-graph semantics, schema/ordering/replay/delivery properties, binding, candidate legality and bounded pure decisions. No DataFusion/Kafka/runtime dependency.
- New opt-in `ade-incremental` crate **or** a narrow clearly owned native module: delta runtime, arrangement, view maintenance, exact local checkpoint/pointer lifecycle, finite epoch source adapter. It may use `ade-physical`/`ade-exec`; no dependency back-edge from native runtime into `ade-frontend`.
- Thin opt-in CLI (`ade-maintain`) / library entrypoint: `explain`, initialize from a source, `apply` one or more finite epochs, `read` a committed view, `resume/verify`, static/forced controls. Existing commands and default features unchanged.
- `scripts/phase19/`: frozen criteria, fixture producer, independent replay/oracle, fault injector, evidence packager/offline verifier and tamper tests. `docs/adr/0026-incremental-maintained-dataflows-v1.md` (if number available), guide, support JSON and implementation report.

Do not let a new module reach into the private internals of Phase 17 epochs or widen `CURRENT3`. If a safe reusable local publication primitive is useful, extract only a generic helper with untouched old behavior and explicit regression proof.

### 2.2 Three genuinely executable recovery/compute choices

For a fixed `plan_hash`, `baseline_hash`, target epoch `E`, input-chain `C(E)` and requested result `R(E)`, each candidate must produce the **identical** committed view bags and output identity:

1. **Restore:** if a completely verified already committed target generation `E` exists, open its arranged state and view snapshot, verify source chain/plan/schema/version and serve the result without applying deltas. Restore of a prior epoch followed by new changes belongs to the incremental route, not a misleading “restore-only” success.
2. **Incremental update:** if a completely verified compatible committed parent `E−1` exists, read bounded target delta(s), apply legal signed changes only to affected keys, atomically publish new arrangement and both views for `E`; neither reprocess unrelated baseline rows nor redo unchanged keys.
3. **Full recompute:** when an immutable verified baseline and complete verified delta history through `E` are available, reconstruct the canonical multiset and both views via ADE-native full scan/replay, atomically publish the **same E**. This is a runnable correctness/disabled-policy baseline, not merely estimated.

When no route is safe, **refuse**. If a root is corrupt but complete verified inputs exist, do not silently overwrite/adopt/erase it; use explicit **new empty generation namespace** or operator-directed rebootstrap without bypassing locks or source attestation. A command to force an unsafe route refuses. A cost ranking cannot override input existence, side-effect, replay or quota constraints.

### 2.3 Optimizer, costs and measurement

Hard admissibility first: semantic plan/version/source chain, parent/target generation, replayability, ledger estimates, available CPU/time/disk, private root and writer uniqueness. Then bounded deterministic candidate list (≤3), fixed-point/checked integer estimates with provenance and uncertainty: estimated bytes/rows scanned, delta count/distinct touched keys, checkpoint bytes, expected write amplification and projected deadline. Prevent untrusted delta cardinality claims from substituting for verified file scans. Disable = declared `full_recompute` baseline (or explicitly refused if inputs unavailable); forced choice is a hard constraint. Record **predicted work units and observed bytes/rows/time** separately; no billed-dollar claims.

Include at least three predeclared workloads: small sparse changes over large baseline, dense changes that favor full replay, and a valid target checkpoint that favors restore; show the engine selects each route and independently verifies the same `R(E)`. Do not bake expected choices into an oracle. Static options remain runnable for paired comparisons; optimizer policy is pure and testable without I/O.

## 3. Work packages (strict order)

| Work package | Deliverables | Adversarial proof before continuation |
| --- | --- | --- |
| **WP0 — prerequisite, baseline and scope** | Record Phase 18 G0–G16 status, the user waiver and current HEAD; inventory old formats/features, source/test/runtime; freeze Phase 19 criteria, independent oracle and failure matrix. | Verify provenance independently and preserve historical archives. |
| **WP1 — ADR / strict delta semantics** | Versioned logical descriptor, identity, checked signed bag/epoch algebra, two named view definitions, explicit source/cut and refusal matrix. | Forged fields, duplicate/unknown JSON, epoch fork/gap, illegal negative multiplicity, overflow and mismatched source refuse without effects. |
| **WP2 — native finite input and full oracle baseline** | Immutable baseline Parquet binding through native ADE, digest-checked finite epoch batches, executable full replay and two view outputs. | Independent reference bag and order/type oracle; baseline/epoch tamper, footer drift, null/unexpected schema, mutation during read, cancellation. |
| **WP3 — bounded reusable arrangement** | One indexed multiset with hot/disk quotas, deterministic enumeration/lookup, shared ownership for two views, exact serialization. | Duplicate/retraction multiplicity, skew, worst-case cardinality, evictions, restarts, no duplicate hidden stores or scan-all on sparse updates. |
| **WP4 — differential maintenance** | Checked filter/project, affected-key sum/count, two named view updates with stable output IDs; delta-neutral/empty epochs. | `−x/+x` cancellation, update-before-commit atomicity, threshold crossings, alias/version stability and oracle differential comparison. |
| **WP5 — durable local generations** | Single writer fence; staged contents, exact-digest manifest, atomic no-clobber publish and committed reader; explicit restore/new namespace. | Kill/ENOSPC/short write at every durable boundary, stale lock, missing pointer, corrupt object, root clone/fork, two writers, zero provisional reads. |
| **WP6 — three-route optimizer** | Pure legal chooser, verified stats, deterministic trace, forced/static controls, real native route dispatch. | Force each route on the same target; stale root cannot be chosen; independently recompute ranking; checked budget/deadline bounds. |
| **WP7 — operational hardening** | Metrics, limits, cancellation, error taxonomy, feature isolation and operator guide. | Low quota, 0/near-limit batch sizes, oversized IDs/plan/trace, disk exhaustion, read-only roots, leaks, symlink/path traversal and cleanup. |
| **WP8 — acceptance and documentation** | `scripts/phase19` campaigns and portable evidence, matched toolchain/smoke, ADR, plan reconciliation, support JSON, implementation report, README/AGENTS. | All gates G0–G15 below, including independent rehashed tamper checks; preserve raw failures and source identity. |

Use an early source-level design challenge before WP2: Does this truly reuse ADE's existing execution and logical contracts, can arrangement and view commits be atomic together, is full replay independently runnable, and is the required workload feasible under the real caps? Record changes in ADR and reconcile the plan, rather than weakening exit gates after coding.

## 4. Formal verification and release gates

The following are **minimum** falsifiable gates; freeze precise fixture sizes, budgets, case inventory, thresholds and benchmark commands before final measured runs. Tests are necessary but not a substitute for observed native execution.

| Gate | Required witness / exact failure criterion |
| --- | --- |
| **G0 Dependency/provenance** | Record the explicit Phase 18 waiver, source/executable/manifest hash/feature/platform inventory and unchanged historical archives. |
| **G1 Strict semantic identity** | Versioned/duplicate/unknown/oversize/depth checks; derived properties and plan hash recomputed independently; changed predicate, view, source, epoch or algebra invalidates lineage. No caller-controlled trace or digest accepted as proof. |
| **G2 Weighted bag algebra** | Independent full-replay multiset oracle over inserts, deletes, updates, reorders, duplicates, zero net, negative weights, min/max i64, overflow and invalid retracts; exact bag equality. |
| **G3 Finite input and provenance** | Native verified Parquet baseline, entire immutable ordered finite epoch chain; same-source parity; forged source/epoch digests, reused ID with altered payload, missing parent/gap, changed files and incomplete histories fail closed. |
| **G4 Native execution and operator parity** | Real native full-replay and incremental routes both execute, not a separate mocked plan. Projection/filter, keyed sum/count and both named views exactly match independent reference on multiple datasets, incl. skew and threshold boundary. |
| **G5 True arrangement reuse** | One physical indexed multiset used by ≥2 named view consumers, referenced across separate runs; bounded allocation/bytes; counters prove no second full index and untouched-key work avoided on sparse updates. |
| **G6 Atomic publication and recovery** | Crash/kill/power-like process interruption at stage/fsync/pointer/output/reader boundaries; old generation intact or new one complete, never partially committed. Concurrent writer fencing; staged partial reads impossible; root/selector/manifest tamper and ENOSPC fail closed. |
| **G7 Idempotent and stable replay** | Same exact epoch retry is a verified no-op; same ID altered bytes, reordered divergent sequence and fork refuse; restore+new epoch vs uninterrupted results identical. No accidental at-least-once double application. |
| **G8 Optimizer legality** | All 3 candidates execute when eligible; three predeclared regimes select expected legal choices, static disable/force and refusal behavior; independent legal/ranking oracle, deterministic trace, checked arithmetic/uncertainty. No cost plan changes semantics. |
| **G9 Hard resource/cancellation** | Bounded source, decoded batches, arrangement hot/cold, index metadata, scan queue, checkpoint generations, output/read, optimizer plan/trace and deadline; peak observed. Low/near-minimum budget tests, no infinite buffering. Explicit decoder/OS/RSS exclusions. |
| **G10 Performance science** | Correctness first. Frozen sparse/dense/restore workload definitions, full/raw matched samples (minimum five valid samples/regime/policy after warmup), environment/input/attempt identities and independent verifier. At least one **meaningful sparse win** (predeclared target: incremental median time ≤0.80× full) and no >1.25× adaptive regression against fastest legal static route on the other regimes; if unmet, report FAIL, do not retune thresholds post hoc. |
| **G11 Compatibility** | Default-off feature; unchanged PlanV1, legacy SQL/typed routes, StreamConfigV1, v1/v2/v3 CURRENT roots, Kafka frontiers/windows/receipts, Phase17 committed-reader fences, benchmark/child/result v1, Phase10 static. No implicit migration. |
| **G12 Toolchain/regressions** | `cargo fmt --all -- --check`; `cargo check --workspace --all-targets --locked`; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo test --workspace --all-features --locked`; `bash scripts/smoke.sh`; focused Phase11/14–18 regressions and relevant Python scripts, with exact PASS/FAIL/NOT_RUN. |
| **G13 Independent evidence** | Fixture bytes/semantic digests, raw delta and output ledger, source/executable fingerprint, test/runtime/service identities, crash seeds/steps, quotas, raw timings, negative logs, verifier code and tamper attacks. Offline verifier independently reconstructs final bags/views and rejects rehashed semantic manipulation—not merely PASS labels. |
| **G14 Honest supported/refused docs** | New ADR, actual API/operator guide, machine-readable support matrix, source-bound report and README/AGENTS status. Describe local durability, source assumptions, historical evidence and explicitly deferred work. |
| **G15 End-to-end multi-epoch acceptance** | Real separate-process `bootstrap -> apply -> read -> kill/resume -> apply -> read`, multiple epochs with positive/negative/replacement events; both views and arrangement survive. Prove each route actually executed and final reader sees one committed target epoch, not a simulated view. |

Performance thresholds above are proposals to freeze *before implementation and measurement*. If resource size/environment cannot yield meaningful timing, pre-register stronger exact work/byte-count acceptance **in addition to** (not as a retroactive replacement for) the time measurement, and leave the empirical gate FAIL/NOT_RUN until a suitable owned runtime is available. Do not claim cloud cost savings or production generality from local trials.

### 4.1 Minimum negative inventory

Include at least: epoch duplicate same/different body; wrong parent/fork; reordered epoch; missing source; source file changed mid-read; counter `u64` overflow; sum `i64` overflow; `i64::MIN` negation; negative final multiset multiplicity; view predicate threshold change; legacy-format root overlap; symlinked root; malformed/overlong JSON; truncated stage/manifest; counterfeit digest with recomputed outer checksum; stale writer lease; two concurrent writers; pointer rollback; orphan stage; crash before/after pointer/fsync; disk-full/short write; read during provisional epoch; zero/sparse/dense/extremely skewed deltas; cancellation and deadline; wrong plan/cursor/epoch restore; forced illegal candidate; and unavailable full replay. Capture invariant and expected refusal before writing the tests.

## 5. Benchmark and evidence protocol

1. Record clean baseline, exact source/runtime/build/feature hashes, relevant services and environment (`uname`, CPU/RAM/disk, filesystems), deterministic fixture seed, sizes, max deltas, distinct keys and measured memory/bytes. **No historical result becomes current-source evidence.**
2. Freeze `scripts/phase19/criteria.json` with required cases and gates, explicit expected refusal types, perf thresholds, count of paired samples and resource caps. Generate baseline/epoch batches and independent oracle outputs in different code paths from the incremental engine.
3. Run isolated child processes for each candidate; verify correctness before marking a timing valid. Paired order randomized or counterbalanced, equal caches/warmups, all failures and samples retained. Include both operator-work counters and wall-clock time; not one best iteration.
4. Include real fault injection with owned temporary roots. For **ENOSPC**, use a controlled bounded filesystem or independently evidenced short-write injection; distinguish injected error from actual OS full-volume behavior. No destructive filesystem operations on external roots.
5. Package portable immutable evidence with a verifier that checks member inventory/hashes and recomputes semantic bags, epoch chain, committed pointer and expected policy; mutate payloads **and recompute outer hashes** to prove semantic validation. Verify the resulting archive offline in a clean environment, with negative tamper cases.
6. Report results by `G0`–`G15` as `PASS`, `FAIL`, `NOT_RUN`, or `BLOCKED`, including why; archive failed attempts and all measured raw data. Avoid broad claims from a local single-host experiment.

## 6. Stop conditions, non-goals, and finish

Stop rather than claim Phase 19 complete if a delta route silently replays all unchanged base rows; arrangements exist only in metadata; two views do not share one actual indexed state; negative weights can silently underflow; root corruption is auto-adopted or committed data overwritten; checkpoint/output are not atomically selected; full-replay and incremental outputs differ; reader exposes provisional output; unsupported Kafka/v3 paths are accidentally admitted; selected route is not actually executed; resource caps are absent or evidence is not matched and independently verified.

Out of scope: generalized differential joins, multi-source incremental joins, arbitrary SQL materialized views, live Kafka CDC/retractions, changed Phase14–18 frontiers or transactional output, remote/distributed arrangements, physical rescale, cross-AZ/production HA, tenant/cross-job caches, arbitrary auto-GC, automatic operator migrations, external exactly-once, Phase20 cyclic dataflow and Phase21 connector breadth. These remain future research, not Phase 19 v1 completion claims.

**Deliver on completion:** one coherent source change set; ADR and reconciled plan; CLI/library/API guide; tests/fixtures; frozen criteria and independent verifier; original evidence plus unchanged old archives; `docs/reports/phase-19-implementation-report.md` with baseline/final SHA, semantic/source/runtime identities, accepted/refused matrix, resource peaks, complete G0–G15 table, raw positive/negative outputs, actual policy choices, all measurement samples, exact commands, limitations and next-step debt; support JSON and accurate README/AGENTS. No push/PR or automatic phase advancement unless separately authorized.

## 7. Bounded implementation reconciliation

ADR-0026 records the native scan and immutable-arrangement design. The opt-in
`ade-incremental/maintained-v1` runtime uses the existing `ade-exec` scan
lowering and synchronous Arrow sink. The Phase 19 descriptor in `ade-logical`
derives a native source from the ordered BLAKE3 file inventory. It owns the
finite epoch semantics and the pure three-route policy.

The two named views are `all_v1` and `filtered_v1`. Both are kept in each
key's record in a single weighted arrangement. The filtered predicate is
an inclusive signed threshold. Sparse updates decode only touched shards.
The planner can bind an exact target-only delta to a verified parent. Full
replay requires complete input and can independently verify an already
selected target without mutating it.

The committed selector is the highest contiguous generation directory with
its immutable witness and matching manifest chain. The private filesystem
root, kernel lease, atomic no-replace directory selection and independently
pinned source chain define the supported durability envelope. A witnessed
stage can be resumed at the exact expected epoch and digest. Legacy roots
retain their contracts.

The frozen criteria, runner, offline verifier and local evidence use the
user-authorized Phase 18 waiver. Performance thresholds retain their
original values. The Phase 18 service acceptance debt stays recorded in
its existing reports.
