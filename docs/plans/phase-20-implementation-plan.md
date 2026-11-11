# Adaptive Dataflow Engine — Phase 20 Implementation Plan

## Iterative and cyclic dataflow (bounded native local v1)

**Prepared:** 2026-11-11 (America/Los_Angeles)
**Repository:** <https://github.com/jasonkli21/adaptive-dataflow-engine>
**Inspected `main`:** [`c0e1c1faec96219fb79c864de1d6194bf5d8571e`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/c0e1c1faec96219fb79c864de1d6194bf5d8571e), “fix: address Phase 19 independent review”
**Status:** Authorized in this session on 2026-11-11. Acceptance remains subject to G0–G15.

## 0. Outcome, authority, and prerequisite disposition

The [PRD](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/main/docs/product/prd.md), Phase 20, calls for **cycles in the physical graph, loop progress, convergence, and graph/iterative primitives**. [Architecture §19](https://github.com/jasonkli21/adaptive-dataflow-engine/blob/main/docs/architecture/architecture.md) requires cycles and iterative progress to extend ADE’s native physical/runtime model. Phase 20 is not satisfied by running an acyclic query N times from a command-line loop, by printing a graph-shaped JSON object, or by borrowing a third-party iterative engine as the executor.

**Proposed bounded exit claim:** `COMPLETE_BOUNDED_CYCLIC_DATAFLOW_V1`, only after every required gate is source-bound PASS. It means:

1. A **real versioned physical cyclic region** with explicitly typed feedback edge(s), delay/round advancement, and deterministic admission/validation. The unchanged acyclic `PlanV1` continues rejecting cycles.
2. A **native bounded feedback executor** whose message/frontier accounting detects a true quiescent fixed point: no eligible work, no queued or in-flight feedback, and no outstanding production rights. It cannot terminate on a temporarily empty queue.
3. Two genuinely executed, finite graph computations sharing the cyclic mechanism: **directed single-source reachability with shortest-hop distances** and **weakly connected-component minimum-label propagation**. These exercise frontier deltas, feedback, deduplication, strict progress, and convergence. Label propagation is a foundation for iterative graph/ML algorithms; the claim does **not** include general iterative ML training or PageRank.
4. Independently verified deterministic output, bounded source/graph/queue/state/output resources, explicit time and round limits, cancellation, idempotent restart from a **committed round-boundary** checkpoint, and no provisional success on nonconvergence.
5. A real offline oracle, negative/failure campaign, native-vs-reference comparisons, raw evidence, and an honest single-host compatibility/support report.

This is a **default-off, single-job, single-host, finite immutable-graph** v1. Logical graph vertices and edges may be large enough to test resource behavior, but there is no general cyclic SQL, dynamic graph updates, Kafka feedback, distributed cyclic exchange, physical worker elasticity, general recursive queries/UDFs, external transactional sink, cross-host HA, automatic GC, or unbounded ML optimization. The v1 graph algorithms are a deliberately bounded demonstration of the **general physical feedback mechanism**, not hardcoded CLI-only iteration.

### 0.1 Read-first and precedence

1. `AGENTS.md`; `docs/product/prd.md` (§§1–9, 13–20, Phase 18–22); `docs/architecture/architecture.md` (§§3, 5–9, 11–19, 26–31; INV-1–10).
2. `docs/adr/0018-physical-ir-and-frontend-v1.md`, `0021-unbounded-source-and-progress-v1.md`, `0022-keyed-state-and-checkpoints-v1.md`, `0023-adaptive-checkpoint-placement-v1.md`, `0024-transactional-output-savepoints-v1.md`, `0025-unified-bounded-unbounded-planning-v1.md`, `0026-incremental-maintained-dataflows-v1.md`, and relevant storage, lineage, and recovery ADRs.
3. `docs/plans/phase-18-implementation-plan.md`, `docs/reports/phase-18-g14-lowering-2026-11-10.md`, Phase 18 reports/support matrix; `docs/plans/phase-19-implementation-plan.md`, Phase 19 implementation/remediation reports and support, the Phase 19 operator guide, and the original/remediation evidence manifests.
4. Actual `Cargo.toml`, `crates/ade-physical/src/lib.rs`, `crates/ade-logical/src/{lib,maintained}.rs`, `crates/ade-exec/src/{lib,physical,row_source,memory}.rs`, `crates/ade-incremental/src/{lib,engine,source,arrangement,storage,main}.rs`, `crates/ade-stream/src/{runtime,unified}.rs`, and the native tests/scripts. Inspect current implementations, not just documentation.

Precedence remains **PRD outcome > architecture invariants > this reconciled active plan > accepted ADR refinements > report/code reality**. A change of phase deliverable requires an explicit plan reconciliation, not a quiet ADR-only deviation.

### 0.2 Baseline and outstanding Phase 18 debt

At the inspected HEAD:

| Baseline item | Verified state and impact |
| --- | --- |
| Phase 19 | `COMPLETE_BOUNDED_INCREMENTAL_MAINTENANCE_V1`; commit `6cc1d19d` plus review fixes at `c0e1c1f`. The separate `ade-incremental/maintained-v1` path uses immutable local Parquet, signed finite epochs, a shared arrangement, and pinned atomic generations. F19-01–F19-05 were remedied, including a real **local temporary-filesystem ENOSPC** test. Preserve original and remediation archives unmodified. |
| Phase 18 | **Still** `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`. G14 native stream lowering is coded and focused tests pass; real Kafka semantic parity and matched current-source reacceptance, G15 native worker-backed join, and G16 8/16 KiB real-Kafka quota evidence remain open. |
| Phase 18 waiver | The explicit waiver applies to **Phase 19**, not automatically to Phase 20. Planning Phase 20 does not close Phase 18. A later implementation session must record a separate operator/user decision if it proceeds with an isolated finite local Phase 20 path while Phase 18 remains partial. It must not claim the Phase 18 integration is verified. |
| Phase 17 and other debt | Versioned transactional state/sink proof and past phases’ archived results are source-bound and narrower than production HA. Phase 10 physical elasticity, Phase 12 HA and Phase 17/18 current-source service reacceptance remain separate. |
| Current physical IR | `ade-physical::PlanV1` is a bounded topologically ordered **acyclic** graph (`MAX_NODES=64`); `PlanV1::infer` rejects inputs not preceding their consumer. This rejection is a compatibility invariant, not a bug to remove globally. |
| Current maintained code | `ade-incremental` uses a crate-level `maintained-v1` feature gate. Its private storage and arrangement code are specific to Phase 19. A Phase 20 implementation must not presume these modules are reusable public APIs or share a selector/root format. |

**G0 decision before implementation:** Recheck the current Phase 18 support and latest commits. Either (a) fully reaccept Phase 18 with its own matched real-service evidence first, or (b) obtain and record an **explicit Phase 20-specific decision** to implement and accept only this isolated finite-local path despite open Phase 18 debt. If neither is true, stop **before Phase 20 implementation writes** and hand off the exact blocker; do not silently inherit Phase 19's waiver. This plan may be refined without changing that prerequisite.

## 1. Executable contract and refusal matrix

| Axis | Phase 20 bounded v1 must demonstrate | Explicitly refused / deferred |
| --- | --- | --- |
| Input | Immutable, byte-hashed local Parquet **vertex set** (`vertex: UInt64`, non-null) and directed **edge set** (`src: UInt64, dst: UInt64`, both non-null), with exact row/file/ordering inventory, full preflight and postflight identity. Empty edge set, isolated vertices and self-loops legal. | Mutable inputs, missing files, unknown endpoints, null/wrong schema, duplicate vertex/edge rows (choose deterministic reject), forged inventory, Kafka CDC, streaming graph updates or late edges. |
| Algorithm | `reachability_bfs_v1(seed)` outputs one `(vertex, distance: u32)` for every reachable vertex. `weak_components_min_label_v1` outputs `(vertex, component_min_vertex)` for every vertex, treating each directed edge as an undirected adjacency **for that algorithm only**. | Weighted/negative shortest paths, arbitrary recursion, dynamic delete/retraction, general graph ML, nondeterministic floating point, user-authored closure functions. |
| Cyclic IR | New opt-in `CyclicPlanV1` (provisional name) with a closed set of typed nodes/feedback arcs, exactly one declared iteration boundary and round-advancing delay; validation of graph, schema, allowed operators, provenance, caps and output semantics. | A malicious arbitrary graph, instantaneous zero-delay feedback, combinational cycles, untyped or dangling arcs, cross-job feedback, changes to `PlanV1` serialized format/admission. |
| Feedback/progress | Frontier/delta messages tagged with `round: u32`, deterministic key order and stable attempt-independent identity. Capability/credit accounting for queued, executing and emitted feedback. Publish success only on proven quiescence with no unacknowledged work. | “Queue currently empty” termination, in-flight message loss, infinite buffering, round wraparound, wall-clock-as-logical-order, unbounded loop or unproved convergence. |
| Convergence | Finite set, monotone progress: BFS discovers each vertex once at shortest hop; components only lower labels and eventually reach the component minimum. Stop at fixed point, with exact terminal state and reason. | Treating `max_rounds`, timeout, cancellation or resource exhaustion as convergence. No partial output gets a success footer or terminal publication. |
| Resources | Explicit checked caps for vertices, edges, adjacency/index, frontier, pending/in-flight tokens, dedup state, active output, snapshots/metadata/disk and deadline. Actual backpressure or deterministic quota refusal. | Unbounded channels, vector growth without checked admission, OS RSS guarantee, speculative spill/remote state, memory limits attributed to external decoder internals. |
| Durability | Opt-in local **round-boundary** checkpoints that atomically bind plan/source hash, root identity, round, complete state, next frontier, zero in-flight work and output digest. One active writer; reader sees only complete terminal snapshot. Idempotent selected-head retry/resume. | Snapshots mid-round with untracked messages, auto-adopting unknown roots, overwriting prior generations, reusing Phase 17 `CURRENT3` or Phase 19 `g-*`/`w-*` format without separate protocol, disk loss/production HA/external exactly-once. |
| Compatibility | Default features unchanged; existing physical/logical/stream/maintained APIs, legacy formats, benchmark result/child schema, previous evidence and static behavior preserved. | Silent migration, replacing the Phase 19 root, injecting feedback into v1 plans, broadening Kafka/state/sink claims. |

### 1.1 Exact semantic decisions

- **Graph model:** a finite *simple* directed graph with distinct `u64` vertices and distinct directed `(src,dst)` edges. Treat `a→b` and `b→a` as distinct edges. Reject duplicates during deterministic full admission; no implicit last-write-wins. Node order and edge-file order do not change results; exact inventory order and content hashes still bind lineage. Unknown endpoint is an error. Zero vertices is valid only with zero edges and no BFS seed; decide whether `seed` may be absent in that case and encode it explicitly. Do not silently synthesize vertices from edges.
- **BFS:** seed is a member of the vertex inventory. Round 0 contains the seed at distance 0. At round `r`, only newly discovered vertices at distance `r` emit neighbors for `r+1`; deduplicate, exclude visited, and select the shortest distance. A converged graph may be disconnected; unreachable vertices remain absent. Distance/round conversion is checked. Self-loops and duplicate *messages* cannot produce duplicate output.
- **Weak components:** initialize each vertex's label to its own ID, expand labels along both orientations of each admitted edge, and strictly lower labels. At convergence each vertex is labeled with the smallest ID in its weak component. A finite source graph permits a fixed point; a request cap may stop first, which is an **incomplete** outcome, not a committed success.
- **Logical round:** initial work at round 0; every feedback transition advances exactly one round. A round barrier/quiescence condition requires all emitted work to be credited/consumed/acknowledged before deciding the next frontier is empty. Sort/dedup at a bounded barrier. A monotonically increasing round is part of semantics; physical scheduling/order never is.
- **Deterministic digest:** define domain-separated hashes of graph semantic description, immutable source bytes/inventory, algorithm and seed, canonical sorted terminal rows and complete round checkpoint. `plan_id` excludes paths, attempt IDs, policy knobs, timers, and output location; `execution_id` may include physical choices. The *output digest* is independent of scheduling, chunking, checkpoint cadence, and restart.
- **Resource limit vs mathematical completion:** if a finite graph exceeds max rounds, capacity, deadline, or permitted output bytes, report `INCOMPLETE_LIMIT`/structured refusal with no terminal commit and no success footer. The optional last **committed round-boundary** checkpoint remains an explicitly labeled resumable state, never a fabricated fixed point.

## 2. Native architecture and integration

### 2.1 Physical cyclic region: separate versioned plan, shared native ownership

Retain `PlanV1` as the immutable acyclic physical contract. Add a **separate strictly admitted physical family**—for example `ade-physical::cycle::CyclicPlanV1`—whose serializer and validation never reach legacy `PlanV1::push` by accident. It may reference a legal acyclic body plus one explicit typed `Feedback`/`Delay` port. Construction must prove:

1. Typed input/output and feedback schema compatibility, canonical node/edge IDs, finite graph/plan byte caps, and no unknown variants/duplicate fields.
2. Removing marked delay arcs yields an acyclic graph; every graph-theoretic cycle crosses a positive delay. Refuse additional zero-delay or undeclared cycles. For v1, **one bounded strongly connected iterative region** suffices; multiple nested/nondeterministically interacting loops refuse.
3. Every feedback path is round-advancing, deterministic and replayable, with no source/sink side effect inside the cycle. All outputs carry separate semantic and execution identities.
4. `explain` derives topology, scheduling/progress invariants and operator schemas from validated data; callers do not submit an already-approved “physical decision” or arbitrary capability count as authority.

**Actual feedback topology (logical shape):** `Seed/Restore → Frontier → Expand/Reduce → Dedup/State update → FeedbackDelay(+1) ↻ Frontier`, plus a terminal committed-output branch. A diagnostic topology alone is not sufficient: the native executor must consume the validated back-edge and traverse it across multiple rounds on nontrivial graphs.

### 2.2 Suggested module ownership (confirm in ADR after code audit)

| Layer | Responsibilities | Constraint |
| --- | --- | --- |
| `ade-logical::iterative` | Strict graph/algorithm descriptor, semantic identities, finite admission/refusal properties, algorithm params, optional static execution controls. | No frontend, DataFusion, worker or Kafka dependency. |
| `ade-physical::cycle` | Typed versioned cyclic IR, only validated feedback/delay, schema/round/topology validation, immutable canonical explanation. | `PlanV1` layout, validation and old users unchanged. |
| `ade-exec::iterative` (opt-in) | Native feedback queue, round progress/capability ledger, bounded adjacency/frontier and checked native BFS/label reduction, cancellation/backpressure/metrics. | Use ADE's physical/native boundaries and existing cancellation/memory patterns; do not implement as CLI host-loop calls to old `execute`. |
| New `ade-iterate` crate (opt-in binary `ade-iterate`) | Verified Parquet input adapter, requests, explain/run/resume/read/verify, local checkpoint publication and operator-facing limits. | Thin orchestration over the shared native feedback executor, not an independent unversioned dataflow runtime. |
| `scripts/phase20/` | Frozen criteria, fixture encoder, standalone Python oracle (BFS and union-find), native acceptance driver, independent offline archive verifier/tamper injector. | Oracle cannot import Rust engine logic or trust engine-generated PASS labels. |

An alternative narrow module placement is permitted **only** if it keeps the above ownership and one actual native cyclic mechanism. Record the rationale and touched dependencies in the Phase 20 ADR. Do not force Phase 20 into `ade-incremental`'s crate-level `#![cfg(feature="maintained-v1")]`, expose its private shard format, or introduce `ade-exec → ade-frontend` / runtime → DataFusion dependencies. If extracting a reusable checked-file or publication helper, preserve old readers and re-run their tests; do not rewrite Phase 19 just for aesthetic deduplication.

### 2.3 Progress and scheduling invariants

Define a small **accounting protocol**, not a timing heuristic. At every transition, checked integer counters enforce:

`produced(round) = queued(round) + executing(round) + acknowledged(round) + explicitly_canceled(round)`

where every message/capability transfer has a unique attempt-independent identity (or an equivalently provable owned-token protocol); credits are returned exactly once. A synchronous barrier implementation is acceptable for v1, but it must explicitly account for all pending/in-flight work, not infer completion from a temporarily empty channel. Rounds may overlap only if the frontier logic proves causal ordering; simplest v1 is one completed round at a time. Detect overflow, duplicate ack, late feedback, missing credits, cancellation, and operator failure. Never acknowledge a round as complete while an executor or bounded sink retains work.

The executor should own a bounded graph adjacency/index, a bounded current frontier and next frontier, a visited/labels array or map, a cap on queued/in-flight feedback, and a bounded output writer. Explicitly account for peak allocations and disk/snapshot footprints. If the accepted source cannot fit under the chosen v1 budget, refuse *before* opening a publishable root. End-to-end memory is a managed ledger plus stated decoder/allocator/OS exclusions, not a process RSS promise.

### 2.4 Checkpoints, replay, and terminal visibility

Implement **checkpoint-free full deterministic replay** as a runnable static baseline and **committed round-boundary resume** as a second runnable mechanism. Configurable checkpoint cadence must not change the mathematical result; a checkpoint is legal only after the round is quiescent with zero outstanding credits. A recovered executor reconstructs its next frontier/state exactly and continues at the next logical round. On any intermediate-epoch divergence it refuses, never merges two histories.

Use a **distinct Phase 20 root, selector and serialization family** (names and version chosen in ADR); retain a pinned digest/expected round chain outside the mutable state directory. Maintain an exclusive local writer lease, private directory/no-symlink path rules, exact file digests and source identity checks, synced staged files, a durable publication witness and atomic no-clobber pointer/generation transition. Readers of terminal output verify the complete selected state and never expose a half-written frontier/result. A checkpoint **is not** a successful terminal output. A fresh process can resume a trusted selected round or rebuild from complete immutable input in a **new empty root**; it cannot adopt an unknown/forked root or roll back a caller-pinned chain. Define the failure boundary honestly: one operator-controlled local filesystem with atomic rename/hardlink/fsync/lock assumptions, process kills and explicit ENOSPC, not power-loss, disk-loss or Byzantine storage guarantees.

### 2.5 API / operations

Provide a strict, versioned request and independently documented supported/refused matrix. Suggested interface:

```text
ade-iterate explain REQUEST.json
ade-iterate run REQUEST.json
ade-iterate resume REQUEST.json
ade-iterate read REQUEST.json
ade-iterate verify REQUEST.json
```

`explain` verifies source/plan/limits and reports derived topology and legal routes **without** creating/publishing state. `run` executes and optionally checkpoints. `resume` validates the exact pinned plan/source/round/chain and continues. `read` emits canonical sorted terminal rows and a **success-only** complete footer; `verify` checks committed terminal identity without returning all rows. Both must enforce the **exact serialized stdout budget including the footer/newline**, or refuse with nonzero exit; streamed partial prefixes are never success. Library callback APIs document caller-owned serialization budgeting if applicable. Source changes, supplying an incomplete named source, or missing checkpoints do not silently downgrade to another mode.

**Reference limits to freeze at WP0** (proposals, not fabricated existing constants): at most 4,096 vertices, 65,536 directed edges, 4,096 rounds, 64 MiB verified input, 64 MiB managed graph/frontier state, 256 MiB retained local disk, 16 MiB serialized output, 120 s operation deadline; bound per-round and in-flight feedback **independently**. Include tests exactly at and one beyond every bound. Use stronger/smaller defaults if justified, but any change requires a documented, pre-acceptance plan/criteria reconciliation. The hard cap may be lower than the rounds needed for a legal large graph; the result must then be explicitly incomplete, not silently incorrect.

## 3. Work packages and ordered implementation

| Work package | Concrete change | Required early falsification |
| --- | --- | --- |
| **WP0 — gate, provenance, design challenge** | Re-read latest HEAD; record Phase 18 status and explicit Phase 20-specific dependency disposition; inventory old formats and tests; freeze criteria/fixtures/oracle/resource bounds. Write a design spike proving a genuine delayed cycle can be represented and executed without changing `PlanV1`. | No authorization/waiver => stop. If cyclic schema or progress correctness cannot be shown, revise ADR/plan before building algorithms. |
| **WP1 — ADR and cyclic admission** | New ADR-0027 *if still available*, versioned semantic/physical plan, strict parser, graph/algorithm source identities, typed backedge and delayed-cycle validator. | Malformed topology, undeclared/zero-delay cycle, dangling/duplicate ports, mismatched schemas, forgery, invalid seed and overflow refuse before effects. |
| **WP2 — verified source and oracle** | Native bounded Parquet vertex/edge reader with file inventory, type/null, row-count/checksum/postflight; independent Python graph oracle and fixtures. | Duplicate rows/endpoints, missing/changed bytes, source swaps, empty/isolated/self-loop/reciprocal graphs and source interruptions. |
| **WP3 — feedback core / progress** | Actual cyclic native operator pipeline and owned round-credit ledger; bounded frontier queues, dedup, cancel, deadline and structured metrics. | Delayed messages, duplicate ack, zero-delay loop, pending/in-flight work at idle, missing credit, round overflow and cap pressure cannot produce false quiescence. |
| **WP4 — two algorithms / deterministic terminal** | BFS and weak components as operators on that same feedback mechanism; canonical sorted outputs and domain-separated identity. | Chains, diamonds, converging paths, cycles, disconnected graph, self-loops, directed vs weak distinction, skew, duplicate messages, maximum hop/label propagation, independent BFS/union-find parity. |
| **WP5 — durable local round checkpoints** | Unique isolated root/protocol, checkpoint after quiescence, verified reopen/resume, atomic terminal commit, strict writer/read locks, fresh-root replay. | Kill before/after stage/witness/selector, incomplete message ledger, two writers, checkpoint tamper, rollback/fork, root alias/symlink, ENOSPC and short write preserve last known committed head. |
| **WP6 — limits and operating surface** | Feature-gated CLI/API, explain/run/resume/read/verify, exact stdout/footer budget, source/disk/queue/metadata bound ledger and truthful timeout semantics. | Lowest/one-below limits, unresponsive output reader, truncated stdout, no complete footer, enormous graph, round cap, cancellation and read-only root all fail without terminal publication. |
| **WP7 — scientific acceptance and compatibility** | Frozen finite datasets, static full-replay baseline, native cyclic samples, optional checkpointed/restarted samples, raw counters/timings, independent evidence verifier and mutation corpus. | Every gate G0–G15 below; independently reconstruct results and convergence; prove *real backedge traversals* and finite work, not merely final-result parity. |
| **WP8 — documentation and final source match** | ADR; reconciled implementation plan; `docs/phase-20-iterative-guide.md`; `docs/reports/phase-20-support.json`; `docs/reports/phase-20-implementation-report.md`; README/AGENTS status and evidence archive. | Source/executable/toolchain/criteria hashes match final changed inputs; no old evidence was overwritten; claim accurately bounded. |

At WP0/WP3 perform an **independent design review of the plan itself**: Is this unnecessarily a separate engine? Does the physical feedback mechanism genuinely generalize beyond the two algorithms? Can an in-flight message make an empty queue look terminal? Can a checkpoint be mistaken for convergence? Does old `PlanV1` reject the same malicious cycle? Reconcile the plan and ADR explicitly before release if these questions expose a flaw.

## 4. Mandatory release gates — freeze falsifiable criteria early

All gates are **PASS / FAIL / NOT_RUN / BLOCKED**, with source-bound evidence. PASS cannot be inferred from a green unit-test summary or an older source archive. Freeze exact expected outcomes, timing thresholds and fixtures **before** the final campaigns. Do not redefine criteria in response to an unfavorable measurement.

| Gate | Required independent witness / failure criterion |
| --- | --- |
| **G0 Dependency and provenance** | Current HEAD/tree, clean/dirty state, feature flags, Rust/Cargo and OS/FS, source inventory, executable/fixture/oracle digests, frozen criteria, explicit Phase 18/Phase 20 dependency decision. Old Phase 19 and earlier archives untouched. |
| **G1 Cyclic IR and strict admission** | Versioned canonical plan, derived node schemas/properties, strict duplicate/unknown/oversized/deep JSON refusal; reject unmarked cycles, zero-delay loop, mismatched feedback types, dangling edges, multiple SCC/nested feedback not allowed. `PlanV1` must continue to reject cycles. |
| **G2 Native actual feedback** | Real native execution of graph with ≥3 rounds; raw instrumentation proves feedback edge exercised, round-advancing batches, owned pending/queued/executing/acknowledged counts, correct physical route; no host CLI loop or DataFusion reference acting as executor. |
| **G3 Progress/quiescence and safety** | Convergence iff all active/queued/in-flight rights are retired and next frontier is empty; adversarial delayed feedback, duplicate/missing ack, producer failure, restart and cancellation cannot create premature success or deadlock. Finite round overflow refuses. |
| **G4 Algorithm semantics** | Independent Python BFS and union-find oracle matches canonical graph/result hashes for directed reachability and weak components, including empty/isolated/self-loop/multi-path/long chain/diamond/cycle/skew; exact shortest hops/labels and checked arithmetic. |
| **G5 Input and semantic lineage** | Native Parquet decode and pre-/postflight file/inventory attestation; duplicates/endpoints/null/type/count/digest/TOCTOU/refusal inventory; plan/source/seed/algorithm changes invalidate resume. No substituted local file is accepted. |
| **G6 Convergence/limits distinction** | Prove both normal fixed points and a reachable nonconverged budget stop; `max_rounds`, deadline, cancellation and quota never emit terminal success or false complete footer. No hidden partial-result success. |
| **G7 Checkpoint/resume** | Checkpoint only after quiescent round, zero in-flight tokens, state/frontier/plan/source/checkpoint-parent digest bound. A **different process** resumes and yields exactly the uninterrupted output/round trace suffix; exact retry idempotent. |
| **G8 Atomic visibility/fencing** | Concurrent reader/writer and dual-writer cases; six or more reached process kill boundaries around stage/write/sync/witness/selection/terminal read; exact old-or-new committed visibility, no provisional results. Real ENOSPC where supported or a clearly marked NOT_RUN; injected short write separately. |
| **G9 Boundedness/backpressure** | Exact caps and high-water ledger for source decode, adjacency, visited/labels, both frontiers, in-flight credit, dedup and queue, output/read, snapshots, retained bytes, filenames and metadata, time; at/under/over limits and slow/stalled sink; exclusions (allocator/OS cache/RSS) explicit. |
| **G10 Determinism and execution knobs** | Input permutation, different chunk/batch sizes, checkpoint cadence, deterministic scheduling perturbation and process restart yield byte-identical canonical terminal rows/result hash. A forced invalid execution mode/unsupported plan refuses; no user control overrides semantics. |
| **G11 Performance/work science** | At least five **valid paired** post-warmup samples for predeclared sparse/chain, dense/cyclic and checkpoint-resume regimes. Real work counters (edges traversed, frontier sizes, duplicates suppressed, rounds, bytes/checkpoints) independently checked; compare a runnable simple scan-all-round/reference baseline and native feedback. Declare numeric timing guard(s) **before** measuring, with exact machine/cap/seed/threshold. If unmet, FAIL/partial, not post-hoc relaxation. No dollar/cloud claims. |
| **G12 Legacy compatibility** | Default-off feature isolation, unchanged `PlanV1` wire/validation, `StreamConfigV1`, Phase 17 selectors, Phase 19 `maintained-v1` and both named views, benchmark/child/result-v1, static/local/distributed/smoke routes. No new dependency from native runtimes into frontend/DataFusion. |
| **G13 Toolchain and regressions** | `cargo fmt --all -- --check`; `cargo check --workspace --all-targets --locked`; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo test --workspace --all-features --locked`; `cargo check -p ade-incremental --no-default-features --locked`; `bash scripts/smoke.sh`; focused Phase 19 and relevant Phase 11/14–18 tests/Python checks. Record environment exceptions as NOT_RUN, not PASS. |
| **G14 Independent evidence / tamper** | Offline verifier obtains raw fixture bytes, full output ledger, per-round fronts/credits, checkpoint chain, hashes, source/build/environment/criteria identity and computes BFS/union-find and terminal digest independently; semantic tamper with recomputed *outer archive* hash fails. Preserve failed candidates and immutable historical evidence. |
| **G15 Operational docs and end-to-end** | Separate-process `run → checkpoint/kill → resume → read/verify` for both algorithms; source-bound results/limits; updated ADR, guide, report, support JSON, README/AGENTS and exact PASS/FAIL/NOT_RUN matrix. No claim of distributed/HA or previously unaccepted Phase 18 capabilities. |

**Empirical threshold decision:** predeclare a modest but meaningful work-science claim before implementation: e.g. frontier-driven actual `edges_examined` on a chain/branch graph is significantly less than a scan-all-edges-every-round baseline, without changing results. Choose a concrete integer ratio and a defensible host-local time threshold in `scripts/phase20/criteria.json` at WP0; keep the work-counter and timing results **separate**. A timing failure does not invalidate correctness, but prevents the corresponding performance acceptance claim. No retrospective substitutions.

### 4.1 Required negative/fault inventory

At least: duplicate vertex/edge; missing endpoint; forged source hash; swapped inode/content during read; null/wrong Parquet schema; unknown JSON/algorithm; seed missing; altered source or algorithm on resume; instant unmarked cycle; zero-delay edge; schema-incompatible feedback; extra disconnected SCC/nested cycle; counter overflow; round wrap; late in-flight feedback after queue empty; duplicate ack; missing ack; stale/provisional checkpoint; selected pointer rollback; selector/witness/object tamper; root clone/alias/symlink/path traversal; concurrent writers; missing/changed pinned chain; kill before/after each commit boundary; short write and actual ENOSPC; read while writer publishes; stalled output; exactly-at/one-above budgets; nonconverged max rounds; timeout; cancellation; empty/isolated/self-loop/cyclic/disconnected graph; forced unsupported action; and rehashed semantic attacks against the offline verifier.

All these tests must assert **the failure mode and absence of terminal publication**, not merely a nonzero status. For crash boundaries, capture the exact reached fault marker and post-restart committed head. Distinguish adversarial test failure from a known unsupported environment.

## 5. Evidence protocol and exact source freeze

1. Record immutable baseline SHA, tree, clean/dirty worktree, source inventory including all Rust/Cargo and acceptance/oracle/criteria inputs, build flags, `rustc/cargo`, architecture, CPU/RAM, OS/filesystem, and executable/fixture encoder checksums. Do not recycle Phase 19 binaries or results as current Phase 20 proof.
2. Predeclare `scripts/phase20/criteria.json`: case matrix, deterministic seeds, exact input/plan graph hashes, graph sizes/degree distributions, byte/row caps, negative/fault names, crash points, round limits, time/perf thresholds, warmups and **≥5** paired measured samples, source identity and variance policy. Pin criteria before the release measurement; retain later amendments with provenance instead of editing history.
3. Generate Parquet graph fixtures and a **distinct implementation** of sorted BFS and union-find reference in Python. Do not reuse the engine’s iteration or graph-building logic. Inspect actual source bytes and decoded row counts offline.
4. Run correctness/fault campaigns and independently verify raw output, checkpoint/frontier/credit ledger, and source/checkpoint DAG. Retain *all* unsuccessful/preliminary candidates, logs and exact failure stage. Never erase a failing candidate because a later run passes.
5. Run paired native cyclic vs runnable repeated-round scan/reference samples on the **same pinned graph and environment**; report median, all per-run values, variance, timeouts, CPU/disk/bytes, and semantics. Do not infer cross-machine speed or billing.
6. Package `docs/evidence/phase20/` with signed/hashed manifest, archived raw cases/outputs/samples, code+source/executable identities and independent verifier. Demonstrate 2+ semantic tamper mutations with rehashed archive manifest are rejected. A portable offline verification pass must not trust `status: PASS` JSON fields.
7. Run the final toolchain and smoke checks on **the final source**; if code/docs/criteria change afterward, re-run affected evidence with matching identities. Record each G0–G15 as PASS/FAIL/NOT_RUN/BLOCKED, plus limitations, in `docs/reports/phase-20-implementation-report.md` and a machine-readable support matrix.

**No silent phase widening:** Preserve Phase 17/18/19 formats, historical frozen acceptance and unresolved tickets. Phase 20 need not own distributed worker replacement, dynamic graph CDC, Phase 21 connector breadth, general SQL recursion, graph UDFs, general MLP, global materialization, multi-tenancy or cross-cloud durability. The correctness of a **real single-host cyclic physical execution mechanism** is the release target.

## 6. Stop conditions, deliverables, and follow-on boundary

Stop and report **partial**, not `COMPLETE_BOUNDED_CYCLIC_DATAFLOW_V1`, if any of the following is true: no Phase 20-specific disposition of open Phase 18 dependency; physical cycle is only descriptive or implemented as repeated acyclic calls; termination ignores credits/in-flight work; two primitives don't traverse the shared feedback mechanism; a round-cap result is labeled converged; graph/source identity is unverified; restart redoes committed partial output or silently adopts a fork; a reader sees provisional data; no enforced resource bound; no independent oracle; wrong or mismatched source/executable/criteria evidence; required gates are FAIL/NOT_RUN/BLOCKED; legacy formats/defaults changed; or acceptance thresholds were weakened after measurement.

**Deliverables:** coherent implementation; new Phase 20 ADR; reconciled active plan; strict typed API/CLI and operator guide; feature/compatibility tests; fixture/oracle/criteria/fault/offline evidence scripts; raw immutable evidence; machine-readable support matrix; final source-bound implementation report with refusal table, resource peaks, matched timing samples, every G0–G15 status, preserved unsuccessful attempts, and explicit carry-forward debt. A separate independent reviewer should critique this plan as well as the implementation before subsequent phase authorization.


## 7. Active reconciliation — 2026-11-11

The user requested implementation under the attached handoff, including its
Phase 20-specific waiver for the isolated finite-local route. G0 uses that
waiver. Phase 18 retains its existing partial qualification. The user
requires one local commit and no subagents. Review is same-session, with an
independent Python semantic oracle and no claim of an independent reviewer.

ADR-0027 defines the exact closed typed region, synchronous delay protocol,
append-only no-clobber selector and independently retained exact pin. Empty
graphs are admitted for components. BFS always requires a present seed.
The checked resource ceilings and performance guards are frozen in
`scripts/phase20/criteria.json`. Reordering source files can change plan
identity while preserving canonical terminal bytes and result digest.

The verified disposition is `COMPLETE_BOUNDED_CYCLIC_DATAFLOW_V1`. The
[implementation report](../reports/phase-20-implementation-report.md)
records the exact final source inventory and G0–G15 witnesses. The
source-matched [evidence archive](../evidence/phase20/README.md) preserves
the original criteria, raw samples, earlier campaigns, and the offline
semantic verifier's results.
