# Adaptive Dataflow Engine — Phase 9 Implementation Plan
## Adaptive Join and Aggregation Algorithms (bounded-batch v1)

**Prepared:** 2026-10-21
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine
**Inspected `main`:** [`c87d2baa5a2489994ddaa32103d03bbf7c696ede`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/c87d2baa5a2489994ddaa32103d03bbf7c696ede)
**Status:** Complete bounded v1 on 2026-10-24, after formal Phase8 closure and explicit user authorization. All declared Phase9 guards and Phase6–8 regressions pass; see the Phase9 implementation report. The gate below is preserved as the historical prerequisite.

## 0. Mandatory Phase 8 go/no-go gate

At the inspected c87d2ba planning baseline, `AGENTS.md`, `README.md`, ADR-0015 and the Phase 8 report all said Phase 8 is **INCOMPLETE** and explicitly prohibit beginning Phase 9. At the inspected HEAD, the ADETCP08 review-remediation release suite had all 39 children pass correctness, but the primary two-worker skew median total latency was **1.1524198889× static** (a 15.24% regression), despite a **22.72%** maximum-exclusive-final-work reduction. Uniform latency was **1.2172439569×** static (inside its 1.30× guard). The fixed no-skew-latency-increase gate failed. Earlier initial, supported-domain and packed runs also failed their respective primary gates. Four-worker gains cannot substitute for the primary two-worker criterion. The external raw bundle auditability item is also open.

**Before any Phase 9 source change:**

1. Re-read the current Phase 8 report/ADR/acceptance script at the *actual* new HEAD, not merely this snapshot. Require independently reproducible, source-bound acceptance satisfying the originally declared Phase 8 gates, or an explicit, well-evidenced product-level decision changing the phase boundary through the established documentation/ADR process. A performance failure must not be silently waived, re-labelled, or fixed inside Phase 9.
2. Resolve/document the actual Phase 8 transition interpretation: its current prefix probe commits an immutable map before original partial aggregation; it is **not** a mid-exchange switch. If the product outcome remains unmet, do not mark Phase 8 complete by nomenclature.
3. Preserve failed data and establish reviewable raw benchmark evidence, source/build identity and relevant regressions. Update `AGENTS.md` and phase status only after closure.
4. If the gate is still red: **stop after feasibility notes**. Do not implement any Phase 9 work packages, create speculative code, or count Phase 8 fixes as Phase 9 progress. This plan and the companion prompts may exist as future-facing documents now.

This prerequisite deliberately takes precedence over all subsequent instructions.

**2026-10-22 gate resolution:** Phase8 closing source ab7404b/runtime432cc7dd
now passes the unchanged numeric gates in both predeclared confirmation suites
and all-nine pooled primary samples, including every measured sample from the
failed listener suite. All117 children,118 workspace tests, smoke and Phase6/7
regressions pass. Original operator prefix work precedes commitment. The source,
raw results, supplemental declaration/deviation and preserved failures are in
[the Phase8 report](../reports/phase-8-implementation-report.md) and its immutable
public evidence release. Phase8 remediation was completed before Phase9 source
work and is not counted toward Phase9 deliverables. The user's single-commit
instruction keeps main HEAD at c87d2ba during this session; exact clean measured
snapshots and matching runtime trees establish source identity in the meantime.

## 1. Source of truth, purpose and acceptance shape

Read in repository-prescribed order: `AGENTS.md`; `docs/product/prd.md` §§7–9, 11, 15–17 and Phase 9; `docs/architecture/architecture.md` §§6–9, 11–17, 22, 25–31; this plan; ADRs 0010–0015 (especially 0013–0015); Phase 6–8 reports; then relevant source/tests. Precedence is PRD outcomes > architecture invariants > active plan > accepted ADRs > report and actual code, with explicit reconciliation when they disagree.

**PRD Phase 9 outcome:** the runtime changes *physical operator strategy* using observations of data, not a configuration name or a post-hoc metric. PRD examples include broadcast versus repartition join, hash versus sort-merge, in-memory versus grace hash, build-side switching and adaptive partial aggregation. Those are illustrative alternatives, not a requirement to implement every algorithm at once. The deliberately narrow **required** v1 slice is:

- A real, exact, two-input **inner equijoin** on bounded synthetic Arrow/Parquet data, executing through ADE's owned local and 2–4-process distributed runtime.
- A pure, deterministic **runtime-observation-based choice** between build-side broadcast and two-sided hash repartition, with a legal build-side choice and safe static controls.
- A memory-accounted **in-memory hash join** that actually transitions to a spillable **grace-hash** mechanism under observed build-memory pressure, before output is committed. A spill-disabled refusal is valid; an unbounded hash map is not.
- An independent, explicit **adaptive partial-aggregation** path for the existing distributed `GroupBySum`, able to choose/transition between reducing hash aggregation and bounded raw-partial passthrough based on observed reduction/memory pressure. The final merge remains exact.
- Measured, independent-oracle-validated tradeoffs, decision provenance and hard limitations. Existing static aggregate behavior stays the default and remains fully functional.

**Not promised in v1:** arbitrary SQL or DataFrame planning, outer/semi/anti joins, NULL-aware semantics, non-`UInt64` keys, more general expression evaluation, full standalone distributed join recovery, in-flight repartition epoch changes, multi-task-per-host scheduling, worker elasticity, streaming/watermarks/state, adaptive mid-output build-side reversal, or a complete physical IR. External sort-merge join, bloom filters, complex join orders and recursive skew salting are deferred unless the v1 correctness domain cannot otherwise be defended. Phase 10 owns the scheduler and Phase 11 owns a stable frontend/IR.

## 2. Actual repository baseline (verified at inspected HEAD)

| Surface | Present contract | Phase 9 implication |
| --- | --- | --- |
| `crates/ade-bench/src/ade_distributed.rs` | Accepts only `Query::GroupBySum`, `synthetic-events-v1`, builds `StaticAggregateJob` | A join needs a separate explicit query/dataset path and oracle, not an invented existing option. |
| `crates/ade-exec/src/lib.rs` | `LocalPlan` has scan/project/filter, fixed/spillable `HashAggregate`, `MergeAggregate`, `Sort`, IPC/external source; **no join** | Add typed two-input operator(s) without claiming the local tree is a stable general IR. |
| `crates/ade-exec/src/aggregate.rs` | Blocking partial aggregate; fixed table max 65,536 slots/32,768 live keys; 16 low-hash spill buckets; oversized bucket can raise `SpillNoProgress` | Do not mistake passing low-cardinality fixtures for bounded arbitrary skew support; declare the domain and provide adaptive fallback, with negative tests. |
| `crates/ade-distributed/src/coordinator.rs` / `worker.rs` | Fixed 2–4 workers, one aggregate producer/final role per worker, file round-robin, gather, bounded exchange and one narrowly recoverable lost final | A two-source join must have explicit side/stage identity, bilateral terminal completeness and no deadlocking serial-send/serial-receive ordering. |
| `topology.rs` / `probe.rs` / `partition.rs` | Static, static-packed and sampled 48-shard pre-exchange ownership for **one** aggregate input; `hash_v1`; immutable layout digest | Preserve `hash_v1` and legacy routing. Join strategy selection is separate from Phase 8's single-source layout. Do not silently reuse its probe to infer a second relation. |
| `exchange.rs` / `retention.rs` / `materialization.rs` | Transfer identities, bounded receiver/retention, terminal fingerprints and complete-copy receipts; private exchange wire v2 | Reuse storage/transfer mechanism, with new join-side stage names and bounded controls; no unverified claim of multi-host durability. |
| `lineage.rs` / `recovery.rs` / `gather.rs` | Verified file-morsel snapshot, final-only single-loss recovery, exact attempt quarantine, gathered final set | Original aggregate recovery stays unchanged. V1 join must **fail closed** on any process loss unless separately proven/reviewed; no accidental reliance on aggregate-only recovery. |
| `protocol.rs` / `metrics.rs` | Private control magic **ADETCP08**; result schema v1; optional JSONL metrics sidecar | Version private messages when changed. Do not mutate benchmark result schema v1 or mislabel old release evidence. |
| `docs/reports/phase-8-implementation-report.md` | Phase 8 candidate, correctness pass/performance failure; raw bundles ignored in Git | Gate Phase 9 and preserve failure provenance. |

The Phase 8 two-host `SpillNoProgress` on the original 262,144-key engineered fixture was a real **aggregate spill-domain limit**; the later 131,072-key supported-domain experiment does not remove that limit. Neither a larger ledger budget nor a join implementation alone fixes this. Also, task ledgers, exchange buffers, selected Arrow batches, encoder transients and side queues are separately bounded, not an overall process RSS guarantee.

## 3. Exact v1 semantic and physical contract

### 3.1 Join query and oracle

Introduce a distinct `InnerEquiJoin` benchmark query with two independently versioned source sides, `left` and `right`, each containing non-null `key: UInt64` and `value_i64: Int64` (other synthetic columns may exist but are projected away). SQL-equivalent semantics: `SELECT l.key, l.value_i64 AS left_value, r.value_i64 AS right_value FROM left l INNER JOIN right r ON l.key = r.key`. Preserve **bag semantics**: for key `k`, `m` left rows and `n` right rows produce exactly `m×n` result rows, even when triples are identical. Disjoint/empty inputs produce zero rows. Order is unspecified. Compare the **multiset including multiplicities**, not a set or hash-only fingerprint; validation may sort canonical tuples with checked row limits. Reject null/schema mismatch and unsupported types explicitly. Do not silently implement a semijoin or preaggregate away multiplicity.

Use an independently executed DataFusion SQL oracle on separately registered left/right tables. Build a deterministic dataset/manifest identity for each side and a combined query identity; source snapshots/fingerprints must never conflate sides. Keep output fixtures within a predeclared validation cardinality cap; during execution stream bounded output batches and enforce an optional explicit output/work cap with a typed refusal rather than unbounded collection. The benchmark harness may collect bounded validation outputs, but runtime execution may not.

### 3.2 Operator and distribution contracts

Create an internal `JoinJob`/typed join configuration rather than overloading `StaticAggregateJob` with ambiguous file lists. Physical choices are separate dimensions:

| Dimension | V1 options | Selection time / constraint |
| --- | --- | --- |
| Distribution | `hash_repartition` or `broadcast_build` | Choose and commit once after **bounded real-source observation**, before first join-side exchange frame. Every worker ACKs the exact strategy/identity. |
| Build relation | `left` or `right` | Choose by observed complete-small-side bytes/rows when defensible; otherwise deterministic conservative default. Never switch sides after output. |
| Local join algorithm | `in_memory_hash`, then `grace_hash` fallback | Real pressure-driven transition **before first result**; no dropped/duplicated buffered build rows. |
| Aggregate partial path | `fixed_hash` (legacy), `adaptive_v1`, forced `raw_partial` diagnostic | For `GroupBySum` only; controlled safe switch with same `(key,sum,count)` partial schema. |

For repartition, route **both** relations' equal keys to the same destination using the same deterministic `hash_v1` mapping; do not implement a mismatched modulo/layout pair. For broadcast, send every build row exactly once to **each** destination (a bounded amplification of `N`) and hash-partition only probe rows. Every probe row is owned by exactly one destination, so join pairs are emitted once. A physically empty producer→destination→side channel still emits a valid terminal. Retain fixed 2–4 worker processes; no fake dynamic task count. Stage/source tags and physical transfer IDs must be disjoint for left, right and gather, and fenced by job/execution/strategy identity, not just a serialized enum. Use the existing exchange transport where safe; start/drain both join-side readers concurrently or otherwise prove no two-sided bounded-channel deadlock.

The join's first version may explicitly support **spill-retained exchange only**; if so, reject other join delivery modes at configuration validation (never silently coerce), explain why, and exercise the existing aggregate modes unchanged. A later extension to direct/buffered/adaptive/complete-copy join must preserve independent side terminal and recovery semantics. Static aggregate `sampled_v1` remains available; Phase 9 join may initially reject combination with the one-sided `sampled_v1` probe, but must expose the incompatibility honestly. Join `hash_repartition` itself must be correct on 2/3/4 hosts.

### 3.3 Observation, policy, commit

- Add typed, bounded join probe config: file/row/decoded-byte/read-byte caps, probe deadline, max broadcast build bytes/rows, amplification guard, memory limit and deterministic tie-breaks. Budget actual source API reads, projected uncompressed decode and selected Arrow buffers separately. A file-size proxy is **not** an in-memory footprint. Never label estimates as complete measurements.
- Observe actual assigned source data with explicit coverage/provenance. Broadcast is permitted only when the build side's entire bounded input is proven to fit the declared per-destination reservation **or** an equivalent sound conservative upper bound has been verified. Prefix-only smallness must **not** authorize broadcasting an unseen huge suffix. Inconclusive/biased probes select repartition or fail closed; they cannot alter join correctness.
- Pure policy should yield a versioned `JoinDecision { distribution, build_side, reason, observations, estimates, digest }`, with checked arithmetic and a *deterministic* static/forced mode for controls. Include a fixed cap on decision metadata and serialized control size. Recorded reasons: complete small-side, neither-side-proven, projected fanout too high, observation cap, forced control, etc.
- Commit the decision as a job-fenced immutable epoch (and ACK from all endpoints) **before join exchange starts**. This is genuine runtime-informed planning, not an in-flight distribution change. Local hash→grace conversion is the required **in-execution** strategy transition. Avoid a full-producer/shuffle barrier for the *decision*; a bounded probe rendezvous is allowed. Measure its time/bytes separately.
- Preserve mechanism/policy separation. The mechanism must produce the same multiset for every valid forced decision, including when the policy chooses poorly. If policy telemetry is missing, use a documented safe fallback; never change semantics.

### 3.4 Local join algorithms, memory and skew

Implement an ADE-owned hash join with checked typed key/value extraction, reservation-backed hash state and bounded output batches; preserve duplicates explicitly (each key may own multiple build rows). Build-side selection cannot erase left/right output orientation. The transition point must be concrete: when a reservation or configured build threshold is reached **before output emission**, evacuate every buffered build row to deterministic spill partitions; stream/partition remaining build and probe inputs, then process corresponding partitions. Do not keep the original in-memory table resident on top of the spilled copy. Require spill headers/schema/version, bounded readers/writers and open-file limits, complete writes before reads, cleanup on success/cancel/errors, and deterministic partition processing.

Spill partitioning must not blindly copy the baseline aggregate's low-4-bit bucket limitation. Use deeper independent hash bits or bounded recursive repartition with strict max depth and a typed skew refusal when a single unsplittable key group exceeds the supported memory/output domain. This refusal is not a performance success. A later sort-merge/skew-group spool is a candidate extension if evidence shows grace hash cannot cover important workloads. Avoid `collect()` on either input or any unreserved `HashMap`/`Vec` growth. Cancellation and deadlines apply during scan, partition write/read, build, pair output and cleanup.

### 3.5 Adaptive producer aggregation

For the existing distributed `GroupBySum`, retain the current `fixed_hash` path unchanged by default. Implement an explicit producer-local adaptive path operating over bounded batches and preserving the **existing partial format** `(key: UInt64, sum: Int64, count: UInt64)`:

1. Accumulate a bounded real-input observation (rows, exact/estimated distinct keys, reduction ratio, bytes, table pressure). Track coverage and error/bias; no full-source pre-scan.
2. If reduction is high and state fits, continue hash partial aggregation. If low reduction/pressure makes the reduction uneconomic, switch at a documented safe boundary to emitting raw rows encoded as `(key, value_i64, 1)`. If any hash state exists at the switch, flush its exact partials **once**, release reservations and then stream the unprocessed tail. Never emit a row twice or drop a row.
3. An initial sample that chooses raw passthrough must replay/emit the sample exactly once, then stream later rows. A conservative forced-hash and forced-raw control must exist. The final merge sums mixed partial/raw contributions with checked sum/count arithmetic, preserves routing/layout/terminal fingerprints, and remains subject to the known bounded aggregation spill domain.
4. Bound output amplification and spill/memory behavior explicitly; choose policy using measured reduction plus network/final-load guard. A higher-cardinality case may choose raw while making end-to-end performance worse; that is a failed policy outcome, not a correctness bug or an excuse to cherry-pick evidence. Do not rewrite the Phase 8 topology heuristic as part of this feature.

Document whether v1 truly transitions hash→raw *after processing* or only chooses once after a prefix; metrics/test assertions must distinguish these. The required exit includes at least one **real in-flight transition** (join hash→grace and/or aggregation hash→raw), not just a plan chosen before any operator work.

## 4. Compatibility and failure envelope

| Workload | Policy/strategy | Supported normal path | Failure/recovery |
| --- | --- | --- | --- |
| Existing distributed `GroupBySum` | static/static-packed/sampled, all prior exchange/retention modes | **Unchanged** by default | Preserve Phase 6 single final-loss replay, Phase 7 restore/recompute and Phase 8 pinned-layout attempts. |
| `GroupBySum` with `aggregate_policy=adaptive_v1` | Explicit opt-in, compatible with documented topology modes | Exactly same final result and transfer accountability | Either prove existing single-final replay/restore with byte-identical replay for chosen mode, or reject the combination at admission until proven; no silent weak recovery. |
| `InnerEquiJoin` | Forced repartition/broadcast and observed-policy; supported join exchange mode(s) | 2–4 fixed workers, both sides complete and independent oracle equal | **Fail closed** on worker/process loss in v1, cancel both sides, quarantine partial gather, clean all spill/retention. No join restoration claimed. |
| Invalid join mode, source drift, duplicate/stale/missing decision, forged side terminal, wrong stage, memory/skew/output cap, deadline, poison | Any | Reject cleanly before result commit | No partial output or stranded background work. |

Version the private control magic (`ADETCP08` is the inspected baseline) if introducing request/reply shapes or wire semantics; keep exchange frame/header v2 unless deliberately and compatibly revised. A decision digest must bind strategy, build side, two source identities, worker count/hash version and execution identity; old aggregate logical IDs and complete-copy receipts remain untouched. Gather must check exactly the expected set of join-final outputs, compare rows/bytes/terminals and commit only full success. Process RSS is not proved bounded merely because the local `MemoryManager` is.

## 5. Work packages, recommended order and ownership

### WP0 — Gate, inventory and ADR before code

- Resolve the Phase 8 go/no-go above. At implementation time pin fresh HEAD and dirty-tree status, read actual AGENTS/current code/ADRs, and record any intervening changes.
- Write `docs/adr/0016-adaptive-operators-v1.md` (next unused ADR number): supported query shape, observation/commit, local fallback and spill guarantees, stage IDs, recovery compatibility matrix, decisions and rejected alternatives. Reconcile the PRD examples not implemented (sort-merge, arbitrary build-side reversal, broader skew handling) without claiming completion of those features.
- Decide whether a smaller first code slice is needed; do not weaken the phase's end-to-end acceptance without an explicit plan change.

### WP1 — Semantics and bounded local physical mechanisms

- Add typed two-input local join node(s)/execution helper with immutable input schemas; implement in-memory hash and real hash→grace fallback, forced modes, cleanup, structured errors, metrics and targeted tests.
- Add producer-local adaptive partial operator with fixed/raw/adaptive controls while preserving the existing `GroupBySum` default/partial codec and checked final merge.
- Prove local cancellation and boundedness on adversarial duplicates, oversized partitions, early sink refusal, 0-row sides and spill-disabled paths **before** distributed plumbing.

### WP2 — Distributed join and immutable strategy protocol

- Add distinct `JoinJob` with two independently validated file assignments/snapshots and fixed 2–4 host placement. Add side-tagged producer/consumer traffic, exact N×N per-side terminal closure, pinned stage/strategy digest, ACK and bounded two-sided draining.
- Use shared exchange/sink mechanisms, not a custom transport. Keep join failure cancellation/cleanup separate from the established aggregate-only one-final recovery path. Reject unsupported modes explicitly. Test missed/stale ACK, mismatched routes and real child failure.
- Preserve existing static/sampled aggregate logic and protocol compatibility through regression tests.

### WP3 — Actual policy, benchmark integration and diagnostic controls

- Wire real observations to a pure deterministic join planner with `forced_broadcast`, `forced_repartition`, and `observed_v1`; record actual distribution and build-side decisions, observation caps, probe overhead and hashes. Ensure forced controls execute identical join semantics and only the declared physical choice differs.
- Introduce `InnerEquiJoin` fixtures/specs with **left and right dataset identities**; add DataFusion SQL oracle and bag-correct canonical comparison. Keep existing independent oracle, result schema v1, process isolation and benchmark engine identity. Reject semantically incomparable run pairs.
- Add explicit aggregate controls and diagnostics for reduction, spill, transmitted bytes and final work; isolate algorithm work from transport, scan and probe times.

### WP4 — Focused verification, empirical acceptance and handoff

- Run small focused Rust tests after each WP; **one** final fmt/check/clippy/workspace test gate and the focused process smoke. Do not repeatedly run full suites to compensate for missing assertions.
- Once all correctness checks pass, build a clean source-bound release and run Phase 9 acceptance with fixed seeds/settings, one warmup and at least three process-isolated measured runs per regime. Preserve all failures, all child JSON, versioned manifests, and exact runtime tree/build identity. Timing thresholds never run in CI.
- Run current Phase 6/7/8 regression/acceptance scripts without misrepresenting known Phase 8 performance failure as Phase 9 success. Distinguish baseline inherited failures from new regressions. Update README/AGENTS/architecture (only real refinements), ADR, plan reconciliations, and `docs/reports/phase-9-implementation-report.md` with an explicit pass/fail conclusion.

**Likely source touch map:** `crates/ade-exec/src/{lib.rs,aggregate.rs,join.rs,memory.rs,tests.rs}`, `crates/ade-distributed/src/{lib.rs,coordinator.rs,worker.rs,protocol.rs,exchange.rs,partition.rs,metrics.rs,gather.rs}` and a small `join_policy.rs` module; `crates/ade-bench/src/{spec.rs,dataset.rs,engine.rs,ade_distributed.rs}`, focused process tests and dedicated `benchmarks/specs/`, `scripts/phase9-{acceptance,verify,bundle}.*`, `docs/{adr,plans,reports,research}`. Do not churn unrelated code simply to fit this list.

## 6. Verification matrix and empirical gates

### 6.1 Focused correctness matrix (automated, CI-safe)

1. Oracle bags: empty/one-side-empty, disjoint, one match, duplicate left/right, repeated identical triples, negative and extreme signed values, zero key and `u64::MAX`; stable across batch/file boundaries and 2/3/4 workers.
2. Every forced join distribution × both build sides × in-memory/grace, plus observed-policy outcomes, yields the same multiset. Assert **actual** side traffic: broadcast build bytes/rows at N destinations, probe rows exactly once; repartition key co-location and exact per-side N×N terminals.
3. Observed small-prefix/large-suffix counterexample never authorizes unsafe broadcast; true completely observed tiny side can broadcast; malformed/overflowing estimates and mixed source identities reject deterministically. Reject duplicate, missing, late, partial or stale strategy ACKs and exchange before commitment.
4. Tiny memory forces a real hash→grace transition with byte/row conservation; spill disabled refuses; unpartitionable hot-key and excessive output refuse *before committed result*, with a clear supported limit. Test strict reservations, transient/source decode bounds, disk/Fd cleanup on output cancellation and error, and no thread/task leakage.
5. Adaptive partial path: exact sum/count under low/high cardinality, mid-batch/batch-boundary transitions, sample replay once, mixed partial/raw, signed overflow/count overflow, zero rows, final spill, full source replay and stored-copy compatibility if admitted. Pin legitimate layout and fingerprint continuity.
6. Real multi-process failure: join worker kill and incomplete gather produce **no** output; cancellation/cleanup completes. Existing Phase 6 one-final recovery, Phase 7 materialization and Phase 8 split/coalesce/biased sample stay passing on their correctness assertions.
7. Benchmark validator rejects tampered duplicate multiplicity, missing rows, changed side identity, conflicting schema, forged decision digest, contradictory actual transfer counts and a best-only timing result.

Do not use wall-clock assertions in normal unit/CI runs; enforce state/bytes/attempt identities and deterministic upper bounds. Keep tests minimal but cover each genuine semantic/cleanup boundary.

### 6.2 Predeclare release experiment before first timed run

Create a versioned `docs/research/phase-9-experiment.md` (or ADR appendix) and acceptance script **before** seeing Phase 9 release timing results. Declare seed, exact left/right row counts/key domains/duplicates, 2-host primary and limited 4-host confirmation, host/toolchain, task/exchange/spill budgets, probe caps, permitted modes, warmups/iterations, metrics and comparison rules. At minimum:

| Regime | Required controls and mechanism evidence | Proposed exit guard (freeze before release measurements) |
| --- | --- | --- |
| Tiny complete build side | forced broadcast, forced repartition, observed-policy; real broadcast and chosen build side | Observed chooses broadcast, oracle passes; median job time at most **1.15×** the better admissible forced strategy; record whether broadcast beats repartition. |
| Balanced/large sides | same controls; broadcast forced arm may legitimately refuse its cap | Observed chooses repartition, oracle passes, no hidden full-side probe; <= **1.20×** forced repartition median job latency. |
| Memory-pressure join | forced in-memory with explicit cap vs grace fallback; same query/output | Real spill transition, no untracked memory growth or incorrect rows; complete in declared supported domain. Quantify latency and disk bytes, with **no fabricated speed-win requirement** versus an infeasible in-memory arm. |
| Low-cardinality `GroupBySum` | legacy fixed hash, forced raw partial, adaptive | Adaptive keeps reduction; same result/lineage; median job <= **1.15×** legacy fixed hash. |
| High-cardinality `GroupBySum` within documented spill domain | three aggregate controls; exact data and network amplification | Adaptive makes a real different choice; median job <= **1.15×** better *feasible* forced control, while showing measured work/bytes tradeoff. |
| Duplicated hot key / pathological skew negative | oracle and bounded outputs for supported case, typed refusal past declared cap | No silent result loss/OOM, no invalid performance comparison; document constraint. |

These numerical bounds are **proposal-time guards**, not post-hoc results. They may be revised only **before** any corresponding release measurement, with explicit rationale and a new experiment version; preserve old versions/runs. Compare medians of at least three independent measured runs after one warmup, with all correctness-valid children included. No best-of-N selection; report dispersion, confidence/coverage, preparation/probe/ACK, operator CPU/work, spill, exchange, total job latency, both sides' byte amplification and actual versus predicted decision. Join strategy and aggregate-policy performance should be reported separately; do not combine unrelated wins to claim the phase passed.

### 6.3 Focused execution commands (adapt to actual scripts/changes)

```bash
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
bash scripts/smoke.sh
# focused Phase 6/7 correctness+release regressions; Phase 8 acceptance remains a separate prerequisite
# after clean, source-bound release build:
cargo build --release --workspace --locked
bash scripts/phase9-acceptance.sh
```

If unavailable (offline deps, permissions, no second host), state **NOT RUN** with reason, never PASS. For multiple-network-host claims, require a genuine supported topology; loopback processes are not independent failure domains. No timing CI gate.

## 7. Exit criteria and required report

The phase is complete only when: (a) Phase 8 is formally closed; (b) exact join bag semantics and the existing aggregate oracles pass; (c) real physical join strategy selection and real local algorithm fallback occur, with forced controls and provenance; (d) adaptive partial aggregation preserves every raw/partial contribution and default behavior; (e) reservations, disk/transport bounds, identity, cancellation and explicit failure limitations are proven in focused tests; (f) all declared supported-domain release guards pass and failed/negative regimes are retained; (g) no regression in accepted Phase 6–8 *correctness* and accepted economic behavior; and (h) documentation, ADR and source-bound raw evidence are complete.

The implementation report must include baseline/final SHA and runtime tree; a changed-files/contract map; support/refusal matrix; actual policy decision examples with estimates versus observations; real conversion and partial-switch tests; oracle semantics and duplicate preservation; bounds (including allocations outside the ledger); protocol and transfer identity; recoverable versus refused failures; exact commands and counts; predeclared experiment version and **all** measured medians/failed gates; raw artifact locations/checksums and artifact accessibility; explicit deviations; stale-doc sweep; and a Phase 10/11 handoff. If only local join or only pre-execution selection exists, report **Phase 9 partial**, not complete.

## 8. Independent-review guidance

The companion review prompt is intentionally **not limited to this plan**. A reviewer must first reconstruct actual PRD/architecture obligations and inspect code/observed behavior independently; then check this plan and implementation for omissions, invalid assumptions, over-narrowing, architectural shortcuts, defects and empirical integrity. A plausible alternative design that is materially safer/better should be reported even if the delivered code faithfully follows this document.


## Implementation reconciliation (2026-10-24)

ADR-0016 records the concrete v1: separate `ade-distributed-join` benchmark adapter,
optional omitted-when-absent manifest extension (old identities preserved), typed
two-source local helper instead of general LocalPlan/IR, spill-only join delivery,
schemas2/3 over context-bound wire2 and ADETCP11 control. The actual prefix
hash-to-raw partial transition supports normal static/threshold/sampled execution;
non-fixed replay, snapshots, copies and failure hooks are rejected upfront.
Existing fixed-hash recovery stays admitted. No required Phase9 outcome is waived.
The frozen experiment v1 uses five measured primary and three confirmation
children after one warmup (118 children,95 measured), preserving the proposed
1.15/1.20 guards. The final report must record actual gate outcomes and all failures.
