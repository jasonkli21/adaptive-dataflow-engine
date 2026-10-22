# Adaptive Dataflow Engine — Phase 8 Implementation Plan
## Runtime Repartitioning and Skew Adaptation (bounded-batch v1)

**Prepared:** 2026-10-21  
**Repository:** https://github.com/jasonkli21/adaptive-dataflow-engine  
**Inspected baseline:** `main` at [`f66addb815deaa8accb2529124b713a8efd85c5e`](https://github.com/jasonkli21/adaptive-dataflow-engine/commit/f66addb815deaa8accb2529124b713a8efd85c5e) (one remediation commit after the original Phase 7 implementation `dc1796e`).  
**Status:** bounded implementation candidate exists with reconciliations in ADR-0015. Phase 8 remains **incomplete** because primary empirical gates failed; see the implementation report.

## 1. Objective, interpretation, and hard boundary

The PRD's Phase 8 goal is that partition topology can change **during a running job**: sample distribution, split oversized logical partitions, coalesce tiny ones, change the downstream task count, apply legal skew-key strategies, and avoid unnecessary full-stage barriers. The measurable exit is reduced single-partition tail domination on severe skew at controlled normal-load overhead. Phases 0–7 are accepted inputs, not disposable scaffolding.

Implement a **small, honest first topology epoch** for the currently supported distributed `GroupBySum` graph. The coordinator learns a bounded runtime distribution sample, commits a deterministic versioned shard-to-final-worker mapping, and the already running job executes under the committed mapping. Keep **2–4 fixed worker processes**. Vary the number of *active final aggregate tasks* from 1 through the worker count, with at most one active final aggregate per worker. Do not pretend that a change in a metric or configuration label is a real task-count change. A split makes multiple distinct key-shards of an originally heavy hash partition execute on different final workers; a coalesce maps distinct small shards to fewer active finals. This is **task/partition adaptation**, **not** worker autoscaling or an arbitrary in-flight rehash of a partially consumed key.

**A necessary scope qualification:** the v1 routing transition commits **before the first affected exchange data segment is emitted**. A short, bounded sampling/commit rendezvous is allowed; a *whole producer completion, whole shuffle, or full-materialization barrier* is not. This avoids double-counting keys across routing epochs and retains the existing final aggregation and recovery mechanisms. Because this is a conservative interpretation of “while execution proceeds,” the independent reviewer must explicitly decide whether it satisfies the PRD's Phase 8 ambition. If the implementation only picks a layout before any meaningful runtime work, or requires whole-producer completion to do so, **do not mark the roadmap exit met**; either implement a safe later routing-epoch switch with keyed reconciliation or record Phase 8 as incomplete. Do not paper over that gap with nomenclature.

The current candidate's bounded work before commitment is the source-prefix probe only; it does not run the original partial aggregate before the layout ACKs. Treat this as pre-exchange runtime-informed planning, not an observed transition during original production. Phase 8 remains incomplete until the product interpretation and empirical gates are satisfied.

### Explicit non-goals

No joins, SQL, streaming/checkpoints, dynamic number of OS workers, generalized morsel scheduler/work stealing, producer-loss or multiple-worker-loss recovery, arbitrary mid-transfer rerouting, salting one `GroupBySum` key without a correct second aggregation, remote cluster durability, host-aware placement, learned policies, segment-level catalog, new result schema, or speculative generalized optimizer. Do not add a second engine execution path just for the benchmark.

## 2. Actual baseline and source-of-truth reconciliation

Read `AGENTS.md`, then `docs/product/prd.md` §§7–9, 11, 15–17, specifically **Phase 8**, `docs/architecture/architecture.md` §§6, 9, 11–15, 16–17, 28–31, Phase 7 plan/report, and ADRs 0010, 0012, 0013, 0014. Use their current revisions at implementation time. The current root `AGENTS.md` intentionally stops at completed Phase 7; update it only after Phase 8 is actually implemented and validated.

Verified implementation facts at the inspected SHA:

- `crates/ade-distributed/src/coordinator.rs`: `StaticAggregateJob`, `validate_topology` requires 2–4 ordered workers; `tasks` launches one producer and one final RPC per worker in parallel; `execute` prepares workers, creates `Gather`, assigns files round-robin, checks producer manifests, handles single final-worker recovery, and performs exchange conservation.
- `crates/ade-distributed/src/worker.rs`: producer does `ParquetScan → Project → HashAggregate` and calls `partition::select` for every destination. `final_task` uses `ade_exec::execute_merge_source_to_sink`. Job state has one exchange `Store` and a one-final guard per worker. Producer, final, and recovery lifetimes and poisoning are intentionally explicit.
- `partition.rs`: `HASH_VERSION=1`, `hash_v1(key)` (`SplitMix64`) and `hash % workers`. Existing hash vectors and static routing must remain unchanged.
- `exchange.rs`: a destination `Store` expects a fixed bounded set of `TransferId`s; adaptive retention currently expects one destination partition per Store. Each producer–destination stream carries ordered frames and a terminal; there are exact sequence/manifest integrity checks. `Store` and its buffers are not an RSS quota.
- `gather.rs`: one gathering store per worker final, final-attempt commit, explicit one-episode recovery, and currently a `collect()` requiring all worker partitions. An active-task subset needs an intentional gather contract change.
- `lineage.rs`, `recovery.rs`: file-morsel source snapshots, whole-producer closure replay, logical-versus-attempt identity, only one post-producer final-worker loss, one recovery episode on a quiescent survivor, no partial successful output.
- `cost.rs`, `materialization.rs`, ADR-0014: explicit `expected_cost_v1`, checked fixed-point accounting, complete-transfer receipts, worker-process-surviving shared fixture, restore-versus-recompute, and execution-UUID-isolated cleanup. Latest remediation uses control `ADETCP06` and provides `scripts/phase7-bundle.py`. **Do not work from the earlier `dc1796e` contract.**
- `crates/ade-bench` owns engine-independent workload semantics, independent oracle, versioned result schema v1, process-isolated runs, and the `ade-distributed` adapter. The existing `hot-key` generator stresses **raw** key frequency; local partial aggregation may collapse that key before exchange. It is **not** sufficient evidence of final-stage skew.

**Baseline hazards to solve, not sidestep:** the existing N×N expected transfers, one `Store` per physical destination, one final RPC/worker, fixed gather cardinality, coordinator conservation equation, full-copy materialization facts, and recovery's assumption that a lost final partition corresponds to a worker ID.

## 3. Semantic model and invariants

Use the following distinct concepts; prefer a small new `topology.rs` or `repartition.rs` module to overloading `partition.rs`:

| Concept | v1 meaning |
|---|---|
| **Worker** | Existing OS process and stable `WorkerId`; count is always 2–4. |
| **Base partition** | Original `hash_v1(key) % worker_count` for comparability with static routing. |
| **Virtual key-shard** | Small, fixed, topology-bounded subdivision of the hash domain (e.g. **48** shards, divisible by 2/3/4); exactly one shard for every key. Derive from the existing v1 hash, e.g. `hash_v1(key) % 48`, not randomized at rerun. |
| **Committed layout** | Immutable, versioned `shard -> physical destination` mapping, active final-worker set, origin base partition, decision evidence and a deterministic digest. A job has one committed v1 layout. |
| **Active final task** | A real execution of the final merge aggregate on one mapped worker; no active final on an idle worker. Retained empty-transfer verification still occurs for inactive destinations. |
| **Transfer identity** | Continue using `JobId/ExchangeId/physical PartitionId/producer TaskId/AttemptId`. Keep stable logical identity and frame sequence within each committed destination. |

**Invariant A — conservation:** for every produced row, exactly one physical destination; all contribution rows for a logical key go to the **same** active final. Do not salt hot keys and concatenate duplicate groups. The existing partial/final SUM is associative under the existing checked arithmetic semantics; never alter overflow, null, sort or oracle expectations.

**Invariant B — plan cutover:** publish the layout to all participants and acknowledge it **before any destination data frame** may be emitted. A failed or partially acknowledged commit has no fallback to an inconsistent layout: cancel and clean up. Early probe traffic must be bounded and cannot leak rows to the wrong epoch. Pin the plan for the job and for recovery. Preserve `HASH_VERSION=1`; version/digest *layout* separately.

**Invariant C — stable failure envelope:** for an eligible single final-worker loss after **all original producers have completed**, replay only original producer source closures for the missing physical destination under the **same layout**; verify its original terminal manifests before committing the recovered final attempt. Valid Phase 7 complete copies remain eligible only when receipt and layout match; all other failures fail closed. If the implementation cannot prove this for dynamic layouts, reject the dynamic/recovery combination explicitly as a **reported Phase 8 limitation** and treat full Phase 8 recovery integration as an unmet exit requirement, not as a passing regression. No silent fallback or expanded recovery claim.

**Invariant D — resource & protocol bounds:** topology metadata O(shards + workers + source files), not O(rows/keys/segments). A bounded probe per original producer; bounded control frames (1 MiB existing limit); existing frame/buffer/staging budgets, cancellation, timeout and poison behavior maintained. Additional probe reads, samples, routing CPU and idle-sink checks must be separately visible; no claim of whole-process RSS enforcement.

**Invariant E — orthogonal policies:** topology policy defaults to `static` and is separately configurable from `exchange_mode`/`adaptive_policy`. All five static exchange mechanisms, `threshold_v1` and `expected_cost_v1` remain selectable and unchanged without Phase 8 opt-in. No benchmark schema v1 change; diagnostic sidecar and engine configuration carry additional topology fields.

## 4. Topology choice and observation contract

### 4.1 Sampling must represent *post-partial-aggregate* load

Raw hot-key frequency is deceptive because each local producer already collapses identical keys. Define a bounded deterministic probe over actual input **before** the main exchange, recording an estimate of distinct `GroupBySum` keys per virtual key-shard **per producer** and/or projected encoded partial-output volume. Start with a small exact capped distinct-key sample or a documented deterministic bounded sketch (not an unbounded `HashMap` of every key). Choose a reproducible sample coverage: e.g. bounded row groups/rows per source file with transparent first-prefix or deterministic stratification limitations. If an extra source read is needed, record bytes, rows, wall time, row groups and effect on latency honestly; reverify immutable snapshot rules and charge overhead. Prefer using the actual scan source to avoid reread, **but do not make a broad local-engine rewrite just for that**.

Specify sample ceiling (e.g. up to 4,096 rows/file and maximum 16,384 sample rows/producer, maximum 48 shard counters and capped key sketch), overflow checking, and deterministic merge order. Bound total control payloads independently of file count; if file lists already exceed a control frame, preserve preexisting failure rather than inventing a new unbounded report. The current fixed 48-shard implementation cannot distinguish **many distinct groups mapping to one occupied virtual shard** from **a single frequent key**; it retains static ownership with `one_occupied_shard`. This is a tested and documented Phase 8 limitation, not successful handling of legal distinct-key skew at finer granularity. The Phase 8 exit remains unmet on this domain. A single already preaggregated hot key is *not* safely parallelizable by hash repartition alone; do not advertise a win.

### 4.2 Pure deterministic policy

Provide a pure function similar to `decide(observations, worker_count, thresholds) -> LayoutDecision` with fixed-point/counted inputs, no networking/storage side effects. Suggested small heuristic:

1. Begin from the static `base_partition` layout and predicted load by destination, using projected **partial output** rather than raw row counts.
2. If estimated largest destination load exceeds an explicit relative imbalance threshold (e.g. 1.5× mean) **and** an absolute minimum work/byte threshold, redistribute a bounded number of its key-disjoint virtual shards to underloaded workers. Never split a single virtual shard in v1.
3. Coalesce negligible shards logically and allow **fewer active final tasks** only when the total small workload and predicted savings justify it. Avoid coalescing unrelated large shards into a new tail. This must be a real RPC/execution difference, not just a label.
4. Hysteresis / minimum-gain gate: skip repartition when gain does not beat estimated probe/dispatch cost. Tie-break by worker/shard IDs so identical input/config produces identical decisions.
5. Reject impossible topology (`0` active, duplicate shard ownership, out-of-range destinations, active bitmap inconsistent with mapping), arithmetic overflow, unsupported hash/layout version and overlarge control payloads.

It is acceptable to keep the cost model **heuristic** rather than pretend Phase 7's materialization prices predict skew-balancing economics. The two policies must remain distinct and replaceable. Record observations, estimated static/adaptive load, thresholds, reason, intended vs actual task count, moved shards and predicted/realized imbalance. A plan must be reproducible from the recorded observations. Explicit topology modes should include `static` and `sampled_v1`; optional forced-layout/testing override may exist **only** if marked an explicit static/diagnostic baseline and validated with the same topology rules.

### 4.3 Runtime commit sequence

Recommended bounded state machine (mechanism may differ if invariants are met):

```
prepare all N workers + stores
  -> start real, bounded probe on pinned source snapshot (N producer assignments)
  -> collect all N compact observations under a whole-probe deadline
  -> pure planner chooses layout, digest, actual active final set
  -> distribute/ack immutable layout to every worker
  -> spawn only active final aggregate RPCs; inactive sinks validate empty terminals
  -> run original producers concurrently with active finals, route every key via layout
  -> terminal ACKs + manifests + receipts; validate routing and conservation
  -> optional one-final-worker recovery using pinned layout
  -> commit active gather attempts, collect/validate oracle result, clean up
```

A **probe-only bounded rendezvous** is permissible; the design **must not read or aggregate the entire dataset before deciding**, nor wait for all producer outputs before starting the final consumers. If the existing blocking local pre-aggregation inherently delays first output, explain that separately from *new* topology barriers. No per-segment coordinator messages. Make probe timeout/cancellation ownership explicit. Static path must bypass new probes, control round trips and routing indirection as much as practical.

### 4.4 Avoiding unsafe skew handling

- Multiple distinct keys that collided into one old `hash % N` destination **can** be moved to separate final tasks because each key maps to exactly one virtual shard.
- A single massive key **cannot** be split into multiple final tasks without a correct second keyed-reduction stage. Current local partial aggregate typically reduces this case already; do not introduce key salting or duplicate final keys without a bounded, correct reconciliation operator.
- A sample can be wrong. Correctness never depends on its accuracy: a bad choice may hurt runtime, **not** results. Report actual post-exchange per-destination rows/bytes, final work and residual imbalance to expose poor predictions.

## 5. Integration work packages (in implementation order)

### WP0 — inspect, ADR and compatibility matrix

Re-inspect current main, including any review handoff and any changes since the pinned SHA. Write `docs/adr/0015-runtime-repartition-v1.md` **before** invasive edits. Include layout identity/version, sampling bias, why no mid-stream reroute, active-vs-physical topology, recovery/failure matrix, sample deadline, no-rebalance cases, and protocol version. If substantial baseline defects are discovered, record them rather than silently broadening Phase 8.

Define a small compatibility table: `static` versus `sampled_v1`, 2/3/4 workers, each retention mechanism, threshold/expected-cost, recovery enabled/no snapshot, lost active final/inactive or producer, normal/no-data transfers. Unsupported combinations must fail clearly at config parse or preparation; accepted static defaults must never silently downgrade.

### WP1 — pure routing/planner core

New `crates/ade-distributed/src/topology.rs` (or equivalently named) owns typed observations, pure layout choice, validation, version/digest, routing. Reuse `partition::hash_v1`; preserve all old hash vectors. Include deterministic no-op, split, coalesce, underestimated-skew, single-key/no-benefit, tie, out-of-range and overflow tests. Per-key `route(key, &layout)` and any fast batch selection must use identical mapping. Do not implement an unbounded key registry.

### WP2 — bounded sample and committed-layout protocol

Add a strictly validated topology configuration in `TaskConfig`/benchmark adapter, `Probe*`/`CommitLayout` control messages (or an equivalent correct round trip), worker pinning, and a producer data-frame gate. Advance the control framing magic from **`ADETCP06` to a new version (nominally `ADETCP07`)** if existing message shapes change; retain exchange transfer wire v2 and hash v1 unless a documented change is unavoidable. Old control frames must fail explicitly. Ensure probe reads are independently bounded, release on cancel and do not leave invisible temp state. A layout digest must be recorded and checked at worker/task/recovery boundaries; it must **not** reuse or overwrite execution UUIDs or logical transfer IDs.

Review remediation extends the observation with exact assigned-source coverage, so the current control magic is `ADETCP08`. Tiny coalescing is allowed only after all assigned rows are sampled; a capped prefix cannot justify a one-final decision.

### WP3 — actual downstream fanout and coalescing

Change `worker::producer` routing from `partition::select(..., N)` to committed layout; each producer still emits one terminal per physical destination, including explicit empties. Keep original `TransferId` and ordered manifests. Start final aggregation only for **active** destinations, with explicit bounded inactive-sink completion/zero-row verification; update `Gather` expected active set and `coordinator::tasks`/conservation. Avoid a global `gather.collect()` materialization **added solely by adaptation**; retain existing result collection semantics and disclose them. Verify every received row satisfies the committed route, or provide an equally strong unforgeable provenance check to prevent a misrouted-but-row-conserving input. Static mode takes the existing well-tested path where possible.

### WP4 — cost materialization, lineage and failure semantics

Pin layout to entire job. Extend the Phase 7 materialization fact/receipt validation and recovery replay so a retained transfer can never be restored under a conflicting layout. Original manifest/fingerprint and execution namespace checks remain required. Replay using the same routing map and **only the failed physical destination**; check recovered manifest and attempt-safe active gather commit. No retry loops. If implementing every combination is disproportionate, make a narrow, explicit, fail-closed matrix and label remaining combinations as **unmet requirements**, not silently disable existing recovery. At minimum demonstrate normal/recovered/adversarial dynamic scenarios without diluting static Phase 6/7 regressions.

### WP5 — benchmark adapter, realistic skew evidence and documentation

Expose `partition_policy`, sample limits, threshold and topology diagnostics through the adapter with strict option compatibility. Add a deterministic **post-partial-aggregate skew** fixture (e.g. many distinct keys deliberately hashing to one original worker destination, with sufficient output state to make final processing measurable); preserve existing `hot-key` and uniform generator identities and independent oracle. If extending synthetic config, preserve old-manifest interpretation and version/identity explicitly. Keep generated large data/results ignored. Add `scripts/phase8-acceptance.sh` and a small verifier (follow Phase 7's predeclared raw evidence / commit binding / bundle pattern, but don't copy a giant matrix). Record source SHA, fixture seed/config, command, effective config, per-child exit, oracle validation, metrics, comparator policy, and failed experiments.

Update `README.md`, `AGENTS.md`, relevant architecture/ADR references, benchmark methodology and `docs/reports/phase-8-implementation-report.md` **after** checks. Update accepted contracts without retroactively rewriting Phases 0–7 as having dynamic topology. Make claims match executable code.

### Recommended file touch list

`crates/ade-distributed/src/topology.rs` (new), `partition.rs`, `lib.rs`, `protocol.rs`, `coordinator.rs`, `worker.rs`, `gather.rs`, `exchange.rs` (only if recipient validation or pinned layout requires), `metrics.rs`, `lineage.rs`/`recovery.rs`/`materialization.rs` (only the necessary identity/fact edges), `crates/ade-bench/src/ade_distributed.rs`, minimal dataset/test support, `docs/adr/0015-...`, `scripts/phase8-*`, report and focused README/AGENTS updates. Avoid broad rewrites of the 66 KB exchange implementation or 47 KB retention implementation without evidence they are necessary.

## 6. Acceptance and verification — predeclare before timing runs

### Correctness / negative (CI-safe focused checks)

1. Routing conservation with 2, 3 and 4 workers; deterministic shard assignment, no duplicate or omitted key, stable key ownership across all producers. Both split and coalesce plans **change the actual active final task set**; output matches the independent oracle, including empty transfers, boundary sizes, signed values and spill.
2. A malformed/partial layout commit, stale epoch/digest, duplicate probe, oversized sample/control, invalid version and misrouted batch are rejected before success. Cancellation/timeout leaves no active leaking producer, partial gather commit, unbounded state or stale cross-job layout. A biased sample hurts only performance.
3. Static `partition_policy` matches old hash behavior and existing metrics/schema; direct, buffered, spill, local materialized, object store and adaptive remain correct; threshold/expected-cost options retain strict validation.
4. With full immutable lineage, one **real** injected final worker-process loss after original producers complete: pinned-layout replay/restore, same logical IDs and attempts, verified terminal fingerprint, no duplicate sink commit. Invalid/missing shared objects, snapshot drift, more than one loss, incomplete producer, invalid target and recover failure fail closed. Where a dynamic combination is intentionally unsupported, a clear rejection is required and the exit marked limited.
5. Boundedness: capped observations and coordinator records independent of row count; exchange buffers and task budgets remain separately accounted. Distinguish unmeasured RSS from reserved memory. Check that a slow/poisoned probe or storage operation does not hang a successful job.

### Empirical experiment (release only, **not** CI timing gate)

Before running a release measurement, commit the workload, seed, row/file count, controls, workers, memory, exchange modes, repetitions, validity rules and thresholds in ADR/script. Recommended **minimal** suite: (a) uniform high-cardinality control, (b) engineered *post-aggregate* hash-bucket skew with splittable distinct keys, (c) single raw hot-key input as a no-benefit/legality counterexample; 2-worker primary, one limited 4-worker confirmation. Compare **same workload/options** `partition_policy=static` and `sampled_v1`, plus optionally a frozen predeclared mapping as a diagnostic, never a substitute for static.

Use independent oracle and manifest equivalence for **every** child; warmup and at least three isolated measured iterations per primary regime; same machine, pinned source commit, clean release build, paired comparable data/config. Report medians and per-run raw values, including **job wall time, final-task exclusive processing/work time, separate final waiting time, last-final completion tail, per-final output/rows/bytes, imbalance, actual active task count, sampling/commit cost, bytes reread, memory/spill and retained object cost**. In the current code `TaskMetrics.elapsed_ms` for a final can include substantial waiting for a producer, so **do not equate `max(final.elapsed_ms)` with compute-skew tail**; instrument or derive a defensible processing/tail metric and retain the raw wall metric as context. Attribute source/producer-bound work separately; do not call input-hot-key concentration an exchange-stage win. A no-benefit or error case must be visible rather than filtered out.

**Proposed falsifiable release gates** (predeclare and adjust only before the first measured run, with rationale): all correctness cases pass; on the engineered severe-skew case, `sampled_v1` reduces median maximum **exclusive final processing/work** time by **at least 20%** versus static and does not increase median end-to-end job latency; on uniform, median end-to-end latency is **no worse than 1.30×** static; the real active final count/layout changes as expected; sample overhead and misprediction are disclosed. If host noise prevents reliable claims, preserve raw evidence and record *inconclusive/failed*, never silently weaken the thresholds. These are research targets, not current results.

### Focused commands / time discipline

During development: compile/format targeted crates, deterministic unit tests, one small distributed oracle test per altered seam. Before handoff, run **once**:

```sh
cargo fmt --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
bash scripts/smoke.sh                # after building required binaries
bash scripts/phase8-acceptance.sh     # release, clean-checkout evidence
# Run focused Phase 6 recovery and Phase 7 economic acceptance regressions
# according to the latest scripts; store raw evidence, don't invent passes.
```

Avoid repeating costly full acceptance after every minor edit. Use unit/integration slices during iteration. Do not add noisy timing assertions to CI; report inability to run a command accurately.

## 7. Completion handoff / stop condition

Phase 8 is **not complete** until real split/coalesce and active final count are exercised, all emitted keys have exactly one owner, pinned plan and failure behavior are correct, normal/static/Phase 7 policies remain compatible, the accepted skew performance gate is measured, and no full producer/shuffle barrier was introduced by sampling. The review should **actively dispute** whether the chosen bounded pre-exchange planning window fully satisfies the PRD; a valid objection can require additional work or an explicit narrower milestone label.

The report must state: source/head SHA; touched modules/ADR; initial and final scope; supported and refused topology/recovery combinations; exact protocol/hash/layout versions; observable decision inputs and why they reflect post-aggregate load; source replay/fact truthfulness; memory/deadline/cancellation limits; test commands/results; release raw artifact locations and verification; negative cases; failed experiments; performance outcomes (including regressions); and deferred **true mid-exchange epoch switching**, multi-task-per-worker, worker elasticity and hot-key salting. Stop after implementation/report/review handoff—do **not** begin Phase 9.

## 8. Implementation sequencing and risk-control decisions

Treat WP1 as a pure core, WP2 as the protocol transition, WP3 as the key integration seam, WP4 as the difficult recovery/cost integration, and WP5 as the proof. The first high-risk decision is whether a cheap bounded *input* sample predicts **post-aggregate output** well enough; if not, improve observation provenance or mark empirical exit inconclusive rather than manufacturing a winning dataset. The second is separating a variable active-task set from immutable N×N transfer sessions and static worker hosts: define expected transfer and gather cardinality **before** editing concurrency code. The third is preserving Phase 7 receipt identity across a new layout: digest checks must not displace existing fingerprint, UUID or ACK safety.

If a different design meets the PRD/invariants with **less** code and fewer barriers, choose it, record the deviation in ADR/report, and make the independent reviewer challenge both designs. This plan is guidance, not a license to sacrifice correctness in order to check its boxes.
