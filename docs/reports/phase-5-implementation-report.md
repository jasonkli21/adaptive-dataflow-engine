# Phase 5 implementation report — Adaptive exchange v1

Date: 2026-10-16.

## Source identity and scope

Baseline: `56ac1daa22ac56f8d64bcef934e3bd0bc3672740` (`Address Phase 4 review findings`).
The supplied Phase 5 plan was initially untracked and is included in the single
Phase 5 implementation commit. The unrelated untracked bootstrap prompt is
preserved and excluded. Final identity is the single commit containing this
report (`git log -1 --format='%H %s' -- docs/reports/phase-5-implementation-report.md`);
the exact resulting SHA is returned in the implementation handoff. A commit
cannot contain its own literal SHA without changing that SHA.

No new dependencies or packages. The one-way `ade-bench -> ade-distributed ->
ade-exec` dependency graph, result schema v1, manifest/child/oracle contracts,
exchange segment wire/schema versions, worker membership and one producer/one
final role per worker remain intact. There is no Phase 6 implementation. Workers
and coordinator must use the same build; the private control protocol has since
been bumped to `ADETCP02` for its adaptive schema.

Material changes:

- `adaptive.rs`: pure deterministic policy, validated configuration, observations,
  transition records, per-destination and job summaries, policy/config tests.
- `retention.rs`: explicit adaptive mode, research pressure configuration,
  observations and mechanism selection through the existing buffer/spool owner.
- `exchange.rs`: serialized adaptive ingress, independently readable metrics,
  bounded object suffix boundaries, common mixed-tier source, pressure hook,
  exact readback, rejection, cancellation and spill containment coverage.
- `coordinator.rs`, `metrics.rs`, `worker.rs`: adaptive conservation/metadata
  validation, job summary and topology validation for research pressure.
- `ade-bench/src/ade_distributed.rs`: strict options and effective metadata.
- Process tests, Phase 5 acceptance/verifier, README, AGENTS and ADR-0012.

## Policy, configuration and observations

Explicit `exchange_mode=adaptive` enables `threshold_v1`. The five static modes
remain independently selectable; `local_materialized` remains default.

| Option | Default | Bounds/meaning |
|---|---:|---|
| `adaptive_policy` | `threshold_v1` | Only supported policy; explicit adapter option requires adaptive mode. |
| `adaptive_buffer_high_pct` | 80 | 1..100, equality escalates. |
| `adaptive_remote_after_bytes` | 67,108,864 | 0 disables remote; otherwise at most 1 TiB. |
| `research_slow_partition` | off | A destination in the configured 2–4 worker topology. |
| `research_delay_after_segments` | 0 | At most 1,000,000 delivered segments. |
| `research_delay_ms` | 0 | At most 2,000 ms, requires a target partition. |

Unknown/duplicate adapter options and unknown/duplicate serialized fields fail.
Adaptive options on static modes fail at the adapter; typed static configs ignore
their serialized adaptive defaults. Effective benchmark metadata records the
policy/thresholds, pressure settings, mode, capacity and object backend.

A snapshot captures incoming payload bytes, current memory occupancy/capacity,
spill backlog, immediate consumer readiness, cumulative existing producer and
consumer wait observations, produced bytes/segments and consumed local progress.
Each transition also records projected buffer percentage and segment lag. This
is exchange pressure, never worker memory/RSS pressure. The policy does not use
lineage/production-cost estimates or inferred nonfatal worker health. Poisoned
workers still fail. Buffer/spool wait counters exclude ingress-lock waiting and
object-provider elapsed time; they are diagnostic observations, not complete
attribution of worker time.

Transitions occur before placing a complete validated segment:

1. Direct → buffered when consumer readiness is absent, queued memory exists,
   or the incoming frame exceeds capacity.
2. Buffered → spill when `(occupancy + incoming) * 100 >= capacity * high_pct`.
3. Spill → object store when an unconsumed spill backlog exists and measured
   produced bytes plus incoming bytes meet the remote threshold.

Evaluation can advance multiple adjacent tiers at one boundary. There is no
downgrade, oscillation, old-segment migration or partition-routing mutation.
Determinism is conditional on observations; OS scheduling changes live snapshots
and can change actual boundaries across executions.

## Lifecycle, mixed-tier reader and boundedness

Validated transfer/segment input enters a destination-private ingress mutex.
Policy does no I/O. The existing queue/accounting mutex remains short and is
never held across file/object I/O. A separate fixed-size snapshot lock lets live
metrics return while ingress waits for rendezvous or retention I/O.

Direct uses real reader rendezvous. Buffered input uses the configured byte
capacity. Adaptive spill writes selected segments to the existing ordered spool
regardless of later free memory. Local memory drains before spool data. Once
object selection begins, every subsequent segment is an object; its original
full transfer/sequence identity supplies the key. The reader waits for validated
transfer terminals and a drained local prefix, then lazily reads each producer's
object suffix. This completion barrier deliberately reuses Phase 4 object
visibility semantics. No additional aggregate implementation exists.

Each accepted segment reaches one logical BatchSource exactly once. Original
per-producer sequence, duplicate/gap rejection, terminal count and empty-transfer
validation precede success; cross-producer order remains unspecified.

Resident adaptive metadata is bounded independently of segment count:

- at most three transition records/four implicit extents per destination;
- four tier byte/segment counters plus cumulative production progress;
- a second fixed-size metrics snapshot, not an observation history;
- at most one object suffix start per expected transfer (Store cap 16; current
  worker jobs at most four), and one lazy reader cursor;
- no per-segment placement heap map, coordinator decision RPC, decision channel
  or per-decision thread.

Current/peak memory and spill-backlog counters remain explicit. Successful final
execution requires both current counters to be zero. Coordinator success also
requires per-tier bytes/segments to equal accepted bytes/segments and transition
metadata to remain within its bound. Job summaries aggregate tier counters,
transition edge/reason counts (one reason per edge in v1), adaptive destinations
and maximum extents. Existing network/row conservation checks still run.

The per-task retained-state budget can overlap for producer and final roles.
Exchange capacity is separate; codec frames, bounded receiver transients, the
consumer's decoded batch and allocator/socket overhead remain separate. This is
not a process RSS cap. Disk/object volume remains unquotaed.

## Failure, cancellation and durability

The first required failure still fails/cancels the job and survivors are cleaned
up. No retries, worker replacement, recomputation, lineage or restore exist.
Adaptive direct waits and research delays poll cancellation. Spill/object paths
reuse ADR-0011's I/O owners, cancellation races and five-second deadlines.
Timed-out non-preemptible work poisons its worker; later PrepareJob fails.

The default object backend is an execution-scoped ephemeral filesystem fixture.
Caller-installed public ObjectStore implementations retain identity/capability
validation. Durability metadata is a caller assertion, not ADE verification.
Object retention does not prove cloud durability or provide recovery. Backend
cleanup semantics remain those of Phase 4; filesystem failure cleanup is best
effort, and stuck helpers can outlive a failed session until the worker exits.

## Validation

The existing offline toolchain was reused with:
`CARGO_HOME=/tmp/ade-cargo RUSTUP_HOME=/tmp/ade-rustup /tmp/ade-cargo/bin/cargo`.
The shell initially had no `cargo` in PATH. Sandboxed socket tests initially
failed with `Operation not permitted`; rerunning with localhost networking
outside that sandbox passed. Python bytecode compilation initially attempted an
unwritable system cache; `PYTHONPYCACHEPREFIX=/tmp/ade-phase5-pycache` passed.

Focused coverage includes pure threshold equality/determinism/overflow/disabled
remote/config rejection; exact direct-buffer-spill-object readback with two
producer suffixes; adaptive empty transfer and duplicate/gap/count rejection;
blocked direct cancellation with readable live metrics; stalled adaptive spill
shutdown/poisoning; shared object put/get cancellation/deadline/backend tests;
and real process oracle correctness, partition divergence and failure cleanup.
The exact readback scenario used tier segment counts `[1,3,1,3]` with transitions
at destination ordinals 1, 4 and 5. The process fixture used 32,768 distinct keys,
a 256 KiB buffer and a 250 ms delay after one segment on partition zero.

| Command | Result |
|---|---|
| `cargo fmt --all --check` | Passed. |
| `cargo check --workspace --all-targets --locked --offline` | Passed. |
| `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` | Passed after correcting local style findings. |
| `cargo test --workspace --all-features --locked --offline` | Passed: 74 tests; all doc-test targets passed. |
| `cargo build --workspace --locked --offline` | Passed. |
| `bash scripts/smoke.sh` | Passed, log `/tmp/ade-phase5-smoke.log`. |
| `git diff --check` | Passed before final documentation/commit verification. |

The Apple linker emitted the existing oversized debug unwind section warning
on debug test/binary linking, with successful exit status. Smoke's single-run
cross-engine comparator reported a regression with incompatible configuration/
engine and dirty-tree warnings; it is not comparable performance evidence.
Release build and acceptance results follow below.

## Release experiment (predeclared before measurement)

The acceptance script fixes one deterministic uniform dataset: one million rows,
four files, cardinality 100,000, seed 42, 8,192-row groups; independent DataFusion
validation and the existing exact oracle. The same aggregate-high-cardinality
workload, 32 MiB per-task managed budget, 1 MiB exchange buffer and prefetch one
apply to all cases. The final adaptive experiment uses high percentage 100 and
remote threshold 1 MiB; the initial 80% attempt is preserved and discussed below.

Normal comparison: two-worker buffered vs adaptive, guard adaptive median <=
1.25× buffered median. Adversarial comparison: two-worker direct, spill and
adaptive; the primary required win is adaptive median < direct median. The hook
delays partition zero for 750 ms after one delivered segment, once; other
partitions are untouched. One four-worker delayed adaptive case checks larger
topology/divergence. Each case uses one isolated warmup and five isolated measured
iterations. Raw iterations are retained; no best-iteration substitution exists.
Timing is local release evidence, never a CI performance gate.

## Initial release attempt and configuration refinement

The first complete six-case run is preserved under
`benchmarks/results/phase5-acceptance-20261016` with generated data at
`benchmarks/datasets/phase5-acceptance-20261016`. It used an 80% high watermark.
All oracle/artifact checks passed. The normal timing ratio was 1.0357 and delayed
adaptive/direct ratio 0.7941. However, its verifier correctly failed the required
two-worker lower-tier-normal-partition criterion: partition one accumulated
817,800 buffered bytes in a late burst and escalated for its final three
segments, yielding `[1,102,1,2]` tier counts. The slow partition used
`[1,37,8,63]`; four-worker normal partitions stayed buffered.

The configuration was refined to a 100% watermark before the final experiment,
so short normal bursts can use the full existing capacity. The policy default
remains 80, the hard byte cap is unchanged, and no performance comparison/guard
was changed. This is a documented failed behavior criterion and a threshold
refinement, not discarded timing evidence. Initial five-iteration medians (ms):
normal buffered 255.401, normal adaptive 264.513, delayed direct 1269.919,
delayed spill 997.622, delayed adaptive 1008.404, four-worker delayed adaptive
923.453. All initial raw iterations remain in those result artifacts.

## Final release acceptance evidence

`cargo build --release --workspace --locked --offline` passed (final build 5m 48s).
`bash scripts/phase5-acceptance.sh` passed with `ADE_PHASE5_DATA` and
`ADE_PHASE5_RESULTS` set to the `phase5-acceptance-20261016-final` directories
under `benchmarks/datasets` and `benchmarks/results`. The verifier passed every
oracle/artifact, partition divergence, tier conservation, bounded metadata and
zero-current-backlog check. All 36 isolated benchmark children (six cases, one
warmup plus five measured) validated; six diagnostic validations and the
DataFusion reference shared fingerprint:

`2cca19703b4136d9cabd624997616a9fd9e78e80adf4e55fa4914bbc7f05d340`.

Dataset manifest fingerprint:
`451d743d6d47fb139843f678a7548f202f26193fe0748cc53fab3d35f50a9bb8`.

| Case | Measured query ms, in original order | Median ms |
|---|---|---:|
| 2-normal-adaptive | 260.698, 263.932, 281.057, 255.159, 254.461 | 260.698 |
| 2-normal-buffered | 254.017, 251.250, 249.159, 253.180, 248.169 | 251.250 |
| 2-slow-adaptive | 1007.629, 1014.295, 1010.612, 1006.824, 1010.225 | 1010.225 |
| 2-slow-direct | 1271.706, 1267.960, 1269.275, 1273.578, 1271.594 | 1271.594 |
| 2-slow-spill | 1000.204, 982.722, 994.532, 991.455, 993.074 | 993.074 |
| 4-slow-adaptive | 919.958, 926.809, 923.617, 915.526, 912.356 | 919.958 |

Normal ratio: **1.0376**, passing <=1.25. Delayed adaptive/direct
ratio: **0.7945**, a **20.55%** median reduction on this defined workload.
Static spill was the best measured adversarial static baseline (993.074 ms);
adaptive did not beat it. The claim is the predeclared narrow win over direct,
with all measured iterations retained, not a general optimality claim.

Actual diagnostic placement histories follow. Tier counts are ordered direct,
buffered, spill, object. Boundaries are zero-based destination acceptance
ordinals; each artifact also retains full transfer identity and producer sequence.

| Case / partition | Tier segment counts | Transitions at ordinals |
|---|---|---|
| 2-normal-adaptive / 0 | `[1, 108, 0, 0]` | 1→buffered |
| 2-normal-adaptive / 1 | `[1, 105, 0, 0]` | 1→buffered |
| 2-slow-adaptive / 0 | `[1, 46, 1, 61]` | 1→buffered, 47→spill, 48→object_store |
| 2-slow-adaptive / 1 | `[1, 105, 0, 0]` | 1→buffered |
| 4-slow-adaptive / 0 | `[1, 43, 1, 51]` | 1→buffered, 44→spill, 45→object_store |
| 4-slow-adaptive / 1 | `[5, 91, 0, 0]` | 5→buffered |
| 4-slow-adaptive / 2 | `[1, 95, 0, 0]` | 1→buffered |
| 4-slow-adaptive / 3 | `[1, 95, 0, 0]` | 1→buffered |

For two-worker delayed partition zero, spill began at producer 1 sequence 25
(ordinal 47, projected pressure 102%, lag 46); object selection began at producer
0 sequence 22 (ordinal 48, projected pressure 102%, lag 47). Projected pressure
includes the incoming segment; the actual queue never exceeds capacity. The
job summary counted `[2,151,1,61]` tier segments and
`[50960,3495352,25480,1388008]` tier payload bytes, with edge/reason counts
`[2,1,1]`, two adaptive partitions and at most four extents per partition.
All local spill and object read/write totals matched. Normal partitions in the
final diagnostic cases performed no spill/object I/O.

Environment: Apple M3, eight physical/logical cores, 24 GiB RAM, macOS aarch64,
kernel 27.0.0; Rust 1.99.0 (`b940084d7`), aarch64-apple-darwin, system allocator,
opt-level 3, thin LTO, one codegen unit, debug-info 1, assertions disabled.
The original build-bound provenance records the Phase 4 baseline SHA with
dirty=true, because that evidence was collected before the single implementation
commit. It is historical development evidence and does not establish acceptance
for the reviewed Phase 5 commit. Coordinator timing includes cleanup; reported
coordinator RSS does not include worker RSS. No cloud execution or CI performance
claim exists.

## Static release regression refresh

`bash scripts/phase4-acceptance.sh` passed using `ADE_PHASE4_DATA` and
`ADE_PHASE4_RESULTS` set to `phase5-static-refresh-20261016` under the ignored
benchmark dataset/result directories. All five static modes passed at two
workers; direct and object store also passed at four workers. All 28 isolated
children and seven diagnostic validations retained oracle correctness, and all
seven artifacts passed `check-result`. Every mode shared fingerprint
`f89c88f70119ffaed5780864029c66eac3d2b3701247fa3fddb3a434109b9c82`.
Two-worker cases had four transfers/32 segments; four-worker cases had
16 transfers/64 segments. Mechanism-specific no-fallback, capacity, transfer/row/
byte conservation and object capability checks passed. Static timings have no
performance claim. Log: `/tmp/ade-phase5-static-refresh.log`.

The Phase 5 implementation has independently correct mixed tiers and divergent
partitions, a pure deterministic policy, bounded metadata/resources, static
compatibility and no recovery expansion. The initial failed divergence attempt
remains explicitly recorded. The historical performance measurements above do
not establish acceptance because they were not bound to a clean final commit;
see the review-remediation record below for commit-bound release validation.

## Limitations and Phase 6 handoff

ADR-0012 records the deliberate object-suffix completion barrier and serialized
adaptive ingress. V1 does not continuously read a live object suffix, migrate
old placements or optimize expected cost. Decisions use local exchange signals;
there is no worker-wide memory accounting, lineage estimate, health prediction,
cloud experiment, durability verification, disk quota or recovery claim.

Phase 6 receives validated identities/sequences, bounded placement transitions,
production sizes, wait observations, backend capabilities as facts and static/
adaptive baselines. Lineage, lost-segment detection and recovery choices require
a new explicit instruction.

## Independent review remediation

- Object-store puts/gets now execute on detached per-operation Tokio owners.
  Their caller uses a bounded response channel with cancellation/deadline checks;
  the helper retains its backend and ephemeral root while blocked. The injected
  provider test blocks synchronously during the initial future poll and checks
  cancellation, poisoning, rejection of later operations and bounded caller
  return.
- Adaptive transition records and current tier are committed only after the
  selected tier accepts the segment. Records identify `threshold_v1`, and
  direct-to-buffered causes distinguish readiness, backlog and oversize.
- Research delay injection is behind the `research-hooks` Cargo feature,
  enabled by `ade-bench`; the default distributed library build has no pressure
  field or source-delay branch.
- The private control protocol is `ADETCP02`; a test rejects an `ADETCP01`
  peer before JSON decoding. Result schema v1 and exchange segment wire v2 are
  unchanged.
- The PRD identifies Phase 5 inputs as candidate signals over the policy's
  evolution and explicitly permits the truthful v1 subset. It forbids fake RSS,
  lineage-cost or nonfatal-health inputs.
- The acceptance verifier checks the clean source commit, all six expected
  configurations, identical environment/build/dataset identities, one warmup,
  five measured iterations and the shared oracle fingerprint. The shell runner
  refuses a dirty checkout. Clean final-commit acceptance evidence is recorded
  in the verification handoff and ignored result artifacts.
