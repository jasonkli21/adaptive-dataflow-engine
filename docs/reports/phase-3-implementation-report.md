# Phase 3 implementation report

Date: 2026-10-15 (America/Los_Angeles).

## Baseline and final commit

Baseline `bc0dbb0759f941f188686d73dc3190645f30ef89` (Fix Phase 2 terminal
resource lifecycle), matching the reviewed prompt. Final commit is the single
commit containing this report, subject `Implement Phase 3 static distributed batch`;
resolve its hash with `git log -1 --format=%H`. A self-referential commit hash is
not embedded. No push. The supplied, initially untracked Phase 3 plan is included;
the unrelated untracked Phase 0 bootstrap prompt is untouched.

The user's explicit Phase 3 instruction supersedes AGENTS' prior Phase 2 stop
boundary. The plan matches current Phase 2 code; no PRD/architecture amendment
was necessary. ADR-0008/0009 refine the relevant architecture boundaries.

## Summary and exact scope

One new crate, `ade-distributed`, owns IDs, partitioning, protocol, exchange,
metrics, worker and coordinator modules plus the `ade-worker` binary. Dependencies
are `ade-bench -> ade-distributed -> ade-exec`; the existing direct local adapter
dependency remains. No frontend or oracle enters either runtime crate. All new
third-party uses are already locked packages; lockfile changes add only the crate
and dependency edges. No speculative crates, adaptation, retry, recovery, lineage,
scheduling, SQL, joins, streaming/state or Phase 4 work.

The real graph is manifest files -> Parquet scan -> projection(key,value_i64) ->
spillable partial hash aggregate -> network hash routing -> local IPC retention ->
IPC scan -> spillable merge aggregate -> network gather. The coordinator
concatenates disjoint key partitions; it never re-aggregates.

## Topology, identities and assignment

One coordinator drives 2–4 static worker processes. Each worker has a stable numeric
WorkerId matching its ordered topology index and separate control/exchange sockets.
IDs distinguish JobId, TaskId, AttemptId, ExchangeId and PartitionId. Producers
are task IDs 0..N-1; finals N..2N-1; attempt zero only. Exchange 0 carries partials;
exchange 1 gathers finals to partition zero. Transfer identity is the full tuple
(job, exchange, partition, producer task, attempt). Source files are assigned in
manifest order by index modulo worker count; duplicate source paths are rejected.
Idle producers still send explicit empty transfers. Extra directory files are
never discovered by this runtime.

Hash version 1 is SplitMix64 followed by unsigned modulo N, fixed by vectors and
row-conservation tests. N final partitions equal N workers. The benchmark adapter
allocates job IDs from a checked instance-local counter. Public coordinator callers
must supply unique IDs across jobs on a reused topology to reject stale streams.

## Control protocol and coordinator lifecycle

ADR-0008 records the private bounded TCP protocol: `ADETCP01`, UInt32 little-endian
length, <=1 MiB strict JSON, echoed per-connection request ID. Requests: Ping,
PrepareJob, RunProducerTask, RunFinalTask, CancelJob, Shutdown. Task requests include
job/task/attempt IDs; Ping verifies the advertised identity/endpoints. Responses
are typed metrics or structured error code/message. Version/unknown-field/variant,
size and truncation failures do not silently decode. Five-second connect/socket
inactivity timeouts and total control-frame deadlines; task RPC/stage deadlines default to 120 seconds. A deadline
wrapper updates remaining socket time before every read/write, bounding total RPC
time even under incremental response delivery. The Rust job API configures the
stage timeout; the adapter records its fixed default.

State progression is Prepared -> ProducersRunning -> ExchangeComplete ->
FinalsRunning -> GatherComplete -> Succeeded; any required failure becomes Failed.
All workers prepare their expected exchange set before producers start. Producer
and final stages each issue concurrent RPCs and enforce a barrier. First received
failure triggers best-effort cancellation before remaining task threads are joined.
Final tasks verify complete exchange before reading. Gather requires one finalized
transfer per final task before collecting. No partial result returns on error.
Ping/prepare/task errors carry job, worker and stage diagnostics; stage timeout uses
an explicit unknown-worker sentinel when no single response identifies the culprit.

## Transport, retention and boundedness

ADR-0009 records the fixed Phase 3 mechanism. Producers lazily open up to N TCP streams as output becomes available, partition
one output batch at a time and encode one destination batch at a time. Frames have
a UInt32 length and standalone Arrow IPC schema/batch/EOS payload. Maximum encoded
frame 1 MiB; maximum rows 1024; non-null key UInt64/sum Int64/count UInt64 schema.
Serialization checks capacity before growing. Receiver prevalidates IPC metadata
and body lengths within the frame before Arrow allocation, forbids compression,
then checks schema, row cap, exact one-batch framing and explicit completion.
There is no whole-exchange Vec or in-memory network queue.

Four receiver handlers maximum per listener; eight control handlers maximum per
worker. Completed thread handles are joined and retired, so metadata does not grow
with connection history. Blocking TCP and synchronous sinks provide backpressure.
Accepted sockets explicitly clear nonblocking mode inherited on macOS. Finite
socket timeouts and total per-header/frame/ACK deadlines bound stalled or trickling
operations and receiver teardown. Blocking aggregate construction does not hold
idle outgoing connections.

Each store owns a unique TempDir and fixed expected identity set (at most 16).
Started identities are rejected on reuse, including failed attempts. Each receiver
writes one NamedTempFile incrementally, observes a zero-length terminator, finishes
the IPC writer, persists without clobbering, records completion, then ACKs the full
transfer ID. Empty streams still produce valid schema/EOS files and ACKs.
Truncated or invalid streams never enter the completed registry; partial files
remove on drop. The final stage requires every expected producer. Missing/duplicate
transfers cannot silently produce a successful partial result.

All exchange state is execution-scoped, worker-local, lossy and non-durable.
No fsync or recovery guarantee. CancelJob removes worker job ownership and signals
its cancellation token. Active task/receiver Arc references defer directory removal
until their file handles close. Gather has cancellation on every exit and joins
its receiver threads before removing storage. TempDir cleanup on filesystem failure
is best effort. Locally managed worker roots are additionally parent-owned and
removed after child kill/wait, including worker process loss.

## Local runtime changes and memory truthfulness

`ade-exec::execute_to_sink` uses the same `drive` loop as collected execution.
It hands over one output batch synchronously without retained-output reservations.
The loop explicitly shuts down operators after sink errors/cancellation as well
as success. RAII removes spill on error; success snapshots metrics after shutdown
and explicitly closes spill. Existing collected output still owns its reservation.

`LocalPlan::MergeAggregate` changes only input contributions from (value,1) to
(partial sum,partial count) in the existing spillable table/partition lifecycle.
It checks positive non-null counts, UInt64 count addition, i128 sum addition and
Int64 output conversion. No second unbounded aggregation implementation exists.
`LocalPlan::IpcScan` iterates files/batches in order, opens one at a time, validates
schema consistency and requires the finalized IPC EOS marker; corrupt/truncated
input fails. The distributed lifecycle owns these input files, not local spill.

Memory remains three distinct categories:

1. Local execution reservations enforce ADE-owned retained aggregate/prefetch/output
   state under each worker task's budget, preserving Phase 2 spill/no-progress behavior.
2. Exchange memory is separately bounded by frames/rows/connection count: at most
   four receiver frames plus bounded decoded fixed-width batches and IPC writers;
   sender retains one bounded destination batch/frame and its current local batch.
3. Protocol, Arrow/Parquet codec/kernel, thread/socket/allocator transient memory is
   outside the local ledger. The budget is not a total-process/network RSS cap.

Coordinator gather and the harness's independent oracle collect the final result,
so this phase does not claim arbitrary larger-than-memory output validation.
Existing fixed 16-way aggregate spill can still fail with a clear no-progress error
under too-small budgets or distinct-key skew. Partial output sums must fit Int64.

## Worker/process lifecycle and failure behavior

Workers allow one active job/task. Separate bounded control handlers can process
CancelJob while local execution or exchange is active. Cancellation is cooperative
at local batch/row and network frame boundaries; an in-progress filesystem call
cannot be preempted. Socket operations have finite waits. Task/network/remote/control
failure cancels the job; there is no retry, recovery or successful partial output.
Unexpected listener termination also cancels current work and joins handlers.

LocalCluster binds workers on loopback port zero, reads a bounded framed readiness
record only after both sockets bind, and owns every Child. Failed/timeout readiness
kills/waits before joining the reader. Drop kills/waits all children before removing
the parent temp root. Worker stdout is readiness-only; diagnostics use stderr.
Benchmark process startup is setup time, graph execution is query time. A separate
coordinator binary is unnecessary; the Rust API and benchmark adapter own that role.
Gather currently binds loopback, so supported deployment is local multi-process.

## Metrics and benchmark integration

DistributedMetrics returns worker count, producer/final task maps and per-worker
received/gather totals. Task metrics include source files, input/output rows,
duration, managed peak, local spill bytes, sent/received rows/transfers/payload
bytes/peak frame and producer destination distribution. Producer input is source
rows; producer output is partial rows; final input is partial rows; final output
is group rows. Byte counters measure encoded IPC payload, excluding header/length/
ACK/control/TCP overhead. On failure the structured error identifies job/worker/stage;
there is no success metrics object or partial result.

`ade-distributed` supports only synthetic GroupBySum; other queries are rejected.
Options: workers 2–4 (default 2), positive batch_size (8192), memory_limit_bytes
(256 MiB), spill_enabled (true), prefetch_batches 0–64 (0). EngineInfo records all
of these and fixed output/hash/frame/task-timeout settings. The worker binary is
resolved beside the harness or from ADE_WORKER_BIN. Manifest/schema validation,
independent generator oracle, DataFusion baseline, process-isolated child protocol,
result schema v1 and measured-child warn logging remain intact. Standalone debug
validation emits the complete metric object; CI smoke audits distribution without
a timing gate. CI builds the workspace so the worker executable is available.

## Deviations and durable decisions

- Added PrepareJob so incoming transfers can validate the full expected set before
  producer execution; this is the smallest explicit control prerequisite.
- Omitted the optional coordinator binary: the public API plus harness already
  provides the coordinator entrypoint. Local loopback deployment satisfies the
  required multi-process exit condition; multi-host advertised gather is deferred.
- Reused local adapter option parsing for the four shared controls rather than
  maintaining duplicate parsers. Query/schema checks remain adapter-owned.
- IPC input intentionally requires finalized streams rather than Arrow's more
  permissive live-stream EOF behavior. Receiver validates advertised allocations
  before invoking the pinned public Arrow streaming decoder.
- ADR numbering preserves 0008/0009 from the architecture backlog; ADR-0007 was
  never separately resolved and is not repurposed (Phase 2 spill is in 0006).

## Commands and validation

Toolchain environment (same cached installation as Phase 2):

```sh
export PATH=/private/tmp/ade-cargo/bin:$PATH
export RUSTUP_HOME=/private/tmp/ade-rustup
export CARGO_HOME=/private/tmp/ade-cargo
```

Commands actually run during focused implementation and integration:

```sh
cargo check -p ade-exec --offline
cargo check -p ade-distributed --offline
cargo check --workspace --all-targets --offline
cargo test -p ade-exec phase3 --offline
cargo test -p ade-distributed -p ade-exec --offline -- --nocapture
cargo test -p ade-distributed --test static_job --offline -- --nocapture
cargo clippy -p ade-distributed -p ade-exec --all-targets --offline -- -D warnings
cargo build --workspace --locked --offline
cargo fmt --all
cargo fmt --all --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
```

The initial sink refactor exposed ownership of SpillManager::close, resolved by
closing at the owning entrypoints after shared driving. Initial adapter compilation
found its module name shadowed the external crate error; absolute crate qualification
fixed it. Focused Clippy found documentation, checked-conversion and small style
issues, all addressed. Initial network tests were blocked by sandbox TCP listener
restrictions; network validation uses approved execution outside that sandbox.
The first process test found inherited macOS nonblocking socket mode, fixed by
explicitly clearing it on accept. These initial failures are not acceptance evidence.

Focused tests cover merge/count overflow and truncated IPC, forced-spill sink
failure/prefetch teardown, post-shutdown zero managed reservations, hash vectors
and row conservation, control version/length/truncation, exchange multi-batch/empty
ACKs, duplicate/oversized/schema-version/disconnect rejection, partial file cleanup,
and a real two-worker graph with hand-computed expected rows. The process case
also repeats a successful job, installs survivor state, kills its peer, observes
bounded structured failure, confirms survivor cleanup by preparing another job,
and verifies parent-root removal after process teardown. Adapter tests cover
option validation and rejection of all unsupported query families without spawning.

Final review additionally found that inactivity timeouts alone could be extended
by a continuously progressing peer. The same total-deadline I/O wrapper now bounds
every control header/message and exchange frame/ACK operation. Connections open
lazily when blocking aggregates produce output, avoiding an idle connection during
aggregate construction. A focused progressing-peer regression test verifies that
steady byte delivery cannot extend the deadline. This late correctness fix requires
rechecking the affected gates/smoke and refreshing the optimized executables; only
the final executables are used for acceptance. A newer toolchain deprecated
AtomicU64::fetch_update, so the adapter's checked per-instance job counter uses a
mutex compatible with the declared MSRV instead of adopting a newer atomic API.

Final gate and release acceptance results follow.

## Workspace gate and smoke

Final format, all-target workspace check and all-target/all-feature Clippy with
`-D warnings` pass, using the locked offline commands above. The full workspace
suite after the deadline/lazy-connection fix passes **51 tests**: 20 benchmark
unit tests, five Phase 0 integrations, one Phase 1, one Phase 2, five distributed
unit tests, one distributed process integration and 18 local-runtime tests.
All doc-test targets pass. The initial integrated suite passed 50 tests before
the added progressing-peer regression. Existing macOS linker compact-unwind size
warnings occur for the large harness executable; they do not fail the gate.

Additional final commands:

```sh
cargo test --workspace --all-features --locked --offline
cargo build --workspace --locked --offline
bash scripts/smoke.sh
cargo build --release --workspace --locked --offline
git diff --check
```

Network tests/smoke execute with sandbox TCP restrictions lifted through the
approved execution tool. No network dependency download, cargo clean, broad chaos
matrix, Criterion run or TPC-H regeneration. Two earlier optimized build attempts were
interrupted during final deadline review (frame operations, then completion ACK); only the refreshed final optimized
worker/harness supply acceptance evidence.

## Known limitations, deferred work and next-phase considerations

- Supported deployment is loopback multi-process with shared local input paths.
  Public worker/coordinator APIs accept explicit endpoints, but coordinator gather
  advertisement for multi-host use is deferred. There is no separate coordinator CLI.
- One active job/task per worker; static whole-file assignments and fixed worker/
  partition count. Control payloads are capped at 1 MiB, including source lists.
  No dynamic membership, placement, work stealing or skew redistribution.
- Only the non-null UInt64-key/Int64-sum/UInt64-count aggregate is distributed.
  Producer partial sums must fit Int64 even if a hypothetical global cancellation
  of larger opposing partials would fit. Existing fixed-partition spill can fail
  explicitly at insufficient budgets; recursive repartitioning remains deferred.
- Exchange is static, fully materialized and non-durable. No compression, TLS,
  authentication, object storage, remote shuffle, replay, retries or recovery.
  Worker failure causes job failure. The runtime is intended for trusted local use.
- Managed retained memory, separately bounded frame/batch buffers and codec/library
  transients are different categories. Coordinator gathering/oracle are collected.
  Harness child peak RSS is not summed worker/cluster RSS; no cluster RSS limit or
  larger-than-memory validation claim is made.
- Five-second operation deadlines can reject a legitimately very slow transfer;
  task deadline defaults to 120 seconds and is configurable through the Rust job
  API. Cancellation is cooperative; blocked filesystem calls are not preemptible.
  Cancel/cleanup responses are best effort after network or process loss.
- RAII removes temporary state on ordinary success/error/cancellation. A hard
  worker kill cannot run local-runtime spill destructors; parent-managed exchange
  roots are removed after wait, while unrelated OS-temp local spill leftovers may
  require OS/manual cleanup. Filesystem cleanup errors on drop are best effort.
- Metrics are diagnostics, not result-schema extensions. Encoded payload bytes
  exclude protocol/TCP overhead, and no speedup or performance threshold is claimed.

Phase 4 should begin with a fresh plan. Consider a second exchange mechanism before
extracting a unified interface, multi-host gather advertisement, explicit external
memory telemetry and cancellation/deadline ownership at that boundary. Preserve
stable identities, the independent oracle and truthful local storage guarantees.
Do not implement lineage, retry/recovery, cost policy or adaptation as cleanup.

## Release acceptance reproduction

Acceptance artifacts stay ignored under `benchmarks/results/phase3-acceptance/`;
inputs under `benchmarks/datasets/phase3-acceptance/`. A temporary script
`/private/tmp/ade-phase3-acceptance.sh` executes the following commands plus row/
byte conservation and per-worker distribution assertions. Choose fresh immutable
paths when repeating:

```sh
BIN=target/release/ade-bench
DATA=benchmarks/datasets/phase3-acceptance
RESULTS=benchmarks/results/phase3-acceptance
mkdir -p "$RESULTS"
$BIN env --format json > "$RESULTS/environment.json"
$BIN data generate synthetic --dataset-id phase3-uniform-v1 --rows 100000 --files 4 --row-group-rows 8192 --key-cardinality 10000 --seed 42 --output "$DATA"
$BIN run --engine datafusion --spec benchmarks/specs/aggregate-high-cardinality.toml --dataset "$DATA" --verify-data full --warmups 1 --iterations 3 --output "$RESULTS/datafusion.json"
$BIN check-result "$RESULTS/datafusion.json"
for workers in 2 4; do
  $BIN run --engine ade-distributed --engine-option workers="$workers" --engine-option memory_limit_bytes=2097152 --engine-option prefetch_batches=1 --spec benchmarks/specs/aggregate-high-cardinality.toml --dataset "$DATA" --verify-data full --warmups 1 --iterations 3 --output "$RESULTS/distributed-$workers.json"
  $BIN check-result "$RESULTS/distributed-$workers.json"
  ADE_LOG=debug $BIN validate --engine ade-distributed --engine-option workers="$workers" --engine-option memory_limit_bytes=2097152 --engine-option prefetch_batches=1 --spec benchmarks/specs/aggregate-high-cardinality.toml --dataset "$DATA" > "$RESULTS/validation-$workers.json" 2> "$RESULTS/metrics-$workers.log"
  $BIN compare "$RESULTS/datafusion.json" "$RESULTS/distributed-$workers.json" --format json > "$RESULTS/compare-$workers.json"
done
```

## Release acceptance results

The final locked optimized workspace build passed in **5m 29s**. The exact final
source also passed the full 51-test gate and refreshed smoke after the ACK deadline
correction. Smoke validates all six synthetic specs on local ADE/DataFusion,
constrained local spill/prefetch, result integrity/comparison, and a two-worker
independently validated distributed aggregate with source/network completeness
assertions. No remote CI execution is claimed. Final whitespace checks include
staged new files; Markdown hard breaks in the supplied plan were normalized to
backslashes to satisfy that gate.

Environment: Apple M3, eight physical/logical cores, 24 GiB RAM, macOS aarch64,
kernel 27.0.0; Rust 1.99.0, aarch64-apple-darwin, system-default allocator,
opt-level=3, thin LTO, one codegen unit, debug-info=1, assertions disabled.
Build-bound provenance is the baseline commit with dirty=true, because evidence
is collected before the single implementation commit. All three result artifacts
pass check-result; both cross-engine compare commands accept semantic compatibility.

Fixture: synthetic-events-v1, 100,000 rows, four 25,000-row Snappy Parquet files,
8192-row groups, 10,000 possible keys, uniform distribution, seed 42, 16-byte payload.
Many keys cross producer boundaries. Logical input fingerprint:
`72091412f6f33c32b4ef0a971fa194fe384e4e09b2af046ed5ba537d392345c7`.

DataFusion and both worker counts return **10,000 groups**, passing the independent
oracle on every warmup, measured iteration and diagnostic validation. All result
fingerprints are:
`f89c88f70119ffaed5780864029c66eac3d2b3701247fa3fddb3a434109b9c82`.

Each run has one warmup and three measured isolated children, full parent data
verification and batch_size=8192. DataFusion 55.2.0 uses two target partitions,
statistics disabled. ADE uses 2 MiB per local task, spill enabled and prefetch=1.
Process startup is setup time. Query timings are local development evidence only;
there is no speedup claim or performance threshold. Compare classifies these ADE
timings as regressions versus the unconstrained DataFusion baseline, with engine/
configuration and dirty-source warnings. Coordinator RSS excludes worker RSS.

| Engine | Median query ms | Mean query ms | Population stddev ms |
|---|---:|---:|---:|
| DataFusion | 4.664375 | 4.773042 | 0.337537 |
| ADE 2 workers | 110.947333 | 110.325708 | 18.904404 |
| ADE 4 workers | 76.870625 | 77.477986 | 4.578517 |

Standalone release diagnostics show the actual work distribution (lists ordered
by WorkerId). Payload byte/row totals sent equal receiver totals; final input rows
equal the producer partial rows. Every worker performs producer/final work and
receives nonzero exchange bytes. Peak frames remain below 1 MiB, and managed peaks
remain below 2,097,152 bytes. The two-worker final stage also spills, proving that
the merge mode reuses the local spill path.

| Metric | 2 workers | 4 workers |
|---|---|---|
| Source files per producer | [2, 2] | [1, 1, 1, 1] |
| Source input rows | [50000, 50000] | [25000, 25000, 25000, 25000] |
| Partial output rows | [9937, 9940] | [9192, 9184, 9202, 9208] |
| Final partial-input rows | [9924, 9953] | [8893, 9259, 9514, 9120] |
| Final output groups | [4995, 5005] | [2415, 2517, 2580, 2488] |
| Exchange transfers | 4 | 16 |
| Exchange payload bytes | 508544 | 946432 |
| Exchange peak frame bytes | 17224 | 16072 |
| Gather rows / payload bytes | 10000 / 255616 | 10000 / 250464 |
| Producer managed peak bytes | [992092, 992092] | [992092, 992092, 992092, 992092] |
| Final managed peak bytes | [418656, 417888] | [427008, 427008, 427008, 427008] |
| Producer local spill write bytes | [1260992, 1252064] | [629952, 625216, 631296, 628128] |
| Final local spill write bytes | [317632, 318560] | [0, 0, 0, 0] |

Raw per-destination metrics, task durations, JSON result artifacts and comparisons
remain in the ignored acceptance directory. The post-run process inventory found
no remaining debug/release ade-worker processes from these runs. Process-root
removal and survivor cancellation are additionally verified by the process test.

## Phase 3 acceptance audit

| Plan requirement | Verified evidence |
|---|---|
| Static distributed graph, 2–4 workers | Two-worker process test and both release topologies; source assignment covers exactly four files; public local operators in both stages |
| Correctness and gather | Hand-computed process fixture; mandatory independent oracle on every benchmark child; identical DataFusion/2-/4-worker fingerprint; gather is concatenation |
| Stable routing and identity | Versioned SplitMix64 vectors and row conservation; explicit job/task/attempt/exchange/partition IDs; task and ACK identity checks |
| Exchange completeness | Prepared expected sets; duplicate starts rejected; explicit empty transfer; partial/disconnected/oversized/wrong-version transfers fail; final/gather check every expected identity |
| Bounded resources | 1024-row/1 MiB frames, bounded codec writer, IPC allocation prevalidation, <=4 receiver and <=8 control handlers, synchronous backpressure; no whole-exchange collection |
| Local prerequisites | Shared sink/collection loop; tested sink failure/spill/prefetch cleanup; incremental finalized IPC scan; checked partial merging in the existing spillable aggregate |
| Failure and lifecycle | Worker loss with preinstalled survivor state fails, cancels and permits subsequent prepare; total progressing-peer deadline regression; finite frame/ACK waits; parent kill/wait and root-removal assertion |
| Observable distribution | Every release worker has input/partial/final rows and nonzero sent/received payload; conservation assertions and per-destination metrics; local managed/spill counters |
| Benchmark contracts | Synthetic GroupBySum only; strict options and unsupported-query tests; manifest/schema/oracle ownership and child/result schema v1 preserved |
| Gates, smoke and documentation | Final fmt/check/Clippy, 51 workspace tests, extended smoke, optimized build, 2-/4-worker acceptance, staged diff check; AGENTS/README/plan/report and ADR-0008/0009 |
| Phase boundary | One new cohesive crate; no adaptive/unified exchange framework, scheduling, lineage, retry/recovery, joins, frontend or streaming/state |

Phase 3 implementation and its acceptance checks are complete. Phase 4 remains
outside the active boundary and requires a new explicit instruction.
