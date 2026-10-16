# Phase 4 implementation report

Date: 2026-10-16 (America/Los_Angeles).

## Baseline and final commit

Phase 4 baseline: `2fa71e372d64323e499ece3421c6cd2ef5c74bfa`, `Fix Phase 3 review
findings`. Phase 4 implementation commit: `e1ba957095464347946e5173f7c7b249813cedb1`,
`Implement Phase 4 static unified exchange`. The independent review remediation
uses that implementation commit as its baseline and is committed as one follow-up;
the final chat response supplies the follow-up hash. No push is part of this task.

The user's explicit instruction authorizes Phase 4 and supersedes AGENTS' Phase 3
stop boundary. The initially untracked Phase 4 plan is included, with status and
whitespace normalization only. The unrelated untracked Phase 0 bootstrap prompt
is preserved and excluded from the commit. PRD scope is unchanged.

## Implemented scope and decisions

ADR-0010 records the unified contract, static mechanisms, segment protocol,
producer/final overlap, resource accounting and durability boundary. It refines
ADR-0008/0009 while retaining their identity, total-deadline, bounded-codec,
monotonic JobId and cleanup guarantees. Three crates remain; there is no
speculative exchange/common crate and no frontend dependency in core execution.

Strict `ExchangeConfig` is sent in private `TaskConfig`, exposed by the benchmark
adapter and recorded in effective engine metadata. The default remains
`local_materialized`. Modes are `direct`, `buffered`, `spill`,
`local_materialized`, `object_store`. Buffer bytes default to 4 MiB and validate
in 1..=256 MiB; buffered mode additionally requires at least the 1 MiB maximum
frame. Direct/local/object modes report but do not consume this capacity setting.
Unknown/duplicate options and invalid mode/capacity are rejected.

Hash distribution remains version-1 SplitMix64 modulo fixed worker count.
`Sender::batch/finish` is the mechanism-independent producer contract.
`Store::reader` implements the one `ade-exec::BatchSource` consumed by every final
aggregate through `execute_merge_source_to_sink`. The existing spillable merge
aggregate, sink/drive loop and shutdown are reused; no second aggregate table is
introduced. Existing finalized IPC scan remains tested and supported. External
source schema changes, source errors and cancellation fail the local operator.

Private exchange wire version 2 adds an explicit u64 segment sequence after every
u32 frame length. Each non-empty standalone IPC batch is a segment. The zero
length terminal carries the final count. Sequences begin at zero; duplicate,
gapped or incorrect terminal counts fail. Empty transfer count zero is valid.
Headers/ACKs still carry the complete job/exchange/partition/producer/attempt
identity, hash and schema versions. IPC metadata/body allocation-envelope checks,
1 MiB frame/1024-row caps, non-null schema validation and finite total socket
operation deadlines remain. Benchmark child/result schema v1 is unchanged.

## Mechanism semantics

| Mode | Implemented behavior | Observable proof |
|---|---|---|
| Direct | One rendezvous slot, only filled while a reader waits; receiver blocks until the frame is taken. No normal local/object retention. | Nonzero bounded handoff peak; zero spill/local/object bytes. |
| Buffered | Destination-wide byte-bounded encoded FIFO; full queue blocks writers; take releases payload accounting. | Peak <= configured bytes; no disk/object retention. |
| Spill | Fixed finite FIFO; new frames that do not fit, or arrive during a disk backlog, append to an anonymous local spool. Drain memory then spool in order, truncate drained spool. | Nonzero equal spill writes/reads under forced threshold; finite resident peak. |
| Local materialized | Incremental per-transfer IPC file, finish/persist without clobber before completion registry/ACK; wait for all expected producers. | One finalized file per required transfer, nonzero local bytes. |
| Object store | Bounded per-segment put through provider-neutral public APIs; completion gate checks every transfer terminal; lazy sequence enumeration and bounded head/range read. | Object count equals segments and object write/read bytes equal received payload. |

Spill metadata is constant-sized: one spool plus read/write offsets, rather than
an unbounded queue of spill-file paths. A private I/O owner serializes log
records outside the shared queue/accounting mutex. A write error cancels the
session. Per-transfer sequence order is preserved; cross-producer ordering is
unspecified. Disk volume is not quota-managed in this phase.

Object keys include job/exchange/partition/producer/attempt/segment identity.
Successful segment uploads alone are insufficient for visibility: final readers
wait for successful transfer completion/counts and all required identities.
The completion registry has at most the fixed expected producer count. Object
reads inspect size before allocating a bounded range, then run the same IPC
prevalidation/decoder. Backend errors propagate as structured library errors.

## Producer/final lifecycle and failure

Each worker has one prepared job, at most one active producer and one active
final role. Started flags prevent duplicate starts after completion; active flags
prevent concurrent duplicates. Prepare requires no remaining active roles and a
strictly increasing JobId. Cancellation removes prepared ownership but active
Arc references retain storage until bounded work ends; the high-watermark remains.

The coordinator launches final RPCs first, then producer RPCs, sharing one
absolute task deadline across connect/request/response for all 2N required tasks.
OS dispatch order is not a readiness guarantee; bounded handoff safely waits for
the reader. Materialized readers wait internally. First required error triggers
survivor cancellation before joining task RPC threads. Failure never returns a
partial result. Success verifies transfer, segment, row and payload conservation,
including final input rows, then collects fixed local-materialized gather.

Receivers remain limited to four handlers and control to eight handlers. Active
receiver cancellation registrations are removed when the handler leaves; metadata
does not grow with connection history. Receiver drop cancels active sessions
before joining, including blocked direct/buffered handoffs. Reader polling checks
cancellation every 2 ms; producer capacity/handoff waits check it at most every
10 ms. Spill/object-store operations add cancellation polling and a five-second
operation deadline. If an underlying blocking helper remains active, its worker
is marked poisoned, task/receiver ownership returns, later `PrepareJob` is
rejected, and shutdown does not join the helper. Local parents kill/wait worker
children before removing their shared temp root, including process loss. No
retry/recovery.

## Memory and durability statements

`memory_limit_bytes` remains a per-local-task retained-state budget. Producer and
final overlap can activate two such budgets per worker. Retained exchange payload
is separately bounded; direct has one frame slot, buffered/spill have configured
encoded-byte capacity. Up to four receiver encoded/decoded frame transients,
consumer decoded input, queue/codec/socket/kernel/allocator overhead and local
scan transients are outside that payload counter and the local ledger. None of
these limits is a total process/worker RSS cap. Gather/oracle output remains
collected at the harness boundary, as in Phase 3.

Object storage is an `Arc<dyn object_store::ObjectStore>`. The default worker
uses `LocalFileSystem` in a unique execution-scoped sibling directory independent
of local exchange IPC/spill files. Embedded callers can install a backend at
worker-session setup and specify its name and declared durability capability in
job configuration; task metrics report both. The capability is caller supplied
and not verified by ADE. Public API behavior is tested with no cloud credentials.
The default demonstrates neither cloud nor multi-host durability. Filesystem
objects and completion metadata are ephemeral and cleaned with the session/parent
root; there is no recovery, durable manifest or cross-job reuse claim.

## Deviations and dependency reconciliation

- Keep `exchange.rs` plus internal `retention.rs`, rather than a speculative module
  hierarchy. Worker code accepts an opaque object-store backend; storage
  implementation details remain behind the public `ObjectStore` trait.
- Add a narrow merge-source sink entrypoint and an internal-use provisional plan
  placeholder, retaining cloneable existing local plans and their drive loop.
- Spill uses a single bounded-record spool rather than per-segment path metadata.
- Object mode uses one bounded object per segment and a completion registry rather
  than multipart whole-transfer objects. This keeps uploads/downloads bounded and
  completion sequence explicit without an unbounded object index.
- The local object backend remains the default fixture. An embedded worker caller
  may inject another implementation; cloud-specific bootstrap and credentials
  remain deployment responsibilities. No cloud durability is implied by the
  provider-neutral interface.
- Source polling is a small synchronous bridge instead of a general async runtime
  or new scheduler. Wait counters are observations, not a precise lag/cost model.
- A rejected transfer cancels its session, so older rejection tests now use fresh
  sessions instead of expecting valid traffic to continue after rejection.
- `object_store` 0.13.2 (`fs`, default features off) and Tokio remain the runtime
  integration. The remediation adds locked `async-trait`/`futures` test helpers
  for a hanging provider double and Tokio sync for release gates; no package
  upgrades. Public locked source APIs were inspected.

## Commands and test results

Toolchain was provisioned at `/tmp/ade-cargo` with rustup at `/tmp/ade-rustup`;
commands used `CARGO_HOME=/tmp/ade-cargo RUSTUP_HOME=/tmp/ade-rustup` and
`/tmp/ade-cargo/bin/cargo`. Dependencies were available offline.

Focused development checks:

- `cargo check -p ade-distributed --offline`.
- `cargo test -p ade-distributed -p ade-exec --offline`.
- `cargo clippy -p ade-exec -p ade-distributed --all-targets --offline -- -D warnings`.
- `cargo fmt --all` after edits.

The focused suite passed all five mechanism round trips, explicit empty transfers,
sequence/completion rejection, stale/duplicate transfer rejection, blocked
producer/consumer cancellation, finite buffer accounting, exact-once spill FIFO
readback/truncation, object key isolation/backend errors, incomplete-object
invisibility, external-source merge/error/cancel/shutdown and existing local
operator regressions. The process test uses four files with 4096 common keys,
verifies exact sums/counts and mode metrics in all five modes using the same live
workers, then checks producer failure with waiting finals, subsequent worker reuse,
worker loss and parent-root removal. Hash and JobId/deadline regressions remain.

The initial sandbox denied localhost bind (`Operation not permitted`); focused
network tests were rerun with approved localhost access and passed. Development
compiler/lint failures were fixed before the final gates. No failed invocation is
represented as passing.

Final gate results:

| Command | Result |
|---|---|
| `cargo fmt --all --check` | Passed. |
| `cargo check --workspace --all-targets --locked --offline` | Passed (1m 44s). |
| `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` | Passed. |
| `cargo test --workspace --all-features --locked --offline` | Passed: 62 tests plus doc-test targets. |
| `cargo test -p ade-distributed --locked --offline` | Final focused refresh passed: 16 unit tests plus process matrix, including the subsequently added explicit duplicate-transfer regression. 63 distinct tests across the final suites. |
| `cargo build --workspace --locked --offline` | Passed (10m 16s including build-lock wait). Apple linker reported an oversized debug unwind section warning; exit status zero. |
| `bash scripts/smoke.sh` | Passed: local/DataFusion synthetic cases, spill/prefetch, artifact checks, comparison and independently validated default distributed mode. |
| `cargo build --release --workspace --locked --offline` | Passed (12m 30s). |
| `git diff --check` | Passed; final staged files also checked. |

The full workspace gate preceded a small test-helper extraction, an added
explicit duplicate-transfer test and effective-backend metadata labeling; the
final focused tests, workspace Clippy and a final focused distributed Clippy check
covered the updated source. The explicit
duplicate test sends just a header, so it verifies the structured server rejection
without depending on whether a TCP reset supersedes an ACK with unread data.
No runtime behavior was relaxed to satisfy a test.

## Release acceptance

`bash scripts/phase4-acceptance.sh` reproduces one compact release matrix, writes
ignored artifacts under `benchmarks/results/phase4-acceptance-20261016/`, and keeps
inputs under the matching ignored datasets directory. Fresh paths can be supplied
via `ADE_PHASE4_DATA`/`ADE_PHASE4_RESULTS`. It generates 100,000 rows in four files,
10,000 possible keys, seed 42, uniform distribution, 8192-row groups. Keys cross
producer boundaries and the hash edge has multiple segments per transfer.

Each topology/mode gets independent-oracle validation, one warmup and three
measured isolated children with full data verification, followed by check-result.
DataFusion validation is an additional reference. Static buffer is 1 MiB except
spill's 1-byte threshold, which guarantees disk use. Local task memory is 2 MiB,
prefetch depth one. Result schema and oracle ownership remain unchanged.

All seven cases passed. Every standalone validation, warmup and measured child
returned 10,000 groups with the same DataFusion-reference fingerprint:

`f89c88f70119ffaed5780864029c66eac3d2b3701247fa3fddb3a434109b9c82`.

There were 28 isolated children (seven cases × one warmup plus three measured
iterations), all oracle validated; all seven artifacts passed `check-result`.
The final script verifier was also run directly against the stored evidence to
check every child's fingerprint and the explicit local object-backend metadata.

| Workers | Mode | Transfers / segments | Partial rows | Payload bytes | Max worker buffer peak | Retention evidence | Median query ms |
|---|---|---:|---:|---:|---:|---|---:|
| 2 | buffered | 4 / 32 | 19,877 | 508,544 | 174,488 | No retained disk/object bytes | 61.818 |
| 2 | direct | 4 / 32 | 19,877 | 508,544 | 17,224 | No retained disk/object bytes | 103.963 |
| 2 | local_materialized | 4 / 32 | 19,877 | 508,544 | 0 | 4 files / 501,152 bytes | 65.461 |
| 2 | object_store | 4 / 32 | 19,877 | 508,544 | 0 | 32 objects / 508,544 write = read bytes | 68.201 |
| 2 | spill | 4 / 32 | 19,877 | 508,544 | 0 | 508,544 write = read bytes | 59.164 |
| 4 | direct | 16 / 64 | 36,786 | 946,432 | 16,072 | No retained disk/object bytes | 102.230 |
| 4 | object_store | 16 / 64 | 36,786 | 946,432 | 0 | 64 objects / 946,432 write = read bytes | 75.102 |

Peak frame sizes were 17,224 bytes (2 workers) and 16,072 bytes (4 workers),
below 1 MiB. All sender/receiver transfer, segment, row and payload totals matched;
final-input rows matched accepted partial rows. Gather returned exactly 10,000
groups. Mechanism checks explicitly prohibit fallback to local materialization.

Environment: Apple M3, eight physical/logical cores, 24 GiB RAM, macOS aarch64,
kernel 27.0.0; Rust 1.99.0, aarch64-apple-darwin, system allocator, opt-level=3,
thin LTO, one codegen unit, debug-info=1, assertions disabled. Build-bound source
provenance records baseline `2fa71e3...` with dirty=true, because acceptance was
collected before the single implementation commit. Process startup is setup time;
coordinator elapsed includes its cleanup; coordinator RSS excludes worker RSS.

Timings are exploratory local evidence only, reported as three-iteration medians.
No speedup or cross-mode performance conclusion is claimed. Full distributions,
environment, effective configuration, manifest identity and raw per-worker metrics
remain in the ignored artifacts. No cloud or remote CI execution is claimed.

## Acceptance audit and known limitations

| Plan requirement | Evidence |
|---|---|
| Same hash semantics under five static modes | Hash vectors unchanged; exact process matrix; seven release cases and DataFusion reference share fingerprint. |
| Common producer/consumer boundary | Worker uses Sender and BatchSource; same spillable merge/drive; existing IPC scan tests retained. |
| Segments and completeness | Empty, stale, duplicate, gapped, incorrect count, partial, oversized and wrong-version regressions; full identity ACK. |
| Producer/final overlap | Direct mode succeeds in real processes with final-first dispatch; at most one active/started role; producer error unblocks finals. |
| Cancellation and worker reuse | Blocked writer/reader, receiver drop, source failure, monotonic JobId, process failure and subsequent prepare tests. |
| Memory discipline | Capacity cancellation test; finite FIFO/spool metadata; forced-spill exact-once read/truncate; release retained peaks within limits. |
| Local materialization baseline | Finalized expected transfer files; default smoke; two-worker release file count/bytes. |
| Object-store semantics | Public locked API, identity keys, incomplete-object invisibility, backend error propagation, bounded head/range; 32/64 objects with equal write/read bytes; explicit local backend metadata. |
| Truthful metrics | Coordinator conservation before success; separate retention/network/local task metrics; result schema v1 unchanged. |
| Validation and scope | Full gate plus final focused refresh, smoke, all five two-worker and two four-worker release cases; ADR/report/README/AGENTS; Phase 5 stop. |

Phase 4 ends at static mechanisms. No adaptive policy, lineage, recomputation,
retry/recovery, dynamic workers, skew repartition, joins/SQL or streaming/state.
Private exchange wire compatibility intentionally changes; all workers must use
the same build. Deployment remains localhost multi-process with fixed hash
partitions. Local aggregate spill can still fail explicitly on no-progress
partitions. Filesystem capacity/latency and OS scheduling are not hard bounded by
ADE; cleanup after filesystem failure is best effort. Object segment request
counts may be expensive for cloud providers; no cloud performance or durability
claim was measured. Consumer polling and rendezvous overhead have no speedup claim.

## Independent review remediation

The attached Phase 4 review identified five findings. The implementation and
verification updates below address them without expanding Phase 4 scope:

| Finding | Resolution |
|---|---|
| Spill filesystem I/O held the retention mutex | A private spill I/O owner now performs log reads/writes outside the shared queue/accounting lock. Waiters poll cancellation and enforce a five-second operation deadline. If the underlying operation remains in flight, the session fails and the worker is poisoned; the task/receiver can return without joining the helper. Failure-injection tests cover stalled writes, reads, and receiver shutdown. |
| Object-store `put`/`get` could outlive cancellation | Provider operations race cancellation and a five-second timeout. A timed-out operation fails and poisons the worker, and Tokio runtime teardown does not wait indefinitely for blocking helpers. Injected hanging-store tests cover both `put` and `get`. |
| Object backend was not configurable | Added `worker::serve_with_object_store` for embedding callers to inject an `Arc<dyn ObjectStore>`. `ExchangeConfig` names the backend and carries a caller-declared durability capability. The value is reported but is not validated by ADE; the built-in worker still defaults to ephemeral local filesystem storage. |
| Current retained bytes were not exposed | Retention metrics expose live in-memory buffer bytes and spill-backlog payload bytes. Successful completion requires both to drain to zero. |
| Comments still described Phase 3 topology | Updated stale module/topology/overlap comments to describe Phase 4. |

The remediation was checked with the following commands (using the provisioned
offline toolchain described above):

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Passed. |
| `cargo check --workspace --all-targets --locked --offline` | Passed. |
| `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` | Passed. |
| `cargo test --workspace --all-features --locked --offline` | Passed: 69 tests; all doc-test targets passed. |
| `cargo build --release --workspace --locked --offline` | Passed. The Apple linker emitted the existing oversized debug unwind section warning. |
| `bash scripts/phase4-acceptance.sh` | Passed all seven acceptance cases: five modes at two workers, Direct and Object Store at four workers. Each mode shared fingerprint `f89c88f70119ffaed5780864029c66eac3d2b3701247fa3fddb3a434109b9c82`; transfer, segment, row, and payload totals matched. |
| `bash scripts/smoke.sh` | Passed result integrity and smoke execution. Its one-iteration comparator emitted a regression classification with configuration/source mismatch warnings; this is not comparable performance evidence. |
| `git diff --check` | Passed before commit. |

The refreshed release acceptance observed 4 transfers / 32 segments and 19,877
partial rows at two workers (508,544 payload bytes), and 16 transfers / 64
segments and 36,786 partial rows at four workers (946,432 payload bytes). Spill
and object-store read/write byte totals matched. Results and generated data were
kept under `/tmp/ade-phase4-remediation-results` and
`/tmp/ade-phase4-remediation-data`, outside the repository. The only untracked
repository file unrelated to this remediation was preserved and excluded from
the commit.

## Phase 5 handoff

The next phase can consume stable transfer/segment identity, static mode baselines,
retained-buffer peaks, producer/consumer wait observations and spill/materialized/
object read/write cost proxies. Define policy separately, including observation
sampling, bounded decision metadata and deterministic correctness-preserving
transitions. Do not infer recovery from object presence or a process RSS limit
from the local ledger. Phase 5 requires a new explicit instruction.
