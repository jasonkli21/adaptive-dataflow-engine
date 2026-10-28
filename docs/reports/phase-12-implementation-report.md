# Phase 12 implementation report — batch reliability and operations

Result: **COMPLETE (bounded v1)**, 2026-10-28. Both static distributed aggregate and
bounded HashOnly inner join execute through durable local operations. This is
single-active-coordinator, same-host, fixed2–4 supervised workers with local atomic
publication. **Full PRD production HA remains incomplete:** replicated metadata,
cross-host failover, worker replacement, general/external sinks, multi-tenant
operations and streaming are deferred. Phase10 remains PARTIAL; no Phase13 begins.

## Identity and source reality

Starting source `fb82b9b454e918f11dcac0776139beb293056acf`; initial untracked files
were the supplied Phase12 plan and unrelated bootstrap prompt. The plan is included;
`sol-medium-phase-0-bootstrap-implementation-prompt.md` remains untouched/untracked.
The user's Phase12 authorization supersedes AGENTS' historical prohibition. No
subagents, push, PR or remote mutation. The initial foundation commit `fcffd77b`
was prematurely stopped and correctly labeled PARTIAL. The user requested
completion; the final single local Phase12 commit folds in that continuation.
Its full SHA is supplied in the handoff (`git log -1 --format=%H`), avoiding a
self-referential document hash. The preserved [foundation report](phase-12-foundation-report.md)
and source archive keep its earlier evidence distinct.

Toolchain: stable rustc1.99.1 (58cf8f9c3 2026-10-13), cargo1.99.1,
aarch64-apple-darwin/macOS27, Apple M3/8cores/24GiB, debug profile, locked cached
dependencies. `fault-injection`/native `research-hooks` are off by default.
All-feature process tests build matching worker executables first. CI adds that
explicit build before workspace tests, remains correctness-only; CI itself and
MSRV1.94 were not executed locally. Dependencies add only ade-ops, existing UUID
serde support and existing BLAKE3 as native build dependency; no version upgrade.

## Supported/refused matrix

| Surface | Executable bounded contract | Refused/deferred |
|---|---|---|
| Aggregate | IR1 static fixed-hash, static topology, Buffered exchange, native partial/final aggregate,32MiB task limit, operator spill disabled | Leased/static_projected/adaptive topology/cost/object modes and operator spill through operations |
| Join | IR1 original two-source inner equijoin, bag semantics, HashOnly, operator spill disabled, byte-budgeted input spill exchange | Grace/operator spill, in-place join recovery, arbitrary DAG/SQL |
| Workers | Fixed2–4 externally supervised loopback endpoints, persistent private scratch/store watermark, boot/runtime/feature/session fences | Adoption of temporary LocalCluster handles, live replacement/rebinding, changed boot or unknown registry |
| Recovery | Same-root coordinator restart, marker reconciliation or proved drain followed by staged resume/fresh instance | Dead/poisoned/unproved worker sessions: NEEDS_OPERATOR, never blind retry |
| Retry |1–3 total attempts, transient native I/O/refusal only,200/400ms backoff, original expiry, unique instance namespace | Integrity/quota/permanent failures; producer-only/partial lineage/leased replay |
| Output | Native streaming terminal proof, exact schema/caps/hash, one immutable same-filesystem no-clobber IPC manifest | Partial visible output, overwrite publication, arbitrary external sinks |
| Operations | Register/run/recover/status/list/history/cancel/verify-sources/verify-output/terminal GC | Multi-tenant fairness/auth, daemon/remote dashboard, history reclamation/reset/backup rollback |

Memory pressure is a supported failure outcome, not a fallback into an uncounted
spill mode. Large native Arrow-accounted output may fail before the serialized
IPC limit; the domains differ. Existing frontend/benchmark defaults retain their
own broader native contracts. The [machine-readable matrix](phase-12-unsupported.json)
and CLI capabilities agree with this table.

## Architecture and requirement mapping

[ADR-0019](../adr/0019-durable-batch-operations.md) reconciles the PRD, architecture
§§17/22/27–28 and INV1/4/5/7/8, plan and implementation. The original outcome is
preserved, not lowered to accept an aggregate-only happy path.

| Requirement | Actual implementation/evidence |
|---|---|
| Durable finite lifecycle/history | `ade-ops/store.rs`: checksummed snapshot v2, atomic durable replacement, short OS metadata lock, checked counters, bounded events/attempts, v1 checksum-valid migration; all-state CAS/stale/overflow/corruption tests |
| Exclusive coordinator and immutable identity | `runner.rs`: separate runner.lock, durable claim before native effects; source/plan/config/worker pin, source-runtime digest; deployment UUID + unique execution UUID + deployment-local numeric JobId mapping |
| Managed worker proof/fence | native `managed.rs`, `worker_managed.rs`: ADEOPS01, persistent store/highwater journal, boot/scratch/endpoint/feature/runtime checks, call gate and drain proof including detached retention ownership |
| Native aggregate and join | `managed::execute_to_sink_observed`: existing prepare/task/conservation/final checks and physical adaptation; opaque native completion proof; no frontend execution fallback |
| Streaming checked sink | `gather.rs`, `publication.rs`: exact final set, sender/receiver terminal verification, incremental IPC, persisted candidate, no-clobber link and exact-marker reconciliation |
| Cancel/retry/deadline | `runner.rs`: durable intent + monitor/token, finite retry wait, global persisted expiry, proved drain before fresh attempt; running/hash/staged/backoff/late-marker tests |
| Managed runtime writes | `disk.rs`, `exchange.rs`, `retention.rs`, `join.rs`: shared cumulative reservations for all admitted exchange/gather files, including failed writes; disabled operator spill; real quota poisoning/no-marker tests |
| Operator cleanup | `runner::gc`: terminal-only, recorded outputs/private UUID namespaces, bounded walk, no links/hard links, committed reference protection, dry run/apply; crash scratch cleanup tests |
| Compatibility | Entire workspace, legacy native recovery/retention/topology/scheduler/join/frontend/benchmark tests and smoke independently exercise the old APIs |

Global execution identity is **not a globally unique u64**: the UUID namespace plus
deployment makes instances distinct, while checked deployment-local numeric IDs
and persistent worker store binding prevent accepted reuse. Neither counter nor
history can be reset/reclaimed. A fresh whole-job instance is distinct from a
Phase6 transfer attempt. Join IR `replayable:false` still forbids **in-place lineage
replay**; a fresh whole-job instance independently reads pinned immutable sources.
No join state or partial producer lineage is adopted. Aggregate snapshot morsels
are the actual manifest-listed files with original hashes/bytes, not invented
row ranges. A coordinator loss closes the entire old execution through proved
drain; fresh execution restarts all files, with no file-morsel lease adoption. Managed aggregate disables
in-place recovery; legacy Phase6 final-only recovery remains unchanged.

The native runtime source digest covers Cargo.lock/workspace and native manifests,
build script and distributed/exec/physical Rust sources. Registration pins its
concrete config hash; worker Hello verifies the same runtime/feature identity.
Scratch/boot/endpoint drift refuses. Config mismatch and counter overflow tests
assert refusal before an engine mapping, output root or worker side effect.

## Durability and visibility

Every transaction copies bounded metadata, writes/syncs state.next, replaces the
committed state.json and syncs the directory. Missing/corrupt/unknown state never
initializes silently; an ambiguous transaction poisons that handle until reopen.
V1 valid snapshots migrate without reissuing identity; legacy unmapped in-flight
claims require an operator. runner.lock spans foreground coordinator work, while
short metadata locks permit status/cancel during native work and hashing.

Drain closes the same gate as native call admission, cancels repeatedly and
requires zero calls, no installed job/join and exclusive ownership of the common
budget token. Stores/writers/detached spool threads hold that token until file
cleanup has ended. This addresses the older CancelJob ownership gap. Dead sockets,
ACKs, boot changes and poisoned workers never substitute for quiescence proof.
Recovery first checks publication; otherwise it drains before retry or staged
resume. A complete candidate can publish after source revalidation without
reexecuting native work. Retry reserves a new numeric ID/owner/UUID before effects.

Data is staged privately, checked against exact output schema, rows, actual IPC
bytes, native terminal set and hashes, then synced with its immutable manifest.
The candidate is durably stored before COMMITTING. Under the metadata lock, final
cancel/expiry check, COMMITTING and hard-link publication serialize. Atomic
no-replace COMMITTED.json creation is visibility's linearization point. A late
cancel cannot erase success. Directory sync completes durability; post-link
errors require exact marker/data/candidate verification and another directory
sync before SUCCEEDED. Corrupt/foreign markers freeze without deletion/replay.
Standalone publisher caller assertions cannot create durable native job success.

Quotas are separate: the stage cap includes IPC+manifest; native runtime budgets
cover exchange/gather; metadata is independently bounded. Rejected stages remain
private debt until controlled GC, never successful partial output. All output
reads use the canonical marker and its referenced file. GC protects those refs.

## Validation and real fault evidence

Final command outcomes and counts are in [phase-12-evidence.json](phase-12-evidence.json).
Raw logs/status hashes/source inventories and all measured samples are ignored at
`benchmarks/results/phase12-completion/`; large fixtures/binaries stay out of Git.
The source inventories bind every Rust source, manifest/lock, toolchain,
CI and smoke script; its digest and verified command statuses are in that summary.
The workspace inventory is SHA256
`49f4a46d811ec89433b0fca588259065a12ecf4b549448caa3f9da54558b67c8` (102files).
A final test-only refinement adds exact canonical result assertions after each
crash recovery, changing only `tests/native_crash.rs` and shared test support.
The final inventory is
`ed8c79bcaeabe8e28ea1801ca2bc7e80f8e0c3772aecbc6c2301f06adc7ca259` (102files).
A follow-up reruns fmt/check/strict Clippy and all10 native fault tests, proves
all production sources identical to the passing workspace gate, and verifies the
final inventory stayed unchanged. The initial pipeline's last inventory assertion
correctly noticed that deliberate test-only refinement; every individual command
had exited0. Both inventories/logs and the explicit production-equality proof are
retained, rather than pretending that assertion passed. The final commit adds
only documentation/evidence after this follow-up. Build-time dirty Git metadata
is not relabeled as a clean historical runtime.

Required checks: fmt, all-target workspace check, all-feature strict Clippy,
feature-matched workspace build, all-feature workspace tests, focused operations
fault suite, scripts/smoke.sh, Python verifier unit tests, and git diff --check.
Results: **238 workspace tests passed,0 failed,1 existing ignored**; the focused
operations suite passed39 tests. The final strengthened10-test native recovery
suite also passed, with canonical value/bag digests at all14 per-shape coordinator
kill points. Python passed20 tests. Format/check/Clippy/build/smoke/diff checks
passed. The macOS linker emits the existing oversized unwind-table warning; it is
not a failed check. One pre-existing local timing diagnostic remains intentionally
ignored. This local evidence does not imply a CI run or full old timing campaign.

Native coordinator SIGKILL witnesses cover **both shapes** at durable claim,
completed native prepare, actual sink gather, STAGED, prepublish, immediately
after marker link and before final success metadata. Separate workers pause after
an actual producer output prefix and final gather prefix; the test kills the
coordinator, proves drain of those original tasks, then executes a fresh instance.
A competing coordinator refuses while the first owns runner.lock. Retry-wait kills
resume with new engine IDs/UUIDs and the same original deadline. Repeated restart
preserves exactly one external manifest. Worker death/reboot/poison yield
NEEDS_OPERATOR at attempt1, with no blind retry. Terminal GC removes old coordinator
scratch while all committed hashes still verify.

IO faults are confirmed after native output at output append, data fsync, manifest
fsync, atomic-link call (including injected EXDEV), marker directory sync and final
success metadata write. Pre-link failures expose no marker; post-link failures
reconcile exactly one valid result without reexecution. Corrupt staged data,
manifest candidate, foreign marker and source drift after stage refuse/freeze.
Deadline and durable cancellation tests witness **actual file hash reads** during
preflight/postflight. Running, pending, backoff, staged and late-marker cancellation
are separately exercised. Late cancel preserves marker bytes and SUCCEEDED.

Independent canonical scalar/group/bag oracles read produced IPC, check exact
schema, signed SUM/COUNT, empty/skew/negative values and duplicate-key joins.
Overflow never publishes. Two-/four-worker fixtures compare values, not rowcount
alone; canonical and actual IPC digests are printed in focused evidence. Native
DataFusion oracle compatibility is also rerun through Phase11 tests and smoke.
The 1m-row projected-key join test independently reaches the output row boundary;
its cap-minus-one case fails without a marker. Source tests admit an actual64MiB
Parquet file and1m input rows, then reject one more byte/row. Total native stage
IPC+manifest has exact-cap and cap-minus-one tests. Atomic runtime writer tests
share an exact8-byte budget, reject byte9 and overflow before writing; real native
runtime1-byte exhaustion and poisoned refusal verify integration. Metadata job/
queue/event/recipe/state-size caps, stale CAS/ID/epoch overflow, snapshot migration,
legacy/managed frame/version/size/stale-reply rejection and old duplicate/late
gather tests retain adversarial coverage.

Historical foundation evidence remains at `phase12-quality/` with214 passed tests,
20 operations tests,20 Python tests and source inventory SHA256
`e66bc540538883cb78658a52bde13c65ca1318b5bda53f9722214e22d382adfe`.
It proved metadata/publication primitives only and is not native completion
acceptance. Preserved continuation negatives include strict-Clippy failures,
the initial corrupt-stage recovery test that exposed a missing NEEDS_OPERATOR
transition (fixed), and a 1m-row three-column join fixture that hit the inherited
Arrow-accounted byte cap. The latter remained a real rejected output; the separate
projected fixture tests row limits without weakening the byte guard. Initial
foundation sandbox/readiness/permission failures remain documented in its report.
Final review also fixed GC deleting once-committed data if a SUCCEEDED marker
went missing; the new regression refuses cleanup and preserves/restores the exact
candidate/data for both shapes and worker counts. Two later verification attempts
are retained: the overflow/config test initially passed a noncanonical temporary
path and hit the intended path refusal before its target branch (fixed fixture),
and a parallel worker-reboot test assumed an immediate metadata read could never
be busy (fixed with the runner's bounded Owned retry, preserving all safety
assertions). All failed source identities/samples are retained, never reclassified
as passing.

## Static-path overhead diagnostic

Instrumentation adds optional wrappers/field checks to native paths, so new local
default-static measurements compare the archived foundation source `fcffd77b`
(native code identical to the starting source) with the completion inventory.
Both use debug/all features, the same100k-row/four-file hot-key dataset, independent
validation, one warmup and five process-isolated measured iterations each at2/4
workers. No compiler/tests ran concurrently with timing. The timing inventory is retained
separately; subsequent changes are confined to operations and its tests, with
native/benchmark production sources and runtime identity unchanged. The measured
runtime identity is
`2eb78ca479e60813d2294ef527621ca8546f8d53291a9d8ae1c5f0a2827799ed`. Archive builds correctly
report unknown Git metadata; the immutable source archive and binary/result hashes
supply provenance. Completion binaries report their actual dirty source parent.

| Workers | Before median [min,max] ms | After median [min,max] ms | After/before |
|---|---|---|---|
|2|140.072 [137.955,140.366]|141.608 [139.809,149.306]|1.0110|
|4|96.088 [93.339,97.772]|95.327 [94.766,95.664]|0.9921|

All20 measured samples validated to fingerprint
`6b076867709e975ff2fc43fa8c68ca4887502ef9700d585fbbccc1fb7a334063`.
All samples, including149.306ms, are retained in the evidence summary/raw results.
This small sequential same-host diagnostic neither establishes statistical
nonregression nor a performance gain; it has no noisy acceptance threshold.
Phase6–10 original release gates/failures/source identities remain untouched.
No old campaign was rerun or promoted as evidence for this source.

## Quota accounting and remaining limitations

Enforced domains:32 retained jobs,8 pending,1 active native execution,32events/job,
3attempts/job,128-byte key,512KiB recipe,8MiB snapshot (two files transiently),
IR256KiB/64nodes,64files/1m rows/64MiB per source table,1MiB input manifests,
1MiB managed control frames, output1m rows/64MiB actual IPC plus independent
Arrow-accounted cap,64KiB publication manifest and configured total stage cap.
Exact tests use both configured small boundaries and the large row/source limits;
not every global numeric maximum is separately materialized in integration.

Runtime quota is cumulative attempted serialized exchange/gather bytes per worker
and coordinator per attempt, including failed/partial writes. It does not refund
uncertain reservations. Whole-job upper bound is max_attempts*(N+1)*runtime_bytes;
coordinator crash remnants stay inside that finite allowance until GC. Bounded
worker journals, lock files, filenames/directories and snapshot bytes are separate
metadata domains. History cannot be evicted to admit an unbounded stream. Failure
resource counters may be unknown/null. There is no process RSS, native transient,
physical free-space, SSD amplification, filesystem-allocation or cloud quota.
Producer/final budgets can overlap. Output verification/metadata serialization and
codec/Arrow/Parquet allocations are bounded by admitted inputs, not a process cap.

Assume trusted single Unix user/private roots, immutable sources, nonrollback
metadata/watermarks, local filesystem honoring fsync/atomic link/rename, stable
wall clock and unchanged fixed worker registry. Same-user malicious change-and-
restore is outside the trust model. Backward clocks refuse; monotonic execution
checks share the original expiry. OS syscalls cannot be preempted; blocked I/O may
delay cancellation/teardown beyond deadline. Real fsync/media power-cut, actual
cross-device mounts, cross-host repeatability and production HA were not tested;
EXDEV/fsync failures are injected at the real publication boundary. Restoring
snapshots or deleting locks to reset epochs is unsupported. No automatic adoption
or worker replacement after NEEDS_OPERATOR, no operator spill/grace in operations,
no generalized recovery/streaming/external exactly-once, and no arbitrary SQL.

## Reproduction and operator recovery

The [operations guide](../phase-12-operations-guide.md) supplies matching builds,
worker registration, recipe format, commands and quarantine instructions. Fixtures
are self-contained in `crates/ade-ops/tests/support/mod.rs`; no retained dataset is
needed for tests. Build all-feature workers before workspace/focused process tests.
After coordinator loss keep the same root/workers/files, run `recover` or `run`.
A valid marker reconciles; otherwise old workers must prove drain. Preserve
NEEDS_OPERATOR roots, markers, pinned registry and supervisor logs; there is no
force-reset/adopt command. Terminal `gc` is dry-run unless `--apply`, protects
committed output and refuses unsafe namespaces. See evidence JSON for exact
commands/source digest and local raw artifact paths.

## Independent review follow-up (2026-10-28)

The review handoff identified three gaps in the bounded implementation. This
follow-up keeps the accepted Phase 12 boundary and historical measurements
unchanged. The review findings are evidence for these fixes, not new phase
authorization.

- **F12-01, owner-lock contention during hashing:** registration now starts the
  original job expiry before freezing sources, releases `owner.lock` during
  source I/O, then reacquires it and reloads the committed snapshot before
  rechecking idempotency and capacity. `verify-sources` uses the same release,
  reload and validation pattern. The monitor treats `OWNER_BUSY` as a retryable
  observation; its own deadline remains active, and other metadata or drain
  errors fail closed. The real-process regression holds job A at the native
  `prepared` stage while a second process hashes sources for both `submit` and
  `verify-sources`. Each case asserts a reached hash witness and at least five
  seconds in the hashing operation, observes A still `RUNNING` and not cancelled,
  then verifies A succeeds with its canonical output and no cancel request.
- **F12-02, coordinator semantic drift:** every new job pins the build-time
  compatibility digest for `ade-ops` and `ade-frontend`, their manifests, the
  workspace manifest/lock and an explicit compatibility epoch. The native
  runtime pin remains separate. The final source compatibility digest is
  `90103c47b4c0be45332588f2003ff5667d711a9a43304a83eb72f7c1e0934592`.
  Uncommitted jobs with a different or missing pin are not replayed and move to
  `NEEDS_OPERATOR`; the regression also verifies that a valid committed marker
  still reconciles across coordinator drift without changing marker bytes.
- **F12-03, unverified success in list output:** list rows retain persisted
  `state`, add `recorded_state`, and report `output_verification: NOT_CHECKED`.
  Single-job `status` verifies a successful marker before reporting
  `output_verification: VERIFIED`. CLI coverage checks valid, missing and
  corrupt markers.

Validation on the local workspace, based on parent
`4a00192f0736bb77f7adf173a8950cec31563a18`:

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Passed. |
| `cargo check --workspace --all-targets --locked --offline` | Passed. |
| `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` | Passed. |
| `cargo test -p ade-ops --all-features --locked --offline` | 43 passed, 0 failed. |
| `cargo test --workspace --all-features --locked --offline` | 242 passed, 0 failed, 1 intentionally ignored local fixture diagnostic. The successful rerun includes all Phase 12 process tests. |
| `cargo build --workspace --all-features --locked --offline` | Passed. The linker emitted its existing `__eh_frame` compact-unwind size warning. |
| `bash scripts/smoke.sh` | Passed schema/integrity and child validation. Its one-iteration comparison reported a regression while also warning that run configuration/verification policy and engine version/configuration differed and the tree was dirty; this is not performance evidence. |
| `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'` | 20 passed. |

The first full workspace attempt had one failure in
`join_pressure_prefix_safety_refusals_and_cleanup`: its source-drift assertion
received I/O EOF/cancellation diagnostics. The test passed alone immediately
afterward and passed again in the complete workspace rerun without a source
change. This transient failure is retained here rather than omitted. No CI run,
independent-host repeatability, or timing acceptance is claimed. Existing Phase
10 failures, Phase 12 foundation evidence, fixed worker membership, and the
deferred HA/replacement/sink boundaries remain unchanged.
