> Historical foundation-only report from `fcffd77b312fda66994051f9f0608ad95674c473`.
> References below to the final source or missing work describe that initial snapshot,
> not the completed continuation. See `phase-12-implementation-report.md` for current state.

# Phase 12 implementation report — durable operations foundation

Status: **PARTIAL**. Date: 2026-10-28 (America/Los_Angeles).
Neither distributed target executes through operations. This is not COMPLETE
bounded v1 or production HA. Full PRD Phase 12 remains open.

## Source and authorization

Base HEAD: `fb82b9b454e918f11dcac0776139beb293056acf`, matching the attached
prompt's reference. Initial dirty state: untracked supplied
`docs/plans/phase-12-implementation-plan.md` and unrelated
`sol-medium-phase-0-bootstrap-implementation-prompt.md`. The supplied plan is
included with explicit implementation reconciliation; the bootstrap prompt is
untouched and excluded from the commit. The user's request authorizes Phase 12,
one final local commit and no subagents. No push/PR/remote changes or delegation.
The final source is the single commit containing this report; obtain its exact
SHA with `git log -1 --format=%H`. Handoff supplies that SHA separately so the
report does not require a self-referential commit hash.

Environment: aarch64-apple-darwin, rustc 1.99.1 (58cf8f9c3 2026-10-13),
cargo 1.99.1 (5f94df478 2026-08-27), stable toolchain, debug builds, cached locked
dependencies, local filesystem/localhost. New `fault-injection` feature is off
by default; all-feature tests enable deterministic pause/witness fixtures.
Dependency lock adds ade-ops and UUID's serde feature, with no upstream version
updates. Native libraries have no operations/frontend dependency added.

Changed scope: root workspace/lock, new `crates/ade-ops` (library, `ade-batch`,
store/recipe/publication and tests), AGENTS/README, ADR index/ADR-0019, supplied
Phase 12 plan, operations guide and this report/machine-readable unsupported
matrix. Native runtime, physical IR, benchmark adapters/oracle and CI workflow
are unchanged. Existing workspace all-feature CI automatically includes the new
correctness/fault tests. No native hot-path instrumentation or timing gate added.

## Implemented and refused contract

| Surface | Implemented | Refused / remaining |
|---|---|---|
| Targets | Validate/persist IR v1 static aggregate and bounded join recipes; exact native manifest/schema/hash checks | **Both execution targets**; run returns UNSUPPORTED with no worker contacts/engine IDs/output |
| Store | Private local root; OS exclusive lock; versioned checksummed finite snapshot; atomic replacement/file+directory sync; checked owner generation; idempotency | SQLite/migrations, rollback/backup restore, cross-host owners, history reclamation |
| Lifecycle | PENDING registration; queued CANCELLED; deadline FAILED; ambiguous metadata states NEEDS_OPERATOR; bounded structured event ring | Live native RUNNING/STAGED/COMMITTING/SUCCEEDED path, integrated attempt history and safe engine-ID mapping |
| Restart | Validate committed snapshot, remove only safe abandoned temp, retain original deadline; never reset in-flight to pending | Worker boot/session verification, drain/fence handshake, automatic whole-job replay, retries/backoff |
| Sources | Exact IR/registered manifest match; source/schema/row/hash validation; pinned dev/inode/mtime/size/hash; repeat checks | Transactional isolation against malicious change-and-restore; remote/cloud inputs |
| Publication | Standalone streaming IPC primitive; exact schema, checked rows/serialized bytes, structural complete terminal set, reread/hash, synced hard-link no-clobber marker | Native terminal proof/source postflight integration, marker-to-job SUCCEEDED reconciliation, job verify-output, active commit/cancel serialization |
| Cancel | Durable idempotent queued cancel; intent recorded on ambiguous metadata without pretending it drained | Active hashing/gather/native task cancellation and proof of quiescence |
| Operations | init/submit/status/list/history/cancel/recover/verify-sources/cleanup/capabilities; stable error codes and JSON version | Output GC, live service/status/cancel concurrency, operator-resolved replay/new cohort |
| Quotas | Finite metadata/input-admission bounds and standalone IPC per-file row/byte bound | Every native runtime write, aggregate staging/debris, spill/exchange/object volume, disk capacity, RSS |

The machine-readable matrix is [phase-12-unsupported.json](phase-12-unsupported.json).
Registration is useful for validating/pinning jobs and evaluating durable operator
metadata, but does not imply execution admission. A primitive completion assertion
is supplied by its caller; it is never evidence of a successfully executed native
job. The store actively refuses a stored SUCCEEDED state in this partial version.
No engine JobId or namespace is dispatched. Feature-only metadata claims have
attempt ordinal 1 plus UUID, explicitly without an engine mapping.

## Source-to-requirement map and decisions

| Requirement / actual source | Resolution |
|---|---|
| PRD §16 Phase 12; architecture §§17,22,27–28 | ADR-0019, reconciled plan and operations guide retain the complete goal and list unimplemented gates |
| `ade-distributed/src/physical.rs` | Existing adapter returns bounded Vec batches; not reused as a pretend streaming durable sink |
| `coordinator.rs`, `gather.rs` | Native per-partition commit retained; local-materialized gather writes lack operations disk quota; execution refuses |
| `join.rs` | Native bounded policy/spill/grace and no-recovery boundary unchanged; operations join execution refuses |
| `worker.rs`, `protocol.rs`, `recovery.rs`, `ids.rs` | CancelJob can drop session ownership while task/retention owners survive; no persistent boot/drain proof; no protocol change or unsafe retry |
| `ade-frontend/src/catalog.rs`, `ade-exec/src/physical.rs` | Reused for full source/manifest/schema binding; ops adds filesystem identity/hash pinning before/after admission |
| `ade-frontend/src/execution.rs` | LocalCluster is temporary parent-owned state, not adopted across coordinator restart |
| `ade-physical/src/lib.rs` | IR1/source/plan/output admission unchanged; native contracts remain frontend-independent |
| `ade-ops/src/store.rs` | Real durable bounded metadata ownership/CAS, cancellation, operator-required reconciliation |
| `ade-ops/src/publication.rs` | Independent local publication mechanism, deliberately unable to mark a durable job successful |

ADR-0019 accepts a small checksummed snapshot store in place of SQLite/WAL,
with copy-on-write transactions under a process-enforced lock. It accepts only
the implemented foundation, not closure of WP2–5. It refines architecture
§§17/22/27/28 and INV1/4/5/7/8 without superseding an invariant. CAS validates
expected state/owner/attempt/sequence and refuses terminal transitions or stale
observations. Snapshots cap serialized metadata at 8MiB; bounded construction
holds additional copies. Missing/incompatible/corrupt state never initializes.
Any ambiguous filesystem transaction poisons the handle until explicit reopen.

The publication primitive constructs all paths under one private local root,
so stage/marker cannot cross filesystems. It writes exact-schema Arrow IPC,
checks conservation against a structurally complete fixed 2–4 terminal set,
re-reads decoded schema/rows plus BLAKE3, syncs file/directory, writes/syncs a small
manifest, and calls hard_link to canonical COMMITTED.json. The marker is never
overwritten. Atomic link is the visibility point; directory sync follows.
After-link errors require verification, never blind replay/deletion. A failed
append permanently aborts its stage, preventing later publication of a prefix.
Cancellation/expiry observed at a cooperative check prevents publication. The
final check-to-link race is not serialized with a concurrent token change; a
future coordinator must order durable cancellation and publication. A token
cancelled after link does not erase the marker. Durable native commit-vs-cancel
integration remains unimplemented. Successful standalone verification cannot mutate durable job state.

## Validation evidence

All required quality commands passed on the final working source:

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | PASS |
| `cargo check --workspace --all-targets --locked --offline` | PASS |
| `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` | PASS |
| `cargo build --workspace --all-features --locked --offline` | PASS; pre-existing macOS large unwind-table linker warning |
| `cargo test --workspace --all-features --locked --offline` | PASS: 214 tests, 0 failures, 1 existing ignored local timing diagnostic; doc tests pass |
| `cargo test -p ade-ops --all-features --locked --offline -- --nocapture` | PASS: 20 tests with final-source reached-stage witnesses |
| `bash scripts/smoke.sh` | PASS: independent native/oracle correctness; one-sample comparison classifies as regression, not a timing acceptance result |
| Python evidence verifier unittest discovery | PASS: 20 tests |
| `git diff --check` | PASS |

The matching all-feature worker was built before workspace/native tests. Those
include 16 static process/recovery/retention/topology tests, 6 leased scheduler
correctness tests, 3 join process tests, and all 11 Phase 11 frontend/oracle/
output-cap tests. They are compatibility evidence, not Phase 12 native operations
acceptance. No CI timing threshold, release campaign, production HA or MSRV1.94
execution is claimed; the locally exercised compiler is 1.99.1.

Raw command logs, initial failures, command exit statuses/log hashes, final binary
hashes and a 94-file SHA256 source inventory stay in ignored
`benchmarks/results/phase12-quality/`. The inventory digest (canonical sorted
JSON of relative file paths to SHA256, compact separators) is
`e66bc540538883cb78658a52bde13c65ca1318b5bda53f9722214e22d382adfe`.
It covers every Rust source/Cargo manifest, lockfile, toolchain file, CI and smoke
script; final source comparison passes. Tests ran the dirty working tree over
the base SHA, subsequently committed without changing those inputs. Captured
binaries retain their build-time base/dirty provenance; final Git identity does
not relabel a historical binary or performance experiment. The focused and
workspace dependency feature graphs differ, and the final binary inventory is
the final capture, not a claim that every command used byte-identical executables.
The small committed [evidence summary](phase-12-evidence.json) records scope,
counts, source digest and fixture digests. No large generated artifacts enter Git.

The implemented foundation's final 20 tests include the CLI roundtrip, bounded
recipe reads and 1024-byte error-message cap. Real process witnesses confirm SIGKILL signal 9
at `snapshot_synced`, `snapshot_renamed`, `claimed`, `output_synced`,
`manifest_synced`, `marker_published`, `marker_durable`. Snapshot tests inspect
old/new on-disk generations; concurrent owners refuse while the child holds its
lock. Claim recovery repeatedly preserves the same attempt/namespace and moves
to NEEDS_OPERATOR, with no replay. Publication tests inspect the **external
COMMITTED.json**, require its absence before link and validate all referenced
IPC after link; duplicate publication preserves the marker bytes.

The fixed empty IPC publication fixture has 200 serialized bytes, data BLAKE3
`ea568db00b61cf279712b667bb42e139832f084212e63d5abe6b1de015e1290c`
and committed marker BLAKE3
`47be5f821bf9225c681dc08fc41f82addd925f2777c6600cca73124847e091ef`.
The two after-link crash points verify the same manifest repeatedly. A separate
signed IPC test reads values [-9,4,7] independently and checks exact schema.
These are **primitive fixtures**, not DataFusion/native query result evidence.

Tests cover metadata corruption/missing/version/checksum/unknown fields,
process/same-process owner contention, stale CAS owner/attempt/state/sequence,
all 121 lifecycle state pairs, overflow, expiry across reopen, write-failure
poisoning, exact event ring and queue/history boundary, idempotency conflicts,
source hash/manifest mutation, symlink and same-content rename replacement,
both recipe targets refusing without accepting a single worker connection,
output schema/row/actual IPC byte cap/cap+1, counted I/O failure/counter overflow,
duplicate/incomplete/overflow terminal sets, cancelled/expired stages, marker
collision, foreign identity, reference traversal, truncation/mutation/hard links,
private permissions, empty IPC and repeated marker verification.

Initial development failures are preserved in `ade-ops-test*.log` and Clippy
logs: the first test run lacked sandbox loopback access; a rename negative
incorrectly expected *new* registration to reject identical contents (corrected:
the old pinned job still rejects inode replacement); standalone fixtures needed
explicit mode0700 directories. The first publisher kill did **not** reach its
intended stage and is retained as a failed fixture, not accepted fault evidence.
Reruns pass only after permissions and witnesses were corrected. Clippy style
failures were fixed. No native acceptance criterion or timing threshold changed.

### Unexercised Phase 12 gates

Native coordinator SIGKILL at prepare/producer/gather/retry wait; actual old worker
poison/drain/registry mismatch and replay; delayed/stale frames with durable owner
fencing; native source deadline/cancel during execution/postflight; lost final
DB success update reconciled from a valid marker; engine retries/backoff/deadline;
native output independent aggregate/join oracle under restart; active cancel and
commit race; runtime-write disk quota and total staging exhaustion; injected
fsync/hard-link/EXDEV errors and real power-cut durability remain **NOT RUN /
UNIMPLEMENTED**. Same-directory construction prevents arbitrary cross-device
publication paths, but does not substitute for an injected EXDEV test. Invalid
completion tests are structural, not actual duplicate late gather tests. The
real claimed metadata test is not a native prepare-stage test.

## Quota accounting, trust and remaining PRD gaps

Strict covered domains: 32 retained jobs, 8 pending recipes, one metadata claim,
32 retained events/job (checked monotonic sequence), 128-byte request keys,
512KiB recipes, 8MiB snapshots, existing IR256KiB/64nodes and source64files/
1m rows/64MiB per table plus 1MiB manifests, and standalone output up to 1m rows/
64MiB actual serialized IPC per attempt with <=8192-row batches/64KiB marker.
Tests use small exact boundaries for jobs/queue/events and IPC actual bytes;
not every global maximum has a cap/cap+1 test. Source admission bounds are inherited
from the tested native validators. No total retained-store disk bytes, metadata
serialization allocations, caller Arrow batches, codec/Parquet transients,
exchange/kernel buffers, runtime spill, number of standalone attempts/debris,
process RSS, physical disk capacity or cloud durability quota is claimed.
Runtime execution refuses because native write coverage is incomplete.

Assumptions: trusted single Unix operator, private roots, same-host local
filesystem honoring fsync/atomic rename/hard links; no malicious same-user
change-and-restore, no clock rollback. Hashing loops use fixed monotonic deadlines;
blocking syscalls are not preemptible. Job expiry starts before source admission;
CLI recipe reading/parsing and shape validation precede that timestamp. A complete
native workflow must propagate one deadline from initial admission through teardown. Snapshot history and owner generations
must never be rolled back from backup. Unknown state stays closed. The lock file
must never be deleted/replaced. No automatic worker replacement or adoption of
LocalCluster handles. Garbage cleanup deletes only an abandoned safe state.next,
preserving all committed/unknown files and history. Output GC is not implemented.

Full PRD Phase 12 still lacks native durable coordinator execution/recovery,
worker session fencing, safe engine instance allocation, retries/backoff,
native terminal-validated output commit/reconciliation, active cancellation,
complete runtime quotas, richer job/attempt history and the full fault suite.
Cross-host HA/replication, automatic membership/replacement, external sinks,
auth/fairness, cloud durability and streaming exactly-once also remain deferred.
Phase 10 physical membership debt remains PARTIAL. All original failed candidates
and source-bound ee9f8da/b4a2a70 local timing evidence retain their identities;
no historical campaign is rerun, renamed or promoted here. Local correctness
and process-kill evidence does not establish power-loss or cross-host reliability.

## Reproduction and next implementation step

See [operations guide](../phase-12-operations-guide.md) for clean-checkout
commands, exact recipe format, JSON errors and operator quarantine/reopen steps.
Deterministic fixtures are in `crates/ade-ops/tests/support/mod.rs`; process
witnesses/assertions are in `tests/process_crash.rs`; no persistent benchmark
fixtures are required. Build a matching all-feature worker before legacy tests:

```bash
cargo build --workspace --all-features --locked --offline
cargo test -p ade-ops --all-features --locked --offline -- --nocapture
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo test --workspace --all-features --locked --offline
bash scripts/smoke.sh
```

Next implement WP2 persistent fixed-worker boot/session fencing/drain and durable
engine-ID allocation, instrument **every** admitted native write, and expose a
streaming native sink with terminal proof. Then connect store/publisher and run
all open Phase 12 gates, including safe replay and native independent oracles.
The remaining work cannot be certified by the foundation's passing tests.
