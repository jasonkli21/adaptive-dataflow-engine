# ADR-0019: bounded durable batch operations

Status: Accepted, 2026-10-28. Phase 12 bounded v1; full production HA is deferred.
Supersedes this ADR's initial foundation-only disposition at `fcffd77b`.

## Context and authority

PRD §16 Phase12 requires metadata, recovery design, retries/backoff, output commit,
cancellation, quotas, history and faults. This refines architecture §§17/22/27–28
and INV1/4/5/7/8 while preserving all invariants. The user's Phase12 request and
continuation supersede the historical prohibition in AGENTS. Phase10 remains
PARTIAL with fixed physical membership and unchanged empirical evidence.

## Decision

`ade-ops` owns a foreground `ade-batch` coordinator, private local snapshot store,
immutable recipes and IPC output publication. It depends on existing native and
frontend libraries; native crates never depend on operations/frontend/DataFusion.
The executable matrix is static fixed-hash aggregate and bounded HashOnly inner
join on externally supervised, fixed 2–4 same-host workers. Operator spill is
disabled in operations. Memory pressure fails instead of silently entering an
unbudgeted spill/grace path. Native default/legacy spill and scheduling remain.

A bounded checksummed snapshot replaces SQLite/WAL. At most32 jobs,8 queued,
32events/job and3 attempts/job permit copy-on-write transactions with an8MiB
serialized cap. A short OS lock serializes metadata; `runner.lock` separately
excludes a second foreground coordinator throughout execution. Write private
`state.next`, sync, replace `state.json`, sync directory. Ambiguous write failures
poison that handle until reopen. Missing/corrupt/unknown state refuses. Snapshot
v2 reads checksum-validated v1 and creates the separate runner lock; legacy
in-flight claims without native mapping require an operator. No backup rollback,
history reclamation or counter reset is supported. Readers do not consume epochs.

Logical job and deployment IDs are nonnil UUIDs. Before worker effects, a durable
claim reserves checked owner and engine counters and a fresh UUID execution
namespace. Global instance identity is the UUID namespace plus deployment, with
monotonic u64 JobId within that deployment. A u64 alone is **not** globally unique.
Worker persistent deployment binding and job/owner watermarks refuse accepted ID
reuse, even after worker boot. All fields remain in bounded attempt history.
Fresh whole-job execution never changes logical IDs within a native instance.

Legacy control stays ADETCP12. Opt-in managed workers exclusively accept
ADEOPS01/version1, bounded1MiB frames and exact reply-fence echoes. Requests bind
store, owner, engine job, UUID execution namespace, worker boot and canonical
persistent scratch path. Hello verifies endpoint, feature and native runtime
source/config identity. The native runtime digest covers native sources/manifests
and workspace lock. Each registered job also pins a separate versioned coordinator
compatibility digest covering `ade-ops`, `ade-frontend`, their manifests, the
workspace lock and an explicit compatibility epoch. A changed coordinator/frontend
implementation refuses uncommitted jobs after safely draining any old attempt.
A checksum-valid v2 job without this newer pin is readable and inspectable, but is
not retried; valid committed output remains verifiable and reconcilable. Different
native source/runtime, coordinator compatibility, registry or boot refuses;
existing uncommitted jobs require matching binaries.
Legacy/managed frames reject each other. Wire2/hash1/IR1/result1 remain unchanged.

Drain closes the same gate used to admit/increment calls, repeatedly cancels the
native session, and requires zero active calls, no installed aggregate/join, and
exclusive ownership of the shared write-budget token. Every store, detached spool
and writer retains that token until its cleanup ends. Token fields drop after
file/directory owners. CancelJob ACK alone, a dead socket, or process identity is
insufficient. Poison/unreachable worker/boot mismatch/drain timeout yields
NEEDS_OPERATOR, never automatic replay. No LocalCluster handles are adopted.

Recovery checks the canonical marker first. Otherwise it drains the old cohort
before staged resumption or a fresh instance. Retry only explicitly transient
native I/O or `transient` replies; permanent integrity/quota/shape failures stop.
A crash with proved drain may retry. Attempts1–3, deterministic200/400ms backoff,
and original persisted wall-clock expiry remain finite across restarts. Native
Phase6 recovery is disabled inside managed instances; its legacy final-only
contract remains. Join's IR nonreplayable flag describes **in-place lineage**:
whole-job restart reads independently pinned immutable local files from scratch;
it does not replay partial join state or flip that flag. This explicitly reconciles
INV8 and the plan's replayability wording. Sources/manifests are rehashed and
filesystem identity/schema/row checks repeated at admission, every preflight,
postflight, staged recovery and immediately before publication.

The new native adapter reuses producer/final/task/conservation checks, streams
committed exact-terminal gather through native physical adaptation, and returns
an opaque completion proof. Aggregate sender/receiver terminal hashes must agree;
join retains its typed terminal/hash and full input/final conservation checks.
The public standalone publisher's caller assertions cannot mark a durable job
successful. Only the integrated native completion becomes a persisted candidate.

Publication checks exact schema, rows, actual IPC bytes, complete native terminals,
data hash and total IPC+manifest staging cap; syncs data and manifest, then
hard-links the immutable manifest to COMMITTED.json with no replacement. Same-root
construction prevents cross-device paths; injected EXDEV refuses. The metadata
lock serializes durable cancel, COMMITTING and the link. The link is visibility's
linearization point: published success wins late cancel. Directory sync completes
durability; post-link errors reconcile by verifying exact persisted candidate,
all referenced bytes, and repeating directory sync before SUCCEEDED. Missing,
foreign or corrupt output never overwrites/replays. Terminal states are immutable.

Cancellation is durable/idempotent and polled through backoff, hashing, native
execution and staging/publication; cancellation/deadline tokens also reach native
tasks. A cancelled terminal requires proved drain. OS syscalls cannot be preempted.
One wall-clock expiry plus monotonic budgets is retained; backward wall clocks
refuse. Registration records its original deadline before source verification,
releases `owner.lock` during bulk hashing, then reloads the committed snapshot and
rechecks idempotency/capacity before persisting. `verify-sources` follows the same
release/reload rule. The monitor treats `OWNER_BUSY` as a retryable observation,
checks its independent monotonic and wall-clock expiry, and fails closed on other
metadata errors. No lock is held across native execution or bulk hashing. The short final
metadata transaction includes the bounded publication syscall, whose OS blocking
behavior remains an explicit limitation.

Managed runtime byte reservations cover all admitted exchange/gather file writes,
including failed writes, per worker and coordinator **per attempt**. Operator spill,
object retention, leased scheduling and in-place recovery refuse. Join's input
exchange uses a byte-budgeted spill log; coordinator gather is byte-budgeted local
materialization. Each token cumulatively reserves write bytes, not live disk usage
or SSD amplification. Output IPC+manifest and metadata have separate budgets.
Finite attempts bound crash debris. Terminal GC walks only recorded private
attempt namespaces, bounds entries/depth/bytes, refuses links and preserves
committed references and unknown output files. These are not RSS, free-space,
Arrow/Parquet transient or filesystem-metadata quotas.

## Evidence and consequences

See the implementation report, operations guide and source-bound evidence summary.
Real process kills cover native prepare/producer/gather/staged/prepublish/link/
post-link/retry waits; strict-prefix witnesses precede kills. Tests verify canonical
values/schema/bag multiplicity, external marker identity, corruption, fences,
write failures, cancellation and quotas. CI is correctness-only and explicitly
builds a feature-matched worker. New local static-path measurements are diagnostics,
never replacements for historical Phase6–10 release evidence.

Assume trusted Unix operator/private roots, immutable sources, nonrollback storage,
same-host local fsync/atomic-link semantics and stable wall clock. Process-kill
proof is not power-loss proof. Automatic worker replacement, operator reset/adoption,
replicated metadata, cloud/external transactional sinks, arbitrary SQL/DAGs,
streaming and multi-tenant fairness remain outside bounded v1.
