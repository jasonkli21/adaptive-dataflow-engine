# ADR-0017 — Bounded file-morsel scheduler v1

Status: Implemented, PARTIAL — bounded correctness and all six unchanged local release guards pass at ee9f8da; physical membership stays fixed. Original failed sources remain recorded below. See the Phase10 implementation/readiness reports.
Original decision date: 2026-10-24. Baseline: 3e89edfd0230674f0e06437cccb58fa99e369422.

Refines architecture §§6.2,9,13,17,22,27–31; preserves INV1–10 and
ade-bench → ade-distributed → ade-exec. The user's implementation request
supersedes the stale Phase9-only AGENTS boundary. Phase8/9 historical timing
identities and failed experiments are unchanged. The original decision did not authorize Phase11; it was subsequently authorized and completed under ADR-0018.

One immutable manifest file is one MorselId. A finite coordinator ledger owns
Pending → Leased → Completed, one outstanding lease per worker, no retries or
reassignment after grant. Nonce, job, execution UUID, epoch1 and schedule digest
fence EVERY control operation. Digest binds ordered source fingerprints, registry,
all task config and final layout. All prepared workers ACK before tasks start;
all producer clients report ready before the first grant. Identical completion
is idempotent; changed owner/nonce/counters rejects and aborts. Expiry, ambiguous
ACK, source drift, producer/final failure or service failure aborts the job.

Pure policy sees pending preferred queues, declared integer capacities1..1000,
optional locality labels, remaining deadline and measured completed work. Work
units are physical source bytes (zero-byte estimates floor to1). Normalize queue
work by declared capacity using u128 fixed-point arithmetic; ties use MorselId.
With local pending work, choose its largest work first. Otherwise steal from the
longest normalized remaining queue, largest file first, with a configurable
minimum byte threshold. Declared matching locality breaks equal-score ties;
near deadline (last quarter of stage budget), critical-path priority precedes
locality. These are estimates, not a deadline guarantee or fabricated CPU rates.
No-steal is a diagnostic control; static retains modulo assignment. Capacity
weights influence donor critical-path selection, not source identity. The audit
log is a capped ring with dropped count; complete coverage stays in the finite
ledger. One service thread serializes bounded RPCs, and each worker has one
independent heartbeat thread so a synchronous scan/send cannot starve renewals.
Heartbeats renew the SAME exclusive owner. Every operation checks stage deadline;
control count and request size are capped. Ordinary blocking OS reads remain
non-preemptible; no process RSS or physical device throughput claim is made.

Each worker owns one persistent sender set using original TaskId(worker), attempt0.
Multiple per-file fixed-hash executions append contributions to that set. Close
exactly N terminals, including empty parked/drained producers, once. Schema4 uses
the original aggregate arrays with a schedule digest and terminal content hash
on wire2; legacy schema1 bytes stay unchanged. Spill-only leased ingress is capped
by rows/encoded bytes/frames. Idle leased receivers poll cancellation between
bounded waits for frames instead of expiring a healthy long-running morsel.
Final receivers stay alive during producer drain. Existing attempt-safe gather
commits only after full lease coverage and N×N row/byte/frame conservation.

Pre-registered producers are Active/Parked/Draining/Drained. Completion-count
controls activate or request drain during the job. Drain stops grants immediately,
finishes current lease, closes terminals and keeps final duties alive. Spot notice
uses the same drain plus a finite grace deadline; inadequate notice fails the job.
Repeated control is idempotent. No last-active drain with pending work is admitted.
Physical process addition/removal, migration, failover and membership epochs remain
PRD gaps; elastic producer participation does NOT complete the full Phase10 PRD.

Admission: leased fixed_hash GroupBySum only,2–4 loopback workers, static hash
layout, spill delivery, verified non-replayable snapshot, recovery_enabled=false.
Reject sampled/prefix, adaptive/raw partial, cost copies, other retention, joins,
replay, unsupported hooks and invalid/capped config BEFORE output. Static paths
retain all accepted Phase6–9 behavior. Private control advances ADETCP11→12;
exchange wire2/hash1/result schema1 remain. Diagnostic sidecar3 binds leased
metrics to run/slot/PID, source inventory, effective config and schedule digest.


Before release, exact-sum review identified a necessary per-file boundary rule:
a file partial can exceed Int64 even when the eventual cross-file result fits.
Leased producers build the existing fixed hash/spill state, then reuse the
existing i128→checked Int64 contribution drain (splitting sum and count exactly).
There is no adaptive switch or raw source tail. Spill fragments can retain
repeated keys, bounded by the admitted original rows and exchange ingress caps;
final merge remains the exact checked aggregate. Static lowering is unchanged.
This narrow ade-exec sink API avoids a new IR or a false per-file overflow refusal.

The source catalog uses at most one eighth of the1MiB diagnostic frame (checked
before preparation); default max256files, explicit ceiling1024. Physical source
bytes default<=1GiB, configurable<=1TiB with checked queue arithmetic. Full file
fingerprints verify at admission, then before and after each leased read; the
stage deadline includes admission. Per-worker source verification, acquire/wait
and completion-ACK wall times are separate from operator wall work. Renewal
attempts arriving before half a heartbeat interval do not extend the lease.

Final premeasurement review also makes leased source verification check the
shared deadline/cancellation between every8KiB hash read, independently of
heartbeats. It must not cite non-preemptible OS syscalls as permission for an
unbounded user-space hashing loop. Static verification behavior stays unchanged.

Policy clarification recorded before release timing: local work is preferred
unless an off-preference candidate is predicted to reduce the donor critical
path by MORE than10%. For queued bytes Q, candidate bytes B and declared weights
C, compare max((Q_requester+B)/C_requester,(Q_donor-B)/C_donor) against
0.90*(Q_donor/C_donor), using the checked fixed-point scores in code. Without
local pending work, the minimum-byte steal threshold applies without that
hysteresis. If it filters every off-preference candidate and the requester has
no local Pending work and every pending morsel's preferred owner is parked,
draining, or drained, the scheduler may take the best remaining morsel for
progress. If a pending preferred owner remains active, the requester waits for
that owner to take its local work. Normal ordering is locality, normalized donor queue, candidate bytes;
critical ordering swaps the first two; ties use MorselId then preferred WorkerId.
This clarifies the earlier prose's local-queue shorthand; the pure function,
reconstruction tests and release guards are unchanged. The final runtime snapshot
predates this explanatory paragraph; no engine byte or timing threshold changes.

Independent review follow-up on source `48a6760`: a producer with a scheduled
future activation remains open while that event is outstanding, even when no
Pending morsel is currently visible. A parked producer with no future activation
can close immediately; the fixed final receiver remains in service. The minimum
steal size is enforced while an eligible candidate exists. If it filters every
off-preference candidate, the requester has no local Pending work, and every
pending preferred owner is parked, draining, or drained, leased_v1 may take the
best remaining morsel with reason `steal_pending_for_progress`. If any pending
preferred owner remains active, the requester waits. LeasedNoSteal never takes
this fallback. The same pure decision is used by offline evidence
reconstruction, using participation transitions recorded before each grant.

Before preparation, admission now validates every completion threshold against
the actual frozen source-catalog size and simulates configured activation/drain
transitions. An activation at the final completion is valid and can immediately
close an empty producer. A drain of the last active producer while catalog work
remains is rejected before worker preparation. These changes refine the §13
control lifecycle and preserve INV-3/5/9; they do not change lease, exchange,
result, or sample-gate schemas.


## Stabilization amendment (2026-10-26)

The earlier authorization and “No Phase11 work” statements describe the original
2026-10-24 session. Phase 11 bounded v1 is now COMPLETE under ADR-0018. The user
separately authorized readiness and Phase 10 remediation; Phase 12 is excluded.
This amendment refines architecture §13.4 and INV-4/5 without widening v1.

A reproduced partial-control stall blocked healthy 20ms heartbeats beyond an
80ms lease. The service now polls at most 2*N+1 nonblocking connections on its
single ledger thread, one bounded read/write per peer per turn. The extra slot
isolates one stalled peer from normal main/renewal requests. Each peer retains a
500ms absolute deadline and at most a 1MiB frame (plus framing/allocation overhead).
Admission saturation, malformed/truncated frames and ambiguous replies still
abort; this is bounded liveness isolation, not malicious-peer availability or HA.
No handler threads, unbounded admission queue or in-flight reassignment is added.

Content verification still hashes the full admission catalog and each lease both
before and after execution. A reusable 256KiB scratch buffer amortizes reads;
cancellation/deadline checks run between every chunk. Scratch is separate from
task and exchange reservations, at most one buffer per concurrent verification
call. OS calls and change-and-restore remain outside snapshot guarantees.
Coalescing partials or skipping a content pass is not authorized by this change.

Release evidence v2 binds three macOS arm64 readiness samples, exact capture
commands, kernel/host, session UUID, monotonic and wall times, postflight capture,
commands/statuses and result run IDs through a SHA256 inventory. Offline checks
apply the original idle/load/swap/process thresholds. These are referenced
observations, not signed host attestation. Research-hook stderr v1 proves an
actual lease and output frame/expiry/spot deadline before a negative can count.
There is no result-v1, child, manifest or ADETCP12/wire2/hash1 change. Legacy v1
bundles use their archived verifier; new verification refuses to promote them.
New optional diagnostic fields subdivide existing exclusive operator wall time;
parallel producer/final sums are never job CPU or critical-path wall time.
The dated readiness report records empirical outcomes and the promotion decision.


Stabilization outcome: clean fcc325e passes 183 Rust/17 Python correctness tests,
all 62 release children and the three reached negative stages, but all six original
timing guards still fail. Only one measured candidate was used; source/scan
critical-path evidence rules out a sufficient contribution/control-only fix.
Leased scheduling stays experimental/unpromoted. See the 2026-10-26 readiness
report; neither this amendment nor Phase 11 closes physical elasticity or permits
Phase 12.


## Continued measured remediation (2026-10-26)

The user explicitly overrode the initial stopping/time-box recommendation after
fcc325e and authorized continued work to address the six failures. Historical
negative results stay unchanged. Refines §§7–9/13/22 and INV-4/5/9: source content
checks may use an explicit bounded, scoped CPU pool (at most eight hash threads
per active job stage; split across the fixed producer registry). No global pool,
unsafe mapping, content-pass removal or metadata-only substitute is admitted.
Parallel BLAKE3 must retain the canonical descriptor hash and cooperative checks
between bounded chunks; threads/FDs/scratch and startup/teardown costs must be
measured. An explicit projected scan may decode only the aggregate's key/value
columns while full-file source hashes still bind all bytes. Legacy static scans
and benchmark result schema remain unchanged. Diagnostic output may use bounded
buffered writes for both controls and leased arms, retaining complete sidecars,
all error propagation and the original measured query boundary. No samples or
sidecar work may be silently excluded from timing.

The continued implementation uses 4MiB hash chunks, lazily reused within the
owned verifier; at most eight buffers/32MiB and eight 512KiB hash-thread stacks
at coordinator admission. Producers split the eight-thread budget by registry
size and retain one 4MiB scratch buffer each. Every pass reopens, reads and hashes
the current file; cached bytes never stand in for identity verification. Grouped
catalog tasks bound suspended nested parallel hashing to the thread count.
All hash tasks/threads join and buffers release on success, cancellation or error.
Task, exchange, hash scratch and process RSS remain distinct resource domains.

Persistent control sessions retain the same ADETCP12 framing and full
job/execution/epoch/digest/worker/lease fences on every request. A producer has
one main socket and one lazily opened renewal socket, within the existing 2*N+1
service cap. Acknowledged idle sessions have no partial-frame deadline; the first
byte of each following request starts its 500ms deadline. EOF at an acknowledged
frame boundary closes cleanly; truncation, pipelining, malformed input, deadlines
and ambiguous responses cancel without retries. Acquire Wait uses bounded
250us–5ms exponential backoff instead of a fixed 5ms sleep, retaining stage and
control-operation caps. Leased worker preparation runs at most N scoped threads;
source admission precedes it and every topology/prepare ACK precedes task launch.
All preparation threads join before cancellation. Static preparation is unchanged.

The isolated hash matrix confirms that extra producer hash threads remain useful
on this host, while 4MiB chunks regress the four-worker path; retain that failed
experiment and return to a 1MiB reusable chunk (8MiB catalog / 1MiB producer
scratch). Continued profiles also identify file-local setup that does no useful
work: opening the first Parquet footer twice, creating/removing an unused spill
directory, and sorting a hash table immediately before its unordered raw partial
drain. The leased raw-partial entry point may reuse its first validated reader,
create its spill directory only on the first real spill, and bypass the unused
sort. It must retain the existing bounded spill codec/merge, checked i128/count
contributions, deterministic traversal, cancellation/error cleanup, and complete
per-file ACKs. Normal aggregate output sorting and eager spill setup remain
unchanged. No cross-file coalescer or contribution-count change is introduced.

A further bounded contribution optimization reuses the existing row-preserving
FramePacker in the leased producer's provisional sender session. This performs
no keyed coalescing: every exact per-file contribution and count survives, in
per-destination order. Before each ACK, cumulative successfully sent rows plus
owned pending rows must advance by exactly that file's output_rows, and the full
post-read content check must pass. Pending storage is at most N*1024 rows (N*24KiB)
plus one transient frame, separate from the task ledger and receiver buffers.
Drain flushes all pending rows before N terminal hashes and Closed. Ambiguity,
source drift, sender failure or producer loss still cancels the entire job; an
ACK remains provisional. Actual wire bytes, terminal hashes and contribution
counters keep their existing meaning, so no protocol/sidecar version changes.
The actual-frame death hook explicitly flushes a pending frame before exiting,
retaining its mid-lease/pre-ACK failure point even on a small fixture.

Vectored writes may join the existing length/sequence/payload buffers into a
single socket operation, without an extra frame copy or altered wire bytes.
Partial writes, interruptions, zero writes and the original total I/O deadline
must be handled. This shared mechanism applies fairly to static and leased arms.
Catalog hashing uses at most eight serial-per-file jobs concurrently, with
256KiB reusable scratch per job (2MiB total) and at most 1024 bounded queued jobs;
single-file producer passes keep their scoped parallel 1MiB chunks. Control polls
use a bounded 10us sleep; control/stage/frame caps and cancellation remain.

The packed diagnostic retains all exact rows but still misses uniform4. The next
source reassessment compacts the shared fixed-capacity table: count=0 is an empty
slot, since source and partial counts must be positive. A `(u64,i128,u64)` slot
avoids the Option discriminant/padding; zero sums and key=0 remain ordinary rows.
Keep the original slot-count/load/spill-capacity rule, hash/probe order, checked
updates and sorted normal output. Charge actual compact storage to the existing
reservation; reject zero-count insertion before mutation. This shared allocation
improvement applies to both static and leased execution and retains the same
spill codec. Test sentinel/zero/overflow/conservation cases. Restore the 100us
service sleep: lower control latency is not sufficient justification for extra
poll CPU competing with mandatory hashes. Preserve both negative experiments.

Continued acceptance: clean source ee9f8da (crates4b780320) passes 192 Rust/18
Python tests and the unchanged 13-arm release: 62 successful children/49 measured
samples, three reached fault stages, all six timing guards. The closest guard is
uniform4=1.143644 against1.15, a narrow local margin. Full source/pre/post checks,
file-level provisional ACKs, exact rows and terminal validation remain. Both
failed sources and seven native diagnostics are preserved in the dated readiness
report. The bounded opt-in research path has local acceptance; static stays the
default and physical process membership/migration/recovery remain deferred.

## Re-review comparator amendment (2026-10-26)

The remediation re-review authorizes supplementary attribution, historical bundle
replay and a separately declared confirmation; the original 13-arm release is
immutable. Refines architecture §§9/13/22 and INV-3/4/5: the diagnostic
`producer_pipeline=static_projected` uses static modulo file ownership, ordered
by descending physical bytes then MorselId within each owner, and the SAME
projected scan/first-footer reuse, per-file fixed-hash contribution drain, lazy
spill, full admission/pre/post content hashes, scoped hash budget and exact
row-preserving FramePacker as leased production. Preparation is parallel in both.
It requires fixed_hash/static topology/spill, verified non-replayable source,
recovery disabled, and no scheduler participation or fault hooks. It has no lease
service or ACK; ordinary static transfer framing remains. It is a diagnostic,
not a replacement default or frontend feature. Missing/changed sources and
provisional-row mismatches fail without successful output.

The existing `leased_no_steal` is the stricter mechanism-matched static-ownership
control: same lease/control/ACK and typed terminal path as leased_v1, differing
only in policy. Report both controls, actual off-preference grants and per-worker
stage wall times; do not add overlapping task timers into job latency. Attribute
any gain over legacy static separately from scheduler policy benefit. Confirmation
uses the original limits/repetitions without selecting the best attempt. Hash
integrity and local host readings remain neither signed attestation nor an
independent-host replication. No physical membership, recovery, protocol version,
result schema, historical source identity or Phase12 boundary changes.


Re-review validation at clean snapshot b4a2a70: historical archived-verifier
replays preserve the earlier FAIL/PASS results; all six supplementary matched
comparisons and all six fresh original confirmation guards pass. Actual steals
occur in imbalanced samples only. Credit a narrow local imbalanced policy gain,
not universal or independently replicated performance. The dated validation
report retains all samples, the zero-measurement post-build readiness refusal,
and the independent host/day limitation. Physical membership remains deferred.
