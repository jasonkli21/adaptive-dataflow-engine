# ADR-0017 — Bounded file-morsel scheduler v1

Status: Implemented, PARTIAL — correctness verified; all Phase10 empirical guards failed. See the Phase10 implementation report.
Date: 2026-10-24. Baseline: 3e89edfd0230674f0e06437cccb58fa99e369422.

Refines architecture §§6.2,9,13,17,22,27–31; preserves INV1–10 and
ade-bench → ade-distributed → ade-exec. The user's implementation request
supersedes the stale Phase9-only AGENTS boundary. Phase8/9 historical timing
identities and failed experiments are unchanged. No Phase11 work is authorized.

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
hysteresis. Normal ordering is locality, normalized donor queue, candidate bytes;
critical ordering swaps the first two; ties use MorselId then preferred WorkerId.
This clarifies the earlier prose's local-queue shorthand; the pure function,
reconstruction tests and release guards are unchanged. The final runtime snapshot
predates this explanatory paragraph; no engine byte or timing threshold changes.
