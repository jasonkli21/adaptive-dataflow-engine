# ADR-0010: Static unified exchange and overlapping task roles

**Status:** Accepted\
**Date:** 2026-10-16

## Context and architecture impact

Phase 4 implements PRD Phase 4 and refines architecture sections 7–10 and
INV-1/2/4/5/6/7/9/10. ADR-0008's one-task restriction and ADR-0009's single
retention mechanism are superseded; their identity, deadline, codec and failure
protections remain. Distribution remains version-1 SplitMix64 hash/modulo.
There is no adaptive selector, lineage, retry or recovery.

## Common boundary and lifecycle

Keep three crates and one-way `ade-bench -> ade-distributed -> ade-exec` ownership.
`Sender::batch/finish` is the common producer writer. Destination `Store::reader`
is one `ade-exec::BatchSource` for every mechanism. `execute_merge_source_to_sink`
installs that source into the existing spillable aggregate and existing drive/
shutdown loop. IPC scan remains available. The small external-source placeholder
is provisional local IR; callers cannot execute it without installing a source.

One prepared job permits at most one producer and one final task per worker.
Role flags bound active concurrency; started flags reject duplicate starts even
after a role finishes. The existing JobId high-watermark survives cancellation.
The coordinator launches final RPCs before producer RPCs and waits for all 2N
responses under one absolute task deadline, including connect. Dispatch does not
promise OS thread start order: direct handoff waits safely for the reader.
Materialized sources wait internally for all expected transfers. First required
failure cancels survivors before joining bounded RPC threads. No partial result
returns. A producer error unblocks waiting final sources; receiver failures cancel
the destination session. Listener shutdown also cancels active receiver sessions
before joining them. Active-session metadata is bounded by four handler slots.

## Segment wire contract

Private exchange headers now require wire version 2 alongside unchanged hash and
schema versions. Every frame is `(u32 payload length, u64 sequence, payload)`.
Sequences start at zero and advance once per non-empty standalone IPC batch. A
zero-length frame carries the final segment count. Duplicate/gapped sequence or
incorrect completion count fails the transfer/session. Empty transfers terminate
with count zero. ACK still echoes the full transfer identity after completion.
No global producer order or lineage identity is promised. Existing 1 MiB/1024-row
caps and IPC allocation-envelope prevalidation remain. Control result schema v1
and benchmark artifacts are unchanged; this private wire is intentionally strict.

## Static mechanisms

| Mode | Retention and pressure |
|---|---|
| `direct` | Single encoded frame rendezvous slot, only filled while a consumer waits; writer waits until taken. No normal disk/object retention. TCP/kernel buffering and bounded receiver frames remain. |
| `buffered` | Destination-wide byte-accounted FIFO of encoded frames. Writers block at capacity; taking a frame releases its accounting. Capacity must fit a maximum legal frame. |
| `spill` | Same finite FIFO, with new arrivals appended to an anonymous execution-local disk log when they do not fit or a disk backlog exists. Drain memory before log; later arrivals cannot overtake the log. Two offsets track disk backlog, avoiding a per-segment in-memory file index. Truncate the log whenever drained. |
| `local_materialized` | Phase 3 transfer IPC files, incrementally written, finalized and persisted without clobber before registry completion/ACK. Reader waits for all producers. Default. |
| `object_store` | One bounded IPC object per segment through public `object_store::ObjectStore` APIs. Full job/exchange/partition/producer/attempt/sequence keys. Reader waits for all transfer completions, then lazily enumerates sequences using O(producer count) metadata. Head/range reads reject objects over the frame cap. |

Buffer size is strictly positive and at most 256 MiB, default 4 MiB. Buffered
mode requires at least 1 MiB; spill accepts smaller limits, including a limit that
forces every segment to disk. Direct/local/object modes validate and report this
setting but do not use it as retention capacity. Spill threshold behavior is a
fixed property of the statically selected mechanism, not Phase 5 policy.

## Durability and object visibility

The provider-neutral backend is an `Arc<dyn ObjectStore>`. Phase 4 provisions a
filesystem-backed instance in a unique execution-scoped sibling directory,
separate from worker-local IPC/spill files. Each bounded put completes atomically
according to the backend before the transfer can complete. Completed segment
objects alone do not establish transfer completion: the session registry must
validate the terminal count and all required identities before readers yield.
No per-exchange payload collection or whole-transfer upload buffer exists.

This filesystem fixture proves API behavior, not cloud/multi-host durability.
Durability is exactly that of the configured backend. Objects and registry are
execution-scoped and removed on teardown; there is no cross-job reuse or recovery
manifest. A future externally configured provider requires an explicit lifecycle
and credential contract, not a claim that ephemeral local storage is durable.
Backend errors fail the job. Blocking filesystem operations are not preemptible.

## Resources and observations

Producer and final local task budgets can coexist: twice the configured per-task
managed limit is possible on a worker. Exchange retention is separate. The buffer
metric counts retained encoded payload bytes; queue metadata, up to four receiver
encoded/decoded frame transients, the consumer's current decoded batch, codec,
socket/kernel and allocator overhead are separate bounded categories. Direct's
one-frame slot can reach the frame cap regardless of the unused buffer option.
No total worker/process RSS limit is claimed. Spill/object/local disk volume is
proportional to unconsumed/materialized input, not given a quota in Phase 4.

Metrics expose mode, completed transfer/frame/row/payload counts, frame peak,
retention peak, local files/bytes, spill writes/reads, object writes/reads/count,
and handoff/capacity wait and reader polling wait nanoseconds. Wait counters are
observations, not exact CPU/transport attribution. Coordinator success requires
sent/received transfer, segment, row and byte conservation plus final-input rows.
Rejected transfer behavior fails the session rather than disappearing into success.
Gather remains fixed local-materialized; final output/oracle collection remains
at the harness boundary.

## Alternatives and follow-up

A new exchange/shared-ID crate hierarchy and async runtime migration would add
scope without improving current ownership. Keep synchronous CPU execution;
Tokio is used only to bridge bounded public object-store operations. The two new
direct dependency edges use already locked packages, with no package upgrades.
Per-segment spill files were rejected because an in-memory path queue would grow
with total exchange size. Phase 5 can use these static baselines, segment identity
and buffer/wait/materialization observations; it must separately specify policy.
