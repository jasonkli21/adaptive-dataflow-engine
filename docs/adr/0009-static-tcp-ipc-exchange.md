# ADR-0009: Static TCP exchange with execution-local Arrow IPC retention

**Status:** Accepted\
**Date:** 2026-10-15

## Context

Phase 3 must prove real hash distribution while preserving architecture INV-2/4/7/10.
Distribution semantics must remain independent of a permanent storage mechanism.
The Phase 2 ledger does not bound network/library memory or process RSS.

## Decision

Version 1 hash distribution uses SplitMix64 over UInt64 keys followed by unsigned
modulo worker count. Test vectors fix the version. `partition` owns routing;
`exchange` implements the single Phase 3 transport/retention mechanism. It is not
a general exchange framework.

Use separate TCP exchange listeners, blocking socket backpressure and no batch
queue. Every stream carries (job, exchange, partition, producer task, attempt),
hash version 1 and aggregate schema version 1. Exchange 0 carries partials;
exchange 1 gathers final tasks. Each frame is UInt32 length plus a standalone
Arrow IPC schema/one-batch/EOS payload, capped at 1 MiB and 1024 rows. Sender
serialization rejects growth before exceeding the cap. Receiver prevalidates
IPC advertised metadata/body lengths within the frame before Arrow decoding,
rejects compression, checks the schema/non-null columns and requires exactly one
batch. There are at most four receiver handlers and one encoded destination batch
at a time per producer. Producers open up to 2–4 bounded sockets lazily when aggregate output becomes available, never retaining the whole exchange.
Five-second connect/inactivity timeouts and total deadlines on each header/frame/ACK operation bound stalled or continuously trickling network operations.

Receivers allow only the prepared expected identity set, permanently reject a
second start for an identity, and stream decoded batches into a NamedTempFile.
A zero frame terminates even an empty transfer. Finish the IPC writer, persist
without clobbering, update the completion registry, then ACK the full identity.
A disconnect/invalid frame cannot become completed and removes its partial file.
Final execution requires all producer transfers; gather requires every final task.
Gather concatenates disjoint key partitions without aggregation.

Roots are unique execution-local TempDirs. Success/cancel drops worker job ownership;
active handlers/tasks retain RAII references until done. Locally managed parents
also remove their root after kill/wait, including process loss. Files are lossy,
non-durable, and never recovery material. No fsync/durability claim is made.

The local runtime gains finalized IPC scan, a merge contribution mode in the
existing spillable aggregate, and a sink using the same operator loop as collect.
Sink errors explicitly shut down the tree before spill RAII cleanup. This refines
ADR-0006's collection-only limitation, preserving its managed-state guarantees.

## Memory and observability

Three separate categories remain explicit: `ade-exec` managed retained memory;
bounded exchange frame/partition/decoded-batch buffers; protocol/Arrow/Parquet
transients and thread/socket/allocator overhead. No process-wide hard cap is claimed.
With four streams, a receiver can hold four bounded encoded frames plus their
decoded fixed-width batches and IPC writer transients. Sender partition indices
and destination batches are bounded by 1024 rows. Final benchmark gathering and
the independent oracle still collect results in coordinator/harness memory.

Per-worker producer/final input/output, source counts, durations, local memory/spill,
transfers, payload bytes, rows, destination distribution, peak frame and gather
counters are returned and debug-logged. Payload bytes count encoded IPC payload,
excluding control messages, length prefixes, headers, ACKs and TCP overhead.
Result schema v1 remains unchanged.

## Alternatives, architecture impact and follow-up

Direct streaming would require overlapping final operators/control semantics;
remote/durable shuffle adds recovery commitments. IPC reuse is adequate for this
static barrier design and preserves INV-10. Refines architecture exchange/storage
sections and INV-2/4/5/7; it does not redefine Hash(key) as a disk policy. Phase 4
should extract a mechanism interface only when a second transport/retention mode
exists. Compression, TLS, recovery, retries, adaptive buffering and skew handling
remain deferred. Existing aggregate partition no-progress errors remain explicit.
