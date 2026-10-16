# ADR-0006: Managed local memory, partition spill, external sort and prefetch

**Status:** Accepted\
**Date:** 2026-10-15

## Context

Phase 2 replaces ADR-0005's observational-only blocking state with finite local
resource management. Architecture sections 7–8 and INV-4/5/6/7/9/10 require
bounded state, observability, frontend independence and disposable local storage.
The synchronous pull model remains useful; local scan overlap needs only one
bounded producer, not an async scheduler.

## Decision

Keep both crates and the provisional pull plan. Add execution configuration with
a default 256 MiB managed limit, spill enabled, prefetch depth zero and 1024-row
output cap. The adapter's four controls are batch_size, memory_limit_bytes,
spill_enabled and prefetch_batches. Prefetch depth is limited to 64.

A mutex-protected execution-local manager grants owned RAII reservations. Growth
is atomic and checked; failed growth leaves bytes/current/peak unchanged while
incrementing attempts/denials. Shrink rejects underflow; drop releases remaining
bytes. Metrics retain per-operator peaks. A separate pressure counter records
state-target events; these are not fabricated reservation denials.

Controlled hash/run storage reserves before allocation. Collection reserves each
retained batch's Arrow buffer estimate plus 128 bytes for batch/vector metadata
before retaining it. Shared Arrow buffers may be conservatively counted more than
once. ExecutionOutput owns the collection reservation. Returned metrics are a
snapshot; operator reservations are gone and collection remains charged.

This is **not a total RSS cap**. Synchronous decoded input, filter kernels,
Parquet decoding, IPC codec internals, allocator overhead, thread stacks, small
plan/service/metrics metadata and library temporaries remain external/observed.
Output builders reserve before allocating vectors. Sort reserves conservative
workspace allowances; Arrow kernels can allocate additional transient storage.
Prefetch reserves decoded batches before publishing/retaining them; the producer
must decode a batch before its exact Arrow size is known. Consumer input after
queue ownership ends is observed rather than charged as blocking state.

The spill service owns one unique TempDir and operator-tagged, create-new files.
Successful execution closes operators/producers before explicitly closing the
root; errors/cancellation use RAII cleanup. Temporary filesystem failures on drop
are best effort. Files are worker-local and lossy, with no recovery claim.

Aggregation uses a fixed power-of-two open-addressed table, reserved using the
actual slot type size, with a maximum half-full load. Table bytes target one
quarter of the configured budget, capped at 65,536 slots. A target event spills
partials into 16 deterministic SplitMix64 hash partitions. Each flush appends to
the same partition files with one 1024-byte buffered writer active at a time;
8192 reserved metadata bytes cover paths/serialization bookkeeping. Internal
codec `ADEAGG01` consists of the eight-byte version followed by little-endian
(u64 key, i128 sum, u64 count) records. Reader validates version, record alignment
and positive counts; checked merges preserve sum/count/output overflow errors.
Finalization reads one partition into the same bounded table and emits bounded
batches. A partition exceeding half-full capacity fails with SpillNoProgress;
recursive repartition is deliberately deferred. Keys within each partition are
sorted in place; spilled output has deterministic partition order rather than a
global ordering promise. GROUP BY semantics remain unordered.

Sort supports non-null UInt64 key/row_id and Int64 value only. Arrow's public
lexsort_to_indices sorts each run by key then row_id. Run workspace targets one
quarter of budget, 128 reserved bytes per row, capped at 65,536 rows. Runs use
Arrow IPC streams with at most 128 rows per record batch. Online binary
compaction retains at most one run at each of 64 levels and merges two runs at a
time. The final fold also uses fan-in two: at most two IPC input chunks and one
output chunk are active, with a 32 KiB reservation for merge buffers/allowance.
Input runs are deleted after successful replacement. Sort output respects both
the configured output cap and the internal 128-row cap. There is no all-run
reload or unbounded run list. Metadata receives a 16 KiB reservation.

Prefetch uses a standard-library thread and a bounded mutex/condvar queue.
Depth zero uses the same reader synchronously. Producer preserves file/batch
order, propagates errors and catches producer panics as PrefetchFailed. Queue
slots and published batches have reservations. Consumer drop sets stop, clears
queued items, wakes a full producer and joins it. Cancellation is cooperative at
file/batch and blocking-operator row/chunk boundaries; an in-progress filesystem
read is not preemptible. There is no detached worker or Tokio dependency.

Arrow's IPC feature and tempfile are enabled in ade-exec using already locked
versions. No new resolved package or lockfile delta is required. Pinned downloaded
Arrow 59.3.1 sort/IPC and Parquet reader public APIs were inspected locally.

## Alternatives and consequences

A growable HashMap would require allocator/capacity over-reservation rules; fixed
slots make admission and capacity explicit. Sixteen partitions with a clear
no-progress error are simpler than recursive skew splitting. Very high distinct
cardinality can therefore require a larger budget even with spill enabled.
Binary compaction bounds both memory and open descriptors without a variable
fan-in priority queue, at the cost of extra local I/O. General sort expressions,
null ordering, durable spill, remote I/O and process-wide arbitration are deferred.

Collection can exhaust the budget even when blocking state spills successfully;
the engine fails explicitly. A streaming sink is deferred. Spill-disabled mode
fails at the derived state target instead of consuming reserved headroom.

## Architecture impact

Supersedes only ADR-0005's Phase 1 memory/spill limitations. Refines architecture
7.3 to one bounded local-file producer while retaining synchronous consumers;
implements execution-scoped reservations under section 8 without a speculative
hierarchy. Preserves all listed invariants and the independent benchmark oracle,
child protocol and result schema. The new sort contract has its own ordered
validation identity; existing integer canonicalization remains unchanged.

## Follow-up

Before Phase 3, consider recursive partition processing, streaming result sinks,
more efficient IPC run batches and explicit external/transient memory telemetry.
Do not assume this managed budget is an allocator-exact worker limit.
