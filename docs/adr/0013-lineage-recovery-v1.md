# ADR-0013: File lineage and single final-worker recovery

**Status:** Accepted\
**Date:** 2026-10-16

## Context and architecture impact

PRD Phase 6 requires selected missing work to recover inside the original job.
This refines architecture sections 11, 17, 22 and 28 and implements INV-1/4/5/7/8.
It supersedes the no-recovery boundaries of ADR-0010/0012 only for the envelope
below. Static topology, hash routing, retention mechanisms and ADR-0011 I/O
containment remain. There is no Phase 7 cost policy.

## Identity and lineage

A `LogicalTransferId` contains job, exchange, destination partition and logical
producer task. `SegmentId` adds sequence, never attempt. Concrete `TransferId`
continues carrying the execution attempt on exchange wire v2.

`SourceSnapshot.version=1` denotes the existing scan/project/partial aggregate,
SplitMix64 hash v1, exchange schema v1 graph. It binds manifest, logical and
schema fingerprints to ordered whole-file morsels, byte sizes and BLAKE3 file
fingerprints. Morsel IDs are manifest ordinals; round-robin assignment determines
producer edges. The prepared job holds the original typed `TaskConfig`; replay
uses that same configuration and original partition count. Workers check source
assignment and physical file fingerprints before original production and replay.
Replayability is explicit. Inputs must remain immutable during execution;
this is validation of local files, not a transactional filesystem snapshot.

Each producer returns one terminal manifest per destination after its completion
ACK: logical ID, attempt, segment/row/byte counts and BLAKE3 content fingerprint.
The incremental hash covers a version/domain, serialized logical ID, each sequence,
encoded length and IPC payload, then terminal version/count. Attempts, paths,
worker IDs and clocks are excluded. The existing sorted aggregate output provides
deterministic batching. Empty transfers have explicit zero-count manifests.

The lineage graph is represented by snapshot morsels, deterministic producer
assignment and logical transfer edges. Missing worker-local data is represented
by one `[0, terminal_count)` range per producer transfer to the missing partition.
For a non-empty transfer, closure replays all files assigned to that producer
because partial aggregation spans those files, while emitting only the missing
destination. A zero-segment transfer gets an attempt-1 empty terminal without
reopening its source files. This is minimal under the current whole-producer
replay primitive, not row/range replay.

## Planning and mechanism

The pure planner requires every original producer terminal, exactly one missing
final worker, every other final successful, and valid replayable lineage. The
coordinator additionally validates survivor gather terminals and confirms that
the candidate lost worker does not respond to Ping. Transport EOF/reset is a
candidate; live-worker execution errors, producer errors, timeouts and a second
loss fail the job. No unproven surviving/durable copy is selected. Recompute is
the first supported valid action; local RAM/disk and the ephemeral filesystem
object fixture are lossy.

Choose the lowest successful surviving WorkerId. A single `RecoverFinal` RPC
owns one episode on that worker after its original roles are quiescent. It
creates a fresh localhost receiver and local-materialized Store expecting only
the missing partition, original producer IDs, original JobId and attempt 1.
The RPC carries compact producer/terminal references; source paths and fingerprints
come from the already-prepared snapshot held by the worker. It sequentially runs
the existing producer pipeline for each non-empty closure and emits empty
terminals for zero-segment transfers, then validates
each terminal fingerprint/count against attempt 0, then runs the existing final
merge aggregate. The host rejects poisoning, non-quiescence, duplicate recovery,
foreign sources/ranges and unexpected attempts. No general scheduler is needed.

This combined RPC is a deliberate simplification of separate prepare/replay/final
RPCs: it avoids distributed recovery-session state, bounds concurrency to one
replayed task, and gives cancellation one original-job token. No larger region
or whole-job restart exists. One episode is allowed; any episode failure cancels
survivors and returns no partial result.

## Gather commit

Original finals use separate local-materialized Stores per logical partition,
all expecting attempt 0. A final is promoted only after its successful task reply
and exact gather terminal conservation validation. On recovery, the failed store
is cancelled and quarantined. An already validated old terminal is ambiguous
and refuses replacement. A fresh receiver/Store expects only that partition's
attempt 1; no old state is merged. A topology-bounded commit map rejects duplicate
success and requires every partition exactly once before collecting output.
Late old traffic is confined to the cancelled original store; unexpected new
traffic fails the recovery store. Neither can append to another committed attempt.
This is an internal batch gather boundary, not a general external sink transaction.

## Bounds, protocol and evidence

Metadata is O(source files + workers squared), with one manifest/range per logical
transfer, at most one recovery plan/attempt and no resident segment history.
The control frame cap remains 1 MiB, so excessively large descriptors fail before
execution rather than create an unbounded control message. Sequential replay
uses one producer task budget, followed by one final budget; exchange/disk and
codec resources retain existing bounds/limitations. Recovery local disk volume
has no quota. The local ledger is not an RSS limit.

Control framing is `ADETCP04`; old `ADETCP03` is rejected before payload decoding.
Exchange header/frame versions remain unchanged. Recovery diagnostics record the
plan, exact ranges, compact producer closures, replayed morsel counts, manifests,
host, elapsed time and committed attempts. The 1 MiB request-boundary test admits
a 5,000-file synthetic source snapshot and confirms the recovery request does not
reserialize its source descriptors. Adaptive summaries contain only observed surviving original
finals; missing-worker adaptive history is unavailable, and the recovered final
reports actual local materialization rather than fabricated adaptive history.

The feature-gated, default-off `research_exit_final_partition` exits the real
worker process with code 86 immediately after sending the first non-empty
gather batch, before remaining batches and its terminal. The acceptance workload
produces multiple batches. A coordinator gate releases it only after all original
producer RPCs succeeded, then confirms the gather store decoded exactly one
attempt-0 batch before starting recovery. The feature-only observation is bounded
to one counter per in-flight transfer. It proves strict-prefix partial-attempt
isolation as well as worker loss. The hook has no default-library setting/branch.
Production recovery does not depend on the hook.

## Limitations

Arbitrary producer loss, simultaneous losses, recovery-host loss, replay drift,
ambiguous completed gather, unknown source identity, non-replayable lineage and
recovery timeout remain failures. There is no durable restore, worker replacement,
coordinator restart, cost model, external exactly-once sink or recursive retry.
