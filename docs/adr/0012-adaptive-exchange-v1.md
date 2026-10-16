# ADR-0012: Adaptive exchange v1

**Status:** Accepted\
**Date:** 2026-10-16

## Context and architecture impact

PRD Phase 5 requires deterministic runtime retention selection within a job.
This implements architecture sections 3.1–3.4, 9, 10, 14 and 16 and preserves
INV-2/3/4/5/6/7/9/10. ADR-0010's fixed selection remains available as five static
baselines; ADR-0011's I/O containment and backend boundary remain mandatory.

## Decision

Keep a pure `adaptive::decide` boundary inside `ade-distributed`. `threshold_v1`
uses a destination snapshot, owns no batches, performs no I/O, and selects only
monotone `direct -> buffered -> spill -> object_store` transitions. Static modes
bypass it. Local materialization remains the default compatibility baseline.

At each validated segment boundary:

- Direct escalates when the consumer is not ready, there is queued memory, or
  the frame exceeds capacity.
- Buffered escalates when `(buffer + incoming) * 100 >= capacity * high_pct`.
  Equality escalates. Default high percentage is 80, validated in 1..100.
- Spill escalates only with an unconsumed spill backlog and observed production
  plus incoming bytes at or above `remote_after_bytes`. Default is 64 MiB;
  zero disables remote escalation; maximum is 1 TiB.

Re-evaluate at the same boundary to skip an unusable tier. No accepted segment
is split or moved retroactively. Hysteresis is structural: no downgrade exists.
A selected spill segment always goes to the existing spool, even if memory later
becomes free. Adaptive direct still performs real rendezvous.

Serialize destination ingress through a private adaptive mutex, separate from
the short queue/accounting lock. The reader never needs that mutex to consume
local data. Spill and object operations retain their existing cancellation and
five-second deadline owners; a non-preemptible timeout poisons the worker.
There are at most four receiver threads and no new per-decision threads/channel.
A second fixed-size metrics snapshot uses an independent short lock so live
metrics do not wait for ingress I/O or rendezvous.

A transition is staged locally while the ingress lock is held and becomes
visible in the current tier and applied transition history only after the
selected mechanism accepts the complete segment. Failed placement does not
publish a successful transition or count that segment in tier totals. Every
transition carries `threshold_v1` explicitly, and direct-to-buffered reasons
distinguish consumer readiness, existing backlog, and an oversized incoming
frame.

## Mixed tiers and bounded metadata

A destination has at most three transition records, four tier counters and four
implicit extents. Each transition records acceptance ordinal, full transfer ID,
producer sequence, reason, projected occupancy percentage, segment lag and
observation snapshot. One object-suffix start per
expected transfer (at most 16 by Store validation, four in current worker jobs)
is sufficient because escalation is monotone. No resident segment placement map
or permanent per-segment decision log exists.

Memory FIFO precedes the ordered spool; after all transfer terminals validate
and the local prefix drains, the common reader lazily reads each producer's
object suffix. Object keys preserve original transfer/sequence identities. This
keeps per-producer order and exactly one final input for each accepted segment;
cross-producer order remains unspecified. The object suffix uses a completion
barrier, reusing finalized object semantics instead of introducing a live object
index. Finals still use the existing `BatchSource` merge aggregate.

## Observation and capability truthfulness

Snapshots contain incoming bytes, occupancy/capacity, spill backlog, readiness,
local produced/consumed progress and existing cumulative buffer/spool wait
counters. These describe exchange pressure, not worker/process memory pressure.
Wait counters exclude ingress-lock wait and object-provider elapsed time. Policy
uses readiness, occupancy, backlog and observed size; it does not infer worker
health, lineage cost, failure probability or recomputability. Scheduling can
change observations and therefore actual transition boundaries; identical
snapshots/configuration yield identical decisions.

The object backend remains injected and capability metadata caller-declared.
The default filesystem backend is execution-scoped and ephemeral. Neither object
presence nor placement history establishes recovery or cloud durability.

## Research instrumentation and alternatives

An explicit, default-off consumer delay targets one partition after a configured
number of delivered segments, once per source. It is capped at two seconds,
polls cancellation, changes no rows/routing, and appears in effective benchmark
configuration. It is research instrumentation, not scheduling policy.

The hook is compiled only with `ade-distributed`'s `research-hooks` feature,
enabled by the benchmark crate; the default library API and source path contain
no delay setting or delay branch. Phase 5 changes the private serialized control
schema, so control framing uses `ADETCP02`. The exchange segment wire remains
version 2 and result artifacts remain schema v1.

Bidirectional adaptation, migration of old segments, a coordinator decision RPC,
and one object index per segment were rejected because they add state and
ordering complexity without serving the Phase 5 outcome. A new policy crate
would not improve the existing one-way dependency graph.

## Consequences and boundary

Object suffixes wait for completion; adaptive ingress is serialized; these are
intentional v1 throughput tradeoffs. Disk/object volume has no quota. Per-task
managed memory is still not an RSS cap, and overlapping producer/final tasks
retain separate budgets. All failures still fail and cancel the whole job.
There is no retry, lineage, replay, recovery, dynamic topology or Phase 6 policy.
The release acceptance script predeclares normal and delayed-consumer baselines;
performance evidence stays machine-local and outside CI correctness gates.
