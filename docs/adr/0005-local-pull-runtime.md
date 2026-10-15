# ADR-0005: Local runtime crate and synchronous pull execution

**Status:** Accepted
**Date:** 2026-10-15

## Decision

`ade-exec` owns the provisional local plan, Arrow batch operators, executor,
cancellation, deterministic postorder operator IDs, metrics and observational
memory hooks. It depends on Arrow/Parquet public APIs and thiserror, never
DataFusion or ade-bench. The benchmark harness depends on its public entrypoint.
Arrow/Parquet versions are shared workspace dependencies; no upgrade is made.

Use synchronous recursive pull execution. Each consumer requests one upstream
batch at a time, providing natural backpressure without channels or a scheduler.
Hash aggregation blocks while building a hash table, then sorts keys for
reproducible output. It uses checked i128 accumulation, checked UInt64 counts,
and checked Int64 output conversion. Aggregate null inputs are rejected; filter
null predicates exclude rows. Projection and limit reuse Arrow buffers.

Memory hooks observe batch buffer estimates, aggregate hash capacity (64 bytes
per capacity slot), and retained collected output. They do not measure allocator
RSS, enforce limits or trigger spill. Operator timing excludes upstream pulls.
Detailed metrics are returned programmatically and optionally logged at debug;
benchmark result schema v1 is unchanged.

## Rationale and alternatives

A separate crate enforces frontend independence and keeps benchmark semantics
outside runtime operators. Async channels, a general expression system and a
stable cross-phase physical IR would add commitments without a Phase 1 need.
This model can change when networking or streaming requires it.

## Architecture impact

Refines architecture sections 5.3, 7–8 and INV-4: the local plan is provisional;
backpressure is synchronous; memory is accounted observationally now, while hard
reservations, bounded aggregate state and spill remain Phase 2. Preserves INV-6
and INV-10. ADR-0001/0004's exclusion of an ADE runtime applied to Phase 0;
Phase 1 now adds only the one crate with real runtime ownership.

## Consequences

Collection and aggregation use memory proportional to result/group cardinality.
Observed memory can count shared Arrow buffers more than once across operators
and collection; Parquet decoder/allocator overhead is excluded. Aggregate
capacity is sampled after each input batch, so within-batch allocation peaks and
finalization temporaries are approximate. These values are useful attribution
hooks, not a bound. Phase 2 must evolve the hooks into reservations before
claiming a memory guarantee.
