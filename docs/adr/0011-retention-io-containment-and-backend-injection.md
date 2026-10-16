# ADR-0011: Retention I/O containment and object-store injection

**Status:** Accepted\
**Date:** 2026-10-16

## Context

Independent review of the Phase 4 implementation found that spill file calls
held the exchange queue mutex and that object-store futures had no operation
deadline or cancellation race. It also found that workers always created an
ephemeral local object store, current retention pressure was not observable, and
nearby comments still described Phase 3 task/lifecycle rules.

## Decision

- Keep spill queue/accounting state behind short mutex sections. A private
  detached I/O owner serializes spill-log reads/writes; callers wait with
  cancellation checks and a five-second operation deadline. If a blocking call
  cannot stop, the session is cancelled and the worker is poisoned. Poisoned
  workers reject a later `PrepareJob`; their I/O helper is not joined during
  worker shutdown.
- Run each object-store operation on a detached, per-operation Tokio owner.
  The exchange caller waits on a bounded response channel while checking the
  session cancellation token and five-second deadline. A cancelled, timed-out,
  disconnected or failed operation poisons the worker. The detached owner keeps
  the backend and any ephemeral filesystem root alive until provider work ends;
  a provider that blocks synchronously inside its first future poll can outlive
  the failed session without holding its receiver or worker teardown path.
- Allow callers to install an `ObjectStoreBackend` when starting a worker. Each
  job names the expected backend and declares its durability capability in
  `ExchangeConfig`; effective task metrics report those values. The worker
  binary's default remains an execution-scoped local filesystem fixture marked
  ephemeral. Durability metadata is a caller assertion, not a verification by
  ADE.
- Expose live current and peak in-memory bytes and current spill backlog through
  `RetentionMetrics`. A successful final task must observe both current values
  at zero.
- Keep hash routing, transfer/segment identity, five static modes, result schema
  v1 and the Phase 4 no-recovery boundary unchanged.

## Alternatives considered

- Holding the queue mutex while doing file I/O was rejected because a slow call
  blocks all queue progress and cancellation observation.
- Treating a timed-out worker as reusable was rejected because the underlying
  filesystem/provider operation may still be running. Poisoning preserves a
  clear failure boundary without claiming that the syscall itself is
  preemptible.
- Hardwiring `LocalFileSystem` was rejected because it prevented callers from
  supplying a backend with a different declared capability through the same
  exchange path.
- Adding a Phase 5 selector or recovery from persisted objects is outside this
  remediation and remains deferred.

## Consequences

Spill and object-store waiters return after cancellation/deadline even if an
underlying blocking helper remains active. Object operations use at most one
helper per bounded receiver operation; poisoning prevents later work from
starting on that worker. Such a worker cannot accept another job and must be
shut down/replaced by its owner. Injected backend creation and
credentials belong to the worker caller. The default benchmark worker continues
to use local ephemeral storage. Live metrics do not change benchmark result
schema v1.

## Architecture impact

Refines architecture sections 3.1, 3.4, 3.7, 9.1, 9.4, 10.2 and invariants
INV-2, INV-4, INV-5, INV-7 and INV-10. It does not add policy, retry, recovery,
durability guarantees or new exchange semantics.

## Follow-up

Failure-injection tests cover blocked spill writes/reads and stalled object-store
puts/gets. Existing Phase 4 process and five-mode release acceptance must be
rerun after this remediation. Cloud durability and cloud-provider performance
remain unverified.
