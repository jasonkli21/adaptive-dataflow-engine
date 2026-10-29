# Architecture Decision Records

Use ADRs for concrete technical decisions that matter beyond one function or file.

## Numbering

Use monotonically increasing four-digit IDs:

```text
0001-phase-0-workspace.md
0002-benchmark-result-schema.md
```

Phase 0 is expected to create ADR-0001 through ADR-0004 described in the implementation plan.

## Template

```md
# ADR-NNNN: Title

**Status:** Proposed | Accepted | Superseded
**Date:** YYYY-MM-DD

## Context

What problem/constraint requires a decision?

## Decision

What are we choosing?

## Alternatives considered

What credible alternatives were rejected and why?

## Consequences

What becomes easier/harder? What new constraints exist?

## Architecture impact

Which PRD/architecture sections or invariants are affected?

## Follow-up

What work or future validation does this decision require?
```

An ADR should not duplicate a large plan. Keep it focused on the decision and rationale.

- [ADR-0017: Bounded file-morsel scheduler v1](0017-morsel-scheduler-v1.md)

- [ADR-0018: Physical IR and bounded frontend v1](0018-physical-ir-and-frontend-v1.md)

- [ADR-0019: Bounded durable batch operations](0019-durable-batch-operations.md) — Phase12 bounded v1 complete; same-host fixed workers, full HA deferred.

- [ADR-0020: Bounded remote disaggregated shuffle v1](0020-remote-disaggregated-shuffle-v1.md) — opt-in signed S3 exchange, immutable manifests, fresh reader stages and scoped quiesced GC.

- [ADR-0022: Bounded keyed state and local checkpoints v1](0022-keyed-state-and-checkpoints-v1.md) — strict stateful admission, tiering, quiescent cuts and logical restore rescale.
