# ADR-0001: Minimal workspace

**Status:** Accepted  
**Date:** 2026-10-15

## Context

Phase 0 needs a small, reproducible measurement substrate before ADE execution exists.

## Decision

One ade-bench crate owns CLI, schemas, dataset management, adapter, recorder, and comparison. No future engine crates are created.

## Alternatives considered

Splitting unused shared crates would freeze speculative interfaces.

## Consequences

Schemas and generator semantics must be versioned when their meaning changes. Cargo.lock pins the resolved implementation. Platform resource metrics may be absent.

## Architecture impact

Architecture sections 25, 33 and the Phase 0 stop boundary are preserved.

## Follow-up

Validate upgrades with the correctness suite and compare measured results cautiously.
