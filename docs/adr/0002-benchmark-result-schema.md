# ADR-0002: Benchmark result schema

**Status:** Accepted  
**Date:** 2026-10-15

## Context

Phase 0 needs a small, reproducible measurement substrate before ADE execution exists.

## Decision

Serde JSON v1 records full semantic spec, manifest identity, environment/build/repository/engine configuration, warmup outcomes, every measured child outcome, query and process wall times, correctness, and R-7 descriptive statistics. Invalid runs retain diagnostics and cannot be compared. Files are immutable: atomic no-clobber publication.

## Alternatives considered

A benchmark database and best-iteration reporting add complexity or hide variance.

## Consequences

Schemas and generator semantics must be versioned when their meaning changes. Cargo.lock pins the resolved implementation. Platform resource metrics may be absent.

## Architecture impact

Architecture 29.3 and correctness invariant INV-3 are preserved.

## Follow-up

Validate upgrades with the correctness suite and compare measured results cautiously.
