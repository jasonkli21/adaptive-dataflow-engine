# ADR-0002: Benchmark result schema

**Status:** Accepted  
**Date:** 2026-10-15

## Context

Phase 0 needs a small, reproducible measurement substrate before ADE execution exists.

## Decision

Serde JSON v1 records the full workload spec, manifest identity, environment/build/repository/engine configuration, warmup outcomes, every measured child outcome, query and process wall times, correctness, and R-7 descriptive statistics. Repository commit and dirty state are captured by `build.rs` from the ADE workspace root and embedded in the executable; runtime working directories never determine source identity. Source archives and builds outside the ADE Git root retain null repository fields.

The existing `build.codegen_overrides` field records effective known performance-sensitive settings from the selected Cargo profile plus whitelisted explicit codegen flags. This includes profile-derived optimization level, debug information, LTO and codegen units when known, and explicit target CPU/features. It does not include build paths or arbitrary environment variables. The JSON structure and schema version remain unchanged because the field population is corrected without changing its type or the result contract.

Invalid runs retain diagnostics and cannot be compared. Files are immutable: atomic no-clobber publication.

## Alternatives considered

A benchmark database and best-iteration reporting add complexity or hide variance.

## Consequences

Schemas and generator semantics must be versioned when their meaning changes. Cargo.lock pins the resolved implementation. Platform resource metrics may be absent.

## Architecture impact

Architecture 29.3 and correctness invariant INV-3 are preserved.

## Follow-up

Validate upgrades with the correctness suite and compare measured results cautiously.
