# ADR-0003: Dataset identity and versioning

**Status:** Accepted  
**Date:** 2026-10-15

## Context

Phase 0 needs a small, reproducible measurement substrate before ADE execution exists.

## Decision

Manifest v1 distinguishes generator logical version from Parquet encoding version. Synthetic logical identity hashes canonical generated rows, independent of file shape. Physical identity hashes the complete manifest including relative file paths, sizes, row counts and checksums. Inspect performs full checksums; run defaults to metadata checks, with an explicit --verify-data full mode that checks file checksums once in the parent; children use metadata checks plus independent output validation. TPC-H identity hashes registered file checksums.

## Alternatives considered

Rehashing every large input per measured iteration would distort measurements. Unversioned paths do not identify data.

## Consequences

Schemas and generator semantics must be versioned when their meaning changes. Cargo.lock pins the resolved implementation. Platform resource metrics may be absent.

## Architecture impact

Architecture 29 and replayability INV-8 are preserved. File shape differences produce comparison warnings.

## Follow-up

Validate upgrades with the correctness suite and compare measured results cautiously.
