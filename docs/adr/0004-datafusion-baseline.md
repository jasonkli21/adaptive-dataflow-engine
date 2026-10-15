# ADR-0004: DataFusion baseline strategy

**Status:** Accepted  
**Date:** 2026-10-15

## Context

Phase 0 needs a small, reproducible measurement substrate before ADE execution exists.

## Decision

Pin DataFusion 55.2.0 with parquet/sql public APIs and Arrow/Parquet 59.3.1. Raise workspace MSRV from scaffold 1.85 to DataFusion-required 1.94 and check it in a dedicated CI job. The CLI dispatches an explicit `EngineRequest`; each adapter owns parsing its raw options and its configuration type. The shared correctness/timing harness is generic over the adapter's associated configuration type. The child protocol always receives the selected engine name and options. This is a small static dispatcher, not a dynamic plugin system.

DataFusion's partitions, batch size and statistics settings remain explicit and are recorded. Synthetic validation uses an independent integer oracle. TPC-H Q1 uses direct Parquet reference reduction, normalizes the supported ship-date input to Arrow `Date32`, compares integer days against the typed cutoff, and applies tolerance-aware numeric checks. Build result metadata records the effective known Cargo profile settings in addition to explicit codegen flags.

## Alternatives considered

Polars is deferred. Private APIs, a generic plugin framework, and an ADE runtime are excluded.

## Consequences

Schemas and generator semantics must be versioned when their meaning changes. Cargo.lock pins the resolved implementation. Platform resource metrics may be absent.

## Architecture impact

Architecture 26.6, 29.2 and 33 are preserved: the benchmark boundary is adapter-neutral, DataFusion configuration stays inside its adapter, and Phase 0 contains no ADE execution path. Release profile uses thin LTO, debug=1, one codegen unit; those effective settings are present in result metadata. The default allocator and portable CPU target remain in use.

## Follow-up

Validate upgrades with the correctness suite and compare measured results cautiously.

## API references inspected

- [DataFusion public API](https://docs.rs/datafusion/55.2.0/datafusion/) and downloaded 55.2.0 source: SessionConfig, SessionContext, read_parquet, into_view, register_table, SQL and collect. Crates.io resolved 55.2.0 after the web documentation initially indexed 55.1.0.
- [Parquet ArrowWriter](https://docs.rs/parquet/59.3.1/parquet/arrow/struct.ArrowWriter.html) and downloaded 59.3.1 writer/reader source.
- [tpchgen-cli 3.0.0](https://docs.rs/crate/tpchgen-cli/3.0.0): downloaded CLI source verified parquet, scale-factor, output-dir and quiet flags.
