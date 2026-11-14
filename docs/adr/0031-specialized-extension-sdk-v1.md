# ADR-0031 — Bounded specialized extension SDK v1

Accepted for implementation 2026-11-14. Release qualification is separate.
Refines architecture §§7–8, 17, 19, 22–23, 25–26 and INV-1, INV-4–9.
Baseline 95e1153b2b75fbe8d9e2445e96b5f9081d293cfa.

## Scope and predecessor decision

PRD §§16 and 22 place domain libraries outside core and list possible families.
Native component sizes adds a reduction that the closed cyclic template cannot
express. Native per-row vector preparation and affine scoring exercise the
separate acyclic mechanism, including Float64 carried through PlanV1.
An independent Rust consumer establishes extensibility beyond these two owners.
General training, CEP, geospatial and ANN require contracts outside these slices.

The submitted Phase 24 handoff permits a scoped local prerequisite carve-out.
Use the Phase 20 native weak-components execution and complete independently
pinned terminal read only, subject to fresh Phase 24 correctness/lifecycle proof.
No Phase 22 runtime is used. The carve-out leaves every predecessor support
matrix and historical archive intact. It cannot substitute for G16 hosted CI.

| Option | Decision |
| --- | --- |
| Direct execute_with_map | Supports exact native schema and plan; accepts arbitrary BatchMap implementations and only checks batch size/schema, so row correlation, identity, replay and extension quotas remain with each caller. |
| Versioned adapter atop execute_with_map | Adopt. A scalar row kernel receives no batch-retention or row-placement authority. The SDK owns ordered Arrow buffers, source/plan binding, contract negotiation, result identity and deadline. Graph composes separately through existing public APIs. |
| New extension IR | Defer. General shape-changing, remote or cyclic plugin execution needs ownership and progress semantics plus a loader/security model. It would change more core surfaces without improving either finite slice. |

## Contracts

The compile-time v1 boundary hosts one explicitly selected trusted Rust kernel
per call. The expected descriptor must match its implementation. The SDK
accepts one exact required primitive schema with unique ascending UInt64 row
IDs in column zero. The row callback may replace declared signed-integer or
finite Float64 slots in the current row. The SDK builds mapped arrays in source
order and carries the same row ID array through projection. It checks type,
finiteness and bit preservation for other fields. It supplies verified model
bytes to the kernel and pins a measured running executable separately from
logical source, contract, model and canonical semantic plan. Attempt UUID,
output cap, batch size and timeout are operational parameters. Canonical result
hashing uses row-major primitive little-endian bits across batch splits. The
SDK's implementation fingerprint hashes the pinned executable digest and the
selected Rust extension type name; that fingerprint participates in the
logical identity and is present in the receipt. This disambiguates distinct
statically selected extension types in one process and different executable
builds. It is provenance, not cryptographic callback attestation: trusted code
can still misrepresent its descriptor or behavior. Consumers must not treat
the logical ID as proof of code authenticity or reuse results outside the
trusted installation context and pinned source/model/executable.

The caller pins immutable operator-controlled local files and installed
executables. Linux can hash /proc/self/exe. macOS relies on operator-immutable
installation when hashing current_exe(). Static code can miscompute, block,
panic, retain buffers or otherwise affect the process. The adapter owns only
its explicit data allocations and cooperative checks. It cannot sandbox code,
cryptographically attribute behavior to a callback, or cap allocator/OS RSS.
Retained native result and conservative map array accounting are independently
bounded.

Graph receives a caller-authenticated Phase 20 pin and reads the selected
terminal through the public in-process API. Terminal row count, digest and
native terminal summary must agree. Before staging, every reported component
leader must be an observed vertex and the minimum vertex in its reported group.
A bounded private one-file Parquet stage
feeds a verified PlanV1 scan and real native HashAggregate. The result is
caller-owned memory after stage cleanup, with no result file or durable
success marker. Interrupted stages are private orphans, never implicit
authority. The Phase 20 root follows its original ownership and selector rules.

Existing formats and defaults remain untouched. Rust API versioning does not
imply a binary plugin ABI. Floating-point operation order is specified with
independent tolerance and explicit signed-zero behavior; portability of
bit-identical results across compiler/CPU releases is outside the contract.
