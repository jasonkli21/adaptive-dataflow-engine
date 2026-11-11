# Phase 21 connector and catalog implementation report

**Status:** COMPLETE_BOUNDED_CONNECTOR_CATALOG_V1. **Date:** 2026-11-11.

## Authorization and source

The implementation request authorized Phase 21 in this session, with one
local commit and no subagents. The starting branch was `main` at
`f49d42d8a4009cce2b22558dfb65cabcd1929c27`, tree
`046a5921e406e1590a2edb853823d7012efb8fb6`. The supplied plan and a separate
Phase 0 prompt were untracked. The plan is included with its implementation
reconciliation. The Phase 0 prompt stays outside the commit.

Phase 18 remains `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`. Phase 20 remains
`PENDING_INDEPENDENT_REACCEPTANCE`. The Phase 21 finite batch lane has no
dependency on either. Its Kafka facade is stateless and independently runs
the admitted native semantic spec, Kafka assignment, progress and receipt
loop. It does not use Phase 18 distributed join, stateful hybrid acceptance
or diagnostic quota claims, or Phase 20 cyclic execution. No predecessor
waiver applies. All Phase 17–20 archives retain their original source and
evidence qualifications.

Candidate 6 has 99 separate process cases with 75 functional cases
and 12 timing pairs. It ran on a Mac15,13 arm64 with eight logical CPUs, 24 GiB
of RAM, APFS and Rust 1.99.1. Its disposable services were Kafka 3.9.1 on
OpenJDK 17, PostgreSQL 17, SeaweedFS 4.34 and PyArrow Flight 21.0.0.
The campaign also used a separate 32 MiB HFS+ image for actual ENOSPC.

The accepted executable and runtime source digests are in the
[manifest](../evidence/phase21/manifest.json). The frozen archive contains the
baseline Git IDs, complete Rust and Cargo inputs, fixture/harness input
digests, raw process output and service identity. The campaign and
implementation ran in the same session. Independent row and protocol
oracles were used. No independent human or agent reviewer is claimed.

## Implementation

[ADR-0028](../adr/0028-connector-catalog-v1.md) records the staging decision.
The separate `ade-connectors` crate owns catalog authority, source lifecycles,
transport bounds and the local sink. Existing native cores never depend on
it. The optional frontend entrypoint registers one verified local physical
`input`. Schema-only SQL binding produces PlanV1 for `ade_exec::physical::execute`.
The existing native preflight and postflight still validate source bytes,
schema and rows. The connector adds early footer, codec, byte-range and
decoded group checks. Cancellations are shared with the local native runtime.
The SDK test implements the public extension with real Parquet in a separate
crate. Native pushdown is disabled.

The local and S3 adapters use independently pinned BLAKE3 inventories.
SeaweedFS 4.34 served signed S3 whole-object reads in a disposable process.
The pinned MinIO binary URL returned HTTP 410. Iceberg verifies format-v1
metadata, Avro manifests, selected ancestry, active files and Parquet field
IDs, then writes canonical staging files. Delta verifies contiguous JSON
history for protocol reader 1/writer 2 and exact active file membership
across add, remove and re-add. PostgreSQL 17 exports bigint from one
ordinary table in a real repeatable-read read-only transaction. Flight uses
real bounded DoGet from an independently implemented PyArrow server. Both
capture adapters declare one-shot replay and expose a materialized content
identity. Kafka preserves explicit assigned starts and the native historical
cut. The real broker accepts a live append after a reached history and idle
barrier.

The local sink caps the IPC stream at 16 MiB. It binds the immutable key,
native plan and payload to the artifact envelope, syncs a staged file, links
it at the selected key without replacing a winner and syncs the directory.
A retry compares the fully verified existing receipt. A directory-sync
failure after linking has an ambiguous receipt that requires reopen. The
operator controls one local directory and retains any independently trusted
receipt. Stage data and interrupted hidden sink files are disposable.
Platform-wide or external exactly-once is outside this contract.

## Verification

| Gate | Result | Evidence |
|---|---|---|
| G0 | PASS | Baseline and exact runtime, harness, fixture and executable hashes with an independent Phase 21 prerequisite disposition. |
| G1 | PASS | Public source and sink lifecycles, separate extension integration test with real Parquet, typed capabilities and strict version/schema grammar. |
| G2 | PASS | Named catalog and explicit snapshot pins, verified ordered splits, metadata/row/schema/content drift refusals and private stage cleanup. |
| G3 | PASS | Native local and signed SeaweedFS S3 Parquet execution, exact row bags, changed bytes, truncated body, credential failure, no-follow redirect and reached timeout. |
| G4 | PASS | Two PyIceberg-produced format-v1 cuts with independent active-file and row checks, mutated metadata and data, schema and partition refusal, v2, deleted entries and malformed manifest/URI. |
| G5 | PASS | Three Delta versions including remove and re-add, checked with delta-rs and independently reconstructed from the JSON logs. Gaps, checkpoint, duplicate protocol, unsafe path and deletion vectors refuse. |
| G6 | PASS | Real Kafka assigned, historical and asynchronously appended live records, idle, restart, semantic identity, broker identity, retention and partition-count refusals. |
| G7 | PASS | Real PostgreSQL transaction and native parity during an overlapping writer, trusted and untrusted TLS, NULL/type/identifier/row-limit refusal, lock cancellation and a disconnect after 260 forwarded data frames. |
| G8 | PASS | Real Flight DoGet, changed one-shot ticket content identity, verified TLS, wrong CA, declared schema and midstream drift, oversize, timeout and cancellation. |
| G9 | PASS | Capped staged and decoded data, S3 one request and capped response, native deadline/cancellation, PostgreSQL raw-wire overflow reached, Flight message bounds and fail-closed source replay. |
| G10 | PASS | Same-key retry and concurrent commit, changed-plan conflict, abort, process kills before selection and after durable selection, independent IPC reopen and actual HFS+ ENOSPC. |
| G11 | PASS | Native SQL and CLI execute PlanV1 in ADE. Connector, source, pushdown, stage and native diagnostics bind to the independently checked result. |
| G12 | PASS | Verified remote auth and TLS where exercised, loopback-only fixture policy, redirect trap unvisited, path traversal and unsupported metadata refusal. Dependency audit shows no added advisory. Existing workspace advisories and transport-specific mitigations are recorded below. |
| G13 | PASS | Workspace feature configurations and native smoke checks pass. Detached Phase 17–20 archive replays match their historical evidence. |
| G14 | PASS | Independent format, service and sink oracles, seven local and five S3 paired process comparisons with raw wall, staging, native managed and RSS observations. |
| G15 | PASS | Matched evidence archive, exact ordered case contract and independent semantic verification. Fully rehashed row, request, inventory, source path, timing, Delta, sink and Kafka mutations refuse. |
| G16 | PASS | Final-source build, format, workspace check, Clippy, tests with and without default features, targeted feature matrix, smoke, Python tests and reproducible operator guide. |

A repeatable verifier command, schema example and supported surface are in
the [operator guide](../phase-21-connectors-guide.md). Raw checks, including
stdout, stderr and exit codes, are in the [evidence directory](../evidence/phase21/).
The prior archive verifiers ran against Phase 17 reacceptance, Phase 18,
Phase 19 review remediation and Phase 20 historical acceptance. The local
legacy archives were verified without rerunning their service campaigns.
No Phase 17–20 archive or gate was rewritten.

The current-source workspace verification used the following commands with
the installed Homebrew OpenSSL include/library paths, `--offline` for Cargo,
and the unchanged lockfile.

```sh
git diff --check
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo test --workspace --all-features --locked --offline
cargo test --workspace --no-default-features --locked --offline
cargo build -p ade-bench -p ade-distributed --bins --locked --offline
bash scripts/smoke.sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'
```

The feature matrix checks `batch`, `delta`, `iceberg`, `flight`, `postgres`,
`kafka` and `s3` individually with default features disabled. The linker emitted
a macOS compact-unwind size warning for several debug DataFusion binaries.
The logs preserve this warning.

## Resource and performance results

Both routes scanned the same 200,000 rows in each pair and matched the full
independent row digest. Pair order alternated. The raw samples include
separate native and staging observations. The host also ran workspace
verification during the campaign. These figures describe the observed local
runs and have no release timing threshold.

| Route | Samples | Wall median (ms) | Wall range (ms) | Stage median (ms) | Peak RSS median (MiB) |
|---|---:|---:|---:|---:|---:|
| Direct local baseline for local staging | 7 | 242.3 | 238.2–245.8 | 0.0 | 42.7 |
| Local staging | 7 | 297.7 | 291.5–299.9 | 36.4 | 43.1 |
| Direct local baseline for S3 staging | 5 | 346.9 | 241.8–573.5 | 0.0 | 42.8 |
| Signed loopback S3 staging | 5 | 674.2 | 448.6–1001.0 | 252.3 | 54.3 |

The staged file and S3 response body were 1,600,536 bytes per
sample. Native managed peak was 1,649,536 bytes in the S3 samples.
The S3 measurement includes connection, signed request and staging costs.
Network headers and packet overhead are outside the payload counter.


The connector caps source bytes and metadata before native input verification,
and separately reports staged bytes, native managed peak and whole-process
RSS. S3 counts one signed GET per split with no transport retry and the
response body equals the staged split bytes in the accepted timing cases.
HTTPS verifies host identity with an explicit address pin. The S3 transport
rejects all non-200 responses before SDK error-body parsing. PostgreSQL bounds
raw inbound traffic to 24 MiB and outbound traffic to 1 MiB. The Flight
message cap is 1 MiB and the encoded stream cap is 16 MiB. The Kafka transport
and offset policy retain their existing bounded native contract.

RustSec audit 0.22.2 against advisory DB
`fd6f39814c4d3c359fdbe223d0280dec4933f78f` reported the same six advisories and
12 unsoundness notices for the baseline and Phase 21 lockfiles. The workspace
audit exits 1 on both. Phase 21 avoids S3 error XML parsing, admits only
uncompressed and Snappy data Parquet, refuses nonprimitive Arrow schemas and
does not use the affected rustix xattr functions. This is a review of the
admitted connector paths. The older dependency findings remain visible in
the audit logs. The local fixture TLS coverage includes trusted and untrusted
CA tests for PostgreSQL and Flight. The S3 service campaign uses signed HTTP
on explicit loopback. Cloud endpoints, IAM, remote commit and platform-wide
portability are outside this evidence envelope.

## Preserved attempts and exclusions

The first five candidate archives are separate from the final archive.
Candidate 1 found that the independent Delta writer includes null placeholders
for protocol feature fields. The reader now admits those placeholders and
refuses non-null features. Candidate 2 failed the timing SQL ordering guard.
Candidate 3 reached the corrected full scan and verified the live Kafka tail,
but the verifier expected a staged file to remain alive after query output
had already moved to sink publication. Candidate 4 added explicit kill-cut
visibility, TLS and wire cap evidence. Its inventory predates the final
format and path negatives. Candidate 5 passed the semantic verifier. Candidate 6
repeated its full case inventory after adding source path and exact source
coverage checks to the verifier. Failed compiler and verifier attempts are
preserved with their exact output where available.

The implemented surface excludes generic JDBC, mutable/unpinned table
discovery, full Iceberg/Delta, Flight SQL, arbitrary custom SQL at the source,
remote distributed scans, source pushdown, CDC, external transactions,
automatic cache management and production HA. No Phase 22 authorization
is inferred.
