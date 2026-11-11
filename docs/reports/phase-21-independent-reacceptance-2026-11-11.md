# Phase 21 independent review remediation — 2026-11-11

**Disposition:** `PARTIAL_REMEDIATION_PENDING_REACCEPTANCE`.

This additive report responds to the attached independent review of
`c48c97cf3c1801aa780d32b51cc6ccc173042f2a`. The Phase 21 candidate 6 report,
archive, source inventory, criteria and all candidate 1–5 attempts remain
unchanged. Phase 18 remains `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`; Phase 20
remains `PENDING_INDEPENDENT_REACCEPTANCE`. No predecessor waiver or new phase
authorization is inferred.

The remediation runtime inventory currently contains 194 Rust/Cargo/harness
inputs with SHA-256
`67c3715b0a110721f06a473962bfbf8881ca7cc495c1c42412bb38cfac421189`. The
candidate 6 inventory `4d54771c...` remains recorded only in its immutable
manifest. No executable or candidate 7 service archive is claimed for this
inventory.

## Remediation disposition

| Finding | Change in this remediation | Current evidence and remaining qualification |
|---|---|---|
| F21-01 negative evidence | Candidate campaign now archives the exact changed local Parquet, Iceberg data Parquet and S3 response body before restoring fixtures. Strict current-source verification checks the request against its positive counterpart, checks the exact byte mutation, and checks the typed snapshot-mismatch message. The rehashed `negative_without_mutation` tamper is included. | Code and verifier tests are being checked on this source. Candidate 7 was not run here, so G15 and affected G3/G4 subsets remain `PARTIAL`. Historical candidate 6 lacks these witnesses and remains historical only. |
| F21-02 Linux CI | The Linux job installs `cmake`, `libcurl4-openssl-dev`, `libsasl2-dev` and `pkg-config` before all-feature Rust checks. | This Mac session cannot establish a green GitHub Linux run. G16 remains `PARTIAL` until every Linux job runs on this source; the previous run is preserved as an inherited failure. |
| F21-03 Iceberg accounting | Stage diagnostics now distinguish downloaded source bytes, final physical stage bytes, normalization bytes written and the high-water estimate of owned stage file lengths. Each source and normalized representation remains separately capped; the guide states that normalization peak may exceed either cap and excludes filesystem allocation overhead. | The estimate follows owned file lengths and is not an OS quota reservation or an independent filesystem high-water measurement. A matched Iceberg ENOSPC/cancellation campaign was not run; G9/G14 remain `PARTIAL`. |
| F21-04 sink path ownership | `LocalSink` retains its opened directory descriptor, requires an effective-user-owned root that is not group/world writable, publishes and reopens relative to that descriptor, verifies the staged inode, and syncs the anchored directory. A rename/path-replacement regression test ensures publication stays in the originally opened directory. | This covers a renamed/replaced path while the descriptor remains open; it does not claim safety against a hostile process running as the same user with access to the directory. Fresh cross-platform and fault campaign evidence remains pending; G10/G12 remain `PARTIAL`. |
| F21-05 one-shot catalog lifecycle | Operator guide and support matrix now describe PostgreSQL and Flight as caller-authorized one-shot capture, not catalog-managed discovery or authorization. Callers must authorize exact endpoint/table or ticket/schema before capture. | A uniform registered one-shot API was not added. The bounded claim is narrowed to the public mechanism actually implemented; G1/G2 remain `PARTIAL` for uniform catalog semantics. |
| F21-06 evidence attribution | Verifier requires an explicit `--mode`. `current-source` requires both `--repo` and `--binary`; `historical-archive` explicitly makes no live source or binary attribution claim. | No new acceptance archive, independently retained receipt, live services or source-matched executable was produced in this session. G0/G14/G15 remain `PARTIAL`. |

The support matrix now classifies the affected gates as partial pending fresh
source-bound evidence. The original candidate 6 all-PASS matrix is not reused
as current-source acceptance.

## Verification run in this remediation

`cargo fmt --all` and `cargo check -p ade-connectors --all-features --locked
--offline` were run on macOS arm64 with Rust 1.99.1. The initial check identified
that rustix's `process` feature was required for the sink owner check; it was
enabled and the check completed. The focused all-feature connector test and
repository Python suite completed (72 tests passed). The Phase 21 archive
verifier itself could not import because this system Python lacks `blake3`;
the Python syntax check passed. `cargo test -p ade-connectors --all-features
--locked --offline` passed all four connector tests, including the new root
rename regression. The linker emitted the existing macOS compact-unwind size
warning. No Linux runner, Kafka,
PostgreSQL, SeaweedFS, Flight service, or candidate 7 acceptance campaign has
been run from the remediation source. No prior phase evidence was modified.

The connector-only all-feature Clippy gate with `-D warnings`, the
no-default-feature connector check, `cargo fmt --all -- --check`, `git diff
--check`, Python syntax parsing and support-matrix JSON parsing also passed.
Workspace all-target check and all-feature Clippy passed. The all-feature
workspace test command reached 67 passing and 29 failing `ade-distributed`
tests; every shown failure was an OS `PermissionDenied` while opening loopback
sockets in this sandbox. The connector tests, including the rename regression,
passed within that workspace run. The repository Python suite was rerun after
the verifier syntax correction and passed all 72 tests.

The implementation changes require a fresh full verification and service
campaign using `--mode current-source`, a newly pinned candidate archive, and a
matched probe executable before any affected gate can return to PASS.
