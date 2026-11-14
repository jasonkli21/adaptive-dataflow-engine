# Phase 22 independent review remediation — 2026-11-14

**Current status: `PARTIAL_REMEDIATION_PENDING_REACCEPTANCE`.** The original
candidate6 report and archives remain immutable evidence for their recorded
source and executables. This additive remediation does not reclassify that
historical evidence or restore the release label for the current source.

## Review disposition

- **F22-01, addressed in source:** `SpillAggregate::flush` now iterates the
  partition path slots with `iter_mut().enumerate()`. Partition order, the
  `hash(key) & 15` ownership rule, lazy path creation, append behavior and byte
  accounting are unchanged. No lint allowance or CI weakening was added.
- **F22-02, open:** no full source-and-executable-matched campaign was run for
  this remediation source. The old candidate6 archive remains historical.
- **F22-03/F22-04, bounded trust assumption retained; probe not run:** the
  service remains for trusted, operator-managed deployments. Interpreter path
  replacement between verification and spawn and mutable imported modules
  have not been hardened or tested here. The script digest is not a
  transitive Python environment digest and does not provide hostile-code
  isolation. Treat the installation, dependencies and host as immutable
  operator-owned inputs for this profile.
- **F22-05, open investigation:** the existing transport config has a 120
  second HTTP/2 connection age plus 15 second grace. A stalled-reader and idle
  TCP test was not run, so this is not claimed as a measured permit-recovery
  bound. Ticket TTL applies to initial redemption; a redeemed stream's
  lifetime remains unqualified.
- **F22-06, open:** the generator and expected rows still share `row()`.
  No supplemental literal fixture was added, and independent fixture
  provenance remains a reacceptance requirement.

## Verification performed

The checkout was based on reviewed commit `54973e90b478318484363f7bf0a66195fa75ee9a`.
The implementation edit is limited to `crates/ade-exec/src/aggregate.rs`;
the remaining edits in this commit reconcile status and record this review.
The current 221-file Phase22 runtime input inventory hashes to
`6febea9b035467905b4f22eca10d6ea35b59169d2b466df14516bee19568c47b`.

| Check | Result |
|---|---|
| `cargo fmt --all -- --check` | PASS, Rust 1.99.1 |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | PASS, Rust 1.99.1 |
| `cargo test --workspace --all-features --locked` | PASS, Rust 1.99.1; rerun with approved local socket access after 29 sandbox socket denials. |
| `cargo check --workspace --all-targets --locked` | PASS, Rust 1.99.1 and Rust 1.94.0 MSRV |
| `cargo test -p ade-gateway --no-default-features --locked` | PASS, Rust 1.99.1 |
| `cargo test -p ade-exec --no-default-features --locked` | PASS, 35 tests, Rust 1.99.1 |
| `bash scripts/smoke.sh` | PASS correctness/schema smoke; one-sample comparison reported a regression classification, which is not a performance gate. |
| `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'` | PASS, 72 tests |
| Python/Java Phase 22 live campaign | BLOCKED: PyArrow, blake3 Python package, protobuf and Java/Maven are unavailable in this environment. |
| Full fault/resource/ENOSPC campaign and current-source verifier | NOT RUN; no disposable mounted filesystem or campaign prerequisites were available. |
| Final-SHA GitHub Actions | NOT RUN from this workspace. The reviewed SHA's failed run `43622953656` remains historical evidence. |

The test suite's only implementation change is an iterator refactor; existing
aggregate spill, partition ownership, spill reader and round-trip tests remain
the regression coverage. No new fault, identity, availability or fixture
acceptance is claimed.

## Release boundary

Keep G15 **FAIL** pending a green required workflow on the new final SHA. G0,
G1–G14 are **PARTIAL** for current-source independent reacceptance until the
matched live campaign, fault/resource probes, independent fixture, current
source verifier, tamper suite and separately retained receipt pin are run.
The original candidate6 archive, criteria, fixture lock, predecessors and
their measured identities must remain unchanged. Phase18, Phase20 and Phase21
retain their separate qualifications. No Phase23 authorization is inferred.
