# Phase 24 independent review remediation — 2026-11-14

**Current qualification: `REMEDIATION_REQUIRED`.** The original Phase 24
implementation report and candidate archive remain historical and unchanged.
This additive remediation addresses the callback identity collision and graph
terminal structural validation findings in source. It does not close the
hosted CI blocker or establish independent release acceptance.

## Frozen review boundary

The review inspected target `7587484cad78c1ac53d2de4147301f2ba3ec3618`, tree
`cf93fd45ec903037bcd4111445b9082aa0b545e9`, with parent
`95e1153b2b75fbe8d9e2445e96b5f9081d293cfa`. The exact-target GitHub Actions
run `43652571438` failed in the `rust` job's existing
`coordinator_kills_recover_exactly_one_native_result_across_both_shapes`
test at `runner::gc`; the Phase 24 standalone consumer and native process steps
were skipped. This is a confirmed CI failure, not evidence that Phase 24 caused
the underlying `ade-ops` failure.

The historical candidate, its seal, attempts and local-check archive were not
modified. The pre-existing untracked
`sol-medium-phase-0-bootstrap-implementation-prompt.md` was left untouched.

## Source changes

- **P24-F02:** `Receipt` now carries an implementation fingerprint derived
  from the measured executable digest and `type_name::<E>()`. The logical ID
  includes that fingerprint. This distinguishes separately selected Rust
  extension types in one executable and changed executable builds. The ADR and
  guide explicitly say this is provenance, not cryptographic attestation or a
  sandbox. Consumers must not treat logical IDs as proof of callback behavior
  or as unrestricted cache keys.
- **P24-F05:** graph terminal validation now checks that each component leader
  exists in the terminal vertex set and is the minimum vertex assigned to its
  group, before staging. This is structural validation only; it does not prove
  that labels match connectivity in the original graph. The independent
  original-edge union-find process oracle remains required.

## Evidence from this remediation

Commands used the installed stable Rust 1.99 toolchain directly because the
Rustup proxy cannot remove its protected `rustup-init` setup file.

| Check | Result |
| --- | --- |
| Standalone external consumer tests, locked/offline | PASS; same-binary duplicate-descriptor test confirms equal executable digests but distinct implementation, logical and result digests. |
| `ade-ext-graph` local-v1 tests, locked/offline | PASS; rejects a foreign leader, a nonminimum group leader and cardinality mismatch; accepts a valid group. |
| `cargo fmt --all -- --check` | PASS. |
| Workspace default-feature tests, locked/offline | PASS outside the restricted sandbox. |
| Workspace all-feature tests, locked/offline | PASS outside the restricted sandbox, including all 13 `native_crash` tests and the Phase 24 graph structural regression. |
| Workspace all-target/all-feature check, locked/offline | PASS. |
| Workspace all-target/all-feature strict Clippy, locked/offline | PASS. |
| Rust 1.94 workspace all-feature check, locked/offline | PASS. |
| Copied external consumer under a separate temporary directory | PASS, locked/offline against the public SDK source path. |
| `python3 -m unittest discover -s scripts/tests -p 'test_*.py'` | PASS, 76 tests. |
| Initial package-scoped strict Clippy attempt | FAIL before reaching all targets: Rust 1.99 Clippy reports `clippy::unnecessary_wraps` in dependency `crates/ade-iterate/src/lib.rs:204` under that narrower feature graph. The full required workspace Clippy command passes; unrelated source was not changed. |
| Exact-target hosted Linux CI failure | CONFIRMED from supplied independent review witness; Phase 24-specific steps skipped. |
| Isolated native crash test on this host | The sandboxed attempt failed before the target assertion with `Operation not permitted` while opening worker IPC. The out-of-sandbox all-feature workspace run passed the target test. This macOS pass does not diagnose or clear the Linux `Owned` failure. |
| Fresh Linux workspace tests and hosted CI for this remediation commit | NOT_RUN. |
| Fresh full graph original-edge process oracle, detached mutation campaign, Linux ENOSPC/fault matrix, independent GitHub receipt, MSRV and full workspace matrix | NOT_RUN. |

## Gate disposition

F02 and F05 have focused source changes and regression tests, but their full
source-bound campaigns remain pending. F01 remains a release blocker because
its Linux ownership failure was not reproduced or fixed here. F03/F04/F06/F07/
F08 and independent G15/G16/G19/G21 evidence remain open. No gate is promoted
to accepted based on the historical archive or the new focused tests. The
support matrix records `REMEDIATION_REQUIRED` and retains all prior source and
evidence identities.

The remaining work requires a clean Linux checkout and a successful hosted run
for the exact final commit. No upstream push or merge was performed.
