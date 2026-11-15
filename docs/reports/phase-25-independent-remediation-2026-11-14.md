# Phase 25 independent review remediation — 2026-11-14

**Current qualification: `PARTIAL_PHASE25_REVIEW_REMEDIATION_PENDING_REACCEPTANCE`.**
The reviewed candidate, its report, archive, seal, observations, and predecessor
status files are historical and unchanged. This report records code changes and
remaining proof; it does not promote Phase 25.

## Baseline and review disposition

The checkout began at reviewed commit `e237d55ea7b18587d5b5b85cf46c874447dc00d3`
and tree `e82382137c18187f06d5a818b0ca8a58d3a9df46`, clean except for the
pre-existing untracked `sol-medium-phase-0-bootstrap-implementation-prompt.md`.
That file was left untouched. The historical archive was listed without
extracting or modifying it: it contains process records and profiles, but no
original executable or Parquet fixture files. Thus the archive alone cannot
support detached replay from a fresh checkout.

| Finding | Remediation / current disposition |
| --- | --- |
| P25-F01 | Preserved the negative economics result. No whole-query win or measured-auto promotion is claimed; Phase 25 stays partial. |
| P25-F02 | Narrowed ADR/operator claims: auto compares scalar and NEON only; Arrow remains a control and cannot be auto-selected. Three-way policy remains unimplemented. |
| P25-F03 | Documented the trusted-operator boundary. Profile digest is an integrity pin, not an issuer signature; admission's data and executable hashing cost is included and was expensive in the recorded run. |
| P25-F04 | Fixed the feature-on source compatibility break. `AdeLocalConfig` is again the original two-field public struct. Kernel settings now use a separate feature-gated config and engine. Added an external consumer compile/test fixture. |
| P25-F05 | Still pending: exact-final-commit hosted CI has not run. Workflow steps now test the feature-on external consumer and portable native process path. |
| P25-F06 | Added a native-plan regression that scans two Parquet files in multiple batches and compares every projected field in order to direct source-array row selection. The handoff's stronger independent ordered process oracle against the detached original fixtures remains pending. |
| P25-F07 | Added a portable feature-on native process smoke and external-client job. A detached verifier/tamper CI lane is still outstanding. |
| P25-F08 | No held-out policy evidence was produced. Same-shape calibration and full rehash admission remain an uneconomic, operator-trusted hint; no beneficial auto claim is made. |
| P25-F09 | Confirmed the checked-in archive omits executable and fixtures. No recoverable historical bundle is claimed. A fresh, final-source candidate with retained executable/fixtures and detached verification is required. |

The plan reconciliation records gates G18–G22 separately. G20 remains open for
trusted, held-out, Arrow-aware policy evidence; G21 awaits exact-SHA hosted CI;
G22 awaits detached artifact retrieval. The existing physical Apple M3 run is
producer evidence for its historical source, not independent current-source
execution. NUMA, dual-architecture economics, GPU, and JIT/fusion remain deferred
or not run. No predecessor phase status changed.

## Implementation details

The legacy `AdeLocalEngine` continues to accept `AdeLocalConfig`. Explicit
kernel options use `AdeLocalKernelEngine` and `AdeLocalKernelConfig`, which wrap
the legacy configuration without changing its public literal shape or the
public `ResolvedEngine` variants. The benchmark result schema, defaults,
PlanV1, and filter mechanism are unchanged. `.github/workflows/ci.yml` runs the
external consumer fixture and portable process smoke on the hosted Rust job. The new native-plan test exercises the filter through
`LocalPlan::Filter`, reading multiple original Parquet files with a batch size
that splits records across batches and checking ordered values and nulls.

## Checks run in this remediation

The rustup proxy attempted to remove a protected setup file, so installed Rust
toolchain binaries were invoked directly. The feature-on `ade-bench` check and
the default-feature `ade-bench --all-targets` check passed. Package Clippy with
all features and `-D warnings` passed. The focused `ade-exec` filter suite
passed (5 tests), including the new
multi-file/multi-batch ordering test. The feature-on external consumer test
passed (1 test). Its initial online dependency resolution was blocked by
unavailable crates.io DNS; it was resolved and tested offline from the local
cache. The feature-on portable process smoke passed scalar/Arrow result identity
and forced-AVX2 refusal. The Python verifier unit suite passed (79 tests).
`cargo fmt` through cargo subcommand discovery remains unavailable due
the rustup setup-file error; edited Rust files were formatted with the installed
stable `rustfmt` binary and `git diff --check` passed.

This is not a full workspace regression, physical ARM remeasurement, detached
campaign, hosted CI result, or release receipt. These remain explicit
reacceptance requirements.
