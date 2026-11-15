# Phase 25 implementation report

**Qualification `PARTIAL_PHASE25_LOCAL_NEON_NO_CALIBRATED_WIN_HOSTED_CI_PENDING`.**
The opt-in implementation ran on a physical Apple M3. Measured auto retained
scalar. The one-commit handoff is local and exact-commit hosted CI was outside
the authorized release action.

## Source and scope

The starting branch was `main` at
`4e0f298b4b0dae7f6ab7398e3f4f590c1a33b4d8`, tree
`12c13873a16c7810857e51a6ff11ae4576fff920`. It matched the handoff's inspected
baseline. The submitted Phase 25 plan and the unrelated Phase 0 prompt were
untracked at the start. The Phase 25 plan is included in the handoff commit.
Git records the final commit and tree for this report. The separate
post-commit receipt reports those IDs without a self-referential commit hash
inside the tracked report.

The archive pins the exact Rust/Cargo inputs, experiment scripts,
workload/spec variants, original dataset bytes and executable used for the
local campaign. The build's embedded Git revision describes the parent checkout
with Phase 25 source edits present. It is paired with the full measured input
inventory so the parent revision cannot stand in for the measured source.
The final commit's runtime files were checked against that inventory.

`ade-exec/filter-kernels-v1` adds typed forced ISA refusal, a bounded ARM64
bitmap implementation, and a measured selection policy on the ordinary
`LocalPlan::Filter` call site. `ade-bench/filter-kernels-v1` owns its explicit
CLI options, profile preparation and separate stderr audit. The new native
entry point uses the existing output collector and propagates failures before
successful publication. No changes to PlanV1, benchmark result v1, source
identity, streaming or distributed protocols were needed. The existing entry
points and `ExecutionConfig` struct literals still work with the feature
enabled. The benchmark adapter's feature-enabled config is built by its option
parser since it carries adapter-owned profile options.

The initial scalar/Arrow release comparison and standalone NEON prototype
justify the candidate on mask mechanism cost. The first integrated archive
ran all cases but the detached verifier incorrectly treated probe and retry
rows as calibration trials. The original archive and failure remain in
`attempts/`. The corrected verifier explicitly excludes nonsampled trials and
the Python regression suite covers that case. The final campaign used the
unchanged release executable and a fresh run with the corrected evidence
contract.

Phase 18 stays `PARTIAL_PHASE18_UNIFIED_ADMISSION_V1`. Phase 20 stays
`PENDING_INDEPENDENT_REACCEPTANCE`. Phases 21 and 22 stay
`PARTIAL_REMEDIATION_PENDING_REACCEPTANCE`. Phase 23 stays
`PARTIAL_PHASE23_INDEPENDENT_REVIEW_REMEDIATION_PENDING_REACCEPTANCE`.
Phase 24 stays `REMEDIATION_REQUIRED`. These paths were not prerequisites
for the native filter.

## Safety and semantics

The NEON routine loads four complete (2×u64) vectors from each eight-row
group, with unsigned vector comparison, bit packing and scalar tails. On the
measured binary the disassembly contains `cmhi.2d`, `uzp1.4s` and `addv.8b`
in the guarded native routine. The audit counts complete compared vectors.
The shared Arrow gather preserves order, payloads, types and row nulls.
The input type/schema check and opt-in batch caps precede mask allocation.
Cancellation is checked between 8,192-row groups and at Arrow call
boundaries. ADE's scratch reservations and retained output budget are
distinct from Arrow allocator costs and OS child RSS.

The focused tests exercised six offsets across 19 sizes, four data/null
layouts and four strict unsigned thresholds, with each result compared to an
independent direct-row/Arrow take oracle. The tests also reach wrong input
type, preallocation refusal, mid-mask cancellation and retry/cleanup. The
process campaign runs the existing benchmark child protocol with verified
source fixtures and independently computes the canonical result from direct
PyArrow reads of the original Parquet data. The local seal is supplied
separately to the verifier and checks byte integrity against that pin.
Local hashes and diagnostics do not independently authenticate the producer.

## Measured economics

The physical host was Apple M3, ARM64 Darwin 27.0.0, with eight logical
cores and AC power. Thermal and energy counters were unavailable. Rust
1.99.1 (58cf8f9c3) and Arrow 59.3.1 built the pinned release with opt-level 3,
ThinLTO, one codegen unit and debug info level 1. Target CPU and target-feature
overrides were absent. The executable SHA-256 was
`c1ac7f414d0e014bf17ecccd379bc59407e1aaf0f5da5dd90dfe42c118fdf529`.

The final campaign recorded 209 native child cases,
including 168 measured macro samples and 24 warmups. The release micro study
used 168 fresh children over seven shapes and three arms.
The table reports benchmark query p50 in milliseconds. The detached
observations record p95, sample range and separate mask, filter, process
wall and RSS distributions. The raw archive retains each observation.

| Distribution | Batch rows | Threshold | Scalar | Arrow | Forced NEON | Measured auto |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| uniform | 1,024 | 0 | 24.023 | 24.403 | 23.500 | 24.131 |
| uniform | 1,024 | 10 | 24.643 | 23.466 | 23.364 | 24.536 |
| uniform | 8,192 | 250 | 23.530 | 22.985 | 22.434 | 24.607 |
| uniform | 8,192 | 500 | 26.498 | 23.932 | 24.023 | 27.168 |
| uniform | 8,192 | 990 | 24.058 | 24.058 | 24.452 | 24.834 |
| hot-key | 32,768 | 1000 | 20.472 | 20.153 | 21.051 | 21.214 |

The original threshold-250 workload had the following medians. The mask and
full filter sum observations across the 64 input batches. Query time includes
scan, projection, collection and audit emission. Process wall includes startup
and the harness oracle. Peak RSS is the waited child's OS high-water mark.

| Arm | Mask ms | Mask + gather ms | Query ms | Process ms | Child peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| scalar | 1.546 | 3.522 | 23.530 | 85.627 | 39.56 |
| arrow | 0.137 | 2.158 | 22.985 | 87.743 | 39.69 |
| force-neon | 0.093 | 2.092 | 22.434 | 86.429 | 39.44 |
| measured-auto | 1.549 | 3.582 | 24.607 | 90.424 | 39.45 |

On the calibration shape, NEON's paired query gain ranged from
-2.00% to 8.31% against scalar and
-2.20% to 4.17% against Arrow.
Of the two controls, the worst paired gain was
-2.19%.
Profile admission took 143.580 ms with
0.000 ms of process headroom.
The measured policy recorded `insufficient-gain` and selected scalar.
The robust whole-query advantage required for acceptance remains unproven.

## Local checks

Rustup's proxy could not manage its protected setup file. The installed stable
and Rust 1.94.0 toolchain binaries ran directly. The full local regressions
needed access to the native workers' local sockets. Logs preserve the sandboxed
smoke attempt. The first all-feature run failed in
`join_pressure_prefix_safety_refusals_and_cleanup` at the
`source drift` assertion. The retry used the same workspace
command. The checks used installed OpenSSL and cached locked dependencies.

| Command or witness | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo check --workspace --all-targets --locked` | PASS |
| `cargo check --workspace --all-targets --all-features --locked` | PASS |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | PASS |
| `cargo build --workspace --locked` | PASS |
| `cargo build --workspace --all-features --locked` | PASS |
| `cargo test --workspace --locked` | PASS |
| `cargo test --workspace --all-features --locked (first attempt)` | FAIL |
| `cargo test --workspace --all-features --locked (exact retry)` | PASS |
| `Rust 1.94.0 cargo check --workspace --all-targets --locked` | PASS |
| `cargo test --locked -p ade-exec --features filter-kernels-v1 filter_kernel` | PASS |
| `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/tests -p 'test_*.py'` | PASS |
| `bash scripts/smoke.sh` | PASS |
| `cargo test --locked --manifest-path examples/phase24-external-consumer/Cargo.toml` | PASS |
| `Standalone ExecutionConfig literal consumer with filter-kernels-v1` | PASS |
| `Explicit ade-bench run and check-result against the original spec` | PASS |

| Release micro and macro campaigns | PASS, 168 micro and 209 native cases. |
| Detached original-executable and original-Parquet verification | PASS. |
| Detached re-sealed semantic mutations | PASS, 16 intended refusals. |
| Hosted Linux on the exact final commit | NOT_RUN. Local-only handoff. The earlier Phase 24 recovery failure remains recorded in its own report. |

## Acceptance matrix

| Gate | Status | Witness |
| --- | --- | --- |
| G0 | PASS | Baseline and runtime input inventory, executable and fixtures pinned. Final Git IDs verified separately. |
| G1 | PASS | Native ownership, static default and ADR-0032 boundaries. |
| G2 | PARTIAL | Legacy path and feature-off regressions checked locally. Hosted cross-platform verification remains open. |
| G3 | PASS | Exact ordered Arrow differential and original-Parquet oracle. |
| G4 | PASS | Physical ARM64 NEON and typed unavailable AVX2 refusal. |
| G5 | PASS | NEON differential matrix and release packed-mask disassembly. |
| G6 | PASS | Actual LocalPlan filter and ADE-local child and parent harness. |
| G7 | PASS | Managed reservation refusal, lifetime cleanup and bounded cancellation. |
| G8 | PASS | Exact profile binding, seven paired observations, conservative fallback and semantic refusals. |
| G9 | PASS | Effective kernel, vector counts, alternative estimates and scoped elapsed/resource observations. |
| G10 | PASS | 209 fresh children and 16 substantively rejected re-sealed mutations. |
| G11 | PASS | Predeclared local economics campaign documented a negative result for the auto guard. |
| G12 | PASS | Apple M3 ARM64. AC power observed. Thermal and energy counters unavailable. |
| G13 | PASS | Detached verification with actual executable, source, original fixtures and separate local seal. |
| G14 | PASS | Required local checks and smoke. See complete local logs. |
| G15 | NOT_RUN | The exact final commit is local. No hosted Linux receipt. |
| G16 | PASS | Operator guide, ADR, per-gate matrix and raw evidence. |
| G17 | PASS | Unmeasured placement, GPU, code generation and cross-architecture lanes explicitly deferred. |

The [support matrix](phase-25-support.json) records the hardware lanes.
The [operator guide](../phase-25-hardware-guide.md) gives the reproduction
commands, and the [experiment report](../research/phase-25-experiment.md) gives
the baseline measurements. The raw evidence is available in the
[candidate archive](../evidence/phase25/candidate.tar.gz) with a
[detached integrity seal](../evidence/phase25/candidate.seal-sha256).
The [verifier output](../evidence/phase25/observations.json),
[mutation results](../evidence/phase25/tamper.txt) and
[local logs](../evidence/phase25/local-checks.tar.gz) record the results.
