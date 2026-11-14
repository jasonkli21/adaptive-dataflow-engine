# Phase 24 implementation report

**Qualification** `PARTIAL_PHASE24_HOSTED_CI_PENDING`

**Baseline** `95e1153b2b75fbe8d9e2445e96b5f9081d293cfa`, tree
`d57deceac64d6d138076b4489cb6190b2089a227`

**Date** 2026-11-14

The tracked baseline was clean. The supplied Phase 24 plan and the separate
Phase 0 bootstrap prompt were untracked at entry. This phase adds the supplied
plan and leaves the bootstrap prompt outside its commit. ADR-0031 admits the
narrow Phase 20 prerequisite carve-out for a finite, local weak-component
terminal. Phase 22 supplies no runtime dependency. The qualifications of
Phases 18, 20, 21, 22 and 23 remain in force.

## Implemented contract

The opt-in SDK negotiates one trusted Rust row kernel with an expected ABI,
extension version, ordered non-null primitive schema and writable slots.
It pins the executable, model and finite local Parquet input. The SDK owns
row placement inside the native `BatchMap`, enforces exact row identity and
buffers, then invokes a native projection compiled from PlanV1. The separate
vector library adds stable L2 normalization and fixed-model affine scoring.
The standalone consumer implements signed sensor calibration independently
against public exports, using its own workspace and lockfile.

The graph library reads the exact caller-retained Phase 20 terminal pin,
checks rows and footer, then writes the label inventory to private capped
Parquet. Native `HashAggregate` sums ones and independently counts rows.
The adapter checks both tallies, removes its stage, and reattests the pin and
source before returning the canonical component list. Results are ephemeral.
The operator may remove orphan stage directories from defunct processes.
Detailed admission, trust, accounting and output boundaries appear in the
[guide](../phase-24-extensions-guide.md).

## Gate observations

| Gate | Local result | Evidence |
| --- | --- | --- |
| G0 Source and baseline | PARTIAL | Frozen 234-path source inventory, baseline, build, executable and fixture identities. Current-source release requires the detached exact-SHA release and hosted receipts. |
| G1 Boundary and defaults | PASS | Opt-in platform crates, dependency direction and workspace regressions. Core sources and existing formats unchanged. |
| G2 Identity and capability | PASS | Public descriptor, installed executable and ordered schema cases. Source/model mismatch, duplicate identity and malformed contract refusals. |
| G3 Native route | PASS | Real scan/map/projection metrics and cyclic round, terminal and native HashAggregate observations. |
| G4 Graph correctness | PASS | Independent union-find over original Parquet, including empty, isolated, single and mixed graphs. Mixed graph takes six rounds. Reordered inputs produce the same components. |
| G5 Graph lifecycle | PASS | Provisional, missing/wrong pin, corrupt source/terminal, changed stage, killed stage and retry around an operator-visible orphan. |
| G6 Vector prep | PASS | Independent hypot reference, signed zero, finite extremes, zero-vector refusal, ordered rows, and every permitted SDK batch split over a fixed input. |
| G7 Fixed scoring | PASS | Independent affine reference, dimensions one and four, exact pinned model, native projection, nonfinite input and intermediate refusal. |
| G8 External consumer | PASS | Standalone locked Cargo tests in the repository fixture and a copied package. |
| G9 Resource limits | PASS | Row cap, retained output cap, exact and one-byte-below map accounting, inline kernel state, stage cap and measured managed peaks. Scope is logical native/extension ownership on trusted input. |
| G10 Failure and cancellation | PASS | Pre-cancel and in-map cancellation, process panic, 65,536-byte stalled-output prefix without a newline-terminated result, stage SIGKILL, postflight drift and actual isolated filesystem ENOSPC. |
| G11 Trust | PASS | Immutable operator file contract, exact digests and canonical source paths. Code and executable installation are trusted. |
| G12 Determinism and attempt | PASS | Same logical, plan and result digest across batch sizes 7 and 256. Distinct attempt UUIDs, canonical output and retry around a killed private stage. |
| G13 Observability | PASS | Native operator counters, cuts and rounds, source/model/plan identities, effective caps and typed errors. Measured and excluded memory are distinguished. |
| G14 Matched measurements | PASS | Five samples per control plus one warmup, correctness before interpretation. Raw clocks, RSS and native observations retained. |
| G15 Detached adversarial verification | PASS | 88 raw process observations, 32 attempts, and 12 fully rehashed mutations rejected for their specified reasons. |
| G16 Build and hosted CI | PARTIAL | Local workspace checks are recorded below. A hosted Linux run and independently pinned receipt for the exact project commit are outstanding. |
| G17 Support reporting | PASS | ADR, plan amendment, guide, fixture lock, frozen evidence and support matrix. |

## Local process and evidence results

The campaign used macOS 27.0.1 arm64, Rust 1.99.1, Arrow/Parquet 59.3.1,
Python 3.9.6 and PyArrow 21.0.0. It built with the dev profile and a BLAKE3-only
optimization override. This preserved the 10-second SDK deadline after an
unoptimized candidate timed out during a simultaneous workspace build.
The measured executable SHA-256 is
`068722bc48ca1d76e5b37d95af42d4f3a0d922972952e0948c7cfc78783f2190`.
The SHA-256 of its provenance record is
`afab6ab3cc733c45b8c0cbc18a96dece50f7d1cbee7f1b487359839965933f14`.

The independent oracle reads the frozen Parquet and model bytes. The verifier
checks required exits, row/result oracle, cut hash chains, stage contents,
resource counters and each measurement against raw observations. The model
swap, fixture swap, omitted and downgraded success, forged terminal, duplicate
result, out-of-bound numeric score, forged route and stage ownership,
case relabeling, false feedback trace and changed sample all fail after the
archive seal is regenerated. The actual ENOSPC occurred while creating the
private stage directory on a verified disposable HFS+ volume with zero bytes
available. The kill reached the after-encoding stage, emitted zero result
bytes and left one private orphan. The next attempt used the same stage
parent and completed without adopting the orphan.

The [archive](../evidence/phase24/candidate.tar.gz), [archive SHA-256](../evidence/phase24/candidate.archive-sha256),
[detached seal pin](../evidence/phase24/candidate.seal-sha256) and
[mutation outputs](../evidence/phase24/tamper.txt) retain the evidence.
[Earlier attempts](../evidence/phase24/attempts/) record the empty Parquet
metadata refusal, a bad harness cycle port, the sandbox image-creation
failure and the concurrent-build deadline. No release gate or threshold was
lowered.

## Measurements

The wall clocks include process startup, verification and output. Graph-native
adds the fresh Phase 20 process and the reduction process. Reference processes
include Python imports and independent Parquet reads. Workspace workload
activity overlapped this local campaign. These are descriptive debug-build
observations without a speedup or cross-host claim.

| Case | Scale | p50 ms | p95 ms | Population SD ms | p50 rows or vertices/s | Max child RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Vector identity map + native | 32,768 | 206.152 | 211.563 | 4.845 | 158,950 | 16.188 |
| Vector normalize + score + native | 32,768 | 258.147 | 261.982 | 19.994 | 126,935 | 17.062 |
| Python vector reference | 32,768 | 391.378 | 392.258 | 4.609 | 83,725 | 69.281 |
| Graph cyclic + native aggregate | 128 | 98.483 | 128.479 | 12.227 | 1,300 | 15.844 |
| Python union-find reference | 128 | 254.012 | 305.944 | 36.160 | 504 | 51.156 |

The first measured combined vector run accounted for 36,992 bytes in the SDK
map and 565,248 bytes in the native managed ledger. Scan, mapping and projection
recorded 11.25, 58.45 and 0.15 ms of native work. Model read took 0.03 ms.
The graph stage contained 2,767 bytes and the aggregate ledger peaked at
2,106,392 bytes. These native counters have distinct scopes from RSS and
process wall time. All measured workloads matched the predeclared oracles.

## Local build and release boundary

Local commands use the installed toolchain binaries directly because the
Rustup proxy tried to delete a protected setup file. The workspace uses its
locked dependency inventory. Homebrew OpenSSL and CMake supply the native
Kafka build prerequisites.

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo check --workspace --all-targets --locked --offline` | PASS |
| `cargo test --workspace --locked --offline` | PASS, 254 passed and 1 ignored |
| `cargo build --workspace --all-features --locked --offline` | PASS |
| `cargo test --workspace --all-features --locked --offline` | PASS, 345 passed and 10 ignored |
| `cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings` | PASS |
| `cargo +1.94.0 check --workspace --all-features --locked --offline` | PASS |
| `cargo test --manifest-path examples/phase24-external-consumer/Cargo.toml --locked --offline` | PASS, including copied-path consumer |
| `python -m unittest discover -s scripts/tests -p 'test_*.py'` | PASS, 76 tests |
| `cargo build --locked --offline -p ade-ext-graph --features fault-injection --example phase24-native` | PASS |

The sandbox initially refused loopback socket operations. Both workspace test
variants passed on rerun with local socket access. The first MSRV attempt
failed CMake's OpenSSL lookup. Supplying Homebrew OpenSSL's root and pkg-config
path resolved it. See the [local check logs](../evidence/phase24/local-checks.tar.gz).

Hosted Linux CI for the project commit remains open. The configured GitHub
credential was invalid and the direct CLI could not reach the API in the
managed environment. The workflow includes a standalone consumer and detached
Phase 24 process verification step. Current-source mode requires a clean
checkout, the matching executable, and two independently pinned external
receipts tying the campaign and successful hosted jobs to the exact commit and
tree. A local historical replay establishes no hosted result.

The v1 surface supports finite graph component sizes, vector feature
preparation and narrow affine batch scoring. General ML training and serving,
ANN, CEP, geospatial, distributed/cyclic third-party operators, streaming,
dynamic plugin loading and untrusted tenant execution remain deferred.
