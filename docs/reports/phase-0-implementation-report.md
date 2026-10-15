# Phase 0 implementation report

Date: 2026-10-15. Phase 0 acceptance complete.

## Summary

Implemented the Architecture Lab and Benchmark Harness in the existing `ade-bench` crate. No ADE physical operators, runtime, exchange, scheduler, lineage, streaming or distributed services were implemented.

## Implemented scope

- CLI: env, synthetic/TPC-H-derived generation, inspect, validate, run, compare and check-result; hidden non-recursive child entrypoint.
- Strict typed/versioned workload, manifest, child protocol and result schemas. Unknown versions/fields are rejected.
- Deterministic eight-column synthetic Parquet generation, uniform and precisely controlled hot-key distributions, configurable cardinality/payload/compression/file/row-group shape and logical/physical fingerprints.
- Six engine-independent specs: scan, filter/project, low/high/skew aggregation and TPC-H-derived Q1.
- DataFusion public-API baseline with explicit configuration, canonicalization and independent correctness checks. A generic adapter execution function shares the correctness contract with future adapters.
- Fresh child per warmup/measured iteration; status/error recording, setup/query/process timing, best-effort peak RSS and invalid diagnostic results.
- Atomic no-clobber JSON artifacts, all measured samples, R-7 min/max/mean/median/population standard deviation/p90/p95; compatible comparisons and warnings.
- TPC-H-derived external generation/registration/Q1, Criterion BLAKE3 microbenchmark, CI smoke script, tests and methodology documentation.

## Deviations from plan

The implementation uses small flat Rust modules rather than the illustrative nested directory tree. Q1 uses the common `--spec` interface rather than a separate `--workload/--query` alias; Parquet is the only generator format. Named dataset sizes are documented recipes rather than preset flags. Synthetic ordering is fixed by row_id; optional sorted variants are deferred.

Result validity is explicit at run and child level rather than a separate duplicated validation object. Failed runs have no performance summary. Atomic publication uses tempfile's no-clobber publication rather than replacement rename because artifacts are immutable.

Q1 uses explicit DOUBLE input arithmetic and an independent Parquet reference with exact counts and finite, tolerance-aware sums/averages. Its canonical comparison fingerprint comes from the deterministic reference after output validation.

The source directory arrived without `.git` metadata, and the system had no Rust executable on PATH. Git fields correctly remain null. A temporary stable Rust toolchain was installed under `/private/tmp`, without changing shell startup files.

## ADRs resolved and API decisions

- ADR-0001: one Phase 0 crate; no speculative future crates.
- ADR-0002: strict JSON result v1, immutable artifacts and retained failed child diagnostics.
- ADR-0003: independent logical dataset identity and complete physical manifest identity; recorded metadata/full verification choice, child metadata checks.
- ADR-0004: DataFusion 55.2.0 with Arrow/Parquet 59.3.1; Cargo.lock pins resolved dependencies. Workspace MSRV is 1.94, required by DataFusion. Current public source/API signatures were inspected.

Release/bench use thin LTO, one codegen unit, debug information level 1, portable CPU defaults and the system allocator. Build metadata captures rustc/profile/target/assertions plus a whitelist of explicit codegen overrides; arbitrary environment variables and paths are excluded.

## Commands run

The temporary toolchain requires this environment in this session:

```sh
export PATH=/private/tmp/ade-cargo/bin:$PATH
export RUSTUP_HOME=/private/tmp/ade-rustup
export CARGO_HOME=/private/tmp/ade-cargo
```

Dependency fetch and external tool installation required network-enabled execution. Validation uses the pinned lockfile and can run offline after fetch.

```sh
cargo fetch
cargo install tpchgen-cli --version 3.0.0 --locked --root /private/tmp/ade-tools
cargo fmt --all --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
cargo test --workspace --all-features --locked --offline
cargo build --release --locked --offline
bash scripts/smoke.sh
cargo bench --locked --offline -p ade-bench --bench smoke -- --test
```

Release fixture preparation additionally ran `env --format json`, synthetic generation for 100,000-row low/high/skew fixtures, repeat skew generation, full inspection, all five synthetic validations, and:

```sh
target/release/ade-bench data generate tpch --scale-factor 0.001 --generator /private/tmp/ade-tools/bin/tpchgen-cli --output benchmarks/datasets/phase0-acceptance/tpch
target/release/ade-bench validate --spec benchmarks/specs/tpch-q1.toml --dataset benchmarks/datasets/phase0-acceptance/tpch
```

The release preparation ran with system queries available so hardware metadata could be recorded. The same workload correctness paths also passed inside the managed sandbox. Final CI and timed acceptance use `--verify-data full`; the default metadata mode avoids mandatory full hashing of very large immutable fixtures.

## Test results

Final format, all-target check, Clippy with warnings denied, and tests pass. Tests: 8 unit and 4 integration scenarios, including all synthetic distributions/specs, exact row read-back across Snappy/Zstandard/uncompressed encoding, Q1 boundary/tolerance checks, schema integrity, child success/crash/nonzero/malformed/unvalidated responses, real correctness failure diagnostics, atomic output/no-clobber/cleanup and comparison incompatibility/improvement/regression/neutral/warnings.

Initial validation exposed and resolved a test borrowing error, build-metadata changes requiring a rebuild, style/numeric-conversion lint findings, sandbox resource-query failures, and a test seed perturbation that preserved the aggregate by permutation. The final failure fixture forces different expected aggregates instead.

## Benchmark smoke results

Release correctness evidence:

| Workload | Input rows | Output rows | Validation |
|---|---:|---:|---|
| scan | 100,000 | 100,000 | exact |
| filter/project (secondary < 250) | 100,000 | 25,099 | exact |
| low-cardinality aggregate | 100,000 | 100 | exact |
| high-cardinality aggregate | 100,000 | 43,164 | exact |
| skew aggregate | 100,000 | 9,930 | exact |
| TPC-H-derived Q1, SF 0.001 | 6,005 lineitem rows | 4 | exact counts, tolerance-aware numeric values |

The external generator produced all eight tables, 8,695 total rows. Repeated skew generation matched logical fingerprint `07669f106031f3b2d3dcdb9b0f493bd05b1d955c6a5246c76c52527c23b1f920`.

Release environment: macOS, kernel 27.0.0, aarch64, Apple M3, 8 physical/logical cores, 25,769,803,776 bytes RAM. Rust 1.99.0 (b940084d7, 2026-09-28), release profile, aarch64-apple-darwin, debug assertions false, system allocator, no explicit codegen overrides; Git fields null.

Criterion smoke completed successfully: `Testing blake3-4k` / `Success` under the optimized bench profile. All timed release runs used one isolated warmup and five isolated measured iterations, `--verify-data full`, batch size 8192 and statistics collection disabled. Skew configurations used target partitions 2 and 4; Q1 used 2. Compilation had finished before timed runs. These are tiny, uncontrolled development-machine samples shortly after compilation, not product performance claims.

Query statistics in milliseconds (population standard deviation; R-7 percentiles):

| Case | Count | Min | Max | Mean | Median | Stddev | P90 | P95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| skew-p2 | 5 | 4.376583 | 4.616833 | 4.461842 | 4.387667 | 0.101901 | 4.590500 | 4.603666 |
| skew-p4 | 5 | 3.341417 | 3.629000 | 3.459658 | 3.458167 | 0.107453 | 3.584617 | 3.606808 |
| tpch-q1 | 5 | 4.775000 | 5.206208 | 4.933658 | 4.916167 | 0.153663 | 5.110958 | 5.158583 |

- skew-p2 query nanoseconds: 4551000, 4377125, 4616833, 4387667, 4376583. Maximum measured child RSS: 35,602,432 bytes.
- skew-p4 query nanoseconds: 3629000, 3341417, 3518042, 3458167, 3351666. Maximum measured child RSS: 37,535,744 bytes.
- tpch-q1 query nanoseconds: 5206208, 4968083, 4802834, 4916167, 4775000. Maximum measured child RSS: 35,307,520 bytes.

Comparison JSON and human output both succeeded. The tool reported a -21.184% median delta for p4 versus p2 and +5.430% maximum child RSS, with explicit engine-configuration and unavailable-source warnings. This proves the comparison path; it does not establish a stable performance advantage.

All three result artifacts passed `check-result`. Local inputs/results remain under `benchmarks/datasets/phase0-acceptance/` and `benchmarks/results/phase0-acceptance/`, which are ignored. `benchmarks/examples/result-v1.json` intentionally preserves the small p2 result as a validated schema example; it contains no home paths or host identifiers.

Reproduce the timed acceptance with:

```sh
BIN=target/release/ade-bench
DATA=benchmarks/datasets/phase0-acceptance
RESULTS=benchmarks/results/phase0-acceptance
$BIN run --spec benchmarks/specs/aggregate-skew.toml --dataset "$DATA/skew" --verify-data full --warmups 1 --iterations 5 --output "$RESULTS/skew-p2.json"
$BIN run --spec benchmarks/specs/aggregate-skew.toml --dataset "$DATA/skew" --verify-data full --warmups 1 --iterations 5 --engine-option target_partitions=4 --output "$RESULTS/skew-p4.json"
$BIN compare "$RESULTS/skew-p2.json" "$RESULTS/skew-p4.json" --format json
$BIN run --spec benchmarks/specs/tpch-q1.toml --dataset "$DATA/tpch" --verify-data full --warmups 1 --iterations 5 --output "$RESULTS/tpch-q1.json"
```

Choose new output paths when repeating; existing artifacts are immutable. Debug CI smoke has passed on 100,000 rows, all five synthetic specs, one warmup/one measured iteration, result integrity validation and self-comparison. This is a correctness smoke, not a performance claim.

## Known limitations

- Local machine results are development evidence only. No significance or official TPC claims are made.
- OS caches are warm-oriented; there is no cache flushing or timeout/cancellation policy.
- Query outputs and synthetic canonical/reference rows are collected in memory. Large scans/high-cardinality validation can dominate memory; peak RSS includes validation, not just the engine.
- Metadata-only verification assumes immutable inputs and cannot detect same-size changes; full verification is available and used for acceptance.
- CPU time is null. RSS is recorded only after a successful resource probe. The managed macOS sandbox denies the `time -l` system query; the runner falls back to null RSS and still runs correctly.
- Git identity is unavailable in this source archive. The minimum Rust 1.94 toolchain was not separately tested; validation uses stable Rust 1.99.0.
- External TPC-H registration uses conservative file-checksum identity; physical encoding changes prevent direct logical comparison across encodings. Synthetic logical identity is independent of file shape.
- Remote GitHub Actions was not triggered; its configured smoke workflow is exercised locally.

## Deferred items

Optional Polars, CPU-time portability, broader resource/engine/I/O metrics, cold-cache automation, timeouts, confidence intervals, graphs/HTML, regression thresholds, external/distributed engines, TPC-H queries beyond Q1, cloud tooling and sorted synthetic variants remain deferred. No Phase 1 work was started.

## Recommended Phase 1 considerations

Implement the minimal ADE local adapter behind the existing workload/manifest/result contract and shared canonical validator. Start with correctness against the same integer oracle. Add actual memory accounting in the ADE runtime, and distinguish operator memory from harness validation memory. Use controlled release comparisons and record configuration/source changes before interpreting small deltas.

## Acceptance audit

The implementation plan remains the normative checklist. All 28 section 29 criteria have been verified with the evidence below. Remote CI execution and platform-specific limitations are distinguished above.

| Group | Criterion | Evidence |
|---|---|---|
| Functional | env works | JSON environment output retained locally and summarized above |
| Functional | deterministic synthetic Parquet | seed/shape/read-back tests; matching repeat fingerprint |
| Functional | written/validated manifest | full inspect and corruption/version/path tests |
| Functional | at least four specs | six checked-in TOML specs |
| Functional | DataFusion executes required specs | release validation table and all-distribution integration matrix |
| Functional | correctness validated | independent integer oracle and direct Q1 reference; failure tests |
| Functional | process isolation | fresh Command per warmup/iteration, hidden child entrypoint, process protocol tests |
| Functional | result JSON v1 | CLI process round-trip test, schema checker, three validated release artifacts |
| Functional | complete metadata | typed embedded environment/build/engine/spec/manifest/run metadata |
| Functional | incompatible runs rejected | workload/input/validation/schema incompatibility tests |
| Functional | timing deltas | faster/slower/neutral tests, smoke self-compare and release p2/p4 comparison |
| Functional | Criterion smoke | optimized Criterion blake3-4k smoke completed successfully |
| Functional | TPC-H-derived setup/query | real tpchgen 3.0.0 SF 0.001 generation and validated Q1 |
| Functional | CI tiny smoke | current scripts/smoke.sh passes locally with full verification; no timing gate |
| Quality | formatting | cargo fmt --all --check passes |
| Quality | Clippy | all targets/features with -D warnings passes |
| Quality | tests | 8 unit + 4 integration scenarios pass |
| Quality | no generated large data committed | generated inputs/results under ignored directories; intentional 7,575-byte schema example |
| Quality | no personal paths in canonical artifacts | whitelisted metadata and relative input paths; canonical example checked for personal paths/host identifiers |
| Quality | noise/limitations documented | methodology, benchmark README and report |
| Quality | ADR-0001 through 0004 | four accepted ADR files |
| Quality | public Rustdoc | documented schema/adapter APIs, error contracts and local invariants |
| Architecture | no ADE operators | one tooling crate, baseline engine only |
| Architecture | independent semantics | typed semantic Query/specs, engine overrides separate |
| Architecture | future engine results | generic EngineInfo/configuration and shared adapter correctness path |
| Architecture | stable/versioned identity | generator v1, canonical logical hash and physical manifest hash |
| Architecture | explicit engine configuration | partitions/batch size/statistics recorded on each result |
| Architecture | correctness gates validity | failed children invalidate runs, suppress summary and prevent comparison |
