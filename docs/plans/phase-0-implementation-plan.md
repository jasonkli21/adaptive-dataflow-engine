# Adaptive Dataflow Engine (ADE) — Phase 0 Detailed Implementation Plan
## Architecture Lab and Benchmark Harness

**Status:** Implemented; acceptance evidence in `docs/reports/phase-0-implementation-report.md`  
**Date:** 2026-10-15  
**Depends on:** `docs/architecture/architecture.md`, ADE PRD  
**Phase:** 0  
**Primary objective:** Build a reproducible benchmark and experimentation foundation before implementing the ADE execution engine.

---

## Implementation Contract

This plan is the detailed implementation contract for Phase 0. The implementing session should read, in order:

1. `AGENTS.md`;
2. `docs/product/prd.md`;
3. `docs/architecture/architecture.md`;
4. this plan;
5. existing ADRs and repository code.

Interpret requirement language as follows:

- **MUST / required** — needed for Phase 0 completion unless a concrete blocker is documented in the implementation report.
- **SHOULD / recommended** — default choice; deviation is allowed when the implementation discovers a better fit and records the reason.
- **MAY / optional** — non-blocking enhancement.

**Phase 0 stop rule:** do not implement ADE physical operators, exchange, scheduler, lineage, or distributed execution. If work would create a real ADE execution path rather than benchmark infrastructure, defer it to Phase 1 or later.

The implementation must end by writing `docs/reports/phase-0-implementation-report.md` and leaving the repository in a clean, reviewable state.

## 1. Phase 0 Objective

Phase 0 establishes the measurement system that every later performance and architecture decision will depend on.

It intentionally does **not** implement ADE's physical execution engine.

At the end of Phase 0, the repository should be able to:

1. generate or register deterministic benchmark datasets;
2. run representative local analytical workloads against at least DataFusion;
3. record process-isolated benchmark results into a versioned JSON schema;
4. capture enough environment/build metadata to make comparisons meaningful;
5. compare two result files and report regressions/improvements;
6. run kernel-level Criterion microbenchmarks separately from whole-workload benchmarks;
7. validate result correctness for supported benchmark cases;
8. provide a stable adapter boundary into which the Phase 1 ADE local engine can later plug.

The result should be useful even if no ADE operator exists yet.

---

## 2. Why Phase 0 Exists

ADE is explicitly a cost-performance research project.

Without a measurement foundation, later work can easily optimize:

- noise;
- warm-cache effects;
- different datasets;
- different build profiles;
- different allocator/compiler settings;
- different semantic workloads;
- non-equivalent query results.

The benchmark harness is therefore not auxiliary tooling. It is core research infrastructure.

Phase 0 also establishes a discipline:

> Every future optimization gets a baseline, a reproducible workload, a structured result, and an explanation.

---

## 3. Phase 0 Scope

### 3.1 In scope

#### Repository-level
- Phase 0 benchmark crate/module.
- benchmark CLI.
- benchmark schemas.
- benchmark specs.
- dataset manifests.
- deterministic synthetic data generation.
- DataFusion baseline runner.
- optional Polars runner for compatible cases.
- macrobenchmark orchestration.
- process isolation.
- result persistence.
- result comparison.
- correctness validation.
- Criterion microbenchmark setup.
- documentation.
- tests.
- CI smoke benchmark/validation path.

#### Workloads
At minimum:

1. Parquet scan;
2. filter + projection;
3. group-by aggregate;
4. high-cardinality aggregate;
5. configurable skew aggregate;
6. derived TPC-H subset or DataFusion-compatible TPC-H fixture path.

#### Metadata
At minimum:

- result schema version;
- run ID;
- timestamp;
- Git commit/dirty state;
- engine/version;
- benchmark case/version;
- dataset identity/version;
- OS/architecture;
- CPU/core count where available;
- memory total where available;
- Rust compiler version;
- Cargo profile;
- engine configuration;
- warmup count;
- iteration count;
- per-iteration wall time;
- summary statistics;
- correctness status.

Peak RSS/CPU time should be implemented where practical without making cross-platform support fragile.

---

### 3.2 Explicitly out of scope

Do not implement:

- ADE operator runtime;
- ADE physical IR beyond benchmark-facing placeholders;
- worker/coordinator services;
- exchange/shuffle;
- lineage;
- adaptive execution;
- cost optimizer;
- distributed benchmark deployment;
- cloud cost calculator;
- Kafka;
- streaming;
- SQL parser;
- custom Parquet reader;
- custom allocator;
- GPU support;
- Kubernetes;
- benchmark web UI.

---

## 4. Phase 0 Architectural Decisions

Phase 0 should resolve only the decisions needed for a durable measurement foundation.

### ADR-0001 — Minimal workspace/module boundary

Recommendation:

Create one primary Phase 0 crate:

```text
crates/ade-bench
```

Optionally create:

```text
crates/ade-common
```

only if shared IDs/errors/config types are genuinely used by more than one crate during scaffolding.

Do **not** create all future ADE crates yet.

---

### ADR-0002 — Macrobenchmark result schema

Use:

- Serde;
- JSON;
- explicit `schema_version`;
- stable field names;
- optional fields for platform-dependent metrics.

Result files are immutable artifacts.

A later schema revision should bump the schema version rather than silently change meaning.

---

### ADR-0003 — Dataset identity

Every dataset used for comparable results needs a manifest.

Manifest captures:

```text
dataset ID
dataset schema version
generator name/version
generator parameters
seed
file format
compression
number of files
row-group settings where known
logical row count
logical byte/physical byte metadata
content fingerprint strategy
```

Large datasets do not require hashing every byte on every run.

Use a generated manifest with stable file metadata/fingerprints, and validate according to a configurable level.

---

### ADR-0004 — Baseline engine strategy

DataFusion is the required Phase 0 baseline. Use its current public APIs at implementation time, pin resolved dependencies through `Cargo.lock`, and avoid private/internal APIs because the benchmark adapter must survive ordinary dependency upgrades. It is the baseline because:

- it is Rust-native;
- Arrow-native;
- analytically relevant;
- easy to embed in the harness;
- directly useful for later ADE/DataFusion integration.

Polars is optional for the cases where an equivalent workload can be expressed without creating artificial differences.

External engines such as Spark/Flink belong in later benchmark phases.

---

## 5. Proposed Phase 0 Repository Layout

The scaffolding step may choose exact top-level conventions, but Phase 0 should target this structure:

```text
crates/
  ade-bench/
    Cargo.toml
    src/
      lib.rs
      main.rs

      cli/
        mod.rs
        data.rs
        run.rs
        compare.rs
        validate.rs
        env.rs

      engine/
        mod.rs
        datafusion.rs
        polars.rs          # optional; feature-gated if added

      dataset/
        mod.rs
        manifest.rs
        synthetic.rs
        tpch.rs

      workload/
        mod.rs
        spec.rs
        scan.rs
        aggregate.rs
        tpch.rs

      runner/
        mod.rs
        orchestrator.rs
        child.rs
        metrics.rs
        correctness.rs

      result/
        mod.rs
        schema.rs
        summary.rs
        compare.rs

      environment/
        mod.rs
        hardware.rs
        build.rs
        git.rs

      error.rs

    benches/
      kernel_smoke.rs

benchmarks/
  README.md

  specs/
    scan-filter-project.toml
    aggregate-low-cardinality.toml
    aggregate-high-cardinality.toml
    aggregate-skew.toml

  datasets/
    .gitkeep

  expected/
    # small correctness fixtures or hashes

  examples/
    result-v1.json

  results/
    .gitignore

docs/
  architecture/
    architecture.md

  plans/
    phase-0-implementation-plan.md

  adr/
    0001-phase-0-workspace.md
    0002-benchmark-result-schema.md
    0003-dataset-manifest.md
    0004-datafusion-baseline.md

  research/
    benchmarking-methodology.md
```

The exact folder names can be adjusted during scaffolding, but responsibilities should remain recognizable.

---

## 6. CLI Contract

Use one CLI:

```text
ade-bench
```

Recommended subcommands:

```text
ade-bench env
ade-bench data generate ...
ade-bench data inspect ...
ade-bench run ...
ade-bench compare ...
ade-bench validate ...
```

Later phases can add:

```text
ade-bench suite
ade-bench report
```

without changing existing contracts.

---

### 6.1 `ade-bench env`

Purpose:

Print the environment fingerprint that will be attached to result files.

Example:

```text
$ ade-bench env

os: macOS 26.x
arch: aarch64
cpu: Apple ...
physical_cores: ...
logical_cores: ...
memory_bytes: ...
rustc: 1.xx.x
profile: release
git_commit: ...
git_dirty: false
```

Output modes:

```text
--format human
--format json
```

---

### 6.2 `ade-bench data generate`

Synthetic example:

```text
ade-bench data generate synthetic \
  --dataset-id skew-v1 \
  --rows 10000000 \
  --files 8 \
  --row-group-rows 65536 \
  --distribution hot-key \
  --hot-key-fraction 0.50 \
  --seed 42 \
  --output benchmarks/datasets/skew-v1
```

TPC-H-derived example:

```text
ade-bench data generate tpch \
  --scale-factor 1 \
  --format parquet \
  --output benchmarks/datasets/tpch-sf1
```

Implementation may shell out to `tpchgen-cli` rather than reimplement TPC-H generation.

Important terminology:

Use "derived from TPC-H" / "TPC-H-derived workload" in documentation/results rather than implying an official TPC benchmark.

---

### 6.3 `ade-bench data inspect`

Example:

```text
ade-bench data inspect benchmarks/datasets/skew-v1
```

Output:

- manifest;
- row count;
- files;
- sizes;
- schema;
- validation result.

---

### 6.4 `ade-bench run`

Examples:

```text
ade-bench run \
  --engine datafusion \
  --spec benchmarks/specs/aggregate-skew.toml \
  --dataset benchmarks/datasets/skew-v1 \
  --warmups 2 \
  --iterations 10 \
  --output benchmarks/results/df-skew.json
```

TPC-H-derived:

```text
ade-bench run \
  --engine datafusion \
  --workload tpch \
  --query 1 \
  --dataset benchmarks/datasets/tpch-sf1 \
  --iterations 5 \
  --output benchmarks/results/df-tpch-q1.json
```

---

### 6.5 `ade-bench compare`

Example:

```text
ade-bench compare \
  benchmarks/results/baseline.json \
  benchmarks/results/candidate.json
```

Human output:

```text
Case                       Baseline     Candidate     Delta
aggregate/skew             1842 ms      1510 ms      -18.0%
peak_rss                   2.10 GiB     2.08 GiB      -1.0%

status: IMPROVEMENT
```

Machine output:

```text
--format json
```

Optional thresholds:

```text
--warn-regression-percent 5
--fail-regression-percent 10
```

Do not use performance threshold failure in noisy general CI by default.

---

### 6.6 `ade-bench validate`

Purpose:

Run correctness checks without performance measurement.

Example:

```text
ade-bench validate \
  --engine datafusion \
  --spec benchmarks/specs/aggregate-skew.toml \
  --dataset benchmarks/datasets/skew-v1
```

Later Phase 1:

```text
--engine ade-local
```

must be able to use the same correctness contract.

---

## 7. Benchmark Specification Format

Use versioned TOML or JSON. TOML is recommended for human-authored cases.

Example:

```toml
schema_version = 1
id = "aggregate-skew"
version = 1
description = "Hash aggregate under controlled hot-key skew."

[dataset]
required_schema = "synthetic-events-v1"

[query]
kind = "group_by_sum"
group_key = "key"
value_column = "value"

[validation]
kind = "canonical_rows"
order = ["key"]

[tags]
category = "aggregation"
stress = "skew"
```

Do not encode engine-specific configuration into the semantic workload spec.

Engine overrides belong in a separate run configuration.

---

## 8. Dataset Schema

Phase 0 synthetic dataset should be intentionally simple but useful across many future engine phases.

Recommended `synthetic-events-v1` schema:

```text
row_id       UInt64
key          UInt64
secondary    UInt64
value_i64    Int64
value_f64    Float64
category     UInt32 or dictionary-compatible string
payload      Utf8
event_time   Timestamp
```

Not every workload uses every column.

Rationale:

- `key`: partition/group/join target;
- `secondary`: second-key/correlation experiments;
- numeric values: aggregation;
- category/payload: projection/encoding/string cost;
- event time: future streaming fixture reuse.

---

## 9. Synthetic Distributions

Phase 0 should implement at least:

### 9.1 Uniform key distribution

Parameters:

```text
key_cardinality
```

---

### 9.2 Low-cardinality aggregation

Example:

```text
10 million rows
100 keys
```

Measures aggregation throughput with small hash state.

---

### 9.3 High-cardinality aggregation

Example:

```text
10 million rows
5 million keys
```

Measures hash-table growth and memory behavior.

---

### 9.4 Hot-key skew

Parameters:

```text
hot_key_fraction
hot_key_count
remaining_key_cardinality
```

Example:

```text
50% rows -> key 0
50% rows -> uniform over 999,999 other keys
```

This is more deterministic and easier to reason about than requiring a Zipf distribution in Phase 0.

Zipf can be added later.

---

### 9.5 File shape

Generator parameters:

- number of files;
- rows/file;
- row-group rows;
- compression;
- sort order;
- seed.

File shape is part of the dataset manifest because it materially affects Parquet scans.

---

## 10. Dataset Manifest

Proposed Rust shape:

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct DatasetManifest {
    pub schema_version: u32,
    pub dataset_id: String,
    pub dataset_version: u32,
    pub generator: GeneratorInfo,
    pub seed: Option<u64>,
    pub format: DataFormat,
    pub compression: Option<String>,
    pub schema_fingerprint: String,
    pub row_count: u64,
    pub files: Vec<FileManifest>,
    pub parameters: BTreeMap<String, serde_json::Value>,
}
```

`FileManifest`:

```rust
pub struct FileManifest {
    pub relative_path: PathBuf,
    pub bytes: u64,
    pub rows: Option<u64>,
    pub fingerprint: Option<String>,
}
```

Do not require cryptographically hashing terabyte-scale datasets on every run.

Generation can store stable fingerprints once.

---

## 11. Benchmark Result Schema

Proposed high-level schema:

```rust
pub struct BenchmarkResultV1 {
    pub schema_version: u32,
    pub run_id: String,
    pub created_at: DateTime<Utc>,

    pub repository: RepositoryInfo,
    pub environment: EnvironmentInfo,
    pub build: BuildInfo,

    pub engine: EngineInfo,
    pub workload: WorkloadInfo,
    pub dataset: DatasetReference,
    pub run_config: RunConfig,

    pub validation: ValidationResult,
    pub iterations: Vec<IterationResult>,
    pub summary: SummaryMetrics,

    pub tags: BTreeMap<String, String>,
}
```

---

### 11.1 Repository info

```text
git_commit
git_branch optional
git_dirty
```

If dirty:

- result is allowed;
- result clearly records dirty state;
- comparison command should warn.

---

### 11.2 Environment info

Best-effort:

```text
hostname optional/hashed or omitted
OS
kernel/version
architecture
CPU model
physical cores
logical cores
total memory
```

Avoid recording user names, full home paths, or machine identifiers that make artifacts unnecessarily personal.

---

### 11.3 Build info

```text
rustc version
Cargo profile
target triple
target_cpu if configured
LTO if known
debug assertions
allocator identifier if non-default
feature flags
```

---

### 11.4 Engine info

```text
name
version
configuration
```

Example:

```json
{
  "name": "datafusion",
  "version": "x.y.z",
  "configuration": {
    "target_partitions": "8",
    "batch_size": "8192"
  }
}
```

---

### 11.5 Iteration result

```text
iteration index
warmup? false for recorded results
wall_time_ns
cpu_time_ns optional
peak_rss_bytes optional
rows_output
result_fingerprint
engine metrics optional
```

Run-level peak RSS can be used instead if child-process measurement makes per-iteration RSS awkward.

---

### 11.6 Summary

At minimum:

```text
count
min
max
mean
median
stddev
p90
p95
```

Do not report only the best run.

Primary comparison statistic:

- median wall time by default.

Reason:
- robust to occasional local noise;
- easy to understand.

Later controlled benchmark infrastructure can add bootstrap confidence intervals.

---

## 12. Process Isolation

Macrobenchmarks should execute measured work in a child process.

Architecture:

```text
ade-bench run
   |
   +-- prepare dataset/spec
   |
   +-- spawn internal child command
            |
            +-- initialize engine
            +-- execute warmup or measurement
            +-- validate result
            +-- print/write structured child result
   |
   +-- parent captures:
         status
         wall time
         platform resource metrics
   |
   +-- aggregate result JSON
```

Why:

- isolate allocator state;
- isolate crashes;
- make max RSS easier to measure;
- avoid engine state leaking across benchmark cases;
- later support external-engine wrappers consistently.

Implementation can use a hidden CLI subcommand:

```text
ade-bench __child-run ...
```

Do not expose it as normal user-facing API.

---

## 13. Warmup and Iteration Semantics

Default:

```text
warmups = 1 or 2
iterations = 5
```

But specs should not hard-code these.

Important distinction:

- engine initialization can be measured separately from query execution;
- data/page cache state affects repeated runs.

Phase 0 should record one cache policy tag:

```text
cache_policy = warm
```

Cold OS cache manipulation is platform-specific and should not be implemented initially.

Document that local benchmark runs are **warm-cache-oriented unless otherwise stated**.

---

## 14. Correctness Validation

Performance results are invalid if outputs differ. A failed or skipped required correctness validation MUST prevent the result from being classified as a valid performance comparison.

### 14.1 Canonicalization

For aggregation workloads:

- sort output by key;
- serialize canonical values;
- hash canonical result.

For unordered scans:

- use deterministic aggregation/checksum rather than row order.

---

### 14.2 Expected results

Two modes:

#### Reference engine mode

DataFusion computes/stores expected fingerprint for a fixture.

#### Direct expected-value mode

Synthetic generators know mathematical expected outcomes for selected cases.

Prefer direct expected values where easy.

---

### 14.3 Floating point

Do not compare raw binary fingerprints for workloads whose floating-point reduction order may differ.

Use:

- integer aggregates for core correctness cases;
- tolerance-based checks for floating point.

---

## 15. DataFusion Baseline Adapter

Required Phase 0 engine.

Responsibilities:

1. create SessionContext;
2. apply explicit config;
3. register Parquet source;
4. execute workload;
5. collect output;
6. expose metrics available through public API;
7. return canonical result.

Do not depend on DataFusion private/internal modules.

---

### 15.1 Baseline queries

#### Scan

```sql
SELECT row_id, key, value_i64
FROM synthetic
```

#### Filter/project

```sql
SELECT key, value_i64
FROM synthetic
WHERE secondary < threshold
```

#### Low/high/skew aggregate

```sql
SELECT key, SUM(value_i64), COUNT(*)
FROM synthetic
GROUP BY key
```

These are simple enough that later ADE operators can implement the same semantics.

---

### 15.2 DataFusion configuration

Make explicit:

- target partitions;
- batch size;
- memory limit if used;
- statistics options relevant to workload.

Record all overrides in result JSON.

Avoid hidden "best performance" tuning in the adapter.

---

## 16. Optional Polars Baseline

Polars can be included if it does not delay the required deliverables.

Use it for:

- scan;
- filter/project;
- group-by.

Do not force TPC-H SQL support through unnatural translation just to have a second engine.

Feature-gate if dependency/build cost is undesirable:

```text
--features polars-baseline
```

Phase 0 passes without Polars. Do not add it merely to increase benchmark count.

---

## 17. TPC-H-Derived Dataset Support

The Phase 0 goal is not full TPC-H benchmarking.

Implement:

- documented integration path to `tpchgen-cli`;
- manifest creation after generation;
- ability to register generated Parquet;
- one or a small number of DataFusion baseline queries.

Recommended starting queries:

- Q1 for scan/filter/aggregation behavior;
- another join query only if implementation remains simple.

Full TPC-H suite is a later benchmark expansion.

Current DataFusion benchmark tooling already uses generated TPC-H-derived data and JSON output; ADE should preserve compatibility with the broader ecosystem rather than create incompatible conventions without reason.

---

## 18. Comparison Tool

Input:

```text
baseline result
candidate result
```

Compatibility checks before comparison:

- same benchmark semantic case/version;
- compatible dataset ID/version/fingerprint;
- same validation semantics;
- warn on hardware mismatch;
- warn on build-profile mismatch;
- warn on dirty git state;
- warn on engine config changes.

Do not silently compute a percent delta for incomparable runs.

---

### 18.1 Output metrics

Default:

```text
median wall time
mean wall time
peak RSS if available
CPU time if available
output rows
```

Classification:

```text
improvement
neutral
regression
incomparable
```

Neutral threshold default can be:

```text
±3%
```

but this is presentation only, not statistical proof.

---

## 19. Microbenchmarks

Use Criterion for tight kernel benchmarks.

Phase 0 needs only infrastructure + one smoke benchmark, because ADE kernels do not exist yet.

Example:

```rust
fn bench_hash_partition_math(...)
```

or an Arrow operation wrapper.

Goals:

- verify Criterion setup;
- establish benchmark profile;
- document invocation;
- prove future crates can add microbenchmarks without changing macrobenchmark infrastructure.

Do not use Criterion as the macrobenchmark runner.

---

## 20. Benchmark Profiles

Add explicit Cargo profiles, for example:

```toml
[profile.release]
debug = 1
lto = "thin"
codegen-units = 1

[profile.bench]
inherits = "release"
```

Exact settings should be captured in ADR/build docs.

Do not prematurely use:

```text
target-cpu=native
PGO
custom allocator
full LTO
```

as defaults.

Those become measured experiments later.

---

## 21. Logging and Output Discipline

During a measured run:

- human logs go to stderr;
- machine child result goes to a dedicated file or stdout protocol;
- result JSON must not be contaminated by logs.

Use `tracing`.

Default measured-run log level should be low enough to avoid benchmark distortion.

Result should capture the configured log level.

---

## 22. Error Model

Errors should distinguish:

```text
invalid benchmark spec
dataset missing/invalid
engine setup failure
query execution failure
validation failure
child process failure
metrics unavailable
result schema error
comparison incompatibility
```

Use structured Rust error types.

CLI errors must include actionable context.

---

## 23. Implementation Workstreams

The implementation should proceed in the following order.

---

### Workstream 0 — Documentation and ADR bootstrap

#### Tasks

- add architecture doc;
- add this Phase 0 plan;
- create ADR template;
- write ADR-0001 through ADR-0004;
- create `benchmarks/README.md`;
- create `docs/research/benchmarking-methodology.md`.

#### `benchmarking-methodology.md` must document

- local benchmark noise;
- warm/cold cache limitations;
- run count;
- median as default comparison;
- hardware consistency;
- release builds;
- background workload cautions;
- TPC-H-derived terminology;
- not publishing local results as official TPC results.

#### Exit criteria

- decisions required to write code are documented;
- no unresolved ambiguity about Phase 0 scope.

---

### Workstream 1 — Core types and CLI skeleton

#### Tasks

Implement:

```text
ade-bench --help
ade-bench env
ade-bench data --help
ade-bench run --help
ade-bench compare --help
ade-bench validate --help
```

Core typed structures:

- `BenchmarkCase`;
- `DatasetManifest`;
- `EnvironmentInfo`;
- `BuildInfo`;
- `EngineInfo`;
- `BenchmarkResultV1`.

#### Tests

- clap parsing tests;
- result schema JSON round trip;
- manifest JSON round trip;
- schema version validation.

#### Exit criteria

CLI and schemas compile with no engine dependency yet.

---

### Workstream 2 — Environment/build fingerprint

#### Tasks

Collect best-effort:

- OS;
- arch;
- CPU model;
- core counts;
- memory;
- Rust version;
- target;
- Git commit;
- dirty state.

#### Privacy rule

Do not include:

- username;
- home path;
- exact machine serial;
- IP/MAC;
- arbitrary environment variables.

#### Tests

- environment collector never panics on missing platform data;
- JSON fields serialize;
- Git collector handles non-Git context gracefully.

#### Exit criteria

`ade-bench env --format json` emits valid schema.

---

### Workstream 3 — Synthetic dataset generator

#### Tasks

Implement generator for `synthetic-events-v1`.

Configurable:

```text
rows
files
row_group_rows
compression
key_cardinality
distribution
hot_key_fraction
hot_key_count
payload_size
seed
```

Write Parquet.

Generate manifest.

#### Determinism

Given identical:

```text
generator version
parameters
seed
```

logical rows must be identical.

Physical Parquet bytes do not have to remain bit-identical across dependency upgrades unless explicitly guaranteed.

The manifest distinguishes logical generator version from file encoding dependency version.

#### Tests

- same seed -> same logical fingerprint;
- different seed -> changed logical fingerprint;
- row counts correct;
- hot-key fraction within exact or documented deterministic tolerance;
- generated Parquet can be read back.

#### Exit criteria

Can generate a small CI dataset and a local larger dataset.

---

### Workstream 4 — Workload specs

#### Tasks

Implement spec loading.

Add:

1. `scan-filter-project.toml`;
2. `aggregate-low-cardinality.toml`;
3. `aggregate-high-cardinality.toml`;
4. `aggregate-skew.toml`.

Each spec includes:

- semantic ID/version;
- required dataset schema;
- query kind/params;
- validation method;
- tags.

#### Tests

- valid specs parse;
- unknown schema version rejects;
- unsupported query kind errors cleanly;
- engine-specific fields are rejected or ignored only by explicit version rules.

#### Exit criteria

Workload semantics are independent of engine adapter.

---

### Workstream 5 — Engine adapter API + DataFusion

#### Tasks

Define public-internal adapter trait.

Implement DataFusion adapter.

Functions:

- prepare;
- execute;
- canonicalize;
- expose engine metadata/config.

#### Correctness

Run each synthetic workload and validate.

#### Tests

- small fixture exact result;
- filter selectivity;
- aggregate keys/count/sum;
- skew result.

#### Exit criteria

`ade-bench validate --engine datafusion ...` passes all synthetic specs.

---

### Workstream 6 — Child-process macrobenchmark runner

#### Tasks

Implement parent/child protocol.

Parent:

- loads spec/manifest;
- validates configuration;
- spawns child;
- measures parent-observable metrics;
- collects child response;
- repeats iterations;
- records result.

Child:

- initializes engine;
- executes exactly one warmup/iteration;
- validates/canonicalizes result;
- emits structured outcome.

#### Important

Do not recursively invoke normal `run` path in a way that creates nested child processes.

Use hidden `__child-run`.

#### Failure behavior

If child crashes:

- parent records failed iteration with process status;
- final run exits non-zero;
- partial diagnostic result may be written if safe.

#### Tests

- successful child;
- intentional child failure;
- timeout if implemented;
- malformed child result;
- validation failure.

#### Exit criteria

Recorded iterations are process-isolated.

---

### Workstream 7 — Result recording

#### Tasks

Write result JSON atomically:

```text
temp file
fsync/flush as practical
rename to requested destination
```

Do not leave a truncated final file after crash.

Human summary prints after successful write.

#### Tests

- round trip;
- overwrite behavior explicit;
- parent dirs;
- invalid destination;
- atomic temp cleanup.

#### Exit criteria

A benchmark run creates a valid `BenchmarkResultV1`.

---

### Workstream 8 — Summary statistics

#### Tasks

Implement:

- min;
- max;
- mean;
- median;
- standard deviation;
- p90;
- p95.

Specify percentile interpolation method in code/docs.

Avoid adding a heavyweight stats library unless it materially reduces risk.

#### Tests

Use fixed arrays with known outputs.

#### Exit criteria

Result summary reproducible and unit tested.

---

### Workstream 9 — Comparison tool

#### Tasks

Validate compatibility.

Compute deltas.

Human table + JSON.

Warnings:

- different hardware;
- different dataset fingerprint;
- different workload version;
- dirty source;
- different compiler/build;
- changed engine configuration.

Hard incompatibility:

- different semantic workload ID/version;
- different dataset logical identity when case requires same input;
- validation failed.

#### Tests

- improvement;
- regression;
- neutral;
- incompatible;
- missing optional metrics.

#### Exit criteria

Two recorded runs can be compared without ad hoc scripts.

---

### Workstream 10 — TPC-H-derived integration

#### Tasks

Document/install `tpchgen-cli`.

Implement wrapper that:

- invokes generator;
- captures tool version;
- emits dataset manifest;
- validates expected tables;
- optionally validates row counts.

Implement one DataFusion benchmark case (Q1 recommended).

#### Important

Do not vendor enormous generated datasets.

#### CI

TPC-H generation does not need to run in normal CI.

#### Exit criteria

A developer can generate SF1 locally using documented commands and record a baseline result.

---

### Workstream 11 — Criterion microbenchmark setup

#### Tasks

Add Criterion dev dependency/bench target.

Implement one stable smoke benchmark.

Document:

```text
cargo bench -p ade-bench
```

#### Exit criteria

Microbenchmark path works independently of macrobenchmark CLI.

---

### Workstream 12 — CI

CI should verify correctness, not make noisy performance claims.

Required:

```text
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo check --workspace
```

Add benchmark smoke:

- generate tiny synthetic fixture;
- validate with DataFusion;
- run 1 warmup / 1 measured iteration;
- validate produced JSON schema;
- run compare against itself.

Do **not** fail CI on absolute timing.

Optional:

- Criterion compile/smoke without statistical threshold.

#### Exit criteria

Fresh checkout passes Phase 0 smoke workflow.

---

## 24. Suggested Dependency Set

Exact versions are chosen during scaffolding/implementation and pinned by `Cargo.lock`.

Likely:

```text
arrow
parquet
datafusion
tokio
serde
serde_json
toml
clap
tracing
tracing-subscriber
thiserror
anyhow only at CLI boundary if useful
uuid
chrono
sysinfo or platform-specific best-effort environment collection
criterion (dev)
tempfile
sha2 or blake3 for fingerprints
```

Guidance:

- prefer `thiserror` for library errors;
- keep `anyhow` at binaries/tooling boundary if used;
- do not add async-trait unless the chosen engine adapter interface requires it;
- do not add Polars unless optional baseline is implemented in this phase;
- keep dependency feature flags minimal.

---

## 25. Data Sizes

Provide named presets rather than one fixed dataset.

## CI

```text
rows: 100k–500k
physical size: tens of MB
purpose: correctness/smoke
```

## Local-small

```text
~1–5 GB
purpose: normal development benchmarking
```

## Local-memory-pressure

```text
configurable relative to machine RAM
purpose: later Phase 2 spill research
```

## Large/cloud

Not Phase 0 implementation.

The dataset manifest must not assume the dataset fits on one machine even though Phase 0 runners do.

---

## 26. Performance Methodology

### 26.1 Required run conditions

Document:

- release profile;
- no debugger;
- record hardware;
- close heavy background workloads where possible;
- use same dataset;
- compare same semantic workload;
- record configuration changes.

---

### 26.2 macOS caution

Local Apple/macOS runs are useful for development but can be affected by:

- efficiency/performance-core scheduling;
- thermal behavior;
- background tasks;
- power management.

Do not over-interpret small deltas.

The harness should surface variance rather than hide it.

---

### 26.3 Statistical interpretation

Phase 0 does not need a formal statistical-significance framework.

Rules:

- record all iterations;
- compare median by default;
- display standard deviation;
- treat <3–5% local differences cautiously;
- repeat suspicious runs.

Later dedicated benchmark infrastructure can add confidence intervals and controlled hosts.

---

## 27. Example End-to-End Phase 0 Flow

```bash
# 1. Inspect environment
cargo run -p ade-bench --release -- env

# 2. Generate data
cargo run -p ade-bench --release -- \
  data generate synthetic \
  --dataset-id skew-v1 \
  --rows 10000000 \
  --files 8 \
  --distribution hot-key \
  --hot-key-fraction 0.5 \
  --seed 42 \
  --output benchmarks/datasets/skew-v1

# 3. Validate workload
cargo run -p ade-bench --release -- \
  validate \
  --engine datafusion \
  --spec benchmarks/specs/aggregate-skew.toml \
  --dataset benchmarks/datasets/skew-v1

# 4. Record baseline
cargo run -p ade-bench --release -- \
  run \
  --engine datafusion \
  --spec benchmarks/specs/aggregate-skew.toml \
  --dataset benchmarks/datasets/skew-v1 \
  --warmups 2 \
  --iterations 7 \
  --output benchmarks/results/df-skew-baseline.json

# 5. Record candidate/config variation
cargo run -p ade-bench --release -- \
  run \
  --engine datafusion \
  --spec benchmarks/specs/aggregate-skew.toml \
  --dataset benchmarks/datasets/skew-v1 \
  --warmups 2 \
  --iterations 7 \
  --engine-option target_partitions=8 \
  --output benchmarks/results/df-skew-p8.json

# 6. Compare
cargo run -p ade-bench --release -- \
  compare \
  benchmarks/results/df-skew-baseline.json \
  benchmarks/results/df-skew-p8.json
```

Phase 1 will replace only the engine choice:

```text
--engine ade-local
```

The rest of the workflow remains stable.

---

## 28. Required Tests

Minimum Phase 0 test matrix.

## Schema tests

- result v1 serializes/deserializes;
- dataset manifest serializes/deserializes;
- unknown result schema version rejected by compare;
- unknown spec version rejected.

## Generator tests

- row counts;
- deterministic seed;
- file count;
- key cardinality;
- hot-key distribution;
- read-back schema.

## Workload tests

- filter expected count;
- aggregate expected output;
- skew aggregate expected output.

## Engine tests

- DataFusion adapter success;
- invalid config;
- missing dataset;
- execution error.

## Runner tests

- child success;
- child crash;
- malformed result;
- non-zero child;
- result output atomicity.

## Comparison tests

- same results;
- faster/slower;
- missing optional fields;
- hardware warning;
- incompatible workload;
- failed validation.

## CLI integration tests

- `--help`;
- tiny data generation;
- validate;
- one-iteration run;
- compare result with itself.

---

## 29. Acceptance Criteria

Phase 0 is complete only when all required criteria below are met.

## Functional

- [x] `ade-bench env` works.
- [x] deterministic synthetic Parquet dataset generation works.
- [x] dataset manifest is written and validated.
- [x] at least four workload specs exist.
- [x] DataFusion baseline adapter executes all required specs.
- [x] output correctness is validated.
- [x] macrobenchmark iterations are process-isolated.
- [x] result JSON v1 is produced.
- [x] result JSON includes environment/build/engine/workload/dataset metadata.
- [x] comparison tool detects incompatible runs.
- [x] comparison tool reports timing deltas.
- [x] Criterion smoke benchmark works.
- [x] TPC-H-derived local setup is documented and one query can run.
- [x] CI smoke workflow works on tiny synthetic data.

## Quality

- [x] `cargo fmt --check` clean.
- [x] Clippy clean under repository policy.
- [x] tests pass.
- [x] no generated large data committed.
- [x] no personal machine paths embedded in canonical artifacts.
- [x] benchmark docs explain noise/limitations.
- [x] ADR-0001 through ADR-0004 are written.
- [x] public APIs/types have useful Rustdoc where appropriate.

## Architecture

- [x] Phase 0 does not implement ADE execution operators.
- [x] workload semantics are engine-independent.
- [x] result schema can accept future `ade-local` / distributed engines.
- [x] dataset identity is stable/versioned.
- [x] engine configuration is explicit in results.
- [x] no benchmark result can be called valid when correctness validation fails.

---

## 30. Phase 0 Deliverables

Required files/artifacts:

```text
docs/architecture/architecture.md
docs/plans/phase-0-implementation-plan.md

docs/adr/0001-*.md
docs/adr/0002-*.md
docs/adr/0003-*.md
docs/adr/0004-*.md

docs/research/benchmarking-methodology.md

crates/ade-bench/...

benchmarks/README.md
benchmarks/specs/*.toml
benchmarks/examples/result-v1.json
```

Generated/local-only:

```text
benchmarks/datasets/*
benchmarks/results/*
```

Large generated data should be Git-ignored.

---

## 31. Phase 0 Implementation Sequence

Recommended implementation order:

```text
Step 1  docs + ADRs
Step 2  schemas + CLI skeleton
Step 3  environment/build fingerprint
Step 4  dataset manifest
Step 5  synthetic generator
Step 6  workload specs
Step 7  engine adapter
Step 8  DataFusion baseline
Step 9  correctness validation
Step 10 child-process runner
Step 11 result recording/statistics
Step 12 compare command
Step 13 TPC-H-derived integration
Step 14 Criterion setup
Step 15 CI + docs polish
Step 16 final acceptance run
```

Do not implement multiple steps in parallel if it forces API guessing.

In particular:

- do not write comparison before result schema stabilizes;
- do not write engine adapters before workload semantics exist;
- do not add Polars before required DataFusion path is complete.

---

## 32. Phase 0 Final Acceptance Run

Before declaring Phase 0 complete, run and retain a development report containing:

## Environment

Output of:

```text
ade-bench env
```

## Synthetic correctness

All four required workload specs.

## Macrobench result

At least:

```text
aggregate-skew
DataFusion
local-small or CI-size fixture
>= 5 measured iterations for local acceptance
```

## Comparison

Compare two DataFusion configurations or two commits to prove the compare path.

## TPC-H-derived

Run Q1 at a reasonable local scale and record result.

## Microbenchmark

Run Criterion smoke.

## CI

All required checks pass.

---

## 33. Phase 0 Implementation Report

The implementation session should produce:

```text
docs/reports/phase-0-implementation-report.md
```

Required report sections:

1. summary;
2. implemented scope;
3. deviations from plan;
4. ADRs resolved;
5. commands run;
6. test results;
7. benchmark smoke results;
8. known limitations;
9. deferred items;
10. recommended Phase 1 considerations.

Do not treat benchmark numbers from one development machine as product claims.

---

## 34. Non-Blocking Deferred Items

These may be deferred without failing Phase 0 if documented:

- Polars adapter;
- CPU-time metric on every platform;
- peak RSS on every platform;
- HTML reports;
- plots;
- statistical confidence intervals;
- cold-cache automation;
- TPC-H queries beyond initial subset;
- external Spark/Flink runners;
- containerized benchmark images;
- cloud pricing;
- network/disk byte accounting.

The required foundation is more important than broad tooling.

---

## 35. Guardrails Against Over-Engineering

Phase 0 should not become a benchmark product.

Avoid:

- plugin framework for arbitrary benchmark engines;
- distributed benchmark coordinator;
- database for benchmark results;
- web dashboard;
- custom DSL;
- dynamic shared-library loading;
- cloud agent fleet;
- sophisticated statistics package;
- benchmark-result service.

Simple Rust traits + files + CLI are sufficient.

---

## 36. Handoff to Phase 1

Phase 1 should be able to implement a minimal local ADE engine and add:

```text
engine/ade_local.rs
```

or equivalent without altering:

- dataset manifest;
- workload semantic spec;
- result schema;
- process isolation;
- comparison tool;
- correctness contract.

Phase 1 then uses the harness to answer:

> How close is the intentionally simple ADE local execution loop to DataFusion on the tiny supported operator subset?

Phase 1 should not be judged on beating DataFusion immediately.

The initial goal is:

- correctness;
- ownership of the execution loop;
- explicit memory hooks;
- understandable architecture;
- measured baseline.

---

## 37. Completion Definition

Phase 0 is successful when a fresh developer—or a fresh coding-agent session—can clone the repository, read the architecture/plan, generate a small deterministic dataset, run a DataFusion workload, obtain a versioned validated benchmark artifact, and compare it to another run without inventing any new conventions.

At that point the project has the experimental foundation required to begin implementing ADE itself.
