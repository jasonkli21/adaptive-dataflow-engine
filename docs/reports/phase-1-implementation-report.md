# Phase 1 implementation report

Date: 2026-10-15. Phase 1 acceptance complete.

## Summary

ADE now owns a synchronous local execution path in `ade-exec`. The `ade-local`
benchmark adapter executes the five existing synthetic specs and the unchanged
Phase 0 oracle validates the results. DataFusion remains an independent baseline.
No Phase 2 work was started.

## Runtime architecture

One new crate, `crates/ade-exec`, contains the provisional `LocalPlan` tree,
compiled internal pull operators, structured errors, `ExecContext`, cancellation,
postorder `OperatorId`, metrics and memory hooks. `execute` builds a fresh operator
tree and collects Arrow batches plus metrics. Unary operators pull one batch on
demand; there are no queues, async runtime or global mutable services.

`ade-bench/src/ade_local.rs` owns manifest-to-plan translation and configuration.
The existing dispatcher forwards `ade-local` and raw options through isolated
children. ADE errors retain their structured source at the harness boundary.
Arrow/Parquet 59.3.1 are shared workspace dependencies; no upstream upgrades.
`cargo tree -p ade-exec --locked --offline --depth 1` confirms only Arrow,
Parquet and thiserror production dependencies (tempfile for tests).

## Operators and workloads

- Parquet scan: explicit caller-ordered files, configured positive batch size,
  per-file schema checks, cancellation between batches/files.
- Filter: UInt64 column less than UInt64 literal; preserves schema/order and
  excludes null predicates.
- Project: selects/reorders columns through Arrow's array-reference projection.
- Hash aggregate: UInt64 keys, checked i128 sum accumulation, checked UInt64
  count, checked Int64 output conversion, sorted key output. Null key/value
  inputs are explicitly unsupported.
- Limit: zero does not pull input; cross-batch slicing stops input at the limit.
- Collect: bounded-query output plus execution/operator metrics.

Scan, filter/project and low/high/skew aggregate specs all execute through ADE.
The adapter accepts only the synthetic-events-v1 Arrow schema and manifest-listed
files. Q1 returns an explicit unsupported error directing users to DataFusion.

## Deviations and decisions

No scope or benchmark-contract changes. A compact runtime module plus test module
is used instead of speculative subcrates. Aggregate emits one sorted result batch;
scan batch size is not a cap on aggregate output size. That is acceptable for
Phase 1's collected bounded output and is a Phase 2 consideration.

ADR-0005 records crate ownership, pull execution, provisional plan status and
observational memory semantics, refining architecture sections 5.3, 7–8 and INV-4.
Benchmark result schema v1, independent validation and process protocol remain
unchanged. One existing Phase 0 test used `ade-local` as its unknown-engine name;
it now uses `missing-engine` so it continues testing the same rejection contract.
The pre-existing untracked Phase 1 plan is included; the unrelated Phase 0 handoff
prompt is left untouched.

## Commands and validation strategy

Used focused crate and adapter tests during implementation, one integrated full
validation pass, and reran affected checks after actual fixes. Commands use the
cached toolchain and dependencies, with:

```sh
export PATH=/private/tmp/ade-cargo/bin:$PATH
export RUSTUP_HOME=/private/tmp/ade-rustup
export CARGO_HOME=/private/tmp/ade-cargo
```

Commands actually run:

```sh
cargo check -p ade-exec --offline
cargo test -p ade-exec --locked --offline
cargo check -p ade-bench --offline
cargo test -p ade-bench --test phase1 --locked --offline
cargo fmt --all
cargo fmt --all --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo test --workspace --all-features --locked --offline
cargo test -p ade-bench --test phase0 cli_process_roundtrip_and_comparison_integrity --locked --offline
cargo tree -p ade-exec --locked --offline --depth 1
bash scripts/smoke.sh
cargo build --release --workspace --locked --offline
git diff --check
```

Initial unlocked offline checks updated Cargo.lock for the new local crate only.
Public Parquet builder and Arrow projection/slice/memory APIs were inspected in
the pinned downloaded source; docs.rs requests were unavailable in the browser.

## Focused tests

Four runtime tests combine the required semantic boundaries: memory current/peak,
overflow/underflow invariants, cancellation before and after batch progress,
deterministic IDs, multi-file/multi-batch scan order, real filter/project and
aggregate pipelines, null predicate exclusion, empty filtered/aggregate output,
known group/sum/count values, sum output overflow, zero/cross-batch limit with
upstream row counters, and invalid projection/predicate types.

One table-driven adapter integration test validates all five specs against the
existing oracle and verifies identical DataFusion fingerprints/output counts,
config resolution/rejection, missing files and Q1 rejection. All pass.

Direct CLI validation under `ade-local` passes all five specs in the extended
smoke. Q1 against the existing Phase 0 TPC-H fixture exits 1 with the expected
unsupported-workload message; no fixture regeneration was needed.

## Final full validation

Formatting, locked all-target workspace check, all-feature/all-target Clippy with
`-D warnings`, and all-feature workspace tests pass. The final suite contains
18 ade-bench unit tests, 4 ade-exec unit tests, 5 Phase 0 integration tests and
1 Phase 1 integration test, plus passing doc tests.

Initial Clippy findings (long dispatcher and nested if) were fixed by extracting
the aggregate routine and collapsing the reader condition. The first full test
pass exposed the stale unsupported-engine assertion described above; its focused
rerun and the full-suite rerun pass. The macOS debug linker emits a nonfatal
`__eh_frame` compact-unwind size warning; no Rust/Clippy warning remains.

The existing CI workflow invokes the extended smoke without timing thresholds.
Smoke validates every synthetic spec with both engines, validates isolated result
artifacts and successfully compares matching cross-engine results. Whole-child
RSS is unavailable inside this sandbox and remains null, as in Phase 0. Remote
GitHub Actions was not triggered.

## Release comparison evidence

The locked optimized workspace build passed (4m 48s). All six isolated release
runs used one warmup, three measured iterations, batch_size=8192 and full parent
input verification. DataFusion used its defaults: two target partitions and
statistics disabled. ADE is synchronous and single-threaded. No tuning was done.

Environment: macOS kernel 27.0.0, aarch64, 8 logical cores, Rust 1.99.0,
aarch64-apple-darwin, system allocator, opt-level=3, thin LTO, codegen-units=1,
debug-info=1, assertions disabled. Sandbox hardware probes leave CPU model,
physical core count and RAM unavailable. All peak child RSS values are null.
Source provenance records build-time commit f6beacb with dirty state true.

Both deterministic fixtures contain 100,000 rows in three Snappy files with
8192-row row groups, seed 42 and the default 16-byte payload. The high fixture
uses 50,000 uniform possible keys; skew uses 10,000 possible keys with default
hot-key parameters. Logical fingerprints:

- high: `dfa66fbd1651c49abb2d026b3f8fc0360b0352d7debe2735feb2f5e8373b40dc`
- skew: `07669f106031f3b2d3dcdb9b0f493bd05b1d955c6a5246c76c52527c23b1f920`

All times below are query milliseconds; standard deviation is population stddev.
These are small warm-cache development samples, not performance claims.

| Workload | Engine | Rows output | Median ms | Mean ms | Stddev ms |
|---|---|---:|---:|---:|---:|
| filter/project | DataFusion | 25,099 | 3.814833 | 3.885194 | 0.100657 |
| filter/project | ADE | 25,099 | 6.266875 | 6.256431 | 0.024576 |
| high aggregate | DataFusion | 43,164 | 6.337792 | 6.337459 | 0.022999 |
| high aggregate | ADE | 43,164 | 12.107792 | 12.148972 | 0.166729 |
| skew aggregate | DataFusion | 9,930 | 4.498791 | 4.507458 | 0.019334 |
| skew aggregate | ADE | 9,930 | 7.370917 | 7.387320 | 0.078681 |

The existing compare command accepted all three pairs with matching validated
fingerprints and logical inputs. It classified ADE's median changes as regression:
+64.277%, +91.041%, +63.842%, respectively. Warnings were engine version/config
and dirty source tree. All six result artifacts passed `check-result`.

The implementation reads each file's data once, reuses projection/slice arrays,
and uses expected linear hash insertion followed by key sorting. No pathological
full-file reread, per-row batch or quadratic algorithm was found. ADE decodes all
columns and uses one thread; DataFusion has a mature optimized scan/aggregate
path and two target partitions. These structural differences plausibly contribute
to the observed gap; the experiment does not isolate their individual costs.

Artifacts remain ignored under `benchmarks/results/phase1-acceptance/` and inputs
under `benchmarks/datasets/phase1-acceptance/`. Direct release `validate` also
passed all five synthetic specs on the high fixture: scan 100,000 rows,
filter/project 25,099 rows, and each aggregate 43,164 groups. Spec names do not
constrain cardinality; the separate skew comparison above uses the skew fixture.
The extended smoke separately validates all five specs on its 9,930-group skew
fixture with both engines.

Reproduction (choose fresh directories because dataset/result outputs are immutable):

```sh
BIN=target/release/ade-bench
DATA=benchmarks/datasets/phase1-acceptance
RESULTS=benchmarks/results/phase1-acceptance
mkdir -p "$DATA" "$RESULTS"
$BIN env --format json
$BIN data generate synthetic --dataset-id phase1-high-v1 --rows 100000 --files 3 --row-group-rows 8192 --key-cardinality 50000 --output "$DATA/high"
$BIN data generate synthetic --dataset-id phase1-skew-v1 --rows 100000 --files 3 --row-group-rows 8192 --key-cardinality 10000 --distribution hot-key --output "$DATA/skew"
for workload in scan scan-filter-project aggregate-low-cardinality aggregate-high-cardinality aggregate-skew; do
  $BIN validate --engine ade-local --engine-option batch_size=8192 --spec "benchmarks/specs/$workload.toml" --dataset "$DATA/high"
done
for workload in scan-filter-project aggregate-high-cardinality aggregate-skew; do
  fixture=high
  if [ "$workload" = aggregate-skew ]; then fixture=skew; fi
  for engine in datafusion ade-local; do
    $BIN run --engine "$engine" --engine-option batch_size=8192 --spec "benchmarks/specs/$workload.toml" --dataset "$DATA/$fixture" --verify-data full --warmups 1 --iterations 3 --output "$RESULTS/$workload-$engine.json"
    $BIN check-result "$RESULTS/$workload-$engine.json"
  done
  $BIN compare "$RESULTS/$workload-datafusion.json" "$RESULTS/$workload-ade-local.json" --format json
done
```

These generation/validate/run/check-result/compare commands were executed through
a temporary acceptance shell script. The first preparation attempt failed before
creating a dataset or result because its parent dataset directory was missing;
creating that directory fixed preparation, with no code change or rebuild.

## Memory and metrics semantics

Each operator reports input/output batches/rows, exclusive work duration and
current/peak estimated memory; scan has no upstream input. Execution reports
collected rows/batches and current/peak tracked bytes. Current execution memory
on success represents retained collected batch estimates; operator current
values are reset when execution ends. Metrics are returned programmatically and
logged at debug by the adapter, without modifying benchmark artifacts.

Batch estimates use Arrow array memory sizes; shared buffers can be counted more
than once across operators and collection. Aggregate state uses capacity times
64 bytes, sampled after input batches, including an allowance for entry/control
storage. Reader internals, filter mask, allocation overhead, within-batch growth
and finalization temporaries are not exhaustively measured. Peak is monotonic;
tracker operations reject underflow/overflow. These are observational attribution
hooks, not allocator-exact RSS or enforceable reservations. Whole-child RSS is a
separate harness metric and includes correctness validation overhead.

## Known limitations

Single-threaded synchronous execution; no scan pushdown; aggregation consumes
memory proportional to cardinality and emits one batch; final outputs collect
in memory; limited predicate/type/null surface; no general SQL or Q1 in ADE.
Cancellation is cooperative at batch/file boundaries, not preemptive inside a
kernel. Limit zero still opens initial Parquet metadata to determine schema.
The physical plan and context APIs remain provisional. Benchmark timing includes
runtime operator construction; adapter preparation validates file schemas.

## Deferred Phase 2+ work

Hard memory limits, enforceable reservations, spilling, external sort, prefetch,
joins, exchange, distribution, scheduling, lineage/recovery, adaptation,
object stores, streaming/state/checkpoints, stable general physical IR, broad
expressions/SQL, hardware specialization and custom allocators are deferred.

## Recommendations for Phase 2

Evolve context-scoped memory hooks into reservations before allocating aggregate
state; distinguish ownership from shared batch observations. Bound output batch
sizes, account finalization/decoder temporaries and test pressure/error cleanup.
Introduce spill with explicit lifecycle and correctness tests, retaining the same
independent oracle and cross-engine benchmark contracts. Measure larger fixtures
before optimizing scan decoding or aggregate state layout.

## Acceptance audit

Every checkbox in plan section 22 has been verified and marked complete.

| Criteria | Evidence |
|---|---|
| Architecture: real independent crate, ADE-owned execution, no future crates, pull backpressure, provisional plan | Runtime code, dependency tree, ADR-0005; only ade-exec added |
| Runtime: scan/filter/project/aggregate/limit/collect | Real Parquet pipeline and edge tests; all five oracle validations |
| Runtime: cancellation, IDs, operator/execution metrics, observational memory | Core/pipeline tests, returned metrics, documented estimates and limitations |
| Integration: dispatcher/config, five synthetic specs, Q1 rejection, DataFusion intact | Table-driven test, direct CLI validation, existing Phase 0 suite |
| Cross-engine equivalence | Matching fingerprints in table test and three accepted release comparisons |
| Quality: focused tests, full tests, fmt/check/clippy | All commands above pass after documented fixes |
| CI: original smoke preserved, ADE added, no timing gates | Extended scripts/smoke.sh passes locally through existing CI entrypoint |
| Documentation and evidence | ADR-0005, AGENTS, README, this report, ignored release artifacts |
| Phase boundary | No memory enforcement, spill, distribution or other Phase 2+ implementation |

Phase 1 is complete. Phase 2 was not started.
