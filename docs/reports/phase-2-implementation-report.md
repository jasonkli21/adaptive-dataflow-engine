# Phase 2 implementation report

Date: 2026-10-15 (America/Los_Angeles).

## Baseline and final state

Baseline: `985e1a9b1a7406de2f7004cc21e097ce99d40942`, on `main`; HEAD matched
the supplied plan's reviewed snapshot. Initially untracked files were the Phase 2
plan and `sol-medium-phase-0-bootstrap-implementation-prompt.md`. The plan is
included in the implementation commit; the unrelated bootstrap prompt remains
untouched and untracked. The user's explicit instruction to implement Phase 2
advances the former Phase 1 boundary in AGENTS.md. No PRD scope change is needed.

Final state is the single Phase 2 commit containing this report, directly atop
the baseline; identify it with `git log -1 --format=%H`. Its hash is returned in
the completion message (a commit cannot embed its own final hash). No push.

## Summary and exact implemented scope

`ade-exec` now owns finite managed reservations, execution-local temporary spill,
spillable partitioned hash aggregation, Arrow-sorted IPC runs with bounded
multi-pass merge, and bounded threaded Parquet prefetch. Scan/filter/project/
limit/collect, deterministic postorder IDs, structured errors, cancellation and
the synchronous consumer pull model remain. No new crates, DataFusion dependency
in the runtime, distributed exchange, scheduler, recovery, SQL or Phase 3 work.

The benchmark adapter records four effective options and supports a sixth
synthetic workload: `sort-key-rowid`, ordered by key then row_id. DataFusion SQL
and a generator-based oracle implement the same contract. Existing workload
identities, integer canonicalization, child protocol and result schema remain
unchanged. Sort validation preserves output order and rejects descending pairs
across batch boundaries before fingerprinting.

## Managed memory and non-guarantees

Default limit is 256 MiB, spill enabled, prefetch disabled. Programmatic output
cap defaults to 1024 rows. Prefetch accepts depths zero through 64; batch size
and memory/output sizes must be positive. Duplicate/unknown adapter options,
invalid booleans, negatives and overflow values are rejected.

MemoryManager is execution-scoped and mutex protected; reservations own bytes,
can grow/shrink and release on drop. Failed growth does not mutate current/peak
or the reservation. Attempts, denials, state-target pressure events and owner
peaks are separate counters. Owner attribution includes collection via the
reserved sentinel OperatorId(u32::MAX), which cannot collide with practical
compiled plan positions.

Controlled table/run storage reserves before allocation. Aggregate output
vectors and sort output buffers reserve before construction. Collection charges
each retained Arrow batch's get_array_memory_size plus 128 bytes of batch/vector
metadata, then reserves Vec capacity before retention. ExecutionOutput keeps an
owned collection reservation; successful metrics show collection current bytes
and zero outstanding operator reservations. Shared buffers can be overcounted.

The configured value is **not a total RSS cap**. Parquet decoder, Arrow kernels,
IPC internals, allocator overhead, thread stacks, synchronous input, small plan/
service/metrics allocations and transient library work are external to the hard
managed ledger. Operator batch estimates remain observational. Prefetch batch
size is known after decode, then reserved before enqueue; dequeue ends queue
ownership. Conservative sort workspace allowances do not make third-party
allocations allocator-exact. A result too large to collect fails explicitly.

## Spill lifecycle and encoding

One execution-owned TempDir creates unique operator-tagged files with create_new.
Success drops the operator tree (joining producer and closing readers), then
explicitly closes the root and propagates cleanup errors. Failure/cancellation
uses RAII TempDir removal after operator teardown. Cleanup on process abort,
filesystem failure during Drop or machine loss is not promised. Spill is local
and disposable, never recovery-durable.

Aggregate codec is internal `ADEAGG01`: eight-byte version followed by 32-byte
little-endian u64 key/i128 sum/u64 count records. Readers validate header, length,
record alignment and positive counts. Sort uses Arrow IPC StreamWriter/Reader
with at most 128 rows per record batch. Consumed merge input files are deleted;
the final run and any partial files are removed with the execution root.

## Aggregate algorithm

A fixed power-of-two open-addressed table reserves actual slot-size bytes before
allocation. Target table bytes are one quarter of the execution budget, capped
at 65,536 slots, with maximum half-full load. Existing keys update checked i128
sum and u64 count without growth. A new group at the target triggers a pressure
event and partitions current partials into 16 deterministic hash buckets. Spill
disabled returns SpillDisabled at this target, preserving headroom. Appending to
one file per partition bounds metadata across spill generations. Only one
1024-byte buffered writer is active; metadata/serialization reserves 8192 bytes.

Finalization reconstructs one partition at a time in the same bounded table,
merges repeated partial keys and sorts its slots in place. Output vectors are
bounded by the configured output cap. A partition still too large returns
SpillNoProgress with an increase-budget diagnostic. No recursive repartition or
silent oversized allocation occurs. Output is deterministic by partition then
key; GROUP BY has no semantic global order. Checked Int64 output conversion
retains the Phase 1 overflow behavior.

## Sort algorithm

Non-null UInt64 key/row_id and Int64 value only; ascending key then row_id.
Arrow lexsort_to_indices sorts run buffers, with a managed workspace allowance
of 128 bytes per row. Capacity targets one quarter of budget and at most 65,536
rows. Each completed run is encoded in bounded IPC chunks. A fixed 64-level
array retains at most one run per binary-compaction level; merging equal levels
and the final fold both use fan-in two. Merge has at most two input chunks and
one output chunk, with 32 KiB reserved for merge buffers/allowance. Metadata
receives 16 KiB. No all-run reload, unbounded descriptor set or input-sized run
metadata vector. Sort output is capped at min(output_batch_rows, 128).

## Prefetch design

Depth zero uses the synchronous reader directly. Positive depth creates one
standard-library producer thread and bounded mutex/condvar queue. Slot metadata
is reserved before allocation; decoded batches are reserved before publication.
File and row order remain identical. Producer errors propagate, producer panics
become PrefetchFailed, and cancellation is checked cooperatively. Consumer drop
sets stop, clears queued reservations, wakes the producer and joins it. Full
queue/early limit/cancellation cannot strand a blocked sender. Blocking file I/O
is not preemptible and can delay shutdown. No detached worker, Tokio dependency
or global mutable service is added to ade-exec.

## Metrics and harness changes

ExecutionOutput exposes managed limit/current/peak, attempts/denials/pressure,
per-owner managed peaks, spill files/events/read/write bytes, per-operator spill
events, initial sort runs/merges, and prefetch produced/consumed/queue/batch peaks.
Existing operator input/output counters and exclusive work time remain. Scan
I/O wait is included in its work time. Prefetch metrics on early stop reflect
activity observed at the last pull; producer teardown is joined before return.

`ADE_LOG=debug` enables concise counters for standalone `validate` on stderr.
Measured child/run paths retain warn-level logging to preserve their recorded
measurement policy. Result schema v1 is unchanged; metrics evidence is captured
separately from measured children. EngineInfo records all effective resource
options. CI smoke preserves original validations/comparisons, adds sort on both
engines, and asserts real aggregate/sort spill, prefetch depth and managed peaks
for constrained validations. No timing gate or unrelated TPC-H regeneration.

## Deviations and durable decisions

ADR-0006 supersedes only ADR-0005's observational memory/spill limitations,
refining architecture 7–8 and preserving INV-4/5/6/7/9/10. Fixed hash slots replace
a growable HashMap to make capacity admission explicit. The plan permits a clear
no-progress error instead of recursive partitioning; that option is implemented.
Two-way online compaction is the bounded multi-pass sort implementation, chosen
over a more general heap/fan-in configuration. Source is modularized into five
cohesive modules, with unary dispatch retained in lib.rs. Tempfile moves from
dev-only to production and Arrow IPC is enabled at existing pinned versions;
Cargo.lock has no delta. Pinned public Arrow sort/IPC and Parquet reader sources
were inspected locally. No upstream/private API or dependency upgrade.

## Commands actually run

The installed cached toolchain is selected with:

```sh
export PATH=/private/tmp/ade-cargo/bin:$PATH
export RUSTUP_HOME=/private/tmp/ade-rustup
export CARGO_HOME=/private/tmp/ade-cargo
```

Focused iteration and final validation commands:

```sh
cargo check -p ade-exec --offline
cargo test -p ade-exec --locked --offline
cargo clippy -p ade-exec --all-targets --locked --offline -- -D warnings
cargo test -p ade-bench --test phase2 --locked --offline
cargo test -p ade-bench sort_validation_rejects_wrong_order --lib --locked --offline
cargo fmt --all
cargo fmt --all --check
cargo check --workspace --all-targets --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo test --workspace --all-features --locked --offline
bash scripts/smoke.sh
git diff --check
cargo build --release --workspace --locked --offline
```

Initial checks found unused imports, missing public memory API documentation,
Clippy formatting/dispatch length findings and a missing Path import during a
refactor; fixed before acceptance. The first integrated Clippy gate found five
missing semicolons in adapter/oracle match arms; fixed and rerun successfully.
A late review restricted optional debug logs to standalone validation to keep
measured-child log policy truthful; subsequent affected checks are recorded below.
Nonfatal macOS linker __eh_frame/compact-unwind size warnings remain as in Phase 1.
No cargo clean, Criterion run, remote CI execution or TPC-H regeneration.

## Focused tests and cleanup evidence

Eleven runtime tests pass. They cover exact managed limit, failed growth/underflow/
overflow without byte mutation, multiple reservations per owner, RAII release,
existing operator semantics, repeated aggregate keys across forced generations,
forced partition spill and sort multi-pass compaction, tie ordering, bounded
output, no-spill rejection, hot-key reduction, empty sort, corrupt spill version/
length, checked partial sum/count overflow, output overflow, and a partition
that cannot make progress under a 64 KiB budget.

Prefetch depths 1/3 match depth-zero row order and obey queue bounds. Missing
producer input files propagate errors; early limit returns and joins. Active
prefetch is cancelled after its first batch and all managed reservations release. Tests
observe a known spill parent after success, post-spill I/O failure and cancellation
of both blocking operators: zero remaining execution directories. A retained
MemoryManager clone reports zero current bytes after cancellation/error teardown.
A scan exceeding collection budget fails with structured MemoryLimit.

An additional spill-reader regression test confirms reading eight bytes from a
100-byte file reports eight bytes, rather than counting file length at open.

The Phase 2 adapter table validates all existing synthetic specs and new sort on
DataFusion and ADE, with generous and constrained configurations; fingerprints
match. Separate oracle test proves wrong order across batches is rejected. Existing
Phase 0 protocol/TPC-H correctness and Phase 1 tests remain intact.

## Integrated gate and smoke

Formatting, workspace/all-target check, all-target/all-feature Clippy with
-D warnings (after the semicolon fix), and all-feature workspace tests pass.
Suite: 19 ade-bench unit tests, 11 ade-exec unit tests, five Phase 0 integration
tests, one Phase 1 test, one Phase 2 test and doc-test targets. Extended smoke
passes all six specs on both engines, immutable result/check-result/compare,
and constrained aggregate/sort assertions. Smoke uses its existing 100,000-row
hot-key fixture, including 9,930 aggregate groups. No performance thresholds.

## Focused release acceptance evidence

The final locked optimized build passed in 5m 26s. Two earlier builds completed
in 6m 19s and 5m 04s before the concrete review fixes; an intermediate build was
interrupted, and superseded executables were excluded from acceptance. All five
runs below use the final executable, one warmup and three measured isolated
children, full parent input verification, batch_size=8192 and warn logging.
Diagnostic standalone validations separately capture runtime counters.

Environment: macOS kernel 27.0.0, aarch64, eight logical cores, Rust 1.99.0,
aarch64-apple-darwin, system allocator, opt-level=3, thin LTO, one codegen unit,
debug-info=1, assertions disabled. CPU model, RAM, physical cores and whole-child
peak RSS are unavailable in the sandbox and recorded as null. Build provenance
is baseline 985e1a9 with dirty=true because the implementation is measured before
its final commit. DataFusion 55.2.0 uses two target partitions and statistics
disabled. ADE remains a single consumer with optional one-thread scan overlap.

The deterministic uniform fixture has 100,000 rows, 20,000 possible keys, seed
42, three Snappy Parquet files, 8192-row groups and 16-byte payload. Logical input
fingerprint: `5235428044ea27e6d8fbf48a0e66c6b08cb866345e56362bbfc45cb55dfe543d`.
Its aggregate result contains 19,870 groups; sort returns all 100,000 rows.

| Workload / engine | Managed budget | Prefetch | Median ms | Mean ms | Population stddev ms |
|---|---:|---:|---:|---:|---:|
| Aggregate ADE generous | 256 MiB | 0 | 8.943000 | 9.124917 | 0.282682 |
| Aggregate ADE constrained | 2 MiB | 1 | 80.956250 | 79.694708 | 5.398701 |
| Aggregate DataFusion | baseline defaults | baseline | 5.208875 | 5.230472 | 0.109646 |
| Sort ADE constrained | 4 MiB | 1 | 215.024000 | 215.039861 | 0.459892 |
| Sort DataFusion | baseline defaults | baseline | 10.047542 | 10.057070 | 0.088303 |

All warmups/measured children pass the independent oracle. All five artifacts
pass check-result. Aggregate generous/constrained/DataFusion fingerprints match:
`e35a05cfd39a20e3ed5c81392671996c011bad5dcd8576da047767d1379aa73b`.
Both ordered sort fingerprints match:
`d6cadf78633a6902647bcada168e56e34497821a58433d84b2ec16107ea371ef`.
The compare tool accepts all three semantic comparisons (aggregate memory pair,
DataFusion versus constrained aggregate, and DataFusion versus sort).

| Diagnostic ADE path | Managed peak / limit, bytes | Pressure events | Files / spill events | Written / read bytes | Sort runs / merges | Prefetch produced / consumed / peak |
|---|---:|---:|---:|---:|---:|---:|
| Generous aggregate | 3,639,728 / 268,435,456 | 0 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 / 0 |
| Constrained aggregate | 992,092 / 2,097,152 | 21 | 16 / 22 | 2,864,064 / 2,864,064 | 0 / 0 | 15 / 15 / 1 |
| Constrained sort | 2,765,312 / 4,194,304 | 12 | 25 / 25 | 13,664,328 / 13,664,328 | 13 / 12 | 15 / 15 / 1 |

Reservation attempts were 42, 98 and 1596 respectively; denials were zero because
state targets trigger proactive spilling before exhausting the global budget.
Retained collection bytes were 485,200, 490,192 and 2,725,312. Aggregate partition
batch boundaries account for the small collection-metadata difference. Output
batch bounds are tested directly in runtime tests. Successful execution explicitly
closes its spill root before returning; known-parent lifecycle tests also verify
success/error/cancel removal.

Compare classifies the timing deltas as regressions: constrained aggregation is
+805.247% versus generous ADE and +1454.198% versus DataFusion; external sort is
+2040.066% versus DataFusion. Warnings are engine/configuration differences and
dirty source. These are small warm-cache development samples, not performance
claims or gates. DataFusion is not constrained to the ADE budget. Conservative
fixed targets, scalar hash probing, tiny IPC chunks and multi-pass local I/O
favor transparent boundedness over speed; the experiment does not isolate their
individual costs. No performance tuning or follow-on scope is included.

Artifacts remain ignored under `benchmarks/results/phase2-acceptance/`; inputs
are ignored under `benchmarks/datasets/phase2-acceptance/uniform/`. The temporary
acceptance script executed generation, five run/check-result pairs, three
standalone metric validations and the three compare commands. Reproduction uses
fresh immutable output paths and the following settings:

```sh
BIN=target/release/ade-bench
DATA=benchmarks/datasets/phase2-acceptance
RESULTS=benchmarks/results/phase2-acceptance
mkdir -p "$DATA" "$RESULTS"
$BIN env --format json
$BIN data generate synthetic --dataset-id phase2-uniform-v1 --rows 100000 --files 3 --row-group-rows 8192 --key-cardinality 20000 --output "$DATA/uniform"
# Run this shape for each engine/configuration in the table above:
$BIN run --engine ade-local --engine-option batch_size=8192 --engine-option memory_limit_bytes=2097152 --engine-option prefetch_batches=1 --spec benchmarks/specs/aggregate-high-cardinality.toml --dataset "$DATA/uniform" --verify-data full --warmups 1 --iterations 3 --output "$RESULTS/aggregate-constrained.json"
$BIN check-result "$RESULTS/aggregate-constrained.json"
ADE_LOG=debug $BIN validate --engine ade-local --engine-option batch_size=8192 --engine-option memory_limit_bytes=2097152 --engine-option prefetch_batches=1 --spec benchmarks/specs/aggregate-high-cardinality.toml --dataset "$DATA/uniform"
# Generous aggregate: same run, memory_limit_bytes=268435456, prefetch_batches=0.
# DataFusion: engine=datafusion, batch_size=8192, omit ADE-only options.
# Sort ADE: sort-key-rowid.toml, memory_limit_bytes=4194304, prefetch_batches=1.
$BIN compare "$RESULTS/aggregate-generous.json" "$RESULTS/aggregate-constrained.json" --format json
$BIN compare "$RESULTS/aggregate-datafusion.json" "$RESULTS/aggregate-constrained.json" --format json
$BIN compare "$RESULTS/sort-datafusion.json" "$RESULTS/sort-ade-local.json" --format json
```

The final source passed the full gate again after the ownership/zero-shrink
review fixes: 37 tests, fmt/check/Clippy, extended smoke and whitespace checks.

## Final review and affected rechecks

Final review changed spill read-byte accounting from file-size-at-open to actual
Read-returned byte counts (including codec read-ahead), and added its regression
test. It also restricted diagnostic logging to standalone validation and included
blocking finalization in exclusive operator timing. Affected runtime tests,
Phase 2 adapter tests, workspace check/Clippy, protocol roundtrip and smoke were
rerun as applicable and pass. The first release build was already in progress;
optimized rebuilds are necessary to include those concrete review fixes. Review
also caught a shadowed original sort Vec surviving a reservation release; an
explicit drop now ends its allocation before the release. Zero-byte shrink of
an empty reservation is tested as a no-op. One superseded build was interrupted
before acceptance; only the final executable supplies release evidence. No
broader benchmark matrix or repeated unchanged release build is used.

Source review checked reservation-before-allocation for fixed hash slots/run
vectors/output buffers; RAII failure paths; partition partial merging; explicit
no-progress termination; fixed merge fan-in and run metadata; bounded output;
ordered validation; full-queue stop/join; collection accounting; independent
runtime dependencies; and absence of global state or Phase 3 abstractions.

## Known limitations and deferred items

- Managed bytes are an explicit ownership ledger with conservative allowances,
  not RSS or an allocator-enforced process limit. External/transient exceptions
  are listed above; whole-child peak RSS is unavailable in this sandbox.
- Sixteen aggregate partitions can return no-progress under distinct-key skew
  or insufficient budget. Recursive skew splitting is deferred. Output order
  for spilled aggregates is partition/key order, with unchanged GROUP BY semantics.
- Blocking state has derived targets and headroom; spill-disabled mode can fail
  before the entire execution budget is consumed. Table/run caps favor simplicity.
- Collection still retains the whole result. It can fail under a budget that is
  otherwise adequate for external operators. There is no streaming output sink.
- Sort has a deliberately narrow type/null/order surface. Equal key/row_id ties
  have no stable-order promise; synthetic row_id is unique. Two-way merging and
  128-row IPC chunks prioritize predictable boundedness over disk efficiency.
- No scan pushdown; decoding uses caller batch size. Prefetch needs a post-decode
  reservation and can return memory pressure when queued ownership cannot fit.
  Cooperative cancellation does not interrupt a blocked filesystem operation.
- TempDir Drop cleanup is best effort on filesystem failures/process termination;
  spill is not recovery material. Read-byte counters include real codec read-ahead.
- Runtime metrics are available programmatically and during diagnostic validation,
  not added to immutable benchmark result schema v1. Successful benchmark children
  run at warn level. Local timing samples are development evidence only.
- Physical plan remains provisional; no joins, broad SQL, TPC-H in ADE, remote
  spill, exchange, distribution, scheduling, recovery, lineage, adaptation or
  streaming work was introduced.

## Recommended next-phase considerations

Use these local mechanisms as the starting point for Phase 3; do not assume a
managed ledger is a process-wide allocator limit. Explicitly design ownership
transfer for exchange buffers and cancellation across workers. Consider recursive
partition processing and a streaming/non-collecting sink before claiming arbitrary
larger-than-memory result support. Improve IPC chunk sizing/fan-in and measure
transient memory before optimizing based on small-machine timings. Preserve
ordered oracle validation and process-isolated cross-engine evidence.

## Acceptance audit

| Phase 2 plan category | Audit and evidence |
|---|---|
| Architecture / boundaries | Two crates only; dependency tree shows Arrow/Parquet/tempfile/thiserror in ade-exec; pull consumers, bounded producer and no Phase 3 code; ADR-0006 |
| Memory manager | Finite validated configuration, actual fixed-slot and run reservations before allocation, atomic failures, RAII release and owner peaks; exact-limit/overflow/underflow/error tests; collection charged; external bytes documented |
| Spill | Unique execution TempDir, create-new operator files, actual read/write/file/event counters; known-parent cleanup after success, error and cancellation; explicitly local/lossy |
| Aggregation | Checked semantics, deterministic partitions, repeated partial merge, forced generations, hot keys, bounded batches; explicit tested no-progress and too-small errors |
| External sort | Arrow run sort, chunked IPC spill, two-way multi-pass merge, fixed level metadata, ordered/tie tests, bounded output, cleanup on error/cancel |
| Prefetch | Configured 0–64 depth; matching row order at depth zero/positive; bounded queue metrics, errors, early limit, cancellation, stop/wake/join lifecycle |
| Benchmark integration | Six synthetic specs on ADE; sort DataFusion SQL and independent ordered oracle; explicit configuration; constrained aggregate/sort validation and unchanged protocol/result schema |
| Quality / validation | Focused tests, workspace fmt/check/Clippy/tests, extended smoke and whitespace checks pass; final affected rechecks described above; no timing gate |
| Release evidence | Five isolated release runs, check-result, semantic comparisons and diagnostic metrics recorded in the release section |
| Documentation / phase boundary | AGENTS, root/benchmark README, supplied Phase 2 plan, ADR-0006 and this report; no Phase 3 implementation |

Phase 2 acceptance is complete. Phase 3 has not started.
