# Adaptive Dataflow Engine (ADE)

ADE is a research-oriented Rust project exploring a distributed execution engine where **materialization and recomputation are adaptive decisions rather than fixed properties of the processing model**.

The long-term goal is a general-purpose data-processing substrate with Spark/Flink-class breadth, but the project deliberately starts much smaller. The early research spine is:

```text
benchmark substrate
    -> local execution
    -> bounded memory / spill
    -> static distribution
    -> unified exchange
    -> adaptive exchange
    -> lineage-aware recovery
    -> cost-aware materialization
    -> adaptive partitioning
```

## Current status

**Phase 13 bounded remote v1 is complete (`COMPLETE_BOUNDED_REMOTE_V1`).** The default-off `remote-s3` feature and separate `ade-remote` API/CLI execute static GroupBySum over a real S3-compatible service, immutable checked shuffle manifests, fresh reader processes after all producers exit, bounded I/O and exact-key quiesced cleanup. The local fixture proves compute/storage process separation; full cloud HA, cross-host/AZ durability, dynamic membership, serverless and remote `ade-batch` remain deferred. All G0–G8 gates pass with source-bound correctness, fault, traffic and cost evidence. See the [Phase 13 guide](docs/phase-13-operations-guide.md), [ADR-0020](docs/adr/0020-remote-disaggregated-shuffle-v1.md) and [report](docs/reports/phase-13-implementation-report.md). Existing defaults and historical evidence are preserved.

**Phase 12 bounded v1 is complete.** The opt-in `ade-ops` / `ade-batch` path runs static distributed aggregates and bounded HashOnly inner joins on fixed, externally supervised 2–4 managed workers. It adds durable jobs/attempts, worker fencing and proved drain before whole-job retry, cancellation, checked runtime/output budgets and atomic no-clobber IPC publication. Ambiguous worker sessions require an operator; operator spill is disabled in operations. Full replicated/cross-host HA and worker replacement remain deferred. See the [operations guide](docs/phase-12-operations-guide.md), [ADR-0019](docs/adr/0019-durable-batch-operations.md) and [implementation report](docs/reports/phase-12-implementation-report.md). Existing query/benchmark defaults and historical Phase10 evidence retain their contracts.

**Phase 11 bounded v1 adds SQL and a typed Rust DataFrame API that compile to ADE-owned physical IR and execute through ADE operators.** It is explicitly authorized despite the separate Phase 10 debt below. Supported targets are local unary queries, static unfiltered sum/count aggregation, and bounded two-source inner joins. See the [query guide](docs/phase-11-query-guide.md), [ADR-0018](docs/adr/0018-physical-ir-and-frontend-v1.md), and [Phase 11 report](docs/reports/phase-11-implementation-report.md). This is a bounded frontend, with explicit refusals for general SQL; no DataFusion execution fallback exists.

**Phase 10 remains partial: its bounded leased scheduler now passes all six unchanged local release timing guards.** Continued remediation at `ee9f8da` passes 62 isolated children, 49 measured samples, independent oracle/source/lease/terminal checks, and the three reached failure stages. The original source and first remediation at `fcc325e` failed all six guards; those results remain preserved. Uniform/4 passes narrowly (1.144× static, limit 1.15×). Static scheduling remains the supported default. Leasing, stealing and producer activation/drain stay within 2–4 fixed processes; physical worker addition/removal is deferred. See the [readiness report](docs/reports/repo-wide-readiness-2026-10-26.md) and [Phase 10 report](docs/reports/phase-10-implementation-report.md).

**Phase 9 bounded v1 is complete with review fixes: observed broadcast/repartition joins, real hash-to-grace conversion and opt-in adaptive partial aggregation preserve the declared result contract.** Phase8 is closed with immutable public evidence. The published Phase9 timing gates remain historical measurements of the frozen source and are not freshly measured for the review-fix commit. `ade-distributed` runs scan → partial aggregate → network hash exchange → final merge aggregate → gather across 2–4 static worker processes. One final-worker loss after all original producers complete can recover the missing partition through verified full-copy restore or lineage replay on a survivor. Other failures cancel the job.

Phase 2 local execution remains available. `ade-exec` owns managed memory, partitioned aggregate spill, external sort and bounded Parquet prefetch alongside scan/filter/project/limit/collect. `ade-local` executes six synthetic specs through the independent oracle, including order-aware sort validation. The aggregate adapter preserves synthetic GroupBySum. The separate `ade-distributed-join` adapter adds bounded two-source inner equijoin with an independent DataFusion SQL bag oracle; its frozen release evidence passes the predeclared Phase9 guards and Phase6–8 regressions. See the [Phase9 report](docs/reports/phase-9-implementation-report.md) for the measured tradeoffs, review-fix checks, and immutable evidence.

The completed Phase 0 architecture/benchmark lab in `crates/ade-bench` provides deterministic datasets, engine-independent workload definitions, a DataFusion baseline, process-isolated macrobenchmarks, versioned result artifacts, correctness validation, comparison tooling, and microbenchmark infrastructure.

## Read first

Implementation work should use this order:

1. [`AGENTS.md`](AGENTS.md) — repository working rules and handoff conventions.
2. [`docs/product/prd.md`](docs/product/prd.md) — product/research thesis and long-term phases.
3. [`docs/architecture/architecture.md`](docs/architecture/architecture.md) — architectural boundaries and invariants.
4. [`docs/plans/phase-13-implementation-plan.md`](docs/plans/phase-13-implementation-plan.md) — accepted bounded remote-object batch scope; Phase12 remains local bounded operations, Phase10 remains PARTIAL and historical evidence retains its source identity.
5. [`docs/adr/`](docs/adr/) — concrete accepted design decisions.
6. [`docs/reports/`](docs/reports/) — implementation state and phase reports.

## Repository shape

```text
.
├── AGENTS.md
├── Cargo.toml
├── crates/
│   ├── ade-ops/                # partial durable operations, ade-batch CLI
│   ├── ade-physical/           # strict versioned ADE graph contract
│   ├── ade-frontend/           # SQL binder, Rust DataFrame, ade-query CLI
│   ├── ade-distributed/        # static coordinator, workers, TCP/IPC exchange
│   ├── ade-exec/               # Phase 2 local runtime
│   └── ade-bench/              # Phase 0 benchmark harness
├── benchmarks/
│   ├── specs/
│   ├── datasets/               # generated; ignored
│   ├── results/                # generated; ignored
│   └── examples/
├── docs/
│   ├── product/prd.md
│   ├── architecture/architecture.md
│   ├── plans/phase-0-implementation-plan.md
│   ├── adr/
│   ├── research/
│   └── reports/
└── .github/workflows/ci.yml
```

The architecture describes additional future crates. **Do not create them until a phase gives them real ownership.** Empty future modules increase context cost and prematurely freeze interfaces.

## Development philosophy

- correctness before performance;
- measure before optimizing;
- mechanism separate from policy;
- explicit memory/resource accounting;
- static baselines for adaptive features;
- observable optimizer decisions;
- reuse Arrow/Parquet/open ecosystem infrastructure where it is not differentiating;
- expand scope only after the current phase is validated.

## Phase 0 quick start

Use stable Rust 1.94 or newer. The implemented workflow is:

```bash
cargo run -p ade-bench --release -- env
cargo run -p ade-bench --release -- data generate synthetic --help
cargo run -p ade-bench --release -- validate --help
cargo run -p ade-bench --release -- run --help
cargo run -p ade-bench --release -- compare --help
cargo bench -p ade-bench
```

See [`benchmarks/README.md`](benchmarks/README.md) for executable generation/run/compare examples, TPC-H-derived setup, and dataset size recipes. Read the [methodology](docs/research/benchmarking-methodology.md) before interpreting local measurements. The [implementation report](docs/reports/phase-0-implementation-report.md) records acceptance evidence and limitations.

Run `bash scripts/smoke.sh` after `cargo build --workspace` for the CI correctness flow. Result artifacts are immutable: choose a new output path for every run.

## ADE local execution

After generating a synthetic fixture using the benchmark quick start:

```bash
target/release/ade-bench validate --engine ade-local --engine-option batch_size=8192 --spec benchmarks/specs/scan-filter-project.toml --dataset benchmarks/datasets/example
target/release/ade-bench run --engine ade-local --spec benchmarks/specs/aggregate-skew.toml --dataset benchmarks/datasets/example --verify-data full --warmups 1 --iterations 3 --output benchmarks/results/ade-example.json
```

The effective options are recorded in benchmark engine configuration:

| Option | Default | Meaning |
|---|---:|---|
| `batch_size` | 8192 | Positive Parquet scan batch size |
| `memory_limit_bytes` | 268435456 | Positive execution managed budget (256 MiB) |
| `spill_enabled` | true | Allow local aggregate/sort spill; false fails at state target |
| `prefetch_batches` | 0 | Queue depth 0–64; zero reads synchronously |

For example:

```bash
ADE_LOG=debug target/release/ade-bench validate --engine ade-local --engine-option memory_limit_bytes=4194304 --engine-option prefetch_batches=1 --spec benchmarks/specs/sort-key-rowid.toml --dataset benchmarks/datasets/example
```

`ADE_LOG=debug` exposes pressure, managed memory, spill/run and prefetch counters
on stderr. Programmatic `ade_exec::execute_with_config` also returns per-operator
metrics and accepts an output batch cap. Result artifacts retain schema v1.

The budget covers reserved blocking state, explicit queued batches and retained
collection; it is **not a total-process RSS cap**. Decoder/kernel/IPC internals,
synchronous input and allocator overhead are external/transient. A collected
result that cannot fit fails explicitly even if its operator spills. Aggregate
uses 16 local partitions and fails clearly if a partition still cannot fit;
external sort uses bounded IPC chunks and two-way multi-pass merging. Temp spill
is cleaned on normal success/error/cancellation, and has no recovery durability.
Prefetch preserves pull backpressure and joins its producer on teardown.

The physical plan is provisional. Sort supports non-null UInt64 key/row_id and
Int64 value, ascending key then row_id. TPC-H Q1 is supported only by DataFusion.
The local tree has no SQL frontend. A separate typed two-source helper in `ade_exec::join` implements bounded bag equijoin, including actual hash-to-grace conversion; it is not a general optimizer or stable physical IR. Distribution is owned by the separate distributed crate below.
See [ADR-0006](docs/adr/0006-managed-local-memory-and-spill.md) and the
[Phase 2 report](docs/reports/phase-2-implementation-report.md) for contracts,
validation and comparison evidence.

## License

No open-source license has been selected in this scaffold. Add one intentionally before relying on public reuse semantics.

## ADE distributed execution

Build both binaries with `cargo build --workspace --release`. The benchmark
adapter starts loopback workers on OS-selected ports, owns their lifecycle, and
validates the collected result with the existing independent oracle.

```sh
ADE_LOG=debug target/release/ade-bench validate --engine ade-distributed \
  --engine-option workers=2 --spec benchmarks/specs/aggregate-skew.toml \
  --dataset benchmarks/datasets/skew-v1
```

Options: `workers=2` (2–4), `batch_size=8192`, `memory_limit_bytes=268435456`,
`spill_enabled=true`, `prefetch_batches=0` (0–64),
`exchange_mode=local_materialized`, `exchange_buffer_bytes=4194304`.
Hash exchange modes are `direct`, `buffered`, `spill`, `local_materialized` and
`object_store`, plus explicit `adaptive`. Buffer limits are 1 byte–256 MiB; `buffered` requires at least
1 MiB. `spill` can use a tiny limit to force disk. Direct/local/object modes
validate/report the buffer setting but do not use it as retention capacity.
Effective configuration and
fixed protocol limits appear in engine metadata; standalone debug validation
prints per-worker/network metrics. `ADE_WORKER_BIN` can select the worker binary;
otherwise `ade-worker` is resolved beside `ade-bench`.

The Rust coordinator API also accepts static endpoints. `ade-worker WORKER_ID
TEMP_PARENT` binds loopback control/exchange ports and writes a framed readiness
record to stdout; logs use stderr. Gather currently binds loopback, so this is a
local multi-process deployment. Final consumers and producers overlap, with at
most one original task of each role per worker. Recovery runs sequentially on a
quiescent survivor after those roles finish. The local memory
limit applies per task; two task budgets may coexist. Exchange retention and
bounded frame transients (1 MiB, 1024 rows) are separate from the local ledger;
this is not a process RSS cap. Direct uses rendezvous handoff, buffered uses a
byte-bounded FIFO, spill uses that FIFO plus a fixed-threshold local disk log,
and materialized modes wait for every producer to finish. Local storage is lossy.
Object-store mode defaults to the public provider-neutral API with an ephemeral
local filesystem backend. Embedded worker callers can install an
`ObjectStoreBackend` through `worker::serve_with_object_store`; job configuration
names the backend and its declared durability capability. ADE reports that
capability but does not verify it. The default does not demonstrate
cloud/multi-host durability or recovery. A retention operation that outlives its
deadline poisons the worker, which then rejects new jobs. All storage is
execution-scoped. Gather stays fixed and bounded in
transit; coordinator gathering/oracle validation collect the final result. See
[Phase 4 report](docs/reports/phase-4-implementation-report.md) and
[unified exchange ADR](docs/adr/0010-static-unified-exchange.md).

Adaptive mode runs deterministic `threshold_v1` at validated segment boundaries.
Destinations independently escalate through direct → buffered → spill → object
store. `adaptive_policy=threshold_v1`, `adaptive_buffer_high_pct=80` (1–100) and
`adaptive_remote_after_bytes=67108864` (0 disables remote, maximum 1 TiB) are
accepted only with `exchange_mode=adaptive`. Typed static configurations ignore
the serialized adaptive defaults. Equality at the high watermark escalates;
remote escalation also requires an unconsumed spill backlog. No downgrade or
routing change occurs. Live local prefixes drain before finalized object suffixes;
all tiers feed the same aggregate. At most three transition records per
partition expose exact ordinal/transfer/sequence, reason and observation values.
Debug metrics include tier byte/segment totals and job transition summaries.

The delay hook is compiled only for the `ade-bench` research-hooks build; the
default `ade-distributed` library API has no delay option. Benchmark options are
`research_slow_partition=N`,
`research_delay_after_segments=1`, `research_delay_ms=750`. They delay that final
consumer once, default off, with a maximum two-second cancellation-aware delay.
Effective metadata records these options. They change no query semantics.

Run `bash scripts/phase5-acceptance.sh` after a release workspace build for the
predeclared normal buffered/adaptive comparison, delayed direct/spill/adaptive
comparison, and limited four-worker adaptive evidence. It preserves raw oracle
validated iterations and checks the local 1.25× normal median guard plus the
narrow adversarial win over direct; these timing checks do not run in CI.
See [ADR-0012](docs/adr/0012-adaptive-exchange-v1.md) and the
[Phase 5 report](docs/reports/phase-5-implementation-report.md).

Phase 6 recovery is enabled by default in the benchmark adapter
(`recovery_enabled=true`), which supplies verified manifest/file lineage. The Rust
`StaticAggregateJob.snapshot` is optional; an absent or non-replayable snapshot
refuses recovery. With every original producer complete, exactly one unavailable
final worker, and every other final successful, the planner chooses the lowest
eligible survivor. It replays original producer file closures emitting only the
missing partition, validates terminal counts and BLAKE3 fingerprints, and runs the
existing final merge aggregate. Original logical IDs and JobId remain unchanged;
attempt 1 replaces the incomplete attempt 0 in a separate gather store. Successful
unaffected finals are reused. One recovery episode is allowed.

This supports a narrow final-worker-loss window. Incomplete producers, multiple
losses, source drift, replay mismatch, poisoning, ambiguous gather state or failed
recovery return no partial output. File morsels are whole manifest files; there
is no row-group replay, worker replacement or coordinator HA. The default
Phase 6 path recomputes; explicitly selected Phase 7 expected-cost policy can
restore verified complete copies as described below. Inputs must remain immutable
during the job.

The research build accepts `research_exit_final_partition=0` (default off), which
exits that real process after sending the first non-empty gather batch, before
remaining output batches and terminal completion. The acceptance fixture emits
multiple batches, proving a strict partial prefix. The coordinator releases the
exit only after all original producers succeed and confirms exactly one batch
reached the gather store before recovery. The default distributed library has no
hook. Effective metadata records the hook and recovery policy. Debug
metrics expose lost ranges, compact producer closures, replay manifests and
morsel counts derived from the prepared snapshot, host, committed attempts and
elapsed time. Zero-segment transfers receive empty replay terminals without
source scans.
Missing-worker adaptive history is unavailable; summaries retain surviving
observations and recovered finals report their actual local materialization.

After a release workspace build on a clean checkout, run
`bash scripts/phase6-acceptance.sh`. It checks normal/recovered/unrecoverable
execution, five static modes and limited adaptive two/four-worker regressions,
with independent oracle validation and source/build provenance. There is no
recovery timing threshold. See [ADR-0013](docs/adr/0013-lineage-recovery-v1.md) and
the [Phase 6 report](docs/reports/phase-6-implementation-report.md).

### Expected-cost complete materialization

Select `exchange_mode=adaptive adaptive_policy=expected_cost_v1` explicitly.
Default/static and `adaptive_policy=threshold_v1` behavior remains available.
Expected-cost delivery uses direct/buffer/spill; a separate complete transfer copy
may survive a worker process loss. The adapter installs a parent-owned shared
filesystem fixture, **not host/AZ/cloud durability**. Snapshot/recovery must be
available. The existing one-final-worker-loss envelope and attempt-safe gather
commit remain; corrupt/missing retained objects fail cleanly.

Options use `cost_` plus the field names in
[`CostConfig`](crates/ade-distributed/src/cost.rs), with explicit units. Defaults,
all prices, configured throughputs/probability, measured work/source/output and
each decision are recorded. For example:

```bash
cargo run -p ade-bench -- validate --engine ade-distributed \
  --engine-option exchange_mode=adaptive \
  --engine-option adaptive_policy=expected_cost_v1 \
  --engine-option cost_failure_probability_ppm=900000 \
  --engine-option cost_source_read_nano_usd_per_byte=1000 \
  --spec benchmarks/specs/aggregate-high-cardinality.toml --dataset YOUR_DATASET
```

`cost_mode=always|never` are full-copy acceptance baselines, default `expected`.
Cost options require expected-cost selection. Deadline penalties use the complete
write-before-loss plus restore-after-loss path, and recovery closures share elapsed
slack as they run. The explicit request-cost scope is the PUT/HEAD/GET transfer
proxy; later cleanup LIST/DELETE charges are excluded. Staging overhead is real and
separately measured. Phase 7 used control framing `ADETCP06`; the current Phase 8 remediation
used `ADETCP10` at its frozen closure source for source-coverage, original-prefix and routing evidence. Current Phase9 control uses `ADETCP11`; aggregate schema1 wire bytes, wire v2, hash v1 and result schema v1 remain.

See [ADR-0014](docs/adr/0014-expected-cost-materialization-v1.md) and the
[Phase 7 report](docs/reports/phase-7-implementation-report.md). Release economic
acceptance: `bash scripts/phase7-acceptance.sh` from a clean checkout; raw results
are ignored under `benchmarks/results/phase7-acceptance`. Package a successful
run with `python3 scripts/phase7-bundle.py RESULTS_DIR OUTPUT.tar.gz` to create a
checksummed, self-identifying evidence bundle.

For raw measured-child diagnostics, set `ADE_DISTRIBUTED_METRICS_FILE` to an
append-only JSONL artifact path. This opt-in sidecar records PID and distributed
metrics; benchmark result schema v1 is unchanged. Phase 7 acceptance uses it.


### Bounded runtime repartitioning (Phase 8)

The [Phase 8 closure report](docs/reports/phase-8-implementation-report.md) records
both passing confirmations, all pooled measurements and prior failures. Raw/source
evidence is available in the [immutable release](https://github.com/jasonkli21/adaptive-dataflow-engine/releases/tag/phase8-evidence-2026-10-22).
Local timing variation limits causal and production claims.

`partition_policy=static` remains the default. Explicit `sampled_v1` probes real
assigned source prefixes after worker preparation and commits one immutable map
of 48 key shards before exchange. Oversized base hash buckets can distribute
**distinct keys** across final tasks. Tiny workloads coalesce only when the probe
covers every assigned source row; a small prefix with an unseen suffix keeps the
static final set. Idle destinations verify their empty terminals and produce no
gather output. Physical processes remain 2–4. A single occupied virtual shard
may contain several distinct keys, and v1 cannot refine that shard. A biased
sample can hurt speed but cannot change results.

The job ingests one original scan/project/hash-aggregate batch before layout
commitment, retains its exact state and source cursor, then resumes after every
worker acknowledges the layout. No exchange data is emitted before those ACKs.
The transition follows original partial-operator work without waiting for
producer completion; routing still has one immutable pre-exchange epoch.

```bash
cargo run -p ade-bench -- validate --engine ade-distributed \
  --engine-option partition_policy=sampled_v1 \
  --spec benchmarks/specs/aggregate-high-cardinality.toml --dataset YOUR_DATASET
```

Controls are `partition_sample_rows` (1–16384), `partition_rows_per_file`
(1–4096), `partition_max_files` (1–4), `partition_sample_bytes` (1–16 MiB),
`partition_split_min`, `partition_coalesce_max`, and `partition_min_gain_pct`.
Tuning requires `sampled_v1`. Defaults cap reads at 8 MiB per producer, including
Parquet metadata, and 16384 decoded keys; selected key chunks exceeding 2 MiB
uncompressed are refused before decoding; exact distinct sampled keys form a
post-partial-output proxy. File-prefix bias, actual rows/bytes, exclusive final
operator work, source-pull wall time and probe/ACK costs appear in diagnostics.
There is no full-producer/shuffle barrier added and no mid-exchange rerouting.

Retention/cost selection is independent. Recovery retains the exact committed
layout, source fingerprints, logical transfer identities, complete-copy UUIDs
and attempt-safe gather. Only one active final-worker loss after all producers
complete can recover; an inactive failure target is refused. See
[ADR-0015](docs/adr/0015-runtime-repartition-v1.md) and
[Phase 8 report](docs/reports/phase-8-implementation-report.md).

Use `--distribution hash-bucket-skew` to generate distinct keys colliding into
static bucket zero for 2/4 workers; this new generator variant is version 2.
Existing uniform/hot-key version 1 manifests retain their interpretation.
`bash scripts/phase8-acceptance.sh` requires a clean release checkout; it retains
raw results and applies fixed skew/uniform gates. Package passing **or failed**
evidence with `python3 scripts/phase8-bundle.py RESULTS_DIR OUTPUT.tar.gz`.


`partition_policy=static_packed` is an explicit attribution diagnostic: static
hash routing and all finals, with bounded destination frame packing. It performs
no topology adaptation. Sampled routing also packs fragments into full frames;
its pending buffers cap at N*1024 contributions (<=96 KiB), with separately
reported capacity/peak rows and additional bounded Arrow/encoding transients.
The release suite retains the original static comparator and a static_packed
control so batching effects are visible.


## Phase9 bounded adaptive operators

```bash
cargo build --workspace --locked
# New directory, two separately identified relations, both with non-null key/value_i64.
target/debug/ade-bench data generate join --dataset-id tiny-join-v1 \
  --left-rows 65536 --right-rows 128 --output /tmp/ade-join-data
target/debug/ade-bench validate --engine ade-distributed-join \
  --spec benchmarks/specs/inner-equi-join.toml --dataset /tmp/ade-join-data
# Diagnostic strategy controls:
# --engine-option join_policy=forced_repartition|forced_broadcast|observed_v1
# --engine-option build_side=left|right|auto
# --engine-option join_algorithm=hash_only|grace|auto
```

Observed policy broadcasts only a completely observed build side that fits the
row/state/fanout bounds; an incomplete prefix selects repartition or refuses.
Every worker ACKs one source/execution/strategy digest before exchange. Both
relations preserve duplicate multiplicity and left/right output orientation.
The local operator transitions before output when its reserved hash-row capacity
is exhausted; 32 deeper-hash spill partitions are processed one at a time.
An oversized/hot partition, output/spill cap, schema/null mismatch or deadline
returns an error. A grace partition is refused before output when it contains
any probe rows and more build rows than `hash_rows`, even if the keys are
different and the true join result for that partition would be empty. One-level
v1 has no recursive refinement or sort-merge rescue; this conservative
`JoinSkew` refusal is part of the supported-domain boundary and never returns a
partial result. Sort-merge, general skew recovery and arbitrary joins are deferred.

Join v1 supports 2–4 fixed localhost worker processes with spill exchange only.
Each side has independent stage identities and exact terminal closure; gather
commits only full success. All join worker losses cancel both sides and discard
partial output. The aggregate-only final-loss recovery is retained for its default
fixed-hash graph. Do not infer independent-host durability from these processes.

For existing GroupBySum, `aggregate_policy=fixed_hash` remains the default.
Opt-in `aggregate_policy=adaptive_v1` ingests an original bounded prefix, keeps
reducing hash state or drains its exact contributions once and releases it before
streaming the raw tail. `raw_partial` is the forced diagnostic control. Use
`recovery_enabled=false` for either non-fixed mode; snapshots/replay, complete
copies and failure hooks are explicitly refused. All five static exchange modes,
threshold retention and normal sampled48-shard routing are supported. Sampling
bias can make a poor policy choice; exactness does not imply a performance win.

Runtime join input/output batches cap at1024 rows/64KiB, each source side at1m
rows/64MiB physical bytes/64files, projected metadata and rowgroup decode at1MiB.
Default join hash capacity is65536 rows, local ledger32MiB, spill64MiB, output1m
rows (the query sets the global cap). Two side queues each cap at4MiB separately
from the ledger; ingress also caps cumulative encoded bytes and frames. Source,
codec, queue and harness output allocations are separately bounded; this is not
an RSS guarantee. See [ADR-0016](docs/adr/0016-adaptive-operators-v1.md) for the
supported domain and [release experiment v1](docs/research/phase-9-experiment.md)
for all forced comparisons. `scripts/phase9-acceptance.sh` requires a clean release
checkout; verifier v2 binds each macrobenchmark-run join sidecar to its run/child slot, effective
configuration, both source inventories and source archive identity. The
immutable original release remains auditable under its original verifier v1;
its measurements were not rewritten. Standalone `validate` diagnostics retain
sidecar v1 because they have no result run/iteration identity. `phase9-bundle.py`
retains source-bound raw evidence.

The [2026-10-26 re-review validation](docs/reports/phase-10-rereview-validation-2026-10-26.md)
replays the historical archive unchanged and adds clean-source matched controls
and a fresh confirmation. All six matched comparisons and all six original
confirmation guards pass locally at `b4a2a70`. This supports an imbalanced
scheduler-policy gain; uniform runs recorded no steals. Independent host/day
repeatability remains unverified, and physical membership remains fixed.

## Opt-in leased aggregation

```bash
cargo build --workspace
ADE_DISTRIBUTED_METRICS_FILE=/tmp/ade-leases.jsonl target/debug/ade-bench run \
  --engine ade-distributed --spec benchmarks/specs/aggregate-low-cardinality.toml \
  --dataset PATH_TO_DATA --engine-option workers=4 \
  --engine-option scheduler_policy=leased_v1 \
  --engine-option recovery_enabled=false --engine-option exchange_mode=spill \
  --warmups 1 --iterations 3 --output /tmp/ade-leased-result.json
```

`static` is unchanged modulo assignment; `leased_no_steal` is the diagnostic
control. For matched per-file static ownership, add
`--engine-option producer_pipeline=static_projected` with `scheduler_policy=static`,
`exchange_mode=spill` and `recovery_enabled=false`. This uses the same projected
scan, content hashes, per-file contributions and frame packing as leased
production. It has no lease service/ACK; no-steal retains those mechanisms for
policy attribution. The comparator rejects scheduler tuning/recovery and is not
admitted through the bounded SQL frontend. Leased execution requires fixed_hash, static hash ownership, spill
retention and a verified non-replayable manifest snapshot. Recovery, complete
copies, sampled routing, adaptive/raw partials and joins are refused. Any leased
producer/final loss or ambiguous lease cancels the entire job with no output.
File morsels cannot split one large file. Locality labels are declared hints,
not evidence of remote placement. Integer capacity weights1..1000 influence
normalized remaining-work choices; they are not measured hardware speeds.

`scheduler_initial_active`, `scheduler_capacities=1,4,4,4`, and bounded JSON
`scheduler_participation` controls support in-job parked→active and drain/spot
notice transitions. Events have `after_completed`, `worker`, and `action` fields;
action is `"activate"`, `"drain"`, or `{"spot_notice":{"grace_ms":5000}}`.
Drain preserves the worker's final-owner receiver until job teardown. All N×N
terminals, including zero-work producers, and full lease coverage must validate
before gather commit. No physical worker add/remove, failover or autoscaler is
claimed. See [ADR0017](docs/adr/0017-morsel-scheduler-v1.md) for bounds and fencing.

Optional `data generate synthetic --file-row-weights 15,1,1,1` creates physical
file imbalance without changing logical rows. Weight count must equal `--files`.
Omitting the option preserves historical serialization and equal file ranges.
Timing gates in `scripts/phase10-acceptance.py` are local release evidence only;
normal CI checks correctness. Protocol is ADETCP12, exchange wire2, result schema1.
