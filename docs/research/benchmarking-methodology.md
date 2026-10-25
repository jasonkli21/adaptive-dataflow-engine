# Phase 0 benchmarking methodology

Use release builds without a debugger. Keep hardware, compiler, build settings, dataset and semantic workload constant. Close heavy background jobs, use consistent power settings, and repeat suspicious measurements. Apple performance/efficiency core scheduling, thermal behavior, power management and background tasks can overwhelm small differences. Local deltas below 3–5% deserve particular caution.

Macrobenchmarks execute each warmup and measured iteration in a fresh process. Defaults are one warmup and five measured iterations. Warmups warm OS file caches; they do not preserve an engine session or allocator. Cache policy is `warm`: no OS cache flushing is attempted. The harness does not claim cold-cache measurements.

The parent verifies manifest/schema/file size/row-group metadata before spawning. `run --verify-data full` additionally verifies stored file checksums once before spawning. The default `metadata` mode avoids rehashing large datasets on every run; it assumes immutable inputs and cannot detect same-size file modifications. Each child checks metadata and executes one explicitly selected engine adapter. DataFusion creates a session; ADE local and distributed adapters run their own execution paths. Query timing covers the selected adapter's query execution and collection, excluding adapter setup. Setup time is recorded separately. Process time includes startup, input checks, engine setup, query, collection, canonicalization and independent validation. Never compare process and query time as equivalent metrics. CLI `validate` performs full file verification as well.

Required validation runs after the query timer. Synthetic outputs are sorted lexicographically and compared exactly, including duplicates, against a generator-based integer oracle. Scan returns row_id/key/value_i64; filter returns key/value_i64 for secondary below the spec threshold; aggregates return key/sum(value_i64)/count. Unknown semantic operations, validation methods and schema versions are rejected. Low/high/skew specs share aggregation semantics; generator parameters select the actual stress case.

The engine adapter boundary receives the engine-independent case and dataset manifest plus an adapter-owned configuration type. The parent forwards `--engine` and every raw `--engine-option` to the isolated child. Dispatch rejects unknown names; each adapter parses only its own options. Adding an adapter does not change dataset, workload, runner or result semantics.

TPC-H-derived Q1 uses a fixed typed `Date32` cutoff for 1998-09-02 and explicit DOUBLE numeric inputs. The direct Parquet reference casts the ship-date column to `Date32` and compares integer day values; it does not use display formatting to decide which rows qualify. It computes the reference groups, four sums, three averages, and counts. Counts match exactly. Other values must be finite and within `1e-6 + 1e-9 * abs(reference)`. The comparison fingerprint is the deterministic reference output, after tolerance validation, rather than the binary floating reduction output. This is a TPC-H-derived analytical workload, **not an official TPC benchmark result**.

All iterations are retained. Any failed child or required validation makes the run invalid and removes the timing summary. Diagnostic artifacts remain inspectable, but comparison refuses them. Summary statistics cover measured query nanoseconds: min, max, mean, median, population standard deviation, p90 and p95. Percentiles use R-7 linear interpolation at `(n-1)*p`. Median is the primary comparison. ±3% is a presentation band, not proof of significance.

When a preliminary `/usr/bin/time` resource probe succeeds, macOS/Linux peak RSS is recorded for the entire child, including validation. It is not engine-only memory: canonicalization and the oracle can dominate, and for distributed adapters it does not include worker-process RSS. Restricted environments that deny resource queries fall back to null RSS. CPU time is currently unavailable (null). Missing optional metrics are never interpreted as zero. Some ADE adapters emit opt-in per-child JSONL metrics sidecars; these are separate from result schema v1 and are not universal hardware counters.

Synthetic generator v1 uses a fixed SplitMix64 mixer keyed by row index and seed. Hot membership is spread across rows using differences of integer floors, yielding exactly `floor(rows * hot_key_fraction)` hot rows. Keys outside that membership never use hot keys. File boundaries do not alter rows or logical fingerprints; file count, compression and row-group shape remain part of physical manifest identity. No physical byte stability is promised across Parquet upgrades.

Result JSON is immutable and atomically published without overwriting existing files. Compare requires the same semantic workload identity: workload schema version, ID, semantic version, dataset requirement, query, and validation contract. Description and tags are informational; changes are comparable and produce a metadata warning. Semantic changes remain hard-incompatible even when an author forgets to increment the workload version. Compare also requires logical dataset identity and validated canonical output, and warns on physical manifest, hardware/OS, compiler/build, engine/configuration or dirty-source differences.

Git commit and dirty state are collected at build time from the ADE repository root and embedded in the executable. Runtime `cwd` cannot substitute another repository's identity; a source archive or source tree nested under an unrelated Git root records null Git fields. Build metadata records the Cargo profile, target, compiler, debug assertions, allocator, and known effective settings such as optimization level, debug information, LTO, codegen units and whitelisted explicit target CPU/features. No repository paths, hostname, username, home path, serial, network address or arbitrary environment variables are collected.

Use dedicated, controlled hosts before drawing product conclusions. CI checks correctness and tiny smoke flows only, with no timing thresholds. Criterion microbenchmarks are an independent path and do not use macrobenchmark warmup/process semantics.


## Phase 8 topology evidence

ADR-0015 and `scripts/phase8-acceptance.sh` predeclare three primary regimes,
2-worker static/sample comparisons, a limited 4-worker skew confirmation, one
warmup and three isolated measured iterations. The engine-independent
hash-bucket-skew generator v2 creates many distinct keys surviving partial
aggregation; uniform and one raw hot key are explicit controls. Old fixtures
retain generator v1. Correctness/oracle and clean source identity apply to every
child. Preserve failed attempts. The verifier records measured-only medians,
exclusive final operator work (including spill), source-pull wall latency
(waiting + I/O/decoding), job wall latency, per-final counts/bytes, selected active
set and probe/commit overhead. Do not label final elapsed as compute-skew tail.
A sample is a biased distinct-prefix proxy, never full cardinality or raw-frequency
load. Metered source reads include metadata/cache traffic, not physical disk I/O.
Release requires >=20% skew maximum-work improvement without job-latency increase
and <=1.30x uniform latency. Failed gates keep Phase 8 incomplete; timing stays
outside CI. Evidence bundles contain raw data, clean source archive and checksums.

## Phase 9 bounded join and partial aggregation

Phase 9 joins use an independent DataFusion SQL oracle and duplicate-aware bag
comparison. Macrobenchmark `run` emits v2 join sidecars bound to the result run
ID and warmup/iteration slot; standalone `validate` diagnostics without a result
identity retain sidecar v1. The release verifier checks the effective join
configuration and both ordered source file inventories against that arm. The
source archive SHA-256 is bound to its source commit and `crates` tree in the
implementation identity; bundled verification checks the archive against the
Git bundle. The immutable Phase 9 release used verifier v1 and did not include
these new arm bindings, so its historical attribution remains qualified and
its raw files are preserved unchanged.

Interpret each measurement according to its boundary. Query time is adapter
wall time. Reported operator work is exclusive task wall time, not CPU time.
Managed peaks are estimates for the local operator ledger, not process or cluster
RSS. Spill bytes are local operator spill I/O. Exchange payload bytes count
encoded data payload at the adapter boundary; they are not physical link-byte
counters, protocol overhead, cloud transfer billing, or total network traffic.
Whole-child peak RSS includes benchmark validation and does not aggregate the
distributed worker processes. These observations support bounded mechanism
and overhead claims, not general hardware, cloud-cost, or latency-optimality
claims. See the Phase 9 report and [ADR-0016](../adr/0016-adaptive-operators-v1.md).
