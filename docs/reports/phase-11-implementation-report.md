# Phase 11 implementation report — bounded physical IR and frontend v1

Status: **COMPLETE bounded v1**, subject to the explicit refusal matrix below.
This does not claim general SQL, Python, arbitrary DAG distribution, or full
Phase 10/long-term PRD closure. Date: 2026-10-25 (America/Los_Angeles).

## Authority and source identity

Baseline: `4eef1b802ad9034fbd1e08fa1cc074898d6f13b8`. The user's explicit request
to implement the attached Phase 11 prompt authorizes the plan §0 option B ordering
exception. ADR-0018 and AGENTS/README reconcile that authority. No subagents,
push, publication, scheduler tuning or Phase 12 work were used.

Phase 10 remains **PARTIAL**. All six frozen release timing guards failed;
physical membership remains fixed. The Phase 10 report, ADR-0017, experiment,
thresholds, historical source identities and failed evidence are untouched.
Phase 8/9 accepted contracts and historical measurement identities remain intact.

The final source is the single commit containing this report (obtain its exact
SHA with `git log -1 --format=%H`). No intermediate commits were created. The
initial untracked Phase 11 plan is included with a session reconciliation; the
unrelated bootstrap prompt remains untracked and untouched. Local, ignored
`benchmarks/results/phase11-quality/source-identity.json` records SHA-256 for
Cargo manifests/lockfile, toolchain file and every Rust source, plus the aggregate
runtime-source digest
`c7d2fd94dea1719ed2e6a803f88347d44599678c0e615769121ef88892d228b9`.
This is correctness evidence from a dirty working tree,
not a frozen performance experiment or a claim that baseline binaries ran it.

## Implemented contract

`ade-physical` owns strict serde `PlanV1`, portable schemas, stable ordinal
source/node IDs, canonical topological trees, source manifest/logical/schema/file
fingerprints, output bounds, semantic stage distribution, ordering,
boundedness, determinism and replayability. Input schemas derive from predecessor
output schemas. Full validation recomputes all declared schemas/properties,
rejects unsupported versions/types/unknown fields, duplicate/dangling/cyclic IDs,
unreachable/shared nodes, bad columns/order/distribution/source identities and
excess bounds. Canonical explain and BLAKE3 digest cover the entire plan, without
absolute dataset-root paths. Equivalent queries need not receive identical hashes
if they retain different harmless projection nodes; there is no general optimizer.

`ade-exec::physical::execute` validates and lowers the actual graph to existing
Parquet/filter/project/hash aggregate/external sort/limit operators. A narrow
`ProjectSchema` native operator handles exact alias/order and checked UInt64
native count → Int64 SQL count conversion, with no frontend dependency. SUM
schema is nullable Int64, COUNT non-null Int64. Non-null group values remain
non-null in data. Existing exact checked i128 accumulation and final Int64
conversion remain; final overflow refuses rather than wrapping. Legacy LocalPlan,
streaming sink/partial merge APIs and defaults remain available.

`ade-distributed::physical::execute` validates the actual graph, binds the
manifest/file inventory into SourceSnapshot for StaticAggregateJob, or matches
both manifest file inventories to the native join Source fingerprints before
join::Spec execution. It invokes the existing coordinator/join runners, never
an unrelated benchmark-query enum. Root projections preserve exact aliases and
column order. Existing gather/terminal completeness and attempt isolation remain
owned by those runners. No partial result returns on job/postflight/cap failure.

`ade-frontend` binds SQL with public DataFusion 55.2.0 `sql_to_statement` and
`statement_to_plan`, using registered schema-only EmptyTable providers. It does
not register filesystem providers, enable URL discovery, retain default table
functions, execute DDL, optimize into DataFusion physical plans, or collect
DataFusion results. Resolved column ordinals, schemas, aggregate functions and
logical expressions drive translation; there is no SQL string-template matching.
A raw typed ON equality may reside in a public unoptimized join filter; exactly
one opposite-source equality is extracted. Casts and unsupported coercions refuse.
The independent benchmark DataFusion oracle is unchanged; oracle execution appears
only in new test code. The typed Rust DataFrame builder constructs this same IR
without SQL round-tripping. `ade-query` provides plan-sql/explain/execute.

| Target | Supported | Refused |
|---|---|---|
| local | Registered input events scan; exact projections/aliases; secondary < exact nonnegative UInt64 literal; one UInt64 grouping key with SUM(value_i64)/COUNT(*); key,row_id ascending sort; sorted LIMIT | Nullable source/data; expressions/casts/UDFs; DISTINCT; unordered LIMIT; arbitrary sorting; sorted SELECT omitting/renaming native key,row_id,value_i64 |
| distributed aggregate | Unfiltered input scan with original key/value sum/count, then projections/aliases; native static exchange/recovery options | Upstream filters/projections, downstream non-projection nodes, sampled layout, leased scheduling and non-fixed partial mode |
| distributed join | Original ade_left/ade_right scans; one UInt64 key INNER JOIN ON; key/left/right values with alias/reordering; observed/forced native policy and hash/grace | Local join; reverse source order; USING/outer/non-equi joins; extra conditions; transformed inputs/arbitrary joined DAGs; recovery |

Aggregate/join outputs are bags, retaining every duplicate. Sorted LIMIT is a
deterministic prefix. Sort admission verifies strictly increasing source row_id
across the frozen manifest inventory with constant metadata; this is stricter
than arbitrary unique IDs and deliberately refuses unproven tie-breaking inputs.
The Rust builder exposes scan/filter/select/group/sort/limit/output bounds; SQL
and native IR also expose the bounded join. Full examples are in the query guide.

## Bounds, failure and invariants

SQL is capped at16KiB, strict plan serialization256KiB, graph64 nodes/two sources,
source64 files/1m rows/64MiB physical bytes per table, footer1MiB, and declared
uncompressed row group16MiB. Registration checks listed file paths under the
canonical root, version/schema/row totals, BLAKE3 contents, actual Arrow schemas,
non-null data and exact decoded row counts. Extra directory files are ignored.
Manifest/SQL reads enforce byte limits while reading regular files. Source hashing
uses8KiB buffers, checks cancellation/deadline between reads and refuses growth
past the descriptor's byte count. Preflight uses1024-row decode batches and
refuses rows beyond the manifest immediately. Local input/output batches<=8192.

Output is capped at1m rows/64MiB accounted Arrow buffer estimates plus batch
metadata; CLI defaults1000rows/16MiB. Caps refuse, never truncate successful
results. Local collection retains its managed reservation; distributed collection
is bounded by the admitted source/output domain and the existing native gather.
Configured task budgets, exchange bounds and transient decoding/Arrow cast
allocations remain separate. No allocator-exact/process RSS or total spill-volume
cap is claimed. Registration/preflight add I/O; no speed claim is made.

Files hash again before output returns. The existing immutable-file assumption
remains: before/after validation is not transactional filesystem isolation and
cannot detect adversarial change-and-restore during a read. Ordinary OS calls
remain non-preemptible; cooperative checks and native task timeouts preserve the
existing supported envelope. Spill/output ownership tears down on failure. Join
process loss cancels both sides; only the existing single final-worker aggregate
loss episode can recover. Producer/multiple losses, replay mismatch, poisoning
and failed recovery cancel survivors. Original logical IDs, lost-range closure,
terminal fingerprints and attempt-safe gather are unchanged.

| Invariants | Assessment |
|---|---|
| INV1/7/8 | Original logical/attempt identity and lossy-state/replay boundaries preserved; explicit source replayability; join replay refused |
| INV2/3/9 | Semantic Hash/HashOrBroadcast/Gather requirements separate from retention; existing native policy admission and static baselines retained |
| INV4/5 | Finite plan/source/output metadata; existing managed/spill/exchange counters; structured actual native metrics, plan hash, source/schema and effective configuration |
| INV6/10 | Physical crate has no runtime/RPC/DataFusion dependencies; exec/distributed have no frontend/DataFusion/benchmark dependency; Arrow/Parquet/public SQL APIs reused |

Private control remains ADETCP12, exchange wire2, hash1, manifest/result schema1
and benchmark child protocol remain unchanged. Dependency-tree inspection for
ade-physical/ade-exec/ade-distributed finds no DataFusion or ade-frontend. Cargo.lock
adds only the two new workspace packages/edges; no upstream versions change.

## Verification and evidence

Environment: Darwin arm64, rustc1.99.1 (58cf8f9c3 2026-10-13), cargo1.99.1
(5f94df478 2026-08-27), debug builds, locked cached dependencies, localhost workers.
Logs, source inventory and dependency trees are under ignored
`benchmarks/results/phase11-quality/`; raw CLI plans/diagnostics and manifest
identities are under ignored `benchmarks/results/phase11-native-evidence/`.

Final verification passes:

- `cargo fmt --all -- --check` — PASS.
- `cargo check --workspace --all-targets --offline` — PASS.
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings` — PASS.
- `cargo build --workspace --all-features --offline` — PASS.
- `cargo test --workspace --all-features --offline` — PASS:172 tests,0 failures,
  plus0 doc tests. This includes14 new tests (5 IR,1 native core,8 frontend/CLI
  matrix tests) and158 existing tests.
- `bash scripts/smoke.sh` — PASS. Its one-sample engine comparison reports a
  regression classification; that is a correctness smoke, not performance
  acceptance or a Phase10 guard result.
- `cargo tree -p ade-physical/ade-exec/ade-distributed --offline` (three separate
  commands) — dependency direction confirmed; no frontend/DataFusion in core.
- Final removal of an unused frontend UUID dependency and scoping its direct
  Parquet dependency to tests changed no Rust code or upstream resolution;
  workspace all-target check passed again after that cleanup.
- Unmeasured CLI plan/execution capture — PASS for local filter/group/sorted
  limit and real2-worker static aggregate/join. `git diff --check` — PASS.

The same final Rust source was verified; the local source inventory includes
final Cargo manifests/lockfile. All requested quality commands ran. No requested
correctness check was skipped; timing campaigns below were intentionally unrun.

Captured source and native execution facts:

| Fixture | Identity and observed result |
|---|---|
| events | seed42,257rows,2files,key domain17; manifest-byte BLAKE3 `c0907492064c236995c15a52eb24ddbfeb09eccefbf67dee71fb1122cc1553b9`; logical `cd05677519c599ed6f6a78e61eadd312e1917a8966a75651149f203a3b4a81fb` |
| local filter/group/sort | ADE operators return7 filtered rows,17 groups and100 sorted-limit rows |
| distributed aggregate | plan `647ec57ebe830a92983ad918edf1bf498aaeb0e0e1fddbe12a8eb0f7ed9ea472`;2 real producers;17 gather rows |
| join | L31/R17,2files/side,key domain5; manifest-byte BLAKE3 `3d9b95911cd491c71c12317b755c31037baf9ea7cf3ba961496cc1d43a97d139`; logical `4b8c2bfc9f04d87333567eae1db6464b88c59f96f4082cebd740a7fac351b044` |
| distributed join | plan `2cc63d18469a8f69a3de2dc1b640a9b84987299ab51aeb214c999310fe46c404`;2 real finals; observed broadcast_build;108 duplicate-preserving gather pairs |

Full descriptors, effective options and actual native decision/resource counters
are retained in the CLI diagnostics and `identity-summary.json`; test oracle
comparisons are separately executed, never supplied by an ADE fallback.

New tests cover all four requested SQL examples; projection aliases/reordering;
filter thresholds0/25/u64::MAX and invalid negative/overflow/fractional/cast
literals; signed exact sums/final overflow; COUNT signed schema; grouped empty
results; tiny1-row,257-row uniform/skew and native empty input; sorted deterministic
prefix/selected-column reordering; SQL/Rust builder equivalence against separately
executed DataFusion results with exact schemas and duplicate-aware bags. Native
IR invocation is tested inside ade-exec with no frontend dependency. Actual native
operator metrics, multi-process producer/final counts and CLI entrypoints prove
that the ADE path executes. Real2/4-worker aggregation, observed2-worker joins,
forced repartition/grace4-worker joins, empty2-worker joins and global join-output
refusal run through the existing worker binary.

Negatives include unknown/forged versions/schema/properties, cycles/dangling/shared
nodes, source identity drift, source traversal/symlink escape, duplicate table,
oversized SQL/SQL file/manifest/plan, unregistered paths/table functions, DDL/DML,
CTE/window/UDF/casts, nullable source, unsupported targets, cancellation/deadline,
tiny managed memory, output row/byte caps and no CLI stdout on admission or runtime
cap refusal. Legacy workspace tests cover five static modes, final-only replay,
unsupported loss, copy restore/cost policy/poisoning cleanup, split/coalesce and
pinned recovery, adaptive joins/partials and leased scheduler correctness.

Development failures are retained: initial sandbox worker bind returned EPERM;
the same real tests passed with loopback access. The initial unoptimized join
filter was conservatively refused until typed equality extraction was added.
Intermediate Clippy style failures were fixed. A passing full gate preceded the
bounded-read safety review; that source's logs are retained separately. The final
gate repeats because source-byte/row bounds changed, not to discard timing samples.
The macOS large-unwind-table linker warning is pre-existing and does not fail
build/tests. Expected native error diagnostics can appear during failure tests.

No Phase6–10 timed release campaigns, new economic/skew speed claims, release
build benchmark, or cloud/multi-host durability experiment was run. Existing
historical evidence remains attributed to its original source. This slice's
acceptance is correctness and explicit bounded execution, not timing acceptance.

## Deviations and next boundary

The plan is reconciled for the bounded tree, exact sort selected columns and
monotonic source row_id, physical preflight ceilings, and use of resolved public
unoptimized logical plans with explicit semantic validation. These narrow
refusals avoid speculative optimizer/operator rewrites and do not change legacy
runtime admission. Manifests bind generator provenance through their exact bytes;
physical verification does not regenerate logical contents from generator specs.

Broader SQL, nullable/general expressions, arbitrary shared DAGs, local joins,
Python/Substrait, streaming/writes, physical membership, Phase10 performance and
Phase12 HA remain future work. Resolve Phase10's original debt separately before
claiming broader phase ordering or production elasticity. No further phase is
authorized by this implementation.
