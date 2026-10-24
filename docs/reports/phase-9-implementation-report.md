# Phase 9 — Bounded adaptive operators v1

Status: COMPLETE — bounded v1 (2026-10-24). Correctness, all declared Phase9 empirical guards and accepted Phase6–8 regressions pass.

## Authority and source identity

User requested Phase8 remediation followed by Phase9, all in this session, no
subagents and one final implementation commit. No subagents were used. The
initial user branch was c87d2baa5a2489994ddaa32103d03bbf7c696ede. The pre-existing
untracked Phase9 plan was incorporated; the unrelated bootstrap prompt remains
untouched and excluded from the commit. Temporary clean Git clones/snapshot
commits establish release identity without adding intermediate commits on the
user's branch.

Phase8 was closed BEFORE Phase9 source changes at ab7404b1c7095ce3e8b60d79f944a78cd9787c34,
runtime432cc7dd212ec41ad6fdec93bc97f957df5a514d. See its implementation report and
[immutable evidence release](https://github.com/jasonkli21/adaptive-dataflow-engine/releases/tag/phase8-evidence-2026-10-22).
All original failures remain. Its supplemental declaration timing/deviation,
117 oracle-valid children, all-nine pooled gates and individually passing B/C
confirmations are explicit; no Phase8 criterion was waived inside Phase9.

Phase9 frozen release source: 9b0330aa533f0ed403dc7856349ff36952ef4214;
`crates` tree: fab08a06457e2f3f1e62877e3223f6c502afecc9. Early unmeasured snapshots
were superseded before timing to fence capture against manifest bytes and keep
terminal receive hashing off the legacy aggregate path. The final implementation
commit is the commit containing this report; its runtime tree must equal this
measured tree. Final commit SHA is also recorded in the release identity artifact
and the session handoff, avoiding a self-referential SHA inside its own Git content.

## Delivered mechanisms and contracts

| Boundary | Implementation | Result |
|---|---|---|
| Local core | `ade-exec/{join,row_source}.rs` | Typed two-source bag join; projected bounded Parquet; reserved fixed hash slots/duplicate chains; genuine pre-output hash-to-grace conversion;32 deeper-hash partitions/checksums; bounded output and typed refusals |
| Producer aggregation | `partial.rs`, `aggregate.rs`, local lowering | Original hash prefix, observed exact reduction/pressure, exact contribution drain then raw tail once; legacy fixed-hash lowering unchanged |
| Distributed graph | `join.rs`, `join_policy.rs`, worker/protocol | Independent source/stage identity, actual bounded probes, pure strategy/build decision, immutable all-worker ACK, existing exchange transport/retention, bilateral closure and attempt-local gather |
| Exchange | typed payload contracts in `exchange.rs` | Wire2 aggregate schema1 unchanged; join rows schema2/pairs schema3, context-bound terminal fingerprint, receive ownership and cumulative ingress bounds |
| Harness | `ade_join.rs`, `join_dataset.rs`, spec/engine/CLI | Separate adapter, two-source manifest, independent DataFusion SQL oracle, exact sorted multisets including duplicates; schema1 results/process isolation preserved |
| Evidence | Phase9 experiment/runner/verifier/bundler, ADR0016 | Forced controls, raw child/decision/traffic/work evidence, source Git bundle, failed/negative artifacts and timing gates kept outside CI |

The local helper is deliberately not a general LocalPlan/SQL/optimizer IR. Join
semantics are non-null UInt64 keys and Int64 values, inner equality, output
`key,left_value,right_value`. Every matching pair appears, even identical triples;
changing the build side preserves output orientation. Empty/disjoint relations
produce no rows. The DataFusion oracle separately registers exactly the two
manifest lists and executes SQL with cap+1; canonical validation rejects excess
output and missing/extra/replaced duplicate contributions. Original synthetic
and Q1 references are unchanged. Old manifests omit the optional join field,
so their serialized physical identity remains unchanged.

Observed policy requires complete source coverage, row/state/fanout bounds and
room for the configured reserved hash state before broadcast. Incomplete or
biased prefix smallness never authorizes an unseen suffix. Both forced controls
perform the same probes and obey admission bounds; forced broadcast can refuse.
A complete smaller admissible side wins; ties and inconclusive observations use
right. The digest binds both source identities, execution UUID, job, config,
worker count/hash1, stages2/3/4, decision and observations. Each worker checks its
own report and the pure decision before ACK. Duplicate/stale/late/missing commits,
precommit traffic and forged context/schema/stage/route/terminal reject.

Repartition routes both sides by identical hash1%N; broadcast sends each build
row to N destinations and each probe row to exactly one. Empty channels terminate.
Spill retention accepts the not-yet-consumed side, so a producer closing all left
transfers before right cannot form a bounded handoff cycle with a right-build
reader. At most N incoming side sessions occupy the four receiver slots; each
producer closes one side before opening the next. Final gather has disjoint stage4
identities, typed pair codec and expected final set. Every producer/final terminal,
row/byte/frame conservation and global output bound must pass before collection
and result return. Any failure discards the attempt's retained output.

## Supported and refused combinations

| Graph | Supported | Explicitly refused/limited |
|---|---|---|
| Default GroupBySum | All five static retention modes, threshold/expected-cost, static/static_packed/sampled48 routing; accepted one-final-loss replay/restore | Existing producer/multiple losses, replay mismatch, poisoning, recovery failure and legacy oversized aggregate spill bucket remain failures |
| Non-fixed GroupBySum | adaptive_v1/raw_partial; all five static modes, threshold retention and normal sampled48 routing on2–4 processes | Source snapshots/replay, complete-copy cost modes and failure hooks; benchmark requires recovery_enabled=false. No byte-identical adaptive replay claim |
| InnerEquiJoin |2–4 fixed localhost workers, observed/forced repartition/broadcast, left/right build, hash-only/auto/grace, spill exchange | Other exchange modes, expected-cost copies/recovery, one-sided sampled layout, prefetch/slow-consumer hooks, arbitrary SQL/types/nulls, general skew/sort-merge/scheduler |
| Join loss/error | Cancel both source stores and all survivors, quarantine partial gather, clean retained local files | No join replay/restore. Dead-worker files are removed on parent-owned fixture teardown; no independent-host/cloud durability claim |

Actual adaptive partial ingests its original prefix into hash state; it is not a
configuration label or a second probe read. When reduction is insufficient or
pressure is observed, drain exact buffered contributions once, free hash/spill
state before raw tail and preserve `(key,sum,count)` with checked arithmetic.
A large i128 prefix sum splits into representable Int64 contributions while
conserving count, so an ultimately representable final sum is not falsely refused.
At most one contribution per original row is needed. A biased prefix can choose
poorly; no claim of adaptation to later unseen distribution changes is made.

## Resource and lifecycle limits

Local join defaults: hash capacity65536 rows,32MiB ledger, output batch1024 rows,
64KiB accepted input batch, output cap1m, input cap1m per side, spill64MiB (config
ceiling1GiB), deadline120s. Hash rows/heads and IO/output transients reserve before
allocation. The auto conversion evacuates every old row, the exact unprocessed
trigger-batch suffix and subsequent inputs before output, then releases the old
hash table. Spill has side/partition/version headers, count/checksum footers and
bounded one-file IO; validate all relevant partitions before output. An oversized
matching build partition, including unsplittable hot keys, returns JoinSkew.
Spill-disabled/hash-only/output/spill/deadline refusals are structured core errors,
carried as bounded remote error codes/messages; none is counted as a valid result.

Full source: <=64files/1m rows/64MiB physical bytes per side. API reads capped64MiB,
metadata1MiB, selected rowgroup key/value decode1MiB, actual selected batch64KiB;
primitive schema/null checks precede acceptance. Probe per worker+side defaults
4096rows/4files/8MiB actual API reads,256row batches, same metadata/decode bounds.
Checksums are captured against the manifest and reverified before original
emission. Immutable files during an executing read remain an external assumption;
this is not transactional filesystem snapshot isolation.

Exchange queues:4MiB per join side store and per gather store, each separately
bounded from the local ledger. One spool per store keeps constant segment metadata;
cumulative ingress <=1m rows/128MiB encoded/1mframes before retention, with at most
four receiver threads and bounded codec/frame allocations. Up to four finals may
retain provisional output before the global1m cap is checked; only then does the
harness collect. Core execution streams synchronous output batches. Harness
canonical/output collection is bounded but outside task ledgers. Parquet metadata,
decode, source verification64KiB buffer, IPC transients, queues, metadata and
parent/child overhead are separate bounds, not a proof of process RSS. Diagnostics
record actual source/decode/batch/frame/queue/table peaks, work and spill bytes.

Cancellation/deadlines apply at source/pull, hashing, partition IO, build/probe,
pair emission, RPC and receiver boundaries. RAII removes spill on sink refusal,
cancel, corrupt files or errors; source cursors shutdown on every helper outcome.
Poisoned uncontained retention IO cannot be reused. Worker job watermarks are
shared across aggregate and join sessions so cancelled IDs cannot be recycled.

## Verification

Frozen-source quality logs are `/tmp/ade-p9-frozen-{fmt,check,clippy,tests,smoke}.log`.
Formatting, workspace/all-target check, all-target/all-feature clippy with warnings
denied and136 workspace tests plus doc tests passed. Smoke includes the independent
join bag oracle, true distributed join diagnostics and non-fixed partial path.
The macOS debug linker warned about large unwind tables; this did not fail tests
or the release build and is not suppressed as an engine correctness result.

Focused tests include2/3/4-worker every forced distribution × both build sides ×
hash/grace/auto; duplicate bags, empty/disjoint and signed extremes; actual pressure
fallback/spill-disabled/hot/output refusal; corrupt/truncated spill, cancellation
and sink error cleanup; prefix1/63/64/65/1024/4096, pressure/spill, exact sums/counts,
raw tail and overflow-sensitive prefix splits; all five partial exchange modes,
threshold and sampled split/coalesce; strategy/context/schema/stage/route/ingress
and terminal corruption; source drift and a real final exit AFTER a gather frame,
which returns no partial output and leaves a responsive cancelled survivor.

One initial test fixture overwrote files for a later matrix arm. The source-drift
fence correctly rejected it; fixtures were isolated and rerun. Development compiler/
clippy/test failures are retained in quality logs and not performance evidence.
Release timing is never run in CI. The final quality gate was repeated after two meaningful source fixes (manifest capture provenance and keeping new receive hashing off aggregate schema1). It ran on the working tree byte-identical to the frozen release runtime; all final commands exited0. The release build took6m27s including build-lock wait; no benchmark overlapped compilation.

## Frozen empirical acceptance and raw evidence

Experiment v1 was declared before any Phase9 release measurements; primary2
workers: one warmup/five measured per arm; limited4: one/three. Exact sizes,
budgets, arm inventory and unchanged1.15/1.20 guards are in
[the experiment](../research/phase-9-experiment.md). All118 successful children,
95 measured, are required along with the3 expected-negative regimes. Each child
must pass the independent oracle before its timing counts. All samples enter the
median; source/environment/config, unique child IDs, strategy/source digest,
actual bilateral/gather traffic and memory/cleanup facts are checked first.

All118 children (95 measured) pass the independent oracle and identity/mechanism
checks. All8 latency guards pass; three expected negatives reject. The first
readiness attempt stopped with ZERO measurements (load36.62→26.94, first idle66.76%,
no swapout change). The unchanged retry passed after the machine settled; no
measured child was removed. The declaration was committed BEFORE timing, SHA256
`2d383b65fb30ffec50bac4af1a07491e375d8744687cad2162618d09952bf881`.
Premeasurement Git bundle SHA256:
`bc1cfa44bebc01c21dc609f57c52194779fffed5018a2d6ec92c9a95211e7602`.

All measured query medians below are milliseconds; ranges and EVERY sample are
in raw verification.json.

| Regime/workers | Control | Median ms |
|---|---|---:|
| tiny/2 | forced_repartition | 27.756 |
| tiny/2 | forced_broadcast | 28.183 |
| tiny/2 | observed_v1 | 27.773 |
| balanced/2 | forced_repartition | 34.687 |
| balanced/2 | observed_v1 | 34.357 |
| pressure/2 | auto | 61.249 |
| pressure/2 | grace | 60.382 |
| low/2 | fixed_hash | 18.474 |
| low/2 | raw_partial | 23.719 |
| low/2 | adaptive_v1 | 18.873 |
| high/2 | fixed_hash | 56.480 |
| high/2 | raw_partial | 27.173 |
| high/2 | adaptive_v1 | 26.733 |
| tiny/4 | forced_repartition | 39.966 |
| tiny/4 | forced_broadcast | 40.521 |
| tiny/4 | observed_v1 | 39.996 |
| balanced/4 | forced_repartition | 45.064 |
| balanced/4 | observed_v1 | 45.977 |
| pressure/4 | auto | 85.929 |
| low/4 | fixed_hash | 29.055 |
| low/4 | adaptive_v1 | 31.144 |
| high/4 | fixed_hash | 34.819 |
| high/4 | adaptive_v1 | 36.241 |

| Guard ratio |2 workers |4 workers | Limit |
|---|---:|---:|---:|
| tiny | 1.000596 | 1.000743 | 1.15 |
| balanced | 0.990500 | 1.020252 | 1.20 |
| low | 1.021596 | 1.071904 | 1.15 |
| high | 0.983782 | 1.040838 | 1.15 |

Tiny observed policy really broadcasts/right:8192 left prefix rows/incomplete,
128 right rows/complete, conservative state upper bounds532480/16384 bytes.
Every right row appears at2/4 destinations, left rows once. One measured child
records probe2.415ms/ACK0.916ms. Forced broadcast did NOT beat repartition
(28.183 vs27.756ms on2 workers;40.521 vs39.966 on4). The predeclared overhead
bound passes; this is not evidence of a broadcast speedup. Balanced sides remain
incomplete and select repartition.

Pressure auto reaches512 build rows and really converts on each final. A2-worker
sample has managed peak219904 <=262144, current0 after cleanup,64 spill files/
one active spill FD per final,269312 total bytes written and797696 read (including
checksum/validation rereads),16384 input rows and16464 exact bag pairs. Auto61.249ms
versus forced grace60.382ms quantifies conversion; hash-only refuses and is not a
feasible speed baseline. Four-worker auto also converts/completes. The hot key
returns the typed32-row capacity refusal; the independent oracle has16384 pairs.
Output-cap and post-gather worker-loss negatives never commit partial results.

Low adaptive keeps hash:32 partial rows/3616 bytes versus raw100000/2578112;
18.873ms versus18.474 fixed. High adaptive actually drains the prefix and releases
state:99988 contributions/2577536 bytes versus fixed78731/1972800. It trades30.65%
more encoded traffic for no producer spill and less hash work:26.733ms versus
56.480 fixed/27.173 raw; fixed writes2725600 spill bytes. The raw54.202ms outlier
is RETAINED in all five samples. Four-worker high adaptive is4.08% SLOWER than
fixed despite lower operator work, with more network bytes; the overhead guard
passes without claiming an unrelated win. These small-n local medians/limited
confirmation are not confidence intervals or production/cloud significance.
Operator work is exclusive wall, not CPU; parallel sums can exceed job latency.
Sidecars retain source verification/pulls, probe/ACK, actual source/frame/buffer/
table peaks, sent/received bytes, spill and parent RSS (not combined process RSS).

Legacy release regressions on the SAME clean source all pass:

- Phase6: five static modes, normal/recovered and limited4-worker adaptive,
  exact one-loss envelope and missing-snapshot unrecoverable negative.
- Phase7: expected-cost proxy4,979,451,149.2225 nanoUSD below
  always6,603,132,411.7045 / never9,388,422,585.352 / threshold9,378,642,551.418499;
  original normal-overhead and decision criteria pass. This is the predeclared
  request-cost proxy, not measured cloud billing.
- Phase8: all39 children correct; primary skew exclusive-final-work ratio0.399457
  (60.05% reduction), job0.702683 (29.73% lower), uniform0.973386. Original gates
  pass. Four-worker skew work0.127074/job0.785824, pinned replay/restore and tiny
  coalescence pass. This refresh is separate from the earlier formal closure;
  its failed experiments and supplemental declaration/deviation remain preserved.

Four copied-artifact attacks reject: decision digest, source identity, contradictory
transfer rows and internally consistent best-only timing selection. Separate SQL
oracle tests reject missing/replaced duplicate pairs. Passing artifacts were not
modified; `negative-verifier/rejections.json` and corrupt copies retain evidence.

Ignored raw closure: `benchmarks/results/phase9-closure-2026-10-24/`, with phase9,
readiness-failed-1, phase6/7/8, negative-verifier, quality logs and premeasurement
source bundle. It verifies without historical source files and regenerates from
versioned specs/source. Immutable external
[Phase9 evidence release](https://github.com/jasonkli21/adaptive-dataflow-engine/releases/tag/phase9-evidence-2026-10-24).
The release tag points to the original public planning baseline, NOT the measured
source or final implementation. Use bundled source.bundle for9b0330a/runtimefab08a0.
The separate
[implementation identity](https://github.com/jasonkli21/adaptive-dataflow-engine/releases/download/phase9-evidence-2026-10-24/implementation-identity.json)
binds the user's one implementation commit/identical runtime tree to the bundle.
Closure bundle SHA256: `0ff08b13acd749de6339f235282576aeaa9e5f5fc98de54c6ff1875b1bfd2b94`.


## Deviations and handoff

A separate adapter and optional manifest extension avoid ambiguous overload of
the aggregate adapter and preserve old serialized identities. The local typed
helper keeps execution independent of the benchmark/SQL frontend. Join spill-only
and explicit non-fixed replay refusal are permitted v1 boundaries, not missing
claimed modes. Five primary samples exceed the plan's minimum three. No numeric
exit guard was weakened after measurements. No Phase10/11 work was started.
General sort-merge/skew spooling, adaptive replay, other join retention modes,
independent-host fault domains and scheduler/IR remain future work.
